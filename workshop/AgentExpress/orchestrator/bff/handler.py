"""
BFF Lambda for the orchestrator UI.

Two modes in one function:
  * API mode (API Gateway HTTP API): serves the UI's REST calls, reads live
    progress from DynamoDB, and serves the workflow definition so the UI renders
    dynamically.
  * Runner mode (async self-invoke): calls the AgentCore runtime
    (InvokeAgentRuntime) for start/resume so the browser never blocks.

Every MUTATING endpoint below is additionally checked against bff/authz.py, which
maps a JWT group claim to the actions a caller may take (workflow.json ->
`authorization`). The API Gateway authorizer only proves WHO the caller is; authz
decides what they may DO. Actions not named in that block stay open to any
authenticated caller, so an absent block behaves as it did before.

That includes STARTING a run ("start"), which is the most expensive action in the
app — it invokes the runtime and spends model tokens across every agent. It had no
action name, so it could not be restricted from config at all while every cheaper
action could.

Endpoints:
  GET    /api/workflow                           -> workflow definition (agents+steps)
  GET    /api/me                                 -> {user, groups, permittedActions}
  GET    /api/models                             -> text models this account can invoke
  POST   /api/sessions               {topic}     -> {session_id}
  GET    /api/sessions                           -> [session summaries]
  GET    /api/sessions/{id}                      -> full snapshot (+ timeline)
  DELETE /api/sessions/{id}                      -> {ok} (remove a previous run)
  POST   /api/sessions/{id}/decision {decision, comment}  -> {ok}
        decision: approve | deny | revise; comment: feedback used on revise
  POST   /api/sessions/{id}/cancel               -> {ok}
  POST   /api/sessions/{id}/rerun    {agentId|agents, comment} -> {ok}
  POST   /api/sessions/{id}/evaluate {agentId, prompt} -> {ok}  (AgentCore Evaluations)
  POST   /api/insights/run           {lookbackHours}   -> {ok}  (cross-run Insights)
  GET    /api/insights                           -> latest Insights findings
  POST   /api/sessions/attachments   {name}  -> presigned POST for one file a run brings
  GET    /api/sessions/{id}/telemetry            -> per-run cost/token drilldown
  GET    /api/telemetry/aggregate                -> by date / model / user rollup
  POST   /api/chat                   {message, history} -> {reply, actions}

Builder builds (bff/builds.py; only when the builder plane is deployed):
  GET    /api/builds                             -> your builds
  GET    /api/builds/{id}                        -> {build, project}
  PUT    /api/builds/{id}            {project}   -> build (create or autosave)
  DELETE /api/builds/{id}                        -> {ok} | {destroying} (destroys first)
  POST   /api/builds/{id}/deploy     {tool}      -> job   tool: cdk | terraform
  POST   /api/builds/{id}/destroy                -> job
  GET    /api/builds/{id}/log                    -> tail of the latest job's log

A run can target a deployed build: POST /api/sessions takes {"build": id}, and
GET /api/sessions and /api/workflow take ?build=id. Every per-run route then finds the
run's build by its id, so the per-run calls are unchanged.
"""

import decimal
import json
import os
import re
import traceback
import urllib.parse
import uuid

import accounts
import audit
import authz
import boto3
import builds
import chatbot
import clock
import codecheck
import designer
import gates
import library
import policies
import registry
import runfiles
import sharing
import triggers
import workflow
from boto3.dynamodb.conditions import Attr, Key

# This deployment's own runs. Absent on a control-plane console (CONSOLE_MODE=builder),
# which runs no workflow of its own and refuses every run route.
STATUS_TABLE = os.environ.get("STATUS_TABLE", "")
EVENTS_TABLE = os.environ.get("EVENTS_TABLE", "")
RUNTIME_ARN = os.environ.get("RUNTIME_ARN", "")
REGION = os.environ.get("AWS_REGION", "us-east-1")
#: "builder": this console is a control plane. People design, build and deploy here, and
#: every build's runs, observability and assistant live in THAT build's own app (its
#: uiUrl), so this BFF refuses the run routes for its own workflow as well as for builds.
#: "app" (the default): a workflow's app, with the Builder beside it when it is enabled.
CONSOLE_MODE = os.environ.get("CONSOLE_MODE", "app")
#: Answered on every run route of a control-plane console.
NOT_HERE = ("this console builds and deploys; runs live in each build's own app. Open a "
            "deployed build and use Open app.")
# `WORKFLOW` is the PROJECTION — the subset of workflow.json the browser may see.
# `GET /api/workflow` returns it verbatim, so nothing may be added to it that the
# page has no business holding. See bff/workflow.py for where it comes from and why
# it is no longer built by the IaC and shipped in an environment variable.
WORKFLOW = workflow.VIEW
NODE_IDS = workflow.NODE_IDS
DEFAULT_TOPIC = workflow.DEFAULT_TOPIC
#: Longest run request accepted (web/src/lib/request.ts REQUEST_MAX says the same).
TOPIC_MAX = 10000

ddb = boto3.resource("dynamodb")
status_tbl = ddb.Table(STATUS_TABLE) if STATUS_TABLE else None
events_tbl = ddb.Table(EVENTS_TABLE) if EVENTS_TABLE else None
TELEMETRY_TABLE = os.environ.get("TELEMETRY_TABLE", "")
telemetry_tbl = ddb.Table(TELEMETRY_TABLE) if TELEMETRY_TABLE else None
lambda_client = boto3.client("lambda")
agentcore = boto3.client("bedrock-agentcore", region_name=REGION)


class _DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return int(o) if o % 1 == 0 else float(o)
        return super().default(o)


def _resp(status: int, body) -> dict:
    return {"statusCode": status, "headers": {"content-type": "application/json"},
            "body": json.dumps(body, cls=_DecimalEncoder)}


def _skeleton(session_id: str, topic: str, node_ids: list[str] | None = None) -> dict:
    return {
        "session_id": session_id, "topic": topic, "overall": "running",
        "nodes": {n: {"status": "pending", "pct": 0, "output": ""}
                  for n in (NODE_IDS if node_ids is None else node_ids)},
        "history": {}, "hitl": None, "result": None,
        "created": clock.now_str(), "updated_at": clock.now_str(),
    }


# --- Where a run lives ---------------------------------------------------------
# A run belongs either to THIS deployment's own workflow, or to a Builder build deployed
# as its own stack. A "target" is everything that differs between the two: the tables
# the run's status lives in, the runtime that executes it, and the workflow it runs.
# Read from module globals at call time, so tests that swap `status_tbl` still work.

def _default_target() -> dict:
    if status_tbl is None:
        raise builds.BuildError(404, NOT_HERE)
    return {"build": "", "name": "", "version": None, "status": status_tbl,
            "events": events_tbl, "telemetry": telemetry_tbl, "runtimeArn": RUNTIME_ARN,
            "view": WORKFLOW, "raw": workflow.RAW}


def _build_target(build_id: str, owner: str | None = None) -> dict:
    t = builds.target(build_id, owner)
    return {**t, "status": ddb.Table(t["statusTable"]), "events": ddb.Table(t["eventsTable"]),
            "telemetry": ddb.Table(t["telemetryTable"])}


def _session_target(session_id: str) -> dict:
    build_id = builds.build_of_run(session_id)
    return _build_target(build_id) if build_id else _default_target()


#: Where this deployment's image agents store their images (terraform/images.tf,
#: the AssetsBucket in cdk/lib/orchestrator-stack.ts); "" when no agent draws.
ASSETS_BUCKET = os.environ.get("ASSETS_BUCKET", "")
IMAGE_KEY_RE = re.compile(r"^runs/([A-Za-z0-9_-]{6,64})/[A-Za-z][A-Za-z0-9_]*/\d+\.(png|jpg)$")
_s3_client = None


def _image_link(event: dict, key: str) -> dict:
    """A 15-minute link to one image an image agent rendered — only for the run's owner.

    The key names the run (runs/<session>/<agent>/<n>.<ext>, app/common/images.py), so
    the check is the same one every run route makes. A run of a Builder build in this
    account reads from that build's bucket, named like this deployment's own."""
    global _s3_client
    m = IMAGE_KEY_RE.match(str(key or ""))
    if not m:
        raise builds.BuildError(400, "invalid image key")
    target = _own_session(event, m.group(1), read=True)
    if target["build"]:
        account = str(target.get("runtimeArn") or "").split(":")[4:5]
        bucket = (f"agentcore-{str(target.get('agentName') or '').replace('_', '-')}-assets-"
                  f"{account[0] if account else ''}")
    else:
        bucket = ASSETS_BUCKET
    if not bucket:
        raise builds.BuildError(404, "this deployment stores no images")
    if _s3_client is None:
        _s3_client = boto3.client("s3", region_name=REGION)
    url = _s3_client.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key},
                                            ExpiresIn=900)
    return {"url": url, "expiresIn": 900}


def _owner(event: dict) -> str:
    """Whose runs and builds these are: the JWT `sub` (see builds.owner_of)."""
    return builds.owner_of(authz.claims(event))


def _is_admin(event: dict) -> bool:
    """The `admin` permission: read every user's builds and runs, and destroy any build.
    Closed unless workflow.json grants it (bff/authz.py CLOSED_UNLESS_GRANTED)."""
    return authz.permitted("admin", event)


#: (admin, kind, id) -> the hour an admin's view of someone else's item was logged, so a
#: page that polls (a run refreshes every few seconds) is logged once an hour, not per poll.
_ADMIN_SEEN: dict[tuple, str] = {}


def _admin_viewed(event: dict, kind: str, item_id: str, whose: str, **detail) -> None:
    """Audit an admin reading another user's build or run: in the admin's own log, as
    `admin.viewed`. Best-effort, like every audit write."""
    if not audit.enabled():
        return
    claims = authz.claims(event)
    me = builds.owner_of(claims)
    hour = clock.now_str()[:13]
    key = (me, kind, item_id, str(detail.get("what") or ""))
    if _ADMIN_SEEN.get(key) == hour:
        return
    _ADMIN_SEEN[key] = hour
    if len(_ADMIN_SEEN) > 5000:
        _ADMIN_SEEN.clear()
    ip = ((event.get("requestContext") or {}).get("http") or {}).get("sourceIp", "")
    audit.record(me, str(claims.get("email") or ""), "admin.viewed", ip=ip, kind=kind,
                 **{kind: item_id}, forUser=whose, **detail)


def _own_session(event: dict, session_id: str, read: bool = False) -> dict:
    """The run's target, ONLY if the caller started the run; otherwise 404.

    EVERY RUN IS PRIVATE TO THE USER WHO STARTED IT. Anyone can sign up to a hosted
    console, so a run id alone must never be enough to read, decide, re-run, evaluate,
    cancel or delete someone else's run. Answered as 404, not 403: whether a run id
    exists is not the caller's business either. A run from before runs had owners has
    no `owner`, so it belongs to nobody and is hidden.
    """
    target = _session_target(session_id)
    item = target["status"].get_item(
        Key={"session_id": session_id}, ProjectionExpression="#o",
        ExpressionAttributeNames={"#o": "owner"}).get("Item")
    if item and item.get("owner") != _owner(event) and read and _is_admin(event):
        # An admin may READ anyone's run (never decide, re-run, cancel or delete it).
        _admin_viewed(event, "session", session_id, str(item.get("owner") or ""))
        return target
    if item and item.get("owner") != _owner(event) and _trigger_viewer(event, str(item.get("owner") or "")):
        # A service trigger's run: its `approvers` read and decide it (bff/triggers.py).
        return target
    if not item or item.get("owner") != _owner(event):
        raise builds.BuildError(404, "unknown session")
    return target


def _service_owners(event: dict) -> list[str]:
    """The service triggers whose runs the caller may see: those listing one of its groups."""
    groups = authz.groups_of(event)
    return [f"trigger:{n}" for n, t in triggers.all_triggers().items()
            if isinstance(t, dict) and t.get("runAs") == "service" and triggers.may_see(t, groups)]


def _trigger_viewer(event: dict, owner: str) -> bool:
    return owner.startswith("trigger:") and owner in _service_owners(event)


def _my_session_ids(tbl, owner: str) -> set[str]:
    """Every run id the caller owns in one status table."""
    ids, kwargs = set(), {"ProjectionExpression": "session_id",
                          "FilterExpression": Attr("owner").eq(owner)}
    while True:
        page = tbl.scan(**kwargs)
        ids.update(i["session_id"] for i in page.get("Items", []))
        if not page.get("LastEvaluatedKey"):
            return ids
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def _query_target(event: dict, owner: str) -> dict:
    """The target a list/workflow read names with ?build=, else this deployment's. An
    admin may name anyone's build (reads only: every route using this is a read)."""
    build_id = ((event.get("queryStringParameters") or {}).get("build") or "").strip()
    if not build_id:
        return _default_target()
    return _build_target(build_id, None if _is_admin(event) else owner)


def _everyone(event: dict) -> bool:
    """?scope=all from an admin: the list covers every user, not just the caller."""
    return ((event.get("queryStringParameters") or {}).get("scope") == "all"
            and _is_admin(event))


def _self_invoke(fn_name: str, payload: dict) -> None:
    lambda_client.invoke(FunctionName=fn_name, InvocationType="Event",
                         Payload=json.dumps(payload).encode())


def _run(event: dict) -> dict:
    action = event["action"]
    session_id = event["session_id"]
    payload = {"action": action, "session_id": session_id}
    payload["user"] = event.get("user", "")  # for observability attribution
    # The run owner's sign-in, for tools that act as the person (_person_token). Only
    # passed on; never stored, logged or returned.
    if event.get("user_token"):
        payload["user_token"] = event["user_token"]
    if action == "start":
        payload["topic"] = event.get("topic", "")
        payload["subject_id"] = event.get("subject_id", "")
        if event.get("attachments"):
            payload["attachments"] = event["attachments"]
        # A trigger with `gates: "auto"` (bff/triggers.py): every gate approves itself.
        if event.get("gates") == "auto":
            payload["gates"] = "auto"
    elif action == "resume":
        payload["decision"] = event.get("decision", "approve")
        payload["comment"] = event.get("comment", "")
        payload["decisions"] = event.get("decisions")  # per-agent map for group gates
    elif action == "rerun_from":
        # Rewind to a chosen agent and re-run it + everything downstream, with the
        # reviewer comment injected as that agent's feedback. `agents` (a list of
        # {agent_id, comment}) re-runs a SUBSET of one parallel stage.
        payload["agent_id"] = event.get("agent_id", "")
        payload["comment"] = event.get("comment", "")
        payload["agents"] = event.get("agents")
    elif action == "evaluate":
        # On-demand AgentCore Evaluation of one agent's run. An optional prompt
        # name scopes to one named model call; empty = all the agent's prompts.
        payload["agent_id"] = event.get("agent_id", "")
        payload["prompt"] = event.get("prompt", "")
    elif action == "run_insights":
        # 0 = let the target runtime use its own `orchestrator.insights.lookbackHours`.
        # The target may be a build with a different workflow, so the BFF must not
        # substitute a window of its own.
        payload["lookback_hours"] = int(event.get("lookback_hours") or 0)
    # Control-plane actions (evaluate/insights) are stateless — they only
    # read/write DynamoDB + CloudWatch and emit logs. Route them to a UNIQUE
    # runtimeSessionId so they always run on a fresh container with the LATEST
    # image, never pinned to a run's warm container that may still be executing an
    # older build. (The workflow actions keep the session-derived id so they
    # resume the same run.)
    if action in ("evaluate", "run_insights"):
        runtime_session = f"{action[:6]}{uuid.uuid4().hex}".ljust(33, "0")[:33]
    else:
        runtime_session = session_id.ljust(33, "0")[:33]
    target = _build_target(event["build"]) if event.get("build") else _default_target()
    try:
        agentcore.invoke_agent_runtime(
            agentRuntimeArn=target["runtimeArn"],
            runtimeSessionId=runtime_session,
            payload=json.dumps(payload).encode())
    except Exception as e:  # noqa: BLE001
        target["status"].update_item(
            Key={"session_id": session_id},
            UpdateExpression="SET overall = :o, updated_at = :u",
            ExpressionAttributeValues={":o": "failed", ":u": clock.now_str()})
        print(f"[runner] invoke failed: {type(e).__name__}: {e}")
    return {"ok": True}


#: The model list changes when AWS ships a model or the account's access changes, not
#: per request — and listing costs two control-plane calls. Cached per warm Lambda.
#: The most timeline events a run's snapshot returns (the newest, in order): enough for a
#: long run of many tool-calling agents, bounded so a runaway run cannot blow up a response.
TIMELINE_MAX = 1000

_MODELS_TTL_S = 3600
_models_cache: dict = {"at": 0.0, "body": None}


NOT_CHAT_MODELS = re.compile(r"rerank|embed|sonic|pegasus|palmyra-vision|marengo")
#: Chat models that cannot do toolMode "model": Bedrock refuses tool use for the first
#: five, and the rest take the tools but answer without ever calling one. Bedrock does
#: not publish tool support, so this comes from probing every listed model through
#: run_llm_tools (tests/test_models_list.py keeps it; ~/qa model_matrix re-probes).
NO_TOOL_MODELS = re.compile(r"deepseek\.r1|llama3-(8b|70b)-instruct|llama3-3-70b|mistral-7b-instruct"
                            r"|mixtral-8x7b|gemma-3|magistral-small")
#: Models Bedrock serves only once the account opts in to their data retention.
OPT_IN_MODELS = re.compile(r"claude-fable")
OPT_IN_NOTE = ("needs the account's Bedrock data-retention opt-in for this model; until then "
               "every call is refused")


def model_problems(workflow: dict) -> list[dict]:
    """Agents whose model cannot do what the agent asks of it: read images (`vision`) or
    choose its own tool calls (toolMode "model"). Only models the account lists are
    judged — a typed id nothing lists (a custom import) is taken on trust. Same shape as
    validate_build issues; the Build view computes the same (builder/models.ts)."""
    listed = {m["id"]: m for m in _models().get("models", [])}
    default = str((workflow.get("orchestrator") or {}).get("defaultModel") or "")
    out = []
    for aid, a in (workflow.get("agents") or {}).items():
        if not isinstance(a, dict) or a.get("runtime") == "a2a":
            continue
        mid = str(a.get("model") or default)
        m = listed.get(mid)
        if not m:
            continue
        where = {"kind": "agent", "id": aid}
        path = f"agents.{aid}.model"
        vision = a.get("vision") if isinstance(a.get("vision"), dict) else {}
        if vision.get("from") and not m.get("vision"):
            out.append({"severity": "error", "where": where, "path": path,
                        "message": f"{mid} cannot read images, and this agent reads them (vision): "
                                   "pick a model that accepts images"})
        if a.get("toolMode") == "model" and not m.get("tools"):
            out.append({"severity": "error", "where": where, "path": path,
                        "message": f'{mid} cannot call tools itself (toolMode "model"): pick another '
                                   'model, or let the framework call the tools (toolMode "direct")'})
        if m.get("note"):
            out.append({"severity": "warning", "where": where, "path": path,
                        "message": f"{mid} {m['note']}"})
    return out


def _caps(model_id: str, fm: dict) -> dict:
    """What an agent can ask of a model. `vision` is Bedrock's own inputModalities, which
    matched a live image probe for every listed model; `tools` is NO_TOOL_MODELS."""
    caps = {"vision": "IMAGE" in (fm.get("inputModalities") or []),
            "tools": not NO_TOOL_MODELS.search(model_id)}
    if OPT_IN_MODELS.search(model_id):
        caps["note"] = OPT_IN_NOTE
    return caps


def _models() -> dict:
    """The Bedrock TEXT models this deployment's account can call in this region.

    For the Build view's model picker. Derived from the account rather than written down
    because any list written down is stale within weeks — AWS ships models faster than a
    framework release — and would offer models the account cannot use. What makes a
    model usable here is exactly what this reads: an inference profile (or on-demand
    foundation model) in this region whose output is text, since every agent's model
    call is Bedrock Converse.

    Inference profiles come first: most current models are only invocable through one,
    and it is the id `workflow.json` wants. A foundation model is added when it is
    on-demand and no profile already covers it. Never raises — the picker falls back to
    free text if this is empty, and says why.
    """
    import time as _t
    now = _t.time()
    if _models_cache["body"] is not None and now - _models_cache["at"] < _MODELS_TTL_S:
        return _models_cache["body"]
    bedrock = boto3.client("bedrock", region_name=REGION)
    try:
        text_models = {
            m["modelId"]: m for m in bedrock.list_foundation_models(
                byOutputModality="TEXT").get("modelSummaries", [])
            if (m.get("modelLifecycle") or {}).get("status", "ACTIVE") == "ACTIVE"
            # Models that list TEXT output but cannot answer an agent's Converse call:
            # a reranker scores passages; Nova Sonic is speech-to-speech; Pegasus
            # answers about video; Palmyra Vision needs an image. Probed live against
            # every model the account lists (tests/test_models_list.py keeps the list).
            and not NOT_CHAT_MODELS.search(m["modelId"])}
        out, covered = [], set()
        token = None
        while True:
            page = bedrock.list_inference_profiles(
                typeEquals="SYSTEM_DEFINED", **({"nextToken": token} if token else {}))
            for p in page.get("inferenceProfileSummaries", []):
                if p.get("status", "ACTIVE") != "ACTIVE":
                    continue
                arns = [m.get("modelArn", "") for m in p.get("models") or []]
                base = arns[0].rsplit("/", 1)[-1] if arns else ""
                fm = text_models.get(base)
                if not fm:
                    continue            # an embedding, image or video model
                covered.add(base)
                out.append({"id": p["inferenceProfileId"],
                            "name": p.get("inferenceProfileName") or p["inferenceProfileId"],
                            "provider": fm.get("providerName", ""), **_caps(base, fm)})
            token = page.get("nextToken")
            if not token:
                break
        for mid, fm in text_models.items():
            if mid not in covered and "ON_DEMAND" in (fm.get("inferenceTypesSupported") or []):
                out.append({"id": mid, "name": fm.get("modelName") or mid,
                            "provider": fm.get("providerName", ""), **_caps(mid, fm)})
        out.sort(key=lambda m: (m["provider"].lower(), m["name"].lower()))
        body = {"region": REGION, "models": out}
    except Exception as e:  # noqa: BLE001 - the picker degrades to free text
        print(f"[models] {type(e).__name__}: {e}")
        return {"region": REGION, "models": [],
                "error": f"Could not list Bedrock models ({type(e).__name__}); type a model id."}
    _models_cache.update(at=now, body=body)
    return body


def _user(event: dict) -> str:
    """Authenticated user from the JWT claims (empty when idp = "none").

    Provider-agnostic: API Gateway puts the validated claims in the same place
    for any JWT authorizer. A readable name first: `email` (Cognito, Auth0, Okta),
    then `preferred_username` (Entra ID, which sends no `email` unless it is added
    as an optional claim; seen live: runs were "started by" an opaque id) and `upn`;
    `sub` only when there is nothing else, then `name` and `cognito:username`.
    """
    claims = (event.get("requestContext", {}).get("authorizer", {})
              .get("jwt", {}).get("claims", {}))
    return (claims.get("email") or claims.get("preferred_username") or claims.get("upn")
            or claims.get("sub") or claims.get("name") or claims.get("cognito:username") or "")


def _forbidden(action: str, event: dict) -> dict | None:
    """A 403 response when the caller may not perform `action`, else None.

    Returned rather than raised so each route reads as a single guard line, and
    logged because a denial is nearly always a group-mapping mistake rather than an
    attack — without the log you only see a 403 in the browser.
    """
    if authz.permitted(action, event):
        return None
    body = authz.denial(action, event)
    # Log the SUBJECT claim, not the email. `_user()` prefers `email`, and these logs
    # have no retention policy — a denial is usually a group-mapping mistake, so the
    # groups are what makes it diagnosable; the address adds nothing but PII.
    claims = (event.get("requestContext", {}).get("authorizer", {})
              .get("jwt", {}).get("claims", {}))
    print(f"[authz] DENY {action} sub={claims.get('sub', '?')!r} "
          f"groups={body['yourGroups']} required={body['requiredGroups']} "
          f"claim={body['groupsClaim']}")
    return _resp(403, body)


def _delete_session(sid: str) -> dict:
    """Delete a previously-run pipeline: its event timeline rows, then its status
    row. (Telemetry rows in the separate cost table are left as historical data.)"""
    if not sid:
        return _resp(400, {"error": "missing session id"})
    target = _session_target(sid)
    try:
        for e in _query_all(target["events"],
                            KeyConditionExpression=Key("session_id").eq(sid)):
            target["events"].delete_item(Key={"session_id": sid, "ts": e["ts"]})
    except Exception as e:  # noqa: BLE001 - never let timeline cleanup block the delete
        print(f"[delete] events cleanup failed: {type(e).__name__}: {e}")
    target["status"].delete_item(Key={"session_id": sid})
    if target["build"]:
        builds.unlink_run(target["build"], sid)
    return _resp(200, {"ok": True})


def _insights_latest(runtime_arn: str | None = None) -> dict:
    """Read the latest cross-run Insights findings from the runtime (synchronous;
    the runtime holds the batch-evaluation state)."""
    try:
        resp = agentcore.invoke_agent_runtime(
            agentRuntimeArn=runtime_arn or RUNTIME_ARN,
            runtimeSessionId="insights-latest".ljust(33, "0")[:33],
            payload=json.dumps({"action": "get_insights"}).encode())
        stream = resp.get("response")
        raw = stream.read() if hasattr(stream, "read") else stream
        data = json.loads(raw)
        # Tolerate a runtime body that is a JSON-encoded string wrapping the JSON.
        if isinstance(data, str):
            data = json.loads(data)
    except Exception as e:  # noqa: BLE001
        print(f"[insights] {type(e).__name__}: {e}")
        return _resp(502, {"error": "insights read failed"})
    return _resp(200, data if isinstance(data, dict) else {"status": "none"})


def _person_token(event: dict, target: dict, run_owner: str) -> str:
    """The caller's sign-in token, for a run whose tools act as the person (auth "user" /
    "obo": app/features/gateway/person.py), and only when the caller IS the run's owner,
    so a tool never acts as anyone but the person who started the run. The API Gateway
    authorizer has verified this very token. "" otherwise."""
    tools = (target.get("raw") or {}).get("tools") or {}
    if not any(isinstance(t, dict) and str(t.get("auth") or "").lower() in ("user", "obo")
               for t in tools.values()):
        return ""
    if not run_owner or run_owner != _owner(event):
        return ""
    # A build run from the Builder console: the caller signed in to the CONSOLE, whose
    # token the build's person Gateway does not trust (it trusts the build's own sign-in).
    # Its person tools then say to run it from the build's own app.
    if target.get("build"):
        return ""
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    auth = str(headers.get("authorization") or "")
    return auth[7:].strip() if auth.lower().startswith("bearer ") else ""


def _token_field(event: dict, target: dict, sid: str) -> dict:
    """{"user_token": ...} for a follow-up action (decide, re-run) by the run's owner."""
    token = _person_token(event, target, _run_owner(sid, target["status"]))
    return {"user_token": token} if token else {}


_identity_client = None


def _identity():
    """The AgentCore Identity data plane (CompleteResourceTokenAuth), made once."""
    global _identity_client
    if _identity_client is None:
        _identity_client = boto3.client("bedrock-agentcore", region_name=REGION)
    return _identity_client


def _run_owner(sid: str, tbl) -> str:
    item = tbl.get_item(Key={"session_id": sid}, ProjectionExpression="#o",
                        ExpressionAttributeNames={"#o": "owner"}).get("Item") or {}
    return str(item.get("owner") or "")


def _session_user(sid: str, tbl=None) -> str:
    """The user a run was started by, for cost attribution on follow-up actions."""
    sess = (status_tbl if tbl is None else tbl).get_item(Key={"session_id": sid},
                               ProjectionExpression="#u",
                               ExpressionAttributeNames={"#u": "user"}).get("Item") or {}
    return sess.get("user", "")


# --- Observability reads ---------------------------------------------------
# Cost is computed and stored per row by the runtime
# (app/features/observability), so the BFF only SUMS pre-computed numbers here —
# no pricing logic lives in the BFF.

def _query_all(table, **kwargs) -> list:
    out = []
    while True:
        page = table.query(**kwargs)
        out.extend(page.get("Items", []))
        lek = page.get("LastEvaluatedKey")
        if not lek:
            break
        kwargs["ExclusiveStartKey"] = lek
    return out


def _telemetry_session(sid: str, tbl=None) -> dict:
    """Per-run detail: every LLM/tool/compute row for the session, plus a
    per-agent rollup and session totals."""
    telemetry_tbl = _session_target(sid)["telemetry"] if tbl is None and sid else tbl
    if telemetry_tbl is None or not sid:
        return _resp(200, {"session_id": sid, "calls": [], "byAgent": [], "totals": {}})
    items = _query_all(telemetry_tbl, KeyConditionExpression=Key("session_id").eq(sid))
    agents: dict = {}
    tot = {"costUsd": decimal.Decimal(0), "inputTokens": 0, "outputTokens": 0,
           "calls": 0, "latencyMs": 0}
    for it in items:
        aid = it.get("agent_id", "?")
        a = agents.setdefault(aid, {"agentId": aid, "costUsd": decimal.Decimal(0),
                                    "inputTokens": 0, "outputTokens": 0, "latencyMs": 0,
                                    "calls": 0, "tools": 0, "models": set()})
        c = it.get("cost_usd", 0) or 0
        a["costUsd"] += c
        a["inputTokens"] += int(it.get("input_tokens", 0) or 0)
        a["outputTokens"] += int(it.get("output_tokens", 0) or 0)
        a["latencyMs"] += int(it.get("latency_ms", 0) or 0)
        a["calls"] += 1
        if it.get("kind") == "tool":
            a["tools"] += 1
        if it.get("kind") == "llm" and it.get("label"):
            a["models"].add(it["label"])
        tot["costUsd"] += c
        tot["inputTokens"] += int(it.get("input_tokens", 0) or 0)
        tot["outputTokens"] += int(it.get("output_tokens", 0) or 0)
        tot["latencyMs"] += int(it.get("latency_ms", 0) or 0)
        tot["calls"] += 1
    by_agent = []
    for a in agents.values():
        a["models"] = sorted(a["models"])
        by_agent.append(a)
    return _resp(200, {"session_id": sid, "calls": items, "byAgent": by_agent, "totals": tot})


# Widest date span the aggregate endpoint will serve (one GSI query per day).
_MAX_AGGREGATE_DAYS = 92


def _telemetry_aggregate(by: str, frm: str | None, to: str | None, tbl=None,
                         mine: set[str] | None = None) -> dict:
    """Roll up by date / model / user over a date range (defaults to last 30 days).
    Uses the by_date GSI, one query per day in range, grouped in-memory.

    `mine` limits the rollup to the caller's own runs. Telemetry rows carry the run id
    and not the owner, so a run the caller has deleted drops out of their totals."""
    telemetry_tbl = globals()["telemetry_tbl"] if tbl is None else tbl
    if telemetry_tbl is None:
        return _resp(200, {"by": by, "buckets": []})
    import datetime as _dt
    today = _dt.date.fromisoformat(clock.today_str())  # Eastern "today"
    to = to or today.isoformat()
    frm = frm or (today - _dt.timedelta(days=30)).isoformat()
    try:
        d, d1 = _dt.date.fromisoformat(frm), _dt.date.fromisoformat(to)
    except ValueError:
        return _resp(400, {"error": "from/to must be YYYY-MM-DD"})
    # One GSI query PER DAY in the range, each paginated into memory. `?from=2020-01-01`
    # is ~2000 sequential queries against a 60s timeout and a 256MB budget, which fails
    # as an opaque 502. Cap the span with an actionable message instead.
    if d1 < d:
        return _resp(400, {"error": "`from` must not be after `to`"})
    if (d1 - d).days + 1 > _MAX_AGGREGATE_DAYS:
        return _resp(400, {"error": f"range too wide: {(d1 - d).days + 1} days, maximum "
                                    f"{_MAX_AGGREGATE_DAYS}. Narrow `from`/`to`."})
    buckets: dict = {}
    bucket_sessions: dict = {}   # key -> set(session_id), for distinct run counts
    all_sessions: set = set()
    while d <= d1:
        for it in _query_all(telemetry_tbl, IndexName="by_date",
                             KeyConditionExpression=Key("date").eq(d.isoformat())):
            if mine is not None and it.get("session_id") not in mine:
                continue
            if by == "model":
                key = it.get("label") if it.get("kind") == "llm" else "(tool/agentcore)"
            elif by == "user":
                key = it.get("user") or "(unattributed)"
            else:
                key = it.get("date")
            b = buckets.setdefault(key, {"key": key, "costUsd": decimal.Decimal(0),
                                         "inputTokens": 0, "outputTokens": 0,
                                         "calls": 0, "latencyMs": 0, "sessions": 0,
                                         "embedTokensEst": 0,
                                         "inRate": decimal.Decimal(0),
                                         "outRate": decimal.Decimal(0)})
            b["costUsd"] += it.get("cost_usd", 0) or 0
            b["inputTokens"] += int(it.get("input_tokens", 0) or 0)
            b["outputTokens"] += int(it.get("output_tokens", 0) or 0)
            b["embedTokensEst"] += int(it.get("embed_tokens_est", 0) or 0)
            b["latencyMs"] += int(it.get("latency_ms", 0) or 0)
            b["calls"] += 1
            sid = it.get("session_id")
            if sid:
                bucket_sessions.setdefault(key, set()).add(sid)
                all_sessions.add(sid)
            # Representative per-1M rates for the by-model view (llm rows only).
            if by == "model" and it.get("kind") == "llm":
                if it.get("in_rate"):
                    b["inRate"] = it["in_rate"]
                if it.get("out_rate"):
                    b["outRate"] = it["out_rate"]
        d += _dt.timedelta(days=1)
    for key, b in buckets.items():
        b["sessions"] = len(bucket_sessions.get(key, ()))
    return _resp(200, {"by": by, "from": frm, "to": to,
                       "totalSessions": len(all_sessions),
                       "buckets": sorted(buckets.values(), key=lambda x: str(x["key"]))})


def _build_field(target: dict) -> dict:
    """The `build` key a runner-mode payload carries, so it invokes the right runtime."""
    return {"build": target["build"]} if target["build"] else {}


def _builds_route(route_key: str, build_id: str, body: dict, event: dict) -> dict:
    """/api/builds*. Deploy and destroy are guarded; saving a draft is not — it
    changes nothing in AWS, and is only ever the caller's own build."""
    if not builds.ENABLED:
        return _resp(404, {"error": "the Builder is not enabled on this deployment"})
    owner = builds.owner_of(authz.claims(event))
    email = str(authz.claims(event).get("email") or "")
    ip = ((event.get("requestContext") or {}).get("http") or {}).get("sourceIp", "")

    def audit(action: str, **detail) -> None:
        builds.audit(owner, email, action, ip=ip, **detail)

    qs = event.get("queryStringParameters") or {}
    admin = _is_admin(event)
    # Whose build a READ is about: the caller's, or for an admin anyone's (each read of
    # someone else's is logged as admin.viewed). Writes keep using `owner`.
    reader = (builds.owner_for(build_id, owner, admin)
              if admin and route_key.startswith(("GET /api/builds/{id}",
                                                 "POST /api/builds/{id}/destroy"))
              else owner)

    def viewed(what: str) -> None:
        if reader != owner:
            _admin_viewed(event, "build", build_id, reader, what=what)

    if route_key == "GET /api/builds":
        if qs.get("scope") == "all":
            if not admin:
                return _resp(403, authz.denial("admin", event))
            return _resp(200, builds.list_all_builds())
        return _resp(200, builds.list_builds(owner))
    if route_key == "GET /api/builds/{id}":
        viewed("build")
        return _resp(200, builds.get_build(build_id, reader))
    if route_key == "PUT /api/builds/{id}":
        return _resp(200, builds.save(
            build_id, owner, body.get("project"), email,
            on_create=lambda b: audit("build.created", build=build_id, name=b.get("name", ""),
                                      agentName=b.get("agentName", ""))))
    # A build's secrets: names out, values in, never values out.
    if route_key == "GET /api/builds/{id}/secrets":
        return _resp(200, builds.secret_names(build_id, reader))
    if route_key == "PUT /api/builds/{id}/secrets":
        names = builds.set_secrets(build_id, owner, body)
        # Which names changed — never a value.
        audit("secrets.updated", **builds.label(build_id, owner),
              changed={k: sorted(v) for k, v in body.items() if isinstance(v, dict) and v})
        return _resp(200, names)
    # Knowledge-base documents for a build's corpora.
    if route_key == "GET /api/builds/{id}/docs":
        return _resp(200, builds.list_docs(build_id, reader))
    if route_key == "POST /api/builds/{id}/docs":
        return _resp(200, builds.doc_upload(build_id, owner, str(body.get("corpus") or ""),
                                            str(body.get("name") or "")))
    if route_key == "DELETE /api/builds/{id}/docs":
        builds.delete_doc(build_id, owner, qs.get("corpus", ""), qs.get("name", ""))
        return _resp(200, {"ok": True})
    # Connected AWS accounts (bff/accounts.py). Connecting grants a deploy role, so it
    # takes the same permission as deploying.
    if route_key == "GET /api/accounts":
        return _resp(200, accounts.list_accounts(owner))
    if route_key == "GET /api/accounts/{id}/launch":
        return _resp(200, accounts.launch_link(owner, build_id))
    if route_key in ("POST /api/accounts", "POST /api/accounts/{id}/verify"):
        denied = _forbidden("deploy", event)
        if denied:
            return denied
        if route_key == "POST /api/accounts":
            return _resp(200, accounts.connect(owner, str(body.get("accountId") or "").strip(),
                                               str(body.get("region") or REGION).strip(),
                                               str(body.get("label") or "")))
        verified = accounts.verify(owner, build_id)
        audit("account.connected", account=build_id, region=verified.get("region", ""))
        return _resp(200, verified)
    if route_key == "PUT /api/accounts/{id}":
        changed = accounts.update(owner, build_id, label=body.get("label"),
                                  region=body.get("region"))
        audit("account.updated", account=build_id, region=changed.get("region", ""),
              label=changed.get("label", ""))
        return _resp(200, changed)
    if route_key == "DELETE /api/accounts/{id}":
        accounts.disconnect(owner, build_id)
        audit("account.disconnected", account=build_id)
        return _resp(200, {"ok": True})
    # A tool whose function is written in the build (tools.<key>.code): checked before it
    # deploys — statically, then run on its test events in an AgentCore Code Interpreter
    # sandbox (bff/codecheck.py). The files come from the page, as the user has them now.
    if route_key == "POST /api/code/check":
        tool = body.get("tool") if isinstance(body.get("tool"), dict) else {}
        names = [s.get("name") for s in tool.get("toolSchema") or [] if isinstance(s, dict) and s.get("name")]
        key = str(body.get("key") or "")
        # A Gateway interceptor written in the build is checked the same way.
        if not validate_key(key) and key not in ("interceptor-request", "interceptor-response"):
            return _resp(400, {"error": "invalid tool key"})
        return _resp(200, codecheck.check(key, body.get("files"), tool.get("code") or {}, names,
                                          run=body.get("run") is not False))
    # After a deploy: call the function with one event, the way the Gateway does. It runs
    # in the build's account and may change things there, so it takes `deploy`.
    if route_key == "POST /api/builds/{id}/test-tool":
        denied = _forbidden("deploy", event)
        if denied:
            return denied
        key = str(body.get("key") or "")
        result = builds.test_tool(build_id, owner, key, str(body.get("tool") or ""), body.get("event"))
        audit("tool.tested", **builds.label(build_id, owner), tool=key, ok=result["ok"])
        return _resp(200, result)
    # The policy library (bff/policies.py): the caller's own Cedar policies, private like
    # connected accounts. Like a draft, it changes nothing in AWS until a build that has
    # one attached is deployed, so it takes no permission.
    if route_key == "GET /api/policies":
        return _resp(200, policies.list_policies(owner))
    # Plain English: AgentCore Policy writes it against the caller's deployed build (its
    # engine and Gateway), asynchronously; the page polls the generation.
    if route_key == "POST /api/policies/generate":
        bid = str(body.get("build") or "")
        started = policies.generate(bid, owner, body.get("text"))
        audit("policy.generated", **builds.label(bid, owner),
              request=" ".join(str(body.get("text") or "").split())[:200])
        return _resp(202, started)
    if route_key == "GET /api/policies/generate/{id}":
        return _resp(200, policies.generation(qs.get("build", ""), owner, build_id))
    if route_key == "POST /api/policies":
        made = policies.create(owner, body)
        audit("policy.created", policy=made["id"], name=made["name"], source=made.get("source", ""))
        return _resp(200, made)
    if route_key == "PUT /api/policies/{id}":
        changed = policies.update(owner, build_id, body)
        audit("policy.updated", policy=build_id, name=changed["name"])
        return _resp(200, changed)
    if route_key == "DELETE /api/policies/{id}":
        gone = policies.delete(owner, build_id)
        audit("policy.deleted", policy=build_id, name=gone["name"])
        return _resp(200, {"ok": True})
    # Sharing (bff/sharing.py): a build with people, groups or everyone — who may then do
    # everything its owner may.
    if route_key == "PUT /api/builds/{id}/shares":
        what = builds.label(build_id, owner)       # before: a share may drop the caller's own access
        shares = builds.set_shares(build_id, owner, body)
        audit("build.shared", **what, shares=shares)
        return _resp(200, shares)
    # The library (bff/library.py): items used live by any build, the caller's own and
    # those shared with them. Like a draft, it changes nothing in AWS until a build that
    # uses one is deployed, so it takes no permission.
    if route_key == "GET /api/library":
        return _resp(200, library.list_items(owner, qs.get("kind", "")))
    if route_key == "GET /api/library/{id}":
        return _resp(200, library._public(library.get(build_id, owner), owner))
    if route_key == "POST /api/library":
        made = library.create(owner, body)
        audit("library.created", item=made["id"], kind=made["kind"], name=made["name"])
        return _resp(200, made)
    if route_key == "PUT /api/library/{id}":
        changed = library.update(owner, build_id, body)
        audit("library.updated", item=build_id, kind=changed["kind"], name=changed["name"])
        return _resp(200, changed)
    if route_key == "DELETE /api/library/{id}":
        gone = library.delete(owner, build_id)
        audit("library.deleted", item=build_id, kind=gone["kind"], name=gone["name"],
              keptIn=[b["id"] for b in gone["keptIn"]])
        # Builds that used it now have their own copy of it.
        return _resp(200, {"ok": True, "keptIn": gone["keptIn"]})
    if route_key == "PUT /api/library/{id}/shares":
        shares = library.set_shares(owner, build_id, body)
        audit("library.shared", item=build_id, shares=shares)
        return _resp(200, shares)
    # AWS Agent Registry (bff/registry.py): what the organization approved, to take into a
    # build. Read only, like browsing the library, so it takes no permission.
    if route_key == "GET /api/registry":
        return _resp(200, builds.registry_call(registry.list_registries))
    if route_key == "GET /api/registry/search":
        return _resp(200, builds.registry_call(registry.search, qs.get("registry"), qs.get("q", ""),
                                               qs.get("kind", "")))
    if route_key == "GET /api/builds/{id}/registry":
        viewed("registry")
        return _resp(200, builds.registry_state(build_id, reader))
    # Publishing to the organization's registry speaks for the organization: admins only.
    if route_key == "POST /api/builds/{id}/publish":
        if not admin:
            return _resp(403, authz.denial("admin", event))
        published = builds.publish(build_id, owner, body, email or owner)
        audit("build.published", **builds.label(build_id, owner), what=str(body.get("what") or ""),
              skill=str(body.get("skill") or ""), registry=str(body.get("registry") or ""))
        return _resp(200, published)
    # Groups to share with: every user may list their names (to pick one); only an admin
    # sees their members and defines them.
    if route_key == "GET /api/groups":
        got = sharing.list_groups()
        return _resp(200, got if admin else [{"name": g["name"]} for g in got])
    if route_key in ("PUT /api/groups/{id}", "DELETE /api/groups/{id}"):
        if not admin:
            return _resp(403, authz.denial("admin", event))
        name = urllib.parse.unquote(build_id)
        if route_key.startswith("DELETE"):
            sharing.delete_group(name)
            audit("group.deleted", group=name)
            return _resp(200, {"ok": True})
        saved = sharing.put_group(name, body.get("members"), clock.now_str())
        audit("group.saved", group=name, members=len(saved["members"]))
        return _resp(200, saved)
    # AgentExpress Assistant (bff/designer.py): the conversation that edits this build's draft.
    # Like saving a draft, it changes nothing in AWS, so it takes no permission.
    if route_key == "GET /api/builds/{id}/design":
        viewed("design")
        return _resp(200, designer.conversation(build_id, reader))
    if route_key == "POST /api/builds/{id}/design":
        fn = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "")
        message = str(body.get("message") or "")
        started = designer.start(build_id, owner, email, message,
                                 lambda payload: _self_invoke(fn, payload),
                                 attachments=body.get("attachments"))
        sent = next((t for t in reversed(started.get("turns") or []) if t.get("role") == "user"), {})
        # What was asked, shortened, and what it brought: who changed a build by chat.
        audit("design.message", **builds.label(build_id, owner),
              message=" ".join(message.split())[:200],
              attachments=[a.get("uri") or a.get("name") for a in sent.get("attachments") or []] or None)
        return _resp(202, started)
    if route_key == "POST /api/builds/{id}/design/attachments":
        return _resp(200, designer.attachment_upload(build_id, owner, body.get("name")))
    if route_key == "DELETE /api/builds/{id}/design":
        return _resp(200, designer.reset(build_id, owner))
    # The owner's first-sign-in password for the build's own console, stored by the
    # deploy runner (deployer/runner.py store_login). Only the build's owner reads it.
    if route_key == "GET /api/builds/{id}/login":
        login = builds.app_login(build_id, owner)
        audit("login.viewed", **builds.label(build_id, owner))
        return _resp(200, login)
    if route_key == "GET /api/builds/{id}/log":
        return _resp(200, builds.job_log(build_id, reader))
    if route_key == "POST /api/builds/{id}/deploy":
        denied = _forbidden("deploy", event)
        if denied:
            return denied
        job = builds.deploy(build_id, owner, str(body.get("tool") or ""),
                            _user(event), str(body.get("account") or "").strip(),
                            str(body.get("region") or "").strip())
        audit("deploy.requested", **builds.label(build_id, owner), version=job["version"],
              tool=job["tool"], account=job.get("account") or "console",
              region=job.get("region") or "")
        return _resp(202, job)
    if route_key == "POST /api/builds/{id}/destroy":
        denied = _forbidden("destroy", event)
        if denied:
            return denied
        # An admin may destroy anyone's build (to clean up); the owner's own log says so.
        whose = reader if admin else owner
        job = builds.destroy(build_id, whose, _user(event))
        what = builds.label(build_id, whose)
        audit("destroy.requested", **what, version=job["version"], tool=job["tool"],
              account=job.get("account") or "console",
              **({"forUser": whose, "asAdmin": True} if whose != owner else {}))
        if whose != owner:
            builds.audit(whose, "", "destroy.requested", **what, version=job["version"],
                         tool=job["tool"], account=job.get("account") or "console",
                         byAdmin=email or owner)
        return _resp(202, job)
    if route_key == "DELETE /api/builds/{id}":
        # A build with anything in AWS is DESTROYED first, and deleted by the deploy
        # runner once the destroy succeeds — so deleting needs the destroy permission.
        if builds.is_deployed_or_partial(build_id, owner):
            denied = _forbidden("destroy", event)
            if denied:
                return denied
            job = builds.destroy(build_id, owner, _user(event), delete_after=True)
            audit("destroy.requested", **builds.label(build_id, owner), version=job["version"],
                  tool=job["tool"], account=job.get("account") or "console", thenDelete=True)
            return _resp(202, {"destroying": True, "job": job})
        what = builds.label(build_id, owner)
        builds.delete(build_id, owner)
        audit("build.deleted", **what)
        return _resp(200, {"ok": True})
    return _resp(404, {"error": f"no route for {route_key}"})


def _audit_run(event: dict, action: str, sid: str, **detail) -> None:
    """Log what someone did to a run: in the console's log, or in a deployed build's own
    (bff/audit.py). Never raises, like every audit write."""
    if not audit.enabled():
        return
    try:
        claims = authz.claims(event)
        ip = ((event.get("requestContext") or {}).get("http") or {}).get("sourceIp", "")
        audit.record(builds.owner_of(claims), str(claims.get("email") or ""), action,
                     session=sid, ip=ip, **{k: v for k, v in detail.items() if v not in (None, "")})
    except Exception as e:  # noqa: BLE001
        print(f"[audit] {action} {sid}: {type(e).__name__}: {e}")


def validate_key(key: str) -> bool:
    return bool(re.match(r"^[A-Za-z][A-Za-z0-9]{0,40}$", key or ""))


def _console_mode() -> str:
    """"builder" only on a console that has the Builder to be a control plane for."""
    return "builder" if CONSOLE_MODE == "builder" and builds.ENABLED else "app"


def _begin_run(context, target: dict, topic: str, owner: str, user: str, *, files=(), subject_id: str = "",
               build_id: str = "", item_extra: dict | None = None, payload_extra: dict | None = None,
               audit_as: tuple = ("", "", ""), audit_detail: dict | None = None,
               user_token: str = "") -> str | None:
    """Create a run's status row and start it: the one run start, for a person (POST
    /api/sessions) and for a trigger (bff/triggers.py). None if the row could not be written."""
    session_id = uuid.uuid4().hex[:12]
    files = runfiles.copy_in(session_id, list(files)) if files else []
    item = _skeleton(session_id, topic, list(target["raw"].get("agents") or {}))
    item["user"] = user
    item["owner"] = owner   # the run is private to whoever started it
    # THE WORKFLOW THIS RUN RAN WITH. The run page draws from this, not from whatever
    # the workflow is now, so an old run keeps showing the graph that produced it.
    item["workflow"] = target["view"]
    if build_id:
        item["build"] = {"id": build_id, "name": target["name"], "version": target["version"]}
    if subject_id:
        item["subject_id"] = subject_id  # so the UI can show it after a refresh
    if files:
        item["attachments"] = files      # the run page lists them
    item.update(item_extra or {})
    try:
        target["status"].put_item(Item=item, ConditionExpression="attribute_not_exists(session_id)")
    except Exception as e:  # noqa: BLE001
        # Only the id-collision race is expected here. Anything else (denied,
        # throttled, malformed) used to be swallowed identically and the run was
        # started anyway — so the UI polled a session that would never appear and
        # nothing was logged. Fail loudly instead.
        code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
        if code != "ConditionalCheckFailedException":
            print(f"[sessions] status write failed: {type(e).__name__}: {e}")
            return None
    if build_id:
        builds.link_run(build_id, session_id)
    if audit.enabled():
        try:
            who, email, ip = audit_as
            audit.record(who, email, "run.started", session=session_id, ip=ip, topic=str(topic)[:200],
                         **{k: v for k, v in {**({"build": build_id, "name": target["name"],
                                                    "version": target["version"]} if build_id else {}),
                                                 **(audit_detail or {})}.items() if v not in (None, "")})
        except Exception as e:  # noqa: BLE001 - never blocks a run
            print(f"[audit] run.started {session_id}: {type(e).__name__}: {e}")
    _self_invoke(context.function_name,
                 {"action": "start", "session_id": session_id, "topic": topic,
                  "subject_id": subject_id, "user": user,
                  **({"attachments": files} if files else {}),
                  **({"build": build_id} if build_id else {}), **(payload_extra or {}),
                  **({"user_token": user_token} if user_token else {})})
    return session_id


def _gate_resume(context):
    """How bff/gates.py hands a gate its decision: as the run page does, logged for the
    run's owner with who (or what) decided."""
    def resume(sid: str, decision: str, comment: str, by: str) -> None:
        item = status_tbl.get_item(Key={"session_id": sid}, ProjectionExpression="#o, #u",
                                   ExpressionAttributeNames={"#o": "owner", "#u": "user"}).get("Item") or {}
        if audit.enabled() and item.get("owner"):
            try:
                audit.record(str(item["owner"]), by, "run.decided", session=sid, decision=decision,
                             by=by, comment=comment[:200] or None)
            except Exception as e:  # noqa: BLE001 - never blocks the decision
                print(f"[audit] run.decided {sid}: {type(e).__name__}: {e}")
        _self_invoke(context.function_name, {"action": "resume", "session_id": sid, "decision": decision,
                                             "comment": comment, "decisions": None,
                                             "user": str(item.get("user") or "")})
    return resume


def _trigger_start(context):
    """How bff/triggers.py starts a run: in this deployment's own workflow, as the
    trigger's identity, with the delivery attached when an agent reads files."""
    def start(topic: str, owner: str, user: str, extra: dict) -> str:
        target = _default_target()
        files = []
        if runfiles.enabled():
            uploads = []
            if extra.get("payload") is not None and runfiles.ASSETS_BUCKET:
                key = f"{runfiles._folder(owner)}{uuid.uuid4().hex[:8]}-payload.json"
                runfiles._s3().put_object(Bucket=runfiles.ASSETS_BUCKET, Key=key, ContentType="application/json",
                                          Body=json.dumps(extra["payload"], default=str).encode()[:4_000_000])
                uploads.append({"key": key, "name": "payload.json"})
            try:
                files = runfiles.resolve(owner, uploads, f"{topic}\n{extra.get('s3') or ''}")
            except builds.BuildError as e:
                # A path the workflow may not read is not attached; the run still starts.
                print(f"[triggers] not attached: {e}")
                files = runfiles.resolve(owner, uploads, "") if uploads else []
        t = extra.get("trigger") or {}
        sid = _begin_run(context, target, topic[:TOPIC_MAX], owner, user, files=files,
                         item_extra={"trigger": t},
                         payload_extra={"gates": "auto"} if extra.get("gates") == "auto" else None,
                         audit_as=(owner, user, ""),
                         audit_detail={"trigger": t.get("name"), "source": t.get("source")})
        if sid is None:
            raise RuntimeError("could not create the session")
        return sid
    return start


def _api(event: dict, context) -> dict:
    """Route one API call. An expected refusal (unknown or someone else's run or build,
    a job already running...) is raised as BuildError anywhere below and answered here."""
    try:
        return _route(event, context)
    except builds.BuildError as e:
        return _resp(e.status, {"error": str(e)})


def _route(event: dict, context) -> dict:
    method = event["requestContext"]["http"]["method"]
    path = event.get("rawPath", "")
    params = event.get("pathParameters") or {}
    route_key = event.get("routeKey", "")
    body = {}
    if event.get("body"):
        try:
            body = json.loads(event["body"])
        except (TypeError, ValueError):     # not JSON, or not a string
            body = {}

    # A webhook trigger (bff/triggers.py): the one route with no JWT authorizer. Its proof
    # is the signature over the body, checked before anything else is read.
    if route_key == "POST /api/hooks/{name}":
        if _console_mode() == "builder" or status_tbl is None:
            return _resp(404, {"error": "unknown trigger"})
        status, answer = triggers.webhook(event, _trigger_start(context))
        return _resp(status, answer)
    # The app's Triggers page: what is wired, each webhook's URL and secret, a test.
    if route_key.startswith(("GET /api/triggers", "POST /api/triggers")):
        denied = _forbidden("admin", event)
        if denied:
            return denied
        if status_tbl is None:
            return _resp(404, {"error": NOT_HERE})
        name = str(params.get("name") or "")
        try:
            if route_key == "GET /api/triggers":
                domain = (event.get("requestContext") or {}).get("domainName", "")
                return _resp(200, {"triggers": triggers.listing(domain)})
            if route_key == "POST /api/triggers/{name}/secret":
                secret = triggers.set_secret(name, str(body.get("value") or ""))
                _audit_run(event, "trigger.secret", "", trigger=name, given=bool(body.get("value")))
                scheme = triggers.trigger(name).get("signature") or "agentexpress"
                return _resp(200, {"secret": secret, "signature": scheme})
            if route_key == "POST /api/triggers/{name}/test":
                # As if delivered, signature aside: the caller is an admin of this app.
                ctx = {"body": body.get("body") if body.get("body") is not None else {}, "headers": {},
                       "event": body.get("event") or {}, "detail": (body.get("event") or {}).get("detail") or {},
                       "time": clock.now_str()}
                got = triggers.fire(name, "test", ctx, f"test-{uuid.uuid4().hex}", _trigger_start(context),
                                    payload=body.get("body") if body.get("body") is not None else body.get("event"))
                _audit_run(event, "trigger.tested", got.get("session_id") or "", trigger=name)
                return _resp(200, got)
        except triggers.TriggerError as e:
            return _resp(e.status, {"error": str(e)})
        return _resp(404, {"error": f"no route for {route_key}"})

    # The activity log (bff/audit.py): your own, or — with the `audit` permission —
    # everyone's on one day. Sign-ins are logged by the Cognito trigger; sign-outs here,
    # called by the page just before it signs out. On a console AND in a build's app.
    if route_key == "GET /api/audit":
        qs = event.get("queryStringParameters") or {}
        everyone = qs.get("scope") == "all"
        if everyone:
            denied = _forbidden("audit", event)
            if denied:
                return denied
        # One day (?day=) or a range (?from=&to=, both included).
        return _resp(200, audit.activity(builds.owner_of(authz.claims(event)), everyone,
                                         qs.get("from") or qs.get("day", ""),
                                         day_to=qs.get("to", "")))
    if route_key == "POST /api/audit/logout":
        claims = authz.claims(event)
        ip = ((event.get("requestContext") or {}).get("http") or {}).get("sourceIp", "")
        audit.record(builds.owner_of(claims), str(claims.get("email") or ""), "logout", ip=ip)
        return _resp(200, {"ok": True})

    # A control-plane console runs nothing itself: no run, no insight, no telemetry, no
    # assistant, no image link — for its own workflow or for a build's.
    if _console_mode() == "builder" and path.startswith(
            ("/api/sessions", "/api/insights", "/api/telemetry", "/api/chat", "/api/images")):
        return _resp(404, {"error": NOT_HERE})

    if method == "GET" and path == "/api/workflow":
        # This deployment's own view needs no run tables: a control-plane console has none,
        # and its page still reads its title and branding here.
        if not ((event.get("queryStringParameters") or {}).get("build") or "").strip():
            return _resp(200, WORKFLOW)
        return _resp(200, _query_target(event, builds.owner_of(authz.claims(event)))["view"])

    if method == "GET" and path == "/api/images":
        qs = event.get("queryStringParameters") or {}
        return _resp(200, _image_link(event, qs.get("key", "")))

    if method == "GET" and path == "/api/models":
        # Read-only and account-level, like /api/workflow: no authz action to gate.
        return _resp(200, _models())

    if method == "GET" and path == "/api/me":
        # Who am I and what may I do — the UI disables the controls it isn't
        # allowed to use instead of offering a button that 403s. Advisory only:
        # every action is still checked server-side on its own route.
        return _resp(200, {"user": _user(event), "owner": _owner(event),
                           "admin": _is_admin(event), "groups": authz.groups_of(event),
                           "permittedActions": authz.permitted_actions(event),
                           "authzEnabled": authz.ENABLED, "builder": builds.ENABLED,
                           "email": str(authz.claims(event).get("email") or ""),
                           "consoleMode": _console_mode(), "audit": audit.enabled()})

    # --- Builder builds (bff/builds.py). Matched on the route key and placed BEFORE the
    # session routes, whose `path.endswith(params["id"])` would otherwise claim them.
    if route_key.startswith(("GET /api/builds", "PUT /api/builds", "POST /api/builds",
                             "DELETE /api/builds", "GET /api/accounts", "POST /api/accounts",
                             "PUT /api/accounts", "DELETE /api/accounts", "GET /api/policies",
                             "POST /api/code", "POST /api/policies",
                             "PUT /api/policies", "DELETE /api/policies",
                             "GET /api/library", "POST /api/library", "PUT /api/library",
                             "DELETE /api/library", "GET /api/groups", "PUT /api/groups",
                             "DELETE /api/groups", "GET /api/registry")):
        return _builds_route(route_key, params.get("id", ""), body, event)

    if method == "POST" and path == "/api/sessions/attachments":
        # A presigned POST for one file the next run will bring (bff/runfiles.py).
        denied = _forbidden("start", event)
        if denied:
            return denied
        return _resp(200, runfiles.upload(_owner(event), str(body.get("name") or "")))

    if method == "POST" and path == "/api/connect":
        # A person back from connecting their account for a tool that uses each person's
        # own account (auth "user"). AgentCore Identity sent them here with the session
        # it opened when the person Gateway asked for consent; binding it with THIS
        # caller's sign-in is what proves the person who consented is the one whose run
        # asked (session binding), so a link forwarded to someone else binds nothing.
        session_uri = str(body.get("sessionUri") or "")
        if not re.fullmatch(r"urn:ietf:params:oauth:request_uri:[A-Za-z0-9._~-]{1,1000}", session_uri):
            return _resp(400, {"error": "not a connection to complete"})
        headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
        auth = str(headers.get("authorization") or "")
        if not auth.lower().startswith("bearer "):
            return _resp(401, {"error": "sign in first"})
        try:
            _identity().complete_resource_token_auth(
                sessionUri=session_uri, userIdentifier={"userToken": auth[7:].strip()})
        except Exception as e:  # noqa: BLE001 - expired, already used, or someone else's
            # The service's own message ("Invalid or expired session", or an IAM denial);
            # it never carries the token.
            print(f"[connect] not bound: {type(e).__name__}: {str(e)[:300]}")
            return _resp(409, {"error": "That connection could not be completed: it expired (10 minutes), was "
                                        "already used, or was started by someone else. Run the step again."})
        return _resp(200, {"ok": True})
    if method == "POST" and path == "/api/sessions":
        denied = _forbidden("start", event)
        if denied:
            return denied
        build_id = str(body.get("build") or "").strip()
        target = (_build_target(build_id, builds.owner_of(authz.claims(event)))
                  if build_id else _default_target())
        default_topic = (DEFAULT_TOPIC if not build_id
                         else str((target["raw"].get("ui") or {}).get("defaultTopic") or ""))
        topic = str(body.get("topic") or default_topic).strip()
        # The request may be pages of instructions (the Start run box is a text area),
        # but not unbounded: it is stored on the run, sent to every agent and used as
        # the long-term memory query.
        if len(topic) > TOPIC_MAX:
            return _resp(400, {"error": f"a request is at most {TOPIC_MAX} characters"})
        # Optional grouping key; scopes long-term memory (insights/{agentId}-{subject}).
        # `subject_id` is accepted too: the console sent that spelling, and dropping it
        # silently lost every subject a user typed.
        subject_id = body.get("subjectId") or body.get("subject_id") or ""
        # Files the run brings (uploads, s3:// paths in the request), for the agents
        # that set `attachments`. Only this deployment's own workflow: a build's app
        # takes its own.
        if build_id and body.get("attachments"):
            return _resp(400, {"error": "attach files in the build's own app"})
        files = [] if build_id else runfiles.resolve(_owner(event), body.get("attachments"), topic)
        claims = authz.claims(event)
        ip = ((event.get("requestContext") or {}).get("http") or {}).get("sourceIp", "")
        session_id = _begin_run(context, target, topic, _owner(event), _user(event), files=files,
                                subject_id=subject_id, build_id=build_id,
                                audit_as=(builds.owner_of(claims), str(claims.get("email") or ""), ip),
                                user_token=_person_token(event, target, _owner(event)))
        if session_id is None:
            return _resp(500, {"error": "could not create the session"})
        return _resp(200, {"session_id": session_id})

    if method == "GET" and path == "/api/sessions":
        # Paginate: a Scan returns at most 1MB of data READ per page (before the
        # ProjectionExpression trims it). Status items are large (they embed every
        # node's output + history + result), so even a few sessions exceed one
        # page — without following LastEvaluatedKey, sessions silently disappear.
        items = []
        target = _query_target(event, builds.owner_of(authz.claims(event)))
        # Only the caller's own runs.
        service = (event.get("queryStringParameters") or {}).get("scope") == "triggers"
        owners = _service_owners(event) if service else []
        if service and not owners:
            return _resp(200, [])
        scan_kwargs = ({"ProjectionExpression": "session_id, topic, overall, hitl, created, "
                                                "#o, #u, #tr",
                        "ExpressionAttributeNames": {"#o": "owner", "#u": "user", "#tr": "trigger"}}
                       if _everyone(event) else
                       {"ProjectionExpression": "session_id, topic, overall, hitl, created, #o, #tr",
                        "ExpressionAttributeNames": {"#o": "owner", "#tr": "trigger"},
                        "FilterExpression": Attr("owner").is_in(owners)}
                       if service else
                       {"ProjectionExpression": "session_id, topic, overall, hitl, created, #tr",
                        "ExpressionAttributeNames": {"#tr": "trigger"},
                        "FilterExpression": Attr("owner").eq(_owner(event))})
        while True:
            page = target["status"].scan(**scan_kwargs)
            items.extend(page.get("Items", []))
            last = page.get("LastEvaluatedKey")
            if not last:
                break
            scan_kwargs["ExclusiveStartKey"] = last
        # `created` is an ET string ("YYYY-MM-DD HH:MM:SS") for new sessions and a
        # legacy epoch-ms int for old ones. Coerce to str so the sort never mixes
        # types; both forms still order newest-first (ET strings sort above ints).
        items.sort(key=lambda x: str(x.get("created", "")), reverse=True)
        return _resp(200, items)

    if method == "GET" and params.get("id") and path.endswith(params["id"]):
        sid = params["id"]
        target = _own_session(event, sid, read=True)
        item = target["status"].get_item(Key={"session_id": sid}).get("Item")
        if not item:
            return _resp(404, {"error": "unknown session"})
        # The whole timeline, up to TIMELINE_MAX events (newest kept). It used to be the
        # newest 40, so on a long run (several tool-calling agents) the first agents'
        # lines — a dedicated runtime's invocation, the first searches — never showed.
        evs, kw = [], {"KeyConditionExpression": boto3.dynamodb.conditions.Key("session_id").eq(sid),
                       "ScanIndexForward": False}
        while len(evs) < TIMELINE_MAX:
            page = target["events"].query(**kw, Limit=TIMELINE_MAX - len(evs))
            evs += page.get("Items", [])
            if not page.get("LastEvaluatedKey"):
                break
            kw["ExclusiveStartKey"] = page["LastEvaluatedKey"]
        evs.reverse()
        item["logs"] = [{"ts": e["ts"], "node": e.get("node"), "msg": e.get("msg", "")} for e in evs]
        return _resp(200, item)

    if method == "DELETE" and params.get("id") and path.endswith(params["id"]):
        denied = _forbidden("delete", event)
        if denied:
            return denied
        _own_session(event, params["id"])
        out = _delete_session(params["id"])
        if out.get("statusCode") == 200:
            _audit_run(event, "run.deleted", params["id"])
        return out

    if method == "POST" and params.get("id") and path.endswith("/cancel"):
        denied = _forbidden("cancel", event)
        if denied:
            return denied
        sid = params["id"]
        tbl = _own_session(event, sid)["status"]
        item = tbl.get_item(Key={"session_id": sid}).get("Item")
        if not item:
            return _resp(404, {"error": "unknown session"})
        if item.get("overall") in ("done", "denied", "failed", "cancelled"):
            return _resp(200, {"ok": True, "overall": item.get("overall")})
        # Set the durable flag (the running workflow polls it) and reflect the
        # stop immediately in the UI by clearing any pending human gate.
        tbl.update_item(
            Key={"session_id": sid},
            UpdateExpression="SET cancel_requested = :c, overall = :o, hitl = :z, updated_at = :u",
            ExpressionAttributeValues={":c": True, ":o": "cancelled", ":z": None, ":u": clock.now_str()})
        _audit_run(event, "run.cancelled", sid)
        return _resp(200, {"ok": True, "overall": "cancelled"})

    if method == "POST" and params.get("id") and path.endswith("/decision"):
        denied = _forbidden("decision", event)
        if denied:
            return denied
        sid = params["id"]
        target = _own_session(event, sid)
        decision = body.get("decision")
        # Per-agent decisions for a parallel-group gate; when present, the single
        # decision need not be one of the three.
        decisions = body.get("decisions")
        if not decisions and decision not in ("approve", "deny", "revise"):
            return _resp(400, {"error": "decision must be 'approve', 'deny' or 'revise', "
                                        "or provide a per-agent 'decisions' map"})
        if decisions:
            # ONLY a parallel gate can consume one. Sent to a single-agent or `sequence`
            # gate, the map is ignored and the resume below falls through to
            # `decision or "approve"` — so "revise this agent" returned 200 and APPROVED
            # the step. Observed on a live run: the analysis was asked to revise, the
            # gate approved, and the report was written from the un-revised version.
            # Rejected rather than coerced, because guessing which of the three the
            # caller meant is how the original bug reads to a reviewer.
            pending = str(((target["status"].get_item(
                Key={"session_id": sid},
                ProjectionExpression="hitl").get("Item") or {}).get("hitl")
                or {}).get("node") or "")
            allowed = workflow.parallel_gate_ids(target["raw"])
            if pending and pending not in allowed:
                return _resp(400, {"error": (
                    f"the gate this run is waiting at ({pending!r}) takes ONE decision, so a "
                    f"per-agent 'decisions' map cannot be applied to it — send "
                    f"{{\"decision\": \"approve|revise|deny\", \"comment\": \"...\"}} instead. "
                    f"A `sequence` gate re-runs its whole chain on revise, by design. "
                    f"Per-agent decisions are only for a parallel group"
                    + (f" ({', '.join(sorted(allowed))})" if allowed else ""))})
        _audit_run(event, "run.decided", sid, decision=decision or "per-agent",
                   decisions={str(k): str((v or {}).get("decision") if isinstance(v, dict) else v)
                              for k, v in (decisions if isinstance(decisions, dict) else {}).items()}
                   or None,
                   comment=str(body.get("comment") or "")[:200])
        _self_invoke(context.function_name,
                     {"action": "resume", "session_id": sid, "decision": decision or "approve",
                      "comment": body.get("comment", ""), "decisions": decisions,
                      "user": _session_user(sid, target["status"]),
                      **_build_field(target), **_token_field(event, target, sid)})
        return _resp(200, {"ok": True})

    if method == "POST" and params.get("id") and path.endswith("/rerun"):
        # Rewind and re-run downstream. Two shapes:
        #   {"agentId": "x", "comment": "..."}                       single agent
        #   {"agents": [{"agentId": "x", "comment": "..."}, ...]}    subset of a
        #     parallel stage (re-runs those agents in parallel, then re-reviews).
        denied = _forbidden("rerun", event)
        if denied:
            return denied
        sid = params["id"]
        target = _own_session(event, sid)
        agents = body.get("agents")
        agent_id = body.get("agentId") or body.get("agent_id")
        if not agents and not agent_id:
            return _resp(400, {"error": "provide 'agentId' or a non-empty 'agents' list"})
        _audit_run(event, "run.rerun", sid,
                   agents=[str(a.get("agentId") or a.get("agent_id") or "") for a in agents
                           if isinstance(a, dict)] if isinstance(agents, list) else [str(agent_id)],
                   comment=str(body.get("comment") or "")[:200])
        _self_invoke(context.function_name,
                     {"action": "rerun_from", "session_id": sid, "agent_id": agent_id,
                      "comment": body.get("comment", ""), "agents": agents,
                      "user": _session_user(sid, target["status"]), **_build_field(target),
                      **_token_field(event, target, sid)})
        return _resp(200, {"ok": True})

    if method == "POST" and params.get("id") and path.endswith("/evaluate"):
        # On-demand AgentCore Evaluation: score one agent's run now.
        denied = _forbidden("evaluate", event)
        if denied:
            return denied
        sid = params["id"]
        target = _own_session(event, sid)
        agent_id = body.get("agentId") or body.get("agent_id")
        if not agent_id:
            return _resp(400, {"error": "agentId is required"})
        # Answer NOW for an agent that has not enabled evaluations, rather than
        # accepting the request and having the runtime decline it where only the
        # timeline would show it. The runtime enforces this too (the gate lives in
        # evaluations.evaluate_agent, which every path goes through); this is the
        # layer that can still return a status code.
        if agent_id not in (target["view"].get("evalAgents") or []):
            return _resp(400, {"error": f"evaluations are not enabled for '{agent_id}'; "
                                        f"set agentcore.evaluations.enabled on that agent "
                                        f"in workflow.json"})
        _audit_run(event, "run.evaluated", sid, agents=[str(agent_id)])
        _self_invoke(context.function_name,
                     {"action": "evaluate", "session_id": sid, "agent_id": agent_id,
                      "prompt": body.get("prompt", ""),
                      "user": _session_user(sid, target["status"]), **_build_field(target)})
        return _resp(200, {"ok": True})

    # AgentCore Insights: run a cross-run batch analysis / read the latest
    # findings. Not tied to a session (spans every run in the lookback window).
    # A deployed build's Insights cover only that build's runs, and only its owner can
    # start those, so the owner may read and run them without the console-wide
    # `insights` permission — which stays required for THIS deployment's own runtime,
    # whose findings span every user.
    insights_build = ((event.get("queryStringParameters") or {}).get("build") or "").strip()
    if method == "POST" and path == "/api/insights/run":
        if insights_build:
            target = _build_target(insights_build, _owner(event))
        else:
            denied = _forbidden("insights", event)
            if denied:
                return denied
            target = _default_target()
        _self_invoke(context.function_name,
                     {"action": "run_insights", "session_id": "insights",
                      "lookback_hours": int(body.get("lookbackHours") or 0),
                      "user": _user(event), **_build_field(target)})
        return _resp(200, {"ok": True})
    if method == "GET" and path == "/api/insights" and insights_build:
        return _insights_latest(_build_target(insights_build, _owner(event))["runtimeArn"])
    if method == "GET" and path == "/api/insights":
        # Insights analyse EVERY run in the window, whoever started it, so reading the
        # findings is guarded by the same action as producing them.
        denied = _forbidden("insights", event)
        if denied:
            return denied
        return _insights_latest()

    if method == "POST" and path == "/api/chat":
        # In-app assistant: a Bedrock tool-use loop that answers questions about
        # runs and triggers the same runtime actions the UI buttons use.
        if not chatbot.is_enabled():
            return _resp(403, {"error": "assistant disabled"})
        # The assistant can approve gates, re-run agents and start evaluations, so
        # it is an alternative path to the guarded routes above. Pass the caller's
        # permitted actions in and let it drop the tools they may not use —
        # otherwise RBAC would be enforced on the buttons and bypassable by asking.
        # With {"build": id}, the assistant works on that deployed build: its runs, its
        # tables, its workflow — and every action it takes is sent to its runtime.
        build_id = str(body.get("build") or "").strip()
        target = _build_target(build_id, _owner(event)) if build_id else None
        result = chatbot.handle_chat(
            body, body.get("session_id", ""),
            lambda payload: _self_invoke(context.function_name,
                                         {**payload, **(_build_field(target) if target else {})}),
            authz.permitted_actions(event), owner=_owner(event), target=target)
        return _resp(400 if result.get("error") else 200, result)

    # --- Observability (telemetry) read endpoints -------------------------
    if route_key == "GET /api/sessions/{id}/telemetry":
        sid = params.get("id", "")
        return _telemetry_session(sid, _own_session(event, sid, read=True)["telemetry"]
                                  if sid else None)
    if route_key == "GET /api/telemetry/aggregate":
        qs = event.get("queryStringParameters") or {}
        target = _query_target(event, _owner(event))
        return _telemetry_aggregate(qs.get("by", "date"), qs.get("from"), qs.get("to"),
                                    target["telemetry"] if qs.get("build") else None,
                                    mine=None if _everyone(event)
                                    else _my_session_ids(target["status"], _owner(event)))

    return _resp(404, {"error": f"no route for {method} {path}"})


def handler(event, context):
    if event.get("action") == "design" and "requestContext" not in event:
        return designer.run_turn(event)       # a Design-with-AI turn, in the background
    if "requestContext" not in event and gates.is_gate_event(event):
        # A review gate decided by an EventBridge event, or by its timeout (bff/gates.py).
        if status_tbl is None:
            return {"decided": False, "reason": "this deployment runs no workflow"}
        return gates.dispatch(event, status_tbl, _gate_resume(context))
    if "requestContext" not in event and triggers.is_trigger_event(event):
        # A schedule, an EventBridge rule or an SQS message (bff/triggers.py).
        return triggers.dispatch(event, _trigger_start(context))
    if "action" in event and "requestContext" not in event:
        return _run(event)
    try:
        return _api(event, context)
    except Exception as e:  # noqa: BLE001
        # NAME THE FAILURE. Without this, anything unhandled in _api escapes to API
        # Gateway, which answers `{"message": "Internal Server Error"}` — and that is
        # what the browser then shows. The route is absent, the exception type is
        # absent, and the only way to learn either is CloudWatch, which a customer
        # running a workshop does not have open.
        #
        # The status stays 500 because it genuinely is one. What changes is that the
        # response says which route and which exception, and the log line carries the
        # traceback next to the route that produced it.
        method = (event.get("requestContext") or {}).get("http", {}).get("method", "?")
        path = event.get("rawPath", "?")
        print(f"[bff] {method} {path} failed: {type(e).__name__}: {e}")
        traceback.print_exc()
        return _resp(500, {"error": f"{type(e).__name__}: {e}",
                           "route": f"{method} {path}"})
