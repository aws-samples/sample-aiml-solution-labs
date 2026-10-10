"""Builder builds, served to the Build view: stored per user, deployed one stack each.

A build is what the Build view edits. It lives in the console's builds store (see
buildstore.py for the layout), so it follows its owner to any browser. Deploying it
freezes the current draft as an immutable VERSION and starts the deploy project
(CodeBuild), which builds that version into its own stack — `agentName` = the build's
fixed `ax_xxxxxxxx` — so one build never touches another's resources.

Builds are private to their owner (the JWT `sub`). Runs are not: once started, a run is
visible like every other run in the console, which is how the rest of the app works.

Authorization (deploy, destroy) is checked in handler.py, next to every other guarded
route, so tests/test_authz.py can see every guard in one file.
"""

from __future__ import annotations

import contextlib
import json
import os
import re

import boto3
import buildstore
import codecheck
import validate_build
import workflow as wf_mod
from boto3.dynamodb.conditions import Attr, Key
from botocore.config import Config as BotoConfig
from botocore.exceptions import ReadTimeoutError

BUILDS_TABLE = os.environ.get("BUILDS_TABLE", "")
BUILDS_BUCKET = os.environ.get("BUILDS_BUCKET", "")
DEPLOY_PROJECT = os.environ.get("DEPLOY_PROJECT", "")
#: Where a build's tool API keys and A2A tokens live: <prefix><build id>.
SECRETS_PREFIX = os.environ.get("SECRETS_PREFIX") or "agentexpress/builds/"
REGION = os.environ.get("AWS_REGION", "us-east-1")

# A console tool test is a synchronous request through the HTTP API, which ends EVERY
# request at 30 s whatever the function's own timeout. The Lambda client used to run on
# boto3's defaults: a 60 s read timeout the request could never reach, and legacy
# retries — so a slow tool that hit the read timeout was INVOKED AGAIN, running a
# non-idempotent tool twice for one click. One attempt, and a read timeout just under
# the gateway's, so the caller gets a clear 504 instead of a bare gateway error.
TEST_TOOL_TIMEOUT_S = 27
_TEST_TOOL_CLIENT = BotoConfig(connect_timeout=5, read_timeout=TEST_TOOL_TIMEOUT_S,
                               retries={"total_max_attempts": 1})
#: Off on a deployment without the builder plane (a build's own stack, for one).
ENABLED = bool(BUILDS_TABLE and BUILDS_BUCKET and DEPLOY_PROJECT)

#: A stored project is JSON the Build view wrote; this is far above any real one.
MAX_PROJECT_BYTES = 2_000_000

_table = boto3.resource("dynamodb").Table(BUILDS_TABLE) if ENABLED else None
_s3 = boto3.client("s3") if ENABLED else None
_codebuild = boto3.client("codebuild", region_name=REGION) if ENABLED else None
_logs = boto3.client("logs", region_name=REGION) if ENABLED else None
_secrets = boto3.client("secretsmanager", region_name=REGION) if ENABLED else None

#: Uploaded knowledge-base documents: what Bedrock Knowledge Bases can parse, and a cap
#: well under what one ingestion accepts per file.
DOC_EXTENSIONS = (".pdf", ".txt", ".md", ".html", ".htm", ".csv", ".doc", ".docx",
                  ".xls", ".xlsx")
DOC_MAX_BYTES = 50 * 1024 * 1024


class BuildError(Exception):
    """An expected refusal, with the status code the route should answer."""

    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


class Who(str):
    """A caller: their `sub` (as a str, so every existing comparison keeps working) and
    their email, which is what a share names them by (bff/sharing.py)."""

    email: str = ""

    def __new__(cls, sub: str, email: str = ""):
        obj = super().__new__(cls, sub)
        obj.email = str(email or "").strip().lower()
        return obj


def owner_of(claims: dict) -> Who:
    """Whose builds these are. `sub` is stable across email changes; "anonymous" when the
    deployment has no identity provider (a GSI key may not be empty)."""
    return Who(str(claims.get("sub") or claims.get("email") or "anonymous"), str(claims.get("email") or ""))


# --- reading ----------------------------------------------------------------------

def _meta(build_id: str, owner: str) -> dict:
    if not buildstore.ID_RE.match(build_id or ""):
        raise BuildError(400, "invalid build id")
    item = _table.get_item(Key=buildstore.meta_key(build_id)).get("Item")
    # Someone else's build answers exactly like a missing one — unless it is shared with
    # the caller (bff/sharing.py), who may then do everything its owner may.
    import sharing
    if not item or not sharing.can_access(item, owner):
        raise BuildError(404, "unknown build")
    return item


def _public(item: dict, who=None) -> dict:
    """The META item minus the storage keys; `shared` when the caller is not its owner."""
    out = {k: v for k, v in item.items() if k not in ("pk", "sk", "owner")}
    if who is not None and str(item.get("owner") or "") != str(who):
        out["shared"] = True
    return out


def _refresh(items: list[dict]) -> None:
    """Reconcile jobs the runner has not finished recording with CodeBuild's own view.

    The runner records success and failure itself; this catches what it cannot — a
    build that died before the runner started, was stopped, or timed out."""
    active = {i["job"]["id"]: i for i in items
              if (i.get("job") or {}).get("status") in buildstore.ACTIVE
              and (i.get("job") or {}).get("id")}
    if not active:
        return
    try:
        found = _codebuild.batch_get_builds(ids=list(active)).get("builds", [])
    except Exception as e:  # noqa: BLE001 - a status read must never break the page
        print(f"[builds] batch_get_builds: {type(e).__name__}: {e}")
        return
    for b in found:
        item = active.get(b.get("id"))
        if not item:
            continue
        job = item["job"]
        link = (b.get("logs") or {}).get("deepLink")
        if link and job.get("logsUrl") != link:
            job["logsUrl"] = link
            buildstore.set_job(_table, item["id"], logsUrl=link)
        status = b.get("buildStatus")
        if status == "IN_PROGRESS" and job.get("status") == "QUEUED":
            job["status"] = "RUNNING"
            buildstore.set_job(_table, item["id"], status="RUNNING")
        elif status in ("FAILED", "FAULT", "STOPPED", "TIMED_OUT"):
            msg = (f"The deploy project ended {status} before recording a result. "
                   f"Open the log for the reason.")
            buildstore.record_failed(_table, item["id"], msg)
            job.update(status="FAILED", error=msg)


def list_builds(owner: str) -> list[dict]:
    items, kwargs = [], {"IndexName": "by_owner",
                         "KeyConditionExpression": Key("owner").eq(owner),
                         "ScanIndexForward": False}
    while True:
        page = _table.query(**kwargs)
        items.extend(page.get("Items", []))
        if not page.get("LastEvaluatedKey"):
            break
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    # And every build shared with the caller (bff/sharing.py), newest first with the rest.
    import sharing
    mine = {i["id"] for i in items}
    for ptr in sharing.shared_with(owner, "BUILD"):
        if ptr["id"] in mine:
            continue
        item = _table.get_item(Key=buildstore.meta_key(ptr["id"])).get("Item")
        if not item:          # deleted since: forget the pointer
            _table.delete_item(Key={"pk": ptr["pk"], "sk": ptr["sk"]})
        elif sharing.can_access(item, owner):
            items.append(item)
    items.sort(key=lambda i: str(i.get("updated") or ""), reverse=True)
    _refresh(items)
    return [_public(i, owner) for i in items]


def list_all_builds() -> list[dict]:
    """EVERY user's builds, newest first, each with its owner — for an admin only (the
    handler checks the `admin` permission before calling this)."""
    items, kwargs = [], {"FilterExpression": Attr("sk").eq("META")}
    while True:
        page = _table.scan(**kwargs)
        items.extend(i for i in page.get("Items", []) if str(i.get("pk", "")).startswith("BUILD#"))
        if not page.get("LastEvaluatedKey"):
            break
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    _refresh(items)
    items.sort(key=lambda i: str(i.get("updated") or ""), reverse=True)
    return [{**_public(i), "owner": i.get("owner", ""), "ownerEmail": i.get("ownerEmail", "")}
            for i in items]


def owner_for(build_id: str, caller: str, admin: bool) -> str:
    """Whose build this is, for a READ: the caller's own, or — for an admin — whoever
    owns it. A non-admin gets their own id back, so the usual 404 still answers."""
    if not admin:
        return caller
    if not buildstore.ID_RE.match(build_id or ""):
        raise BuildError(400, "invalid build id")
    item = _table.get_item(Key=buildstore.meta_key(build_id)).get("Item")
    if not item:
        raise BuildError(404, "unknown build")
    return str(item.get("owner") or caller)


def _draft(build_id: str) -> dict:
    try:
        body = _s3.get_object(Bucket=BUILDS_BUCKET, Key=buildstore.draft_key(build_id))["Body"]
        return json.loads(body.read())
    except _s3.exceptions.NoSuchKey:
        return {}


def get_build(build_id: str, owner: str) -> dict:
    """The build, its draft, and the shared items the draft uses (bff/library.py), so the
    page shows them live."""
    import library
    item = _meta(build_id, owner)
    _refresh([item])
    project = _draft(build_id)
    return {"build": _public(item, owner), "project": project,
            "refs": library.refs_for(project, owner, str(item.get("owner") or ""))}


def set_shares(build_id: str, who, shares) -> dict:
    """Share a build (co-building): whoever it is shared with may do what its owner may."""
    import sharing
    item = _meta(build_id, who)
    clean = sharing.normalise(shares)
    _table.update_item(Key=buildstore.meta_key(build_id), UpdateExpression="SET shares = :s",
                       ExpressionAttributeValues={":s": clean})
    sharing.write_pointers("BUILD", build_id, str(item["owner"]), item.get("shares"), clean)
    return clean


# --- writing ----------------------------------------------------------------------

def save(build_id: str, owner: str, project: dict, email: str = "", on_create=None) -> dict:
    """Create or update a build from the Build view's autosave. `on_create(summary)` is
    called once, by the save that created it (the audit log's build.created)."""
    if not buildstore.ID_RE.match(build_id or ""):
        raise BuildError(400, "invalid build id")
    if not isinstance(project, dict) or not isinstance(project.get("workflow"), dict):
        raise BuildError(400, "body must be {\"project\": {..., \"workflow\": {...}}}")
    name = str(project.get("name") or "").strip()
    if not name or len(name) > buildstore.NAME_MAX:
        raise BuildError(400, f"a build name is 1 to {buildstore.NAME_MAX} characters")
    project = {**project, "id": build_id, "name": name}
    base = project.pop("rev", None)       # the save this edit is based on (see below)
    blob = json.dumps(project, ensure_ascii=False).encode()
    if len(blob) > MAX_PROJECT_BYTES:
        raise BuildError(413, "this build is too large to store")

    stamp = buildstore.now()
    agent_name = buildstore.new_agent_name()
    # Co-building: the stored owner, not the caller, stays the owner. And a save based on
    # an older copy than the stored one is refused (409), so two people editing the same
    # build cannot silently overwrite each other: `rev` counts saves.
    import library
    import sharing
    current = _table.get_item(Key=buildstore.meta_key(build_id)).get("Item")
    if current and not sharing.can_access(current, owner):
        raise BuildError(404, "unknown build")
    if current and base is not None and int(current.get("rev") or 0) != int(base):
        raise BuildError(409, f"this build was changed by {current.get('lastEditor') or 'someone else'} "
                              f"since you opened it: reload to get their changes")
    owner_id = str(current.get("owner")) if current else str(owner)
    # Every change to a code tool's files is in the owner's log (lambda.code.updated),
    # whoever made it: the page, or AgentExpress Assistant.
    hashes = code_hashes(project)
    before = (_table.get_item(Key=buildstore.meta_key(build_id), ProjectionExpression="codeHashes, #o",
                              ExpressionAttributeNames={"#o": "owner"}).get("Item") or {})
    try:
        # Create-or-update in one conditional write: a new id gets its agentName here,
        # once; an existing one keeps it; and another user's id is refused.
        res = _table.update_item(
            Key=buildstore.meta_key(build_id),
            UpdateExpression="SET #n = :n, updated = :t, id = :id, "
                             "created = if_not_exists(created, :t), "
                             "agentName = if_not_exists(agentName, :a), "
                             "#o = if_not_exists(#o, :o), versions = if_not_exists(versions, :z), "
                             # Who to invite into the build's own console once deployed: the
                             # owner's address, not a collaborator's.
                             "ownerEmail = if_not_exists(ownerEmail, :e), codeHashes = :h, "
                             "rev = if_not_exists(rev, :z) + :one, lastEditor = :e, "
                             # Which library items it uses, so deleting one finds its
                             # builds without reading every draft (library.detach).
                             "libraryUses = :u",
            ConditionExpression="attribute_not_exists(pk) OR #o = :o",
            ExpressionAttributeNames={"#n": "name", "#o": "owner"},
            ExpressionAttributeValues={":n": name, ":t": stamp, ":id": build_id,
                                       ":a": agent_name, ":o": owner_id, ":one": 1,
                                       ":z": 0, ":e": email, ":h": hashes,
                                       ":u": library.ids_in(project)},
            ReturnValues="ALL_NEW")
    except _table.meta.client.exceptions.ConditionalCheckFailedException:
        raise BuildError(404, "unknown build") from None
    _s3.put_object(Bucket=BUILDS_BUCKET, Key=buildstore.draft_key(build_id), Body=blob,
                   ContentType="application/json")
    if current and str(current.get("owner")) == str(owner) and email and current.get("ownerEmail") != email:
        _table.update_item(Key=buildstore.meta_key(build_id), UpdateExpression="SET ownerEmail = :e",
                           ExpressionAttributeValues={":e": email})
    summary = _public(res["Attributes"], owner)
    was = before.get("codeHashes") or {} if before.get("owner") == owner_id else {}
    changed = sorted(k for k in set(hashes) | set(was) if hashes.get(k) != was.get(k))
    if changed:
        audit(owner_id, email, "lambda.code.updated", build=build_id, name=name, tools=changed,
              removed=sorted(k for k in changed if k not in hashes))
    # The agentName just drawn stuck only if the build did not exist: this save made it.
    if on_create and res["Attributes"].get("agentName") == agent_name:
        on_create(summary)
    return summary


def code_hashes(project: dict) -> dict:
    """A fingerprint of each code function's files: what the log compares between saves."""
    import hashlib
    coded = buildstore.code_functions(project.get("workflow") or {})
    return {k: hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()[:16]
            for k, files in (project.get("toolCode") or {}).items() if k in coded}


def code_errors(project: dict) -> list[str]:
    """What stops a code function from deploying: the static checks' errors (bff/codecheck.py)."""
    out = []
    tools = (project.get("workflow") or {}).get("tools") or {}
    for key in buildstore.code_functions(project.get("workflow") or {}):
        names = [s.get("name") for s in (tools.get(key) or {}).get("toolSchema") or [] if isinstance(s, dict)]
        where = (f"orchestrator.interceptors.{key.split('-', 1)[1]}" if key.startswith("interceptor-")
                 else f"tools.{key}")
        files = (project.get("toolCode") or {}).get(key)
        for p in codecheck.static(files, names):
            if p["severity"] == "error":
                at = f" line {p['line']}" if p["line"] else ""
                out.append(f"{where} {p['file']}{at}: {p['message']}")
    return out


def test_tool(build_id: str, owner: str, key: str, tool: str, event) -> dict:
    """Call a deployed code tool with one event, in the account it runs in."""
    item = _meta(build_id, owner)
    deployed = item.get("deployed") or {}
    if not deployed.get("version"):
        raise BuildError(409, "this build is not deployed: deploy it, then test its tools")
    wf = _version_workflow(build_id, int(deployed["version"]))
    interceptor = key in buildstore.code_interceptors(wf)
    if key not in buildstore.code_tools(wf) and not interceptor:
        raise BuildError(404, f"the deployed version has no tool {key!r} written in the build")
    if interceptor:
        # An interceptor is called with the Gateway's own payload, not a tool's arguments.
        tool = ""
    else:
        names = [s.get("name") for s in wf["tools"][key].get("toolSchema") or [] if isinstance(s, dict)]
        tool = tool or (names[0] if names else "")
        if tool not in names:
            raise BuildError(400, f"{tool!r} is not one of its tools ({', '.join(map(str, names))})")
    if not isinstance(event, dict) or len(json.dumps(event)) > 256_000:
        raise BuildError(400, "the event is a JSON object, at most 256 KB")
    fn = f"ToolLambda-{item['agentName']}-{key}"
    if deployed.get("account"):
        import accounts
        conn = accounts.require_connected(str(item["owner"]), str(deployed["account"]))
        client = accounts._assume({**conn, "region": deployed.get("region") or conn.get("region")},
                                  "agentexpress-test-tool").client("lambda", config=_TEST_TOOL_CLIENT)
    else:
        client = boto3.client("lambda", region_name=REGION, config=_TEST_TOOL_CLIENT)
    try:
        return codecheck.invoke(client, fn, key, tool, event)
    except client.exceptions.ResourceNotFoundException:
        raise BuildError(404, f"{fn} is not deployed") from None
    except ReadTimeoutError:
        spec = (((wf.get("orchestrator") or {}).get("interceptors") or {}).get(key.split("-", 1)[1])
                if interceptor else wf["tools"][key]) or {}
        limit = (spec.get("code") or {}).get("timeoutSeconds")
        raise BuildError(504, (
            f"{fn} did not answer within {TEST_TOOL_TIMEOUT_S}s, the most a console test can "
            f"wait (API Gateway ends every console request at 30s). It may still be running"
            + (f" — its own limit is {limit}s" if limit else "")
            + "; its CloudWatch log group /aws/lambda/" + fn + " has the outcome.")) from None


def _start(item: dict, action: str, tool: str, version: int, user: str,
           delete_after: bool = False, account: str = "", region: str = "") -> dict:
    """Reserve the build for one job, then start CodeBuild. The reservation is a
    conditional write, so two clicks cannot start two jobs on one stack."""
    bid = item["id"]
    job = {"action": action, "tool": tool, "version": version, "status": "QUEUED",
           "phase": "queued", "startedAt": buildstore.now(), "by": user,
           "deleteAfter": delete_after,
           # "" = this console's own account; else a connected account (accounts.py).
           # The runner reads it from here, never from the request.
           "account": account,
           # The region in that account ("" = the connection's, or this console's own).
           "region": region}
    try:
        _table.update_item(
            Key=buildstore.meta_key(bid),
            UpdateExpression="SET #j = :j, tool = :tool, account = :acct, #rg = :rg, updated = :t",
            ConditionExpression="attribute_not_exists(#j) OR NOT (#j.#s IN (:q, :r))",
            ExpressionAttributeNames={"#j": "job", "#s": "status", "#rg": "region"},
            ExpressionAttributeValues={":j": job, ":tool": tool, ":acct": account, ":rg": region,
                                       ":t": job["startedAt"],
                                       ":q": "QUEUED", ":r": "RUNNING"})
    except _table.meta.client.exceptions.ConditionalCheckFailedException:
        raise BuildError(409, "a deploy or destroy of this build is already running") from None
    env = {"ACTION": action, "TOOL": tool, "BUILD_ID": bid, "VERSION": str(version),
           "AGENT_NAME": item["agentName"], "DELETE_AFTER": "1" if delete_after else "0"}
    try:
        started = _codebuild.start_build(
            projectName=DEPLOY_PROJECT,
            environmentVariablesOverride=[{"name": k, "value": v, "type": "PLAINTEXT"}
                                          for k, v in env.items()])
    except Exception as e:
        buildstore.record_failed(_table, bid, f"could not start the deploy project: "
                                              f"{type(e).__name__}: {e}")
        raise BuildError(502, f"could not start the deploy project ({type(e).__name__})") from e
    job_id = started["build"]["id"]
    buildstore.set_job(_table, bid, id=job_id)
    return {**job, "id": job_id}


def deploy(build_id: str, owner: str, tool: str, user: str, account: str = "",
           region: str = "") -> dict:
    if tool not in buildstore.TOOLS:
        raise BuildError(400, f"tool must be one of {', '.join(buildstore.TOOLS)}")
    item = _meta(build_id, owner)
    current = item.get("tool")
    if current and current != tool:
        # Both tools would create resources with the same names; the second would fail
        # half-way and leave a stack neither can cleanly remove.
        raise BuildError(409, f"this build is deployed with {current}. Destroy it first to "
                              f"deploy it with {tool}.")
    if current and str(item.get("account") or "") != account:
        where = item.get("account") or "this console's account"
        raise BuildError(409, f"this build is deployed in {where}. Destroy it first to "
                              f"deploy it somewhere else.")
    region = str(region or "").strip()
    if account:
        import accounts
        # The owner's connection, whoever deploys: a collaborator deploys where the build
        # lives (deployer/runner.py resolves it the same way).
        conn = accounts.require_connected(str(item["owner"]), account)
        # Any region of a connected account: its deploy role is an IAM role, and IAM is
        # global. The connection's region is the default.
        region = region or str(conn.get("region") or REGION)
        if not buildstore.REGION_RE.match(region):
            raise BuildError(400, "invalid region")
    elif region and region != REGION:
        raise BuildError(400, f"a build in this console's account deploys to its region, "
                              f"{REGION}. To use {region}, connect an AWS account and deploy "
                              f"there.")
    else:
        region = REGION
    if current and str(item.get("region") or region) != region:
        raise BuildError(409, f"this build is deployed in {item.get('region')}. Destroy it first "
                              f"to deploy it to {region}.")
    # Shared (library) items the draft uses, resolved as they are NOW: what deploys is a
    # snapshot of them, frozen into this version like the rest (bff/library.py).
    import library
    draft = _draft(build_id)
    project = library.resolve(draft, library.refs_for(draft, owner, str(item.get("owner") or "")))
    # Items kept in sync with an Agent Registry, at their newest approved version: frozen
    # into this version like the library's (bff/registry.py). Never stops a deploy.
    import registry
    project, synced = registry.sync(project)
    if synced:
        print(f"[deploy] {build_id}: from the registry: {'; '.join(synced)}")
    wf = project.get("workflow") or {}
    if not wf.get("agents") or not wf.get("steps"):
        raise BuildError(400, "this build has no agents or no steps to deploy")
    # The Build view's own checks, run here too: a build saved with an error, or sent
    # straight to the API, is refused now rather than failing minutes into CodeBuild.
    problems = validate_build.errors(wf)
    if problems:
        shown = "; ".join(f"{p['path']}: {p['message']}" for p in problems[:3])
        more = f" (and {len(problems) - 3} more)" if len(problems) > 3 else ""
        raise BuildError(400, f"this build has {len(problems)} error"
                              f"{'s' if len(problems) != 1 else ''} to fix before it can "
                              f"deploy — {shown}{more}")
    wrong = code_errors(project)
    if wrong:
        raise BuildError(400, f"a tool's code has {len(wrong)} error{'s' if len(wrong) != 1 else ''} "
                              f"to fix before it can deploy — {'; '.join(wrong[:3])}")
    # What the region cannot run, said now rather than half-way through a deploy.
    search = sorted(k for k, t in (wf.get("tools") or {}).items()
                    if isinstance(t, dict) and str(t.get("type") or "").lower() == "websearch")
    web_regions = validate_build.vocab("webSearchRegions")
    if search and region not in web_regions:
        raise BuildError(400, f"web search ({', '.join(search)}) is only available in "
                              f"{', '.join(web_regions)}, not {region}. Pick one of those "
                              f"regions, or remove the tool.")
    empty = empty_corpora(build_id, owner, wf)
    if empty:
        # Otherwise the synth fails minutes in, CDK and Terraform alike: "corpora names
        # folder(s) that do not exist under orchestrator/kb_docs/" (observed live).
        named = "; ".join(f"{c} (tool {k})" for k, c in empty)
        raise BuildError(400, f"the knowledge base has no documents in {named}. Upload at least one "
                              f"under Build manually → Tools → {empty[0][0]} → Documents, then deploy.")
    # `versions` counts SUCCESSFUL deploys (set by the runner on success), so a failed
    # attempt does not use up a number: the retry deploys the same version again, from
    # a freshly frozen bundle. Only a version that deployed stays as it was.
    version = int(item.get("versions") or 0) + 1
    # Frozen BEFORE the job starts: the runner reads this object, never the draft, so
    # editing the build while it deploys cannot change what is being deployed.
    _s3.put_object(Bucket=BUILDS_BUCKET, Key=buildstore.version_key(build_id, version),
                   Body=json.dumps(buildstore.bundle_of(project, version),
                                   ensure_ascii=False).encode(),
                   ContentType="application/json")
    return _start(item, "deploy", tool, version, user, account=account, region=region)


def _destroy_version(item: dict) -> int:
    return int((item.get("deployed") or {}).get("version")
               or (item.get("job") or {}).get("version") or item.get("versions") or 0)


def destroy(build_id: str, owner: str, user: str, delete_after: bool = False) -> dict:
    item = _meta(build_id, owner)
    tool = item.get("tool")
    if not tool:
        raise BuildError(409, "this build has nothing deployed")
    version = _destroy_version(item)
    if version < 1:
        raise BuildError(409, "this build has nothing deployed")
    # Published to an Agent Registry: its records leave discovery with it (best effort;
    # its skills are not the deployment's and stay).
    if (item.get("registry") or {}).get("records"):
        import registry
        registry.deprecate(item["registry"], f"{item.get('name') or build_id} was destroyed")
    # The tool AND the account the build was DEPLOYED with, never ones the caller names.
    return _start(item, "destroy", tool, version, user, delete_after,
                  account=str(item.get("account") or ""), region=str(item.get("region") or ""))


def app_login(build_id: str, owner: str) -> dict:
    """The caller's login to the build's own console: {user, password, temporary}. The
    owner's was made by the deploy; someone the build is shared with gets theirs here, on
    first ask."""
    item = _meta(build_id, owner)
    if str(item.get("owner") or "") != str(owner):
        return _invite_collaborator(build_id, item, owner)
    try:
        data = json.loads(_secrets.get_secret_value(
            SecretId=f"{_secret_id(build_id)}-login")["SecretString"])
    except _secrets.exceptions.ResourceNotFoundException:
        raise BuildError(404, "no stored login for this build: it was invited before the "
                              "console kept one — use the password from its first email, or "
                              "'Forgot your password?' on its sign-in page") from None
    if not data.get("user"):  # the secret holds only collaborators' logins
        raise BuildError(404, "no stored login for this build: use 'Forgot your password?' "
                              "on its sign-in page")
    return {"user": data.get("user", ""), "password": data.get("password", ""),
            "temporary": bool(data.get("temporary", True)), "at": data.get("at", "")}


def _invite_collaborator(build_id: str, item: dict, who) -> dict:
    """Give someone a build is shared with a login to its own console, like the owner's:
    a temporary password, and every group of the build's pool. The password is kept with
    the owner's (the build's `-login` secret, under `collaborators`), so asking again
    shows it again rather than nothing; a destroy deletes that secret with the pool."""
    import secrets as _s
    deployed = item.get("deployed") or {}
    pool = str(deployed.get("userPoolId") or "")
    email = getattr(who, "email", "")
    if not pool:
        raise BuildError(409, "this build is not deployed, or its app has no sign-in")
    if not email:
        raise BuildError(400, "your sign-in has no email address to invite")
    sid = f"{_secret_id(build_id)}-login"
    try:
        stored = json.loads(_secrets.get_secret_value(SecretId=sid)["SecretString"])
    except _secrets.exceptions.ResourceNotFoundException:
        stored = None
    mine = ((stored or {}).get("collaborators") or {}).get(email.lower())
    if mine and mine.get("pool") == pool:
        return {"user": email, "password": mine.get("password", ""), "temporary": True,
                "at": mine.get("at", "")}
    if deployed.get("account"):
        import accounts
        conn = accounts.require_connected(str(item["owner"]), str(deployed["account"]))
        idp = accounts._assume({**conn, "region": deployed.get("region") or conn.get("region")},
                               "agentexpress-invite").client("cognito-idp")
    else:
        idp = boto3.client("cognito-idp", region_name=deployed.get("region") or REGION)
    password = "Ax-" + _s.token_urlsafe(9) + "7a!"
    try:
        idp.admin_create_user(UserPoolId=pool, Username=email, TemporaryPassword=password,
                              DesiredDeliveryMediums=["EMAIL"],
                              UserAttributes=[{"Name": "email", "Value": email},
                                              {"Name": "email_verified", "Value": "true"}])
        made = True
    except idp.exceptions.UsernameExistsException:
        made = False
    for g in idp.list_groups(UserPoolId=pool).get("Groups", []):
        idp.admin_add_user_to_group(UserPoolId=pool, Username=email, GroupName=g["GroupName"])
    at = buildstore.now()
    if made:
        blob = dict(stored or {})
        blob["collaborators"] = {**(blob.get("collaborators") or {}),
                                 email.lower(): {"password": password, "pool": pool, "at": at}}
        if stored is None:
            _secrets.create_secret(Name=sid, SecretString=json.dumps(blob),
                                   Description=f"AgentExpress build {build_id}: temporary "
                                               "passwords for the build's own console")
        else:
            _secrets.put_secret_value(SecretId=sid, SecretString=json.dumps(blob))
    return {"user": email, "password": password if made else "", "temporary": made,
            "at": at, **({} if made else {"existing": True})}


def label(build_id: str, owner: str) -> dict:
    """What the audit log names a build by. Read before the action, since a delete
    leaves nothing to read after."""
    item = _meta(build_id, owner)
    return {"build": build_id, "name": item.get("name", ""), "agentName": item.get("agentName", "")}


def audit(owner: str, email: str, action: str, **detail) -> None:
    buildstore.record_audit(_table, owner, email, action, **detail)


def activity(owner: str, everyone: bool = False, day: str = "", limit: int = 500) -> list[dict]:
    """The caller's own audit events, or with `everyone` all users' on one UTC day."""
    if everyone:
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", day or ""):
            raise BuildError(400, "day must be YYYY-MM-DD")
        return buildstore.audit_of(_table, f"AUDIT_DAY#{day}", limit)
    return buildstore.audit_of(_table, f"AUDIT#{owner}", limit)


def is_deployed_or_partial(build_id: str, owner: str) -> bool:
    """Whether deleting this build must first destroy something in AWS."""
    return bool(_meta(build_id, owner).get("tool"))


def delete(build_id: str, owner: str) -> None:
    """Delete a build that has nothing in AWS (see is_deployed_or_partial)."""
    item = _meta(build_id, owner)
    if item.get("tool"):
        raise BuildError(409, "destroy this build before deleting it")
    delete_secrets(build_id)
    import sharing
    sharing.drop_pointers("BUILD", build_id, item.get("shares"))
    buildstore.purge(_table, _s3, BUILDS_BUCKET, build_id, str(item["owner"]))


# --- secrets: tool API keys and A2A tokens ------------------------------------------
# Credentials a build's tools and remote agents need at deploy time. Write-only through
# this API: the page can set, replace and clear a value, and see WHICH ones are set, but
# no route ever returns one. The deploy runner reads them (TOOL_API_KEYS / A2A_TOKENS).

#: What a build's secret holds: tool API keys (and OAuth client secrets) by tool, bearer
#: tokens by remote agent, and each identity's secret by identity name.
SECRET_KINDS = ("toolApiKeys", "a2aTokens", "identitySecrets")


def _secret_id(build_id: str) -> str:
    return f"{SECRETS_PREFIX}{build_id}"


def _read_secrets(build_id: str) -> dict:
    try:
        raw = _secrets.get_secret_value(SecretId=_secret_id(build_id))["SecretString"]
        data = json.loads(raw)
    except _secrets.exceptions.ResourceNotFoundException:
        return {k: {} for k in SECRET_KINDS}
    return {k: dict(data.get(k) or {}) for k in SECRET_KINDS}


def secret_names(build_id: str, owner: str) -> dict:
    """Which secrets are set — names only, never values."""
    _meta(build_id, owner)
    data = _read_secrets(build_id)
    return {k: sorted(v) for k, v in data.items()}


def set_secrets(build_id: str, owner: str, body: dict) -> dict:
    """Merge {"toolApiKeys": {tool: key}, "a2aTokens": {agent: token}}; "" clears one."""
    _meta(build_id, owner)
    data = _read_secrets(build_id)
    for kind in SECRET_KINDS:
        for name, value in dict(body.get(kind) or {}).items():
            if not isinstance(name, str) or not name or len(name) > 128:
                raise BuildError(400, f"invalid {kind} name")
            if value in ("", None):
                data[kind].pop(name, None)
            elif not isinstance(value, str) or len(value) > 4096:
                raise BuildError(400, f"{kind}.{name} must be a string of at most 4096 characters")
            else:
                data[kind][name] = value
    blob = json.dumps(data)
    try:
        _secrets.put_secret_value(SecretId=_secret_id(build_id), SecretString=blob)
    except _secrets.exceptions.ResourceNotFoundException:
        _secrets.create_secret(Name=_secret_id(build_id), SecretString=blob,
                               Description=f"AgentExpress build {build_id}: tool API keys "
                                           f"and A2A tokens")
    return {k: sorted(v) for k, v in data.items()}


def delete_secrets(build_id: str) -> None:
    with contextlib.suppress(_secrets.exceptions.ResourceNotFoundException):
        _secrets.delete_secret(SecretId=_secret_id(build_id), ForceDeleteWithoutRecovery=True)


# --- knowledge-base documents ---------------------------------------------------------
# A browser cannot put files into kb_docs/, so a build's documents are uploaded here
# (straight to S3, with a presigned POST that caps the size) and the deploy runner copies
# them into kb_docs/<corpus>/ before it deploys.

def _doc_key(build_id: str, corpus: str, name: str) -> str:
    if not buildstore.CORPUS_RE.match(corpus or ""):
        raise BuildError(400, "a corpus name is lower-case letters, digits, - and _")
    base = os.path.basename(str(name or "")).strip()
    if not base or base != name or base.startswith(".") or len(base) > 200:
        raise BuildError(400, "invalid file name")
    if not base.lower().endswith(DOC_EXTENSIONS):
        raise BuildError(400, f"a document must be one of {', '.join(DOC_EXTENSIONS)}")
    return f"{buildstore.kb_prefix(build_id)}{corpus}/{base}"


def doc_upload(build_id: str, owner: str, corpus: str, name: str) -> dict:
    _meta(build_id, owner)
    key = _doc_key(build_id, corpus, name)
    post = _s3.generate_presigned_post(
        Bucket=BUILDS_BUCKET, Key=key, ExpiresIn=900,
        Conditions=[["content-length-range", 1, DOC_MAX_BYTES]])
    return {"url": post["url"], "fields": post["fields"], "maxBytes": DOC_MAX_BYTES}


def list_docs(build_id: str, owner: str) -> list[dict]:
    _meta(build_id, owner)
    prefix, out = buildstore.kb_prefix(build_id), []
    for page in _s3.get_paginator("list_objects_v2").paginate(Bucket=BUILDS_BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            corpus, _, name = o["Key"][len(prefix):].partition("/")
            if name:
                out.append({"corpus": corpus, "name": name, "size": o["Size"],
                            "uploaded": o["LastModified"].strftime("%Y-%m-%dT%H:%M:%SZ")})
    return out


def empty_corpora(build_id: str, owner: str, workflow: dict) -> list[tuple[str, str]]:
    """(tool key, corpus) for each corpus a knowledge base deployed from uploads names
    that has no document uploaded for this build. The console's deploy source carries no
    sample documents (observed live: "Found: (none)"), so an upload is the only way a
    corpus gets one. A knowledge base on the customer's own bucket (`s3Uri`) or an
    existing one (`knowledgeBaseId`) is not checked: its corpora are values in their
    metadata."""
    kbs = [(k, t) for k, t in (workflow.get("tools") or {}).items()
           if isinstance(t, dict) and t.get("type") == "kb" and not t.get("s3Uri") and not t.get("knowledgeBaseId")]
    if not kbs:
        return []
    have = {d["corpus"] for d in list_docs(build_id, owner)}
    return [(k, str(c)) for k, t in kbs for c in t.get("corpora") or [] if str(c) not in have]


def delete_doc(build_id: str, owner: str, corpus: str, name: str) -> None:
    _meta(build_id, owner)
    _s3.delete_object(Bucket=BUILDS_BUCKET, Key=_doc_key(build_id, corpus, name))


def job_log(build_id: str, owner: str, limit: int = 200) -> dict:
    """The tail of the latest job's CodeBuild log, so a failure is readable in place."""
    job = _meta(build_id, owner).get("job") or {}
    if not job.get("id"):
        return {"lines": []}
    builds = _codebuild.batch_get_builds(ids=[job["id"]]).get("builds", [])
    logs = (builds[0].get("logs") or {}) if builds else {}
    if not logs.get("groupName") or not logs.get("streamName"):
        return {"lines": [], "status": builds[0].get("buildStatus") if builds else None}
    events = _logs.get_log_events(logGroupName=logs["groupName"],
                                  logStreamName=logs["streamName"],
                                  limit=limit, startFromHead=False).get("events", [])
    return {"lines": [e.get("message", "").rstrip("\n") for e in events],
            "status": builds[0].get("buildStatus"), "logsUrl": logs.get("deepLink")}


# --- running a deployed build -------------------------------------------------------

_versions: dict[tuple[str, int], dict] = {}


def _version_workflow(build_id: str, version: int) -> dict:
    """The workflow a deployed version carries. Immutable, so cached per warm Lambda."""
    key = (build_id, version)
    if key not in _versions:
        body = _s3.get_object(Bucket=BUILDS_BUCKET,
                              Key=buildstore.version_key(build_id, version))["Body"]
        _versions[key] = json.loads(body.read()).get("workflow") or {}
    return _versions[key]


def target(build_id: str, owner: str | None = None) -> dict:
    """Everything needed to run against a deployed build.

    `owner` is checked when a run is STARTED (only your own builds appear in your
    picker); session calls afterwards resolve by run id and are not owner-scoped, like
    every other run in the console."""
    if not ENABLED:
        raise BuildError(404, "the Builder is not enabled on this deployment")
    item = (_meta(build_id, owner) if owner is not None
            else _table.get_item(Key=buildstore.meta_key(build_id)).get("Item"))
    if not item:
        raise BuildError(404, "unknown build")
    deployed = item.get("deployed") or {}
    if not deployed.get("runtimeArn"):
        raise BuildError(409, "this build is not deployed")
    if deployed.get("account"):
        # Deployed into the user's own account: it runs from its own console there
        # (deployed.uiUrl), with its data never passing through this one.
        raise BuildError(409, "this build runs in its own console, in the account it is "
                              "deployed to; open it from the build's Deployment panel")
    if (item.get("job") or {}).get("action") == "destroy" \
            and (item.get("job") or {}).get("status") in buildstore.ACTIVE:
        raise BuildError(409, "this build is being destroyed")
    version = int(deployed["version"])
    raw = _version_workflow(build_id, version)
    return {"build": build_id, "name": item.get("name", ""), "version": version,
            "agentName": item.get("agentName", ""),
            "runtimeArn": deployed["runtimeArn"], "statusTable": deployed["statusTable"],
            "eventsTable": deployed["eventsTable"],
            "telemetryTable": deployed["telemetryTable"],
            "raw": raw, "view": wf_mod.project(raw)}


def build_of_run(session_id: str) -> str:
    """Which build a run belongs to; "" for a run of this deployment's own workflow."""
    if not ENABLED or not session_id:
        return ""
    item = _table.get_item(Key=buildstore.run_key(session_id)).get("Item")
    return str((item or {}).get("build") or "")


def link_run(build_id: str, session_id: str) -> None:
    _table.put_item(Item={**buildstore.run_key(session_id), "build": build_id,
                          "created": buildstore.now()})
    _table.put_item(Item={**buildstore.link_key(build_id, session_id),
                          "created": buildstore.now()})


def unlink_run(build_id: str, session_id: str) -> None:
    _table.delete_item(Key=buildstore.run_key(session_id))
    _table.delete_item(Key=buildstore.link_key(build_id, session_id))


# --- AWS Agent Registry (bff/registry.py) ---------------------------------------------

def registry_call(fn, *args):
    """A registry call, its refusal said plainly: no access, no such registry, or a throttle."""
    try:
        return fn(*args)
    except BuildError:
        raise
    except Exception as e:
        err = getattr(e, "response", {}).get("Error", {}) if hasattr(e, "response") else {}
        code = err.get("Code") or type(e).__name__
        # A Conflict is the caller's to resolve (another build already published that
        # name and version), not a fault of the registry.
        status = (403 if "AccessDenied" in code else 404 if "NotFound" in code
                  else 409 if "Conflict" in code else 502)
        raise BuildError(status, f"the Agent Registry answered {code}: {str(err.get('Message') or e)[:300]}") from e


def registry_state(build_id: str, owner: str) -> dict:
    """For the Builder: a newer approved version of each item this build took from a
    registry, and where the build is published (R2)."""
    import registry
    item = _meta(build_id, owner)
    draft = _draft(build_id)
    ups = registry_call(registry.updates, draft) if registry._linked(draft) else []
    published = item.get("registry") or {}
    return {"updates": ups,
            "published": registry_call(registry.statuses, published) if published else {}}


def publish(build_id: str, owner: str, body: dict, user: str) -> dict:
    """Publish to an Agent Registry, for approval there: the deployed build (`what`
    "build": its workflow, and its Gateway's tools) or one of its skills ("skill"). The
    route lets only an admin call this. Publishing again submits the next version of the
    same record(s)."""
    import library
    import registry
    rid = registry._registry_id(body.get("registry"))
    what = str(body.get("what") or "")
    item = _meta(build_id, owner)
    pub = json.loads(json.dumps(item.get("registry") or {}, default=str))
    if what == "build":
        dep = item.get("deployed") or {}
        if not dep.get("version"):
            raise BuildError(409, "deploy the build first: what is published is the deployed version")
        version = int(dep["version"])
        specs = registry.build_records(item, _version_workflow(build_id, version))
        records = dict(pub.get("records") or {})
        for key, spec in specs.items():
            records[key] = registry_call(registry.put_record, rid, spec, f"{version}.0.0", records.get(key))
        pub.update(registryId=rid, version=version, at=buildstore.now(), by=user, records=records)
    elif what == "skill":
        name = str(body.get("skill") or "")
        draft = _draft(build_id)
        project = library.resolve(draft, library.refs_for(draft, owner, str(item.get("owner") or "")))
        skill = ((project.get("workflow") or {}).get("skills") or {}).get(name)
        if not isinstance(skill, dict) or "library" in skill or not str(skill.get("instructions") or "").strip():
            raise BuildError(404, f"this build has no skill {name!r} with instructions to publish")
        skills = dict(pub.get("skills") or {})
        prior = skills.get(name) or {}
        n = int(prior.get("n") or 0) + 1
        rec = registry_call(registry.put_record, rid, registry.skill_record(name, skill), f"{n}.0.0", prior)
        skills[name] = {**rec, "n": n, "at": buildstore.now(), "by": user}
        pub.update(skills=skills)
        pub.setdefault("registryId", rid)
    else:
        raise BuildError(400, 'what: "build" or "skill"')
    _table.update_item(Key=buildstore.meta_key(build_id), UpdateExpression="SET #r = :r",
                       ExpressionAttributeNames={"#r": "registry"}, ExpressionAttributeValues={":r": pub})
    return registry_call(registry.statuses, pub)
