"""Where Builder builds live, and the ONE description of that layout.

Shared by the BFF (bff/builds.py), which serves the Build view, and by the deploy
runner (deployer/runner.py), which runs in CodeBuild and records what a deploy or a
destroy did. Both import this file, so the key layout cannot drift between them. It
reads no environment and creates no clients: callers pass their own table and S3
client, which is also what keeps it testable.

DynamoDB (`<agentName>_builds`, pk `pk`, sk `sk`, GSI `by_owner` on owner/updated):

    BUILD#<id>  META         the build: name, owner, framework-free metadata, the
                             last deploy/destroy `job`, and what is `deployed`
    BUILD#<id>  RUN#<sid>    a run started against this build (for cleanup)
    RUN#<sid>   BUILD        which build a run belongs to (to route session calls)
    USER#<sub>  ACCOUNT#<n>  an AWS account the user connected, to deploy builds into

Secrets Manager: <SECRETS_PREFIX><id>  a build's tool API keys and A2A tokens, write-only

S3 (`builds bucket`):

    builds/<id>/draft.json          the project the Build view edits (autosaved)
    builds/<id>/versions/<n>.json   the bundle deploy n used — written once, never
                                    changed, so any version can be rebuilt exactly
    builds/<id>/kb/<corpus>/<file>  documents uploaded for a knowledge-base corpus
    connect/<externalId>.json       the role template a user launches to connect an
                                    account
    tfstate/<owner>/<id>/...        Terraform state of a build deployed with Terraform:
                                    one prefix per USER and BUILD, and deleted after every
                                    successful destroy (a destroyed build keeps none)

A build's AWS resources are named from `agentName`, which is assigned once, when the
build is created, and never changes — renaming a build changes only its label. It is
`ax_` + 8 hex characters: 11 characters, well inside the tightest AWS limit it feeds
(the UI bucket name `agentcore-<name>-ui-<account>-<region>` must fit in 63, which
leaves 22 for the name; runtime and memory names allow 48, IAM role names 64).
"""

from __future__ import annotations

import datetime as _dt
import re
import secrets
from pathlib import Path

#: Every deployed build's resource names start with this, which is what lets the
#: console's own role be scoped to `table/ax_*` and `runtime/ax_*` instead of `*`.
AGENT_PREFIX = "ax_"
#: A build's display name. Only a label — see the module docstring for why its length
#: never reaches an AWS resource name.
NAME_MAX = 64
#: Client-generated project ids (web/src/builder/model.ts newProject).
ID_RE = re.compile(r"^[a-z0-9]{6,40}$")
AGENT_RE = re.compile(r"^ax_[0-9a-f]{8}$")
ACCOUNT_RE = re.compile(r"^\d{12}$")
REGION_RE = re.compile(r"^[a-z]{2}(-[a-z]+)+-\d$")
#: A corpus is a top-level folder under kb_docs/, and becomes a doc_type.
CORPUS_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$")
#: The role a connected account creates for the deploy project. Every connection's role
#: starts with this, which is what the console's roles may assume — nothing else.
CONNECT_ROLE_PREFIX = "AgentExpressDeploy-"


def _framework_version() -> str:
    """orchestrator/VERSION: staged next to this module in the BFF package, one level up
    in a source checkout (the runner, the tests)."""
    here = Path(__file__).resolve().parent
    for path in (here / "VERSION", here.parent / "VERSION"):
        if path.exists():
            return path.read_text().strip()
    return ""


#: The framework this console runs, recorded in every bundle it freezes so a build
#: always says what it was built on.
FRAMEWORK_VERSION = _framework_version()

TOOLS = ("cdk", "terraform")
#: A job in one of these is still running; no second deploy/destroy may start.
ACTIVE = ("QUEUED", "RUNNING")


def now() -> str:
    return _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def new_agent_name() -> str:
    return AGENT_PREFIX + secrets.token_hex(4)


def stack_name(agent_name: str) -> str:
    """The CloudFormation stack name cdk/bin/orchestrator.ts derives."""
    return f"{agent_name.replace('_', '-')}-stack"


def meta_key(build_id: str) -> dict:
    return {"pk": f"BUILD#{build_id}", "sk": "META"}


def link_key(build_id: str, session_id: str) -> dict:
    return {"pk": f"BUILD#{build_id}", "sk": f"RUN#{session_id}"}


def run_key(session_id: str) -> dict:
    return {"pk": f"RUN#{session_id}", "sk": "BUILD"}


def account_key(owner: str, account: str) -> dict:
    return {"pk": f"USER#{owner}", "sk": f"ACCOUNT#{account}"}


def connect_role_name(external_id: str) -> str:
    return f"{CONNECT_ROLE_PREFIX}{external_id[:12]}"


def kb_prefix(build_id: str) -> str:
    return f"builds/{build_id}/kb/"


#: The audit log: who did what, to which build, where, and when — plus each sign-in and
#: sign-out. Each event is written TWICE in this table, under the user and under the
#: day, so "my activity" and "everyone's activity on a day" are each one Query:
#:     AUDIT#<sub>        <ts>#<rand>   the user's own events, newest last
#:     AUDIT_DAY#<date>   <ts>#<rand>   every user's events that day (UTC)
#: Neither carries `updated`, so neither enters the by_owner index the builds list reads.
#: Kept, like builds, until someone deletes them: they are a few hundred bytes each.
AUDIT_ACTIONS = ("login", "logout",
                 "build.created", "design.message",
                 "deploy.requested", "deploy.succeeded", "deploy.failed",
                 "destroy.requested", "destroy.succeeded", "destroy.failed",
                 "build.deleted", "account.connected", "account.disconnected",
                 "account.updated", "secrets.updated", "login.viewed",
                 "run.started", "run.decided", "run.cancelled", "run.deleted", "run.rerun",
                 "run.completed", "run.failed", "run.denied",
                 "run.evaluated", "policy.generated", "policy.created", "policy.updated",
                 "policy.deleted", "lambda.code.updated", "tool.tested",
                 "build.shared", "library.created", "library.updated", "library.deleted",
                 "library.shared", "group.saved", "group.deleted")


def audit_items(owner: str, email: str, action: str, detail: dict,
                ts: str = "", rand: str = "") -> list[dict]:
    """The two items one audit event is stored as. Mirrored, key for key, by the Cognito
    trigger (signup_lambda/handler.py), which logs sign-ins and cannot import this."""
    # To the microsecond, so a request and its outcome a moment later sort in order.
    ts = ts or _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    sk = f"{ts}#{rand or secrets.token_hex(4)}"
    body = {"ts": ts, "owner": owner, "email": email, "action": action,
            "detail": {k: v for k, v in detail.items() if v not in (None, "")}}
    return [{"pk": f"AUDIT#{owner}", "sk": sk, **body},
            {"pk": f"AUDIT_DAY#{ts[:10]}", "sk": sk, **body}]


def record_audit(table, owner: str, email: str, action: str, **detail) -> None:
    """Log one event. Never raises: an audit write that fails must not fail (or, worse,
    half-complete) the deploy or destroy it describes — it is printed instead, so it
    still reaches the function's or the job's CloudWatch log."""
    try:
        # Two PutItems, not a BatchWriteItem: the same permission everything else here
        # already holds.
        for item in audit_items(owner, email, action, detail):
            table.put_item(Item=item)
    except Exception as e:  # noqa: BLE001
        print(f"[audit] could not record {action} by {owner}: {type(e).__name__}: {e} "
              f"{detail}")


def _audit_public(item: dict) -> dict:
    return {"id": item["sk"], **{k: item[k] for k in ("ts", "owner", "email", "action",
                                                        "detail") if k in item}}


def audit_of(table, pk: str, limit: int) -> list[dict]:
    """Newest first, at most `limit`."""
    from boto3.dynamodb.conditions import Key
    items = table.query(KeyConditionExpression=Key("pk").eq(pk), ScanIndexForward=False,
                        Limit=limit).get("Items", [])
    return [_audit_public(i) for i in items]


def audit_all(table, pk: str, limit: int) -> list[dict]:
    """Newest first, every page of the partition, at most `limit`: one busy day holds
    more than a single Query returns."""
    from boto3.dynamodb.conditions import Key
    out: list[dict] = []
    kw = {"KeyConditionExpression": Key("pk").eq(pk), "ScanIndexForward": False}
    while len(out) < limit:
        page = table.query(**kw, Limit=limit - len(out))
        out += [_audit_public(i) for i in page.get("Items", [])]
        if "LastEvaluatedKey" not in page:
            break
        kw["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    return out


def draft_key(build_id: str) -> str:
    return f"builds/{build_id}/draft.json"


def version_key(build_id: str, version: int) -> str:
    return f"builds/{build_id}/versions/{int(version)}.json"


def state_prefix(owner: str, build_id: str) -> str:
    """Where a build's Terraform state lives. Per owner AND build, so no user's state
    is ever in another's path, whatever the build ids."""
    safe = re.sub(r"[^A-Za-z0-9@._-]", "_", owner or "anonymous")
    return f"tfstate/{safe}/{build_id}/"


def bundle_of(project: dict, version: int) -> dict:
    """The bundle `scaffold.py apply` reads, from a stored project.

    Mirrors toBundle() in web/src/builder/model.ts: prompts only for agents that still
    exist and are not remote. Built server-side, at deploy time, from the stored draft,
    so what is deployed is exactly what is stored — never a copy the browser sent.
    """
    workflow = project.get("workflow") or {}
    agents = workflow.get("agents") or {}
    prompts = {aid: p for aid, p in (project.get("prompts") or {}).items()
               if aid in agents and (agents[aid] or {}).get("runtime") != "a2a"}
    # The files of each tool written in the build (tools.<key>.code), and only those.
    coded = code_tools(workflow)
    code = {k: files for k, files in (project.get("toolCode") or {}).items() if k in coded}
    return {"format": "agentexpress-bundle", "version": 1,
            "framework": {"version": FRAMEWORK_VERSION},
            "project": {"id": project.get("id"), "name": project.get("name"),
                        "buildVersion": int(version)},
            "workflow": workflow, "prompts": prompts,
            **({"toolCode": code} if code else {})}


def code_tools(workflow: dict) -> list[str]:
    """The tool keys whose function is written in the build (scaffold.py code_tools)."""
    return [k for k, t in (workflow.get("tools") or {}).items()
            if isinstance(t, dict) and str(t.get("type") or "").lower() == "lambda" and "code" in t]


def needs_gateway(workflow: dict) -> bool:
    """A workflow with tools needs the tool plane; one without deploys without it."""
    return bool(workflow.get("tools"))


# --- writes the runner makes -----------------------------------------------------

def set_job(table, build_id: str, **fields) -> None:
    """Update fields of the current job (the map is written when the job starts)."""
    if not fields:
        return
    names = {"#j": "job", "#u": "updated"}
    values = {":u": now()}
    sets = ["#u = :u"]
    for i, (k, v) in enumerate(fields.items()):
        names[f"#f{i}"] = k
        values[f":v{i}"] = v
        sets.append(f"#j.#f{i} = :v{i}")
    table.update_item(Key=meta_key(build_id), UpdateExpression="SET " + ", ".join(sets),
                      ExpressionAttributeNames=names, ExpressionAttributeValues=values)


def record_deployed(table, build_id: str, deployed: dict) -> None:
    table.update_item(
        Key=meta_key(build_id),
        UpdateExpression="SET deployed = :d, versions = :v, #j.#s = :ok, #j.finishedAt = :t, "
                         "#j.phase = :p, updated = :t",
        ExpressionAttributeNames={"#j": "job", "#s": "status"},
        ExpressionAttributeValues={":d": deployed, ":v": int(deployed.get("version") or 0),
                                   ":ok": "SUCCEEDED", ":t": now(), ":p": "done"})


def record_destroyed(table, build_id: str) -> None:
    """Nothing of the build is left in AWS: forget the stack and the tool it used."""
    table.update_item(
        Key=meta_key(build_id),
        UpdateExpression="REMOVE deployed, tool, account SET #j.#s = :ok, #j.finishedAt = :t, "
                         "#j.phase = :p, updated = :t",
        ExpressionAttributeNames={"#j": "job", "#s": "status"},
        ExpressionAttributeValues={":ok": "SUCCEEDED", ":t": now(), ":p": "done"})


def record_failed(table, build_id: str, error: str) -> None:
    table.update_item(
        Key=meta_key(build_id),
        UpdateExpression="SET #j.#s = :f, #j.#e = :e, #j.finishedAt = :t, updated = :t",
        ExpressionAttributeNames={"#j": "job", "#s": "status", "#e": "error"},
        ExpressionAttributeValues={":f": "FAILED", ":e": error[-4000:], ":t": now()})


def run_ids(table, build_id: str) -> list[str]:
    """Every run started against a build."""
    out, kwargs = [], {
        "KeyConditionExpression": "pk = :p AND begins_with(sk, :r)",
        "ExpressionAttributeValues": {":p": f"BUILD#{build_id}", ":r": "RUN#"},
    }
    while True:
        page = table.query(**kwargs)
        out.extend(i["sk"][len("RUN#"):] for i in page.get("Items", []))
        if not page.get("LastEvaluatedKey"):
            return out
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def forget_runs(table, build_id: str) -> int:
    """Drop the run index for a build whose stack — and so whose runs — are gone."""
    ids = run_ids(table, build_id)
    for sid in ids:
        table.delete_item(Key=link_key(build_id, sid))
        table.delete_item(Key=run_key(sid))
    return len(ids)


def delete_prefix(s3, bucket: str, prefix: str) -> int:
    """Delete every object (and every version, the bucket is versioned) under prefix."""
    n = 0
    paginator = s3.get_paginator("list_object_versions")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objs = [{"Key": v["Key"], "VersionId": v["VersionId"]}
                for v in (page.get("Versions") or []) + (page.get("DeleteMarkers") or [])]
        for i in range(0, len(objs), 1000):
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objs[i:i + 1000], "Quiet": True})
            n += len(objs[i:i + 1000])
    return n


def purge(table, s3, bucket: str, build_id: str, owner: str) -> None:
    """Remove every trace of a build: its runs index, its stored versions, its state."""
    forget_runs(table, build_id)
    delete_prefix(s3, bucket, f"builds/{build_id}/")
    delete_prefix(s3, bucket, state_prefix(owner, build_id))
    table.delete_item(Key=meta_key(build_id))
