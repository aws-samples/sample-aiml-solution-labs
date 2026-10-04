"""A user's policy library: Cedar policies saved once and attached to any of their builds.

Three ways to write one, all ending as one Cedar statement that the user reviews before
it is saved (bff/cedar.py checks it):

  * in plain English — AgentCore Policy writes it (StartPolicyGeneration), against the
    policy engine and Gateway of the user's DEPLOYED build, so it knows that build's real
    tools and arguments. `generate` starts it and `generation` returns what AgentCore
    wrote, each statement with AgentCore's own findings and ours, for review: nothing
    here saves it. A build that is not deployed yet, or deploys with the policy engine
    off, has no engine to ask, so plain English needs a deploy first;
  * with the guided form in the Build view (web/src/builder/cedar.ts fromGuided);
  * as raw Cedar.

Stored in the builds table as `USER#<sub>` / `POLICY#<id>`, next to the user's connected
accounts, and private like them: nobody else lists, reads or changes them. An item has
no `updated`, so it stays out of the by_owner index the builds list reads.

Attaching one to a build COPIES it into that build's workflow.json
(orchestrator.policy.custom, with its `libraryId`), so a build is self-contained and a
deploy is reproducible: editing the library later changes no build until the user
attaches it again.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import secrets

import boto3
import cedar

REGION = os.environ.get("AWS_REGION", "us-east-1")
LIMIT = 100
DESCRIPTION_MAX = 1000
REQUEST_MAX = 2000
SOURCES = ("english", "form", "cedar")
PID_RE = re.compile(r"^[0-9a-f]{8}$")
#: StartPolicyGeneration returns at once; the page polls `generation` (about 15 s).
GENERATION_RE = re.compile(r"^[A-Za-z0-9_-]{1,100}$")


def _clients():
    import builds
    return builds._table, builds.BuildError


def _key(owner: str, pid: str) -> dict:
    return {"pk": f"USER#{owner}", "sk": f"POLICY#{pid}"}


def _public(item: dict) -> dict:
    out = {k: item[k] for k in ("id", "name", "description", "statement", "source",
                                "created", "updatedAt") if k in item}
    out.update(cedar.summary(item.get("statement")))
    return out


def _flat(item: dict) -> dict:
    """A library policy in this API's shape (the page predates the library's kinds)."""
    d = item.get("definition") or {}
    out = {k: item[k] for k in ("id", "name", "description", "created", "updatedAt", "ownerEmail",
                                "shares", "mine") if k in item}
    out.update({"statement": d.get("statement", ""), "source": item.get("source") or "cedar"})
    out.update(cedar.summary(out["statement"]))
    return out


def list_policies(owner: str) -> list[dict]:
    """The caller's policies and those shared with them (the library's kind "policy")."""
    import library
    return [_flat(i) for i in library.list_items(owner, "policy")]


def _fields(body: dict) -> dict:
    _, BuildError = _clients()
    name = str(body.get("name") or "")
    if not cedar.NAME_RE.match(name):
        raise BuildError(400, "a policy's name is a letter, then letters and digits (32 at most)")
    description = " ".join(str(body.get("description") or "").split())
    if len(description) > DESCRIPTION_MAX:
        raise BuildError(400, f"a description is at most {DESCRIPTION_MAX} characters")
    statement = body.get("statement")
    # Not against a build's tools: a library policy is attached (and checked) per build.
    wrong = cedar.problems(statement, None)
    if wrong:
        raise BuildError(400, f"the statement {wrong[0]}")
    source = str(body.get("source") or "cedar")
    if source not in SOURCES:
        raise BuildError(400, f"source is one of: {', '.join(SOURCES)}")
    return {"name": name, "description": description, "statement": str(statement).strip(),
            "source": source}


def _body(fields: dict) -> dict:
    return {"kind": "policy", "name": fields["name"], "description": fields["description"],
            "definition": {"statement": fields["statement"]}, "source": fields["source"]}


def create(owner: str, body: dict) -> dict:
    import library
    return _flat(library.create(owner, _body(_fields(body))))


def update(owner: str, pid: str, body: dict) -> dict:
    import library
    item = library.get(pid, owner)
    if item.get("kind") != "policy":
        raise _clients()[1](404, "no such policy")
    d = item.get("definition") or {}
    fields = _fields({"name": item["name"], "description": item.get("description", ""),
                      "statement": d.get("statement"), "source": item.get("source") or "cedar", **body})
    return _flat(library.update(owner, pid, _body(fields)))


def delete(owner: str, pid: str) -> dict:
    import library
    item = library.get(pid, owner)
    if item.get("kind") != "policy":
        raise _clients()[1](404, "no such policy")
    return library.delete(owner, pid)


# --- plain English -> Cedar, by AgentCore Policy ----------------------------------------

def _control(build_id: str, owner: str):
    """(bedrock-agentcore-control client, build meta, deployed workflow) for a deployed
    build of the caller's, in the account and region it runs in."""
    import builds
    item = builds._meta(build_id, owner)
    deployed = item.get("deployed") or {}
    if not deployed.get("version"):
        raise builds.BuildError(409, "plain English is written by AgentCore Policy from your build's own "
                                     "Gateway, so deploy the build first. Until then, use the form or Cedar.")
    region = str(deployed.get("region") or item.get("region") or REGION)
    if deployed.get("account"):
        import accounts
        conn = accounts.require_connected(str(item["owner"]), str(deployed["account"]))
        session = accounts._assume({**conn, "region": region}, "agentexpress-policy")
    else:
        session = boto3.Session(region_name=region)
    return (session.client("bedrock-agentcore-control", region_name=region), item,
            builds._version_workflow(build_id, int(deployed["version"])))


def _engine_and_gateway(ctl, agent_name: str) -> tuple[str, str]:
    """The build's policy engine id (named <agentName>_policy) and Gateway ARN
    (<agent-name>-gw): the names both IaC paths give them."""
    import builds
    engine = gateway = ""
    token = None
    while not engine:
        page = ctl.list_policy_engines(**({"nextToken": token} if token else {}))
        engine = next((e["policyEngineId"] for e in page.get("policyEngines", [])
                       if e.get("name") == f"{agent_name}_policy"), "")
        token = page.get("nextToken")
        if not token:
            break
    if not engine:
        raise builds.BuildError(409, "this build is deployed with the policy engine off "
                                     "(orchestrator.policy.enabled): turn it on and deploy again")
    token = None
    while not gateway:
        page = ctl.list_gateways(**({"nextToken": token} if token else {}))
        gw = next((g for g in page.get("items", []) if g.get("name") == f"{agent_name.replace('_', '-')}-gw"), None)
        if gw:
            gateway = ctl.get_gateway(gatewayIdentifier=gw["gatewayId"])["gatewayArn"]
        token = page.get("nextToken")
        if not token:
            break
    if not gateway:
        raise builds.BuildError(409, "this build's Gateway is not deployed: it has no tools to write a policy for")
    return engine, gateway


def generate(build_id: str, owner: str, text) -> dict:
    """Ask AgentCore Policy to write Cedar for this request. Returns the generation to poll."""
    import builds
    request = " ".join(str(text or "").split())
    if not request:
        raise builds.BuildError(400, "describe what the policy should allow or block")
    if len(request) > REQUEST_MAX:
        raise builds.BuildError(400, f"a description is at most {REQUEST_MAX} characters")
    ctl, item, _wf = _control(build_id, owner)
    engine, gateway = _engine_and_gateway(ctl, item["agentName"])
    try:
        r = ctl.start_policy_generation(policyEngineId=engine, resource={"arn": gateway},
                                        content={"rawText": request}, name=f"ax_{secrets.token_hex(6)}")
    except Exception as e:  # said to the user, who can write it by hand
        print(f"[policies] start_policy_generation: {type(e).__name__}: {e}")
        raise builds.BuildError(502, f"AgentCore Policy could not start writing it ({type(e).__name__}): try "
                                     "again, or write it with the form or in Cedar") from e
    return {"generationId": r["policyGenerationId"], "status": r.get("status", "GENERATING")}


def _statement(asset: dict) -> str:
    d = asset.get("definition") or {}
    return str(((d.get("cedar") or d.get("policy") or {}).get("statement")) or "")


def generation(build_id: str, owner: str, generation_id: str) -> dict:
    """What AgentCore wrote: one asset per requirement it read, each with its Cedar (the
    Gateway ARN put back as {{gateway}}), AgentCore's findings, and our own check."""
    import builds
    if not GENERATION_RE.match(generation_id or ""):
        raise builds.BuildError(400, "invalid generation id")
    ctl, item, wf = _control(build_id, owner)
    engine, gateway = _engine_and_gateway(ctl, item["agentName"])
    try:
        g = ctl.get_policy_generation(policyEngineId=engine, policyGenerationId=generation_id)
    except ctl.exceptions.ResourceNotFoundException:
        raise builds.BuildError(404, "no such generation") from None
    status = g.get("status", "")
    out: dict = {"status": status, "reasons": [str(r) for r in g.get("statusReasons") or []]}
    if status != "GENERATED":
        return out
    assets, token = [], None
    # Read the response body as AgentCore sent it. An asset's definition is a union, and
    # the member AgentCore Policy writes (`policy`) is newer than the boto3 in Lambda's
    # Python runtime, which parses it as SDK_UNKNOWN_MEMBER and drops the statement.
    raw: list = []

    def keep(http_response=None, **_):
        with contextlib.suppress(ValueError, AttributeError):
            raw.append(json.loads(http_response.content))
    event = "after-call.bedrock-agentcore-control.ListPolicyGenerationAssets"
    if hasattr(getattr(ctl, "meta", None), "events"):
        ctl.meta.events.register(event, keep)
    try:
        while True:
            raw.clear()
            page = ctl.list_policy_generation_assets(policyEngineId=engine, policyGenerationId=generation_id,
                                                     maxResults=100, **({"nextToken": token} if token else {}))
            assets += (raw[0].get("policyGenerationAssets") if raw and isinstance(raw[0], dict)
                       else page.get("policyGenerationAssets")) or []
            token = page.get("nextToken")
            if not token:
                break
    finally:
        if hasattr(getattr(ctl, "meta", None), "events"):
            ctl.meta.events.unregister(event, keep)
    keys = list((wf.get("tools") or {}).keys())
    out["assets"] = []
    for a in assets:
        statement = _statement(a).replace(gateway, cedar.GATEWAY).strip()
        findings = [{"type": str(f.get("type") or ""), "description": str(f.get("description") or "")}
                    for f in a.get("findings") or []]
        out["assets"].append({"fragment": str(a.get("rawTextFragment") or ""), "statement": statement,
                              "findings": findings,
                              "problems": cedar.problems(statement, keys) if statement else
                              ["AgentCore could not turn this part into a policy: say it another way"]})
    return out
