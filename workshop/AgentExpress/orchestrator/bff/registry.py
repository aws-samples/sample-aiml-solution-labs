"""AWS Agent Registry, for the Builder: find MCP servers, agents and skills an organization
has approved, and take one into a build (R1, S2); keep it in sync when asked to.

  list_registries()          the registries in this console's account and region
  search(registry, q, kind)  APPROVED records only (the discoverable API never returns others),
                             each mapped to what the build would hold (to_entry)
  updates(project)           for each item that came from a registry: a newer approved
                             version, if there is one
  sync(project)              those marked `registry.sync`, brought to the newer version
                             (before a deploy, and when the Builder opens the build)

What a record becomes (to_entry):
  MCP    -> an `mcp` tool: its endpoint (a streamable-http remote) and its tools as
            `toolSchema` (which a User login tool needs). How it connects stays the
            build's choice: nothing here sets auth.
  AGENT  -> a `runtime: "a2a"` agent at its A2A card's URL.
  SKILL  -> a skill: its SKILL.md's description and body.
A record this cannot use (no endpoint, no card URL, no SKILL.md) comes back with `why`.
Nothing is ever fetched from a record's URLs here: a record is data, and the build author
chooses to take it.
"""
from __future__ import annotations

import json
import os
import re

REGION = os.environ.get("AWS_REGION", "us-east-1")
_clients: dict = {}
#: The two Registry API models, shipped with the BFF: the Lambda runtime's own boto3
#: predates them (seen live: "Unknown service: 'agent-registry-control'"). Searched
#: LAST, so a runtime whose botocore has them uses its own.
MODELS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "botocore_data")


def _client(name: str):
    if name not in _clients:
        import boto3
        import botocore.session
        core = botocore.session.get_session()
        loader = core.get_component("data_loader")
        if MODELS not in loader.search_paths:
            loader.search_paths.append(MODELS)
        _clients[name] = boto3.session.Session(botocore_session=core).client(name, region_name=REGION)
    return _clients[name]


def _ctl():
    if "ctl" not in _clients:
        _clients["ctl"] = _client("agent-registry-control")
    return _clients["ctl"]


def _dp():
    if "dp" not in _clients:
        _clients["dp"] = _client("agent-registry")
    return _clients["dp"]


def _error():
    import builds
    return builds.BuildError


ID_RE = re.compile(r"^[A-Za-z0-9]{6,64}$")
KINDS = {"tool": "MCP", "agent": "AGENT", "skill": "SKILL"}
#: SearchDiscoverableRegistryRecords' maxResults limit.
SEARCH_MAX = 20
KEY_RE = re.compile(r"^[A-Za-z][A-Za-z0-9]{0,31}$")
#: toolSchema property types (keys.json): anything else is passed as a string.
PROP_TYPES = {"string", "number", "integer", "boolean", "array", "object"}


def list_registries() -> list[dict]:
    """Every registry here, READY ones first. The Builder picks the only one by itself."""
    out, token = [], None
    while True:
        got = _ctl().list_registries(**({"nextToken": token} if token else {}))
        out += [{"id": r["registryId"], "name": r.get("name", ""), "description": r.get("description", ""),
                 "status": r.get("status", "")} for r in got.get("registries", [])]
        token = got.get("nextToken")
        if not token or len(out) >= 200:
            break
    return sorted(out, key=lambda r: (r["status"] != "READY", r["name"].lower()))


def _registry_id(value) -> str:
    rid = str(value or "")
    if not ID_RE.match(rid):
        raise _error()(400, "registry: a registry id")
    return rid


def _json(text) -> dict:
    try:
        v = json.loads(text) if isinstance(text, str) and text.strip() else {}
    except ValueError:
        return {}
    return v if isinstance(v, dict) else {}


def key_for(name: str) -> str:
    """"refund-policy" -> "refundPolicy": a name a build takes (letters and digits)."""
    parts = [p for p in re.split(r"[^A-Za-z0-9]+", str(name or "")) if p]
    out = "".join(p[0].lower() + p[1:] if i == 0 else p[0].upper() + p[1:] for i, p in enumerate(parts))
    if not re.match(r"^[A-Za-z]", out):
        out = f"item{out}"
    return out[:32]


def parse_skill_md(text: str) -> dict:
    """A SKILL.md's frontmatter name and description, and its body (SkillForm.tsx parseSkillMd)."""
    m = re.match(r"^\ufeff?---\s*\r?\n(.*?)\r?\n---\s*(?:\r?\n|$)(.*)$", text or "", re.DOTALL)
    if not m:
        return {"name": "", "description": "", "instructions": (text or "").strip()}

    def field(k):
        f = re.search(rf"^{k}\s*:\s*(.*)$", m.group(1), re.MULTILINE)
        v = f.group(1).strip() if f else ""
        if len(v) >= 2 and v[0] == v[-1] == '"':
            try:
                return str(json.loads(v))     # a double-quoted YAML scalar, as skill_md writes it
            except ValueError:
                return v[1:-1]
        return v[1:-1] if len(v) >= 2 and v[0] == v[-1] == "'" else v
    return {"name": field("name"), "description": field("description"), "instructions": m.group(2).strip()}


def _tool_schema(tools: list) -> list[dict]:
    """MCP tool definitions as workflow.json toolSchema entries."""
    out = []
    for t in tools[:50]:
        if not isinstance(t, dict) or not isinstance(t.get("name"), str):
            continue
        schema = t.get("inputSchema") if isinstance(t.get("inputSchema"), dict) else {}
        props = schema.get("properties") if isinstance(schema.get("properties"), dict) else {}
        required = set(schema.get("required") or [])
        out.append({"name": t["name"][:64], "description": str(t.get("description") or t["name"])[:500],
                    "properties": {p: {"type": d.get("type") if d.get("type") in PROP_TYPES else "string",
                                       "required": p in required,
                                       "description": str(d.get("description") or p)[:300]}
                                   for p, d in props.items() if isinstance(d, dict)}})
    return out


def _ref(rec: dict, registry_id: str, sync: bool = False) -> dict:
    return {"registryId": registry_id, "recordId": rec.get("recordId", ""), "name": rec.get("name", ""),
            "version": str(rec.get("recordVersion") or ""), "sync": bool(sync)}


def to_entry(rec: dict, registry_id: str, sync: bool = False) -> dict:
    """{"kind", "key", "entry"} for what the build would hold, or {"kind", "why"}."""
    rtype = rec.get("recordType")
    d = rec.get("descriptors") or {}
    key = key_for(rec.get("name") or "")
    about = str(rec.get("description") or rec.get("displayName") or rec.get("name") or "")
    if rtype == "MCP" or (rtype == "AGENT" and d.get("mcpServer")):
        mcp = d.get("mcpServer") or {}
        server = _json(mcp.get("data"))
        remotes = [r for r in server.get("remotes") or [] if isinstance(r, dict) and isinstance(r.get("url"), str)]
        remote = next((r for r in remotes if r.get("type") in (None, "streamable-http")), None)
        if not remote or not remote["url"].startswith("https://"):
            return {"kind": "tool", "why": "the record names no https streamable-HTTP endpoint for this server"}
        tools = _json(((mcp.get("additionalData") or {}).get("tools") or {}).get("data")).get("tools") or []
        entry = {"type": "mcp", "description": str(server.get("description") or about)[:500] or key,
                 "endpoint": remote["url"], "registry": _ref(rec, registry_id, sync)}
        schema = _tool_schema(tools if isinstance(tools, list) else [])
        if schema:
            entry["toolSchema"] = schema
        return {"kind": "tool", "key": key, "entry": entry, "tools": [s["name"] for s in schema]}
    if rtype == "AGENT":
        card = _json((d.get("a2aAgentCard") or {}).get("data"))
        url = card.get("url")
        if not isinstance(url, str) or not url.startswith("https://"):
            return {"kind": "agent", "why": "the record has no A2A agent card with an https URL"}
        return {"kind": "agent", "key": key, "entry": {
            "name": str(card.get("name") or rec.get("name") or key)[:80], "runtime": "a2a", "agentCard": url,
            "produces": key, "registry": _ref(rec, registry_id, sync)},
            "skills": [s.get("name") for s in card.get("skills") or [] if isinstance(s, dict)]}
    if rtype == "SKILL":
        sk = d.get("agentSkillsDefinition") or {}
        md = ((sk.get("additionalData") or {}).get("skillMd") or {}).get("data")
        if not isinstance(md, str) or not md.strip():
            return {"kind": "skill", "why": "the record carries no SKILL.md"}
        got = parse_skill_md(md)
        if not got["instructions"]:
            return {"kind": "skill", "why": "its SKILL.md has no instructions"}
        return {"kind": "skill", "key": key_for(got["name"] or rec.get("name") or ""), "entry": {
            "description": (got["description"] or about)[:1000], "instructions": got["instructions"],
            "registry": _ref(rec, registry_id, sync)}}
    return {"kind": None, "why": f"a {rtype or 'record'} of this type cannot be added to a build"}


def _summary(rec: dict, registry_id: str) -> dict:
    out = {"recordId": rec.get("recordId"), "name": rec.get("name"), "displayName": rec.get("displayName") or "",
           "description": rec.get("description") or "", "type": rec.get("recordType"),
           "version": str(rec.get("recordVersion") or ""), "updatedAt": str(rec.get("updatedAt") or "")}
    return {**out, **to_entry(rec, registry_id)}


def search(registry_id, query, kind: str = "") -> list[dict]:
    """Approved records matching `query` (natural language or keywords), as the build would
    take them. `kind`: tool | agent | skill, to narrow."""
    rid = _registry_id(registry_id)
    q = str(query or "").strip()[:500]
    if kind and kind not in KINDS:
        raise _error()(400, "kind: tool, agent or skill")
    if q:
        # 20 is the most a search returns (seen live: a ValidationException at 25).
        recs = _dp().search_discoverable_registry_records(searchQuery=q, registryIds=[rid],
                                                          maxResults=SEARCH_MAX).get("registryRecords", [])
    else:
        # Browsing: the catalog, newest first (no query to rank by).
        args = {"registryId": rid, "maxResults": 50}
        if kind:
            args["filters"] = [{"name": "recordType", "values": [KINDS[kind]]}]
        listed = _dp().list_discoverable_registry_records(**args).get("registryRecords", [])
        ids = [r["recordId"] for r in listed][:50]
        recs = _batch(rid, ids)
        recs.sort(key=lambda r: str(r.get("updatedAt") or ""), reverse=True)
    if kind:
        recs = [r for r in recs if r.get("recordType") == KINDS[kind]
                or (kind == "tool" and r.get("recordType") == "AGENT"
                    and (r.get("descriptors") or {}).get("mcpServer"))]
    return [_summary(r, rid) for r in recs]


def _batch(registry_id: str, record_ids: list[str]) -> list[dict]:
    out = []
    for i in range(0, len(record_ids), 20):
        got = _dp().batch_get_discoverable_registry_record(
            entries=[{"registryId": registry_id, "recordIds": record_ids[i:i + 20]}])
        out += got.get("registryRecords", [])
    return out


def _newer(a: str, b: str) -> bool:
    """Whether version `a` is newer than `b` (dotted numbers compared as numbers)."""
    def parts(v):
        return [(0, int(x)) if x.isdigit() else (1, x) for x in re.split(r"[.\-+]", str(v or ""))]
    try:
        return parts(a) > parts(b)
    except TypeError:
        return str(a) != str(b)


#: workflow map -> what a record there becomes
_MAPS = {"tools": "tool", "agents": "agent", "skills": "skill"}


def _linked(project: dict) -> list[tuple[str, str, dict]]:
    """(map, key, registry ref) for each item that came from a registry."""
    wf = project.get("workflow") or {}
    out = []
    for m in _MAPS:
        for key, entry in (wf.get(m) or {}).items():
            ref = entry.get("registry") if isinstance(entry, dict) else None
            if isinstance(ref, dict) and ID_RE.match(str(ref.get("registryId") or "")) and ref.get("recordId"):
                out.append((m, key, ref))
    return out


def updates(project: dict) -> list[dict]:
    """A newer approved version for each item that came from a registry: the same record
    re-versioned, or another record of the same name and type with a higher version."""
    found = []
    by_registry: dict[str, list] = {}
    for m, key, ref in _linked(project):
        by_registry.setdefault(ref["registryId"], []).append((m, key, ref))
    for rid, items in by_registry.items():
        current = {r["recordId"]: r for r in _batch(rid, sorted({ref["recordId"] for _, _, ref in items}))}
        catalog = None
        for m, key, ref in items:
            best = current.get(ref["recordId"])
            best = best if best and _newer(str(best.get("recordVersion")), ref.get("version", "")) else None
            if catalog is None:
                catalog = _dp().list_discoverable_registry_records(registryId=rid, maxResults=100).get(
                    "registryRecords", [])
            for r in catalog:
                if r.get("name") == ref.get("name") and r.get("recordId") != ref["recordId"] and _newer(
                        str(r.get("recordVersion")), str((best or {}).get("recordVersion") or ref.get("version", ""))):
                    best = (_batch(rid, [r["recordId"]]) or [None])[0]
            if best:
                mapped = to_entry(best, rid, bool(ref.get("sync")))
                if mapped.get("entry"):
                    found.append({"map": m, "key": key, "sync": bool(ref.get("sync")),
                                  "from": ref.get("version", ""), "to": str(best.get("recordVersion") or ""),
                                  "entry": mapped["entry"]})
    return found


#: What a newer version replaces, per map: the rest (how it connects, call, args,
#: policies, a skill's reference files) stays the build's.
_TAKES = {"tools": ("description", "endpoint", "toolSchema", "registry"),
          "agents": ("agentCard", "registry"),
          "skills": ("description", "instructions", "registry")}


def apply(project: dict, ups: list[dict], only_sync: bool = True) -> tuple[dict, list[str]]:
    """The project with these updates applied (by default only those marked sync)."""
    wf = dict(project.get("workflow") or {})
    changed = []
    for u in ups:
        if only_sync and not u.get("sync"):
            continue
        current = (wf.get(u["map"]) or {}).get(u["key"])
        if not isinstance(current, dict):
            continue
        nxt = dict(current)
        for k in _TAKES[u["map"]]:
            if k in u["entry"]:
                nxt[k] = u["entry"][k]
            elif k != "registry":
                nxt.pop(k, None)
        wf[u["map"]] = {**(wf.get(u["map"]) or {}), u["key"]: nxt}
        changed.append(f"{u['map']}.{u['key']} {u['from']} -> {u['to']}")
    return {**project, "workflow": wf}, changed


def sync(project: dict) -> tuple[dict, list[str]]:
    """Items marked `registry.sync`, brought to their newest approved version. A registry
    that cannot be reached leaves the build as it is (said in the result, never fatal)."""
    if not _linked(project):
        return project, []
    try:
        return apply(project, updates(project))
    except Exception as e:  # noqa: BLE001 - a deploy is not stopped by a catalog outage
        return project, [f"registry not reached ({type(e).__name__}): kept the versions in the build"]


# --- publishing (R2: a deployed build; S2: a skill) -------------------------------------
# A record is created as a DRAFT and submitted for approval: the console never approves
# its own records (a curator does, or the registry's own auto-approval). Publishing again
# updates the same record to the new version and submits it again; the registry keeps
# serving the last APPROVED version until the new one is approved (seen live).

#: How long a new record may take to leave CREATING before it can be submitted.
CREATE_WAIT_S = 45


def _kebab(name: str) -> str:
    return re.sub(r"([a-z0-9])([A-Z])", r"\1-\2", str(name or "")).lower()


def skill_md(name: str, skill: dict) -> str:
    """The SKILL.md a skill is published as (SkillForm.tsx toSkillMd)."""
    # Quoted: a description is prose, and an unquoted one with ": " in it is not valid
    # YAML. Seen live: the registry refused it ("skillMd frontmatter is not valid").
    desc = json.dumps(re.sub(r"\s+", " ", str(skill.get("description") or "")).strip(), ensure_ascii=False)
    return f"---\nname: {_kebab(name)}\ndescription: {desc}\n---\n\n{str(skill.get('instructions') or '').strip()}\n"


def _input_schema(props: dict) -> dict:
    """toolSchema properties ({p: {type, required, description}}) as a JSON Schema."""
    props = props if isinstance(props, dict) else {}
    return {"type": "object",
            "properties": {p: {"type": d.get("type") if d.get("type") in PROP_TYPES else "string",
                               "description": str(d.get("description") or p)}
                           for p, d in props.items() if isinstance(d, dict)},
            "required": [p for p, d in props.items() if isinstance(d, dict) and d.get("required")]}


def gateway_tools(workflow: dict) -> list[dict]:
    """What the build's (machine) Gateway publishes, as MCP tool definitions, from its
    workflow: `<tool>___<name>` as the Gateway composes it. A tool whose operations are
    only known from its server or API (an MCP server, an API Gateway stage) is listed by
    its key. Tools that act as the person are on the person Gateway: not here."""
    out = []
    for key, t in (workflow.get("tools") or {}).items():
        if not isinstance(t, dict) or str(t.get("auth") or "").lower() in ("user", "obo"):
            continue
        typ, about = str(t.get("type") or "").lower(), str(t.get("description") or key)[:500]
        schema = t.get("toolSchema") if isinstance(t.get("toolSchema"), list) else []
        doc = t.get("schema") if isinstance(t.get("schema"), dict) else {}
        if schema:
            out += [{"name": f"{key}___{s.get('name')}", "description": str(s.get("description") or about)[:500],
                     "inputSchema": _input_schema(s.get("properties"))} for s in schema if isinstance(s, dict)]
        elif typ == "openapi" and isinstance(doc.get("paths"), dict):
            for ops in doc["paths"].values():
                for op in (ops or {}).values() if isinstance(ops, dict) else []:
                    if isinstance(op, dict) and op.get("operationId"):
                        params = {p["name"]: {"type": (p.get("schema") or {}).get("type", "string"),
                                              "required": bool(p.get("required")),
                                              "description": p.get("description") or p["name"]}
                                  for p in op.get("parameters") or [] if isinstance(p, dict) and p.get("name")}
                        out.append({"name": f"{key}___{op['operationId']}",
                                    "description": str(op.get("summary") or op.get("description") or about)[:500],
                                    "inputSchema": _input_schema(params)})
        else:
            word = {"kb": "retrieve", "websearch": "search"}.get(typ)
            out.append({"name": f"{key}___{word}" if word else key, "description": about,
                        "inputSchema": _input_schema({"query": {"type": "string", "required": True,
                                                                "description": "What to find."}})})
    return out[:100]


def build_records(item: dict, workflow: dict) -> dict[str, dict]:
    """The records a deployed build is published as: its workflow (an AGENT record; a
    build is not an A2A server, so a custom descriptor says what it is and where its app
    is) and, when it has one, its Gateway (an MCP server record with its tools)."""
    dep = item.get("deployed") or {}
    dash = str(item.get("agentName") or item.get("id") or "build").replace("_", "-")
    name = str(item.get("name") or dash)
    agents = workflow.get("agents") or {}
    about = str(((workflow.get("ui") or {}).get("subtitle")) or f"An AgentExpress workflow: {name}.")
    workflow_doc = {
        "kind": "agentexpress-workflow", "name": name, "description": about, "version": str(dep.get("version") or ""),
        "app": dep.get("uiUrl") or "", "api": dep.get("apiUrl") or "",
        "agents": [{"id": aid, "name": (a or {}).get("name", aid), "runtime": (a or {}).get("runtime"),
                    "tools": (a or {}).get("tool") or [], "skills": (a or {}).get("skills") or []}
                   for aid, a in agents.items() if isinstance(a, dict)],
        "tools": [{"key": k, "type": (t or {}).get("type"), "description": (t or {}).get("description", "")}
                  for k, t in (workflow.get("tools") or {}).items() if isinstance(t, dict)],
        "skills": sorted((workflow.get("skills") or {}).keys())}
    out = {"workflow": {"recordType": "AGENT", "name": f"{dash}-workflow", "displayName": name,
                        "description": about[:1000], "descriptors": {"custom": {"data": json.dumps(workflow_doc)}}}}
    gw = dep.get("gatewayUrl")
    tools = gateway_tools(workflow)
    if isinstance(gw, str) and gw.startswith("https://") and tools:
        server = {"name": f"agentexpress/{dash}", "description": f"The tools of {name}, through its AgentCore Gateway.",
                  "version": str(dep.get("version") or "1"), "remotes": [{"type": "streamable-http", "url": gw}]}
        out["gateway"] = {"recordType": "MCP", "name": f"{dash}-tools", "displayName": f"{name} tools",
                          "description": f"The tools of {name}, through its AgentCore Gateway."[:1000],
                          "descriptors": {"mcpServer": {
                              "data": json.dumps(server), "dataSchemaVersion": "2025-12-11",
                              "additionalData": {"tools": {"data": json.dumps({"tools": tools}),
                                                           "dataSchemaVersion": "2025-11-25"}}}}}
    return out


def skill_record(name: str, skill: dict) -> dict:
    return {"recordType": "SKILL", "name": _kebab(name), "displayName": name,
            "description": str(skill.get("description") or "")[:1000],
            "descriptors": {"agentSkillsDefinition": {"data": "{}", "dataSchemaVersion": "0.1.0",
                                                      "additionalData": {"skillMd": {"data": skill_md(name, skill)}}}}}


def _wrapped(v):
    """A descriptor as UpdateRegistryRecord takes it: every field under {"optionalValue"}."""
    if isinstance(v, dict):
        return {"optionalValue": {k: _wrapped(x) for k, x in v.items()}}
    return {"optionalValue": v}


def _wait_draft(registry_id: str, record_id: str) -> str:
    import time
    status = ""
    for _ in range(CREATE_WAIT_S // 3):
        status = _ctl().get_registry_record(registryId=registry_id, recordId=record_id).get("status", "")
        if status not in ("CREATING", "UPDATING"):
            break
        time.sleep(3)
    if status in ("CREATE_FAILED", "UPDATE_FAILED"):
        got = _ctl().get_registry_record(registryId=registry_id, recordId=record_id)
        raise _error()(400, f"the registry refused the record: {str(got.get('statusReason') or status)[:300]}")
    return status


def put_record(registry_id: str, spec: dict, version: str, prior: dict | None) -> dict:
    """Create the record (or update the one published before) at `version`, and submit
    it for approval. {recordId, name, version, status}."""
    rid = _registry_id(registry_id)
    record_id = (prior or {}).get("recordId") if (prior or {}).get("registryId", rid) == rid else None
    if record_id:
        try:
            _ctl().update_registry_record(
                registryId=rid, recordId=record_id, recordVersion=version,
                description={"optionalValue": spec["description"]},
                descriptors={"optionalValue": {k: _wrapped(v) for k, v in spec["descriptors"].items()}})
        except Exception as e:  # deleted in the registry: publish it afresh
            if "NotFound" not in type(e).__name__ and "ResourceNotFound" not in str(e):
                raise
            record_id = None
    if not record_id:
        made = _ctl().create_registry_record(
            registryId=rid, name=spec["name"], displayName=spec["displayName"], description=spec["description"],
            recordType=spec["recordType"], descriptors=spec["descriptors"], recordVersion=version,
            tags={"agentexpress:published": "true"})
        record_id = made.get("recordId") or made["recordArn"].split("/")[-1]
    status = _wait_draft(rid, record_id)
    if status == "DRAFT":
        _ctl().submit_registry_record_for_approval(registryId=rid, recordId=record_id)
        status = _ctl().get_registry_record(registryId=rid, recordId=record_id).get("status", "PENDING_APPROVAL")
    return {"registryId": rid, "recordId": record_id, "name": spec["name"], "version": version, "status": status}


def statuses(published: dict) -> dict:
    """The published record(s) with their status now (approved, rejected and why...)."""
    out = json.loads(json.dumps(published or {}, default=str))
    groups = [out.get("records") or {}, out.get("skills") or {}]
    for group in groups:
        for rec in group.values():
            if not isinstance(rec, dict) or not rec.get("recordId"):
                continue
            try:
                got = _ctl().get_registry_record(registryId=rec.get("registryId") or out.get("registryId"),
                                                 recordId=rec["recordId"])
                rec["status"], rec["statusReason"] = got.get("status", ""), got.get("statusReason", "")
            except Exception as e:  # noqa: BLE001 - deleted in the registry
                rec["status"], rec["statusReason"] = "MISSING", f"{type(e).__name__}"
    return out


def deprecate(published: dict, reason: str) -> list[str]:
    """Take a destroyed build's records out of discovery (DEPRECATED). Best effort."""
    done = []
    for key, rec in ((published or {}).get("records") or {}).items():
        try:
            _ctl().update_registry_record_status(registryId=rec.get("registryId") or published.get("registryId"),
                                                 recordId=rec["recordId"], status="DEPRECATED", statusReason=reason)
            done.append(key)
        except Exception as e:  # noqa: BLE001 - never blocks a destroy
            print(f"[registry] could not deprecate {key}: {type(e).__name__}")
    return done
