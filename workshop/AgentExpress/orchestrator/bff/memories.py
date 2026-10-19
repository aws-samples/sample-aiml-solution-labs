"""The memory inspector: browse, search and delete long-term memory records.

Agents with `agentcore.memory.longTerm` (or a named memory, `agentcore.memory.use`)
store each run's insight as an AgentCore Memory event, and the memory's strategies
extract long-term RECORDS from it into namespaces (app/common/context.py
`_namespace_for`). Until now nobody could see what had been remembered, or remove a
wrong or sensitive record. These routes do that.

PRIVACY MODEL: ADMINS ONLY
--------------------------
Every route here requires the `admin` permission (bff/authz.py, closed unless
workflow.json grants it), checked by the handler BEFORE this module touches AWS. Every
read and every delete is an audit event naming the agent and the namespace.

Why not let a person see their own records? A record is keyed by its ACTOR, which
`AgentContext._memory_actor` builds from the agent and, depending on
`agentcore.memory.scope`, a digest of the user, the run's subject, or the run:

    user    -> <agent>-u<sha256(user)[:10]>[-<subject>]
    subject -> <agent>-<subject>          (whoever ran it)
    agent   -> <agent>[-<subject>]        (everyone)
    run     -> <agent>-u<digest>-r<session>

None of those proves that a namespace holds one person's data:
  * `subject` and `agent` scopes are shared by every user who runs that agent;
  * the subject is free text from whoever starts a run, slugged, so a subject of
    "u0123456789" under scope `agent` produces exactly the actor that scope `user` gives
    the person whose digest is 0123456789. A namespace that LOOKS like mine can hold
    another user's facts;
  * with no sign-in (idp "none") there is no user digest at all.
So per-user access would leak, and the safe default holds: admins only.

WHAT IS SHOWN, AND WHAT NEVER IS
--------------------------------
Only the deployment's LONG-TERM memories: the semantic memory (SEMANTIC_MEMORY_ID) and
the named ones (MEMORIES). The LangGraph checkpointer is a separate AgentCore Memory
(MEMORY_ID, `awscc_bedrockagentcore_memory.orchestrator` / the CDK `Memory`) whose id
this module never reads and whose ARN the BFF role is never granted, so checkpoint data
cannot be listed or deleted from here by construction. Short-term events are not
records and are not listed either (ListMemoryRecords returns records only).

A namespace is accepted only if one of the deployment's strategies can produce it for
the chosen agent (the templates in terraform/evaluations.tf, mirrored in
memoryStrategiesFor in cdk/lib/orchestrator-stack.ts). Nothing else is passed to AWS,
and every record coming back is re-checked against the namespace asked for.

HOW NAMESPACES ARE MATCHED (the AgentCore developer guide, Oct 2026)
---------------------------------------------------------------------
AgentCore stores a namespace with one slash at each end ("/insights/<actor>/"), and reads
take it in that form. ListMemoryRecords and RetrieveMemoryRecords take ONE of:
  * `namespace`, an EXACT match: one actor-scoped namespace ("/insights/<actor>/");
  * `namespacePath`, every namespace under a path: a session-scoped template's records
    ("/summary/<actor>/" finds "/summary/<actor>/<session>/").
Prefix matching on `namespace` continues only for a grace period, so nothing here relies
on it. An agent's namespaces are found from its actors (ListActors), never by a name prefix.
https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/long-term-retrieve-records.html
https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/specify-long-term-memory-organization.html

The routes' own `namespace` values stay without the outer slashes ("insights/<actor>",
"summary/<actor>/" for a session-scoped one), as the page sends them.

Operations: ListActors, ListMemoryRecords, RetrieveMemoryRecords, GetMemoryRecord,
DeleteMemoryRecord. `namespacePath` is newer than the Lambda runtime's boto3 may be, so the
client is built from the bedrock-agentcore model shipped in bff/botocore_data (as the
registry's are). `namespace` on Get/Delete is not sent.
"""

from __future__ import annotations

import json
import os
import re

import boto3
import workflow
from botocore.exceptions import ClientError

REGION = os.environ.get("AWS_REGION", "us-east-1")

ROUTES = frozenset({
    "GET /api/memory/agents",
    "GET /api/memory/namespaces",
    "GET /api/memory/records",
    "POST /api/memory/search",
    "DELETE /api/memory/records/{id}",
})

#: Records per page (ListMemoryRecords allows 100; a page of record texts is enough).
PAGE_MAX = 50
#: Results of one search.
SEARCH_MAX = 20
#: Longest search query. AgentCore takes 10,000 characters; a search box needs far fewer.
QUERY_MAX = 1000
#: Pages of 100 actors read to find an agent's actors.
DISCOVER_PAGES = 3
#: Actors of one agent whose namespaces are listed in one request (each is one call per strategy).
ACTORS_MAX = 50
#: A paging token is opaque, but not unbounded.
TOKEN_MAX = 4096

AGENT_ID_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,127}$")
# AgentCore's MemoryRecordId: mem- then 36 to 46 of [a-zA-Z0-9-_].
RECORD_ID_RE = re.compile(r"^mem-[a-zA-Z0-9_-]{36,46}$")
# A run's session id in a session-scoped namespace (summary/…/{sessionId}).
SESSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,99}$")
# What _memory_actor appends to the agent id: slugs, the user digest, the run.
_ACTOR_TAIL = r"(?:-[a-z0-9]+)*"


class MemoryRefused(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


def _ids() -> tuple[str, dict]:
    """The long-term memories this BFF may inspect: the semantic one and the named ones.
    Read at call time so tests can set them. Never MEMORY_ID (the checkpointer)."""
    try:
        named = json.loads(os.environ.get("MEMORIES") or "{}")
    except ValueError:
        named = {}
    return os.environ.get("SEMANTIC_MEMORY_ID", ""), named if isinstance(named, dict) else {}


_client = None

#: The bedrock-agentcore API model shipped with the BFF (bff/botocore_data), searched FIRST:
#: the Lambda runtime's boto3 can be older than `namespacePath`.
MODELS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "botocore_data")


def _agentcore():
    global _client
    if _client is None:
        import botocore.session
        core = botocore.session.get_session()
        loader = core.get_component("data_loader")
        if MODELS not in loader.search_paths:
            loader.search_paths.insert(0, MODELS)
        _client = boto3.session.Session(botocore_session=core).client("bedrock-agentcore", region_name=REGION)
    return _client


def _scope(namespace: str) -> dict:
    """The AgentCore argument for one of this module's namespaces, in AgentCore's stored
    form: a session-scoped template (ending "/") is a `namespacePath`, anything else an
    exact `namespace`."""
    full = "/" + _norm(namespace) + "/"
    return {"namespacePath": full} if namespace.endswith("/") else {"namespace": full}


# --- What the deployment's strategies can produce ---------------------------------

def _agents(raw: dict) -> dict:
    agents = raw.get("agents") or {}
    return agents if isinstance(agents, dict) else {}


def _mem(agent: dict) -> dict:
    m = ((agent.get("agentcore") or {}) if isinstance(agent, dict) else {}).get("memory") or {}
    return m if isinstance(m, dict) else {}


def _long_term(m: dict) -> list[str]:
    lt = m.get("longTerm")
    return [str(s) for s in lt] if isinstance(lt, list) else (["semantic"] if lt is True else [])


def _templates(raw: dict, memory: str) -> list[tuple[str, str]]:
    """(kind, template) for every namespace a memory's strategies write. `memory` is ""
    for the semantic memory or a `memories` name. Mirrors local.memory_strategies and
    local.named_memory_strategies in terraform/evaluations.tf."""
    if memory:
        want = list(((raw.get("memories") or {}).get(memory) or {}).get("strategies") or [])
        customs: list[str] = []
    else:
        named = {s for a in _agents(raw).values() for s in _long_term(_mem(a))}
        want = ["semantic", "summary"] + [s for s in ("userPreference", "episodic") if s in named]
        customs = [aid for aid, a in _agents(raw).items()
                   if isinstance(_mem(a).get("custom"), dict) and _mem(a)["custom"].get("base")]
    out: list[tuple[str, str]] = []
    if "semantic" in want:
        out.append(("semantic", "insights/{actor}"))
    if "summary" in want:
        out.append(("summary", "summary/{actor}/"))
    if "userPreference" in want:
        out.append(("userPreference", "preferences/{actor}"))
    if "episodic" in want:
        out += [("episodic", "episodes/{actor}/"), ("episodic reflection", "episodes/{actor}")]
    # A custom strategy extracts from EVERY event in the memory, not only its own agent's,
    # so another agent's facts can sit under custom-<that agent>/<this actor>.
    out += [(f"custom ({c})", f"custom-{c}/{{actor}}") for c in customs]
    return out


def agents(raw: dict | None = None) -> list[dict]:
    """The agents that keep long-term memory, with the memory and strategies each writes."""
    raw = workflow.RAW if raw is None else raw
    out = []
    for aid, a in _agents(raw).items():
        m = _mem(a)
        use = str(m.get("use") or "")
        if not (use or _long_term(m) or m.get("custom")):
            continue
        out.append({"id": aid, "name": str((a or {}).get("name") or aid), "memory": use or "default",
                    "kinds": [k for k, _ in _templates(raw, use)]})
    return out


def _agent_memory(raw: dict, agent_id: str) -> tuple[str, str]:
    """(memory id, `memories` name or "") for an agent that keeps long-term memory."""
    if not AGENT_ID_RE.match(agent_id or ""):
        raise MemoryRefused(400, "agent must be an agent id")
    a = _agents(raw).get(agent_id)
    m = _mem(a) if isinstance(a, dict) else {}
    use = str(m.get("use") or "")
    if not isinstance(a, dict) or not (use or _long_term(m) or m.get("custom")):
        raise MemoryRefused(404, f"agent {agent_id!r} keeps no long-term memory here")
    semantic, named = _ids()
    mid = str(named.get(use) or "") if use else semantic
    if not mid:
        raise MemoryRefused(404, "this deployment has no long-term memory to inspect")
    return mid, use


def _actor_re(agent_id: str) -> str:
    return re.escape(agent_id) + _ACTOR_TAIL


def parse(raw: dict, memory: str, agent_id: str, namespace: str) -> str:
    """The kind of `namespace` when the memory's strategies can produce it for this
    agent; else MemoryRefused(400). The only gate between a request and AWS."""
    if not isinstance(namespace, str) or not 1 <= len(namespace) <= 1024:
        raise MemoryRefused(400, "namespace is required")
    actor = _actor_re(agent_id)
    for kind, tpl in _templates(raw, memory):
        head, tail = tpl.split("{actor}")
        if re.fullmatch(re.escape(head) + actor + re.escape(tail), namespace):
            return kind
    raise MemoryRefused(400, "that namespace is not one this agent's memory strategies write")


def _norm(ns: str) -> str:
    """A namespace without its outer slashes. Seen live: AgentCore returns a record's
    namespace as "/insights/<actor>/" for the template "insights/{actorId}", so a literal
    comparison matched no real record and the inspector showed none."""
    return ns.strip("/")


def _holds(namespace: str, record_ns: str) -> bool:
    """Does a record's namespace fall under the one asked for? Exact (outer slashes aside),
    except a session-scoped template (ending in "/") whose records are one level below it."""
    want, got = _norm(namespace), _norm(record_ns)
    if namespace.endswith("/"):
        return got.startswith(want + "/") and bool(SESSION_RE.fullmatch(got[len(want) + 1:]))
    return got == want


def _text(record: dict) -> str:
    content = record.get("content")
    if isinstance(content, dict):
        return str(content.get("text") or "")
    return content if isinstance(content, str) else ""


def _public(record: dict, namespace: str) -> dict:
    created = record.get("createdAt")
    out = {"id": str(record.get("memoryRecordId") or ""), "text": _text(record),
           "createdAt": created.isoformat() if hasattr(created, "isoformat") else str(created or ""),
           "strategyId": str(record.get("memoryStrategyId") or ""),
           "namespace": _norm(next((n for n in record.get("namespaces") or [] if _holds(namespace, n)), namespace))}
    if record.get("score") is not None:
        out["score"] = float(record["score"])
    return out


def _token(value) -> str | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str) or len(value) > TOKEN_MAX:
        raise MemoryRefused(400, "next is not a paging token")
    return value


def _aws(call, **kw) -> dict:
    """One AgentCore call, its refusals answered as statuses. The error CODE only: a
    message can quote the request."""
    try:
        return call(**kw)
    except ClientError as e:
        code = str(e.response.get("Error", {}).get("Code") or "")
        if code == "ResourceNotFoundException":
            raise MemoryRefused(404, "no such memory record") from None
        if code in ("ValidationException", "InvalidInputException"):
            raise MemoryRefused(400, f"AgentCore Memory refused the request ({code})") from None
        print(f"[memories] {kw.get('memoryId', '')}: {code}")
        raise MemoryRefused(502, f"AgentCore Memory failed ({code or 'error'})") from None


# --- The routes ----------------------------------------------------------------------

def namespaces(raw: dict, agent_id: str) -> dict:
    """The namespaces holding records for one agent, found from the records themselves
    (an actor whose events expired still has its records), with a count each."""
    mid, use = _agent_memory(raw, agent_id)
    actor = _actor_re(agent_id)
    # Templates by the prefix they are listed under: episodic's two share one, read once.
    by_head: dict[str, list[tuple[str, str, re.Pattern]]] = {}
    for kind, tpl in _templates(raw, use):
        head, tail = tpl.split("{actor}")
        by_head.setdefault(head, []).append((kind, tail, re.compile(
            re.escape(head) + "(" + actor + ")" + re.escape(tail)
            + (r"[A-Za-z0-9][A-Za-z0-9_-]{0,99}" if tail else ""))))
    # The agent's actors first: a namespace is found from its full actor, never by a name
    # prefix ("insights/<agent>" is not "insights/<agent>-u…"). ListActors names them (the
    # actors whose events the memory holds).
    actor_re = re.compile(actor)
    actors: list[str] = []
    truncated = False
    token = None
    for _ in range(DISCOVER_PAGES):
        page = _aws(_agentcore().list_actors, memoryId=mid, maxResults=100,
                    **({"nextToken": token} if token else {}))
        actors += [str(a.get("actorId") or "") for a in page.get("actorSummaries") or []
                   if actor_re.fullmatch(str(a.get("actorId") or ""))]
        token = page.get("nextToken")
        if not token:
            break
    truncated = bool(token)
    found: dict[str, dict] = {}
    for a in actors[:ACTORS_MAX]:
        for head, shapes in by_head.items():
            # Everything under /<head><actor>/: the actor's own namespace and, for a
            # session-scoped strategy, every session's below it.
            page = _aws(_agentcore().list_memory_records, memoryId=mid,
                        namespacePath="/" + _norm(head + a) + "/", maxResults=100)
            truncated = truncated or bool(page.get("nextToken"))
            for r in page.get("memoryRecordSummaries") or []:
                for ns in r.get("namespaces") or []:
                    for kind, tail, pattern in shapes:
                        m = pattern.fullmatch(_norm(ns))     # "/insights/<actor>/" as returned
                        if not m:
                            continue
                        key = head + m.group(1) + tail
                        row = found.setdefault(key, {"namespace": key, "actor": m.group(1),
                                                     "kind": kind, "records": 0})
                        row["records"] += 1
    truncated = truncated or len(actors) > ACTORS_MAX
    return {"agent": agent_id, "memory": use or "default",
            "namespaces": sorted(found.values(), key=lambda r: (r["actor"], r["namespace"])),
            "truncated": truncated}


def records(raw: dict, agent_id: str, namespace: str, next_token=None) -> dict:
    mid, use = _agent_memory(raw, agent_id)
    kind = parse(raw, use, agent_id, namespace)
    token = _token(next_token)
    page = _aws(_agentcore().list_memory_records, memoryId=mid, **_scope(namespace),
                maxResults=PAGE_MAX, **({"nextToken": token} if token else {}))
    rows = [_public(r, namespace) for r in page.get("memoryRecordSummaries") or []
            if any(_holds(namespace, n) for n in r.get("namespaces") or [])]
    return {"agent": agent_id, "namespace": namespace, "kind": kind, "records": rows[:PAGE_MAX],
            "next": page.get("nextToken") or None}


def search(raw: dict, agent_id: str, namespace: str, query) -> dict:
    mid, use = _agent_memory(raw, agent_id)
    kind = parse(raw, use, agent_id, namespace)
    q = query.strip() if isinstance(query, str) else ""
    if not q:
        raise MemoryRefused(400, "query is required")
    if len(q) > QUERY_MAX:
        raise MemoryRefused(400, f"a search is at most {QUERY_MAX} characters")
    page = _aws(_agentcore().retrieve_memory_records, memoryId=mid, **_scope(namespace),
                searchCriteria={"searchQuery": q, "topK": SEARCH_MAX}, maxResults=SEARCH_MAX)
    rows = [_public(r, namespace) for r in page.get("memoryRecordSummaries") or []
            if any(_holds(namespace, n) for n in r.get("namespaces") or [])]
    return {"agent": agent_id, "namespace": namespace, "kind": kind, "records": rows[:SEARCH_MAX]}


def delete(raw: dict, agent_id: str, namespace: str, record_id: str) -> dict:
    """Delete one record, after checking it is in the namespace named: the record id
    alone would reach any record in the memory."""
    mid, use = _agent_memory(raw, agent_id)
    kind = parse(raw, use, agent_id, namespace)
    if not RECORD_ID_RE.match(record_id or ""):
        raise MemoryRefused(400, "not a memory record id")
    record = _aws(_agentcore().get_memory_record, memoryId=mid, memoryRecordId=record_id).get("memoryRecord") or {}
    if not any(_holds(namespace, n) for n in record.get("namespaces") or []):
        raise MemoryRefused(404, "no such memory record")
    _aws(_agentcore().delete_memory_record, memoryId=mid, memoryRecordId=record_id)
    return {"deleted": record_id, "agent": agent_id, "namespace": namespace, "kind": kind}


def route(route_key: str, params: dict, qs: dict, body: dict, record) -> tuple[int, dict]:
    """Answer one memory route. The caller has ALREADY checked `admin`. `record(action,
    **detail)` writes the audit event; it is called before a read reaches AWS (an
    attempted look is still a look) and after a delete succeeds."""
    raw = workflow.RAW
    try:
        if route_key == "GET /api/memory/agents":
            semantic, named = _ids()
            return 200, {"agents": agents(raw), "deployed": bool(semantic or named)}
        if route_key == "GET /api/memory/namespaces":
            agent_id = str(qs.get("agent") or "")
            _agent_memory(raw, agent_id)
            record("memory.viewed", agent=agent_id, what="namespaces")
            return 200, namespaces(raw, agent_id)
        if route_key == "GET /api/memory/records":
            agent_id, ns = str(qs.get("agent") or ""), str(qs.get("namespace") or "")
            _, use = _agent_memory(raw, agent_id)
            parse(raw, use, agent_id, ns)
            record("memory.viewed", agent=agent_id, namespace=ns, what="records")
            return 200, records(raw, agent_id, ns, qs.get("next"))
        if route_key == "POST /api/memory/search":
            agent_id, ns = str(body.get("agent") or ""), str(body.get("namespace") or "")
            _, use = _agent_memory(raw, agent_id)
            parse(raw, use, agent_id, ns)
            # Not the query itself: it can name the very thing being looked for.
            record("memory.viewed", agent=agent_id, namespace=ns, what="search")
            return 200, search(raw, agent_id, ns, body.get("query"))
        if route_key == "DELETE /api/memory/records/{id}":
            agent_id, ns = str(qs.get("agent") or ""), str(qs.get("namespace") or "")
            out = delete(raw, agent_id, ns, str(params.get("id") or ""))
            record("memory.deleted", agent=agent_id, namespace=ns, record=out["deleted"])
            print(f"[memories] deleted {out['deleted']} from {ns}")
            return 200, out
    except MemoryRefused as e:
        return e.status, {"error": str(e)}
    return 404, {"error": f"no route for {route_key}"}
