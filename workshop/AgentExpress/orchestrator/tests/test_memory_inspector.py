"""The memory inspector (bff/memories.py): admins browse, search and delete long-term
memory records.

What has to hold:
  * only an admin gets an answer, and anyone else is refused BEFORE any AgentCore call;
  * a namespace is accepted only when the deployment's strategies can write it for that
    agent, and AgentCore's PREFIX matching never leaks a neighbour's records;
  * paging and search are capped, and the paging token round-trips;
  * every read and every delete is in the activity log, naming the namespace;
  * delete reaches only a record in the namespace named, and a missing one is a 404;
  * the checkpointer's memory is never read, listed or deleted;
  * the routes are on both IaC route lists, and the IAM is scoped to the long-term
    memory ARNs on both paths.

DynamoDB (the activity log) is moto's. AgentCore Memory is a fake that behaves like the
service where it matters: `namespace` is a prefix, paging tokens are opaque and checked,
an unknown record is ResourceNotFoundException, and every call's parameters are
validated against the installed botocore model AND the first bedrock-agentcore model
(the Lambda runtime's boto3 may be that old).
"""
from __future__ import annotations

import datetime as dt
import importlib
import json
import re
import sys
from pathlib import Path

import boto3
import botocore.session
import botocore.validate
import pytest
from botocore.exceptions import ClientError

moto = pytest.importorskip("moto")

from test_builds import CTX, MODULES, _events_table, _status_table, create_builds_table  # noqa: E402 - after the moto importorskip

ORCH = Path(__file__).resolve().parents[1]
SEMANTIC = "orch_semantic-abcdefghij"
BRAND = "orch_m_brand-abcdefghij"
CHECKPOINT = "orch_memory-checkpoint"

RULES = {"groupsClaim": "cognito:groups", "actions": {"admin": ["admins"], "audit": ["auditors"]}}
WORKFLOW = {
    "agents": {
        "research": {"name": "Research", "agentcore": {"memory": {"longTerm": ["semantic", "summary"]}}},
        "research_v2": {"name": "Research 2", "agentcore": {"memory": {"longTerm": ["semantic"]}}},
        "critic": {"name": "Critic", "agentcore": {"memory": {
            "longTerm": ["semantic"], "custom": {"base": "semantic", "instructions": "Keep critiques."}}}},
        "writer": {"name": "Writer", "agentcore": {"memory": {"use": "brand"}}},
        "plain": {"name": "Plain"},
    },
    "steps": [{"agent": "research"}, {"agent": "critic"}, {"agent": "writer"}, {"agent": "plain"}],
    "memories": {"brand": {"strategies": ["userPreference"]}},
    "authorization": RULES,
}

# Requests are checked against the bedrock-agentcore model the BFF ships.
# A parameter outside these fails on a Lambda runtime whose boto3 is that old.
# The bedrock-agentcore model the BFF ships (bff/botocore_data) and builds its client from:
# a parameter outside it would fail on the Lambda runtime.
def _shipped_model():
    import botocore.loaders
    import botocore.model
    loader = botocore.loaders.Loader(extra_search_paths=[str(ORCH / "bff" / "botocore_data")],
                                     include_default_search_paths=False)
    return botocore.model.ServiceModel(loader.load_service_model("bedrock-agentcore", "service-2"), "bedrock-agentcore")


_MODEL = _shipped_model()
_OPS = {"list_actors": "ListActors", "list_memory_records": "ListMemoryRecords", "retrieve_memory_records": "RetrieveMemoryRecords",
        "get_memory_record": "GetMemoryRecord", "delete_memory_record": "DeleteMemoryRecord"}


def _err(code: str, op: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": f"{code} (fake)"}}, op)


def rid(n: int) -> str:
    return f"mem-{n:036d}"


class FakeMemory:
    """AgentCore Memory's record data plane, in memory."""

    def __init__(self):
        self.memories: dict[str, dict[str, dict]] = {SEMANTIC: {}, BRAND: {}, CHECKPOINT: {}}
        self.calls: list[tuple[str, dict]] = []
        self._n = 0

    def add(self, memory: str, namespace: str, text: str, strategy: str = "insights-abc") -> str:
        self._n += 1
        # As AgentCore returns it (seen live): "/insights/<actor>/" for "insights/{actorId}".
        r = {"memoryRecordId": rid(self._n), "content": {"text": text}, "memoryStrategyId": strategy,
             "namespaces": [f"/{namespace.strip('/')}/"],
             "createdAt": dt.datetime(2026, 1, 1, tzinfo=dt.UTC) + dt.timedelta(minutes=self._n)}
        self.memories[memory][r["memoryRecordId"]] = r
        return r["memoryRecordId"]

    def _check(self, op: str, kw: dict):
        self.calls.append((op, kw))
        botocore.validate.validate_parameters(kw, _MODEL.operation_model(_OPS[op]).input_shape)
        if op in ("list_memory_records", "retrieve_memory_records"):
            # "Either namespace or namespacePath is required"; and in AgentCore's stored form.
            given = [k for k in ("namespace", "namespacePath") if k in kw]
            assert len(given) == 1, f"{op}: send exactly one of namespace / namespacePath, got {given}"
            v = kw[given[0]]
            assert v.startswith("/") and v.endswith("/"), f"{op}: {v!r} is not in the /…/ form"
        if kw["memoryId"] not in self.memories:
            raise _err("ResourceNotFoundException", _OPS[op])
        return self.memories[kw["memoryId"]]

    def _under(self, store: dict, kw: dict) -> list[dict]:
        # As the developer guide defines it: `namespace` is an EXACT match, `namespacePath`
        # matches that path and everything under it, on whole segments.
        if "namespace" in kw:
            want = kw["namespace"]
            hit = lambda n: n == want  # noqa: E731 - a one-line test helper
        else:
            path = kw["namespacePath"]
            hit = lambda n: n.startswith(path)  # noqa: E731 - a one-line test helper
        rows = [r for r in store.values() if any(hit(n) for n in r["namespaces"])]
        return sorted(rows, key=lambda r: r["createdAt"], reverse=True)

    def _page(self, rows: list[dict], kw: dict, op: str) -> dict:
        start = 0
        if kw.get("nextToken"):
            m = re.fullmatch(r"tok-(\d+)", kw["nextToken"])
            if not m:
                raise _err("ValidationException", op)
            start = int(m.group(1))
        size = kw.get("maxResults", 20)
        out = {"memoryRecordSummaries": rows[start:start + size]}
        if start + size < len(rows):
            out["nextToken"] = f"tok-{start + size}"
        return out

    def list_actors(self, **kw):
        """The actors with events: here, the second segment of every record's namespace."""
        store = self._check("list_actors", kw)
        actors = sorted({n.strip("/").split("/")[1] for r in store.values() for n in r["namespaces"]
                         if n.strip("/").count("/") >= 1})
        return self._page([{"actorId": a} for a in actors], kw, "ListActors") | {
            "actorSummaries": [{"actorId": a} for a in actors][:kw.get("maxResults", 20)]}

    def list_memory_records(self, **kw):
        store = self._check("list_memory_records", kw)
        return self._page(self._under(store, kw), kw, "ListMemoryRecords")

    def retrieve_memory_records(self, **kw):
        store = self._check("retrieve_memory_records", kw)
        words = set(kw["searchCriteria"]["searchQuery"].lower().split())
        rows = [{**r, "score": len(words & set(r["content"]["text"].lower().split())) / max(len(words), 1)}
                for r in self._under(store, kw)]
        rows = sorted([r for r in rows if r["score"] > 0], key=lambda r: -r["score"])
        return {"memoryRecordSummaries": rows[:kw["searchCriteria"].get("topK", 20)]}

    def get_memory_record(self, **kw):
        store = self._check("get_memory_record", kw)
        if kw["memoryRecordId"] not in store:
            raise _err("ResourceNotFoundException", "GetMemoryRecord")
        return {"memoryRecord": {k: v for k, v in store[kw["memoryRecordId"]].items() if k != "score"}}

    def delete_memory_record(self, **kw):
        store = self._check("delete_memory_record", kw)
        if kw["memoryRecordId"] not in store:
            raise _err("ResourceNotFoundException", "DeleteMemoryRecord")
        del store[kw["memoryRecordId"]]
        return {"memoryRecordId": kw["memoryRecordId"]}


@pytest.fixture()
def env(monkeypatch):
    for k, v in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing",
                 "AWS_DEFAULT_REGION": "us-east-1", "AWS_REGION": "us-east-1",
                 "WORKFLOW_JSON": json.dumps(WORKFLOW),
                 "STATUS_TABLE": "console_status", "EVENTS_TABLE": "console_events",
                 "RUNTIME_ARN": "arn:aws:bedrock-agentcore:us-east-1:1:runtime/console",
                 # A build's own app, where the inspector lives: its own activity log.
                 "AUDIT_TABLE": "app_audit",
                 "SEMANTIC_MEMORY_ID": SEMANTIC, "MEMORIES": json.dumps({"brand": BRAND}),
                 # Set as the runtime has it, to show the inspector never reads it.
                 "MEMORY_ID": CHECKPOINT}.items():
        monkeypatch.setenv(k, v)
    with moto.mock_aws():
        ddb = boto3.client("dynamodb")
        create_builds_table(ddb, "app_audit")
        _status_table(ddb, "console_status")
        _events_table(ddb, "console_events")
        for mod in (*MODULES, "memories"):
            sys.modules.pop(mod, None)
        handler = importlib.import_module("handler")
        fake = FakeMemory()
        # The module the handler holds, not one imported here (other tests pop modules).
        monkeypatch.setattr(handler.memories, "_client", fake)
        yield type("Env", (), {"handler": handler, "mem": fake, "memories": handler.memories})
    for mod in (*MODULES, "memories"):
        sys.modules.pop(mod, None)


def call(e, route_key, *, params=None, qs=None, body=None, sub="admin1", groups=("admins",)):
    method, template = route_key.split(" ", 1)
    claims = {"sub": sub, "email": f"{sub}@example.com", "cognito:groups": list(groups)}
    event = {"requestContext": {"http": {"method": method, "sourceIp": "10.0.0.1"},
                                "authorizer": {"jwt": {"claims": claims}}},
             "rawPath": template.replace("{id}", (params or {}).get("id", "")),
             "routeKey": route_key, "pathParameters": params or {}}
    if body is not None:
        event["body"] = json.dumps(body)
    if qs:
        event["queryStringParameters"] = qs
    resp = e.handler.handler(event, CTX)
    return resp["statusCode"], json.loads(resp["body"])


def records(e, agent, ns, **qs):
    return call(e, "GET /api/memory/records", qs={"agent": agent, "namespace": ns, **qs})


def my_audit(e, sub="admin1"):
    status, events = call(e, "GET /api/audit", sub=sub)
    assert status == 200
    return [ev for ev in events if ev["action"].startswith("memory.")]


ROUTE_CALLS = [
    ("GET /api/memory/agents", {}, {}, None),
    ("GET /api/memory/namespaces", {}, {"agent": "research"}, None),
    ("GET /api/memory/records", {}, {"agent": "research", "namespace": "insights/research"}, None),
    ("POST /api/memory/search", {}, {}, {"agent": "research", "namespace": "insights/research", "query": "x"}),
    ("DELETE /api/memory/records/{id}", {"id": rid(1)}, {"agent": "research", "namespace": "insights/research"}, None),
]


# --- who may ask ------------------------------------------------------------------------

@pytest.mark.parametrize(("route", "params", "qs", "body"), ROUTE_CALLS, ids=[r[0] for r in ROUTE_CALLS])
def test_a_non_admin_is_refused_before_any_aws_call(env, route, params, qs, body):
    env.mem.add(SEMANTIC, "insights/research", "a fact")
    status, resp = call(env, route, params=params, qs=qs, body=body, sub="bob", groups=("operators",))
    assert status == 403 and resp["requiredGroups"] == ["admins"]
    assert env.mem.calls == []
    assert len(env.mem.memories[SEMANTIC]) == 1


def test_every_route_is_one_the_module_answers(env):
    assert {r[0] for r in ROUTE_CALLS} == set(env.memories.ROUTES)


def test_with_no_authorization_block_nobody_is_an_admin(env, monkeypatch):
    # `admin` is closed unless workflow.json grants it (bff/authz.py CLOSED_UNLESS_GRANTED).
    monkeypatch.setattr(env.handler.authz, "_RULES", {})
    monkeypatch.setattr(env.handler.authz, "ENABLED", False)
    assert call(env, "GET /api/memory/namespaces", qs={"agent": "research"})[0] == 403
    assert env.mem.calls == []


# --- which agents, which namespaces --------------------------------------------------------

def test_it_lists_the_agents_that_keep_memory_and_what_their_memory_writes(env):
    status, body = call(env, "GET /api/memory/agents")
    assert status == 200 and body["deployed"] is True
    by_id = {a["id"]: a for a in body["agents"]}
    assert set(by_id) == {"research", "research_v2", "critic", "writer"}       # not `plain`
    # Every strategy on the agent's MEMORY extracts from its events, not only the ones it
    # reads: research reads semantic+summary, and critic's custom strategy sees its events too.
    assert by_id["research"]["kinds"] == ["semantic", "summary", "custom (critic)"]
    assert by_id["writer"] == {"id": "writer", "name": "Writer", "memory": "brand", "kinds": ["userPreference"]}
    assert env.mem.calls == []


def test_namespaces_are_found_from_the_records_and_only_this_agents(env):
    env.mem.add(SEMANTIC, "insights/research-u0123456789", "fact one")
    env.mem.add(SEMANTIC, "insights/research-u0123456789", "fact two")
    env.mem.add(SEMANTIC, "insights/research-acme", "acme fact")
    env.mem.add(SEMANTIC, "summary/research-u0123456789/abc123def456", "a summary")
    env.mem.add(SEMANTIC, "summary/research-u0123456789/fff000fff000", "another summary")
    env.mem.add(SEMANTIC, "custom-critic/research-acme", "critic's take on research")
    # A neighbour whose id starts the same: AgentCore's prefix match returns it.
    env.mem.add(SEMANTIC, "insights/research_v2-u0123456789", "not research's")
    # Under the agent's prefix but no template writes it.
    env.mem.add(SEMANTIC, "insights/research-x/extra", "not a strategy's namespace")
    status, body = call(env, "GET /api/memory/namespaces", qs={"agent": "research"})
    assert status == 200 and body["truncated"] is False
    got = {(n["namespace"], n["kind"], n["records"]) for n in body["namespaces"]}
    assert got == {("insights/research-u0123456789", "semantic", 2),
                   ("insights/research-acme", "semantic", 1),
                   ("summary/research-u0123456789/", "summary", 2),
                   ("custom-critic/research-acme", "custom (critic)", 1)}
    assert {n["actor"] for n in body["namespaces"]} == {"research-u0123456789", "research-acme"}
    assert {c[1]["memoryId"] for c in env.mem.calls} == {SEMANTIC}


def test_a_named_memory_is_read_for_the_agent_that_uses_it(env):
    env.mem.add(BRAND, "preferences/writer-u0123456789", "prefers short sentences")
    env.mem.add(SEMANTIC, "preferences/writer-u0123456789", "in the wrong memory")
    body = call(env, "GET /api/memory/namespaces", qs={"agent": "writer"})[1]
    assert [n["namespace"] for n in body["namespaces"]] == ["preferences/writer-u0123456789"]
    assert {c[1]["memoryId"] for c in env.mem.calls} == {BRAND}
    assert records(env, "writer", "preferences/writer-u0123456789")[1]["records"][0]["text"] == "prefers short sentences"


def test_discovery_reads_a_bounded_number_of_actors_and_says_when_it_stopped(env, monkeypatch):
    monkeypatch.setattr(env.memories, "DISCOVER_PAGES", 2)
    for i in range(250):
        env.mem.add(SEMANTIC, f"insights/research-s{i}", f"fact {i}")
    body = call(env, "GET /api/memory/namespaces", qs={"agent": "research"})[1]
    assert body["truncated"] is True
    assert len(body["namespaces"]) == env.memories.ACTORS_MAX
    assert sum(1 for op, _ in env.mem.calls if op == "list_actors") == 2


def test_a_name_prefix_finds_nothing_so_discovery_goes_by_actor(env):
    """Seen live: listing "insights/<agent>" returned none of the records under
    "insights/<agent>-u…"; only the full actor does."""
    env.mem.add(SEMANTIC, "insights/research-u0123456789", "The user's name is Priya.")
    assert env.mem.list_memory_records(memoryId=SEMANTIC, namespace="/insights/research/")["memoryRecordSummaries"] == []
    body = call(env, "GET /api/memory/namespaces", qs={"agent": "research"})[1]
    assert [n["namespace"] for n in body["namespaces"]] == ["insights/research-u0123456789"]


def test_the_checkpointer_memory_is_never_read(env):
    env.mem.add(CHECKPOINT, "insights/research-u0123456789", "a checkpoint, not a memory")
    for route, params, qs, body in ROUTE_CALLS:
        call(env, route, params=params, qs=qs, body=body)
    assert env.mem.calls and all(kw["memoryId"] != CHECKPOINT for _, kw in env.mem.calls)
    assert len(env.mem.memories[CHECKPOINT]) == 1
    assert not re.search(r"""["']MEMORY_ID["']""", (ORCH / "bff" / "memories.py").read_text())


def test_no_long_term_memory_deployed_is_a_404_not_a_call(env, monkeypatch):
    monkeypatch.delenv("SEMANTIC_MEMORY_ID")
    monkeypatch.delenv("MEMORIES")
    status, body = call(env, "GET /api/memory/agents")
    assert status == 200 and body["deployed"] is False
    assert call(env, "GET /api/memory/namespaces", qs={"agent": "research"})[0] == 404
    assert env.mem.calls == []


# --- the namespace gate -------------------------------------------------------------------

@pytest.mark.parametrize(("agent", "ns"), [
    ("research", "preferences/research-u0123456789"),   # no agent names userPreference here
    ("research", "insights/critic-u0123456789"),        # another agent's actor
    ("research", "insights/research_v2"),               # a neighbour by prefix
    ("research", "insights/research-u0123456789/x"),    # deeper than the template
    ("research", "summary/research-u0123456789"),       # a session template, without its /
    ("research", "insights/*"),
    ("research", "insights/"),
    ("research", "insights/research-U0123"),            # actor tails are lowercase slugs
    ("research", "custom-research/research-u1"),        # research has no custom strategy
    ("writer", "insights/writer-u0123456789"),          # brand has userPreference only
    ("plain", "insights/plain"),                        # keeps no memory: 404
    ("ghost", "insights/ghost"),                        # not an agent: 404
    ("../x", "insights/research"),                      # not an agent id: 400
])
def test_a_namespace_no_strategy_writes_is_refused_before_any_call(env, agent, ns):
    status, _ = records(env, agent, ns)
    assert status in (400, 404)
    assert call(env, "POST /api/memory/search", body={"agent": agent, "namespace": ns, "query": "x"})[0] in (400, 404)
    assert call(env, "DELETE /api/memory/records/{id}", params={"id": rid(1)},
                qs={"agent": agent, "namespace": ns})[0] in (400, 404)
    assert env.mem.calls == []
    assert my_audit(env) == []


def test_each_reader_checks_the_namespace_itself_before_calling_aws(env):
    # The route checks first; each function checks again, so a new caller cannot skip it.
    m, raw = env.memories, json.loads(json.dumps(WORKFLOW))
    for fn, args in ((m.records, ()), (m.search, ("q",)), (m.delete, (rid(1),))):
        with pytest.raises(m.MemoryRefused):
            fn(raw, "research", "insights/research_v2", *args)
    assert env.mem.calls == []


def test_the_namespaces_the_strategies_write_are_accepted(env):
    for agent, ns in [("research", "insights/research"), ("research", "insights/research-u0123456789-acme"),
                      ("research", "summary/research-u0123456789/"), ("research", "custom-critic/research-acme"),
                      ("critic", "custom-critic/critic"), ("writer", "preferences/writer-u0123456789")]:
        assert records(env, agent, ns)[0] == 200, ns


# --- reading -------------------------------------------------------------------------------

def test_records_are_exactly_the_namespace_asked_for(env):
    mine = env.mem.add(SEMANTIC, "insights/research", "shared fact")
    env.mem.add(SEMANTIC, "insights/research-u0123456789", "a user's fact")    # prefix sibling
    status, body = records(env, "research", "insights/research")
    assert status == 200
    assert [r["id"] for r in body["records"]] == [mine]
    r = body["records"][0]
    assert r == {"id": mine, "text": "shared fact", "createdAt": r["createdAt"], "strategyId": "insights-abc",
                 "namespace": "insights/research"}
    assert r["createdAt"].startswith("2026-01-01T00:")


def test_a_session_template_lists_every_session_below_it(env):
    a = env.mem.add(SEMANTIC, "summary/research-acme/abc123def456", "summary a", "summary-abc")
    b = env.mem.add(SEMANTIC, "summary/research-acme/fff000fff000", "summary b", "summary-abc")
    env.mem.add(SEMANTIC, "summary/research-acme-x/abc123def456", "another actor's")
    body = records(env, "research", "summary/research-acme/")[1]
    assert {r["id"] for r in body["records"]} == {a, b}
    assert {r["namespace"] for r in body["records"]} == {"summary/research-acme/abc123def456",
                                                         "summary/research-acme/fff000fff000"}


def test_paging_is_capped_and_the_token_round_trips(env):
    ids = [env.mem.add(SEMANTIC, "insights/research-acme", f"fact {i}") for i in range(70)]
    first = records(env, "research", "insights/research-acme", limit="100")[1]
    assert len(first["records"]) == env.memories.PAGE_MAX == 50 and first["next"]
    assert max(kw["maxResults"] for _, kw in env.mem.calls) == 50
    second = records(env, "research", "insights/research-acme", next=first["next"])[1]
    assert len(second["records"]) == 20 and second["next"] is None
    assert {r["id"] for r in first["records"] + second["records"]} == set(ids)


def test_a_bad_paging_token_is_a_400(env):
    assert records(env, "research", "insights/research", next="nonsense")[0] == 400
    assert records(env, "research", "insights/research", next="t" * 5000)[0] == 400


def test_search_is_scoped_capped_and_needs_a_query(env):
    hit = env.mem.add(SEMANTIC, "insights/research-acme", "acme prefers serverless designs")
    env.mem.add(SEMANTIC, "insights/research-acme-x", "serverless for someone else")
    env.mem.add(SEMANTIC, "insights/research-acme", "unrelated")
    status, body = call(env, "POST /api/memory/search",
                        body={"agent": "research", "namespace": "insights/research-acme", "query": "serverless"})
    assert status == 200 and [r["id"] for r in body["records"]] == [hit]
    assert body["records"][0]["score"] > 0
    _, kw = env.mem.calls[-1]
    assert kw["searchCriteria"]["topK"] <= env.memories.SEARCH_MAX and kw["maxResults"] <= env.memories.SEARCH_MAX
    n = len(env.mem.calls)
    too_long = "x" * (env.memories.QUERY_MAX + 1)
    for q in (too_long, "", "   ", None, 7):
        status, _resp = call(env, "POST /api/memory/search",
                             body={"agent": "research", "namespace": "insights/research-acme", "query": q})
        assert status == 400, q
    assert len(env.mem.calls) == n
    assert call(env, "POST /api/memory/search", body={"agent": "research", "namespace": "insights/research-acme",
                                                      "query": "x" * env.memories.QUERY_MAX})[0] == 200


def test_reads_are_audited_by_namespace_and_a_search_without_its_query(env):
    env.mem.add(SEMANTIC, "insights/research-acme", "fact")
    call(env, "GET /api/memory/namespaces", qs={"agent": "research"})
    records(env, "research", "insights/research-acme")
    call(env, "POST /api/memory/search",
         body={"agent": "research", "namespace": "insights/research-acme", "query": "SECRET-QUERY"})
    events = my_audit(env)
    assert sorted(e["detail"]["what"] for e in events) == ["namespaces", "records", "search"]
    assert all(e["action"] == "memory.viewed" and e["detail"]["agent"] == "research" for e in events)
    assert {e["detail"].get("namespace") for e in events} == {None, "insights/research-acme"}
    assert "SECRET-QUERY" not in json.dumps(events)
    assert all(e["email"] == "admin1@example.com" and e["detail"]["ip"] == "10.0.0.1" for e in events)


# --- deleting ------------------------------------------------------------------------------

def test_delete_removes_the_record_and_is_audited(env):
    doomed = env.mem.add(SEMANTIC, "insights/research-acme", "a wrong fact")
    kept = env.mem.add(SEMANTIC, "insights/research-acme", "a right fact")
    status, body = call(env, "DELETE /api/memory/records/{id}", params={"id": doomed},
                        qs={"agent": "research", "namespace": "insights/research-acme"})
    assert status == 200 and body["deleted"] == doomed
    assert set(env.mem.memories[SEMANTIC]) == {kept}
    [event] = [e for e in my_audit(env) if e["action"] == "memory.deleted"]
    assert event["detail"]["namespace"] == "insights/research-acme"
    assert event["detail"]["record"] == doomed and event["detail"]["agent"] == "research"
    # The text of what was deleted is not copied into the log.
    assert "a wrong fact" not in json.dumps(my_audit(env))


def test_deleting_a_missing_record_is_a_404_and_not_audited(env):
    status, body = call(env, "DELETE /api/memory/records/{id}", params={"id": rid(99)},
                        qs={"agent": "research", "namespace": "insights/research-acme"})
    assert status == 404 and body["error"] == "no such memory record"
    assert [op for op, _ in env.mem.calls] == ["get_memory_record"]
    assert my_audit(env) == []


def test_a_record_outside_the_namespace_named_is_not_deleted(env):
    theirs = env.mem.add(SEMANTIC, "insights/research_v2-acme", "another agent's")
    sibling = env.mem.add(SEMANTIC, "insights/research-acme-x", "another actor's")
    for record in (theirs, sibling):
        status, _ = call(env, "DELETE /api/memory/records/{id}", params={"id": record},
                         qs={"agent": "research", "namespace": "insights/research-acme"})
        assert status == 404
    assert set(env.mem.memories[SEMANTIC]) == {theirs, sibling}
    assert "delete_memory_record" not in [op for op, _ in env.mem.calls]


def test_a_record_deleted_between_the_check_and_the_delete_is_a_404(env, monkeypatch):
    doomed = env.mem.add(SEMANTIC, "insights/research-acme", "racing")
    get = env.mem.get_memory_record

    def get_then_vanish(**kw):
        out = get(**kw)
        env.mem.memories[SEMANTIC].pop(doomed)
        return out
    monkeypatch.setattr(env.mem, "get_memory_record", get_then_vanish)
    status, _ = call(env, "DELETE /api/memory/records/{id}", params={"id": doomed},
                     qs={"agent": "research", "namespace": "insights/research-acme"})
    assert status == 404 and [e for e in my_audit(env) if e["action"] == "memory.deleted"] == []


@pytest.mark.parametrize("bad", ["mem-short", "x" * 40, "mem-" + "a" * 36 + "/../", "mem-" + "a" * 47])
def test_a_malformed_record_id_is_a_400_before_any_call(env, bad):
    status, _ = call(env, "DELETE /api/memory/records/{id}", params={"id": bad},
                     qs={"agent": "research", "namespace": "insights/research-acme"})
    assert status == 400 and env.mem.calls == []


def test_an_aws_failure_names_the_code_and_not_the_message(env, monkeypatch):
    def denied(**kw):
        raise ClientError({"Error": {"Code": "AccessDeniedException", "Message": "secret detail"}}, "ListMemoryRecords")
    monkeypatch.setattr(env.mem, "list_memory_records", denied)
    status, body = records(env, "research", "insights/research")
    assert status == 502 and "AccessDeniedException" in body["error"] and "secret detail" not in body["error"]


# --- the IaC -------------------------------------------------------------------------------

def test_the_routes_are_on_both_route_lists_behind_the_jwt_authorizer():
    tf = (ORCH / "terraform" / "bff.tf").read_text()
    authorized = tf.split('resource "aws_apigatewayv2_route" "routes"')[1].split("\n}\n")[0]
    cdk = (ORCH / "cdk" / "lib" / "orchestrator-stack.ts").read_text()
    routes = cdk.split("const routes: Array<")[1].split("];")[0]
    for route in sorted(importlib.import_module("memories").ROUTES):
        method, path = route.split(" ", 1)
        assert f'"{route}"' in authorized, route
        assert f'path: "{path}", methods: [HttpMethod.{method}]' in routes, route
    # The one unauthenticated route stays the only one.
    assert "/api/memory" not in tf.split('resource "aws_apigatewayv2_route" "hook"')[1].split("\n}\n")[0]


def test_the_iam_is_the_same_on_both_paths_and_only_on_the_long_term_memories():
    tf = (ORCH / "terraform" / "memories.tf").read_text()
    cdk = (ORCH / "cdk" / "lib" / "orchestrator-stack.ts").read_text()

    def tf_actions(sid):
        block = tf.split(f'Sid      = "{sid}"')[1].split("}")[0]
        return sorted(re.findall(r'"(bedrock-agentcore:\w+)"', block))

    def cdk_actions(sid):
        block = cdk.split(f'sid: "{sid}"')[1].split("}));")[0]
        return sorted(re.findall(r'"(bedrock-agentcore:\w+)"', block))
    assert tf_actions("MemoryInspectorRead") == cdk_actions("MemoryInspectorRead") == [
        "bedrock-agentcore:GetMemoryRecord", "bedrock-agentcore:ListActors",
        "bedrock-agentcore:ListMemoryRecords", "bedrock-agentcore:RetrieveMemoryRecords"]
    assert tf_actions("MemoryInspectorDelete") == cdk_actions("MemoryInspectorDelete") == [
        "bedrock-agentcore:DeleteMemoryRecord"]
    # Terraform: the semantic and named memories' ARNs, never the checkpointer's, no wildcard.
    assert "awscc_bedrockagentcore_memory.semantic[0].memory_arn" in tf
    assert "awscc_bedrockagentcore_memory.named" in tf
    assert "awscc_bedrockagentcore_memory.orchestrator" not in tf.split("locals {")[1].split("\n}\n")[0]
    assert '"*"' not in tf and "/*" not in tf.split("locals {")[1]
    assert tf.count("Resource = local.inspected_memory_arns") == 2
    # CDK: the same list, built from the semantic memory and each named one only.
    assert "const inspectedMemoryArns: string[] = [semanticMemoryArn];" in cdk
    assert "inspectedMemoryArns.push(arn);" in cdk
    assert cdk.count("resources: plane.longTerm.arns") == 2
    # Both hand the BFF the same two ids and nothing for the checkpointer.
    assert "SEMANTIC_MEMORY_ID = awscc_bedrockagentcore_memory.semantic[0].memory_id" in tf
    assert "MEMORIES           = local.memories_env" in tf
    assert 'bff.addEnvironment("SEMANTIC_MEMORY_ID", plane.longTerm.semanticId)' in cdk
    assert 'bff.addEnvironment("MEMORIES", this.toJsonString(plane.longTerm.named))' in cdk
    assert "local.memory_bff_env" in (ORCH / "terraform" / "bff.tf").read_text()


def test_reads_use_exact_namespace_or_namespace_path_never_a_prefix(env):
    """The developer guide: `namespace` is exact, `namespacePath` the hierarchy, both in the
    stored /…/ form. Prefix matching on `namespace` lasts only a grace period."""
    env.mem.add(SEMANTIC, "insights/research-u0123456789", "fact")
    env.mem.add(SEMANTIC, "summary/research-u0123456789/s1", "summary one")
    env.mem.add(SEMANTIC, "summary/research-u0123456789/s2", "summary two")
    st, body = call(env, "GET /api/memory/namespaces", qs={"agent": "research"})
    assert st == 200
    assert {(n["namespace"], n["records"]) for n in body["namespaces"]} == {
        ("insights/research-u0123456789", 1), ("summary/research-u0123456789/", 2)}
    env.mem.calls.clear()
    st, body = call(env, "GET /api/memory/records", qs={"agent": "research", "namespace": "summary/research-u0123456789/"})
    assert st == 200 and len(body["records"]) == 2
    assert env.mem.calls[-1][1]["namespacePath"] == "/summary/research-u0123456789/"
    st, body = call(env, "GET /api/memory/records", qs={"agent": "research", "namespace": "insights/research-u0123456789"})
    assert st == 200 and [r["text"] for r in body["records"]] == ["fact"]
    assert env.mem.calls[-1][1]["namespace"] == "/insights/research-u0123456789/"


def test_the_client_is_built_from_the_shipped_model(monkeypatch):
    """namespacePath is newer than the Lambda runtime's boto3 may be."""
    sys.path.insert(0, str(ORCH / "bff"))
    import memories
    monkeypatch.setattr(memories, "_client", None)
    c = memories._agentcore()
    assert "namespacePath" in c.meta.service_model.operation_model("ListMemoryRecords").input_shape.members
    assert memories.MODELS in c._loader.search_paths[:1]
