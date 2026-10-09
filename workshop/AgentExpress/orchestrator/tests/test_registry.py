"""AWS Agent Registry for the Builder (bff/registry.py): finding approved records and
taking one into a build, and keeping it in sync when the author asked for that.
What has to hold:
  * an MCP record becomes an `mcp` tool with its endpoint and its tools as toolSchema,
    an agent card a remote (a2a) agent, a SKILL record a skill: and a record that lacks
    what that needs is shown with why, never half-imported;
  * nothing about HOW a tool connects comes from a record: auth stays the build's;
  * a newer approved version is found both ways it can appear (the same record
    re-versioned, or a new record of the same name), only `sync` items are changed
    automatically, and an update keeps the build's own settings;
  * a registry that cannot be reached never stops a deploy.
The Registry is faked here; the live probe is ~/.agentexpress/qa/qa_registry.py.
"""
from __future__ import annotations

import json

import pytest
import registry  # bff/ is on sys.path (conftest)

RID = "VABICydSql8rW47l"
TOOLS = {"tools": [{"name": "listOrders", "description": "Orders.",
                    "inputSchema": {"type": "object", "required": ["status"],
                                    "properties": {"status": {"type": "string", "description": "Which"},
                                                   "limit": {"type": "integer"},
                                                   "when": {"type": "date"}}}}]}


def mcp(rid="r1", version="1.0.0", url="https://mcp.example.com/mcp", name="orders-mcp", tools=TOOLS):
    server = {"name": "acme/orders", "description": "Orders MCP server", "version": version,
              **({"remotes": [{"type": "streamable-http", "url": url}]} if url else {})}
    return {"recordId": rid, "name": name, "recordType": "MCP", "recordVersion": version, "status": "APPROVED",
            "updatedAt": "2026-10-08", "descriptors": {"mcpServer": {
                "data": json.dumps(server), "additionalData": {"tools": {"data": json.dumps(tools)}}}}}


def agent(rid="r2", url="https://agents.example.com/a2a"):
    card = {"name": "Orders agent", "description": "Answers order questions", "url": url,
            "skills": [{"id": "orders", "name": "Orders"}]}
    return {"recordId": rid, "name": "orders-agent", "recordType": "AGENT", "recordVersion": "1.0.0",
            "descriptors": {"a2aAgentCard": {"data": json.dumps(card)}}}


def skill(rid="r3", version="1.0.0", body="1. Check the date.", md=True):
    text = f"---\nname: refund-policy\ndescription: When a customer asks for money back.\n---\n\n{body}\n"
    return {"recordId": rid, "name": "refund-policy", "recordType": "SKILL", "recordVersion": version,
            "descriptors": {"agentSkillsDefinition": {"data": "{}", **(
                {"additionalData": {"skillMd": {"data": text}}} if md else {})}}}


class FakeDp:
    def __init__(self, records):
        self.records = {r["recordId"]: r for r in records}
        self.calls = []

    def search_discoverable_registry_records(self, **kw):
        self.calls.append(("search", kw))
        return {"registryRecords": list(self.records.values())}

    def list_discoverable_registry_records(self, **kw):
        self.calls.append(("list", kw))
        return {"registryRecords": [{k: v for k, v in r.items() if k != "descriptors"} for r in self.records.values()]}

    def batch_get_discoverable_registry_record(self, entries):
        ids = entries[0]["recordIds"]
        return {"registryRecords": [self.records[i] for i in ids if i in self.records]}


@pytest.fixture()
def dp(monkeypatch):
    def use(records):
        fake = FakeDp(records)
        monkeypatch.setitem(registry._clients, "dp", fake)
        return fake
    yield use
    registry._clients.clear()


def test_an_mcp_record_becomes_a_tool_with_its_tools_and_no_auth():
    got = registry.to_entry(mcp(), RID, sync=True)
    assert got["kind"] == "tool" and got["key"] == "ordersMcp" and got["tools"] == ["listOrders"]
    e = got["entry"]
    assert e["type"] == "mcp" and e["endpoint"] == "https://mcp.example.com/mcp"
    assert e["toolSchema"] == [{"name": "listOrders", "description": "Orders.", "properties": {
        "status": {"type": "string", "required": True, "description": "Which"},
        "limit": {"type": "integer", "required": False, "description": "limit"},
        "when": {"type": "string", "required": False, "description": "when"}}}]
    assert e["registry"] == {"registryId": RID, "recordId": "r1", "name": "orders-mcp", "version": "1.0.0",
                             "sync": True}
    assert not {"auth", "oauth", "identity"} & set(e)


@pytest.mark.parametrize("rec,kind,why", [
    (mcp(url=""), "tool", "no https streamable-HTTP endpoint"),
    (mcp(url="http://insecure.example.com/mcp"), "tool", "no https"),
    (agent(url=""), "agent", "no A2A agent card"),
    (skill(md=False), "skill", "carries no SKILL.md"),
    ({"recordId": "x", "name": "x", "recordType": "CUSTOM", "descriptors": {"custom": {"data": "{}"}}}, None,
     "cannot be added"),
])
def test_a_record_missing_what_it_needs_says_why(rec, kind, why):
    got = registry.to_entry(rec, RID)
    assert got["kind"] == kind and why in got["why"] and "entry" not in got


def test_an_agent_card_and_a_skill_map_to_a_remote_agent_and_a_skill():
    a = registry.to_entry(agent(), RID)
    assert a["entry"] == {"name": "Orders agent", "runtime": "a2a", "agentCard": "https://agents.example.com/a2a",
                          "produces": "ordersAgent", "registry": {"registryId": RID, "recordId": "r2",
                                                                  "name": "orders-agent", "version": "1.0.0",
                                                                  "sync": False}}
    s = registry.to_entry(skill(), RID)
    assert s["key"] == "refundPolicy"
    assert s["entry"]["description"] == "When a customer asks for money back."
    assert s["entry"]["instructions"] == "1. Check the date."


def test_search_narrows_by_kind_and_browses_without_a_query(dp):
    fake = dp([mcp(), agent(), skill()])
    hits = registry.search(RID, "orders", "tool")
    assert [h["recordId"] for h in hits] == ["r1"]
    assert hits[0]["entry"]["endpoint"] == "https://mcp.example.com/mcp"
    browsed = registry.search(RID, "", "skill")
    assert [h["key"] for h in browsed] == ["refundPolicy"]
    assert fake.calls[-1][1]["filters"] == [{"name": "recordType", "values": ["SKILL"]}]
    with pytest.raises(Exception, match="registry id"):
        registry.search("../x", "q")


def project(sync_tool=True, sync_skill=False):
    tool = {**registry.to_entry(mcp(), RID, sync_tool)["entry"], "auth": "apikey", "policies": ["readOnly"]}
    sk = {**registry.to_entry(skill(), RID, sync_skill)["entry"], "files": {"notes.md": "mine"}}
    return {"workflow": {"tools": {"orders": tool, "local": {"type": "mcp", "endpoint": "https://x"}},
                         "skills": {"refunds": sk}, "agents": {"a": {"name": "A"}}}}


def test_a_newer_version_is_found_either_way_and_only_sync_items_change(dp):
    # The tool's record re-versioned in place; the skill superseded by a new record of the
    # same name with a higher version.
    dp([mcp(version="1.1.0", url="https://mcp2.example.com/mcp"), skill(), skill(rid="r9", version="2.0.0",
                                                                              body="1. New steps.")])
    ups = registry.updates(project())
    assert {(u["map"], u["key"], u["from"], u["to"], u["sync"]) for u in ups} == {
        ("tools", "orders", "1.0.0", "1.1.0", True), ("skills", "refunds", "1.0.0", "2.0.0", False)}
    synced, changed = registry.apply(project(), ups)
    t = synced["workflow"]["tools"]["orders"]
    assert changed == ["tools.orders 1.0.0 -> 1.1.0"]
    assert t["endpoint"] == "https://mcp2.example.com/mcp" and t["registry"]["version"] == "1.1.0"
    # The build's own choices stay: how it connects, its policies.
    assert t["auth"] == "apikey" and t["policies"] == ["readOnly"]
    # Not marked sync: left as imported.
    assert synced["workflow"]["skills"]["refunds"]["instructions"] == "1. Check the date."
    everything, _ = registry.apply(project(), ups, only_sync=False)
    s = everything["workflow"]["skills"]["refunds"]
    assert s["instructions"] == "1. New steps." and s["files"] == {"notes.md": "mine"}
    assert s["registry"]["recordId"] == "r9"


def test_nothing_newer_means_no_update(dp):
    dp([mcp(), skill()])
    assert registry.updates(project()) == []


def test_a_registry_that_cannot_be_reached_never_stops_a_deploy(monkeypatch):
    class Down:
        def batch_get_discoverable_registry_record(self, **kw):
            raise ConnectionError("no route")
    monkeypatch.setitem(registry._clients, "dp", Down())
    p = project()
    same, said = registry.sync(p)
    registry._clients.clear()
    assert same is p and "kept the versions in the build" in said[0]
    plain = {"workflow": {"tools": {"x": {"type": "mcp", "endpoint": "https://x"}}}}
    assert registry.sync(plain) == (plain, [])


def test_the_registries_listed_ready_first(monkeypatch):
    class Ctl:
        def list_registries(self, **kw):
            return {"registries": [{"registryId": "b", "name": "zeta", "status": "READY"},
                                   {"registryId": "c", "name": "alpha", "status": "CREATING"},
                                   {"registryId": "a", "name": "Beta", "status": "READY"}]}
    monkeypatch.setitem(registry._clients, "ctl", Ctl())
    try:
        assert [r["id"] for r in registry.list_registries()] == ["a", "b", "c"]
    finally:
        registry._clients.clear()


def test_a_refusal_is_said_plainly():
    import builds

    class Denied(Exception):
        def __init__(self, msg):
            super().__init__(msg)
            self.response = {"Error": {"Code": "AccessDeniedException", "Message": "not allowed"}}

    def boom():
        raise Denied("x")
    with pytest.raises(builds.BuildError) as got:
        builds.registry_call(boom)
    assert got.value.status == 403 and "AccessDeniedException: not allowed" in str(got.value)


# --- publishing (R2 / S2) ---------------------------------------------------------------

class FakeCtl:
    """Records go CREATING -> DRAFT on the next read, then PENDING_APPROVAL when submitted."""
    def __init__(self):
        self.records, self.calls, self.n = {}, [], 0

    def create_registry_record(self, **kw):
        self.calls.append(("create", kw))
        self.n += 1
        rid = f"rec{self.n}"
        self.records[rid] = {"status": "CREATING", **kw}
        return {"recordArn": f"arn:aws:agent-registry:us-east-1:1:registry/{kw['registryId']}/record/{rid}"}

    def update_registry_record(self, **kw):
        self.calls.append(("update", kw))
        self.records[kw["recordId"]].update(status="UPDATING", recordVersion=kw["recordVersion"])

    def get_registry_record(self, registryId, recordId):
        r = self.records[recordId]
        if r["status"] in ("CREATING", "UPDATING"):
            r["status"] = "DRAFT"
            return {**r, "status": "CREATING"}
        return r

    def submit_registry_record_for_approval(self, registryId, recordId):
        self.calls.append(("submit", recordId))
        self.records[recordId]["status"] = "PENDING_APPROVAL"

    def update_registry_record_status(self, **kw):
        self.calls.append(("status", kw))
        self.records[kw["recordId"]]["status"] = kw["status"]


@pytest.fixture()
def ctl(monkeypatch):
    fake = FakeCtl()
    monkeypatch.setitem(registry._clients, "ctl", fake)
    monkeypatch.setattr(registry, "CREATE_WAIT_S", 9)
    import time
    monkeypatch.setattr(time, "sleep", lambda s: None)
    yield fake
    registry._clients.clear()


WF = {"agents": {"a": {"name": "Writer", "tool": ["calc", "kb"], "skills": ["tone"]}},
      "tools": {"calc": {"type": "lambda", "description": "Counts words",
                         "toolSchema": [{"name": "countWords", "properties": {
                             "text": {"type": "string", "required": True, "description": "The text"}}}]},
                "kb": {"type": "kb", "description": "Docs"},
                "me": {"type": "openapi", "auth": "user", "description": "Person's own"},
                "api": {"type": "openapi", "description": "Orders", "schema": {"paths": {"/o": {"get": {
                    "operationId": "listOrders",
                    "parameters": [{"name": "status", "in": "query", "required": True}]}}}}}},
      "skills": {"tone": {"description": "When writing.", "instructions": "Be brief."}}}
ITEM = {"id": "b1", "name": "Blog Studio", "agentName": "ax_12345678",
        "deployed": {"version": 3, "uiUrl": "https://app.example.com", "apiUrl": "https://api.example.com",
                     "gatewayUrl": "https://gw.example.com/mcp"}}


def test_a_deployed_build_is_published_as_its_workflow_and_its_gateways_tools():
    specs = registry.build_records(ITEM, WF)
    assert set(specs) == {"workflow", "gateway"}
    wf = json.loads(specs["workflow"]["descriptors"]["custom"]["data"])
    assert specs["workflow"]["recordType"] == "AGENT" and specs["workflow"]["name"] == "ax-12345678-workflow"
    assert wf["app"] == "https://app.example.com" and wf["version"] == "3" and wf["skills"] == ["tone"]
    mcp_spec = specs["gateway"]["descriptors"]["mcpServer"]
    assert json.loads(mcp_spec["data"])["remotes"] == [{"type": "streamable-http", "url": "https://gw.example.com/mcp"}]
    tools = {t["name"]: t for t in json.loads(mcp_spec["additionalData"]["tools"]["data"])["tools"]}
    # As the Gateway names them; the person's tool is on the other Gateway, so not here.
    assert set(tools) == {"calc___countWords", "kb___retrieve", "api___listOrders"}
    assert tools["calc___countWords"]["inputSchema"]["required"] == ["text"]
    assert tools["api___listOrders"]["inputSchema"]["required"] == ["status"]
    no_gw = registry.build_records({**ITEM, "deployed": {"version": 3}}, WF)
    assert set(no_gw) == {"workflow"}


def test_publishing_creates_then_updates_the_same_record_and_never_approves_it(ctl):
    spec = registry.build_records(ITEM, WF)["workflow"]
    first = registry.put_record(RID, spec, "3", None)
    assert first["status"] == "PENDING_APPROVAL" and first["version"] == "3"
    again = registry.put_record(RID, spec, "4", first)
    assert again["recordId"] == first["recordId"]
    kinds = [c[0] for c in ctl.calls]
    assert kinds == ["create", "submit", "update", "submit"]
    upd = ctl.calls[2][1]
    # UpdateRegistryRecord's shape: every descriptor field under {"optionalValue"}.
    assert upd["descriptors"] == {"optionalValue": {"custom": {"optionalValue": {
        "data": {"optionalValue": spec["descriptors"]["custom"]["data"]}}}}}
    assert not any(c[0] == "status" and c[1]["status"] == "APPROVED" for c in ctl.calls)


def test_a_skill_is_published_as_its_skill_md():
    rec = registry.skill_record("refundPolicy", {"description": "When asked\nfor money back.",
                                                 "instructions": "1. Check the date."})
    assert rec["recordType"] == "SKILL" and rec["name"] == "refund-policy"
    md = rec["descriptors"]["agentSkillsDefinition"]["additionalData"]["skillMd"]["data"]
    assert md == "---\nname: refund-policy\ndescription: When asked for money back.\n---\n\n1. Check the date.\n"
    assert registry.parse_skill_md(md)["instructions"] == "1. Check the date."


def test_statuses_are_read_live_and_a_destroy_deprecates_the_build_not_its_skills(ctl):
    spec = registry.build_records(ITEM, WF)["workflow"]
    rec = registry.put_record(RID, spec, "3", None)
    sk = registry.put_record(RID, registry.skill_record("tone", WF["skills"]["tone"]), "1.0.0", None)
    pub = {"registryId": RID, "records": {"workflow": rec}, "skills": {"tone": sk}}
    ctl.records[rec["recordId"]]["status"] = "REJECTED"
    ctl.records[rec["recordId"]]["statusReason"] = "Needs an owner tag"
    got = registry.statuses(pub)
    assert got["records"]["workflow"]["status"] == "REJECTED"
    assert got["records"]["workflow"]["statusReason"] == "Needs an owner tag"
    assert registry.deprecate(pub, "destroyed") == ["workflow"]
    assert ctl.records[rec["recordId"]]["status"] == "DEPRECATED"
    assert ctl.records[sk["recordId"]]["status"] == "PENDING_APPROVAL"


def test_the_bff_ships_the_registry_api_models_its_lambda_lacks():
    """Seen live: the Lambda runtime's boto3 said "Unknown service: 'agent-registry-control'".
    The models under bff/botocore_data alone must make both clients."""
    from botocore.loaders import Loader
    only_ours = Loader(extra_search_paths=[registry.MODELS], include_default_search_paths=False)
    for name, op in (("agent-registry-control", "CreateRegistryRecord"),
                     ("agent-registry", "SearchDiscoverableRegistryRecords")):
        model = only_ours.load_service_model(name, "service-2")
        assert op in model["operations"]
        assert only_ours.load_service_model(name, "endpoint-rule-set-1")["rules"]


def test_a_search_asks_for_no_more_than_the_registry_returns(dp):
    """SearchDiscoverableRegistryRecords takes maxResults up to 20 (seen live: 25 was refused)."""
    fake = dp([mcp()])
    registry.search(RID, "orders")
    kind, kw = fake.calls[-1]
    assert kind == "search" and kw["maxResults"] <= 20
