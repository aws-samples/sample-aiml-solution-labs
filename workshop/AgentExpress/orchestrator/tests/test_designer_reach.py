"""The Assistant reaches what the tabs reach: skills, interceptors written in the build,
and the organization's AWS Agent Registry (search, import, update, keep in sync).

  * a skill is written with set_block, used by an agent, and removed by setting it null;
  * an interceptor's code is generated on the server exactly as the Interceptors tab
    generates it, hand-edited code is kept unless asked, and a lambdaArn drops the code;
  * a registry record is searched, then imported by its id: the server builds the entry
    (never the model), one registry is picked by itself and several must be named;
  * publishing is not something the Assistant can do.
The registry is faked as in tests/test_registry.py.
"""
# ruff: noqa: F811 - `env` and `model` are fixtures, imported so every test can take them
from __future__ import annotations

import pytest
from test_builds import CTX, call, env, save  # noqa: F401
from test_designer import BASE, draft, model, run_background, says, send, tool_use  # noqa: F401
from test_registry import RID, FakeDp, agent, mcp, skill

pytest.importorskip("moto")


def ops(e):
    return e.handler.designer.apply_ops


# --- skills -----------------------------------------------------------------------------

SKILL = {"description": "When a customer asks for money back.", "instructions": "1. Check the date.",
         "files": {"policy.md": "Refunds within 30 days."}}


def test_a_skill_is_written_used_and_removed(env):
    new, changed = ops(env)(BASE, [
        {"op": "set_block", "name": "skills", "value": {"refundPolicy": SKILL}},
        {"op": "update_agent", "id": "a", "set": {"skills": ["refundPolicy"]}}])
    assert new["workflow"]["skills"] == {"refundPolicy": SKILL}
    assert "skills" in changed["blocks"] and changed["agents"] == ["a"]
    v = env.handler.designer.validate_build
    assert not [i for i in v.validate(new["workflow"]) if i["path"].startswith("skills")]
    gone, _ = ops(env)(new, [{"op": "set_block", "name": "skills", "value": {"refundPolicy": None}},
                             {"op": "update_agent", "id": "a", "unset": ["skills"]}])
    assert gone["workflow"]["skills"] == {}


def test_a_null_removes_an_entry_only_from_a_named_map(env):
    base = {**BASE, "workflow": {**BASE["workflow"], "memories": {"prefs": {"strategies": ["semantic"]}},
                                 "ui": {"title": "T"}}}
    new, _ = ops(env)(base, [{"op": "set_block", "name": "memories", "value": {"prefs": None}},
                             {"op": "set_block", "name": "ui", "value": {"subtitle": None}}])
    assert new["workflow"]["memories"] == {}
    assert new["workflow"]["ui"] == {"title": "T", "subtitle": None}   # not a named map


# --- interceptors -----------------------------------------------------------------------

def test_an_interceptor_is_generated_as_the_tab_generates_it(env):
    import interceptor_code as ic
    templates = {"audit": {}, "injectContext": {"arguments": {"userId": "user"}}}
    new, changed = ops(env)(BASE, [{"op": "set_interceptor", "point": "request", "templates": templates}])
    got = new["workflow"]["orchestrator"]["interceptors"]["request"]
    assert got == {"code": {}, "templates": templates, "passRequestHeaders": True}   # injectContext reads them
    assert new["toolCode"]["interceptor-request"] == ic.files("request", templates, new["workflow"])
    assert "def inject_context(" in new["toolCode"]["interceptor-request"]["handler.py"]
    assert changed["blocks"] == ["orchestrator"]
    v = env.handler.designer.validate_build
    assert not [i for i in v.errors(new["workflow"]) if i["path"].startswith("orchestrator.interceptors")]
    assert env.builds.code_errors(new) == []


def test_hand_edited_interceptor_code_is_kept_unless_regenerated(env):
    d = ops(env)
    first, _ = d(BASE, [{"op": "set_interceptor", "point": "response", "templates": {"custom": {}}}])
    handler = first["toolCode"]["interceptor-response"]["handler.py"] + "\n# mine\n"
    edited, _ = d(first, [{"op": "set_tool_code", "key": "interceptor-response", "files": {"handler.py": handler}}])
    kept, changed = d(edited, [{"op": "set_interceptor", "point": "response",
                                "templates": {"custom": {}, "capResult": {"maxChars": 500}}}])
    assert kept["toolCode"]["interceptor-response"]["handler.py"] == handler
    assert kept["workflow"]["orchestrator"]["interceptors"]["response"]["templates"]["capResult"] == {"maxChars": 500}
    assert "edited by hand" in changed["notes"][0]
    again, _ = d(kept, [{"op": "set_interceptor", "point": "response", "regenerate": True}])
    assert "# mine" not in again["toolCode"]["interceptor-response"]["handler.py"]
    assert "def cap_result(" in again["toolCode"]["interceptor-response"]["handler.py"]


def test_an_own_function_drops_the_code_and_removing_turns_it_off(env):
    d = ops(env)
    coded, _ = d(BASE, [{"op": "set_interceptor", "point": "request", "templates": {"audit": {}}}])
    arn = "arn:aws:lambda:us-east-1:123456789012:function:mine"
    own, _ = d(coded, [{"op": "set_interceptor", "point": "request", "lambdaArn": arn}])
    assert own["workflow"]["orchestrator"]["interceptors"]["request"] == {"lambdaArn": arn}
    assert "interceptor-request" not in own["toolCode"]
    off, _ = d(own, [{"op": "remove_interceptor", "point": "request"}])
    assert "interceptors" not in off["workflow"]["orchestrator"]


@pytest.mark.parametrize("bad,why", [
    ([{"op": "set_tool_code", "key": "interceptor-request", "files": {"handler.py": "x"}}], "set_interceptor"),
    ([{"op": "set_interceptor", "point": "sideways", "templates": {}}], "request or response"),
    ([{"op": "remove_interceptor", "point": "request"}], "no 'request' interceptor"),
])
def test_an_interceptor_edit_that_cannot_apply_is_named(env, bad, why):
    with pytest.raises(env.handler.designer.OpError, match=why):
        ops(env)(BASE, bad)


# --- the registry -----------------------------------------------------------------------

class FakeCtl:
    def __init__(self, regs):
        self.regs = regs

    def list_registries(self, **kw):
        return {"registries": self.regs}


ONE = [{"registryId": RID, "name": "ax-qa-org", "status": "READY"}]
TWO = [*ONE, {"registryId": "OtherRegistry01", "name": "partners", "status": "READY"}]


@pytest.fixture()
def reg(env, monkeypatch):
    registry = env.handler.registry

    def use(records, regs=ONE):
        fake = FakeDp(records)
        monkeypatch.setitem(registry._clients, "dp", fake)
        monkeypatch.setitem(registry._clients, "ctl", FakeCtl(regs))
        return fake
    yield use
    registry._clients.clear()


def test_search_says_what_each_record_becomes_or_why_not(env, reg):
    reg([mcp(), agent(), skill(), mcp(rid="r4", name="no-url", url=None)])
    got = env.handler.designer.search_registry({"query": "orders"})
    assert got["status"] == "ok" and got["registry"] == {"id": RID, "name": "ax-qa-org"}
    by = {r["recordId"]: r for r in got["records"]}
    assert by["r1"]["becomes"] == "tools.ordersMcp" and by["r1"]["tools"] == ["listOrders"]
    assert by["r2"]["becomes"] == "agents.ordersAgent" and by["r2"]["skills"] == ["Orders"]
    assert by["r3"]["becomes"] == "skills.refundPolicy"
    assert "streamable-HTTP" in by["r4"]["cannotAdd"] and "becomes" not in by["r4"]


def test_several_registries_must_be_named(env, reg):
    reg([mcp()], regs=TWO)
    d = env.handler.designer
    got = d.search_registry({"query": "orders"})
    assert got["status"] == "error" and "ax-qa-org, partners" in got["error"]
    assert d.search_registry({"query": "orders", "registry": "AX-QA-ORG"})["status"] == "ok"
    assert "no Agent Registry 'nope'" in d.search_registry({"registry": "nope"})["error"]


def test_an_import_is_built_by_the_server_from_the_record(env, reg):
    reg([mcp(), agent(), skill()])
    new, changed = ops(env)(BASE, [
        {"op": "import_from_registry", "recordId": "r1", "sync": True},
        {"op": "import_from_registry", "recordId": "r2"},
        {"op": "import_from_registry", "recordId": "r3", "registry": "ax-qa-org"},
        {"op": "update_agent", "id": "a", "set": {"tool": ["kb", "db", "ordersMcp"]}}])
    wf = new["workflow"]
    reg_mod = env.handler.registry
    assert wf["tools"]["ordersMcp"] == reg_mod.to_entry(mcp(), RID, True)["entry"]
    assert wf["tools"]["ordersMcp"]["registry"]["sync"] is True and "auth" not in wf["tools"]["ordersMcp"]
    assert wf["agents"]["ordersAgent"]["runtime"] == "a2a" and wf["agents"]["ordersAgent"]["registry"]["sync"] is False
    assert wf["skills"]["refundPolicy"]["instructions"] == "1. Check the date."
    assert changed["tools"] == ["ordersMcp"] and set(changed["agents"]) == {"ordersAgent", "a"}
    assert "skills" in changed["blocks"]
    assert wf["agents"]["a"]["tool"][-1] == "ordersMcp"      # later edits see the import
    with pytest.raises(env.handler.designer.OpError, match=r"already has tools\.ordersMcp"):
        ops(env)(new, [{"op": "import_from_registry", "recordId": "r1"}])
    second, _ = ops(env)(new, [{"op": "import_from_registry", "recordId": "r1", "key": "ordersTwo"}])
    assert "ordersTwo" in second["workflow"]["tools"]


@pytest.mark.parametrize("op,why", [
    ({"op": "import_from_registry", "recordId": "gone12"}, "no approved record"),
    ({"op": "import_from_registry", "recordId": "../x"}, "recordId"),
    ({"op": "import_from_registry", "recordId": "r4"}, "cannot be added"),
    ({"op": "update_from_registry", "key": "kb"}, "no item 'kb' that came from a registry"),
])
def test_a_registry_edit_that_cannot_apply_is_named(env, reg, op, why):
    reg([mcp(rid="r4", url=None)])
    with pytest.raises(env.handler.designer.OpError, match=why):
        ops(env)(BASE, [op])


def test_an_item_is_updated_to_the_newest_version_and_its_sync_set(env, reg):
    fake = reg([mcp()])
    have, _ = ops(env)(BASE, [{"op": "import_from_registry", "recordId": "r1"},
                              {"op": "update_tool", "key": "ordersMcp", "set": {"auth": "apikey"}}])
    with pytest.raises(env.handler.designer.OpError, match="already the newest approved version"):
        ops(env)(have, [{"op": "update_from_registry", "key": "ordersMcp"}])
    fake.records["r1"] = mcp(version="1.1.0", url="https://mcp2.example.com/mcp")
    new, changed = ops(env)(have, [{"op": "update_from_registry", "key": "ordersMcp", "map": "tool"}])
    t = new["workflow"]["tools"]["ordersMcp"]
    assert t["endpoint"] == "https://mcp2.example.com/mcp" and t["registry"]["version"] == "1.1.0"
    assert t["auth"] == "apikey"                             # the build's own choice stays
    assert changed["notes"] == ["tools.ordersMcp: 1.0.0 -> 1.1.0"]
    synced, _ = ops(env)(new, [{"op": "set_registry_sync", "key": "ordersMcp", "sync": True}])
    assert synced["workflow"]["tools"]["ordersMcp"]["registry"]["sync"] is True


def test_in_a_turn_the_model_searches_then_imports(env, reg, model):
    reg([mcp()])
    save(env)
    script = model(
        tool_use(("search_registry", {"query": "orders", "kind": "tool"})),
        tool_use(("apply_changes", {"summary": "Added the orders MCP server from the registry",
                                    "ops": [{"op": "import_from_registry", "recordId": "r1", "sync": True}],
                                    "defaults_used": []})),
        says("Added **ordersMcp** from ax-qa-org, kept in sync."))
    assert send(env, "import the orders MCP from the registry and keep it in sync")[0] == 202
    assert run_background(env) == {"ok": True}
    assert draft(env)["workflow"]["tools"]["ordersMcp"]["registry"]["sync"] is True
    found = script.calls[1]["messages"][-1]["content"][0]["toolResult"]["content"][0]["json"]
    assert found["records"][0]["becomes"] == "tools.ordersMcp"
    names = [t["toolSpec"]["name"] for t in script.calls[0]["toolConfig"]["tools"]]
    assert names == ["apply_changes", "undo_last_change", "search_registry"]   # nothing publishes
