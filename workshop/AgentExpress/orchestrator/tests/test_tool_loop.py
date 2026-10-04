"""Model-driven tool calls (`toolMode: "model"`, app/common/tool_loop.py).

What has to hold, whatever the model asks for:
  * it is offered exactly the agent's bound tools — narrowed to `call` — each with a real
    input schema, and nothing else;
  * a Knowledge Base search keeps the agent's corpus, and a tool's fixed `args` win over
    the model's, so config still decides the scope;
  * a refused or failed call is told to the model and reported as a limitation, never
    turned into an invented result; `maxToolCalls` is a hard limit;
  * what came back reaches the agent in the same labelled evidence shape as direct mode.
The model and the Gateway are faked here.
"""
from __future__ import annotations

import asyncio
import sys
import types

import pytest
from conftest import workflow

TOOLS = {"policyDocs": {"type": "kb", "corpora": ["policies"], "description": "Policy wording"},
         "claimsDb": {"type": "lambda", "description": "Claims warehouse",
                      "lambdaArn": "arn:aws:lambda:us-east-1:123456789012:function:f",
                      "toolSchema": [{"name": "q", "properties": {"query": {"type": "string"}}}],
                      "args": {"limit": 5}},
         "docs": {"type": "mcp", "endpoint": "https://example.com/mcp", "call": "search"}}


def wf(tool, **agent) -> dict:
    return {"orchestrator": {}, "tools": TOOLS,
            "agents": {"triage": {"name": "Triage", "runtime": "dedicated", "maxTokens": 100,
                                  "tool": tool, "corpus": "policies", **agent}},
            "steps": [{"agent": "triage"}]}


class FakeTool:
    def __init__(self, name, description="", schema=None, answer="rows", error=None):
        self.name, self.description = name, description
        self.args_schema = schema or {"type": "object", "properties": {"claim": {"type": "string"}},
                                      "required": ["claim"]}
        self.answer, self.error, self.seen = answer, error, []

    async def ainvoke(self, args):
        self.seen.append(args)
        if self.error:
            raise RuntimeError(self.error)
        return self.answer


class AI:
    """An AIMessage as the loop reads it."""
    def __init__(self, *calls, text=""):
        self.content = text
        self.tool_calls = [{"id": f"c{i}", "name": n, "args": a} for i, (n, a) in enumerate(calls)]


class FakeCtx:
    def __init__(self, tools, max_tool_calls=6):
        self.tools, self.tool = tools, tools[0]
        self.corpus = "policies"
        self.agent_id, self.agent_name = "triage", "Triage"
        self.topic, self.feedback = "Is claim 88213 covered?", None
        self.session_id, self.model, self.max_tokens = "s1", None, 800
        self.tool_mode, self.max_tool_calls = "model", max_tool_calls
        self.calls: list[tuple] = []
        self.logs: list[str] = []

    def _retrieval_query(self):
        return "claim 88213 coverage"

    async def retrieve(self, query, doc_type=None):
        self.calls.append(("retrieve", query, doc_type))
        return "Clause 4.2: water damage is covered.", "gateway"

    async def call_tool(self, key, query):
        self.calls.append(("call_tool", key, query))
        return f"{key} text", "gateway"

    async def log(self, msg):
        self.logs.append(msg)


def _run(monkeypatch, imp, ctx, script, published):
    loop = imp("app.common.tool_loop")
    llm = imp("app.common.llm")
    client = imp("app.features.gateway.client")
    turns: list[dict] = []

    async def fake_turn(name, system, messages, tools, **kw):
        turns.append({"name": name, "system": system, "messages": list(messages), "tools": tools})
        return script.pop(0)

    async def fake_listing():
        return [t for ts in published.values() for t in ts]
    monkeypatch.setattr(llm, "run_llm_tools", fake_turn)
    # The real narrowing (published_for, `call`) runs against a fake Gateway listing.
    monkeypatch.setattr(client, "GATEWAY_URL", "https://gw.example.com/mcp")
    monkeypatch.setattr(client, "_gateway_tools", fake_listing)
    monkeypatch.setattr(loop, "is_cancelled", lambda sid: False)
    monkeypatch.setattr(client, "_record_tool", lambda *a, **k: None)
    monkeypatch.setattr(client, "_record_policy", lambda *a, **k: None)
    evidence = imp("app.subagents._shared.evidence")
    text, gaps = asyncio.run(evidence.gather(ctx))
    return text, gaps, turns


def test_the_model_chooses_the_calls_and_the_evidence_comes_back_labelled(monkeypatch):
    keys = ["policyDocs", "claimsDb", "docs"]
    with workflow(wf(keys, toolMode="model")) as imp:
        db = FakeTool("claimsDb___q", "Look up a claim", answer="claim 88213: burst pipe, 2 May")
        docs = FakeTool("docs___search", "Search the docs")
        other = FakeTool("docs___delete", "not selected by `call`")
        ctx = FakeCtx(keys)
        script = [AI(("claimsDb___q", {"claim": "88213", "limit": 999})),
                  AI(("policyDocs___retrieve", {"query": "water damage", "corpus": "everything"})),
                  AI(text="Found the claim and the clause; nothing missing.")]
        text, gaps, turns = _run(monkeypatch, imp, ctx, script,
                                 {"claimsDb": [db], "docs": [docs, other]})
    # Offered exactly the bound tools, `call` honoured, each with its real schema.
    names = [t["name"] for t in turns[0]["tools"]]
    assert names == ["policyDocs___retrieve", "claimsDb___q", "docs___search"]
    assert turns[0]["tools"][1]["input_schema"]["required"] == ["claim"]
    assert "Is claim 88213 covered?" in turns[0]["messages"][0].content
    # The fixed `args` beat the model's, and the KB keeps the agent's corpus.
    assert db.seen == [{"claim": "88213", "limit": 5}]
    assert ctx.calls == [("retrieve", "water damage", "policies")]
    # Each result went back to the model before its next choice...
    assert "burst pipe" in turns[1]["messages"][-1].content
    # ...and reaches the agent labelled, like direct mode, saying who chose the call.
    assert "=== TOOL FUNCTION: claimsDb -> claimsDb___q(" in text
    assert "=== KNOWLEDGE BASE: policyDocs -> policyDocs___retrieve(" in text
    assert "chosen by the model" in text and gaps == []
    assert [m.split(":")[0] for m in ctx.logs] == ["Tool call 1/6", "Tool call 2/6"]


def test_a_refusal_or_failure_is_told_to_the_model_and_reported_never_invented(monkeypatch):
    with workflow(wf(["claimsDb", "docs"], toolMode="model")) as imp:
        db = FakeTool("claimsDb___q", error="AccessDeniedException: not authorized by policy")
        docs = FakeTool("docs___search", error="connection reset")
        ctx = FakeCtx(["claimsDb", "docs"])
        script = [AI(("claimsDb___q", {"claim": "1"}), ("docs___search", {"query": "x"}),
                     ("nosuch___tool", {})),
                  AI(text="Could not get anything.")]
        text, gaps, turns = _run(monkeypatch, imp, ctx, script, {"claimsDb": [db], "docs": [docs]})
    results = [m.content for m in turns[1]["messages"][-3:]]
    assert results[0].startswith("REFUSED by the policy")
    assert results[1].startswith("FAILED")
    assert results[2].startswith("There is no tool named nosuch___tool")
    assert text == ""
    assert any("refused by the Gateway's policy" in g for g in gaps)
    assert any("'docs___search' failed" in g for g in gaps)
    # The refusal is on the run's timeline, not only in what the model was told.
    assert any(m.startswith("Refused by Cedar policy: claimsDb___q") for m in ctx.logs)


def test_max_tool_calls_is_a_hard_limit(monkeypatch):
    with workflow(wf(["claimsDb"], toolMode="model", maxToolCalls=2)) as imp:
        db = FakeTool("claimsDb___q", answer="row")
        ctx = FakeCtx(["claimsDb"], max_tool_calls=2)
        script = [AI(("claimsDb___q", {"claim": "1"}), ("claimsDb___q", {"claim": "2"}),
                     ("claimsDb___q", {"claim": "3"})),
                  AI(("claimsDb___q", {"claim": "4"})),
                  AI(text="done")]
        _text, _gaps, turns = _run(monkeypatch, imp, ctx, script, {"claimsDb": [db]})
    assert [a["claim"] for a in db.seen] == ["1", "2"]
    assert turns[1]["messages"][-1].content.startswith("Not called: this agent may make at most 2")
    assert ctx.logs[-1].startswith("Tool calls: reached this agent's limit of 2")


def test_a_tool_the_gateway_hides_is_named_and_none_at_all_fails_the_agent(monkeypatch):
    # Live: a forbid with no condition made the Gateway list nothing, the loop handed the
    # agent empty evidence, and the model wrote a tool call in prose with invented numbers.
    with workflow(wf(["claimsDb", "docs"], toolMode="model")) as imp:
        db = FakeTool("claimsDb___q", answer="row")
        ctx = FakeCtx(["claimsDb", "docs"])
        _text, gaps, _turns = _run(monkeypatch, imp, ctx, [AI(text="nothing needed")], {"claimsDb": [db]})
        assert any(g.startswith("'docs' was not offered") and "forbids it outright" in g for g in gaps)
        with pytest.raises(imp("app.common.errors").ToolUnavailable, match="lists none of this agent's tools"):
            _run(monkeypatch, imp, FakeCtx(["claimsDb", "docs"]), [], {})


def test_direct_mode_is_unchanged_and_the_registry_reads_the_mode():
    with workflow(wf(["policyDocs"], toolMode="model", maxToolCalls=3)) as imp:
        registry = imp("app.orchestrator.registry")
        spec = wf(["policyDocs"], toolMode="model", maxToolCalls=3)["agents"]["triage"]
        agent = registry._configure(imp("app.common.base").Agent(), "triage", spec)
        assert (agent.tool_mode, agent.max_tool_calls) == ("model", 3)
        plain = registry._configure(imp("app.common.base").Agent(), "triage",
                                    wf(["policyDocs"])["agents"]["triage"])
        assert (plain.tool_mode, plain.max_tool_calls) == ("direct", 6)
        evidence = imp("app.subagents._shared.evidence")
        ctx = FakeCtx(["policyDocs"])
        ctx.tool_mode = "direct"
        text, _ = asyncio.run(evidence.gather(ctx))
    assert ctx.calls == [("retrieve", "claim 88213 coverage", "policies")]
    assert "chosen by the model" not in text


def test_a_tool_turn_offers_the_specs_and_adapts_to_a_refused_setting(monkeypatch):
    from app.common import llm as llm_mod
    seen: list[dict] = []

    class _Msg:
        def __init__(self):
            self.content = ""
            self.usage_metadata: dict = {}
            self.response_metadata = {"stopReason": "tool_use"}
            self.tool_calls = [{"id": "1", "name": "t", "args": {}}]

    class _FakeLLM:
        def __init__(self, **k):
            self.k = k

        def bind_tools(self, specs):
            seen.append({"k": self.k, "specs": specs})
            return self

        async def ainvoke(self, convo):
            seen[-1]["convo"] = convo
            if "temperature" in self.k:
                raise ValueError("ValidationException: `temperature` is deprecated for this model.")
            return _Msg()
    fake = types.ModuleType("langchain_aws")
    fake.ChatBedrockConverse = _FakeLLM
    monkeypatch.setitem(sys.modules, "langchain_aws", fake)
    from langchain_core.messages import HumanMessage
    msg = asyncio.run(llm_mod.run_llm_tools(
        "triage.tools", "choose", [HumanMessage(content="task")],
        [{"name": "t", "description": "d", "input_schema": {"type": "object", "properties": {}}}]))
    assert msg.tool_calls[0]["name"] == "t"
    assert len(seen) == 2 and "temperature" not in seen[1]["k"]
    assert seen[1]["specs"] == [{"type": "function", "function": {
        "name": "t", "description": "d", "parameters": {"type": "object", "properties": {}}}}]
    assert seen[1]["convo"][0].content == "choose"


def test_the_tool_turn_sees_what_earlier_steps_approved(monkeypatch):
    """A tool may work ON an earlier step's output (a checker run on the draft). Observed
    live: shown only the request, the editor had no draft to pass and skipped the call."""
    with workflow(wf(["docs"], toolMode="model")) as imp:
        loop = imp("app.common.tool_loop")
        config = imp("app.common.config")
        monkeypatch.setattr(config, "upstream_of", lambda aid: ["writer", "planner"])
        ctx = FakeCtx(["docs"])
        ctx.input = lambda aid: {"writer": '{"body": "## Intro\\nThe draft."}', "planner": None}[aid]
        task = loop._task(ctx, "")
    assert "APPROVED UPSTREAM OUTPUTS" in task
    assert "--- WRITER ---" in task and "The draft." in task
    assert "PLANNER" not in task                                   # nothing approved: not shown
    assert "Today's date is" in task
    # The agent's own instructions reach the turn that makes the calls.
    ctx.agent_prompt = "Always call the Datamuse tool at least once."
    with workflow(wf(["docs"], toolMode="model")) as imp:
        assert "Always call the Datamuse tool" in imp("app.common.tool_loop")._task(ctx, "")
