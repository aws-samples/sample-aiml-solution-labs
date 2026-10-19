"""An agent asks a person mid-step, and a tool call waits for approval (app/common/asks.py).

What has to hold:
  * a question pauses the run, and the run resumes with the answer in the model's context,
    labelled as the person's untrusted words;
  * the model turns and tool calls made BEFORE the question are not made again on resume
    (LangGraph re-runs the whole node; the journal replays them). Asserted by call counts;
  * a tool with `approval` is not called until a person approves the exact arguments; a
    denial is told to the model, which carries on;
  * a tool the Gateway's policy hides is never put to a person;
  * an unattended run (gates "auto") does not wait: no answer, no call;
  * a question with a timeout is decided by the BFF's sweep, as a gate is;
  * the question cap holds;
  * the decision route takes an answer, refuses what cannot answer it, and still takes
    every payload it took before.

The graph, its nodes, the tool loop, the journal and the checkpointer (MemorySaver) are the
real ones. Only the model and the Gateway are faked.
"""
from __future__ import annotations

import asyncio
import importlib
import json
import sys
import types

import pytest
from conftest import workflow

SESSION = "ask-test"
TOOLS = {"lookup": {"type": "mcp", "description": "Look up a claim", "endpoint": "https://example.com/mcp",
                    "call": "find"},
         "payments": {"type": "lambda", "description": "Pay a refund",
                      "lambdaArn": "arn:aws:lambda:us-east-1:123456789012:function:pay",
                      "toolSchema": [{"name": "pay", "properties": {"amount": {"type": "number"}}}],
                      "args": {"currency": "EUR"}, "approval": "person"}}


def wf(agent: dict | None = None, *, tools: dict | None = None, steps=None, agents=None) -> dict:
    spec = {"name": "Triage", "runtime": "main", "maxTokens": 400, "toolMode": "model",
            "tool": ["lookup", "payments"], "askPerson": True, **(agent or {})}
    return {"orchestrator": {}, "tools": tools or TOOLS,
            "agents": agents or {"triage": spec},
            "steps": steps or [{"agent": "triage"}]}


class FakeTool:
    def __init__(self, name, answer="claim 7: burst pipe", error=None):
        self.name, self.description, self.answer, self.error = name, "", answer, error
        self.args_schema = {"type": "object", "properties": {"q": {"type": "string"}}}
        self.seen: list[dict] = []

    async def ainvoke(self, args):
        self.seen.append(args)
        if self.error:
            raise RuntimeError(self.error)
        return self.answer


def ai(*calls, text=""):
    from langchain_core.messages import AIMessage
    return AIMessage(content=text, tool_calls=[
        {"name": n, "args": a, "id": f"call{i}-{n}", "type": "tool_call"} for i, (n, a) in enumerate(calls)])


class Run:
    """A one-agent (or more) workflow run through the REAL graph with a fake model and Gateway."""

    def __init__(self, imp, monkeypatch, script, published, agent_ids=("triage",), scripts=None,
                 plan_first=False):
        self.imp = imp
        base = imp("app.common.base")
        sink = imp("app.common.sink")
        self.bus = imp("app.common.bus").bus
        gb = imp("app.orchestrator.graph_builder")
        llm = imp("app.common.llm")
        client = imp("app.features.gateway.client")
        context = imp("app.common.context")
        evidence = imp("app.subagents._shared.evidence")
        meter = imp("app.features.observability.meter")
        self.asks = imp("app.common.asks")
        config = imp("app.common.config")
        monkeypatch.setattr(sink, "STATUS_TABLE", None)
        monkeypatch.setattr(sink, "EVENTS_TABLE", None)
        monkeypatch.setattr(client, "GATEWAY_URL", "https://gw.example.com/mcp")
        monkeypatch.setattr(client, "_record_tool", lambda *a, **k: None)
        monkeypatch.setattr(client, "_record_policy", lambda *a, **k: None)
        self.published = published

        async def listing():
            return [t for ts in self.published.values() for t in ts]
        monkeypatch.setattr(client, "_gateway_tools", listing)
        # One model script per agent; a turn is answered by its index, so a turn that is
        # replayed from the journal instead of made again leaves the count where it was.
        self.scripts = scripts or {agent_ids[0]: script}
        self.turns: dict[str, list] = {a: [] for a in agent_ids}
        self.offered: dict[str, list] = {a: [] for a in agent_ids}
        self.prompts: list[str] = []

        async def fake_turn(name, system, messages, tools, **kw):
            aid = name.split(".")[0]
            self.turns[aid].append(list(messages))
            self.offered[aid].append([t["name"] for t in tools])
            return self.scripts[aid][len(self.turns[aid]) - 1]
        monkeypatch.setattr(llm, "run_llm_tools", fake_turn)

        async def fake_llm(name, system, user, **kw):
            self.prompts.append(user)
            return f"answer from {name}", False
        monkeypatch.setattr(context, "run_llm", fake_llm)
        self.flow: list[tuple] = []
        monkeypatch.setattr(meter, "record_person",
                            lambda **kw: self.flow.append((kw["label"], kw["status"])))

        def load_agents():
            out = {}
            for aid in agent_ids:
                spec = config.AGENTS[aid]
                agent = base.Agent()
                agent.id, agent.name, agent.agentcore = aid, spec.get("name", aid), {}
                agent.tools = config.tool_list(spec.get("tool"))
                agent.tool = agent.tools[0] if agent.tools else None
                agent.tool_mode = spec.get("toolMode", "direct")

                async def run(ctx):
                    if plan_first:
                        # A model call of the agent's own, before its tool turn.
                        await ctx.llm("Plan.", "What should be looked up?", name="plan")
                    found, gaps = await evidence.gather(ctx)
                    return await ctx.llm("You triage claims.", f"EVIDENCE:\n{found}\nGAPS:\n" + "\n".join(gaps))
                agent.run = run
                out[aid] = agent
            return out
        gb.load_agents = load_agents
        self.bus.create(SESSION, "Refund claim 7")
        self.graph = gb.build_graph()
        self.cfg = {"configurable": {"thread_id": SESSION}}

    async def start(self, **state):
        return await self.graph.ainvoke({"topic": "Refund claim 7", "status": {}, "outputs": {},
                                         **state}, self.cfg)

    async def decide(self, decision, comment=""):
        cmd = await self.asks.resume_command(self.graph, self.cfg, SESSION,
                                             {"decision": decision, "comment": comment, "decisions": None})
        out = await self.graph.ainvoke(cmd, self.cfg)
        state = await self.graph.aget_state(self.cfg)
        if state.next:
            await self.asks.show_next(SESSION, state)
        return out

    def snap(self) -> dict:
        return self.bus.snapshot(SESSION)

    def logs(self) -> list[str]:
        return [line["msg"] for line in self.snap()["logs"]]


def published():
    return {"lookup": [FakeTool("lookup___find")], "payments": [FakeTool("payments___pay", answer="paid")]}


# ---------------------------------------------------------------------------
# A question
# ---------------------------------------------------------------------------

def test_a_question_pauses_and_the_run_resumes_with_the_answer_and_nothing_repeated(monkeypatch):
    with workflow(wf()) as imp:
        tools = published()
        script = [ai(("lookup___find", {"q": "7"})),
                  ai(("ask_person", {"question": "Which region did the claimant mean?"})),
                  ai(text="enough")]
        r = Run(imp, monkeypatch, script, tools)

        async def go():
            first = await r.start()
            assert "__interrupt__" in first
            hitl = r.snap()["hitl"]
            assert (hitl["kind"], hitl["question"], hitl["node"]) == (
                "question", "Which region did the claimant mean?", "triage")
            assert r.snap()["overall"] == "waiting_human"
            assert r.snap()["nodes"]["triage"]["status"] == "waiting_human"
            # What happened before the question: two model turns, one tool call, no answer yet.
            assert (len(r.turns["triage"]), len(tools["lookup"][0].seen), len(r.prompts)) == (2, 1, 0)
            await r.decide("approve", "eu-west-1, the Dublin office")
        asyncio.run(go())
    # ONE more model turn on resume, and the tool was not called again.
    assert len(r.turns["triage"]) == 3
    assert len(tools["lookup"][0].seen) == 1
    told = r.turns["triage"][2][-1].content
    assert told.startswith("A PERSON ANSWERED") and "untrusted" in told
    assert "eu-west-1, the Dublin office" in told
    # The agent finished with that, and the run is no longer waiting.
    assert len(r.prompts) == 1 and "burst pipe" in r.prompts[0]
    assert r.snap()["hitl"] is None
    assert r.snap()["nodes"]["triage"]["status"] == "done"
    logs = r.logs()
    # Each line once: a replayed call is not logged again.
    assert sum(m.startswith("Tool call 1/6: lookup___find") for m in logs) == 1
    assert logs.count('Triage asks a person: "Which region did the claimant mean?"') == 1
    assert logs.count('A person answered Triage: "eu-west-1, the Dublin office"') == 1
    assert sum("started" in m for m in logs) >= 1
    assert r.flow == [("question", "asked"), ("question", "answered")]


def test_the_agents_own_model_call_before_the_question_is_not_billed_again(monkeypatch):
    with workflow(wf()) as imp:
        r = Run(imp, monkeypatch, [ai(("ask_person", {"question": "Which claim?"})), ai(text="ok")],
                published(), plan_first=True)

        async def go():
            await r.start()
            assert r.prompts == ["What should be looked up?"]
            await r.decide("approve", "7")
        asyncio.run(go())
    # The plan call once, then the final call: two in all, not three.
    assert len(r.prompts) == 2 and r.prompts[0] == "What should be looked up?"
    assert len(r.turns["triage"]) == 2


def test_an_agent_that_cannot_ask_journals_nothing(monkeypatch):
    with workflow(wf({"askPerson": False, "tool": ["lookup"]})) as imp:
        asks = imp("app.common.asks")
        ctx = types.SimpleNamespace(agent_id="triage", tools=["lookup"])
        assert asks.journaling(ctx) is False
        ran = []

        async def fn():
            ran.append(1)
            return ("text", "gateway")      # a tuple stays a tuple: no journal, no conversion
        assert asyncio.run(asks.once(ctx, fn)) == ("text", "gateway") and ran == [1]


def test_two_questions_each_resume_replays_everything_before_it(monkeypatch):
    with workflow(wf()) as imp:
        tools = published()
        script = [ai(("ask_person", {"question": "Which claim?"})),
                  ai(("lookup___find", {"q": "7"})),
                  ai(("ask_person", {"question": "Refund in full?"})),
                  ai(text="done")]
        r = Run(imp, monkeypatch, script, tools)

        async def go():
            await r.start()
            assert len(r.turns["triage"]) == 1
            await r.decide("approve", "claim 7")
            assert r.snap()["hitl"]["question"] == "Refund in full?"
            assert (len(r.turns["triage"]), len(tools["lookup"][0].seen)) == (3, 1)
            await r.decide("approve", "yes")
        asyncio.run(go())
    assert (len(r.turns["triage"]), len(tools["lookup"][0].seen), len(r.prompts)) == (4, 1, 1)
    assert r.logs().count('Triage asks a person: "Which claim?"') == 1


def test_a_question_then_a_tool_approval_in_one_step_waits_twice(monkeypatch):
    """Seen live: answered, the agent then wanted an approval-gated tool, and the run ended
    'done' at once with the agent still waiting and no output. A second wait in the same
    step, after a resume, must pause the run again and finish only once it is decided."""
    with workflow(wf()) as imp:
        tools = published()
        r = Run(imp, monkeypatch, [ai(("ask_person", {"question": "Which claim?"})),
                                   ai(("payments___pay", {"amount": 40})), ai(text="paid claim 7")], tools)

        async def go():
            await r.start()
            assert r.snap()["hitl"]["kind"] == "question"
            await r.decide("approve", "Claim 7")
            snap = r.snap()
            assert snap["overall"] == "waiting_human", snap["overall"]
            assert (snap["hitl"] or {}).get("kind") == "toolApproval"
            assert tools["payments"][0].seen == []
            # What runtime._finalize reads to decide the run is over: it must still be waiting,
            # although LangGraph's state.next is empty here (the cause of the live bug).
            state = await r.graph.aget_state(r.cfg)
            assert r.asks.is_waiting(state), "the run would be finished while it waits"
            await r.decide("approve")
            assert not r.asks.is_waiting(await r.graph.aget_state(r.cfg))
        asyncio.run(go())
    assert tools["payments"][0].seen == [{"amount": 40, "currency": "EUR"}]
    assert r.snap()["nodes"]["triage"]["status"] == "done"
    assert len(r.turns["triage"]) == 3


def test_finalize_does_not_end_a_run_that_waits_again_after_a_resume(monkeypatch):
    """runtime._finalize itself, on the state the live bug left: no 'Workflow finished'."""
    monkeypatch.setenv("MEMORY_ID", "mem-test")      # the runtime module needs one to import (test_liveconfig.py)
    with workflow(wf()) as imp:
        tools = published()
        r = Run(imp, monkeypatch, [ai(("ask_person", {"question": "Which claim?"})),
                                   ai(("payments___pay", {"amount": 40})), ai(text="paid")], tools)
        runtime = imp("app.orchestrator.runtime")
        monkeypatch.setattr(runtime, "graph", r.graph)
        monkeypatch.setattr(runtime, "_config", lambda sid: r.cfg)

        async def go():
            await r.start()
            from langgraph.types import Command
            invoked = await r.graph.ainvoke(Command(resume={"decision": "approve", "comment": "Claim 7"}), r.cfg)
            await runtime._finalize("ask-test", invoked)
            assert r.snap()["overall"] == "waiting_human"
            await runtime._finalize("ask-test")           # without the invoke result too
            assert r.snap()["overall"] == "waiting_human"
            assert not any(m.startswith("Workflow finished") for m in r.logs())
        asyncio.run(go())


def test_a_declined_question_is_told_as_unanswered(monkeypatch):
    with workflow(wf()) as imp:
        r = Run(imp, monkeypatch, [ai(("ask_person", {"question": "Which claim?"})), ai(text="ok")],
                published())

        async def go():
            await r.start()
            await r.decide("deny", "")
        asyncio.run(go())
    assert r.turns["triage"][1][-1].content.startswith("The person did not answer")
    assert any(m.startswith("Triage's question was not answered") for m in r.logs())


def test_the_question_cap_holds(monkeypatch):
    with workflow(wf({"askPerson": {"maxQuestions": 1}})) as imp:
        script = [ai(("ask_person", {"question": "First?"})),
                  ai(("ask_person", {"question": "Second?"})),
                  ai(text="ok")]
        r = Run(imp, monkeypatch, script, published())

        async def go():
            await r.start()
            await r.decide("approve", "one")
            assert r.snap()["hitl"] is None  # the second was not asked
        asyncio.run(go())
    assert r.turns["triage"][2][-1].content.startswith("Not asked: this step may ask a person at most 1")
    assert not any("Second?" in m for m in r.logs())


def test_a_long_question_is_cut_and_an_answer_is_capped(monkeypatch):
    with workflow(wf()) as imp:
        r = Run(imp, monkeypatch, [ai(("ask_person", {"question": "Q" * 900})), ai(text="ok")], published())

        async def go():
            await r.start()
            assert len(r.snap()["hitl"]["question"]) == r.asks.QUESTION_CHARS
            await r.decide("approve", "A" * 9000)
        asyncio.run(go())
    told = r.turns["triage"][1][-1].content
    assert told.split("<<<\n", 1)[1].split("\n>>>", 1)[0] == "A" * r.asks.ANSWER_CHARS


def test_an_agent_with_no_tool_can_still_ask(monkeypatch):
    with workflow(wf({"tool": None})) as imp:
        r = Run(imp, monkeypatch, [ai(("ask_person", {"question": "Which claim?"})), ai(text="ok")], {})

        async def go():
            await r.start()
            assert r.snap()["hitl"]["kind"] == "question"
            await r.decide("approve", "7")
        asyncio.run(go())
    assert r.offered["triage"][0] == ["ask_person"]
    assert len(r.prompts) == 1


# ---------------------------------------------------------------------------
# A tool call that waits for approval
# ---------------------------------------------------------------------------

def test_an_approved_call_runs_once_with_the_arguments_shown(monkeypatch):
    with workflow(wf()) as imp:
        tools = published()
        r = Run(imp, monkeypatch, [ai(("payments___pay", {"amount": 40})), ai(text="paid")], tools)

        async def go():
            await r.start()
            hitl = r.snap()["hitl"]
            assert (hitl["kind"], hitl["tool"]) == ("toolApproval", "payments___pay")
            # The arguments the Gateway will be sent, fixed `args` included.
            assert json.loads(hitl["args"]) == {"amount": 40, "currency": "EUR"}
            assert tools["payments"][0].seen == []          # not before a person approves
            await r.decide("approve")
        asyncio.run(go())
    assert tools["payments"][0].seen == [{"amount": 40, "currency": "EUR"}]
    assert len(r.turns["triage"]) == 2
    assert any(m.startswith("A person approved Triage's call to payments___pay") for m in r.logs())
    assert r.flow == [("payments___pay", "waiting"), ("payments___pay", "approved")]


def test_a_denied_call_is_never_made_and_the_model_carries_on(monkeypatch):
    with workflow(wf()) as imp:
        tools = published()
        r = Run(imp, monkeypatch, [ai(("payments___pay", {"amount": 4000})), ai(text="not paid")], tools)

        async def go():
            await r.start()
            await r.decide("deny", "too much without a manager")
        asyncio.run(go())
    assert tools["payments"][0].seen == []
    told = r.turns["triage"][1][-1].content
    assert told.startswith("REFUSED by a person") and "too much without a manager" in told
    assert "'payments___pay' was not called" in r.prompts[0]      # a limitation the agent reports
    assert r.snap()["nodes"]["triage"]["status"] == "done"


def test_a_tool_the_policy_hides_never_reaches_a_person(monkeypatch):
    """The Gateway hides a tool a forbid always blocks, so it is not offered, and a call
    the model invents anyway is refused as unknown — nobody is asked."""
    with workflow(wf()) as imp:
        tools = {"lookup": [FakeTool("lookup___find")]}          # payments hidden by policy
        r = Run(imp, monkeypatch, [ai(("payments___pay", {"amount": 1})), ai(text="ok")], tools)
        asyncio.run(r.start())
    assert "payments___pay" not in r.offered["triage"][0]
    assert r.turns["triage"][1][-1].content.startswith("There is no tool named payments___pay")
    assert r.snap()["hitl"] is None
    assert not any("wants to call" in m for m in r.logs())


def test_a_conditional_policy_refusal_after_approval_is_told_as_policy(monkeypatch):
    with workflow(wf()) as imp:
        tools = {"payments": [FakeTool("payments___pay", error="AccessDeniedException: not authorized by policy")],
                 "lookup": [FakeTool("lookup___find")]}
        r = Run(imp, monkeypatch, [ai(("payments___pay", {"amount": 1})), ai(text="ok")], tools)

        async def go():
            await r.start()
            await r.decide("approve")
        asyncio.run(go())
    assert r.turns["triage"][1][-1].content.startswith("REFUSED by the policy")


def test_direct_mode_asks_before_the_call_and_a_refusal_is_a_limitation(monkeypatch):
    with workflow(wf({"toolMode": "direct", "askPerson": False, "tool": ["payments"]})) as imp:
        tools = published()
        r = Run(imp, monkeypatch, [], tools)

        async def go():
            await r.start()
            hitl = r.snap()["hitl"]
            assert hitl["kind"] == "toolApproval" and hitl["tool"] == "payments___pay"
            assert tools["payments"][0].seen == []
            await r.decide("deny", "no")
        asyncio.run(go())
    assert tools["payments"][0].seen == []
    assert "'payments' was not called: REFUSED by a person" in r.prompts[0]


def test_direct_mode_does_not_ask_about_a_tool_the_gateway_does_not_list(monkeypatch):
    with workflow(wf({"toolMode": "direct", "askPerson": False, "tool": ["payments"]})) as imp:
        r = Run(imp, monkeypatch, [], {})
        with pytest.raises(imp("app.common.errors").ToolUnavailable, match="publishes no 'payments' tool"):
            asyncio.run(r.start())
    assert r.snap()["hitl"] is None
    assert r.snap()["nodes"]["triage"]["status"] == "failed"     # the call fails as it always has


# ---------------------------------------------------------------------------
# Unattended runs, timeouts, two agents waiting at once
# ---------------------------------------------------------------------------

def test_an_unattended_run_never_waits(monkeypatch):
    with workflow(wf()) as imp:
        tools = published()
        script = [ai(("ask_person", {"question": "Which claim?"}), ("payments___pay", {"amount": 1})),
                  ai(text="ok")]
        r = Run(imp, monkeypatch, script, tools)
        out = asyncio.run(r.start(gates="auto"))
    assert "__interrupt__" not in out
    results = [m.content for m in r.turns["triage"][1][-2:]]
    assert results[0].startswith("No person is available: this run was started unattended")
    assert results[1].startswith("REFUSED: this tool needs a person's approval") and "unattended" in results[1]
    assert tools["payments"][0].seen == []
    assert r.snap()["hitl"] is None and len(r.prompts) == 1


def test_a_question_with_a_timeout_is_decided_by_the_sweep(monkeypatch):
    with workflow(wf({"askPerson": {"timeout": "30m"}})) as imp:
        r = Run(imp, monkeypatch, [ai(("ask_person", {"question": "Which claim?"})), ai(text="ok")],
                published())
        asyncio.run(r.start())
        hitl = r.snap()["hitl"]
        assert hitl["timeoutAction"] == "deny" and hitl["timeoutAt"]
        sys.path.insert(0, str(__import__("conftest").ORCH_ROOT / "bff"))
        sys.modules.pop("gates", None)
        bff_gates = importlib.import_module("gates")

        class Table:
            def __init__(self):
                self.item = {"session_id": SESSION, "overall": "waiting_human",
                             "hitl": {**hitl, "timeoutAt": "2000-01-01T00:00:00Z"}}

            def scan(self, **kw):
                return {"Items": [self.item]}

            def update_item(self, **kw):
                # The claim is conditional, as DynamoDB applies it: once only.
                if "timeoutAt" not in self.item["hitl"] or self.item["overall"] != "waiting_human":
                    raise RuntimeError("ConditionalCheckFailedException")
                del self.item["hitl"]["timeoutAt"]
        resumed = []
        table = Table()
        bff_gates.sweep(table, lambda sid, decision, comment, by: resumed.append((sid, decision, by)))
        bff_gates.sweep(table, lambda sid, decision, comment, by: resumed.append((sid, decision, by)))
        assert resumed == [(SESSION, "deny", "timeout")]
        asyncio.run(r.decide("deny", "No decision by 2000-01-01T00:00:00Z: denyd by the gate's timeout."))
        sys.modules.pop("gates", None)
    assert r.turns["triage"][1][-1].content.startswith("The person did not answer")


def test_two_agents_of_a_stage_each_wait_and_each_answer_reaches_its_own(monkeypatch):
    agents = {a: {"name": a.title(), "runtime": "main", "maxTokens": 400, "toolMode": "model",
                  "askPerson": True} for a in ("left", "right")}
    with workflow(wf(agents=agents, steps=[{"parallel": ["left", "right"]}])) as imp:
        scripts = {"left": [ai(("ask_person", {"question": "Left?"})), ai(text="l")],
                   "right": [ai(("ask_person", {"question": "Right?"})), ai(text="r")]}
        r = Run(imp, monkeypatch, None, {}, agent_ids=("left", "right"), scripts=scripts)

        async def go():
            await r.start()
            first = r.snap()["hitl"]
            await r.decide("approve", f"answer for {first['node']}")
            second = r.snap()["hitl"]
            assert second and second["node"] != first["node"]       # the other one is shown
            await r.decide("approve", f"answer for {second['node']}")
            return first["node"], second["node"]
        a, b = asyncio.run(go())
    assert f"answer for {a}" in r.turns[a][1][-1].content
    assert f"answer for {b}" in r.turns[b][1][-1].content
    assert r.snap()["hitl"] is None and len(r.prompts) == 2
    # Each agent's first turn was made once.
    assert len(r.turns[a]) == 2 and len(r.turns[b]) == 2


def test_with_nothing_that_asks_resume_is_exactly_as_before(monkeypatch):
    with workflow({"orchestrator": {}, "tools": {}, "agents": {"a": {"name": "A", "maxTokens": 1}},
                   "steps": [{"agent": "a", "hitl": True}]}) as imp:
        asks = imp("app.common.asks")
        assert asks.ENABLED is False

        class NoState:
            async def aget_state(self, cfg):
                raise AssertionError("read the checkpoint for a workflow that cannot ask")
        cmd = asyncio.run(asks.resume_command(NoState(), {}, "s", {"decision": "approve"}))
        assert cmd.resume == {"decision": "approve"}
        asyncio.run(asks.show_next("s", NoState()))   # nothing read, nothing shown


def test_a_gate_still_resumes_with_a_plain_command(monkeypatch):
    """One interrupt pending (every gate) is resumed by value, as it always was."""
    with workflow(wf()) as imp:
        asks = imp("app.common.asks")

        class OneWaiting:
            async def aget_state(self, cfg):
                return types.SimpleNamespace(tasks=[types.SimpleNamespace(
                    result=None, interrupts=[types.SimpleNamespace(id="i1", value={"node": "triage"})])])
        cmd = asyncio.run(asks.resume_command(OneWaiting(), {}, "s", {"decision": "approve"}))
        assert cmd.resume == {"decision": "approve"}


# ---------------------------------------------------------------------------
# The status store
# ---------------------------------------------------------------------------

class CondTable:
    """The one condition the sink writes, applied as DynamoDB applies it."""

    def __init__(self, hitl):
        self.item = {"session_id": SESSION, "overall": "waiting_human", "hitl": hitl}
        self.calls = []

    def update_item(self, **kw):
        self.calls.append(kw)
        if kw.get("ConditionExpression") == "hitl.askId = :a":
            if (self.item.get("hitl") or {}).get("askId") != kw["ExpressionAttributeValues"][":a"]:
                raise RuntimeError("ConditionalCheckFailedException")
        if "hitl = :z" in kw["UpdateExpression"]:
            self.item["hitl"] = None
            self.item["overall"] = kw["ExpressionAttributeValues"][":ov"]
        return {"Attributes": {}}

    def put_item(self, **kw):
        return {}


def test_one_answer_never_clears_another_agents_question(monkeypatch):
    with workflow(wf()) as imp:
        sink = imp("app.common.sink")
        table = CondTable({"node": "right", "askId": "right.v1.q1", "kind": "question"})
        monkeypatch.setattr(sink, "STATUS_TABLE", "t")
        monkeypatch.setattr(sink, "EVENTS_TABLE", None)
        monkeypatch.setattr(sink, "_table", lambda name: table)
        sink._write_ddb(SESSION, {"type": "ask_resolved", "node": "left", "askId": "left.v1.q1"})
        assert table.item["hitl"]["node"] == "right"
        sink._write_ddb(SESSION, {"type": "ask_resolved", "node": "right", "askId": "right.v1.q1"})
        assert table.item["hitl"] is None and table.item["overall"] == "running"


def test_the_waiting_run_carries_the_kind_and_the_call(monkeypatch):
    with workflow(wf()) as imp:
        sink = imp("app.common.sink")
        seen = []

        class T(CondTable):
            def update_item(self, **kw):
                seen.append(kw)
                return {"Attributes": {"overall": "waiting_human"}}
        monkeypatch.setattr(sink, "STATUS_TABLE", "t")
        monkeypatch.setattr(sink, "EVENTS_TABLE", None)
        monkeypatch.setattr(sink, "_table", lambda name: T({}))
        sink._write_ddb(SESSION, {"type": "hitl_request", "node": "triage", "kind": "toolApproval",
                                  "askId": "triage.v1.t1", "tool": "payments___pay", "args": "{}",
                                  "question": "Approve?", "approval": "console"})
        hitl = seen[0]["ExpressionAttributeValues"][":h"]
        assert hitl == {"node": "triage", "question": "Approve?", "approval": "console",
                        "kind": "toolApproval", "askId": "triage.v1.t1", "tool": "payments___pay",
                        "args": "{}"}
        # A gate's request is unchanged: no kind, nothing extra.
        seen.clear()
        sink._write_ddb(SESSION, {"type": "hitl_request", "node": "triage", "question": "Review"})
        assert seen[0]["ExpressionAttributeValues"][":h"] == {"node": "triage", "question": "Review"}


# ---------------------------------------------------------------------------
# The BFF: the decision route, events, the assistant
# ---------------------------------------------------------------------------

BFF_WF = {**wf(), "authorization": {"actions": {"decision": ["approvers"]}}}


class FakeTable:
    def __init__(self, item=None):
        self.item = item

    def get_item(self, **kw):
        return {"Item": dict(self.item)} if self.item else {}

    def update_item(self, **kw):
        return {}

    def put_item(self, **kw):
        return {}

    def query(self, **kw):
        return {"Items": []}


@pytest.fixture()
def bff(monkeypatch):
    monkeypatch.setenv("WORKFLOW_JSON", json.dumps(BFF_WF))
    monkeypatch.setenv("STATUS_TABLE", "t-status")
    monkeypatch.setenv("EVENTS_TABLE", "t-events")
    monkeypatch.setenv("RUNTIME_ARN", "arn:aws:bedrock-agentcore:us-east-1:1:runtime/x")
    for mod in ("workflow", "authz", "chatbot", "handler", "asks"):
        sys.modules.pop(mod, None)
    handler = importlib.import_module("handler")
    invoked: list[dict] = []
    audited: list[tuple] = []
    monkeypatch.setattr(handler, "events_tbl", FakeTable())
    monkeypatch.setattr(handler, "_self_invoke", lambda fn, payload: invoked.append(payload))
    monkeypatch.setattr(handler, "_audit_run", lambda event, action, sid, **d: audited.append((action, d)))
    handler._test_invoked, handler._test_audited = invoked, audited

    def waiting(hitl):
        monkeypatch.setattr(handler, "status_tbl", FakeTable(
            {"session_id": "s1", "overall": "waiting_human", "user": "u@example.com",
             "owner": "user@example.com", "hitl": hitl}))
    handler._test_waiting = waiting
    yield handler
    for mod in ("workflow", "authz", "chatbot", "handler", "asks"):
        sys.modules.pop(mod, None)


def decide(bff, body, groups=("approvers",)):
    claims = {"email": "user@example.com", "cognito:groups": list(groups)}
    event = {"requestContext": {"http": {"method": "POST"}, "authorizer": {"jwt": {"claims": claims}}},
             "rawPath": "/api/sessions/s1/decision", "routeKey": "POST /api/sessions/{id}/decision",
             "pathParameters": {"id": "s1"}, "body": json.dumps(body)}
    resp = bff._api(event, types.SimpleNamespace(function_name="bff-fn"))
    return resp["statusCode"], json.loads(resp["body"])


QUESTION = {"node": "triage", "question": "Which region?", "kind": "question", "askId": "triage.v1.q1"}
APPROVAL = {"node": "triage", "question": "Approve?", "kind": "toolApproval", "askId": "triage.v1.t1",
            "tool": "payments___pay", "args": "{}"}


def test_an_answer_goes_through_the_decision_route(bff):
    bff._test_waiting(QUESTION)
    status, _ = decide(bff, {"decision": "approve", "comment": "eu-west-1"})
    assert status == 200
    sent = bff._test_invoked[-1]
    assert (sent["action"], sent["decision"], sent["comment"]) == ("resume", "approve", "eu-west-1")
    assert bff._test_audited[-1][0] == "run.answered"
    assert bff._test_audited[-1][1]["kind"] == "question"


@pytest.mark.parametrize("hitl,body,why", [
    (QUESTION, {"decision": "approve", "comment": "   "}, "an answer needs text"),
    (QUESTION, {"decision": "revise", "comment": "x"}, "approve or deny"),
    (QUESTION, {"decision": "approve", "comment": "x" * 4001}, "at most 4000"),
    (QUESTION, {"decisions": {"triage": {"decision": "approve"}}}, "not at a parallel gate"),
    (APPROVAL, {"decision": "revise", "comment": "x"}, "approve or deny"),
    (APPROVAL, {"decision": "approve", "comment": 5}, "must be text"),
])
def test_what_cannot_answer_is_refused_and_nothing_resumes(bff, hitl, body, why):
    bff._test_waiting(hitl)
    status, resp = decide(bff, body)
    assert status == 400 and why in resp["error"]
    assert bff._test_invoked == []


class ClaimTable(FakeTable):
    """The status row, with the decision claim's condition checked as DynamoDB checks it."""

    def update_item(self, **kw):
        v = kw.get("ExpressionAttributeValues") or {}
        if kw.get("ConditionExpression"):
            hitl = self.item.get("hitl") or {}
            ok = (self.item.get("overall") == v.get(":w") and "decidedAt" not in hitl
                  and (":a" not in v or hitl.get("askId") == v[":a"]))
            if not ok:
                from botocore.exceptions import ClientError
                raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "UpdateItem")
            self.item["hitl"] = {**hitl, "decidedAt": v[":t"]}
        return {}


def _claimable(bff, monkeypatch, hitl, overall="waiting_human"):
    tbl = ClaimTable({"session_id": "s1", "overall": overall, "user": "u@example.com",
                      "owner": "user@example.com", "hitl": dict(hitl) if hitl else None})
    monkeypatch.setattr(bff, "status_tbl", tbl)
    return tbl


@pytest.mark.parametrize("hitl", [APPROVAL, QUESTION, {"node": "review", "question": "Review"}],
                         ids=["toolApproval", "question", "gate"])
def test_a_second_decision_for_the_same_pause_is_refused_and_resumes_nothing(bff, monkeypatch, hitl):
    """Seen live: a second Approve a second after the first resumed the run twice, and the
    approved tool call ran twice. Each pause is decided once."""
    _claimable(bff, monkeypatch, hitl)
    body = {"decision": "approve", "comment": "Claim 7"}
    assert decide(bff, body)[0] == 200
    status, resp = decide(bff, body)
    assert status == 409 and "already decided" in resp["error"]
    assert len(bff._test_invoked) == 1
    assert len([a for a in bff._test_audited if a[0].startswith("run.")]) == 1   # logged once


def test_the_next_pause_can_be_decided_again(bff, monkeypatch):
    """The runtime writes a new hitl (without the claim) when the run waits again."""
    tbl = _claimable(bff, monkeypatch, QUESTION)
    assert decide(bff, {"decision": "approve", "comment": "Claim 7"})[0] == 200
    tbl.item["hitl"] = dict(APPROVAL)                        # the agent now waits for an approval
    assert decide(bff, {"decision": "approve"})[0] == 200
    assert len(bff._test_invoked) == 2


def test_a_decision_when_the_run_is_not_waiting_resumes_nothing(bff, monkeypatch):
    _claimable(bff, monkeypatch, None, overall="running")
    assert decide(bff, {"decision": "approve"})[0] == 409
    assert bff._test_invoked == []


def test_an_event_and_a_person_deciding_the_same_pause_resume_once(bff, monkeypatch):
    _claimable(bff, monkeypatch, {"node": "review", "question": "Review"})
    resume = bff._gate_resume(types.SimpleNamespace(function_name="bff-fn"))
    resume("s1", "approve", "", "event:servicenow")
    assert decide(bff, {"decision": "deny"})[0] == 409
    resume("s1", "approve", "", "timeout")
    assert len(bff._test_invoked) == 1


def test_the_claim_against_dynamodb_itself(bff):
    """_claim_decision's condition, evaluated by DynamoDB (moto), not by a fake that
    re-implements it: the expression the code sends is what is tested."""
    moto = pytest.importorskip("moto")
    import boto3
    with moto.mock_aws():
        ddb = boto3.resource("dynamodb", region_name="us-east-1")
        tbl = ddb.create_table(TableName="st", KeySchema=[{"AttributeName": "session_id", "KeyType": "HASH"}],
                               AttributeDefinitions=[{"AttributeName": "session_id", "AttributeType": "S"}],
                               BillingMode="PAY_PER_REQUEST")
        def put(overall, hitl):
            tbl.put_item(Item={"session_id": "s1", "overall": overall, "hitl": hitl})
        claim = bff._claim_decision
        put("waiting_human", dict(APPROVAL))
        assert claim(tbl, "s1", APPROVAL) is True
        assert claim(tbl, "s1", APPROVAL) is False                       # the second click
        put("waiting_human", dict(APPROVAL))
        assert claim(tbl, "s1", {**APPROVAL, "askId": "triage.v1.t9"}) is False   # meant for another ask
        put("running", None)
        assert claim(tbl, "s1", None) is False                           # not waiting
        put("waiting_human", {"node": "review", "question": "Review"})
        assert claim(tbl, "s1", {"node": "review"}) is True              # a gate: no askId
        assert claim(tbl, "s1", {"node": "review"}) is False
        put("waiting_human", dict(QUESTION))                             # the runtime waits again
        assert claim(tbl, "s1", QUESTION) is True
        with pytest.raises(Exception):                                   # anything else is not "taken"
            claim(ddb.Table("missing"), "s1", QUESTION)


def test_a_tool_decision_is_logged_as_one(bff):
    bff._test_waiting(APPROVAL)
    assert decide(bff, {"decision": "deny", "comment": "no"})[0] == 200
    assert bff._test_audited[-1][0] == "run.tool_decided"
    assert bff._test_audited[-1][1]["tool"] == "payments___pay"


def test_the_permission_is_checked_before_anything_is_read(bff):
    bff._test_waiting(QUESTION)
    status, _ = decide(bff, {"decision": "approve", "comment": "x"}, groups=("interns",))
    assert status == 403 and bff._test_invoked == []


@pytest.mark.parametrize("body", [
    {"decision": "approve"}, {"decision": "deny"}, {"decision": "revise", "comment": "again"},
    {"decision": "approve", "comment": "x" * 5000},
])
def test_a_gate_takes_every_payload_it_took_before(bff, body):
    bff._test_waiting({"node": "triage", "question": "Review triage"})
    status, _ = decide(bff, body)
    assert status == 200
    assert bff._test_invoked[-1]["decision"] == body["decision"]
    assert bff._test_audited[-1][0] == "run.decided"


def test_an_event_decides_only_a_tool_call_that_takes_events(monkeypatch):
    monkeypatch.setenv("WORKFLOW_JSON", json.dumps(BFF_WF))
    monkeypatch.setenv("APP_NAME", "")
    for mod in ("workflow", "gates"):
        sys.modules.pop(mod, None)
    gates = importlib.import_module("gates")
    try:
        def table(hitl):
            return FakeTable({"session_id": "s1", "overall": "waiting_human", "hitl": hitl})
        resumed = []

        def resume(*a):
            resumed.append(a)
        detail = {"session": "s1", "decision": "approve", "by": "slack"}
        assert gates.decide(detail, table(QUESTION), resume)["decided"] is False
        assert gates.decide(detail, table({**APPROVAL, "approval": "console"}), resume)["decided"] is False
        assert gates.decide({**detail, "decision": "revise"}, table({**APPROVAL, "approval": "event"}),
                            resume)["decided"] is False
        assert gates.decide(detail, table({**APPROVAL, "approval": "event"}), resume)["decided"] is True
        assert resumed == [("s1", "approve", "", "event:slack")]
    finally:
        for mod in ("workflow", "gates"):
            sys.modules.pop(mod, None)


def test_the_assistant_does_not_answer_for_a_person(bff, monkeypatch):
    monkeypatch.setattr(bff.chatbot, "_status", FakeTable(
        {"session_id": "s1", "overall": "waiting_human", "hitl": APPROVAL}))
    calls = []
    out, said = bff.chatbot._t_review({"session_id": "s1", "decision": "approve"}, {}, calls.append)
    assert "run page" in out["error"] and calls == [] and said is None


def test_the_bff_and_the_runtime_share_their_limits():
    sys.modules.pop("asks", None)
    bff_asks = importlib.import_module("asks")
    with workflow(wf()) as imp:
        app_asks = imp("app.common.asks")
        assert bff_asks.ANSWER_MAX == app_asks.ANSWER_CHARS
        assert set(bff_asks.APPROVALS) == set(app_asks.APPROVALS)
        assert bff_asks.KINDS == app_asks.KINDS
    assert bff_asks.enabled(wf()) and not bff_asks.enabled({"agents": {"a": {}}, "tools": {}})
    sys.modules.pop("asks", None)
