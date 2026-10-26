"""v139 Track D: operator decisions mid-run.

What has to hold:
  * a review gate with `hitl.options` waits with its options on the run, and the option a
    person chooses is the run field `decision` that the step's `branch.when` routes on;
  * a gate that approves itself, or an option that is not one of the gate's, chooses none:
    the branch takes its default;
  * an agent with `askPerson.options` can REQUEST a decision mid-step (app/common/asks.py):
    the run pauses with the options, the answer resumes it, the model reads the choice and a
    branch on the agent's step routes on it, with nothing before the request made again;
  * an existing approve / revise / deny gate is exactly as before: same state update, same
    request, same timeline line, same routing;
  * the decision route takes {option, comment}, refuses anything else at a choice, logs who
    chose what, and the Run Assistant does not choose;
  * both validators agree on the new keys (the cases are also in validation_cases.json).

The graph, its nodes, routers and the checkpointer are the real ones; agents are stubs or a
faked model, as in test_branching.py and test_asks.py.
"""
from __future__ import annotations

import asyncio
import sys

import pytest
from conftest import ORCH_ROOT, wf, workflow
from test_asks import FakeTable, Run, ai, decide, published
from test_asks import bff as bff  # the fixture, re-exported for these tests
from test_asks import wf as asks_wf
from test_branching import config, stub_graph

OPTIONS = ["Override", "Abort"]
#   check ─ gate (Override / Abort) ─┬─ decision Abort ────> END
#                                    ├─ decision Override ─> deliver
#                                    └─ (default) ────────> fix ─> deliver
GATED = [
    {"agent": "check", "hitl": {"options": OPTIONS}, "branch": {
        "when": [{"field": "decision", "equals": "Abort", "goto": "END"},
                 {"field": "decision", "equals": "Override", "goto": "deliver"}],
        "default": "fix"}},
    {"agent": "fix", "branch": {"default": "deliver"}},
    {"agent": "deliver"},
]
STUBS = {"check": '{"passed": false}', "fix": "{}", "deliver": "{}"}


def _run_to_gate(imp, outputs=STUBS, **state):
    graph, ran, snapshot = stub_graph(imp, outputs)

    async def start():
        return await graph.ainvoke({"topic": "a batch", "status": {}, "outputs": {}, **state}, config())
    first = asyncio.run(start())
    return graph, ran, snapshot, first


def _resume(graph, payload):
    from langgraph.types import Command

    async def go():
        await graph.ainvoke(Command(resume=payload), config())
        return (await graph.aget_state(config())).values
    return asyncio.run(go())


def _logs(snap) -> list[str]:
    return [line["msg"] for line in snap["logs"]]


# ---------------------------------------------------------------------------
# A gate with named options
# ---------------------------------------------------------------------------

def test_the_gate_waits_with_its_options_on_the_run():
    with workflow(wf(GATED)) as imp:
        _, ran, snapshot, first = _run_to_gate(imp)
        assert "__interrupt__" in first and ran == ["check"]
        hitl = snapshot()["hitl"]
        assert (hitl["node"], hitl["options"]) == ("check", OPTIONS)
        assert snapshot()["overall"] == "waiting_human"


@pytest.mark.parametrize("option,ran_after,skipped", [
    ("Override", ["check", "deliver"], ["fix"]),
    ("abort ", ["check"], ["fix", "deliver"]),          # labels compare like branch strings
])
def test_the_chosen_option_routes_the_run(option, ran_after, skipped):
    with workflow(wf(GATED)) as imp:
        graph, ran, snapshot, _ = _run_to_gate(imp)
        values = _resume(graph, {"decision": "approve", "option": option, "comment": "checked by hand"})
        assert ran == ran_after
        for a in skipped:
            assert snapshot()["nodes"][a]["status"] == "skipped"
        label = option.strip().title()
        assert values["choices"]["check_gate"] == {"option": label, "comment": "checked by hand"}
        logs = _logs(snapshot())
        assert f'Human decision (check): {label} \u2014 note: "checked by hand"' in logs
        assert any(m.startswith(f"Branch after check: decision equals '{label}'") for m in logs)


def test_the_choice_wins_over_a_field_of_the_same_name_in_the_output():
    with workflow(wf(GATED)) as imp:
        graph, ran, _, _ = _run_to_gate(imp, {**STUBS, "check": '{"decision": "Abort"}'})
        _resume(graph, {"decision": "approve", "option": "Override"})
        assert ran == ["check", "deliver"]


@pytest.mark.parametrize("payload", [
    {"decision": "approve", "option": "Ship it anyway"},   # not one of the options
    {"decision": "approve"},                              # a timeout's approve: no option
])
def test_no_option_chosen_takes_the_default(payload):
    with workflow(wf(GATED)) as imp:
        graph, ran, snapshot, _ = _run_to_gate(imp)
        values = _resume(graph, payload)
        assert ran == ["check", "fix", "deliver"]
        assert values["choices"]["check_gate"]["option"] == ""
        assert "Human decision (check): approve (no option chosen)" in _logs(snapshot())


def test_a_timeout_deny_stops_the_run_as_on_any_gate():
    with workflow(wf(GATED)) as imp:
        graph, ran, _, _ = _run_to_gate(imp)
        values = _resume(graph, {"decision": "deny", "comment": "No decision by the deadline"})
        assert ran == ["check"] and values["decisions"]["check"] == "deny"
        assert "check_gate" not in (values.get("choices") or {})


def test_an_unattended_run_chooses_nothing_and_takes_the_default():
    with workflow(wf(GATED)) as imp:
        _, ran, _, first = _run_to_gate(imp, gates="auto")
        assert "__interrupt__" not in first
        assert ran == ["check", "fix", "deliver"]


ONLY_ON_FAILURE = [
    {"agent": "check", "hitl": {"mode": "threshold", "when": [{"field": "passed", "equals": False}],
                                "options": OPTIONS},
     "branch": {"when": [{"field": "decision", "equals": "Abort", "goto": "END"}], "default": "deliver"}},
    {"agent": "escalate"},
    {"agent": "deliver"},
]


@pytest.mark.parametrize("check,option,ran_after", [
    ('{"passed": true}', None, ["check", "deliver"]),               # passed: nobody is asked
    ('{"passed": false}', "Override", ["check", "deliver"]),
    ('{"passed": false}', "Abort", ["check"]),
])
def test_ask_only_when_the_check_fails_then_deliver_or_stop(check, option, ran_after):
    """Deliver only if the check passed, otherwise the operator overrides or aborts."""
    outputs = {"check": check, "escalate": "{}", "deliver": "{}"}
    with workflow(wf(ONLY_ON_FAILURE)) as imp:
        graph, ran, _, first = _run_to_gate(imp, outputs)
        assert ("__interrupt__" in first) is (option is not None)
        if option:
            _resume(graph, {"decision": "approve", "option": option})
        assert ran == ran_after


def test_a_sequence_gate_offers_options_too():
    steps = [{"sequence": ["parse", "check"], "gateId": "verify", "gateName": "Verify",
              "hitl": {"options": OPTIONS},
              "branch": {"when": [{"field": "decision", "equals": "Abort", "goto": "END"}]}},
             {"agent": "deliver"}]
    with workflow(wf(steps)) as imp:
        graph, ran, snapshot, _ = _run_to_gate(imp, {"parse": "{}", "check": "{}", "deliver": "{}"})
        assert snapshot()["hitl"]["options"] == OPTIONS
        values = _resume(graph, {"decision": "approve", "option": "Abort"})
        assert ran == ["parse", "check"]
        assert values["choices"]["verify_gate"]["option"] == "Abort"
        assert "Verify review: Abort" in _logs(snapshot())


# ---------------------------------------------------------------------------
# Existing gates are unchanged
# ---------------------------------------------------------------------------

@pytest.fixture()
def gate_env(monkeypatch):
    from app.orchestrator import nodes
    emitted: list[dict] = []

    async def emit(sid, ev):
        emitted.append(ev)
    monkeypatch.setattr(nodes, "emit", emit)
    monkeypatch.setattr(nodes, "is_cancelled", lambda sid: False)
    seen: list = []

    def resume_with(payload):
        monkeypatch.setattr(nodes, "interrupt", lambda request: (seen.append(request), payload)[1])
    return nodes, emitted, resume_with, seen


CFG = {"configurable": {"thread_id": "s1"}}


@pytest.mark.parametrize("payload,update", [
    ({"decision": "approve", "comment": ""}, {"decisions": {"intake": "approve"}}),
    ({"decision": "approve", "option": "Override"}, {"decisions": {"intake": "approve"}}),
    ({"decision": "revise", "comment": "again"}, {"decisions": {"intake": "revise"}, "feedback": {"intake": "again"}}),
    ({"decision": "deny"}, {"decisions": {"intake": "deny"}, "status": {"intake": "denied"}}),
])
def test_a_plain_gate_returns_and_asks_exactly_as_before(gate_env, payload, update):
    nodes, emitted, resume_with, seen = gate_env
    resume_with(payload)
    got = asyncio.run(nodes.make_gate_node("intake", "Intake", True)({"outputs": {"intake": "{}"}}, CFG))
    assert got == update                                   # no `choices`: an option means nothing here
    request = next(e for e in emitted if e["type"] == "hitl_request")
    assert "options" not in request and request["approval"] == "console"
    assert seen == [{"node": "intake", "question": "Review Intake (intake) output: {}"}]


def test_a_plain_gate_routes_exactly_as_before():
    steps = [{"agent": "a", "hitl": True}, {"agent": "b"}]
    with workflow(wf(steps)) as imp:
        graph, ran, snapshot, _ = _run_to_gate(imp, {"a": "{}", "b": "{}"})
        assert "options" not in snapshot()["hitl"]
        values = _resume(graph, {"decision": "approve", "comment": ""})
        assert ran == ["a", "b"] and not values.get("choices")
        assert "Human decision (a): approve" in _logs(snapshot())


def test_a_branch_without_a_choice_reads_the_output_as_before():
    from app.common import branching
    spec = {"when": [{"field": "decision", "equals": "Abort", "goto": "END"}], "default": "next"}
    assert branching.choose(spec, '{"decision": "abort"}')[0] == "END"
    assert branching.choose(spec, '{"decision": "abort"}', None)[0] == "END"
    assert branching.choose(spec, '{"decision": "abort"}', {"decision": "Override"})[0] == "next"
    # A run field only answers for its own name; every other rule still reads the output.
    other = {"when": [{"field": "passed", "equals": True, "goto": "hit"}], "default": "miss"}
    assert branching.choose(other, '{"passed": true}', {"decision": "Override"})[0] == "hit"


# ---------------------------------------------------------------------------
# Runtime validation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("hitl,why", [
    ({"options": ["Only"]}, "2 to 6"),
    ({"options": ["A", "B", "C", "D", "E", "F", "G"]}, "2 to 6"),
    ({"options": ["Go", "go"]}, "must differ"),
    ({"options": ["Go", "x" * 61]}, "at most 60"),
    ({"options": ["Go", ""]}, "2 to 6"),
    ({"options": OPTIONS, "approval": "event"}, "an event cannot choose"),
])
def test_a_malformed_gate_is_refused_at_start(hitl, why):
    with workflow(wf(GATED)) as imp:
        gates = imp("app.common.gates")
        with pytest.raises(ValueError, match=why):
            gates.validate(hitl, "steps[0]")
        gates.validate({"options": OPTIONS, "timeout": {"after": "1h", "action": "approve"}}, "steps[0]")


def test_options_on_a_parallel_gate_fail_the_container_at_start():
    steps = [{"parallel": ["a", "b"], "gateId": "g", "hitl": {"options": OPTIONS}}, {"agent": "c"}]
    with workflow(wf(steps)) as imp, pytest.raises(ValueError, match="not supported on a `parallel` step"):
        imp("app.orchestrator.graph_builder").validate_branches()


def test_the_runtime_bounds_are_the_vocabularys():
    with workflow(wf(GATED)) as imp:
        gates = imp("app.common.gates")
        vocabulary = imp("app.common.vocabulary")
        field, bounds = gates.DECISION_FIELD, (gates.MIN_OPTIONS, gates.MAX_OPTIONS, gates.OPTION_CHARS)
        assert field == vocabulary.values("decisionOptions")[0]
        assert field == "decision"
        assert bounds == (vocabulary.field("decisionOptions", "minOptions"),
                          vocabulary.field("decisionOptions", "maxOptions"),
                          vocabulary.field("decisionOptions", "labelChars"))


# ---------------------------------------------------------------------------
# An agent requests a decision mid-step (asks.py)
# ---------------------------------------------------------------------------

DECIDES = {"askPerson": {"options": ["Load images", "Continue"]}, "tool": ["lookup"]}


def _decide_wf(steps=None):
    report = {"name": "Report", "runtime": "main", "maxTokens": 400, "toolMode": "model", "tool": ["lookup"]}
    base = asks_wf(DECIDES)
    return {**base, "agents": {**base["agents"], "report": report},
            "steps": steps or [{"agent": "triage", "branch": {"when": [
                {"field": "decision", "equals": "Load images", "goto": "END"}]}}, {"agent": "report"}]}


def _resume_ask(r, payload):
    async def go():
        cmd = await r.asks.resume_command(r.graph, r.cfg, "ask-test", payload)
        await r.graph.ainvoke(cmd, r.cfg)
        return (await r.graph.aget_state(r.cfg)).values
    return asyncio.run(go())


@pytest.mark.parametrize("option,report_runs", [("Continue", True), ("load images", False)])
def test_a_requested_decision_pauses_resumes_and_routes(monkeypatch, option, report_runs):
    with workflow(_decide_wf()) as imp:
        tools = published()
        script = [ai(("lookup___find", {"q": "7"})),
                  ai(("request_decision", {"question": "Two images are missing. Load them, or continue?"})),
                  ai(text="enough")]
        r = Run(imp, monkeypatch, script, tools, agent_ids=("triage", "report"),
                scripts={"triage": script, "report": [ai(text="report")]})
        first = asyncio.run(r.start())
        assert "__interrupt__" in first
        assert "request_decision" in r.offered["triage"][0] and "ask_person" in r.offered["triage"][0]
        hitl = r.snap()["hitl"]
        assert (hitl["kind"], hitl["options"], hitl["askId"]) == ("decision", ["Load images", "Continue"],
                                                                  "triage.v1.d1")
        assert hitl["question"] == "Two images are missing. Load them, or continue?"
        assert (len(r.turns["triage"]), len(tools["lookup"][0].seen)) == (2, 1)
        values = _resume_ask(r, {"decision": "approve", "option": option, "comment": "as agreed", "decisions": None})
    label = "Continue" if report_runs else "Load images"
    # Nothing before the request was made again, and the model read the choice.
    assert len(r.turns["triage"]) == 3 and len(tools["lookup"][0].seen) == 1
    told = r.turns["triage"][2][-1].content
    assert told.startswith(f'A PERSON CHOSE: "{label}"') and "as agreed" in told and "untrusted" in told
    assert values["choices"]["triage"] == {"option": label, "comment": "as agreed"}
    assert ("report" in values["outputs"]) is report_runs
    logs = r.logs()
    assert logs.count('Triage asks a person to choose: "Two images are missing. Load them, or continue?" '
                      "(Load images / Continue)") == 1
    assert f'A person chose "{label}" for Triage \u2014 comment: "as agreed"' in logs
    assert r.flow == [("decision", "waiting"), ("decision", "chosen")]


def test_an_unchosen_request_is_told_and_chooses_nothing(monkeypatch):
    with workflow(_decide_wf()) as imp:
        r = Run(imp, monkeypatch, [ai(("request_decision", {"question": "Load or continue?"})), ai(text="ok")],
                published(), agent_ids=("triage", "report"),
                scripts={"triage": [ai(("request_decision", {"question": "Load or continue?"})), ai(text="ok")],
                         "report": [ai(text="report")]})
        asyncio.run(r.start())
        values = _resume_ask(r, {"decision": "deny", "comment": "", "decisions": None})   # the sweep's timeout
    assert r.turns["triage"][1][-1].content.startswith("The person did not choose an option")
    assert values["choices"]["triage"] == {} and "report" in values["outputs"]


def test_an_unattended_run_never_waits_for_a_decision(monkeypatch):
    with workflow(_decide_wf()) as imp:
        script = [ai(("request_decision", {"question": "Load or continue?"})), ai(text="ok")]
        r = Run(imp, monkeypatch, script, published(), agent_ids=("triage", "report"),
                scripts={"triage": script, "report": [ai(text="report")]})
        out = asyncio.run(r.start(gates="auto"))
    assert "__interrupt__" not in out
    assert r.turns["triage"][1][-1].content.startswith("No person is available")


def test_without_options_no_decision_tool_is_offered(monkeypatch):
    with workflow(asks_wf({"tool": ["lookup"]})) as imp:
        r = Run(imp, monkeypatch, [ai(text="ok")], published())
        asyncio.run(r.start())
        assert "request_decision" not in r.offered["triage"][0]
        values = asyncio.run(r.graph.aget_state(r.cfg)).values
    assert not values.get("choices")


def test_askperson_options_are_checked_at_start():
    with (workflow(asks_wf({"askPerson": {"options": ["Only one"]}, "tool": ["lookup"]})) as imp,
          pytest.raises(ValueError, match="askPerson")):
        imp("app.common.asks").validate()


# ---------------------------------------------------------------------------
# The BFF
# ---------------------------------------------------------------------------

GATE_WAIT = {"node": "triage", "question": "Review triage", "approval": "console", "options": OPTIONS}
ASKED = {"node": "triage", "question": "Load or continue?", "kind": "decision", "askId": "triage.v1.d1",
         "options": ["Load images", "Continue"], "approval": "console"}


@pytest.mark.parametrize("hitl,option", [(GATE_WAIT, "Override"), (ASKED, "Continue")], ids=["gate", "asked"])
def test_an_option_goes_through_the_decision_route_and_is_logged(bff, hitl, option):
    bff._test_waiting(hitl)
    status, _ = decide(bff, {"option": option.lower(), "comment": "checked"})
    assert status == 200
    sent = bff._test_invoked[-1]
    assert (sent["action"], sent["decision"], sent["option"], sent["comment"]) == ("resume", "approve", option, "checked")
    action, detail = bff._test_audited[-1]
    assert action == "run.decided" and detail["option"] == option and detail["decision"] == "approve"


@pytest.mark.parametrize("hitl,body,why", [
    (GATE_WAIT, {"decision": "approve"}, "`option` must be one of: Override, Abort"),
    (GATE_WAIT, {"option": "Ship"}, "`option` must be one of"),
    (GATE_WAIT, {"decision": "revise", "comment": "x", "option": "Abort"}, "choose one of its options"),
    (GATE_WAIT, {"decision": "deny"}, "choose one of its options"),
    (GATE_WAIT, {"option": "Abort", "comment": "x" * 4001}, "at most 4000"),
    (ASKED, {"decision": "approve", "comment": "Continue"}, "`option` must be one of"),
    (ASKED, {"decisions": {"triage": {"decision": "approve"}}, "option": "Continue"}, "choose one of its options"),
])
def test_what_cannot_choose_is_refused_and_nothing_resumes(bff, hitl, body, why):
    bff._test_waiting(hitl)
    status, resp = decide(bff, body)
    assert status == 400 and why in resp["error"]
    assert bff._test_invoked == []


def test_an_option_sent_to_a_plain_gate_changes_nothing(bff):
    bff._test_waiting({"node": "triage", "question": "Review triage"})
    status, _ = decide(bff, {"decision": "approve", "option": "Override"})
    assert status == 200
    sent = bff._test_invoked[-1]
    assert sent["decision"] == "approve" and "option" not in sent
    assert "option" not in bff._test_audited[-1][1]


def test_the_assistant_does_not_choose_for_a_person(bff, monkeypatch):
    for hitl in (GATE_WAIT, ASKED):
        monkeypatch.setattr(bff.chatbot, "_status", FakeTable(
            {"session_id": "s1", "overall": "waiting_human", "hitl": hitl}))
        calls = []
        out, said = bff.chatbot._t_review({"session_id": "s1", "decision": "approve"}, {}, calls.append)
        assert "run page" in out["error"] and calls == [] and said is None


def test_an_event_does_not_choose(monkeypatch):
    import importlib
    import json
    monkeypatch.setenv("WORKFLOW_JSON", json.dumps({**wf(GATED)}))
    monkeypatch.setenv("APP_NAME", "")
    for mod in ("workflow", "gates"):
        sys.modules.pop(mod, None)
    try:
        gates = importlib.import_module("gates")
        resumed = []
        table = FakeTable({"session_id": "s1", "overall": "waiting_human", "hitl": dict(ASKED, approval="event")})
        assert gates.decide({"session": "s1", "decision": "approve"}, table, lambda *a: resumed.append(a))[
            "decided"] is False
        table = FakeTable({"session_id": "s1", "overall": "waiting_human", "hitl": {**GATE_WAIT, "node": "check"}})
        assert gates.decide({"session": "s1", "decision": "approve"}, table, lambda *a: resumed.append(a))[
            "decided"] is False
        assert resumed == []
    finally:
        for mod in ("workflow", "gates"):
            sys.modules.pop(mod, None)


def test_the_runtime_payload_carries_the_option(bff, monkeypatch):
    """bff/handler.py _run's resume payload to the runtime: the option is passed on, and a
    decision without one is exactly as before."""
    import json
    sent = []

    class Client:
        def invoke_agent_runtime(self, **kw):
            sent.append(json.loads(kw["payload"]))
            return {}
    monkeypatch.setattr(bff, "agentcore", Client())
    monkeypatch.setattr(bff, "_default_target", lambda: {"runtimeArn": "arn:x", "status": FakeTable()})
    resume = {"action": "resume", "session_id": "s1", "decision": "approve", "comment": "c", "decisions": None}
    bff._run({**resume, "option": "Override"})
    bff._run(resume)
    assert sent[0]["option"] == "Override" and sent[0]["decision"] == "approve"
    assert "option" not in sent[1]
    assert {k: v for k, v in sent[0].items() if k != "option"} == sent[1]


# ---------------------------------------------------------------------------
# The validators (bff/validate_build.py; validate.ts runs the same cases from the fixture)
# ---------------------------------------------------------------------------

sys.path.insert(0, str(ORCH_ROOT / "bff"))


def _issues(workflow_def):
    import validate_build
    return [(i["severity"], i["path"], i["message"]) for i in validate_build.validate(workflow_def)
            if "options" in i["path"] or "branch.when" in i["path"]]


def _main(steps, **agent):
    defn = wf(steps)
    for a in defn["agents"].values():
        a.update(runtime="main", **agent)
    return defn


def test_validation_accepts_a_routed_gate():
    assert _issues(_main(GATED)) == []


def test_validation_names_each_mistake():
    steps = [{"agent": "check", "hitl": {"options": ["Go", "go"], "approval": "event"},
              "branch": {"when": [{"field": "decision", "equals": "Overide", "goto": "END"}]}},
             {"parallel": ["a", "b"], "gateId": "g", "hitl": {"options": OPTIONS}},
             {"agent": "deliver"}]
    got = _issues(_main(steps))
    assert ("error", "steps[0].hitl.options", "the options must differ (ignoring case)") in got
    event = ('named options are chosen on the run page; an event cannot choose one, so they do not go '
             'with approval "event"')
    assert ("error", "steps[0].hitl.options", event) in got
    assert ("warning", "steps[0].branch.when[0]",
            '`decision` is one of: Go, go; "Overide" is not one, so this rule never matches') in got
    assert any(p == "steps[1].hitl.options" and "parallel stage" in m for _, p, m in got)
    assert ("warning", "steps[1].hitl.options",
            "no `branch` rule on this stage reads `decision`, so every option continues to the next stage") in got


def test_validation_checks_askperson_options():
    steps = [{"agent": "triage"}]
    defn = _main(steps, toolMode="model", askPerson={"options": ["Only"]})
    import validate_build
    msgs = [(i["path"], i["message"]) for i in validate_build.validate(defn)]
    assert ("agents.triage.askPerson.options", 'must be a list of 2 to 6 option labels, e.g. ["Override", "Abort"]') in msgs
    defn = _main(steps, toolMode="model", askPerson={"options": ["Load images", "Continue"]})
    assert not [m for m in validate_build.validate(defn) if "askPerson" in m["path"]]
