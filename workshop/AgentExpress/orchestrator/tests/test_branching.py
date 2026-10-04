"""Content-based branching — the agent's own output choosing what runs next.

Three layers, because they fail in three different ways:

  1. the RULE LANGUAGE (app/common/branching.py) — a pure function over parsed JSON,
     so every operator and every near-miss is asserted directly;
  2. the WIRING (graph_builder) — routers and validation, asserted without running
     anything;
  3. the BEHAVIOUR — a graph that is actually INVOKED, with stub agents, proving the
     chosen path ran, the other did not, and the bypassed agents were marked skipped.

Layer 3 exists because layers 1 and 2 can both pass on a graph that deadlocks. The
routers are pure, so the only way to know LangGraph agrees with them is to run it.

The workflow under test is a claims triage, not this repo's research pipeline — the
framework must not have an opinion about the subject matter, and a foreign domain is
the only way to check that it does not (see test_foreign_use_case.py).
"""

import asyncio

import pytest
from conftest import wf, workflow
from langgraph.graph import END

# A claims triage with two mutually exclusive specialist paths that rejoin.
#
#   triage ─┬─ escalate / amount >= 10000 ──> investigator ──┐
#           ├─ claimId missing ─────────────> END            ├─> settlement
#           └─ (default) ──────────────────> adjuster ───────┘
#
# `investigator` carries `{"default": "settlement"}` — a branch with no rules, i.e. an
# unconditional jump. Without it the escalated path would fall into the adjuster's
# step on its way out, and the two paths would not be exclusive.
TRIAGE_STEPS = [
    {"agent": "triage", "branch": {
        "when": [
            {"field": "disposition", "equals": "escalate", "goto": "investigator"},
            {"field": "amount", "gte": 10000, "goto": "investigator"},
            {"field": "claimId", "exists": False, "goto": "END"},
        ],
        "default": "adjuster"}},
    {"agent": "investigator", "branch": {"default": "settlement"}},
    {"agent": "adjuster"},
    {"agent": "settlement"},
]


# ---------------------------------------------------------------------------
# 1. The rule language
# ---------------------------------------------------------------------------

def _choose(spec, output):
    from app.common import branching
    return branching.choose(spec, output)[0]


def rule(**kw):
    return {"when": [{"goto": "hit", **kw}], "default": "miss"}


@pytest.mark.parametrize("spec,output,expected", [
    # --- equality, and the normalisation that keeps a model's casing harmless ---
    (rule(field="d", equals="escalate"), '{"d": "escalate"}', "hit"),
    (rule(field="d", equals="escalate"), '{"d": "Escalate"}', "hit"),
    (rule(field="d", equals="escalate"), '{"d": "  ESCALATE "}', "hit"),
    (rule(field="d", equals="escalate"), '{"d": "settle"}', "miss"),
    (rule(field="d", notEquals="settle"), '{"d": "escalate"}', "hit"),
    (rule(field="d", notEquals="settle"), '{"d": "Settle"}', "miss"),
    # A model asked for a score returns "85" about as often as 85, so neither the
    # textual nor the numeric operators care which one arrived.
    (rule(field="n", equals=85), '{"n": "85"}', "hit"),
    (rule(field="n", gte=85), '{"n": "85"}', "hit"),
    # --- alternatives ---
    (rule(field="d", **{"in": ["escalate", "refer"]}), '{"d": "refer"}', "hit"),
    (rule(field="d", **{"in": ["escalate", "refer"]}), '{"d": "settle"}', "miss"),
    # --- numeric, including a list/dict comparing by LENGTH ---
    (rule(field="a", gt=10000), '{"a": 10001}', "hit"),
    (rule(field="a", gt=10000), '{"a": 10000}', "miss"),
    (rule(field="a", gte=10000), '{"a": 10000}', "hit"),
    (rule(field="a", lt=10), '{"a": 9.5}', "hit"),
    (rule(field="a", lte=10), '{"a": 10}', "hit"),
    (rule(field="items", gt=2), '{"items": ["x", "y", "z"]}', "hit"),
    (rule(field="items", gt=2), '{"items": ["x", "y"]}', "miss"),
    (rule(field="a", gt=1), '{"a": "not a number"}', "miss"),
    # --- membership / substring ---
    (rule(field="flags", contains="fraud"), '{"flags": ["fraud", "late"]}', "hit"),
    (rule(field="flags", contains="FRAUD"), '{"flags": ["fraud"]}', "hit"),
    (rule(field="note", contains="fraud"), '{"note": "possible fraud here"}', "hit"),
    (rule(field="by", contains="risk"), '{"by": {"risk": 1}}', "hit"),
    (rule(field="flags", contains="fraud"), '{"flags": []}', "miss"),
    # --- presence ---
    (rule(field="id", exists=True), '{"id": "C-1"}', "hit"),
    (rule(field="id", exists=True), '{"id": ""}', "miss"),
    (rule(field="id", exists=True), '{"id": null}', "miss"),
    (rule(field="id", exists=True), '{"other": 1}', "miss"),
    (rule(field="id", exists=False), '{"other": 1}', "hit"),
    (rule(field="items", exists=True), '{"items": []}', "miss"),
    # --- dot paths and list indexes: nothing here knows a schema ---
    (rule(field="scope.tier", equals="gold"), '{"scope": {"tier": "gold"}}', "hit"),
    (rule(field="findings.0.claim", equals="x"), '{"findings": [{"claim": "x"}]}', "hit"),
    (rule(field="findings.1.claim", equals="x"), '{"findings": [{"claim": "x"}]}', "miss"),
    (rule(field="scope.tier", equals="gold"), '{"scope": "gold"}', "miss"),
    (rule(field="a.b.c", equals="x"), '{"a": 1}', "miss"),
    # --- no `field`: compare the agent's RAW output, so prose is branchable ---
    (rule(contains="URGENT"), "this is urgent, escalate", "hit"),
    (rule(contains="URGENT"), "routine claim", "miss"),
    # --- several operators in one rule are ANDed ---
    ({"when": [{"field": "a", "gte": 100, "lt": 500, "goto": "hit"}], "default": "miss"},
     '{"a": 200}', "hit"),
    ({"when": [{"field": "a", "gte": 100, "lt": 500, "goto": "hit"}], "default": "miss"},
     '{"a": 700}', "miss"),
    # --- output that is not JSON at all: rules miss, the default still applies ---
    (rule(field="d", equals="escalate"), "sorry, I could not comply", "miss"),
    (rule(field="d", equals="escalate"), "", "miss"),
])
def test_operator_semantics(spec, output, expected):
    assert _choose(spec, output) == expected


def test_first_matching_rule_wins():
    """Order is the precedence, so a customer can put the specific case first."""
    spec = {"when": [{"field": "a", "gte": 1, "goto": "first"},
                     {"field": "a", "gte": 100, "goto": "second"}]}
    assert _choose(spec, '{"a": 1000}') == "first"


def test_no_match_and_no_default_falls_through():
    """The property that makes `branch` safe to add: it can redirect a run, never
    strand one. No rule matched and no default means "carry on as configured"."""
    from app.common import branching
    target, reason = branching.choose({"when": [{"field": "a", "equals": "x", "goto": "z"}]},
                                      '{"a": "y"}')
    assert target is None
    assert "continuing to the next step" in reason


def test_default_alone_is_an_unconditional_jump():
    assert _choose({"default": "settlement"}, '{"anything": 1}') == "settlement"


def test_the_matched_rule_is_reported_for_the_timeline():
    """A branch nobody can explain is worse than no branch, so the reason travels
    with the decision into the run's timeline."""
    from app.common import branching
    target, reason = branching.choose(
        {"when": [{"field": "riskScore", "gte": 80, "goto": "investigator"}]},
        '{"riskScore": 92}')
    assert target == "investigator"
    assert "riskScore" in reason and "gte" in reason and "80" in reason


def test_a_truncated_payload_is_still_branchable():
    """Research-sized assets get cut off at the agent's token budget. The salvaging
    parser every asset already goes through is used here too, so a branch field that
    arrived intact still decides."""
    truncated = '{"disposition": "escalate", "notes": "the adjuster sugges'
    assert _choose(rule(field="disposition", equals="escalate"), truncated) == "hit"


def test_fenced_json_is_branchable():
    assert _choose(rule(field="d", equals="x"), '```json\n{"d": "x"}\n```') == "hit"


# --- spec validation: every case here would otherwise be SILENT ------------

@pytest.mark.parametrize("spec,match", [
    ("escalate", "must be an object"),
    ({"whn": [{"field": "a", "equals": 1, "goto": "b"}]}, "unknown key"),
    ({}, "needs `when`"),
    ({"when": []}, "non-empty list"),
    ({"when": [{"field": "a", "equals": 1, "goto": "b"}], "default": "  "}, "is empty"),
    ({"when": ["nope"]}, "must be an object"),
    ({"when": [{"field": "a", "equal": 1, "goto": "b"}]}, "unknown key"),
    ({"when": [{"field": "a", "equals": 1}]}, "needs a `goto`"),
    ({"when": [{"field": "a", "goto": "b"}]}, "has no comparison"),
    ({"when": [{"field": "a", "in": "x", "goto": "b"}]}, "takes a list"),
    ({"when": [{"field": "a", "exists": "yes", "goto": "b"}]}, "takes true or false"),
    ({"when": [{"field": "a", "gte": "big", "goto": "b"}]}, "takes a number"),
    ({"when": [{"field": "a", "equals": ["x", "y"], "goto": "b"}]}, "takes a single value"),
])
def test_malformed_specs_are_rejected(spec, match):
    from app.common import branching
    with pytest.raises(ValueError, match=match):
        branching.validate_spec(spec, "steps[0]")


def test_a_misspelled_operator_is_rejected_rather_than_ignored():
    """The specific reason validation exists. `gte` mistyped as `gt3` leaves a rule
    with no comparison, so it never matches and every run takes the default — a
    branch that looks like it works and routes nothing."""
    from app.common import branching
    with pytest.raises(ValueError, match="unknown key"):
        branching.validate_spec(
            {"when": [{"field": "amount", "gt3": 10000, "goto": "investigator"}]}, "steps[0]")


# ---------------------------------------------------------------------------
# 2. Wiring: topology validation, routing, and what gets skipped
# ---------------------------------------------------------------------------

def test_a_branching_topology_compiles():
    with workflow(wf(TRIAGE_STEPS)) as imp:
        graph = imp("app.orchestrator.graph_builder").build_graph()
        nodes = set(graph.get_graph().nodes)
        assert {"triage", "investigator", "adjuster", "settlement"} <= nodes
        # One branch node per branching step, named from the step.
        assert {"triage_branch", "investigator_branch"} <= nodes


def test_a_branch_composes_with_a_review_gate():
    """Gated: the human approves the output FIRST, then the branch reads it. So the
    gate's "approve" leg routes into the branch node, not into the next step."""
    steps = [{"agent": "triage", "hitl": True,
              "branch": {"when": [{"field": "d", "equals": "x", "goto": "c"}]}},
             {"agent": "b"}, {"agent": "c"}]
    with workflow(wf(steps)) as imp:
        gb = imp("app.orchestrator.graph_builder")
        graph = gb.build_graph()
        nodes = set(graph.get_graph().nodes)
        assert {"triage_gate", "triage_branch"} <= nodes
        # The gate forwards to the branch node; deny/revise are unchanged.
        route = gb._make_router("triage", ["triage_branch"], lambda _s: "triage")
        assert route({"decisions": {"triage": "approve"}}) == ["triage_branch"]
        assert route({"decisions": {"triage": "deny"}}) == END
        assert route({"decisions": {"triage": "revise"}}) == "triage"


def test_a_branch_on_a_sequence_is_decided_by_its_last_agent():
    steps = [{"sequence": ["a", "b"], "gateId": "stage",
              "branch": {"when": [{"field": "d", "equals": "x", "goto": "d"}]}},
             {"agent": "c"}, {"agent": "d"}]
    with workflow(wf(steps)) as imp:
        gb = imp("app.orchestrator.graph_builder")
        assert gb._decider(steps[0]) == "b"
        assert "stage_branch" in set(gb.build_graph().get_graph().nodes)


def test_branch_router_reads_the_recorded_decision():
    """The router is a pure read of state — the node made and logged the decision —
    which is what keeps routing assertable without running an agent."""
    with workflow(wf(TRIAGE_STEPS)) as imp:
        gb = imp("app.orchestrator.graph_builder")
        route = gb._make_branch_router("triage", ["investigator"])
        assert route({"branch": {"triage": "adjuster"}}) == ["adjuster"]
        assert route({"branch": {"triage": "investigator"}}) == ["investigator"]
        assert route({"branch": {"triage": "END"}}) == END
        # Nothing recorded, or a target naming no step, falls through to the next
        # step rather than stranding the run.
        assert route({}) == ["investigator"]
        assert route({"branch": {"triage": "deleted_agent"}}) == ["investigator"]


def test_a_branch_target_enters_a_whole_group_not_one_member():
    """A target names a STEP. Routing to one member of a parallel group would leave
    its gate waiting for siblings that never ran."""
    steps = [{"agent": "a", "branch": {"default": "review"}},
             {"agent": "b"},
             {"parallel": ["p1", "p2"], "hitl": True, "gateId": "review"},
             {"agent": "z"}]
    with workflow(wf(steps)) as imp:
        gb = imp("app.orchestrator.graph_builder")
        assert gb._make_branch_router("a", ["b"])({"branch": {"a": "review"}}) == ["p1", "p2"]


def test_bypassed_agents_are_the_steps_jumped_over():
    with workflow(wf(TRIAGE_STEPS)) as imp:
        gb = imp("app.orchestrator.graph_builder")
        assert gb._bypassed_agents(0, "adjuster") == ["investigator"]
        assert gb._bypassed_agents(0, "investigator") == []
        assert gb._bypassed_agents(1, "settlement") == ["adjuster"]
        # Ending the run skips everything after the branching step.
        assert gb._bypassed_agents(0, "END") == ["investigator", "adjuster", "settlement"]
        assert gb._bypassed_agents(0, None) == []


@pytest.mark.parametrize("steps,match", [
    # A group has no single agent whose output decides.
    ([{"parallel": ["a", "b"], "gateId": "g", "branch": {"default": "c"}}, {"agent": "c"}],
     "not supported on a `parallel` step"),
    # Nowhere to route from the end of the pipeline.
    ([{"agent": "a"}, {"agent": "b", "branch": {"default": "a"}}], "nowhere to route"),
    # A target that names nothing routes nowhere, silently.
    ([{"agent": "a", "branch": {"default": "typo"}}, {"agent": "b"}], "names no step"),
    # A backward edge is a cycle the run could not leave.
    ([{"agent": "a"}, {"agent": "b", "branch": {"default": "a"}}, {"agent": "c"}],
     "at or before this one"),
    # Its own step is backward too.
    ([{"agent": "a", "branch": {"default": "a"}}, {"agent": "b"}], "at or before this one"),
    # And the spec's own shape is checked here too, naming the step.
    ([{"agent": "a", "branch": {"when": [{"field": "x", "goto": "b"}]}}, {"agent": "b"}],
     "has no comparison"),
])
def test_unworkable_branches_are_rejected_at_build(steps, match):
    """Rejected when the graph is built — i.e. at container start — and mirrored by
    both IaC paths so a customer normally sees it at plan/synth time instead."""
    with workflow(wf(steps)) as imp, pytest.raises(ValueError, match=match):
        imp("app.orchestrator.graph_builder").build_graph()


def test_duplicate_step_names_are_rejected_once_anything_branches():
    """A target names a step, so two steps called the same thing make it ambiguous —
    and the lookup would quietly pick the later one."""
    steps = [{"agent": "a", "branch": {"default": "dup"}},
             {"parallel": ["b", "c"], "gateId": "dup"},
             {"sequence": ["d", "e"], "gateId": "dup"}]
    with workflow(wf(steps)) as imp, pytest.raises(ValueError, match="duplicate step name"):
        imp("app.orchestrator.graph_builder").build_graph()


def test_duplicate_step_names_are_tolerated_when_nothing_branches():
    """The check is scoped to branching: without a branch nothing looks a step up by
    name, and failing an existing workflow would be a gratuitous breaking change."""
    steps = [{"agent": "a"}, {"parallel": ["b", "c"], "gateId": "dup"},
             {"sequence": ["d", "e"], "gateId": "dup"}]
    with workflow(wf(steps)) as imp:
        imp("app.orchestrator.graph_builder").validate_branches()


def test_a_group_step_is_targetable_by_its_gate_id():
    steps = [{"agent": "a", "branch": {"default": "deep"}},
             {"agent": "b"},
             {"sequence": ["s1", "s2"], "gateId": "deep"}]
    with workflow(wf(steps)) as imp:
        gb = imp("app.orchestrator.graph_builder")
        gb.validate_branches()  # does not raise
        assert gb._make_branch_router("a", ["b"])({"branch": {"a": "deep"}}) == ["s1"]


# ---------------------------------------------------------------------------
# 3. Behaviour: a graph that is actually run
# ---------------------------------------------------------------------------

SESSION = "branch-test"


def stub_graph(imp, outputs: dict[str, str]):
    """(graph, ran, snapshot) with stub agents wired into the REAL graph.

    The agents are stubs so the tests assert ROUTING rather than any agent's
    behaviour, but everything between them is the real thing: the real agent nodes,
    the real branch node, the real routers, the real LangGraph execution. `ran`
    records the agents that actually executed, in order.
    """
    base = imp("app.common.base")
    sink = imp("app.common.sink")
    bus = imp("app.common.bus").bus
    gb = imp("app.orchestrator.graph_builder")

    # No DynamoDB: progress goes to the in-process snapshot the dev server reads,
    # which is fed by the same events the deployed UI sees.
    sink.STATUS_TABLE = None
    sink.EVENTS_TABLE = None

    ran: list[str] = []

    def load_agents():
        registry = {}
        for agent_id, out in outputs.items():
            agent = base.Agent()
            agent.id, agent.name, agent.agentcore = agent_id, agent_id, {}

            async def run(_ctx, _id=agent_id, _out=out):
                ran.append(_id)
                return _out

            agent.run = run
            registry[agent_id] = agent
        return registry

    gb.load_agents = load_agents
    bus.create(SESSION, "a claim")
    return gb.build_graph(), ran, lambda: bus.snapshot(SESSION)


def config():
    return {"configurable": {"thread_id": SESSION}}


def run_graph(imp, outputs: dict[str, str]) -> tuple[dict, list[str], dict]:
    graph, ran, snapshot = stub_graph(imp, outputs)
    state = asyncio.run(graph.ainvoke({"topic": "a claim", "status": {}, "outputs": {}},
                                      config()))
    return state, ran, snapshot()


ESCALATED = '{"claimId": "C-1", "disposition": "escalate", "amount": 250}'
ROUTINE = '{"claimId": "C-2", "disposition": "settle", "amount": 250}'
LARGE = '{"claimId": "C-3", "disposition": "settle", "amount": 40000}'
UNUSABLE = '{"disposition": "settle", "amount": 10}'


STUBS = {"investigator": "{}", "adjuster": "{}", "settlement": "{}"}


def test_the_default_path_runs_and_the_other_is_skipped():
    with workflow(wf(TRIAGE_STEPS)) as imp:
        state, ran, snap = run_graph(imp, {"triage": ROUTINE, **STUBS})
        assert ran == ["triage", "adjuster", "settlement"]
        assert "investigator" not in state["outputs"]
        assert state["branch"] == {"triage": "adjuster"}
        assert snap["nodes"]["investigator"]["status"] == "skipped"
        assert snap["nodes"]["settlement"]["status"] == "done"


def test_a_field_in_the_output_sends_the_run_down_the_other_path():
    """The whole point: same workflow, same request, different output — a different
    set of agents runs."""
    with workflow(wf(TRIAGE_STEPS)) as imp:
        state, ran, snap = run_graph(imp, {"triage": ESCALATED, **STUBS})
        assert ran == ["triage", "investigator", "settlement"]
        assert "adjuster" not in state["outputs"]
        # `investigator`'s own unconditional jump is what makes the two paths
        # exclusive: without it the escalated run would fall into the adjuster's step.
        assert state["branch"] == {"triage": "investigator", "investigator": "settlement"}
        assert snap["nodes"]["adjuster"]["status"] == "skipped"


def test_a_numeric_threshold_routes_the_same_way_as_the_classification():
    with workflow(wf(TRIAGE_STEPS)) as imp:
        _, ran, _ = run_graph(imp, {"triage": LARGE, **STUBS})
        assert ran == ["triage", "investigator", "settlement"]


def test_a_branch_can_end_the_run_early():
    with workflow(wf(TRIAGE_STEPS)) as imp:
        _, ran, snap = run_graph(imp, {"triage": UNUSABLE, **STUBS})
        assert ran == ["triage"]
        for node in ("investigator", "adjuster", "settlement"):
            assert snap["nodes"][node]["status"] == "skipped"


def test_each_decision_and_its_reason_reach_the_timeline():
    with workflow(wf(TRIAGE_STEPS)) as imp:
        _, _, snap = run_graph(imp, {"triage": ESCALATED, **STUBS})
        lines = [entry["msg"] for entry in snap["logs"] if entry["msg"].startswith("Branch after")]
        assert lines == [
            "Branch after triage: disposition equals 'escalate' -> investigator",
            ("Branch after investigator: no rule matched, took `default` -> settlement "
             "· skipping adjuster"),
        ]


def test_a_workflow_with_no_branch_is_unchanged():
    """The substitution that wires branching must be inert when nothing branches."""
    with workflow(wf([{"agent": "a"}, {"agent": "b"}])) as imp:
        _, ran, snap = run_graph(imp, {"a": "{}", "b": "{}"})
        assert ran == ["a", "b"]
        assert not [n for n in snap["nodes"].values() if n["status"] == "skipped"]


# --- the rewind ("re-run from this agent") across a branch ------------------

def test_rerun_across_a_branch_seeds_the_decision_to_reach_the_target():
    """A branch decides where flow goes, so a rewind past one has to say which way.
    Asking to re-run an agent IS a decision about the path, and recording it is more
    honest than routing around the branch node."""
    with workflow(wf(TRIAGE_STEPS)) as imp:
        gb = imp("app.orchestrator.graph_builder")
        p = gb.rerun_plan("investigator")
        assert p["as_node"] == "triage_branch"
        assert p["branch_seed"] == {"triage": "investigator"}
        assert p["decision_key"] is None
        # The step after a second branch is entered from THAT branch's node.
        assert gb.rerun_plan("adjuster")["as_node"] == "investigator_branch"
        assert gb.rerun_plan("adjuster")["branch_seed"] == {"investigator": "adjuster"}
        # Re-running the branching agent itself is an ordinary rewind.
        assert gb.rerun_plan("triage")["branch_seed"] == {}


def test_a_rewind_can_send_a_settled_run_down_the_path_it_did_not_take():
    """The plan is only half of it — the seeded decision has to make LangGraph
    actually route there. Run the default path to completion, then re-run the agent on
    the branch that was NOT taken, exactly as runtime._rewind does it."""
    with workflow(wf(TRIAGE_STEPS)) as imp:
        gb = imp("app.orchestrator.graph_builder")
        graph, ran, _ = stub_graph(imp, {"triage": ROUTINE, **STUBS})

        async def scenario():
            await graph.ainvoke({"topic": "a claim", "status": {}, "outputs": {}}, config())
            plan = gb.rerun_plan("investigator")
            await graph.aupdate_state(config(), {"branch": plan["branch_seed"]},
                                      as_node=plan["as_node"])
            return await graph.ainvoke(None, config())

        state = asyncio.run(scenario())
        assert ran == ["triage", "adjuster", "settlement", "investigator", "settlement"]
        assert state["branch"]["triage"] == "investigator"


def test_rerun_plans_carry_an_empty_seed_when_nothing_branches():
    steps = [{"agent": "a", "hitl": True}, {"agent": "b"}]
    with workflow(wf(steps)) as imp:
        assert imp("app.orchestrator.graph_builder").rerun_plan("b")["branch_seed"] == {}
