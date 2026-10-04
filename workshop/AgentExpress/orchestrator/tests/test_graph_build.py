"""The LangGraph graph actually compiles for arbitrary topologies.

`build_graph` is the piece that makes `steps` executable. A topology it cannot
wire is not a bad answer — it is a container that crashes on start, after a
successful deploy. So the point of these tests is coverage of SHAPES: single
agents, parallel groups, sequence groups, gated and ungated, in the awkward
positions (first step, last step, back to back).

Compiling is a real assertion here, not a smoke test: `add_conditional_edges`
validates that every routing target is a declared node, so a mis-wired gate fails
at compile time.
"""

import pytest
from conftest import all_ids, expected_gate_nodes, wf, workflow

# Each entry: (name, steps). All of these must compile.
TOPOLOGIES = [
    ("single agent, no gate", [{"agent": "a"}]),
    ("single agent, gated", [{"agent": "a", "hitl": True}]),
    ("two agents chained by steps", [{"agent": "a"}, {"agent": "b"}]),
    ("gate between two agents", [{"agent": "a", "hitl": True}, {"agent": "b"}]),
    ("parallel first, ungated", [{"parallel": ["a", "b"]}, {"agent": "c"}]),
    ("parallel first, gated",
     [{"parallel": ["a", "b"], "hitl": True, "gateId": "g"}, {"agent": "c"}]),
    ("parallel last, gated",
     [{"agent": "a"}, {"parallel": ["b", "c"], "hitl": True, "gateId": "g"}]),
    ("sequence first, ungated", [{"sequence": ["a", "b"]}, {"agent": "c"}]),
    ("sequence first, gated",
     [{"sequence": ["a", "b"], "hitl": True, "gateId": "g"}, {"agent": "c"}]),
    ("sequence last, ungated", [{"agent": "a"}, {"sequence": ["b", "c"]}]),
    ("two gated groups back to back",
     [{"parallel": ["a", "b"], "hitl": True, "gateId": "g1"},
      {"sequence": ["c", "d"], "hitl": True, "gateId": "g2"}]),
    ("gates on every step",
     [{"agent": "a", "hitl": True},
      {"parallel": ["b", "c"], "hitl": True, "gateId": "g1"},
      {"sequence": ["d", "e"], "hitl": True, "gateId": "g2"},
      {"agent": "f"}]),
    ("wide parallel group",
     [{"agent": "a"}, {"parallel": ["b", "c", "d", "e", "f"], "hitl": True, "gateId": "g"}]),
    ("long sequence", [{"sequence": ["a", "b", "c", "d", "e"]}]),
]


@pytest.mark.parametrize("name,steps", TOPOLOGIES, ids=[t[0] for t in TOPOLOGIES])
def test_topology_compiles(name, steps):
    with workflow(wf(steps)) as imp:
        graph = imp("app.orchestrator.graph_builder").build_graph()
        nodes = set(graph.get_graph().nodes)
        for expected in wf(steps)["agents"]:
            assert expected in nodes, f"{name}: agent node {expected} missing"


def test_shipped_workflow_compiles(shipped):
    """The sample's own topology. Expectations are derived from its `steps`, so a
    renamed or added agent does not need this test edited."""
    with workflow(shipped) as imp:
        graph = imp("app.orchestrator.graph_builder").build_graph()
        nodes = set(graph.get_graph().nodes)
        assert set(all_ids(shipped)) <= nodes
        # Exactly one gate node per gated step, and none for an ungated one.
        gates = expected_gate_nodes(shipped)
        assert gates <= nodes
        assert {n for n in nodes if n.endswith("_gate")} == gates


def test_gate_nodes_are_named_from_the_config():
    """`gateId` names the gate node, so the BFF/UI can address it. A parallel or
    sequence step without one falls back to a positional id."""
    steps = [{"agent": "a", "hitl": True},
             {"parallel": ["b", "c"], "hitl": True, "gateId": "review"},
             {"sequence": ["d", "e"], "hitl": True}]
    with workflow(wf(steps)) as imp:
        nodes = set(imp("app.orchestrator.graph_builder").build_graph().get_graph().nodes)
        assert "a_gate" in nodes          # single-agent gate: "<agentId>_gate"
        assert "review_gate" in nodes     # explicit gateId
        assert "group2_gate" in nodes     # no gateId -> "group<stepIndex>"


# --- gate routers ----------------------------------------------------------
# The routers are the HITL semantics: approve continues, deny ends the run, revise
# loops back. Pure functions of state, so they can be asserted directly. One factory
# serves all three gate kinds; they differ only in the revise target, which is why
# these tests pass it in.

def test_single_agent_router_semantics():
    with workflow(wf([{"agent": "a", "hitl": True}, {"agent": "b"}])) as imp:
        gb = imp("app.orchestrator.graph_builder")
        from langgraph.graph import END
        route = gb._make_router("a", ["b"], lambda _s: "a")
        assert route({"decisions": {"a": "approve"}}) == ["b"]
        assert route({}) == ["b"]                            # no decision -> continue
        assert route({"decisions": {"a": "deny"}}) == END
        assert route({"decisions": {"a": "revise"}}) == "a"   # re-run this agent


def test_group_router_reruns_only_the_flagged_subset():
    """The behaviour that makes a parallel gate per-agent: revise must re-run the
    agents the reviewer flagged, not the whole stage."""
    with workflow(wf([{"parallel": ["a", "b", "c"], "hitl": True, "gateId": "g"},
                      {"agent": "d"}])) as imp:
        gb = imp("app.orchestrator.graph_builder")
        from langgraph.graph import END
        # The group's revise target: the flagged subset, else the whole group.
        route = gb._make_router("g", ["d"], lambda s:
            (s.get("group_rerun") or {}).get("g") or ["a", "b", "c"])
        assert route({"decisions": {"g": "approve"}}) == ["d"]
        assert route({"decisions": {"g": "deny"}}) == END
        assert route({"decisions": {"g": "revise"},
                      "group_rerun": {"g": ["b"]}}) == ["b"]
        # revise with no subset recorded falls back to the whole group rather than
        # stalling with nothing to run.
        assert route({"decisions": {"g": "revise"}}) == ["a", "b", "c"]


def test_sequence_router_loops_back_to_the_start_of_the_chain():
    with workflow(wf([{"sequence": ["a", "b"], "hitl": True, "gateId": "g"},
                      {"agent": "c"}])) as imp:
        gb = imp("app.orchestrator.graph_builder")
        from langgraph.graph import END
        route = gb._make_router("g", ["c"], lambda _s: "a")
        assert route({"decisions": {"g": "approve"}}) == ["c"]
        assert route({"decisions": {"g": "deny"}}) == END
        # Not "b": revising a sequence re-runs the whole chain, since a later
        # member's output depends on the earlier ones.
        assert route({"decisions": {"g": "revise"}}) == "a"


def test_router_on_the_last_step_ends_the_run():
    with workflow(wf([{"agent": "a", "hitl": True}])) as imp:
        gb = imp("app.orchestrator.graph_builder")
        from langgraph.graph import END
        assert gb._make_router("a", [END], lambda _s: "a")(
            {"decisions": {"a": "approve"}}) == END
