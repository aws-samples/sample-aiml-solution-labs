"""Rewind planning — "re-run this agent and everything after it".

`rerun_plan` decides which node a state write is attributed to, so that the
target's step becomes the graph's pending work. Get it wrong and the wrong stage
re-runs, or the run silently ends. It is pure topology arithmetic, so it can be
asserted exactly.
"""

import pytest
from conftest import ids_of, wf, workflow
from langgraph.graph import START


def plan(steps, agent):
    with workflow(wf(steps)) as imp:
        return imp("app.orchestrator.graph_builder").rerun_plan(agent)


def group_plan(steps, agents):
    with workflow(wf(steps)) as imp:
        return imp("app.orchestrator.graph_builder").group_rerun_plan(agents)


# --- rerun_plan ------------------------------------------------------------

def test_rerunning_the_first_agent_rewinds_to_START():
    p = plan([{"agent": "a", "hitl": True}, {"agent": "b"}], "a")
    assert p["as_node"] == START
    assert p["decision_key"] is None          # nothing upstream to re-approve
    assert p["step_agents"] == ["a"]
    assert p["downstream_agents"] == ["a", "b"]
    assert p["step_index"] == 0


def test_rerun_enters_from_the_previous_gate_and_forces_approve():
    """A gated predecessor must be told "approve", or its router would loop or halt
    instead of forwarding into the agent we want to re-run."""
    p = plan([{"agent": "a", "hitl": True}, {"agent": "b"}], "b")
    assert p["as_node"] == "a_gate"
    assert p["decision_key"] == "a"


def test_rerun_enters_from_an_ungated_single_predecessor():
    p = plan([{"agent": "a"}, {"agent": "b"}], "b")
    assert p["as_node"] == "a"
    assert p["decision_key"] is None


def test_rerunning_one_member_of_a_parallel_stage_reruns_the_whole_stage():
    """A parallel stage re-runs as a unit: its members join at one gate, so a
    partial re-entry would leave the gate waiting on branches that never ran.
    (The subset flow is group_rerun_plan, below.)"""
    steps = [{"agent": "a", "hitl": True},
             {"parallel": ["b", "c", "d"], "hitl": True, "gateId": "g"},
             {"agent": "e"}]
    p = plan(steps, "c")
    assert p["step_agents"] == ["b", "c", "d"]
    assert p["as_node"] == "a_gate"
    assert p["downstream_agents"] == ["b", "c", "d", "e"]


def test_rerunning_mid_sequence_reruns_only_the_tail():
    """Unlike a parallel group, a sequence can be entered part-way: the members
    before the target already produced the inputs the tail consumes."""
    steps = [{"agent": "a"}, {"sequence": ["s1", "s2", "s3"], "hitl": True, "gateId": "g"}]
    p = plan(steps, "s2")
    assert p["as_node"] == "s1"               # the agent immediately before it
    assert p["decision_key"] is None
    assert p["step_agents"] == ["s2", "s3"]   # the tail, not the whole chain


def test_rerunning_the_head_of_a_sequence_reruns_the_whole_chain():
    steps = [{"agent": "a", "hitl": True}, {"sequence": ["s1", "s2", "s3"]}]
    p = plan(steps, "s1")
    assert p["as_node"] == "a_gate"
    assert p["step_agents"] == ["s1", "s2", "s3"]


def test_downstream_agents_covers_every_later_step():
    steps = [{"agent": "a", "hitl": True},
             {"parallel": ["b", "c"], "hitl": True, "gateId": "g1"},
             {"sequence": ["d", "e"], "hitl": True, "gateId": "g2"},
             {"agent": "f"}]
    assert plan(steps, "a")["downstream_agents"] == ["a", "b", "c", "d", "e", "f"]
    assert plan(steps, "d")["downstream_agents"] == ["d", "e", "f"]
    assert plan(steps, "f")["downstream_agents"] == ["f"]


def test_rerun_rejects_an_unknown_agent():
    with pytest.raises(ValueError, match="unknown agent"):
        plan([{"agent": "a"}], "nope")


def test_rerun_refuses_to_cross_an_ungated_parallel_group():
    """An ungated parallel group has several exit nodes, so there is no single node
    to attribute the state write to. Guessing one would re-run an arbitrary branch,
    so this is rejected rather than approximated."""
    steps = [{"parallel": ["a", "b"]}, {"agent": "c"}]
    with pytest.raises(ValueError, match="non-gated parallel group"):
        plan(steps, "c")


def test_shipped_workflow_rerun_plans(shipped):
    """Every agent in the shipped workflow has a usable plan, and each plan's
    `step_agents` is exactly its own step — derived, so renaming an agent does not
    need this test edited."""
    steps = shipped["steps"]
    with workflow(shipped) as imp:
        gb = imp("app.orchestrator.graph_builder")
        for i, step in enumerate(steps):
            members = ids_of(step)
            for agent_id in members:
                plan = gb.rerun_plan(agent_id)
                # A whole step re-runs as a unit, except a sequence, which is
                # entered at the member being re-run.
                if "sequence" in step:
                    assert plan["step_agents"] == members[members.index(agent_id):]
                else:
                    assert plan["step_agents"] == members
                # Re-running the very first step has no gate before it to rewind to.
                if i > 0 or members.index(agent_id) > 0:
                    assert plan["as_node"], agent_id


# --- group_rerun_plan (subset of one parallel stage) -----------------------

def test_group_rerun_targets_the_stages_own_gate():
    """The subset flow reuses the group gate's existing "revise a subset" routing,
    so the write is attributed to that gate rather than to the step before it."""
    steps = [{"agent": "a", "hitl": True},
             {"parallel": ["b", "c", "d"], "hitl": True, "gateId": "research"},
             {"agent": "e"}]
    p = group_plan(steps, ["c", "d"])
    assert p["as_node"] == "research_gate"
    assert p["group_id"] == "research"
    assert p["subset"] == ["c", "d"]
    # "b" is NOT downstream: an unselected sibling keeps its existing output.
    assert p["downstream_agents"] == ["c", "d", "e"]


def test_group_rerun_deduplicates_and_keeps_order():
    steps = [{"parallel": ["a", "b", "c"], "hitl": True, "gateId": "g"}]
    assert group_plan(steps, ["c", "a", "c"])["subset"] == ["c", "a"]


def test_group_rerun_rejects_agents_from_different_stages():
    steps = [{"parallel": ["a", "b"], "hitl": True, "gateId": "g1"},
             {"parallel": ["c", "d"], "hitl": True, "gateId": "g2"}]
    with pytest.raises(ValueError, match="same stage"):
        group_plan(steps, ["a", "c"])


def test_group_rerun_rejects_a_stage_that_is_not_a_gated_parallel_group():
    with pytest.raises(ValueError, match="gated parallel stage"):
        group_plan([{"sequence": ["a", "b"], "hitl": True, "gateId": "g"}], ["a"])
    with pytest.raises(ValueError, match="gated parallel stage"):
        group_plan([{"parallel": ["a", "b"]}], ["a"])       # ungated


def test_group_rerun_rejects_empty_and_unknown_selections():
    steps = [{"parallel": ["a", "b"], "hitl": True, "gateId": "g"}]
    with pytest.raises(ValueError, match="no agents selected"):
        group_plan(steps, [])
    with pytest.raises(ValueError, match="unknown agent"):
        group_plan(steps, ["a", "zzz"])
