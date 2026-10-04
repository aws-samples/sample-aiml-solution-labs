"""Builds the LangGraph StateGraph from workflow.json.

Each item in "steps" is one of:
  * a single agent            {"agent": "intake"}
  * a "parallel" group        {"parallel": ["knowledge_research", "web_search"]}
  * a "sequence" group        {"sequence": ["analysis", "recommendation"]}
Steps run in order. A parallel group runs its agents concurrently and joins; a
sequence group runs its agents one after another. Any step may carry "hitl": true
for a human-approval gate after it. HITL gates use conditional routing:
approve -> continue, deny -> END, revise -> loop back and re-run with feedback
(a single agent / the whole sequence, or the flagged subset of a parallel group).

    step: {"agent": "intake", "hitl": true}                       -> agent, then a human gate
    step: {"parallel": ["knowledge_research", "web_search"]}     -> both concurrently, join after
    step: {"sequence": ["analysis", "recommendation"]}            -> one after another, one gate after both

A step may also carry "branch", which lets the step's OWN OUTPUT choose what runs
next (see app/common/branching.py for the rule language). It compiles to one extra
node after the step — after its gate, when gated — that evaluates the rules, records
the chosen step, and marks the bypassed agents skipped. The router itself stays a
pure read of state, so routing is asserted directly in the tests.

    step: {"agent": "triage", "branch": {"when": [...], "default": "fast_track"}}
"""

import itertools

from langgraph.graph import END, START, StateGraph

from app.common import branching
from app.common.config import STEPS
from app.common.config import step_agents as agents_in
from app.common.state import State
from app.orchestrator.nodes import (
    make_agent_node,
    make_branch_node,
    make_gate_node,
    make_group_gate_node,
    make_sequence_gate_node,
)
from app.orchestrator.registry import load_agents


def _entries(step: dict) -> list:
    """Node(s) where incoming edges land. A sequence begins at its FIRST agent;
    a parallel group begins at all of its agents; a single step at its agent."""
    if "sequence" in step:
        return [step["sequence"][0]]
    return step.get("parallel") or [step["agent"]]


def _gate_id(step: dict, i: int) -> str:
    """Synthetic decision/gate key for a gated group."""
    return step.get("gateId") or f"group{i}"


def _step_name(step: dict, i: int) -> str:
    """The name a step is addressed by: its `agent` id, or a group's `gateId`.

    This is what a `branch` target names, and what the gate/branch node ids are
    built from, so a group step reachable by a branch wants a readable `gateId`.
    """
    return step.get("agent") or _gate_id(step, i)


def _step_names() -> dict[str, int]:
    return {_step_name(s, i): i for i, s in enumerate(STEPS)}


def _decider(step: dict) -> str:
    """The agent whose output a `branch` on this step reads. A sequence decides with
    its LAST agent; a parallel group has no single decider and is rejected by
    validate_branches."""
    return step.get("agent") or (step.get("sequence") or [""])[-1]


def _branch_node(step: dict, i: int) -> str:
    return f"{_step_name(step, i)}_branch"


def _target_entries(target: str | None) -> list | None:
    """The node(s) a branch target routes to, or None when it names no step.

    A target names a STEP, not an agent within one, so jumping to a parallel group
    enters all of its members rather than stranding the gate waiting on siblings
    that never ran.
    """
    if not target:
        return None
    if target == branching.END_TARGET:
        return [END]
    j = _step_names().get(target)
    return None if j is None else _entries(STEPS[j])


def _bypassed_agents(i: int, target: str | None) -> list[str]:
    """Agents skipped when step `i` branches to `target`: everything in the steps
    jumped over, or everything after step `i` when the branch ends the run."""
    if not target:
        return []
    if target == branching.END_TARGET:
        stop = len(STEPS)
    else:
        j = _step_names().get(target)
        if j is None:
            return []
        stop = j
    return [a for s in STEPS[i + 1: stop] for a in agents_in(s)]


def validate_branches() -> None:
    """Reject a `branch` that cannot work, at container start, naming the step.

    Called by build_graph, and mirrored by both IaC paths so a customer normally
    sees these at plan/synth time instead. Every case below would otherwise be
    silent: a target that names nothing, or a rule that can never match, just makes
    the run take the default forever.
    """
    names = _step_names()
    if any(step.get("branch") for step in STEPS) and len(names) != len(STEPS):
        # A branch target names a step, so two steps sharing a name make a target
        # ambiguous — and _step_names() would silently resolve it to the later one.
        # (A duplicate also collides the gate/branch NODE ids, but only when both
        # steps are gated, so it cannot be relied on to surface this.)
        raise ValueError(
            f"workflow.json `steps` has duplicate step name(s), which makes a `branch` target "
            f"ambiguous. Each step is named by its `agent` id or its `gateId`; give every step a "
            f"distinct one. Names in order: "
            f"{', '.join(_step_name(s, i) for i, s in enumerate(STEPS))}.")
    for i, step in enumerate(STEPS):
        spec = step.get("branch")
        if spec is None:
            continue
        where = f"workflow.json steps[{i}] ({_step_name(step, i)})"
        if "parallel" in step:
            raise ValueError(
                f"{where}: `branch` is not supported on a `parallel` step, because a group has "
                f"no single agent whose output decides. Put the branch on a single-agent step, "
                f"or on a `sequence` step (its LAST agent decides).")
        if i == len(STEPS) - 1:
            raise ValueError(
                f"{where}: `branch` on the LAST step has nowhere to route. Use it on an earlier "
                f'step, or drop it — "END" is already where the last step goes.')
        branching.validate_spec(spec, where)
        for t in branching.targets(spec):
            if t == branching.END_TARGET:
                continue
            j = names.get(t)
            if j is None:
                raise ValueError(
                    f"{where}: branch target {t!r} names no step. A target is "
                    f'"{branching.END_TARGET}", a single-agent step\'s `agent` id, or a group '
                    f"step's `gateId`. Known step names: {', '.join(names)}.")
            if j <= i:
                raise ValueError(
                    f"{where}: branch target {t!r} is step {j}, at or before this one. Targets "
                    f"must be LATER steps — a backward edge is a cycle the run could not leave, "
                    f"and re-running earlier work is what a review gate's `revise` is for.")


def _make_router(decision_key: str, next_entries: list, revise_target):
    """Route one gate's decision: deny -> END, revise -> `revise_target`, else onward.

    `revise_target` is a callable over the state because that is the ONLY thing the
    three gate kinds differ on — a single agent loops back to itself, a sequence to its
    first agent, a parallel group to just the subset the reviewer flagged. This was
    three copies of the same five lines, so deny/onward behaviour could drift between
    gate kinds.
    """
    def router(state: dict):
        decision = (state.get("decisions") or {}).get(decision_key)
        if decision == "deny":
            return END
        if decision == "revise":
            return revise_target(state)
        return END if next_entries == [END] else next_entries
    return router


def _make_branch_router(branch_id: str, fallthrough: list):
    """Route a branch step by the target its branch node recorded in state.

    A pure read on purpose: the decision was made (and logged) by the node, so this
    stays assertable without running anything. No recorded target — no rule matched
    and no `default`, or a seeded value that names no step — falls through to the
    next step, so a branch can only ever redirect a run, never strand it.
    """
    def router(state: dict):
        entries = _target_entries((state.get("branch") or {}).get(branch_id)) or fallthrough
        return END if entries == [END] else entries
    return router


def build_graph(checkpointer=None):
    if checkpointer is None:
        from langgraph.checkpoint.memory import MemorySaver
        checkpointer = MemorySaver()

    validate_branches()
    registry = load_agents()
    g = StateGraph(State)

    # Agent nodes.
    for agent_id, agent in registry.items():
        g.add_node(agent_id, make_agent_node(agent))

    # Gate nodes. A single-agent gate loops back to the one agent on "revise";
    # a parallel-group gate re-runs the flagged agents; a sequence gate re-runs
    # the whole chain from its first agent.
    gate_of: dict[int, str] = {}
    for i, step in enumerate(STEPS):
        if not step.get("hitl"):
            continue
        if "agent" in step:
            aid = step["agent"]
            gname = f"{aid}_gate"
            g.add_node(gname, make_gate_node(aid, registry[aid].name))
        elif "parallel" in step:
            gid = _gate_id(step, i)
            gname = f"{gid}_gate"
            g.add_node(gname, make_group_gate_node(gid, step["parallel"],
                                                   step.get("gateName") or gid))
        else:  # sequence
            gid = _gate_id(step, i)
            gname = f"{gid}_gate"
            g.add_node(gname, make_sequence_gate_node(gid, step["sequence"],
                                                      step.get("gateName") or gid))
        gate_of[i] = gname

    # Branch nodes. One per step carrying "branch": it resolves the decision from the
    # deciding agent's output, records it, and marks the bypassed agents skipped.
    branch_of: dict[int, str] = {}
    for i, step in enumerate(STEPS):
        if not step.get("branch"):
            continue
        bname = _branch_node(step, i)
        g.add_node(bname, make_branch_node(
            _step_name(step, i), step["branch"], _decider(step),
            lambda t, i=i: _bypassed_agents(i, t)))
        branch_of[i] = bname

    # Chain the agents inside every sequence group (agent[k] -> agent[k+1]).
    for step in STEPS:
        seq = step.get("sequence")
        if seq:
            for a, b in itertools.pairwise(seq):
                g.add_edge(a, b)

    # START -> first step's entry node(s).
    for entry in _entries(STEPS[0]):
        g.add_edge(START, entry)

    # Wire each step to the next.
    for i, step in enumerate(STEPS):
        is_last = i == len(STEPS) - 1
        fallthrough = [END] if is_last else _entries(STEPS[i + 1])
        # A branch step points at its branch node instead of the next step; the
        # branch node then routes on. This one substitution is the whole of the
        # branching wiring — the four step shapes below are untouched.
        bname = branch_of.get(i)
        next_entries = [bname] if bname else fallthrough
        hitl = step.get("hitl")

        if "agent" in step and hitl:
            aid = step["agent"]
            gname = gate_of[i]
            g.add_edge(aid, gname)  # agent -> its gate
            # Targets include `aid` so a "revise" decision can loop back to it.
            g.add_conditional_edges(
                gname, _make_router(aid, next_entries, lambda _s, a=aid: a),
                list({*next_entries, END, aid}))
        elif "parallel" in step and hitl:
            # Parallel group with a join gate: every agent -> gate; revise
            # re-runs the flagged agents.
            gid = _gate_id(step, i)
            gname = gate_of[i]
            group_ids = step["parallel"]
            for a in group_ids:
                g.add_edge(a, gname)
            # revise re-runs ONLY the flagged agents; the gate re-runs after them.
            g.add_conditional_edges(
                gname,
                _make_router(gid, next_entries,
                             lambda s, k=gid, ids=group_ids:
                                 (s.get("group_rerun") or {}).get(k) or ids),
                list({*next_entries, END, *group_ids}))
        elif "sequence" in step and hitl:
            # Sequence group with one gate after the LAST agent; revise loops
            # back to the FIRST agent (the whole chain re-runs).
            gid = _gate_id(step, i)
            gname = gate_of[i]
            seq = step["sequence"]
            g.add_edge(seq[-1], gname)
            g.add_conditional_edges(
                gname, _make_router(gid, next_entries, lambda _s, f=seq[0]: f),
                list({*next_entries, END, seq[0]}))
        elif "sequence" in step:
            # Ungated sequence: last agent flows into the next step.
            for ne in next_entries:
                g.add_edge(step["sequence"][-1], ne)
        else:
            # Ungated single agent or parallel group: each agent flows into the
            # next step's entries.
            for a in agents_in(step):
                for ne in next_entries:
                    g.add_edge(a, ne)

        if bname:
            # The branch node's own edges. Every declared target, plus the
            # fallthrough (no rule matched and no `default`) and END.
            reachable = {n for t in branching.targets(step["branch"])
                         for n in (_target_entries(t) or [])}
            g.add_conditional_edges(bname, _make_branch_router(_step_name(step, i), fallthrough),
                                    list({*reachable, *fallthrough, END}))

    return g.compile(checkpointer=checkpointer)


# --- Rewind / "rerun from an agent" planning -------------------------------
#
# Given a target agent, compute how to rewind the graph so that agent (and
# everything downstream) re-runs, WITHOUT restarting the whole workflow.
#
# The mechanism (see runtime._rewind / server._rewind):
#   1. graph.aupdate_state(cfg, values, as_node=<predecessor>) — attributes a
#      state write to the node that routes INTO the target, which makes the
#      graph's pending tasks become the target entry node(s).
#   2. graph.ainvoke(None, cfg) — runs those pending tasks and then cascades
#      forward through every downstream agent and gate. Gate nodes always call
#      interrupt(), so each downstream gate naturally RE-PAUSES for human review.
#
# `values` seeds the predecessor gate's routing decision as "approve" (so the
# router forwards to the target rather than looping/halting) and injects the
# reviewer's feedback into the target agent.


def _step_index_of(agent_id: str) -> int:
    for i, step in enumerate(STEPS):
        if agent_id in agents_in(step):
            return i
    return -1


def _predecessor_of_step(si: int) -> tuple[str, str | None, dict]:
    """(as_node, decision_key, branch_seed) for the node that routes INTO step `si`.

    A gated step is entered from its gate (whose router must be told "approve");
    an ungated one is entered straight from the single node before it.

    When the previous step BRANCHES, the node that routes here is its branch node,
    and its router reads the target recorded in state. So the plan also carries a
    `branch_seed` pointing that decision at this step: the reviewer asked for this
    agent to run, which is a genuine change to the path the run takes, and recording
    it is more honest than routing around it.
    """
    if si == 0:
        return START, None, {}
    prev, pi = STEPS[si - 1], si - 1
    if prev.get("branch"):
        return (_branch_node(prev, pi), None,
                {_step_name(prev, pi): _step_name(STEPS[si], si)})
    if prev.get("hitl"):
        if "agent" in prev:
            return f"{prev['agent']}_gate", prev["agent"], {}
        gid = _gate_id(prev, pi)
        return f"{gid}_gate", gid, {}
    # Ungated predecessor: a sequence ends at its last agent; a single agent is
    # itself. A non-gated parallel group has no single predecessor node, so a
    # rewind across it would be ambiguous — reject it rather than guess.
    if "sequence" in prev:
        return prev["sequence"][-1], None, {}
    prev_agents = agents_in(prev)
    if len(prev_agents) != 1:
        raise ValueError(
            "cannot rewind across the previous step: it is a non-gated parallel "
            "group with no single predecessor node")
    return prev_agents[0], None, {}


def rerun_plan(agent_id: str) -> dict:
    """Plan a rewind so `agent_id` re-runs and cascades downstream.

    Returns:
      as_node          - node to attribute the state write to (its successors
                         become the next tasks): START for the first step, the
                         previous step's gate or branch node, the node before it in
                         a sequence, or the single ungated predecessor.
      decision_key     - decision to force to "approve" so the predecessor gate's
                         router forwards here (None when there is no gate).
      branch_seed      - branch decision(s) to write so a predecessor BRANCH routes
                         here ({} when the predecessor does not branch).
      step_agents      - the agents that will re-run in the target step. A
                         parallel group re-runs as a unit; a sequence re-runs from
                         the target agent onward.
      downstream_agents- step_agents + all agents after the target step (for UI reset).
      step_index       - index of the target step.

    Raises ValueError for an unknown agent, or a rewind that would cross a
    non-gated parallel step (ambiguous single predecessor).
    """
    si = _step_index_of(agent_id)
    if si < 0:
        raise ValueError(f"unknown agent '{agent_id}'")

    step = STEPS[si]
    later_agents: list[str] = []
    for s in STEPS[si + 1:]:
        later_agents += agents_in(s)

    seq = step.get("sequence")
    if seq and agent_id != seq[0]:
        # Mid-sequence target: enter from the agent immediately before it, and
        # re-run the tail of the chain from there.
        k = seq.index(agent_id)
        step_agents = seq[k:]
        return {"as_node": seq[k - 1], "decision_key": None, "branch_seed": {},
                "step_agents": step_agents,
                "downstream_agents": step_agents + later_agents,
                "step_index": si}

    # The target is the step's entry: a single agent, any member of a parallel
    # group (the stage re-runs as a unit), or the first agent of a sequence.
    step_agents = agents_in(step)
    as_node, decision_key, branch_seed = _predecessor_of_step(si)
    return {"as_node": as_node, "decision_key": decision_key, "branch_seed": branch_seed,
            "step_agents": step_agents,
            "downstream_agents": step_agents + later_agents,
            "step_index": si}


def group_rerun_plan(agent_ids: list[str]) -> dict:
    """Plan a rewind that re-runs a SUBSET of a gated parallel stage.

    Used for the post-completion "re-run these agents" flow: the user picks 2+
    agents from one parallel stage, only those re-run in parallel, then the
    stage's review gate re-pauses so the refreshed outputs can be reviewed before
    the workflow cascades downstream.

    Mechanism: this reuses the group gate's existing subset routing. We seed the
    gate's decision to "revise" with group_rerun=<subset>, so the group router
    (_make_router above) loops back to exactly that subset. The rewind
    therefore attributes its state write to the group's OWN gate node.

    Returns:
      as_node          - the group gate node (its router forwards to the subset).
      group_id         - the synthetic gate/group id (e.g. "research").
      subset           - the selected agents to re-run (validated as a subset).
      downstream_agents- subset + every agent in later steps (for UI reset). The
                         non-selected siblings in the SAME stage keep their output.
      step_index       - index of the target parallel stage.

    Raises ValueError if the agents are empty, span more than one step, or the
    target step is not a gated parallel group.
    """
    ids = [a for a in dict.fromkeys(agent_ids or []) if a]  # de-dup, keep order
    if not ids:
        raise ValueError("no agents selected to re-run")

    indices = {_step_index_of(a) for a in ids}
    if -1 in indices:
        bad = [a for a in ids if _step_index_of(a) < 0]
        raise ValueError(f"unknown agent(s): {', '.join(bad)}")
    if len(indices) != 1:
        raise ValueError("selected agents must all belong to the same stage")

    si = indices.pop()
    step = STEPS[si]
    if "parallel" not in step or not step.get("hitl"):
        raise ValueError("multi-agent re-run requires a gated parallel stage")

    group_id = _gate_id(step, si)
    downstream_agents: list[str] = list(ids)
    for s in STEPS[si + 1:]:
        downstream_agents += agents_in(s)

    return {"as_node": f"{group_id}_gate", "group_id": group_id,
            "subset": ids, "downstream_agents": downstream_agents,
            "step_index": si}
