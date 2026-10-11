"""A reviewer's note is the reason for a deny or a revise, so the gate decision's Timeline
line carries it. Before, the line read only "Human decision (intake): deny" and the note
was nowhere in the run's story (only in the audit record)."""
from __future__ import annotations

import asyncio

import pytest


@pytest.fixture()
def gate_env(monkeypatch):
    from app.orchestrator import nodes
    emitted: list[dict] = []

    async def emit(sid, ev):
        emitted.append(ev)
    monkeypatch.setattr(nodes, "emit", emit)
    monkeypatch.setattr(nodes, "is_cancelled", lambda sid: False)

    def resume_with(payload):
        monkeypatch.setattr(nodes, "interrupt", lambda _request: payload)
    return nodes, emitted, resume_with


def _resolved(emitted):
    return next(e["log"] for e in emitted if e["type"] == "hitl_resolved")


CFG = {"configurable": {"thread_id": "s1"}}


def test_a_deny_on_one_agent_names_the_reason(gate_env):
    nodes, emitted, resume_with = gate_env
    resume_with({"decision": "deny", "comment": "Personal trip,\n not covered by the policy."})
    got = asyncio.run(nodes.make_gate_node("intake", "Intake")({"outputs": {"intake": "{}"}}, CFG))
    assert got["decisions"] == {"intake": "deny"}
    assert _resolved(emitted) == ('Human decision (intake): deny \u2014 note: '
                                  '"Personal trip, not covered by the policy."')


def test_an_approval_without_a_note_reads_as_before(gate_env):
    nodes, emitted, resume_with = gate_env
    resume_with({"decision": "approve", "comment": "  "})
    asyncio.run(nodes.make_gate_node("intake", "Intake")({"outputs": {"intake": "{}"}}, CFG))
    assert _resolved(emitted) == "Human decision (intake): approve"


def test_a_sequence_gate_keeps_its_note(gate_env):
    nodes, emitted, resume_with = gate_env
    resume_with({"decision": "revise", "comment": "Shorter, please"})
    asyncio.run(nodes.make_sequence_gate_node("ar", ["a", "r"], "Analysis")({"outputs": {"r": "{}"}}, CFG))
    assert _resolved(emitted) == 'Analysis review: revise \u2014 note: "Shorter, please"'


def test_a_parallel_gate_names_each_agents_note(gate_env):
    nodes, emitted, resume_with = gate_env
    resume_with({"decisions": {"a": {"decision": "revise", "comment": "Cite the fare"},
                               "b": {"decision": "approve", "comment": ""}}})
    asyncio.run(nodes.make_group_gate_node("g", ["a", "b"], "Research")({"outputs": {}}, CFG))
    assert _resolved(emitted) == 'Research review: re-run a \u2014 note: "a: Cite the fare"'


def test_a_long_note_is_cut_on_the_timeline(gate_env):
    nodes, emitted, resume_with = gate_env
    resume_with({"decision": "deny", "comment": "x" * 1000})
    asyncio.run(nodes.make_gate_node("intake", "Intake")({"outputs": {}}, CFG))
    assert _resolved(emitted).endswith('"' + "x" * nodes.NOTE_MAX + '"')
