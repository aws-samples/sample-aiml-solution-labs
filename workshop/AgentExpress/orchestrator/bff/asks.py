"""Answering an agent that asked a person mid-step (app/common/asks.py), on the BFF.

A waiting run's `hitl` says what it waits for: a review gate has no `kind`; an agent's
question has kind "question", a tool call waiting for approval "toolApproval". Both are
answered through the existing decision route, so its contract for a gate does not change:

    question       {"decision": "approve", "comment": "<the answer>"}   or  {"decision": "deny"} to decline
    toolApproval   {"decision": "approve" | "deny", "comment": "<an optional note>"}

`revise` and a per-agent `decisions` map are gate decisions, so they are refused here, and
the answer is capped (the runtime caps it again before the model reads it). The BFF ships
as its own zip and cannot import app.*, so the few values it shares are repeated here and
held to the runtime's by tests/test_asks.py.
"""
from __future__ import annotations

KINDS = ("question", "toolApproval")
#: The longest answer or note accepted. app/common/asks.py ANSWER_CHARS.
ANSWER_MAX = 4000
#: tools.<name>.approval values. app/common/asks.py APPROVALS.
APPROVALS = ("person", "event")


def enabled(raw: dict) -> bool:
    """Whether a run of this workflow can wait on anything but a gate. When not, the
    decision route reads nothing more than it did before."""
    agents = (raw or {}).get("agents") or {}
    tools = (raw or {}).get("tools") or {}
    return (any(isinstance(a, dict) and (a.get("askPerson") is True or isinstance(a.get("askPerson"), dict))
                for a in agents.values())
            or any(isinstance(t, dict) and t.get("approval") in APPROVALS for t in tools.values()))


def waiting(status_table, sid: str) -> dict:
    """The run's `hitl` ({} when it waits for nothing)."""
    item = status_table.get_item(Key={"session_id": sid}, ProjectionExpression="hitl").get("Item") or {}
    return dict(item.get("hitl") or {})


def refusal(body: dict, hitl: dict) -> str | None:
    """Why this decision cannot answer what the run waits for, or None when it can (and
    always None when the run waits at a gate, which keeps the route's checks as they were)."""
    kind = (hitl or {}).get("kind")
    if kind not in KINDS:
        return None
    what = "a question from an agent" if kind == "question" else "a person to approve a tool call"
    if body.get("decisions"):
        return (f"this run is waiting for {what}, not at a parallel gate: send "
                f'{{"decision": "approve" | "deny", "comment": "..."}}')
    decision = body.get("decision")
    if decision not in ("approve", "deny"):
        return (f"this run is waiting for {what}: the decision is approve or deny"
                + (" (approve with your answer in `comment`, deny to decline)" if kind == "question" else ""))
    comment = body.get("comment", "")
    if comment is None:
        comment = ""
    if not isinstance(comment, str):
        return "`comment` must be text"
    if len(comment) > ANSWER_MAX:
        return f"`comment` is at most {ANSWER_MAX} characters"
    if kind == "question" and decision == "approve" and not comment.strip():
        return "an answer needs text: put it in `comment`, or send decision deny to decline the question"
    return None


def audit_action(hitl: dict) -> str:
    """What a decision is called in the activity log."""
    kind = (hitl or {}).get("kind")
    return {"question": "run.answered", "toolApproval": "run.tool_decided"}.get(kind, "run.decided")
