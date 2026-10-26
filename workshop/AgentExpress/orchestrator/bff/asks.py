"""Answering an agent that asked a person mid-step (app/common/asks.py), on the BFF.

A waiting run's `hitl` says what it waits for: a review gate has no `kind`; an agent's
question has kind "question", a tool call waiting for approval "toolApproval", and an
agent's request for a decision "decision". All are answered through the existing decision
route, so its contract for a gate does not change:

    question       {"decision": "approve", "comment": "<the answer>"}   or  {"decision": "deny"} to decline
    toolApproval   {"decision": "approve" | "deny", "comment": "<an optional note>"}
    decision       {"option": "<one of hitl.options>", "comment": "<an optional comment>"}

A review gate with named options (steps[].hitl.options, v139 Track D) has no `kind` but carries
`options`, and is answered like a decision: {"option": ..., "comment": ...}.

`revise` and a per-agent `decisions` map are gate decisions, so they are refused here, and
the answer is capped (the runtime caps it again before the model reads it). The BFF ships
as its own zip and cannot import app.*, so the few values it shares are repeated here and
held to the runtime's by tests/test_asks.py.
"""
from __future__ import annotations

KINDS = ("question", "toolApproval", "decision")
#: The longest answer or note accepted. app/common/asks.py ANSWER_CHARS.
ANSWER_MAX = 4000
#: tools.<name>.approval values. app/common/asks.py APPROVALS.
APPROVALS = ("person", "event")


def enabled(raw: dict) -> bool:
    """Whether a run of this workflow can wait on anything but a plain gate. When not, the
    decision route reads nothing more than it did before."""
    agents = (raw or {}).get("agents") or {}
    tools = (raw or {}).get("tools") or {}
    steps = (raw or {}).get("steps") or []
    return (any(isinstance(a, dict) and (a.get("askPerson") is True or isinstance(a.get("askPerson"), dict))
                for a in agents.values())
            or any(isinstance(t, dict) and t.get("approval") in APPROVALS for t in tools.values())
            # v139 Track D: a gate with named options.
            or any(isinstance(s, dict) and isinstance(s.get("hitl"), dict) and s["hitl"].get("options")
                   for s in steps))


def waiting(status_table, sid: str) -> dict:
    """The run's `hitl` ({} when it waits for nothing)."""
    item = status_table.get_item(Key={"session_id": sid}, ProjectionExpression="hitl").get("Item") or {}
    return dict(item.get("hitl") or {})


def refusal(body: dict, hitl: dict) -> str | None:
    """Why this decision cannot answer what the run waits for, or None when it can (and
    always None when the run waits at a plain gate, which keeps the route's checks as they were)."""
    kind = (hitl or {}).get("kind")
    if kind == "decision":
        return _option_refusal(body, hitl, "an agent's request for a decision")
    if kind not in KINDS:
        return _option_refusal(body, hitl, "a choice at a review gate") if (hitl or {}).get("options") else None
    what = "a question from an agent" if kind == "question" else "a person to approve a tool call"
    if body.get("decisions"):
        return (f"this run is waiting for {what}, not at a parallel gate: send "
                f'{{"decision": "approve" | "deny", "comment": "..."}}')
    decision = body.get("decision")
    if decision not in ("approve", "deny"):
        return (f"this run is waiting for {what}: the decision is approve or deny"
                + (" (approve with your answer in `comment`, deny to decline)" if kind == "question" else ""))
    bad = _comment_refusal(body)
    if bad:
        return bad
    if kind == "question" and decision == "approve" and not str(body.get("comment") or "").strip():
        return "an answer needs text: put it in `comment`, or send decision deny to decline the question"
    return None


def _comment_refusal(body: dict) -> str | None:
    comment = body.get("comment", "")
    if comment is None:
        comment = ""
    if not isinstance(comment, str):
        return "`comment` must be text"
    if len(comment) > ANSWER_MAX:
        return f"`comment` is at most {ANSWER_MAX} characters"
    return None


# --- begin v139 Track D (decide): named options ------------------------------------------

def chosen_option(body: dict, hitl: dict) -> str:
    """The option this decision chose, spelled as the run offered it, or "" when it chose
    none or the run offers no options."""
    want = body.get("option")
    if not isinstance(want, str) or not want.strip():
        return ""
    want = want.strip().casefold()
    return next((str(o) for o in ((hitl or {}).get("options") or []) if str(o).strip().casefold() == want), "")


def _option_refusal(body: dict, hitl: dict, what: str) -> str | None:
    options = [str(o) for o in ((hitl or {}).get("options") or [])]
    shape = f'{{"option": one of {", ".join(repr(o) for o in options)}, "comment": "..."}}'
    if body.get("decisions") or body.get("decision") not in (None, "", "approve"):
        return f"this run is waiting for {what}: choose one of its options, send {shape}"
    if not chosen_option(body, hitl):
        return f"`option` must be one of: {', '.join(options)}"
    return _comment_refusal(body)
# --- end v139 Track D (decide) -----------------------------------------------------------


def audit_action(hitl: dict) -> str:
    """What a decision is called in the activity log."""
    kind = (hitl or {}).get("kind")
    return {"question": "run.answered", "toolApproval": "run.tool_decided"}.get(kind, "run.decided")
