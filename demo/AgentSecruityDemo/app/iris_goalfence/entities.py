"""Core data types crossing the engine boundary.

Nothing framework-shaped lives here — an Action is built from a Strands tool-call event
by the adapter, but the engine only ever sees these plain dataclasses. That is what makes
the engine testable in isolation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Decision(str, Enum):
    """The four fence outcomes, ordered loosely by severity.

    ALLOW    — the action serves the charter; let it run (adapter: no-op).
    DENY     — this ONE action is out of charter; block it, but the model keeps
               reasoning and may explain itself (adapter: event.cancel_tool).
    ESCALATE — the action is high-impact/irreversible; pause and ask a human, resume on
               approval (adapter: event.interrupt(...)).
    HALT     — stop the agent cold; it must not produce any further response
               (adapter: raise FenceHalt so the server cuts the stream).
    """

    ALLOW = "ALLOW"
    DENY = "DENY"
    ESCALATE = "ESCALATE"
    HALT = "HALT"


# Decisions that stop the action (everything except ALLOW). Convenience for callers.
BLOCKING = frozenset({Decision.DENY, Decision.ESCALATE, Decision.HALT})


class Phase(str, Enum):
    """When in the agent loop the engine is being consulted.

    Rules opt in to the phases they care about — e.g. the record-count rule only has data
    at AFTER_TOOL; the scope/scale/value/sequence rules gate at BEFORE_TOOL.
    """

    BEFORE_INVOKE = "before_invoke"   # once per user turn, before the model runs
    BEFORE_TOOL = "before_tool"       # about to run a tool — the main veto point
    AFTER_TOOL = "after_tool"         # tool has returned — count records, observe results


@dataclass
class Action:
    """One thing the agent is about to do (or just did), normalized for the engine.

    verified_caller is the caller identity the ADAPTER resolved from a trusted source
    (the verified token / invocation_state) — NEVER from the tool arguments. The scope
    rule compares the target in args against this; trusting args would defeat the point.
    """

    tool_name: str                       # bare tool name, e.g. "process_refund"
    args: dict[str, Any] = field(default_factory=dict)
    verified_caller: str | None = None   # resolved identity; None => cannot attribute
    phase: Phase = Phase.BEFORE_TOOL
    result: dict[str, Any] | None = None  # populated only at AFTER_TOOL

    # Optional semantic-drift score for THIS trajectory, computed by the adapter (an
    # embedding cosine distance, once per invoke) and passed through. The engine only
    # compares it to the charter threshold — it never embeds anything itself, so drift
    # stays unit-testable with a plain float. None => drift not evaluated this action.
    drift_score: float | None = None


@dataclass
class TrajectoryState:
    """Everything the engine accumulates across a single trajectory (one agent run).

    Owned and mutated by the engine via observe(); the adapter holds one instance per
    run and resets it on BeforeInvocationEvent.
    """

    action_count: int = 0                              # tool calls seen this trajectory
    records_returned: int = 0                          # cumulative rows returned by tools
    tool_history: list[str] = field(default_factory=list)  # ordered bare tool names
    drift_score: float = 0.0                           # latest trajectory drift score


@dataclass
class Verdict:
    """The engine's answer for one Action."""

    decision: Decision
    rule: str = "none"          # which rule fired: scope|scale|value|sequence|drift|fail_closed
    reason: str = ""            # human-readable; shown in UI and returned to the model
    detail: dict[str, Any] = field(default_factory=dict)  # structured context for the flow panel

    @property
    def blocked(self) -> bool:
        return self.decision in BLOCKING

    # --- constructors, so rules read cleanly -------------------------------------------
    @classmethod
    def allow(cls, rule: str = "none", reason: str = "within charter", **detail) -> "Verdict":
        return cls(Decision.ALLOW, rule, reason, detail)

    @classmethod
    def deny(cls, rule: str, reason: str, **detail) -> "Verdict":
        return cls(Decision.DENY, rule, reason, detail)

    @classmethod
    def escalate(cls, rule: str, reason: str, **detail) -> "Verdict":
        return cls(Decision.ESCALATE, rule, reason, detail)

    @classmethod
    def halt(cls, rule: str, reason: str, **detail) -> "Verdict":
        return cls(Decision.HALT, rule, reason, detail)
