"""GoalFenceEngine — the pure decision core.

Holds a Charter and an ordered rule pipeline. Two responsibilities, both side-effect-free
with respect to the outside world:

    evaluate(state, action) -> Verdict
        Run the rules in order; the FIRST blocking verdict (DENY/ESCALATE/HALT) wins. If
        none fire, ALLOW. A rule that raises is treated per the charter's fail-closed
        posture (HALT if fail_closed, else ignored) — a buggy rule must never silently
        open the fence.

    observe(state, action)
        Fold an action into the trajectory state (counts, history, record totals, drift).
        The engine owns accumulation so rules can stay pure; the adapter calls observe()
        once the action is admitted (and after a tool returns, to count records).

The engine imports nothing but its own types + rules — no Strands, no boto3, no network.
That is the whole point: it is testable with literals.
"""
from __future__ import annotations

from collections.abc import Callable, Sequence

from .charter import Charter
from .rules import DEFAULT_RULES
from .entities import Action, Decision, Phase, TrajectoryState, Verdict

Rule = Callable[[Charter, TrajectoryState, Action], "Verdict | None"]


class GoalFenceEngine:
    def __init__(self, charter: Charter, rules: Sequence[Rule] | None = None):
        self.charter = charter
        self.rules: tuple[Rule, ...] = tuple(rules) if rules is not None else DEFAULT_RULES

    def new_state(self) -> TrajectoryState:
        """A fresh trajectory state — one per agent run (call at BeforeInvocationEvent)."""
        return TrajectoryState()

    # -- decision -----------------------------------------------------------------------
    def evaluate(self, state: TrajectoryState, action: Action) -> Verdict:
        """Decide the fate of one action. First blocking rule wins; else ALLOW."""
        for rule in self.rules:
            try:
                verdict = rule(self.charter, state, action)
            except Exception as e:  # a broken rule must fail CLOSED, never open
                if self.charter.fail_closed:
                    return Verdict.halt(
                        getattr(rule, "__name__", "rule"),
                        f"fence rule error ({e!r}) — failing closed and stopping the agent.",
                        error=repr(e),
                    )
                continue
            if verdict is not None and verdict.decision is not Decision.ALLOW:
                return verdict
        return Verdict.allow()

    # -- accumulation -------------------------------------------------------------------
    def observe(self, state: TrajectoryState, action: Action) -> TrajectoryState:
        """Fold an action into the trajectory state. Call ONCE per admitted action.

        At BEFORE_TOOL: increment the call count + append to tool history (this is what
        the scale/sequence rules read on the NEXT call). At AFTER_TOOL: add the number of
        records the tool returned. Drift score, when provided, is latched onto the state.
        """
        if action.phase == Phase.BEFORE_TOOL:
            state.action_count += 1
            state.tool_history.append(action.tool_name)
        elif action.phase == Phase.AFTER_TOOL:
            state.records_returned += _count_records(action.result)
        if action.drift_score is not None:
            state.drift_score = action.drift_score
        return state


def _count_records(result: dict | None) -> int:
    """Best-effort count of records in a tool result. Conservative: unknown shape => 0,
    so the record cap never trips on something we couldn't actually measure."""
    if not isinstance(result, dict):
        return 0
    for key in ("records", "orders", "items", "results", "rows", "data"):
        val = result.get(key)
        if isinstance(val, list):
            return len(val)
    return 0
