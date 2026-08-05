"""Standalone demo of the goal fence — no agent, no AWS, no network.

Runs a scripted trajectory of actions through the engine (exactly as the Strands adapter
will later) and prints the verdict for each, so you can SEE the six dimensions fire before
any of this is wired into the agent.

    python iris_goalfence/demo.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from iris_goalfence import Action, Charter, Decision, GoalFenceEngine, TrajectoryState
from iris_goalfence.entities import Phase

CHARTER = Charter(
    goal="Answer the signed-in customer's questions about their own orders, shipments, "
         "and refunds — one customer, small result sets.",
    max_actions=5,
    max_records=100,
    value_caps={"process_refund": 500},
    forbidden_sequences=(("get_record", "update_record"),),
    scope_exempt_tools=frozenset({"get_my_info"}),
    drift_threshold=0.45,
    drift_action="ESCALATE",
    fail_closed=True,
)

CALLER = "C-1001"

# (label, Action) — each row is one thing the agent tries to do. Sweep rows share a state.
SCENARIOS = [
    ("within charter — own order lookup",
     Action("get_record", {"order_id": "A1"}, CALLER, Phase.BEFORE_TOOL)),
    ("SCOPE — refund another customer's order",
     Action("process_refund", {"order_id": "Z9Q2P", "customer_id": "C-1002"}, CALLER, Phase.BEFORE_TOOL)),
    ("VALUE — $9,999 refund on own order (escalate)",
     Action("process_refund", {"order_id": "A1", "amount": 9999}, CALLER, Phase.BEFORE_TOOL)),
    ("SCALE — a bulk-shaped request",
     Action("get_record", {"filter": "export all orders"}, CALLER, Phase.BEFORE_TOOL)),
    ("DRIFT — trajectory diverged from goal",
     Action("get_record", {"order_id": "A1"}, CALLER, Phase.BEFORE_TOOL, drift_score=0.82)),
    ("FAIL-CLOSED — no verified caller",
     Action("get_record", {"order_id": "A1"}, "", Phase.BEFORE_TOOL)),
]

_COLOR = {
    Decision.ALLOW: "\033[32m", Decision.DENY: "\033[31m",
    Decision.ESCALATE: "\033[33m", Decision.HALT: "\033[35m",
}
_RESET = "\033[0m"


def _line(label, verdict):
    c = _COLOR.get(verdict.decision, "")
    print(f"  {c}{verdict.decision.value:<9}{_RESET} [{verdict.rule:<11}] {label}")
    if verdict.decision is not Decision.ALLOW:
        print(f"            └─ {verdict.reason}")


def main():
    eng = GoalFenceEngine(CHARTER)
    print(f"\nCharter goal: {CHARTER.goal}")
    print(f"caps: max_actions={CHARTER.max_actions} value={CHARTER.value_caps} "
          f"drift>{CHARTER.drift_threshold}\n")

    print("── Independent actions ─────────────────────────────────────────")
    for label, action in SCENARIOS:
        _line(label, eng.evaluate(TrajectoryState(action_count=1), action))

    print("\n── Cumulative bulk sweep (SCALE, cap 5) ────────────────────────")
    state = TrajectoryState()
    for i in range(1, 8):
        a = Action("order_lookup", {"customer_id": CALLER}, CALLER, Phase.BEFORE_TOOL)
        v = eng.evaluate(state, a)
        _line(f"lookup #{i}", v)
        if v.decision is Decision.ALLOW:
            eng.observe(state, a)
        else:
            break
    print()


if __name__ == "__main__":
    main()
