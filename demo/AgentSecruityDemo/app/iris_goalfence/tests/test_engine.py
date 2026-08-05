"""Standalone tests for the goal-fence engine — no Strands, no AWS, no network.

Run from the app dir:
    python -m pytest iris_goalfence/tests -q
or without pytest installed:
    python iris_goalfence/tests/test_engine.py
"""
from __future__ import annotations

import os
import sys

# Allow running as a plain script (python tests/test_engine.py) by putting the app dir
# (which contains the iris_goalfence package) on the path.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from iris_goalfence import (  # noqa: E402
    Action, Charter, Decision, GoalFenceEngine, TrajectoryState,
)
from iris_goalfence.entities import Phase  # noqa: E402


# A representative charter exercising every dimension.
CHARTER = Charter(
    goal="Answer the signed-in customer's questions about their own orders, shipments, "
         "and refunds — one customer, small result sets.",
    max_actions=5,
    max_records=100,
    value_caps={"process_refund": 500},
    forbidden_sequences=(("get_record", "update_record"),),
    scope_exempt_tools=frozenset({"get_my_info"}),
    drift_threshold=0.45,
    drift_action="DENY",
    fail_closed=True,
)


def _engine():
    return GoalFenceEngine(CHARTER)


def _act(tool, args=None, caller="C-1001", phase=Phase.BEFORE_TOOL, **kw):
    return Action(tool_name=tool, args=args or {}, verified_caller=caller, phase=phase, **kw)


# -- ALLOW: the happy path --------------------------------------------------------------
def test_within_charter_allows():
    v = _engine().evaluate(TrajectoryState(), _act("get_record", {"order_id": "A1"}))
    assert v.decision is Decision.ALLOW


def test_own_customer_id_allowed():
    # Passing your OWN id is fine.
    v = _engine().evaluate(TrajectoryState(), _act("order_lookup", {"customer_id": "C-1001"}))
    assert v.decision is Decision.ALLOW


# -- FAIL-CLOSED ------------------------------------------------------------------------
def test_missing_caller_halts():
    v = _engine().evaluate(TrajectoryState(), _act("get_record", {"order_id": "A1"}, caller=""))
    assert v.decision is Decision.HALT
    assert v.rule == "fail_closed"


def test_missing_caller_allowed_when_not_failclosed():
    charter = Charter(fail_closed=False)
    v = GoalFenceEngine(charter).evaluate(TrajectoryState(), _act("get_record", caller=""))
    assert v.decision is Decision.ALLOW


# -- SCOPE ------------------------------------------------------------------------------
def test_cross_actor_denied():
    v = _engine().evaluate(
        TrajectoryState(),
        _act("order_lookup", {"customer_id": "C-1002"}, caller="C-1001"))
    assert v.decision is Decision.DENY
    assert v.rule == "scope"
    assert v.detail["target"] == "C-1002"


def test_scope_exempt_tool_ignores_target_arg():
    # get_my_info acts only on the caller; a stray customer_id must not trip scope.
    v = _engine().evaluate(
        TrajectoryState(), _act("get_my_info", {"customer_id": "C-1002"}))
    assert v.decision is Decision.ALLOW


# -- SCALE ------------------------------------------------------------------------------
def test_sixth_call_halts():
    # 5 calls already made; this would be the 6th -> over cap -> HALT.
    v = _engine().evaluate(TrajectoryState(action_count=5),
                           _act("order_lookup", {"customer_id": "C-1001"}))
    assert v.decision is Decision.HALT
    assert v.rule == "scale"
    assert v.detail["action_count"] == 6


def test_fifth_call_still_allowed():
    v = _engine().evaluate(TrajectoryState(action_count=4),
                           _act("order_lookup", {"customer_id": "C-1001"}))
    assert v.decision is Decision.ALLOW


def test_bulk_marker_denied():
    v = _engine().evaluate(TrajectoryState(), _act("get_record", {"filter": "all orders"}))
    assert v.decision is Decision.DENY
    assert v.rule == "scale"
    assert v.detail["marker"] == "all"


def test_over_limit_number_denied():
    v = _engine().evaluate(TrajectoryState(), _act("get_record", {"limit": 5000}))
    assert v.decision is Decision.DENY
    assert v.rule == "scale"
    assert v.detail["requested"] == 5000


def test_small_number_allowed():
    v = _engine().evaluate(TrajectoryState(), _act("get_record", {"order_id": "A12"}))
    assert v.decision is Decision.ALLOW


# -- VALUE (escalate to human) ----------------------------------------------------------
def test_over_cap_refund_escalates():
    v = _engine().evaluate(
        TrajectoryState(action_count=1),
        _act("process_refund", {"order_id": "A1", "amount": 9999}))
    assert v.decision is Decision.ESCALATE
    assert v.rule == "value"
    assert v.detail["amount"] == 9999


def test_under_cap_refund_allowed():
    v = _engine().evaluate(
        TrajectoryState(), _act("process_refund", {"order_id": "A1", "amount": 50}))
    assert v.decision is Decision.ALLOW


def test_refund_without_amount_allowed():
    v = _engine().evaluate(TrajectoryState(), _act("process_refund", {"order_id": "A1"}))
    assert v.decision is Decision.ALLOW


# -- SEQUENCE ---------------------------------------------------------------------------
def test_forbidden_sequence_denied():
    # history ends with get_record; this call is update_record -> completes the pattern.
    state = TrajectoryState(action_count=1, tool_history=["get_record"])
    v = _engine().evaluate(state, _act("update_record", {"field": "email", "value": "x@y.z"}))
    assert v.decision is Decision.DENY
    assert v.rule == "sequence"


def test_update_alone_allowed():
    v = _engine().evaluate(TrajectoryState(), _act("update_record", {"field": "phone"}))
    assert v.decision is Decision.ALLOW


# -- DRIFT ------------------------------------------------------------------------------
def test_drift_over_threshold_denied():
    v = _engine().evaluate(TrajectoryState(),
                           _act("get_record", {"order_id": "A1"}, drift_score=0.8))
    assert v.decision is Decision.DENY
    assert v.rule == "drift"


def test_drift_under_threshold_allowed():
    v = _engine().evaluate(TrajectoryState(),
                           _act("get_record", {"order_id": "A1"}, drift_score=0.1))
    assert v.decision is Decision.ALLOW


def test_drift_escalate_variant():
    charter = Charter(drift_threshold=0.45, drift_action="ESCALATE", fail_closed=True)
    v = GoalFenceEngine(charter).evaluate(
        TrajectoryState(), _act("get_record", {"order_id": "A1"}, drift_score=0.9))
    assert v.decision is Decision.ESCALATE


# -- PRECEDENCE: fail_closed beats everything, scope beats scale ------------------------
def test_precedence_failclosed_first():
    # No caller AND a bulk marker: fail_closed (HALT) should win over scale (DENY).
    v = _engine().evaluate(TrajectoryState(), _act("get_record", {"q": "all"}, caller=""))
    assert v.decision is Decision.HALT
    assert v.rule == "fail_closed"


def test_precedence_scope_before_scale():
    # Cross-actor AND would be the 6th call: scope (DENY) is checked before scale (HALT).
    v = _engine().evaluate(TrajectoryState(action_count=5),
                           _act("order_lookup", {"customer_id": "C-1002"}))
    assert v.rule == "scope"


# -- observe(): accumulation ------------------------------------------------------------
def test_observe_counts_actions_and_history():
    eng = _engine()
    state = TrajectoryState()
    eng.observe(state, _act("get_record", {"order_id": "A1"}))
    eng.observe(state, _act("order_lookup", {"customer_id": "C-1001"}))
    assert state.action_count == 2
    assert state.tool_history == ["get_record", "order_lookup"]


def test_observe_counts_records():
    eng = _engine()
    state = TrajectoryState()
    eng.observe(state, _act("order_lookup", phase=Phase.AFTER_TOOL,
                            args={}, ))  # result set below
    # separately with a result payload
    a = _act("order_lookup", phase=Phase.AFTER_TOOL)
    a.result = {"orders": [1, 2, 3]}
    eng.observe(state, a)
    assert state.records_returned == 3


def test_observe_then_evaluate_trips_cap():
    # Simulate a real sweep: observe 5 admitted calls, then the 6th must HALT.
    eng = _engine()
    state = TrajectoryState()
    for _ in range(5):
        nxt = _act("order_lookup", {"customer_id": "C-1001"})
        assert eng.evaluate(state, nxt).decision is Decision.ALLOW
        eng.observe(state, nxt)
    v = eng.evaluate(state, _act("order_lookup", {"customer_id": "C-1001"}))
    assert v.decision is Decision.HALT


# -- tiny runner so the file works without pytest ---------------------------------------
if __name__ == "__main__":
    fns = [f for name, f in sorted(globals().items())
           if name.startswith("test_") and callable(f)]
    passed = failed = 0
    for fn in fns:
        try:
            fn()
            passed += 1
            print(f"  ok   {fn.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"  FAIL {fn.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"  ERR  {fn.__name__}: {e!r}")
    print(f"\n{passed} passed, {failed} failed, {len(fns)} total")
    sys.exit(1 if failed else 0)
