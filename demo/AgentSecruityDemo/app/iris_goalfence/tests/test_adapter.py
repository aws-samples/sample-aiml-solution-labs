"""Adapter behavior tests — verify the enforcement mapping WITHOUT a Strands runtime.

We can't import ag_ui_strands/strands here, so we test the adapter's decision-to-event
logic directly: build the hook, hand it a FAKE event (mimicking BeforeToolCallEvent's
duck-typed surface: .tool_use, .cancel_tool, .agent), and assert what the adapter does.

Focus: the NEW hard-stop wiring —
  DENY     -> cancel_tool set, agent.cancel() NOT called
  HALT     -> cancel_tool set AND agent.cancel() called (hard stop), halted=True
  ESCALATE -> treated as DENY (cancel_tool set, agent.cancel() NOT called; HITL dropped)
  ALLOW    -> nothing set, observe() advances the trajectory

    python3 iris_goalfence/tests/test_adapter.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# The adapter imports strands at module load. Stub the two symbols it needs so the import
# succeeds in a bare environment (we never invoke real Strands here).
import types as _types  # noqa: E402

if "strands" not in sys.modules:
    strands = _types.ModuleType("strands")
    hooks = _types.ModuleType("strands.hooks")
    class _Evt:  # minimal stand-ins; only used as callback keys
        pass
    hooks.HookProvider = object
    hooks.HookRegistry = object
    hooks.BeforeToolCallEvent = _Evt
    hooks.BeforeModelCallEvent = _Evt
    hooks.BeforeInvocationEvent = _Evt
    strands.hooks = hooks
    sys.modules["strands"] = strands
    sys.modules["strands.hooks"] = hooks

from iris_goalfence import Charter, GoalFenceEngine  # noqa: E402
from iris_goalfence.adapter_strands import GoalFenceHook  # noqa: E402

CHARTER = Charter(
    goal="Answer the caller's own order questions — one customer, small result sets.",
    max_actions=5, value_caps={"process_refund": 500},
    forbidden_sequences=(("get_record", "update_record"),),
    scope_exempt_tools=frozenset({"get_my_info", "get_record", "get_shipment",
                                  "process_refund", "update_record"}),
    drift_threshold=0.5, fail_closed=True,
)


class FakeAgent:
    def __init__(self):
        self.cancelled = False

    def cancel(self):
        self.cancelled = True


class FakeEvent:
    """Duck-typed BeforeToolCallEvent."""
    def __init__(self, name, args, agent):
        self.tool_use = {"name": name, "input": args}
        self.cancel_tool = False
        self.agent = agent


def _hook(caller="C-1001", drift=None):
    return GoalFenceHook(GoalFenceEngine(CHARTER), verified_caller=caller, drift_score=drift)


def _fire(hook, name, args, agent):
    ev = FakeEvent(name, args, agent)
    hook._gate(ev)
    return ev


def test_allow_sets_nothing_and_advances():
    hook, agent = _hook(), FakeAgent()
    ev = _fire(hook, "get_record", {"order_id": "A1"}, agent)
    assert ev.cancel_tool is False
    assert agent.cancelled is False
    assert hook.state.action_count == 1          # observe() advanced
    assert hook.verdicts[-1]["decision"] == "ALLOWED"


def test_deny_scope_soft_block_no_cancel():
    hook, agent = _hook(), FakeAgent()
    ev = _fire(hook, "order_lookup", {"customer_id": "C-1002"}, agent)
    assert ev.cancel_tool  # message set
    assert agent.cancelled is False              # DENY does NOT hard-stop
    assert hook.verdicts[-1]["kind"] == "DENY"
    assert hook.verdicts[-1]["rule"] == "scope"


def test_halt_scale_hard_stops_agent():
    hook, agent = _hook(), FakeAgent()
    # advance to the cap so the next call is the 6th -> HALT (scale)
    hook.state.action_count = 5
    ev = _fire(hook, "order_lookup", {"order_id": "A1"}, agent)
    assert ev.cancel_tool
    assert agent.cancelled is True               # HALT hard-stops the whole run
    assert hook.halted is True
    assert hook.verdicts[-1]["kind"] == "HALT"


def test_halt_failclosed_hard_stops():
    hook, agent = _hook(caller=""), FakeAgent()
    _fire(hook, "get_record", {"order_id": "A1"}, agent)
    assert agent.cancelled is True
    assert hook.verdicts[-1]["rule"] == "fail_closed"


def test_escalate_treated_as_deny_no_hardstop():
    hook, agent = _hook(), FakeAgent()
    ev = _fire(hook, "process_refund", {"order_id": "A1", "amount": 9999}, agent)
    assert ev.cancel_tool
    assert "human approval" in ev.cancel_tool.lower()   # explains, but…
    assert agent.cancelled is False                     # …no hard stop (HITL dropped)
    assert hook.halted is False
    assert hook.verdicts[-1]["kind"] == "ESCALATE"


def test_missing_agent_handle_is_safe():
    # If the event somehow has no .agent, HALT must still cancel the tool and not crash.
    hook = _hook()
    ev = FakeEvent("order_lookup", {"order_id": "A1"}, agent=None)
    hook.state.action_count = 5
    hook._gate(ev)
    assert ev.cancel_tool
    assert hook.halted is True


if __name__ == "__main__":
    fns = [f for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    passed = failed = 0
    for fn in fns:
        try:
            fn(); passed += 1; print(f"  ok   {fn.__name__}")
        except AssertionError as e:
            failed += 1; print(f"  FAIL {fn.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            failed += 1; print(f"  ERR  {fn.__name__}: {e!r}")
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
