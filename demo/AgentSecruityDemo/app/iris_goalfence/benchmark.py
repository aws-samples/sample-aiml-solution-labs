"""Performance benchmark for the goal fence — measures the overhead the DETERMINISTIC
engine adds to an agent loop.

It replays a realistic Iris conversation (2-3 turns, the same tool calls the app makes)
through the engine exactly as the Strands adapter will: evaluate() before each tool call,
observe() once admitted. Then it hammers the same trajectory N times to get stable
per-evaluate timings.

Note: this measures ONLY the engine (pure Python, no network). The drift rule's embedding
call is NOT here — it lives in the adapter and is measured separately; the engine just
compares a float. So these numbers are the true fixed cost added to every tool call.

    python3 iris_goalfence/benchmark.py
"""
from __future__ import annotations

import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from iris_goalfence import Action, Charter, Decision, GoalFenceEngine, TrajectoryState
from iris_goalfence.entities import Phase

CALLER = "C-1001"

CHARTER = Charter(
    goal="Answer the signed-in customer's questions about their own orders, shipments, "
         "and refunds — one customer, small result sets.",
    max_actions=5,
    max_records=100,
    value_caps={"process_refund": 500},
    forbidden_sequences=(("get_record", "update_record"),),
    scope_exempt_tools=frozenset({"get_my_info"}),
    drift_threshold=0.45,
    fail_closed=True,
)


def _tool_call(tool, args, drift=None):
    """One before-tool Action, plus its after-tool result (for record counting)."""
    return (
        Action(tool, args, CALLER, Phase.BEFORE_TOOL, drift_score=drift),
        Action(tool, args, CALLER, Phase.AFTER_TOOL,
               result={"records": [{"id": "x"}]}),
    )


# A realistic 3-turn support conversation, as tool calls (what the model would emit):
#   Turn 1: "where's my order A1?"  -> get_my_info, get_record, get_shipment
#   Turn 2: "is it refundable?"     -> get_record (refund_eligible)
#   Turn 3: "refund $40 then"       -> process_refund   (under cap -> allowed)
# drift_score attached at the first call of each turn (adapter computes once per invoke).
CONVERSATION = [
    _tool_call("get_my_info", {}, drift=0.08),
    _tool_call("get_record", {"order_id": "A1"}),
    _tool_call("get_shipment", {"order_id": "A1"}),
    _tool_call("get_record", {"order_id": "A1"}, drift=0.11),
    _tool_call("process_refund", {"order_id": "A1", "amount": 40}, drift=0.09),
]


def run_once(eng: GoalFenceEngine) -> tuple[int, list[float]]:
    """Replay the whole conversation through a fresh trajectory. Returns (#evals, timings)."""
    state = TrajectoryState()
    timings = []
    for before, after in CONVERSATION:
        t0 = time.perf_counter()
        verdict = eng.evaluate(state, before)
        timings.append(time.perf_counter() - t0)
        if verdict.decision is Decision.ALLOW:
            eng.observe(state, before)
            eng.observe(state, after)
    return len(timings), timings


def fmt_us(seconds: float) -> str:
    return f"{seconds * 1e6:8.2f} µs"


def main():
    eng = GoalFenceEngine(CHARTER)

    # Warm up (JIT-free CPython, but warms caches / import-time costs out of the numbers).
    for _ in range(1000):
        run_once(eng)

    ITERS = 50_000
    all_timings: list[float] = []
    convo_times: list[float] = []
    for _ in range(ITERS):
        t0 = time.perf_counter()
        _, timings = run_once(eng)
        convo_times.append(time.perf_counter() - t0)
        all_timings.extend(timings)

    all_timings.sort()
    n = len(all_timings)

    def pct(p):
        return all_timings[min(n - 1, int(p / 100 * n))]

    print("\n=== Goal fence — per tool-call evaluate() overhead ===")
    print(f"conversation      : 3 turns, {len(CONVERSATION)} tool calls")
    print(f"samples           : {ITERS:,} replays  →  {n:,} evaluate() calls")
    print(f"mean              : {fmt_us(statistics.mean(all_timings))}")
    print(f"median (p50)      : {fmt_us(pct(50))}")
    print(f"p90               : {fmt_us(pct(90))}")
    print(f"p99               : {fmt_us(pct(99))}")
    print(f"max               : {fmt_us(all_timings[-1])}")
    print(f"\nper-CONVERSATION (all {len(CONVERSATION)} evals + observes):")
    print(f"mean              : {fmt_us(statistics.mean(convo_times))}")
    print(f"median            : {fmt_us(statistics.median(convo_times))}")

    # Context: how this compares to a single model/tool round-trip.
    model_ms = 800   # a conservative Sonnet step ~ hundreds of ms to a couple seconds
    per_call_us = statistics.mean(all_timings) * 1e6
    print("\n=== Context ===")
    print(f"fence per tool call : ~{per_call_us:.1f} µs "
          f"({per_call_us/1000:.4f} ms)")
    print(f"one model step      : ~{model_ms} ms (typical)")
    print(f"fence overhead      : ~{(per_call_us/1000)/model_ms*100:.5f}% of a model step")
    print()


if __name__ == "__main__":
    main()
