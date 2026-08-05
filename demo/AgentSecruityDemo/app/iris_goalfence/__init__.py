"""iris_goalfence — a standalone, framework-agnostic GOAL FENCE decision engine.

The engine watches an agent's TRAJECTORY (not one action in isolation) and decides
whether the next action still serves the chartered goal, at the chartered scope and
scale. It is deliberately PURE: no Strands, no boto3, no network, no I/O. Its whole
universe is three plain values —

    evaluate(Charter, TrajectoryState, Action) -> Verdict

— so it can be unit-tested on a laptop with dicts and literals, with no agent, no AWS
credentials, and no model. The Strands adapter (added later, separately) is the only
place that imports a framework; it translates hook events into Action objects, calls
this engine, and applies the Verdict to the event.

Public surface (pure — no third-party imports):
    Charter, Action, TrajectoryState, Verdict, Decision   (data)
    GoalFenceEngine                                        (the pure decision core)

Adapters live in their own modules and are imported explicitly WHERE their dependency
exists, so importing this package never drags in Strands or boto3:
    from iris_goalfence.adapter_strands import GoalFenceHook   # needs strands
    from iris_goalfence.drift import DriftScorer               # needs boto3 at call time
"""
from .entities import Action, TrajectoryState, Verdict, Decision
from .charter import Charter
from .engine import GoalFenceEngine

__all__ = [
    "Charter",
    "Action",
    "TrajectoryState",
    "Verdict",
    "Decision",
    "GoalFenceEngine",
]
