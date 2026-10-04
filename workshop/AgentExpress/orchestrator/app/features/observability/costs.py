"""Non-per-call cost components (AgentCore infrastructure).

Per-call model/tool costs are metered at the chokepoints (meter.py). This module
covers the costs that aren't tied to a single call — today the AgentCore Runtime
compute a session consumes. It's kept separate so the "what does a run actually
cost" math is easy to find and extend (add Memory, Gateway idle, etc. here).
"""

from __future__ import annotations

from decimal import Decimal

from app.features.observability import pricing


def session_compute_cost(active_seconds: float) -> Decimal:
    """Estimated AgentCore Runtime compute cost for one active burst of a session
    (start or resume invocation). Summing these across a session's bursts gives
    total compute, excluding time paused at HITL gates (which isn't billed)."""
    return pricing.compute_cost_for_seconds(active_seconds)
