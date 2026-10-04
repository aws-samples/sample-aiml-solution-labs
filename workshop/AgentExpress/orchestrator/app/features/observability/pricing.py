"""Price book + cost math — every observability cost figure flows through here.

Where a model's per-token rate comes from, first match wins:

  1. "config"     `orchestrator.modelRates` in workflow.json — a customer's own (e.g.
                  negotiated) rate beats anything AWS publishes.
  2. "pricelist"  the AWS Price List API for THIS region (pricelist.py): every Bedrock
                  model AWS lists, at today's public on-demand price, regional vs
                  `global.` profile told apart. Off with `orchestrator.priceList: false`.
  3. "table"      the built-in snapshot below — used when the Price List is unreachable
                  (no `pricing:GetProducts`, no network) or has no row for the model.
  4. "fallback"   a haiku-ish guess, so the panel is not blank. The row is marked
                  `rates_known=False` and the UI shows the figure as an estimate.

The source is recorded on every llm telemetry row (`rates_source`), so a reader can see
which of these priced it.

The non-model rates (embedding, AgentCore Runtime compute, Gateway) are AWS public list
prices for us-east-1 as of 2026-08-20, overridable per key with `orchestrator.serviceRates`.

All of it is an ESTIMATE for planning, not your bill: private pricing, discounts and
reconciliation differ. Everything returns a Decimal USD amount so the sums are exact.
"""

from __future__ import annotations

import contextlib
from decimal import Decimal

# --- Bedrock foundation models: USD per 1,000,000 tokens (input, output) ------
# The offline snapshot (source 3). Keyed by a substring matched against the model id;
# first match wins, so keep more specific ids above generic ones.
_MODEL_PER_MTOK: list[tuple[str, Decimal, Decimal]] = [
    ("claude-haiku-4-5",   Decimal("1.00"),  Decimal("5.00")),
    ("claude-sonnet-4-5",  Decimal("3.00"),  Decimal("15.00")),
    ("claude-sonnet-5",    Decimal("2.00"),  Decimal("10.00")),
    ("claude-opus-4",      Decimal("15.00"), Decimal("75.00")),
    ("claude-3-5-haiku",   Decimal("0.80"),  Decimal("4.00")),
    ("claude-3-5-sonnet",  Decimal("3.00"),  Decimal("15.00")),
]
_MODEL_FALLBACK = (Decimal("1.00"), Decimal("5.00"))  # unknown model -> haiku-ish

SOURCES = ("config", "pricelist", "table", "fallback")


# Per-model rates from `orchestrator.modelRates` in workflow.json (source 1).
# Shape: {"<model id substring>": {"input": <usd per Mtok>, "output": <usd per Mtok>}}.
# Substring match, longest key first so a specific id beats a general one regardless of
# the order they were written in.
def _configured_rates() -> list[tuple[str, Decimal, Decimal]]:
    from app.common.config import MODEL_RATES
    out: list[tuple[str, Decimal, Decimal]] = []
    for key, rates in (MODEL_RATES or {}).items():
        with contextlib.suppress(TypeError, ValueError, ArithmeticError):
            out.append((str(key).lower(),
                        Decimal(str((rates or {}).get("input", 0))),
                        Decimal(str((rates or {}).get("output", 0)))))
    return sorted(out, key=lambda r: len(r[0]), reverse=True)


# --- Non-model rates (overridable with orchestrator.serviceRates) -------------
# key in serviceRates -> built-in default. Each is read at call time, so a test (or a
# future hot reload) sees the configured value.
_SERVICE_DEFAULTS: dict[str, Decimal] = {
    # Titan Text Embeddings V2 — the KB retrieve embeds each query.
    "embeddingPerMillionTokens": Decimal("0.02"),
    # AgentCore Runtime, per vCPU-hour and per GB-hour.
    "runtimeVcpuHour": Decimal("0.0895"),
    "runtimeGbHour": Decimal("0.00945"),
    # AgentCore Gateway, per 1,000 InvokeTool / ListTools / Ping.
    "gatewayPerThousandInvocations": Decimal("0.005"),
    # One ctx.call_tool()/ctx.retrieve() goes through the Gateway; counted as one
    # ListTools + one InvokeTool for a conservative estimate.
    "gatewayInvocationsPerToolCall": Decimal(2),
    # The microVM footprint the per-session compute ESTIMATE assumes. Billing is by
    # actual vCPU/GB-hours; until the vended usage logs are read, compute is estimated
    # from ACTIVE runtime seconds (AgentCore doesn't bill while paused at a HITL gate).
    "runtimeVcpu": Decimal("1.0"),
    "runtimeGb": Decimal("2.0"),
}


def service_rate(key: str) -> Decimal:
    """A non-model rate: `orchestrator.serviceRates[key]`, else the built-in default."""
    from app.common.config import SERVICE_RATES
    raw = (SERVICE_RATES or {}).get(key)
    if raw is not None:
        with contextlib.suppress(TypeError, ValueError, ArithmeticError):
            value = Decimal(str(raw))
            if value >= 0:
                return value
    return _SERVICE_DEFAULTS[key]


# Module-level names kept for readers and callers of the old constants.
RUNTIME_VCPU_HOUR = _SERVICE_DEFAULTS["runtimeVcpuHour"]
RUNTIME_GB_HOUR = _SERVICE_DEFAULTS["runtimeGbHour"]
GATEWAY_PER_1K_INVOCATIONS = _SERVICE_DEFAULTS["gatewayPerThousandInvocations"]
RUNTIME_ASSUMED_VCPU = _SERVICE_DEFAULTS["runtimeVcpu"]
RUNTIME_ASSUMED_GB = _SERVICE_DEFAULTS["runtimeGb"]

_MTOK = Decimal(1000000)


def _price_list_enabled() -> bool:
    from app.common.config import PRICE_LIST
    return bool(PRICE_LIST)


def price(model_id: str) -> tuple[Decimal, Decimal, str]:
    """(input rate, output rate, source) — USD per 1M tokens, and which of SOURCES
    priced it. See the module docstring for the order."""
    mid = (model_id or "").lower()
    for key, in_rate, out_rate in _configured_rates():
        if key and key in mid:
            return in_rate, out_rate, "config"
    if _price_list_enabled():
        from app.features.observability import pricelist
        listed = pricelist.lookup(model_id)
        if listed:
            return listed[0], listed[1], "pricelist"
    for key, in_rate, out_rate in _MODEL_PER_MTOK:
        if key in mid:
            return in_rate, out_rate, "table"
    return _MODEL_FALLBACK[0], _MODEL_FALLBACK[1], "fallback"


def _model_rates(model_id: str) -> tuple[Decimal, Decimal, bool]:
    """(input rate, output rate, known). `known` is False only for the fallback guess:
    that is what `rates_known=False` on a telemetry row means."""
    in_rate, out_rate, source = price(model_id)
    return in_rate, out_rate, source != "fallback"


def model_rates(model_id: str) -> tuple[Decimal, Decimal]:
    """(input, output) USD per 1,000,000 tokens for a model — for display."""
    in_rate, out_rate, _ = _model_rates(model_id)
    return in_rate, out_rate


def rates_known(model_id: str) -> bool:
    """False when the rates for this model are the fallback guess, not a real price."""
    return _model_rates(model_id)[2]


def model_cost(model_id: str, input_tokens: int, output_tokens: int) -> Decimal:
    """USD for one model call from its token usage."""
    in_rate, out_rate, _ = _model_rates(model_id)
    return cost_at(in_rate, out_rate, input_tokens, output_tokens)


def cost_at(in_rate: Decimal, out_rate: Decimal, input_tokens: int, output_tokens: int) -> Decimal:
    """USD for a token count at given per-1M rates."""
    return (Decimal(max(0, input_tokens)) / _MTOK * in_rate
            + Decimal(max(0, output_tokens)) / _MTOK * out_rate)


def embedding_cost(tokens: int) -> Decimal:
    return Decimal(max(0, tokens)) / _MTOK * service_rate("embeddingPerMillionTokens")


def gateway_tool_cost() -> Decimal:
    """USD for the Gateway invocations behind one tool/KB call."""
    return (service_rate("gatewayInvocationsPerToolCall") / Decimal(1000)
            * service_rate("gatewayPerThousandInvocations"))


def runtime_compute_cost(vcpu_hours: Decimal | float, gb_hours: Decimal | float) -> Decimal:
    """USD for AgentCore Runtime compute consumed by a session (from the vended
    CPU/GB-hour usage logs)."""
    return (Decimal(str(vcpu_hours)) * service_rate("runtimeVcpuHour")
            + Decimal(str(gb_hours)) * service_rate("runtimeGbHour"))


def compute_cost_for_seconds(active_seconds: float) -> Decimal:
    """Estimated AgentCore Runtime compute cost for a burst of active seconds,
    using the assumed microVM footprint (serviceRates runtimeVcpu / runtimeGb)."""
    hours = Decimal(str(max(0, active_seconds))) / Decimal(3600)
    return runtime_compute_cost(hours * service_rate("runtimeVcpu"),
                                hours * service_rate("runtimeGb"))


def image_price(model_id: str) -> tuple[Decimal, str]:
    """(USD per generated image, source) for an image model. Same order as `price`:
    `orchestrator.imageRates` ({"<model id substring>": usd}), the AWS Price List, then
    the snapshot in vocabulary.json (`imageModels.pricePerImageUsd`). An image model none
    of them knows is "fallback" at 0 — shown as an unknown rate, not invented."""
    from app.common.config import IMAGE_RATES
    mid = (model_id or "").lower()
    for key, usd in sorted((IMAGE_RATES or {}).items(), key=lambda kv: len(kv[0]), reverse=True):
        if str(key).lower() in mid:
            with contextlib.suppress(TypeError, ValueError, ArithmeticError):
                return Decimal(str(usd)), "config"
    if _price_list_enabled():
        from app.features.observability import pricelist
        listed = pricelist.lookup_image(model_id)
        if listed:
            return listed, "pricelist"
    from app.common import images
    if model_id in images.PRICES:
        return Decimal(str(images.PRICES[model_id])), "table"
    return Decimal(0), "fallback"
