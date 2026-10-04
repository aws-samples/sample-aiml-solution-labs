"""OpenTelemetry helpers for AgentCore GenAI Observability (CloudWatch).

Thin, dependency-tolerant wrappers used to:
  1. group every span of one workflow run under a `session.id` (so the
     CloudWatch "Sessions View" shows one run as one session), and
  2. create a readable span per agent and per MCP tool call, and
  3. propagate the trace context across the InvokeAgentRuntime boundary so the
     dedicated sub-agent runtimes' spans join the SAME trace as the orchestrator.

If OpenTelemetry is not importable for any reason, every function degrades to a
no-op — observability must never break a run. The Bedrock model-call spans and
their token counts are produced automatically by aws-opentelemetry-distro (ADOT,
installed in the Dockerfile) via its botocore/bedrock-runtime instrumentation.
ADOT's bundled LangChain instrumentation is DISABLED (Terraform sets
OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=aws_langchain) because it emitted a second,
token-less "chat" span per call. This module only adds the things ADOT can't:
session grouping, readable per-agent/per-tool spans, per-agent token stamping,
and trace-context propagation across the InvokeAgentRuntime boundary.
"""

from __future__ import annotations

import contextlib
import contextvars
from contextlib import contextmanager

try:
    from opentelemetry import baggage, context, trace
    from opentelemetry.propagate import extract, inject

    _OTEL = True
except Exception:  # noqa: BLE001 - OTEL optional; degrade to no-ops
    _OTEL = False

# Per-agent token accumulator. ADOT's botocore auto-instrumentation names the
# Bedrock call span "chat <model>" but (as of ADOT 0.19.0) does NOT extract the
# Converse API's usage tokens, so those spans report gen_ai.usage.* = 0. We DO
# have the real counts from LangChain's msg.usage_metadata, so we accumulate them
# per agent (an agent may make several LLM calls) and stamp gen_ai.usage.* onto
# the agent span — giving real token numbers without adding duplicate spans.
_token_accum: contextvars.ContextVar = contextvars.ContextVar("otel_token_accum", default=None)


def _get_tracer():
    """Fetch the tracer lazily (NOT at import). The global TracerProvider is set
    by opentelemetry-instrument at startup; grabbing the tracer per-call avoids
    caching a no-op provider if our module is imported before init completes."""
    return trace.get_tracer("multiagent.orchestrator")


# AgentCore's InvokeAgentRuntime requires runtimeSessionId to be 33+ chars, so
# the BFF pads the app session id with trailing zeros: session_id.ljust(33,"0")[:33].
# AgentCore derives its runtime-level "session" from that padded id, while our
# spans carry the raw id in baggage — producing TWO session rows for one run in
# the CloudWatch Sessions View. Padding the baggage session.id the SAME way makes
# both refer to one session, so one run shows as exactly one session.
_RUNTIME_SESSION_LEN = 33


def normalize_session_id(session_id: str) -> str:
    """Pad the app session id to AgentCore's runtimeSessionId form so the OTEL
    baggage session.id matches the runtime-level session (one run = one session).
    Mirrors the BFF's `session_id.ljust(33, "0")[:33]`."""
    if not session_id:
        return session_id
    return session_id.ljust(_RUNTIME_SESSION_LEN, "0")[:_RUNTIME_SESSION_LEN]


# Setting baggage does NOT by itself put anything on a span: something has to copy
# baggage entries onto spans as they start. That something is BaggageSpanProcessor
# (the pattern the AgentCore samples use in 01-features/06-observe.../01-observe/
# baggage_context.py). Without it only the spans where we set session.id BY HAND
# carried it — the agent spans — while the MCP tool spans and ADOT's automatic
# Bedrock model spans did not, so a run's own model calls sat outside its session
# in the CloudWatch Sessions View.
#
# No new dependency: opentelemetry-processor-baggage==0.65b0 is already pinned by
# aws-opentelemetry-distro==0.19.0 (installed in the Dockerfile).
#
# We pass a predicate rather than ALLOW_ALL_BAGGAGE_KEYS deliberately. Baggage
# travels in outgoing HTTP headers, so treating it as an open channel onto spans
# means anything a future caller puts in baggage lands in telemetry. Only
# `session.id` is copied.
_BAGGAGE_SPAN_KEYS = ("session.id",)
_baggage_processor_ready = False


def _ensure_baggage_processor() -> None:
    """Register BaggageSpanProcessor once, lazily.

    Lazily for the same reason _get_tracer() is lazy: the real TracerProvider is
    installed by `opentelemetry-instrument` at startup, and a provider fetched too
    early can be the API's no-op proxy, which has no add_span_processor. Leaving
    the flag False on failure means a later call simply tries again.
    """
    global _baggage_processor_ready
    if _baggage_processor_ready:
        return
    try:
        from opentelemetry.processor.baggage import BaggageSpanProcessor

        tp = trace.get_tracer_provider()
        if not hasattr(tp, "add_span_processor"):
            return  # proxy/no-op provider — ADOT has not initialised yet
        tp.add_span_processor(
            BaggageSpanProcessor(lambda key: key in _BAGGAGE_SPAN_KEYS))
        _baggage_processor_ready = True
    except Exception:  # noqa: BLE001 - observability must never break a run
        _baggage_processor_ready = True  # do not retry a hard failure every span


def set_session(session_id: str):
    """Attach `session.id` to OTEL baggage so all spans emitted while it is
    active are grouped under this session in CloudWatch. The id is normalized to
    AgentCore's padded runtimeSessionId form so the run appears as ONE session
    rather than a duplicate pair. Returns an opaque token to pass to detach()
    when the run/burst ends (or None if OTEL is absent)."""
    if not _OTEL or not session_id:
        return None
    # Register the processor that copies this baggage onto spans (once per process).
    _ensure_baggage_processor()
    try:
        ctx = baggage.set_baggage("session.id", normalize_session_id(session_id))
        return context.attach(ctx)
    except Exception:  # noqa: BLE001
        return None


def detach(token) -> None:
    """Detach a context token previously returned by set_session/attach_carrier."""
    if _OTEL and token is not None:
        with contextlib.suppress(Exception):
            context.detach(token)


@contextmanager
def span(name: str, scope: str | None = None, **attributes):
    """Start a span named `name` with optional attributes. No-op without OTEL.

    `scope` overrides the instrumentation scope (tracer name). AgentCore
    Evaluations only accepts spans whose scope is a recognised agent-framework
    instrumentation library, so agent spans are emitted under a supported
    LangChain scope name (see nodes.py) rather than our internal tracer name."""
    if not _OTEL:
        yield None
        return
    tracer = trace.get_tracer(scope) if scope else _get_tracer()
    with tracer.start_as_current_span(name) as sp:
        for key, value in attributes.items():
            if value is None:
                continue
            with contextlib.suppress(Exception):
                sp.set_attribute(key, value)
        yield sp


def set_attrs(sp, **attributes) -> None:
    """Set attributes on an already-open span (e.g. to add an agent's output
    after it has run). No-op without OTEL or a null span."""
    if not _OTEL or sp is None:
        return
    for key, value in attributes.items():
        if value is None:
            continue
        with contextlib.suppress(Exception):
            sp.set_attribute(key, value)


@contextmanager
def accumulate_tokens_on(sp):
    """Within this block, add_tokens() accumulates input/output token counts;
    on exit the running totals are written to span `sp` as the GenAI
    semantic-convention attributes gen_ai.usage.input_tokens / output_tokens.

    Used to wrap an agent's run so the agent span reports the sum of all its
    LLM calls' tokens (which the CloudWatch span-metrics panel and session
    totals read). No-op / transparent when OTEL is absent or sp is None."""
    if not _OTEL:
        yield
        return
    token = _token_accum.set({"in": 0, "out": 0})
    try:
        yield
    finally:
        totals = _token_accum.get() or {"in": 0, "out": 0}
        with contextlib.suppress(Exception):
            _token_accum.reset(token)
        if sp is not None and (totals["in"] or totals["out"]):
            with contextlib.suppress(Exception):
                sp.set_attribute("gen_ai.usage.input_tokens", int(totals["in"]))
                sp.set_attribute("gen_ai.usage.output_tokens", int(totals["out"]))


# Capture the agent's primary prompt (system + user of its first model call) so
# the AGENT span can carry the REAL task — the instructions and source inputs the
# agent worked from — as gen_ai.task.input, which AgentCore Evaluations reads.
# Without this the span carries only a generic role paraphrase and the judges
# grade role/format compliance instead of substance (did it reason/extract
# correctly and follow the rules).
_prompt_capture: contextvars.ContextVar = contextvars.ContextVar("otel_prompt_capture", default=None)


def begin_prompt_capture(holder: dict):
    """Start capturing the model prompts of the current agent run into `holder`
    (a mutable dict the caller reads after run()). Returns a token to pass to
    end_prompt_capture()."""
    return _prompt_capture.set(holder)


def end_prompt_capture(token) -> None:
    with contextlib.suppress(Exception):
        _prompt_capture.reset(token)


def capture_prompt(name: str, system: str, user: str) -> None:
    """Record the (system, user) prompt of the model call `name` for the current
    agent run. An agent can issue several distinct prompts (e.g. "analysis" and
    "analysis-scoring"); each is captured separately so eval can scope to ONE
    prompt. Also keeps the FIRST call as the agent span's primary prompt
    (gen_ai.task.input) for the Traces View. No-op outside an agent run. Arms
    output capture so the SAME call's response is paired with this prompt."""
    try:
        holder = _prompt_capture.get()
    except Exception:  # noqa: BLE001
        return
    if holder is None:
        return
    # Per-prompt capture (keyed by the run_llm name).
    captured = holder.setdefault("captured", {})
    captured[name or ""] = {"system": system or "", "user": user or "", "output": ""}
    holder["_pending_name"] = name or ""
    # First call also becomes the agent span's primary prompt.
    if not holder.get("prompt"):
        holder["prompt"] = f"{system or ''}\n\n=== INPUT ===\n{user or ''}"
        holder["system"] = system or ""
        holder["_await_output"] = True


def capture_output(text: str) -> None:
    """Record the response of the model call whose prompt we just captured.

    Pairs the output with its prompt in the per-prompt `captured` map (keyed by
    the last-armed name), and also keeps the FIRST call's output as the agent
    span's primary output. No-op outside a capture."""
    try:
        holder = _prompt_capture.get()
    except Exception:  # noqa: BLE001
        return
    if holder is None:
        return
    pending = holder.get("_pending_name")
    if pending is not None:
        cap = (holder.get("captured") or {}).get(pending)
        if cap is not None and not cap.get("output"):
            cap["output"] = text or ""
        holder["_pending_name"] = None
    if holder.get("_await_output") and not holder.get("output"):
        holder["output"] = text or ""
        holder["_await_output"] = False


def add_tokens(input_tokens: int, output_tokens: int) -> None:
    """Record the token usage of one LLM call. If an accumulate_tokens_on()
    block is active (normal path, inside an agent run) the counts are summed for
    that agent; otherwise they are stamped directly on the current span. No-op
    without OTEL."""
    if not _OTEL:
        return
    it, ot = int(input_tokens or 0), int(output_tokens or 0)
    if not (it or ot):
        return
    acc = _token_accum.get()
    if acc is not None:
        acc["in"] += it
        acc["out"] += ot
        return
    with contextlib.suppress(Exception):
        sp = trace.get_current_span()
        sp.set_attribute("gen_ai.usage.input_tokens", it)
        sp.set_attribute("gen_ai.usage.output_tokens", ot)


def force_flush(timeout_millis: int = 8000) -> None:
    """Flush buffered spans to the exporter NOW.

    Critical for the async-task execution model: each workflow burst runs in a
    background task and, when it completes at a HITL gate, AgentCore may freeze
    or reclaim the (now idle) container. The OpenTelemetry BatchSpanProcessor
    exports on a background timer, so spans still buffered when the container
    freezes are lost — which is why full multi-gate runs produced NO spans while
    a single quick invocation sometimes did. Calling this at the end of each
    burst pushes spans out before the container can be frozen. No-op without OTEL.
    """
    if not _OTEL:
        return
    with contextlib.suppress(Exception):
        tp = trace.get_tracer_provider()
        if hasattr(tp, "force_flush"):
            tp.force_flush(timeout_millis)


def carrier() -> dict:
    """Serialize the current trace context into a plain dict (W3C traceparent)
    so it can travel in an InvokeAgentRuntime JSON payload."""
    if not _OTEL:
        return {}
    out: dict = {}
    try:
        inject(out)
    except Exception:  # noqa: BLE001
        return {}
    return out


def attach_carrier(carrier_dict: dict):
    """Re-attach a trace context propagated from the orchestrator (sub-agent
    runtime side). Returns a token to detach() when done, or None."""
    if not _OTEL or not carrier_dict:
        return None
    try:
        ctx = extract(carrier_dict)
        return context.attach(ctx)
    except Exception:  # noqa: BLE001
        return None
