"""Metering API — the only surface the rest of the app calls.

The chokepoints that call in:
  * app/common/llm.py               -> record_llm(...)        after each Bedrock call
  * app/features/gateway/client.py  -> record_tool(...)       after each Gateway/KB call
                                    -> record_policy(...)     Cedar decision on that call
  * app/common/context.py           -> record_memory(...)     long-term recall/store
                                    -> record_guardrail(...)  input/output check
  * app/features/evaluations/…       -> record_eval(...)       one judge result
  * app/orchestrator/runtime.py     -> record_session_compute(...) per active burst

Everything is best-effort and swallows its own errors: capturing telemetry must
never change the behaviour or success of a workflow run.
"""

from __future__ import annotations

import math
import os

from app.features.observability import pricing, store
from app.features.observability.records import CallRecord
from app.features.observability.scope import get_scope

# Start loading this region's Bedrock prices now, in the background, so they are usually
# in hand before the first model call returns (pricelist.py). Only where telemetry is
# written — a local run or a test has nothing to price and should make no AWS call.
if store.TELEMETRY_TABLE and pricing._price_list_enabled():
    from app.features.observability import pricelist as _pricelist
    _pricelist.prefetch()

# monotonically increasing per-process sequence for stable ordering within a ms
_seq = 0


def _next_seq() -> int:
    global _seq
    _seq = (_seq + 1) % 10000
    return _seq


def est_tokens(text: str) -> int:
    """Rough token estimate (~4 chars/token) for the KB query embedding, which
    Bedrock does not report separately, and as a fallback for the system-prompt
    slice when the exact CountTokens call is unavailable."""
    return math.ceil(len(text or "") / 4)


# Max characters of captured prompt/input/output stored PER FIELD per event, so a
# large input (e.g. a prompt carrying full document text) still shows in the
# observability UI. Configurable via OBS_MAX_CAPTURE_CHARS. store.py additionally
# guarantees the whole row stays under DynamoDB's 400KB item limit, so a big value
# here can never cause a dropped telemetry row.
_MAX_CAPTURE_CHARS = int(os.getenv("OBS_MAX_CAPTURE_CHARS", "100000"))


def _truncate(text: str) -> str:
    s = str(text or "")
    if len(s) <= _MAX_CAPTURE_CHARS:
        return s
    return s[:_MAX_CAPTURE_CHARS] + f"\n\n…[truncated {len(s) - _MAX_CAPTURE_CHARS} more characters]"


def _system_tokens(model: str, system_text: str, mode: str) -> tuple[int, bool]:
    """Exact system-prompt tokens via Bedrock CountTokens (cached), falling back
    to the ~4-chars/token estimate. Returns (tokens, is_exact)."""
    # mode "error" means the call failed before Bedrock answered, so there is
    # nothing to count exactly — fall back to the estimate.
    if mode == "error" or not system_text:
        return est_tokens(system_text), False
    from app.features.observability import tokens as _tok
    exact = _tok.exact_system_tokens(model, system_text)
    if exact is None:
        return est_tokens(system_text), False
    return exact, True


def record_llm(*, model: str, input_tokens: int, output_tokens: int,
               system_text: str = "", latency_ms: int = 0, mode: str = "bedrock",
               user_input: str = "", output_text: str = "",
               temperature: float = 0.0, max_tokens: int = 0,
               finish_reason: str = "", prompt: str = "") -> None:
    """One Bedrock model call: exact tokens, rates, cost, and the captured I/O the
    "Prompts & I/O" inspector (and AgentCore Evaluations) read back."""
    try:
        s = get_scope()
        # One lookup: the rates, and which source priced them. Carried onto the row so a
        # reader can tell a listed price from the built-in snapshot or the fallback guess
        # — the fallback was silent, which made every figure for an unrecognised model
        # look measured.
        in_rate, out_rate, source = pricing.price(model)
        known = source != "fallback"
        # A failed call ("error") consumed no billable tokens.
        cost = (pricing.Decimal("0") if mode == "error"
                else pricing.cost_at(in_rate, out_rate, input_tokens, output_tokens))
        sys_tokens, sys_exact = _system_tokens(model, system_text, mode)
        store.put(CallRecord(
            session_id=s.session_id, agent_id=s.agent_id, user=s.user,
            kind="llm", label=model, mode=mode,
            input_tokens=int(input_tokens or 0), output_tokens=int(output_tokens or 0),
            system_tokens=sys_tokens, system_tokens_exact=sys_exact,
            latency_ms=int(latency_ms or 0), cost_usd=cost,
            in_rate=in_rate, out_rate=out_rate, rates_known=known, rates_source=source,
            seq=_next_seq(),
            temperature=pricing.Decimal(str(temperature or 0)), max_tokens=int(max_tokens or 0),
            finish_reason=finish_reason or "", status=("error" if mode == "error" else "ok"),
            system_prompt=_truncate(system_text), user_input=_truncate(user_input),
            output_text=_truncate(output_text), version=s.version, prompt=prompt or "",
        ))
    except Exception as e:  # noqa: BLE001
        print(f"[observability] record_llm skipped: {type(e).__name__}: {e}")


def record_session_compute(*, session_id: str, user: str = "", active_seconds: float = 0.0) -> None:
    """Write the AgentCore Runtime compute cost for one active burst (one runtime
    invocation) of a session. Called at the end of each start/resume run."""
    try:
        from app.features.observability import costs
        store.put(CallRecord(
            session_id=session_id, agent_id="__session__", user=user or "",
            kind="agentcore", label="runtime-compute", mode="agentcore",
            latency_ms=int(active_seconds * 1000),
            cost_usd=costs.session_compute_cost(active_seconds), seq=_next_seq(),
        ))
    except Exception as e:  # noqa: BLE001
        print(f"[observability] record_session_compute skipped: {type(e).__name__}: {e}")


def record_tool(*, provider: str, query: str = "", latency_ms: int = 0,
                mode: str = "gateway", result_text: str = "",
                tool_type: str = "") -> None:
    """A tool/KB call. Its Gateway invocation cost is charged here; the KB path
    also embeds the query (Titan). The RESULT tokens are billed as input tokens
    on the next model call, so they are captured by record_llm, not here.

    `provider` is the tool's LABEL in workflow.json (whatever the customer called it)
    and is what the observability panel displays. `tool_type` is its declared TYPE,
    and it is a separate argument because the embedding charge below depends on the
    KIND of tool, not on its name.

    Those were one argument until an audit caught it: the retrieval branch compared
    `provider == "kb"`, which is THIS SAMPLE's key for its Knowledge Base. A customer
    who called theirs `policies` — as they are told they may — got no embedding cost
    on any retrieval and a permanently zero `embed_tokens_est`, with no error.
    """
    try:
        s = get_scope()
        if mode == "error":
            cost = pricing.Decimal("0")
            embed = 0
        else:
            cost = pricing.gateway_tool_cost()
            # Compared against the declared TYPE — framework vocabulary, the same in
            # every deployment — not against the label, which is the customer's.
            embed = est_tokens(query) if tool_type.lower() == "kb" else 0
            if embed:
                cost += pricing.embedding_cost(embed)
        # embed tokens are an estimate and are NOT model tokens, so keep them out
        # of input_tokens (which stays exact) — store them in their own field.
        store.put(CallRecord(
            session_id=s.session_id, agent_id=s.agent_id, user=s.user,
            kind="tool", label=provider, mode=mode,
            input_tokens=0, embed_tokens_est=embed, latency_ms=int(latency_ms or 0),
            cost_usd=cost, seq=_next_seq(),
            status=("error" if mode == "error" else "ok"),
            user_input=_truncate(query), output_text=_truncate(result_text),
            version=s.version,
        ))
    except Exception as e:  # noqa: BLE001
        print(f"[observability] record_tool skipped: {type(e).__name__}: {e}")


def record_image(*, model: str, prompt: str = "", latency_ms: int = 0, cost_usd: float = 0,
                 status: str = "ok", result_text: str = "", rates_source: str = "") -> None:
    """An image render (app/common/images.py): priced per image by
    pricing.image_price (orchestrator.imageRates, the AWS Price List, then the snapshot
    in vocabulary.json); `rates_source` says which."""
    try:
        s = get_scope()
        store.put(CallRecord(
            session_id=s.session_id, agent_id=s.agent_id, user=s.user,
            kind="image", label=model, mode="bedrock" if status == "ok" else "error",
            latency_ms=int(latency_ms or 0),
            cost_usd=pricing.Decimal(str(cost_usd if status == "ok" else 0)), seq=_next_seq(),
            rates_source=rates_source if status == "ok" else "",
            status="ok" if status == "ok" else "error",
            user_input=_truncate(prompt), output_text=_truncate(result_text),
            version=s.version,
        ))
    except Exception as e:  # noqa: BLE001
        print(f"[observability] record_image skipped: {type(e).__name__}: {e}")


def record_memory(*, op: str, namespace: str = "", query: str = "",
                  result_text: str = "", latency_ms: int = 0, mode: str = "agentcore") -> None:
    """A long-term memory read (op="recall") or write (op="store").

    Captured so the observability UI can show exactly what an agent recalled from
    / stored to memory during a run (namespace + query + content). Best-effort.
    """
    try:
        s = get_scope()
        store.put(CallRecord(
            session_id=s.session_id, agent_id=s.agent_id, user=s.user,
            kind="memory", label=op, mode=mode, latency_ms=int(latency_ms or 0),
            cost_usd=pricing.Decimal("0"), seq=_next_seq(),
            namespace=namespace, status=mode,
            user_input=_truncate(query), output_text=_truncate(result_text),
            version=s.version,
        ))
    except Exception as e:  # noqa: BLE001
        print(f"[observability] record_memory skipped: {type(e).__name__}: {e}")


def record_guardrail(*, source: str, action: str, detail: str = "", latency_ms: int = 0) -> None:
    """A guardrail check on agent input/output. action = "passed" | "blocked".
    Captured so the observability UI shows what the guardrail did. Best-effort."""
    try:
        s = get_scope()
        store.put(CallRecord(
            session_id=s.session_id, agent_id=s.agent_id, user=s.user,
            kind="guardrail", label=source, mode="bedrock", status=action,
            latency_ms=int(latency_ms or 0), cost_usd=pricing.Decimal("0"), seq=_next_seq(),
            output_text=_truncate(detail), version=s.version,
        ))
    except Exception as e:  # noqa: BLE001
        print(f"[observability] record_guardrail skipped: {type(e).__name__}: {e}")


def record_policy(*, tool: str, decision: str, filter_value: str = "",
                  mode: str = "", latency_ms: int = 0) -> None:
    """A Gateway Cedar-policy evaluation on a tool call. decision =
    "allowed" | "denied" | "log-only". Enforcement is server-side at the Gateway;
    in LOG_ONLY the authoritative decision is in CloudWatch (we can't see it app
    side), so we record "log-only". Captured for the observability UI."""
    try:
        s = get_scope()
        note = (f"Gateway policy mode={mode}. "
                + ("Decision logged to CloudWatch (LOG_ONLY); not enforced."
                   if decision == "log-only" else
                   ("Blocked at the Gateway by Cedar policy." if decision == "denied"
                    else "Allowed by Cedar policy.")))
        store.put(CallRecord(
            session_id=s.session_id, agent_id=s.agent_id, user=s.user,
            kind="policy", label=tool, mode=mode or "gateway", status=decision,
            latency_ms=int(latency_ms or 0), cost_usd=pricing.Decimal("0"), seq=_next_seq(),
            user_input=(f"filter={filter_value}" if filter_value else ""),
            output_text=note, version=s.version,
        ))
    except Exception as e:  # noqa: BLE001
        print(f"[observability] record_policy skipped: {type(e).__name__}: {e}")


def record_eval(*, evaluator: str, value=None, label: str = "", explanation: str = "",
                agent_id: str = "", latency_ms: int = 0, status: str = "ok",
                input_tokens: int = 0, output_tokens: int = 0, prompt: str = "") -> None:
    """One AgentCore Evaluations (LLM-as-judge) result for an agent's run.

    evaluator = e.g. "Builtin.Faithfulness"; value = 0.0-1.0 score; label = the
    judge's own qualitative band ("Very Helpful"), stored verbatim so the UI shows
    what the evaluator said rather than a band re-derived from the score;
    explanation = the judge's reasoning.
    status = "ok" | "error" (a partial failure carries the error text in
    explanation). agent_id overrides the scope agent (on-demand evaluation runs
    outside the agent's own scope). Captured for the observability UI."""
    try:
        s = get_scope()
        store.put(CallRecord(
            session_id=s.session_id, agent_id=agent_id or s.agent_id, user=s.user,
            kind="eval", label=evaluator, eval_label=label, mode="agentcore", status=status,
            latency_ms=int(latency_ms or 0), cost_usd=pricing.Decimal("0"), seq=_next_seq(),
            input_tokens=int(input_tokens or 0), output_tokens=int(output_tokens or 0),
            value=(pricing.Decimal(str(value)) if value is not None else pricing.Decimal("0")),
            output_text=_truncate(explanation), version=s.version, prompt=prompt or "",
        ))
    except Exception as e:  # noqa: BLE001
        print(f"[observability] record_eval skipped: {type(e).__name__}: {e}")
