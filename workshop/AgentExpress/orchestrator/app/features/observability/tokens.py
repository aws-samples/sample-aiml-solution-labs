"""Exact token counting via the Bedrock CountTokens API.

Used to report the EXACT system-prompt token slice of a model call, instead of a
~4-chars/token estimate. The count is derived by subtraction, exactly as the
Bedrock docs recommend: CountTokens(dummy_message + system) - CountTokens(dummy_message)
isolates the system block (including its structural overhead).

System prompts are static per agent, so results are cached per (model, system)
and the per-model baseline is cached too — a whole deployment does at most a
couple of CountTokens calls per distinct system prompt per process. Everything is
best-effort: any failure returns None so the caller falls back to the estimate.
CountTokens itself is not billed.

Models that do NOT support CountTokens are skipped up front (static denylist) and
remembered after a ValidationException, because every failed call is
auto-instrumented by botocore and would otherwise surface as an ERROR span,
inflating the trace's error count in GenAI Observability.
"""

from __future__ import annotations

import hashlib
import threading

from app.common.config import REGION

_client = None
_lock = threading.Lock()
_system_cache: dict[tuple[str, str], int] = {}   # (model, system-hash) -> exact tokens
_baseline_cache: dict[str, int] = {}             # model -> tokens of the dummy message alone
# Direct model ids that returned ValidationException for CountTokens — never call
# them again (each failed call would surface as an ERROR span).
_unsupported: set[str] = set()

# Minimal non-empty user turn; its token count is the baseline we subtract off.
_DUMMY = [{"role": "user", "content": [{"text": "."}]}]

# Models known NOT to support the Bedrock CountTokens API. Skipping the call up
# front means not even the first invocation produces a benign error span; token
# reporting cleanly falls back to the ~4-chars/token estimate for these.
_COUNT_TOKENS_UNSUPPORTED = ("claude-haiku-4-5",)


def _runtime():
    global _client
    if _client is None:
        import boto3
        _client = boto3.client("bedrock-runtime", region_name=REGION)
    return _client


def _direct_model_id(model_id: str) -> str:
    """CountTokens accepts only direct model IDs — strip a cross-region prefix
    (us./eu./apac.) if present."""
    for p in ("us.", "eu.", "apac."):
        if model_id.startswith(p):
            return model_id[len(p):]
    return model_id


def _count(model_id: str, messages: list, system: str | None = None) -> int:
    inp: dict = {"converse": {"messages": messages}}
    if system:
        inp["converse"]["system"] = [{"text": system}]
    res = _runtime().count_tokens(modelId=model_id, input=inp)
    return int(res.get("inputTokens", 0) or 0)


def exact_system_tokens(model_id: str, system_text: str) -> int | None:
    """Exact token count of the system prompt for this model, or None on failure.

    Cached per (model, system prompt); the per-model baseline is cached too."""
    if not system_text:
        return 0
    # sha256 as a cache key, not for security. It was sha1, which flags in any
    # security lint and buys nothing here — the cost difference is irrelevant next
    # to the CountTokens call this cache exists to avoid.
    key = (model_id, hashlib.sha256(system_text.encode("utf-8")).hexdigest())
    with _lock:
        if key in _system_cache:
            return _system_cache[key]
    mid = _direct_model_id(model_id)
    # Skip models that don't support CountTokens (static denylist or a prior
    # ValidationException) so no error span is produced; caller falls back to est.
    if any(u in mid for u in _COUNT_TOKENS_UNSUPPORTED):
        return None
    with _lock:
        if mid in _unsupported:
            return None
    try:
        with_system = _count(mid, _DUMMY, system=system_text)
        with _lock:
            baseline = _baseline_cache.get(mid)
        if baseline is None:
            baseline = _count(mid, _DUMMY)
            with _lock:
                _baseline_cache[mid] = baseline
        val = max(0, with_system - baseline)
        with _lock:
            _system_cache[key] = val
        return val
    except Exception as e:  # noqa: BLE001 - best-effort; caller falls back to estimate
        # A ValidationException means this model doesn't support CountTokens —
        # remember it so we never call (and never emit another error span) again.
        if type(e).__name__ == "ValidationException" or "ValidationException" in str(e):
            with _lock:
                _unsupported.add(mid)
        print(f"[observability] count_tokens fallback: {type(e).__name__}: {e}")
        return None
