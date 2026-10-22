"""Prompt-caching insights per agent over a date range, from recorded telemetry only.

Built from the `kind="llm"` telemetry rows' prompt-cache fields (app/features/observability/
meter.py): cache_requested, cache_read_tokens, cache_write_tokens, cache_read_cost_usd,
cache_write_cost_usd, cache_saving_usd, cache_cost_known and rates_known. Nothing here is
estimated: a cost that was not recorded (no published rate) is reported as unknown.

Three kinds, at most one per agent:
  * "paying_off"    the recorded net saving is above zero (the cached tokens at the normal
                    input rate, minus what the cache reads and writes cost).
  * "rarely_read"   caching was requested, cache tokens were written, and READS < WRITES
                    over the range, with no positive net saving to show for it.
  * "nothing_cached" caching was requested, and no token was read from or written to the
                    cache over the range: the cached prefix is likely below the model's
                    minimum tokens per cache checkpoint, read from vocabulary.json
                    promptCacheModels (the AWS table, copied with its source link).
An agent that never requested caching gets none.
"""
from __future__ import annotations

import decimal
import json
from pathlib import Path

#: The "rarely read" threshold, stated once: reads below writes over the range.
RARELY_READ_RULE = "cache read tokens < cache write tokens over the range"

# Staged next to this module by both IaC paths (as bff/authz.py reads it); the source
# checkout falls back to app/.
_VOCAB_PATH = Path(__file__).resolve().parent / "vocabulary.json"
if not _VOCAB_PATH.exists():
    _VOCAB_PATH = Path(__file__).resolve().parent.parent / "app" / "vocabulary.json"


def _vocab() -> dict:
    try:
        return json.loads(_VOCAB_PATH.read_text())
    except (OSError, ValueError):
        return {}


def _base_id(model: str, geos: list) -> str:
    """A model id, inference-profile id or ARN, without its geography prefix
    (as bff/prompt_cache_check.py resolves it)."""
    mid = str(model or "").strip()
    if "/" in mid:
        mid = mid.rsplit("/", 1)[-1]
    head, _, rest = mid.partition(".")
    return rest if rest and head.lower() in geos else mid


def _num(v) -> decimal.Decimal:
    try:
        return decimal.Decimal(str(v or 0))
    except decimal.InvalidOperation:
        return decimal.Decimal(0)


class Rollup:
    """Per-agent prompt-cache sums, fed one telemetry row at a time."""

    def __init__(self) -> None:
        self.agents: dict = {}

    def add(self, it: dict) -> None:
        if it.get("kind") != "llm":
            return
        aid = it.get("agent_id") or "?"
        a = self.agents.setdefault(aid, {
            "requestedCalls": 0, "calls": 0, "readTokens": 0, "writeTokens": 0,
            "readCost": decimal.Decimal(0), "writeCost": decimal.Decimal(0),
            "saving": decimal.Decimal(0), "known": True, "models": {}})
        a["calls"] += 1
        read = int(_num(it.get("cache_read_tokens")))
        write = int(_num(it.get("cache_write_tokens")))
        a["readTokens"] += read
        a["writeTokens"] += write
        a["readCost"] += _num(it.get("cache_read_cost_usd"))
        a["writeCost"] += _num(it.get("cache_write_cost_usd"))
        a["saving"] += _num(it.get("cache_saving_usd"))
        # A cache token with no published rate, or a model priced at the fallback rate,
        # leaves the cost (and so the saving) unrecorded: unknown, never guessed.
        if (read or write) and (it.get("cache_cost_known") is False
                                or it.get("rates_known") is False):
            a["known"] = False
        if str(it.get("cache_requested") or "").strip():
            a["requestedCalls"] += 1
            model = str(it.get("label") or "")
            if model and model not in a["models"]:
                a["models"][model] = str(it.get("cache_min_text") or "")

    def insights(self, vocab: dict | None = None) -> list[dict]:
        return compute(self.agents, _vocab() if vocab is None else vocab)


def _minimums(models: dict, vocab: dict) -> list[dict]:
    """Each model's documented minimum tokens per cache checkpoint, from the table."""
    table = vocab.get("promptCacheModels") or {}
    rows = table.get("byModel") or {}
    geos = [str(g).lower() for g in ((vocab.get("inferenceProfileGeos") or {}).get("values") or [])]
    out = []
    for model, recorded in sorted(models.items()):
        row = rows.get(_base_id(model, geos)) or {}
        text = str(row.get("minTokensText") or row.get("minTokens") or recorded or "")
        out.append({"model": model, "name": row.get("name") or model,
                    "minTokens": text or None})
    return out


def compute(agents: dict, vocab: dict) -> list[dict]:
    """The insights for a Rollup's agents, most actionable first."""
    table = vocab.get("promptCacheModels") or {}
    source = table.get("source") or ""
    out = []
    for aid, a in sorted(agents.items()):
        if not a["requestedCalls"]:
            continue
        read, write, known = a["readTokens"], a["writeTokens"], a["known"]
        base = {"agentId": aid, "requestedCalls": a["requestedCalls"], "calls": a["calls"],
                "cacheReadTokens": read, "cacheWriteTokens": write, "costKnown": known,
                "cacheReadCostUsd": a["readCost"] if known else None,
                "cacheWriteCostUsd": a["writeCost"] if known else None,
                "netSavingUsd": a["saving"] if known else None}
        if not read and not write:
            out.append({**base, "kind": "nothing_cached",
                        "models": _minimums(a["models"], vocab), "source": source})
        elif known and a["saving"] > 0:
            out.append({**base, "kind": "paying_off"})
        elif write and read < write:
            out.append({**base, "kind": "rarely_read", "rule": RARELY_READ_RULE})
    order = {"rarely_read": 0, "nothing_cached": 1, "paying_off": 2}
    return sorted(out, key=lambda i: order[i["kind"]])
