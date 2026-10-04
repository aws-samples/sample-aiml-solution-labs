"""DynamoDB writer for telemetry records (best-effort).

Writes never raise — a telemetry failure must never break a workflow run. When
TELEMETRY_TABLE is unset (local dev), writes are no-ops so the flow still runs.
Reads and aggregation live in the BFF (bff/handler.py), which only sums the
pre-computed cost per row.
"""

from __future__ import annotations

import os
import time
from decimal import Decimal

from app.features.observability.records import CallRecord

TELEMETRY_TABLE = os.getenv("TELEMETRY_TABLE", "")

# How long a telemetry row lives. Both IaC paths enable DynamoDB TTL on a `ttl`
# attribute, but NOTHING wrote one — so rows carrying captured system prompts, user
# input and model output were retained forever on a table whose whole point is
# short-term cost/quality inspection. 0 disables expiry (keep everything).
TELEMETRY_TTL_DAYS = int(os.getenv("TELEMETRY_TTL_DAYS", "90"))

_table = None


def _tbl():
    global _table
    if _table is None:
        import boto3
        _table = boto3.resource("dynamodb").Table(TELEMETRY_TABLE)
    return _table


def _clean(item: dict) -> dict:
    # DynamoDB rejects float; Decimal is fine.
    out = {}
    for k, v in item.items():
        if isinstance(v, float):
            v = Decimal(str(v))
        out[k] = v
    return out


# DynamoDB's hard per-item limit is 400KB; stay safely under it (leaving room for
# attribute names + numeric fields). The captured text fields, largest first, are
# trimmed to fit — a row is never dropped for size (which would silently lose the
# whole telemetry record).
_MAX_ITEM_BYTES = 380_000
_TEXT_FIELDS = ("system_prompt", "user_input", "output_text", "namespace")
_FIT_NOTE = "\n\n…[truncated to fit the 400KB storage limit]"


def _item_bytes(item: dict) -> int:
    return sum(len(str(k).encode("utf-8")) + len(str(v).encode("utf-8"))
               for k, v in item.items())


def _fit(item: dict) -> dict:
    """Trim the largest captured text field(s) until the item fits under the
    DynamoDB size cap, so put_item never fails on size."""
    for _ in range(len(_TEXT_FIELDS)):
        over = _item_bytes(item) - _MAX_ITEM_BYTES
        if over <= 0:
            break
        present = [(f, len(str(item.get(f, "")).encode("utf-8")))
                   for f in _TEXT_FIELDS if item.get(f)]
        if not present:
            break
        field, cur = max(present, key=lambda x: x[1])
        keep = max(0, cur - over - len(_FIT_NOTE.encode("utf-8")) - 64)
        # Byte-trim then decode defensively (never split a multibyte char).
        trimmed = str(item[field]).encode("utf-8")[:keep].decode("utf-8", "ignore")
        item[field] = trimmed + _FIT_NOTE
    return item


def _with_ttl(item: dict) -> dict:
    """Stamp the DynamoDB TTL attribute the tables are already configured for."""
    if TELEMETRY_TTL_DAYS > 0:
        item["ttl"] = int(time.time()) + TELEMETRY_TTL_DAYS * 86400
    return item


def put(record: CallRecord) -> None:
    if not TELEMETRY_TABLE:
        return
    try:
        _tbl().put_item(Item=_clean(_with_ttl(_fit(record.to_item()))))
    except Exception as e:  # noqa: BLE001 - telemetry must never break a run
        print(f"[observability] telemetry write failed: {type(e).__name__}: {e}")
