"""Progress sink: fans progress events out to the local bus and, when configured,
to DynamoDB (the store the CloudFront UI reads for live per-session progress).

Granular nested-map updates mean parallel agents never conflict; timeline log
lines go to a separate append-only events table.
"""

import contextlib
import os
import time
import uuid

from app.common import audit, clock
from app.common.bus import bus
from app.common.config import EVENTS_TABLE, NODE_IDS, STATUS_TABLE

# Runs expire after this many days when set (the status and events tables have
# DynamoDB TTL on `ttl`). 0, the default, keeps run history, as before.
RUN_TTL_DAYS = int(os.getenv("RUN_TTL_DAYS", "0") or 0)

_ddb = None


def _with_ttl(item: dict) -> dict:
    if RUN_TTL_DAYS > 0:
        item["ttl"] = int(time.time()) + RUN_TTL_DAYS * 86400
    return item


def _table(name: str):
    global _ddb
    if _ddb is None:
        import boto3
        _ddb = boto3.resource("dynamodb")
    return _ddb.Table(name)


def _key(session_id: str) -> dict:
    return {"session_id": session_id}


class WorkflowCancelled(Exception):
    """Raised by a node or heartbeat when the user has cancelled the session."""


def is_cancelled(session_id: str) -> bool:
    """True when the user requested a stop (durable flag in the status store).

    Checked at node boundaries and during long-running heartbeats so a run
    stops promptly without relying on reaching the same runtime instance.
    """
    if not STATUS_TABLE:
        return False
    try:
        item = _table(STATUS_TABLE).get_item(
            Key=_key(session_id), ProjectionExpression="cancel_requested"
        ).get("Item") or {}
        return bool(item.get("cancel_requested"))
    except Exception:  # noqa: BLE001 - never let a status read break a run
        return False


async def ensure_session(session_id: str, topic: str) -> None:
    """Create the initial status record (idempotent)."""
    if not bus.exists(session_id):
        bus.create(session_id, topic)
    if not STATUS_TABLE:
        return
    item = {
        "session_id": session_id, "topic": topic, "overall": "running",
        "nodes": {n: {"status": "pending", "pct": 0, "output": ""} for n in NODE_IDS},
        "history": {}, "hitl": None, "result": None,
        "created": clock.now_str(), "updated_at": clock.now_str(),
    }
    with contextlib.suppress(Exception):
        _table(STATUS_TABLE).put_item(
            Item=_with_ttl(item), ConditionExpression="attribute_not_exists(session_id)")


async def emit(session_id: str, event: dict) -> None:
    await bus.publish(session_id, event)
    if STATUS_TABLE:
        try:
            _write_ddb(session_id, event)
        except Exception as e:  # noqa: BLE001 - progress writes must never break a run
            print(f"[sink] ddb write failed: {type(e).__name__}: {e}")


def _write_ddb(sid: str, ev: dict) -> None:
    t = ev.get("type")
    n = ev.get("node")
    now = clock.now_str()
    tbl = _table(STATUS_TABLE)

    if t == "node_status" and n:
        expr = "SET #nodes.#n.#s = :s, updated_at = :u"
        names = {"#nodes": "nodes", "#n": n, "#s": "status"}
        vals = {":s": ev["status"], ":u": now}
        if "output" in ev:
            expr += ", #nodes.#n.#o = :o"
            names["#o"] = "output"
            vals[":o"] = ev.get("output", "")
        if "history" in ev:
            # Per-agent version history, overwritten wholesale with the list the
            # node computed (requires the top-level `history` map to exist —
            # ensure_session/skeleton create it).
            expr += ", #hist.#n = :hist"
            names["#hist"] = "history"
            vals[":hist"] = ev["history"]
        tbl.update_item(Key=_key(sid), UpdateExpression=expr,
                        ExpressionAttributeNames=names, ExpressionAttributeValues=vals)
    elif t == "heartbeat" and n:
        tbl.update_item(Key=_key(sid),
                        UpdateExpression="SET #nodes.#n.pct = :p, updated_at = :u",
                        ExpressionAttributeNames={"#nodes": "nodes", "#n": n},
                        ExpressionAttributeValues={":p": int(ev.get("pct", 0)), ":u": now})
    elif t == "hitl_request":
        # overall + hitl always. The per-node status is best-effort: a parallel
        # group gate emits a synthetic node id (e.g. "research") that has no
        # entry in the nodes map, so that update would fail — do it separately.
        tbl.update_item(
            Key=_key(sid),
            UpdateExpression="SET overall = :ov, hitl = :h, updated_at = :u",
            ExpressionAttributeValues={":ov": "waiting_human",
                                       ":h": {"node": n, "question": ev.get("question", "")},
                                       ":u": now})
        if n:
            with contextlib.suppress(Exception):  # synthetic gate id, no per-node entry
                tbl.update_item(
                    Key=_key(sid),
                    UpdateExpression="SET #nodes.#n.#s = :w, updated_at = :u",
                    ExpressionAttributeNames={"#nodes": "nodes", "#n": n, "#s": "status"},
                    ExpressionAttributeValues={":w": "waiting_human", ":u": now})
    elif t == "hitl_resolved":
        tbl.update_item(Key=_key(sid),
                        UpdateExpression="SET hitl = :z, overall = :ov, updated_at = :u",
                        ExpressionAttributeValues={":z": None, ":ov": "running", ":u": now})
    elif t == "session_status":
        expr = "SET overall = :ov, updated_at = :u"
        names, vals = {}, {":ov": ev["status"], ":u": now}
        if "result" in ev:
            expr += ", #r = :r"
            names["#r"] = "result"
            vals[":r"] = ev.get("result") or ""
        kwargs = {"ExpressionAttributeNames": names} if names else {}
        # How the run ended goes in the activity log with its owner, user and topic —
        # once, when it first reaches that status (the same status again is no new end).
        # ALL_OLD: a run has only a few status changes, and this is the item as it was.
        before = tbl.update_item(Key=_key(sid), UpdateExpression=expr, ExpressionAttributeValues=vals,
                                 ReturnValues="ALL_OLD", **kwargs).get("Attributes") or {}
        if ev["status"] in audit.FINISHED and before.get("overall") != ev["status"]:
            err = ev.get("log", "") if ev["status"] == "failed" else ""
            audit.run_finished(ev["status"], before, sid, err.removeprefix("Workflow error: "))

    if ev.get("log") and EVENTS_TABLE:
        _table(EVENTS_TABLE).put_item(Item=_with_ttl({
            "session_id": sid, "ts": f"{now}#{uuid.uuid4().hex[:6]}",
            "node": n or "-", "msg": ev["log"]}))


def nodes_in_status(sid: str, status: str) -> list[str]:
    """Node ids sitting at `status` in the live status doc.

    Used to reconcile a SETTLED run so the UI never contradicts it: nodes still
    'running' under a failed/cancelled run, and nodes still 'pending' under a
    completed one (a branch bypassed them). Best-effort: returns [] with no table or
    on any read error, because reconciliation must never break the path it tidies.
    """
    if not STATUS_TABLE:
        # Local dev mirror: fall back to the in-process snapshot.
        item = bus.snapshot(sid) or {}
    else:
        try:
            item = _table(STATUS_TABLE).get_item(Key=_key(sid)).get("Item") or {}
        except Exception:  # noqa: BLE001
            return []
    nodes = item.get("nodes") or {}
    return [nid for nid, v in nodes.items() if (v or {}).get("status") == status]
