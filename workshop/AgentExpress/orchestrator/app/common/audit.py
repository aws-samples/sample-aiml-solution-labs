"""How a run ended, in the activity log (bff/audit.py) — written here because the run
ends in the runtime, not in the BFF that logged it starting. AUDIT_TABLE is the same
table the BFF writes: a build's own `<agentName>_audit`, or a console's builds table.
Best effort, like every audit write: a log line that fails never fails the run.
"""
import os
import secrets
from datetime import UTC, datetime

AUDIT_TABLE = os.environ.get("AUDIT_TABLE", "")

#: A run's final status -> the action logged. A cancel is logged by the BFF when it is
#: asked for (run.cancelled), so it is not logged a second time here.
FINISHED = {"done": "run.completed", "failed": "run.failed", "denied": "run.denied"}

_table = None


def audit_items(owner: str, email: str, action: str, detail: dict,
                ts: str = "", rand: str = "") -> list[dict]:
    """bff/buildstore.audit_items, copied: the runtime ships without the BFF.
    tests/test_run_audit.py holds the two to the same output."""
    ts = ts or datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    sk = f"{ts}#{rand or secrets.token_hex(4)}"
    body = {"ts": ts, "owner": owner, "email": email, "action": action,
            "detail": {k: v for k, v in detail.items() if v not in (None, "")}}
    return [{"pk": f"AUDIT#{owner}", "sk": sk, **body},
            {"pk": f"AUDIT_DAY#{ts[:10]}", "sk": sk, **body}]


def run_finished(status: str, run: dict, session_id: str, error: str = "") -> None:
    """Log that a run ended with `status`, for its owner (`run` is its status item)."""
    action = FINISHED.get(status)
    owner = str(run.get("owner") or "")
    if not (AUDIT_TABLE and action and owner):
        return
    global _table
    try:
        if _table is None:
            import boto3
            _table = boto3.resource("dynamodb").Table(AUDIT_TABLE)
        detail = {"session": session_id, "topic": str(run.get("topic") or "")[:200],
                  "error": error[:300] or None}
        for item in audit_items(owner, str(run.get("user") or ""), action, detail):
            _table.put_item(Item=item)
    except Exception as e:  # noqa: BLE001 - the run is over; its log line must not fail it
        print(f"[audit] could not record {action} for {session_id}: {type(e).__name__}: {e}")
