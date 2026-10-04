"""The activity log, wherever this BFF runs.

On a console with the Builder it is the builds table (bff/buildstore.py AUDIT# and
AUDIT_DAY# items), next to the builds it describes. On a deployed build's own app —
which has no builds table — it is that app's `<agentName>_audit` table, named by
AUDIT_TABLE and laid out the same way, so a build's owner sees who signed in to their app
and who started, decided, cancelled, re-ran or deleted a run in it.

Every write is best-effort (buildstore.record_audit never raises): a log write that fails
must not fail the action it describes.
"""

from __future__ import annotations

import datetime as _dt
import os
import re

import boto3
import buildstore

AUDIT_TABLE = os.environ.get("AUDIT_TABLE", "")
_own = boto3.resource("dynamodb").Table(AUDIT_TABLE) if AUDIT_TABLE else None


def _table():
    if _own is not None:
        return _own
    import builds
    return builds._table if builds.ENABLED else None


def enabled() -> bool:
    return _table() is not None


def record(owner: str, email: str, action: str, **detail) -> None:
    table = _table()
    if table is not None:
        buildstore.record_audit(table, owner, email, action, **detail)


#: The longest range of days one request reads, and the most events it returns.
MAX_DAYS = 31
MAX_EVENTS = 10000


def _day(value: str, name: str) -> _dt.date:
    import builds
    try:
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", value or ""):
            raise ValueError
        return _dt.date.fromisoformat(value)
    except ValueError:
        raise builds.BuildError(400, f"{name} must be YYYY-MM-DD") from None


def activity(owner: str, everyone: bool = False, day: str = "", limit: int = 500,
             day_to: str = "") -> list[dict]:
    """The caller's own events, or with `everyone` all users' from `day` to `day_to`
    (UTC, both included; `day_to` defaults to `day`), newest first. A range reads every
    event of each day in it, up to MAX_EVENTS in all."""
    import builds
    table = _table()
    if table is None:
        raise builds.BuildError(404, "this deployment keeps no activity log")
    if not everyone:
        return buildstore.audit_of(table, f"AUDIT#{owner}", limit)
    start = _day(day, "day" if not day_to else "from")
    end = _day(day_to, "to") if day_to else start
    if end < start:
        raise builds.BuildError(400, "to must not be before from")
    if (end - start).days + 1 > MAX_DAYS:
        raise builds.BuildError(400, f"a range is at most {MAX_DAYS} days")
    out: list[dict] = []
    d = end
    while d >= start and len(out) < MAX_EVENTS:
        out += buildstore.audit_all(table, f"AUDIT_DAY#{d.isoformat()}", MAX_EVENTS - len(out))
        d -= _dt.timedelta(days=1)
    return out
