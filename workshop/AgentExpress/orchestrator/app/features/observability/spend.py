"""The running spend behind `orchestrator.limits`: a user's total for the day, and the
deployment's total for the month.

Every metered call (model, tool, compute) already writes a telemetry row with its cost.
Adding those up per start would mean reading rows that carry whole prompts and answers,
so instead each call also adds its cost to two small reserved rows:

    session_id = _limit#spend#<YYYY-MM-DD>#<user>      sk = total      spent_usd   (userDailyUsd)
    session_id = _limit#month#<YYYY-MM>                sk = total      spent_usd   (monthlyUsd)

The BFF reads them when a run starts (bff/limits.py, which builds the SAME keys;
tests/test_limits.py holds the two together), and this module reads them again before each
agent step starts (`guard`), so a long run stops at the next step boundary rather than
spending on past the limit.

Each row is written only when ITS limit is configured: with none, it would be an extra
DynamoDB write per metered call for a number nothing reads. Turning a limit on starts that
period's total from that moment. Best-effort like telemetry itself: a failed add is logged
and never breaks a run, so a total can run a little behind but never blocks work. `guard`
fails open for the same reason.
"""
from __future__ import annotations

import os
import time
from datetime import datetime
from decimal import Decimal

from app.common import clock, config
from app.common.errors import BudgetReached

PREFIX = "_limit#"
ANONYMOUS = "(anonymous)"
USER_MAX = 200
SPEND_TTL_SECONDS = 3 * 86400
MONTH_TTL_SECONDS = 62 * 86400

_table = None


def who(user: str) -> str:
    """Same rule as bff/limits.py: lower-cased and trimmed, so one person is one total."""
    return (str(user or "").strip().lower()[:USER_MAX]) or ANONYMOUS


def spend_key(day: str, user: str) -> str:
    return f"{PREFIX}spend#{day}#{who(user)}"


def month_key(month: str) -> str:
    return f"{PREFIX}month#{month}"


def _limit(name: str) -> Decimal:
    lim = config.ORCHESTRATOR.get("limits") or {}
    v = lim.get(name) if isinstance(lim, dict) else None
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return Decimal(0)
    return Decimal(str(v)) if v > 0 else Decimal(0)


def daily_limit() -> Decimal:
    """`orchestrator.limits.userDailyUsd`, or 0 when no daily limit is set."""
    return _limit("userDailyUsd")


def monthly_limit() -> Decimal:
    """`orchestrator.limits.monthlyUsd`, or 0 when no monthly limit is set."""
    return _limit("monthlyUsd")


def usd(v) -> str:
    """Money for a message: cents for a dollar or more, four places below that. Same as bff/limits.py."""
    d = Decimal(str(v))
    return f"${d:,.2f}" if d >= 1 or d == 0 else f"${d:,.4f}"


def _tbl():
    global _table
    if _table is None:
        import boto3
        _table = boto3.resource("dynamodb").Table(os.getenv("TELEMETRY_TABLE", ""))
    return _table


def _add_to(table, key: str, cost: Decimal, ttl_seconds: int) -> None:
    table.update_item(
        Key={"session_id": key, "sk": "total"},
        UpdateExpression="ADD spent_usd :c SET #ttl = if_not_exists(#ttl, :ttl)",
        ExpressionAttributeNames={"#ttl": "ttl"},
        ExpressionAttributeValues={":c": cost, ":ttl": int(time.time()) + ttl_seconds})


def add(user: str, day: str, cost, *, table=None) -> None:
    """Add `cost` (USD) to `user`'s total for `day`, and to the month's. A no-op for a total
    whose limit is not set, and for no cost."""
    if not os.getenv("TELEMETRY_TABLE") and table is None:
        return
    daily, monthly = daily_limit(), monthly_limit()
    if not daily and not monthly:
        return
    try:
        cost = Decimal(str(cost or 0))
    except Exception:  # noqa: BLE001 - not a number: nothing to add
        return
    if cost <= 0:
        return
    t = table or _tbl()
    for wanted, key, ttl in ((daily, spend_key(day, user), SPEND_TTL_SECONDS),
                             (monthly, month_key(str(day)[:7]), MONTH_TTL_SECONDS)):
        if not wanted:
            continue
        try:
            _add_to(t, key, cost, ttl)
        except Exception as e:  # noqa: BLE001 - telemetry-grade: never break a run
            print(f"[limits] spend not added: {type(e).__name__}: {e}")


def _read(table, key: str) -> Decimal:
    item = table.get_item(Key={"session_id": key, "sk": "total"}, ConsistentRead=True).get("Item") or {}
    return Decimal(str(item.get("spent_usd") or 0))


def guard(user: str, *, table=None, now: datetime | None = None) -> None:
    """Raise BudgetReached when a spend limit is already reached, so the step about to start
    does not. Called before each agent step (app/orchestrator/nodes.py).

    The BFF refuses a new run at the door; this is the other half, for a run that was let in
    under the limit and has since crossed it. The step has done nothing yet, so stopping here
    leaves nothing half-finished. Fails open: unreadable counters never stop a run."""
    daily, monthly = daily_limit(), monthly_limit()
    if not daily and not monthly:
        return
    if not os.getenv("TELEMETRY_TABLE") and table is None:
        return
    now = now or clock.now_et()
    try:
        t = table or _tbl()
        if monthly:
            used = _read(t, month_key(now.strftime("%Y-%m")))
            if used >= monthly:
                raise BudgetReached(
                    f"This app's monthly spend limit of {usd(monthly)} is reached ({usd(used)} used), so this "
                    "step did not start. It resets on the 1st, Eastern time; run it again then, "
                    "or ask for the limit to be raised.")
        if daily:
            used = _read(t, spend_key(now.strftime(clock.DATE_FMT), user))
            if used >= daily:
                raise BudgetReached(
                    f"Today's spend limit of {usd(daily)} is reached ({usd(used)} used), so this step did "
                    "not start. It resets at midnight Eastern time; run it again then, "
                    "or ask for the limit to be raised.")
    except BudgetReached:
        raise
    except Exception as e:  # noqa: BLE001 - a cost guard must not take a run down
        print(f"[limits] not enforced, counters unavailable: {type(e).__name__}: {e}")
