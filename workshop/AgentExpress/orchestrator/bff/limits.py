"""Per-user limits on starting runs: a daily spend limit and a runs-per-hour limit.

Set in workflow.json, off unless set:

    "orchestrator": { "limits": { "userDailyUsd": 5, "userRunsPerHour": 20, "monthlyUsd": 200 } }

WHAT IS CHECKED, AND WHEN
A run is refused (HTTP 429, with the reason and when it resets) when
  * this deployment has already spent `monthlyUsd` this calendar month (Eastern time), or
  * its user has already spent `userDailyUsd` today (Eastern time, the same day the
    telemetry table files a row under), or
  * its user has already started `userRunsPerHour` runs in the current clock hour.
It is checked where a run starts (`_begin_run`: a person on Start run, and a trigger),
and where one re-runs (spend only: a re-run is more model calls, not a new run).

A run that is let in and then crosses a spend limit is stopped at the NEXT STEP: the runtime
checks the same totals before each agent starts (features/observability/spend.py `guard`), and
the step shows as failed with the limit and its reset time. That bounds the overshoot to one
step's work, instead of the rest of the run.

WHAT IS NOT PROMISED
  * A step that is already going when the limit is crossed finishes: the check is at step
    boundaries, not inside a model call. So a user can overshoot by about one step's cost.
    Counting runs per hour is what bounds how many can be started before spend catches up.
  * The hour is a fixed clock hour, not a sliding window: a user can start the limit's
    worth just before the hour and again just after it.
  * It fails OPEN. If a counter cannot be read or written, the run starts and the failure
    is logged. This is a cost guard, not a security control, and (like telemetry) it must
    not take the app down with it.
  * With `idp = "none"` nobody is identified, so everyone shares one budget.

WHERE THE NUMBERS LIVE
Two kinds of reserved row in the telemetry table, which has the keys already (session_id,
sk) and a TTL. Run ids are 12 hex digits and never start with `_limit#`, and neither row
has a `date` attribute, so the by_date index the aggregation reads never sees them.

    spend  session_id = _limit#spend#<YYYY-MM-DD>#<user>   sk = total   spent_usd
    month  session_id = _limit#month#<YYYY-MM>             sk = total   spent_usd
    runs   session_id = _limit#runs#<YYYY-MM-DDTHH>#<user> sk = count   runs

`spent_usd` (spend and month) is added to by the runtime as each model or tool call is metered
(app/features/observability/spend.py, which builds the SAME key; tests/test_limits.py
holds the two together). `runs` is bumped here with a conditional write, so two requests
at once cannot both take the last slot.
"""
from __future__ import annotations

import decimal
import time
from datetime import timedelta

import builds
import clock

PREFIX = "_limit#"
#: How long a counter row is kept. DynamoDB deletes expired rows lazily, so reads always
#: use the exact key for the current day or hour and never depend on the delete.
SPEND_TTL_SECONDS = 3 * 86400
MONTH_TTL_SECONDS = 62 * 86400
RUNS_TTL_SECONDS = 3 * 3600
ANONYMOUS = "(anonymous)"
USER_MAX = 200


def who(user: str) -> str:
    """The name a user's counters are filed under. Lower-cased and trimmed so one person
    is one budget however their sign-in spells the address."""
    return (str(user or "").strip().lower()[:USER_MAX]) or ANONYMOUS


def spend_key(day: str, user: str) -> str:
    return f"{PREFIX}spend#{day}#{who(user)}"


def month_key(month: str) -> str:
    return f"{PREFIX}month#{month}"


def runs_key(hour: str, user: str) -> str:
    return f"{PREFIX}runs#{hour}#{who(user)}"


def settings(raw: dict | None) -> dict | None:
    """`orchestrator.limits`, normalised; None when no limit is set. A value that is not
    a usable number is treated as unset here (the Builder's validator is what rejects it
    with a message), so a hand-edited file cannot turn a typo into a lockout."""
    lim = ((raw or {}).get("orchestrator") or {}).get("limits")
    if not isinstance(lim, dict):
        return None
    daily = _money(lim.get("userDailyUsd"))
    hourly = lim.get("userRunsPerHour")
    hourly = hourly if isinstance(hourly, int) and not isinstance(hourly, bool) and hourly > 0 else 0
    monthly = _money(lim.get("monthlyUsd"))
    if not daily and not hourly and not monthly:
        return None
    return {"daily": daily, "hourly": hourly, "monthly": monthly}


def _money(v) -> decimal.Decimal:
    if isinstance(v, bool) or not isinstance(v, (int, float, decimal.Decimal)):
        return decimal.Decimal(0)
    d = decimal.Decimal(str(v))
    return d if d > 0 else decimal.Decimal(0)


def usd(v) -> str:
    """Money for a message: cents for a dollar or more, four places below that."""
    d = decimal.Decimal(str(v))
    return f"${d:,.2f}" if d >= 1 or d == 0 else f"${d:,.4f}"


def _hour_key(now) -> str:
    return now.strftime("%Y-%m-%dT%H")


def spent_today(table, user: str, now=None) -> decimal.Decimal:
    """What `user` has spent so far today, from the running total the runtime keeps."""
    now = now or clock.now_et()
    item = table.get_item(Key={"session_id": spend_key(now.strftime(clock.DATE_FMT), user), "sk": "total"},
                          ConsistentRead=True).get("Item") or {}
    return decimal.Decimal(str(item.get("spent_usd") or 0))


def spent_this_month(table, now=None) -> decimal.Decimal:
    """What this deployment has spent so far this month, from the running total."""
    now = now or clock.now_et()
    item = table.get_item(Key={"session_id": month_key(now.strftime("%Y-%m")), "sk": "total"},
                          ConsistentRead=True).get("Item") or {}
    return decimal.Decimal(str(item.get("spent_usd") or 0))


def runs_this_hour(table, user: str, now=None) -> int:
    now = now or clock.now_et()
    item = table.get_item(Key={"session_id": runs_key(_hour_key(now), user), "sk": "count"},
                          ConsistentRead=True).get("Item") or {}
    return int(item.get("runs") or 0)


def enforce(table, raw: dict | None, user: str, *, count_run: bool = True, now=None) -> None:
    """Raise `builds.BuildError(429, ...)` when `user` may not start (or re-run) now.

    Spend is read first, so a refusal for spend does not use up one of the hour's runs.
    `count_run=False` is a re-run: checked against spend, not counted as a new run.
    Any failure of the counters themselves lets the run through (see the module docstring)."""
    cfg = settings(raw)
    if cfg is None or table is None:
        return
    now = now or clock.now_et()
    try:
        if cfg["monthly"]:
            used = spent_this_month(table, now)
            if used >= cfg["monthly"]:
                raise builds.BuildError(
                    429, f"This app's monthly spend limit of {usd(cfg['monthly'])} is reached ({usd(used)} used). "
                         "It resets on the 1st, Eastern time.")
        if cfg["daily"]:
            spent = spent_today(table, user, now)
            if spent >= cfg["daily"]:
                raise builds.BuildError(
                    429, f"Today's spend limit of {usd(cfg['daily'])} is reached ({usd(spent)} used). "
                         "It resets at midnight Eastern time.")
        if cfg["hourly"] and count_run:
            _take_run_slot(table, user, cfg["hourly"], now)
    except builds.BuildError:
        raise
    except Exception as e:  # noqa: BLE001 - a cost guard must not take the app down
        print(f"[limits] not enforced, counters unavailable: {type(e).__name__}: {e}")


def _take_run_slot(table, user: str, limit: int, now) -> None:
    """Count one started run in this clock hour, refusing the one past `limit`. One
    conditional write: the check and the count cannot be split by a second request."""
    try:
        table.update_item(
            Key={"session_id": runs_key(_hour_key(now), user), "sk": "count"},
            UpdateExpression="ADD runs :one SET #ttl = if_not_exists(#ttl, :ttl)",
            ConditionExpression="attribute_not_exists(runs) OR runs < :max",
            ExpressionAttributeNames={"#ttl": "ttl"},
            ExpressionAttributeValues={":one": 1, ":max": limit, ":ttl": int(time.time()) + RUNS_TTL_SECONDS})
    except Exception as e:
        code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
        if code != "ConditionalCheckFailedException":
            raise
        nxt = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)).strftime("%H:%M")
        raise builds.BuildError(
            429, f"You have started {limit} runs this hour, which is the limit. "
                 f"You can start another at {nxt} Eastern time.") from None


#: Show a warning once usage reaches this share of a limit. A constant, not a setting: it only
#: changes a colour in the page, and a setting that changes nothing is one more thing to get wrong.
WARN_AT = decimal.Decimal("0.8")


def usage(table, raw: dict | None, user: str, now=None) -> dict:
    """What `user` has used against each limit that is set, for the page. `{"limits": False}`
    when none is. A counter that cannot be read is left out (the page shows no figure), never
    a made-up zero."""
    cfg = settings(raw)
    if cfg is None or table is None:
        return {"limits": False}
    now = now or clock.now_et()
    out: dict = {"limits": True}

    def put(name: str, limit, read, money: bool, resets: str) -> None:
        if not limit:
            return
        try:
            used = read()
        except Exception as e:  # noqa: BLE001 - unreadable usage is logged and does not block
            print(f"[limits] usage not read ({name}): {type(e).__name__}: {e}")
            return
        share = decimal.Decimal(str(used)) / decimal.Decimal(str(limit))
        out[name] = {"limit": float(limit), "used": float(used), "resets": resets,
                     "warn": share >= WARN_AT, "reached": share >= 1}

    put("daily", cfg["daily"], lambda: spent_today(table, user, now), True, "midnight Eastern time")
    put("monthly", cfg["monthly"], lambda: spent_this_month(table, now), True, "the 1st, Eastern time")
    nxt = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)).strftime("%H:%M")
    put("hourly", cfg["hourly"], lambda: runs_this_hour(table, user, now), False, f"{nxt} Eastern time")
    return out
