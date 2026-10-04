"""Eastern-Time clock — the ONE place time is formatted for the whole project.

Every STORED timestamp uses US Eastern Time (America/New_York) in the format
``YYYY-MM-DD HH:MM:SS``. They are sort keys and the telemetry table's `date` GSI
partition, so the stored zone is fixed: changing it would re-key existing rows. The
console DISPLAYS each one in the viewer's own timezone (web/src/lib/clock.ts converts
with this same DST rule). This module is dependency-free (no tzdata
needed on Lambda or in the container): it computes the US Eastern UTC offset with
the statutory DST rule — EDT (UTC-4) from the 2nd Sunday of March 02:00 to the
1st Sunday of November 02:00, otherwise EST (UTC-5).

NOTE: bff/clock.py duplicates this module's CODE (the BFF Lambda ships as its own
zip and cannot import app.common). The two are kept in step by
tests/test_clock_parity.py, which compares their ASTs — the docstrings differ, and
a comment asking the next reader to "keep them in sync" was not keeping them in
sync.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

FMT = "%Y-%m-%d %H:%M:%S"
DATE_FMT = "%Y-%m-%d"


def _et_offset_hours(dt_utc: datetime) -> int:
    """US Eastern offset (in hours) for a given UTC instant: -4 (EDT) or -5 (EST)."""
    y = dt_utc.year
    # 2nd Sunday of March at 02:00 EST == 07:00 UTC.
    mar = datetime(y, 3, 8, 7, tzinfo=UTC)
    mar += timedelta(days=(6 - mar.weekday()) % 7)
    # 1st Sunday of November at 02:00 EDT == 06:00 UTC.
    nov = datetime(y, 11, 1, 6, tzinfo=UTC)
    nov += timedelta(days=(6 - nov.weekday()) % 7)
    return -4 if mar <= dt_utc < nov else -5


def _to_et(dt_utc: datetime) -> datetime:
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=UTC)
    return dt_utc + timedelta(hours=_et_offset_hours(dt_utc))


def now_et() -> datetime:
    """Current Eastern wall-clock time as a naive datetime (tz stripped)."""
    return _to_et(datetime.now(UTC)).replace(tzinfo=None)


def now_str() -> str:
    """Current Eastern time as 'YYYY-MM-DD HH:MM:SS'."""
    return now_et().strftime(FMT)


def today_str() -> str:
    """Current Eastern date as 'YYYY-MM-DD'."""
    return now_et().strftime(DATE_FMT)
