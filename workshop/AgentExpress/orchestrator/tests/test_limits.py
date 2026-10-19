"""Per-user limits on starting runs (bff/limits.py) and the running total behind them
(app/features/observability/spend.py).

The two halves are separate deployments (the BFF Lambda cannot import app.*), joined only
by the key of one DynamoDB row. So the first test holds the key together, and the rest
drive each half against a fake table that does what DynamoDB does where it matters: an ADD
that creates the item, and a conditional write that is checked and applied as one step.
"""
from __future__ import annotations

import decimal
import json
import re
import sys
from datetime import datetime
from pathlib import Path

import pytest

ORCH = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ORCH / "bff"))

import builds  # noqa: E402
import limits  # noqa: E402

from app.features.observability import spend  # noqa: E402

D = decimal.Decimal


class ConditionalCheckFailed(Exception):
    response = {"Error": {"Code": "ConditionalCheckFailedException"}}


class Throttled(Exception):
    response = {"Error": {"Code": "ProvisionedThroughputExceededException"}}


class FakeTable:
    """Just the surface the two modules use. `update_item` understands the two expressions
    they send, and applies a ConditionExpression before the write, as DynamoDB does."""

    def __init__(self):
        self.items: dict[tuple, dict] = {}
        self.updates: list[dict] = []
        self.fail_with: Exception | None = None

    def get_item(self, Key, ConsistentRead=False, **kw):
        if self.fail_with:
            raise self.fail_with
        it = self.items.get((Key["session_id"], Key["sk"]))
        return {"Item": dict(it)} if it else {}

    def update_item(self, Key, UpdateExpression, ExpressionAttributeValues, ConditionExpression=None, **kw):
        if self.fail_with:
            raise self.fail_with
        self.updates.append({"Key": Key, "Update": UpdateExpression, "Values": ExpressionAttributeValues})
        k = (Key["session_id"], Key["sk"])
        it = self.items.get(k)
        if ConditionExpression is not None:
            # "attribute_not_exists(runs) OR runs < :max"
            ok = it is None or "runs" not in it or it["runs"] < ExpressionAttributeValues[":max"]
            if not ok:
                raise ConditionalCheckFailed()
        it = self.items.setdefault(k, {"session_id": k[0], "sk": k[1]})
        for attr in ("runs", "spent_usd"):
            if re.search(rf"ADD {attr} ", UpdateExpression):
                add = ExpressionAttributeValues[":one" if attr == "runs" else ":c"]
                it[attr] = it.get(attr, 0) + add
        it.setdefault("ttl", ExpressionAttributeValues[":ttl"])
        return {}


def at(hour=14, minute=30, day=10):
    return datetime(2026, 9, day, hour, minute)


def wf(daily=None, hourly=None):
    lim = {}
    if daily is not None:
        lim["userDailyUsd"] = daily
    if hourly is not None:
        lim["userRunsPerHour"] = hourly
    return {"orchestrator": {"limits": lim}}


# ---- the two halves agree on the row -------------------------------------------------------

def test_the_bff_and_the_runtime_file_the_running_total_under_the_same_key():
    for user in ("Alice@Example.com", " alice@example.com ", "", "x" * 500):
        assert limits.spend_key("2026-09-10", user) == spend.spend_key("2026-09-10", user)
    assert limits.month_key("2026-09") == spend.month_key("2026-09")
    assert limits.PREFIX == spend.PREFIX
    assert limits.ANONYMOUS == spend.ANONYMOUS
    assert limits.SPEND_TTL_SECONDS == spend.SPEND_TTL_SECONDS
    assert limits.MONTH_TTL_SECONDS == spend.MONTH_TTL_SECONDS
    assert limits.usd(D("0.0042")) == spend.usd(D("0.0042")) and limits.usd(12) == spend.usd(12)


def test_one_person_is_one_budget_however_their_sign_in_spells_the_address():
    assert limits.who("  Alice@Example.COM ") == "alice@example.com"
    assert limits.who("") == limits.ANONYMOUS == limits.who(None)


def test_the_counter_rows_cannot_be_mistaken_for_a_run_or_reach_the_date_index():
    # A run id is 12 hex digits; the by_date index is keyed on `date`, which these rows lack.
    assert not re.fullmatch(r"[0-9a-f]{12}", limits.spend_key("2026-09-10", "a"))
    t = FakeTable()
    spend_add(t, "a", "2026-09-10", "0.5")
    assert all("date" not in row for row in t.items.values())


# ---- what is configured --------------------------------------------------------------------

def test_no_limits_block_means_no_limits():
    assert limits.settings({}) is None
    assert limits.settings({"orchestrator": {}}) is None
    assert limits.settings(wf()) is None
    assert limits.settings(wf(daily=0, hourly=0)) is None


def test_a_value_that_is_not_usable_is_unset_here_rather_than_a_lockout():
    """The Builder's validator rejects these with a message; a hand-edited file must not
    turn a typo into 'refuse everyone'."""
    for bad in ("5", -3, True, None, [], {}):
        assert limits.settings(wf(daily=bad)) is None
    for bad in ("3", -1, 2.5, True):
        assert limits.settings(wf(hourly=bad)) is None


def test_settings_are_read_as_exact_money():
    cfg = limits.settings(wf(daily=0.1, hourly=7))
    assert cfg == {"daily": D("0.1"), "hourly": 7, "monthly": D(0)}
    assert limits.settings({"orchestrator": {"limits": {"monthlyUsd": 200}}}) == {
        "daily": D(0), "hourly": 0, "monthly": D(200)}


# ---- the daily spend limit -----------------------------------------------------------------

@pytest.fixture(autouse=True)
def _daily_limit_is_configured(monkeypatch):
    """The runtime adds to the total only when a daily limit is configured. On for every test
    here, and put back afterwards, so nothing leaks into the rest of the suite."""
    monkeypatch.setattr(spend, "daily_limit", lambda: D(1))
    monkeypatch.setattr(spend, "monthly_limit", lambda: D(0))


def spend_add(table, user, day, cost):
    """The runtime side adding one call's cost."""
    spend.add(user, day, D(str(cost)), table=table)


def test_a_user_under_the_limit_may_start():
    t = FakeTable()
    spend_add(t, "a@example.com", "2026-09-10", "1.20")
    limits.enforce(t, wf(daily=5), "a@example.com", now=at())


def test_a_user_at_the_limit_is_refused_with_the_amount_and_when_it_resets():
    t = FakeTable()
    spend_add(t, "a@example.com", "2026-09-10", "5.00")
    with pytest.raises(builds.BuildError) as e:
        limits.enforce(t, wf(daily=5), "a@example.com", now=at())
    assert e.value.status == 429
    assert "$5.00" in str(e.value) and "midnight Eastern" in str(e.value)


def test_spend_adds_up_across_calls_and_is_per_user_and_per_day():
    t = FakeTable()
    for c in ("1.00", "2.25", "0.75"):
        spend_add(t, "a@example.com", "2026-09-10", c)
    spend_add(t, "b@example.com", "2026-09-10", "100")
    spend_add(t, "a@example.com", "2026-09-09", "100")        # yesterday
    assert limits.spent_today(t, "a@example.com", at()) == D("4.00")
    with pytest.raises(builds.BuildError):
        limits.enforce(t, wf(daily=4), "a@example.com", now=at())
    limits.enforce(t, wf(daily=4.01), "a@example.com", now=at())     # one cent of room
    assert limits.spent_today(t, "nobody@example.com", at()) == 0


def test_a_new_day_starts_the_total_again():
    t = FakeTable()
    spend_add(t, "a@example.com", "2026-09-10", "9")
    limits.enforce(t, wf(daily=5), "a@example.com", now=at(day=11))


def test_a_small_amount_is_shown_with_enough_places_to_read():
    assert limits.usd(D("0.0042")) == "$0.0042"
    assert limits.usd(D("12.5")) == "$12.50"
    assert limits.usd(0) == "$0.00"


def test_a_refusal_for_spend_does_not_use_up_one_of_the_hours_runs():
    t = FakeTable()
    spend_add(t, "a@example.com", "2026-09-10", "9")
    for _ in range(5):
        with pytest.raises(builds.BuildError):
            limits.enforce(t, wf(daily=5, hourly=2), "a@example.com", now=at())
    assert not any("runs#" in u["Key"]["session_id"] for u in t.updates if "runs" in u["Update"])


def test_the_runtime_writes_nothing_without_a_daily_limit_or_a_cost(monkeypatch):
    t = FakeTable()
    monkeypatch.setattr(spend, "daily_limit", lambda: D(0))
    spend.add("a@example.com", "2026-09-10", D("3"), table=t)
    monkeypatch.setattr(spend, "daily_limit", lambda: D(1))
    spend.add("a@example.com", "2026-09-10", D(0), table=t)
    spend.add("a@example.com", "2026-09-10", None, table=t)
    spend.add("a@example.com", "2026-09-10", "not a number", table=t)
    spend.add("a@example.com", "2026-09-10", D("-1"), table=t)
    assert t.updates == []


def test_a_failed_add_is_logged_and_never_raises_into_a_run(capsys):
    t = FakeTable()
    t.fail_with = Throttled()
    spend_add(t, "a@example.com", "2026-09-10", "1")
    assert "spend not added" in capsys.readouterr().out


# ---- the runs-per-hour limit ---------------------------------------------------------------

def test_the_run_past_the_hourly_limit_is_refused_with_when_to_come_back():
    t = FakeTable()
    cfg = wf(hourly=3)
    for _ in range(3):
        limits.enforce(t, cfg, "a@example.com", now=at(hour=14, minute=5))
    with pytest.raises(builds.BuildError) as e:
        limits.enforce(t, cfg, "a@example.com", now=at(hour=14, minute=50))
    assert e.value.status == 429
    assert "3 runs this hour" in str(e.value) and "15:00 Eastern" in str(e.value)


def test_the_next_hour_and_another_user_each_start_fresh():
    t = FakeTable()
    cfg = wf(hourly=1)
    limits.enforce(t, cfg, "a@example.com", now=at(hour=14))
    limits.enforce(t, cfg, "b@example.com", now=at(hour=14))
    limits.enforce(t, cfg, "a@example.com", now=at(hour=15))
    with pytest.raises(builds.BuildError):
        limits.enforce(t, cfg, "a@example.com", now=at(hour=15, minute=59))


def test_the_last_slot_goes_to_one_request_not_two():
    """The check and the count are one conditional write, so a second request arriving
    'at the same time' meets the count the first one made."""
    t = FakeTable()
    cfg = wf(hourly=1)
    limits.enforce(t, cfg, "a@example.com", now=at())
    with pytest.raises(builds.BuildError):
        limits.enforce(t, cfg, "a@example.com", now=at())
    writes = [u for u in t.updates if "runs" in u["Update"]]
    assert len(writes) == 2                       # both went to the table, one condition decided


def test_a_rerun_is_held_to_the_spend_limit_but_is_not_a_new_run():
    t = FakeTable()
    cfg = wf(daily=5, hourly=1)
    limits.enforce(t, cfg, "a@example.com", now=at())                 # the run: takes the hour's one slot
    for _ in range(3):
        limits.enforce(t, cfg, "a@example.com", count_run=False, now=at())
    spend_add(t, "a@example.com", "2026-09-10", "6")
    with pytest.raises(builds.BuildError) as e:
        limits.enforce(t, cfg, "a@example.com", count_run=False, now=at())
    assert "spend limit" in str(e.value)


def test_the_counter_row_expires_on_its_own():
    t = FakeTable()
    limits.enforce(t, wf(hourly=5), "a@example.com", now=at())
    (row,) = t.items.values()
    assert row["runs"] == 1 and row["ttl"] > 0


# ---- when the counters are unavailable -----------------------------------------------------

def test_a_counter_that_cannot_be_read_lets_the_run_start_and_says_so(capsys):
    t = FakeTable()
    t.fail_with = Throttled()
    limits.enforce(t, wf(daily=1, hourly=1), "a@example.com", now=at())
    assert "not enforced" in capsys.readouterr().out


def test_no_table_or_no_limit_is_a_no_op_that_touches_nothing():
    t = FakeTable()
    limits.enforce(t, {}, "a@example.com", now=at())
    limits.enforce(None, wf(daily=1), "a@example.com", now=at())
    assert t.updates == [] and t.items == {}


# ---- wired into the BFF --------------------------------------------------------------------

def _handler():
    """bff/handler.py as text: the wiring is what is asserted, not a Lambda invocation."""
    return (ORCH / "bff" / "handler.py").read_text()


def test_a_run_start_and_a_rerun_both_go_through_the_limits():
    src = _handler()
    # _begin_run: first thing, before the run is copied or written.
    begin = src[src.index("def _begin_run"):]
    body = begin[:begin.index("session_id = uuid.uuid4()")]
    assert "limits.enforce(target.get(\"telemetry\"), target.get(\"raw\"), user)" in body
    # /rerun: spend only, as the run's own user.
    rerun = src[src.index('path.endswith("/rerun")'):]
    rerun = rerun[:rerun.index('path.endswith("/evaluate")')]
    assert "limits.enforce(" in rerun and "count_run=False" in rerun and "_session_user(sid" in rerun
    # A trigger and a person share _begin_run, so one check covers both.
    assert src.count("_begin_run(") >= 3


def test_the_limit_blocks_in_the_telemetry_store_path_when_a_cost_is_recorded(monkeypatch):
    """store.put ends by adding the call's cost to the user's total."""
    from app.features.observability import store
    from app.features.observability.records import CallRecord
    t = FakeTable()
    t.put_item = lambda Item: None
    monkeypatch.setattr(store, "TELEMETRY_TABLE", "t")
    monkeypatch.setattr(store, "_tbl", lambda: t)
    monkeypatch.setenv("TELEMETRY_TABLE", "t")
    # The `spend` module store itself holds: another test may have reloaded the package, which
    # would leave the one imported at the top of this file a different object.
    monkeypatch.setattr(store.spend, "daily_limit", lambda: D(1))
    rec = CallRecord(session_id="s1", agent_id="a", user="Al@example.com", kind="llm", label="m", mode="bedrock",
                     cost_usd=D("0.25"), date="2026-09-10")
    store.put(rec)
    store.put(rec)
    assert limits.spent_today(t, "al@example.com", at()) == D("0.50")


def test_a_failed_telemetry_write_adds_nothing_to_the_total(monkeypatch):
    from app.features.observability import store
    from app.features.observability.records import CallRecord
    t = FakeTable()

    def boom(Item):
        raise Throttled()
    t.put_item = boom
    monkeypatch.setattr(store, "TELEMETRY_TABLE", "t")
    monkeypatch.setattr(store, "_tbl", lambda: t)
    monkeypatch.setenv("TELEMETRY_TABLE", "t")
    monkeypatch.setattr(store.spend, "daily_limit", lambda: D(1))
    store.put(CallRecord(session_id="s1", agent_id="a", user="a@example.com", kind="llm", label="m",
                         mode="bedrock", cost_usd=D("1"), date="2026-09-10"))
    assert t.updates == []


def test_a_trigger_run_is_limited_as_whoever_it_runs_as():
    """_begin_run gets the (owner, user) that triggers.run_identity picked, and keys the limit on
    `user`. A service trigger is its own user; an owner trigger spends the owner's budget."""
    import triggers  # noqa: PLC0415
    svc = triggers.run_identity("nightly", {"runAs": "service"})
    assert svc == ("trigger:nightly", "trigger:nightly")
    t = FakeTable()
    cfg = wf(hourly=1)
    limits.enforce(t, cfg, svc[1], now=at())
    with pytest.raises(builds.BuildError):
        limits.enforce(t, cfg, svc[1], now=at())
    limits.enforce(t, cfg, "a@example.com", now=at())          # a person is a different budget


def test_the_key_is_documented_where_the_framework_documents_keys():
    keys = json.loads((ORCH / "app" / "keys.json").read_text())
    entry = keys["orchestrator"]["keys"]["limits"]
    assert entry["type"] == "object"
    assert set(entry["default"]) == {"userDailyUsd", "userRunsPerHour", "monthlyUsd"}
    assert all(v == 0 for v in entry["default"].values())          # off unless set
    assert "limits" in json.loads((ORCH / "app" / "workflow.schema.json").read_text())["properties"]["orchestrator"]["properties"]


# ---- the month, for the whole deployment ---------------------------------------------------

def month_wf(monthly):
    return {"orchestrator": {"limits": {"monthlyUsd": monthly}}}


@pytest.fixture
def monthly_on(monkeypatch):
    monkeypatch.setattr(spend, "daily_limit", lambda: D(0))
    monkeypatch.setattr(spend, "monthly_limit", lambda: D(100))


def test_the_month_adds_up_across_users_and_days_and_refuses_everyone_at_the_limit(monthly_on):
    t = FakeTable()
    spend.add("a@example.com", "2026-09-03", D("60"), table=t)
    spend.add("b@example.com", "2026-09-20", D("39.99"), table=t)
    limits.enforce(t, month_wf(100), "c@example.com", now=at())               # $0.01 of room
    spend.add("c@example.com", "2026-09-21", D("0.01"), table=t)
    for who in ("a@example.com", "b@example.com", "new@example.com"):
        with pytest.raises(builds.BuildError) as e:
            limits.enforce(t, month_wf(100), who, now=at())
        assert e.value.status == 429
        assert "monthly spend limit of $100.00" in str(e.value) and "the 1st" in str(e.value)


def test_a_new_month_starts_again(monthly_on):
    t = FakeTable()
    spend.add("a@example.com", "2026-09-28", D("500"), table=t)
    limits.enforce(t, month_wf(100), "a@example.com", now=datetime(2026, 10, 1, 0, 5))


def test_the_month_is_cut_by_the_date_the_cost_was_incurred_not_by_when_it_was_written(monthly_on):
    t = FakeTable()
    spend.add("a@example.com", "2026-08-31", D("500"), table=t)             # last month's call
    assert spend._read(t, spend.month_key("2026-09")) == 0
    assert spend._read(t, spend.month_key("2026-08")) == D("500")


def test_the_month_is_checked_before_the_day(monthly_on, monkeypatch):
    monkeypatch.setattr(spend, "daily_limit", lambda: D(1))
    t = FakeTable()
    spend.add("a@example.com", "2026-09-10", D("500"), table=t)
    cfg = {"orchestrator": {"limits": {"monthlyUsd": 100, "userDailyUsd": 5}}}
    with pytest.raises(builds.BuildError) as e:
        limits.enforce(t, cfg, "a@example.com", now=at())
    assert "monthly" in str(e.value)


def test_each_total_is_written_only_when_its_own_limit_is_set(monkeypatch):
    t = FakeTable()
    monkeypatch.setattr(spend, "daily_limit", lambda: D(1))
    monkeypatch.setattr(spend, "monthly_limit", lambda: D(0))
    spend.add("a@example.com", "2026-09-10", D("1"), table=t)
    assert [k[0].split("#")[1] for k in t.items] == ["spend"]                 # day only
    t2 = FakeTable()
    monkeypatch.setattr(spend, "daily_limit", lambda: D(0))
    monkeypatch.setattr(spend, "monthly_limit", lambda: D(100))
    spend.add("a@example.com", "2026-09-10", D("1"), table=t2)
    assert [k[0].split("#")[1] for k in t2.items] == ["month"]                # month only
    t3 = FakeTable()
    monkeypatch.setattr(spend, "daily_limit", lambda: D(1))
    spend.add("a@example.com", "2026-09-10", D("1"), table=t3)
    assert sorted(k[0].split("#")[1] for k in t3.items) == ["month", "spend"]  # both


def test_a_failed_day_write_does_not_stop_the_month_from_being_added(monkeypatch, capsys):
    monkeypatch.setattr(spend, "monthly_limit", lambda: D(100))
    t = FakeTable()
    real = t.update_item

    def flaky(Key, **kw):
        if "#spend#" in Key["session_id"]:
            raise Throttled()
        return real(Key=Key, **kw)
    t.update_item = flaky
    spend.add("a@example.com", "2026-09-10", D("2"), table=t)
    assert "spend not added" in capsys.readouterr().out
    assert spend._read(t, spend.month_key("2026-09")) == 2


# ---- the step guard: a run that was let in and has since crossed a limit --------------------

from app.common.errors import BudgetReached  # noqa: E402


def test_a_step_does_not_start_once_the_users_day_is_spent():
    t = FakeTable()
    spend_add(t, "a@example.com", "2026-09-10", "1.00")
    with pytest.raises(BudgetReached) as e:
        spend.guard("a@example.com", table=t, now=at())
    assert "Today's spend limit of $1.00 is reached" in str(e.value) and "midnight Eastern" in str(e.value)
    assert "did not start" in str(e.value)


def test_a_step_does_start_while_there_is_room_and_for_someone_else():
    t = FakeTable()
    spend_add(t, "a@example.com", "2026-09-10", "0.99")
    spend.guard("a@example.com", table=t, now=at())
    spend_add(t, "a@example.com", "2026-09-09", "50")                               # yesterday
    spend.guard("a@example.com", table=t, now=at())
    spend_add(t, "b@example.com", "2026-09-10", "1.00")
    spend.guard("a@example.com", table=t, now=at())


def test_the_guard_honours_the_month_for_everyone(monthly_on):
    t = FakeTable()
    spend.add("other@example.com", "2026-09-04", D("100"), table=t)
    with pytest.raises(BudgetReached) as e:
        spend.guard("a@example.com", table=t, now=at())
    assert "monthly spend limit" in str(e.value) and "the 1st" in str(e.value)


def test_the_guard_does_nothing_and_reads_nothing_without_a_limit(monkeypatch):
    monkeypatch.setattr(spend, "daily_limit", lambda: D(0))
    monkeypatch.setattr(spend, "monthly_limit", lambda: D(0))

    class Boom(FakeTable):
        def get_item(self, **kw):
            raise AssertionError("must not read a counter when no limit is set")
    spend.guard("a@example.com", table=Boom(), now=at())


def test_the_guard_fails_open_when_the_counters_cannot_be_read(capsys):
    t = FakeTable()
    t.fail_with = Throttled()
    spend.guard("a@example.com", table=t, now=at())
    assert "not enforced" in capsys.readouterr().out


def test_a_budget_stop_is_a_dependency_failure_so_the_step_reports_it_like_any_other():
    # Read both classes off ONE module object: another test pops app.common.* from sys.modules,
    # so a name imported at the top of this file can belong to an older copy of the module.
    import app.common.errors as errors
    assert issubclass(errors.BudgetReached, errors.DependencyUnavailable)


def test_the_agent_node_checks_the_budget_before_it_does_any_work():
    src = (ORCH / "app" / "orchestrator" / "nodes.py").read_text()
    node = src[src.index("async def node(state: dict, config: RunnableConfig)"):]
    node = node[:node.index("def make_branch_node")]
    guard = node.index("spend.guard(ctx.user)")
    # inside the try that reports a failed step, before the input guardrail and the agent itself
    assert node.rindex("try:", 0, guard) > node.index("_pcap = otel.begin_prompt_capture")
    assert guard < node.index("_guardrail_input(ctx)") < node.index("await agent.run(ctx)")


# ---- usage, for the page -------------------------------------------------------------------

def test_usage_says_nothing_without_a_limit_or_a_table():
    assert limits.usage(FakeTable(), {}, "a@example.com", now=at()) == {"limits": False}
    assert limits.usage(None, wf(daily=5), "a@example.com", now=at()) == {"limits": False}


def test_usage_reports_each_limit_that_is_set_and_flags_close_and_reached():
    t = FakeTable()
    spend_add(t, "a@example.com", "2026-09-10", "4.50")
    limits.enforce(t, wf(hourly=5), "a@example.com", now=at())
    limits.enforce(t, wf(hourly=5), "a@example.com", now=at())
    cfg = {"orchestrator": {"limits": {"userDailyUsd": 5, "userRunsPerHour": 2}}}
    u = limits.usage(t, cfg, "a@example.com", now=at(hour=14, minute=30))
    assert u["limits"] is True
    assert u["daily"] == {"limit": 5.0, "used": 4.5, "resets": "midnight Eastern time", "warn": True, "reached": False}
    assert u["hourly"] == {"limit": 2.0, "used": 2.0, "resets": "15:00 Eastern time", "warn": True, "reached": True}
    assert "monthly" not in u                       # not set, so not shown


def test_usage_is_per_user():
    t = FakeTable()
    spend_add(t, "a@example.com", "2026-09-10", "4.50")
    assert limits.usage(t, wf(daily=5), "b@example.com", now=at())["daily"]["used"] == 0


def test_a_counter_that_cannot_be_read_is_left_out_not_shown_as_zero(capsys):
    t = FakeTable()
    t.fail_with = Throttled()
    u = limits.usage(t, wf(daily=5, hourly=3), "a@example.com", now=at())
    assert u == {"limits": True}
    assert "usage not read" in capsys.readouterr().out


def test_the_usage_route_exists_in_the_handler_and_both_iac_route_lists():
    h = (ORCH / "bff" / "handler.py").read_text()
    assert 'path == "/api/limits"' in h and "limits.usage(telemetry_tbl, workflow.RAW" in h
    assert '"GET /api/limits"' in (ORCH / "terraform" / "bff.tf").read_text()
    assert '"/api/limits", methods: [HttpMethod.GET]' in (ORCH / "cdk" / "lib" / "orchestrator-stack.ts").read_text()
