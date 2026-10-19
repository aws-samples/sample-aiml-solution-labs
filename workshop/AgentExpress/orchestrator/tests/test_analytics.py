"""Operational analytics (bff/analytics.py) and GET /api/analytics.

What has to hold:
  * the counts are right, and a row of an unknown kind or status is counted as "other",
    never dropped;
  * p50/p95 are nearest-rank over the rows read, including on one to a few rows;
  * the health score is the written formula, recomputable by hand, with its reasons;
  * agents no guardrail screens, and tools no Cedar policy decides, are listed;
  * the route is for `admin` only, refuses before reading, keeps the aggregate's date-span
    cap, asks for the analytics fields only (never prompt or answer text), says when it
    stopped short, and is declared on both IaC route lists.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import analytics as A
import boto3
import pytest

moto = pytest.importorskip("moto")

from test_builds import call  # noqa: E402
from test_builds import env as env  # noqa: E402

SECRET = "PROMPT-TEXT-" + "THAT-MUST-NOT-LEAVE"   # a marker, joined so scanners do not take it for a password


def row(kind="llm", status="ok", agent="a", label="m", ms=100):
    out = {"kind": kind, "status": status, "agent_id": agent, "label": label}
    if ms is not None:
        out["latency_ms"] = ms
    return out


def agent(summary, aid):
    return next(a for a in summary["agents"] if a["agentId"] == aid)


def tool(summary, key):
    return next(t for t in summary["tools"] if t["tool"] == key)


# --- counting ----------------------------------------------------------------------------

def test_no_rows_is_an_empty_report():
    s = A.summarize([])
    assert s["rowsRead"] == 0 and s["capped"] is False and s["maxRows"] == A.MAX_ROWS
    assert s["tools"] == [] and s["agents"] == [] and s["kinds"] == {}


def test_one_model_call():
    s = A.summarize([row(ms=420)])
    a = agent(s, "a")
    assert (a["calls"], a["errors"], a["errorRate"]) == (1, 0, 0)
    assert a["p50Ms"] == 420 and a["p95Ms"] == 420
    assert a["health"] == 100 and a["reasons"] == []
    assert s["kinds"] == {"llm": 1} and s["tools"] == []


def test_mixed_kinds_are_each_counted_where_they_belong():
    rows = [
        row("llm", ms=100), row("llm", "error", ms=50),
        row("tool", label="kb", ms=30), row("tool", "error", label="kb", ms=70),
        row("policy", "allowed", label="kb___retrieve", ms=30),
        row("policy", "denied", label="kb___retrieve", ms=5),
        row("policy", "log-only", label="docs", ms=5),
        row("guardrail", "passed", label="INPUT"), row("guardrail", "blocked", label="OUTPUT"),
        row("guardrail", "anonymized", label="INPUT"),
        row("memory", "agentcore", label="recall"), row("eval", "ok", label="Builtin.Helpfulness"),
        row("agentcore", "ok", agent="__session__", label="runtime-compute", ms=9000),
    ]
    s = A.summarize(rows)
    assert s["rowsRead"] == 13
    assert s["kinds"] == {"agentcore": 1, "eval": 1, "guardrail": 3, "llm": 2, "memory": 1,
                          "policy": 3, "tool": 2}
    # The runtime's compute rows are the session's, not an agent's.
    assert [x["agentId"] for x in s["agents"]] == ["a"]
    a = agent(s, "a")
    assert a["rows"] == 12
    assert (a["calls"], a["errors"]) == (4, 2) and a["errorRate"] == 0.5
    assert (a["guardrailChecks"], a["guardrailBlocks"]) == (3, 1)
    assert (a["allowed"], a["denied"], a["logOnly"]) == (1, 1, 1) and a["denialRate"] == 0.5
    # Model latency only: the two llm rows, not the tool or compute rows.
    assert (a["p50Ms"], a["p95Ms"]) == (50, 100)
    kb = tool(s, "kb")
    assert (kb["calls"], kb["errors"], kb["errorRate"]) == (2, 1, 0.5)
    assert (kb["allowed"], kb["denied"], kb["denialRate"]) == (1, 1, 0.5)
    assert (kb["p50Ms"], kb["p95Ms"]) == (30, 70)
    # A tool seen only through a log-only decision: counted, with no rate claimed.
    docs = tool(s, "docs")
    assert (docs["calls"], docs["logOnly"], docs["denialRate"], docs["errorRate"]) == (0, 1, None, None)


def test_an_unknown_kind_is_counted_as_other_not_dropped():
    s = A.summarize([row(), row("telepathy", "ok")])
    assert s["rowsRead"] == 2 and s["kinds"] == {"llm": 1, "other": 1}
    a = agent(s, "a")
    assert a["rows"] == 2 and a["other"] == 1 and a["calls"] == 1


def test_an_unknown_status_is_counted_as_other_status():
    s = A.summarize([row("tool", "timeout", label="kb"), row("guardrail", "weird"),
                     row("policy", "maybe", label="kb")])
    a = agent(s, "a")
    assert a["otherStatus"] == 3 and a["errors"] == 0 and a["calls"] == 1
    assert tool(s, "kb")["otherStatus"] == 2


def test_a_row_without_an_agent_or_a_latency_is_still_counted():
    s = A.summarize([{"kind": "llm", "status": "ok"}])
    a = agent(s, "(unknown)")
    assert a["calls"] == 1 and a["p50Ms"] is None


@pytest.mark.parametrize("values, p50, p95", [
    ([], None, None),
    ([100], 100, 100),
    ([10, 20], 10, 20),
    ([40, 10, 30, 20], 20, 40),
    ([1, 2, 3, 4, 5], 3, 5),
    (list(range(1, 21)), 10, 19),
    (list(range(1, 101)), 50, 95),
])
def test_percentiles_are_nearest_rank(values, p50, p95):
    assert A.percentile(values, 50) == p50 and A.percentile(values, 95) == p95


# --- health ------------------------------------------------------------------------------

def counts(**kw):
    base = {"calls": 0, "errors": 0, "guardrailChecks": 0, "guardrailBlocks": 0,
            "allowed": 0, "denied": 0, "latencies": []}
    return {**base, **kw}


def test_the_docstring_example():
    # 20 calls, 2 errors; 1 of 4 Cedar decisions denied; no blocks; steady latency.
    score, reasons = A.health(counts(calls=20, errors=2, allowed=3, denied=1, latencies=[100] * 18))
    assert score == 89
    assert reasons == ["error rate 10% (2 of 20 calls) (-6)",
                       "Cedar denied 25% of tool calls (1 of 4) (-5)"]


def test_everything_wrong_is_zero():
    score, reasons = A.health(counts(calls=5, errors=5, allowed=0, denied=3, guardrailChecks=2,
                                     guardrailBlocks=2, latencies=[1, 1, 1, 1, 100]))
    assert score == 0 and len(reasons) == 4


def test_guardrail_blocks_cost_ten_points_at_most():
    score, reasons = A.health(counts(calls=1, guardrailChecks=4, guardrailBlocks=1, latencies=[5]))
    assert score == 98 and reasons == ["guardrail blocked 25% of checks (1 of 4) (-2.5)"]


def test_latency_spread_needs_five_calls_and_more_than_three_times_the_median():
    spread = [100, 100, 100, 100, 1000]
    assert A.health(counts(calls=5, latencies=spread)) == (
        90, ["p95 model latency 1000 ms is more than 3x its median 100 ms (-10)"])
    assert A.health(counts(calls=4, latencies=spread[1:]))[0] == 100      # too few to judge
    assert A.health(counts(calls=5, latencies=[100, 100, 100, 100, 300]))[0] == 100  # exactly 3x


def test_the_score_rounds_half_up():
    # 3 of 8 denied: 100 - 20 * 0.375 = 92.5 -> 93 (Python's round() would say 92).
    assert A.health(counts(calls=1, allowed=5, denied=3, latencies=[1]))[0] == 93


def test_an_agent_with_nothing_in_range_has_no_score():
    assert A.health(counts()) == (None, ["no calls in this range"])


# --- coverage ----------------------------------------------------------------------------

WF = {
    "guardrail": {"contentFilters": {"HATE": "HIGH"}},
    "guardrails": {"medical": {"deniedTopics": [{"name": "x", "definition": "y"}]}},
    "agents": {
        "bare": {"name": "Bare"},
        "screened": {"name": "Screened", "agentcore": {"guardrails": {"input": True}}},
        "named": {"agentcore": {"guardrails": {"output": True, "use": "medical"}}},
        "missing": {"agentcore": {"guardrails": {"output": True, "use": "legal"}}},
        "own": {"agentcore": {"guardrails": {"input": True, "output": True, "guardrailId": "gr-1"}}},
        "idonly": {"agentcore": {"guardrails": {"guardrailId": "gr-1"}}},
    },
    "tools": {"kb": {"type": "kb"}, "closed": {"type": "lambda", "policy": {"permit": False}},
              "attached": {"type": "mcp", "policy": {"permit": False}, "policies": ["p1"]},
              "custom": {"type": "openapi", "policy": {"permit": False}}},
    "orchestrator": {"policy": {"custom": [{"name": "c", "statement": (
        'permit(principal, action in AgentCore::Action::"custom", '
        'resource == AgentCore::Gateway::"{{gateway}}") when { true };')}]}},
}


def by_id(items, key):
    return {i[key]: i for i in items}


def test_agents_no_guardrail_screens_are_listed():
    agents = by_id(A.coverage(WF)["agents"], "agentId")
    assert agents["bare"]["warning"].startswith("no guardrail")
    assert agents["idonly"]["warning"].startswith("no guardrail")     # an id with no side on
    assert agents["missing"]["warning"].startswith("names guardrail 'legal'")
    for ok in ("screened", "named", "own"):
        assert agents[ok]["warning"] is None, ok
    assert agents["screened"]["guardrail"] == "the deployment guardrail"
    assert agents["named"]["guardrail"] == "named guardrail medical"
    assert agents["own"]["guardrail"] == "its own guardrailId"
    assert (agents["own"]["input"], agents["own"]["output"]) == (True, True)
    assert agents["bare"]["name"] == "Bare" and agents["named"]["name"] == "named"


def test_a_side_switched_on_with_no_guardrail_deployed_is_a_warning():
    wf = {**WF, "guardrail": {"blockedInputMessage": "no"}}   # enforces nothing: none is created
    agents = by_id(A.coverage(wf)["agents"], "agentId")
    assert "nothing is checked" in agents["screened"]["warning"]
    assert agents["named"]["warning"] is None and agents["own"]["warning"] is None


def test_tools_no_cedar_policy_decides_are_listed():
    tools = by_id(A.coverage(WF)["tools"], "tool")
    assert tools["kb"]["warning"] is None
    assert "denies every call" in tools["closed"]["warning"]
    assert tools["attached"]["warning"] is None and tools["custom"]["warning"] is None


def test_an_engine_off_or_log_only_flags_every_tool():
    off = A.coverage({**WF, "orchestrator": {"policy": {"enabled": False}}})
    assert off["policyEngine"] == {"enabled": False, "mode": "ENFORCE"}
    assert all("engine is off" in t["warning"] for t in off["tools"])
    log = A.coverage({**WF, "orchestrator": {"policy": {"mode": "LOG_ONLY"}}})
    assert all("LOG_ONLY" in t["warning"] for t in log["tools"])


def test_an_empty_workflow_has_nothing_to_flag():
    assert A.coverage({}) == {"agents": [], "tools": [],
                              "policyEngine": {"enabled": True, "mode": "ENFORCE"}}


# --- reading -----------------------------------------------------------------------------

class DayTable:
    """The by_date index: rows per day, answered in pages of two, as past 1 MB."""

    def __init__(self, days: dict[str, list[dict]]):
        self.days, self.calls = days, []

    def query(self, **kw):
        self.calls.append(kw)
        day = kw["KeyConditionExpression"].get_expression()["values"][1]
        items = self.days.get(day, [])
        start = int((kw.get("ExclusiveStartKey") or {}).get("n", 0))
        out = {"Items": items[start:start + 2]}
        if start + 2 < len(items):
            out["LastEvaluatedKey"] = {"n": start + 2}
        return out


def full(n, day):
    return {**row(ms=n), "date": day, "system_prompt": SECRET, "output_text": SECRET, "user": "x@y"}


def test_it_reads_newest_day_first_and_says_when_it_stopped_short():
    t = DayTable({"2025-01-01": [full(1, "2025-01-01"), full(2, "2025-01-01")],
                  "2025-01-02": [full(3, "2025-01-02"), full(4, "2025-01-02"), full(5, "2025-01-02")]})
    rows, capped = A.read_rows(t, date(2025, 1, 1), date(2025, 1, 2), max_rows=4)
    assert capped is True and [r["latency_ms"] for r in rows] == [3, 4, 5, 1]
    assert t.calls[0]["IndexName"] == "by_date" and t.calls[0]["ScanIndexForward"] is False
    rows, capped = A.read_rows(t, date(2025, 1, 1), date(2025, 1, 2), max_rows=5)
    assert capped is False and len(rows) == 5          # exactly at the cap is not cut


def test_it_asks_for_the_analytics_fields_only():
    t = DayTable({"2025-01-01": [full(1, "2025-01-01")]})
    rows, _ = A.read_rows(t, date(2025, 1, 1), date(2025, 1, 1))
    names = t.calls[0]["ExpressionAttributeNames"]
    assert set(names.values()) == set(A.FIELDS)
    assert t.calls[0]["ProjectionExpression"] == ", ".join(names)
    # Even if the store answered with more, only the fields leave.
    assert all(set(r) <= set(A.FIELDS) for r in rows) and SECRET not in str(rows)
    assert not {"system_prompt", "user_input", "output_text", "user", "namespace"} & set(A.FIELDS)


@pytest.mark.parametrize("frm, to, error", [
    ("2025-01-01", "2025-06-01", "range too wide: 152 days, maximum 92. Narrow `from`/`to`."),
    ("2025-02-01", "2025-01-01", "`from` must not be after `to`"),
    ("yesterday", None, "from/to must be YYYY-MM-DD"),
])
def test_a_bad_range_is_refused_before_any_read(frm, to, error):
    t = DayTable({})
    assert A.report(t, {}, frm, to, "2025-06-01", 92) == (400, {"error": error})
    assert t.calls == []


def test_the_range_defaults_to_the_last_30_days_and_allows_the_maximum():
    assert A.date_range(None, None, "2025-06-01", 92) == (date(2025, 5, 2), date(2025, 6, 1))
    assert A.date_range("2025-03-02", "2025-06-01", "2025-06-01", 92)[0] == date(2025, 3, 2)


# --- the route ---------------------------------------------------------------------------

def _telemetry_table(env):
    boto3.client("dynamodb").create_table(
        TableName="console_telemetry", BillingMode="PAY_PER_REQUEST",
        KeySchema=[{"AttributeName": "session_id", "KeyType": "HASH"},
                   {"AttributeName": "sk", "KeyType": "RANGE"}],
        AttributeDefinitions=[{"AttributeName": a, "AttributeType": "S"}
                              for a in ("session_id", "sk", "date")],
        GlobalSecondaryIndexes=[{"IndexName": "by_date",
                                 "KeySchema": [{"AttributeName": "date", "KeyType": "HASH"},
                                               {"AttributeName": "sk", "KeyType": "RANGE"}],
                                 "Projection": {"ProjectionType": "ALL"}}])
    tel = boto3.resource("dynamodb").Table("console_telemetry")
    env.handler.telemetry_tbl = tel
    return tel


def _put(tel, n, day, **over):
    tel.put_item(Item={"session_id": f"s{n}", "sk": f"{day} 10:00:{n:02d}#0001#abc",
                       "date": day, "agent_id": "a", "kind": "tool", "label": "kb",
                       "status": "ok", "latency_ms": 100 + n, "cost_usd": Decimal("0.01"),
                       "system_prompt": SECRET, "user_input": SECRET, "output_text": SECRET,
                       "user": "someone@example.com", **over})


def _analytics(env, groups=("admins",), **qs):
    return call(env, "GET /api/analytics", qs=qs or None, groups=groups)


def test_the_route_is_for_admin_only_and_refuses_before_reading(env, monkeypatch):
    _telemetry_table(env)
    read = []
    monkeypatch.setattr(env.handler.analytics, "read_rows", lambda *a, **k: read.append(a) or ([], False))
    status, body = _analytics(env, groups=("operators", "auditors"))
    assert status == 403 and "admin" in body["error"] and read == []
    assert _analytics(env)[0] == 200 and len(read) == 1


def test_the_route_reports_every_user_without_any_text(env):
    tel = _telemetry_table(env)
    _put(tel, 1, "2025-03-01")
    _put(tel, 2, "2025-03-02", status="error")
    _put(tel, 3, "2025-03-02", kind="policy", status="denied", label="kb___retrieve")
    status, body = _analytics(env, **{"from": "2025-03-01", "to": "2025-03-02"})
    assert status == 200 and SECRET not in str(body) and "someone@example.com" not in str(body)
    assert (body["from"], body["to"], body["rowsRead"], body["capped"]) == ("2025-03-01", "2025-03-02", 3, False)
    kb = body["tools"][0]
    assert (kb["tool"], kb["calls"], kb["errors"], kb["denied"], kb["p95Ms"]) == ("kb", 2, 1, 1, 102)
    assert body["agents"][0]["health"] == 100 - 30 - 20
    # The deployed workflow's coverage: agent "a" has no guardrail.
    assert body["coverage"]["agents"][0]["warning"].startswith("no guardrail")


def test_the_route_says_when_it_stopped_short(env, monkeypatch):
    tel = _telemetry_table(env)
    for n in range(1, 4):
        _put(tel, n, "2025-03-01")
    monkeypatch.setattr(env.handler.analytics, "MAX_ROWS", 2)
    body = _analytics(env, **{"from": "2025-03-01", "to": "2025-03-01"})[1]
    assert body["capped"] is True and body["rowsRead"] == 2 and body["maxRows"] == 2


def test_the_route_keeps_the_aggregate_span_cap(env):
    _telemetry_table(env)
    assert env.handler._MAX_AGGREGATE_DAYS == 92
    status, body = _analytics(env, **{"from": "2025-01-01", "to": "2025-06-01"})
    assert status == 400 and body["error"].startswith("range too wide: 152 days, maximum 92")


def test_the_route_refuses_a_malformed_build_id(env):
    status, body = _analytics(env, build="../etc")
    assert status == 400 and body["error"] == "invalid build id"


def test_a_control_plane_console_has_no_analytics(env, monkeypatch):
    monkeypatch.setattr(env.handler, "CONSOLE_MODE", "builder")
    status, body = _analytics(env)
    assert status == 404 and "Open app" in body["error"]


def test_no_telemetry_table_is_an_empty_report(env):
    env.handler.telemetry_tbl = None
    status, body = _analytics(env)
    assert status == 200 and body["rowsRead"] == 0 and body["agents"] == []


def test_it_is_declared_on_both_route_lists():
    root = Path(__file__).resolve().parents[1]
    tf = (root / "terraform" / "bff.tf").read_text()
    cdk = (root / "cdk" / "lib" / "orchestrator-stack.ts").read_text()
    assert '    "GET /api/analytics",\n' in tf
    assert 'path: "/api/analytics", methods: [HttpMethod.GET]' in cdk
