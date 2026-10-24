"""GET /api/sessions/{id}/flow: the run's events for the Flow tab, and nothing more.

What has to hold:
  * only the run's owner reads it (anyone else gets the same 404 as every run route);
  * a row comes back with the flow fields only, never a prompt, an answer or a tool result;
  * the response is capped, and SAYS so when it cut;
  * it is declared on the Terraform and CDK route lists (the parity test pins the two together).
"""
from __future__ import annotations

import json
from decimal import Decimal

import boto3
import pytest

moto = pytest.importorskip("moto")

from test_builds import call  # noqa: E402 - after the moto importorskip
from test_builds import env as env  # noqa: E402 - after the moto importorskip

SECRET = "PROMPT-TEXT-" + "THAT-MUST-NOT-LEAVE"   # a marker, joined so scanners do not take it for a password
FLOW_KEYS = {"agent_id", "kind", "label", "status", "latency_ms", "ts", "seq", "version",
             "mode", "cost_usd"}


def _telemetry_table(env):
    boto3.client("dynamodb").create_table(
        TableName="console_telemetry", BillingMode="PAY_PER_REQUEST",
        KeySchema=[{"AttributeName": "session_id", "KeyType": "HASH"},
                   {"AttributeName": "sk", "KeyType": "RANGE"}],
        AttributeDefinitions=[{"AttributeName": a, "AttributeType": "S"}
                              for a in ("session_id", "sk")])
    tel = boto3.resource("dynamodb").Table("console_telemetry")
    env.handler.telemetry_tbl = tel
    return tel


def _row(sid, n, kind="llm", **over):
    item = {"session_id": sid, "sk": f"2025-01-01 10:00:{n:02d}#{n:04d}#abc123",
            "agent_id": "researcher", "kind": kind, "label": "search_documentation",
            "status": "ok", "latency_ms": 120 + n, "ts": f"2025-01-01 10:00:{n:02d}", "seq": n,
            "version": 1, "mode": "gateway", "cost_usd": Decimal("0.0012"),
            "system_prompt": SECRET, "user_input": SECRET, "output_text": SECRET,
            "input_tokens": 99, "namespace": "insights/x"}
    item.update(over)
    return item


def _start(env, sub="alice"):
    return call(env, "POST /api/sessions", body={"topic": "t"}, sub=sub)[1]["session_id"]


def _flow(env, sid, sub="alice"):
    return call(env, "GET /api/sessions/{id}/flow", params={"id": sid},
                path=f"/api/sessions/{sid}/flow", sub=sub)


def test_it_returns_the_flow_fields_and_never_the_text(env):
    tel = _telemetry_table(env)
    sid = _start(env)
    for n in (1, 2):
        tel.put_item(Item=_row(sid, n))
    status, body = _flow(env, sid)
    assert status == 200 and body["capped"] is False
    assert len(body["events"]) == 2
    for ev in body["events"]:
        assert set(ev) == FLOW_KEYS
    assert SECRET not in str(body)
    assert [e["seq"] for e in body["events"]] == [1, 2]


def test_it_answers_only_to_the_owner(env):
    tel = _telemetry_table(env)
    sid = _start(env, "alice")
    tel.put_item(Item=_row(sid, 1))
    status, resp = _flow(env, sid, sub="bob")
    assert status == 404 and resp["error"] == "unknown session"


def test_it_does_not_return_another_runs_rows(env):
    tel = _telemetry_table(env)
    mine, theirs = _start(env, "alice"), _start(env, "bob")
    tel.put_item(Item=_row(mine, 1))
    tel.put_item(Item=_row(theirs, 2, label="other"))
    assert [e["label"] for e in _flow(env, mine)[1]["events"]] == ["search_documentation"]


def test_it_says_when_it_cut_the_run_short(env, monkeypatch):
    tel = _telemetry_table(env)
    sid = _start(env)
    for n in range(1, 6):
        tel.put_item(Item=_row(sid, n))
    monkeypatch.setattr(env.handler, "FLOW_MAX_ROWS", 3)
    body = _flow(env, sid)[1]
    assert body["capped"] is True and body["max"] == 3
    assert [e["seq"] for e in body["events"]] == [1, 2, 3]       # the earliest, not a random 3


def test_a_run_exactly_at_the_cap_is_not_called_capped(env, monkeypatch):
    tel = _telemetry_table(env)
    sid = _start(env)
    for n in range(1, 4):
        tel.put_item(Item=_row(sid, n))
    monkeypatch.setattr(env.handler, "FLOW_MAX_ROWS", 3)
    body = _flow(env, sid)[1]
    assert len(body["events"]) == 3 and body["capped"] is False


def test_a_run_with_no_telemetry_table_is_an_empty_flow(env):
    env.handler.telemetry_tbl = None
    sid = _start(env)
    status, body = _flow(env, sid)
    assert status == 200 and body["events"] == [] and body["capped"] is False


def test_it_is_declared_on_both_route_lists():
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    tf = (root / "terraform" / "bff.tf").read_text()
    cdk = (root / "cdk" / "lib" / "orchestrator-stack.ts").read_text()
    assert '"GET /api/sessions/{id}/flow"' in tf
    assert 'path: "/api/sessions/{id}/flow", methods: [HttpMethod.GET]' in cdk


class PagedTable:
    """A table that answers a query in pages of two, as DynamoDB does past 1 MB."""

    def __init__(self, items):
        self.items, self.calls = items, []

    def query(self, **kw):
        self.calls.append(kw)
        start = int((kw.get("ExclusiveStartKey") or {}).get("n", 0))
        page = self.items[start:start + 2]
        out = {"Items": page}
        if start + 2 < len(self.items):
            out["LastEvaluatedKey"] = {"n": start + 2}
        return out


def test_it_follows_the_pages_and_asks_for_the_flow_fields_only(env, monkeypatch):
    table = PagedTable([_row("s", n) for n in range(1, 6)])
    body = json.loads(env.handler._flow_session("s", table)["body"])
    assert [e["seq"] for e in body["events"]] == [1, 2, 3, 4, 5] and body["capped"] is False
    assert len(table.calls) == 3
    names = set(table.calls[0]["ExpressionAttributeNames"].values())
    assert names == FLOW_KEYS                 # nothing but the flow fields is requested
    assert all(t.startswith("#f") for t in table.calls[0]["ProjectionExpression"].split(", "))


def test_the_cap_holds_across_pages(env, monkeypatch):
    table = PagedTable([_row("s", n) for n in range(1, 8)])
    monkeypatch.setattr(env.handler, "FLOW_MAX_ROWS", 3)
    body = json.loads(env.handler._flow_session("s", table)["body"])
    assert [e["seq"] for e in body["events"]] == [1, 2, 3] and body["capped"] is True
    assert len(table.calls) == 2              # it stopped reading once it knew it was cut
