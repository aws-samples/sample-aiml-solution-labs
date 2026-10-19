"""Read-only links to one run (bff/runshares.py, the share routes in bff/handler.py).

What has to hold:
  * only the run's owner makes, lists and revokes its links: anyone else (an admin too) gets
    the same 404 as every run route;
  * a link opens for any SIGNED-IN caller, never for one with no identity;
  * an unknown, malformed, revoked or expired link is the same 404, even while its row
    still exists (TTL deletion is late, so it is never what refuses a link);
  * the shared read is the run's snapshot only: no telemetry, no files, no owner id;
    holding a link opens no write route and no other read route of that run;
  * ids are checked by their pattern before anything is read; a run has at most 10 links;
  * making, opening and revoking a link are events in the OWNER's activity log;
  * the routes are on both IaC route lists, behind the JWT authorizer.
DynamoDB is moto's, so the conditional revoke is checked and applied as DynamoDB does.
"""
from __future__ import annotations

import json
import re
import sys
from decimal import Decimal
from pathlib import Path

import boto3
import pytest

moto = pytest.importorskip("moto")

from test_builds import CTX, call  # noqa: E402
from test_builds import env as env  # noqa: E402

SECRET = "CAPTURED-PROMPT-" + "THAT-MUST-NOT-LEAVE"   # a marker, joined so scanners do not take it for a password
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _fresh_runshares():
    # Autouse runs before `env` imports the handler, so each test gets its own runshares.
    sys.modules.pop("runshares", None)
    yield
    sys.modules.pop("runshares", None)


def _rs(env):
    # The module the handler holds, not whatever sys.modules has now (COMMON trap 2).
    return env.handler.runshares


def _start(env, sub="alice", topic="Compare the two offers"):
    status, body = call(env, "POST /api/sessions", body={"topic": topic}, sub=sub)
    assert status == 200
    return body["session_id"]


def _create(env, sid, sub="alice", **body):
    return call(env, "POST /api/sessions/{id}/shares", params={"id": sid},
                path=f"/api/sessions/{sid}/shares", body=body, sub=sub)


def _list(env, sid, sub="alice"):
    return call(env, "GET /api/sessions/{id}/shares", params={"id": sid},
                path=f"/api/sessions/{sid}/shares", sub=sub)


def _revoke(env, sid, share, sub="alice"):
    return call(env, "DELETE /api/sessions/{id}/shares/{shareId}", params={"id": sid, "shareId": share},
                path=f"/api/sessions/{sid}/shares/{share}", sub=sub)


def _open(env, share, sub="bob", **kw):
    return call(env, "GET /api/shared/{shareId}", params={"shareId": share},
                path=f"/api/shared/{share}", sub=sub, **kw)


def _events(env):
    return boto3.resource("dynamodb").Table("console_events")


def _log(env, sub):
    return [e for e in call(env, "GET /api/audit", sub=sub)[1]]


def _actions(env, sub):
    return [e["action"] for e in _log(env, sub)]


# --- making and listing -----------------------------------------------------------------

def test_the_owner_makes_a_link_that_lasts_seven_days_by_default(env):
    sid = _start(env)
    rs = _rs(env)
    status, made = _create(env, sid)
    assert status == 200
    assert rs.SHARE_RE.match(made["shareId"]) and made["url"] == f"/#shared:{made['shareId']}"
    import datetime as dt
    expires = dt.datetime.strptime(made["expires"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.UTC)
    left = expires.timestamp() - dt.datetime.now(dt.UTC).timestamp()
    assert 7 * 86400 - 120 < left <= 7 * 86400
    status, listed = _list(env, sid)
    assert status == 200 and [s["shareId"] for s in listed["shares"]] == [made["shareId"]]
    assert listed["max"] == 10


@pytest.mark.parametrize("choice, seconds", [("1h", 3600), ("1d", 86400), ("30d", 30 * 86400)])
def test_each_expiry_choice_is_honoured(env, choice, seconds):
    sid = _start(env)
    made = _create(env, sid, expiresIn=choice)[1]
    row = _events(env).get_item(Key={"session_id": f"share#{made['shareId']}", "ts": "link"})["Item"]
    assert int(row["expiresAt"]) - int(row["created"]) == seconds
    assert int(row["ttl"]) > int(row["expiresAt"])          # TTL cleans up AFTER expiry, never before


@pytest.mark.parametrize("bad", ["90d", "forever", 3600, ["7d"]])
def test_an_expiry_not_offered_is_refused(env, bad):
    sid = _start(env)
    status, body = _create(env, sid, expiresIn=bad)
    assert status == 400 and "expiresIn" in body["error"]
    assert _list(env, sid)[1]["shares"] == []


def test_nobody_but_the_owner_makes_lists_or_revokes_a_link(env):
    sid = _start(env, "alice")
    made = _create(env, sid)[1]
    # bob, and an admin (who may READ any run), are refused alike, as if the run did not exist.
    for sub, groups in (("bob", ("operators",)), ("root", ("admins",))):
        assert call(env, "GET /api/sessions/{id}", params={"id": sid}, sub=sub, groups=groups)[0] == (
            200 if sub == "root" else 404)
        for status, body in (
                call(env, "POST /api/sessions/{id}/shares", params={"id": sid},
                     path=f"/api/sessions/{sid}/shares", body={}, sub=sub, groups=groups),
                call(env, "GET /api/sessions/{id}/shares", params={"id": sid},
                     path=f"/api/sessions/{sid}/shares", sub=sub, groups=groups),
                call(env, "DELETE /api/sessions/{id}/shares/{shareId}",
                     params={"id": sid, "shareId": made["shareId"]},
                     path=f"/api/sessions/{sid}/shares/{made['shareId']}", sub=sub, groups=groups)):
            assert (status, body) == (404, {"error": "unknown session"})
    assert [s["shareId"] for s in _list(env, sid)[1]["shares"]] == [made["shareId"]]
    assert _open(env, made["shareId"])[0] == 200              # bob's revoke did nothing


def test_a_run_that_does_not_exist_has_no_links(env):
    assert _create(env, "abcdef123456") == (404, {"error": "unknown session"})


def test_a_run_has_at_most_ten_active_links(env):
    sid = _start(env)
    made = [_create(env, sid)[1] for _ in range(10)]
    status, body = _create(env, sid)
    assert status == 409 and "at most 10" in body["error"]
    assert _revoke(env, sid, made[0]["shareId"])[0] == 200
    assert _create(env, sid)[0] == 200
    assert len(_list(env, sid)[1]["shares"]) == 10


def test_expired_links_do_not_count_against_the_cap(env, monkeypatch):
    sid = _start(env)
    for _ in range(10):
        _create(env, sid, expiresIn="1h")
    rs = _rs(env)
    now = rs._now()
    monkeypatch.setattr(rs, "_now", lambda: now + 3601)
    assert _list(env, sid)[1]["shares"] == []
    assert _create(env, sid)[0] == 200


# --- opening ----------------------------------------------------------------------------

def _seed_run(env, sid):
    """What a finished run holds: outputs, a timeline line, telemetry with a prompt, a file."""
    status = boto3.resource("dynamodb").Table("console_status")
    status.update_item(
        Key={"session_id": sid},
        UpdateExpression="SET overall = :o, nodes = :n, attachments = :a, cancel_requested = :c, "
                         "futureField = :f",
        ExpressionAttributeValues={":o": "done", ":n": {"a": {"status": "done", "output": "The answer"}},
                                   ":a": [{"key": f"runs/{sid}/files/x.pdf", "name": "x.pdf", "size": 1}],
                                   ":c": False, ":f": SECRET})
    _events(env).put_item(Item={"session_id": sid, "ts": "2025-01-01 10:00:00#abc123",
                                "node": "a", "msg": "Agent a finished"})
    boto3.client("dynamodb").create_table(
        TableName="console_telemetry", BillingMode="PAY_PER_REQUEST",
        KeySchema=[{"AttributeName": "session_id", "KeyType": "HASH"},
                   {"AttributeName": "sk", "KeyType": "RANGE"}],
        AttributeDefinitions=[{"AttributeName": a, "AttributeType": "S"} for a in ("session_id", "sk")])
    tel = boto3.resource("dynamodb").Table("console_telemetry")
    tel.put_item(Item={"session_id": sid, "sk": "2025-01-01 10:00:00#0001", "agent_id": "a", "kind": "llm",
                       "system_prompt": SECRET, "user_input": SECRET, "cost_usd": Decimal("0.01")})
    env.handler.telemetry_tbl = tel


def test_any_signed_in_user_opens_the_run_read_only(env):
    sid = _start(env, "alice", topic="Compare the two offers")
    _seed_run(env, sid)
    made = _create(env, sid)[1]
    status, snap = _open(env, made["shareId"], sub="bob")
    assert status == 200
    assert snap["readOnly"] is True and snap["sharedBy"] == "alice@example.com"
    assert snap["expires"] == made["expires"]
    assert snap["session_id"] == sid and snap["topic"] == "Compare the two offers"
    assert snap["nodes"]["a"]["output"] == "The answer" and snap["overall"] == "done"
    assert [line["msg"] for line in snap["logs"]] == ["Agent a finished"]
    assert snap["workflow"]["agents"]                        # enough to draw the run's graph


def test_the_shared_read_is_the_snapshot_only(env):
    sid = _start(env)
    _seed_run(env, sid)
    made = _create(env, sid)[1]
    snap = _open(env, made["shareId"])[1]
    rs = _rs(env)
    assert set(snap) <= set(rs.SNAPSHOT_FIELDS) | {"logs", "sharedBy", "expires", "readOnly"}
    text = json.dumps(snap)
    assert SECRET not in text                                 # no telemetry, no field added later
    for key in ("owner", "attachments", "cancel_requested", "calls", "byAgent", "futureField"):
        assert key not in snap
    assert f"runs/{sid}/files" not in text                    # no file keys


def test_holding_a_link_opens_no_other_route_of_that_run(env):
    sid = _start(env, "alice")
    _seed_run(env, sid)
    made = _create(env, sid)[1]
    assert _open(env, made["shareId"], sub="bob")[0] == 200
    env.invoked.clear()
    for route, path, body in (
            ("GET /api/sessions/{id}", f"/api/sessions/{sid}", None),
            ("POST /api/sessions/{id}/decision", f"/api/sessions/{sid}/decision", {"decision": "approve"}),
            ("POST /api/sessions/{id}/cancel", f"/api/sessions/{sid}/cancel", None),
            ("POST /api/sessions/{id}/rerun", f"/api/sessions/{sid}/rerun", {"agentId": "a"}),
            ("POST /api/sessions/{id}/evaluate", f"/api/sessions/{sid}/evaluate", {"agentId": "a"}),
            ("DELETE /api/sessions/{id}", f"/api/sessions/{sid}", None),
            ("GET /api/sessions/{id}/telemetry", f"/api/sessions/{sid}/telemetry", None),
            ("GET /api/sessions/{id}/flow", f"/api/sessions/{sid}/flow", None)):
        status, resp = call(env, route, params={"id": sid}, path=path, body=body, sub="bob")
        assert (status, resp) == (404, {"error": "unknown session"}), route
    assert env.invoked == []                                  # nothing was started or resumed
    assert call(env, "GET /api/sessions/{id}", params={"id": sid}, sub="alice")[0] == 200


def test_an_expired_link_is_gone_while_its_row_still_exists(env, monkeypatch):
    sid = _start(env)
    made = _create(env, sid, expiresIn="1h")[1]
    rs = _rs(env)
    now = rs._now()
    monkeypatch.setattr(rs, "_now", lambda: now + 3599)
    assert _open(env, made["shareId"])[0] == 200
    monkeypatch.setattr(rs, "_now", lambda: now + 3600)
    assert _open(env, made["shareId"]) == (404, {"error": rs.GONE})
    assert _events(env).get_item(Key={"session_id": f"share#{made['shareId']}", "ts": "link"}).get("Item")


def test_a_revoked_link_is_gone_while_its_row_still_exists(env):
    sid = _start(env)
    made = _create(env, sid)[1]
    assert _revoke(env, sid, made["shareId"]) == (200, {"ok": True})
    rs = _rs(env)
    assert _open(env, made["shareId"]) == (404, {"error": rs.GONE})
    row = _events(env).get_item(Key={"session_id": f"share#{made['shareId']}", "ts": "link"})["Item"]
    assert row["revoked"]
    assert _list(env, sid)[1]["shares"] == []
    assert _revoke(env, sid, made["shareId"])[0] == 404       # once


def test_a_link_is_revoked_only_through_its_own_run(env):
    one, two = _start(env), _start(env)
    made = _create(env, one)[1]
    assert _revoke(env, two, made["shareId"])[0] == 404       # alice's other run: not this link's
    assert _open(env, made["shareId"])[0] == 200


def test_unknown_and_malformed_links_are_the_same_404(env):
    rs = _rs(env)
    for share in ("A" * 32, "short", "x" * 33, "../../etc", "a b" + "c" * 29):
        assert _open(env, share) == (404, {"error": rs.GONE}), share


def test_ids_are_checked_before_anything_is_read(env, monkeypatch):
    reads = []

    class Spy:
        def __getattr__(self, name):
            reads.append(name)
            raise AssertionError(f"read {name}")

    monkeypatch.setattr(env.handler, "events_tbl", Spy())
    monkeypatch.setattr(env.handler, "_session_target", lambda sid: reads.append(sid))
    rs = _rs(env)
    assert _open(env, "not/a-share") == (404, {"error": rs.GONE})
    for bad in ("a/b", "x" * 65, "abc"):
        assert call(env, "GET /api/sessions/{id}/shares", params={"id": bad},
                    path=f"/api/sessions/{bad}/shares", sub="alice")[0] == 404
    assert reads == []


def test_a_deleted_runs_links_are_gone(env):
    sid = _start(env)
    made = _create(env, sid)[1]
    assert call(env, "DELETE /api/sessions/{id}", params={"id": sid}, sub="alice")[0] == 200
    assert _open(env, made["shareId"])[0] == 404


def test_a_link_stops_when_its_run_is_no_longer_its_sharers(env):
    sid = _start(env, "alice")
    made = _create(env, sid)[1]
    boto3.resource("dynamodb").Table("console_status").update_item(
        Key={"session_id": sid}, UpdateExpression="SET #o = :o",
        ExpressionAttributeNames={"#o": "owner"}, ExpressionAttributeValues={":o": "carol"})
    rs = _rs(env)
    assert _open(env, made["shareId"]) == (404, {"error": rs.GONE})


def _unsigned(env, route, path, params):
    event = {"requestContext": {"http": {"method": route.split()[0]}}, "rawPath": path,
             "routeKey": route, "pathParameters": params}
    resp = env.handler.handler(event, CTX)
    return resp["statusCode"], json.loads(resp["body"])


def test_without_sign_in_there_are_no_links(env):
    sid = _start(env)
    made = _create(env, sid)[1]
    status, body = _unsigned(env, "GET /api/shared/{shareId}", f"/api/shared/{made['shareId']}",
                             {"shareId": made["shareId"]})
    assert status == 403 and "sign-in" in body["error"]
    assert _unsigned(env, "POST /api/sessions/{id}/shares", f"/api/sessions/{sid}/shares",
                     {"id": sid})[0] == 403


def test_a_control_plane_console_has_no_links(env, monkeypatch):
    monkeypatch.setattr(env.handler, "CONSOLE_MODE", "builder")
    status, body = _open(env, "A" * 32)
    assert status == 404 and "Open app" in body["error"]


# --- the activity log -------------------------------------------------------------------

def test_making_opening_and_revoking_are_in_the_owners_log(env):
    sid = _start(env, "alice")
    made = _create(env, sid, expiresIn="1d")[1]
    _open(env, made["shareId"], sub="bob")
    _open(env, made["shareId"], sub="bob")                  # a polling page: logged once an hour
    _revoke(env, sid, made["shareId"])
    log = _log(env, "alice")
    by_action = {}
    for e in log:
        by_action.setdefault(e["action"], []).append(e)
    assert len(by_action["run.shared"]) == 1 and len(by_action["run.share.opened"]) == 1
    assert len(by_action["run.share.revoked"]) == 1
    opened = by_action["run.share.opened"][0]
    assert opened["email"] == "bob@example.com" and opened["detail"]["by"] == "bob@example.com"
    for e in (by_action["run.shared"][0], opened, by_action["run.share.revoked"][0]):
        assert e["detail"]["session"] == sid
        assert e["detail"]["share"] == made["shareId"][:6]   # a hint, never the whole link
        assert made["shareId"] not in json.dumps(e)
    assert by_action["run.shared"][0]["detail"]["expires"] == made["expires"]
    # bob's own log does not carry alice's link.
    assert not {"run.shared", "run.share.opened", "run.share.revoked"} & set(_actions(env, "bob"))


# --- the route lists --------------------------------------------------------------------

def test_the_routes_are_on_both_lists_behind_the_authorizer(env):
    tf = (ROOT / "terraform" / "bff.tf").read_text()
    cdk = (ROOT / "cdk" / "lib" / "orchestrator-stack.ts").read_text()
    # The routes resource: every key in it gets the JWT authorizer. The webhook route is
    # declared on its own, without one.
    routes_block = tf[tf.index('resource "aws_apigatewayv2_route" "routes"'):
                      tf.index('resource "aws_apigatewayv2_route" "hook"')]
    assert 'authorization_type = local.auth_enabled ? "JWT" : "NONE"' in routes_block
    cdk_routes = cdk[cdk.index("const routes: Array<"):cdk.index("for (const r of routes)")]
    for route in _rs(env).ROUTES:
        assert f'"{route}"' in routes_block, route
        method, path = route.split(" ", 1)
        assert re.search(r'path: "' + re.escape(path) + r'", methods: \[[^\]]*HttpMethod\.' + method, cdk_routes), route
