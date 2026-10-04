"""The console as a control plane, and every build's app with its own activity log.

What has to hold:
  * a console with CONSOLE_MODE=builder builds, designs and deploys, and runs nothing:
    every run, insight, telemetry, assistant and image route answers 404 with where to go;
  * builder mode needs the Builder: without it, a deployment stays an app;
  * a deployed build's app (no builds table) keeps its own log in AUDIT_TABLE: sign-outs
    and run actions land there, the owner reads theirs, everyone's needs `audit`.
"""
from __future__ import annotations

import importlib
import json
import sys

import boto3
import pytest

moto = pytest.importorskip("moto")
from test_builds import (  # noqa: E402
    MODULES,
    RULES,
    _events_table,
    _status_table,
    call,
    create_builds_table,
)
from test_builds import env as env  # noqa: E402

# --- a control-plane console ------------------------------------------------------------

@pytest.fixture()
def console(env, monkeypatch):
    monkeypatch.setattr(env.handler, "CONSOLE_MODE", "builder")
    return env


def test_me_says_it_is_a_control_plane(console):
    me = call(console, "GET /api/me")[1]
    assert me["consoleMode"] == "builder" and me["builder"] is True and me["audit"] is True


@pytest.mark.parametrize("route, path", [
    ("POST /api/sessions", "/api/sessions"),
    ("GET /api/sessions", "/api/sessions"),
    ("GET /api/sessions/{id}", "/api/sessions/abc"),
    ("POST /api/sessions/{id}/decision", "/api/sessions/abc/decision"),
    ("GET /api/insights", "/api/insights"),
    ("POST /api/insights/run", "/api/insights/run"),
    ("GET /api/telemetry/aggregate", "/api/telemetry/aggregate"),
    ("GET /api/sessions/{id}/telemetry", "/api/sessions/abc/telemetry"),
    ("POST /api/chat", "/api/chat"),
    ("GET /api/images", "/api/images"),
])
def test_it_runs_nothing_and_says_where_runs_live(console, route, path):
    status, body = call(console, route, path=path, params={"id": "abc"}, body={"topic": "x"})
    assert status == 404 and "Open app" in body["error"]


def test_it_still_builds_and_keeps_its_log(console):
    assert call(console, "GET /api/builds")[0] == 200
    assert call(console, "GET /api/accounts")[0] == 200
    assert call(console, "GET /api/policies")[0] == 200
    assert call(console, "GET /api/workflow")[0] == 200
    call(console, "POST /api/audit/logout")
    assert [e["action"] for e in call(console, "GET /api/audit")[1]] == ["logout"]


def test_builder_mode_needs_the_builder(console, monkeypatch):
    monkeypatch.setattr(console.builds, "ENABLED", False)
    assert call(console, "GET /api/me")[1]["consoleMode"] == "app"


# --- a build's own app ------------------------------------------------------------------

APP_WORKFLOW = {"agents": {"a": {"name": "A"}}, "steps": [{"agent": "a"}], "authorization": RULES}


@pytest.fixture()
def app(monkeypatch):
    for k, v in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing",
                 "AWS_DEFAULT_REGION": "us-east-1", "AWS_REGION": "us-east-1",
                 "WORKFLOW_JSON": json.dumps(APP_WORKFLOW),
                 "STATUS_TABLE": "ax_1_status", "EVENTS_TABLE": "ax_1_events",
                 "RUNTIME_ARN": "arn:aws:bedrock-agentcore:us-east-1:1:runtime/ax_1",
                 "AUDIT_TABLE": "ax_1_audit"}.items():
        monkeypatch.setenv(k, v)
    for k in ("BUILDS_TABLE", "BUILDS_BUCKET", "DEPLOY_PROJECT", "CONSOLE_MODE"):
        monkeypatch.delenv(k, raising=False)
    with moto.mock_aws():
        ddb = boto3.client("dynamodb")
        create_builds_table(ddb, "ax_1_audit")
        _status_table(ddb, "ax_1_status")
        _events_table(ddb, "ax_1_events")
        for mod in MODULES:
            sys.modules.pop(mod, None)
        handler = importlib.import_module("handler")
        monkeypatch.setattr(handler, "_self_invoke", lambda fn, p: None)
        import types
        yield types.SimpleNamespace(handler=handler, builds=handler.builds,
                                    table=boto3.resource("dynamodb").Table("ax_1_audit"))
    for mod in MODULES:
        sys.modules.pop(mod, None)


def test_an_app_keeps_its_own_log_of_runs_and_sign_outs(app):
    me = call(app, "GET /api/me")[1]
    assert me["builder"] is False and me["audit"] is True and me["consoleMode"] == "app"
    status, run = call(app, "POST /api/sessions", body={"topic": "Check claim 42"})
    assert status == 200
    call(app, "POST /api/audit/logout")
    mine = call(app, "GET /api/audit")[1]
    # Same-second events have no order between them, so compare as a set.
    assert sorted(e["action"] for e in mine) == ["logout", "run.started"]
    started = next(e for e in mine if e["action"] == "run.started")
    assert started["detail"]["session"] == run["session_id"]
    # Private: another user's log is their own.
    assert call(app, "GET /api/audit", sub="u2")[1] == []
    # Everyone's needs the `audit` permission.
    assert call(app, "GET /api/audit", qs={"scope": "all", "day": "2026-09-09"})[0] == 403
    status, day = call(app, "GET /api/audit", groups=("auditors",),
                       qs={"scope": "all", "day": mine[0]["ts"][:10]})
    assert status == 200 and len(day) == 2
    # The Builder's routes are not here.
    assert call(app, "GET /api/builds")[0] == 404


def test_an_app_with_no_log_says_so(app, monkeypatch):
    import audit
    monkeypatch.setattr(audit, "_own", None)
    assert call(app, "GET /api/me")[1]["audit"] is False
    assert call(app, "GET /api/audit")[0] == 404
    assert call(app, "POST /api/sessions", body={"topic": "t"})[0] == 200   # runs still work


# --- a control-plane console with no workflow plane at all ------------------------------

@pytest.fixture()
def bare(env, monkeypatch):
    """What the IaC deploys with consoleMode=builder: no run tables, no runtime."""
    monkeypatch.setattr(env.handler, "CONSOLE_MODE", "builder")
    monkeypatch.setattr(env.handler, "status_tbl", None)
    monkeypatch.setattr(env.handler, "events_tbl", None)
    monkeypatch.setattr(env.handler, "RUNTIME_ARN", "")
    return env


def test_a_console_without_its_own_workflow_still_builds_deploys_and_logs(bare):
    assert call(bare, "GET /api/me")[1]["consoleMode"] == "builder"
    assert call(bare, "GET /api/workflow")[0] == 200
    assert call(bare, "GET /api/builds")[0] == 200
    assert call(bare, "GET /api/policies")[0] == 200
    call(bare, "POST /api/audit/logout")
    assert [e["action"] for e in call(bare, "GET /api/audit")[1]] == ["logout"]
    for route, path in (("POST /api/sessions", "/api/sessions"), ("GET /api/sessions", "/api/sessions")):
        status, body = call(bare, route, path=path, body={"topic": "x"})
        assert status == 404 and "Open app" in body["error"]


def test_the_bff_imports_without_run_tables(monkeypatch):
    """The Lambda's environment on a control-plane console: no STATUS_TABLE, EVENTS_TABLE
    or RUNTIME_ARN. Importing the handler must not need them."""
    for k in ("STATUS_TABLE", "EVENTS_TABLE", "RUNTIME_ARN", "TELEMETRY_TABLE", "BUILDS_TABLE",
              "BUILDS_BUCKET", "DEPLOY_PROJECT"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("WORKFLOW_JSON", json.dumps(APP_WORKFLOW))
    for mod in MODULES:
        sys.modules.pop(mod, None)
    try:
        handler = importlib.import_module("handler")
        assert handler.status_tbl is None and handler.RUNTIME_ARN == ""
        with pytest.raises(handler.builds.BuildError):
            handler._default_target()
    finally:
        for mod in MODULES:
            sys.modules.pop(mod, None)
