"""scripts/e2e_smoke.py, run against a FAKE of the deployed app's API on 127.0.0.1.

There is no AWS here, so the script has never touched a real deployment. These tests are only as
good as the fake, so the fake does what the real app does where it matters (bff/handler.py):
  * a run goes running -> a review gate waits -> (after `approve`) the next stage -> ... -> done;
    a decision sent when no gate waits is refused, and the gate only clears when it is told to
  * a rejected token is 401, a limit is 429, both with the app's `{"error": ...}` body
  * snapshots, the gate (`hitl.node`), telemetry (`calls`/`byAgent`/`totals`) and /api/limits have
    the shapes the handler returns
The script is loaded BY PATH and calls no `app.*` code, so it is unaffected by the module-identity
trap (other tests re-import `app.common.*`).
"""
from __future__ import annotations

import importlib.util
import json
import logging
import os
import subprocess
import sys
import threading
import time
import types
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "e2e_smoke.py"
# Fake values, joined at run time so secret scanners do not read them as real credentials.
TOKEN = ".".join(["eyJhbGciOiJSUzI1NiJ9", "eyJzdWIiOiJ1c2VyLTEifQ", "c2lnbmF0dXJlLXNlY3JldA"])
PASSWORD = "hunter2-" + "Correct-Horse"
SID = "0123456789ab"


def _load():
    spec = importlib.util.spec_from_file_location("e2e_smoke_under_test", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


smoke = _load()


# --- The fake app ---------------------------------------------------------------------------------
DEFAULT_AGENTS = [("intake", "main"), ("research", "dedicated"), ("analysis", "a2a"), ("report", "main")]


class Scenario:
    def __init__(self, **kw):
        self.agents = DEFAULT_AGENTS
        self.fail_agent = False        # the last stage ends with its first tail agent failed
        self.never_done = False        # stays running forever
        self.stuck_gate = False        # accepts the approval and never lets the gate go
        self.telemetry = "normal"      # normal | empty | zero_cost | missing_agent | error
        self.limits = None             # None -> {"limits": false}; or the dict to return
        self.limits_status = 200
        self.unauthorised = False      # every call is 401
        self.limit_on_start = False    # POST /api/sessions -> 429
        self.start_status = 200
        self.start_body = None
        self.skip_tail = False         # the tail agents end `skipped`
        self.console_mode = None
        self.redirect_me_to = None
        self.limits_echo_token = False
        self.limits_echo_other = False         # echoes a token that is NOT the one the script holds
        self.session_id = SID
        self.__dict__.update(kw)


class FakeApp:
    def __init__(self, sc: Scenario):
        self.sc = sc
        self.requests: list[tuple[str, str]] = []      # (method, path)
        self.auth_seen: list[str] = []
        self.raw_paths: list[str] = []
        self.decisions: list[dict] = []
        self.get_times: list[float] = []
        self.phase = 0
        self.created = False
        self.deleted = False
        self.lock = threading.Lock()
        n = len(sc.agents)
        # phases: run0, gate0, run1, gate1, run-tail, done  (agents 0 and 1 are gated)
        self.phases = ["run:0", "gate:0", "run:1", "gate:1", "run:tail", "done"] if n > 2 \
            else ["run:0", "gate:0", "run:1", "gate:1", "done"]
        owner = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):  # silence
                pass

            def _send(self, status, body, headers=None):
                raw = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(raw)))
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(raw)

            def _body(self):
                n = int(self.headers.get("content-length") or 0)
                return json.loads(self.rfile.read(n) or b"{}") if n else {}

            def do_GET(self):
                owner.handle(self, "GET")

            def do_POST(self):
                owner.handle(self, "POST")

            def do_DELETE(self):
                owner.handle(self, "DELETE")

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()

    # -- state of the run
    def _nodes(self):
        ids = [a for a, _ in self.sc.agents]
        ph = self.phases[self.phase]
        out = {}
        for i, a in enumerate(ids):
            if i == 0:
                st = "running" if ph == "run:0" else "done"
            elif i == 1:
                st = "pending" if ph in ("run:0", "gate:0") else ("running" if ph == "run:1" else "done")
            else:
                st = {"run:tail": "running", "done": "done"}.get(ph, "pending")
                if ph == "done" and self.sc.skip_tail:
                    st = "skipped"
                if ph == "done" and self.sc.fail_agent and i == 2:
                    st = "failed"
            out[a] = {"status": st, "pct": 100 if st == "done" else 0, "output": ""}
        return out

    def _snapshot(self):
        ph = self.phases[self.phase]
        gate = ph.startswith("gate:")
        overall = "waiting_human" if gate else ("done" if ph == "done" else "running")
        if ph == "done" and self.sc.fail_agent:
            overall = "failed"
        hitl = None
        if gate:
            hitl = {"node": self.sc.agents[int(ph.split(":")[1])][0], "question": "Approve?"}
        return {"session_id": self.sc.session_id, "topic": "t", "overall": overall,
                "nodes": self._nodes(), "hitl": hitl, "result": "ok" if overall == "done" else None,
                "logs": []}

    def _telemetry(self):
        t = self.sc.telemetry
        if t == "empty":
            return {"session_id": SID, "calls": [], "byAgent": [], "totals": {}}
        rows = []
        for a, rt in self.sc.agents:
            if rt == "a2a":
                continue
            if t == "missing_agent" and a == self.sc.agents[-1][0]:
                continue
            rows.append({"session_id": SID, "agent_id": a, "kind": "llm", "label": "model-x",
                         "input_tokens": 100, "output_tokens": 50,
                         "cost_usd": 0 if t == "zero_cost" else 0.0125})
            rows.append({"session_id": SID, "agent_id": a, "kind": "tool", "label": "kb", "cost_usd": 0})
        cost = sum(r["cost_usd"] for r in rows)
        return {"session_id": SID, "calls": rows,
                "byAgent": [{"agentId": a} for a, _ in self.sc.agents],
                "totals": {"costUsd": cost, "inputTokens": 100 * len(rows), "outputTokens": 50 * len(rows),
                           "calls": len(rows), "latencyMs": 10}}

    def handle(self, h, method):
        path = h.path.split("?")[0]
        with self.lock:
            self.requests.append((method, path))
            self.raw_paths.append(h.path)
            self.auth_seen.append(h.headers.get("Authorization") or "")
            if method == "GET" and path.startswith("/api/sessions/") and path.count("/") == 3:
                self.get_times.append(time.monotonic())
            sc = self.sc
            if sc.redirect_me_to and path == "/api/me":
                return h._send(302, {}, {"Location": sc.redirect_me_to})
            if sc.unauthorised or h.headers.get("Authorization") != f"Bearer {TOKEN}":
                return h._send(401, {"error": "Unauthorized"})
            if method == "GET" and path == "/api/me":
                return h._send(200, {"user": "smoke@example.com", "groups": [], "consoleMode": sc.console_mode})
            if method == "GET" and path == "/api/workflow":
                return h._send(200, {"agents": {a: {"name": a.title(), "runtime": rt} for a, rt in sc.agents},
                                     "steps": [{"agent": a} for a, _ in sc.agents], "ui": {"defaultTopic": "Default topic"}})
            if method == "GET" and path == "/api/limits":
                if sc.limits_echo_token:
                    return h._send(500, {"error": f"upstream said: Bearer {TOKEN} was refused for {PASSWORD}"})
                if sc.limits_echo_other:
                    return h._send(500, {"error": "refused Bearer other-token-123 and eyJhbGciOi.eyJzdWIiOiJ4.sigpart now"})
                if sc.limits_status != 200:
                    return h._send(sc.limits_status, {"error": "no such route"})
                return h._send(200, sc.limits if sc.limits is not None else {"limits": False})
            if method == "POST" and path == "/api/sessions":
                body = h._body()
                self.topic = body.get("topic")
                if sc.limit_on_start:
                    return h._send(429, {"error": "You have started 20 runs this hour, which is the limit. "
                                                  "You can start another at 15:00 Eastern time."})
                if sc.start_status != 200:
                    return h._send(sc.start_status, sc.start_body or {"error": "boom"})
                self.created = True
                return h._send(200, sc.start_body or {"session_id": sc.session_id})
            base = f"/api/sessions/{sc.session_id}"
            if method == "GET" and path == base:
                snap = self._snapshot()
                # a run moves on between polls; a gate moves only when it is decided
                if self.phases[self.phase].startswith("run:") and not sc.never_done:
                    self.phase += 1
                return h._send(200, snap)
            if method == "POST" and path == base + "/decision":
                body = h._body()
                self.decisions.append(body)
                if body.get("decision") not in ("approve", "deny", "revise"):
                    return h._send(400, {"error": "decision must be 'approve', 'deny' or 'revise'"})
                if not self.phases[self.phase].startswith("gate:"):
                    return h._send(409, {"error": "the run is not waiting at a gate"})
                if not sc.stuck_gate:
                    self.phase += 1
                return h._send(200, {"ok": True})
            if method == "POST" and path == base + "/cancel":
                return h._send(200, {"ok": True, "overall": "cancelled"})
            if method == "GET" and path == base + "/telemetry":
                if sc.telemetry == "error":
                    return h._send(500, {"error": "telemetry table unavailable"})
                return h._send(200, self._telemetry())
            if method == "DELETE" and path == base:
                self.deleted = True
                return h._send(200, {"ok": True})
            return h._send(404, {"error": f"no route for {method} {path}"})

    def count(self, method, path):
        return sum(1 for m, p in self.requests if m == method and p == path)


@pytest.fixture(autouse=True)
def _local_network(monkeypatch):
    for k in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "all_proxy", "ALL_PROXY"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")


@pytest.fixture
def make_app():
    apps = []

    def make(**kw) -> FakeApp:
        a = FakeApp(Scenario(**kw))
        apps.append(a)
        return a
    yield make
    for a in apps:
        a.close()


FAST = ["--poll", "0.02", "--poll-max", "0.05", "--gate-grace", "0.2", "--timeout", "20"]


def go(app, *extra, env=None, token=True):
    argv = ["--url", app.url, *FAST, *extra]
    e = {"AX_ID_TOKEN": TOKEN} if token else {}
    e.update(env or {})
    return smoke.run(argv, env=e)


def rows(out: str) -> dict:
    """The summary table as {check: (result, evidence)}."""
    found = {}
    for line in out.splitlines():
        parts = line.split("  ")
        parts = [p.strip() for p in parts if p.strip()]
        if len(parts) >= 3 and parts[1] in ("PASS", "FAIL", "WARN", "SKIP"):
            found[parts[0]] = (parts[1], parts[2])
    return found


def no_secrets(capsys_out: str, *more: str):
    for secret in (TOKEN, PASSWORD, "c2lnbmF0dXJlLXNlY3JldA", *more):
        assert secret not in capsys_out, f"{secret!r} leaked"


# --- The happy path -------------------------------------------------------------------------------
def test_happy_path_two_gates(make_app, capsys):
    app = make_app()
    code = go(app)
    out, err = capsys.readouterr()
    assert code == 0, out + err
    table = rows(out)
    for name in ("app answers and who it is", "run reached done", "review gates cleared",
                 "every agent ended done or skipped", "telemetry returned rows", "run cost is above zero",
                 "each model-calling agent has rows", "limits reachable and consistent", "run deleted"):
        assert table[name][0] == "PASS", (name, table.get(name), out)
    assert "approved: intake, research" in table["review gates cleared"][1]
    # exactly one approval per gate, in order, and the run deleted at the end
    assert [d["decision"] for d in app.decisions] == ["approve", "approve"]
    assert app.deleted and app.created
    assert app.topic == "Default topic"          # the workflow's own default topic, since none was given
    # every call carried the token and went to /api/*
    assert set(app.auth_seen) == {f"Bearer {TOKEN}"}
    assert all(p.startswith("/api/") for _, p in app.requests)
    no_secrets(out + err)


def test_discovers_agents_instead_of_assuming_names(make_app, capsys):
    app = make_app(agents=[("alpha", "main"), ("beta", "dedicated"), ("gamma", "main"), ("delta", "main")])
    assert go(app) == 0
    out = capsys.readouterr().out
    assert "alpha=done" in out and "delta=done" in out
    assert rows(out)["each model-calling agent has rows"][1].startswith("4 agents")


def test_topic_option_is_sent(make_app):
    app = make_app()
    assert go(app, "--topic", "my request") == 0
    assert app.topic == "my request"


def test_approve_as_label_goes_in_the_comment(make_app):
    app = make_app()
    assert go(app, "--approve-as", "release-bot") == 0
    assert all("release-bot" in d["comment"] for d in app.decisions)


def test_skipped_agent_is_not_a_failure(make_app, capsys):
    app = make_app(skip_tail=True)
    assert go(app) == 0
    assert "analysis=skipped" in capsys.readouterr().out


def test_a2a_agent_needs_no_telemetry_row_but_a_metered_one_does(make_app, capsys):
    app = make_app(telemetry="missing_agent")
    assert go(app) == 1
    row = rows(capsys.readouterr().out)["each model-calling agent has rows"]
    assert row[0] == "FAIL" and "report" in row[1] and "analysis" not in row[1].split("for:")[1]


# --- Things that go wrong -------------------------------------------------------------------------
def test_failed_agent_fails_the_run(make_app, capsys):
    app = make_app(fail_agent=True)
    code = go(app)
    out = capsys.readouterr().out
    assert code == 1
    t = rows(out)
    assert t["run reached done"][0] == "FAIL" and "failed" in t["run reached done"][1]
    assert t["every agent ended done or skipped"][0] == "FAIL"
    assert "analysis=failed" in t["every agent ended done or skipped"][1]
    assert app.deleted                      # still cleaned up


def test_timeout_fails_cancels_and_cleans_up(make_app, capsys):
    app = make_app(never_done=True)
    t0 = time.monotonic()
    code = smoke.run(["--url", app.url, "--poll", "0.02", "--poll-max", "0.05", "--timeout", "0.6"],
                     env={"AX_ID_TOKEN": TOKEN})
    out = capsys.readouterr().out
    assert code == 1 and time.monotonic() - t0 < 5
    assert rows(out)["run reached done"][0] == "FAIL" and "timeout" in rows(out)["run reached done"][1]
    methods = [(m, p) for m, p in app.requests if m != "GET"]
    assert ("POST", f"/api/sessions/{SID}/cancel") in methods
    assert methods.index(("POST", f"/api/sessions/{SID}/cancel")) < methods.index(("DELETE", f"/api/sessions/{SID}"))


def test_a_gate_that_never_clears_is_not_approved_forever(make_app, capsys):
    app = make_app(stuck_gate=True)
    code = go(app, "--gate-grace", "0.05", "--max-approvals", "3")
    out = capsys.readouterr().out
    assert code == 1
    assert len(app.decisions) == 3
    t = rows(out)
    assert t["review gates cleared"][0] == "FAIL" and "3 approvals" in t["review gates cleared"][1]
    assert app.count("POST", f"/api/sessions/{SID}/cancel") == 1


def test_a_gate_is_not_approved_twice_while_the_first_approval_is_still_taking_effect(make_app):
    # with a long grace, one slow-to-clear gate gets one approval, not one per poll
    app = make_app(stuck_gate=True)
    smoke.run(["--url", app.url, "--poll", "0.01", "--poll-max", "0.02", "--gate-grace", "60",
               "--timeout", "0.5"], env={"AX_ID_TOKEN": TOKEN})
    assert len(app.decisions) == 1


def test_rejected_token_is_exit_2_and_starts_nothing(make_app, capsys):
    app = make_app(unauthorised=True)
    code = go(app)
    out, err = capsys.readouterr()
    assert code == 2
    assert "HTTP 401" in err and "Unauthorized" in err
    assert not app.created and app.count("POST", "/api/sessions") == 0
    no_secrets(out + err)


def test_expired_or_wrong_token_is_exit_2(make_app, capsys):
    app = make_app()
    code = smoke.run(["--url", app.url, *FAST], env={"AX_ID_TOKEN": "not.the.token"})
    assert code == 2 and "HTTP 401" in capsys.readouterr().err


def test_limit_refusal_is_reported_not_a_crash(make_app, capsys):
    app = make_app(limit_on_start=True)
    code = go(app)
    out, err = capsys.readouterr()
    assert code == 2
    assert "429" in err and "limit" in err and "15:00 Eastern time" in err
    assert "unexpected" not in err and "Traceback" not in err + out
    assert not app.created


def test_server_error_shows_status_and_error_text_only(make_app, capsys):
    app = make_app(start_status=500, start_body={"error": "boom", "trace": "SECRET-TRACE-LINE", "env": "X"})
    code = go(app)
    out, err = capsys.readouterr()
    assert code == 2 and "HTTP 500: boom" in err
    assert "SECRET-TRACE-LINE" not in out + err


def test_empty_telemetry_fails(make_app, capsys):
    app = make_app(telemetry="empty")
    assert go(app) == 1
    t = rows(capsys.readouterr().out)
    assert t["telemetry returned rows"][0] == "FAIL"
    assert t["run cost is above zero"][0] == "SKIP"


def test_telemetry_route_error_fails_and_shows_the_error_text(make_app, capsys):
    app = make_app(telemetry="error")
    assert go(app) == 1
    t = rows(capsys.readouterr().out)
    assert t["telemetry returned rows"][0] == "FAIL" and "telemetry table unavailable" in t["telemetry returned rows"][1]


def test_zero_cost_is_a_warning_not_a_failure(make_app, capsys):
    app = make_app(telemetry="zero_cost")
    code = go(app)
    t = rows(capsys.readouterr().out)
    assert code == 0
    assert t["run cost is above zero"] == ("WARN", "cost is zero: prices unknown?")


def test_keep_does_not_delete(make_app, capsys):
    app = make_app()
    assert go(app, "--keep") == 0
    assert not app.deleted and app.count("DELETE", f"/api/sessions/{SID}") == 0
    assert rows(capsys.readouterr().out)["run deleted"][0] == "SKIP"


def test_control_plane_console_is_not_runnable(make_app, capsys):
    app = make_app(console_mode="builder")
    assert go(app) == 2
    assert "Control Plane" in capsys.readouterr().err and not app.created


def test_unreachable_app_is_exit_2(capsys):
    code = smoke.run(["--url", "http://127.0.0.1:9", "--timeout", "5"], env={"AX_ID_TOKEN": TOKEN})
    assert code == 2 and "unreachable" in capsys.readouterr().err


def test_a_bad_session_id_from_the_server_is_not_put_in_a_url(make_app, capsys):
    app = make_app(start_body={"session_id": "../../admin"})
    assert go(app) == 2
    assert "usable session id" in capsys.readouterr().err
    assert all("admin" not in p for _, p in app.requests)


def test_redirect_is_not_followed_so_the_token_stays_with_the_given_url(make_app, capsys):
    other = make_app()
    app = make_app(redirect_me_to=other.url + "/api/me")
    code = go(app)
    assert code == 2
    assert other.requests == []
    assert "HTTP 302" in capsys.readouterr().err


# --- /api/limits ----------------------------------------------------------------------------------
GOOD = {"limits": True,
        "daily": {"limit": 5.0, "used": 0.5, "resets": "midnight Eastern time", "warn": False, "reached": False},
        "hourly": {"limit": 20, "used": 3, "resets": "15:00 Eastern time", "warn": False, "reached": False}}


def _limits_with(**over):
    d = json.loads(json.dumps(GOOD))
    for k, v in over.items():
        d[k].update(v)
    return d


def test_limits_none_set(make_app, capsys):
    app = make_app()
    assert go(app) == 0
    row = rows(capsys.readouterr().out)["limits reachable and consistent"]
    assert row[0] == "PASS" and "no limit is set" in row[1]


def test_limits_consistent(make_app, capsys):
    app = make_app(limits=GOOD)
    assert go(app) == 0
    row = rows(capsys.readouterr().out)["limits reachable and consistent"]
    assert row[0] == "PASS" and "daily 0.5 of 5" in row[1] and "hourly 3 of 20" in row[1]


@pytest.mark.parametrize("limits,needle", [
    (_limits_with(daily={"warn": True}), "daily: warn/reached disagree"),
    (_limits_with(daily={"used": 6.0, "reached": False}), "daily: warn/reached disagree"),
    (_limits_with(hourly={"used": 0}), "hourly: shows 0 runs"),
    (_limits_with(daily={"used": 0.001}), "below this run's own cost"),
    (_limits_with(daily={"limit": "five"}), "daily: malformed"),
    ({"limits": True}, "no limit is listed"),
    ({"nope": 1}, "did not answer"),
])
def test_limits_inconsistent_fails(make_app, capsys, limits, needle):
    app = make_app(limits=limits)
    assert go(app) == 1
    row = rows(capsys.readouterr().out)["limits reachable and consistent"]
    assert row[0] == "FAIL" and needle in row[1], row


def test_limit_reached_is_a_warning(make_app, capsys):
    app = make_app(limits=_limits_with(hourly={"used": 20, "warn": True, "reached": True}))
    assert go(app) == 0
    row = rows(capsys.readouterr().out)["limits reachable and consistent"]
    assert row[0] == "WARN" and "REACHED" in row[1]


def test_limits_route_missing_is_skipped_not_failed(make_app, capsys):
    app = make_app(limits_status=404)
    assert go(app) == 0
    assert rows(capsys.readouterr().out)["limits reachable and consistent"][0] == "SKIP"


def test_limits_server_error_fails(make_app, capsys):
    app = make_app(limits_status=500)
    assert go(app) == 1


# --- Dry run --------------------------------------------------------------------------------------
def test_dry_run_calls_nothing_and_prints_no_token(make_app, capsys):
    app = make_app()
    code = smoke.run(["--url", app.url, "--dry-run"], env={"AX_ID_TOKEN": TOKEN})
    out, err = capsys.readouterr()
    assert code == 0
    assert app.requests == []
    assert "DRY RUN" in out and "/api/sessions" in out and "DELETE" in out
    no_secrets(out + err)


def test_dry_run_with_srp_names_the_user_but_not_the_password(make_app, capsys, monkeypatch):
    app = make_app()
    monkeypatch.setitem(sys.modules, "pycognito", None)    # would fail if it tried to import it
    code = smoke.run(["--url", app.url, "--dry-run", "--pool-id", "us-east-1_Pool", "--client-id", "cid",
                      "--username", "me@example.com"], env={"AX_PASSWORD": PASSWORD})
    out, err = capsys.readouterr()
    assert code == 0 and "me@example.com" in out and app.requests == []
    no_secrets(out + err)


def test_dry_run_with_keep_does_not_list_a_delete(make_app, capsys):
    app = make_app()
    smoke.run(["--url", app.url, "--dry-run", "--keep"], env={})
    assert "DELETE" not in capsys.readouterr().out


# --- Arguments ------------------------------------------------------------------------------------
@pytest.mark.parametrize("argv,env,needle", [
    (["--url", "https://x.example"], {}, "no credentials"),
    (["--url", "https://x.example", "--id-token", "tok-abcdef", "--pool-id", "p_1", "--client-id", "c",
      "--username", "u"], {}, "not both"),
    (["--url", "https://x.example", "--pool-id", "p_1"], {"AX_PASSWORD": "x" * 9}, "go together"),
    (["--url", "https://x.example", "--pool-id", "p_1", "--client-id", "c", "--username", "u"], {},
     "AX_PASSWORD is not set"),
    (["--url", "http://app.example.com", "--id-token", "abcd1234"], {}, "must be https"),
    (["--url", "ftp://app.example.com", "--id-token", "abcd1234"], {}, "http(s) address"),
    (["--url", "https://u:p@app.example.com", "--id-token", "abcd1234"], {}, "no credentials, query"),
    (["--url", "https://x.example", "--id-token", "ab cd"], {}, "whitespace"),
    (["--url", "https://x.example", "--id-token", "abcd1234", "--timeout", "0"], {}, "above zero"),
])
def test_bad_arguments_are_exit_2(argv, env, needle, capsys):
    assert smoke.run(argv, env=env) == 2
    err = capsys.readouterr().err
    assert needle in err
    assert "abcd1234" not in err and "ab cd" not in err


def test_there_is_no_password_option(capsys):
    with pytest.raises(SystemExit) as e:
        smoke.build_parser().parse_args(["--url", "https://x.example", "--password", PASSWORD])
    assert e.value.code == 2
    # and an unknown option through run() is also "could not run", with the password not echoed back
    assert smoke.run(["--url", "https://x.example", "--password", PASSWORD], env={}) == 2
    assert PASSWORD not in capsys.readouterr().err


def test_a_bad_value_is_not_echoed_back(capsys):
    # argparse would print "invalid float value: 'tok-secret-xyz'": a token pasted into the wrong option
    assert smoke.run(["--url", "https://x.example", "--timeout", "tok-secret-xyz"], env={}) == 2
    assert "tok-secret-xyz" not in capsys.readouterr().err


def test_api_suffix_and_trailing_slash_in_the_url_are_accepted():
    assert smoke.normalise_base("https://d1.cloudfront.net/") == "https://d1.cloudfront.net"
    assert smoke.normalise_base("https://d1.cloudfront.net/api") == "https://d1.cloudfront.net"
    assert smoke.normalise_base("https://x.example/stage/api/") == "https://x.example/stage"


def test_a_bearer_prefix_on_the_token_is_tolerated(make_app):
    app = make_app()
    assert go(app, env={"AX_ID_TOKEN": f"Bearer {TOKEN}"}) == 0


# --- Sign in with SRP -----------------------------------------------------------------------------
def _fake_pycognito(monkeypatch, *, fail=None):
    calls = {}

    class Cognito:
        def __init__(self, pool, client, user_pool_region=None, username=None):
            calls.update(pool=pool, client=client, region=user_pool_region, username=username)
            self.id_token = TOKEN

        def authenticate(self, password):
            calls["password_given"] = password == PASSWORD
            if fail:
                raise RuntimeError(fail)

        def check_token(self):
            calls["checked"] = calls.get("checked", 0) + 1
    mod = types.ModuleType("pycognito")
    mod.Cognito = Cognito
    monkeypatch.setitem(sys.modules, "pycognito", mod)
    return calls


SRP = ["--pool-id", "us-east-1_AbCdEf", "--client-id", "client123", "--username", "me@example.com"]


def test_srp_sign_in_and_run(make_app, capsys, monkeypatch):
    app = make_app()
    calls = _fake_pycognito(monkeypatch)
    code = smoke.run(["--url", app.url, *FAST, *SRP], env={"AX_PASSWORD": PASSWORD})
    out, err = capsys.readouterr()
    assert code == 0, out + err
    assert calls["password_given"] and calls["region"] == "us-east-1" and calls["username"] == "me@example.com"
    assert calls["checked"] >= 1                      # a long run asks for a current token each call
    assert set(app.auth_seen) == {f"Bearer {TOKEN}"}
    no_secrets(out + err)


def test_srp_failure_is_exit_2_and_the_password_is_not_echoed(make_app, capsys, monkeypatch):
    app = make_app()
    _fake_pycognito(monkeypatch, fail=f"NotAuthorizedException: Incorrect username or password {PASSWORD}")
    code = smoke.run(["--url", app.url, *FAST, *SRP], env={"AX_PASSWORD": PASSWORD})
    out, err = capsys.readouterr()
    assert code == 2 and "sign-in failed" in err and "NotAuthorizedException" in err
    no_secrets(out + err)
    assert app.requests == []


def test_missing_pycognito_says_how_to_install_it(make_app, capsys, monkeypatch):
    app = make_app()
    monkeypatch.setitem(sys.modules, "pycognito", None)
    code = smoke.run(["--url", app.url, *SRP], env={"AX_PASSWORD": PASSWORD})
    err = capsys.readouterr().err
    assert code == 2 and "pip install pycognito" in err and "--id-token" in err


def test_pycognito_is_not_imported_for_a_token_run(make_app, monkeypatch):
    app = make_app()
    monkeypatch.setitem(sys.modules, "pycognito", None)    # an import would raise ImportError
    assert go(app) == 0
    src = SCRIPT.read_text()
    top = src.split("def srp_token_provider")[0]
    assert "import pycognito" not in top and "from pycognito" not in top
    assert "boto3" not in src.replace("boto3_client", "")


# --- Secrets never appear -------------------------------------------------------------------------
@pytest.mark.parametrize("kw", [
    {}, {"fail_agent": True}, {"limit_on_start": True}, {"unauthorised": True}, {"telemetry": "error"},
    {"limits_echo_token": True}, {"start_status": 500, "start_body": {"error": f"Bearer {TOKEN} {PASSWORD}"}},
])
@pytest.mark.parametrize("verbose", [False, True])
def test_no_token_or_password_in_any_output(make_app, capsys, caplog, kw, verbose):
    app = make_app(**kw)
    caplog.set_level(logging.DEBUG)
    argv = ["--url", app.url, *FAST, *(["-v"] if verbose else [])]
    smoke.run(argv, env={"AX_ID_TOKEN": TOKEN, "AX_PASSWORD": PASSWORD})
    out, err = capsys.readouterr()
    no_secrets(out + err + caplog.text)
    if verbose:
        assert "GET /api/me -> " in out or "GET /api/me -> " in err or kw.get("unauthorised")
    assert "Authorization" not in out + err


def test_a_token_shaped_string_the_server_echoes_is_masked_too(make_app, capsys):
    app = make_app(limits_echo_other=True)
    assert go(app) == 1
    out = capsys.readouterr().out
    assert "other-token-123" not in out and "eyJzdWIiOiJ4" not in out
    assert "refused ***" in out


def test_the_token_travels_only_in_the_header(make_app):
    app = make_app()
    assert go(app) == 0
    assert all("?" not in p and "eyJ" not in p for p in app.raw_paths)


def test_the_token_given_on_the_command_line_is_not_echoed_either(make_app, capsys):
    app = make_app()
    smoke.run(["--url", app.url, *FAST, "-v", "--id-token", TOKEN], env={})
    out, err = capsys.readouterr()
    no_secrets(out + err)


def test_an_unexpected_exception_prints_no_traceback_and_no_secret(make_app, capsys, monkeypatch):
    app = make_app()

    def boom(self, *a, **k):
        raise ZeroDivisionError(f"oops Authorization: Bearer {TOKEN} {PASSWORD}")
    monkeypatch.setattr(smoke, "check_nodes", boom)
    code = smoke.run(["--url", app.url, *FAST], env={"AX_ID_TOKEN": TOKEN, "AX_PASSWORD": PASSWORD})
    out, err = capsys.readouterr()
    assert code == 2 and "unexpected ZeroDivisionError" in err and "Traceback" not in out + err
    no_secrets(out + err)


# --- Polling --------------------------------------------------------------------------------------
def test_polling_backs_off(make_app):
    app = make_app(never_done=True)
    smoke.run(["--url", app.url, "--poll", "0.03", "--poll-max", "0.4", "--timeout", "1.5"],
              env={"AX_ID_TOKEN": TOKEN})
    gaps = [b - a for a, b in zip(app.get_times, app.get_times[1:])]
    assert len(gaps) >= 4
    assert gaps[-1] > gaps[0] * 3          # it slows down
    assert max(gaps) < 0.6                 # and stops at poll-max


# --- Exit codes through a real process ------------------------------------------------------------
def _proc(*argv, env=None):
    e = {k: v for k, v in os.environ.items() if k not in ("AX_ID_TOKEN", "AX_PASSWORD")}
    e.update({"NO_PROXY": "127.0.0.1", "no_proxy": "127.0.0.1"})
    e.update(env or {})
    # Runs this repo's own script with the test's arguments; no outside input reaches it.
    return subprocess.run([sys.executable, str(SCRIPT), *argv],  # noqa: S603  # nosec B603
                          env=e, capture_output=True, text=True, timeout=60)


def test_process_exit_codes(make_app):
    ok = make_app()
    p = _proc("--url", ok.url, *FAST, env={"AX_ID_TOKEN": TOKEN})
    assert p.returncode == 0, p.stdout + p.stderr
    assert "all checks passed" in p.stdout
    bad = make_app(fail_agent=True)
    p = _proc("--url", bad.url, *FAST, env={"AX_ID_TOKEN": TOKEN})
    assert p.returncode == 1 and "a check failed" in p.stdout
    p = _proc("--url", ok.url)                             # no credentials
    assert p.returncode == 2
    before = list(ok.requests)
    p = _proc("--url", ok.url, "--dry-run", env={"AX_ID_TOKEN": TOKEN})
    assert p.returncode == 0 and ok.requests == before
    assert TOKEN not in p.stdout + p.stderr


def test_process_help_works_without_boto3_or_pycognito():
    p = _proc("--help")
    assert p.returncode == 0 and "--approve-as" in p.stdout
