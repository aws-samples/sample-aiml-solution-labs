"""Sending runs to Langfuse, Datadog, Grafana Cloud, Honeycomb or any OTLP backend: the BFF
routes and the presets (bff/traceexport.py; export-contract.md section 6).
What has to hold:
  * the console's test sends ONE OTLP/HTTP protobuf span to the preset address with the headers
    the page composed, through the SSRF guard, and says what happened in plain words;
  * a header value never comes back, even when the vendor echoes it; saved keys are used only
    for the address they were saved for, so an editor cannot send them anywhere else;
  * every route checks who is asking before it reads or sends anything: another user's build is
    404, a test without `deploy` 403, the app's test and pause admin-only, a re-send the admin's
    or the run owner's;
  * off (no orchestrator.export), the app's routes call no runtime and change nothing;
  * the runtime is called with exactly the contract's payloads, parameters the shipped
    bedrock-agentcore model accepts, and no trace header;
  * the pause row is the contract's, and never shows as a run;
  * a saved setup in the library keeps no keys;
  * the presets agree with the page's copy (tests/fixtures/export_presets.json), and the routes
    are on both IaC route lists behind the JWT authorizer.
DynamoDB, S3 and Secrets Manager are moto's. The vendor is a fake opener in place of
netguard.urlopen; the SSRF tests use the real netguard, refused before any connection.
"""
from __future__ import annotations

import importlib
import io
import json
import struct
import sys
import types
import urllib.error
from pathlib import Path

import boto3
import botocore.loaders
import botocore.model
import botocore.validate
import pytest

moto = pytest.importorskip("moto")
from test_builds import _events_table, _status_table, call, create_builds_table, env, save  # noqa: E402,F401 - after the moto importorskip; env is the fixture, taken by name

ORCH = Path(__file__).resolve().parents[1]
PRESETS = json.loads((ORCH / "tests" / "fixtures" / "export_presets.json").read_text())
LF_URL = "https://cloud.langfuse.com/api/public/otel/v1/traces"
LF_HEADERS = {"Authorization": "Basic cGstbGYtMTIzNDpzay1sZi01Njc4", "x-langfuse-ingestion-version": "4"}


def _te(e):
    return e.handler.traceexport


# --- a protobuf reader, to check the wire format without the SDK ----------------------------

def _fields(b: bytes) -> list[tuple[int, int, object]]:
    """(field, wire type, value) of one protobuf message; length-delimited values as bytes."""
    out, i = [], 0
    while i < len(b):
        key, i = _uvarint(b, i)
        field, wire = key >> 3, key & 7
        if wire == 0:
            v, i = _uvarint(b, i)
        elif wire == 1:
            v, i = struct.unpack("<Q", b[i:i + 8])[0], i + 8
        elif wire == 2:
            n, i = _uvarint(b, i)
            v, i = b[i:i + n], i + n
        else:
            raise AssertionError(f"unexpected wire type {wire}")
        out.append((field, wire, v))
    assert i == len(b)
    return out


def _uvarint(b: bytes, i: int) -> tuple[int, int]:
    n = shift = 0
    while True:
        c = b[i]
        i += 1
        n |= (c & 0x7F) << shift
        shift += 7
        if not c & 0x80:
            return n, i


def _one(fields, n):
    got = [v for f, _, v in fields if f == n]
    assert len(got) == 1, (n, got)
    return got[0]


def _attrs(fields, n) -> dict:
    out = {}
    for f, _, kv in fields:
        if f == n:
            kvf = _fields(kv)
            any_value = _fields(_one(kvf, 2))
            out[_one(kvf, 1).decode()] = (_one(any_value, 1).decode() if any_value[0][0] == 1
                                          else bool(_one(any_value, 2)))
    return out


def _decode(body: bytes) -> dict:
    rs = _fields(_one(_fields(body), 1))                     # ExportTraceServiceRequest.resource_spans
    resource = _attrs(_fields(_one(rs, 1)), 1)               # ResourceSpans.resource.attributes
    ss = _fields(_one(rs, 2))                                # ResourceSpans.scope_spans
    scope = _fields(_one(ss, 1))
    span = _fields(_one(ss, 2))
    status = _fields(_one(span, 15))
    return {"resource": resource, "scope": _one(scope, 1).decode(), "traceId": _one(span, 1).hex(),
            "spanId": _one(span, 2).hex(), "name": _one(span, 5).decode(), "kind": _one(span, 6),
            "start": _one(span, 7), "end": _one(span, 8), "attrs": _attrs(span, 9), "status": _one(status, 3)}


# --- the console's test ------------------------------------------------------------------------

class _Resp:
    def __init__(self, status):
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class Vendor:
    """In place of netguard.urlopen: records each request, answers with `status` and `body`."""

    def __init__(self):
        self.status, self.body = 200, b""
        self.requests: list[dict] = []

    def __call__(self, req, timeout=None):
        self.requests.append({"url": req.full_url, "method": req.get_method(), "timeout": timeout,
                              "headers": {k.lower(): v for k, v in req.header_items()}, "body": req.data})
        if self.status >= 400:
            raise urllib.error.HTTPError(req.full_url, self.status, "x", {}, io.BytesIO(self.body))
        return _Resp(self.status)


@pytest.fixture()
def vendor(env, monkeypatch):  # noqa: F811 - `env` is test_builds' fixture, which pytest injects by name
    v = Vendor()
    monkeypatch.setattr(_te(env).netguard, "urlopen", v)
    _te(env)._tests.clear()
    return v


def _lf_test(env, **over):  # noqa: F811 - `env` is test_builds' fixture, which pytest injects by name
    body = {"destination": "langfuse", "region": "eu", "headers": LF_HEADERS, **over}
    return call(env, "POST /api/builds/{id}/export/test", params={"id": "pclaims01"}, body=body)


def test_a_test_sends_one_protobuf_span_to_the_preset_with_the_headers(env, vendor):  # noqa: F811 - `env` is test_builds' fixture, which pytest injects by name
    save(env)
    status, got = _lf_test(env)
    assert status == 200 and got["ok"] is True and got["message"] == "Langfuse accepted a test trace."
    assert got["url"] == LF_URL and len(vendor.requests) == 1
    sent = vendor.requests[0]
    assert sent["url"] == LF_URL and sent["method"] == "POST" and sent["timeout"] == 5
    assert sent["headers"]["content-type"] == "application/x-protobuf"
    assert sent["headers"]["authorization"] == LF_HEADERS["Authorization"]
    assert sent["headers"]["x-langfuse-ingestion-version"] == "4"
    span = _decode(sent["body"])
    agent = env.table.get_item(Key={"pk": "BUILD#pclaims01", "sk": "META"})["Item"]["agentName"]
    assert span["traceId"] == got["traceId"] and len(span["traceId"]) == 32 and len(span["spanId"]) == 16
    assert span["name"] == "AgentExpress connection test" and span["kind"] == 1 and span["status"] == 1
    assert span["resource"] == {"service.name": agent} and span["scope"] == "agentexpress"
    assert span["attrs"]["agentexpress.test"] is True and span["start"] < span["end"]
    events = call(env, "GET /api/audit")[1]
    tested = [e for e in events if e["action"] == "export.tested"]
    assert tested and tested[0]["detail"]["ok"] is True and tested[0]["detail"]["destination"] == "langfuse"
    assert "Basic" not in json.dumps(events)


@pytest.mark.parametrize(("status", "words"), [
    (401, "Langfuse refused the key (HTTP 401). Check the public and secret keys"),
    (404, "Nothing receives traces at"), (503, "could not take it just now"), (400, "refused the test trace")])
def test_a_refusal_is_said_plainly_and_no_header_value_comes_back(env, vendor, status, words):  # noqa: F811 - `env` is test_builds' fixture, which pytest injects by name
    save(env)
    # A vendor that echoes the credentials back in its error.
    vendor.status = status
    vendor.body = json.dumps({"message": f"bad auth {LF_HEADERS['Authorization']} / cGstbGYtMTIzNDpzay1sZi01Njc4"}).encode()
    code, got = _lf_test(env)
    assert code == 200 and got["ok"] is False and got["status"] == status
    assert words in got["error"] and "[redacted]" in got["error"]
    assert "cGstbGYtMTIzNDpzay1sZi01Njc4" not in json.dumps(got)


def test_a_timeout_and_an_unreachable_host_are_said_plainly(env, vendor, monkeypatch):  # noqa: F811 - `env` is test_builds' fixture, which pytest injects by name
    save(env)
    monkeypatch.setattr(_te(env).netguard, "urlopen", lambda req, timeout=None: (_ for _ in ()).throw(TimeoutError()))
    assert _lf_test(env)[1]["error"] == "cloud.langfuse.com did not answer within 5 seconds."
    monkeypatch.setattr(_te(env).netguard, "urlopen",
                        lambda req, timeout=None: (_ for _ in ()).throw(urllib.error.URLError("Name or service not known")))
    assert _lf_test(env)[1]["error"] == "Could not reach cloud.langfuse.com: Name or service not known."


@pytest.mark.parametrize("endpoint", ["https://127.0.0.1/v1/traces", "https://169.254.169.254/v1/traces",
                                      "https://10.0.0.8:4318/v1/traces", "https://[::1]/v1/traces"])
def test_the_ssrf_guard_refuses_a_private_address_before_connecting(env, endpoint, monkeypatch):  # noqa: F811 - `env` is test_builds' fixture, which pytest injects by name
    save(env)
    _te(env)._tests.clear()
    connected = []
    monkeypatch.setattr("socket.socket.connect", lambda *a: connected.append(a))
    status, got = call(env, "POST /api/builds/{id}/export/test", params={"id": "pclaims01"},
                       body={"destination": "otlp", "endpoint": endpoint, "headers": {"x-api-key": "k-123456"}})
    if "[" in endpoint:   # not a shape the endpoint pattern takes at all
        assert status == 400
    else:
        assert status == 200 and got["ok"] is False and got["error"].startswith("That address is not allowed")
    assert connected == []


@pytest.mark.parametrize(("body", "words"), [
    ({"destination": "otlp", "endpoint": "http://collector.example.com/v1/traces"}, "https://"),
    ({"destination": "otlp", "endpoint": "https://u:p@collector.example.com/v1/traces"}, "https://"),
    ({"destination": "datadog", "region": "evil.example.com"}, "Datadog site"),
    ({"destination": "langfuse", "region": "eu", "headers": {"Host": "evil.example.com"}}, "set by the client"),
    ({"destination": "langfuse", "region": "eu", "headers": {"x-a": "one\r\nx-b: two"}}, "one line"),
    ({"destination": "langfuse", "region": "eu", "headers": {f"h{i}": "v" for i in range(11)}}, "at most 10"),
])
def test_a_bad_setup_is_refused_before_anything_is_sent(env, vendor, body, words):  # noqa: F811 - `env` is test_builds' fixture, which pytest injects by name
    save(env)
    status, got = call(env, "POST /api/builds/{id}/export/test", params={"id": "pclaims01"},
                       body={"headers": {"x-api-key": "k"}, **body})
    assert status == 400 and words in got["error"] and vendor.requests == []


def test_saved_keys_are_used_only_for_the_address_they_were_saved_for(env, vendor):  # noqa: F811 - `env` is test_builds' fixture, which pytest injects by name
    save(env)
    name = _te(env).secret_name(LF_URL)
    assert call(env, "PUT /api/builds/{id}/secrets", params={"id": "pclaims01"},
                body={"exportHeaders": {name: json.dumps(LF_HEADERS)}})[0] == 200
    names = call(env, "GET /api/builds/{id}/secrets", params={"id": "pclaims01"})[1]
    assert names["exportHeaders"] == [name] and "cGstbGYt" not in json.dumps(names)
    status, got = _lf_test(env, headers=None)
    assert status == 200 and got["ok"] and vendor.requests[-1]["headers"]["authorization"] == LF_HEADERS["Authorization"]
    # The same build, the same saved keys, an address of the caller's choosing: nothing is sent.
    for body in ({"destination": "otlp", "endpoint": "https://collector.attacker.example/v1/traces"},
                 {"destination": "langfuse", "region": "us"}):
        status, got = call(env, "POST /api/builds/{id}/export/test", params={"id": "pclaims01"}, body=body)
        assert status == 400 and "none are saved for this address" in got["error"]
    assert len(vendor.requests) == 1


def test_only_someone_who_may_change_the_build_and_deploy_may_test(env, vendor):  # noqa: F811 - `env` is test_builds' fixture, which pytest injects by name
    save(env)
    assert call(env, "POST /api/builds/{id}/export/test", params={"id": "pclaims01"}, sub="u2",
                body={"destination": "langfuse", "region": "eu", "headers": LF_HEADERS})[0] == 404
    assert call(env, "POST /api/builds/{id}/export/test", params={"id": "pclaims01"}, groups=("viewers",),
                body={"destination": "langfuse", "region": "eu", "headers": LF_HEADERS})[0] == 403
    assert call(env, "POST /api/builds/{id}/export/test", params={"id": "../etc"},
                body={"destination": "langfuse", "region": "eu", "headers": LF_HEADERS})[0] == 400
    assert vendor.requests == []


def test_tests_are_held_to_ten_a_minute(env, vendor):  # noqa: F811 - `env` is test_builds' fixture, which pytest injects by name
    save(env)
    assert all(_lf_test(env)[0] == 200 for _ in range(10))
    status, got = _lf_test(env)
    assert status == 429 and "10 tests a minute" in got["error"] and len(vendor.requests) == 10


# --- the build's app ---------------------------------------------------------------------------

RULES = {"groupsClaim": "cognito:groups", "actions": {"admin": ["admins"], "audit": ["admins", "users"]}}
EXPORT = {"destination": "langfuse", "region": "eu", "secret": "export-89ecd80e38f1f3f3", "project": "proj1"}
MODULES = ("workflow", "authz", "chatbot", "handler", "builds", "buildstore", "accounts",
           "designer", "policies", "audit", "traceexport", "library")


def _shipped_model():
    loader = botocore.loaders.Loader(extra_search_paths=[str(ORCH / "bff" / "botocore_data")],
                                     include_default_search_paths=False)
    return botocore.model.ServiceModel(loader.load_service_model("bedrock-agentcore", "service-2"),
                                       "bedrock-agentcore")


class Runtime:
    """InvokeAgentRuntime, checked against the model the BFF ships; answers as the contract says."""
    shape = _shipped_model().operation_model("InvokeAgentRuntime").input_shape

    def __init__(self):
        self.calls: list[dict] = []
        self.answer = {"ok": True, "status": 200, "error": "", "traceId": "ab" * 16}

    def invoke_agent_runtime(self, **kw):
        botocore.validate.validate_parameters(kw, self.shape)
        self.calls.append(kw)
        return {"response": io.BytesIO(json.dumps(self.answer).encode())}

    def payloads(self):
        return [json.loads(c["payload"]) for c in self.calls]


def _app(monkeypatch, export=EXPORT, mode="app"):
    wf = {"agents": {"a": {"name": "A"}}, "steps": [{"agent": "a"}], "authorization": RULES,
          **({"orchestrator": {"export": export}} if export else {})}
    for k, v in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing", "AWS_REGION": "us-east-1",
                 "AWS_DEFAULT_REGION": "us-east-1", "WORKFLOW_JSON": json.dumps(wf),
                 "STATUS_TABLE": "app_status", "EVENTS_TABLE": "app_events", "CONSOLE_MODE": mode,
                 "RUNTIME_ARN": "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/app-X",
                 "BUILDS_TABLE": "app_builds", "BUILDS_BUCKET": "app-builds", "DEPLOY_PROJECT": "deploy"}.items():
        monkeypatch.setenv(k, v)
    for mod in MODULES:
        sys.modules.pop(mod, None)
    ddb = boto3.client("dynamodb")
    _status_table(ddb, "app_status")
    _events_table(ddb, "app_events")
    create_builds_table(ddb, "app_builds")
    boto3.client("s3").create_bucket(Bucket="app-builds")
    handler = importlib.import_module("handler")
    rt = Runtime()
    monkeypatch.setattr(handler, "agentcore", rt)
    return types.SimpleNamespace(handler=handler, rt=rt, status=boto3.resource("dynamodb").Table("app_status"))


@pytest.fixture()
def app(monkeypatch):
    with moto.mock_aws():
        yield _app(monkeypatch)
    for mod in MODULES:
        sys.modules.pop(mod, None)


@pytest.fixture()
def app_off(monkeypatch):
    with moto.mock_aws():
        yield _app(monkeypatch, export=None)
    for mod in MODULES:
        sys.modules.pop(mod, None)


def acall(a, route_key, *, params=None, body=None, sub="alice", groups=("users",), qs=None):
    return call(a, route_key, params=params, body=body, sub=sub, groups=groups, qs=qs,
                path=route_key.split(" ", 1)[1].replace("{id}", (params or {}).get("id", "")))


def _run(a, sid, owner, export=None):
    a.status.put_item(Item={"session_id": sid, "owner": owner, "topic": "t", "overall": "done",
                            "created": "2026-01-01 10:00:00", **({"export": export} if export else {})})


def test_off_every_route_but_the_status_is_404_and_nothing_is_called(app_off):
    assert acall(app_off, "GET /api/export") == (200, {"on": False})
    _run(app_off, "aaaaaaaaaaaa", "alice")
    for route, params, body in (("POST /api/export/test", None, None), ("PUT /api/export/pause", None, {"paused": True}),
                                ("POST /api/sessions/{id}/export", {"id": "aaaaaaaaaaaa"}, None)):
        status, got = acall(app_off, route, params=params, body=body, groups=("admins",))
        assert status == 404 and "Settings tab" in got["error"]
    assert app_off.rt.calls == []
    assert "Item" not in app_off.status.get_item(Key={"session_id": "_export#paused"})


def test_a_control_plane_console_answers_404(monkeypatch):
    with moto.mock_aws():
        a = _app(monkeypatch, mode="builder")
        for route in a.handler.traceexport.ROUTES:
            assert acall(a, route, params={"id": "aaaaaaaaaaaa"}, groups=("admins",))[0] == 404
        assert a.rt.calls == []
    for mod in MODULES:
        sys.modules.pop(mod, None)


def test_the_status_says_where_runs_go_and_admins_also_get_the_last_day(app):
    now = app.handler.traceexport._dt.datetime.now(app.handler.traceexport._dt.UTC)
    iso = lambda h: (now - app.handler.traceexport._dt.timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: E731 - a one-line test helper
    _run(app, "s00000000001", "alice", {"status": "sent", "traceId": "a" * 32, "at": iso(1)})
    _run(app, "s00000000002", "bob", {"status": "failed", "error": "HTTP 401 from Langfuse", "at": iso(2)})
    _run(app, "s00000000003", "bob", {"status": "failed", "error": "older", "at": iso(5)})
    _run(app, "s00000000004", "bob", {"status": "sent", "at": iso(30)})          # outside the day
    _run(app, "s00000000005", "bob")
    status, got = acall(app, "GET /api/export")
    assert status == 200 and got == {
        "on": True, "destination": "langfuse", "label": "Langfuse (EU)", "paused": False, "canManage": False,
        "traceLink": "https://cloud.langfuse.com/project/proj1/traces/{traceId}",
        "send": {"content": False, "evals": True, "metrics": True}}
    got = acall(app, "GET /api/export", groups=("admins",))[1]
    assert got["canManage"] is True
    assert got["recent"] == {"sent": 1, "failed": 2, "lastError": "HTTP 401 from Langfuse", "lastErrorAt": iso(2)}
    assert "secret" not in json.dumps(got) and "export-89ecd80e" not in json.dumps(got)


def test_the_test_trace_is_for_admins_and_calls_the_runtime_as_the_contract_says(app):
    assert acall(app, "POST /api/export/test")[0] == 403
    assert app.rt.calls == []
    status, got = acall(app, "POST /api/export/test", groups=("admins",))
    assert status == 200 and got == {"ok": True, "status": 200, "error": "", "traceId": "ab" * 16}
    (c,) = app.rt.calls
    assert json.loads(c["payload"]) == {"action": "export_test"}
    assert c["agentRuntimeArn"].endswith("runtime/app-X") and len(c["runtimeSessionId"]) >= 33
    assert not {"traceParent", "traceId", "traceState", "baggage"} & set(c)       # ADOT signs those itself


def test_a_runtime_that_fails_is_said_and_never_raises(app):
    def boom(**kw):
        raise RuntimeError("down")
    app.rt.invoke_agent_runtime = boom
    status, got = acall(app, "POST /api/export/test", groups=("admins",))
    assert status == 200 and got["ok"] is False and "could not be reached (RuntimeError)" in got["error"]


def test_a_run_is_sent_again_by_its_owner_or_an_admin_only(app):
    _run(app, "aaaaaaaaaaaa", "alice")
    app.rt.answer = {**app.rt.answer, "spans": 14}
    assert acall(app, "POST /api/sessions/{id}/export", params={"id": "aaaaaaaaaaaa"}, sub="bob")[0] == 404
    assert acall(app, "POST /api/sessions/{id}/export", params={"id": "bbbbbbbbbbbb"}, groups=("admins",))[0] == 404
    assert acall(app, "POST /api/sessions/{id}/export", params={"id": "../x"})[0] == 400
    assert app.rt.calls == []
    status, got = acall(app, "POST /api/sessions/{id}/export", params={"id": "aaaaaaaaaaaa"})
    assert status == 200 and got["spans"] == 14
    assert acall(app, "POST /api/sessions/{id}/export", params={"id": "aaaaaaaaaaaa"}, sub="root",
                 groups=("admins",))[0] == 200
    assert app.rt.payloads() == [{"action": "export_run", "session_id": "aaaaaaaaaaaa"}] * 2
    # A run from before runs had owners belongs to nobody: an admin only.
    _run(app, "cccccccccccc", "")
    assert acall(app, "POST /api/sessions/{id}/export", params={"id": "cccccccccccc"}, sub="")[0] == 404


def test_pause_is_the_contracts_row_and_is_never_a_run(app):
    assert acall(app, "PUT /api/export/pause", body={"paused": True})[0] == 403
    assert acall(app, "PUT /api/export/pause", body={"paused": "yes"}, groups=("admins",))[0] == 400
    status, got = acall(app, "PUT /api/export/pause", body={"paused": True}, groups=("admins",))
    assert status == 200 and got["paused"] is True and got["by"] == "alice@example.com"
    row = app.status.get_item(Key={"session_id": "_export#paused"})["Item"]
    assert row["paused"] is True and row["by"] == "alice@example.com" and row["at"].endswith("Z")
    assert acall(app, "GET /api/export")[1]["paused"] is True
    _run(app, "aaaaaaaaaaaa", "alice")
    listed = acall(app, "GET /api/sessions", groups=("admins",), qs={"scope": "all"})[1]
    assert [r["session_id"] for r in listed] == ["aaaaaaaaaaaa"]
    assert acall(app, "PUT /api/export/pause", body={"paused": False}, groups=("admins",))[1]["paused"] is False
    actions = [e["action"] for e in acall(app, "GET /api/audit")[1]]
    assert "export.paused" in actions and "export.resumed" in actions


# --- the library -------------------------------------------------------------------------------

@pytest.mark.parametrize(("definition", "ok"), [
    ({"destination": "datadog", "region": "us5.datadoghq.com", "send": {"content": False}}, True),
    ({"destination": "otlp", "endpoint": "https://c.example.com/v1/traces",
      "secret": "arn:aws:secretsmanager:us-east-1:123456789012:secret:otlp-headers-AbCdEf"}, True),
    ({"destination": "langfuse", "region": "eu", "secret": "export-89ecd80e38f1f3f3"}, False),
    ({"destination": "langfuse", "region": "eu", "headers": {"Authorization": "Basic x"}}, False),
    ({"destination": "langfuse", "region": "mars"}, False),
    ({"destination": "langfuse", "region": "eu", "send": {"content": "yes"}}, False),
])
def test_a_saved_setup_keeps_no_keys(env, definition, ok):  # noqa: F811 - `env` is test_builds' fixture, which pytest injects by name
    status, got = call(env, "POST /api/library", body={"kind": "export", "name": "teamLangfuse", "definition": definition})
    assert (status == 200) is ok, got
    if ok:
        listed = call(env, "GET /api/library", qs={"kind": "export"})[1]
        assert [i["definition"] for i in listed] == [definition]


# --- presets and IaC ---------------------------------------------------------------------------

@pytest.mark.parametrize("case", PRESETS["cases"], ids=[json.dumps(c["setup"]) for c in PRESETS["cases"]])
def test_the_presets_match_the_shared_fixture(case):
    sys.path.insert(0, str(ORCH / "bff"))
    te = importlib.import_module("traceexport")
    if "error" in case:
        with pytest.raises(te.Refused) as e:
            te.traces_url(case["setup"])
        assert case["error"] in str(e.value)
        return
    assert te.traces_url(case["setup"]) == case["url"]
    assert te.label(case["setup"]) == case["label"]
    assert te.trace_link(case["setup"]) == case["link"]


def test_the_secret_names_match_the_shared_fixture():
    sys.path.insert(0, str(ORCH / "bff"))
    te = importlib.import_module("traceexport")
    for c in PRESETS["secretNames"]:
        assert te.secret_name(c["url"]) == c["name"]


def test_every_route_is_on_both_route_lists_behind_the_authorizer():
    sys.path.insert(0, str(ORCH / "bff"))
    te = importlib.import_module("traceexport")
    tf = (ORCH / "terraform" / "bff.tf").read_text()
    cdk = (ORCH / "cdk" / "lib" / "orchestrator-stack.ts").read_text()
    routes_block = tf[tf.index('resource "aws_apigatewayv2_route" "routes"'):tf.index('resource "aws_apigatewayv2_route" "hook"')]
    assert 'authorization_type = local.auth_enabled ? "JWT" : "NONE"' in routes_block
    cdk_routes = cdk[cdk.index("const routes: Array<"):cdk.index("for (const r of routes)")]
    for route in (*te.ROUTES, te.CONSOLE_ROUTE):
        method, path = route.split(" ", 1)
        assert f'"{route}",' in routes_block, route
        assert f'{{ path: "{path}", methods: [HttpMethod.{method}] }}' in cdk_routes, route
        assert path not in tf[tf.index('resource "aws_apigatewayv2_route" "hook"'):]
