"""External triggers (orchestrator.triggers, bff/triggers.py) and review gates decided
without the run page (bff/gates.py, app/common/gates.py).

What has to hold:
  * a webhook starts a run only with a valid, fresh signature, in every supported scheme;
    a delivery seen before starts no second run; a trigger that is off, or over its
    hourly cap, starts none; the answer never carries anything the run produced;
  * a schedule, an EventBridge event, an S3 object and an SQS message each start one run,
    with the prompt filled from the delivery, and a failed SQS message goes back;
  * the run belongs to the owner, or to the trigger with its approvers able to see it;
  * a gate approves itself only when its spec says so (or a trigger's `gates: "auto"`),
    and says so; an event decides only a gate that takes events; a timeout decides once.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import importlib
import json
import sys
import time

import pytest

TRIGGERS = {
    "hook": {"type": "webhook", "prompt": "Triage {{body.issue.key}}: {{body.issue.title}} ({{headers.x-team}})",
             "attachPayload": True, "maxRunsPerHour": 3},
    "gh": {"type": "webhook", "signature": "github", "prompt": "PR {{body.number}}"},
    "slack": {"type": "webhook", "signature": "slack", "prompt": "{{body.event.text}}"},
    "stripe": {"type": "webhook", "signature": "stripe", "prompt": "Payment {{body.data.object.id}}"},
    "tok": {"type": "webhook", "signature": "token", "prompt": "x {{body.a}}", "runAs": "service",
            "approvers": ["ops"]},
    "off": {"type": "webhook", "prompt": "x", "enabled": False},
    "nightly": {"type": "schedule", "expression": "rate(1 day)", "prompt": "Report for {{time}}", "gates": "auto"},
    "alarms": {"type": "eventbridge", "pattern": {"source": ["aws.cloudwatch"]},
               "prompt": "Investigate {{detail.alarmName}} in {{event.region}}"},
    "inbox": {"type": "s3", "bucket": "acme", "prompt": "Review {{detail.object.key}}"},
    "queue": {"type": "sqs", "prompt": "{{body.text}}"},
}
SECRETS = {"hook": "s" * 32, "gh": "g" * 32, "slack": "k" * 32, "stripe": "w" * 32, "tok": "t" * 32}
WORKFLOW = {"orchestrator": {"triggers": TRIGGERS},
            "agents": {"a": {"name": "A"}}, "steps": [{"agent": "a", "hitl": {"approval": "event"}}]}


class Table:
    """The events table's surface the triggers use: conditional puts and an ADD counter."""

    def __init__(self):
        self.items: dict = {}

    def put_item(self, Item, ConditionExpression=None):
        k = (Item["session_id"], Item["ts"])
        if ConditionExpression and k in self.items:
            raise type("ConditionalCheckFailedException", (Exception,), {})()
        self.items[k] = Item

    def update_item(self, Key, ExpressionAttributeValues, **kw):
        k = (Key["session_id"], Key["ts"])
        n = self.items.get(k, {}).get("n", 0)
        if n >= ExpressionAttributeValues[":max"]:
            raise type("ConditionalCheckFailedException", (Exception,), {})()
        self.items[k] = {**Key, "n": n + 1}

    def query(self, **kw):
        return {"Items": sorted((v for (s, t), v in self.items.items() if t.startswith("delivery#")),
                                key=lambda i: i["ts"], reverse=True)}


@pytest.fixture()
def trig(monkeypatch):
    monkeypatch.setenv("WORKFLOW_JSON", json.dumps(WORKFLOW))
    monkeypatch.setenv("TRIGGER_SECRET_ARN", "arn:aws:secretsmanager:us-east-1:1:secret:x")
    monkeypatch.setenv("TRIGGER_QUEUES", json.dumps({"arn:aws:sqs:us-east-1:1:q": "queue"}))
    monkeypatch.setenv("EVENTS_TABLE", "t-events")
    for mod in ("workflow", "triggers"):
        sys.modules.pop(mod, None)
    t = importlib.import_module("triggers")
    t._clients["table"] = Table()
    t._cache["secrets"] = (time.time() + 600, dict(SECRETS))
    started: list = []

    def start(topic, owner, user, extra):
        started.append({"topic": topic, "owner": owner, "user": user, **extra})
        return f"sid{len(started)}"
    t._test_start, t._test_started = start, started
    t.OWNER_EMAIL, t.USER_POOL_ID = "owner@example.com", "pool"
    t._cache["owner"] = ("owner-sub", "owner@example.com")
    yield t
    for mod in ("workflow", "triggers"):
        sys.modules.pop(mod, None)


def _hex(secret, msg: bytes) -> str:
    return hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()


def hook(t, name, body: dict, headers: dict):
    raw = json.dumps(body)
    return t.webhook({"pathParameters": {"name": name}, "headers": headers, "body": raw}, t._test_start)


def signed(name, body, ts=None, **extra):
    ts = str(int(time.time()) if ts is None else ts)
    raw = json.dumps(body)
    return {"X-AX-Timestamp": ts, "X-AX-Signature": "sha256=" + _hex(SECRETS[name], f"{ts}.{raw}".encode()), **extra}


BODY = {"issue": {"key": "OPS-7", "title": "Disk full"}}


# --- the prompt ---------------------------------------------------------------------------

def test_render_fills_paths_and_never_runs_anything(trig):
    ctx = {"body": {"a": {"b": [1, {"c": "deep"}]}, "n": 5, "obj": {"x": 1}}}
    assert trig.render("{{body.a.b.1.c}} {{body.n}} {{body.obj}} [{{body.nope}}]", ctx) == 'deep 5 {"x": 1} []'
    assert trig.render("{{ __import__('os').system('x') }}", ctx) == "{{ __import__('os').system('x') }}"
    assert len(trig.render("{{body.big}}", {"body": {"big": "y" * 9000}})) == trig.VALUE_MAX


# --- webhooks -------------------------------------------------------------------------------

def test_a_signed_webhook_starts_one_run_with_its_payload(trig):
    status, got = hook(trig, "hook", BODY, signed("hook", BODY, **{"X-Team": "infra", "X-AX-Delivery": "d1"}))
    assert status == 202 and got == {"started": True, "session_id": "sid1"}
    run = trig._test_started[0]
    assert run["topic"] == "Triage OPS-7: Disk full (infra)"
    assert run["owner"] == "owner-sub" and run["payload"] == BODY
    assert run["trigger"] == {"name": "hook", "type": "webhook", "source": "webhook", "runAs": "owner"}
    # The same delivery again starts nothing.
    status, again = hook(trig, "hook", BODY, signed("hook", BODY, **{"X-AX-Delivery": "d1"}))
    assert status == 200 and again["duplicate"] and len(trig._test_started) == 1


@pytest.mark.parametrize("headers", [
    {},
    {"X-AX-Timestamp": str(int(time.time())), "X-AX-Signature": "sha256=" + "0" * 64},
    "stale",
    "other-body",
], ids=["unsigned", "wrong", "stale", "replayed-for-another-body"])
def test_a_webhook_without_a_valid_fresh_signature_is_refused(trig, headers):
    if headers == "stale":
        headers = signed("hook", BODY, ts=int(time.time()) - 3600)
    elif headers == "other-body":
        headers = signed("hook", {"issue": {"key": "OTHER"}})
    status, got = hook(trig, "hook", BODY, headers)
    assert status == 401 and not trig._test_started
    assert "session_id" not in got


def test_github_slack_stripe_and_token_signatures(trig):
    pr = {"number": 42}
    raw = json.dumps(pr).encode()
    assert hook(trig, "gh", pr, {"X-Hub-Signature-256": "sha256=" + _hex(SECRETS["gh"], raw),
                                 "X-GitHub-Delivery": "g1"})[0] == 202
    ev = {"type": "event_callback", "event_id": "E1", "event": {"text": "deploy failed"}}
    ts = str(int(time.time()))
    sig = "v0=" + _hex(SECRETS["slack"], f"v0:{ts}:".encode() + json.dumps(ev).encode())
    assert hook(trig, "slack", ev, {"X-Slack-Request-Timestamp": ts, "X-Slack-Signature": sig})[0] == 202
    challenge = {"type": "url_verification", "challenge": "abc"}
    sig = "v0=" + _hex(SECRETS["slack"], f"v0:{ts}:".encode() + json.dumps(challenge).encode())
    assert hook(trig, "slack", challenge, {"X-Slack-Request-Timestamp": ts,
                                           "X-Slack-Signature": sig}) == (200, {"challenge": "abc"})
    pay = {"id": "evt_1", "data": {"object": {"id": "pi_9"}}}
    sig = _hex(SECRETS["stripe"], f"{ts}.".encode() + json.dumps(pay).encode())
    assert hook(trig, "stripe", pay, {"Stripe-Signature": f"t={ts},v1=bad,v1={sig}"})[0] == 202
    assert hook(trig, "tok", {"a": 1}, {"X-AX-Token": SECRETS["tok"]})[0] == 202
    assert hook(trig, "tok", {"a": 1}, {"X-AX-Token": "nope"})[0] == 401
    assert [r["topic"] for r in trig._test_started] == ["PR 42", "deploy failed", "Payment pi_9", "x 1"]
    # A service trigger's run is its own, not the owner's.
    assert trig._test_started[-1]["owner"] == "trigger:tok"


def test_off_unknown_and_over_the_cap(trig):
    assert hook(trig, "off", BODY, {})[0] == 401          # no secret set: refused first
    trig._cache["secrets"][1]["off"] = "o" * 32
    ts = str(int(time.time()))
    raw = json.dumps(BODY)
    h = {"X-AX-Timestamp": ts, "X-AX-Signature": "sha256=" + _hex("o" * 32, f"{ts}.{raw}".encode())}
    assert hook(trig, "off", BODY, h) == (200, {"started": False, "reason": "the trigger is off"})
    assert hook(trig, "ghost", BODY, {})[0] == 404
    assert hook(trig, "nightly", BODY, {})[0] == 404      # not a webhook
    for i in range(4):
        _, got = hook(trig, "hook", BODY, signed("hook", BODY, **{"X-AX-Delivery": f"r{i}"}))
    assert got["started"] is False and "over 3 runs" in got["reason"]
    outcomes = [d["outcome"] for d in trig.deliveries("hook")]
    assert outcomes.count("started") == 3 and "throttled" in outcomes


def test_a_body_over_the_limit_is_refused(trig):
    big = {"x": "y" * (trig.MAX_BODY + 1)}
    assert hook(trig, "hook", big, signed("hook", big))[0] == 413


# --- the other deliveries ------------------------------------------------------------------

def test_schedule_eventbridge_and_s3(trig):
    got = trig.dispatch({"axTrigger": "nightly", "time": "2026-10-07T06:00:00Z", "id": "x1"}, trig._test_start)
    assert got["started"] and trig._test_started[0]["topic"] == "Report for 2026-10-07T06:00:00Z"
    assert trig._test_started[0]["gates"] == "auto"
    assert trig.dispatch({"axTrigger": "nightly", "id": "x1"}, trig._test_start)["duplicate"]
    ev = {"id": "e1", "source": "aws.cloudwatch", "region": "us-east-1", "detail": {"alarmName": "CPU"}}
    assert trig.dispatch({"axTrigger": "alarms", "event": ev}, trig._test_start)["started"]
    assert trig._test_started[1]["topic"] == "Investigate CPU in us-east-1"
    s3 = {"id": "e2", "detail": {"bucket": {"name": "acme"}, "object": {"key": "in/contract.pdf"}}}
    trig.dispatch({"axTrigger": "inbox", "event": s3}, trig._test_start)
    assert trig._test_started[2]["s3"] == "s3://acme/in/contract.pdf"
    assert trig.dispatch({"axTrigger": "ghost"}, trig._test_start)["reason"] == "unknown trigger"


def test_sqs_one_run_per_message_and_a_failure_goes_back(trig):
    calls = []

    def start(topic, owner, user, extra):
        calls.append(topic)
        if topic == "boom":
            raise RuntimeError("no")
        return "sid"
    event = {"Records": [
        {"eventSource": "aws:sqs", "eventSourceARN": "arn:aws:sqs:us-east-1:1:q", "messageId": "m1",
         "body": json.dumps({"text": "hello"})},
        {"eventSource": "aws:sqs", "eventSourceARN": "arn:aws:sqs:us-east-1:1:q", "messageId": "m2", "body": "boom"},
        {"eventSource": "aws:sqs", "eventSourceARN": "arn:aws:sqs:us-east-1:1:other", "messageId": "m3", "body": "x"}]}
    assert trig.is_trigger_event(event)
    got = trig.dispatch(event, start)
    assert calls == ["hello", "boom"]
    assert got == {"batchItemFailures": [{"itemIdentifier": "m2"}, {"itemIdentifier": "m3"}]}


def test_service_runs_are_seen_by_their_approvers_only(trig):
    assert trig.may_see(TRIGGERS["tok"], ["ops"]) and not trig.may_see(TRIGGERS["tok"], ["dev"])
    assert trig.run_identity("tok", TRIGGERS["tok"]) == ("trigger:tok", "trigger:tok")
    trig._cache.pop("owner")
    trig.OWNER_EMAIL = ""
    with pytest.raises(trig.TriggerError, match="runAs"):
        trig.run_identity("hook", TRIGGERS["hook"])


def test_a_set_secret_is_returned_once_and_stored(trig, monkeypatch):
    stored = {}

    class SM:
        def get_secret_value(self, SecretId):
            return {"SecretString": json.dumps(stored.get("v", {}))}

        def put_secret_value(self, SecretId, SecretString):
            stored["v"] = json.loads(SecretString)
    trig._clients["secretsmanager"] = SM()
    trig._cache.pop("secrets")
    made = trig.set_secret("hook")
    assert made.startswith("axwh_") and stored["v"] == {"hook": made}
    assert trig.set_secret("gh", "github-shared-secret-1234") == "github-shared-secret-1234"
    with pytest.raises(trig.TriggerError, match="only a webhook"):
        trig.set_secret("nightly")
    sig = trig.sign(made, '{"a": 1}', 100)
    assert sig["X-AX-Signature"] == "sha256=" + _hex(made, b'100.{"a": 1}')


# --- gates: in the runtime ------------------------------------------------------------------

def test_a_gate_decides_by_its_spec():
    from app.common import gates
    always, auto = gates.spec_of(True), gates.spec_of({"mode": "auto"})
    thr = gates.spec_of({"mode": "threshold", "when": [{"field": "risk", "gte": 80}, {"contains": "URGENT"}]})
    assert gates.auto_reason(always, {}, ['{"risk": 99}']) == ""
    assert gates.auto_reason(auto, {}, []) == "this gate approves itself"
    assert gates.auto_reason(always, {"gates": "auto"}, []).startswith("the trigger")
    assert gates.auto_reason(thr, {}, ['{"risk": 90}']) == ""
    assert gates.auto_reason(thr, {}, ['{"risk": 10} URGENT']) == ""
    assert gates.auto_reason(thr, {}, ['{"risk": 10}']).startswith("no rule called for a person")
    extra = gates.request_extra(gates.spec_of({"approval": "event", "timeout": {"after": "2h", "action": "deny"}}))
    assert extra["approval"] == "event" and extra["timeoutAction"] == "deny" and extra["timeoutAt"].endswith("Z")


@pytest.mark.parametrize("hitl, fragment", [
    ({"mode": "never"}, "hitl.mode"), ({"mode": "threshold"}, "needs `when`"),
    ({"when": [{"field": "x"}]}, "comparison"), ({"approval": "slack"}, "hitl.approval"),
    ({"timeout": {"after": "1w", "action": "deny"}}, "<n>m"), ({"timeout": {"after": "31d", "action": "deny"}}, "30d"),
    ({"colour": 1}, "unknown key"),
])
def test_a_malformed_gate_fails_at_start(hitl, fragment):
    from app.common import gates
    with pytest.raises(ValueError, match=fragment):
        gates.validate(hitl, "steps[0]")
    gates.validate(True, "steps[0]")
    gates.validate({"mode": "threshold", "when": [{"exists": True}], "timeout": {"after": "30m", "action": "approve"}},
                   "steps[0]")


def test_an_auto_gate_never_waits_and_says_so(monkeypatch):
    from app.orchestrator import nodes
    emitted = []

    async def emit(sid, ev):
        emitted.append(ev)
    monkeypatch.setattr(nodes, "emit", emit)
    monkeypatch.setattr(nodes, "is_cancelled", lambda sid: False)
    monkeypatch.setattr(nodes.gates, "audit_auto", lambda *a: None)
    monkeypatch.setattr(nodes, "interrupt", lambda payload: pytest.fail("an auto gate must not wait"))
    gate = nodes.make_gate_node("a", "A", {"mode": "auto"})
    got = asyncio.run(gate({"outputs": {"a": "{}"}}, {"configurable": {"thread_id": "s1"}}))
    assert got == {"decisions": {"a": "approve"}}
    assert any(e["type"] == "hitl_auto" and "approved automatically" in e["log"] for e in emitted)
    assert not any(e["type"] == "hitl_request" for e in emitted)
    group = nodes.make_group_gate_node("g", ["a", "b"], "G",
                                       {"mode": "threshold", "when": [{"field": "risk", "gt": 5}]})
    waited = []
    monkeypatch.setattr(nodes, "interrupt", lambda payload: waited.append(payload) or {"decision": "approve"})
    asyncio.run(group({"outputs": {"a": '{"risk": 1}', "b": '{"risk": 9}'}}, {"configurable": {"thread_id": "s1"}}))
    assert waited, "b's output matches the rule, so a person decides"


# --- gates: in the BFF ----------------------------------------------------------------------

class StatusTable:
    def __init__(self, items):
        self.items = {i["session_id"]: i for i in items}

    def get_item(self, Key, **kw):
        i = self.items.get(Key["session_id"])
        return {"Item": dict(i)} if i else {}

    def scan(self, **kw):
        return {"Items": list(self.items.values())}

    def update_item(self, Key, ConditionExpression=None, ExpressionAttributeValues=None, **kw):
        i = self.items[Key["session_id"]]
        if "timeoutAt" not in (i.get("hitl") or {}):
            raise type("ConditionalCheckFailedException", (Exception,), {})()
        del i["hitl"]["timeoutAt"]


@pytest.fixture()
def gates_bff(monkeypatch):
    monkeypatch.setenv("WORKFLOW_JSON", json.dumps(WORKFLOW))
    monkeypatch.setenv("APP_NAME", "ax_1")
    for mod in ("workflow", "gates"):
        sys.modules.pop(mod, None)
    g = importlib.import_module("gates")
    yield g
    for mod in ("workflow", "gates"):
        sys.modules.pop(mod, None)


def test_an_event_decides_only_a_waiting_gate_that_takes_events(gates_bff):
    table = StatusTable([{"session_id": "s1", "overall": "waiting_human", "hitl": {"node": "a"}},
                         {"session_id": "s2", "overall": "running"}])
    decided = []
    resume = lambda *a: decided.append(a)  # noqa: E731
    ev = lambda **d: {"axGate": "decision", "event": {"detail": {"app": "ax_1", **d}}}  # noqa: E731
    assert gates_bff.dispatch(ev(session="s1", gate="a", decision="approve", by="slack:ana"), table, resume)["decided"]
    assert decided == [("s1", "approve", "", "event:slack:ana")]
    assert not gates_bff.dispatch(ev(session="s2", decision="approve"), table, resume)["decided"]
    assert not gates_bff.dispatch(ev(session="s1", gate="other", decision="approve"), table, resume)["decided"]
    assert not gates_bff.dispatch(ev(session="s1", decision="maybe"), table, resume)["decided"]
    other_app = {"axGate": "decision", "event": {"detail": {"app": "ax_2", "session": "s1", "decision": "deny"}}}
    assert not gates_bff.dispatch(other_app, table, resume)["decided"]
    assert len(decided) == 1


def test_a_gate_in_the_app_ignores_events(gates_bff, monkeypatch):
    monkeypatch.setattr(gates_bff, "gate_spec", lambda node: {})
    table = StatusTable([{"session_id": "s1", "overall": "waiting_human", "hitl": {"node": "a"}}])
    got = gates_bff.decide({"app": "ax_1", "session": "s1", "decision": "approve"}, table, lambda *a: None)
    assert got == {"decided": False, "reason": "that gate does not take decisions by event"}


def test_a_timeout_decides_once(gates_bff):
    past, future = "2020-01-01T00:00:00Z", "2999-01-01T00:00:00Z"
    table = StatusTable([
        {"session_id": "late", "overall": "waiting_human",
         "hitl": {"node": "a", "timeoutAt": past, "timeoutAction": "deny"}},
        {"session_id": "early", "overall": "waiting_human",
         "hitl": {"node": "a", "timeoutAt": future, "timeoutAction": "deny"}}])
    decided = []
    resume = lambda *a: decided.append(a)  # noqa: E731
    assert gates_bff.dispatch({"axGate": "sweep"}, table, resume) == {"timedOut": ["late"]}
    assert decided[0][:2] == ("late", "deny") and decided[0][3] == "timeout"
    assert gates_bff.sweep(table, resume) == {"timedOut": []}


# --- through the BFF ------------------------------------------------------------------------

class RunTable:
    def __init__(self):
        self.items: dict = {}

    def put_item(self, Item, ConditionExpression=None):
        self.items[Item["session_id"]] = Item

    def get_item(self, Key, **kw):
        i = self.items.get(Key["session_id"])
        return {"Item": dict(i)} if i else {}

    def scan(self, **kw):
        return {"Items": list(self.items.values())}


@pytest.fixture()
def app(monkeypatch):
    wf = {**WORKFLOW, "authorization": {"groupsClaim": "cognito:groups", "actions": {"admin": ["admins"]}}}
    monkeypatch.setenv("WORKFLOW_JSON", json.dumps(wf))
    monkeypatch.setenv("STATUS_TABLE", "t-status")
    monkeypatch.setenv("EVENTS_TABLE", "t-events")
    monkeypatch.setenv("RUNTIME_ARN", "arn:aws:bedrock-agentcore:us-east-1:1:runtime/x")
    for mod in ("workflow", "authz", "chatbot", "handler", "triggers", "gates"):
        sys.modules.pop(mod, None)
    # bff/ first: another test may have put app/tools/pricing (its own handler.py) ahead.
    monkeypatch.syspath_prepend(str(BFF))
    h = importlib.import_module("handler")
    runs, invoked = RunTable(), []
    monkeypatch.setattr(h, "status_tbl", runs)
    monkeypatch.setattr(h, "_self_invoke", lambda fn, payload: invoked.append(payload))
    h.triggers._clients["table"] = Table()
    h.triggers._cache["secrets"] = (time.time() + 600, dict(SECRETS))
    h.triggers._cache["owner"] = ("owner-sub", "owner@example.com")
    h.triggers.OWNER_EMAIL, h.triggers.USER_POOL_ID = "owner@example.com", "pool"
    h._test = (runs, invoked)
    yield h
    for mod in ("workflow", "authz", "chatbot", "handler", "triggers", "gates"):
        sys.modules.pop(mod, None)


CTX = type("Ctx", (), {"function_name": "AgentCoreBFF-test"})()
BFF = __import__("pathlib").Path(__file__).resolve().parent.parent / "bff"


def api(h, method, route, path, *, params=None, body=None, headers=None, groups=None, raw=None):
    event = {"requestContext": {"http": {"method": method}, "domainName": "abc.execute-api.us-east-1.amazonaws.com"},
             "rawPath": path, "routeKey": f"{method} {route}", "pathParameters": params or {},
             "headers": headers or {}}
    if groups is not None:
        event["requestContext"]["authorizer"] = {"jwt": {"claims": {"email": "ana@example.com", "sub": "ana",
                                                                   "cognito:groups": groups}}}
    if raw is not None or body is not None:
        event["body"] = raw if raw is not None else json.dumps(body)
    r = h._api(event, CTX)
    return r["statusCode"], json.loads(r["body"])


def test_a_webhook_through_the_bff_creates_and_starts_the_run(app):
    runs, invoked = app._test
    raw = json.dumps(BODY)
    status, got = api(app, "POST", "/api/hooks/{name}", "/api/hooks/hook", params={"name": "hook"},
                      raw=raw, headers=signed("hook", BODY))
    assert status == 202 and got["started"]
    run = runs.items[got["session_id"]]
    assert run["owner"] == "owner-sub" and run["topic"].startswith("Triage OPS-7")
    assert run["trigger"]["name"] == "hook"
    assert invoked[-1]["action"] == "start" and invoked[-1]["topic"] == run["topic"]
    assert api(app, "POST", "/api/hooks/{name}", "/api/hooks/hook", params={"name": "hook"}, raw=raw)[0] == 401


def test_a_trigger_with_gates_auto_tells_the_runtime(app, monkeypatch):
    _, invoked = app._test
    got = app.handler({"axTrigger": "nightly", "id": "n1"}, CTX)
    assert got["started"] and invoked[-1]["gates"] == "auto"
    # ...and the runner hands it on to the runtime (it was dropped there once).
    sent = []
    monkeypatch.setattr(app.agentcore, "invoke_agent_runtime", lambda **kw: sent.append(json.loads(kw["payload"])))
    app.handler(invoked[-1], CTX)
    assert sent[-1]["action"] == "start" and sent[-1]["gates"] == "auto"


def test_the_triggers_page_is_for_admins(app):
    assert api(app, "GET", "/api/triggers", "/api/triggers", groups=["members"])[0] == 403
    status, got = api(app, "GET", "/api/triggers", "/api/triggers", groups=["admins"])
    hook_row = next(t for t in got["triggers"] if t["name"] == "hook")
    assert hook_row["url"] == "https://abc.execute-api.us-east-1.amazonaws.com/api/hooks/hook"
    assert hook_row["hasSecret"] and "secret" not in json.dumps(hook_row).replace("hasSecret", "")
    status, got = api(app, "POST", "/api/triggers/{name}/test", "/api/triggers/alarms/test",
                      params={"name": "alarms"}, groups=["admins"],
                      body={"event": {"region": "eu-west-1", "detail": {"alarmName": "Disk"}}})
    assert status == 200 and got["started"]
    assert app._test[0].items[got["session_id"]]["topic"] == "Investigate Disk in eu-west-1"


def test_a_service_runs_approvers_see_and_decide_it(app):
    runs, _ = app._test
    raw = json.dumps({"a": 1})
    _, got = api(app, "POST", "/api/hooks/{name}", "/api/hooks/tok", params={"name": "tok"}, raw=raw,
                 headers={"X-AX-Token": SECRETS["tok"]})
    sid = got["session_id"]
    assert runs.items[sid]["owner"] == "trigger:tok"
    app._own_session({"requestContext": {"authorizer": {"jwt": {"claims": {
        "sub": "x", "cognito:groups": ["ops"]}}}}}, sid)
    with pytest.raises(app.builds.BuildError):
        app._own_session({"requestContext": {"authorizer": {"jwt": {"claims": {
            "sub": "x", "cognito:groups": ["dev"]}}}}}, sid)


def test_an_event_decision_resumes_the_run_and_is_logged(app, monkeypatch):
    runs, invoked = app._test
    runs.items["s9"] = {"session_id": "s9", "overall": "waiting_human", "hitl": {"node": "a"},
                        "owner": "o1", "user": "u@example.com"}
    logged = []
    monkeypatch.setattr(app.audit, "enabled", lambda: True)
    monkeypatch.setattr(app.audit, "record", lambda *a, **k: logged.append((a, k)))
    monkeypatch.setenv("APP_NAME", "")
    got = app.handler({"axGate": "decision", "event": {"detail": {
        "session": "s9", "decision": "revise", "comment": "more detail", "by": "servicenow"}}}, CTX)
    assert got["decided"]
    assert invoked[-1] == {"action": "resume", "session_id": "s9", "decision": "revise", "comment": "more detail",
                           "decisions": None, "user": "u@example.com"}
    assert logged[0][0][2] == "run.decided" and logged[0][1]["by"] == "event:servicenow"
