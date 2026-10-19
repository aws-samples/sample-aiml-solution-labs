"""This workflow served over A2A to another team's agent (bff/a2a_server.py).

Driven through `handler._api` with real API Gateway event shapes, so the dispatch, the
ownership checks and the run start are the BFF's own code, not a copy. DynamoDB is a fake
that does what DynamoDB does where it matters here (a conditional put is checked, an update
is applied), and the runtime self-invoke is recorded rather than made. Nothing touches AWS.

The authorizer is API Gateway's job and cannot run here; what the IaC asks of it (its own
audience, the scope) is pinned at the bottom of this file and in cdk/test/a2a-server.test.ts.
What runs here is the BFF's second check of the same claims.

The round-trip test at the end points this deployment's own A2A client
(app/common/a2a_agent.py) at this server, with the wire bridged into the handler.
"""
from __future__ import annotations

import asyncio
import importlib
import io
import json
import re
import sys
import urllib.error
from pathlib import Path

import pytest
from conftest import workflow

ORCH = Path(__file__).resolve().parent.parent
BFF = ORCH / "bff"
DOMAIN = "a2a.agents.example"
CARD_ROUTE = "GET /api/a2a/.well-known/agent-card.json"
RPC_ROUTE = "POST /api/a2a"
CLIENT = "a2aclient123"
OWNER = f"a2a:{CLIENT}"
A2A_CLAIMS = {"client_id": CLIENT, "sub": CLIENT, "scope": "a2a/invoke", "token_use": "access"}
PERSON = {"email": "ana@example.com", "sub": "ana-sub", "aud": "spa-client", "token_use": "id"}

WORKFLOW = {
    "ui": {"title": "Claims Desk", "defaultTopic": "Assess claim 42"},
    "agents": {"intake": {"name": "Intake"}, "report": {"name": "Report"}},
    "steps": [{"agent": "intake"}, {"agent": "report"}],
    "authorization": {"groupsClaim": "cognito:groups", "actions": {"admin": ["admins"]}},
}
# limits, builds and audit too: limits raises builds.BuildError and a2a_server catches it, so
# both must hold the same module. Left cached by an earlier BFF test file (test_builds.py,
# test_liveconfig.py, test_run_shares.py), the refusal was another class here and escaped
# instead of becoming a JSON-RPC error; the default (alphabetical) order happened to hide it.
MODS = ("workflow", "authz", "chatbot", "handler", "triggers", "gates", "a2a_server", "limits", "builds", "audit")


class ConditionalCheckFailed(Exception):
    response = {"Error": {"Code": "ConditionalCheckFailedException"}}


class RunTable:
    """The status table: a put that respects attribute_not_exists, a get, the cancel's SET."""

    def __init__(self):
        self.items: dict = {}
        self.updates: list[dict] = []

    def put_item(self, Item, ConditionExpression=None):
        if ConditionExpression and Item["session_id"] in self.items:
            raise ConditionalCheckFailed()
        self.items[Item["session_id"]] = dict(Item)

    def get_item(self, Key, **kw):
        i = self.items.get(Key["session_id"])
        return {"Item": json.loads(json.dumps(i))} if i else {}

    def update_item(self, Key, UpdateExpression, ExpressionAttributeValues, **kw):
        self.updates.append({"Key": Key, "Update": UpdateExpression, "Values": ExpressionAttributeValues})
        i = self.items[Key["session_id"]]
        for attr, ph in re.findall(r"(\w+) = (:\w+)", UpdateExpression):
            i[attr] = ExpressionAttributeValues[ph]
        return {}

    def scan(self, FilterExpression=None, **kw):
        """Applies the owner filter the list route sends (`=` or `IN`), as DynamoDB would."""
        items = list(self.items.values())
        if FilterExpression is not None:
            e = FilterExpression.get_expression()
            allowed = [e["values"][1]] if e["operator"] == "=" else list(e["values"][1])
            items = [i for i in items if i.get("owner") in allowed]
        return {"Items": items}

    def query(self, **kw):
        return {"Items": []}


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setenv("WORKFLOW_JSON", json.dumps(WORKFLOW))
    monkeypatch.setenv("STATUS_TABLE", "t-status")
    monkeypatch.setenv("EVENTS_TABLE", "t-events")
    monkeypatch.setenv("RUNTIME_ARN", "arn:aws:bedrock-agentcore:us-east-1:1:runtime/x")
    monkeypatch.setenv("A2A_ENABLED", "true")
    monkeypatch.setenv("A2A_CLIENT_IDS", json.dumps([CLIENT]))
    monkeypatch.setenv("A2A_SCOPE", "a2a/invoke")
    monkeypatch.setenv("A2A_TOKEN_URL", "https://pool.auth.us-east-1.amazoncognito.com/oauth2/token")
    monkeypatch.setenv("A2A_APPROVERS", json.dumps(["reviewers"]))
    yield monkeypatch


def load(monkeypatch):
    for mod in MODS:
        sys.modules.pop(mod, None)
    # bff/ first: another test may have put app/tools/pricing (its own handler.py) ahead.
    monkeypatch.syspath_prepend(str(BFF))
    h = importlib.import_module("handler")
    runs, invoked, audited = RunTable(), [], []
    monkeypatch.setattr(h, "status_tbl", runs)
    monkeypatch.setattr(h, "events_tbl", RunTable())
    monkeypatch.setattr(h, "telemetry_tbl", None)
    monkeypatch.setattr(h, "_self_invoke", lambda fn, payload: invoked.append(payload))
    # The activity log, from the module the code under test holds.
    monkeypatch.setattr(h.audit, "enabled", lambda: True)
    monkeypatch.setattr(h.audit, "record", lambda *a, **kw: audited.append((a, kw)))
    h._t = {"runs": runs, "invoked": invoked, "audited": audited}
    return h


@pytest.fixture()
def h(env):
    handler = load(env)
    yield handler
    for mod in MODS:
        sys.modules.pop(mod, None)


CTX = type("Ctx", (), {"function_name": "AgentCoreBFF-test"})()


def event(route, *, body=None, raw=None, claims=None, params=None, path=None, b64=False, qs=None):
    method, route_path = route.split(" ", 1)
    ev = {"requestContext": {"http": {"method": method, "sourceIp": "203.0.113.9"}, "domainName": DOMAIN,
                             "authorizer": {"jwt": {"claims": A2A_CLAIMS if claims is None else claims}}},
          "rawPath": path or route_path, "routeKey": route, "pathParameters": params or {}, "headers": {}}
    if qs:
        ev["queryStringParameters"] = qs
    if raw is not None or body is not None:
        ev["body"] = raw if raw is not None else json.dumps(body)
        if b64:
            import base64
            ev["body"], ev["isBase64Encoded"] = base64.b64encode(ev["body"].encode()).decode(), True
    return ev


def call(h, route, **kw):
    r = h._api(event(route, **kw), CTX)
    return r["statusCode"], json.loads(r["body"])


def rpc(h, method, params=None, *, rpc_id="r1", claims=None):
    body = {"jsonrpc": "2.0", "id": rpc_id, "method": method}
    if params is not None:
        body["params"] = params
    return call(h, RPC_ROUTE, body=body, claims=claims)


def send(h, text="Assess claim 42", **message):
    return rpc(h, "message/send", {"message": {"role": "user", "messageId": "m1",
                                               "parts": [{"kind": "text", "text": text}], **message}})


def started(h) -> dict:
    status, got = send(h)
    assert status == 200 and "result" in got, got
    return h._t["runs"].items[got["result"]["id"]]


# --- the Agent Card ---------------------------------------------------------------------

def test_the_card_describes_the_deployed_workflow_as_one_skill(h):
    status, card = call(h, CARD_ROUTE)
    assert status == 200
    assert card["protocolVersion"] == "0.3.0"
    assert card["name"] == "Claims Desk"
    assert card["url"] == f"https://{DOMAIN}/api/a2a"
    assert card["preferredTransport"] == "JSONRPC"
    assert card["additionalInterfaces"] == [{"transport": "JSONRPC", "url": card["url"]}]
    assert card["supportedInterfaces"] == card["additionalInterfaces"]
    assert card["defaultInputModes"] == ["text/plain"] and card["defaultOutputModes"] == ["text/plain"]
    assert card["capabilities"]["streaming"] is False and card["capabilities"]["pushNotifications"] is False
    [skill] = card["skills"]
    assert skill["id"] == "run-workflow" and skill["examples"] == ["Assess claim 42"]
    assert "Intake" in skill["description"] and "Report" in skill["description"]
    flow = card["securitySchemes"]["oauth2"]["flows"]["clientCredentials"]
    assert flow["tokenUrl"] == "https://pool.auth.us-east-1.amazoncognito.com/oauth2/token"
    assert list(flow["scopes"]) == ["a2a/invoke"]
    assert card["security"] == [{"oauth2": ["a2a/invoke"]}]


def test_the_card_tells_a_caller_to_bring_a_bearer_token_when_no_token_url_is_known(env):
    env.setenv("A2A_TOKEN_URL", "")
    h = load(env)
    _, card = call(h, CARD_ROUTE)
    assert card["securitySchemes"]["oauth2"]["type"] == "http"
    assert card["securitySchemes"]["oauth2"]["scheme"] == "bearer"


# --- message/send -----------------------------------------------------------------------

def test_a_message_starts_a_run_owned_by_the_client_and_returns_a_task_without_waiting(h):
    status, got = send(h, "Assess claim 42", contextId="ctx-7")
    assert status == 200 and got["id"] == "r1" and got["jsonrpc"] == "2.0"
    task = got["result"]
    assert task["kind"] == "task" and task["status"] == {"state": "submitted"}
    assert re.fullmatch(r"[0-9a-f]{12}", task["id"]) and task["contextId"] == "ctx-7"
    item = h._t["runs"].items[task["id"]]
    assert item["owner"] == OWNER and item["user"] == OWNER
    assert item["topic"] == "Assess claim 42"
    assert item["a2a"] == {"client": CLIENT, "contextId": "ctx-7"}
    [payload] = h._t["invoked"]
    assert payload["action"] == "start" and payload["session_id"] == task["id"]
    assert payload["user"] == OWNER
    # Its gates wait for a person: nothing asks the runtime to approve them itself.
    assert "gates" not in payload
    [(args, detail)] = [a for a in h._t["audited"] if a[0][2] == "run.started"]
    assert args[0] == OWNER and detail["via"] == "a2a" and detail["ip"] == "203.0.113.9"


def test_without_a_context_id_the_task_is_its_own_context(h):
    _, got = send(h)
    assert got["result"]["contextId"] == got["result"]["id"]


def test_a_data_part_is_read_as_json_text(h):
    _, got = rpc(h, "message/send", {"message": {"role": "user", "parts": [
        {"kind": "text", "text": "Assess"}, {"kind": "data", "data": {"claim": 42}}]}})
    assert h._t["runs"].items[got["result"]["id"]]["topic"] == 'Assess\n{"claim": 42}'


def test_the_body_may_arrive_base64_encoded(h):
    body = {"jsonrpc": "2.0", "id": 1, "method": "message/send",
            "params": {"message": {"role": "user", "parts": [{"kind": "text", "text": "hi"}]}}}
    status, got = call(h, RPC_ROUTE, body=body, b64=True)
    assert status == 200 and got["result"]["status"]["state"] == "submitted"


# --- tasks/get: every run status ---------------------------------------------------------

@pytest.mark.parametrize("overall, nodes, state", [
    ("running", {"intake": {"status": "pending"}, "report": {"status": "pending"}}, "submitted"),
    ("running", {"intake": {"status": "running"}, "report": {"status": "pending"}}, "working"),
    ("waiting_human", {}, "input-required"),
    ("done", {}, "completed"),
    ("denied", {}, "rejected"),
    ("failed", {}, "failed"),
    ("cancelled", {}, "canceled"),
    ("archived-by-a-future-version", {}, "unknown"),
    (None, {}, "unknown"),
])
def test_every_run_status_maps_to_a_task_state(h, overall, nodes, state):
    item = started(h)
    item.update(overall=overall, nodes=nodes)
    status, got = rpc(h, "tasks/get", {"id": item["session_id"]})
    assert status == 200 and got["result"]["status"]["state"] == state
    assert got["result"]["id"] == item["session_id"] and got["result"]["kind"] == "task"


def test_a_completed_task_carries_the_runs_result_as_its_artifact(h):
    item = started(h)
    item.update(overall="done", result='{"summary": "approve the claim"}')
    _, got = rpc(h, "tasks/get", {"id": item["session_id"]})
    [artifact] = got["result"]["artifacts"]
    assert artifact["parts"] == [{"kind": "text", "text": '{"summary": "approve the claim"}'}]


def test_a_task_at_a_gate_says_a_person_decides_it_in_the_console(h):
    item = started(h)
    item.update(overall="waiting_human", hitl={"node": "intake", "question": "ok?"})
    _, got = rpc(h, "tasks/get", {"id": item["session_id"]})
    status = got["result"]["status"]
    assert status["state"] == "input-required"
    text = status["message"]["parts"][0]["text"]
    assert "person" in text and "console" in text and "intake" in text
    assert "artifacts" not in got["result"]


def test_a_denied_run_says_why(h):
    item = started(h)
    item.update(overall="denied", result="Denied by human. Workflow halted.")
    _, got = rpc(h, "tasks/get", {"id": item["session_id"]})
    assert got["result"]["status"]["message"]["parts"][0]["text"] == "Denied by human. Workflow halted."


def test_another_clients_run_and_a_persons_run_are_not_found(h):
    item = started(h)
    h._t["runs"].items["bbbbbbbbbbbb"] = {"session_id": "bbbbbbbbbbbb", "owner": "ana-sub", "overall": "done"}
    item["owner"] = "a2a:someoneelse"
    for tid in (item["session_id"], "bbbbbbbbbbbb", "cccccccccccc"):
        _, got = rpc(h, "tasks/get", {"id": tid})
        assert got["error"]["code"] == -32001, tid


@pytest.mark.parametrize("params", [{}, {"id": 12}, {"id": "../x"}, {"id": "A" * 12}])
def test_a_malformed_task_id_is_invalid_params(h, params):
    _, got = rpc(h, "tasks/get", params)
    assert got["error"]["code"] == -32602


# --- tasks/cancel -----------------------------------------------------------------------

def test_cancel_stops_a_running_task_the_way_the_console_does(h):
    item = started(h)
    _, got = rpc(h, "tasks/cancel", {"id": item["session_id"]})
    assert got["result"]["status"]["state"] == "canceled"
    assert item["cancel_requested"] is True and item["overall"] == "cancelled" and item["hitl"] is None
    assert any(a[0][2] == "run.cancelled" and a[1]["via"] == "a2a" for a in h._t["audited"])


@pytest.mark.parametrize("overall", ["done", "denied", "failed", "cancelled"])
def test_a_finished_task_cannot_be_canceled(h, overall):
    item = started(h)
    item["overall"] = overall
    _, got = rpc(h, "tasks/cancel", {"id": item["session_id"]})
    assert got["error"]["code"] == -32002
    assert h._t["runs"].updates == []


def test_cancel_of_someone_elses_task_is_not_found_and_changes_nothing(h):
    item = started(h)
    item["owner"] = "a2a:someoneelse"
    _, got = rpc(h, "tasks/cancel", {"id": item["session_id"]})
    assert got["error"]["code"] == -32001 and item["overall"] == "running"


# --- JSON-RPC errors --------------------------------------------------------------------

@pytest.mark.parametrize("raw, code", [
    ("{not json", -32700),
    ("[]", -32600),
    ('[{"jsonrpc": "2.0", "id": 1, "method": "tasks/get"}]', -32600),
    ('{"jsonrpc": "1.0", "id": 1, "method": "tasks/get", "params": {}}', -32600),
    ('{"jsonrpc": "2.0", "id": 1, "method": "tasks/resubscribe", "params": {}}', -32601),
    ('{"jsonrpc": "2.0", "id": 1, "method": "agent/getAuthenticatedExtendedCard"}', -32601),
    ('{"jsonrpc": "2.0", "id": 1, "method": "tasks/get", "params": []}', -32602),
    ('{"jsonrpc": "2.0", "id": 1, "method": "message/send", "params": {}}', -32602),
])
def test_a_bad_request_is_a_jsonrpc_error_over_http_200(h, raw, code):
    status, got = call(h, RPC_ROUTE, raw=raw)
    assert status == 200 and got["jsonrpc"] == "2.0" and got["error"]["code"] == code
    assert h._t["invoked"] == []


def test_the_error_answers_the_request_id(h):
    _, got = rpc(h, "nope", {}, rpc_id=7)
    assert got["id"] == 7 and got["error"]["code"] == -32601


@pytest.mark.parametrize("message, code", [
    ({"role": "user", "parts": []}, -32602),
    ({"role": "user", "parts": [{"kind": "text", "text": "   "}]}, -32602),
    ({"role": "agent", "parts": [{"kind": "text", "text": "hi"}]}, -32602),
    ({"role": "user", "parts": ["hi"]}, -32602),
    ({"role": "user", "parts": [{"kind": "file", "file": {"uri": "https://x.example/a.pdf"}}]}, -32005),
    ({"role": "user", "taskId": "aaaaaaaaaaaa", "parts": [{"kind": "text", "text": "approve"}]}, -32004),
    ({"role": "user", "contextId": "bad context!", "parts": [{"kind": "text", "text": "hi"}]}, -32602),
])
def test_a_message_this_agent_cannot_take_is_refused_before_a_run_starts(h, message, code):
    _, got = rpc(h, "message/send", {"message": message})
    assert got["error"]["code"] == code
    assert h._t["runs"].items == {} and h._t["invoked"] == []


def test_the_body_is_capped(h):
    big = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "message/send",
                      "params": {"message": {"role": "user", "parts": [{"kind": "text", "text": "x" * 70000}]}}})
    _, got = call(h, RPC_ROUTE, raw=big)
    assert got["error"]["code"] == -32600 and "KB" in got["error"]["message"]
    assert h._t["runs"].items == {}


def test_the_request_is_held_to_what_the_start_run_box_accepts(h):
    assert h.a2a_server.TOPIC_MAX == h.TOPIC_MAX
    _, got = send(h, "x" * (h.TOPIC_MAX + 1))
    assert got["error"]["code"] == -32602 and h._t["runs"].items == {}
    _, ok = send(h, "x" * h.TOPIC_MAX)
    assert "result" in ok


# --- orchestrator.limits ----------------------------------------------------------------

class Counters:
    """The telemetry table's run counter: an ADD guarded by `runs < :max`, as one step."""

    def __init__(self):
        self.rows: dict = {}

    def get_item(self, Key, **kw):
        return {}

    def update_item(self, Key, ConditionExpression=None, ExpressionAttributeValues=None, **kw):
        k = Key["session_id"]
        if k in self.rows and self.rows[k] >= ExpressionAttributeValues[":max"]:
            raise ConditionalCheckFailed()
        self.rows[k] = self.rows.get(k, 0) + 1
        return {}


def test_a_limits_refusal_is_a_jsonrpc_error_and_starts_nothing(env):
    env.setenv("WORKFLOW_JSON", json.dumps({**WORKFLOW, "orchestrator": {"limits": {"userRunsPerHour": 1}}}))
    h = load(env)
    counters = Counters()
    env.setattr(h, "telemetry_tbl", counters)
    _, first = send(h)
    assert "result" in first
    _, second = send(h)
    assert second["error"]["code"] == -32010
    assert second["error"]["data"] == {"httpStatus": 429}
    assert "runs this hour" in second["error"]["message"]
    assert len(h._t["runs"].items) == 1 and len(h._t["invoked"]) == 1
    # Counted under the client's name, not a person's and not "(anonymous)".
    assert [k for k in counters.rows] and all(OWNER in k for k in counters.rows)


# --- who may call -----------------------------------------------------------------------

def test_off_means_both_routes_answer_404(env):
    env.delenv("A2A_ENABLED")
    h = load(env)
    assert call(h, CARD_ROUTE)[0] == 404
    status, _ = call(h, RPC_ROUTE, body={"jsonrpc": "2.0", "id": 1, "method": "message/send", "params": {}})
    assert status == 404 and h._t["invoked"] == []


def test_enabled_with_no_client_ids_refuses_everything(env):
    env.setenv("A2A_CLIENT_IDS", "[]")
    h = load(env)
    assert call(h, CARD_ROUTE)[0] == 404


@pytest.mark.parametrize("claims", [
    PERSON,
    {**PERSON, "client_id": "spa-client", "scope": "openid email"},
    {**A2A_CLAIMS, "scope": "openid"},
    {**A2A_CLAIMS, "scope": ""},
    {**A2A_CLAIMS, "client_id": "another-client"},
    {},
])
def test_only_an_a2a_client_with_the_scope_is_served(h, claims):
    assert call(h, CARD_ROUTE, claims=claims)[0] == 403
    status, _ = rpc(h, "message/send", {"message": {"role": "user", "parts": [{"kind": "text", "text": "x"}]}},
                    claims=claims)
    assert status == 403 and h._t["invoked"] == []


def test_an_array_scope_claim_as_api_gateway_renders_it_is_read(h):
    assert call(h, CARD_ROUTE, claims={**A2A_CLAIMS, "scope": "[openid a2a/invoke]"})[0] == 200


@pytest.mark.parametrize("route, params", [
    ("GET /api/sessions", None), ("POST /api/sessions", None), ("GET /api/me", None),
    ("GET /api/workflow", None), ("GET /api/sessions/{id}", {"id": "aaaaaaaaaaaa"}),
    ("POST /api/sessions/{id}/decision", {"id": "aaaaaaaaaaaa"}),
])
def test_an_a2a_clients_token_is_refused_on_every_other_route(h, route, params):
    path = route.split(" ", 1)[1].replace("{id}", (params or {}).get("id", ""))
    status, got = call(h, route, params=params, path=path, body={"topic": "x", "decision": "approve"})
    assert status == 403 and "/api/a2a" in got["error"]
    assert h._t["invoked"] == [] and h._t["runs"].items == {}


def test_a_person_still_reaches_the_other_routes(h):
    assert call(h, "GET /api/me", claims=PERSON)[0] == 200


# --- who decides an A2A run's gates -----------------------------------------------------

def test_an_approver_reads_lists_and_decides_an_a2a_run(h):
    item = started(h)
    item.update(overall="waiting_human", hitl={"node": "intake"})
    sid = item["session_id"]
    approver = {**PERSON, "cognito:groups": ["reviewers"]}
    status, got = call(h, "GET /api/sessions/{id}", params={"id": sid}, path=f"/api/sessions/{sid}", claims=approver)
    assert status == 200 and got["session_id"] == sid
    status, listed = call(h, "GET /api/sessions", claims=approver, qs={"scope": "triggers"})
    assert status == 200 and [r["session_id"] for r in listed] == [sid]
    status, _ = call(h, "POST /api/sessions/{id}/decision", params={"id": sid}, path=f"/api/sessions/{sid}/decision",
                     claims=approver, body={"decision": "approve"})
    assert status == 200
    assert h._t["invoked"][-1]["action"] == "resume" and h._t["invoked"][-1]["decision"] == "approve"


def test_an_approver_sees_a2a_runs_beside_their_own_on_the_runs_page(h):
    sid = started(h)["session_id"]
    h._t["runs"].items["dddddddddddd"] = {"session_id": "dddddddddddd", "owner": "ana-sub", "created": "1"}
    h._t["runs"].items["eeeeeeeeeeee"] = {"session_id": "eeeeeeeeeeee", "owner": "bob-sub", "created": "2"}
    _, listed = call(h, "GET /api/sessions", claims={**PERSON, "cognito:groups": ["reviewers"]})
    assert sorted(r["session_id"] for r in listed) == sorted([sid, "dddddddddddd"])
    _, listed = call(h, "GET /api/sessions", claims=PERSON)
    assert [r["session_id"] for r in listed] == ["dddddddddddd"]


def test_anyone_else_cannot_see_or_decide_an_a2a_run(h):
    item = started(h)
    sid = item["session_id"]
    for claims in (PERSON, {**PERSON, "cognito:groups": ["admins-of-something-else"]}):
        status, _ = call(h, "GET /api/sessions/{id}", params={"id": sid}, path=f"/api/sessions/{sid}", claims=claims)
        assert status == 404
        status, _ = call(h, "POST /api/sessions/{id}/decision", params={"id": sid},
                         path=f"/api/sessions/{sid}/decision", claims=claims, body={"decision": "approve"})
        assert status == 404
    assert [p["action"] for p in h._t["invoked"]] == ["start"]


def test_with_no_approver_groups_nobody_sees_a2a_runs(env):
    env.setenv("A2A_APPROVERS", "[]")
    h = load(env)
    assert h.a2a_server.owners_for(["reviewers"]) == []


# --- our own A2A client, against this server --------------------------------------------

class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class Bridge:
    """urllib.request.urlopen, answered by the BFF handler. Plays API Gateway's part: a
    request without the expected bearer token is a 401 and never reaches the handler; one
    with it carries the A2A client's claims."""

    def __init__(self, h, on_poll=None):
        self.h, self.on_poll, self.calls = h, on_poll, []

    def __call__(self, request, timeout=None):
        url = request.full_url
        headers = {k.lower(): v for k, v in request.header_items()}
        if headers.get("authorization") != "Bearer tok-a2a":
            raise urllib.error.HTTPError(url, 401, "Unauthorized", {}, io.BytesIO(b'{"message":"Unauthorized"}'))
        route = CARD_ROUTE if url.endswith("/.well-known/agent-card.json") else RPC_ROUTE
        body = request.data.decode() if request.data else None
        method = json.loads(body)["method"] if body else "card"
        self.calls.append(method)
        if method == "tasks/get" and self.on_poll:
            self.on_poll(self.h)
        r = self.h._api(event(route, raw=body), CTX)
        if r["statusCode"] != 200:
            raise urllib.error.HTTPError(url, r["statusCode"], "error", {}, io.BytesIO(r["body"].encode()))
        return _Resp(r["body"].encode())


REMOTE = {
    "orchestrator": {"a2aInvoke": {"timeoutSeconds": 5, "pollIntervalSeconds": 0, "maxPollSeconds": 5}},
    "tools": {},
    # oauth2: the caller's outbound identity mints a client-credentials token, which is how
    # a deployment calls another one's A2A endpoint.
    "agents": {"partner": {"name": "Claims Desk", "runtime": "a2a", "auth": "oauth2",
                           "agentcore": {"identity": {"outbound": ["claims-desk-a2a"]}},
                           "agentCard": f"https://{DOMAIN}/api/a2a"}},
    "steps": [{"agent": "partner"}],
}


class ClientCtx:
    topic = "Assess claim 42"
    session_id = "caller-session"
    agent_id = "partner"
    feedback = ""

    def __init__(self):
        self.state, self.logs = {"outputs": {}}, []

    def input(self, agent_id):
        return None

    async def log(self, msg):
        self.logs.append(msg)

    async def get_identity_token(self, provider=""):
        return "tok-a2a"


def finish(h):
    for item in h._t["runs"].items.values():
        item.update(overall="done", result='{"summary": "approve the claim"}')


def run_client(h, on_poll):
    bridge = Bridge(h, on_poll)
    with workflow(REMOTE) as imp:
        mod = imp("app.common.a2a_agent")
        # The client's one seam, in front of the SSRF guard (tests/test_a2a.py does the same);
        # the guard itself is tests/test_netguard.py's.
        mod._open = lambda request, timeout: bridge(request, timeout=timeout)
        mod._ALLOW_PRIVATE = True   # no DNS lookup of the test host
        agent = imp("app.orchestrator.registry").load_agents()["partner"]
        errors = imp("app.common.errors")
        try:
            return asyncio.run(agent.run(ClientCtx())), bridge, None
        except errors.RemoteAgentUnavailable as e:
            return None, bridge, e


def test_this_deployments_own_a2a_client_runs_the_workflow_end_to_end(h):
    text, bridge, error = run_client(h, finish)
    assert error is None, error
    assert text == '{"summary": "approve the claim"}'
    assert bridge.calls[:2] == ["card", "message/send"] and "tasks/get" in bridge.calls
    [item] = h._t["runs"].items.values()
    assert item["owner"] == OWNER and item["topic"].startswith('{"request": "Assess claim 42"')
    assert item["a2a"]["contextId"] == "caller-session-partner"


def test_our_client_stops_at_a_gate_instead_of_polling_its_whole_budget(h):
    def at_gate(h):
        for item in h._t["runs"].items.values():
            item.update(overall="waiting_human", hitl={"node": "intake"})
    _, bridge, error = run_client(h, at_gate)
    assert error is not None and "input-required" in str(error)
    assert bridge.calls.count("tasks/get") == 1


def test_our_client_without_the_token_never_reaches_the_handler(h, monkeypatch):
    monkeypatch.setattr(ClientCtx, "get_identity_token", lambda self, provider="": _none())
    _, bridge, error = run_client(h, finish)
    assert error is not None and "401" in str(error)
    assert bridge.calls == [] and h._t["runs"].items == {}


async def _none():
    return ""


# --- the IaC: off by default, its own authorizer, the scope, both planes ------------------

TF = ORCH / "terraform"
CDK = ORCH / "cdk"


def _hcl_block(src: str, header: str) -> str:
    start = src.index(header)
    depth, i = 0, src.index("{", start)
    for j in range(i, len(src)):
        depth += {"{": 1, "}": -1}.get(src[j], 0)
        if depth == 0:
            return src[start:j + 1]
    raise AssertionError(header)


def test_the_three_route_lists_agree():
    sys.path.insert(0, str(BFF))
    server = importlib.import_module("a2a_server")
    tf = (TF / "a2a_server.tf").read_text()
    tf_routes = re.findall(r'^\s*"((?:GET|POST) /api/a2a[^"]*)",?$', _hcl_block(tf, "a2a_routes = ["), re.M)
    ts = (CDK / "lib" / "a2a-server.ts").read_text()
    cdk_routes = json.loads(re.search(r"export const A2A_ROUTES = (\[[^\]]*\]);", ts).group(1))
    assert tf_routes == cdk_routes == list(server.ROUTES)


def test_the_main_route_lists_do_not_carry_the_a2a_routes():
    # They are added only when exposed, by their own resource with their own authorizer.
    assert "/api/a2a" not in (TF / "bff.tf").read_text()
    assert "/api/a2a" not in (CDK / "lib" / "orchestrator-stack.ts").read_text()


def test_terraform_creates_nothing_unless_exposed():
    tf = (TF / "a2a_server.tf").read_text()
    variables = (TF / "variables.tf").read_text()
    assert re.search(r'variable "expose_a2a" \{[^}]*type\s*=\s*bool\s*default\s*=\s*false', variables, re.S)
    for header in ('resource "aws_cognito_resource_server" "a2a"', 'resource "aws_cognito_user_pool_client" "a2a"'):
        assert re.search(r"count\s*=\s*local\.a2a_create \? 1 : 0", _hcl_block(tf, header)), header
    assert re.search(r"local\.a2a_create\s*=|a2a_create\s*=\s*var\.expose_a2a &&", tf)
    assert re.search(r"count\s*=\s*var\.expose_a2a \? 1 : 0", _hcl_block(tf, 'resource "aws_apigatewayv2_authorizer" "a2a"'))
    route = _hcl_block(tf, 'resource "aws_apigatewayv2_route" "a2a"')
    assert "toset(var.expose_a2a ? local.a2a_routes : [])" in route
    assert re.search(r'a2a_bff_env = !var\.expose_a2a \? \{\} :', tf)
    assert "local.a2a_bff_env" in (TF / "bff.tf").read_text()
    assert "expose_a2a" not in (TF / "terraform.tfvars.example").read_text()


def test_terraform_puts_the_a2a_routes_behind_their_own_authorizer_and_the_scope():
    tf = (TF / "a2a_server.tf").read_text()
    route = _hcl_block(tf, 'resource "aws_apigatewayv2_route" "a2a"')
    assert re.search(r'authorization_type\s*=\s*"JWT"', route)
    assert re.search(r"authorizer_id\s*=\s*aws_apigatewayv2_authorizer\.a2a\[0\]\.id", route)
    assert re.search(r"authorization_scopes\s*=\s*\[local\.a2a_scope\]", route)
    authorizer = _hcl_block(tf, 'resource "aws_apigatewayv2_authorizer" "a2a"')
    assert re.search(r"audience\s*=\s*\[local\.a2a_audience\]", authorizer)
    assert "aws_cognito_user_pool_client.a2a[0].id" in tf.split("a2a_audience =")[1].split("\n  a2a_client_ids")[0]
    # The people's authorizer still has only the sign-in client as its audience.
    main = _hcl_block((TF / "bff.tf").read_text(), 'resource "aws_apigatewayv2_authorizer" "jwt"')
    assert re.search(r"audience\s*=\s*\[local\.jwt_audience\]", main)
    client = _hcl_block(tf, 'resource "aws_cognito_user_pool_client" "a2a"')
    assert '["client_credentials"]' in client and "generate_secret                      = true" in client
    assert "aws_cognito_resource_server.a2a[0].scope_identifiers" in client


def test_terraform_never_outputs_or_passes_on_the_client_secret():
    tf = (TF / "a2a_server.tf").read_text()
    assert "client_secret" not in tf


def test_terraform_fails_the_plan_rather_than_deploy_an_open_or_uncallable_endpoint():
    authorizer = _hcl_block((TF / "a2a_server.tf").read_text(), 'resource "aws_apigatewayv2_authorizer" "a2a"')
    for condition in ("local.auth_enabled", "local.workflow_plane", "local.a2a_configured",
                      "var.a2a_identity.audience != local.jwt_audience"):
        assert condition in authorizer, condition
