"""Live config (orchestrator.liveConfig): an agent's prompt and model settings changed on a
running deployment, pinned per run (bff/liveconfig.py, app/common/liveconfig.py).

What has to hold:
  * off (the default): no table read at all when a run starts, routes 404, no IAM statement;
  * a run records the version active when it started, and EVERY step of it, after a gate
    resume on another container and in a dedicated runtime, uses that version even after the
    pointer has moved;
  * an unreadable table starts the run on version 0, with the log line, and does not fail it;
  * only the six settings change: tools, gates and the rest are refused by name; a model the
    runtimes' IAM grant does not cover is refused; the five settings are held to the same rules
    as workflow.json;
  * only admins reach the routes, and each change is in the activity log with before/after;
  * the BFF's grant is held to the _liveconfig# rows, the same on Terraform and CDK.

DynamoDB is moto's, because the conditional writes are what a hand-rolled fake gets wrong.
"""
from __future__ import annotations

import asyncio
import importlib
import io
import json
import re
import sys
import types

import boto3
import pytest
from conftest import ORCH_ROOT, workflow

moto = pytest.importorskip("moto")

from test_builds import CTX, FakeCodeBuild, _events_table, _status_table, create_builds_table  # noqa: E402 - after the moto importorskip

TF = ORCH_ROOT / "terraform"
STACK = (ORCH_ROOT / "cdk" / "lib" / "orchestrator-stack.ts").read_text()
ACCOUNT = "123456789012"
MODEL = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
RULES = {"groupsClaim": "cognito:groups", "actions": {"admin": ["admins"], "audit": ["admins"]}}


def _wf(on: bool = True) -> dict:
    return {"orchestrator": {"liveConfig": on},
            "agents": {"a": {"name": "A", "maxTokens": 1000},
                       "b": {"name": "B", "runtime": "dedicated", "maxTokens": 2000, "model": MODEL},
                       "r": {"name": "R", "runtime": "a2a", "agentCard": "https://agent.example.com"}},
            "steps": [{"agent": "a", "hitl": True}, {"agent": "b"}, {"agent": "r"}],
            "authorization": RULES}


MODULES = ("workflow", "authz", "chatbot", "handler", "builds", "buildstore", "accounts",
           "designer", "policies", "audit", "liveconfig")


def _telemetry_table(ddb):
    ddb.create_table(TableName="console_telemetry", BillingMode="PAY_PER_REQUEST",
                     KeySchema=[{"AttributeName": "session_id", "KeyType": "HASH"},
                                {"AttributeName": "sk", "KeyType": "RANGE"}],
                     AttributeDefinitions=[{"AttributeName": a, "AttributeType": "S"}
                                           for a in ("session_id", "sk")])
    return boto3.resource("dynamodb").Table("console_telemetry")


def _make_env(monkeypatch, wf: dict):
    for k, v in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing",
                 "AWS_DEFAULT_REGION": "us-east-1", "AWS_REGION": "us-east-1",
                 "WORKFLOW_JSON": json.dumps(wf), "TELEMETRY_TABLE": "console_telemetry",
                 "STATUS_TABLE": "console_status", "EVENTS_TABLE": "console_events",
                 "RUNTIME_ARN": "arn:aws:bedrock-agentcore:us-east-1:1:runtime/console",
                 "BUILDS_TABLE": "console_builds", "BUILDS_BUCKET": "console-builds",
                 "DEPLOY_PROJECT": "agentexpress-deploy",
                 "SECRETS_PREFIX": "agentexpress/console/builds/",
                 "DEPLOY_ROLE_ARN": f"arn:aws:iam::{ACCOUNT}:role/console-deploy",
                 "BFF_ROLE_ARN": f"arn:aws:iam::{ACCOUNT}:role/console-bff"}.items():
        monkeypatch.setenv(k, v)


@pytest.fixture()
def make(monkeypatch):
    """make(wf) -> an env whose BFF serves `wf`; one moto account for the test."""
    with moto.mock_aws():
        ddb = boto3.client("dynamodb")
        create_builds_table(ddb)
        _status_table(ddb, "console_status")
        _events_table(ddb, "console_events")
        tel = _telemetry_table(ddb)
        boto3.client("s3").create_bucket(Bucket="console-builds")

        def build(wf: dict):
            _make_env(monkeypatch, wf)
            for mod in MODULES:
                sys.modules.pop(mod, None)
            handler = importlib.import_module("handler")
            monkeypatch.setattr(handler.builds, "_codebuild", FakeCodeBuild())
            invoked: list[dict] = []
            monkeypatch.setattr(handler, "_self_invoke", lambda fn, p: invoked.append(p))
            # No Bedrock in a test: the model list is empty, so no capability is judged.
            monkeypatch.setattr(handler, "_models", lambda: {"models": []})
            handler.liveconfig._pointer_cache.update(at=0.0, value=None)
            handler.liveconfig._version_cache.clear()
            return types.SimpleNamespace(handler=handler, lc=handler.liveconfig, invoked=invoked, tel=tel,
                                         status=boto3.resource("dynamodb").Table("console_status"))
        yield build
    for mod in MODULES:
        sys.modules.pop(mod, None)


def call(e, route_key, *, params=None, body=None, sub="admin1", groups=("admins",), path=None):
    method, template = route_key.split(" ", 1)
    claims = {"sub": sub, "email": f"{sub}@example.com", "cognito:groups": list(groups)}
    raw = path or template
    for k, v in (params or {}).items():
        raw = raw.replace("{" + k + "}", str(v))
    event = {"requestContext": {"http": {"method": method}, "accountId": ACCOUNT,
                                "authorizer": {"jwt": {"claims": claims}}},
             "rawPath": raw, "routeKey": route_key, "pathParameters": params or {}}
    if body is not None:
        event["body"] = json.dumps(body)
    resp = e.handler.handler(event, CTX)
    return resp["statusCode"], json.loads(resp["body"])


def _start(e, sub="user1"):
    status, body = call(e, "POST /api/sessions", body={"topic": "t"}, sub=sub, groups=())
    assert status == 200, body
    sid = body["session_id"]
    return sid, e.status.get_item(Key={"session_id": sid})["Item"], e.invoked[-1]


def _save(e, agents, note="n"):
    return call(e, "POST /api/liveconfig/versions", body={"agents": agents, "note": note})


def _activate(e, n, frm=None):
    return call(e, "PUT /api/liveconfig/active", body={"version": n, **({"from": frm} if frm is not None else {})})


class Spy:
    """A table that records every call and answers like an empty one."""

    def __init__(self):
        self.calls: list[str] = []

    def __getattr__(self, name):
        def record(*_a, **_kw):
            self.calls.append(name)
            return {}
        return record


# --- off -------------------------------------------------------------------------------------

def test_off_a_run_start_reads_no_table_and_records_no_version(make):
    e = make(_wf(on=False))
    spy = Spy()
    e.handler.telemetry_tbl = spy
    _sid, item, payload = _start(e)
    assert spy.calls == []                                   # not one call on the table
    assert "configVersion" not in item and "liveconfig" not in payload


def test_off_every_route_is_404_even_for_an_admin(make):
    e = make(_wf(on=False))
    for rk in ("GET /api/liveconfig", "GET /api/liveconfig/versions", "POST /api/liveconfig/versions",
               "PUT /api/liveconfig/active"):
        status, body = call(e, rk, body={})
        assert status == 404 and "liveConfig" in body["error"]
    assert e.handler.WORKFLOW["liveConfig"] is None


def test_off_the_runtime_ignores_a_version_in_the_payload():
    wf = _wf(on=False)
    with workflow(wf) as imp:
        lc = imp("app.common.liveconfig")
        assert lc.from_payload({"liveconfig": {"version": 3, "agents": {"a": {"model": "x"}}}}) is None


# --- a run picks its version ----------------------------------------------------------------

def test_on_before_anything_is_activated_a_run_is_on_version_0(make):
    e = make(_wf())
    _sid, item, payload = _start(e)
    assert item["configVersion"] == 0 and "liveconfig" not in payload
    assert e.handler.WORKFLOW["liveConfig"] is True


def test_a_run_records_and_carries_the_active_version(make):
    e = make(_wf())
    assert _save(e, {"a": {"systemPrompt": "Be brief.", "temperature": 0.3}})[0] == 200
    assert _save(e, {"b": {"maxTokens": 3000}})[0] == 200
    assert _activate(e, 2, 0)[0] == 200
    _sid, item, payload = _start(e)
    assert item["configVersion"] == 2
    assert payload["liveconfig"] == {"version": 2, "agents": {"b": {"maxTokens": 3000}}}
    # And the runner hands it to the runtime on start only.
    sent = {}
    e.handler.agentcore = types.SimpleNamespace(invoke_agent_runtime=lambda **kw: sent.update(kw))
    e.handler._run(payload)
    assert json.loads(sent["payload"])["liveconfig"] == payload["liveconfig"]
    e.handler._run({"action": "resume", "session_id": "s1", "liveconfig": {"version": 9}})
    assert "liveconfig" not in json.loads(sent["payload"])


def test_a_decision_after_the_pointer_moved_sends_no_version_and_reads_none(make):
    e = make(_wf())
    _save(e, {"a": {"temperature": 0.5}})
    _activate(e, 1, 0)
    sid, item, _ = _start(e)
    assert item["configVersion"] == 1
    _save(e, {"a": {"temperature": 0.9}})
    _activate(e, 2, 1)
    spy = Spy()
    e.handler.telemetry_tbl = spy
    # Waiting at a gate: a decision is taken only then (handler._claim_decision).
    e.status.update_item(Key={"session_id": sid}, UpdateExpression="SET overall = :w, hitl = :h",
                         ExpressionAttributeValues={":w": "waiting_human", ":h": {"node": "a"}})
    status, _ = call(e, "POST /api/sessions/{id}/decision", params={"id": sid},
                     path=f"/api/sessions/{sid}/decision", body={"decision": "approve"}, sub="user1", groups=())
    assert status == 200
    assert "liveconfig" not in e.invoked[-1] and spy.calls == []
    assert e.status.get_item(Key={"session_id": sid})["Item"]["configVersion"] == 1


def test_an_unreadable_table_starts_the_run_on_version_0_and_says_why(make, capsys):
    e = make(_wf())

    class Broken:
        def get_item(self, **_kw):
            raise RuntimeError("AccessDenied")
    e.handler.telemetry_tbl = Broken()
    _sid, item, payload = _start(e)
    assert item["configVersion"] == 0 and "liveconfig" not in payload
    assert "[liveconfig] using deployed config: RuntimeError: AccessDenied" in capsys.readouterr().out


def test_the_pointer_is_trusted_for_a_few_seconds_at_most(make, monkeypatch):
    e = make(_wf())
    _save(e, {"a": {"temperature": 0.1}})
    _save(e, {"a": {"temperature": 0.2}})
    _activate(e, 1, 0)
    now = [1000.0]
    monkeypatch.setattr(e.lc.time, "time", lambda: now[0])
    assert e.lc.for_run(e.tel)["version"] == 1
    # Another container moves the pointer (straight to the table, not through this cache).
    e.tel.update_item(Key={"session_id": e.lc.POINTER_PK, "sk": "active"},
                      UpdateExpression="SET #p = :p", ExpressionAttributeNames={"#p": "pointer"},
                      ExpressionAttributeValues={":p": {"active": 2}})
    now[0] += e.lc.POINTER_TTL_S - 1
    assert e.lc.for_run(e.tel)["version"] == 1
    now[0] += 1
    assert e.lc.for_run(e.tel)["version"] == 2
    assert e.lc.POINTER_TTL_S <= 5


def test_activating_here_is_seen_by_the_next_run_here_at_once(make):
    e = make(_wf())
    _save(e, {"a": {"temperature": 0.1}})
    assert _start(e)[1]["configVersion"] == 0
    _activate(e, 1, 0)
    assert _start(e)[1]["configVersion"] == 1


# --- what a version may change --------------------------------------------------------------

@pytest.mark.parametrize("key", ["tool", "hitl", "agentcore", "runtime", "toolMode", "guardrails", "name"])
def test_anything_but_the_prompt_and_model_settings_is_refused_by_name(make, key):
    e = make(_wf())
    status, body = _save(e, {"a": {key: "x"}})
    assert status == 400
    assert f"agents.a.{key}" in body["error"] and "needs a redeploy" in body["error"]
    assert call(e, "GET /api/liveconfig/versions")[1]["versions"] == []


def test_a_remote_agent_takes_no_change(make):
    e = make(_wf())
    status, body = _save(e, {"r": {"temperature": 0.2}})
    assert status == 400 and "a2a" in body["error"]


def test_an_unknown_agent_and_an_empty_change_are_refused(make):
    e = make(_wf())
    assert "not an agent" in _save(e, {"zz": {"temperature": 0.2}})[1]["error"]
    assert "at least one setting" in _save(e, {"a": {}})[1]["error"]
    assert "at least one agent" in _save(e, {})[1]["error"]
    assert "unknown field" in call(e, "POST /api/liveconfig/versions",
                                   body={"agents": {"a": {"temperature": 0}}, "tools": {}})[1]["error"]


def test_the_settings_are_held_to_the_workflow_json_rules(make):
    e = make(_wf())
    status, body = _save(e, {"a": {"temperature": "hot", "maxTokens": 0, "stopSequences": [1]}})
    assert status == 400
    assert "agents.a.temperature: must be a number" in body["error"]
    assert "agents.a.maxTokens: must be greater than 0" in body["error"]
    assert "agents.a.stopSequences[0]: must be a string" in body["error"]


def test_a_model_the_runtimes_iam_does_not_cover_is_refused(make):
    e = make(_wf())
    for model in (f"arn:aws:bedrock:us-east-1:{ACCOUNT}:custom-model/abc",
                  f"arn:aws:bedrock:us-east-1:{ACCOUNT}:provisioned-model/abc",
                  "arn:aws:bedrock:us-east-1:999999999999:inference-profile/us.x.y",
                  f"arn:aws:bedrock:eu-west-1:{ACCOUNT}:application-inference-profile/abc",
                  "not a model"):
        status, body = _save(e, {"a": {"model": model}})
        assert status == 400 and "agents.a.model" in body["error"], model
    for model in (MODEL, "anthropic.claude-3-haiku-20240307-v1:0",
                  "arn:aws:bedrock:us-west-2::foundation-model/anthropic.claude-3-haiku-20240307-v1:0",
                  f"arn:aws:bedrock:us-east-1:{ACCOUNT}:application-inference-profile/abc123"):
        assert _save(e, {"a": {"model": model}})[0] == 200, model


def test_the_model_allow_list_is_the_runtimes_iam_grant_on_both_paths():
    """`model_allowed` mirrors BedrockModelInvocation for the runtimes. Were a path to narrow
    or widen its grant, this list would be wrong and must change with it."""
    grant_tf = ['"arn:aws:bedrock:*::foundation-model/*"',
                '"arn:aws:bedrock:${var.region}:${local.account_id}:inference-profile/*"',
                '"arn:aws:bedrock:${var.region}:${local.account_id}:application-inference-profile/*"']
    for f in ("main.tf", "subagent_runtimes.tf"):
        src = (TF / f).read_text()
        for g in grant_tf:
            assert g in src, (f, g)
    grant_cdk = ['"arn:aws:bedrock:*::foundation-model/*"',
                 "`arn:aws:bedrock:${this.region}:${this.account}:inference-profile/*`",
                 "`arn:aws:bedrock:${this.region}:${this.account}:application-inference-profile/*`"]
    # CDK: one statement, given to the main runtime and to each dedicated agent's role.
    block = STACK.split("const bedrockInvoke = new iam.PolicyStatement({", 1)[1].split("});", 1)[0]
    for g in grant_cdk:
        assert g in block, g
    assert "[bedrockInvoke, priceListRead" in STACK and STACK.count("bedrockInvoke,") >= 2


def test_a_model_that_cannot_do_what_the_agent_asks_is_refused(make, monkeypatch):
    e = make(_wf())
    seen = []

    def problems(wf):
        seen.append(wf["agents"]["a"].get("model"))
        if wf["agents"]["a"].get("model") == "acme.text-only-v1":
            return [{"severity": "error", "path": "agents.a.model", "message": "acme.text-only-v1 cannot read images"}]
        return []
    monkeypatch.setattr(e.handler, "model_problems", problems)
    status, body = _save(e, {"a": {"model": "acme.text-only-v1"}})
    assert status == 400 and "cannot read images" in body["error"]
    assert "acme.text-only-v1" in seen and None in seen          # judged on the merged and the deployed


def test_sizes_are_capped(make):
    e = make(_wf())
    assert "at most" in _save(e, {"a": {"systemPrompt": "x" * (e.lc.PROMPT_MAX + 1)}})[1]["error"]
    assert "note" in _save(e, {"a": {"temperature": 0}}, note="n" * (e.lc.NOTE_MAX + 1))[1]["error"]
    assert "prompt's text" in _save(e, {"a": {"systemPrompt": "   "}})[1]["error"]


def test_a_version_is_written_once_and_kept(make, monkeypatch):
    e = make(_wf())
    assert _save(e, {"a": {"temperature": 0.1}})[1]["version"] == 1
    # Two admins saving at once: both computed "next is 1"; the second must not overwrite.
    monkeypatch.setattr(e.lc, "versions_top", lambda table: 0)
    status, _body = _save(e, {"a": {"temperature": 0.9}})
    assert status == 409
    got = call(e, "GET /api/liveconfig/versions/{version}", params={"version": "1"})[1]
    assert got["overlay"] == {"a": {"temperature": 0.1}}


def test_reading_one_version_and_the_list(make):
    e = make(_wf())
    _save(e, {"a": {"temperature": 0.1}}, note="cooler")
    _save(e, {"b": {"maxTokens": 2500}, "a": {"topP": 0.9}})
    listed = call(e, "GET /api/liveconfig/versions")[1]["versions"]
    assert [v["version"] for v in listed] == [2, 1]
    assert listed[1]["note"] == "cooler" and listed[0]["agents"] == ["a", "b"]
    assert listed[0]["by"] == "admin1@example.com"
    status, one = call(e, "GET /api/liveconfig/versions/{version}", params={"version": "2"})
    assert status == 200 and one["overlay"] == {"a": {"topP": 0.9}, "b": {"maxTokens": 2500}}
    assert call(e, "GET /api/liveconfig/versions/{version}", params={"version": "7"})[0] == 404
    assert call(e, "GET /api/liveconfig/versions/{version}", params={"version": "x1"})[0] == 400
    state = call(e, "GET /api/liveconfig")[1]
    assert state["pointer"]["active"] == 0 and set(state["agents"]) == {"a", "b"}   # not the a2a one
    assert state["agents"]["b"]["deployed"] == {"maxTokens": 2000, "model": MODEL}


# --- activation, rollback, audit -------------------------------------------------------------

def _events(e):
    return call(e, "GET /api/audit")[1]


def test_activation_and_rollback_are_audited_with_before_and_after(make):
    e = make(_wf())
    _save(e, {"a": {"temperature": 0.1}})
    _save(e, {"a": {"temperature": 0.2}})
    assert _activate(e, 2, 0)[1]["pointer"]["active"] == 2
    assert _activate(e, 1, 2)[0] == 200                       # rollback
    assert _activate(e, 0, 1)[0] == 200                       # back to the deployed config
    acts = [ev["detail"] for ev in _events(e) if ev["action"] == "liveconfig.activated"]
    assert sorted((d["before"], d["after"]) for d in acts) == [(0, 2), (1, 0), (2, 1)]
    created = [ev["detail"]["version"] for ev in _events(e) if ev["action"] == "liveconfig.created"]
    assert sorted(created) == [1, 2]


def test_a_stale_activation_is_refused_not_overwritten(make):
    e = make(_wf())
    _save(e, {"a": {"temperature": 0.1}})
    _save(e, {"a": {"temperature": 0.2}})
    _activate(e, 1, 0)
    status, _body = _activate(e, 2, 0)                       # this admin last saw v0 active
    assert status == 409
    assert call(e, "GET /api/liveconfig")[1]["pointer"]["active"] == 1


def test_activating_a_version_that_does_not_exist_is_404(make):
    e = make(_wf())
    assert _activate(e, 4)[0] == 404
    assert _activate(e, -1)[0] == 400
    assert call(e, "PUT /api/liveconfig/active", body={"version": True})[0] == 400


def test_the_pointer_write_is_conditional_on_what_was_read(make, monkeypatch):
    """The 409 above is decided by DynamoDB, not only by the read before it: a pointer moved
    between the read and the write is still caught."""
    e = make(_wf())
    _save(e, {"a": {"temperature": 0.1}})
    _save(e, {"a": {"temperature": 0.2}})
    _activate(e, 1, 0)
    monkeypatch.setattr(e.lc, "pointer", lambda table, consistent=True: {"active": 0})
    status, _ = _activate(e, 2)
    assert status == 409


@pytest.mark.parametrize("rk,params,body", [
    ("GET /api/liveconfig", None, None),
    ("GET /api/liveconfig/versions", None, None),
    ("GET /api/liveconfig/versions/{version}", {"version": "1"}, None),
    ("POST /api/liveconfig/versions", None, {"agents": {"a": {"temperature": 0.5}}}),
    ("PUT /api/liveconfig/active", None, {"version": 1}),
])
def test_every_route_refuses_a_caller_who_is_not_an_admin(make, rk, params, body):
    e = make(_wf())
    _save(e, {"a": {"temperature": 0.1}})
    status, resp = call(e, rk, params=params, body=body, sub="bob", groups=("operators",))
    assert status == 403 and resp["requiredGroups"] == ["admins"]
    assert [v["version"] for v in call(e, "GET /api/liveconfig/versions")[1]["versions"]] == [1]
    assert call(e, "GET /api/liveconfig")[1]["pointer"]["active"] == 0


def test_routes_are_declared_on_both_route_lists():
    tf = (TF / "bff.tf").read_text()
    for rk in ("GET /api/liveconfig", "GET /api/liveconfig/versions", "POST /api/liveconfig/versions",
               "GET /api/liveconfig/versions/{version}", "PUT /api/liveconfig/active"):
        assert f'"{rk}"' in tf, rk
    for line in ('{ path: "/api/liveconfig", methods: [HttpMethod.GET] }',
                 '{ path: "/api/liveconfig/versions", methods: [HttpMethod.GET, HttpMethod.POST] }',
                 '{ path: "/api/liveconfig/versions/{version}", methods: [HttpMethod.GET] }',
                 '{ path: "/api/liveconfig/active", methods: [HttpMethod.PUT] }'):
        assert line in STACK, line


# --- IAM ------------------------------------------------------------------------------------

def test_the_grant_is_the_bffs_only_on_the_reserved_rows_and_the_same_on_both_paths():
    import liveconfig as bff_lc
    tf = (TF / "liveconfig.tf").read_text()
    assert bff_lc.PREFIX + "*" == "_liveconfig#*"
    assert '"dynamodb:LeadingKeys" = ["_liveconfig#*"]' in tf
    assert 'const LIVE_CONFIG_KEYS = "_liveconfig#*";' in STACK
    assert '"dynamodb:LeadingKeys": [LIVE_CONFIG_KEYS]' in STACK
    actions = '["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:Query"]'
    assert f"Action   = {actions}" in tf and f"actions: {actions}," in STACK
    assert "role  = aws_iam_role.bff.id" in tf and 'Sid      = "LiveConfig"' in tf
    # Only when the workflow turns it on, on both paths.
    assert "count = local.live_config ? 1 : 0" in tf
    assert "try(local.workflow_def.orchestrator.liveConfig, false) == true" in tf
    assert "if (workflow.orchestrator?.liveConfig === true) {" in STACK
    # No runtime is granted anything on these rows: they never read them.
    for f in TF.glob("*.tf"):
        if f.name != "liveconfig.tf":
            assert "_liveconfig" not in f.read_text(), f.name
    assert STACK.count("LIVE_CONFIG_KEYS") == 2


def test_the_two_halves_agree_on_the_fields():
    import liveconfig as bff_lc
    with workflow(_wf()) as imp:
        assert imp("app.common.liveconfig").FIELDS == bff_lc.FIELDS


# --- the runtime ----------------------------------------------------------------------------

def test_the_start_payload_is_cleaned_to_the_six_settings_and_known_agents():
    with workflow(_wf()) as imp:
        lc = imp("app.common.liveconfig")
        got = lc.from_payload({"liveconfig": {"version": 2, "agents": {
            "a": {"temperature": 0.2, "tool": "kb", "hitl": True}, "zz": {"model": "m"}, "b": {"tool": "x"}}}})
        assert got == {"version": 2, "agents": {"a": {"temperature": 0.2}}}
        assert lc.from_payload({"liveconfig": {"version": 0, "agents": {"a": {"temperature": 1}}}}) is None
        assert lc.from_payload({"liveconfig": {"version": True}}) is None
        assert lc.from_payload({}) is None


def _agent(imp, aid="a", prompt="DEPLOYED PROMPT"):
    base = imp("app.common.base")
    a = base.Agent()
    a.id, a.name, a.agentcore, a.model, a.temperature, a.max_tokens = aid, aid, {}, "m0", 0, 1000
    a.top_p, a.stop_sequences, a.system_prompt = None, [], prompt
    return a


CFG = {"configurable": {"thread_id": "s1"}}


def test_the_context_carries_the_runs_version_and_leaves_the_agent_alone():
    with workflow(_wf()) as imp:
        ctx_mod = imp("app.common.context")
        a = _agent(imp)
        live = {"version": 3, "agents": {"a": {"model": "m3", "temperature": 0.7, "topP": 0.8, "maxTokens": 2500,
                                               "stopSequences": ["END"], "systemPrompt": "LIVE PROMPT"}}}
        c = ctx_mod.AgentContext(a, {"topic": "t", "liveconfig": live}, CFG)
        assert (c.model, c.temperature, c.top_p, c.max_tokens, c.stop_sequences) == ("m3", 0.7, 0.8, 2500, ["END"])
        assert c.agent_prompt == "LIVE PROMPT" and c.config_version == 3
        assert (a.model, a.temperature, a.max_tokens, a.system_prompt) == ("m0", 0, 1000, "DEPLOYED PROMPT")
        plain = ctx_mod.AgentContext(a, {"topic": "t"}, CFG)
        assert (plain.model, plain.max_tokens, plain.config_version) == ("m0", 1000, 0)


def test_ctx_llm_swaps_in_the_live_prompt_and_says_so_when_it_cannot(monkeypatch):
    with workflow(_wf()) as imp:
        ctx_mod = imp("app.common.context")
        sent = []

        async def fake_run_llm(name, system, user, **kw):
            sent.append((system, kw["model"], kw["temperature"]))
            return "ok", False
        monkeypatch.setattr(ctx_mod, "run_llm", fake_run_llm)
        live = {"version": 2, "agents": {"a": {"systemPrompt": "LIVE PROMPT", "model": "m2"}}}
        c = ctx_mod.AgentContext(_agent(imp), {"topic": "t", "liveconfig": live}, CFG)
        logged = []

        async def log(msg):
            logged.append(msg)
        c.log = log
        asyncio.run(c.llm("DEPLOYED PROMPT\n\nextra", "u"))
        assert sent[0][0].startswith("LIVE PROMPT\n\nextra") and "DEPLOYED" not in sent[0][0]
        assert sent[0][1] == "m2" and logged == []
        asyncio.run(c.llm("something the agent built itself", "u"))
        asyncio.run(c.llm("and again", "u"))
        assert sent[1][0].startswith("something the agent built itself")
        assert len(logged) == 1 and "config v2" in logged[0] and "not applied" in logged[0]


def _stub_graph(imp, seen: dict, saver):
    base = imp("app.common.base")
    sink = imp("app.common.sink")
    gb = imp("app.orchestrator.graph_builder")
    ctx_mod = imp("app.common.context")
    sink.STATUS_TABLE = None
    sink.EVENTS_TABLE = None

    async def fake_run_llm(name, system, user, **kw):
        return f"{system}|{kw['model']}", False
    ctx_mod.run_llm = fake_run_llm

    def load_agents():
        reg = {}
        for aid in ("a", "b"):
            agent = base.Agent()
            agent.id, agent.name, agent.agentcore, agent.system_prompt = aid, aid, {}, f"P-{aid}"

            async def run(ctx, _id=aid, _agent=agent):
                text = await ctx.llm(_agent.system_prompt, "u")
                seen.setdefault(_id, []).append((ctx.config_version, ctx.model, ctx.max_tokens,
                                                  text.split("\n")[0], text.rsplit("|", 1)[-1]))
                return "{}"
            agent.run = run
            reg[aid] = agent
        return reg
    gb.load_agents = load_agents
    imp("app.common.bus").bus.create("pin1", "t")
    return gb.build_graph(saver)


def test_a_run_keeps_its_version_across_a_gate_resume_on_another_container():
    """Start on v1, pause at the gate, then resume from the checkpoint with a NEW graph (as a
    later invocation on another container does) while the pointer now says v2. The step after
    the gate still runs v1, because the version is in the checkpointed state and nothing
    re-reads it."""
    from langgraph.checkpoint.memory import MemorySaver
    from langgraph.types import Command
    wf = {"orchestrator": {"liveConfig": True},
          "agents": {"a": {"name": "A", "maxTokens": 100}, "b": {"name": "B", "maxTokens": 100}},
          "steps": [{"agent": "a", "hitl": True}, {"agent": "b"}]}
    v1 = {"version": 1, "agents": {"a": {"model": "m-a1"}, "b": {"model": "m-b1", "systemPrompt": "LIVE-b1",
                                                                  "maxTokens": 777}}}
    with workflow(wf) as imp:
        saver, seen = MemorySaver(), {}
        cfg = {"configurable": {"thread_id": "pin1"}}
        first = _stub_graph(imp, seen, saver)
        asyncio.run(first.ainvoke({"topic": "t", "status": {}, "outputs": {}, "liveconfig": v1}, cfg))
        assert seen["a"] == [(1, "m-a1", 4000, "P-a", "m-a1")] and "b" not in seen
        second = _stub_graph(imp, seen, saver)              # another container, same checkpoints
        asyncio.run(second.ainvoke(Command(resume={"decision": "approve"}), cfg))
        assert seen["b"] == [(1, "m-b1", 777, "LIVE-b1", "m-b1")]


def test_the_runtime_takes_the_version_at_start_and_never_on_resume(monkeypatch):
    wf = {"orchestrator": {"liveConfig": True},
          "agents": {"intake": {"name": "Intake", "maxTokens": 100}}, "steps": [{"agent": "intake"}]}
    monkeypatch.setenv("MEMORY_ID", "mem-test")
    with workflow(wf) as imp:
        runtime = imp("app.orchestrator.runtime")
        runs, pending = [], []

        async def fake_run(session_id, initial=None, resume=None, rerun=None, user=""):
            runs.append({"initial": initial, "resume": resume, "rerun": rerun})

        async def no_session(*_a):
            return None
        monkeypatch.setattr(runtime, "_run", fake_run)
        monkeypatch.setattr(runtime, "ensure_session", no_session)
        monkeypatch.setattr(runtime, "_spawn", lambda coro, session_id="": pending.append(coro))

        async def go(payload):
            await runtime.invoke(payload)
            while pending:
                await pending.pop()
        live = {"version": 4, "agents": {"intake": {"temperature": 0.4}}}
        later = {"version": 5, "agents": {"intake": {"temperature": 0.9}}}
        asyncio.run(go({"action": "start", "session_id": "s1", "topic": "t", "liveconfig": live}))
        asyncio.run(go({"action": "resume", "session_id": "s1", "decision": "approve", "liveconfig": later}))
        asyncio.run(go({"action": "rerun_from", "session_id": "s1", "agent_id": "intake", "liveconfig": later}))
        assert len(runs) == 3
        assert runs[0]["initial"]["liveconfig"] == live
        assert "liveconfig" not in json.dumps(runs[1]["resume"]) and "liveconfig" not in json.dumps(runs[2]["rerun"])


def test_a_dedicated_agent_is_sent_the_runs_version_for_itself_only(monkeypatch):
    with workflow(_wf()) as imp:
        ac = imp("app.common.agentcore_agent")
        sent = {}

        class Client:
            def invoke_agent_runtime(self, **kw):
                sent.update(json.loads(kw["payload"]))
                return {"response": io.BytesIO(json.dumps({"output": "{}"}).encode())}
        monkeypatch.setitem(ac._RUNTIME_ARNS, "b", "arn:aws:test")
        monkeypatch.setattr(ac, "_agentcore", lambda: Client())
        agent = ac.AgentCoreRuntimeAgent()
        agent.id = "b"
        ctx = types.SimpleNamespace(session_id="s1", topic="t", feedback="", lines=[],
                                    state={"outputs": {}, "liveconfig": {"version": 6, "agents": {
                                        "a": {"model": "secret-to-b"}, "b": {"maxTokens": 4321}}}})

        async def log(m):
            ctx.lines.append(m)
        ctx.log = log
        asyncio.run(agent.run(ctx))
        assert sent["liveconfig"] == {"version": 6, "agents": {"b": {"maxTokens": 4321}}}
        sent.clear()
        ctx.state = {"outputs": {}}
        asyncio.run(agent.run(ctx))
        assert "liveconfig" not in sent


def test_the_dedicated_container_runs_the_version_it_is_sent(monkeypatch):
    shipped = json.loads((ORCH_ROOT / "app" / "workflow.json").read_text())
    shipped["orchestrator"] = {**shipped.get("orchestrator", {}), "liveConfig": True}
    monkeypatch.setenv("AGENT_ID", "web_search")
    with workflow(shipped) as imp:
        sub = imp("app.subagent_runtime")
        seen = []

        async def run(ctx):
            seen.append((ctx.config_version, ctx.max_tokens, ctx.model))
            return "{}"
        monkeypatch.setattr(sub._agent, "run", run)
        got = asyncio.run(sub.invoke({"agent_id": "web_search", "session_id": "s1", "topic": "t",
                                      "liveconfig": {"version": 6, "agents": {"web_search": {
                                          "maxTokens": 4321, "model": "m6", "tool": "x"}}}}))
        assert got.get("output") == "{}", got
        assert seen == [(6, 4321, "m6")]


def test_runtime_and_state_carry_the_key_where_the_design_says():
    """Pins the wiring the tests above exercise, so a refactor cannot quietly drop a hop."""
    state = (ORCH_ROOT / "app" / "common" / "state.py").read_text()
    assert re.search(r"^\s+liveconfig: dict\s*$", state, re.MULTILINE)          # a plain channel, set once
    rt = (ORCH_ROOT / "app" / "orchestrator" / "runtime.py").read_text()
    assert rt.count("liveconfig.from_payload(payload)") == 1             # at start, nowhere else
