"""Which model answers (app/common/model_routing.py, bff/model_override.py).

What has to hold:
  * selection order: the run's override, else the first matching route rule, else the agent's
    model; then its fallbacks, without the model already tried;
  * a fallback is tried only on the Bedrock Runtime "busy or not ready" errors
    (ThrottlingException, ServiceUnavailableException, ModelNotReadyException) or a LiteLLM
    429/503; never on a validation, access, timeout or guardrail failure;
  * an overridden agent uses that model only;
  * the override needs the `override` grant (closed unless granted), is held to
    orchestrator.models.overrides on the server and in the runtime, is stored on the run, and
    keeps through a gate resume and a re-run because it lives in the checkpointed state;
  * each call's telemetry row says which model answered and why (`chosen_by`).
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
from botocore.exceptions import ClientError
from conftest import ORCH_ROOT, workflow

moto = pytest.importorskip("moto")
from test_builds import CTX, FakeCodeBuild, _events_table, _status_table, create_builds_table  # noqa: E402 - after the moto importorskip

H = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
S = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
OP = "us.anthropic.claude-opus-4-1-20250805-v1:0"
RULES = {"groupsClaim": "cognito:groups", "actions": {"override": ["tuners"]}}


def _wf(**agent_a) -> dict:
    return {"orchestrator": {"models": {"overrides": [S, OP]}},
            "agents": {"a": {"name": "A", "maxTokens": 100, **agent_a},
                       "b": {"name": "B", "runtime": "dedicated", "maxTokens": 100},
                       "h": {"name": "H", "runtime": "harness", "instructions": "x"},
                       "r": {"name": "R", "runtime": "a2a", "agentCard": "https://agent.example.com"}},
            "steps": [{"agent": "a", "hitl": True}, {"agent": "b"}, {"agent": "h"}, {"agent": "r"}],
            "authorization": RULES}


def _agent(imp, aid="a", model=None):
    base = imp("app.common.base")
    a = base.Agent()
    a.id, a.name, a.agentcore, a.model, a.temperature, a.max_tokens = aid, aid, {}, model, 0, 1000
    a.top_p, a.stop_sequences, a.system_prompt = None, [], "P"
    return a


CFG = {"configurable": {"thread_id": "s1"}}


def _ctx(imp, state=None, model=None, aid="a"):
    c = imp("app.common.context").AgentContext(_agent(imp, aid, model), {"topic": "t", **(state or {})}, CFG)
    c.lines = []

    async def log(m):
        c.lines.append(m)
    c.log = log
    return c


def _busy(code: str, imp):
    errors = imp("app.common.errors")
    try:
        raise ClientError({"Error": {"Code": code, "Message": "slow down"}}, "Converse")
    except ClientError as e:
        try:
            raise errors.ModelUnavailable(f"failed: {code}") from e
        except errors.ModelUnavailable as wrapped:
            return wrapped


# --- selection order -------------------------------------------------------------------------

def test_without_a_models_block_or_override_a_call_is_the_agents_model_alone():
    with workflow(_wf()) as imp:
        mr = imp("app.common.model_routing")
        assert mr.candidates(_ctx(imp, model=H), "s", "u") == [(H, "", "")]
        assert mr.candidates(_ctx(imp), "s", "u") == [(None, "", "")]          # the deployment default


def test_route_rules_first_match_wins_else_the_agents_model_then_the_fallbacks():
    models = {"route": [{"model": OP, "inputTokensOver": 100}, {"model": S, "matches": "invoice|refund"}],
              "fallback": [S, H, OP]}
    with workflow(_wf(model=H, models=models)) as imp:
        mr = imp("app.common.model_routing")
        small = _ctx(imp, model=H)
        assert [m for m, *_ in mr.candidates(small, "s", "u")] == [H, S, OP]     # own model, then fallbacks
        big = mr.candidates(small, "x" * 300, "y" * 200)                         # 500 chars = 125 tokens
        assert big[0] == (OP, "route 1", "route 1: about 125 tokens in, over 100")
        assert [m for m, *_ in big] == [OP, S, H]                                  # OP is not retried
        about = _ctx(imp, {"topic": "Please process this REFUND"}, model=H)
        assert mr.candidates(about, "s", "u")[0][:2] == (S, "route 2")
        subject = _ctx(imp, {"topic": "hello", "subject_id": "invoice-77"}, model=H)
        assert mr.candidates(subject, "s", "u")[0][:2] == (S, "route 2")
        both = _ctx(imp, {"topic": "refund"}, model=H)
        assert mr.candidates(both, "x" * 1000, "u")[0][1] == "route 1"            # first match wins
        assert mr.candidates(about, "s", "u", explicit=H)[0] == (H, "", "")       # the code's own pick


def test_the_override_wins_and_is_the_only_model():
    models = {"route": [{"model": OP, "inputTokensOver": 1}], "fallback": [H]}
    with workflow(_wf(model=H, models=models)) as imp:
        mr = imp("app.common.model_routing")
        c = _ctx(imp, {"modelOverride": {"all": S}}, model=H)
        assert mr.candidates(c, "x" * 100, "u") == [(S, "override", "the run's model override")]
        c = _ctx(imp, {"modelOverride": {"all": S, "agents": {"a": OP}}}, model=H)
        assert mr.candidates(c, "s", "u")[0][0] == OP                              # its own pick over all


def test_a_bad_pattern_never_fails_a_run():
    with workflow(_wf(models={"route": [{"model": OP, "matches": "("}, {"model": S, "matches": "t"}]})) as imp:
        mr = imp("app.common.model_routing")
        assert mr.candidates(_ctx(imp), "s", "u")[0][:2] == (S, "route 2")


# --- error classification ---------------------------------------------------------------------

def test_the_fallback_codes_are_bedrock_runtimes_own():
    """Each code is a modeled Converse error in botocore, and the documented 429/503 ones."""
    import botocore.session
    m = botocore.session.get_session().get_service_model("bedrock-runtime")
    shapes = {e.name: e.metadata.get("error", {}).get("httpStatusCode")
              for e in m.operation_model("Converse").error_shapes}
    with workflow(_wf()) as imp:
        mr = imp("app.common.model_routing")
        assert {c: shapes[c] for c in mr.FALLBACK_CODES} == {
            "ThrottlingException": 429, "ServiceUnavailableException": 503, "ModelNotReadyException": 429}


@pytest.mark.parametrize("code,moves", [
    ("ThrottlingException", True), ("ServiceUnavailableException", True), ("ModelNotReadyException", True),
    ("ValidationException", False), ("AccessDeniedException", False), ("ResourceNotFoundException", False),
    ("ModelTimeoutException", False), ("InternalServerException", False), ("ModelErrorException", False),
])
def test_only_busy_or_not_ready_moves_to_a_fallback(code, moves):
    with workflow(_wf()) as imp:
        mr = imp("app.common.model_routing")
        assert mr.failover_code(_busy(code, imp)) == (code if moves else None)


def test_a_litellm_429_or_503_moves_on_and_nothing_else_does():
    with workflow(_wf()) as imp:
        mr = imp("app.common.model_routing")
        proxy = imp("app.common.litellm_proxy")
        errors = imp("app.common.errors")

        def wrapped(status):
            try:
                raise proxy.ProxyError("the proxy answered", status=status)
            except proxy.ProxyError as e:
                try:
                    raise errors.ModelUnavailable("x") from e
                except errors.ModelUnavailable as w:
                    return w
        assert mr.failover_code(wrapped(429)) == "HTTP 429"
        assert mr.failover_code(wrapped(503)) == "HTTP 503"
        assert mr.failover_code(wrapped(400)) is None and mr.failover_code(wrapped(500)) is None
        guard = imp("app.features.guardrails.client")
        assert mr.failover_code(guard.GuardrailBlocked("blocked")) is None
        assert mr.failover_code(errors.ModelUnavailable("no cause")) is None


# --- ctx.llm --------------------------------------------------------------------------------

def _calls(imp, monkeypatch, fail: dict):
    """Fake run_llm: raises `fail[model]` (an error code) or answers; records (model, chosen_by)."""
    ctx_mod = imp("app.common.context")
    mr = imp("app.common.model_routing")
    seen = []

    async def fake_run_llm(name, system, user, **kw):
        seen.append((kw["model"], mr.chosen_by()))
        if kw["model"] in fail:
            raise _busy(fail[kw["model"]], imp)
        return f"answer from {kw['model']}", False
    monkeypatch.setattr(ctx_mod, "run_llm", fake_run_llm)
    return seen


def test_a_busy_model_falls_back_in_order_and_the_timeline_says_so(monkeypatch):
    with workflow(_wf(model=H, models={"fallback": [S, OP]})) as imp:
        seen = _calls(imp, monkeypatch, {H: "ThrottlingException", S: "ModelNotReadyException"})
        c = _ctx(imp, model=H)
        assert asyncio.run(c.llm("P", "u")) == f"answer from {OP}"
        assert seen == [(H, ""), (S, "fallback 1"), (OP, "fallback 2")]
        assert c.lines == [f"{H} is busy or not ready (ThrottlingException); trying {S} (fallback 1 of 2)",
                           f"{S} is busy or not ready (ModelNotReadyException); trying {OP} (fallback 2 of 2)"]


def test_a_validation_error_never_falls_back(monkeypatch):
    with workflow(_wf(model=H, models={"fallback": [S]})) as imp:
        errors = imp("app.common.errors")
        seen = _calls(imp, monkeypatch, {H: "ValidationException"})
        with pytest.raises(errors.ModelUnavailable):
            asyncio.run(_ctx(imp, model=H).llm("P", "u"))
        assert seen == [(H, "")]


def test_the_last_fallback_failing_fails_the_step(monkeypatch):
    with workflow(_wf(model=H, models={"fallback": [S]})) as imp:
        errors = imp("app.common.errors")
        seen = _calls(imp, monkeypatch, {H: "ThrottlingException", S: "ServiceUnavailableException"})
        with pytest.raises(errors.ModelUnavailable):
            asyncio.run(_ctx(imp, model=H).llm("P", "u"))
        assert [m for m, _ in seen] == [H, S]


def test_an_overridden_agent_never_falls_back(monkeypatch):
    with workflow(_wf(model=H, models={"fallback": [OP]})) as imp:
        errors = imp("app.common.errors")
        seen = _calls(imp, monkeypatch, {S: "ThrottlingException"})
        c = _ctx(imp, {"modelOverride": {"all": S}}, model=H)
        with pytest.raises(errors.ModelUnavailable):
            asyncio.run(c.llm("P", "u"))
        assert seen == [(S, "override")]
        assert c.lines == [f"Using {S} (the run's model override)"]


def test_a_routed_call_says_once_which_model_and_why(monkeypatch):
    with workflow(_wf(model=H, models={"route": [{"model": S, "matches": "invoice"}]})) as imp:
        seen = _calls(imp, monkeypatch, {})
        c = _ctx(imp, {"topic": "an invoice"}, model=H)
        asyncio.run(c.llm("P", "u"))
        asyncio.run(c.llm("P", "u"))
        assert seen == [(S, "route 1"), (S, "route 1")]
        assert c.lines == [f"Using {S} (route 1: the request matches /invoice/)"]


def test_the_telemetry_row_says_which_model_answered_and_why(monkeypatch):
    with workflow(_wf(model=H, models={"fallback": [S]})) as imp:
        meter = imp("app.features.observability.meter")
        mr = imp("app.common.model_routing")
        rows = []
        monkeypatch.setattr(meter.store, "put", rows.append)

        async def attempt(model):
            meter.record_llm(model=model, input_tokens=1, output_tokens=1, mode="bedrock")
            if model == H:
                raise _busy("ThrottlingException", imp)
            return "ok"
        c = _ctx(imp, model=H)
        assert asyncio.run(mr.call(c, mr.candidates(c, "s", "u"), attempt)) == "ok"
        assert [(r.label, r.chosen_by) for r in rows] == [(H, ""), (S, "fallback 1")]
        assert "chosen_by" in rows[1].to_item()
        meter.record_llm(model=H, input_tokens=1, output_tokens=1)            # outside a routed call
        assert rows[-1].chosen_by == ""


# --- the runtime keeps the run's override --------------------------------------------------

def test_the_start_payload_is_held_to_the_list_and_to_local_agents():
    with workflow(_wf()) as imp:
        mr = imp("app.common.model_routing")
        got = mr.from_payload({"modelOverride": {"all": "not-listed", "agents": {
            "a": S, "b": OP, "h": S, "r": S, "zz": S, "a2": "x"}}})
        assert got == {"agents": {"a": S, "b": OP}}
        assert mr.from_payload({"modelOverride": {"all": S}}) == {"all": S}
        assert mr.from_payload({"modelOverride": "S"}) is None and mr.from_payload({}) is None
        assert mr.override_for({"modelOverride": {"all": S}}, "h") is None    # a harness keeps its own
    with workflow({**_wf(), "orchestrator": {}}) as imp:
        assert imp("app.common.model_routing").from_payload({"modelOverride": {"all": S}}) is None


def _stub_graph(imp, seen: dict, saver):
    base = imp("app.common.base")
    sink = imp("app.common.sink")
    gb = imp("app.orchestrator.graph_builder")
    ctx_mod = imp("app.common.context")
    sink.STATUS_TABLE = None
    sink.EVENTS_TABLE = None

    async def fake_run_llm(name, system, user, **kw):
        return kw["model"], False
    ctx_mod.run_llm = fake_run_llm

    def load_agents():
        reg = {}
        for aid in ("a", "b"):
            agent = base.Agent()
            agent.id, agent.name, agent.agentcore, agent.system_prompt, agent.model = aid, aid, {}, "P", H

            async def run(ctx, _id=aid):
                seen.setdefault(_id, []).append(await ctx.llm("P", "u"))
                return "{}"
            agent.run = run
            reg[aid] = agent
        return reg
    gb.load_agents = load_agents
    imp("app.common.bus").bus.create("ov1", "t")
    return gb.build_graph(saver)


def test_a_run_keeps_its_override_across_a_gate_resume_and_a_rerun_on_another_container():
    from langgraph.checkpoint.memory import MemorySaver
    from langgraph.types import Command
    wf = {"orchestrator": {"models": {"overrides": [S, OP]}},
          "agents": {"a": {"name": "A"}, "b": {"name": "B"}},
          "steps": [{"agent": "a", "hitl": True}, {"agent": "b"}]}
    with workflow(wf) as imp:
        saver, seen = MemorySaver(), {}
        cfg = {"configurable": {"thread_id": "ov1"}}
        first = _stub_graph(imp, seen, saver)
        asyncio.run(first.ainvoke({"topic": "t", "status": {}, "outputs": {},
                                   "modelOverride": {"all": S, "agents": {"b": OP}}}, cfg))
        assert seen == {"a": [S]}
        second = _stub_graph(imp, seen, saver)                  # another container, same checkpoints
        asyncio.run(second.ainvoke(Command(resume={"decision": "revise", "comment": "again"}), cfg))
        assert seen["a"] == [S, S]                              # the re-run keeps it
        asyncio.run(_stub_graph(imp, seen, saver).ainvoke(Command(resume={"decision": "approve"}), cfg))
        assert seen["b"] == [OP]


def test_the_runtime_takes_the_override_at_start_and_never_on_resume_or_rerun(monkeypatch):
    wf = {"orchestrator": {"models": {"overrides": [S, OP]}},
          "agents": {"intake": {"name": "Intake"}}, "steps": [{"agent": "intake"}]}
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
        asyncio.run(go({"action": "start", "session_id": "s1", "topic": "t", "modelOverride": {"all": S}}))
        asyncio.run(go({"action": "resume", "session_id": "s1", "modelOverride": {"all": OP}}))
        asyncio.run(go({"action": "rerun_from", "session_id": "s1", "agent_id": "intake",
                        "modelOverride": {"all": OP}}))
        assert runs[0]["initial"]["modelOverride"] == {"all": S}
        assert "modelOverride" not in json.dumps(runs[1]["resume"])
        assert "modelOverride" not in json.dumps(runs[2]["rerun"])


def test_a_dedicated_agent_is_sent_its_own_model_only(monkeypatch):
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
                                    state={"outputs": {}, "modelOverride": {"all": S, "agents": {"a": OP}}})

        async def log(m):
            ctx.lines.append(m)
        ctx.log = log
        asyncio.run(agent.run(ctx))
        assert sent["modelOverride"] == {"agents": {"b": S}}
        sent.clear()
        ctx.state = {"outputs": {}}
        asyncio.run(agent.run(ctx))
        assert "modelOverride" not in sent


def test_the_dedicated_container_uses_the_model_it_is_sent(monkeypatch):
    shipped = json.loads((ORCH_ROOT / "app" / "workflow.json").read_text())
    shipped["orchestrator"] = {**shipped.get("orchestrator", {}), "models": {"overrides": [S]}}
    monkeypatch.setenv("AGENT_ID", "web_search")
    with workflow(shipped) as imp:
        sub = imp("app.subagent_runtime")
        mr = imp("app.common.model_routing")
        seen = []

        async def run(ctx):
            seen.append(mr.candidates(ctx, "s", "u")[0])
            return "{}"
        monkeypatch.setattr(sub._agent, "run", run)
        got = asyncio.run(sub.invoke({"agent_id": "web_search", "session_id": "s1", "topic": "t",
                                      "modelOverride": {"agents": {"web_search": S}}}))
        assert got.get("output") == "{}", got
        assert seen == [(S, "override", "the run's model override")]


def test_the_wiring_is_where_the_design_says():
    state = (ORCH_ROOT / "app" / "common" / "state.py").read_text()
    assert re.search(r"^\s+modelOverride: dict\s*$", state, re.MULTILINE)
    rt = (ORCH_ROOT / "app" / "orchestrator" / "runtime.py").read_text()
    assert rt.count("model_routing.from_payload(payload)") == 1
    loop = (ORCH_ROOT / "app" / "common" / "tool_loop.py").read_text()
    assert "model_routing.call(ctx, chain" in loop and "model=getattr(ctx, \"model\"" not in loop


# --- the BFF --------------------------------------------------------------------------------

MODULES = ("workflow", "authz", "chatbot", "handler", "builds", "buildstore", "accounts",
           "designer", "policies", "audit", "liveconfig", "model_override")


@pytest.fixture()
def make(monkeypatch):
    with moto.mock_aws():
        ddb = boto3.client("dynamodb", region_name="us-east-1")
        for k, v in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing",
                     "AWS_DEFAULT_REGION": "us-east-1", "AWS_REGION": "us-east-1",
                     "STATUS_TABLE": "console_status", "EVENTS_TABLE": "console_events",
                     "RUNTIME_ARN": "arn:aws:bedrock-agentcore:us-east-1:1:runtime/console",
                     "BUILDS_TABLE": "console_builds", "BUILDS_BUCKET": "console-builds",
                     "DEPLOY_PROJECT": "agentexpress-deploy",
                     "SECRETS_PREFIX": "agentexpress/console/builds/"}.items():
            monkeypatch.setenv(k, v)
        create_builds_table(ddb)
        _status_table(ddb, "console_status")
        _events_table(ddb, "console_events")
        boto3.client("s3").create_bucket(Bucket="console-builds")

        def build(wf: dict):
            monkeypatch.setenv("WORKFLOW_JSON", json.dumps(wf))
            for mod in MODULES:
                sys.modules.pop(mod, None)
            handler = importlib.import_module("handler")
            monkeypatch.setattr(handler.builds, "_codebuild", FakeCodeBuild())
            invoked: list[dict] = []
            monkeypatch.setattr(handler, "_self_invoke", lambda fn, p: invoked.append(p))
            monkeypatch.setattr(handler, "_models", lambda: {"models": []})
            return types.SimpleNamespace(handler=handler, invoked=invoked,
                                         status=boto3.resource("dynamodb").Table("console_status"))
        yield build
    for mod in MODULES:
        sys.modules.pop(mod, None)


def start(e, body, groups=("tuners",)):
    claims = {"sub": "u1", "email": "u1@example.com", "cognito:groups": list(groups)}
    event = {"requestContext": {"http": {"method": "POST"}, "authorizer": {"jwt": {"claims": claims}}},
             "rawPath": "/api/sessions", "routeKey": "POST /api/sessions", "body": json.dumps(body)}
    resp = e.handler.handler(event, CTX)
    return resp["statusCode"], json.loads(resp["body"])


def test_picking_models_needs_the_override_grant(make):
    e = make(_wf())
    status, body = start(e, {"topic": "t", "modelOverride": {"all": S}}, groups=("others",))
    assert status == 403 and "override" in body["error"] and e.invoked == []
    # Not listed at all is closed too, unlike start/decision/...: it changes what a run costs.
    e = make({**_wf(), "authorization": {"groupsClaim": "cognito:groups", "actions": {"decision": ["x"]}}})
    assert start(e, {"topic": "t", "modelOverride": {"all": S}})[0] == 403
    e = make({k: v for k, v in _wf().items() if k != "authorization"})
    assert start(e, {"topic": "t", "modelOverride": {"all": S}})[0] == 403
    assert start(e, {"topic": "t"})[0] == 200                          # a plain run is not held up


def test_a_granted_pick_is_stored_on_the_run_and_sent_on_start_only(make):
    e = make(_wf())
    status, body = start(e, {"topic": "t", "modelOverride": {"all": S, "agents": {"b": OP}}})
    assert status == 200, body
    item = e.status.get_item(Key={"session_id": body["session_id"]})["Item"]
    assert item["modelOverride"] == {"all": S, "agents": {"b": OP}}
    assert e.invoked[-1]["modelOverride"] == {"all": S, "agents": {"b": OP}}
    sent = {}
    e.handler.agentcore = types.SimpleNamespace(invoke_agent_runtime=lambda **kw: sent.update(kw))
    e.handler._run(e.invoked[-1])
    assert json.loads(sent["payload"])["modelOverride"] == {"all": S, "agents": {"b": OP}}
    e.handler._run({"action": "rerun_from", "session_id": "s1", "modelOverride": {"all": OP}})
    assert "modelOverride" not in json.loads(sent["payload"])


def test_a_rerun_the_runtime_refuses_is_logged_not_dropped(make, capsys):
    """POST /rerun answers at once and the worker invokes the runtime; a rewind it refuses
    (a step after an ungated parallel stage) changed nothing and left no trace anywhere."""
    e = make(_wf())
    answer = io.BytesIO(json.dumps({"error": "cannot rewind across the previous step"}).encode())
    e.handler.agentcore = types.SimpleNamespace(invoke_agent_runtime=lambda **kw: {"response": answer})
    e.handler._run({"action": "rerun_from", "session_id": "s1", "agent_id": "b"})
    assert "re-run refused for s1: cannot rewind across the previous step" in capsys.readouterr().out


def test_a_run_without_a_pick_carries_nothing(make):
    e = make(_wf())
    status, body = start(e, {"topic": "t"})
    item = e.status.get_item(Key={"session_id": body["session_id"]})["Item"]
    assert status == 200 and "modelOverride" not in item and "modelOverride" not in e.invoked[-1]


@pytest.mark.parametrize("pick,words", [
    ({"all": H}, "is not one of this workflow's override models"),
    ({"agents": {"a": "x" * 500}}, "agents.a: that model is not one of"),
    ({"agents": {"zz": S}}, "agents.zz: no such agent"),
    ({"agents": {"a-b": S}}, "letters, digits and underscores"),
    ({"agents": {"r": S}}, "a a2a agent keeps its own model"),
    ({"agents": {"h": S}}, "a harness agent keeps its own model"),
    ({"model": S}, "modelOverride must be"),
    ({"agents": []}, "modelOverride must be"),
    ("S", "modelOverride must be"),
])
def test_anything_outside_the_list_is_refused(make, pick, words):
    e = make(_wf())
    status, body = start(e, {"topic": "t", "modelOverride": pick})
    assert status == 400 and words in body["error"] and e.invoked == []


def test_no_list_or_a_build_takes_no_override(make):
    e = make({**_wf(), "orchestrator": {}})
    status, body = start(e, {"topic": "t", "modelOverride": {"all": S}})
    assert status == 400 and "offers no model override" in body["error"]
    e = make(_wf())
    e.handler._build_target = lambda *_a, **_k: e.handler._default_target()   # a build that resolves
    status, body = start(e, {"topic": "t", "build": "b1", "modelOverride": {"all": S}})
    assert status == 400 and "build's own app" in body["error"] and e.invoked == []


def test_the_page_is_told_the_models_and_nothing_else_new(make):
    e = make(_wf())
    assert e.handler.WORKFLOW["modelOverrides"] == [S, OP]
    assert make({**_wf(), "orchestrator": {}}).handler.WORKFLOW["modelOverrides"] is None
