"""AgentCore Harness agents (runtime "harness", app/common/harness_agent.py).

What has to hold:
  * the step's request to InvokeHarness is one the service accepts (checked against the
    installed botocore model): the harness ARN, a 33-100 character session id, one user
    message carrying the request and the earlier outputs, and allowedTools — no tools at all
    unless `harness.shell`, the built-in ones with it;
  * the stream is read as the service sends it (events checked against the botocore stream
    shape): the last assistant message is the answer, tokens are metered, an error event or
    a stop before end_turn fails the step, a stop by the user cancels it;
  * the registry builds a HarnessAgent for runtime "harness" and refuses one it cannot run;
  * Terraform and CDK build the same harness: same allowedTools literals, memory disabled,
    the same execution-role statements, the orchestrator allowed to invoke only these, and
    HARNESS_ARNS only when a harness agent exists.
"""

from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path

import botocore.session
import botocore.validate
import pytest

from app.common import harness_agent as ha

ORCH = Path(__file__).resolve().parents[1]
ARN = "arn:aws:bedrock-agentcore:us-east-1:123456789012:harness/app_writer-AbCdEf1234"
_SESSION = botocore.session.get_session()
_DATA = _SESSION.get_service_model("bedrock-agentcore")


def _check_request(kw: dict) -> None:
    botocore.validate.validate_parameters(kw, _DATA.operation_model("InvokeHarness").input_shape)


def _check_events(events: list[dict]) -> None:
    """Each event is one member of the InvokeHarness stream union, shaped as botocore says."""
    stream = _DATA.operation_model("InvokeHarness").output_shape.members["stream"]
    for event in events:
        assert len(event) == 1 and next(iter(event)) in stream.members, event
        report = botocore.validate.ParamValidator().validate(event, stream)
        assert not report.has_errors(), report.generate_report()


def _events(*answers: str, stop: str = "end_turn", tokens=(120, 30)) -> list[dict]:
    """A stream as the harness sends it: one assistant message per answer, a tool call
    between them, then the final stop and the usage."""
    out: list[dict] = []
    for i, text in enumerate(answers):
        # A text block has no contentBlockStart: that union holds only toolUse and toolResult.
        out += [{"messageStart": {"role": "assistant"}},
                {"contentBlockDelta": {"contentBlockIndex": 0, "delta": {"text": text[: len(text) // 2]}}},
                {"contentBlockDelta": {"contentBlockIndex": 0, "delta": {"text": text[len(text) // 2:]}}},
                {"contentBlockStop": {"contentBlockIndex": 0}}]
        if i < len(answers) - 1:
            out += [{"contentBlockStart": {"contentBlockIndex": 1, "start": {"toolUse": {
                        "toolUseId": "t1", "name": "shell", "type": "server_tool_use"}}}},
                    {"contentBlockDelta": {"contentBlockIndex": 1, "delta": {"toolUse": {"input": "{\"command\": \"ls\"}"}}}},
                    {"contentBlockStop": {"contentBlockIndex": 1}},
                    {"messageStop": {"stopReason": "tool_use"}},
                    {"messageStart": {"role": "user"}},
                    {"contentBlockDelta": {"contentBlockIndex": 0, "delta": {"toolResult": [{"text": "out.csv"}]}}},
                    {"messageStop": {"stopReason": "tool_result"}}]
    out += [{"messageStop": {"stopReason": stop}},
            {"metadata": {"usage": {"inputTokens": tokens[0], "outputTokens": tokens[1],
                                    "totalTokens": sum(tokens)}, "metrics": {"latencyMs": 900}}}]
    _check_events(out)
    return out


class Stream(list):
    closed = False

    def close(self):
        self.closed = True


class FakeClient:
    def __init__(self, events):
        self.events, self.calls = events, []

    def invoke_harness(self, **kw):
        _check_request(kw)
        self.calls.append(kw)
        self.stream = Stream(self.events)
        return {"stream": self.stream}


class Ctx:
    def __init__(self, outputs=None):
        self.topic = "draft the summary"
        self.session_id = "run-7f3a"
        self.agent_id = "writer"
        self.feedback = ""
        self.recalled_memory = []
        self.state = {"outputs": dict(outputs or {})}
        self.logs: list[str] = []

    async def log(self, msg):
        self.logs.append(msg)

    def input(self, agent_id):
        return self.state["outputs"].get(agent_id, "")


@pytest.fixture
def wired(monkeypatch):
    """A HarnessAgent with a deployed harness, a fake client and a recording meter."""
    from app.features.observability import meter
    metered: list[dict] = []
    monkeypatch.setattr(meter, "record_llm", lambda **kw: metered.append(kw))
    monkeypatch.setattr(ha, "_ARNS", {"writer": ARN})

    def make(events, spec=None):
        client = FakeClient(events)
        monkeypatch.setattr(ha, "_client", lambda timeout: client)
        agent = ha.HarnessAgent()
        agent.id, agent.name, agent.model, agent.temperature, agent.max_tokens = "writer", "Writer", None, 0, 4000
        agent.use({"runtime": "harness", "instructions": "Write it.", **(spec or {})})
        return agent, client
    make.metered = metered
    return make


def run(agent, ctx):
    return asyncio.run(agent.run(ctx))


# --- the request ------------------------------------------------------------------------

def test_the_request_is_one_the_service_accepts_and_allows_no_tool(wired):
    agent, client = wired(_events('{"draft": "ok"}'))
    out = run(agent, Ctx({"intake": '{"objective": "x"}'}))
    assert out == '{"draft": "ok"}'
    (kw,) = client.calls
    assert kw["harnessArn"] == ARN
    assert kw["allowedTools"] == [ha.NO_TOOLS]
    assert 33 <= len(kw["runtimeSessionId"]) <= 100
    assert re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9-_]*", kw["runtimeSessionId"])
    sent = json.loads(kw["messages"][0]["content"][0]["text"])
    assert kw["messages"][0]["role"] == "user"
    assert sent == {"request": "draft the summary", "upstreamOutputs": {"intake": '{"objective": "x"}'}}
    assert client.stream.closed


def test_shell_allows_the_built_in_tools_and_says_so(wired):
    agent, client = wired(_events("done"), {"harness": {"shell": True, "timeoutSeconds": 120}})
    ctx = Ctx()
    run(agent, ctx)
    assert client.calls[0]["allowedTools"] == [ha.BUILTIN_TOOLS]
    assert "shell and files allowed" in ctx.logs[0]


def test_allowed_tools_are_exactly_what_the_entry_names_in_a_fixed_order():
    spec = {"runtime": "harness", "tool": ["docs", "Pricing"], "harness": {
        "shell": True, "mcp": [{"name": "wiki", "url": "https://w.example"}], "browser": True,
        "codeInterpreter": "arn:aws:bedrock-agentcore:us-east-1:123456789012:code-interpreter-custom/ci-AbCdEf1234",
        "skills": [{"aws": []}]}}
    assert ha.allowed_tools(spec) == [
        "@builtin", "@gateway/docs___*", "@gateway/Pricing___*", "@gateway/pricing___*", "@wiki",
        "browser", "@browser", "code_interpreter", "@code_interpreter", "skills", "@builtin/skills"]
    assert ha.allowed_tools({"runtime": "harness"}) == [ha.NO_TOOLS]
    # A harness you bring keeps its own tools.
    assert ha.allowed_tools({"runtime": "harness", "harness": {"arn": ARN}}) is None
    # Each pattern is one botocore accepts (HarnessAllowedTool: 1-64 characters, its pattern).
    shape = _SESSION.get_service_model("bedrock-agentcore-control").shape_for("HarnessAllowedTool")
    for pattern in ha.allowed_tools(spec):
        assert re.fullmatch(shape.metadata["pattern"], pattern) and len(pattern) <= shape.metadata["max"]


def test_gateway_tools_are_called_with_their_patterns_and_an_endpoint_as_the_qualifier(wired):
    agent, client = wired(_events("done"), {"tool": "docs", "harness": {"endpoint": "live"}})
    ctx = Ctx()
    run(agent, ctx)
    (kw,) = client.calls
    assert kw["allowedTools"] == ["@gateway/docs___*"] and kw["qualifier"] == "live"
    assert "actorId" not in kw  # no memory on the harness, nothing to scope
    assert "at endpoint live" in ctx.logs[0]


def test_a_harness_you_bring_is_called_at_its_arn_with_its_own_tools_and_a_per_user_actor(wired, monkeypatch):
    other = "arn:aws:bedrock-agentcore:us-east-1:123456789012:harness/research_agent-ZyXwVu9876"
    monkeypatch.setattr(ha, "_ARNS", {})
    agent, client = wired(_events("done"), {"harness": {"arn": other, "endpoint": "prod", "maxIterations": 4, "timeoutSeconds": 60}})
    ctx = Ctx()
    ctx.user = "Ana@Example.com"
    run(agent, ctx)
    (kw,) = client.calls
    assert kw["harnessArn"] == other and kw["qualifier"] == "prod"
    assert "allowedTools" not in kw and (kw["maxIterations"], kw["timeoutSeconds"]) == (4, 60)
    import hashlib
    assert kw["actorId"] == "writer-u" + hashlib.sha256(b"ana@example.com").hexdigest()[:10]
    # Its model is not known here: tokens recorded, no price claimed.
    (m,) = wired.metered
    assert m["model"] == "harness:research_agent-ZyXwVu9876" and (m["cost_usd"], m["rates_source"]) == (0, "none")


def test_another_providers_model_is_metered_with_no_price_and_memory_scopes_the_actor(wired, monkeypatch):
    from app.common import config
    monkeypatch.setattr(config, "WORKFLOW", {"memories": {"notes": {"strategies": ["semantic"], "scope": "agent"}}})
    agent, client = wired(_events("done"), {"harness": {"provider": "openai", "apiKey": "oa", "memory": "notes"}})
    agent.model = "gpt-5.4"
    run(agent, Ctx())
    assert client.calls[0]["actorId"] == "writer"  # the memory's scope "agent": shared by everyone
    (m,) = wired.metered
    assert (m["model"], m["cost_usd"], m["rates_source"]) == ("gpt-5.4", 0, "none")


def test_a_session_id_is_padded_cleaned_and_stable_per_run_and_agent():
    sid = ha.session_id("_a b/c", "writer")
    assert re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9-_]{32,99}", sid)
    assert sid == ha.session_id("_a b/c", "writer") != ha.session_id("_a b/c", "other")
    assert len(ha.session_id("x" * 200, "writer")) == 100


# --- the stream -------------------------------------------------------------------------

def test_the_last_assistant_message_is_the_answer_and_the_call_is_metered(wired):
    agent, _ = wired(_events("Let me list the files.", '{"draft": "final"}', tokens=(500, 80)))
    ctx = Ctx()
    assert run(agent, ctx) == '{"draft": "final"}'
    (m,) = wired.metered
    assert (m["input_tokens"], m["output_tokens"], m["mode"], m["finish_reason"]) == (500, 80, "harness", "end_turn")
    assert m["system_text"] == "Write it." and m["latency_ms"] == 900
    assert "Writer: used shell" in ctx.logs


@pytest.mark.parametrize("stop, why", [
    ("max_iterations_exceeded", "harness.maxIterations"),
    ("timeout_exceeded", "harness.timeoutSeconds"),
    ("tool_use", "a tool this deployment does not run"),
    ("malformed_model_output", "stopped with malformed_model_output"),
])
def test_a_stop_before_end_turn_fails_the_step_and_says_why(wired, stop, why):
    agent, _ = wired(_events("half an answer", stop=stop))
    with pytest.raises(ha.HarnessFailed, match=why):
        run(agent, Ctx())


def test_an_error_event_fails_the_step_with_the_service_words(wired):
    events = [{"messageStart": {"role": "assistant"}},
              {"runtimeClientError": {"message": "session could not start"}}]
    _check_events(events)
    agent, client = wired(events)
    with pytest.raises(ha.HarnessFailed, match="runtimeClientError: session could not start"):
        run(agent, Ctx())
    assert client.stream.closed


def test_no_text_is_not_an_answer(wired):
    agent, _ = wired([{"messageStart": {"role": "assistant"}}, {"messageStop": {"stopReason": "end_turn"}}])
    with pytest.raises(ha.HarnessFailed, match="returned no text"):
        run(agent, Ctx())


def test_a_stop_by_the_user_closes_the_stream_and_cancels(monkeypatch):
    from app.common.sink import WorkflowCancelled
    monkeypatch.setattr(ha, "_CANCEL_CHECK_SECONDS", -1)
    stream = Stream(_events("one", "two"))
    got = ha.read_stream(stream, lambda: True)
    assert got["stop"] == "cancelled" and stream.closed
    assert WorkflowCancelled  # the class run() raises for this stop


def test_no_deployed_harness_fails_loudly(monkeypatch):
    monkeypatch.setattr(ha, "_ARNS", {})
    agent = ha.HarnessAgent().use({"runtime": "harness", "instructions": "x"})
    agent.id = "writer"
    with pytest.raises(ha.HarnessFailed, match="no harness was deployed"):
        run(agent, Ctx())


# --- the registry -----------------------------------------------------------------------

def _wf(writer: dict, **intake) -> dict:
    return {"orchestrator": {"defaultModel": "m"}, "tools": {"docs": {"type": "mcp", "endpoint": "https://x.example/mcp"}},
            "agents": {"intake": {"name": "I", "runtime": "dedicated", "maxTokens": 100, **intake},
                       "writer": {"name": "W", "runtime": "harness", "produces": "draft", **writer}},
            "steps": [{"agent": "intake"}, {"agent": "writer"}]}


def test_the_registry_builds_a_harness_agent_from_its_entry():
    from conftest import workflow
    with workflow(_wf({"instructions": "Write.", "harness": {"shell": True, "maxIterations": 5}})) as imp:
        agents = imp("app.orchestrator.registry").load_agents()
        writer = agents["writer"]
        assert type(writer).__name__ == "HarnessAgent"
        assert (writer.instructions, writer.limits, writer.tools_allowed) == ("Write.", {"shell": True, "maxIterations": 5}, ["@builtin"])


def test_a_json_answer_gets_the_asset_envelope_of_its_produces(monkeypatch):
    from conftest import workflow
    with workflow(_wf({"instructions": "Write."})) as imp:
        mod = imp("app.common.harness_agent")
        meter = imp("app.features.observability.meter")
        monkeypatch.setattr(meter, "record_llm", lambda **kw: None)
        monkeypatch.setattr(mod, "_ARNS", {"writer": ARN})
        monkeypatch.setattr(mod, "_client", lambda timeout: FakeClient(_events('{"draft": "ok"}')))
        writer = imp("app.orchestrator.registry").load_agents()["writer"]
        asset = json.loads(run(writer, Ctx()))
        assert (asset["assetType"], asset["createdByAgent"], asset["version"], asset["draft"]) == ("draft", "writer", 1, "ok")


@pytest.mark.parametrize("writer, intake, why", [
    ({}, {}, "needs `instructions`"),
    ({"instructions": "  "}, {}, "needs `instructions`"),
    ({"instructions": "x", "corpus": "docs"}, {}, "also corpus"),
    ({"instructions": "x", "harness": {"maxIterations": 0}}, {}, "from 1 to 100"),
    ({"instructions": "x", "harness": {"timeoutSeconds": 901}}, {}, "from 1 to 900"),
    ({"instructions": "x", "harness": {"shell": "yes"}}, {}, "true or false"),
    ({"instructions": "x", "harness": {"hooks": []}}, {}, "not a setting"),
    ({"instructions": "x", "harness": {"provider": "azure"}}, {}, "harness.provider must be one of"),
    ({"instructions": "x", "harness": {"endpoint": "DEFAULT"}}, {}, "not DEFAULT"),
    ({"instructions": "x", "harness": {"mcp": [{"url": "https://x.example"}]}}, {}, "harness.mcp[0] needs a `name`"),
    ({"instructions": "x", "harness": {"memory": "notes"}, "agentcore": {"memory": {"longTerm": True}}}, {}, "Keep one"),
    ({"harness": {"arn": "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/x"}}, {}, "must be a harness ARN"),
    ({"instructions": "x", "harness": {"arn": ARN, "shell": True}}, {}, "has its own instructions, harness.shell"),
    ({"instructions": "x"}, {"instructions": "y"}, "read only for runtime \"harness\""),
])
def test_the_registry_refuses_a_harness_agent_it_cannot_run(writer, intake, why):
    from conftest import workflow
    with workflow(_wf(writer, **intake)) as imp, pytest.raises(ValueError, match=re.escape(why)):
        imp("app.orchestrator.registry").load_agents()


# --- the two IaC paths ------------------------------------------------------------------

TF = (ORCH / "terraform" / "harness.tf").read_text()
TS = (ORCH / "cdk" / "lib" / "harness.ts").read_text()


def test_both_paths_allow_the_same_tools_and_disable_memory():
    for literal in (ha.NO_TOOLS, ha.BUILTIN_TOOLS, *ha.SKILLS_TOOLS):
        assert f'"{literal}"' in TF and f'"{literal}"' in TS
    for name in (ha.GATEWAY_TOOL, ha.BROWSER_TOOL, ha.CODE_TOOL):
        assert f'name = "{name}"' in TF and f'name: "{name}"' in TS
    # Memory is off unless harness.memory names one of the build's memories.
    assert 'dynamic "disabled" {\n      for_each = local.harness_spec[each.key].memory == "" ? [1] : []' in TF
    assert "{ agentCoreMemoryConfiguration: { arn: memoryArn(s.memory) } } : { disabled: {} }" in TS
    assert "allowed_tools   = local.harness_tools[each.key]" in TF and "allowedTools: harnessTools(a)" in TS


def test_both_paths_grant_the_same_statements():
    tf_sids = re.findall(r'Sid\s+= "(\w+)"', TF)
    # harness.ts writes the base statements (and the orchestrator's) as `sid:` and the
    # optional ones through statement(...); harness.tf declares the optional ones (a local)
    # above the base policy. Same names, same order within each.
    base = re.findall(r'sid: "(\w+)"', TS)
    extra = re.findall(r'statement\("(\w+)"', TS)
    assert len(extra) == 14 and tf_sids == extra + base
    for action in ("bedrock:InvokeModel", "ecr-public:GetAuthorizationToken", "sts:GetServiceBearerToken",
                   "bedrock-agentcore:InvokeHarness", "bedrock-agentcore:InvokeAgentRuntime",
                   "bedrock-agentcore:GetWorkloadAccessTokenForJWT", "logs:PutResourcePolicy",
                   "bedrock-agentcore:GetResourceOauth2Token", "bedrock-agentcore:GetResourceApiKey",
                   "bedrock-agentcore:StartBrowserSession", "bedrock-agentcore:InvokeCodeInterpreter",
                   "bedrock-agentcore:RetrieveMemoryRecords", "elasticfilesystem:ClientMount", "s3files:ClientMount",
                   "ecr:BatchGetImage", "ecr:GetAuthorizationToken"):
        assert f'"{action}"' in TF and f'"{action}"' in TS
    # The Gateway is reached with an OAuth2 token, not SigV4 (it takes only the build's JWTs).
    assert "InvokeGateway" not in TF and "InvokeGateway" not in TS


def test_the_orchestrator_gets_harness_arns_only_when_there_is_a_harness():
    main_tf = (ORCH / "terraform" / "main.tf").read_text()
    stack = (ORCH / "cdk" / "lib" / "orchestrator-stack.ts").read_text()
    # One of the maps merged into the orchestrator Runtime's environment_variables (with
    # export.tf's after it, so the call no longer ends at harness_env).
    assert re.search(r"environment_variables = merge\(\{.*?\}, [^\n]*\blocal\.harness_env\b", main_tf, re.DOTALL)
    assert "harness_env = length(local.harness_agents) > 0 ?" in TF
    assert "...(Object.keys(harnessPlane.arns).length ? { HARNESS_ARNS:" in stack


def test_the_console_deploy_role_may_create_harnesses_and_pass_their_roles():
    doc = json.loads((ORCH / "terraform" / "deploy-role-policy.json").read_text())
    actions = {a for s in doc["Statement"] for a in (s["Action"] if isinstance(s["Action"], list) else [s["Action"]])}
    assert {"bedrock-agentcore:CreateHarness", "bedrock-agentcore:GetHarness", "bedrock-agentcore:UpdateHarness",
            "bedrock-agentcore:DeleteHarness"} <= actions
    passrole = next(s for s in doc["Statement"] if s["Sid"] == "IamPassRoleToServices")
    assert "arn:aws:iam::*:role/AgentCoreHarness-*" in passrole["Resource"]


# --- the BFF ----------------------------------------------------------------------------

def test_the_bff_labels_a_harness_agent_and_refuses_to_live_edit_it(monkeypatch):
    import sys
    sys.path.insert(0, str(ORCH / "bff"))
    try:
        import liveconfig as bff_live
        import workflow as bff_workflow
        assert bff_workflow.data_source({"runtime": "harness"}, {}) == "AgentCore Harness"
        assert bff_workflow.data_source({"runtime": "harness", "harness": {"shell": True}}, {}) == "AgentCore Harness \u00b7 shell"
        raw = {"agents": {"writer": {"name": "W", "runtime": "harness", "instructions": "x"}}}
        assert bff_live.editable(raw) == {}
        issues = bff_live.check(raw, {"writer": {"systemPrompt": "y"}}, "us-east-1", "123456789012")
        assert any("AgentCore Harness" in str(i) for i in issues)
    finally:
        sys.path.remove(str(ORCH / "bff"))
