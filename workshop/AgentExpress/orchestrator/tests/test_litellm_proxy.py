"""Non-Bedrock models through a LiteLLM proxy (app/common/litellm_proxy.py).

An agent whose model is `litellm/<name>` is answered by the deployment's LiteLLM proxy over
its OpenAI-compatible /v1/chat/completions; everything else is Bedrock as before. These
tests stand a fake proxy in for netguard.urlopen (the one way the module fetches) and a fake
Secrets Manager (inputs checked against botocore's GetSecretValue shape), and call the REAL
run_llm / run_llm_tools. No AWS, no network.
"""
from __future__ import annotations

import asyncio
import io
import json
import re
import socket
import sys
import types
import urllib.error
from decimal import Decimal
from pathlib import Path

import pytest

ORCH = Path(__file__).resolve().parent.parent
BASE = "https://litellm.example.com"
ARN = "arn:aws:secretsmanager:eu-west-1:210987654321:secret:litellm-key-AbC123"
KEY = "sk-virtual-0123456789abcdef"


def _mods():
    from app.common import litellm_proxy, llm
    return llm, litellm_proxy


class FakeSecrets:
    def __init__(self, value: str = KEY):
        self.value, self.calls = value, []

    def get_secret_value(self, **kw):
        from botocore.session import get_session
        from botocore.validate import validate_parameters
        validate_parameters(kw, get_session().get_service_model("secretsmanager")
                            .operation_model("GetSecretValue").input_shape)
        self.calls.append(kw)
        return {"ARN": kw["SecretId"], "SecretString": self.value}


class _Resp:
    def __init__(self, body: dict, headers: dict | None = None):
        self._raw = json.dumps(body).encode()
        self.headers = {"content-type": "application/json", **(headers or {})}

    def read(self, n=-1):
        return self._raw

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def completion(text="Hello", finish="stop", tool_calls=None, prompt=11, out=7):
    return {"id": "chatcmpl-1", "object": "chat.completion", "model": "gpt-4o",
            "choices": [{"index": 0, "finish_reason": finish,
                         "message": {"role": "assistant", "content": text, "tool_calls": tool_calls}}],
            "usage": {"prompt_tokens": prompt, "completion_tokens": out, "total_tokens": prompt + out}}


class FakeProxy:
    """netguard.urlopen, answering from a script: each item is a dict (a 200 body), a
    (dict, headers) pair, an int (that HTTP error) or an exception to raise."""

    def __init__(self, *script):
        self.script, self.requests = list(script), []

    def __call__(self, req, timeout=10, allow_private=False):
        self.requests.append({"url": req.full_url, "headers": dict(req.header_items()),
                              "body": json.loads(req.data), "timeout": timeout,
                              "allow_private": allow_private})
        item = self.script.pop(0)
        if isinstance(item, BaseException):
            raise item
        if isinstance(item, tuple) and isinstance(item[0], int):
            code, body = item
            raise urllib.error.HTTPError(req.full_url, code, "err", {}, io.BytesIO(json.dumps(body).encode()))
        if isinstance(item, int):
            raise urllib.error.HTTPError(req.full_url, item, "err", {}, io.BytesIO(b'{"error": {"message": "busy"}}'))
        body, headers = item if isinstance(item, tuple) else (item, {})
        return _Resp(body, headers)


@pytest.fixture
def proxy(monkeypatch):
    """LiteLLM on, with a fake proxy, secret and telemetry store. Returns a namespace."""
    llm, lp = _mods()
    monkeypatch.setenv(lp.BASE_URL_ENV, BASE)
    monkeypatch.setenv(lp.KEY_SECRET_ENV, ARN)
    monkeypatch.setattr(lp, "_key", None)
    secrets = FakeSecrets()
    regions: list = []

    def client(service, region_name=None):
        assert service == "secretsmanager"
        regions.append(region_name)
        return secrets
    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(client=client))
    monkeypatch.setattr(lp.asyncio, "sleep", _no_sleep)
    rows: list = []
    from app.features.observability import meter
    monkeypatch.setattr(meter, "store", types.SimpleNamespace(put=rows.append, TELEMETRY_TABLE="t"))
    ns = types.SimpleNamespace(llm=llm, lp=lp, secrets=secrets, regions=regions, rows=rows)

    def script(*items):
        fake = FakeProxy(*items)
        monkeypatch.setattr(lp.netguard, "urlopen", fake)
        ns.fake = fake
        return fake
    ns.script = script
    return ns


async def _no_sleep(_s):
    return None


def run(coro):
    return asyncio.run(coro)


# --- routing and the request ----------------------------------------------------------------

def test_a_litellm_model_goes_to_the_proxy_with_the_openai_request(proxy):
    fake = proxy.script(completion("Paris."))
    text, truncated = run(proxy.llm.run_llm("plan", "You plan.", "Capital of France?",
                                            model="litellm/gpt-4o", temperature=0.2, max_tokens=500,
                                            top_p=0.9, stop_sequences=["END"]))
    assert (text, truncated) == ("Paris.", False)
    [req] = fake.requests
    assert req["url"] == f"{BASE}/v1/chat/completions"
    assert req["headers"]["Authorization"] == f"Bearer {KEY}"
    assert req["allow_private"] is False            # the SSRF guard's address check holds
    assert req["body"] == {"model": "gpt-4o", "max_tokens": 500, "temperature": 0.2, "top_p": 0.9,
                           "stop": ["END"],
                           "messages": [{"role": "system", "content": "You plan."},
                                        {"role": "user", "content": "Capital of France?"}]}


def test_a_bedrock_model_never_touches_the_proxy(proxy, monkeypatch):
    fake = proxy.script()
    calls = []

    class _Msg:
        content, response_metadata, usage_metadata = "ok", {"stopReason": "end_turn"}, {}

    class FakeConverse:
        def __init__(self, **kw):
            calls.append(kw["model"])

        async def ainvoke(self, _m):
            return _Msg()
    monkeypatch.setitem(sys.modules, "langchain_aws", types.SimpleNamespace(ChatBedrockConverse=FakeConverse))
    assert run(proxy.llm.run_llm("a", "s", "u", model="us.anthropic.claude-haiku-4-5-20251001-v1:0"))[0] == "ok"
    assert calls and fake.requests == [] and proxy.secrets.calls == []


def test_off_by_default_a_litellm_model_fails_clearly_and_calls_nothing(monkeypatch):
    llm, lp = _mods()
    monkeypatch.delenv(lp.BASE_URL_ENV, raising=False)
    monkeypatch.delenv(lp.KEY_SECRET_ENV, raising=False)
    fake = FakeProxy()
    monkeypatch.setattr(lp.netguard, "urlopen", fake)
    from app.common.errors import ModelUnavailable
    with pytest.raises(ModelUnavailable, match="LiteLLM is not set up on this deployment"):
        run(llm.run_llm("a", "s", "u", model="litellm/gpt-4o"))
    assert fake.requests == []
    assert lp.configured() is False


def test_a_finish_reason_of_length_is_reported_as_truncated(proxy):
    proxy.script(completion('{"a": ', finish="length"))
    assert run(proxy.llm.run_llm("a", "s", "u", model="litellm/gpt-4o"))[1] is True


def test_images_and_files_become_openai_content_parts(proxy):
    fake = proxy.script(completion())
    images = [{"key": "runs/s/img.png", "format": "png", "bytes": b"\x89PNG"}]
    files = [{"kind": "document", "format": "csv", "name": "q3.csv", "bytes": b"a,b\n1,2"},
             {"kind": "document", "format": "pdf", "name": "plan.pdf", "bytes": b"%PDF"},
             {"kind": "image", "format": "jpeg", "name": "p.jpg", "bytes": b"\xff\xd8"}]
    run(proxy.llm.run_llm("a", "s", "Read these", model="litellm/gpt-4o", images=images, files=files))
    parts = fake.requests[0]["body"]["messages"][1]["content"]
    assert parts == [
        {"type": "text", "text": "Read these"},
        {"type": "image_url", "image_url": {"url": "data:image/png;base64,iVBORw=="}},
        {"type": "text", "text": "--- FILE: q3.csv ---\na,b\n1,2"},
        {"type": "file", "file": {"filename": "plan.pdf", "file_data": "data:application/pdf;base64,JVBERg=="}},
        {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64,/9g="}},
    ]


# --- tool calling ---------------------------------------------------------------------------

def test_tool_calls_round_trip_in_openai_format(proxy):
    from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
    call = {"id": "call_1", "type": "function", "function": {"name": "kb___retrieve", "arguments": '{"query": "q3"}'}}
    fake = proxy.script(completion(None, finish="tool_calls", tool_calls=[call]), completion("Done."))
    tools = [{"name": "kb___retrieve", "description": "Search", "input_schema": {"type": "object"}}]
    history = [HumanMessage(content="Find Q3")]
    ai = run(proxy.llm.run_llm_tools("a.tools", "SYS", history, tools, model="litellm/gpt-4o", max_tokens=900))
    assert ai.tool_calls == [{"name": "kb___retrieve", "args": {"query": "q3"}, "id": "call_1", "type": "tool_call"}]
    first = fake.requests[0]["body"]
    assert first["tools"] == [{"type": "function", "function": {"name": "kb___retrieve", "description": "Search",
                                                                 "parameters": {"type": "object"}}}]
    assert first["messages"] == [{"role": "system", "content": "SYS"}, {"role": "user", "content": "Find Q3"}]
    assert first["max_tokens"] == 900
    # The loop appends the AIMessage and the tool's result; the next turn carries both back.
    history += [ai, ToolMessage(content="Q3 was up 4%", tool_call_id="call_1")]
    again = run(proxy.llm.run_llm_tools("a.tools", "SYS", history, tools, model="litellm/gpt-4o"))
    assert again.tool_calls == [] and again.content == "Done."
    assert fake.requests[1]["body"]["messages"][2:] == [
        {"role": "assistant", "content": None, "tool_calls": [
            {"id": "call_1", "type": "function",
             "function": {"name": "kb___retrieve", "arguments": '{"query": "q3"}'}}]},
        {"role": "tool", "tool_call_id": "call_1", "content": "Q3 was up 4%"}]
    assert isinstance(AIMessage(content=again.content), AIMessage)


def test_a_tool_call_with_arguments_that_are_not_a_json_object_is_never_made(proxy):
    bad = [{"id": "c1", "type": "function", "function": {"name": "t", "arguments": "{not json"}},
           {"id": "c2", "type": "function", "function": {"name": "t", "arguments": "[1, 2]"}},
           {"id": "c3", "type": "function", "function": {"name": "t", "arguments": ""}}]
    proxy.script(completion(None, finish="tool_calls", tool_calls=bad))
    ai = run(proxy.llm.run_llm_tools("a.tools", "S", [], [], model="litellm/gpt-4o"))
    # An empty arguments string is the model calling with no arguments: {}.
    assert ai.tool_calls == [{"name": "t", "args": {}, "id": "c3", "type": "tool_call"}]
    assert [c["id"] for c in ai.invalid_tool_calls] == ["c1", "c2"]


def test_the_tool_loop_runs_on_a_litellm_model(proxy, monkeypatch):
    """tool_loop.gather, unchanged, on a litellm/ model: a call, its result, then the end."""
    from app.common import tool_loop
    call = {"id": "call_9", "type": "function",
            "function": {"name": "kb___retrieve", "arguments": '{"query": "rates"}'}}
    fake = proxy.script(completion(None, finish="tool_calls", tool_calls=[call]), completion("Found it."))

    async def specs(_ctx, _keys):
        return ([{"name": "kb___retrieve", "description": "d", "input_schema": {"type": "object"}}],
                {"kb___retrieve": ("kb", "kb", None)})

    async def call_tool(_ctx, route, args):
        return f"rates are 4% ({args['query']})", "kb"
    monkeypatch.setattr(tool_loop, "_specs", specs)
    monkeypatch.setattr(tool_loop, "_call", call_tool)
    monkeypatch.setattr(tool_loop, "is_cancelled", lambda _s: False)
    monkeypatch.setattr(tool_loop, "TOOLS", {"kb": {"type": "kb"}})
    from app.common import asks
    monkeypatch.setattr(asks, "ask_spec", lambda _a: None)
    monkeypatch.setattr(asks, "journaling", lambda _c: False)

    async def once(_ctx, fn):
        return await fn()
    monkeypatch.setattr(asks, "once", once)
    logs = []

    async def log(m):
        logs.append(m)
    ctx = types.SimpleNamespace(agent_id="research", agent_name="Research", session_id="s1",
                                topic="What are rates?", model="litellm/gpt-4o", max_tokens=1500,
                                max_tool_calls=3, skills=[], feedback="", agent_prompt="", log=log,
                                input=lambda _a: None, _retrieval_query=lambda: "rates")
    evidence, limits = run(tool_loop.gather(ctx, "rates", tools=["kb"]))
    assert "rates are 4% (rates)" in evidence and limits == []
    assert [r["body"]["model"] for r in fake.requests] == ["gpt-4o", "gpt-4o"]
    assert fake.requests[1]["body"]["messages"][-1] == {
        "role": "tool", "tool_call_id": "call_9", "content": "rates are 4% (rates)"}


# --- errors, retries, timeouts, cancellation --------------------------------------------------

def test_a_busy_proxy_is_retried_once(proxy):
    fake = proxy.script(503, completion("ok"))
    assert run(proxy.llm.run_llm("a", "s", "u", model="litellm/gpt-4o"))[0] == "ok"
    assert len(fake.requests) == 2


def test_two_failures_fail_the_call_and_record_an_error_row(proxy):
    from app.common.errors import ModelUnavailable
    fake = proxy.script(429, 429)
    with pytest.raises(ModelUnavailable, match="HTTP 429"):
        run(proxy.llm.run_llm("a", "s", "u", model="litellm/gpt-4o"))
    assert len(fake.requests) == 2
    assert [(r.mode, r.status, r.cost_usd) for r in proxy.rows] == [("error", "error", Decimal(0))]


def test_a_refusal_is_not_retried(proxy):
    from app.common.errors import ModelUnavailable
    fake = proxy.script((400, {"error": {"message": "Invalid model name passed in model=gpt-9"}}))
    with pytest.raises(ModelUnavailable, match="Invalid model name"):
        run(proxy.llm.run_llm("a", "s", "u", model="litellm/gpt-9"))
    assert len(fake.requests) == 1


@pytest.mark.parametrize("error", [TimeoutError("timed out"), urllib.error.URLError(ConnectionResetError("reset"))])
def test_a_timeout_or_dropped_connection_is_retried_once(proxy, error):
    fake = proxy.script(error, completion("ok"))
    assert run(proxy.llm.run_llm("a", "s", "u", model="litellm/gpt-4o"))[0] == "ok"
    assert len(fake.requests) == 2
    assert fake.requests[0]["timeout"] == proxy.lp.READ_TIMEOUT_S


def test_a_stop_between_attempts_cancels_instead_of_retrying(proxy, monkeypatch):
    from app.common import sink
    from app.features.observability.scope import set_scope
    monkeypatch.setattr(sink, "is_cancelled", lambda sid: sid == "s-stop")
    set_scope("s-stop", "agent")
    try:
        fake = proxy.script(503, completion("never"))
        with pytest.raises(sink.WorkflowCancelled):
            run(proxy.llm.run_llm("a", "s", "u", model="litellm/gpt-4o"))
        assert len(fake.requests) == 1
    finally:
        set_scope("", "")


def test_a_refused_temperature_is_dropped_and_the_call_retried(proxy):
    refusal = "Unsupported value: 'temperature' does not support 0 with this model."
    fake = proxy.script((400, {"error": {"message": refusal}}),
                        completion("ok"))
    assert run(proxy.llm.run_llm("a", "s", "u", model="litellm/o3-mini"))[0] == "ok"
    assert "temperature" in fake.requests[0]["body"] and "temperature" not in fake.requests[1]["body"]


def test_the_key_never_appears_in_an_error(proxy):
    from app.common.errors import ModelUnavailable
    echoed = f"Authentication Error, Invalid proxy server token passed. Received API Key = {KEY}"
    proxy.script((401, {"error": {"message": echoed}}))
    with pytest.raises(ModelUnavailable) as e:
        run(proxy.llm.run_llm("a", "s", "u", model="litellm/gpt-4o"))
    assert KEY not in str(e.value) and "HTTP 401" in str(e.value)


def test_a_response_with_no_choices_fails_the_call(proxy):
    from app.common.errors import ModelUnavailable
    proxy.script({"object": "chat.completion", "choices": []})
    with pytest.raises(ModelUnavailable, match="no choices"):
        run(proxy.llm.run_llm("a", "s", "u", model="litellm/gpt-4o"))


# --- the key and the URL ----------------------------------------------------------------------

@pytest.mark.parametrize("stored", [KEY, json.dumps({"apiKey": KEY})])
def test_the_key_is_read_once_from_the_secret_in_its_own_region(proxy, stored):
    proxy.secrets.value = stored
    fake = proxy.script(completion(), completion())
    run(proxy.llm.run_llm("a", "s", "u", model="litellm/gpt-4o"))
    run(proxy.llm.run_llm("a", "s", "u", model="litellm/gpt-4o"))
    assert proxy.secrets.calls == [{"SecretId": ARN}] and proxy.regions == ["eu-west-1"]
    assert all(r["headers"]["Authorization"] == f"Bearer {KEY}" for r in fake.requests)


@pytest.mark.parametrize("base", ["http://litellm.example.com", "https://u:p@litellm.example.com",
                                  "https://litellm.example.com/?x=1"])
def test_only_an_https_url_without_userinfo_or_query_is_used(proxy, monkeypatch, base):
    from app.common.errors import ModelUnavailable
    monkeypatch.setenv(proxy.lp.BASE_URL_ENV, base)
    fake = proxy.script()
    with pytest.raises(ModelUnavailable, match="https://"):
        run(proxy.llm.run_llm("a", "s", "u", model="litellm/gpt-4o"))
    assert fake.requests == []


def test_a_proxy_on_a_private_address_is_refused_by_the_ssrf_guard(proxy, monkeypatch):
    """The REAL netguard.urlopen: the name resolves to a private address, so nothing is sent."""
    from app.common import netguard
    from app.common.errors import ModelUnavailable
    monkeypatch.setattr(proxy.lp.netguard, "urlopen", netguard.urlopen)
    monkeypatch.setattr(netguard.socket, "getaddrinfo",
                        lambda *a, **k: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.5", 443))])
    with pytest.raises(ModelUnavailable, match="non-public address"):
        run(proxy.llm.run_llm("a", "s", "u", model="litellm/gpt-4o"))


# --- telemetry ------------------------------------------------------------------------------

def test_a_call_is_metered_with_its_tokens_and_the_proxys_cost(proxy):
    proxy.script((completion(prompt=120, out=30), {"x-litellm-response-cost": "0.00042"}))
    run(proxy.llm.run_llm("plan", "S", "U", model="litellm/gpt-4o", max_tokens=400))
    [row] = proxy.rows
    assert (row.kind, row.label, row.mode, row.prompt) == ("llm", "litellm/gpt-4o", "litellm", "plan")
    assert (row.input_tokens, row.output_tokens) == (120, 30)
    assert (row.cost_usd, row.rates_source, row.rates_known) == (Decimal("0.00042"), "proxy", True)
    assert row.finish_reason == "stop" and row.status == "ok"
    # No CountTokens for a model Bedrock does not serve: the system slice is an estimate.
    assert row.system_tokens_exact is False


@pytest.mark.parametrize("headers", [{}, {"x-litellm-response-cost": "nan"}, {"x-litellm-response-cost": "-1"},
                                     {"x-litellm-response-cost": "abc"}])
def test_without_a_usable_cost_header_the_cost_is_zero_and_says_so(proxy, headers):
    proxy.script((completion(), headers))
    run(proxy.llm.run_llm("a", "s", "u", model="litellm/gpt-4o"))
    [row] = proxy.rows
    assert (row.cost_usd, row.rates_source, row.rates_known) == (Decimal(0), "none", False)


def test_a_bedrock_row_is_priced_as_before(monkeypatch):
    from app.features.observability import meter
    rows: list = []
    monkeypatch.setattr(meter, "store", types.SimpleNamespace(put=rows.append, TELEMETRY_TABLE="t"))
    monkeypatch.setattr(meter, "_system_tokens", lambda *a: (0, False))
    meter.record_llm(model="us.anthropic.claude-haiku-4-5-20251001-v1:0", input_tokens=1000, output_tokens=0)
    assert rows[0].rates_source in ("config", "pricelist", "table") and rows[0].cost_usd > 0


# --- what the node wrapper does around every model --------------------------------------------

def test_guardrails_wrap_the_agent_not_the_model_call():
    """ApplyGuardrail runs in the node wrapper, before agent.run and on its output, whatever
    the agent's model; nothing in the LiteLLM branch could skip it."""
    nodes = (ORCH / "app" / "orchestrator" / "nodes.py").read_text()
    i_in = nodes.index('await ctx.guardrail(screened, "INPUT")')
    i_run = nodes.index("out = await agent.run(ctx)")
    i_out = nodes.index('await ctx.guardrail(str(out), "OUTPUT")')
    assert i_in < i_run < i_out
    lp = (ORCH / "app" / "common" / "litellm_proxy.py").read_text()
    assert "guardrail" not in re.sub(r'""".*?"""', "", lp, flags=re.DOTALL).lower()
