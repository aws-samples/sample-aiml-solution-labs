"""Bedrock prompt caching: cache point placement (app/common/prompt_cache.py), the
llm.py wiring, the Price List cache rows (pricelist.py) and the cost math (pricing.py).

Rules under test come from https://docs.aws.amazon.com/bedrock/latest/userguide/prompt-caching.html
and https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_CachePointBlock.html.
Rates below are test fixtures shaped like live us-east-1 Price List rows."""

import asyncio
import json
import sys
import types
from decimal import Decimal

import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from app.common import prompt_cache as pc

HAIKU45 = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
HAIKU45_GLOBAL = "global.anthropic.claude-haiku-4-5-20251001-v1:0"
SONNET37 = "us.anthropic.claude-3-7-sonnet-20250219-v1:0"
NOVA = "us.amazon.nova-lite-v1:0"
ALL = {"system": True, "tools": True, "messages": True, "ttl": "1h"}


# --- the table and id resolution ------------------------------------------------

def test_profiles_resolve_to_their_base_model():
    assert pc.base_id(HAIKU45) == "anthropic.claude-haiku-4-5-20251001-v1:0"
    assert pc.base_id(HAIKU45_GLOBAL) == "anthropic.claude-haiku-4-5-20251001-v1:0"
    assert pc.base_id("arn:aws:bedrock:us-east-1::foundation-model/amazon.nova-pro-v1:0") \
        == "amazon.nova-pro-v1:0"
    assert pc.support(HAIKU45)["minTokensText"] == "4,096"
    assert pc.support(NOVA)["minTokensText"] == "1K"


def test_the_table_keeps_its_source():
    t = pc.table()
    assert t["source"] == "https://docs.aws.amazon.com/bedrock/latest/userguide/prompt-caching.html"
    assert t["copied"] == "2026-10-21"
    assert set(t["values"]) == set(t["byModel"])


@pytest.mark.parametrize("model", ["anthropic.claude-unknown-9", "openai.gpt-6-sol",
                                   "litellm/gpt-4o", "meta.llama3-3-70b-instruct-v1:0"])
def test_an_unknown_or_unsupported_model_gets_no_cache_point(model):
    assert pc.support(model) is None
    assert pc.plan(model, ALL, call="tools", has_tools=True) is pc.OFF


def test_off_unless_set():
    assert pc.plan(HAIKU45, None, call="single") is pc.OFF
    assert pc.plan(HAIKU45, {}, call="single") is pc.OFF
    assert pc.plan(HAIKU45, {"system": False, "ttl": "1h"}, call="single") is pc.OFF


# --- placement ----------------------------------------------------------------

def test_claude_gets_tools_system_and_messages_with_1h():
    p = pc.plan(HAIKU45, ALL, call="tools", has_tools=True)
    assert p.sections == ("tools", "system", "messages")
    assert p.point == {"cachePoint": {"type": "default", "ttl": "1h"}}
    assert p.rule == "newest"


def test_ttl_is_omitted_for_5m_and_on_5m_only_models():
    assert pc.plan(HAIKU45, {"system": True}, call="single").point == {"cachePoint": {"type": "default"}}
    # Claude 3.7 Sonnet lists 5m only: sending ttl can be a ValidationException.
    assert pc.plan(SONNET37, {"system": True, "ttl": "1h"}, call="single").point == \
        {"cachePoint": {"type": "default"}}


def test_a_single_call_caches_only_the_system_prompt():
    assert pc.plan(HAIKU45, ALL, call="single").sections == ("system",)


def test_nova_never_caches_tools_and_uses_one_point():
    p = pc.plan(NOVA, ALL, call="tools", has_tools=True)
    assert p.sections == ("messages",)
    assert p.point == {"cachePoint": {"type": "default"}}     # Nova rejects ttl
    assert pc.plan(NOVA, {"system": True, "tools": True}, call="tools", has_tools=True).sections \
        == ("system",)


def test_system_blocks_put_the_point_between_static_and_dynamic():
    p = pc.plan(HAIKU45, {"system": True}, call="single")
    s = pc.SystemPrompt("instructions", "today")
    assert str(s) == "instructions\n\ntoday" and f"{s}" == "instructions\n\ntoday"
    assert pc.system_content(s, p) == [{"type": "text", "text": "instructions"},
                                       {"cachePoint": {"type": "default"}},
                                       {"type": "text", "text": "today"}]
    assert pc.system_content("plain", p) == [{"type": "text", "text": "plain"},
                                             {"cachePoint": {"type": "default"}}]
    assert pc.system_content("plain", pc.OFF) is None


def test_claude_messages_point_is_on_the_newest_turn_and_nothing_is_mutated():
    p = pc.plan(HAIKU45, {"messages": True}, call="tools", has_tools=True)
    convo = [SystemMessage(content="s"), HumanMessage(content="task"),
             AIMessage(content="", tool_calls=[{"name": "t", "args": {}, "id": "1"}]),
             ToolMessage(content="result", tool_call_id="1")]
    out = pc.with_message_point(convo, p)
    assert out[-1].content == [{"type": "text", "text": "result"}, {"cachePoint": {"type": "default"}}]
    assert convo[-1].content == "result" and out[1] is convo[1]


def test_nova_messages_point_is_on_the_first_user_message_never_a_tool_result():
    p = pc.plan(NOVA, {"messages": True}, call="tools", has_tools=True)
    convo = [SystemMessage(content="s"), HumanMessage(content="task"),
             AIMessage(content="", tool_calls=[{"name": "t", "args": {}, "id": "1"}]),
             ToolMessage(content="result", tool_call_id="1")]
    out = pc.with_message_point(convo, p)
    assert out[1].content[-1] == {"cachePoint": {"type": "default"}}
    assert out[-1].content == "result"


def test_tools_point_goes_after_the_last_tool():
    p = pc.plan(HAIKU45, {"tools": True}, call="tools", has_tools=True)
    specs = [{"type": "function", "function": {"name": "a"}}]
    assert pc.tools_with_point(specs, p) == [*specs, {"cachePoint": {"type": "default"}}]
    assert len(specs) == 1


# --- usage ------------------------------------------------------------------

def test_usage_reads_langchain_breakdown_without_double_counting():
    msg = AIMessage(content="x", usage_metadata={
        "input_tokens": 6589, "output_tokens": 5, "total_tokens": 6594,
        "input_token_details": {"cache_read": 0, "cache_creation": 0,
                                "ephemeral_1h_input_tokens": 6576}})
    u = pc.usage_of(msg)
    assert u == {"read": 0, "write": 6576, "write5m": 0, "write1h": 6576}
    nova = AIMessage(content="x", usage_metadata={
        "input_tokens": 1500, "output_tokens": 5, "total_tokens": 1505,
        "input_token_details": {"cache_read": 0, "cache_creation": 1400}})
    # No cacheDetails: Nova lists 5m only, so the write is 5m.
    assert pc.split_ttl(pc.usage_of(nova), NOVA, pc.OFF) == (1400, 0, 0, 0)
    # Implicit write on a 5m+1h model with no cache point sent: TTL unknown.
    assert pc.split_ttl(pc.usage_of(nova), HAIKU45, pc.OFF) == (0, 0, 0, 1400)


# --- the request llm.py sends ---------------------------------------------------

class _Msg:
    def __init__(self):
        self.content = "ok"
        self.usage_metadata = {"input_tokens": 10, "output_tokens": 1,
                               "input_token_details": {"cache_read": 0, "cache_creation": 0}}
        self.response_metadata = {"stopReason": "end_turn"}


def test_run_llm_sends_the_system_point_and_keeps_it_through_a_sampling_retry(monkeypatch):
    from app.common import llm as llm_mod
    sent: list = []

    class _FakeLLM:
        def __init__(self, **k):
            self.k = k

        async def ainvoke(self, messages):
            sent.append((dict(self.k), messages))
            if "temperature" in self.k:
                raise ValueError("ValidationException: `temperature` is deprecated for this model.")
            return _Msg()

    fake = types.ModuleType("langchain_aws")
    fake.ChatBedrockConverse = _FakeLLM
    monkeypatch.setitem(sys.modules, "langchain_aws", fake)
    asyncio.run(llm_mod.run_llm("a", pc.SystemPrompt("static", "dyn"), "user", model=HAIKU45,
                                max_tokens=10, prompt_cache={"system": True}))
    assert len(sent) == 2 and "temperature" not in sent[1][0]
    for _k, messages in sent:
        assert messages[0].content == [{"type": "text", "text": "static"},
                                       {"cachePoint": {"type": "default"}},
                                       {"type": "text", "text": "dyn"}]


def test_run_llm_without_prompt_cache_sends_plain_text(monkeypatch):
    from app.common import llm as llm_mod
    sent: list = []

    class _FakeLLM:
        def __init__(self, **k):
            pass

        async def ainvoke(self, messages):
            sent.append(messages)
            return _Msg()

    fake = types.ModuleType("langchain_aws")
    fake.ChatBedrockConverse = _FakeLLM
    monkeypatch.setitem(sys.modules, "langchain_aws", fake)
    asyncio.run(llm_mod.run_llm("a", "sys", "user", model=HAIKU45, max_tokens=10))
    assert sent[0][0] == ("system", "sys")


def test_a_refused_cache_point_is_retried_without_it(monkeypatch):
    from app.common import llm as llm_mod
    sent: list = []

    class _FakeLLM:
        def __init__(self, **k):
            pass

        async def ainvoke(self, messages):
            sent.append(messages)
            if not isinstance(messages[0], tuple):
                raise ValueError("ValidationException: extraneous key [cachePoint] is not permitted")
            return _Msg()

    fake = types.ModuleType("langchain_aws")
    fake.ChatBedrockConverse = _FakeLLM
    monkeypatch.setitem(sys.modules, "langchain_aws", fake)
    text, _ = asyncio.run(llm_mod.run_llm("a", "sys", "user", model=HAIKU45, max_tokens=10,
                                          prompt_cache={"system": True}))
    assert text == "ok" and len(sent) == 2 and sent[1][0] == ("system", "sys")


# --- Price List cache rows and cost -----------------------------------------------

def _row(usage, infer, usd, desc, unit="1M tokens", **attrs):
    return json.dumps({"product": {"attributes": {
        "servicename": "Claude Haiku 4.5 (Amazon Bedrock Edition)", "model": "Claude Haiku 4.5",
        "usagetype": usage, "inferenceType": infer, "feature": "On-demand Inference", **attrs}},
        "terms": {"OnDemand": {"t": {"priceDimensions": {"d": {
            "unit": unit, "pricePerUnit": {"USD": usd}, "description": desc}}}}}})


ROWS = [
    _row("USE1-MP:USE1_InputTokenCount-Units", "Input tokens", "1.10", "Million Input Tokens Regional"),
    _row("USE1-MP:USE1_OutputTokenCount-Units", "Output tokens", "5.50", "Million Output Tokens Regional"),
    _row("USE1-MP:USE1_CacheReadInputTokenCount-Units", "Prompt cache read input tokens", "0.11",
         "Million Cache Read Input Tokens Regional"),
    _row("USE1-MP:USE1_CacheWriteInputTokenCount-Units", "Prompt cache write input tokens", "1.375",
         "Million Cache Write Input Tokens Regional"),
    _row("USE1-MP:USE1_CacheWrite1hInputTokenCount-Units", "Prompt cache write input tokens", "2.20",
         "Million 1 hour Cache Write Input Tokens Regional"),
    _row("USE1-MP:USE1_InputTokenCount_Global-Units", "Input tokens", "1.00", "Million Input Tokens Global"),
    _row("USE1-MP:USE1_OutputTokenCount_Global-Units", "Output tokens", "5.00", "Million Output Tokens Global"),
    # A batch cache row: not the on-demand tier.
    _row("USE1-MP:USE1_CacheReadInputTokenCount_Batch-Units", "Prompt cache read input tokens", "0.05",
         "Million Batch Cache Read Input Tokens"),
]


@pytest.fixture
def priced(monkeypatch):
    from app.common import config
    from app.features.observability import pricelist as pl
    monkeypatch.setattr(config, "PRICE_LIST", True, raising=False)
    monkeypatch.setattr(config, "MODEL_RATES", {}, raising=False)
    pl._reset_for_tests()
    pl._book = pl.parse_products(ROWS)
    pl._ready.set()
    yield pl
    pl._reset_for_tests()


def test_cache_rows_are_kept_per_scope_with_no_cross_scope_fallback(priced):
    assert priced.lookup(HAIKU45) == (Decimal("1.10"), Decimal("5.50"))
    assert priced.lookup_cache(HAIKU45) == {"cacheRead": Decimal("0.11"),
                                            "cacheWrite5m": Decimal("1.375"),
                                            "cacheWrite1h": Decimal("2.20")}
    assert priced.lookup_cache(HAIKU45_GLOBAL) == {}


def test_cost_uses_the_cache_rates_and_the_saving(priced):
    from app.features.observability import pricing
    # 1,000,000 total input: 600,000 read + 300,000 written (5m) + 100,000 uncached.
    c = pricing.call_cost(HAIKU45, 1_000_000, 0, read=600_000, write5m=300_000)
    assert c["cache_read_cost_usd"] == Decimal("0.066")
    assert c["cache_write_cost_usd"] == Decimal("0.4125")
    assert c["cost_usd"] == Decimal("0.11") + Decimal("0.066") + Decimal("0.4125")
    assert c["cache_saving_usd"] == Decimal("0.99") - Decimal("0.066") - Decimal("0.4125")
    assert c["cache_cost_known"] is True and c["cache_rates_source"] == "pricelist"


def test_a_missing_cache_rate_is_unknown_not_guessed(priced):
    from app.features.observability import pricing
    c = pricing.call_cost(HAIKU45_GLOBAL, 1000, 0, read=500)
    assert c["cache_cost_known"] is False and c["cache_saving_usd"] == 0
    # The 500 read tokens are left out; only the 500 uncached are priced (global input 1.00).
    assert c["cost_usd"] == Decimal(500) / Decimal(1_000_000) * Decimal("1.00")


def test_no_cache_tokens_costs_exactly_what_it_did(priced):
    from app.features.observability import pricing
    c = pricing.call_cost(HAIKU45, 1000, 100)
    assert c["cost_usd"] == pricing.cost_at(Decimal("1.10"), Decimal("5.50"), 1000, 100)
    assert c["cache_cost_known"] is True


# --- BFF: the run rollup and the run assistant's cost answer ---------------------

ROWS_RUN = [
    {"agent_id": "intake", "kind": "llm", "label": HAIKU45, "cost_usd": Decimal("0.01"),
     "input_tokens": 5000, "output_tokens": 10, "cache_read_tokens": 0, "cache_write_tokens": 4200,
     "cache_read_cost_usd": Decimal(0), "cache_write_cost_usd": Decimal("0.005775"),
     "cache_saving_usd": Decimal("-0.001155"), "cache_cost_known": True},
    {"agent_id": "intake", "kind": "llm", "label": HAIKU45, "cost_usd": Decimal("0.002"),
     "input_tokens": 5000, "output_tokens": 10, "cache_read_tokens": 4200, "cache_write_tokens": 0,
     "cache_read_cost_usd": Decimal("0.000462"), "cache_write_cost_usd": Decimal(0),
     "cache_saving_usd": Decimal("0.004158"), "cache_cost_known": True},
    {"agent_id": "report", "kind": "llm", "label": HAIKU45, "cost_usd": Decimal("0.003"),
     "input_tokens": 100, "output_tokens": 10},
]


def _bff(monkeypatch, name):
    """The BFF module loaded the way tests/test_asks.py (handler) and tests/test_authz.py
    (chatbot) load it."""
    import importlib
    monkeypatch.setenv("WORKFLOW_JSON", json.dumps(
        {"orchestrator": {"chatbot": {"enabled": True, "tools": {}}}, "agents": {}}))
    if name == "handler":
        # Its own module object, so sys.modules["handler"] (shared with other tests) is untouched.
        import importlib.util
        from pathlib import Path
        path = Path(__file__).resolve().parent.parent / "bff" / "handler.py"
        spec = importlib.util.spec_from_file_location("_prompt_cache_bff_handler", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    for cached in ("workflow", "chatbot"):
        sys.modules.pop(cached, None)
    module = importlib.import_module("chatbot")
    for cached in ("workflow", "chatbot"):
        monkeypatch.delitem(sys.modules, cached, raising=False)
    return module


def test_run_rollup_sums_cache_tokens_cost_and_saving(monkeypatch):
    handler = _bff(monkeypatch, "handler")
    monkeypatch.setattr(handler, "_query_all", lambda tbl, **kw: [dict(r) for r in ROWS_RUN])
    body = json.loads(handler._telemetry_session("s1", tbl=object())["body"])
    intake = next(a for a in body["byAgent"] if a["agentId"] == "intake")
    assert (intake["cacheReadTokens"], intake["cacheWriteTokens"]) == (4200, 4200)
    assert Decimal(str(intake["cacheSavingUsd"])) == Decimal("0.003003")
    assert Decimal(str(body["totals"]["cacheCostUsd"])) == Decimal("0.006237")
    assert Decimal(str(body["totals"]["costUsd"])) == Decimal("0.015")


def test_run_assistant_cost_answer_carries_the_cache(monkeypatch):
    chatbot = _bff(monkeypatch, "chatbot")
    monkeypatch.setattr(chatbot, "_tel_rows", lambda sid: [dict(r) for r in ROWS_RUN])
    monkeypatch.setattr(chatbot, "_sid", lambda args, ctx: "s1")
    out, _ = chatbot._t_costs({}, {}, None)
    assert out["totalCostUsd"] == 0.015          # cost_usd already includes the cache cost
    assert out["promptCacheByAgent"]["intake"]["cacheReadTokens"] == 4200
    assert out["promptCacheByAgent"]["intake"]["cacheWriteTokens"] == 4200
    assert out["promptCacheByAgent"]["intake"]["cacheSavingUsd"] == pytest.approx(0.003003)
    assert "report" not in out["promptCacheByAgent"]


def test_run_assistant_says_unknown_for_cache_tokens_with_no_rate(monkeypatch):
    """Live test 2026-10-22: us.openai.gpt-5.5 read 1,886 implicit cache tokens with no Price
    List rate; Observability showed the cache cost as unknown, the run assistant said $0.0."""
    chatbot = _bff(monkeypatch, "chatbot")
    rows = [{"agent_id": "gpt", "kind": "llm", "cost_usd": 0.001, "cache_read_tokens": 1886,
             "cache_write_tokens": 0, "cache_read_cost_usd": 0, "cache_write_cost_usd": 0,
             "cache_saving_usd": 0, "cache_cost_known": False}]
    monkeypatch.setattr(chatbot, "_tel_rows", lambda sid: rows)
    monkeypatch.setattr(chatbot, "_sid", lambda args, ctx: "s1")
    out, _ = chatbot._t_costs({}, {}, None)
    gpt = out["promptCacheByAgent"]["gpt"]
    assert gpt["cacheReadTokens"] == 1886
    assert gpt["cacheCostUsd"] == "unknown" and gpt["cacheSavingUsd"] == "unknown"
    assert "no published rate" in out["promptCacheNote"]


def test_run_assistant_retries_without_a_refused_temperature(monkeypatch):
    from botocore.exceptions import ClientError
    chatbot = _bff(monkeypatch, "chatbot")
    sent = []

    class _Bedrock:
        def converse(self, **kw):
            sent.append(dict(kw["inferenceConfig"]))
            if "temperature" in kw["inferenceConfig"]:
                raise ClientError({"Error": {"Code": "ValidationException", "Message":
                                   "`temperature` is deprecated for this model."}}, "Converse")
            return {"output": {"message": {"role": "assistant", "content": [{"text": "ok"}]}}}
    monkeypatch.setattr(chatbot, "_bedrock", _Bedrock())
    out = chatbot._converse("us.anthropic.claude-haiku-5-5", [{"text": "s"}],
                            [{"role": "user", "content": [{"text": "hi"}]}], {"tools": []})
    assert out["output"]["message"]["content"][0]["text"] == "ok"
    assert sent == [{"maxTokens": 1500, "temperature": 0}, {"maxTokens": 1500}]


def test_harness_cache_tokens_are_recorded_not_priced(monkeypatch):
    from app.features.observability import meter, store
    rows = []
    monkeypatch.setattr(store, "put", rows.append)
    meter.record_llm(model=HAIKU45, input_tokens=100, output_tokens=10, mode="harness",
                     cache={"read": 50, "write": 0, "write5m": 0, "write1h": 0}, cache_priced=False)
    r = rows[0]
    assert r.cache_read_tokens == 50 and r.cache_cost_known is False and r.cache_read_cost_usd == 0


def test_the_observability_csv_exports_carry_the_cache_columns():
    """Live test 2026-10-22: the Run detail JSON export had every cache_* field but the CSV
    (and the Overview CSV) had none, so a spreadsheet of a cached run showed no cache tokens."""
    from pathlib import Path
    js = (Path(__file__).resolve().parent.parent / "web" / "legacy" / "observability.js").read_text()
    flow = js[js.index("function exportFlow"):js.index("// ---- per-agent")]
    for col in ("cache_requested", "cache_read_tokens", "cache_write_tokens", "cache_write_5m_tokens",
                "cache_write_1h_tokens", "cache_read_cost_usd", "cache_write_cost_usd", "cache_saving_usd",
                "cache_cost_known"):
        assert f'"{col}"' in flow, col
    overview = js[js.index("function exportOverview"):js.index("function exportFlow")]
    for col in ("cacheReadTokens", "cacheWriteTokens", "cacheCostUsd", "cacheSavingUsd", "cacheCostKnown"):
        assert f'"{col}"' in overview and f"b.{col}" in overview, col


# --- OpenAI GPT-5.6+ explicit caching through InvokeModel ---------------------------
# https://docs.aws.amazon.com/bedrock/latest/userguide/prompt-caching.html ("Prompt caching
# for models from OpenAI" and the table); request/response shapes checked live in the dev
# account 2026-10-22 (.agents/tasks/prompt-cache-2026-10-21/probe-openai-explicit-results.json).

SOL = "us.openai.gpt-5.6-sol"


@pytest.mark.parametrize("model", ["us.openai.gpt-5.6-sol", "global.openai.gpt-5.6-terra",
                                   "us.openai.gpt-5.6-luna", "us.openai.gpt-6-astra",
                                   "us.openai.gpt-6.1-sol"])
def test_doc_listed_openai_models_cache_the_system_prompt_through_invoke_model(model):
    p = pc.plan(model, ALL, call="single")
    assert p.api == "InvokeModel" and p.sections == ("system",) and p.ttl == "30m"
    assert p.point == {"prompt_cache_breakpoint": {"mode": "explicit"}}
    assert p.min_text == "1,024"
    # Tool-calling turns stay on Converse, which has no explicit caching for these models.
    assert pc.plan(model, ALL, call="tools", has_tools=True) is pc.OFF
    assert pc.plan(model, {"tools": True, "messages": True}, call="single") is pc.OFF


@pytest.mark.parametrize("model", ["us.openai.gpt-6-sol", "global.openai.gpt-6-luna",
                                   "us.openai.gpt-5.5", "openai.gpt-oss-120b-1:0"])
def test_openai_models_not_in_the_table_stay_implicit(model):
    assert pc.support(model) is None
    assert pc.plan(model, {"system": True}, call="single") is pc.OFF


def test_openai_body_puts_the_breakpoint_on_the_static_system_text():
    p = pc.plan(SOL, {"system": True, "ttl": "30m"}, call="single")
    body = pc.openai_body(pc.SystemPrompt("static", "dyn"), "hi", p, max_tokens=50,
                          sampling={"temperature": 0, "top_p": 0.5})
    assert body == {
        "messages": [
            {"role": "system", "content": [
                {"type": "text", "text": "static", "prompt_cache_breakpoint": {"mode": "explicit"}},
                {"type": "text", "text": "dyn"}]},
            {"role": "user", "content": [{"type": "text", "text": "hi"}]}],
        "max_completion_tokens": 50,
        "prompt_cache_options": {"mode": "explicit", "ttl": "30m"},
        "temperature": 0, "top_p": 0.5}
    # A ttl the model does not have is never sent: 30m is the only documented value.
    p1h = pc.plan(SOL, {"system": True, "ttl": "1h"}, call="single")
    assert pc.openai_body("s", "u", p1h, max_tokens=5, sampling={})["prompt_cache_options"]["ttl"] == "30m"


def _openai_reply(read=0, write=0, text="Hi.", finish="stop"):
    # The shape returned live by InvokeModel for us.openai.gpt-5.6-sol.
    return {"choices": [{"message": {"role": "assistant", "content": text}, "finish_reason": finish}],
            "usage": {"prompt_tokens": 3553, "completion_tokens": 37, "total_tokens": 3590,
                      "prompt_tokens_details": {"cached_tokens": read, "cache_write_tokens": write}}}


def test_openai_usage_maps_to_the_cache_read_and_30m_write():
    msg = pc.openai_message(_openai_reply(write=3542))
    assert msg.usage_metadata["input_tokens"] == 3553        # total input, as on Converse
    u = pc.usage_of(msg)
    assert u == {"read": 0, "write": 3542, "write5m": 0, "write1h": 0}
    p = pc.plan(SOL, {"system": True}, call="single")
    assert pc.split_ttl(u, SOL, p) == (0, 0, 3542, 0)
    assert pc.usage_of(pc.openai_message(_openai_reply(read=3542)))["read"] == 3542
    assert pc.openai_message(_openai_reply(text="<reasoning>x</reasoning>Hi.")).content == "Hi."


def _fake_runtime(monkeypatch, replies, sent):
    from app.common import llm as llm_mod

    class _Body:
        def __init__(self, data):
            self.data = data

        def read(self):
            return json.dumps(self.data).encode()

    class _Client:
        def invoke_model(self, **k):
            sent.append(json.loads(k["body"]) | {"_modelId": k["modelId"]})
            r = replies.pop(0)
            if isinstance(r, Exception):
                raise r
            return {"body": _Body(r)}

    monkeypatch.setattr(llm_mod, "_runtime", _Client())
    return llm_mod


def test_run_llm_calls_invoke_model_and_drops_a_refused_temperature(monkeypatch):
    sent: list = []
    llm_mod = _fake_runtime(monkeypatch, [
        ValueError("ValidationException: Unsupported value: 'temperature' does not support 0 "
                   "with this model. Only the default (1) value is supported."),
        _openai_reply(write=3542)], sent)
    metered: list = []
    monkeypatch.setattr(llm_mod, "_meter_llm", lambda *a, **k: metered.append((a, k)))
    text, cut = asyncio.run(llm_mod.run_llm("a", pc.SystemPrompt("static", "dyn"), "u", model=SOL,
                                            max_tokens=10, prompt_cache={"system": True}))
    assert (text, cut) == ("Hi.", False)
    assert len(sent) == 2 and "temperature" in sent[0] and "temperature" not in sent[1]
    assert sent[1]["_modelId"] == SOL
    assert sent[1]["messages"][0]["content"][0]["prompt_cache_breakpoint"] == {"mode": "explicit"}
    (args, kw), = metered
    assert kw["cache_plan"].api == "InvokeModel" and pc.usage_of(args[1])["write"] == 3542


def test_run_llm_falls_back_to_converse_when_the_breakpoint_is_refused(monkeypatch):
    sent: list = []
    llm_mod = _fake_runtime(monkeypatch, [ValueError("ValidationException: unknown field prompt_cache_options")], sent)
    conv: list = []

    class _FakeLLM:
        def __init__(self, **k):
            pass

        async def ainvoke(self, messages):
            conv.append(messages)
            return _Msg()

    fake = types.ModuleType("langchain_aws")
    fake.ChatBedrockConverse = _FakeLLM
    monkeypatch.setitem(sys.modules, "langchain_aws", fake)
    text, _ = asyncio.run(llm_mod.run_llm("a", "sys", "u", model=SOL, max_tokens=10,
                                          prompt_cache={"system": True}))
    assert text == "ok" and len(sent) == 1 and conv[0][0] == ("system", "sys")


def test_run_llm_keeps_converse_for_openai_without_prompt_cache_or_with_stop(monkeypatch):
    sent: list = []
    llm_mod = _fake_runtime(monkeypatch, [], sent)
    conv: list = []

    class _FakeLLM:
        def __init__(self, **k):
            pass

        async def ainvoke(self, messages):
            conv.append(messages)
            return _Msg()

    fake = types.ModuleType("langchain_aws")
    fake.ChatBedrockConverse = _FakeLLM
    monkeypatch.setitem(sys.modules, "langchain_aws", fake)
    asyncio.run(llm_mod.run_llm("a", "sys", "u", model=SOL, max_tokens=10))
    asyncio.run(llm_mod.run_llm("a", "sys", "u", model=SOL, max_tokens=10, stop_sequences=["X"],
                                prompt_cache={"system": True}))
    assert sent == [] and len(conv) == 2 and conv[1][0] == ("system", "sys")


def _astra_row(usage, infer, usd):
    return json.dumps({"product": {"attributes": {
        "servicename": "OpenAI GPT-6 Astra", "model": "OpenAI GPT-6 Astra",
        "usagetype": usage, "inferenceType": infer, "feature": "On-demand Inference"}},
        "terms": {"OnDemand": {"t": {"priceDimensions": {"d": {
            "unit": "1M tokens", "pricePerUnit": {"USD": usd}, "description": ""}}}}}})


# Shaped like the live us-east-1 Price List rows for GPT-6 Astra
# (.agents/tasks/prompt-cache-2026-10-21/pricelist-openai-gpt56-gpt6-us-east-1.tsv).
ASTRA_ROWS = [
    _astra_row("USE1-MP:USE1_input_tokens_standard-Units", "Input tokens", "11.0000000000"),
    _astra_row("USE1-MP:USE1_output_tokens_standard-Units", "Output tokens", "55.0000000000"),
    _astra_row("USE1-MP:USE1_cache_read_tokens_standard-Units", "Prompt cache read input tokens", "1.1000000000"),
    _astra_row("USE1-MP:USE1_cache_write_tokens_30m_standard-Units", "Prompt cache write input tokens", "13.7500000000"),
    _astra_row("USE1-MP:USE1_cache_write_tokens_30m_long_ctx_standard-Units", "Prompt cache write input tokens", "27.5000000000"),
]


def test_openai_30m_writes_are_priced_at_the_price_list_row(priced):
    from app.features.observability import pricing
    priced._book = priced.parse_products(ROWS + ASTRA_ROWS)
    astra = "us.openai.gpt-6-astra"
    assert priced.lookup_cache(astra) == {"cacheRead": Decimal("1.1"), "cacheWrite30m": Decimal("13.75")}
    c = pricing.call_cost(astra, 1_000_000, 0, read=500_000, write30m=400_000)
    assert c["cache_read_cost_usd"] == Decimal("0.55")
    assert c["cache_write_cost_usd"] == Decimal("5.5")
    assert c["cost_usd"] == Decimal("1.1") + Decimal("0.55") + Decimal("5.5")
    assert c["cache_cost_known"] is True and c["cache_rates_source"] == "pricelist"
    # 30m rows never become a Claude 5m write rate.
    assert "cacheWrite30m" not in priced.lookup_cache(HAIKU45)


def test_openai_30m_writes_fall_back_to_the_documented_1_25x_only_with_a_known_input_rate(priced, monkeypatch):
    from app.common import config
    from app.features.observability import pricing
    # No Price List row for GPT-5.6 Sol (us-east-1, 2026-10-22): input rate unknown -> unknown.
    c = pricing.call_cost(SOL, 1000, 0, read=0, write30m=800)
    assert c["rates_source"] == "fallback" and c["cache_cost_known"] is False
    # With a configured input rate, the write is 1.25x it (doc); the read has no rate: unknown.
    monkeypatch.setattr(config, "MODEL_RATES", {"gpt-5.6-sol": {"input": 4.4, "output": 22}}, raising=False)
    c = pricing.call_cost(SOL, 1_000_000, 0, write30m=1_000_000)
    assert c["cache_write_cost_usd"] == Decimal("5.500") and c["cache_cost_known"] is True
    assert c["cache_rates_source"] == "doc"
    c = pricing.call_cost(SOL, 1_000_000, 0, read=1_000_000)
    assert c["cache_cost_known"] is False
    # Never for a model without the documented multiple (GPT-6 Sol is not in the table).
    monkeypatch.setattr(config, "MODEL_RATES", {"gpt-6-sol": {"input": 4.4, "output": 22}}, raising=False)
    assert pricing.call_cost("us.openai.gpt-6-sol", 1000, 0, write30m=500)["cache_cost_known"] is False
