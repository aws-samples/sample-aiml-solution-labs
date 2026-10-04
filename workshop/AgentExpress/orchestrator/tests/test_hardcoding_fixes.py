"""Values that used to be hard-coded and are now config, looked up, or derived.

  1. MODEL PRICES come from the AWS Price List for the deployment's region, after
     `orchestrator.modelRates` and before the built-in snapshot; every llm row records
     which priced it. The non-model rates are overridable with `serviceRates`.
  2. MODEL IDS move to the region's inference-profile geography: the framework default
     and the sample are `us.` profiles, which fail every call outside the US.
  3. INSIGHTS use `orchestrator.insights.lookbackHours` — a literal 168 was passed on
     every path, so the key did nothing.
  4. A CONSOLE TOOL TEST is one attempt with a timeout under API Gateway's 30 s, not
     boto3's default 60 s with retries that re-ran a non-idempotent tool.
"""

from __future__ import annotations

import json
from decimal import Decimal

import pytest
from conftest import workflow


def _wf(**orch) -> dict:
    return {"orchestrator": {"defaultModel": "us.amazon.nova-pro-v1:0", **orch},
            "agents": {"solo": {"name": "Solo", "maxTokens": 100}},
            "steps": [{"agent": "solo"}]}


def _row(service: str, usage: str, unit: str, usd: str, desc: str = "", **attrs) -> dict:
    return {"product": {"attributes": {"servicename": service, "usagetype": usage, **attrs}},
            "terms": {"OnDemand": {"t": {"priceDimensions": {"d": {
                "unit": unit, "pricePerUnit": {"USD": usd}, "description": desc}}}}}}


#: The shapes the real Price List uses (checked against the us-east-1 offer files).
PRODUCTS = [
    # Marketplace (Anthropic): servicename carries the model; regional vs global rows.
    _row("Claude Haiku 4.5 (Amazon Bedrock Edition)", "USE1-MP:USE1_InputTokenCount-Units",
         "1M tokens", "1.10", "Million Input Tokens Regional"),
    _row("Claude Haiku 4.5 (Amazon Bedrock Edition)", "USE1-MP:USE1_OutputTokenCount-Units",
         "1M tokens", "5.50", "Million Output Tokens Regional"),
    _row("Claude Haiku 4.5 (Amazon Bedrock Edition)", "USE1-MP:USE1_InputTokenCount_Global-Units",
         "1M tokens", "1.00", "Million Input Tokens Global"),
    _row("Claude Haiku 4.5 (Amazon Bedrock Edition)", "USE1-MP:USE1_OutputTokenCount_Global-Units",
         "1M tokens", "5.00", "Million Output Tokens Global"),
    # Not the standard on-demand rate: must not win the max().
    _row("Claude Haiku 4.5 (Amazon Bedrock Edition)", "USE1-MP:USE1_CacheWriteInputTokenCount-Units",
         "1M tokens", "9.99", "Million Cache Write Input Tokens"),
    _row("Claude Haiku 4.5 (Amazon Bedrock Edition)", "USE1-MP:USE1_InputTokenCount_Batch-Units",
         "1M tokens", "0.50", "Million Batch Input Tokens"),
    # First-party: `model` display name, per-1K units.
    _row("Amazon Bedrock", "USE1-NovaPro-input-tokens", "1K tokens", "0.0008",
         model="Nova Pro", inferenceType="Input tokens", feature="On-demand Inference"),
    _row("Amazon Bedrock", "USE1-NovaPro-output-tokens", "1K tokens", "0.0032",
         model="Nova Pro", inferenceType="Output tokens", feature="On-demand Inference"),
    _row("Amazon Bedrock", "USE1-NovaPro-input-tokens-priority", "1K tokens", "0.0014",
         model="Nova Pro", inferenceType="Input tokens priority", feature="On-demand Inference"),
    # A display name without `-instruct`, and an id embedded in the usage type.
    _row("Amazon Bedrock", "USE1-Llama3-3-70B-input-tokens", "1K tokens", "0.00072",
         model="Llama 3.3 70B", inferenceType="Input tokens"),
    _row("Amazon Bedrock", "USE1-Llama3-3-70B-output-tokens", "1K tokens", "0.00072",
         model="Llama 3.3 70B", inferenceType="Output tokens"),
    _row("Amazon Bedrock", "USE1-deepseek.v3.2-mantle-input-tokens-standard", "1K tokens", "0.00062",
         model="DeepSeek v3.2", inferenceType="Input tokens", service_tier="standard"),
    _row("Amazon Bedrock", "USE1-deepseek.v3.2-mantle-output-tokens-standard", "1K tokens", "0.00185",
         model="DeepSeek v3.2", inferenceType="Output tokens", service_tier="standard"),
]


@pytest.fixture
def book():
    from app.features.observability import pricelist as pl
    pl._reset_for_tests()
    pl._book = pl.parse_products([json.dumps(p) for p in PRODUCTS])
    pl._ready.set()
    yield pl
    pl._reset_for_tests()


@pytest.mark.parametrize(("model", "expected"), [
    ("us.anthropic.claude-haiku-4-5-20251001-v1:0", ("1.10", "5.50")),   # regional profile
    ("eu.anthropic.claude-haiku-4-5-20251001-v1:0", ("1.10", "5.50")),
    ("global.anthropic.claude-haiku-4-5-20251001-v1:0", ("1.00", "5.00")),
    ("us.amazon.nova-pro-v1:0", ("0.8", "3.2")),                          # 1K -> 1M
    ("us.meta.llama3-3-70b-instruct-v1:0", ("0.72", "0.72")),
    ("deepseek.v3.2", ("0.62", "1.85")),
])
def test_the_price_list_prices_a_model_by_its_id(book, model, expected):
    got = book.lookup(model)
    assert got is not None, f"{model} found no row (keys tried: {book.candidate_keys(model)})"
    assert got == (Decimal(expected[0]), Decimal(expected[1]))


def test_exact_keys_only_so_a_sibling_model_is_not_priced_by_substring(book):
    # `claude-haiku-4` is not `claude-haiku-4-5`; a substring join would have priced it.
    assert book.lookup("us.anthropic.claude-haiku-4-20250101-v1:0") is None
    assert book.lookup("cohere.command-r-v1:0") is None


def test_a_lookup_never_starts_a_fetch(monkeypatch):
    """Local runs and tests must make no AWS call: only prefetch() loads, and meter.py
    calls it only where telemetry is written."""
    from app.features.observability import pricelist as pl
    pl._reset_for_tests()
    monkeypatch.setattr(pl, "_fetch", lambda region: pytest.fail("lookup fetched prices"))
    assert pl.lookup("us.amazon.nova-pro-v1:0") is None


def test_a_failed_load_falls_back_and_is_not_retried_at_once(monkeypatch, tmp_path):
    from app.features.observability import pricelist as pl
    pl._reset_for_tests()
    monkeypatch.setattr(pl.tempfile, "gettempdir", lambda: str(tmp_path))
    calls = []

    def boom(region):
        calls.append(region)
        raise RuntimeError("AccessDenied: pricing:GetProducts")
    monkeypatch.setattr(pl, "_fetch", boom)
    pl.prefetch()
    pl._thread.join(5)
    assert pl.lookup("us.amazon.nova-pro-v1:0") is None
    pl.prefetch()
    assert len(calls) == 1, "a failure was retried immediately"
    pl._reset_for_tests()


def test_precedence_config_then_price_list_then_table_then_fallback(book):
    with workflow(_wf(modelRates={"nova-pro": {"input": 9, "output": 9}})) as imp:
        pricing = imp("app.features.observability.pricing")
        assert pricing.price("us.amazon.nova-pro-v1:0") == (Decimal(9), Decimal(9), "config")
    with workflow(_wf()) as imp:
        pricing = imp("app.features.observability.pricing")
        pl = imp("app.features.observability.pricelist")
        pl._book = book._book
        assert pricing.price("us.amazon.nova-pro-v1:0")[2] == "pricelist"
        # In the snapshot but (here) not in the Price List.
        assert pricing.price("us.anthropic.claude-sonnet-4-5-20250929-v1:0")[2] == "table"
        assert pricing.price("mistral.unknown-v1:0")[2] == "fallback"
        assert pricing.rates_known("mistral.unknown-v1:0") is False
        pl._reset_for_tests()


def test_price_list_off_keeps_pricing_offline(book):
    with workflow(_wf(priceList=False)) as imp:
        pricing = imp("app.features.observability.pricing")
        pl = imp("app.features.observability.pricelist")
        pl._book = book._book
        assert pricing.price("us.amazon.nova-pro-v1:0")[2] == "fallback"
        pl._reset_for_tests()


def test_the_llm_row_records_what_priced_it():
    from app.features.observability.records import CallRecord
    assert CallRecord.__dataclass_fields__["rates_source"].default == ""


def test_service_rates_override_the_built_in_non_model_rates():
    with workflow(_wf(serviceRates={"gatewayPerThousandInvocations": 1,
                                    "gatewayInvocationsPerToolCall": 1,
                                    "runtimeVcpuHour": 0, "runtimeGbHour": 1, "runtimeGb": 4,
                                    "embeddingPerMillionTokens": "bad"})) as imp:
        pricing = imp("app.features.observability.pricing")
        assert pricing.gateway_tool_cost() == Decimal("0.001")
        # One hour: 0 x vCPU + 4 GB x $1.
        assert pricing.compute_cost_for_seconds(3600) == Decimal(4)
        # An unusable override falls back to the built-in rate rather than raising.
        assert pricing.embedding_cost(1_000_000) == Decimal("0.02")


# --- 2. model ids follow the region -------------------------------------------

CASES = [
    ("us.anthropic.claude-haiku-4-5-20251001-v1:0", "us-east-1", "us.anthropic.claude-haiku-4-5-20251001-v1:0"),
    ("us.anthropic.claude-haiku-4-5-20251001-v1:0", "eu-west-1", "eu.anthropic.claude-haiku-4-5-20251001-v1:0"),
    ("us.anthropic.claude-haiku-4-5-20251001-v1:0", "ap-southeast-2", "apac.anthropic.claude-haiku-4-5-20251001-v1:0"),
    ("us.anthropic.claude-haiku-4-5-20251001-v1:0", "ca-central-1", "global.anthropic.claude-haiku-4-5-20251001-v1:0"),
    ("us.anthropic.claude-sonnet-5", "us-gov-west-1", "us-gov.anthropic.claude-sonnet-5"),
    ("eu.amazon.nova-pro-v1:0", "us-west-2", "us.amazon.nova-pro-v1:0"),
    ("global.anthropic.claude-haiku-4-5-20251001-v1:0", "eu-west-1", "global.anthropic.claude-haiku-4-5-20251001-v1:0"),
    ("amazon.nova-pro-v1:0", "eu-west-1", "amazon.nova-pro-v1:0"),
    ("anthropic.claude-3-5-sonnet-20240620-v1:0", "ap-south-1", "anthropic.claude-3-5-sonnet-20240620-v1:0"),
]


@pytest.mark.parametrize(("model", "region", "expected"), CASES)
def test_a_model_id_moves_to_the_regions_geography(model, region, expected):
    import workflow as bff_workflow

    from app.common import vocabulary
    assert vocabulary.regional_model_id(model, region) == expected
    assert bff_workflow.regional_model_id(model, region) == expected, "the BFF copy drifted"


def test_the_cdk_copy_is_held_to_the_same_cases():
    """cdk/test/regional-model.test.ts reads this file's CASES through a fixture."""
    from pathlib import Path
    fixture = Path(__file__).parent / "fixtures" / "regional_model_cases.json"
    assert json.loads(fixture.read_text()) == [list(c) for c in CASES], (
        "regenerate tests/fixtures/regional_model_cases.json from CASES")


def test_the_runtime_default_follows_the_region(monkeypatch):
    monkeypatch.delenv("BEDROCK_MODEL_ID", raising=False)
    monkeypatch.setenv("AWS_REGION", "eu-central-1")
    with workflow(_wf(defaultModel="us.anthropic.claude-haiku-4-5-20251001-v1:0")) as imp:
        cfg = imp("app.common.config")
        assert cfg.MODEL_ID == "eu.anthropic.claude-haiku-4-5-20251001-v1:0"
        assert cfg.model_for("us.amazon.nova-pro-v1:0") == "eu.amazon.nova-pro-v1:0"
        assert cfg.model_for(None) == cfg.MODEL_ID


# --- 3. insights lookback ------------------------------------------------------

def test_insights_use_the_configured_lookback_when_none_is_given(monkeypatch):
    with workflow(_wf(insights={"lookbackHours": 24})) as imp:
        insights = imp("app.features.optimization.insights")
        ev = imp("app.features.evaluations.service")
        seen = {}
        monkeypatch.setattr(ev, "runtime_trace_sources",
                            lambda: {"logGroupNames": ["g"], "serviceNames": ["s"]})

        class Client:
            def start_batch_evaluation(self, **req):
                seen.update(req)
                raise RuntimeError("stop here")
        monkeypatch.setattr(insights, "_agentcore", lambda: Client())
        monkeypatch.setattr(insights, "_store", lambda item: None)
        assert insights.run_batch(0)["status"] == "error"
        window = seen["dataSourceConfig"]["cloudWatchLogs"]["filterConfig"]["timeRange"]
        assert (window["endTime"] - window["startTime"]).total_seconds() == 24 * 3600
        assert seen["description"].endswith("24h")


def test_nothing_substitutes_a_window_of_its_own():
    from pathlib import Path
    root = Path(__file__).resolve().parent.parent
    assert "or 168" not in (root / "bff" / "handler.py").read_text()
    assert "_DEFAULT_INSIGHTS_LOOKBACK = 0" in (root / "app" / "orchestrator" / "runtime.py").read_text()


# --- 4. console tool test ------------------------------------------------------

def test_the_tool_test_client_makes_one_attempt_inside_the_gateway_limit():
    import builds
    cfg = builds._TEST_TOOL_CLIENT
    assert builds.TEST_TOOL_TIMEOUT_S < 30
    assert cfg.read_timeout == builds.TEST_TOOL_TIMEOUT_S
    assert cfg.retries == {"total_max_attempts": 1}


# --- 5. image prices -------------------------------------------------------------

IMAGE_ROW = _row("Stable Diffusion 3.5 Large v1.0 (Amazon Bedrock Edition)",
                 "USE1-MP:USE1_Created_image-Units", "image", "0.09",
                 "AWS Marketplace software usage|us-east-1|Output image, SD3.5 Large")


def test_the_price_list_prices_an_image_model_per_image():
    from app.features.observability import pricelist as pl
    pl._reset_for_tests()
    pl._book = pl.parse_products([json.dumps(IMAGE_ROW)])
    assert pl.lookup_image("stability.sd3-5-large-v1:0") == Decimal("0.09")
    assert pl.lookup("stability.sd3-5-large-v1:0") is None, "an image price is not a token rate"
    pl._reset_for_tests()


def test_image_price_order_config_then_price_list_then_snapshot():
    with workflow(_wf(imageRates={"sd3-5": 0.5})) as imp:
        pricing = imp("app.features.observability.pricing")
        assert pricing.image_price("stability.sd3-5-large-v1:0") == (Decimal("0.5"), "config")
    with workflow(_wf()) as imp:
        pricing = imp("app.features.observability.pricing")
        pl = imp("app.features.observability.pricelist")
        assert pricing.image_price("stability.sd3-5-large-v1:0") == (Decimal("0.08"), "table")
        pl._book = pl.parse_products([json.dumps(IMAGE_ROW)])
        assert pricing.image_price("stability.sd3-5-large-v1:0") == (Decimal("0.09"), "pricelist")
        assert pricing.image_price("stability.unknown-v1:0") == (Decimal(0), "fallback")
        pl._reset_for_tests()


# --- 6. model call read timeout ------------------------------------------------

def test_a_model_call_waits_longer_than_botos_60_seconds():
    """An 8000-token report on Claude Haiku 4.5 takes ~60-80 s and Converse returns
    nothing until it is done, so boto3's default 60 s read timeout failed the run."""
    from app.common import llm
    cfg = llm._client_config()
    assert cfg.read_timeout == 300 and cfg.retries["total_max_attempts"] == 2
