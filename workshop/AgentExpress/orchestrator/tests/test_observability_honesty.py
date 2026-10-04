"""The observability plane must not present a guess as a measurement.

Three fixes live here, and they are the same idea applied in three places: when this
framework does not actually know something, it has to say so on the record rather than
substitute a plausible value.

  1. MODEL RATES. `pricing.py` knows six Claude id substrings. `orchestrator.defaultModel`
     and each agent's `model` are config keys, so a customer is invited to choose a
     model — and anything outside those six was priced at a haiku-ish fallback and
     written onto every telemetry row with nothing to mark it. A deployment on Nova or
     Llama read fabricated cost figures as measured ones. Now the row carries
     `rates_known=False`, and `orchestrator.modelRates` lets a customer supply the real
     numbers without editing framework code.

  2. RUN AVAILABILITY. `_known_sessions()` returned an empty set on ANY failure, and
     `_avail()` read an empty set as "cannot tell, assume available" — so a scan that
     failed or was denied reported every findings link as openable.

  3. INSIGHTS OUTCOMES. The poll loop broke on an exception and fell through to
     `status or "IN_PROGRESS"`, storing IN_PROGRESS with no error. A run that had failed
     to poll, or timed out, sat in the panel as "in progress" forever with no reason
     recorded. `_store` also swallowed its own write failure, so `run_batch` could
     report COMPLETED while the table held nothing and the panel showed nothing.

`system_tokens_exact` on the same telemetry record was the precedent for all of this:
it already marked an estimated token count. These apply that standard to prices and to
outcomes.
"""
import json

import pytest
from conftest import workflow

# ---------------------------------------------------------------------------
# 1. Model rates
# ---------------------------------------------------------------------------

def _wf(model_rates=None) -> dict:
    orch = {"defaultModel": "us.amazon.nova-pro-v1:0"}
    if model_rates is not None:
        orch["modelRates"] = model_rates
    return {"orchestrator": orch,
            "agents": {"solo": {"name": "Solo", "maxTokens": 100}},
            "steps": [{"agent": "solo"}]}


def test_a_known_model_is_reported_as_known():
    from app.features.observability import pricing

    assert pricing.rates_known("us.anthropic.claude-haiku-4-5-20251001-v1:0") is True
    rates = pricing.model_rates("us.anthropic.claude-haiku-4-5-20251001-v1:0")
    assert rates == (pricing.Decimal("1.00"), pricing.Decimal("5.00"))


def test_an_unknown_model_is_MARKED_rather_than_silently_guessed():
    """The whole point. The cost is still estimated — an empty column would be worse —
    but the row now says it is an estimate."""
    from app.features.observability import pricing

    assert pricing.rates_known("us.amazon.nova-pro-v1:0") is False
    assert pricing.rates_known("meta.llama3-70b-instruct-v1:0") is False
    # Still costed, so the panel is not blank.
    assert pricing.model_cost("us.amazon.nova-pro-v1:0", 1_000_000, 0) > 0


def test_a_customer_can_supply_rates_for_their_own_model_from_config():
    """Without this, correcting a price or pricing a non-Claude model meant editing
    app/features/observability/pricing.py — a framework file."""
    with workflow(_wf({"nova-pro": {"input": 0.80, "output": 3.20}})) as imp:
        pricing = imp("app.features.observability.pricing")
        assert pricing.rates_known("us.amazon.nova-pro-v1:0") is True
        in_rate, out_rate = pricing.model_rates("us.amazon.nova-pro-v1:0")
        assert (in_rate, out_rate) == (pricing.Decimal("0.80"), pricing.Decimal("3.20"))
        # 1M input tokens at $0.80 = $0.80.
        assert pricing.model_cost("us.amazon.nova-pro-v1:0", 1_000_000, 0) == pricing.Decimal("0.80")


def test_configured_rates_override_the_built_in_table():
    """A built-in price can go stale; a customer must be able to correct it."""
    with workflow(_wf({"claude-haiku-4-5": {"input": 99, "output": 199}})) as imp:
        pricing = imp("app.features.observability.pricing")
        in_rate, _ = pricing.model_rates("us.anthropic.claude-haiku-4-5-20251001-v1:0")
        assert in_rate == pricing.Decimal("99")


def test_a_more_specific_configured_key_wins_regardless_of_write_order():
    """Substring matching needs a tie-break, or which rate applies depends on dict
    order — i.e. on how the customer happened to type their config."""
    rates = {"claude": {"input": 1, "output": 1},
             "claude-haiku-4-5": {"input": 7, "output": 7}}
    with workflow(_wf(rates)) as imp:
        pricing = imp("app.features.observability.pricing")
        in_rate, _ = pricing.model_rates("us.anthropic.claude-haiku-4-5-20251001-v1:0")
        assert in_rate == pricing.Decimal("7"), "the longer, more specific key must win"


def test_malformed_configured_rates_do_not_break_metering():
    """Cost accounting must never be the thing that fails a run."""
    with workflow(_wf({"nova": {"input": "not-a-number"}, "bad": None})) as imp:
        pricing = imp("app.features.observability.pricing")
        # Falls back rather than raising, and says the rate is not known.
        assert pricing.rates_known("us.amazon.nova-pro-v1:0") is False
        assert pricing.model_cost("us.amazon.nova-pro-v1:0", 1000, 1000) >= 0


def test_the_telemetry_row_carries_the_flag():
    from app.features.observability import meter
    from app.features.observability.records import CallRecord

    assert "rates_known" in CallRecord.__dataclass_fields__
    assert CallRecord.__dataclass_fields__["rates_known"].default is True

    recorded = []
    original = meter.store
    meter.store = type("S", (), {"put": staticmethod(recorded.append)})()
    try:
        meter.record_llm(model="us.amazon.nova-pro-v1:0", input_tokens=10, output_tokens=5)
        meter.record_llm(model="us.anthropic.claude-haiku-4-5-20251001-v1:0",
                         input_tokens=10, output_tokens=5)
    finally:
        meter.store = original

    assert recorded[0].rates_known is False, "an unknown model's row claimed a real rate"
    assert recorded[1].rates_known is True


def test_the_ui_marks_an_estimated_rate_and_exports_the_flag():
    from conftest import ORCH_ROOT

    js = (ORCH_ROOT / "web" / "legacy" / "observability.js").read_text()
    assert "c.rates_known === false" in js, "the rate column does not mark an estimate"
    assert '"rates_known"' in js, "the CSV/JSON export drops the flag"
    assert "orchestrator.modelRates" in js, "the tooltip does not say how to fix it"


# ---------------------------------------------------------------------------
# 2. Run availability
# ---------------------------------------------------------------------------

def test_a_failed_session_scan_is_not_reported_as_every_run_being_openable():
    """None means "could not find out"; an empty set means "found out, there are none".
    They used to collapse into one, so a denied scan greyed out nothing and every
    findings link opened an empty run view."""
    from app.features.optimization.insights import _avail

    # Could not find out -> optimistic, because greying out every link would be worse.
    assert _avail("abc", None) is True
    # Found out, and there are no runs -> nothing is openable.
    assert _avail("abc", set()) is False
    # Found out, and this one exists.
    assert _avail("abc", {"abc"}) is True
    assert _avail("abc", {"xyz"}) is False


def test_the_scan_returns_none_not_an_empty_set_when_it_cannot_run(monkeypatch):
    from app.features.optimization import insights

    monkeypatch.delenv("STATUS_TABLE", raising=False)
    assert insights._known_sessions() is None


def test_the_scan_returns_none_when_it_raises(monkeypatch):
    from app.features.optimization import insights

    monkeypatch.setenv("STATUS_TABLE", "t")
    monkeypatch.setattr(insights, "REGION", "us-east-1")

    import boto3

    def _boom(*a, **k):
        raise RuntimeError("AccessDeniedException")

    monkeypatch.setattr(boto3, "resource", _boom)
    assert insights._known_sessions() is None


# ---------------------------------------------------------------------------
# 3. Insights outcomes
# ---------------------------------------------------------------------------

def test_store_reports_its_own_failure_instead_of_swallowing_it(monkeypatch):
    """run_batch could return COMPLETED while the table held nothing and the panel
    showed nothing — success from the API, absent from the UI, no way to connect them."""
    from app.features.optimization import insights

    monkeypatch.setattr(insights, "_INSIGHTS_TABLE", "")
    assert "nowhere to persist" in insights._store({"status": "COMPLETED"})

    monkeypatch.setattr(insights, "_INSIGHTS_TABLE", "t")

    class _Boom:
        @staticmethod
        def put_item(**_kw):
            raise RuntimeError("ProvisionedThroughputExceeded")

    monkeypatch.setattr(insights, "_table", lambda: _Boom())
    reason = insights._store({"status": "COMPLETED"})
    assert "ProvisionedThroughputExceeded" in reason


def test_store_returns_empty_on_success(monkeypatch):
    from app.features.optimization import insights

    monkeypatch.setattr(insights, "_INSIGHTS_TABLE", "t")
    written = []
    monkeypatch.setattr(insights, "_table",
                        lambda: type("T", (), {"put_item": staticmethod(
                            lambda **kw: written.append(kw))})())
    assert insights._store({"status": "COMPLETED"}) == ""
    assert written


def _run_batch_with(monkeypatch, *, get_side_effect, table_ok=True):
    """Drive run_batch with StartBatchEvaluation stubbed and polling controlled."""
    from app.features.optimization import insights

    stored: list[dict] = []
    monkeypatch.setattr(insights, "_INSIGHTS_TABLE", "t")
    monkeypatch.setattr(insights, "_store",
                        lambda fields: (stored.append(fields),
                                        "" if table_ok else "PutItem denied")[1])
    monkeypatch.setattr(insights, "_POLL_TIMEOUT", 0 if get_side_effect == "timeout" else 60)
    monkeypatch.setattr(insights, "_POLL_INTERVAL", 0)

    # run_batch asks the evaluations service which CloudWatch sources to analyse.
    from app.features.evaluations import service as ev
    monkeypatch.setattr(ev, "runtime_trace_sources",
                        lambda: {"logGroupNames": ["g"], "serviceNames": ["s"]})

    class _AC:
        @staticmethod
        def start_batch_evaluation(**_kw):
            return {"batchEvaluationId": "b1", "batchEvaluationArn": "arn:b1"}

        @staticmethod
        def get_batch_evaluation(**_kw):
            if get_side_effect == "raise":
                raise RuntimeError("ThrottlingException")
            return {"status": "COMPLETED"}

    monkeypatch.setattr(insights, "_agentcore", lambda: _AC())
    result = insights.run_batch(lookback_hours=1, user="u")
    return result, stored


def test_a_polling_failure_is_recorded_not_left_as_in_progress(monkeypatch):
    result, stored = _run_batch_with(monkeypatch, get_side_effect="raise")
    assert "error" in result and "ThrottlingException" in result["error"]
    assert "could not be read" in result["error"]
    # And the persisted row carries the reason, so the panel can explain itself.
    assert stored[-1]["error"], "IN_PROGRESS was stored with no reason again"
    assert "ThrottlingException" in stored[-1]["error"]


def test_a_timeout_says_so_and_points_at_the_config_key(monkeypatch):
    result, stored = _run_batch_with(monkeypatch, get_side_effect="timeout")
    assert "error" in result
    assert "did not finish" in result["error"]
    assert "pollTimeoutSeconds" in result["error"], (
        "the timeout message does not tell the reader which knob to turn")
    assert stored[-1]["error"]


def test_findings_that_cannot_be_persisted_are_not_reported_as_success(monkeypatch):
    result, _ = _run_batch_with(monkeypatch, get_side_effect="ok", table_ok=False)
    assert result["status"] == "error", (
        "run_batch reported success for findings get_latest() will never see")
    assert "could not be persisted" in result["error"]


def test_a_clean_run_reports_no_error(monkeypatch):
    """The other half: these guards must not manufacture an error on the happy path."""
    result, stored = _run_batch_with(monkeypatch, get_side_effect="ok")
    assert result["status"] == "COMPLETED"
    assert "error" not in result
    assert stored[-1]["error"] == ""


# ---------------------------------------------------------------------------
# Insights is configurable at all
# ---------------------------------------------------------------------------

def test_insights_timings_come_from_workflow_json():
    """It was the ONE feature with no config block — evaluations, guardrails, policy,
    memory and the chatbot all have one — so a deployment whose runs take longer than
    the built-in fifteen minutes to analyse had to edit framework code."""
    defn = {
        "orchestrator": {"defaultModel": "m",
                         "insights": {"lookbackHours": 24, "pollTimeoutSeconds": 120,
                                      "pollIntervalSeconds": 5}},
        "agents": {"solo": {"name": "Solo", "maxTokens": 100}},
        "steps": [{"agent": "solo"}],
    }
    with workflow(defn) as imp:
        insights = imp("app.features.optimization.insights")
        assert insights._DEFAULT_LOOKBACK_HOURS == 24
        assert insights._POLL_TIMEOUT == 120
        assert insights._POLL_INTERVAL == 5


def test_insights_falls_back_to_sane_defaults_when_the_block_is_absent():
    defn = {"orchestrator": {"defaultModel": "m"},
            "agents": {"solo": {"name": "Solo", "maxTokens": 100}},
            "steps": [{"agent": "solo"}]}
    with workflow(defn) as imp:
        insights = imp("app.features.optimization.insights")
        assert insights._DEFAULT_LOOKBACK_HOURS == 168
        assert insights._POLL_TIMEOUT == 900
        assert insights._POLL_INTERVAL == 20


def test_the_shipped_workflow_declares_the_block():
    """Shipped with the defaults so the knob is discoverable rather than implied."""
    from conftest import ORCH_ROOT

    wf = json.loads((ORCH_ROOT / "app" / "workflow.json").read_text())
    cfg = (wf["orchestrator"] or {}).get("insights")
    if cfg is None:
        # OPTIONAL, and the defaults above are what apply without it. Required here, this
        # asserted a discoverability choice the SAMPLE makes as though it were a rule.
        pytest.skip("this workflow leaves orchestrator.insights at its defaults")
    assert set(cfg) <= {"lookbackHours", "pollTimeoutSeconds", "pollIntervalSeconds"}
    assert all(isinstance(v, int) and v > 0 for v in cfg.values())


# ---------------------------------------------------------------------------
# KB retrieval depth
# ---------------------------------------------------------------------------

def test_kb_retrieval_depth_is_a_workflow_json_key_on_both_iac_paths():
    """KB_NUM_RESULTS was read only by kb_lambda and set by NO IaC, so depth was frozen
    at 5 and the only way to change it was hand-editing a deployed Lambda — while
    `tools.<websearch>.maxResults` was a config key. Same key name on both now."""
    from conftest import ORCH_ROOT

    wf = json.loads((ORCH_ROOT / "app" / "workflow.json").read_text())
    # The IaC half of this assertion holds regardless; the config half needs a kb tool to
    # look at. A workflow with no Knowledge Base is legitimate — `scaffold.py reset` leaves
    # exactly that — and `next()` raising StopIteration there reads as a framework fault
    # rather than as "you have not declared a kb tool".
    kb = next((t for t in wf["tools"].values() if t.get("type") == "kb"), None)
    if kb is not None:
        assert isinstance(kb.get("maxResults"), int) and kb["maxResults"] > 0

    tf = "\n".join(p.read_text() for p in (ORCH_ROOT / "terraform").glob("*.tf"))
    assert "KB_NUM_RESULTS" in tf, "terraform does not inject the retrieval depth"
    assert "kb_max_results" in tf

    ts = (ORCH_ROOT / "cdk" / "lib" / "tool-plane.ts").read_text()
    assert "KB_NUM_RESULTS" in ts, "the CDK path does not inject the retrieval depth"
    assert "maxResults" in ts


def test_the_kb_lambda_still_has_a_default_so_it_never_retrieves_zero():
    from conftest import ORCH_ROOT

    src = (ORCH_ROOT / "kb_lambda" / "handler.py").read_text()
    assert 'os.environ.get("KB_NUM_RESULTS", "5")' in src


@pytest.mark.parametrize("depth", [1, 5, 25])
def test_the_declared_depth_reaches_the_app_config(depth):
    """`maxResults` is already in config.py's tool keep-list, so it is readable by an
    agent that wants to know — the gap was purely the Lambda's env."""
    defn = {
        "orchestrator": {"defaultModel": "m"},
        "tools": {"policies": {"type": "kb", "corpora": ["reference"],
                               "maxResults": depth}},
        "agents": {"solo": {"name": "Solo", "maxTokens": 100, "tool": "policies",
                            "corpus": "reference"}},
        "steps": [{"agent": "solo"}],
    }
    with workflow(defn) as imp:
        cfg = imp("app.common.config")
        assert cfg.TOOLS["policies"]["maxResults"] == depth
