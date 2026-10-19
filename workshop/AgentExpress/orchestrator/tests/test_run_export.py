"""Sending each run to an observability tool (workflow.json orchestrator.export,
app/features/observability/runexport.py), and its IaC twins (terraform/export.tf,
cdk/lib/export.ts).

Off sends, reads and builds nothing. On, a run that has ended becomes ONE trace rebuilt from
its telemetry rows, in the OTLP JSON shape (https://opentelemetry.io/docs/specs/otlp/), with
the GenAI attributes and Langfuse's own where Langfuse documents them; content only when the
deploy allowed it; scores to Langfuse's scores API; delta metrics where accepted; every
request through netguard; the key in no error, log or span; an admin's pause honoured.

The DynamoDB fakes check every request against the installed botocore model, and the HTTP
side is a fake `_post` that records what would have gone on the wire. Live delivery to
Langfuse, Datadog, Grafana Cloud or Honeycomb is NOT covered here.
"""
from __future__ import annotations

import asyncio
import json
import re
import sys
import urllib.error
from decimal import Decimal
from pathlib import Path

import botocore.session
import pytest
from boto3.dynamodb.types import TypeSerializer
from botocore.validate import validate_parameters

ORCH = Path(__file__).resolve().parent.parent
ARN = "arn:aws:secretsmanager:us-east-1:123456789012:secret:agentexpress/x/runtime-export-headers-AbC123"
# Fake key values, joined at run time so secret scanners do not read them as real ones.
LF_AUTH = "Basic " + "cGstbGYtMTpzay1sZi0y"
DD_KEY = "dd" + "0123456789abcdef"
SID = "a1b2c3d4e5f6"


def _mod():
    # The module the code under test uses (other tests purge app.* from sys.modules).
    import app.features.observability.runexport as runexport
    return runexport


_DDB = botocore.session.get_session().get_service_model("dynamodb")
_SER = TypeSerializer()


def _wire(value):
    return {k: _SER.serialize(v) for k, v in value.items()}


def _check(op: str, table: str, kwargs: dict) -> None:
    """The request as the low-level client would send it, validated against botocore."""
    params = {"TableName": table, **kwargs}
    for k in ("Key", "ExclusiveStartKey"):
        if k in params:
            params[k] = _wire(params[k])
    if "ExpressionAttributeValues" in params:
        params["ExpressionAttributeValues"] = _wire(params["ExpressionAttributeValues"])
    validate_parameters(params, _DDB.operation_model(op).input_shape)


class FakeStatus:
    def __init__(self, items=None, fail=False):
        self.items = dict(items or {})
        self.fail, self.updates, self.gets = fail, [], []

    def get_item(self, **kw):
        _check("GetItem", "status", kw)
        self.gets.append(kw["Key"]["session_id"])
        if self.fail:
            raise RuntimeError("ProvisionedThroughputExceeded")
        item = self.items.get(kw["Key"]["session_id"])
        return {"Item": dict(item)} if item else {}

    def update_item(self, **kw):
        _check("UpdateItem", "status", kw)
        assert kw["ConditionExpression"] == "attribute_exists(session_id)"
        self.updates.append(kw)
        sid = kw["Key"]["session_id"]
        if sid in self.items:
            self.items[sid][kw["ExpressionAttributeNames"]["#e"]] = kw["ExpressionAttributeValues"][":e"]
        return {}


class FakeTelemetry:
    def __init__(self, rows, page=0):
        self.rows, self.page, self.calls = rows, page, 0

    def query(self, **kw):
        _check("Query", "telemetry", kw)
        assert kw["KeyConditionExpression"] == "session_id = :s"
        self.calls += 1
        sid = kw["ExpressionAttributeValues"][":s"]
        mine = [r for r in self.rows if r["session_id"] == sid]
        start = int(kw.get("ExclusiveStartKey", {}).get("n", 0)) if kw.get("ExclusiveStartKey") else 0
        if not self.page:
            return {"Items": mine}
        chunk = mine[start:start + self.page]
        out = {"Items": chunk}
        if start + self.page < len(mine):
            out["LastEvaluatedKey"] = {"session_id": sid, "sk": chunk[-1]["sk"], "n": start + self.page}
        return out


class Wire:
    """Stands in for runexport._post: records each request, answers from `replies`."""
    def __init__(self, replies=None):
        self.sent, self.replies = [], list(replies or [])

    def __call__(self, url, body, headers, timeout):
        self.sent.append({"url": url, "body": json.loads(body), "headers": dict(headers), "timeout": timeout})
        reply = self.replies.pop(0) if self.replies else (200, "{}")
        if isinstance(reply, Exception):
            raise reply
        return reply

    def to(self, part):
        return [s for s in self.sent if s["url"].endswith(part)]


def _row(kind, agent="writer", label="", ts="2026-03-02 10:00:05", version=1, **kw):
    n = kw.pop("n", 0)
    return {"session_id": SID, "sk": f"{ts}#{n:04d}#abc{n:03d}", "kind": kind, "agent_id": agent,
            "user": "pat@example.com", "label": label, "ts": ts, "version": version,
            "latency_ms": kw.pop("latency_ms", 1200), "cost_usd": Decimal(kw.pop("cost", "0")), **kw}


ROWS = [
    _row("llm", label="us.anthropic.claude-haiku-4-5", n=1, mode="bedrock", input_tokens=120, output_tokens=40,
         cost="0.0012", chosen_by="route 1", prompt="draft", finish_reason="end_turn",
         system_prompt="SYSTEM-SECRET-PROMPT", user_input="USER-QUESTION", output_text="MODEL-ANSWER"),
    _row("tool", label="crm", n=2, mode="gateway", status="ok", user_input="TOOL-ARGS", output_text="TOOL-RESULT"),
    _row("guardrail", label="input", n=3, status="blocked"),
    _row("policy", label="crm", n=4, status="allowed"),
    _row("memory", label="recall", n=5, status="ok", namespace="insights/acme-corp"),
    _row("person", label="question", n=6, status="answered", user_input="ASK-Q", output_text="ASK-A"),
    _row("eval", label="Builtin.Helpfulness", n=7, value=Decimal("0.83"), eval_label="Very Helpful",
         output_text="JUDGE-EXPLANATION", status="ok"),
    _row("agentcore", agent="__session__", label="runtime-compute", n=8, cost="0.0005", latency_ms=30000),
    _row("llm", agent="reviewer", label="us.amazon.nova-pro", n=9, mode="bedrock", input_tokens=10,
         output_tokens=5, cost="0.0001", ts="2026-03-02 10:00:09"),
]
RUN = {"session_id": SID, "overall": "done", "topic": "RUN-REQUEST", "result": "RUN-RESULT",
       "created": "2026-03-02 09:59:58", "updated_at": "2026-03-02 10:00:12", "user": "pat@example.com",
       "build": {"id": "b1", "name": "claims", "version": 3}, "workflowVersion": 3,
       "nodes": {"writer": {"status": "done"}, "reviewer": {"status": "done"}}}
CONTENT_MARKERS = ("SYSTEM-SECRET-PROMPT", "USER-QUESTION", "MODEL-ANSWER", "TOOL-ARGS", "TOOL-RESULT",
                   "ASK-Q", "ASK-A", "JUDGE-EXPLANATION", "RUN-REQUEST", "RUN-RESULT", "acme-corp")


@pytest.fixture
def rx(monkeypatch):
    """The module, with clean caches and no export settings in the environment."""
    m = _mod()
    for k in ("OTLP_EXPORT_DESTINATION", "OTLP_EXPORT_HEADERS_SECRET_ARN", "OTLP_EXPORT_INCLUDE_CONTENT",
              "OTLP_EXPORT_ENDPOINT", "APP_NAME"):
        monkeypatch.delenv(k, raising=False)
    m._headers_cache.clear()
    m._warned.clear()
    m._pause.update(value=False, until=0.0)
    return m


def _on(monkeypatch, rx, block, content=False, headers=None, status=None, rows=ROWS, page=0):
    """Turn the export on as the IaC would; returns (wire, status table, telemetry table)."""
    monkeypatch.setenv("OTLP_EXPORT_DESTINATION", block["destination"])
    monkeypatch.setenv("OTLP_EXPORT_HEADERS_SECRET_ARN", ARN)
    monkeypatch.setenv("OTLP_EXPORT_INCLUDE_CONTENT", "true" if content else "false")
    monkeypatch.setenv("APP_NAME", "claims_app")
    monkeypatch.setattr(rx, "_export_block", lambda: block)
    rx._headers_cache[ARN] = dict(headers or {"Authorization": LF_AUTH})
    st = status if status is not None else FakeStatus({SID: dict(RUN)})
    tel = FakeTelemetry(rows, page)
    monkeypatch.setattr(rx, "_table", lambda env: {"STATUS_TABLE": st, "TELEMETRY_TABLE": tel}[env])
    wire = Wire()
    monkeypatch.setattr(rx, "_post", wire)
    return wire, st, tel


def _all_keys(spans):
    keys = set()
    for s in spans:
        keys |= {a["key"] for a in s["attributes"]}
        for e in s.get("events", ()):
            keys |= {a["key"] for a in e["attributes"]}
    return keys


def _attr(span, key):
    for a in span["attributes"]:
        if a["key"] == key:
            return next(iter(a["value"].values()))
    return None


# --- off ------------------------------------------------------------------------------------------

def test_off_reads_builds_and_sends_nothing(rx, monkeypatch):
    monkeypatch.setattr(rx, "_table", lambda env: pytest.fail("a table was read"))
    monkeypatch.setattr(rx, "_post", lambda *a: pytest.fail("something was sent"))
    monkeypatch.setattr(rx, "load_headers", lambda *a: pytest.fail("the secret was read"))
    monkeypatch.setattr(rx, "_export_block", lambda: pytest.fail("the workflow was read"))
    assert rx.settings() is None
    assert rx.on_run_end(SID) is None
    assert rx.after_eval(SID, "writer", "") == 0
    asyncio.run(rx.after_burst(SID))
    assert rx.export_test()["ok"] is False and "not set up" in rx.export_test()["error"]
    assert rx.export_run(SID)["error"] == rx.OFF


def test_a_workflow_that_does_not_match_what_was_deployed_is_off(rx, monkeypatch, capsys):
    monkeypatch.setenv("OTLP_EXPORT_DESTINATION", "datadog")
    monkeypatch.setenv("OTLP_EXPORT_HEADERS_SECRET_ARN", ARN)
    monkeypatch.setattr(rx, "_export_block", lambda: {"destination": "langfuse", "secret": "x"})
    assert rx.settings() is None and rx.settings() is None
    assert capsys.readouterr().out.count("does not match") == 1   # said once, not per run


@pytest.mark.parametrize("arn", ["", "arn:aws:secretsmanager:us-east-1:123456789012:secret:short", "*"])
def test_an_unusable_secret_arn_is_off(rx, monkeypatch, arn):
    monkeypatch.setenv("OTLP_EXPORT_DESTINATION", "otlp")
    monkeypatch.setenv("OTLP_EXPORT_HEADERS_SECRET_ARN", arn)
    monkeypatch.setattr(rx, "_export_block", lambda: {"destination": "otlp", "endpoint": "https://c.example.com"})
    assert rx.settings() is None


# --- presets ----------------------------------------------------------------------------------------

@pytest.mark.parametrize("dest,region,endpoint,traces,metrics,scores", [
    ("langfuse", "", "", "https://cloud.langfuse.com/api/public/otel/v1/traces", "", "https://cloud.langfuse.com/api/public/scores"),
    ("langfuse", "us", "", "https://us.cloud.langfuse.com/api/public/otel/v1/traces", "", "https://us.cloud.langfuse.com/api/public/scores"),
    ("langfuse", "self-hosted", "https://lf.example.com/api/public/otel/",
     "https://lf.example.com/api/public/otel/v1/traces", "", "https://lf.example.com/api/public/scores"),
    ("langfuse", "self-hosted", "https://lf.example.com", "https://lf.example.com/api/public/otel/v1/traces", "", "https://lf.example.com/api/public/scores"),
    ("datadog", "", "", "https://otlp.datadoghq.com/v1/traces", "https://otlp.datadoghq.com/v1/metrics", ""),
    ("datadog", "datadoghq.eu", "", "https://otlp.datadoghq.eu/v1/traces", "https://otlp.datadoghq.eu/v1/metrics", ""),
    ("datadog", "us5.datadoghq.com", "", "https://otlp.us5.datadoghq.com/v1/traces",
     "https://otlp.us5.datadoghq.com/v1/metrics", ""),
    ("honeycomb", "", "", "https://api.honeycomb.io/v1/traces", "https://api.honeycomb.io/v1/metrics", ""),
    ("honeycomb", "eu", "", "https://api.eu1.honeycomb.io/v1/traces", "https://api.eu1.honeycomb.io/v1/metrics", ""),
    ("grafana", "", "https://otlp-gateway-prod-us-east-0.grafana.net/otlp",
     "https://otlp-gateway-prod-us-east-0.grafana.net/otlp/v1/traces", "", ""),
    ("otlp", "", "https://collector.example.com:4318/v1/traces", "https://collector.example.com:4318/v1/traces",
     "https://collector.example.com:4318/v1/metrics", ""),
])
def test_each_destination_resolves_to_its_documented_address(rx, dest, region, endpoint, traces, metrics, scores):
    assert rx.urls(dest, region, endpoint) == {"traces": traces, "metrics": metrics, "scores": scores}


@pytest.mark.parametrize("dest,region", [("langfuse", "jp"), ("datadog", "datadoghq.cn"), ("honeycomb", "ap")])
def test_an_unknown_region_has_no_address(rx, dest, region):
    with pytest.raises(KeyError):
        rx.urls(dest, region)


def test_the_bff_presets_are_the_runtimes():
    """bff/export_check.py (the console's connection test, the validator) cannot import app.*."""
    sys.path.insert(0, str(ORCH / "bff"))
    import export_check
    rx = _mod()
    for name in ("DESTINATIONS", "LANGFUSE_HOSTS", "LANGFUSE_REGIONS", "DATADOG_SITES", "HONEYCOMB_HOSTS",
                 "HONEYCOMB_REGIONS", "DEFAULT_REGION", "FIXED_HEADERS", "METRICS_DESTINATIONS"):
        assert getattr(export_check, name) == getattr(rx, name), name
    for dest in rx.DESTINATIONS:
        for region in ["", *list(export_check.REGIONS.get(dest, ()))]:
            assert export_check.urls(dest, region, "https://h.example.com/x") == rx.urls(dest, region, "https://h.example.com/x")


def test_the_page_validator_names_the_same_destinations_and_regions():
    ts = (ORCH / "web" / "src" / "builder" / "exportCheck.ts").read_text()
    rx = _mod()

    def ts_list(name):
        body = re.search(rf"export const {name} = \[([^\]]*)\]", ts, re.DOTALL).group(1)
        return tuple(re.findall(r'"([^"]+)"', body))
    assert ts_list("EXPORT_DESTINATIONS") == rx.DESTINATIONS
    assert ts_list("LANGFUSE_REGIONS") == rx.LANGFUSE_REGIONS
    assert ts_list("DATADOG_SITES") == rx.DATADOG_SITES
    assert ts_list("HONEYCOMB_REGIONS") == rx.HONEYCOMB_REGIONS
    assert ts_list("METRICS_DESTINATIONS") == rx.METRICS_DESTINATIONS


# --- the trace -----------------------------------------------------------------------------------------

def _settings(rx, dest="otlp", content=False, **kw):
    return rx.Settings(destination=dest, content=content, secret_arn=ARN,
                       urls=rx.urls(dest, kw.pop("region", ""), "https://c.example.com"), **kw)


def test_the_trace_is_valid_otlp_json(rx):
    tid, spans = rx.build_trace(SID, RUN, ROWS, _settings(rx))
    assert re.fullmatch(r"[0-9a-f]{32}", tid)
    ids = {s["spanId"] for s in spans}
    assert len(ids) == len(spans)
    for s in spans:
        assert s["traceId"] == tid and re.fullmatch(r"[0-9a-f]{16}", s["spanId"])
        assert set(s) <= {"traceId", "spanId", "parentSpanId", "name", "kind", "startTimeUnixNano",
                          "endTimeUnixNano", "attributes", "status", "events"}
        assert isinstance(s["kind"], int) and isinstance(s["status"]["code"], int)   # enums as integers
        assert re.fullmatch(r"\d{19}", s["startTimeUnixNano"])
        assert int(s["endTimeUnixNano"]) >= int(s["startTimeUnixNano"])
        for a in s["attributes"]:
            (vtype, v), = a["value"].items()
            assert vtype in ("stringValue", "intValue", "doubleValue", "boolValue", "arrayValue")
            if vtype == "intValue":
                assert isinstance(v, str) and v.lstrip("-").isdigit()   # int64 as a decimal string
        if "parentSpanId" in s:
            assert s["parentSpanId"] in ids


def test_one_root_one_span_per_agent_run_and_its_calls_under_it(rx):
    _, spans = rx.build_trace(SID, RUN, ROWS, _settings(rx))
    root = spans[0]
    assert root["name"] == "invoke_workflow claims" and "parentSpanId" not in root
    agents = {s["name"]: s for s in spans if s.get("parentSpanId") == root["spanId"]}
    assert set(agents) == {"invoke_agent writer", "invoke_agent reviewer"}
    writer = agents["invoke_agent writer"]
    under = [s["name"] for s in spans if s.get("parentSpanId") == writer["spanId"]]
    assert under == ["chat us.anthropic.claude-haiku-4-5", "execute_tool crm"]
    chat = next(s for s in spans if s["name"].startswith("chat us.anthropic"))
    assert _attr(chat, "gen_ai.operation.name") == "chat"
    assert _attr(chat, "gen_ai.provider.name") == "aws.bedrock"
    assert _attr(chat, "gen_ai.request.model") == "us.anthropic.claude-haiku-4-5"
    assert _attr(chat, "gen_ai.usage.input_tokens") == "120" and _attr(chat, "gen_ai.usage.output_tokens") == "40"
    assert _attr(chat, "gen_ai.usage.cost") == pytest.approx(0.0012)
    assert _attr(chat, "agentexpress.model.chosen_by") == "route 1"
    for s in spans:   # the session and the user on every span, as Langfuse asks
        assert _attr(s, "session.id") == SID and _attr(s, "user.id") == "pat@example.com"
    assert _attr(root, "gen_ai.workflow.name") == "claims" and _attr(root, "agentexpress.workflow.version") == "3"
    assert _attr(root, "gen_ai.usage.input_tokens") == "130"
    assert _attr(root, "agentexpress.cost_usd") == pytest.approx(0.0018)
    tool = next(s for s in spans if s["name"] == "execute_tool crm")
    assert _attr(tool, "gen_ai.tool.name") == "crm" and _attr(tool, "agentexpress.tool.target") == "gateway"


def test_decisions_and_scores_are_events_on_the_agent_span(rx):
    _, spans = rx.build_trace(SID, RUN, ROWS, _settings(rx))
    writer = next(s for s in spans if s["name"] == "invoke_agent writer")
    events = {e["name"]: {a["key"]: next(iter(a["value"].values())) for a in e["attributes"]} for e in writer["events"]}
    assert events["guardrail"]["agentexpress.outcome"] == "blocked"
    assert events["cedar.decision"] == {"gen_ai.tool.name": "crm", "agentexpress.outcome": "allowed"}
    assert events["memory.recall"]["agentexpress.outcome"] == "ok"
    assert events["hitl.ask"]["agentexpress.outcome"] == "answered"
    ev = events["gen_ai.evaluation.result"]
    assert ev["gen_ai.evaluation.name"] == "Builtin.Helpfulness"
    assert ev["gen_ai.evaluation.score.value"] == pytest.approx(0.83)
    assert ev["gen_ai.evaluation.score.label"] == "Very Helpful"
    root = spans[0]
    assert [e["name"] for e in root["events"]] == ["agentcore.compute"]


def test_with_evals_off_no_score_is_sent_anywhere(rx):
    _, spans = rx.build_trace(SID, RUN, ROWS, _settings(rx, evals=False))
    assert "gen_ai.evaluation.name" not in _all_keys(spans)


def test_without_content_no_prompt_answer_or_request_leaves(rx):
    for dest in ("otlp", "langfuse"):
        _tid, spans = rx.build_trace(SID, RUN, ROWS, _settings(rx, dest))
        text = json.dumps(spans)
        for marker in CONTENT_MARKERS:
            assert marker not in text, (dest, marker)
        export = sys.modules["app.features.observability.export"]
        assert not [k for k in _all_keys(spans) if export.is_content(k)]
        assert not {"langfuse.observation.input", "langfuse.observation.output"} & _all_keys(spans)
        assert not rx.build_scores(SID, ROWS, False)[0].get("comment", "").count("JUDGE")


def test_with_content_the_prompts_and_answers_are_sent_in_the_genai_shape(rx):
    _, spans = rx.build_trace(SID, RUN, ROWS, _settings(rx, "langfuse", content=True))
    chat = next(s for s in spans if s["name"].startswith("chat us.anthropic"))
    assert json.loads(_attr(chat, "gen_ai.input.messages")) == [
        {"role": "user", "parts": [{"type": "text", "content": "USER-QUESTION"}]}]
    assert json.loads(_attr(chat, "gen_ai.output.messages"))[0]["parts"][0]["content"] == "MODEL-ANSWER"
    assert json.loads(_attr(chat, "gen_ai.system_instructions"))[0]["content"] == "SYSTEM-SECRET-PROMPT"
    assert _attr(chat, "langfuse.observation.input") == _attr(chat, "gen_ai.input.messages")
    assert _attr(spans[0], "langfuse.observation.input") == "RUN-REQUEST"
    tool = next(s for s in spans if s["name"] == "execute_tool crm")
    assert _attr(tool, "gen_ai.tool.call.arguments") == "TOOL-ARGS"


def test_langfuse_gets_its_documented_attributes(rx):
    _, spans = rx.build_trace(SID, RUN, ROWS, _settings(rx, "langfuse"))
    by_name = {s["name"]: s for s in spans}
    assert _attr(spans[0], "langfuse.trace.name") == "claims"
    assert _attr(spans[0], "langfuse.version") == "3"
    assert _attr(by_name["invoke_agent writer"], "langfuse.observation.type") == "agent"
    chat = by_name["chat us.anthropic.claude-haiku-4-5"]
    assert _attr(chat, "langfuse.observation.type") == "generation"
    assert json.loads(_attr(chat, "langfuse.observation.usage_details")) == {"input": 120, "output": 40}
    assert json.loads(_attr(chat, "langfuse.observation.cost_details")) == {"total": 0.0012}
    assert _attr(by_name["execute_tool crm"], "langfuse.observation.type") == "tool"
    # Langfuse documents guardrail and event observation types: small observations, not events.
    assert _attr(by_name["guardrail"], "langfuse.observation.type") == "guardrail"
    assert _attr(by_name["guardrail"], "langfuse.observation.level") == "WARNING"
    assert _attr(by_name["cedar.decision"], "langfuse.observation.type") == "guardrail"
    assert _attr(by_name["hitl.ask"], "langfuse.observation.type") == "event"
    assert "gen_ai.evaluation.result" not in json.dumps(spans)   # scores go to the scores API


def test_the_ids_are_the_same_every_time_so_a_resend_updates_the_same_trace(rx):
    a = rx.build_trace(SID, RUN, ROWS, _settings(rx))
    b = rx.build_trace(SID, RUN, list(ROWS), _settings(rx))
    assert a == b
    assert rx.build_trace("other", RUN, ROWS, _settings(rx))[0] != a[0]


def test_a_rerun_is_a_second_agent_span(rx):
    rows = [*ROWS, _row("llm", label="m", n=20, version=2, ts="2026-03-02 10:01:00")]
    _, spans = rx.build_trace(SID, RUN, rows, _settings(rx))
    writers = [s for s in spans if s["name"] == "invoke_agent writer"]
    assert sorted(_attr(s, "agentexpress.agent.version") for s in writers) == ["1", "2"]


def test_a_failed_run_and_a_failed_call_are_errors(rx):
    run = {**RUN, "overall": "failed", "nodes": {"writer": {"status": "failed"}}}
    rows = [_row("llm", label="m", n=1, mode="error", status="error")]
    _, spans = rx.build_trace(SID, run, rows, _settings(rx))
    assert [s["status"]["code"] for s in spans] == [2, 2, 2]


def test_eastern_time_rows_become_utc(rx):
    # 2026-03-02 is EST (UTC-5); 2026-07-01 is EDT (UTC-4).
    assert rx._et_ns("2026-03-02 10:00:05") == 1772463605 * 10**9
    assert rx._et_ns("2026-07-01 10:00:05") == 1782914405 * 10**9
    assert rx._et_ns("not a time") is None


# --- metrics ------------------------------------------------------------------------------------

def test_metrics_are_per_run_delta_sums_per_agent_and_model(rx):
    body = rx.build_metrics(RUN, ROWS, _settings(rx, "datadog"))
    metrics = {m["name"]: m for m in body["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]}
    assert set(metrics) == {"agentexpress.runs", "agentexpress.run.failures", "agentexpress.run.duration",
                            "agentexpress.cost", "agentexpress.tokens"}
    for m in metrics.values():
        assert m["sum"]["aggregationTemporality"] == 1 and m["sum"]["isMonotonic"] is True   # DELTA
    assert metrics["agentexpress.runs"]["sum"]["dataPoints"][0]["asInt"] == "1"
    assert metrics["agentexpress.run.failures"]["sum"]["dataPoints"][0]["asInt"] == "0"
    assert metrics["agentexpress.run.duration"]["sum"]["dataPoints"][0]["asDouble"] == pytest.approx(14.0)
    tokens = {(_pt(p, "gen_ai.agent.name"), _pt(p, "gen_ai.token.type")): p["asInt"]
              for p in metrics["agentexpress.tokens"]["sum"]["dataPoints"]}
    assert tokens[("writer", "input")] == "120" and tokens[("reviewer", "output")] == "5"
    costs = {_pt(p, "gen_ai.agent.name"): p["asDouble"] for p in metrics["agentexpress.cost"]["sum"]["dataPoints"]}
    assert costs["agentcore"] == pytest.approx(0.0005)
    failed = rx.build_metrics({**RUN, "overall": "failed"}, ROWS, _settings(rx, "datadog"))
    fm = {m["name"]: m for m in failed["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]}
    assert fm["agentexpress.run.failures"]["sum"]["dataPoints"][0]["asInt"] == "1"


def _pt(p, key):
    return next((next(iter(a["value"].values())) for a in p["attributes"] if a["key"] == key), None)


def test_datadog_names_the_service_after_project(rx, monkeypatch):
    monkeypatch.setenv("APP_NAME", "claims_app")
    attrs = rx.resource(_settings(rx, "datadog", project="claims-svc"), RUN)["attributes"]
    assert {"key": "service.name", "value": {"stringValue": "claims-svc"}} in attrs
    attrs = rx.resource(_settings(rx, "langfuse", project="cm1x"), RUN)["attributes"]
    assert {"key": "service.name", "value": {"stringValue": "claims_app"}} in attrs


# --- sending ------------------------------------------------------------------------------------

def test_a_run_that_ended_is_sent_once_with_metrics_and_its_status_recorded(rx, monkeypatch):
    wire, st, _ = _on(monkeypatch, rx, {"destination": "datadog", "secret": "dd"},
                      headers={"dd-api-key": DD_KEY})
    result = rx.on_run_end(SID)
    assert result["ok"] and result["traceId"] == rx.trace_id_for(SID) and result["spans"] > 3
    traces, metrics = wire.to("/v1/traces"), wire.to("/v1/metrics")
    assert [t["url"] for t in traces] == ["https://otlp.datadoghq.com/v1/traces"]
    assert traces[0]["headers"] == {"dd-api-key": DD_KEY, "dd-otlp-source": "llmobs",
                                    "Content-Type": "application/json"}
    assert traces[0]["timeout"] == rx.REQUEST_TIMEOUT_S
    # Datadog's metrics intake takes only the key.
    assert metrics[0]["headers"] == {"dd-api-key": DD_KEY, "Content-Type": "application/json"}
    export = st.items[SID]["export"]
    assert export["status"] == "sent" and export["traceId"] == result["traceId"] and export["at"].endswith("Z")
    assert "error" not in export


def test_a_run_waiting_at_a_gate_is_not_sent(rx, monkeypatch):
    wire, st, _ = _on(monkeypatch, rx, {"destination": "otlp", "secret": "s", "endpoint": "https://c.example.com"},
                      status=FakeStatus({SID: {**RUN, "overall": "waiting_human"}}))
    assert rx.on_run_end(SID) is None
    assert wire.sent == [] and st.updates == []


def test_langfuse_gets_the_trace_then_each_score_with_only_the_key(rx, monkeypatch):
    wire, _st, _ = _on(monkeypatch, rx, {"destination": "langfuse", "region": "us", "secret": "lf"})
    result = rx.on_run_end(SID)
    assert result["ok"]
    assert wire.sent[0]["url"] == "https://us.cloud.langfuse.com/api/public/otel/v1/traces"
    assert wire.sent[0]["headers"]["x-langfuse-ingestion-version"] == "4"
    assert wire.sent[0]["headers"]["Authorization"] == LF_AUTH
    scores = wire.to("/api/public/scores")
    assert len(scores) == 1
    assert scores[0]["headers"] == {"Authorization": LF_AUTH, "Content-Type": "application/json"}
    body = scores[0]["body"]
    assert body["traceId"] == result["traceId"] and body["name"] == "Builtin.Helpfulness"
    assert body["value"] == pytest.approx(0.83) and body["dataType"] == "NUMERIC" and body["comment"] == "Very Helpful"
    assert body["observationId"] == rx.agent_span_id(SID, "writer", 1)
    assert wire.to("/v1/metrics") == []   # Langfuse computes its own


def test_the_saved_headers_win_over_the_fixed_ones(rx, monkeypatch):
    wire, _, _ = _on(monkeypatch, rx, {"destination": "langfuse", "secret": "lf"},
                     headers={"Authorization": LF_AUTH, "X-Langfuse-Ingestion-Version": "5"})
    rx.on_run_end(SID)
    h = wire.sent[0]["headers"]
    assert h["X-Langfuse-Ingestion-Version"] == "5" and "x-langfuse-ingestion-version" not in h


@pytest.mark.parametrize("reply,status,fragment", [
    (urllib.error.HTTPError("u", 401, "Unauthorized", {}, None), 401, "HTTP 401"),
    (TimeoutError("timed out"), 0, "TimeoutError"),
    ((200, '{"partialSuccess": {"rejectedSpans": "2", "errorMessage": "bad span"}}'), 200, "2 rejected: bad span"),
])
def test_a_failed_send_is_recorded_and_never_quotes_the_key(rx, monkeypatch, capsys, reply, status, fragment):
    wire, st, _ = _on(monkeypatch, rx, {"destination": "otlp", "secret": "s", "endpoint": "https://c.example.com"},
                      headers={"Authorization": LF_AUTH})
    wire.replies = [reply]
    result = rx.on_run_end(SID)
    assert result["ok"] is False and result["status"] == status and fragment in result["error"]
    assert st.items[SID]["export"]["status"] == "failed"
    out = capsys.readouterr().out + json.dumps(st.items[SID]["export"]) + json.dumps(result)
    assert LF_AUTH not in out and "cGstbGYtMTpzay1sZi0y" not in out


def test_the_key_is_not_on_any_span(rx, monkeypatch):
    wire, _, _ = _on(monkeypatch, rx, {"destination": "otlp", "secret": "s", "endpoint": "https://c.example.com"},
                     content=True)
    rx.on_run_end(SID)
    assert all(LF_AUTH not in json.dumps(s["body"]) for s in wire.sent)


def test_every_request_goes_through_the_ssrf_guard(rx, monkeypatch):
    """The real _post: an endpoint resolving to a private address is refused, unconnected."""
    import socket
    m = _mod()
    private = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.7", 443))]
    monkeypatch.setattr(socket, "getaddrinfo", lambda *a, **k: private)
    monkeypatch.setattr(socket.socket, "connect", lambda *a: pytest.fail("connected"))
    ok, status, error = m.send("https://collector.internal.example.com/v1/traces", {"resourceSpans": []}, {})
    assert (ok, status) == (False, 0) and "non-public" in error
    ok, _, error = m.send("http://collector.example.com/v1/traces", {}, {})
    assert ok is False and "https" in error


def test_paused_by_an_admin_nothing_is_sent_and_the_run_says_so(rx, monkeypatch):
    status = FakeStatus({SID: dict(RUN), "_export#paused": {"session_id": "_export#paused", "paused": True}})
    wire, st, _ = _on(monkeypatch, rx, {"destination": "otlp", "secret": "s", "endpoint": "https://c.example.com"},
                      status=status)
    rx.on_run_end(SID)
    assert wire.sent == [] and st.items[SID]["export"]["status"] == "paused"
    assert rx.export_run(SID)["error"] == "sending is paused by an admin"


def test_an_unreadable_pause_flag_counts_as_paused_and_is_read_every_30_s(rx, monkeypatch):
    t = [100.0]
    failing = FakeStatus(fail=True)
    assert rx.is_paused(failing, now=lambda: t[0]) is True
    good = FakeStatus({})
    t[0] += 10
    assert rx.is_paused(good, now=lambda: t[0]) is True and good.gets == []   # cached
    t[0] += 25
    assert rx.is_paused(good, now=lambda: t[0]) is False and good.gets == ["_export#paused"]


def test_the_run_is_read_across_pages(rx, monkeypatch):
    wire, _, tel = _on(monkeypatch, rx, {"destination": "otlp", "secret": "s", "endpoint": "https://c.example.com"},
                       page=3)
    result = rx.on_run_end(SID)
    assert tel.calls == 3 and result["ok"]
    names = [s["name"] for s in wire.to("/v1/traces")[0]["body"]["resourceSpans"][0]["scopeSpans"][0]["spans"]]
    assert "invoke_agent reviewer" in names


def test_a_run_too_large_is_sent_without_content_first(rx, monkeypatch):
    monkeypatch.setattr(_mod(), "MAX_BODY", 6000)
    wire, _st, _ = _on(monkeypatch, rx, {"destination": "otlp", "secret": "s", "endpoint": "https://c.example.com"},
                      content=True)
    result = rx.on_run_end(SID)
    assert result["ok"] and "too large" in result["error"]
    sent = json.dumps(wire.to("/v1/traces")[0]["body"])
    assert len(sent) <= 6000 and "MODEL-ANSWER" not in sent


def test_after_burst_is_bounded_and_never_raises(rx, monkeypatch):
    _on(monkeypatch, rx, {"destination": "otlp", "secret": "s", "endpoint": "https://c.example.com"})
    monkeypatch.setattr(rx, "on_run_end", lambda sid: (_ for _ in ()).throw(RuntimeError("boom")))
    asyncio.run(rx.after_burst(SID))
    import time as _t
    monkeypatch.setattr(rx, "RUN_END_WAIT_S", 0.05)
    monkeypatch.setattr(rx, "on_run_end", lambda sid: _t.sleep(0.5))

    async def timed():
        started = _t.monotonic()
        await rx.after_burst(SID)
        return _t.monotonic() - started
    # The run's task waits at most the bound; the send thread finishes on its own.
    assert asyncio.run(timed()) < 0.4


# --- the runtime actions ---------------------------------------------------------------------------

def test_export_test_sends_one_small_trace_now(rx, monkeypatch):
    wire, st, _ = _on(monkeypatch, rx, {"destination": "honeycomb", "region": "eu", "secret": "hc"},
                      headers={"x-honeycomb-team": "hc" + "key"})
    result = rx.action({"action": "export_test"})
    assert result["ok"] and result["status"] == 200 and re.fullmatch(r"[0-9a-f]{32}", result["traceId"])
    assert len(wire.sent) == 1 and wire.sent[0]["url"] == "https://api.eu1.honeycomb.io/v1/traces"
    spans = wire.sent[0]["body"]["resourceSpans"][0]["scopeSpans"][0]["spans"]
    assert len(spans) == 1 and spans[0]["name"] == "AgentExpress connection test"
    assert st.updates == []
    assert rx.action({"action": "export_test"})["traceId"] != result["traceId"]


def test_export_run_sends_the_trace_and_scores_but_not_metrics_again(rx, monkeypatch):
    wire, st, _ = _on(monkeypatch, rx, {"destination": "otlp", "secret": "s", "endpoint": "https://c.example.com"})
    result = rx.action({"action": "export_run", "session_id": SID})
    assert result["ok"] and result["spans"] > 3 and set(result) == {"ok", "status", "error", "traceId", "spans"}
    assert wire.to("/v1/metrics") == [] and len(wire.to("/v1/traces")) == 1
    assert st.items[SID]["export"]["status"] == "sent"


@pytest.mark.parametrize("sid", [None, "", "_export#paused", "../x", "a" * 65, 7])
def test_export_run_refuses_what_is_not_a_run_id(rx, monkeypatch, sid):
    wire, _, _ = _on(monkeypatch, rx, {"destination": "otlp", "secret": "s", "endpoint": "https://c.example.com"})
    assert rx.export_run(sid) == {"ok": False, "status": 0, "error": "session_id must be a run id", "traceId": ""}
    assert wire.sent == []


def test_export_run_of_an_unknown_run(rx, monkeypatch):
    wire, _st, _ = _on(monkeypatch, rx, {"destination": "otlp", "secret": "s", "endpoint": "https://c.example.com"},
                      status=FakeStatus({}))
    assert rx.export_run("feedbeef0000")["error"] == "no such run" and wire.sent == []


def test_a_later_evaluation_sends_its_scores(rx, monkeypatch):
    rows = [*ROWS, _row("eval", label="Builtin.Correctness", n=30, ts="2026-03-02 11:00:00",
                        value=Decimal("0.5"), status="ok")]
    wire, _, _ = _on(monkeypatch, rx, {"destination": "langfuse", "secret": "lf"}, rows=rows)
    assert rx.after_eval(SID, "writer", "2026-03-02 10:30:00") == 1
    assert [s["body"]["name"] for s in wire.sent] == ["Builtin.Correctness"]
    otlp = {"destination": "otlp", "secret": "s", "endpoint": "https://c.example.com"}
    wire2, _, _ = _on(monkeypatch, rx, otlp, rows=rows)
    assert rx.after_eval(SID, "writer", "2026-03-02 10:30:00") == 1
    span = wire2.sent[0]["body"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
    assert span["parentSpanId"] == rx.agent_span_id(SID, "writer", 1)
    assert span["events"][0]["name"] == "gen_ai.evaluation.result"


def test_scores_are_idempotent_and_carry_the_explanation_only_with_content(rx):
    a, b = rx.build_scores(SID, ROWS, False), rx.build_scores(SID, ROWS, False)
    assert a == b and a[0]["id"].startswith("ax-") and len(a[0]["id"]) == 32
    assert "JUDGE-EXPLANATION" in rx.build_scores(SID, ROWS, True)[0]["comment"]


# --- wiring -------------------------------------------------------------------------------------

def test_the_runtime_sends_at_the_end_of_a_burst_and_answers_the_two_actions():
    src = (ORCH / "app" / "orchestrator" / "runtime.py").read_text()
    run = src[src.index("async def _run(session_id"):src.index("async def _run_eval")]
    sent, flushed = run.index("runexport.after_burst(session_id)"), run.index("otel.force_flush()")
    assert sent < flushed < run.index("complete_async_task")
    assert 'if action in ("export_test", "export_run"):' in src
    assert "runexport.action, payload" in src
    ev = src[src.index("async def _run_eval"):src.index("async def _run_insights")]
    assert "runexport.after_eval, session_id, agent_id, since" in ev


# --- the IaC twins ------------------------------------------------------------------------------

TF = (ORCH / "terraform" / "export.tf").read_text()
CDK = (ORCH / "cdk" / "lib" / "export.ts").read_text()


def test_both_iac_paths_set_the_same_variables_on_the_orchestrator_only():
    tf_env = re.search(r"export_env = local\.export_on \? \{(.*?)\} : \{\}", TF, re.DOTALL).group(1)
    tf_keys = re.findall(r"^\s*(OTLP_EXPORT_[A-Z_]+)\s*=", tf_env, re.MULTILINE)
    cdk_keys = re.findall(r"(OTLP_EXPORT_[A-Z_]+):", CDK.split("return {")[-1])
    assert tf_keys == cdk_keys == ["OTLP_EXPORT_DESTINATION", "OTLP_EXPORT_HEADERS_SECRET_ARN",
                                   "OTLP_EXPORT_INCLUDE_CONTENT"]
    rx = _mod()
    export = sys.modules["app.features.observability.export"]
    assert {rx.DESTINATION_ENV, export.HEADERS_SECRET_ENV, export.INCLUDE_CONTENT_ENV} == set(tf_keys)
    main = (ORCH / "terraform" / "main.tf").read_text()
    sub = (ORCH / "terraform" / "subagent_runtimes.tf").read_text()
    assert len(re.findall(r"\blocal\.export_env\b", main)) == 1
    assert not re.findall(r"\blocal\.export_env\b", sub)
    stack = (ORCH / "cdk" / "lib" / "orchestrator-stack.ts").read_text()
    assert stack.count("...runExportEnv,") == 1


def test_both_iac_paths_accept_the_same_destinations_and_secret_arns():
    rx = _mod()
    export = sys.modules["app.features.observability.export"]
    tf_list = re.search(r"export_destinations\s*=\s*\[([^\]]*)\]", TF).group(1)
    cdk_list = re.search(r"EXPORT_DESTINATIONS = \[([^\]]*)\]", CDK).group(1)
    assert tuple(re.findall(r'"([^"]+)"', tf_list)) == tuple(re.findall(r'"([^"]+)"', cdk_list)) == rx.DESTINATIONS
    assert export.SECRET_ARN_PATTERN in TF
    cdk_pattern = re.search(r"EXPORT_SECRET_ARN_PATTERN = /(.*)/;", CDK).group(1).replace("\\/", "/")
    assert cdk_pattern == export.SECRET_ARN_PATTERN


def test_terraform_creates_and_grants_only_when_on_and_only_on_that_secret():
    assert "count                   = local.export_creates_secret ? 1 : 0" in TF
    assert "count         = local.export_creates_secret ? 1 : 0" in TF
    assert "count = local.export_on ? 1 : 0" in TF
    assert re.search(r'export_on\s*=\s*local\.workflow_plane && local\.export_cfg != null', TF)
    assert 'Action   = ["secretsmanager:GetSecretValue"]\n      Resource = local.export_secret_arn' in TF
    assert "role  = aws_iam_role.runtime[0].id" in TF and "subagent" not in TF.split("resource")[-1]
    main = (ORCH / "terraform" / "main.tf").read_text()
    wait = re.search(r'resource "time_sleep" "iam_propagation" \{(.*?)\n\}', main, re.DOTALL).group(1)
    assert "aws_iam_role_policy.runtime_export" in wait and "aws_secretsmanager_secret_version.export_headers" in wait


def test_the_created_secret_is_one_the_builder_deploy_role_may_create():
    policy = json.loads((ORCH / "terraform" / "deploy-role-policy.json").read_text())
    patterns = next(s["Resource"] for s in policy["Statement"] if s.get("Sid") == "RuntimeSecrets")
    secrets = (ORCH / "terraform" / "secrets.tf").read_text()
    prefix = re.search(r'runtime_secret_prefix = "([^"]+)"', secrets).group(1).replace("${var.agent_name}", "claims")
    name = prefix + "export-headers-abc"
    assert 'name_prefix             = "${local.runtime_secret_prefix}export-headers-"' in TF
    import fnmatch
    assert any(fnmatch.fnmatch(f"arn:aws:secretsmanager:us-east-1:1:secret:{name}", p) for p in patterns)


def test_the_workflow_key_wins_over_the_otlp_deploy_settings_on_both_paths():
    otlp = (ORCH / "terraform" / "otlp_export.tf").read_text()
    assert 'otlp_export_enabled = var.otlp_endpoint != "" && !local.export_on' in otlp
    assert 'check "export_replaces_otlp_settings"' in TF
    stack = (ORCH / "cdk" / "lib" / "orchestrator-stack.ts").read_text()
    assert "const otlp = otlpExport(runExport ? {} : props);" in stack


def test_the_headers_reach_terraform_and_cdk_from_the_build_secret():
    variables = (ORCH / "terraform" / "variables.tf").read_text()
    block = re.search(r'variable "export_headers" \{(.*?)\n\}', variables, re.DOTALL).group(1)
    assert "sensitive   = true" in block and "default     = {}" in block
    runner = (ORCH / "deployer" / "runner.py").read_text()
    assert 'out["EXPORT_HEADERS"] = out["TF_VAR_export_headers"]' in runner
    bin_ts = (ORCH / "cdk" / "bin" / "orchestrator.ts").read_text()
    assert 'parseSecretMap("EXPORT_HEADERS"' in bin_ts and "process.env.EXPORT_HEADERS" in bin_ts


# --- the secret check at save time --------------------------------------------------------------------

@pytest.mark.parametrize("value,ok", [
    ('{"Authorization": "Basic x"}', True),
    ('{"dd-api-key": "k", "dd-otlp-source": "llmobs"}', True),
    ("Basic x", False), ("{}", False), ("[]", False), ('{"a b": "x"}', False),
    ('{"x": "a\\r\\nInjected: 1"}', False), ('{"x": 1}', False),
    (json.dumps({f"h{i}": "v" for i in range(21)}), False),
])
def test_an_export_headers_secret_must_be_a_json_object_of_headers(value, ok):
    sys.path.insert(0, str(ORCH / "bff"))
    import export_check
    problem = export_check.headers_problem(value)
    assert (problem == "") is ok
    assert "Basic x" not in problem and "Injected" not in problem
