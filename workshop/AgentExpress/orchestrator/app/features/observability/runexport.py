"""Send each run to the build's observability tool: Langfuse, Datadog, Grafana Cloud,
Honeycomb or any OTLP/HTTP backend (workflow.json `orchestrator.export`, off by default).

What is sent, once a run has ended (done, failed, denied or cancelled):
* ONE trace per run, rebuilt from the run's telemetry rows (features/observability/store.py),
  so it holds every agent, dedicated ones included, whichever runtime ran them:
  - the run: session id, user id, workflow name and version; the request and the result
    only with `send.content`;
  - one span per agent run (a re-run is a second span, with its version);
  - one span per model call, with the OpenTelemetry GenAI attributes (model, provider,
    input/output tokens, finish reason), its cost in USD from this app's own meter and why
    that model was chosen; the prompt and the answer only with `send.content`;
  - one span per tool call (tool, target, latency, status); arguments and result only with
    `send.content`;
  - guardrail checks, Cedar decisions, memory reads and writes, a person asked mid-step, as
    span events on the agent span with their outcome (for Langfuse, which documents
    observation types for them, as small observations of type guardrail / event);
  - evaluation scores (`send.evals`): Langfuse through its scores API on the trace, others as
    `gen_ai.evaluation.result` events on the agent span.
* Metrics (`send.metrics`), where the destination takes per-run (delta) numbers: runs,
  failures, duration, cost and tokens per agent and model. Datadog, Honeycomb and generic
  OTLP; not Langfuse (it computes its own) and not Grafana Cloud (its metrics store expects
  cumulative counters).

The trace and span ids are derived from the session id and the telemetry row keys, so sending
a run again (`export_run`) produces the same ids: Langfuse updates the same trace rather than
adding a second one. Metrics are not sent again by `export_run`, so a run is counted once.

Encoding: OTLP/HTTP with the JSON Protobuf encoding (https://opentelemetry.io/docs/specs/otlp/),
which Langfuse, Datadog's OTLP intake, Grafana Cloud's OTLP gateway and Honeycomb document
accepting. JSON keeps this module to the standard library: the protobuf classes are not part
of what the tests can import, and one request per run is far below the volumes where the
binary encoding matters.

Safety, as for the rest of observability:
* Off (no OTLP_EXPORT_DESTINATION, which the IaC sets only when the key is in workflow.json):
  every entry point returns at once, having read nothing and called nothing.
* Never raises into a run and never delays one: it runs after the run's final status is
  written, in a worker thread, every request bounded by REQUEST_TIMEOUT_S.
* Every request goes through app/common/netguard.py: https only, no user info, a public
  address checked on connect and on every redirect. The endpoint comes from workflow.json.
* The headers (the destination's API key) come from Secrets Manager, by ARN, and go into the
  request and nowhere else: not logged, not in an error, not on a span, not in the environment.
* Paused by an admin (status-table item `_export#paused`, read at most every PAUSE_CACHE_S):
  nothing is sent and the run is marked "paused". A pause flag that cannot be read counts as
  paused, because the pause is the switch an admin uses to stop data leaving.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from app.common import clock, netguard
from app.features.observability.export import (
    HEADERS_SECRET_ENV,
    INCLUDE_CONTENT_ENV,
    SettingError,
    load_headers,
    require_secret_arn,
)


def _log(message: str) -> None:
    print(f"[run-export] {message}", flush=True)


DESTINATION_ENV = "OTLP_EXPORT_DESTINATION"

# --- where each destination receives data (the docs each value comes from) -------------------
#: Mirrors bff/export_check.py; tests/test_run_export.py holds the two equal.
DESTINATIONS = ("langfuse", "datadog", "grafana", "honeycomb", "otlp")
#: https://langfuse.com/integrations/native/opentelemetry: the OTLP endpoint per data region.
LANGFUSE_HOSTS = {"us": "https://us.cloud.langfuse.com", "eu": "https://cloud.langfuse.com"}
LANGFUSE_REGIONS = ("us", "eu", "self-hosted")
#: Datadog sites whose OTLP intake is https://otlp.<site>/v1/traces and /v1/metrics
#: (docs.datadoghq.com/opentelemetry/setup/otlp_ingest/traces/ and .../metrics/).
DATADOG_SITES = ("datadoghq.com", "us3.datadoghq.com", "us5.datadoghq.com", "datadoghq.eu",
                 "ap1.datadoghq.com", "ap2.datadoghq.com", "uk1.datadoghq.com")
#: https://docs.honeycomb.io/send-data/metrics/application/: the US and EU instances.
HONEYCOMB_HOSTS = {"us": "https://api.honeycomb.io", "eu": "https://api.eu1.honeycomb.io"}
HONEYCOMB_REGIONS = ("us", "eu")
#: The region used when none is given: each service's own default.
DEFAULT_REGION = {"langfuse": "eu", "datadog": "datadoghq.com", "honeycomb": "us"}
#: Headers a destination's docs ask for that carry no secret. The saved headers win.
#: Langfuse: without x-langfuse-ingestion-version 4 directly sent spans can take up to ten
#: minutes to appear. Datadog: dd-otlp-source llmobs routes GenAI spans to LLM Observability
#: (docs.datadoghq.com/llm_observability/instrument/otel_instrumentation/).
FIXED_HEADERS = {"langfuse": {"x-langfuse-ingestion-version": "4"},
                 "datadog": {"dd-otlp-source": "llmobs"}}
#: Metrics go only where per-run (delta) sums are accepted.
METRICS_DESTINATIONS = ("datadog", "honeycomb", "otlp")


def _base(endpoint: str) -> str:
    """An OTLP base URL from what a person pastes: with or without /v1/traces."""
    url = (endpoint or "").strip().rstrip("/")
    for suffix in ("/v1/traces", "/v1/metrics"):
        url = url.removesuffix(suffix)
    return url


def _langfuse_base(endpoint: str) -> str:
    url = _base(endpoint)
    return url.removesuffix("/api/public/otel")


def urls(destination: str, region: str = "", endpoint: str = "") -> dict:
    """{"traces", "metrics", "scores"} for a destination; "" where it takes none.
    KeyError for a region the destination does not have."""
    region = region or DEFAULT_REGION.get(destination, "")
    if destination == "langfuse":
        base = _langfuse_base(endpoint) if region == "self-hosted" else LANGFUSE_HOSTS[region]
        return {"traces": f"{base}/api/public/otel/v1/traces", "metrics": "",
                "scores": f"{base}/api/public/scores"}
    if destination == "datadog":
        if region not in DATADOG_SITES:
            raise KeyError(region)
        base = f"https://otlp.{region}"
    elif destination == "honeycomb":
        base = HONEYCOMB_HOSTS[region]
    else:
        base = _base(endpoint)
    return {"traces": f"{base}/v1/traces",
            "metrics": f"{base}/v1/metrics" if destination in METRICS_DESTINATIONS else "",
            "scores": ""}


# --- settings ---------------------------------------------------------------------------------

@dataclass(frozen=True)
class Settings:
    destination: str
    region: str = ""
    endpoint: str = ""
    project: str = ""
    secret_arn: str = ""
    content: bool = False
    evals: bool = True
    metrics: bool = True
    urls: dict = field(default_factory=dict)


def _export_block():
    from app.common import config
    orch = config.WORKFLOW.get("orchestrator") if isinstance(config.WORKFLOW, dict) else None
    return orch.get("export") if isinstance(orch, dict) else None


_warned: set = set()


def _warn_once(why: str) -> None:
    if why not in _warned:
        _warned.add(why)
        _log(f"off: {why}")


def settings() -> Settings | None:
    """The export's settings, or None when it is off. Off is decided from the environment
    first, so a deployment without the key reads nothing else."""
    destination = os.getenv(DESTINATION_ENV, "").strip()
    if not destination:
        return None
    try:
        block = _export_block()
    except Exception:  # noqa: BLE001 - a config problem turns the export off, nothing else
        block = None
    if not isinstance(block, dict) or block.get("destination") != destination:
        _warn_once("orchestrator.export in workflow.json does not match what was deployed; redeploy")
        return None
    if destination not in DESTINATIONS:
        _warn_once(f"unknown destination {destination!r}")
        return None
    try:
        arn = require_secret_arn(os.getenv(HEADERS_SECRET_ENV, "").strip())
    except SettingError as e:
        _warn_once(str(e))
        return None
    send_block = block.get("send") if isinstance(block.get("send"), dict) else {}
    region = str(block.get("region") or DEFAULT_REGION.get(destination, ""))
    endpoint = str(block.get("endpoint") or "")
    try:
        where = urls(destination, region, endpoint)
    except KeyError:
        _warn_once(f"unknown region {region!r} for {destination}")
        return None
    return Settings(
        destination=destination, region=region, endpoint=endpoint,
        project=str(block.get("project") or ""), secret_arn=arn,
        # What was DEPLOYED decides content: the IaC writes send.content into the env.
        content=os.getenv(INCLUDE_CONTENT_ENV, "").strip().lower() == "true",
        evals=send_block.get("evals", True) is not False,
        metrics=send_block.get("metrics", True) is not False and bool(where["metrics"]),
        urls=where)


_headers_lock = threading.Lock()
_headers_cache: dict = {}


def _headers(s: Settings, client=None) -> dict:
    """The fixed headers for the destination, then the saved ones (read once per process)."""
    with _headers_lock:
        if s.secret_arn not in _headers_cache:
            _headers_cache[s.secret_arn] = load_headers(s.secret_arn, client)
        saved = _headers_cache[s.secret_arn]
    names = {n.lower() for n in saved}
    fixed = {k: v for k, v in FIXED_HEADERS.get(s.destination, {}).items() if k.lower() not in names}
    return {**fixed, **saved}


def _header(headers: dict, name: str) -> dict:
    """Just the header `name` (any case), for a call that needs only the key."""
    return {k: v for k, v in headers.items() if k.lower() == name.lower()}


# --- the tables ---------------------------------------------------------------------------------

_ddb = None


def _table(env: str):
    global _ddb
    if _ddb is None:
        import boto3
        _ddb = boto3.resource("dynamodb")
    return _ddb.Table(os.environ[env])


PAUSE_KEY = "_export#paused"
PAUSE_CACHE_S = 30.0
_pause = {"value": False, "until": 0.0}


def is_paused(table=None, now=time.monotonic) -> bool:
    if now() < _pause["until"]:
        return _pause["value"]
    try:
        item = (table or _table("STATUS_TABLE")).get_item(
            Key={"session_id": PAUSE_KEY}).get("Item") or {}
        value = item.get("paused") is True
    except Exception as e:  # noqa: BLE001 - see the module docstring: unreadable = paused
        _log(f"could not read the pause flag ({type(e).__name__}); not sending")
        value = True
    _pause.update(value=value, until=now() + PAUSE_CACHE_S)
    return value


_RUN_FIELDS = ("session_id", "topic", "overall", "created", "updated_at", "user", "owner",
               "build", "workflowVersion", "nodes", "result")
ENDED = ("done", "failed", "denied", "cancelled")


def run_item(session_id: str, table=None) -> dict:
    names = {f"#f{i}": f for i, f in enumerate(_RUN_FIELDS)}
    return (table or _table("STATUS_TABLE")).get_item(
        Key={"session_id": session_id}, ProjectionExpression=", ".join(names),
        ExpressionAttributeNames=names).get("Item") or {}


MAX_ROWS = 2000
MAX_PAGES = 10


def run_rows(session_id: str, table=None) -> list[dict]:
    """The run's telemetry rows, oldest first (the sort key starts with the time)."""
    t = table or _table("TELEMETRY_TABLE")
    kwargs = {"KeyConditionExpression": "session_id = :s",
              "ExpressionAttributeValues": {":s": session_id}}
    rows: list[dict] = []
    for _ in range(MAX_PAGES):
        resp = t.query(**kwargs)
        rows.extend(resp.get("Items") or [])
        if not resp.get("LastEvaluatedKey") or len(rows) >= MAX_ROWS:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return rows[:MAX_ROWS]


def write_status(session_id: str, result: dict, table=None) -> None:
    """The run item's "export" field. Only on a run that exists (never creates an item)."""
    value = {"status": result.get("status_word") or ("sent" if result.get("ok") else "failed"),
             "traceId": result.get("traceId") or "",
             "at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")}
    if result.get("error"):
        value["error"] = str(result["error"])[:200]
    try:
        (table or _table("STATUS_TABLE")).update_item(
            Key={"session_id": session_id}, UpdateExpression="SET #e = :e",
            ConditionExpression="attribute_exists(session_id)",
            ExpressionAttributeNames={"#e": "export"}, ExpressionAttributeValues={":e": value})
    except Exception as e:  # noqa: BLE001 - the export status is information, never a failure
        _log(f"could not record the export status of {session_id} ({type(e).__name__})")


# --- building the trace -------------------------------------------------------------------------

def _hex(*parts: str, n: int) -> str:
    return hashlib.sha256("\x1f".join(parts).encode()).hexdigest()[:n]


def trace_id_for(session_id: str) -> str:
    return _hex("agentexpress-trace", session_id, n=32)


def agent_span_id(session_id: str, agent_id: str, version: int) -> str:
    return _hex("agent", session_id, agent_id, str(version), n=16)


def _row_span_id(session_id: str, row: dict) -> str:
    return _hex("row", session_id, str(row.get("sk") or ""), n=16)


def _et_ns(stamp) -> int | None:
    """An Eastern-time 'YYYY-MM-DD HH:MM:SS' (app/common/clock.py) as Unix nanoseconds."""
    try:
        et = datetime.strptime(str(stamp)[:19], clock.FMT)
    except (TypeError, ValueError):
        return None
    for hours in (-4, -5):
        utc = (et - timedelta(hours=hours)).replace(tzinfo=UTC)
        if clock._et_offset_hours(utc) == hours:
            return int(utc.timestamp()) * 1_000_000_000
    # The hour that does not exist when clocks go forward: either reading is an hour off.
    return int((et + timedelta(hours=5)).replace(tzinfo=UTC).timestamp()) * 1_000_000_000


MAX_TEXT = 8000


def _text(v) -> str:
    s = "" if v is None else str(v)
    return s if len(s) <= MAX_TEXT else s[:MAX_TEXT] + "…[truncated]"


def _num(v) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def _int(v) -> int:
    try:
        return int(v or 0)
    except (TypeError, ValueError):
        return 0


def _value(v) -> dict:
    if isinstance(v, bool):
        return {"boolValue": v}
    if isinstance(v, int):
        return {"intValue": str(v)}
    if isinstance(v, (float, Decimal)):
        return {"doubleValue": float(v)}
    if isinstance(v, (list, tuple)):
        return {"arrayValue": {"values": [_value(x) for x in v]}}
    return {"stringValue": str(v)}


def _attrs(d: dict) -> list:
    return [{"key": k, "value": _value(v)} for k, v in d.items() if v is not None and v != ""]


STATUS_UNSET, STATUS_OK, STATUS_ERROR = 0, 1, 2
KIND_INTERNAL, KIND_CLIENT = 1, 3


def _span(trace_id, span_id, parent, name, start, end, attrs, events=(), status=STATUS_UNSET,
          kind=KIND_INTERNAL) -> dict:
    span = {"traceId": trace_id, "spanId": span_id, "name": name, "kind": kind,
            "startTimeUnixNano": str(start), "endTimeUnixNano": str(max(end, start)),
            "attributes": _attrs(attrs), "status": {"code": status}}
    if parent:
        span["parentSpanId"] = parent
    if events:
        span["events"] = list(events)
    return span


def _event(name: str, at: int, attrs: dict) -> dict:
    return {"name": name, "timeUnixNano": str(at), "attributes": _attrs(attrs)}


def _messages(role: str, text: str) -> str:
    return json.dumps([{"role": role, "parts": [{"type": "text", "content": _text(text)}]}],
                      ensure_ascii=False)


def _workflow_name(run: dict) -> str:
    build = run.get("build") if isinstance(run.get("build"), dict) else {}
    return str(build.get("name") or os.getenv("APP_NAME") or "agentexpress")


def _workflow_version(run: dict) -> str:
    v = run.get("workflowVersion")
    return "" if v in (None, "") else str(_int(v))


def resource(s: Settings, run: dict) -> dict:
    """The resource every span and metric carries. For Datadog, `project` names the service,
    so the "View in Datadog" link the console builds from it finds these traces."""
    service = s.project if s.destination == "datadog" and s.project else (
        os.getenv("APP_NAME") or "agentexpress")
    return {"attributes": _attrs({"service.name": service,
                                  "service.version": _workflow_version(run) or None,
                                  "agentexpress.app": os.getenv("APP_NAME") or None})}


SCOPE = {"name": "agentexpress.export", "version": "1"}
_PROVIDERS = {"bedrock": "aws.bedrock"}


def _row_window(row: dict) -> tuple[int, int]:
    end = _et_ns(row.get("ts")) or 0
    return end - _int(row.get("latency_ms")) * 1_000_000, end


@dataclass
class _Agent:
    agent_id: str
    version: int
    span_id: str
    start: int = 0
    end: int = 0
    rows: list = field(default_factory=list)


def _agent_key(row: dict) -> tuple[str, int]:
    return str(row.get("agent_id") or ""), _int(row.get("version"))


def _model_span(tid: str, sid: str, parent: str, a: _Agent, row: dict, s: Settings, common: dict) -> dict:
    kind, label = str(row.get("kind")), str(row.get("label") or "")
    op = "chat" if kind == "llm" else "generate_content"
    tin, tout, cost = _int(row.get("input_tokens")), _int(row.get("output_tokens")), _num(row.get("cost_usd"))
    attrs = {
        "gen_ai.operation.name": op,
        "gen_ai.provider.name": _PROVIDERS.get(str(row.get("mode") or "")),
        "gen_ai.request.model": label,
        "gen_ai.usage.input_tokens": tin, "gen_ai.usage.output_tokens": tout,
        "gen_ai.request.temperature": _num(row.get("temperature")) if row.get("temperature") else None,
        "gen_ai.request.max_tokens": _int(row.get("max_tokens")) or None,
        "gen_ai.response.finish_reasons": [str(row["finish_reason"])] if row.get("finish_reason") else None,
        "gen_ai.agent.name": a.agent_id,
        # Langfuse reads gen_ai.usage.cost as the generation's total cost.
        "gen_ai.usage.cost": cost, "agentexpress.cost_usd": cost,
        "agentexpress.model.chosen_by": str(row.get("chosen_by") or "") or None,
        "agentexpress.prompt.name": str(row.get("prompt") or "") or None,
        "agentexpress.rates_source": str(row.get("rates_source") or "") or None,
        **common}
    if s.destination == "langfuse":
        attrs["langfuse.observation.type"] = "generation"
        attrs["langfuse.observation.usage_details"] = json.dumps({"input": tin, "output": tout})
        attrs["langfuse.observation.cost_details"] = json.dumps({"total": cost})
    if s.content:
        if row.get("system_prompt"):
            attrs["gen_ai.system_instructions"] = json.dumps(
                [{"type": "text", "content": _text(row.get("system_prompt"))}], ensure_ascii=False)
        attrs["gen_ai.input.messages"] = _messages("user", row.get("user_input") or "")
        attrs["gen_ai.output.messages"] = _messages("assistant", row.get("output_text") or "")
        if s.destination == "langfuse":
            attrs["langfuse.observation.input"] = attrs["gen_ai.input.messages"]
            attrs["langfuse.observation.output"] = attrs["gen_ai.output.messages"]
    start, end = _row_window(row)
    err = row.get("status") == "error" or row.get("mode") == "error"
    return _span(tid, sid, parent, f"{op} {label}", start, end, attrs,
                 status=STATUS_ERROR if err else STATUS_UNSET, kind=KIND_CLIENT)


def _tool_span(tid: str, sid: str, parent: str, row: dict, s: Settings, common: dict) -> dict:
    label = str(row.get("label") or "")
    attrs = {"gen_ai.operation.name": "execute_tool", "gen_ai.tool.name": label,
             "agentexpress.tool.target": str(row.get("mode") or ""),
             "agentexpress.tool.status": str(row.get("status") or ""),
             "agentexpress.latency_ms": _int(row.get("latency_ms")),
             "agentexpress.cost_usd": _num(row.get("cost_usd")), **common}
    if s.destination == "langfuse":
        attrs["langfuse.observation.type"] = "tool"
    if s.content:
        attrs["gen_ai.tool.call.arguments"] = _text(row.get("user_input"))
        attrs["gen_ai.tool.call.result"] = _text(row.get("output_text"))
    start, end = _row_window(row)
    err = row.get("status") == "error" or row.get("mode") == "error"
    return _span(tid, sid, parent, f"execute_tool {label}", start, end, attrs,
                 status=STATUS_ERROR if err else STATUS_UNSET, kind=KIND_CLIENT)


def _decision(kind: str, row: dict, content: bool) -> tuple[str, dict]:
    """A guardrail check, a Cedar decision, a memory read or write, or a person asked."""
    label, outcome = str(row.get("label") or ""), str(row.get("status") or "")
    if kind == "guardrail":
        return "guardrail", {"agentexpress.guardrail.source": label, "agentexpress.outcome": outcome}
    if kind == "policy":
        return "cedar.decision", {"gen_ai.tool.name": label, "agentexpress.outcome": outcome}
    if kind == "memory":
        # The namespace names whose memory it is (a customer, a subject): content.
        return f"memory.{label or 'op'}", {
            "agentexpress.memory.op": label, "agentexpress.outcome": outcome,
            "agentexpress.memory.namespace": _text(row.get("namespace")) if content else None}
    return "hitl.ask", {"agentexpress.ask.kind": label, "agentexpress.outcome": outcome,
                        "agentexpress.ask.question": _text(row.get("user_input")) if content else None,
                        "agentexpress.ask.answer": _text(row.get("output_text")) if content else None}


def _evaluation(row: dict, content: bool) -> dict:
    """The OpenTelemetry GenAI gen_ai.evaluation.result attributes. The judge's explanation
    can quote the answer, so it goes only with content."""
    return {"gen_ai.evaluation.name": str(row.get("label") or ""),
            "gen_ai.evaluation.score.value": _num(row.get("value")),
            "gen_ai.evaluation.score.label": str(row.get("eval_label") or "") or None,
            "gen_ai.evaluation.explanation": (_text(row.get("output_text"))
                                              if content and row.get("output_text") else None),
            "agentexpress.prompt.name": str(row.get("prompt") or "") or None}


def build_trace(session_id: str, run: dict, rows: list[dict], s: Settings) -> tuple[str, list[dict]]:
    """(trace id, spans) for one run, in the OTLP JSON shape. The root span comes first."""
    tid, root_id = trace_id_for(session_id), _hex("root", session_id, n=16)
    lf = s.destination == "langfuse"
    user = str(run.get("user") or next((r.get("user") for r in rows if r.get("user")), "") or "")
    name, version = _workflow_name(run), _workflow_version(run)
    # On every span: Langfuse filters by these per observation, not only on the root.
    common = {"session.id": session_id, "gen_ai.conversation.id": session_id, "user.id": user or None,
              **({"langfuse.version": version or None} if lf else {})}
    nodes = run.get("nodes") if isinstance(run.get("nodes"), dict) else {}

    agents: dict[tuple[str, int], _Agent] = {}
    root_events: list = []
    totals = {"cost": 0.0, "in": 0, "out": 0}
    for row in rows:
        kind = str(row.get("kind") or "")
        start, end = _row_window(row)
        totals["cost"] += _num(row.get("cost_usd"))
        aid, ver = _agent_key(row)
        if kind == "agentcore" or not aid or aid.startswith("__"):
            if kind == "agentcore":
                root_events.append(_event("agentcore.compute", end, {
                    "agentexpress.compute.seconds": _int(row.get("latency_ms")) / 1000,
                    "agentexpress.cost_usd": _num(row.get("cost_usd"))}))
            continue
        a = agents.get((aid, ver))
        if a is None:
            a = agents[(aid, ver)] = _Agent(aid, ver, agent_span_id(session_id, aid, ver), start, end)
        a.start, a.end = min(a.start, start), max(a.end, end)
        a.rows.append(row)

    starts = [a.start for a in agents.values()] + [_et_ns(run.get("created")) or 0]
    first = min([t for t in starts if t] or [time.time_ns()])
    last = max([a.end for a in agents.values()] + [_et_ns(run.get("updated_at")) or 0, first])

    spans: list = []
    for a in agents.values():
        events, cost, tin, tout, failed = [], 0.0, 0, 0, False
        for row in a.rows:
            kind, outcome = str(row.get("kind") or ""), str(row.get("status") or "")
            sid = _row_span_id(session_id, row)
            failed = failed or outcome == "error" or row.get("mode") == "error"
            cost += _num(row.get("cost_usd"))
            if kind in ("llm", "image"):
                tin += _int(row.get("input_tokens"))
                tout += _int(row.get("output_tokens"))
                spans.append(_model_span(tid, sid, a.span_id, a, row, s, common))
            elif kind == "tool":
                spans.append(_tool_span(tid, sid, a.span_id, row, s, common))
            elif kind in ("guardrail", "policy", "memory", "person"):
                ev_name, attrs = _decision(kind, row, s.content)
                start, end = _row_window(row)
                if lf:
                    warn = outcome in ("blocked", "denied", "error")
                    lf_type = "guardrail" if kind in ("guardrail", "policy") else "event"
                    spans.append(_span(tid, sid, a.span_id, ev_name, start, end, {
                        **attrs, "langfuse.observation.type": lf_type,
                        "langfuse.observation.level": "WARNING" if warn else None, **common}))
                else:
                    events.append(_event(ev_name, end, attrs))
            elif kind == "eval" and s.evals and not lf:
                # Langfuse gets scores through its scores API instead (build_scores).
                events.append(_event("gen_ai.evaluation.result", _row_window(row)[1], _evaluation(row, s.content)))
        totals["in"] += tin
        totals["out"] += tout
        node = nodes.get(a.agent_id) if isinstance(nodes.get(a.agent_id), dict) else {}
        failed = failed or node.get("status") == "failed"
        attrs = {"gen_ai.operation.name": "invoke_agent", "gen_ai.agent.name": a.agent_id,
                 "agentexpress.agent.version": a.version or None,
                 "gen_ai.usage.input_tokens": tin, "gen_ai.usage.output_tokens": tout,
                 "agentexpress.cost_usd": round(cost, 6), **common}
        if lf:
            attrs["langfuse.observation.type"] = "agent"
        spans.append(_span(tid, a.span_id, root_id, f"invoke_agent {a.agent_id}", a.start, a.end,
                           attrs, events, STATUS_ERROR if failed else STATUS_UNSET))

    overall = str(run.get("overall") or "")
    root = {"gen_ai.operation.name": "invoke_workflow", "gen_ai.workflow.name": name,
            "agentexpress.run.status": overall or None, "agentexpress.workflow.version": version or None,
            "agentexpress.cost_usd": round(totals["cost"], 6),
            "gen_ai.usage.input_tokens": totals["in"], "gen_ai.usage.output_tokens": totals["out"],
            **common}
    if lf:
        root["langfuse.trace.name"] = name
        root["langfuse.observation.type"] = "chain"
    if s.content:
        root["agentexpress.run.request"] = _text(run.get("topic"))
        root["agentexpress.run.result"] = _text(run.get("result"))
        if lf:
            root["langfuse.observation.input"] = _text(run.get("topic"))
            root["langfuse.observation.output"] = _text(run.get("result"))
    spans.insert(0, _span(tid, root_id, "", f"invoke_workflow {name}", first, last, root, root_events,
                          STATUS_ERROR if overall == "failed" else STATUS_UNSET))
    return tid, spans


def build_scores(session_id: str, rows: list[dict], content: bool) -> list[dict]:
    """Langfuse score bodies (POST /api/public/scores) for the run's evaluation rows. The id
    is derived from the row, so sending a run again on the same day replaces rather than
    duplicates (Langfuse replaces a score whose id, name and date match)."""
    tid, out = trace_id_for(session_id), []
    for row in rows:
        if row.get("kind") != "eval" or row.get("status") not in (None, "", "ok"):
            continue
        aid, ver = _agent_key(row)
        comment = str(row.get("eval_label") or "")
        if content and row.get("output_text"):
            comment = f"{comment}: {row['output_text']}" if comment else str(row["output_text"])
        score = {"id": "ax-" + _hex("score", session_id, str(row.get("sk") or ""), n=29),
                 "traceId": tid, "name": str(row.get("label") or "evaluation"),
                 "value": _num(row.get("value")), "dataType": "NUMERIC"}
        if aid and not aid.startswith("__"):
            score["observationId"] = agent_span_id(session_id, aid, ver)
        if comment:
            score["comment"] = comment[:500]
        out.append(score)
    return out


def build_metrics(run: dict, rows: list[dict], s: Settings) -> dict:
    """Per-run delta sums: Datadog's OTLP metrics intake accepts only delta."""
    start = _et_ns(run.get("created")) or min((_row_window(r)[0] for r in rows), default=time.time_ns())
    end = max([_et_ns(run.get("updated_at")) or 0, start] + [_row_window(r)[1] for r in rows])
    workflow, status = _workflow_name(run), str(run.get("overall") or "")
    by: dict = {}
    for row in rows:
        kind = str(row.get("kind") or "")
        if kind not in ("llm", "image", "tool", "agentcore"):
            continue
        aid = "agentcore" if kind == "agentcore" else str(row.get("agent_id") or "")
        model = str(row.get("label") or "") if kind in ("llm", "image") else ""
        b = by.setdefault((aid, model), {"cost": 0.0, "in": 0, "out": 0})
        b["cost"] += _num(row.get("cost_usd"))
        b["in"] += _int(row.get("input_tokens"))
        b["out"] += _int(row.get("output_tokens"))

    def point(attrs: dict, value) -> dict:
        p = {"attributes": _attrs({"agentexpress.workflow": workflow, **attrs}),
             "startTimeUnixNano": str(start), "timeUnixNano": str(end)}
        if isinstance(value, int):
            p["asInt"] = str(value)
        else:
            p["asDouble"] = float(value)
        return p

    def total(name, unit, desc, points) -> dict:
        return {"name": name, "unit": unit, "description": desc,
                "sum": {"dataPoints": points, "aggregationTemporality": 1, "isMonotonic": True}}

    metrics = [
        total("agentexpress.runs", "{run}", "Runs that ended", [point({"agentexpress.run.status": status}, 1)]),
        total("agentexpress.run.failures", "{run}", "Runs that failed",
              [point({}, 1 if status == "failed" else 0)]),
        total("agentexpress.run.duration", "s", "Time from a run's start to its end",
              [point({"agentexpress.run.status": status}, round((end - start) / 1e9, 3))]),
        total("agentexpress.cost", "USD", "Metered cost, per agent and model",
              [point({"gen_ai.agent.name": a, "gen_ai.request.model": m or None}, round(b["cost"], 6))
               for (a, m), b in sorted(by.items())]),
        total("agentexpress.tokens", "{token}", "Model tokens, per agent, model and direction",
              [point({"gen_ai.agent.name": a, "gen_ai.request.model": m, "gen_ai.token.type": t}, b[k])
               for (a, m), b in sorted(by.items()) if m for t, k in (("input", "in"), ("output", "out"))]),
    ]
    return {"resourceMetrics": [{"resource": resource(s, run), "scopeMetrics": [
        {"scope": SCOPE, "metrics": [m for m in metrics if m["sum"]["dataPoints"]]}]}]}


def traces_body(s: Settings, run: dict, spans: list[dict]) -> dict:
    return {"resourceSpans": [{"resource": resource(s, run), "scopeSpans": [{"scope": SCOPE, "spans": spans}]}]}


# --- sending --------------------------------------------------------------------------------------

REQUEST_TIMEOUT_S = 5.0
MAX_BODY = 4_000_000   # under Grafana Cloud's 5 MB per trace
MAX_SCORES = 50


def _post(url: str, body: bytes, headers: dict, timeout: float) -> tuple[int, str]:
    """One POST through the SSRF guard. Returns (status, response text)."""
    # netguard.urlopen opens https only, so no file: or other scheme is reachable from here.
    req = urllib.request.Request(url, data=body, method="POST", headers=headers)  # noqa: S310
    with netguard.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read(4096).decode("utf-8", "replace")


def _short(text: str) -> str:
    return re.sub(r"[\x00-\x1f\x7f]+", " ", text or "").strip()[:200]


def send(url: str, payload: dict, headers: dict) -> tuple[bool, int, str]:
    """(ok, HTTP status, error). The error never carries a header value."""
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if len(body) > MAX_BODY:
        return False, 0, f"the request is {len(body)} bytes, over the {MAX_BODY}-byte limit"
    try:
        status, text = _post(url, body, {**headers, "Content-Type": "application/json"}, REQUEST_TIMEOUT_S)
    except urllib.error.HTTPError as e:
        try:
            detail = e.read(4096).decode("utf-8", "replace")
        except Exception:  # noqa: BLE001
            detail = ""
        return False, e.code, _short(f"HTTP {e.code} {detail}")
    except urllib.error.URLError as e:
        # The guard refuses either before the request (a BlockedURL) or on connect, where
        # urllib wraps its BlockedURL in a URLError: say why in both cases.
        reason = e.reason.reason if isinstance(e.reason, netguard.BlockedURL) else e.reason
        if isinstance(reason, str):
            return False, 0, _short(reason)
        return False, 0, f"could not reach the address ({type(reason).__name__})"
    except Exception as e:  # noqa: BLE001 - an unreachable endpoint is a failed send, nothing more
        return False, 0, type(e).__name__
    # OTLP partial success: a 2xx that says what was rejected (https://opentelemetry.io/docs/specs/otlp/).
    try:
        partial = (json.loads(text or "{}") or {}).get("partialSuccess") or {}
    except (ValueError, AttributeError):
        partial = {}
    rejected = _int(partial.get("rejectedSpans") or partial.get("rejectedDataPoints"))
    if rejected:
        return False, status, _short(f"{rejected} rejected: {partial.get('errorMessage') or ''}")
    return 200 <= status < 300, status, ""


def _fits(s: Settings, run: dict, spans: list) -> bool:
    return len(json.dumps(traces_body(s, run, spans), separators=(",", ":"), ensure_ascii=False)
               .encode("utf-8")) <= MAX_BODY


def _fit(s: Settings, session_id: str, run: dict, rows: list[dict]) -> tuple[str, list[dict], str]:
    """The trace; if too large, without content; then with fewer spans, until it fits."""
    tid, spans = build_trace(session_id, run, rows, s)
    note = ""
    if not _fits(s, run, spans) and s.content:
        tid, spans = build_trace(session_id, run, rows, replace(s, content=False))
        note = "sent without prompts and answers (the run was too large)"
    while len(spans) > 1 and not _fits(s, run, spans):
        spans = spans[: max(1, len(spans) // 2)]
        note = "sent only part of the run (it was too large)"
    return tid, spans, note


def export(session_id: str, run: dict, rows: list[dict], s: Settings, metrics: bool) -> dict:
    """Send one run. Returns {ok, status, error, traceId, spans}."""
    headers = _headers(s)
    tid, spans, note = _fit(s, session_id, run, rows)
    ok, status, error = send(s.urls["traces"], traces_body(s, run, spans), headers)
    problems = [p for p in (error, note) if p]
    if ok and s.evals and s.urls.get("scores"):
        failed = sum(not send(s.urls["scores"], score, _header(headers, "Authorization"))[0]
                     for score in build_scores(session_id, rows, s.content)[:MAX_SCORES])
        if failed:
            problems.append(f"{failed} evaluation score(s) not sent")
    if metrics and s.metrics and s.urls.get("metrics"):
        # Datadog's metrics intake takes only the API key; the others, the same headers.
        mh = _header(headers, "dd-api-key") if s.destination == "datadog" else headers
        m_ok, _, m_err = send(s.urls["metrics"], build_metrics(run, rows, s), mh)
        if not m_ok:
            problems.append(f"metrics not sent ({m_err})")
    return {"ok": ok, "status": status, "error": "; ".join(problems), "traceId": tid, "spans": len(spans)}


# --- entry points (app/orchestrator/runtime.py) ------------------------------------------------------

def on_run_end(session_id: str) -> dict | None:
    """After a burst of a run: send it if the run has ended. None when nothing was done."""
    s = settings()
    if s is None:
        return None
    try:
        run = run_item(session_id)
        if run.get("overall") not in ENDED:
            return None   # waiting at a gate or for a person: the run is not over
        if is_paused():
            result = {"ok": False, "status_word": "paused", "traceId": trace_id_for(session_id)}
        else:
            result = export(session_id, run, run_rows(session_id), s, metrics=True)
        write_status(session_id, result)
        if result.get("error"):
            _log(f"{session_id}: {result['error']}")
        return result
    except Exception as e:  # noqa: BLE001 - never reaches the run
        _log(f"{session_id}: not sent ({type(e).__name__})")
        return None


def after_eval(session_id: str, agent_id: str, since: str) -> int:
    """After an on-demand evaluation (runtime._run_eval): send the scores it recorded at or
    after `since` (an Eastern-time stamp). Returns how many were sent."""
    s = settings()
    if s is None or not s.evals:
        return 0
    try:
        if is_paused():
            return 0
        rows = [r for r in run_rows(session_id) if r.get("kind") == "eval"
                and str(r.get("agent_id") or "") == agent_id and str(r.get("ts") or "") >= since]
        if not rows:
            return 0
        headers = _headers(s)
        if s.urls.get("scores"):
            return sum(send(s.urls["scores"], sc, _header(headers, "Authorization"))[0]
                       for sc in build_scores(session_id, rows, s.content)[:MAX_SCORES])
        # Elsewhere a sent span cannot change: one small span per score, under the agent's.
        tid, spans = trace_id_for(session_id), []
        for r in rows:
            aid, ver = _agent_key(r)
            start, end = _row_window(r)
            spans.append(_span(tid, _row_span_id(session_id, r), agent_span_id(session_id, aid, ver),
                               f"evaluation {r.get('label') or ''}", start, end,
                               {"session.id": session_id, "gen_ai.agent.name": aid},
                               [_event("gen_ai.evaluation.result", end, _evaluation(r, s.content))]))
        ok = send(s.urls["traces"], traces_body(s, run_item(session_id), spans), headers)[0]
        return len(spans) if ok else 0
    except Exception as e:  # noqa: BLE001
        _log(f"{session_id}: scores not sent ({type(e).__name__})")
        return 0


_SESSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
OFF = "sending runs is not set up for this app (orchestrator.export in workflow.json, then deploy)"


def _refusal(error: str, tid: str = "") -> dict:
    return {"ok": False, "status": 0, "error": error, "traceId": tid}


def export_test() -> dict:
    """{"action": "export_test"}: one small trace, now, to check the key and the address."""
    s = settings()
    if s is None:
        return _refusal(OFF)
    try:
        now = time.time_ns()
        tid, sid = os.urandom(16).hex(), os.urandom(8).hex()
        attrs = {"agentexpress.test": True, "gen_ai.workflow.name": _workflow_name({})}
        if s.destination == "langfuse":
            attrs["langfuse.trace.name"] = "AgentExpress connection test"
        span = _span(tid, sid, "", "AgentExpress connection test", now - 1_000_000, now, attrs)
        ok, status, error = send(s.urls["traces"], traces_body(s, {}, [span]), _headers(s))
        return {"ok": ok, "status": status, "error": error, "traceId": tid}
    except SettingError as e:
        return _refusal(str(e))
    except Exception as e:  # noqa: BLE001
        return _refusal(f"not sent ({type(e).__name__})")


def export_run(session_id) -> dict:
    """{"action": "export_run", "session_id": id}: send that run (again): trace and scores."""
    if not isinstance(session_id, str) or not _SESSION_RE.match(session_id):
        return _refusal("session_id must be a run id")
    s = settings()
    if s is None:
        return _refusal(OFF)
    try:
        run = run_item(session_id)
        if not run:
            return _refusal("no such run")
        if is_paused():
            return _refusal("sending is paused by an admin", trace_id_for(session_id))
        result = export(session_id, run, run_rows(session_id), s, metrics=False)
        write_status(session_id, result)
        return result
    except SettingError as e:
        return _refusal(str(e))
    except Exception as e:  # noqa: BLE001
        return _refusal(f"not sent ({type(e).__name__})")


def action(payload: dict) -> dict:
    """The runtime's export actions (contract: export_test, export_run)."""
    if payload.get("action") == "export_test":
        return export_test()
    return export_run(payload.get("session_id"))


#: How long the end of a run may wait for its export, all requests included.
RUN_END_WAIT_S = 20.0


async def after_burst(session_id: str) -> None:
    """Awaited at the end of each burst (runtime._run). Returns at once when off."""
    if not os.getenv(DESTINATION_ENV, "").strip():
        return
    try:
        await asyncio.wait_for(asyncio.to_thread(on_run_end, session_id), RUN_END_WAIT_S)
    except Exception as e:  # noqa: BLE001
        _log(f"{session_id}: not sent ({type(e).__name__})")
