"""A second destination for the runtime's spans: any OTLP/HTTP endpoint (off by default).

Teams that already read traces in Langfuse, their own OpenTelemetry Collector or another
OTLP backend can have each run's spans sent there too. AgentCore Observability in
CloudWatch keeps receiving exactly what it does without this.

How the tracer provider is set up in this image (read from the installed source of
aws-opentelemetry-distro 0.19.0 and opentelemetry-sdk 1.44.0, the versions requirements.lock
pins, not from memory):

* The Dockerfile starts the process as `opentelemetry-instrument python -m app.entry`. Before
  `app.entry` runs, ADOT's configurator (`aws_opentelemetry_configurator._init_tracing`)
  builds ONE `opentelemetry.sdk.trace.TracerProvider`, adds a `BatchSpanProcessor` per
  exporter named in OTEL_TRACES_EXPORTER, then `_customize_span_processors` adds, because
  AGENT_OBSERVABILITY_ENABLED=true, a `BatchUnsampledSpanProcessor` whose exporter posts to
  AgentCore's managed endpoint (SigV4-signed when it is an X-Ray endpoint), plus
  `GenAiNestedClientSpanProcessor` and `BaggageSpanProcessor`. Then `set_tracer_provider`.
* `TracerProvider.add_span_processor` APPENDS to its `SynchronousMultiSpanProcessor`; nothing
  is replaced. `otel._ensure_baggage_processor` already adds one this way. So a second
  exporter is one more processor on the same provider, and the CloudWatch path is untouched.

What this module adds, when OTLP_EXPORT_ENDPOINT is set (the IaC sets it only when the
deploy setting is): one processor that copies each ended span, strips the copy of prompt and
answer content unless OTLP_EXPORT_INCLUDE_CONTENT is "true", and hands the copy to a
`BatchSpanProcessor` around the SDK's `OTLPSpanExporter` (opentelemetry-exporter-otlp-proto-
http, already installed by ADOT; no new dependency).

Why a COPY, made when the span ends. ADOT's CloudWatch exporter (`OTLPAwsSpanExporter`) runs
`LLOHandler.process_spans`, which moves content into log events and removes it from the span
object IN PLACE. Both processors receive the same span object, so reading it later from our
own export thread would race that. The copy is made synchronously in `on_end`; stripping
works on the copy, so the span CloudWatch exports is never modified by us. In the rare case
ADOT's thread strips a span before our copy is made, the copy simply has no content, which is
the safe direction.

Headers come from a Secrets Manager secret (OTLP_EXPORT_HEADERS_SECRET_ARN, a JSON object of
header name -> value), read once at startup. They are passed to the exporter and nowhere else:
not logged, not put in the environment, not on a span.

The exporter is built with explicit arguments because `OTLPSpanExporter` otherwise falls back
to OTEL_EXPORTER_OTLP_* environment variables meant for the CloudWatch path:
* `headers`: an empty dict would fall back to OTEL_EXPORTER_OTLP_(TRACES_)HEADERS, which would
  then go to a third party. So the secret must hold at least one header.
* `session`: without one it may load a credential provider from
  OTEL_PYTHON_EXPORTER_OTLP_HTTP_TRACES_CREDENTIAL_PROVIDER. A plain `requests.Session` means a
  request to the third party is never SigV4-signed with the runtime's role.
* `certificate_file=True`: verify TLS against the default CA bundle whatever the env says.
* `timeout`, `compression`: fixed here, not read from the env.

A run is never affected by this. A bad setting, an unreadable secret or a missing package
turns the export off with one warning at startup. A failing or slow endpoint drops spans with
a rate-limited warning; nothing raises into a run. `force_flush` (called at the end of each
burst, from the event loop) waits at most FLUSH_WAIT_S for this exporter, because the SDK's
`BatchProcessor.force_flush` ignores its timeout and would otherwise block for the full
export timeout on a hung endpoint.
"""
from __future__ import annotations

import ipaddress
import json
import logging
import os
import re
import socket
import threading
import time
from urllib.parse import urlsplit


def _log(message: str) -> None:
    # print, as the rest of the runtime does: stdout is what reaches the runtime's log group.
    print(f"[otlp-export] {message}", flush=True)


ENDPOINT_ENV = "OTLP_EXPORT_ENDPOINT"
HEADERS_SECRET_ENV = "OTLP_EXPORT_HEADERS_SECRET_ARN"  # noqa: S105 - an env var name, not a secret  # nosec B105
INCLUDE_CONTENT_ENV = "OTLP_EXPORT_INCLUDE_CONTENT"

#: The traces URL (the full path, e.g. .../v1/traces), https only, no user info, query or
#: fragment. The same pattern as var.otlp_endpoint (terraform/variables.tf) and
#: OTLP_ENDPOINT_PATTERN (cdk/lib/otlp-export.ts); tests/test_otlp_export.py pins all three.
ENDPOINT_PATTERN = r"^https://[A-Za-z0-9.-]+(:[0-9]{1,5})?(/[^\s?#]*)?$"
#: A full secret ARN, with the six-character suffix Secrets Manager adds: IAM grants on
#: exactly this ARN, and a partial ARN would not match it.
SECRET_ARN_PATTERN = r"^arn:aws:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@!-]+-[A-Za-z0-9]{6}$"  # noqa: S105 - a pattern  # nosec B105

EXPORT_TIMEOUT_S = 10
FLUSH_WAIT_S = 3.0
WARN_EVERY_S = 60.0
_MAX_HEADERS = 20
_MAX_HEADER_VALUE = 8192
_HEADER_NAME = re.compile(r"^[A-Za-z0-9!#$%&'*+.^_`|~-]+$")

#: Attributes that carry prompt, answer or tool input/output text. The exact keys are ADOT's
#: own list of such attributes (`LLO_PATTERNS` in amazon/opentelemetry/distro/llo_handler.py,
#: 0.19.0) plus the ones this app sets: gen_ai.task.* on agent spans (orchestrator/nodes.py)
#: and gen_ai.tool.call.* on tool spans (features/gateway/client.py).
CONTENT_KEYS = frozenset({
    "gen_ai.prompt", "gen_ai.completion", "gen_ai.content.revised_prompt",
    "gen_ai.agent.actual_output", "gen_ai.agent.human_input",
    "gen_ai.input.messages", "gen_ai.output.messages", "gen_ai.system_instructions",
    "gen_ai.user.message", "gen_ai.assistant.message", "gen_ai.system.message",
    "gen_ai.tool.message", "gen_ai.choice",
    "traceloop.entity.input", "traceloop.entity.output",
    "crewai.crew.tasks_output", "crewai.crew.result",
    "input.value", "output.value", "system_prompt", "tool.result", "llm.prompts",
    "gen_ai.task.input", "gen_ai.task.output",
    "agentcore.invocation.user_prompt", "agentcore.invocation.agent_response",
    "gen_ai.tool.call.arguments", "gen_ai.tool.call.result",
})
#: ADOT's indexed patterns (gen_ai.prompt.0.content, llm.input_messages.0.message.content, ...).
#: The whole family goes, roles included.
CONTENT_PREFIXES = ("gen_ai.prompt.", "gen_ai.completion.", "llm.input_messages.", "llm.output_messages.")


def is_content(key: str) -> bool:
    return key in CONTENT_KEYS or key.startswith(CONTENT_PREFIXES)


class SettingError(ValueError):
    """A deploy setting this module cannot use. The message never contains a header value."""


def require_endpoint(url: str) -> str:
    """The endpoint, or SettingError. https only (the request carries the auth headers), and
    the host must be a public address: the same rule as app/common/a2a_agent.py's
    _require_public_host, not imported because that module loads the workflow config."""
    if not re.match(ENDPOINT_PATTERN, url or ""):
        raise SettingError(f"{ENDPOINT_ENV} must be an https:// URL with no user info, query or fragment")
    host = urlsplit(url).hostname or ""
    try:
        infos = socket.getaddrinfo(host, urlsplit(url).port or 443, proto=socket.IPPROTO_TCP)
    except (socket.gaierror, UnicodeError):
        return url  # left to fail on the request, which only drops spans
    for info in infos:
        if not ipaddress.ip_address(info[4][0].split("%", 1)[0]).is_global:
            raise SettingError(f"{ENDPOINT_ENV} host {host} is not a public address")
    return url


def require_secret_arn(arn: str) -> str:
    if not re.match(SECRET_ARN_PATTERN, arn or ""):
        raise SettingError(f"{HEADERS_SECRET_ENV} must be the full ARN of a Secrets Manager secret")
    return arn


def load_headers(arn: str, client=None) -> dict[str, str]:
    """The secret's JSON object of header name -> value. Errors name the problem, never a value."""
    if client is None:
        import boto3
        # The secret's own region, so it need not be in the runtime's.
        client = boto3.client("secretsmanager", region_name=arn.split(":")[3])
    raw = client.get_secret_value(SecretId=arn).get("SecretString") or ""
    try:
        values = json.loads(raw)
    except ValueError:
        raise SettingError("the headers secret is not JSON") from None
    if not isinstance(values, dict) or not values:
        raise SettingError("the headers secret must be a JSON object with at least one header")
    if len(values) > _MAX_HEADERS:
        raise SettingError(f"the headers secret has more than {_MAX_HEADERS} headers")
    headers: dict[str, str] = {}
    for i, (name, value) in enumerate(values.items(), 1):
        if not isinstance(name, str) or not _HEADER_NAME.match(name):
            raise SettingError(f"header {i} in the headers secret has an invalid name")
        if (not isinstance(value, str) or len(value) > _MAX_HEADER_VALUE
                or any(c in value for c in "\r\n\0")):
            raise SettingError(f"header {name!r} in the headers secret has an invalid value")
        headers[name] = value
    return headers


def _otlp_exporter(endpoint: str, headers: dict[str, str]):
    """The SDK's OTLP/HTTP exporter, every input explicit (see the module docstring)."""
    import requests
    from opentelemetry.exporter.otlp.proto.http import Compression
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    return OTLPSpanExporter(endpoint=endpoint, headers=dict(headers), timeout=EXPORT_TIMEOUT_S,
                            compression=Compression.NoCompression, session=requests.Session(),
                            certificate_file=True)


# Records the SDK exporter logs from inside OUR export call are summarised by the
# rate-limited warning below instead. A thread-local flag, not a logger level: ADOT's
# CloudWatch exporter logs through the same logger from its own thread, and those records
# must keep reaching the log exactly as before.
_in_export = threading.local()


class _QuietOwnExport(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return not getattr(_in_export, "on", False)


_SDK_EXPORTER_LOGGER = "opentelemetry.exporter.otlp.proto.http.trace_exporter"
_QUIET = _QuietOwnExport()  # one instance, so adding it again is a no-op


try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import Event, ReadableSpan, SpanProcessor
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult
    from opentelemetry.trace import Status
    _SDK = True
except Exception:  # noqa: BLE001 - no SDK: the export cannot exist, and nothing else changes
    _SDK = False


def snapshot(span, include_content: bool):
    """A copy of an ended span for export; with include_content False, without content.

    Without content: content attributes are removed; events are dropped except "exception",
    which keeps only exception.type (its message and stack trace can quote model text); the
    status keeps its code but not its description, which is the exception text."""
    attributes = dict(span.attributes or {})
    events = list(span.events or ())
    status = span.status
    if not include_content:
        attributes = {k: v for k, v in attributes.items() if not is_content(k)}
        events = [Event("exception", {k: v for k, v in (e.attributes or {}).items() if k == "exception.type"},
                        e.timestamp) for e in events if e.name == "exception"]
        if status is not None and status.description:
            status = Status(status.status_code)
    return ReadableSpan(
        name=span.name, context=span.context, parent=span.parent, resource=span.resource,
        attributes=attributes, events=events, links=span.links, kind=span.kind, status=status,
        start_time=span.start_time, end_time=span.end_time,
        instrumentation_scope=span.instrumentation_scope)


if _SDK:
    class _Guarded(SpanExporter):
        """Never raises; a failure is counted and reported at most once a minute."""

        def __init__(self, inner, host: str, clock=time.monotonic):
            self._inner, self._host, self._clock = inner, host, clock
            self._failed = 0
            self._last_warning = None

        def export(self, spans):
            _in_export.on = True
            try:
                result = self._inner.export(spans)
                why = "" if result == SpanExportResult.SUCCESS else "the endpoint refused or timed out"
            except Exception as e:  # noqa: BLE001 - an export failure must never reach a run
                result, why = SpanExportResult.FAILURE, type(e).__name__
            finally:
                _in_export.on = False
            if why:
                self._warn(why, len(spans))
            return result

        def _warn(self, why: str, n: int) -> None:
            self._failed += n
            now = self._clock()
            if self._last_warning is not None and now - self._last_warning < WARN_EVERY_S:
                return
            _log(f"{self._failed} span(s) not delivered to {self._host} since the last warning ({why})")
            self._failed, self._last_warning = 0, now

        def shutdown(self):
            try:
                self._inner.shutdown()
            except Exception:  # noqa: BLE001 - shutting down must not raise either
                return

        def force_flush(self, timeout_millis: int = 30000) -> bool:
            return True

    class ExportProcessor(SpanProcessor):
        """Copies each ended span (stripped by default) into its own batch processor."""

        def __init__(self, exporter, include_content: bool):
            self._include = include_content
            self._batch = BatchSpanProcessor(exporter)
            self._flusher: threading.Thread | None = None

        def on_start(self, span, parent_context=None) -> None:
            return None

        def on_end(self, span) -> None:
            # An unsampled span (recorded but not sampled, e.g. under ADOT's AlwaysRecordSampler)
            # is dropped by BatchSpanProcessor.on_end, as it is for the CloudWatch path.
            try:
                self._batch.on_end(snapshot(span, self._include))
            except Exception:  # noqa: BLE001 - the span still reaches CloudWatch; only our copy is lost
                return

        def shutdown(self) -> None:
            self._batch.shutdown()

        def force_flush(self, timeout_millis: int = 30000) -> bool:
            # In a thread, joined for at most FLUSH_WAIT_S: see the module docstring. One
            # flush at a time; a later burst does not start a second behind a hung one.
            if self._flusher is None or not self._flusher.is_alive():
                self._flusher = threading.Thread(target=self._batch.force_flush, name="otlp-export-flush",
                                                 daemon=True)
                self._flusher.start()
            self._flusher.join(min(FLUSH_WAIT_S, max(timeout_millis, 0) / 1000))
            return not self._flusher.is_alive()


_installed = False


def install(client=None, exporter_factory=None, provider=None) -> bool:
    """Add the export processor to the global tracer provider, once per process.

    Off (OTLP_EXPORT_ENDPOINT unset or empty): returns False having read nothing and built
    nothing. On: True when the processor was added; False, with one warning, when a setting
    or the secret is unusable. Never raises."""
    global _installed
    endpoint = os.getenv(ENDPOINT_ENV, "").strip()
    if _installed or not endpoint:
        return False
    # One attempt per process: a bad setting is reported once, not on every run.
    _installed = True
    try:
        if not _SDK:
            raise SettingError("the OpenTelemetry SDK is not installed")
        require_endpoint(endpoint)
        arn = require_secret_arn(os.getenv(HEADERS_SECRET_ENV, "").strip())
        tp = provider or trace.get_tracer_provider()
        if not hasattr(tp, "add_span_processor"):
            raise SettingError("no SDK tracer provider (the process was not started by opentelemetry-instrument)")
        include = os.getenv(INCLUDE_CONTENT_ENV, "").strip().lower() == "true"
        headers = load_headers(arn, client)
        host = urlsplit(endpoint).hostname or ""
        exporter = (exporter_factory or _otlp_exporter)(endpoint, headers)
        logging.getLogger(_SDK_EXPORTER_LOGGER).addFilter(_QUIET)
        tp.add_span_processor(ExportProcessor(_Guarded(exporter, host), include))
        _log(f"on: spans also go to {host}, "
             + ("WITH prompt and answer content" if include else "without prompt or answer content"))
        return True
    except Exception as e:  # noqa: BLE001 - the export is optional; a run must start without it
        why = str(e) if isinstance(e, SettingError) else type(e).__name__
        _log(f"off: {why}")
        return False
