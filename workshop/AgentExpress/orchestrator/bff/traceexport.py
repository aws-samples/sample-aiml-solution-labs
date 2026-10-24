"""Sending a build's runs to Langfuse, Datadog, Grafana Cloud, Honeycomb or any OTLP/HTTP
backend: the BFF side (`orchestrator.export` in workflow.json). The runtime does the
exporting (app/features/observability/export.py); this module is what the pages call.

THE CONSOLE (control plane): POST /api/builds/{id}/export/test sends ONE small test span from
this Lambda, so the Builder can say "Langfuse accepted a test trace" before anything deploys.
  * Every send goes through bff/netguard.py: https only, no user:password@, every resolved
    address public, redirects checked again. 5 seconds at most.
  * Headers typed on the page are used once and never stored here (the page saves them as the
    build's `exportHeaders` secret through PUT /api/builds/{id}/secrets, like any build secret).
  * Without typed headers, the build's SAVED ones are used, but only for the address they were
    saved for: the secret's name is `export-` + the first 16 hex digits of SHA-256 of the traces
    URL (secret_name), and the console looks up the name of the URL it is about to send to.
    Saved keys are write-only everywhere else; without this, anyone who may edit a build could
    send its Langfuse keys to a server of their own with one test.
  * Neither a header value nor the secret ever comes back: the vendor's answer is shortened and
    any header value in it is cut out before it is returned.

THE BUILD'S APP (execution plane), on its own runtime (export-contract.md sections 3-6):
    GET  /api/export                  where runs go, paused or not; admins also get the last 24 h
    POST /api/export/test             admin: the runtime's export_test
    POST /api/sessions/{id}/export    admin or the run's owner: the runtime's export_run
    PUT  /api/export/pause {paused}   admin: the `_export#paused` row the runtime reads
Every one but GET answers 404 while `orchestrator.export` is absent, before any AWS call.

The presets below (where each destination receives traces, its label, the link to a trace) are
copied in web/src/builder/exportSetup.ts; tests/fixtures/export_presets.json holds the two to the
same answers. Sources, read on the date of this change:
  Langfuse   https://langfuse.com/integrations/native/opentelemetry (endpoints per region, Basic
             auth, x-langfuse-ingestion-version: 4), https://langfuse.com/docs/tracing-features/url
             (a trace's URL: <host>/project/<projectId>/traces/<traceId>)
  Datadog    https://docs.datadoghq.com/opentelemetry/setup/otlp_ingest/traces/ (dd-api-key,
             compute_stats, http/protobuf); the endpoint per site is that page's
             otlp_trace_endpoint (https://otlp.<site>/v1/traces) and the app host its dd_full_site
             (DataDog/documentation hugo/assets/scripts/config/regions.config.js)
  Grafana    https://grafana.com/docs/grafana-cloud/send-data/otlp/send-data-otlp/ (the stack's
             OTLP endpoint; the SDK appends /v1/traces to it)
  Honeycomb  https://docs.honeycomb.io/send-data/opentelemetry/ (api.honeycomb.io,
             api.eu1.honeycomb.io, x-honeycomb-team)
  OTLP       https://opentelemetry.io/docs/specs/otlp/ (OTLP/HTTP, binary protobuf) and the
             opentelemetry-proto trace, common and resource .proto files for the field numbers.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import re
import secrets
import socket
import struct
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import netguard

DESTINATIONS = ("langfuse", "datadog", "grafana", "honeycomb", "otlp")
NAMES = {"langfuse": "Langfuse", "datadog": "Datadog", "grafana": "Grafana Cloud",
         "honeycomb": "Honeycomb", "otlp": "Your OTLP backend"}
#: Langfuse Cloud's hosts; "self-hosted" (or Langfuse's Japan and HIPAA regions) take `endpoint`.
LANGFUSE_HOSTS = {"us": "https://us.cloud.langfuse.com", "eu": "https://cloud.langfuse.com"}
LANGFUSE_REGIONS = ("us", "eu", "self-hosted")
LANGFUSE_PATH = "/api/public/otel"
#: Datadog site -> the host its web app is on (regions.config.js dd_full_site). The US1-FED and
#: US2-FED sites are left out: the OTLP traces intake page does not say it serves them.
DATADOG_SITES = {"datadoghq.com": "app.datadoghq.com", "us3.datadoghq.com": "us3.datadoghq.com",
                 "us5.datadoghq.com": "us5.datadoghq.com", "datadoghq.eu": "app.datadoghq.eu",
                 "ap1.datadoghq.com": "ap1.datadoghq.com", "ap2.datadoghq.com": "ap2.datadoghq.com",
                 "uk1.datadoghq.com": "uk1.datadoghq.com"}
HONEYCOMB_HOSTS = {"us": "https://api.honeycomb.io", "eu": "https://api.eu1.honeycomb.io"}
#: The OTLP endpoint shape the deploy settings already allow (terraform var.otlp_endpoint,
#: cdk/lib/otlp-export.ts, app/features/observability/export.py): https, a host, an optional
#: port and path, no user info, query or fragment.
ENDPOINT_PATTERN = re.compile(r"^https://[A-Za-z0-9.-]+(:[0-9]{1,5})?(/[^\s?#]*)?$")
ENDPOINT_MAX = 2048
#: A full Secrets Manager ARN (cdk/lib/otlp-export.ts OTLP_SECRET_ARN_PATTERN).
SECRET_ARN_PATTERN = re.compile(
    r"^arn:aws:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@!-]+-[A-Za-z0-9]{6}$")
SEND_KEYS = ("content", "evals", "metrics")
SEND_DEFAULTS = {"content": False, "evals": True, "metrics": True}
SETUP_KEYS = ("destination", "region", "endpoint", "secret", "project", "send")
PROJECT_MAX = 200

#: A header name is an RFC 9110 token; these are set by the HTTP client, never by a person.
_HEADER_NAME = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]{1,64}$")
_RESERVED_HEADERS = {"host", "content-length", "content-type", "content-encoding", "transfer-encoding",
                     "connection", "te", "upgrade", "expect", "trailer", "proxy-authorization"}
HEADERS_MAX = 10
HEADER_VALUE_MAX = 2048

TEST_TIMEOUT_S = 5
#: Test sends per person per minute, per warm Lambda: the route makes an outbound request.
TESTS_PER_MINUTE = 10
_tests: dict[str, list[float]] = {}

#: The status-table row the runtime reads before it exports (export-contract.md section 5).
#: Run ids are 12 hex digits, so no run can be named this; it has no owner, so no run route
#: acts on it, and the admins' list of every run leaves it out (bff/handler.py).
PAUSE_KEY = "_export#paused"
SESSION_RE = re.compile(r"^[A-Za-z0-9_-]{6,64}$")
#: The build app's routes (the console's own is CONSOLE_ROUTE, in bff/handler.py _builds_route).
ROUTES = ("GET /api/export", "POST /api/export/test", "POST /api/sessions/{id}/export",
          "PUT /api/export/pause")
CONSOLE_ROUTE = "POST /api/builds/{id}/export/test"
OFF = "runs are not sent anywhere: set it up on the build's Settings tab (Export) and deploy"
ERROR_MAX = 300


class Refused(Exception):
    """A request this module will not act on; `status` is the HTTP answer."""

    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


# --- what a setup means ---------------------------------------------------------------------

def _endpoint(value) -> str:
    url = str(value or "").strip()
    if not url:
        return ""
    if len(url) > ENDPOINT_MAX or not ENDPOINT_PATTERN.match(url):
        raise Refused(400, "the address must be an https:// URL with no user name, password, query or fragment")
    return url.rstrip("/")


#: The region a destination takes when none is given (each service's own default).
DEFAULT_REGION = {"langfuse": "eu", "datadog": "datadoghq.com", "honeycomb": "us"}


def _base(endpoint: str) -> str:
    """An OTLP base from what a person pastes, with or without /v1/traces (or /v1/metrics):
    the same rule the runtime sends by (app/features/observability/runexport.py urls)."""
    for suffix in ("/v1/traces", "/v1/metrics"):
        endpoint = endpoint.removesuffix(suffix)
    return endpoint


def traces_url(setup: dict) -> str:
    """Where `setup` sends traces: the preset for its destination and region, or its endpoint."""
    dest = str(setup.get("destination") or "")
    region = str(setup.get("region") or "") or DEFAULT_REGION.get(dest, "")
    endpoint = _endpoint(setup.get("endpoint"))
    if dest == "langfuse":
        if region not in LANGFUSE_REGIONS:
            raise Refused(400, "the Langfuse region is us, eu or self-hosted")
        if region != "self-hosted":
            return LANGFUSE_HOSTS[region] + LANGFUSE_PATH + "/v1/traces"
        if not endpoint:
            raise Refused(400, "a self-hosted Langfuse needs its address")
        return _base(endpoint).removesuffix(LANGFUSE_PATH) + LANGFUSE_PATH + "/v1/traces"
    if dest == "datadog":
        if region not in DATADOG_SITES:
            raise Refused(400, f"the Datadog site is one of: {', '.join(DATADOG_SITES)}")
        return f"https://otlp.{region}/v1/traces"
    if dest == "honeycomb":
        if region not in HONEYCOMB_HOSTS:
            raise Refused(400, "the Honeycomb region is us or eu")
        return HONEYCOMB_HOSTS[region] + "/v1/traces"
    if dest in ("grafana", "otlp"):
        if not endpoint:
            raise Refused(400, "Grafana Cloud needs your stack's OTLP endpoint" if dest == "grafana"
                          else "an OTLP backend needs its OTLP endpoint")
        # Grafana's tile shows the base (.../otlp); an SDK given a base appends /v1/traces.
        return _base(endpoint) + "/v1/traces"
    raise Refused(400, f"the destination is one of: {', '.join(DESTINATIONS)}")


def label(setup: dict) -> str:
    """How the pages name it: "Langfuse (EU)", "Datadog (us5.datadoghq.com)", ..."""
    dest = str(setup.get("destination") or "")
    region = str(setup.get("region") or "") or DEFAULT_REGION.get(dest, "")
    if dest == "langfuse":
        return {"us": "Langfuse (US)", "eu": "Langfuse (EU)"}.get(region, "Langfuse (self-hosted)")
    if dest == "datadog":
        return f"Datadog ({region})"
    if dest == "honeycomb":
        return "Honeycomb (EU)" if region == "eu" else "Honeycomb (US)"
    if dest == "grafana":
        return "Grafana Cloud"
    return "OTLP backend"


def trace_link(setup: dict) -> str:
    """A link to one run's trace, with `{traceId}` where its 32 hex digits go; "" when the
    destination documents no such link or the setup lacks what it needs (Langfuse: `project`)."""
    dest = str(setup.get("destination") or "")
    region = str(setup.get("region") or "") or DEFAULT_REGION.get(dest, "")
    if dest == "langfuse":
        project = str(setup.get("project") or "").strip()
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,200}", project):
            return ""
        if region in LANGFUSE_HOSTS:
            host = LANGFUSE_HOSTS[region]
        else:
            try:
                host = "https://" + urllib.parse.urlsplit(traces_url(setup)).netloc
            except Refused:
                return ""
        return f"{host}/project/{project}/traces/{{traceId}}"
    if dest == "datadog" and region in DATADOG_SITES:
        # Datadog documents no deep link to one trace; the Trace Explorer's query on the
        # reserved `trace_id` attribute finds it.
        return f"https://{DATADOG_SITES[region]}/apm/traces?query=trace_id%3A{{traceId}}"
    return ""


def secret_name(url: str) -> str:
    """The `exportHeaders` secret a traces URL's headers are saved under (see the docstring)."""
    return "export-" + hashlib.sha256(url.encode()).hexdigest()[:16]


def clean_headers(headers) -> dict[str, str]:
    """Headers a person typed (or a saved secret holds), checked before anything is sent."""
    if not isinstance(headers, dict) or not headers:
        raise Refused(400, "headers are an object of header name to value")
    if len(headers) > HEADERS_MAX:
        raise Refused(400, f"at most {HEADERS_MAX} headers")
    out = {}
    for name, value in headers.items():
        if not isinstance(name, str) or not _HEADER_NAME.match(name):
            raise Refused(400, "a header name is letters, digits and - (64 at most)")
        if name.lower() in _RESERVED_HEADERS:
            raise Refused(400, f"{name} is set by the client and cannot be given")
        if not isinstance(value, str) or not value or len(value) > HEADER_VALUE_MAX \
                or any(ord(c) < 32 or ord(c) == 127 for c in value):
            raise Refused(400, f"the value of {name} must be one line of text "
                               f"(at most {HEADER_VALUE_MAX} characters)")
        out[name] = value
    return out


def check_definition(definition: dict, error) -> dict:
    """A saved export setup for the library (bff/library.py COPY_KINDS): the setup without
    its keys. A build that takes it asks for its own; only an existing secret's ARN may come
    with it, since an ARN is not the secret."""
    unknown = sorted(set(definition) - set(SETUP_KEYS))
    if unknown:
        raise error(400, f"an export setup takes only: {', '.join(SETUP_KEYS)} (not {', '.join(unknown)})")
    try:
        traces_url(definition)
    except Refused as e:
        raise error(400, str(e)) from None
    secret = definition.get("secret")
    if secret not in (None, "") and not (isinstance(secret, str) and SECRET_ARN_PATTERN.match(secret)):
        raise error(400, "a saved setup keeps no keys: each build saves its own (an existing secret's ARN may stay)")
    project = definition.get("project", "")
    if not isinstance(project, str) or len(project) > PROJECT_MAX:
        raise error(400, f"project is text of at most {PROJECT_MAX} characters")
    send = definition.get("send", {})
    if not isinstance(send, dict) or set(send) - set(SEND_KEYS) or not all(isinstance(v, bool) for v in send.values()):
        raise error(400, f"send takes true or false for: {', '.join(SEND_KEYS)}")
    return definition


# --- one test span, as OTLP/HTTP binary protobuf -------------------------------------------
# The opentelemetry-proto field numbers: ExportTraceServiceRequest.resource_spans = 1;
# ResourceSpans.resource = 1, .scope_spans = 2; Resource.attributes = 1; ScopeSpans.scope = 1,
# .spans = 2; InstrumentationScope.name = 1, .version = 2; Span.trace_id = 1, .span_id = 2,
# .name = 5, .kind = 6, .start_time_unix_nano = 7 (fixed64), .end_time_unix_nano = 8 (fixed64),
# .attributes = 9, .status = 15; Status.code = 3 (1 = OK); KeyValue.key = 1, .value = 2;
# AnyValue.string_value = 1, .bool_value = 2. Written by hand: the BFF ships without the SDK.

def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def _tag(field: int, wire: int) -> bytes:
    return _varint(field << 3 | wire)


def _bytes(field: int, b: bytes) -> bytes:
    return _tag(field, 2) + _varint(len(b)) + b


def _attr(key: str, value) -> bytes:
    any_value = (_tag(2, 0) + _varint(int(value))) if isinstance(value, bool) else _bytes(1, str(value).encode())
    return _bytes(1, key.encode()) + _bytes(2, any_value)


def test_request(trace_id: bytes, span_id: bytes, start_ns: int, end_ns: int, service: str) -> bytes:
    """ExportTraceServiceRequest holding one finished, OK span."""
    span = (_bytes(1, trace_id) + _bytes(2, span_id) + _bytes(5, b"AgentExpress connection test")
            + _tag(6, 0) + _varint(1)                                    # SPAN_KIND_INTERNAL
            + _tag(7, 1) + struct.pack("<Q", start_ns) + _tag(8, 1) + struct.pack("<Q", end_ns)
            + _bytes(9, _attr("agentexpress.test", True))
            + _bytes(9, _attr("langfuse.trace.name", "AgentExpress connection test"))
            + _bytes(15, _tag(3, 0) + _varint(1)))                      # STATUS_CODE_OK
    resource = _bytes(1, _attr("service.name", service))
    scope = _bytes(1, b"agentexpress") + _bytes(2, b"connection-test")
    resource_spans = _bytes(1, resource) + _bytes(2, _bytes(1, scope) + _bytes(2, span))
    return _bytes(1, resource_spans)


def _readable(raw: bytes, headers: dict[str, str]) -> str:
    """The vendor's answer, short and printable, with every header value cut out of it."""
    text = raw[:4096].decode("utf-8", "replace")
    try:
        body = json.loads(text)
        if isinstance(body, dict):
            text = str(body.get("message") or body.get("error") or body.get("errors") or text)
    except ValueError:
        pass
    for v in headers.values():
        for secret in sorted({v, *v.split()}, key=len, reverse=True):   # "Basic <token>": the token too
            if len(secret) >= 6:
                text = text.replace(secret, "[redacted]")
    text = "".join(c if c.isprintable() else " " for c in text)
    return " ".join(text.split())[:ERROR_MAX]


_CHECK = {"langfuse": "the public and secret keys, and the region",
          "datadog": "the API key, and that the site matches your Datadog account",
          "grafana": "the instance ID and the token, and that the token may write traces",
          "honeycomb": "the API key (an ingest key), and US or EU",
          "otlp": "the headers"}


def send_test(url: str, headers: dict[str, str], destination: str, service: str,
              opener=None) -> dict:
    """POST one test span to `url`. Never raises; the answer says what happened in plain words."""
    name = NAMES.get(destination, "The backend")
    trace_id, span_id = secrets.token_bytes(16), secrets.token_bytes(8)
    end = time.time_ns()
    body = test_request(trace_id, span_id, end - 1_000_000, end, service)
    req = urllib.request.Request(url, data=body, method="POST",  # noqa: S310 - opened by netguard, https only
                                 headers={**headers, "Content-Type": "application/x-protobuf"})
    host = urllib.parse.urlsplit(url).hostname or url
    out = {"ok": False, "status": 0, "error": "", "traceId": trace_id.hex(), "url": url}
    try:
        with (opener or netguard.urlopen)(req, timeout=TEST_TIMEOUT_S) as resp:
            out["status"] = int(getattr(resp, "status", 200) or 200)
    except netguard.BlockedURL as e:
        out["error"] = f"That address is not allowed: {e.reason}."
        return out
    except urllib.error.HTTPError as e:
        out["status"] = e.code
        detail = _readable(e.read() if e.fp else b"", headers)
        if e.code in (401, 403):
            out["error"] = f"{name} refused the key (HTTP {e.code}). Check {_CHECK.get(destination, 'the keys')}."
        elif e.code == 404:
            out["error"] = f"Nothing receives traces at {url} (HTTP 404). Check the address."
        elif e.code in (408, 429) or e.code >= 500:
            out["error"] = f"{name} could not take it just now (HTTP {e.code}). Try again in a minute."
        else:
            out["error"] = f"{name} refused the test trace (HTTP {e.code})."
        if detail:
            out["error"] += f" It said: {detail}"
        return out
    except TimeoutError:
        out["error"] = f"{host} did not answer within {TEST_TIMEOUT_S} seconds."
        return out
    except (urllib.error.URLError, OSError, ValueError) as e:
        reason = getattr(e, "reason", e)
        if isinstance(reason, netguard.BlockedURL):
            # Refused at connect time (a name resolving to a private address): urllib wraps it.
            out["error"] = f"That address is not allowed: {reason.reason}."
        elif isinstance(reason, (TimeoutError, socket.timeout)):
            out["error"] = f"{host} did not answer within {TEST_TIMEOUT_S} seconds."
        else:
            out["error"] = f"Could not reach {host}: {_readable(str(reason).encode(), headers)}."
        return out
    if 200 <= out["status"] < 300:
        out["ok"] = True
        out["message"] = f"{name} accepted a test trace."
    else:
        out["error"] = f"{name} answered HTTP {out['status']}."
    return out


def _throttle(who: str, now: float | None = None) -> None:
    now = time.time() if now is None else now
    recent_tests = [t for t in _tests.get(who, []) if now - t < 60]
    if len(recent_tests) >= TESTS_PER_MINUTE:
        raise Refused(429, f"at most {TESTS_PER_MINUTE} tests a minute: wait a moment and try again")
    _tests[who] = [*recent_tests, now]
    if len(_tests) > 5000:
        _tests.clear()


def console_test(build_id: str, owner, body: dict, opener=None) -> dict:
    """POST /api/builds/{id}/export/test. The caller must be able to change the build: it is
    checked before the build's secret is read or anything is sent."""
    import builds
    item = builds._meta(build_id, owner)
    if not isinstance(body, dict):
        raise Refused(400, "send {destination, region, endpoint, headers}")
    setup = {k: body.get(k) for k in ("destination", "region", "endpoint")}
    url = traces_url(setup)
    if body.get("headers") not in (None, {}):
        headers = clean_headers(body["headers"])
    else:
        saved = (builds._read_secrets(build_id).get("exportHeaders") or {}).get(secret_name(url))
        if not saved:
            raise Refused(400, "enter the keys first: none are saved for this address yet")
        try:
            headers = clean_headers(json.loads(saved))
        except (ValueError, Refused):
            raise Refused(400, "the keys saved for this address are not usable: enter them again") from None
    _throttle(str(owner))
    return send_test(url, headers, str(setup["destination"]), str(item.get("agentName") or "agentexpress"),
                     opener=opener)


# --- the build's app --------------------------------------------------------------------------

def setup_of(raw: dict | None) -> dict | None:
    """`orchestrator.export` when it names a destination this module knows, else None (off)."""
    exp = ((raw or {}).get("orchestrator") or {}).get("export")
    if not isinstance(exp, dict) or exp.get("destination") not in DESTINATIONS:
        return None
    return exp


def _now_iso() -> str:
    return _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _when(at) -> _dt.datetime | None:
    try:
        t = _dt.datetime.fromisoformat(str(at))
    except ValueError:
        return None
    return t if t.tzinfo else t.replace(tzinfo=_dt.UTC)


def paused(table) -> dict:
    item = table.get_item(Key={"session_id": PAUSE_KEY}).get("Item") or {}
    return {"paused": item.get("paused") is True, "by": str(item.get("by") or ""), "at": str(item.get("at") or "")}


def recent(table, now: _dt.datetime | None = None) -> dict:
    """Sent and failed exports in the last 24 hours (by the run item's export.at), and the newest
    error. A Scan, as the runs list is: only for an admin's Export card."""
    from boto3.dynamodb.conditions import Attr
    since = (now or _dt.datetime.now(_dt.UTC)) - _dt.timedelta(hours=24)
    out = {"sent": 0, "failed": 0, "lastError": "", "lastErrorAt": ""}
    kw = {"ProjectionExpression": "session_id, #e", "ExpressionAttributeNames": {"#e": "export"},
          "FilterExpression": Attr("export").exists()}
    newest_failure = None
    while True:
        page = table.scan(**kw)
        for it in page.get("Items", []):
            exp = it.get("export") if isinstance(it.get("export"), dict) else {}
            at = _when(exp.get("at"))
            if at is None or at < since:
                continue
            if exp.get("status") == "sent":
                out["sent"] += 1
            elif exp.get("status") == "failed":
                out["failed"] += 1
                if newest_failure is None or at > newest_failure:
                    newest_failure = at
                    out["lastError"] = str(exp.get("error") or "")[:ERROR_MAX]
                    out["lastErrorAt"] = str(exp.get("at") or "")
        if not page.get("LastEvaluatedKey"):
            return out
        kw["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def invoke_runtime(client, runtime_arn: str, payload: dict) -> dict:
    """One synchronous runtime action; its answer as a dict. A fresh runtime session each time,
    as evaluate and insights use, so it never lands on a run's busy container. No trace header
    is passed: ADOT rewrites it after signing and the call fails."""
    try:
        resp = client.invoke_agent_runtime(
            agentRuntimeArn=runtime_arn,
            runtimeSessionId=f"export{uuid.uuid4().hex}".ljust(33, "0")[:33],
            payload=json.dumps(payload).encode())
        stream = resp.get("response")
        data = json.loads(stream.read() if hasattr(stream, "read") else stream)
        if isinstance(data, str):
            data = json.loads(data)
    except Exception as e:  # noqa: BLE001 - the page says it failed; the log says why
        print(f"[export] {payload.get('action')}: {type(e).__name__}: {str(e)[:300]}")
        return {"ok": False, "status": 0, "error": f"the runtime could not be reached ({type(e).__name__})",
                "traceId": ""}
    if not isinstance(data, dict):
        return {"ok": False, "status": 0, "error": "the runtime gave no answer", "traceId": ""}
    out = {"ok": data.get("ok") is True, "status": int(data.get("status") or 0),
           "error": str(data.get("error") or "")[:ERROR_MAX], "traceId": str(data.get("traceId") or "")}
    if "spans" in data:
        out["spans"] = int(data.get("spans") or 0)
    return out


def route(route_key: str, params: dict, body: dict, *, raw: dict, table, admin: bool, caller: str,
          who: str, run_owner, invoke, audit) -> tuple[int, dict]:
    """The build app's export routes. `run_owner(sid)` is the run's owner ("" when it has none,
    None when there is no such run); `invoke(payload)` calls this deployment's runtime;
    `audit(action, sid, **detail)` writes the activity log."""
    setup = setup_of(raw)
    if route_key == "GET /api/export":
        if setup is None:
            return 200, {"on": False}
        state = paused(table)
        out = {"on": True, "destination": setup["destination"], "label": label(setup),
               "traceLink": trace_link(setup), "send": {**SEND_DEFAULTS, **(setup.get("send") or {})},
               "paused": state["paused"], "canManage": admin}
        if admin:
            out["pausedBy"], out["pausedAt"] = state["by"], state["at"]
            out["recent"] = recent(table)
        return 200, out
    if setup is None:
        return 404, {"error": OFF}
    if route_key == "POST /api/sessions/{id}/export":
        sid = str(params.get("id") or "")
        if not SESSION_RE.match(sid):
            return 400, {"error": "invalid session id"}
        owner = run_owner(sid)
        if owner is None or (not admin and (not owner or owner != caller)):
            return 404, {"error": "unknown session"}
        got = invoke({"action": "export_run", "session_id": sid})
        audit("export.resent", sid, destination=setup["destination"], ok=got["ok"])
        return 200, got
    if not admin:
        return 403, {"error": "only an admin may do this"}
    if route_key == "POST /api/export/test":
        got = invoke({"action": "export_test"})
        audit("export.tested", "", destination=setup["destination"], ok=got["ok"])
        return 200, got
    if route_key == "PUT /api/export/pause":
        if not isinstance(body, dict) or not isinstance(body.get("paused"), bool):
            return 400, {"error": 'send {"paused": true} or {"paused": false}'}
        item = {"session_id": PAUSE_KEY, "paused": body["paused"], "by": who, "at": _now_iso()}
        table.put_item(Item=item)
        audit("export.paused" if body["paused"] else "export.resumed", "", destination=setup["destination"])
        return 200, {"paused": item["paused"], "by": item["by"], "at": item["at"]}
    return 404, {"error": f"no route for {route_key}"}
