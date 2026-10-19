"""Checks for `orchestrator.export` (sending each run to Langfuse, Datadog, Grafana Cloud,
Honeycomb or any OTLP backend: app/features/observability/runexport.py), for
validate_build.py; and the check on an `exportHeaders` build secret, for builds.py.

The generic key checks (app/keys.json) already say `export` is an object; these are the rest.
Mirrors web/src/builder/exportCheck.ts word for word (both run
tests/fixtures/validation_cases.json). The presets mirror runexport.py's (the BFF ships as its
own zip and cannot import app.*); tests/test_run_export.py holds the two equal.
"""
from __future__ import annotations

import json
import re

DESTINATIONS = ("langfuse", "datadog", "grafana", "honeycomb", "otlp")
LANGFUSE_HOSTS = {"us": "https://us.cloud.langfuse.com", "eu": "https://cloud.langfuse.com"}
LANGFUSE_REGIONS = ("us", "eu", "self-hosted")
DATADOG_SITES = ("datadoghq.com", "us3.datadoghq.com", "us5.datadoghq.com", "datadoghq.eu",
                 "ap1.datadoghq.com", "ap2.datadoghq.com", "uk1.datadoghq.com")
HONEYCOMB_HOSTS = {"us": "https://api.honeycomb.io", "eu": "https://api.eu1.honeycomb.io"}
HONEYCOMB_REGIONS = ("us", "eu")
DEFAULT_REGION = {"langfuse": "eu", "datadog": "datadoghq.com", "honeycomb": "us"}
FIXED_HEADERS = {"langfuse": {"x-langfuse-ingestion-version": "4"},
                 "datadog": {"dd-otlp-source": "llmobs"}}
METRICS_DESTINATIONS = ("datadog", "honeycomb", "otlp")
REGIONS = {"langfuse": LANGFUSE_REGIONS, "datadog": DATADOG_SITES, "honeycomb": HONEYCOMB_REGIONS}

SEND_KEYS = ("content", "evals", "metrics")
#: https only, no user info, query or fragment: the same pattern as export.ENDPOINT_PATTERN.
ENDPOINT_RE = re.compile(r"^https://[A-Za-z0-9.-]+(:[0-9]{1,5})?(/[^\s?#]*)?$")
SECRET_ARN_RE = re.compile(
    r"^arn:aws:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@!-]+-[A-Za-z0-9]{6}$")
#: A build secret's name (bff/builds.py SECRET_KINDS exportHeaders).
SECRET_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
MAX_PROJECT = 200
MAX_HEADERS = 20
MAX_HEADER_VALUE = 8192
HEADER_NAME_RE = re.compile(r"^[A-Za-z0-9!#$%&'*+.^_`|~-]+$")


def _base(endpoint: str) -> str:
    url = (endpoint or "").strip().rstrip("/")
    for suffix in ("/v1/traces", "/v1/metrics"):
        url = url.removesuffix(suffix)
    return url


def _langfuse_base(endpoint: str) -> str:
    url = _base(endpoint)
    return url.removesuffix("/api/public/otel")


def urls(destination: str, region: str = "", endpoint: str = "") -> dict:
    """{"traces", "metrics", "scores"} for a destination; "" where it takes none. For the
    console's connection test. KeyError for a region the destination does not have."""
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


def headers_problem(value) -> str:
    """Why an exportHeaders secret value is unusable, or "". Never quotes a value."""
    try:
        headers = json.loads(value) if isinstance(value, str) else None
    except ValueError:
        headers = None
    if not isinstance(headers, dict) or not headers:
        return "must be a JSON object of HTTP headers, with at least one"
    if len(headers) > MAX_HEADERS:
        return f"has more than {MAX_HEADERS} headers"
    for i, (name, v) in enumerate(headers.items(), 1):
        if not isinstance(name, str) or not HEADER_NAME_RE.match(name):
            return f"header {i} has an invalid name"
        if not isinstance(v, str) or len(v) > MAX_HEADER_VALUE or any(c in v for c in "\r\n\0"):
            return f"header {name!r} has an invalid value"
    return ""


def _needs_endpoint(destination: str, region) -> bool:
    return destination in ("otlp", "grafana") or (destination == "langfuse" and region == "self-hosted")


def check(orch, add) -> None:
    """The keys' names and JSON types are checked from keys.json (validate_build._check_entry);
    a value of the wrong type is left to that check, so it is reported once."""
    ex = orch.get("export") if isinstance(orch, dict) else None
    if not isinstance(ex, dict):
        return
    where = {"kind": "block", "name": "orchestrator"}
    p = "orchestrator.export"
    dest = ex.get("destination")
    if dest is None:
        add("error", where, f"{p}.destination", f"is required: one of {', '.join(DESTINATIONS)}")
        return
    if not isinstance(dest, str):
        return
    if dest not in DESTINATIONS:
        add("error", where, f"{p}.destination", f"must be one of: {', '.join(DESTINATIONS)}")
        return
    region = ex.get("region")
    if isinstance(region, str):
        if dest not in REGIONS:
            add("error", where, f"{p}.region", "applies only to langfuse, datadog and honeycomb; remove it")
        elif region not in REGIONS[dest]:
            add("error", where, f"{p}.region", f"must be one of: {', '.join(REGIONS[dest])}")
    endpoint = ex.get("endpoint")
    if endpoint is None:
        if _needs_endpoint(dest, region):
            add("error", where, f"{p}.endpoint", f"is required for {dest}: the https:// address to send to")
    elif not isinstance(endpoint, str):
        pass
    elif not _needs_endpoint(dest, region):
        add("error", where, f"{p}.endpoint",
            "is set from destination and region; remove it, or use destination otlp to send anywhere")
    elif not ENDPOINT_RE.match(endpoint):
        add("error", where, f"{p}.endpoint", "must be an https:// URL with no user info, query or fragment")
    secret = ex.get("secret")
    if secret is None:
        add("error", where, f"{p}.secret",
            "is required: the name of the build secret holding the headers, or a Secrets Manager secret ARN")
    elif not isinstance(secret, str):
        pass
    elif secret.startswith("arn:"):
        if not SECRET_ARN_RE.match(secret):
            add("error", where, f"{p}.secret",
                "must be the full ARN of a Secrets Manager secret, including the six-character suffix")
    elif not SECRET_NAME_RE.match(secret):
        add("error", where, f"{p}.secret", "must be a secret name: letters, digits, . _ - (up to 64)")
    project = ex.get("project")
    if isinstance(project, str) and (len(project) > MAX_PROJECT or re.search(r"[\x00-\x1f]", project)):
        add("error", where, f"{p}.project", f"must be text, up to {MAX_PROJECT} characters")
    send = ex.get("send")
    if not isinstance(send, dict):
        return
    for k, v in send.items():
        if k not in SEND_KEYS:
            add("error", where, f"{p}.send.{k}", f"unknown key; send takes: {', '.join(SEND_KEYS)}")
        elif not isinstance(v, bool):
            add("error", where, f"{p}.send.{k}", "must be true or false")
    if send.get("content") is True:
        add("warning", where, f"{p}.send.content",
            f"sends prompts, answers and tool input and output to {dest}, outside your AWS account; "
            "check that your data may go there")
    if send.get("metrics") is True and dest not in METRICS_DESTINATIONS:
        add("warning", where, f"{p}.send.metrics",
            f"has no effect for {dest}: metrics go only to {', '.join(METRICS_DESTINATIONS)}")
