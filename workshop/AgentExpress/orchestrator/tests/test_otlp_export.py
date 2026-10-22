"""The optional second trace destination (app/features/observability/export.py).

Off unless the deploy setting is given; on, it adds exactly one processor beside ADOT's and
never touches the spans CloudWatch receives; prompt and answer content stays in the account
unless a second setting allows it; the auth headers come from Secrets Manager and appear in
no log; an endpoint that fails or hangs never reaches a run.

These run against the real opentelemetry-sdk TracerProvider and BatchSpanProcessor with a fake
exporter in place of OTLPSpanExporter (the test environment's OTLP exporter package does not
import against its SDK; the image pins matching versions). Live delivery to a backend is not
covered here.
"""
from __future__ import annotations

import logging
import re
import threading
import time
import types
from pathlib import Path

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExportResult
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import Status, StatusCode

ORCH = Path(__file__).resolve().parent.parent
ARN = "arn:aws:secretsmanager:us-east-1:123456789012:secret:otlp-headers-AbC123"
ENDPOINT = "https://cloud.langfuse.com/api/public/otel/v1/traces"
# A fake header value, joined at run time so secret scanners do not read it as a real one.
SECRET_VALUE = "Basic " + "cGstbGYtMTIzOnNrLWxmLXNlY3JldA=="


def _mod():
    # Read from the module the code under test uses (other tests purge app.* from sys.modules).
    import app.features.observability.export as export
    return export


class FakeSecrets:
    def __init__(self, value=f'{{"Authorization": "{SECRET_VALUE}", "x-langfuse-ingestion-version": "4"}}'):
        self.value, self.calls = value, []

    def get_secret_value(self, SecretId):  # boto3's argument name
        self.calls.append(SecretId)
        return {"SecretString": self.value}


class FakeExporter:
    """Stands in for OTLPSpanExporter: records what it would have sent."""

    def __init__(self, fail=None, block=None):
        self.batches, self.fail, self.block = [], fail, block

    def export(self, spans):
        if self.block is not None:
            self.block.wait(5)
        if self.fail == "raise":
            raise ConnectionError("endpoint down")
        if self.fail == "result":
            return SpanExportResult.FAILURE
        self.batches.append(list(spans))
        return SpanExportResult.SUCCESS

    def shutdown(self):
        pass

    def force_flush(self, timeout_millis=30000):
        return True

    @property
    def spans(self):
        return [s for b in self.batches for s in b]


@pytest.fixture
def otlp(monkeypatch):
    """A fresh module state, a public DNS answer, and a provider with a CloudWatch stand-in."""
    export = _mod()
    monkeypatch.setattr(export, "_installed", False)
    monkeypatch.setattr(export.socket, "getaddrinfo",
                        lambda host, port, **kw: [(2, 1, 6, "", ("104.16.0.1", port))])
    for k in (export.ENDPOINT_ENV, export.HEADERS_SECRET_ENV, export.INCLUDE_CONTENT_ENV):
        monkeypatch.delenv(k, raising=False)
    provider = TracerProvider()
    cloudwatch = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(cloudwatch))
    built = {}

    def factory(endpoint, headers):
        built["endpoint"], built["headers"] = endpoint, headers
        built["exporter"] = built.get("exporter") or FakeExporter()
        return built["exporter"]

    def turn_on(include=None, endpoint=ENDPOINT, arn=ARN):
        monkeypatch.setenv(export.ENDPOINT_ENV, endpoint)
        monkeypatch.setenv(export.HEADERS_SECRET_ENV, arn)
        if include is not None:
            monkeypatch.setenv(export.INCLUDE_CONTENT_ENV, include)

    o = types.SimpleNamespace(export=export, provider=provider, cloudwatch=cloudwatch, built=built,
                              factory=factory, turn_on=turn_on)
    o.secrets = FakeSecrets()
    o.install = lambda **kw: export.install(client=kw.get("client", o.secrets),
                                            exporter_factory=kw.get("factory", factory), provider=provider)
    yield o
    provider.shutdown()


def processors(provider):
    return list(provider._active_span_processor._span_processors)


def emit_agent_span(provider):
    """A span shaped like the app's agent span (nodes.py) with an exception recorded."""
    # The patient-style sample is synthetic: no real person. Stripping content before export is
    # one control; a workload with real PHI also needs the HIPAA controls the customer owns under
    # the shared responsibility model (https://aws.amazon.com/compliance/hipaa-compliance/).
    tracer = provider.get_tracer("amazon.opentelemetry.distro.instrumentation.langchain")
    with tracer.start_as_current_span("agent.intake") as sp:
        sp.set_attribute("gen_ai.agent.id", "intake")
        sp.set_attribute("gen_ai.usage.input_tokens", 12)
        sp.set_attribute("session.id", "s" * 33)
        sp.set_attribute("gen_ai.task.input", "PATIENT NAME: Jane Roe")
        sp.set_attribute("gen_ai.task.output", "Approved for Jane Roe")
        sp.set_attribute("gen_ai.prompt.0.content", "system prompt text")
        sp.set_attribute("gen_ai.prompt.0.role", "system")
        sp.set_attribute("gen_ai.tool.call.arguments", '{"member": "Jane Roe"}')
        sp.add_event("gen_ai.user.message", {"content": "Jane Roe's claim"})
        sp.record_exception(ValueError("could not parse 'Jane Roe'"))
        sp.set_status(Status(StatusCode.ERROR, "ValueError: could not parse 'Jane Roe'"))


def flush(provider):
    provider.force_flush(5000)


# --- off --------------------------------------------------------------------------------

@pytest.mark.parametrize("endpoint", [None, "", "  "])
def test_off_by_default_builds_nothing_reads_no_secret_and_says_nothing(otlp, monkeypatch, capsys, endpoint):
    if endpoint is not None:
        monkeypatch.setenv(otlp.export.ENDPOINT_ENV, endpoint)
    before = processors(otlp.provider)
    assert otlp.install() is False
    assert otlp.secrets.calls == []
    assert otlp.built == {}
    assert processors(otlp.provider) == before
    assert capsys.readouterr().out == ""
    # Off stays off: a later call (e.g. after a test sets the env) is not blocked by this one.
    assert otlp.export._installed is False


def test_off_never_imports_boto3_or_the_exporter(otlp, monkeypatch):
    import builtins
    real = builtins.__import__
    seen = []
    monkeypatch.setattr(builtins, "__import__",
                        lambda name, *a, **k: (seen.append(name), real(name, *a, **k))[1])
    assert otlp.export.install() is False
    assert not [n for n in seen if n == "boto3" or n.startswith(("opentelemetry.exporter", "requests"))]


# --- on ---------------------------------------------------------------------------------

def test_on_adds_exactly_one_processor_after_the_existing_ones(otlp):
    before = processors(otlp.provider)
    otlp.turn_on()
    assert otlp.install() is True
    after = processors(otlp.provider)
    assert len(after) == len(before) + 1
    assert after[: len(before)] == before
    assert type(after[-1]).__name__ == "ExportProcessor"


def test_a_second_install_adds_nothing(otlp):
    otlp.turn_on()
    assert otlp.install() is True
    n = len(processors(otlp.provider))
    assert otlp.install() is False
    assert len(processors(otlp.provider)) == n
    assert otlp.secrets.calls == [ARN]


def test_headers_come_from_the_secret_and_go_only_to_the_exporter(otlp, capsys):
    otlp.turn_on()
    assert otlp.install() is True
    assert otlp.secrets.calls == [ARN]
    assert otlp.built["endpoint"] == ENDPOINT
    assert otlp.built["headers"] == {"Authorization": SECRET_VALUE, "x-langfuse-ingestion-version": "4"}
    out = capsys.readouterr()
    assert SECRET_VALUE not in out.out + out.err
    assert "cloud.langfuse.com" in out.out and "without prompt or answer content" in out.out
    import os
    assert SECRET_VALUE not in "".join(os.environ.values())


def test_the_cloudwatch_path_still_gets_every_span_unchanged(otlp):
    otlp.turn_on()
    otlp.install()
    emit_agent_span(otlp.provider)
    flush(otlp.provider)
    [cw] = otlp.cloudwatch.get_finished_spans()
    assert cw.attributes["gen_ai.task.input"] == "PATIENT NAME: Jane Roe"
    assert cw.attributes["gen_ai.prompt.0.content"] == "system prompt text"
    assert [e.name for e in cw.events] == ["gen_ai.user.message", "exception"]
    assert cw.status.description == "ValueError: could not parse 'Jane Roe'"
    assert cw.name == "agent.intake"


def test_content_is_stripped_by_default(otlp):
    otlp.turn_on()
    otlp.install()
    emit_agent_span(otlp.provider)
    flush(otlp.provider)
    [sent] = otlp.built["exporter"].spans
    assert sent.name == "agent.intake"
    assert sent.context.trace_id == otlp.cloudwatch.get_finished_spans()[0].context.trace_id
    assert dict(sent.attributes) == {"gen_ai.agent.id": "intake", "gen_ai.usage.input_tokens": 12,
                                     "session.id": "s" * 33}
    assert [e.name for e in sent.events] == ["exception"]
    assert dict(sent.events[0].attributes) == {"exception.type": "ValueError"}
    assert sent.status.status_code == StatusCode.ERROR and not sent.status.description
    assert sent.instrumentation_scope.name == "amazon.opentelemetry.distro.instrumentation.langchain"
    assert "Jane Roe" not in repr(sent.to_json())


@pytest.mark.parametrize("value", ["", "false", "yes", "1"])
def test_anything_but_true_strips(otlp, value):
    otlp.turn_on(include=value)
    otlp.install()
    emit_agent_span(otlp.provider)
    flush(otlp.provider)
    assert "Jane Roe" not in otlp.built["exporter"].spans[0].to_json()


def test_content_is_kept_only_with_the_flag(otlp, capsys):
    otlp.turn_on(include="true")
    otlp.install()
    assert "WITH prompt and answer content" in capsys.readouterr().out
    emit_agent_span(otlp.provider)
    flush(otlp.provider)
    [sent] = otlp.built["exporter"].spans
    assert sent.attributes["gen_ai.task.input"] == "PATIENT NAME: Jane Roe"
    assert sent.attributes["gen_ai.prompt.0.content"] == "system prompt text"
    assert [e.name for e in sent.events] == ["gen_ai.user.message", "exception"]
    assert sent.status.description == "ValueError: could not parse 'Jane Roe'"


def test_the_export_copy_survives_adot_stripping_the_original_in_place(otlp):
    """ADOT's CloudWatch exporter removes content from the span object it holds. Our copy is
    taken when the span ends, so with the flag on what we send is still whole."""
    export = otlp.export
    otlp.turn_on(include="true")
    otlp.install()
    emit_agent_span(otlp.provider)
    [original] = otlp.cloudwatch.get_finished_spans()
    original._attributes = {k: v for k, v in original.attributes.items() if not export.is_content(k)}
    flush(otlp.provider)
    assert otlp.built["exporter"].spans[0].attributes["gen_ai.task.input"] == "PATIENT NAME: Jane Roe"


@pytest.mark.parametrize("key", [*sorted(_mod().CONTENT_KEYS), "gen_ai.completion.3.content",
                                 "llm.input_messages.0.message.content", "llm.output_messages.1.message.content"])
def test_every_content_key_is_stripped(otlp, key):
    otlp.turn_on()
    otlp.install()
    with otlp.provider.get_tracer("t").start_as_current_span("chat") as sp:
        sp.set_attribute(key, "SECRET-TEXT")
        sp.set_attribute("gen_ai.request.model", "m")
    flush(otlp.provider)
    assert dict(otlp.built["exporter"].spans[0].attributes) == {"gen_ai.request.model": "m"}


def test_a_recorded_but_unsampled_span_is_not_exported(otlp):
    """ADOT wraps the sampler in AlwaysRecordSampler when Application Signals is on: spans are
    recorded (so processors see them) but not sampled, and must not be exported."""
    from opentelemetry.sdk.trace.sampling import Decision, Sampler, SamplingResult

    class RecordOnly(Sampler):
        def should_sample(self, *a, **k):
            return SamplingResult(Decision.RECORD_ONLY)

        def get_description(self):
            return "RecordOnly"
    otlp.turn_on()
    otlp.install()
    otlp.provider.sampler = RecordOnly()
    with otlp.provider.get_tracer("t").start_as_current_span("x") as sp:
        assert sp.is_recording()
        sp.set_attribute("gen_ai.task.input", "x")
    flush(otlp.provider)
    assert otlp.built["exporter"].spans == []


# --- failures never reach a run ----------------------------------------------------------

@pytest.mark.parametrize("fail", ["raise", "result"])
def test_an_export_failure_does_not_raise_and_warns_once_a_minute(otlp, capsys, monkeypatch, fail):
    export = otlp.export
    clock = [1000.0]
    otlp.built["exporter"] = FakeExporter(fail=fail)
    otlp.turn_on()
    otlp.install()
    guarded = processors(otlp.provider)[-1]._batch._batch_processor._exporter
    monkeypatch.setattr(guarded, "_clock", lambda: clock[0])
    capsys.readouterr()
    for _ in range(3):
        emit_agent_span(otlp.provider)   # must not raise
        flush(otlp.provider)
    lines = [ln for ln in capsys.readouterr().out.splitlines() if "not delivered" in ln]
    assert len(lines) == 1 and "1 span(s)" in lines[0] and "cloud.langfuse.com" in lines[0]
    clock[0] += export.WARN_EVERY_S + 1
    emit_agent_span(otlp.provider)
    flush(otlp.provider)
    lines = [ln for ln in capsys.readouterr().out.splitlines() if "not delivered" in ln]
    assert len(lines) == 1 and "3 span(s)" in lines[0]
    assert len(otlp.cloudwatch.get_finished_spans()) == 4


def test_the_sdk_exporters_own_log_lines_are_held_back_only_inside_our_export(otlp, caplog):
    export = otlp.export
    sdk_log = logging.getLogger(export._SDK_EXPORTER_LOGGER)

    class Noisy(FakeExporter):
        def export(self, spans):
            sdk_log.error("Failed to export span batch code: 401, reason: Unauthorized")
            return SpanExportResult.FAILURE
    otlp.built["exporter"] = Noisy()
    otlp.turn_on()
    otlp.install()
    with caplog.at_level(logging.ERROR, logger=export._SDK_EXPORTER_LOGGER):
        emit_agent_span(otlp.provider)
        flush(otlp.provider)
        assert not [r for r in caplog.records if "401" in r.getMessage()]
        # ADOT's CloudWatch exporter logs through the same logger, from its own thread.
        sdk_log.error("cloudwatch export failed")
    assert [r.getMessage() for r in caplog.records] == ["cloudwatch export failed"]


def test_a_hung_endpoint_holds_a_flush_for_at_most_the_flush_wait(otlp, monkeypatch):
    export = otlp.export
    monkeypatch.setattr(export, "FLUSH_WAIT_S", 0.2)
    gate = threading.Event()
    otlp.built["exporter"] = FakeExporter(block=gate)
    otlp.turn_on()
    otlp.install()
    try:
        emit_agent_span(otlp.provider)
        start = time.monotonic()
        otlp.provider.force_flush(8000)
        assert time.monotonic() - start < 2
        assert len(otlp.cloudwatch.get_finished_spans()) == 1
    finally:
        gate.set()


def test_a_span_that_cannot_be_copied_is_dropped_quietly(otlp, monkeypatch):
    otlp.turn_on()
    otlp.install()

    def boom(*a, **k):
        raise RuntimeError("copy failed")
    monkeypatch.setattr(otlp.export, "snapshot", boom)
    emit_agent_span(otlp.provider)  # must not raise
    flush(otlp.provider)
    assert len(otlp.cloudwatch.get_finished_spans()) == 1


# --- bad settings turn it off, once, saying why ------------------------------------------

@pytest.mark.parametrize("endpoint", [
    "http://cloud.langfuse.com/api/public/otel/v1/traces",
    "file:///etc/passwd",
    "https://user:pw@collector.example.com/v1/traces",
    "https://collector.example.com/v1/traces?token=x",
    "https://collector.example.com/v1/traces#x",
    "https://collector example.com/v1/traces",
    "cloud.langfuse.com/api/public/otel/v1/traces",
])
def test_only_an_https_endpoint_is_used(otlp, capsys, endpoint):
    otlp.turn_on(endpoint=endpoint)
    assert otlp.install() is False
    assert otlp.secrets.calls == [] and otlp.built == {}
    assert "off:" in capsys.readouterr().out


@pytest.mark.parametrize("address", ["127.0.0.1", "169.254.169.254", "10.0.0.5", "::1"])
def test_an_endpoint_on_a_private_address_is_refused(otlp, monkeypatch, capsys, address):
    monkeypatch.setattr(otlp.export.socket, "getaddrinfo",
                        lambda host, port, **kw: [(2, 1, 6, "", (address, port))])
    otlp.turn_on()
    assert otlp.install() is False
    assert otlp.secrets.calls == []
    assert "not a public address" in capsys.readouterr().out


@pytest.mark.parametrize("arn", ["", "arn:aws:secretsmanager:us-east-1:123456789012:secret:otlp-headers",
                                 "arn:aws:s3:::bucket", "*"])
def test_the_secret_must_be_a_full_secret_arn(otlp, arn):
    otlp.turn_on(arn=arn)
    assert otlp.install() is False
    assert otlp.secrets.calls == []


@pytest.mark.parametrize("value", [
    "not json", "[]", "{}", '{"Authorization": 5}', '{"Bad Name": "x"}',
    f'{{"Authorization": "Basic {SECRET_VALUE}\\r\\nX-Injected: 1"}}',
])
def test_an_unusable_secret_turns_export_off_without_printing_it(otlp, capsys, value):
    otlp.secrets = FakeSecrets(value)
    otlp.turn_on()
    before = processors(otlp.provider)
    assert otlp.install() is False
    assert processors(otlp.provider) == before and otlp.built == {}
    out = capsys.readouterr().out
    assert "off:" in out and SECRET_VALUE not in out and "not json" not in out


def test_an_unreadable_secret_turns_export_off_naming_only_the_error_type(otlp, capsys):
    class Denied:
        def get_secret_value(self, SecretId):
            raise PermissionError(f"AccessDenied on {SecretId} value={SECRET_VALUE}")
    otlp.turn_on()
    assert otlp.install(client=Denied()) is False
    out = capsys.readouterr().out
    assert "off: PermissionError" in out and SECRET_VALUE not in out


def test_a_bad_setting_is_reported_once_not_on_every_run(otlp, capsys):
    otlp.turn_on(endpoint="http://x.example.com/v1/traces")
    otlp.install()
    otlp.install()
    assert capsys.readouterr().out.count("off:") == 1


def test_without_an_sdk_provider_it_is_off(otlp, capsys):
    from opentelemetry.trace import ProxyTracerProvider
    otlp.turn_on()
    assert otlp.export.install(client=otlp.secrets, exporter_factory=otlp.factory,
                               provider=ProxyTracerProvider()) is False
    assert otlp.secrets.calls == []
    assert "no SDK tracer provider" in capsys.readouterr().out


def test_the_secret_is_read_in_its_own_region(otlp, monkeypatch):
    import sys
    seen = {}

    def client(service, region_name=None):
        seen["service"], seen["region"] = service, region_name
        return FakeSecrets()
    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(client=client))
    otlp.turn_on(arn=ARN.replace("us-east-1", "eu-west-1"))
    assert otlp.export.install(exporter_factory=otlp.factory, provider=otlp.provider) is True
    assert seen == {"service": "secretsmanager", "region": "eu-west-1"}


# --- wiring and the twins ---------------------------------------------------------------

def test_both_roles_install_it_before_the_app_is_imported():
    entry = (ORCH / "app" / "entry.py").read_text()
    assert entry.index("_otlp_export.install()") < entry.index("from app.subagent_runtime import app")
    assert 'CMD ["opentelemetry-instrument"' in (ORCH / "Dockerfile").read_text()


def test_the_exporter_package_is_already_pinned():
    lock = (ORCH / "requirements.lock").read_text()
    assert re.search(r"^opentelemetry-exporter-otlp-proto-http==", lock, re.MULTILINE)
    assert re.search(r"^opentelemetry-sdk==", lock, re.MULTILINE)


def _tf_regex(text: str, var: str) -> str:
    block = text.split(f'variable "{var}"')[1].split("\nvariable ")[0]
    found = re.findall(r'regex\("((?:[^"\\]|\\.)*)"', block)
    assert found, f"no regex in variable {var}"
    return found[0].replace("\\\\", "\\")


def _ts_regex(text: str, name: str) -> str:
    m = re.search(rf"export const {name} = /(.*)/;", text)
    assert m, name
    return m.group(1).replace("\\/", "/")


def test_the_three_validators_use_the_same_patterns():
    export = _mod()
    tf = (ORCH / "terraform" / "variables.tf").read_text()
    ts = (ORCH / "cdk" / "lib" / "otlp-export.ts").read_text()
    assert _tf_regex(tf, "otlp_endpoint") == export.ENDPOINT_PATTERN == _ts_regex(ts, "OTLP_ENDPOINT_PATTERN")
    assert (_tf_regex(tf, "otlp_headers_secret_arn") == export.SECRET_ARN_PATTERN
            == _ts_regex(ts, "OTLP_SECRET_ARN_PATTERN"))


def test_both_iac_paths_set_the_same_environment_and_only_when_on():
    tf = (ORCH / "terraform" / "otlp_export.tf").read_text()
    ts = (ORCH / "cdk" / "lib" / "otlp-export.ts").read_text()
    export = _mod()
    for name in (export.ENDPOINT_ENV, export.HEADERS_SECRET_ENV, export.INCLUDE_CONTENT_ENV):
        assert re.search(rf"\b{name}\b", tf) and re.search(rf"\b{name}\b", ts), name
    assert re.search(r"otlp_export_env\s*=\s*local\.otlp_export_enabled \? \{", tf)
    main = (ORCH / "terraform" / "main.tf").read_text()
    sub = (ORCH / "terraform" / "subagent_runtimes.tf").read_text()
    # (litellm.tf's settings are merged in after it, on the same line.)
    assert all(len(re.findall(r"\}, local\.otlp_export_env[,)]", t)) == 1 for t in (main, sub))
    stack = (ORCH / "cdk" / "lib" / "orchestrator-stack.ts").read_text()
    assert "...otlp.env," in stack


def test_terraform_grants_the_secret_only_when_on_and_only_on_that_arn():
    tf = (ORCH / "terraform" / "otlp_export.tf").read_text()
    runtime = tf.split('resource "aws_iam_role_policy" "runtime_otlp_export"')[1].split("\n}\n")[0]
    sub = tf.split('resource "aws_iam_role_policy" "subagent_otlp_export"')[1].split("\n}\n")[0]
    assert "count  = local.workflow_plane && local.otlp_export_enabled ? 1 : 0" in runtime
    assert "for_each = { for id, a in local.dedicated_agents : id => id if local.otlp_export_enabled }" in sub
    policy = tf.split("otlp_export_policy = ")[1].split("\n  })\n")[0]
    assert 'Action   = ["secretsmanager:GetSecretValue"]' in policy
    assert "Resource = var.otlp_headers_secret_arn" in policy
    assert '"kms:ViaService"' in policy and '"kms:EncryptionContext:SecretARN" = var.otlp_headers_secret_arn' in policy
    assert '"*"' not in policy
    # The key grant is scoped to the secret's own account and region, not to every key.
    assert ('"arn:aws:kms:${local.otlp_secret_arn_parts[3]}:${local.otlp_secret_arn_parts[4]}:key/*"' in policy)
    # The runtimes read the secret once, at start: they wait for the grant like any other.
    main = (ORCH / "terraform" / "main.tf").read_text()
    subagents = (ORCH / "terraform" / "subagent_runtimes.tf").read_text()
    wait = main.split('resource "time_sleep" "iam_propagation"')[1].split("\n}\n")[0]
    sub_wait = subagents.split('resource "time_sleep" "subagent_iam_propagation"')[1].split("\n}\n")[0]
    assert re.search(r"depends_on\s*=\s*\[[^\]]*\baws_iam_role_policy\.runtime_otlp_export\b", wait)
    assert re.search(r"depends_on\s*=\s*\[[^\]]*\baws_iam_role_policy\.subagent_otlp_export\b", sub_wait)


def test_terraform_variables_are_off_by_default_and_not_in_the_example():
    tf = (ORCH / "terraform" / "variables.tf").read_text()
    for var, default in (("otlp_endpoint", '""'), ("otlp_headers_secret_arn", '""'),
                         ("otlp_include_content", "false")):
        block = tf.split(f'variable "{var}"')[1].split("\nvariable ")[0]
        assert re.search(rf"default\s*=\s*{re.escape(default)}\n", block), var
    example = (ORCH / "terraform" / "terraform.tfvars.example").read_text()
    assert not re.search(r"^otlp_", example, re.MULTILINE)
