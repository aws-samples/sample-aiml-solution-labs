"""The orchestrator must not let the SDK run a dedicated agent twice.

InvokeAgentRuntime is synchronous, slow and NOT idempotent. boto3's default client
retries (`retries={'mode': 'legacy'}`, up to 5 attempts), and a retry does not
cancel the attempt it replaced — the remote container is already working and cannot
tell that the caller stopped listening. So one transient blip means the agent runs
the whole thing again, a second model call is billed, and the orchestrator returns
whichever attempt answered last. Nothing logs it: the node logs "Invoking dedicated
AgentCore Runtime" once, before any retry exists.

This happened on a live run. The web_search runtime logged two invocations with
different requestIds for ONE node execution:

    13:55:29  Invocation completed successfully (17.792s)  req 95297bd8...
    13:55:41  Invocation completed successfully (16.669s)  req 26ad54ce...

Two identical calls at 6579 input tokens, one answer discarded, $0.0158 wasted on
a $0.2448 run — and the telemetry table shows the same shape in 2 of 22 recorded
sessions, so it is not a one-off.

These tests pin the client's retry behaviour to config. The failure they guard
against is invisible in normal operation: everything succeeds, the deliverable is
fine, and the bill is quietly wrong.
"""

from __future__ import annotations

import json

import pytest
from conftest import ORCH_ROOT, wf, workflow


def _client(defn: dict):
    """Build the dedicated-runtime client under `defn`, bypassing the module cache."""
    with workflow(defn) as imp:
        mod = imp("app.common.agentcore_agent")
        mod._client = None  # the module memoises; force a fresh build per test
        return mod._agentcore()


def test_retries_are_off_by_default():
    """No `runtimeInvoke` block at all must still mean one attempt.

    The engine default carries this, not workflow.json, so a customer who deletes
    the block or starts from a hand-written workflow is protected too.
    """
    c = _client(wf([{"agent": "a"}]))
    assert c.meta.config.retries["total_max_attempts"] == 1, (
        "the bedrock-agentcore client is retrying InvokeAgentRuntime; a retry re-runs "
        "the whole remote agent and bills a second model call whose answer is dropped")


def test_boto3_default_would_have_been_wrong():
    """The thing being defended against, stated as a fact rather than a comment.

    If a future boto3 ships with retries off, this test fails and the guard above
    becomes redundant — which is worth being told about explicitly.
    """
    import boto3

    plain = boto3.client("bedrock-agentcore", region_name="us-east-1")
    assert plain.meta.config.retries.get("mode") == "legacy", (
        "boto3's default retry mode changed; re-check whether RUNTIME_INVOKE is still "
        "needed")
    assert plain.meta.config.retries.get("total_max_attempts") != 1


def test_both_numbers_come_from_config():
    """A key nothing reads is worse than no key. Prove each one reaches the client."""
    defn = wf([{"agent": "a"}])
    defn["orchestrator"] = {"runtimeInvoke": {"maxAttempts": 3,
                                             "readTimeoutSeconds": 45}}
    cfg = _client(defn).meta.config
    assert cfg.retries["total_max_attempts"] == 3
    assert cfg.read_timeout == 45


def test_read_timeout_exceeds_the_default_so_a_slow_agent_is_not_cut_off():
    """With retries off, a read timeout is fatal to the run, so it must be generous.

    The measured research agents take 15-18s; boto3's own default is 60s. Anything
    at or below that would trade a duplicate for a truncated run, which is not the
    trade being made here.
    """
    with workflow(wf([{"agent": "a"}])) as imp:
        assert imp("app.common.config").RUNTIME_INVOKE["readTimeoutSeconds"] > 60


def test_shipped_workflow_declares_the_block():
    """The sample should show the setting rather than rely on a hidden default."""
    shipped = json.loads((ORCH_ROOT / "app" / "workflow.json").read_text())
    block = shipped["orchestrator"].get("runtimeInvoke")
    assert block, "orchestrator.runtimeInvoke is missing from the shipped workflow"
    assert block["maxAttempts"] == 1


@pytest.mark.parametrize("bad", [{"maxAttempts": 0}, {"maxAttempts": -1}])
def test_zero_or_negative_attempts_is_rejected(bad):
    """`maxAttempts: 0` is nonsense — it would mean never calling the agent.

    It fails loudly at client construction rather than silently doing nothing, which
    is why the implementation uses botocore's `total_max_attempts` (minimum 1) and
    not `max_attempts` (minimum 0).
    """
    from botocore.exceptions import InvalidMaxRetryAttemptsError

    defn = wf([{"agent": "a"}])
    defn["orchestrator"] = {"runtimeInvoke": bad}
    with pytest.raises(InvalidMaxRetryAttemptsError):
        _client(defn)


def test_max_attempts_is_total_attempts_not_retries():
    """The config key must not be off by one against the name it uses.

    botocore's `max_attempts` counts retries AFTER the first, so writing
    `maxAttempts: 1` with that key would still permit two executions of the agent —
    the exact bug this block exists to prevent. This pins the mapping.
    """
    defn = wf([{"agent": "a"}])
    defn["orchestrator"] = {"runtimeInvoke": {"maxAttempts": 2}}
    assert _client(defn).meta.config.retries["total_max_attempts"] == 2


def test_a_retryable_transport_failure_is_not_retried(monkeypatch):
    """The behaviour, not just the setting.

    Asserting on `Config` proves the number was passed; it does not prove the client
    stops. This drives a real InvokeAgentRuntime through botocore's retry handler with
    the socket failing in the way that caused the duplicate — a connect timeout, which
    botocore classifies as retryable — and counts how many times the request actually
    goes out. Exactly one, or the agent could run twice again.
    """
    from botocore.exceptions import ConnectTimeoutError

    # Signing happens before the send, so the request needs credentials to exist.
    # Dummy ones keep the test hermetic — nothing leaves the process; the socket is
    # replaced below.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.delenv("AWS_PROFILE", raising=False)

    client = _client(wf([{"agent": "a"}]))
    sends: list[object] = []

    def _fail(request, *a, **kw):
        sends.append(request)
        raise ConnectTimeoutError(endpoint_url="https://bedrock-agentcore.invalid")

    client._endpoint.http_session.send = _fail

    with pytest.raises(ConnectTimeoutError):
        client.invoke_agent_runtime(
            agentRuntimeArn="arn:aws:bedrock-agentcore:us-east-1:1:runtime/r",
            runtimeSessionId="s".ljust(33, "0"),
            payload=b"{}")

    assert len(sends) == 1, (
        f"the request went out {len(sends)} times; with a non-idempotent invoke every "
        f"attempt after the first is another full agent run on the customer's bill")
