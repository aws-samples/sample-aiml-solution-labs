"""A guardrail set to ANONYMIZE sensitive information masks it and the run carries on;
anything it BLOCKS still stops the agent. Before this, every intervention was a block, so
a request holding an email address failed where the guardrail was meant to mask it."""
from __future__ import annotations

import asyncio

import pytest

from app.features.guardrails import client


class FakeBedrock:
    def __init__(self, resp):
        self.resp, self.calls = resp, []

    def apply_guardrail(self, **kw):
        self.calls.append(kw)
        return self.resp


def _run(resp, monkeypatch, text="Write to jane@example.com"):
    monkeypatch.setattr(client, "_bedrock", lambda: FakeBedrock(resp))
    return asyncio.run(client.check(text, "INPUT", guardrail_id="g1", version="1"))


ANON = {"action": "GUARDRAIL_INTERVENED", "outputs": [{"text": "Write to {EMAIL}"}],
        "assessments": [{"sensitiveInformationPolicy": {"piiEntities": [
            {"type": "EMAIL", "match": "jane@example.com", "action": "ANONYMIZED", "detected": True}]}}]}


def test_an_anonymized_request_comes_back_masked(monkeypatch):
    assert _run(ANON, monkeypatch) == "Write to {EMAIL}"


def test_a_block_anywhere_still_stops_the_agent(monkeypatch):
    blocked = {**ANON, "outputs": [{"text": "This request was blocked."}], "assessments": [
        *ANON["assessments"],
        {"topicPolicy": {"topics": [{"name": "legal", "type": "DENY", "action": "BLOCKED", "detected": True}]}}]}
    with pytest.raises(client.GuardrailBlocked) as e:
        _run(blocked, monkeypatch)
    assert e.value.message == "This request was blocked."
    # The timeline names what blocked it: the guardrail's own message never does.
    assert e.value.reasons == ["denied topic legal"]


@pytest.mark.parametrize("assessments", [None, [], [{"contentPolicy": {"filters": []}}]])
def test_an_intervention_that_masked_nothing_it_can_name_is_a_block(monkeypatch, assessments):
    """Fail closed: no assessment, or one with no ANONYMIZED entry, is read as a block."""
    with pytest.raises(client.GuardrailBlocked):
        _run({**ANON, "assessments": assessments}, monkeypatch)


def test_a_pass_returns_the_text_unchanged(monkeypatch):
    assert _run({"action": "NONE", "outputs": []}, monkeypatch) == "Write to jane@example.com"


def test_the_agent_gets_the_masked_request_and_feedback(monkeypatch):
    from app.orchestrator import nodes

    class Ctx:
        topic, feedback, agent_name = "Mail jane@example.com", "Call 555-123-4567", "Planner"

        def __init__(self):
            self.logs: list = []

        async def guardrail(self, text, source):
            return text.replace("jane@example.com", "{EMAIL}").replace("555-123-4567", "{PHONE}")

        async def log(self, msg):
            self.logs.append(msg)

    ctx = Ctx()
    asyncio.run(nodes._mask_input(ctx))
    assert (ctx.topic, ctx.feedback) == ("Mail {EMAIL}", "Call {PHONE}")
    assert ctx.logs == ["Planner: the guardrail masked sensitive information in the request"]
