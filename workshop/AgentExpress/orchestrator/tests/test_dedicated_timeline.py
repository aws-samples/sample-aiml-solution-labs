"""A dedicated agent's timeline lines reach the run's timeline.

A `runtime: "dedicated"` agent runs in its own AgentCore Runtime, which has no status
or events table. Its ctx.log lines (tool calls, "Refused by Cedar policy: …") were
therefore lost: the timeline showed a refusal for an in-process agent but never for a
dedicated one, even when Observability recorded the denial. The container now returns
them, and the orchestrator side replays them with ctx.log.
"""
import asyncio
import io
import json

from app.common import agentcore_agent


class _Ctx:
    def __init__(self):
        self.session_id = "sess123"
        self.topic = "a topic"
        self.state = {"outputs": {}}
        self.feedback = ""
        self.lines: list[str] = []

    async def log(self, msg: str) -> None:
        self.lines.append(msg)


class _Client:
    def __init__(self, body: dict):
        self.body = body

    def invoke_agent_runtime(self, **_kw):
        return {"response": io.BytesIO(json.dumps(self.body).encode())}


def _run(monkeypatch, body: dict):
    monkeypatch.setitem(agentcore_agent._RUNTIME_ARNS, "fact_researcher", "arn:aws:test")
    monkeypatch.setattr(agentcore_agent, "_agentcore", lambda: _Client(body))
    agent = agentcore_agent.AgentCoreRuntimeAgent()
    agent.id = "fact_researcher"
    ctx = _Ctx()
    out = asyncio.run(agent.run(ctx))
    return out, ctx.lines


def test_a_dedicated_agents_refusal_is_replayed_on_the_timeline(monkeypatch):
    refusal = 'Refused by Cedar policy: webSearch___search({"query": "Acme"})'
    out, lines = _run(monkeypatch, {"output": "{}", "logs": ["Tool call 1/6: webSearch___search", refusal]})
    assert out == "{}"
    assert lines[0].startswith("Invoking dedicated AgentCore Runtime")
    assert lines[1:] == ["Tool call 1/6: webSearch___search", refusal]


def test_logs_are_replayed_when_the_dedicated_agent_fails_too(monkeypatch):
    out, lines = _run(monkeypatch, {"error": "ToolDenied: refused", "logs": ["Refused by Cedar policy: x"]})
    assert "dedicated runtime error" in out
    assert lines[-1] == "Refused by Cedar policy: x"


def test_an_older_runtime_without_logs_still_works(monkeypatch):
    out, lines = _run(monkeypatch, {"output": "done"})
    assert out == "done"
    assert len(lines) == 1
