"""Per-agent AgentCore Runtime entrypoint.

A dedicated agent (workflow.json `runtime: "dedicated"`) runs in its OWN
AgentCore Runtime container. That container runs THIS app, with the agent to
run selected by the AGENT_ID environment variable. It runs the agent's real
in-process implementation (build_agent_module) — the "dedicated" placement only
moves where the code executes, not what it does.

Invoke contract (called by the orchestrator's AgentCoreRuntimeAgent):
  {"agent_id","topic","subject_id","outputs":{id:text,...},"feedback",
   "session_id","otel_context"}
Returns: {"agent_id","output"} (or {"error"} on failure).

The propagated `otel_context` makes this runtime's spans join the SAME CloudWatch
trace as the orchestrator, so a run reads as one distributed trace.
"""

import os

from bedrock_agentcore.runtime import BedrockAgentCoreApp

from app.common.bus import bus
from app.common.context import AgentContext
from app.features.observability import otel
from app.orchestrator.registry import build_agent_module

AGENT_ID = os.environ["AGENT_ID"]
_agent = build_agent_module(AGENT_ID)  # this runtime hosts exactly one agent

app = BedrockAgentCoreApp()


@app.entrypoint
async def invoke(payload, context=None):
    session_id = payload.get("session_id", "")
    # Re-attach the orchestrator's trace context (so this runtime's spans join
    # the same trace) and group them under the same session id.
    _ctx_token = otel.attach_carrier(payload.get("otel_context") or {})
    _sess_token = otel.set_session(session_id)
    try:
        state = {
            "topic": payload.get("topic", ""),
            "subject_id": payload.get("subject_id", ""),
            "outputs": payload.get("outputs", {}) or {},
            # AgentContext reads feedback[self.id]; scope it to this agent.
            "feedback": {AGENT_ID: payload.get("feedback", "") or ""},
        }
        config = {"configurable": {"thread_id": session_id or "subagent"}}
        # This container has no status or events table, so the agent's timeline
        # lines (tool calls, "Refused by Cedar policy", …) are collected here and
        # returned; the orchestrator writes them to the run's timeline.
        bus.create(session_id, state["topic"])
        ctx = AgentContext(_agent, state, config)
        with otel.span(f"agent.{AGENT_ID}",
                       scope="amazon.opentelemetry.distro.instrumentation.langchain",
                       **{"gen_ai.agent.id": AGENT_ID,
                          "gen_ai.operation.name": "invoke_agent",
                          "aws.genai.span_kind": "AGENT",
                          "session.id": otel.normalize_session_id(session_id)}) as sp, otel.accumulate_tokens_on(sp):
            # Long-term memory works here exactly as in-process (same
            # config-driven no-ops when disabled for this agent).
            ctx.recalled_memory = await ctx.memory_recall(ctx.topic or AGENT_ID)
            out = await _agent.run(ctx)
            await ctx.memory_store(out)
        return {"agent_id": AGENT_ID, "output": out, "logs": _timeline(session_id)}
    except Exception as e:  # noqa: BLE001
        return {"agent_id": AGENT_ID, "error": f"{type(e).__name__}: {e}",
                "logs": _timeline(session_id)}
    finally:
        bus.delete(session_id)
        # Flush spans before returning — the dedicated runtime's container may be
        # frozen/reclaimed right after the response, stranding buffered spans.
        otel.force_flush()
        otel.detach(_sess_token)
        otel.detach(_ctx_token)


def _timeline(session_id: str) -> list[str]:
    """The timeline lines this agent logged during this invocation, oldest first."""
    logs = (bus.snapshot(session_id) or {}).get("logs") or []
    return [str(entry.get("msg", "")) for entry in logs if entry.get("node") == AGENT_ID]
if __name__ == "__main__":
    app.run()
