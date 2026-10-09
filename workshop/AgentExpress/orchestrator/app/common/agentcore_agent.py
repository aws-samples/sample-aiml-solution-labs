"""AgentCoreRuntimeAgent — the orchestrator-side node body for an agent that runs
in its OWN AgentCore Runtime (workflow.json `runtime: "dedicated"`).

Instead of running the agent in-process, it calls bedrock-agentcore
InvokeAgentRuntime against that agent's dedicated runtime ARN, passing the same
inputs an in-process agent would read (topic, subject, upstream outputs,
reviewer feedback) and returning the runtime's output text. Same Agent
interface and node wrapper as an in-process agent, so the graph wiring is
identical — only where the compute happens differs.

The per-agent runtime ARNs are injected by Terraform as AGENT_RUNTIME_ARNS
(a JSON map of agent_id -> runtime ARN).
"""

from __future__ import annotations

import asyncio
import json
import os

import boto3
from botocore.config import Config

from app.common.base import Agent
from app.common.config import RUNTIME_INVOKE
from app.features.gateway import person

_RUNTIME_ARNS: dict = json.loads(os.getenv("AGENT_RUNTIME_ARNS", "{}"))
_REGION = os.getenv("AWS_REGION", "us-east-1")
_client = None


def _agentcore():
    """The bedrock-agentcore client, with RETRIES OFF by default.

    boto3's default is `retries={'mode': 'legacy'}` (up to 5 attempts), which is
    wrong for this call: InvokeAgentRuntime is not idempotent, so a retry re-runs
    the whole remote agent and bills a second model call whose answer is thrown
    away. Measured on a live run — see config.RUNTIME_INVOKE for the log excerpt.
    Both numbers are `orchestrator.runtimeInvoke` in workflow.json.
    """
    global _client
    if _client is None:
        _client = boto3.client(
            "bedrock-agentcore", region_name=_REGION,
            config=Config(
                # `total_max_attempts`, NOT `max_attempts`: botocore reads the
                # latter as retries-AFTER-the-first, so `max_attempts: 1` still
                # allows two executions of the agent — precisely the bug. This key
                # counts total attempts and rejects 0, so the config number means
                # what `maxAttempts` says it means.
                retries={"total_max_attempts": int(RUNTIME_INVOKE["maxAttempts"]),
                         "mode": "standard"},
                read_timeout=float(RUNTIME_INVOKE["readTimeoutSeconds"]),
            ))
    return _client


class AgentCoreRuntimeAgent(Agent):
    """Runs this agent by invoking its dedicated AgentCore Runtime."""

    # The dedicated container owns this agent's memory lifecycle: it recalls before
    # the real run() and stores after it (app/subagent_runtime.py), which is the only
    # place that CAN work, because that is where ctx.llm injects the recalled insights
    # into the prompt.
    #
    # Doing it here as well was two separate defects per run. The recall was billed,
    # metered as a recall row, and discarded — `recalled_memory` is read only by
    # ctx.llm and the payload below never carried it. The store wrote the same insight
    # to the same actor namespace twice, and duplicates come back as two copies on
    # every later recall.
    recall_in_orchestrator = False
    store_in_orchestrator = False

    async def run(self, ctx) -> str:
        arn = _RUNTIME_ARNS.get(self.id)
        if not arn:
            # No dedicated runtime wired (e.g. local dev). Be explicit rather
            # than silently producing nothing.
            await ctx.log(f"No dedicated runtime ARN for '{self.id}' (AGENT_RUNTIME_ARNS)")
            return f"[dedicated runtime for {self.id} not configured]"

        from app.features.observability import otel
        payload = {
            "agent_id": self.id,
            "topic": ctx.topic,
            "subject_id": (ctx.state or {}).get("subject_id", ""),
            "outputs": (ctx.state or {}).get("outputs", {}) or {},
            "feedback": getattr(ctx, "feedback", "") or "",
            "session_id": ctx.session_id,
            # The run's files, for an agent with `attachments` (it reads them itself).
            **({"attachments": (ctx.state or {}).get("attachments") or []}
               if getattr(self, "attachments", False) else {}),
            # Propagate the trace context so this dedicated runtime's spans join
            # the SAME CloudWatch trace as the orchestrator (distributed trace).
            "otel_context": otel.carrier(),
            # The run's person, for tools that act as them (gateway/person.py).
            **({"user_token": person.token()} if person.token() else {}),
        }
        # Session id must be >= 33 chars; make it deterministic per (session, agent).
        session = f"{ctx.session_id}-{self.id}".ljust(33, "0")[:33]

        await ctx.log(f"Invoking dedicated AgentCore Runtime for '{self.id}'")
        resp = await asyncio.to_thread(
            _agentcore().invoke_agent_runtime,
            agentRuntimeArn=arn,
            runtimeSessionId=session,
            payload=json.dumps(payload).encode(),
        )
        stream = resp.get("response")
        raw = stream.read() if hasattr(stream, "read") else stream
        data = json.loads(raw)
        if isinstance(data, str):  # tolerate a JSON-string-wrapped body
            data = json.loads(data)
        if isinstance(data, dict):
            # Replay the dedicated agent's timeline lines (tool calls, Cedar
            # refusals): its container has no events table to write them to.
            for line in data.get("logs") or []:
                await ctx.log(str(line))
            if data.get("error"):
                return f"[dedicated runtime error for {self.id}: {data['error']}]"
            return data.get("output", "")
        return str(data)


agent = AgentCoreRuntimeAgent()
