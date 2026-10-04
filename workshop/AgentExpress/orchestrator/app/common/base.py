"""Base class for agents.

Every agent is a package under app/subagents/<name>/ (agent.py with an Agent
subclass implementing run(), plus __init__.py doing `from .agent import agent`),
listed under "agents" in workflow.json and placed in "steps".

Its `runtime` decides WHERE it runs, not what it does:
  * "main" (default) — runs in-process as a node in the orchestrator runtime.
  * "dedicated"      — runs in its OWN AgentCore Runtime (provisioned by Terraform
                       from workflow.json); the orchestrator invokes it via
                       InvokeAgentRuntime. Same run() code either way.

Its `agentcore` block declares which AgentCore features it uses (memory,
guardrails, evaluations, policy, …). The framework applies them around run()
based on that config, so enabling a capability for an agent is a workflow.json
edit — not a code change. See app/features/ for what each one does.

The framework wires either mode into the graph, status tracking, and UI.
"""

from app.common import defaults
from app.common.context import AgentContext


class Agent:
    # Populated by the registry from workflow.json:
    id: str = ""
    name: str = ""
    # These two mirror workflow.json keys, so their defaults come from the spec rather
    # than being written out again here. `registry._configure` overwrites both for every
    # real agent; the field defaults matter only for an Agent constructed directly, which
    # is what the test suite does — and a field default that disagreed with the spec would
    # make those tests assert the wrong placement.
    kind: str = defaults.get("agent", "kind")       # "sync" or "async"; metadata for the UI
    runtime: str = defaults.get("agent", "runtime")  # in-process node, dedicated, or a2a
    tool: str | None = None     # optional tool label from workflow.json `tools`
    corpus: str | None = None   # for a type=kb tool: which corpus to retrieve from
    model: str | None = None    # optional per-agent model id (defaults to config.MODEL_ID)
    temperature: float = defaults.get("agent", "temperature")
    top_p: float | None = None          # workflow.json `topP`; None = the model's default
    stop_sequences: list = None         # workflow.json `stopSequences`
    # Output budget in tokens, from `maxTokens` in this agent's workflow.json entry.
    # Set it there, not here: an agent emitting a large structured payload needs a
    # bigger budget, and too small a budget truncates the JSON mid-object.
    max_tokens: int = defaults.get("agent", "maxTokens")
    # Who decides which tools to call: "direct" (all, before reasoning) or "model"
    # (the model chooses, one result at a time). workflow.json `toolMode`/`maxToolCalls`.
    tool_mode: str = defaults.get("agent", "toolMode")
    max_tool_calls: int = defaults.get("agent", "maxToolCalls")
    agentcore: dict = None      # AgentCore features config (workflow.json "agentcore" block)
    vision: dict = None         # workflow.json `vision`: earlier agents' images this one reads

    # --- where the long-term memory lifecycle runs -------------------------
    # The node wrapper (app/orchestrator/nodes.py) recalls before run() and stores
    # after it, which is what makes memory purely config-driven for an ordinary
    # in-process agent. Neither is right for an agent whose work happens somewhere
    # else, so the two are separate flags rather than one.
    #
    # Both were unconditionally true, and for a `dedicated` agent that was a measured
    # defect on both counts. The RECALL was computed, billed, written to the telemetry
    # table as a recall row — and then thrown away, because `recalled_memory` is only
    # ever consumed by `ctx.llm` (app/common/context.py) and the InvokeAgentRuntime
    # payload does not carry it. The STORE ran twice, once here and once inside the
    # dedicated container (app/subagent_runtime.py), writing the same insight to the
    # same actor namespace twice per run — and duplicates are then recalled as two
    # copies, which skews what the extraction strategy makes of them.
    #: Does this agent's `run()` actually read `ctx.recalled_memory`?
    recall_in_orchestrator: bool = True
    #: Should the node wrapper store this agent's output as a long-term insight?
    store_in_orchestrator: bool = True

    # Overridden by subclasses:
    system_prompt: str = ""

    async def run(self, ctx: AgentContext) -> str:
        """Do the agent's work and return its output text.

        Use ctx.input('X') for upstream outputs, ctx.llm(...) for the model,
        ctx.call_tool(...)/ctx.retrieve(...) for tools, and ctx.heartbeat(pct)/ctx.log(msg)
        for progress. The AgentCore feature helpers (ctx.guardrail,
        ctx.memory_recall, ctx.get_identity_token, ctx.policy_check) are safe to
        call unconditionally — each is a no-op when disabled in workflow.json.
        """
        raise NotImplementedError
