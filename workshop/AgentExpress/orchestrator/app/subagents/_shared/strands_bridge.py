"""Use Strands Agents to author a sub-agent, without giving up this framework's
governance.

Strands talks to a model through a `strands.models.Model` provider. Its default
provider (`BedrockModel`) holds its own boto3 client, and that is exactly what an
agent in this repo must not do: guardrails, cost and token telemetry, long-term
memory injection, truncation detection and cancellation all live in `ctx.llm`, so
an agent that reaches Bedrock directly loses all of them AND STILL REPORTS
SUCCESS. There is no error to notice — just an agent that no longer appears in the
cost panel, is no longer screened by the guardrail, and whose cut-off responses
look complete.

`ContextModel` is therefore a Strands model provider whose only transport is
`ctx.llm`. Strands still owns the agent: the event loop, the system prompt, the
conversation state, its hooks. This repo still owns the model call.

    from app.subagents._shared import research
    from app.subagents._shared.strands_bridge import strands_thinker

    class WebSearchAgent(Agent):
        async def run(self, ctx):
            return await research.synthesize(
                ctx, system_prompt=SYSTEM_PROMPT,
                think=strands_thinker(ctx))

WHAT THIS CANNOT DO: native tool calling. `ctx.llm` returns text — it has no
`toolUse` content blocks to hand back, because `run_llm` calls Converse with a
system and a user message and nothing else. A Strands `@tool` would therefore
never be invoked, so `stream` RAISES when Strands offers it tool specs rather than
dropping them: an agent author who passes `tools=[...]` and sees a plausible answer
would have no way to tell the tool was never called.

That is not a gap in what an agent can reach. Data access here is declared in
workflow.json (`tools` + the agent's `tool` key) and called through
`ctx.call_tool` / `ctx.retrieve`, which is what keeps a data source swappable
without a code change. The evidence is gathered before the model call and handed
to it; see research.synthesize.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterable
from typing import Any

# Claude's context window. Only used to answer Strands' own context-management
# questions; the OUTPUT budget is the agent's `maxTokens` in workflow.json, which
# ctx.llm already applies.
CONTEXT_WINDOW_TOKENS = 200_000


def _user_text(messages) -> str:
    """Flatten a Strands message list into the single user string ctx.llm takes.

    Strands accumulates a conversation; `ctx.llm` is one system + one user prompt.
    For a single-turn agent (the shipped ones) this is the one user message. For a
    multi-turn one it is the turns so far, oldest first, which is the honest
    reading of "everything the model should see now".

    Non-text blocks (images, documents, toolResult) are skipped rather than
    stringified: a Python repr of a content block is not something a model can
    read, and pretending otherwise would put junk in the prompt.
    """
    return "\n\n".join(
        block["text"]
        for message in messages or []
        for block in message.get("content", []) or []
        if isinstance(block, dict) and isinstance(block.get("text"), str))


#: Cache for the lazily built Model subclass (see _model_class).
_MODEL_CLASS: type | None = None


def _model_class() -> type:
    """Build (once) the concrete `strands.models.Model` subclass.

    Declared inside a function rather than at module level so that importing this
    module does not import Strands. Every agent in this repo ships in ONE container
    image, so a top-level `from strands.models import Model` would be paid for by
    agents that never use Strands — and would turn a missing optional dependency
    into an import error for all of them.
    """
    global _MODEL_CLASS
    if _MODEL_CLASS is not None:
        return _MODEL_CLASS

    from strands.models import Model

    class ContextModel(Model):
        """Strands' Model interface, implemented over one ctx.llm call."""

        def __init__(self, ctx, name: str):
            self._ctx = ctx
            self._name = name
            # Strands lets a caller push config onto a provider. Nothing here can
            # act on it — model, temperature and maxTokens are the agent's
            # workflow.json entry and ctx.llm applies them — so it is stored and
            # reported back rather than silently accepted and ignored.
            self._config: dict[str, Any] = {
                "model_id": getattr(ctx, "model", None),
                "context_window_limit": CONTEXT_WINDOW_TOKENS,
            }

        def update_config(self, **model_config: Any) -> None:
            self._config.update(model_config)

        def get_config(self) -> dict[str, Any]:
            return dict(self._config)

        def structured_output(self, output_model, prompt, system_prompt=None,
                              **kwargs: Any) -> AsyncGenerator[dict, None]:
            """Not available through this provider — say so instead of guessing.

            Strands implements this by asking the model for a tool call shaped like
            the pydantic model, and `ctx.llm` cannot carry a tool call. Returning
            best-effort parsed text here would hand back an object that validates
            while having been produced by a different mechanism than the caller
            asked for. The agents in this repo parse their own JSON with
            `assets.extract_json`, which is explicit about being a repair.
            """
            raise NotImplementedError(
                "ContextModel does not support Strands structured_output(): it needs "
                "a tool call, and ctx.llm returns text. Ask for JSON in the prompt "
                "and parse it with app.common.assets.extract_json, which is what "
                "the shipped agents do.")

        async def stream(self, messages, tool_specs=None, system_prompt=None,
                         **kwargs: Any) -> AsyncIterable[dict]:
            """One ctx.llm call, reported back as a Bedrock-shaped event stream.

            Strands consumes the Converse streaming event shape, so the whole
            answer is emitted as a single text delta. ctx.llm is not a streaming
            call (it awaits the full response), and inventing intermediate deltas
            would misreport when text actually arrived.
            """
            if tool_specs:
                # See the module docstring. Dropping these would leave the author's
                # tools installed, never called, and no error to see.
                names = ", ".join(
                    str((spec or {}).get("name", "?")) for spec in tool_specs)
                raise NotImplementedError(
                    f"ContextModel cannot offer tools to the model ({names}). ctx.llm "
                    f"calls Bedrock Converse with a system and a user message only, so "
                    f"a Strands @tool would never be invoked and the answer would look "
                    f"fine anyway. Declare the data source in workflow.json `tools` and "
                    f"call it with ctx.call_tool/ctx.retrieve before reasoning — see "
                    f"app/subagents/_shared/research.py.")

            text = await self._ctx.llm(system_prompt or "", _user_text(messages),
                                       name=self._name)

            yield {"messageStart": {"role": "assistant"}}
            yield {"contentBlockDelta": {"delta": {"text": text}}}
            yield {"contentBlockStop": {}}
            # "end_turn", not "max_tokens", even when the response WAS truncated:
            # ctx.llm already detected that, logged it on the timeline and recorded
            # it in ctx.truncated_calls, and the agent adds a limitation to its
            # asset. Reporting max_tokens here would additionally make Strands
            # treat the turn as needing continuation, which would spend another
            # model call on a response this repo has already accounted for.
            yield {"messageStop": {"stopReason": "end_turn"}}
            # Usage is reported as zero ON PURPOSE. The real token counts are
            # metered inside ctx.llm from the Bedrock response (see
            # app/common/llm.py `_meter_llm`) and are already on the telemetry row
            # and the OTEL span. Numbers repeated here would be a second, guessed
            # copy of a figure this deployment measures.
            yield {"metadata": {
                "usage": {"inputTokens": 0, "outputTokens": 0, "totalTokens": 0},
                "metrics": {"latencyMs": 0},
            }}

    _MODEL_CLASS = ContextModel
    return ContextModel


def strands_agent(ctx, *, system_prompt: str, name: str | None = None,
                  tools: list | None = None):
    """A `strands.Agent` whose model calls go through `ctx.llm`.

    `name` names the model call for observability and AgentCore Evaluations
    (ctx.llm's `name`); it defaults to the agent id, same as ctx.llm does.

    `tools` is accepted so the failure is immediate and legible rather than a
    silent no-op at model time — see the module docstring.
    """
    if tools:
        raise NotImplementedError(
            "Strands tools cannot be used through ContextModel — the model call goes "
            "through ctx.llm, which cannot carry a tool call. Bind the data source in "
            "workflow.json instead and gather evidence with ctx.call_tool/ctx.retrieve.")
    from strands import Agent as StrandsAgent

    model = _model_class()(ctx, name or ctx.agent_id)
    # callback_handler=None: Strands' default handler PRINTS the response to stdout,
    # which in the runtime means the agent's full output in CloudWatch logs on every
    # run. The timeline and the captured asset are where output belongs.
    return StrandsAgent(model=model, system_prompt=system_prompt,
                        callback_handler=None)


def strands_thinker(ctx, *, name: str | None = None):
    """A `think` hook for research.synthesize that reasons inside a Strands agent.

    The system prompt is not fixed here: synthesize passes it per call, so the
    agent keeps its identity in its own prompts.py.
    """
    async def think(system: str, user: str) -> str:
        agent = strands_agent(ctx, system_prompt=system, name=name)
        result = await agent.invoke_async(user)
        return str(result)

    return think
