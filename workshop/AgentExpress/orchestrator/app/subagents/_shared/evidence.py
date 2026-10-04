"""Evidence from an agent's tools, gathered before it reasons.

An agent's `tool` in workflow.json is one key in the `tools` block or a LIST of them.
This queries each, in the order listed, and returns the results as labelled blocks a
model can cite — plus a note for any tool that answered with nothing, so the model is
told plainly what it does not know instead of being left to fill the gap.

Every agent that reads tools goes through here: the research runner
(`research.synthesize`) and the agent `scaffold.py` / the Builder generate. So "give an
agent a second tool" is a config change for all of them, and the call shape for each
tool still comes from that tool's own entry (its `type`, `call`, `arg`, `args`), never
from the agent.

A tool that cannot be called RAISES (ToolUnavailable / ToolDenied) — see
app/common/errors.py. The framework never substitutes placeholder evidence.
"""

from __future__ import annotations

from app.common.config import TOOLS

#: How each tool type is introduced to the model. Named by type, not by the customer's
#: key, so a tool called `claimsDb` still reads as what it is.
LABELS = {
    "kb": "KNOWLEDGE BASE",
    "websearch": "WEB SEARCH",
    "mcp": "MCP SERVER",
    "openapi": "REST API",
    # A customer's own Lambda, fronting whatever the Gateway cannot reach directly
    # (a warehouse, an internal service, something inside a VPC). The label is
    # deliberately generic: only the customer knows what is behind it, and naming
    # the transport is more honest than guessing at the source.
    "lambda": "TOOL FUNCTION",
    # An API Gateway REST API stage: the customer's own API, like openapi.
    "apigateway": "REST API",
}


def tools_of(ctx) -> list[str]:
    """The agent's tools, in declared order. `ctx.tools` when set, else `ctx.tool`."""
    listed = getattr(ctx, "tools", None)
    if listed is not None:
        return list(listed)
    one = getattr(ctx, "tool", None)
    return [one] if one else []


async def gather(ctx, query: str | None = None) -> tuple[str, list[str]]:
    """Query every tool this agent is bound to. Returns (evidence, limitations).

    `evidence` is the labelled blocks joined for a prompt ("" when the agent has no
    tool); `limitations` names each tool that returned nothing for this query.
    `query` defaults to the run's retrieval query (the brief's objective, else the
    topic) — pass one when a tool wants something else.
    """
    parts: list[str] = []
    limitations: list[str] = []
    if query is None:
        query = ctx._retrieval_query()
    if getattr(ctx, "tool_mode", "direct") == "model" and tools_of(ctx):
        # The model chooses which tools to call and with what (app/common/tool_loop.py);
        # what they returned comes back in the same labelled shape as below.
        from app.common import tool_loop
        return await tool_loop.gather(ctx, query, tools=tools_of(ctx), labels=LABELS)
    for tool_key in tools_of(ctx):
        kind = str((TOOLS.get(tool_key) or {}).get("type", "mcp")).lower()
        label = LABELS.get(kind, "TOOL")
        if kind == "kb":
            corpus = getattr(ctx, "corpus", None)
            text, mode = await ctx.retrieve(query, doc_type=corpus)
            scope = f", corpus={corpus}" if corpus else ""
        else:
            text, mode = await ctx.call_tool(tool_key, query)
            scope = ""
        # A failed call raises, so reaching here means the tool answered.
        if text:
            parts.append(f"=== {label}: {tool_key} (mode={mode}{scope}) ===\n{text}")
        else:
            # The tool ran and legitimately had nothing to say. That is real
            # information, not a failure — name it so the model does not invent.
            limitations.append(f"'{tool_key}' returned no matching results for this query.")
    return "\n\n".join(parts), limitations
