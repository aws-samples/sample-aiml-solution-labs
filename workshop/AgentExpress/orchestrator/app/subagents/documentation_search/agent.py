"""MCP Documentation Research — a remote MCP server (step 2, parallel).

This agent exists to demonstrate the BRING-YOUR-OWN-MCP-SERVER path. It is bound
by workflow.json to a tool of type="mcp"; to point it at your own server, change
that tool's `endpoint` and nothing else.

It is also the plainest of the four research agents: one model call through
`ctx.llm`, no agentic framework inside it. `web_search` does the same job with a
Strands agent and `knowledge_research` with a nested LangGraph, which is what makes
this one worth reading first — the framework is an authoring choice, and this is
what the choice looks like when you decline it.
"""
from app.common.base import Agent
from app.common.context import AgentContext
from app.subagents._shared import research

from .prompts import SYSTEM_PROMPT


class DocumentationSearchAgent(Agent):
    system_prompt = SYSTEM_PROMPT

    async def run(self, ctx: AgentContext) -> str:
        return await research.synthesize(ctx, system_prompt=SYSTEM_PROMPT)


agent = DocumentationSearchAgent()
