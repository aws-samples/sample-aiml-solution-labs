"""Web Search Research — the live public web (step 2, parallel).

Bound by workflow.json to a tool of type="websearch", which the IaC provisions as
the managed AgentCore Web Search connector on the Gateway. No API keys, no
endpoint, no schema: the connector is AWS-operated and queries never leave AWS.

Nothing about the search is coded here — `maxResults` and any domain filters come
from the tool's entry in workflow.json.

AUTHORED WITH STRANDS AGENTS. This is the demonstration that the agentic framework
inside an agent is YOUR choice: the reasoning step runs in a `strands.Agent`, while
`knowledge_research` uses a nested LangGraph and `documentation_search` and
`cost_research` use no framework at all. All four gather evidence the same declared
way and emit the same ResearchOutput contract, so the choice changes nothing outside
this file. (Strands and LangGraph are the two shipped; CrewAI is documented as a
pattern in README.md but deliberately not in requirements.txt — it measured 804 MB
against this image's 153, and every agent shares one image.)

The one rule is that the model call goes through `ctx.llm` — see
app/subagents/_shared/strands_bridge.py for what that buys and what it costs.
"""
from app.common.base import Agent
from app.common.context import AgentContext
from app.subagents._shared import research
from app.subagents._shared.strands_bridge import strands_thinker

from .prompts import SYSTEM_PROMPT


class WebSearchAgent(Agent):
    system_prompt = SYSTEM_PROMPT

    async def run(self, ctx: AgentContext) -> str:
        # `think` replaces the default single ctx.llm call with a Strands agent
        # loop. Evidence gathering (the websearch tool) and asset assembly are
        # unchanged and still live in the shared runner.
        return await research.synthesize(ctx, system_prompt=SYSTEM_PROMPT,
                                         think=strands_thinker(ctx))


agent = WebSearchAgent()
