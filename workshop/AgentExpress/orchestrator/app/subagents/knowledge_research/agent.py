"""Knowledge Base Research — RAG over your own documents (step 2, parallel).

Its data source is DECLARED, not coded: workflow.json binds this agent to the
tool named by its `tool` field (type="kb") and scopes retrieval to its `corpus`.
The shared runner in app/subagents/_shared/research.py does the rest.

To point this at your own documents, replace the contents of kb_docs/ — each
top-level folder is a corpus. No change here.

AUTHORED WITH A NESTED LANGGRAPH. Its sibling research agents use Strands
(`web_search`) and no framework at all (`documentation_search`, `cost_research`).
LangGraph is already this repo's outer orchestrator, and
there is nothing stopping an agent from running a graph of its own inside a single
node of that one — which is what this does.

It earns the second framework here rather than just wrapping one call: the graph
has a conditional edge, so an unusable draft gets ONE repair attempt instead of
failing the run. The happy path still costs exactly one model call, the same as
before, because the check between the two nodes is plain Python and not a model.
"""
from typing import TypedDict

from app.common import assets
from app.common.base import Agent
from app.common.context import AgentContext
from app.subagents._shared import research

from .prompts import SYSTEM_PROMPT

# One repair attempt, not a loop. A model that produced unparseable JSON twice is
# not going to be talked round by a third identical request, and an agent that can
# spend unbounded tokens on retries is a cost incident waiting for a bad prompt.
MAX_ATTEMPTS = 2


class DraftState(TypedDict):
    """State of the nested graph. `system`/`user` are the prompts handed down by
    research.synthesize; `draft` is the answer; `attempts` bounds the repair."""

    system: str
    user: str
    draft: str
    attempts: int


def _usable(draft: str) -> bool:
    """Would research.synthesize be able to build an asset out of this?

    Deliberately the same test synthesize applies after `think` returns (a parseable
    object carrying a summary or findings), using the same salvaging parser. If this
    said yes where synthesize says no, the repair edge would never fire on the case
    it exists for.
    """
    payload = assets.extract_json(draft or "") or {}
    return bool(str(payload.get("summary") or "").strip() or payload.get("findings"))


def _build_graph(ctx: AgentContext):
    """Compile the nested graph. Built per run because its nodes close over ctx."""
    from langgraph.graph import END, START, StateGraph

    async def draft(state: DraftState) -> dict:
        text = await ctx.llm(state["system"], state["user"], name=ctx.agent_id)
        return {"draft": text, "attempts": state.get("attempts", 0) + 1}

    async def repair(state: DraftState) -> dict:
        # Names the actual failure and shows the model its own output back. A bare
        # "try again" re-runs the prompt that already failed once.
        await ctx.log("The first draft could not be parsed into a research asset; "
                      "asking once more for JSON only.")
        text = await ctx.llm(
            state["system"],
            state["user"]
            + "\n\n=== YOUR PREVIOUS ATTEMPT WAS REJECTED ===\n"
            + "It could not be parsed as a JSON object carrying a `summary` or "
              "`findings`. Return ONLY the JSON document — no preamble, no code "
              "fence, no commentary after it.\n\n"
            + (state.get("draft") or "")[:2000],
            name=f"{ctx.agent_id}.repair")
        return {"draft": text, "attempts": state.get("attempts", 0) + 1}

    def review(state: DraftState) -> str:
        """The conditional edge, and the only thing that ends the cycle below.

        No model call — this is why the happy path still costs exactly one call, the
        same as the un-framework-ed version of this agent.
        """
        if _usable(state.get("draft", "")):
            return END
        if state.get("attempts", 0) >= MAX_ATTEMPTS:
            # Out of attempts. Hand the draft back anyway and let synthesize raise
            # ModelOutputUnusable, which already explains itself properly.
            return END
        return "repair"

    graph = StateGraph(DraftState)
    graph.add_node("draft", draft)
    graph.add_node("repair", repair)
    graph.add_edge(START, "draft")
    graph.add_conditional_edges("draft", review, {"repair": "repair", END: END})
    # A CYCLE, and `review` is what ends it. An earlier version wired repair
    # straight to END, which bounded the work at two calls by topology — and left
    # MAX_ATTEMPTS above reading like the safety limit while governing nothing. A
    # mutation run caught it: raising MAX_ATTEMPTS to 99 changed no behaviour and no
    # test. Either the counter is the bound or it should not be there, so now it is.
    graph.add_conditional_edges("repair", review, {"repair": "repair", END: END})
    # No checkpointer: this graph is one agent's internal reasoning, and the OUTER
    # graph already checkpoints the run to AgentCore Memory. A second checkpointer
    # here would persist a duplicate of state nobody resumes from.
    return graph.compile()


def langgraph_thinker(ctx: AgentContext):
    """A `think` hook for research.synthesize that reasons inside a nested graph."""
    async def think(system: str, user: str) -> str:
        result = await _build_graph(ctx).ainvoke(
            {"system": system, "user": user, "draft": "", "attempts": 0})
        return str(result.get("draft") or "")

    return think


class KnowledgeResearchAgent(Agent):
    system_prompt = SYSTEM_PROMPT

    async def run(self, ctx: AgentContext) -> str:
        # Optional, app-level Cedar check — the ONE place this sample shows the
        # secondary policy path. It is NOT what protects the Knowledge Base: the
        # retrieval inside synthesize() goes through the Gateway, and the attached
        # policy engine authorizes that call server-side (app/features/policy/,
        # terraform/policy.tf). Keep this pattern for gating an in-process action
        # that never crosses the Gateway. Fail-open, and a no-op when policy is
        # disabled for this agent in workflow.json.
        if not await ctx.policy_check("retrieve_knowledge"):
            await ctx.log("Policy denied retrieve_knowledge for this agent")
            # Reason over upstream inputs only, with no retrieval. BOTH names: the
            # evidence gatherer reads `tools`, and `tool` is its first entry.
            ctx.tools = []
            ctx.tool = None
        return await research.synthesize(ctx, system_prompt=SYSTEM_PROMPT,
                                         think=langgraph_thinker(ctx))


agent = KnowledgeResearchAgent()
