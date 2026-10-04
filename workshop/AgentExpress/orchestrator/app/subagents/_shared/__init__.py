"""Code THIS SAMPLE's agents share. Not framework — yours to change or delete.

The framework is `app/common/` and `app/orchestrator/`: it knows about topology,
gates, tools, memory, observability and the asset envelope, and nothing about any
subject matter. It never imports from here.

What lives here is the part of this sample that happens to be reused by more than
one of its agents:

    research.py     the evidence-gathering flow shared by the four research
                    agents: read the brief, call the agent's configured tool,
                    ask the model to classify what it found, emit a
                    ResearchOutput.
    synthesis.py    the flow shared by analysis, recommendation and report:
                    gather the approved upstream assets, ask for JSON that traces
                    claims back to them, hand the payload to the agent.
    contracts/      the five asset shapes those agents emit.

Because this is a leaf directory under `app/subagents/` and not an agent id in
workflow.json, the registry never loads it as an agent.

BUILDING YOUR OWN WORKFLOW
You do not have to use any of it. An agent needs exactly two things: a `run(ctx)`
that returns a string, and an entry in workflow.json. Everything else — which
model, which tool, which corpus, how many tokens, whether it runs in its own
runtime, which gate follows it — is config the framework applies for you.

    # app/subagents/my_agent/agent.py
    from app.common.base import Agent

    class MyAgent(Agent):
        async def run(self, ctx):
            answer = await ctx.llm(SYSTEM, f"{ctx.topic}")
            return answer

Reach for these runners when your agent's shape matches theirs; copy one into
your own folder and edit it when it nearly does; ignore them when it does not.
"""
