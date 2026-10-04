"""Intake — turns the user's request into a structured brief (step 1).

The first stage of the pipeline. It takes the free-text request (ctx.topic) and
asks the model to distil it into a RequestBrief: a clear objective, scope, the
key questions to answer, constraints, and open questions for the human to
confirm at the HITL gate.

On a "revise" gate decision the reviewer's feedback arrives as ctx.feedback and
the agent re-derives the brief taking that guidance into account; the version
bumps so prior versions are preserved.
"""

from __future__ import annotations

import json

from app.common import assets, clock
from app.common.base import Agent
from app.common.context import AgentContext
from app.common.contracts.base import AssetStatus
from app.subagents._shared import synthesis
from app.subagents._shared.contracts import RequestBrief

from .prompts import SCHEMA, SYSTEM_PROMPT


class IntakeAgent(Agent):
    system_prompt = SYSTEM_PROMPT

    async def run(self, ctx: AgentContext) -> str:
        user = f"=== USER REQUEST ===\n{ctx.topic}\n"
        if ctx.feedback:
            user += f"\n=== REVIEWER GUIDANCE ===\n{ctx.feedback}\n"
        user += f"\nReturn ONLY JSON matching this schema:\n{SCHEMA}"

        payload = assets.extract_json(await ctx.llm(SYSTEM_PROMPT, user)) or {}
        version = synthesis.prior_version(ctx)
        # A brief cut off by the token ceiling is the worst one to let through
        # silently: it frames every agent after it, and a missing scope or question
        # list narrows the whole run. `assumptions` is where this brief records what a
        # reviewer should check, which is exactly what this is.
        truncated = ([
            ("This brief was assembled from a model response that hit its output token "
             "limit and was cut off, so the lists below may be incomplete. Raise "
             "`maxTokens` for this agent in workflow.json.")
        ] if ctx.truncated_calls else [])

        title = str(payload.get("title") or ctx.topic or "Untitled request").strip()
        objective = str(payload.get("objective") or "").strip()
        if not objective:
            objective = ctx.topic

        asset = RequestBrief(
            assetId=f"asset-request-brief-{synthesis.slug(title)}-v{version}",
            version=version,
            status=AssetStatus.IN_REVIEW,
            createdAt=clock.now_et(),
            createdByAgent=ctx.agent_id,
            executiveSummary=str(payload.get("executiveSummary") or objective).strip() or None,
            title=title,
            objective=objective,
            # Passed through, not str()-ed: the contract's Scope model accepts
            # either the {inScope, outOfScope} object the model returns or a
            # plain string. str() on a dict produced a Python repr.
            scope=payload.get("scope") or {},
            keyQuestions=synthesis.str_list(payload, "keyQuestions"),
            constraints=synthesis.str_list(payload, "constraints"),
            assumptions=synthesis.str_list(payload, "assumptions") + truncated,
            openQuestions=synthesis.str_list(payload, "openQuestions"),
        )
        return json.dumps(asset.model_dump(by_alias=True, mode="json"), indent=2)


agent = IntakeAgent()
