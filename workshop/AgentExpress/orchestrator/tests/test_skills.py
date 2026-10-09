"""Skills (workflow.json `skills`, an agent's `skills`; app/common/skills.py).
What has to hold:
  * with toolMode "model" and tools, the tool turn is offered `use_skill` for the agent's
    OWN skills only, lists them by name and description, and opening one is not a tool
    call: it does not use up maxToolCalls and never becomes evidence;
  * every model call the agent makes after that carries the skills it opened in full,
    and the others by name only (progressive disclosure);
  * with no model-driven tool turn, the agent's skills reach its prompt in full,
    reference files included, within a budget;
  * a skill's text is only ever read: there is nothing that runs it.
"""
from __future__ import annotations

from conftest import workflow
from test_tool_loop import AI, FakeCtx, FakeTool, _run

SKILLS = {
    "refunds": {"description": "When a customer asks for money back.",
                "instructions": "1. Check the purchase date.\n2. Within 30 days: refund. Otherwise: store credit.",
                "files": {"policy.md": "Refunds need a receipt."}},
    "tone": {"description": "When writing to a customer.", "instructions": "Be brief and kind."},
    "secret": {"description": "Another agent's.", "instructions": "Not for triage."},
}
TOOLS = {"claimsDb": {"type": "lambda", "description": "Claims warehouse",
                      "lambdaArn": "arn:aws:lambda:us-east-1:123456789012:function:f",
                      "toolSchema": [{"name": "q", "properties": {"query": {"type": "string"}}}]}}


def wf(**agent) -> dict:
    return {"orchestrator": {}, "tools": TOOLS, "skills": SKILLS,
            "agents": {"triage": {"name": "Triage", "runtime": "dedicated", "maxTokens": 100,
                                  "tool": "claimsDb", **agent}},
            "steps": [{"agent": "triage"}]}


def test_the_tool_turn_opens_its_own_skills_and_that_is_not_a_tool_call(monkeypatch):
    with workflow(wf(toolMode="model", maxToolCalls=1, skills=["refunds", "tone"])) as imp:
        db = FakeTool("claimsDb___q", "Look up a claim", answer="bought 3 May")
        ctx = FakeCtx(["claimsDb"], max_tool_calls=1)
        ctx.skills, ctx.opened_skills = ["refunds", "tone"], set()
        script = [AI(("use_skill", {"name": "refunds"}), ("use_skill", {"name": "refunds", "file": "policy.md"}),
                     ("use_skill", {"name": "secret"})),
                  AI(("claimsDb___q", {"claim": "88213"})),
                  AI(text="done")]
        text, _gaps, turns = _run(monkeypatch, imp, ctx, script, {"claimsDb": [db]})
    spec = turns[0]["tools"][0]
    assert spec["name"] == "use_skill"
    assert spec["input_schema"]["properties"]["name"]["enum"] == ["refunds", "tone"]
    task = turns[0]["messages"][0].content
    assert "- refunds: When a customer asks for money back. (reference files: policy.md)" in task
    assert "secret" not in task
    replies = [m.content for m in turns[1]["messages"][-3:]]
    assert "Within 30 days: refund" in replies[0]
    assert "Refunds need a receipt." in replies[1]
    assert "no skill named 'secret' for you" in replies[2]
    # Opening skills used none of the one allowed tool call: the Gateway call still ran.
    assert db.seen == [{"claim": "88213"}]
    assert ctx.opened_skills == {"refunds"}
    assert "Opened skill: refunds" in ctx.logs and "Opened skill: refunds / policy.md" in ctx.logs
    # Know-how, not evidence.
    assert "Within 30 days" not in text and "bought 3 May" in text


def test_the_agents_model_calls_carry_what_it_opened_in_full_and_the_rest_by_name():
    with workflow(wf(toolMode="model", skills=["refunds", "tone"])) as imp:
        skills = imp("app.common.skills")
        block = skills.prompt_block(["refunds", "tone"], {"refunds"}, full=False)
    assert "--- SKILL: refunds ---" in block and "Within 30 days" in block
    assert "Refunds need a receipt." not in block       # its file was not added unasked
    assert "Be brief and kind." not in block
    assert "- tone: When writing to a customer." in block


def test_without_a_model_tool_turn_the_skills_are_in_the_prompt_in_full():
    with workflow(wf(skills=["refunds", "tone"])) as imp:
        skills = imp("app.common.skills")
        block = skills.prompt_block(["refunds", "tone", "unknown"], set(), full=True)
        assert skills.prompt_block([], set(), full=True) == ""
    for want in ("Within 30 days", "--- refunds / policy.md ---\nRefunds need a receipt.", "Be brief and kind."):
        assert want in block
    assert "unknown" not in block


def test_a_skill_beyond_the_budget_is_named_not_pasted(monkeypatch):
    with workflow(wf(skills=["refunds", "tone"])) as imp:
        skills = imp("app.common.skills")
        first = len(skills.prompt_block(["refunds"], set(), full=True).split("\n\n")[1])
        monkeypatch.setattr(skills, "FULL_CHARS", first + 5)   # room for refunds, not for tone too
        block = skills.prompt_block(["refunds", "tone"], set(), full=True)
    assert "Within 30 days" in block
    assert "Skill tone not shown in full" in block and "When writing to a customer." in block


def test_ctx_llm_adds_the_skills_by_tool_mode(monkeypatch):
    """ctx.llm is the one place every agent framework's model call goes through."""
    import asyncio
    import types
    with workflow(wf(toolMode="model", skills=["refunds", "tone"])) as imp:
        context = imp("app.common.context")
        seen = []

        async def fake(name, system, user, **kw):
            seen.append(system)
            return "ok", False
        monkeypatch.setattr(context, "run_llm", fake)

        async def none():
            return []
        ctx = context.AgentContext.__new__(context.AgentContext)
        ctx.__dict__.update({"recalled_memory": [], "agent_id": "triage", "max_tokens": 100, "model": None,
                             "temperature": 0, "top_p": None, "stop_sequences": [], "vision": {},
                             "truncated_calls": [], "session_id": "s1", "skills": ["refunds", "tone"],
                             "opened_skills": {"tone"}, "tool_mode": "model", "tools": ["claimsDb"]})
        ctx._vision_images = none
        ctx._run_files = none
        ctx.log = types.MethodType(lambda self, m: none(), ctx)
        asyncio.run(ctx.llm("SYS", "USER"))
        ctx.tool_mode = "direct"           # no tool turn to open them in: all in full
        asyncio.run(ctx.llm("SYS", "USER"))
    assert len(seen) == 2
    assert "- refunds: When a customer asks for money back." in seen[0] and "Be brief and kind." in seen[0]
    assert "Within 30 days" not in seen[0]
    assert "Within 30 days" in seen[1] and "Refunds need a receipt." in seen[1]
