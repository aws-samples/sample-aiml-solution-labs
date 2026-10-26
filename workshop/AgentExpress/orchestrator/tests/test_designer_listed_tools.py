"""AgentExpress Assistant (bff/designer.py, bff/review.py): every tool the user lists is a tool.

Seen on a live session: a brief listed its tools by name, one of them a parser for the
file the run is started with. The first build gave the reading agent `attachments: true`
and no parse tool ("parsing is done by the model"); another try of the same brief did add
it. The guide now says a listed capability is its own tool even when a model could do the
work, that `attachments` does not replace it, and to count the tools before replying; the
reviewer looks for a listed tool with no tool of its own, and its note gets the change
revised before it is saved.

Scenarios: staff onboarding and device telemetry; the model and the reviewer are scripted.
"""

# ruff: noqa: F811 - `env`, `model` and `reviewer` are fixtures, imported so every test can take them
from __future__ import annotations

import pytest
from test_builds import call, env, save  # noqa: F401 - env is the builds fixture, taken by name
from test_designer import draft, model, run_background, says, tool_use  # noqa: F401
from test_designer_review import reviewer, tool_result  # noqa: F401

moto = pytest.importorskip("moto")

AGENT = {"runtime": "main", "maxTokens": 1500, "toolMode": "model",
         "agentcore": {"guardrails": {"input": True, "output": True}, "evaluations": {"enabled": True}}}
HANDLER = "def lambda_handler(event, context):\n    return {\"ok\": True}\n"


def guide(env) -> str:
    return " ".join(env.handler.designer.system_prompt()[0]["text"].split())


def test_the_guide_and_the_reviewer_say_a_listed_capability_is_its_own_tool(env):
    text = guide(env)
    for part in (("A capability the user lists as a tool is its own tool even when an agent's model could do that "
                  "work itself"),
                 ("`attachments: true` lets an agent read the run's files, but a listed parser, "
                  "extractor or checker still becomes a tool bound to that agent"),
                 "Count them before you reply: every tool the user listed has a key in `tools`",
                 ("It does not replace a tool the user listed to parse or check those files (rule 3b): give the "
                  "agent both")):
        assert part in text, part
    assert ("a tool the user listed by name that has no tool of its own (done by an agent's model instead)"
            in env.handler.designer._review.SYSTEM)


def _build(agent_id: str, tools: list[str]) -> tuple[str, dict]:
    ops = [{"op": "set_tool", "key": t, "tool": {"type": "lambda", "description": t, "code": {"grants": {}},
                                                 "call": t, "arg": "input", "toolSchema": [
                                                     {"name": t, "properties": {"input": {"type": "string",
                                                                                          "required": True}}}]}}
           for t in tools]
    ops += [{"op": "set_tool_code", "key": t, "files": {"handler.py": HANDLER}} for t in tools]
    ops += [{"op": "set_agent", "id": agent_id,
             "agent": {**AGENT, "name": agent_id.title(), "attachments": True,
                       **({"tool": tools} if tools else {})},
             "prompt": {"systemPrompt": "You read the attached file and act on it.", "schema": '{"ok": true}'}},
            {"op": "set_steps", "steps": [{"agent": agent_id, "hitl": True}, {"agent": "claims_intake"},
                                          {"agent": "partner"}]}]
    return ("apply_changes", {"summary": f"{agent_id} with {len(tools)} tools", "version_name": "first draft",
                              "ops": ops})


@pytest.mark.parametrize("request_text,agent_id,listed,parser", [
    (("Build an onboarding flow. Tools: parseOfferLetter (reads the attached offer letter), createAccounts, "
      "checkBackground."), "onboarding", ["parseOfferLetter", "createAccounts", "checkBackground"], "parseOfferLetter"),
    (("Build a telemetry triage flow. Tools: parseReadings (reads the attached CSV of readings), scoreAnomaly, "
      "notifyOncall."), "telemetry", ["parseReadings", "scoreAnomaly", "notifyOncall"], "parseReadings"),
])
def test_a_listed_parser_left_to_the_model_is_flagged_and_built_as_a_tool(env, model, reviewer, request_text,
                                                                          agent_id, listed, parser):
    save(env)
    without = [t for t in listed if t != parser]
    script = model(tool_use(_build(agent_id, without)), tool_use(_build(agent_id, listed)),
                   says(f"Built {agent_id} with {len(listed)} tools."))
    seen = reviewer({"verdict": "issues",
                     "notes": [f"{parser} was listed as a tool but has no tool of its own; add it to {agent_id}."]})
    call(env, "POST /api/builds/{id}/design", params={"id": "pclaims01"},
         body={"message": request_text, "review": "always"})
    run_background(env)
    # The reviewer was given the user's list, and held the change that left one out.
    assert request_text in seen[0]
    held = tool_result(script, 1)
    assert held["status"] == "review" and held["applied"] is False and parser in held["review_notes"][0]
    assert tool_result(script, 2)["status"] == "applied"
    wf = draft(env)["workflow"]
    assert sorted(k for k in wf["tools"]) == sorted(listed)
    assert wf["agents"][agent_id]["tool"] == listed and wf["agents"][agent_id]["attachments"] is True
