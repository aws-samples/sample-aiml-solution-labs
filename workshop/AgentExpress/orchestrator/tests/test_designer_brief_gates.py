"""AgentExpress Assistant (bff/designer.py GUIDE 1b): a gate with named options and a loop
the brief asks for are built in the first draft, never asked about.

Scenario: a newsletter for a community garden club (drafted, fact-checked, then a person
picks Publish or Hold; a failed check is redone and checked again). The model is scripted.
"""
# ruff: noqa: F811 - `env` and `model` are fixtures, imported so every test can take them
from __future__ import annotations

import pytest
from test_builds import env, save  # noqa: F401 - env is the builds fixture, taken by name
from test_designer import conversation, draft, model, run_background, says, send, tool_use  # noqa: F401
from test_designer_requests import AGENT, guide

pytest.importorskip("moto")

BRIEF = ("Build a newsletter flow for our garden club: a writer drafts the issue, a checker "
         "verifies the dates and facts; if the check fails, redo the draft and check it again. "
         "Then ask the editor to Publish or Hold. Publish sends it to the mailing folder.")


def test_the_guide_says_to_build_the_gates_and_loops_the_brief_asks_for(env):
    text = guide(env)
    for part in ("1b. BUILD WHAT THE BRIEF ASKS FOR", "a review gate with named choices",
                 "or for a loop (redo the work and check it again, retry until it passes)",
                 "build it in this draft with what the framework has (rule 3e)",
                 "a gate whose `revise` re-runs the work and its check (both in one `sequence` stage",
                 "Never ask whether to add a gate, an option or a loop the brief asked for",
                 "Ask only about what the brief leaves open"):
        assert part in text, part


def test_a_brief_with_named_options_and_a_retry_loop_builds_them_and_asks_nothing(env, model):
    import validate_build
    save(env)
    agents = {"writer": "Write the issue.", "fact_checker": "Check every date and fact.",
              "publisher": "Prepare the issue for the mailing folder."}
    ops = [{"op": "set_agent", "id": a, "agent": {**AGENT, "name": a.replace("_", " ").title(),
                                                   "agentcore": {**AGENT["agentcore"],
                                                                 "evaluations": {"enabled": True}}},
            "prompt": {"systemPrompt": p, "schema": '{"ok": true}'}} for a, p in agents.items()]
    ops += [{"op": "remove_agent", "id": "claims_intake"}, {"op": "remove_agent", "id": "partner"},
            {"op": "set_steps", "steps": [
                {"sequence": ["writer", "fact_checker"], "gateId": "editor_review", "gateName": "Editor review",
                 "hitl": {"options": ["Publish", "Hold"]},
                 "branch": {"when": [{"field": "decision", "equals": "Hold", "goto": "END"}],
                            "default": "publisher"}},
                {"agent": "publisher"}]}]
    script = model(tool_use(("apply_changes", {"summary": "Newsletter flow with an editor gate",
                                               "version_name": "newsletter draft", "ops": ops})),
                   says("Built it: the writer and checker run as one stage; a failed check is sent "
                        "back with Revise, which redoes the draft and the check. The editor then "
                        "picks Publish or Hold."))
    send(env, BRIEF)
    run_background(env)
    assert script.calls[1]["messages"][-1]["content"][0]["toolResult"]["content"][0]["json"]["status"] == "applied"
    wf = draft(env)["workflow"]
    gate = wf["steps"][0]
    assert gate["sequence"] == ["writer", "fact_checker"]          # revise re-runs the draft and its check
    assert gate["hitl"]["options"] == ["Publish", "Hold"]
    assert gate["branch"]["when"][0] == {"field": "decision", "equals": "Hold", "goto": "END"}
    steps_errors = [i for i in validate_build.validate(wf)
                    if i["severity"] == "error" and i["path"].startswith("steps")]
    assert not steps_errors, steps_errors
    reply = conversation(env)[1]["turns"][-1]
    assert "?" not in reply["text"] and not reply.get("questions")
