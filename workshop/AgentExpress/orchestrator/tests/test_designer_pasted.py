"""AgentExpress Assistant (bff/designer.py GUIDE 1a): pasted content with no instruction.

Rule 1a acts on an instruction, and also on pasted content that clearly edits parts the
build already has (it names them exactly and redescribes them). Anything else pasted with
no instruction (a log, a spec or list of things the build does not have) gets ONE
question and no change. Scenarios: a greenhouse irrigation controller, a print shop's
order flow, a museum ticketing flow. The model is scripted, as in tests/test_designer.py.
"""
# ruff: noqa: F811 - `env` and `model` are fixtures, imported so every test can take them
from __future__ import annotations

import pytest
from test_builds import env, save  # noqa: F401 - env is the builds fixture, taken by name
from test_designer import conversation, draft, model, run_background, says, send, tool_use  # noqa: F401
from test_designer_requests import guide, newest, one_agent_with_tools

pytest.importorskip("moto")

TOOLS = ["readMoisture", "openValve", "logCycle"]


def with_three_tools(env, model):
    save(env)
    model(tool_use(one_agent_with_tools("irrigation_agent", TOOLS)), says("One agent with 3 tools."))
    send(env, "Make it one irrigation agent with readMoisture, openValve and logCycle.")
    run_background(env)
    return draft(env)


def test_the_guide_says_when_pasted_content_is_an_edit(env):
    text = guide(env)
    for part in (("with no instruction, pasted content is still an edit when it clearly maps onto parts the "
                 "build already has"), "each item matches exactly one part",
                 ("a specification or list of things the build does not have, or a list where any item "
                 "matches no part or several")):
        assert part in text, part


def test_a_pasted_list_that_redescribes_existing_tools_updates_them(env, model):
    before = with_three_tools(env, model)
    pasted = ("readMoisture - read the soil probe for one bed, in percent\n"
              "openValve - open one bed's valve for a number of seconds\n"
              "logCycle - record the bed, the reading and how long the valve was open")
    new = dict(line.split(" - ", 1) for line in pasted.splitlines())
    script = model(tool_use(("apply_changes", {
        "summary": "New descriptions for 3 tools", "version_name": "tool descriptions",
        "ops": [{"op": "update_tool", "key": k, "set": {"description": d}} for k, d in new.items()]})),
        says("Updated the descriptions of readMoisture, openValve and logCycle."))
    send(env, pasted)
    run_background(env)
    assert script.calls[1]["messages"][-1]["content"][0]["toolResult"]["content"][0]["json"]["status"] == "applied"
    after = draft(env)
    for k in TOOLS:
        assert after["workflow"]["tools"][k]["description"] == new[k]
    # Nothing but those descriptions changed.
    strip = {k: {**t, "description": None} for k, t in after["workflow"]["tools"].items()}
    assert strip == {k: {**t, "description": None} for k, t in before["workflow"]["tools"].items()}
    assert after["workflow"]["agents"] == before["workflow"]["agents"]
    reply = conversation(env)[1]["turns"][-1]
    assert reply["text"].count("\n") == 0 and "?" not in reply["text"]
    assert newest(script.calls[0])[1]["text"] == pasted


@pytest.mark.parametrize("pasted", [
    # An error log.
    ("Traceback (most recent call last):\n  File \"orders.py\", line 41, in submit\n"
     "    queue.put(job)\nConnectionResetError: [Errno 54] Connection reset by peer"),
    # New capabilities the build does not have.
    ("quoteJob - price a print job from size, stock and quantity\n"
     "proofCheck - compare the proof against the customer's file\n"
     "bookCourier - book a pickup for the finished order"),
])
def test_pasted_content_that_maps_to_nothing_gets_one_question_and_no_change(env, model, pasted):
    before = with_three_tools(env, model)
    versions = conversation(env)[1]["versions"]
    model(says("Should I add these to the build, or did you want something else done with them?"))
    send(env, pasted)
    run_background(env)
    reply = conversation(env)[1]["turns"][-1]
    assert reply["status"] == "done" and not reply.get("changes")
    assert reply["text"].count("?") == 1
    assert draft(env)["workflow"] == before["workflow"] and conversation(env)[1]["versions"] == versions


def test_an_instruction_with_a_pasted_spec_is_acted_on(env, model):
    save(env)
    message = ("Build this ticketing flow:\n"
               "checkSlot - confirm a time slot has room\nissueTicket - make the QR ticket\n"
               "sendReceipt - email the receipt")
    tools = ["checkSlot", "issueTicket", "sendReceipt"]
    script = model(tool_use(one_agent_with_tools("ticket_agent", tools)), says("Built the ticketing agent."))
    send(env, message)
    run_background(env)
    assert script.calls[1]["messages"][-1]["content"][0]["toolResult"]["content"][0]["json"]["status"] == "applied"
    assert sorted(draft(env)["workflow"]["tools"]) == sorted(tools)
