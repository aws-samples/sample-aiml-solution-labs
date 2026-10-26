"""AgentExpress Assistant (bff/designer.py): telling a request from pasted content.

Seen on a live session: the build JSON went to the model in the same user message as the
user's text, labelled only as "the build as it is right now", so the model read it as
something the user had pasted. A real change request ("make it one agent with these
tools") was then answered "you pasted the build state, but no request", nothing changed,
and a later "revert" went back to an empty build. The rules the model gets are general:
the server's context is said to be the server's, a message is judged by the user's own
text, a request that carries pasted detail is acted on, and pasted content with no
request at all still gets one question and no change.

Every scenario here is from a domain of its own (invoices, staff onboarding, device
alerts, research reports); the model is scripted, as in tests/test_designer.py.
"""

# ruff: noqa: F811 - `env` and `model` are fixtures, imported so every test can take them
from __future__ import annotations

import pytest
from test_builds import env, save  # noqa: F401 - env is the builds fixture, taken by name
from test_designer import conversation, draft, model, run_background, says, send, tool_use  # noqa: F401

moto = pytest.importorskip("moto")


def guide(env) -> str:
    return " ".join(env.handler.designer.system_prompt()[0]["text"].split())


def newest(call) -> list[dict]:
    """The text blocks of the newest user message of a model call."""
    return [b for b in call["messages"][-1]["content"] if "text" in b]


AGENT = {"runtime": "main", "maxTokens": 1500, "agentcore": {"guardrails": {"input": True, "output": True}}}


def one_agent_with_tools(agent_id: str, tools: list[str]) -> tuple[str, dict]:
    """A change that makes the build one agent bound to `tools` (Lambda tools written in it)."""
    ops = [{"op": "set_tool", "key": t, "tool": {
        "type": "lambda", "description": f"{t} for the agent", "code": {"grants": {}},
        "call": t, "arg": "input",
        "toolSchema": [{"name": t, "description": t, "properties": {
            "input": {"type": "string", "required": True}}}]}} for t in tools]
    ops += [{"op": "set_tool_code", "key": t, "files": {
        "handler.py": "def lambda_handler(event, context):\n    return {\"ok\": True, \"input\": event.get(\"input\")}\n"}}
        for t in tools]
    ops += [{"op": "set_agent", "id": agent_id,
             "agent": {**AGENT, "name": agent_id.replace("_", " ").title(), "tool": tools, "toolMode": "model",
                       "agentcore": {**AGENT["agentcore"], "evaluations": {"enabled": True}}},
             "prompt": {"systemPrompt": "You do the whole job with your tools.", "schema": '{"done": true}'}},
            {"op": "remove_agent", "id": "claims_intake"}, {"op": "remove_agent", "id": "partner"},
            {"op": "set_steps", "steps": [{"agent": agent_id, "hitl": True}]}]
    return ("apply_changes", {"summary": f"One {agent_id} agent with {len(tools)} tools",
                              "version_name": "single agent", "ops": ops})


# --- the server's context is the server's ------------------------------------------------

@pytest.mark.parametrize("message", [
    "Add a step that checks each invoice total against its purchase order before posting.",
    "Device alerts: raise anything above 80 C to the on-call engineer and log the rest.",
])
def test_the_build_state_is_labelled_as_the_servers_and_the_users_text_stands_alone(env, model, message):
    save(env)
    script = model(says("Done."))
    send(env, message)
    run_background(env)
    blocks = [b["text"] for b in newest(script.calls[0])]
    context, user, notes = blocks[0], blocks[1], blocks[2]
    # The build JSON is said to come from the server, and where it ends is said too.
    assert context.startswith("From the server, not the user (the user did not write, paste or send")
    assert '"claims_intake"' in context
    assert context.rstrip().endswith("The user's own message is the next text block, and only that "
                                      "block (with any files they attached).")
    # The user's message is its own block, exactly as they wrote it.
    assert user == message
    assert notes.startswith("Conversation notes (from the server, not the user):")
    assert "The user's message this turn is only the text block between the build state" in notes
    assert "judge whether the user asked for something from their own text alone" in notes


def test_the_guide_defines_a_request_by_the_users_own_text(env):
    text = guide(env)
    for part in ("1a. WHAT THE USER ASKED", "The user wrote none of it, so never say they pasted or sent the build",
                 "The user's message is only their own text block (and the files they attached)",
                 "It IS A REQUEST when any part of it asks for the workflow they want",
                 "a correction of the current build (what it is not, or what it should be instead)",
                 "Pasted content that comes with a request is the detail of that request, however long: act on it",
                 "once a draft exists, apply the change; never ask first",
                 "When the request matches what the build already is, say so and change nothing",
                 ("It is A MESSAGE WITH NO REQUEST when it is pasted content with no instruction that "
                  "does not map like that"),
                 "PASTED CONTENT THAT EDITS THE BUILD",
                 "every item names an existing agent, tool or step by its exact key or name",
                 "Apply it to those parts only (nothing else changes), and say in one line which parts",
                 "that line is the whole reply (no list, no other notes, no question or offer)",
                 "any options go inside that one sentence, so the reply has exactly one question mark"):
        assert part in text, part


# --- a request that carries pasted detail is acted on ------------------------------------

@pytest.mark.parametrize("message,agent_id,tools", [
    # An instruction followed by a pasted tool list.
    (("Restructure this into a single agent that has these tools:\n"
      "1. extractFields - read the supplier, date, lines and total from an invoice PDF\n"
      "2. checkOrder - look up the purchase order and compare the lines\n"
      "3. postLedger - post an approved invoice to the ledger"),
     "invoice_agent", ["extractFields", "checkOrder", "postLedger"]),
    # A correction of the current build, with no imperative verb at all.
    (("It's not a chain of separate agents; it's one onboarding agent that calls its tools in order.\n"
      "Tools:\n- createAccount: make the new hire's accounts\n- assignLaptop: reserve a laptop\n"
      "- bookOrientation: book the first-day session"),
     "onboarding_agent", ["createAccount", "assignLaptop", "bookOrientation"]),
])
def test_a_request_with_pasted_detail_is_applied_once_a_draft_exists(env, model, message, agent_id, tools):
    save(env)
    script = model(tool_use(one_agent_with_tools(agent_id, tools)), says("It is now one agent with its tools."))
    send(env, message)
    run_background(env)
    result = script.calls[1]["messages"][-1]["content"][0]["toolResult"]["content"][0]["json"]
    assert result["status"] == "applied", result
    wf = draft(env)["workflow"]
    assert list(wf["agents"]) == [agent_id] and wf["agents"][agent_id]["tool"] == tools
    assert sorted(wf["tools"]) == sorted(tools)
    reply = conversation(env)[1]["turns"][-1]
    assert reply["status"] == "done" and reply["changes"][0]["versionName"] == "single agent"
    # The model judged the user's own block: the message reached it whole, on its own.
    assert newest(script.calls[0])[1]["text"] == message


# --- pasted content with no request at all: one question, no change ----------------------

@pytest.mark.parametrize("pasted", [
    # A device log.
    ("2026-03-02T10:15:04Z sensor-17 temp=81.2C status=WARN\n"
     "2026-03-02T10:15:09Z sensor-17 temp=83.9C status=ALERT\n"
     "2026-03-02T10:15:14Z sensor-04 temp=22.0C status=OK"),
    # A specification, on its own.
    ("Research report spec\nSections: summary, method, findings, references\nLength: 6-8 pages\n"
     "Citations: one per claim, with a link"),
])
def test_pasted_content_with_no_request_gets_one_question_and_no_change(env, model, pasted):
    assert "A MESSAGE WITH NO REQUEST" in guide(env)
    save(env)
    before = draft(env)["workflow"]
    script = model(says("Should I build a workflow from this, or change the current build to match it?"))
    send(env, pasted)
    run_background(env)
    reply = conversation(env)[1]["turns"][-1]
    assert reply["status"] == "done" and not reply.get("changes")
    assert reply["text"].count("?") == 1
    assert draft(env)["workflow"] == before and conversation(env)[1]["versions"] == []
    assert newest(script.calls[0])[1]["text"] == pasted
