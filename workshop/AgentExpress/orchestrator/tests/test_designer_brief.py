"""AgentExpress Assistant (bff/designer.py): what a production session got wrong, kept fixed.

A user asked it for a pipeline that parses an uploaded CSV, renders three documents from
templates in parallel, checks them, and delivers them only if the check passes, otherwise
asks the operator to override or abort. The Assistant said steps cannot run in parallel,
folded seven requested tools into one, said there was no rendering tool, repeated a
"Not covered" list every turn, guessed when a message held no request, reverted to a
version nobody expected, and stalled on a bucket name. One test per item, in that order;
the model is scripted, as in tests/test_designer.py. scripts/assistant_regression.py asks
a deployed console the same things.
"""

# ruff: noqa: F811 - `env` and `model` are fixtures, imported so every test can take them
from __future__ import annotations

import pytest
from test_builds import call, env, save  # noqa: F401 - env is the builds fixture, taken by name
from test_designer import add_fraud, conversation, draft, model, run_background, says, send, tool_use  # noqa: F401

moto = pytest.importorskip("moto")


def guide(env) -> str:
    """The system prompt the model reads, on one line."""
    return " ".join(env.handler.designer.system_prompt()[0]["text"].split())


def notes(call) -> str:
    """The conversation notes that came with the newest message of a model call."""
    content = call["messages"][-1]["content"]
    return next(b["text"] for b in content if b.get("text", "").startswith("Conversation notes"))


GENERATOR = {"runtime": "main", "maxTokens": 1500,
             "agentcore": {"guardrails": {"input": True, "output": True}}}


# --- 1. parallel steps ------------------------------------------------------------------

def test_run_x_y_and_z_in_parallel_is_one_parallel_stage(env, model):
    """It said steps cannot run in parallel. The guide says they can and how, the reviewer
    flags requested parallel work that is not, and such a change is applied as asked."""
    text = guide(env)
    for part in ("PARALLEL STAGES EXIST", "put those agents in ONE `parallel` stage",
                 "Never say that steps cannot run in parallel",
                 "put the check, the gate or the branch on the stage after it"):
        assert part in text, part
    assert "not in one `parallel` stage" in env.handler.designer._review.SYSTEM
    save(env)
    gens = ("pdf_report", "pptx_deck", "docx_summary")
    ops = [{"op": "set_agent", "id": g, "agent": {**GENERATOR, "name": g, "produces": g},
            "prompt": {"systemPrompt": f"You write the {g}.", "schema": "{}"}} for g in gens]
    ops.append({"op": "set_steps", "steps": [
        {"agent": "claims_intake", "hitl": True},
        {"parallel": list(gens), "gateId": "generate", "gateName": "Generate"},
        {"agent": "partner"}]})
    script = model(tool_use(("apply_changes", {"summary": "Three generators in parallel",
                                               "version_name": "parallel generators", "ops": ops})),
                   says("The three generators run in parallel."))
    send(env, "Build three generators and run them in parallel")
    run_background(env)
    result = script.calls[1]["messages"][-1]["content"][0]["toolResult"]["content"][0]["json"]
    assert result["status"] == "applied", result
    assert draft(env)["workflow"]["steps"][1]["parallel"] == list(gens)


# --- 2. one tool per listed tool --------------------------------------------------------

def test_n_listed_tools_are_n_tools_never_one_catch_all(env, model):
    text = guide(env)
    for part in ("ONE TOOL PER LISTED TOOL", "create N tools, each with its own key",
                 "Never collapse them into one catch-all tool", "say exactly why in your reply"):
        assert part in text, part
    assert "collapsed into one catch-all tool" in env.handler.designer._review.SYSTEM
    save(env)
    script = model(says("Which of these should I build first?"))
    send(env, "Tools: a CSV parser, a PDF generator, a PPTX generator and a checker")
    run_background(env)
    assert "ONE TOOL PER LISTED TOOL" in script.calls[0]["system"][0]["text"]


# --- 3. file outputs --------------------------------------------------------------------

def test_files_are_the_document_tool_or_a_written_lambda_never_no_rendering_tool(env):
    text = guide(env)
    for part in ('the built-in Document generator, a tool {"type": "lambda", "source": "docgen", "formats"',
                 '"templateS3Uri"?', '"outputPrefix"?', "one per document the user names",
                 "generate_document", '"call": "generate_document", "arg": "data"', "`toolSchema`",
                 "optional strings `runId` and `agent`, which the workflow sets on every call",
                 "never on a harness agent",
                 "set orchestrator.files", 'an agent with `output: "image"`',
                 "a Lambda tool you write (5b) with its libraries pinned in requirements.txt",
                 "Never tell the user the framework has no rendering or file tool"):
        assert part in text, part


# --- 4. scope once ----------------------------------------------------------------------

def test_the_scope_is_said_in_the_first_reply_only(env, model):
    text = guide(env)
    for part in ("SAY THE SCOPE ONCE", "a deployable multi-agent workflow plus its tools",
                 "a custom UI, SDK or client code, unit tests, or a custom server",
                 "in your FIRST reply of the conversation",
                 'Later replies never repeat a "Not covered" list'):
        assert part in text, part
    save(env)
    script = model(says("AgentExpress builds the workflow and its tools, not a custom UI."),
                   says("Added."))
    send(env, "Build a claims workflow, a React UI for it and its unit tests")
    run_background(env)
    send(env, "Use a longer intake prompt")
    run_background(env)
    assert "This is your FIRST reply" in notes(script.calls[0])
    later = notes(script.calls[1])
    assert "This is NOT your first reply" in later and 'do not repeat a "Not covered" list' in later
    # Start over is a new conversation: the scope may be said again.
    call(env, "DELETE /api/builds/{id}/design", params={"id": "pclaims01"})
    script = model(says("Hello again."))
    send(env, "Build a claims workflow and its SDK")
    run_background(env)
    assert "This is your FIRST reply" in notes(script.calls[0])


# --- 5. no request ----------------------------------------------------------------------

def test_pasted_content_with_no_ask_gets_one_question_and_no_change(env, model):
    text = guide(env)
    for part in ("A MESSAGE WITH NO REQUEST", "change nothing: no apply_changes, no undo, no restore",
                 "Ask ONE short question", "Never guess the request"):
        assert part in text, part
    save(env)
    before = draft(env)
    script = model(says("Should I build a workflow from this spec, or change the current build to match it?"))
    send(env, "Columns: region, units, revenue\nRows: 1200\nSource: monthly export")
    run_background(env)
    assert "A MESSAGE WITH NO REQUEST" in script.calls[0]["system"][0]["text"]
    reply = conversation(env)[1]["turns"][-1]
    assert reply["status"] == "done" and not reply.get("changes")
    assert draft(env)["workflow"] == before["workflow"] and conversation(env)[1]["versions"] == []


# --- 6. versions ------------------------------------------------------------------------

def test_versions_are_named_listed_and_restored_and_undo_says_which(env, model):
    text = guide(env)
    for part in ("VERSIONS. Every change you save is a named version", "`version_name`",
                 "Before you undo or go back, say in one line which version it restores",
                 "`restore_version`"):
        assert part in text, part
    save(env)
    original = draft(env)["workflow"]
    longer = ("apply_changes", {"summary": "Longer intake", "version_name": "longer intake",
                                "ops": [{"op": "update_agent", "id": "claims_intake",
                                         "set": {"maxTokens": 2222}}]})
    script = model(
        tool_use(add_fraud(version_name="multi-agent draft")), says("Added."),
        tool_use(longer), says("Longer."),
        tool_use(("restore_version", {"version": 1}), text="This goes back to v1: multi-agent draft."),
        says("Back on v1."),
        tool_use(("undo_last_change", {})), says("Undone."),
        tool_use(("restore_version", {"version": 9})), says("There is no v9."))
    send(env, "Add a fraud check")
    run_background(env)
    send(env, "Give intake more room")
    run_background(env)
    doc = conversation(env)[1]
    assert [(v["v"], v["name"], v["current"]) for v in doc["versions"]] == [
        (0, "before the Assistant's first change", False), (1, "multi-agent draft", False),
        (2, "longer intake", True)]
    assert doc["version"] == 2 and doc["turns"][-1]["changes"][0]["versionName"] == "longer intake"

    send(env, "Go back to v1")
    run_background(env)
    # Before the change, the model was told the versions and what undo would put back.
    told = notes(script.calls[4])
    assert "v0: before the Assistant's first change; v1: multi-agent draft; v2: longer intake" in told
    assert "Current: v2: longer intake" in told and "undo_last_change would put back v1: multi-agent draft" in told
    result = script.calls[5]["messages"][-1]["content"][0]["toolResult"]["content"][0]["json"]
    assert result == {"status": "restored", "revision": 3, "restored": "v1: multi-agent draft"}
    wf = draft(env)["workflow"]
    assert "fraud_check" in wf["agents"] and wf["agents"]["claims_intake"]["maxTokens"] == 2000
    reply = conversation(env)[1]["turns"][-1]
    assert reply["text"].startswith("This goes back to v1") and reply["changes"][0]["summary"] == \
        "Went back to v1: multi-agent draft"
    assert conversation(env)[1]["version"] == 1

    # Undo takes the restore back, and says to which version.
    send(env, "Undo that")
    run_background(env)
    result = script.calls[7]["messages"][-1]["content"][0]["toolResult"]["content"][0]["json"]
    assert result["restored"] == "v2: longer intake"
    assert conversation(env)[1]["turns"][-1]["changes"][0]["summary"] == \
        "Undid the last change: back to v2: longer intake"
    assert draft(env)["workflow"]["agents"]["claims_intake"]["maxTokens"] == 2222

    # An unknown version is refused with the list; nothing changes.
    send(env, "Go back to v9")
    run_background(env)
    result = script.calls[9]["messages"][-1]["content"][0]["toolResult"]["content"][0]["json"]
    assert result["status"] == "error" and "v1: multi-agent draft" in result["error"]
    assert draft(env)["workflow"]["agents"]["claims_intake"]["maxTokens"] == 2222

    # Start over keeps them, and v0 is the build as it was before the first change.
    call(env, "DELETE /api/builds/{id}/design", params={"id": "pclaims01"})
    assert len(conversation(env)[1]["versions"]) == 3
    model(tool_use(("restore_version", {"version": "v0"})), says("Back to the start."))
    send(env, "Go back to v0")
    run_background(env)
    assert draft(env)["workflow"] == original


# --- 7. storage -------------------------------------------------------------------------

def test_a_tool_that_needs_s3_without_a_bucket_gets_build_storage(env):
    text = guide(env)
    for part in ("and the user named no bucket, do not stop to ask for one",
                 'set orchestrator.storage {"create": true}', 'the "${storage}" token',
                 '"${storage}/templates/report.docx"', 'List it under "Defaults used"',
                 "Ask for a bucket only when the user says their files are already in one"):
        assert part in text, part


def test_build_storage_offers_an_own_bucket_without_asking_and_never_asks_for_unnamed_targets(env):
    # Live replay of the brief: once storage was created, a change still listed "Deliver to the
    # build's own bucket (default), or to a bucket of yours?" in needs_input, and a reply
    # ended with "Should the delivery also send an email through SES?" for an S3-only brief.
    text = guide(env)
    for part in (("say they can name their own bucket instead, as a statement there: never a "
                  "question and never a `needs_input` item"),
                 'a file they must upload, such as a template, is one: say where, e.g. "${storage}/templates/"',
                 ("Never ask whether to add a kind of target the user did not mention (an email when "
                  "they said S3)")):
        assert part in text, part


# --- the delivery step and the operator's decision ---------------------------------------

def test_it_knows_the_delivery_step_and_named_gate_options(env):
    text = guide(env)
    for part in ('an agent with runtime "task" and a task of kind "deliver" with its `targets`',
                 '{"type": "s3", "uri": "${storage}/delivered/"}', "`askPerson` {\"options\": [...]}",
                 "the delivery stage comes AFTER that branch or gate", "so it also needs orchestrator.files",
                 "Email recipients are what the user configures, never the model's output",
                 '`hitl` {"options": ["Override", "Abort"]}', "the run field `decision`",
                 "With no choice made (the gate did not stop) the branch takes `default`",
                 ('`hitl` {"mode": "threshold", "when": [{"field": "<pass field>", "equals": false}], '
                  '"options": ["Override", "Abort"]}'),
                 '[{"field": "decision", "equals": "Abort", "goto": "END"}], "default": "<the deliver stage>"}',
                 "a pass or Override delivers, Abort ends"):
        assert part in text, part
    assert "delivery step placed before the check or gate" in env.handler.designer._review.SYSTEM
