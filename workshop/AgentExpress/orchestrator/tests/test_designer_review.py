"""Review changes (bff/review.py): a second model checks the Assistant's proposed change
before it is saved.
  * Auto decides on the CHANGE, never on the wording: a new build, agents or stages added
    or removed, a security-relevant setting, or more than LARGE_CHANGE items; a prompt
    tweak or a model swap is not reviewed;
  * one round: "ok" saves as proposed; "issues" gives the Assistant ONE revision with the
    notes as data, and that revision is saved without a second review;
  * a reviewer that fails, times out or answers non-JSON never blocks the change;
  * Off never calls the reviewer.
Both models are faked here.
"""
# ruff: noqa: F811 - `env` and `model` are fixtures, imported so every test can take them
from __future__ import annotations

import copy
import json

import pytest
import review
from botocore.exceptions import ReadTimeoutError
from test_builds import CTX, call, env, save  # noqa: F401
from test_designer import FRAUD, FRAUD_PROMPT, add_fraud, draft, model, run_background, says, tool_use  # noqa: F401

pytest.importorskip("moto")

BEFORE = {"name": "B", "workflow": {
    "agents": {"a": {"name": "A", "runtime": "main", "model": "m1", "tool": "kb"},
               "b": {"name": "B", "runtime": "main", "model": "m1"}},
    "tools": {"kb": {"type": "mcp", "endpoint": "https://kb.example.com/mcp"}},
    "steps": [{"agent": "a"}, {"agent": "b"}]},
    "prompts": {"a": {"systemPrompt": "You read.", "schema": "{}"}}}


def changed(fn):
    after = copy.deepcopy(BEFORE)
    fn(after)
    return after


# --- the Auto rule -------------------------------------------------------------------

def test_a_build_drafted_from_scratch_is_reviewed():
    empty = {"name": "New", "workflow": {"agents": {}, "steps": []}}
    assert review.needs_review(empty, BEFORE, [{"op": "set_agent"}]) == "new build"


def test_adding_or_removing_an_agent_is_reviewed():
    added = changed(lambda p: p["workflow"]["agents"].update(c={"name": "C", "runtime": "main"}))
    removed = changed(lambda p: p["workflow"]["agents"].pop("b"))
    assert review.needs_review(BEFORE, added, [{}]) == "agents added or removed"
    assert review.needs_review(BEFORE, removed, [{}]) == "agents added or removed"


def test_adding_or_removing_a_stage_is_reviewed():
    fewer = changed(lambda p: p["workflow"].update(steps=[{"parallel": ["a", "b"]}]))
    assert review.needs_review(BEFORE, fewer, [{}]) == "stages added or removed"


@pytest.mark.parametrize("edit", [
    lambda p: p["workflow"].update(guardrails={"strict": {"contentFilters": "HIGH"}}),
    lambda p: p["workflow"].update(identities={"crm": {"type": "oauth2"}}),
    lambda p: p["workflow"].update(policies={"readonly": {"statement": "permit(...)"}}),
    lambda p: p["workflow"]["agents"]["a"].update(network={"mode": "VPC", "subnetIds": ["s"]}),
    lambda p: p["workflow"]["agents"]["a"].update(agentcore={"guardrails": {"input": False}}),
    lambda p: p["workflow"]["tools"]["kb"].update(endpoint="https://elsewhere.example.com/mcp"),
    lambda p: p["workflow"]["tools"]["kb"].update(auth="apikey", identity="crm"),
    lambda p: p["workflow"]["tools"].update(fn={"type": "lambda", "code": {"codeSource": {"git": "x"}}}),
    lambda p: p.update(toolCode={"fn": {"handler.py": "print(1)"}}),
], ids=["guardrails", "identities", "policies", "network", "agent-guardrails", "tool-endpoint",
        "tool-auth", "new-tool-connection", "code"])
def test_a_security_relevant_change_is_reviewed(edit):
    assert review.needs_review(BEFORE, changed(edit), [{}])


def _set(path: str, value):
    """An edit that sets one dotted path in the workflow (creating the dicts on the way)."""
    def edit(p):
        node, *keys = p["workflow"], *path.split(".")
        for k in keys[:-1]:
            node = node.setdefault(k, {})
        node[keys[-1]] = value
    return edit


# One single edit per category on an existing agent, tool or block: secrets, code
# sources, IAM-ish capabilities, external connections, network, identities.
SECURITY_EDITS = {
    "harness-mcp": "agents.a.harness.mcp", "harness-apiBase": "agents.a.harness.apiBase",
    "harness-shell": "agents.a.harness.shell", "harness-container": "agents.a.harness.container",
    "harness-arn": "agents.a.harness.arn", "harness-browser": "agents.a.harness.browser",
    "harness-codeInterpreter": "agents.a.harness.codeInterpreter",
    "agent-tool": "agents.b.tool", "agent-agentCard": "agents.b.agentCard",
    "image-allowCrossRegion": "agents.a.image.allowCrossRegion",
    "vectorStore-secret": "tools.kb.vectorStore.credentialsSecretArn",
    "vectorStore-connectionString": "tools.kb.vectorStore.connectionString",
    "vectorStore-domainEndpoint": "tools.kb.vectorStore.domainEndpoint",
    "vectorStore-resourceArn": "tools.kb.vectorStore.resourceArn",
    "vectorStore-endpointServiceName": "tools.kb.vectorStore.endpointServiceName",
    "dataSource-hostUrl": "tools.kb.dataSource.hostUrl",
    "dataSource-authType": "tools.kb.dataSource.authType",
    "tool-allowDomains": "tools.kb.allowDomains", "tool-restApiId": "tools.kb.restApiId",
    "tool-schemaS3Uri": "tools.kb.schemaS3Uri", "tool-s3Uri": "tools.kb.s3Uri",
    "tool-kmsKeyArn": "tools.kb.kmsKeyArn", "tool-approval": "tools.kb.approval",
    "tool-policy": "tools.kb.policy.permit", "tool-code-env": "tools.kb.code.environment",
    "gatewayIdentity": "orchestrator.gatewayIdentity", "export": "orchestrator.export.endpoint",
    "attachments-s3": "orchestrator.attachments.s3", "tool-knowledgeBaseId": "tools.kb.knowledgeBaseId",
}


@pytest.mark.parametrize("path", SECURITY_EDITS.values(), ids=SECURITY_EDITS.keys())
def test_each_security_relevant_setting_is_reviewed(path):
    assert review.needs_review(BEFORE, changed(_set(path, "x")), [{}]).startswith("security setting")


def test_an_entry_named_like_a_security_key_is_not_one():
    named = copy.deepcopy(BEFORE)
    named["workflow"]["agents"]["secrets"] = named["workflow"]["agents"].pop("b")
    named["workflow"]["steps"][1] = {"agent": "secrets"}
    after = copy.deepcopy(named)
    after["workflow"]["agents"]["secrets"]["maxTokens"] = 900
    assert review.needs_review(named, after, [{}]) == ""


@pytest.mark.parametrize("block", ["memories", "evaluators", "skills"])
def test_adding_or_removing_a_block_is_reviewed(block):
    entry = {block: {"x": {"description": "d"}}}
    added = changed(lambda p: p["workflow"].update(entry))
    assert review.needs_review(BEFORE, added, [{}]) == "blocks added or removed"
    assert review.needs_review(added, BEFORE, [{}]) == "blocks added or removed"
    more = copy.deepcopy(added)
    more["workflow"][block]["y"] = {"description": "e"}
    assert review.needs_review(added, more, [{}]) == "blocks added or removed"


def test_the_block_lists_match_the_designers(env):
    d = env.handler.designer
    assert review.NAMED_MAPS == d.NAMED_MAPS
    assert set(review.SINGLE_BLOCKS) == set(d.BLOCKS) - set(d.NAMED_MAPS)


def test_a_change_to_more_than_n_items_or_ops_is_reviewed():
    many = {"name": "B", "workflow": {
        "agents": {f"a{i}": {"name": f"A{i}", "runtime": "main", "model": "m1"}
                   for i in range(review.LARGE_CHANGE + 1)},
        "steps": [{"parallel": [f"a{i}" for i in range(review.LARGE_CHANGE + 1)]}]}}
    swapped = copy.deepcopy(many)
    for a in swapped["workflow"]["agents"].values():
        a["model"] = "m2"
    assert review.needs_review(many, swapped, [{}]) == "large change"
    one = changed(lambda p: p["workflow"]["agents"]["a"].update(maxTokens=900))
    assert review.needs_review(BEFORE, one, [{}] * (review.LARGE_CHANGE + 1)) == "large change"
    assert review.needs_review(BEFORE, one, [{}] * review.LARGE_CHANGE) == ""


def test_a_prompt_tweak_or_a_model_swap_is_not_reviewed():
    tweak = changed(lambda p: p["prompts"]["a"].update(systemPrompt="You read carefully."))
    swap = changed(lambda p: p["workflow"]["agents"]["b"].update(model="m2"))
    assert review.needs_review(BEFORE, tweak, [{"op": "update_agent"}]) == ""
    assert review.needs_review(BEFORE, swap, [{"op": "update_agent"}]) == ""
    for path, value in (("agents.a.temperature", 0.2), ("agents.a.instructions", "Be brief."),
                        ("agents.a.models.fallback", ["m2"]), ("tools.kb.maxResults", 3)):
        assert review.needs_review(BEFORE, changed(_set(path, value)), [{}]) == "", path


def test_the_mode_is_off_auto_or_always_and_anything_else_is_auto():
    assert [review.mode_of(v) for v in ("off", "ALWAYS", "auto", None, "x")] == \
        ["off", "always", "auto", "auto", "auto"]


# --- what the reviewer is sent and how its answer is read --------------------------

def test_the_reviewer_gets_the_request_the_affected_parts_and_the_problems(monkeypatch):
    sent = {}

    class Client:
        def converse(self, **kw):
            sent.update(kw)
            return {"stopReason": "end_turn", "usage": {"inputTokens": 9, "outputTokens": 3},
                    "output": {"message": {"content": [
                        {"reasoningContent": {"reasoningText": {"text": "…"}}},
                        {"text": '```json\n{"verdict": "issues", "notes": ["Put c in a stage."]}\n```'}]}}}
    monkeypatch.setattr(review, "_client", Client())
    after = changed(lambda p: p["workflow"]["agents"].update(c={"name": "C", "runtime": "main"}))
    text = review.request_text("Add a checker", BEFORE, after, [{"op": "set_agent"}],
                               [{"severity": "error", "path": "agents.c", "message": "in no stage"}])
    assert review.call_reviewer(text) == {"verdict": "issues", "notes": ["Put c in a stage."]}
    assert sent["modelId"] == review.MODEL == "us.openai.gpt-6.1-sol"
    assert sent["inferenceConfig"] == {"maxTokens": review.MAX_TOKENS}     # no temperature
    body = sent["messages"][0]["content"][0]["text"]
    assert "Add a checker" in body and '"c"' in body and "in no stage" in body
    assert '"b"' not in body.split('"before"')[1].split('"steps"')[0]      # only the affected parts


@pytest.mark.parametrize("junk", ["", "no json here", '{"verdict": "maybe"}', "[1, 2]"])
def test_an_answer_that_is_not_the_expected_json_is_refused(junk):
    with pytest.raises(ValueError):
        review.parse(junk)


def test_issues_without_notes_is_ok_and_notes_are_capped():
    assert review.parse('{"verdict": "issues", "notes": []}') == {"verdict": "ok", "notes": []}
    many = review.parse(json.dumps({"verdict": "issues", "notes": ["x"] * 20}))
    assert len(many["notes"]) == review.MAX_NOTES


# --- the flow in a turn ----------------------------------------------------------------

def send(e, message, mode):
    return call(e, "POST /api/builds/{id}/design", params={"id": "pclaims01"},
                body={"message": message, "review": mode})


@pytest.fixture()
def reviewer(env, monkeypatch):
    """The reviewer, faked: each call returns the next answer (or raises it)."""
    def use(*answers):
        seen: list[str] = []
        queue = list(answers)

        def fake(text):
            seen.append(text)
            a = queue.pop(0)
            if isinstance(a, Exception):
                raise a
            return a
        monkeypatch.setattr(env.handler.designer._review, "call_reviewer", fake)
        return seen
    return use


def last_reply(e):
    return call(e, "GET /api/builds/{id}/design", params={"id": "pclaims01"})[1]["turns"][-1]


def tool_result(script, n):
    return script.calls[n]["messages"][-1]["content"][0]["toolResult"]["content"][0]["json"]


def test_ok_saves_the_change_as_proposed_with_no_revision(env, model, reviewer):
    save(env)
    script = model(tool_use(add_fraud()), says("Added it."))
    seen = reviewer({"verdict": "ok", "notes": []})
    send(env, "Add a fraud check after intake", "auto")
    run_background(env)
    assert len(seen) == 1 and "Add a fraud check after intake" in seen[0]
    assert len(script.calls) == 2 and tool_result(script, 1)["status"] == "applied"
    reply = last_reply(env)
    assert reply["review"] == {"status": "ok"} and [c["revision"] for c in reply["changes"]] == [1]
    assert "fraud_check" in draft(env)["workflow"]["agents"]


def test_issues_give_one_revision_and_no_second_review(env, model, reviewer):
    save(env)
    revised = add_fraud()
    revised[1]["ops"][0]["agent"] = {**FRAUD, "maxTokens": 2500}
    script = model(tool_use(add_fraud()), tool_use(revised), says("Added it, with more room."))
    seen = reviewer({"verdict": "issues", "notes": ["Give fraud_check more maxTokens."]})
    send(env, "Add a fraud check after intake", "always")
    run_background(env)
    assert len(seen) == 1                              # the revision was not reviewed again
    held = tool_result(script, 1)
    assert held["status"] == "review" and held["applied"] is False
    assert held["review_notes"] == ["Give fraud_check more maxTokens."]
    assert tool_result(script, 2)["status"] == "applied"
    reply = last_reply(env)
    assert reply["review"] == {"status": "applied", "count": 1}
    assert [c["revision"] for c in reply["changes"]] == [1]      # saved once, as revised
    assert draft(env)["workflow"]["agents"]["fraud_check"]["maxTokens"] == 2500
    assert draft(env)["prompts"]["fraud_check"] == FRAUD_PROMPT


def test_notes_the_assistant_does_not_take_are_noted_and_the_change_still_saved(env, model, reviewer):
    save(env)
    # It re-sends the same change, then a reply that never re-sends at all.
    script = model(tool_use(add_fraud()), tool_use(add_fraud()), says("Kept it as it was."))
    reviewer({"verdict": "issues", "notes": ["Maybe rename it.", "Maybe a guardrail."]})
    send(env, "Add a fraud check after intake", "auto")
    run_background(env)
    assert len(script.calls) == 3
    assert last_reply(env)["review"] == {"status": "noted", "count": 2}
    assert "fraud_check" in draft(env)["workflow"]["agents"]


def test_a_held_change_the_assistant_never_resends_is_saved_as_proposed(env, model, reviewer):
    save(env)
    model(tool_use(add_fraud()), says("I'll leave it as it is."))
    reviewer({"verdict": "issues", "notes": ["Maybe rename it."]})
    send(env, "Add a fraud check after intake", "auto")
    run_background(env)
    reply = last_reply(env)
    assert reply["review"] == {"status": "noted", "count": 1} and len(reply["changes"]) == 1
    assert "fraud_check" in draft(env)["workflow"]["agents"]


@pytest.mark.parametrize("failure", [RuntimeError("AccessDeniedException"),
                                     ReadTimeoutError(endpoint_url="https://bedrock-runtime"),
                                     "not json"], ids=["error", "timeout", "invalid-json"])
def test_a_reviewer_failure_never_blocks_the_change(env, model, reviewer, failure):
    save(env)
    script = model(tool_use(add_fraud()), says("Added it."))
    if failure == "not json":
        failure = ValueError("no JSON object")         # what review.parse raises
    reviewer(failure)
    send(env, "Add a fraud check after intake", "always")
    run_background(env)
    assert tool_result(script, 1)["status"] == "applied"
    reply = last_reply(env)
    assert reply["status"] == "done" and reply["review"] == {"status": "unavailable"}
    assert "fraud_check" in draft(env)["workflow"]["agents"]


def test_too_late_in_the_invocation_the_review_is_skipped_and_the_change_saved(env, model, reviewer,
                                                                              monkeypatch):
    """A review that could outlast the function's timeout is not started: the change is
    saved before the invocation ends rather than lost with it."""
    designer = env.handler.designer
    save(env)
    script = model(tool_use(add_fraud()), says("Added it."))
    seen = reviewer()                                 # any call would fail the test
    monkeypatch.setattr(designer, "_continue", lambda ev: None)
    now = [0.0]
    stream = script.converse_stream

    def slow(**kw):
        out = stream(**kw)
        # The model call ended late: less than a review's worst case is left.
        now[0] = review.INVOCATION_S - review.TIMEOUT_S
        return out
    monkeypatch.setattr(script, "converse_stream", slow)
    monkeypatch.setattr(designer.time, "monotonic", lambda: now[0])
    send(env, "Add a fraud check after intake", "always")
    # Past the turn budget too, so the rest of the turn goes on in a fresh invocation.
    assert run_background(env) == {"ok": True, "continued": 1}
    assert seen == []
    reply = last_reply(env)
    assert reply["review"] == {"status": "unavailable"} and len(reply["changes"]) == 1
    assert "fraud_check" in draft(env)["workflow"]["agents"]


def test_notes_back_after_the_turn_budget_reach_the_assistant_in_the_continuation(env, model, reviewer,
                                                                                 monkeypatch):
    """The turn runs out of time while the change is held for its revision: the next
    invocation gets the notes and the proposal, and its apply_changes is the one revision."""
    designer = env.handler.designer
    save(env)
    revised = add_fraud()
    revised[1]["ops"][0]["agent"] = {**FRAUD, "maxTokens": 2500}
    script = model(tool_use(add_fraud()), tool_use(revised), says("Added it, with more room."))
    continued: list[dict] = []
    monkeypatch.setattr(designer, "_continue", lambda ev: continued.append(ev))
    now = [0.0]

    def late(text):
        now[0] = designer.TURN_BUDGET_S + 1          # the notes come back past the budget
        return {"verdict": "issues", "notes": ["Give fraud_check more maxTokens."]}
    monkeypatch.setattr(designer._review, "call_reviewer", late)
    monkeypatch.setattr(designer.time, "monotonic", lambda: now[0])
    send(env, "Add a fraud check after intake", "always")
    assert run_background(env) == {"ok": True, "continued": 1}
    assert "fraud_check" not in draft(env)["workflow"]["agents"]       # held, not saved

    seen = reviewer()                                  # any call would fail the test
    now[0] = 0.0
    assert env.handler.handler(continued[0], CTX) == {"ok": True}
    told = json.dumps(script.calls[1]["messages"][-1])
    assert "Give fraud_check more maxTokens." in told and "fraud_check" in told
    reply = last_reply(env)
    assert reply["review"] == {"status": "applied", "count": 1} and len(reply["changes"]) == 1
    assert draft(env)["workflow"]["agents"]["fraud_check"]["maxTokens"] == 2500
    assert seen == []


def test_a_second_apply_changes_in_the_same_reply_is_not_the_revision(env, model, reviewer):
    save(env)
    revised = add_fraud()
    revised[1]["ops"][0]["agent"] = {**FRAUD, "maxTokens": 2500}
    script = model(tool_use(add_fraud(), add_fraud()), tool_use(revised), says("Done."))
    seen = reviewer({"verdict": "issues", "notes": ["Give fraud_check more maxTokens."]})
    send(env, "Add a fraud check after intake", "always")
    run_background(env)
    results = [r["toolResult"]["content"][0]["json"]
               for r in script.calls[1]["messages"][-1]["content"] if "toolResult" in r]
    assert [r["status"] for r in results] == ["review", "review"]
    assert results[0]["review_notes"] and "review_notes" not in results[1]
    assert len(seen) == 1 and tool_result(script, 2)["status"] == "applied"
    reply = last_reply(env)
    assert reply["review"] == {"status": "applied", "count": 1} and len(reply["changes"]) == 1
    assert draft(env)["workflow"]["agents"]["fraud_check"]["maxTokens"] == 2500


def test_off_never_calls_the_reviewer_and_shows_nothing(env, model, reviewer):
    save(env)
    script = model(tool_use(add_fraud()), says("Added it."))
    seen = reviewer()
    send(env, "Add a fraud check after intake", "off")
    run_background(env)
    assert seen == [] and len(script.calls) == 2
    assert "review" not in last_reply(env)
    assert "fraud_check" in draft(env)["workflow"]["agents"]


def test_auto_skips_a_small_change(env, model, reviewer):
    save(env)
    small = ("apply_changes", {"summary": "More room for intake", "ops": [
        {"op": "update_agent", "id": "claims_intake", "set": {"maxTokens": 3000}}]})
    model(tool_use(small), says("Done."))
    seen = reviewer()
    send(env, "Give intake more room", "auto")
    run_background(env)
    assert seen == [] and last_reply(env)["review"] == {"status": "skipped"}
    assert draft(env)["workflow"]["agents"]["claims_intake"]["maxTokens"] == 3000
