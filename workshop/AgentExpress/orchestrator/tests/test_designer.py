"""AgentExpress Assistant (bff/designer.py): a conversation that edits the build it belongs to.

What has to hold, whatever the model says:
  * it changes the build only through targeted edits, applied to the LATEST draft — so
    an agent the user edited by hand while the model was thinking keeps that edit;
  * every edit is checked by the Build view's own rules, and the model is made to fix
    what fails; only a value the user must give may stay open, and then deploy refuses;
  * each change can be undone; the conversation and the build stay the owner's alone;
  * a turn runs in the background, once, and always ends — reply or error.
The model is scripted here; tests/test_designer_live.py (opt-in) talks to the real one.
"""

# ruff: noqa: F811 - `env` is the builds fixture, imported so every test can take it
from __future__ import annotations

import copy
import json

import pytest
from test_builds import CTX, call, env, project, save  # noqa: F401

moto = pytest.importorskip("moto")


class Script:
    """A fake Bedrock: each converse() returns the next scripted reply and records the
    request, so a test can see what the model was told."""

    def __init__(self, *replies):
        self.replies = list(replies)
        self.calls: list[dict] = []
        self.on_event = None                      # a test can look at the page mid-reply

    summaries: list[dict]

    def converse(self, **kw):
        """Only the running summary calls plain Converse (the turn itself streams)."""
        self.__dict__.setdefault("summaries", []).append(copy.deepcopy(kw))
        n = len(self.summaries)
        return {"output": {"message": {"role": "assistant", "content": [
            {"text": f"- Goal: claims triage (summary {n})"}]}}}

    def converse_stream(self, **kw):
        self.calls.append(copy.deepcopy(kw))      # the caller keeps appending to messages
        r = self.replies.pop(0)
        if isinstance(r, Exception):
            raise r
        return {"stream": self._events(r)}

    def _events(self, r):
        """The scripted reply as ConverseStream sends it: text in small pieces, tool
        input as JSON fragments."""
        yield {"messageStart": {"role": "assistant"}}
        for i, block in enumerate(r["output"]["message"]["content"]):
            if "text" in block:
                t = block["text"]
                for j in range(0, len(t), 7):
                    yield {"contentBlockDelta": {"contentBlockIndex": i,
                                                 "delta": {"text": t[j:j + 7]}}}
                    if self.on_event:
                        self.on_event()
            elif "reasoningContent" in block:
                rt = block["reasoningContent"]["reasoningText"]
                yield {"contentBlockDelta": {"contentBlockIndex": i, "delta": {
                    "reasoningContent": {"text": rt["text"]}}}}
                yield {"contentBlockDelta": {"contentBlockIndex": i, "delta": {
                    "reasoningContent": {"signature": rt["signature"]}}}}
            else:
                use = block["toolUse"]
                yield {"contentBlockStart": {"contentBlockIndex": i, "start": {
                    "toolUse": {"toolUseId": use["toolUseId"], "name": use["name"]}}}}
                raw = json.dumps(use["input"])
                for j in range(0, len(raw), 40):
                    yield {"contentBlockDelta": {"contentBlockIndex": i,
                                                 "delta": {"toolUse": {"input": raw[j:j + 40]}}}}
            yield {"contentBlockStop": {"contentBlockIndex": i}}
        yield {"messageStop": {"stopReason": r["stopReason"]}}
        yield {"metadata": {"usage": {}}}


def tool_use(*uses, text=""):
    content = ([{"text": text}] if text else []) + [
        {"toolUse": {"toolUseId": f"t{i}", "name": name, "input": args}}
        for i, (name, args) in enumerate(uses)]
    return {"stopReason": "tool_use", "output": {"message": {"role": "assistant", "content": content}}}


def says(text):
    return {"stopReason": "end_turn",
            "output": {"message": {"role": "assistant", "content": [{"text": text}]}}}


FRAUD = {"name": "Fraud Check", "runtime": "main", "produces": "fraud-flag", "maxTokens": 1500,
         "agentcore": {"guardrails": {"input": True, "output": True}}}
FRAUD_PROMPT = {"systemPrompt": "You check a claim for fraud signals.",
                "schema": '{"suspicious": false, "reasons": []}'}


def add_fraud(**extra):
    return ("apply_changes", {
        "summary": "Added a fraud check after intake",
        "ops": [{"op": "set_agent", "id": "fraud_check", "agent": FRAUD, "prompt": FRAUD_PROMPT},
                {"op": "set_steps", "steps": [{"agent": "claims_intake", "hitl": True},
                                              {"agent": "fraud_check"}, {"agent": "partner"}]}],
        "defaults_used": ["maxTokens 1500", "guardrails on for input and output"], **extra})


def send(e, message, bid="pclaims01", sub="u1"):
    return call(e, "POST /api/builds/{id}/design", params={"id": bid}, body={"message": message},
                sub=sub)


def run_background(e):
    """What Lambda does with the self-invocation the POST queued."""
    payload = e.invoked.pop()
    assert payload["action"] == "design"
    return e.handler.handler(payload, CTX)


def draft(e, bid="pclaims01"):
    return call(e, "GET /api/builds/{id}", params={"id": bid})[1]["project"]


def conversation(e, bid="pclaims01", sub="u1"):
    return call(e, "GET /api/builds/{id}/design", params={"id": bid}, sub=sub)


@pytest.fixture()
def model(env, monkeypatch):
    def use(*replies):
        s = Script(*replies)
        monkeypatch.setattr(env.handler.designer, "_bedrock", s)
        monkeypatch.setattr(env.handler.designer, "_model_ids",
                            lambda: ["us.anthropic.claude-haiku-4-5-20251001-v1:0"])
        return s
    return use


def test_a_turn_runs_in_the_background_and_edits_the_build(env, model):
    save(env)
    script = model(tool_use(add_fraud()), says("Added **Fraud Check** after intake."))
    status, doc = send(env, "Add a fraud check after intake")
    assert status == 202 and doc["status"] == "thinking"
    assert doc["turns"][-1]["status"] == "thinking" and doc["model"].endswith("claude-sonnet-5-5")
    assert run_background(env) == {"ok": True}

    wf = draft(env)["workflow"]
    assert wf["agents"]["fraud_check"] == FRAUD
    assert [s.get("agent") for s in wf["steps"]] == ["claims_intake", "fraud_check", "partner"]
    assert draft(env)["prompts"]["fraud_check"] == FRAUD_PROMPT
    doc = conversation(env)[1]
    reply = doc["turns"][-1]
    assert doc["status"] == "idle" and reply["status"] == "done"
    assert reply["text"] == "Added **Fraud Check** after intake."
    change = reply["changes"][0]
    assert change["summary"] == "Added a fraud check after intake" and change["revision"] == 1
    assert change["changed"]["agents"] == ["fraud_check"] and change["changed"]["steps"] is True
    assert change["defaultsUsed"] == ["maxTokens 1500", "guardrails on for input and output"]
    # The model was given the framework's key spec, cached, and the build as it is now.
    system = script.calls[0]["system"]
    assert "`maxTokens`" in system[0]["text"] and system[1] == {"cachePoint": {"type": "default"}}
    context = script.calls[0]["messages"][-1]["content"]
    assert '"claims_intake"' in context[0]["text"] and context[1]["text"] == "Add a fraud check after intake"
    assert "inferenceConfig" in script.calls[0] and "temperature" not in script.calls[0]["inferenceConfig"]


def test_an_edit_that_breaks_the_rules_goes_back_to_the_model_to_fix(env, model):
    save(env)
    broken = ("apply_changes", {"summary": "Added a fraud check", "ops": [
        {"op": "set_agent", "id": "fraud_check", "agent": FRAUD, "prompt": FRAUD_PROMPT}]})
    script = model(tool_use(broken), tool_use(add_fraud()), says("Done."))
    send(env, "Add a fraud check")
    run_background(env)
    # The first attempt left fraud_check in no stage: rejected, nothing applied...
    first = script.calls[1]["messages"][-1]["content"][0]["toolResult"]["content"][0]["json"]
    assert first["status"] == "rejected" and first["applied"] is False
    assert any(e["path"] == "agents.fraud_check" for e in first["errors"])
    # ...and the corrected one landed, as ONE change.
    reply = conversation(env)[1]["turns"][-1]
    assert [c["revision"] for c in reply["changes"]] == [1]
    assert draft(env)["workflow"]["steps"][1] == {"agent": "fraud_check"}
    # The applied result says exactly what it saved, so the reply cannot claim more:
    # observed live, a rejected call carried one edit, the resent one only another,
    # and the reply said both were done.
    second = script.calls[2]["messages"][-1]["content"][0]["toolResult"]["content"][0]["json"]
    assert second["status"] == "applied" and second["changed"]["agents"] == ["fraud_check"]
    assert "NOT applied" in second["check"]


def test_a_value_only_the_user_can_give_is_asked_for_and_blocks_the_deploy(env, model):
    save(env)
    crm = ("apply_changes", {
        "summary": "Added a CRM lookup", "ops": [
            {"op": "set_tool", "key": "crm", "tool": {"type": "mcp", "description": "CRM"}},
            {"op": "update_agent", "id": "claims_intake", "set": {"tool": "crm"}}],
        "needs_input": [{"path": "tools.crm", "question": "What is your CRM's MCP endpoint?"}]})
    model(tool_use(crm), says("I need your CRM endpoint."))
    send(env, "Look customers up in our CRM")
    run_background(env)
    change = conversation(env)[1]["turns"][-1]["changes"][0]
    assert change["needsInput"] == [{"path": "tools.crm", "question": "What is your CRM's MCP endpoint?"}]
    assert any(p["path"].startswith("tools.crm") for p in change["problems"])
    assert draft(env)["workflow"]["tools"]["crm"]["type"] == "mcp"
    status, body = call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
                        body={"tool": "cdk"})
    assert status == 400 and "tools.crm" in body["error"]


def test_it_edits_the_latest_draft_so_a_manual_edit_made_meanwhile_survives(env, model):
    save(env)
    model(tool_use(add_fraud()), says("Done."))
    send(env, "Add a fraud check")
    # While the model thinks, the user retunes intake by hand in the Build view.
    manual = draft(env)
    manual["workflow"]["agents"]["claims_intake"]["maxTokens"] = 3333
    call(env, "PUT /api/builds/{id}", params={"id": "pclaims01"}, body={"project": manual})
    run_background(env)
    wf = draft(env)["workflow"]
    assert wf["agents"]["claims_intake"]["maxTokens"] == 3333 and "fraud_check" in wf["agents"]


def test_a_secret_is_asked_for_in_the_secure_field_never_in_the_chat(env, model):
    save(env)
    crm = ("apply_changes", {
        "summary": "Added the CRM API", "ops": [
            {"op": "set_tool", "key": "crm", "tool": {"type": "mcp", "description": "CRM",
                                                      "endpoint": "https://crm.example.com/mcp",
                                                      "auth": "apikey"}},
            {"op": "update_agent", "id": "claims_intake", "set": {"tool": "crm"}}],
        "secrets_needed": [{"tool": "crm", "why": "The CRM's API key"},
                           {"tool": "crm", "why": "twice"},
                           {"tool": "nosuch", "why": "a tool that does not exist"},
                           {"agent": "claims_intake", "why": "not a remote agent"},
                           {"agent": "partner", "why": "The partner's token"}]})
    script = model(tool_use(crm), says("Enter the CRM key in the secure field."))
    send(env, "Look customers up in our CRM")
    run_background(env)
    change = conversation(env)[1]["turns"][-1]["changes"][0]
    assert change["secretsNeeded"] == [
        {"kind": "toolApiKeys", "name": "crm", "why": "The CRM's API key", "label": "API key"},
        {"kind": "a2aTokens", "name": "partner", "why": "The partner's token", "label": "Bearer token"}]
    guide = script.calls[0]["system"][0]["text"]
    assert "SECRETS NEVER GO THROUGH THE CHAT" in guide and "apigateway" in guide


def test_the_last_change_can_be_undone_by_asking(env, model):
    save(env)
    before = draft(env)
    model(tool_use(add_fraud()), says("Added."), tool_use(("undo_last_change", {})), says("Undone."))
    send(env, "Add a fraud check")
    run_background(env)
    send(env, "Undo that")
    run_background(env)
    after = draft(env)
    assert after["workflow"] == before["workflow"] and after["prompts"] == before["prompts"]
    reply = conversation(env)[1]["turns"][-1]
    assert reply["changes"][0]["undo"] is True and conversation(env)[1]["revision"] == 0


def test_the_conversation_is_the_owners_alone_and_one_turn_at_a_time(env, model):
    save(env)
    model(says("Hello"))
    assert conversation(env, sub="u2")[0] == 404
    assert send(env, "hi", sub="u2")[0] == 404
    assert send(env, "hi")[0] == 202
    assert send(env, "again")[0] == 409                      # still replying
    assert send(env, "")[0] == 400
    assert call(env, "DELETE /api/builds/{id}/design", params={"id": "pclaims01"})[0] == 409


def test_a_turn_always_ends_and_a_redelivered_turn_does_not_run_twice(env, model):
    save(env)
    model(RuntimeError("boom"))
    send(env, "hi")
    payload = env.invoked[-1]
    run_background(env)
    reply = conversation(env)[1]["turns"][-1]
    assert reply["status"] == "error" and "try again" in reply["text"]
    assert conversation(env)[1]["status"] == "idle"
    assert env.handler.handler(payload, CTX) == {"ok": True, "skipped": True}


def test_a_turn_that_was_cut_off_is_reported_not_retried(env, model):
    save(env)
    model(says("never used"))
    send(env, "hi")
    payload = env.invoked.pop()
    doc = env.handler.designer._load("pclaims01")
    doc["attempts"] = 1                                     # the first delivery died mid-turn
    env.handler.designer._store("pclaims01", doc)
    assert env.handler.handler(payload, CTX) == {"ok": False}
    assert "interrupted" in conversation(env)[1]["turns"][-1]["text"]


def test_the_reply_streams_into_the_conversation_as_it_is_written(env, model, monkeypatch):
    save(env)
    monkeypatch.setattr(env.handler.designer._Live, "FLUSH_S", 0)
    long = "Here is the plan. " * 6
    script = model(tool_use(add_fraud(), text="I'll add a fraud check."), says(long.strip()))
    seen: list[tuple[str, str]] = []

    def look():
        reply = conversation(env)[1]["turns"][-1]
        seen.append((reply.get("phase"), reply["text"]))
    script.on_event = look
    send(env, "Add a fraud check")
    run_background(env)
    texts = [t for _p, t in seen]
    # The page saw the text grow a piece at a time, in the writing phase...
    assert texts[0] == "I'll ad" and len(set(texts)) > 10
    assert {p for p, _t in seen} == {"writing"}
    # ...and the final reply keeps both things it said, in order, with no phase left over.
    reply = conversation(env)[1]["turns"][-1]
    assert reply["text"] == "I'll add a fraud check.\n\n" + long.strip()
    assert all(reply["text"].startswith(t) for t in texts)
    assert reply["status"] == "done" and "phase" not in reply
    assert "fraud_check" in draft(env)["workflow"]["agents"]      # tool input survived streaming


def test_a_reasoning_block_goes_back_to_the_model_with_its_signature(env, model):
    save(env)
    thought = {"reasoningContent": {"reasoningText": {"text": "They want fraud.", "signature": "sig1"}}}
    first = tool_use(add_fraud())
    first["output"]["message"]["content"].insert(0, thought)
    script = model(first, says("Done."))
    send(env, "Add a fraud check")
    run_background(env)
    replayed = script.calls[1]["messages"][-2]["content"]
    assert replayed[0] == thought and replayed[1]["toolUse"]["input"]["summary"]
    assert "They want fraud" not in conversation(env)[1]["turns"][-1]["text"]


def test_a_throttled_call_is_retried_before_anything_was_written(env, model, monkeypatch):
    save(env)
    monkeypatch.setattr(env.handler.designer.time, "sleep", lambda s: None)
    model(RuntimeError("ThrottlingException: slow down"), says("Hello."))
    send(env, "hi")
    run_background(env)
    reply = conversation(env)[1]["turns"][-1]
    assert reply["status"] == "done" and reply["text"] == "Hello."


def test_a_read_timeout_is_retried_and_the_client_waits_long_enough(env, model, monkeypatch):
    """Live: a long streamed reply paused past boto3's 60 s default and the turn failed
    with ReadTimeoutError. The client now waits READ_TIMEOUT_S, and one early timeout is
    retried like a throttle."""
    d = env.handler.designer
    assert d.READ_TIMEOUT_S >= 120
    assert d._bedrock.meta.config.read_timeout == d.READ_TIMEOUT_S
    save(env)
    monkeypatch.setattr(d.time, "sleep", lambda s: None)
    model(RuntimeError("ReadTimeoutError: Read timed out."), says("Hello."))
    send(env, "hi")
    run_background(env)
    reply = conversation(env)[1]["turns"][-1]
    assert reply["status"] == "done" and reply["text"] == "Hello."


def test_a_long_conversation_keeps_its_early_context_as_a_running_summary(env, model):
    save(env)
    d = env.handler.designer
    total = d.HISTORY_TURNS + d.SUMMARY_BATCH + 4          # turns (user + reply) before the next
    doc = d._load("pclaims01")
    for i in range(total // 2):
        doc["turns"] += [{"id": f"uold{i}", "role": "user", "text": f"old message {i}", "at": ""},
                         {"id": f"old{i}", "role": "assistant", "text": f"old reply {i}", "at": "",
                          "status": "done"}]
    d._store("pclaims01", doc)
    script = model(says("Carrying on."))
    send(env, "Now add a fraud check")
    run_background(env)
    # One summary call, with a small model, over exactly the turns that no longer fit.
    assert len(script.summaries) == 1
    ask = script.summaries[0]
    assert ask["modelId"] == d.SUMMARY_MODEL and "haiku" in ask["modelId"]
    folded = total - d.HISTORY_TURNS
    prompt = ask["messages"][0]["content"][0]["text"]
    assert "old message 0" in prompt and f"old reply {folded // 2 - 1}" in prompt
    assert f"old message {folded // 2}" not in prompt
    # The turn gets the summary, then only the newer turns verbatim.
    sent = script.calls[0]["messages"]
    verbatim = " ".join(m["content"][0]["text"] for m in sent[:-1])
    assert "old message 0" not in verbatim and f"old message {folded // 2}" in verbatim
    assert sent[-1]["content"][0]["text"].startswith("Summary of the earlier part")
    assert "summary 1" in sent[-1]["content"][0]["text"]
    assert conversation(env)[1]["summarized"] == folded
    # The next message does not summarise again: nothing new has fallen out yet.
    model(says("Sure."))
    send(env, "And a report")
    run_background(env)
    assert d._load("pclaims01")["summary"]["through"] == folded


def test_a_failed_summary_leaves_the_turns_verbatim(env, model, monkeypatch):
    save(env)
    d = env.handler.designer
    doc = d._load("pclaims01")
    for i in range(d.HISTORY_TURNS + d.SUMMARY_BATCH + 2):
        doc["turns"].append({"id": f"x{i}", "role": "user" if i % 2 == 0 else "assistant",
                             "text": f"t{i}", "at": ""})
    d._store("pclaims01", doc)
    script = model(says("ok"))
    monkeypatch.setattr(script, "converse", lambda **kw: (_ for _ in ()).throw(RuntimeError("down")))
    send(env, "hi")
    run_background(env)
    assert "summary" not in d._load("pclaims01")
    assert conversation(env)[1]["turns"][-1]["status"] == "done"


def test_starting_over_keeps_the_build_and_its_undo_history(env, model):
    save(env)
    model(tool_use(add_fraud()), says("Added."))
    send(env, "Add a fraud check")
    run_background(env)
    status, doc = call(env, "DELETE /api/builds/{id}/design", params={"id": "pclaims01"})
    assert status == 200 and doc["turns"] == [] and doc["revision"] == 1
    assert "fraud_check" in draft(env)["workflow"]["agents"]
    assert [s["title"] for s in doc["starters"]][-1] == "Explain this build"


# --- the edit operations, directly ------------------------------------------------------

def ops(env):
    return env.handler.designer.apply_ops


BASE = {"name": "B", "workflow": {
    "agents": {"a": {"name": "A", "runtime": "main", "maxTokens": 1, "tool": ["kb", "db"]},
               "b": {"name": "B", "runtime": "main", "maxTokens": 1},
               "c": {"name": "C", "runtime": "main", "maxTokens": 1}},
    "tools": {"kb": {"type": "kb"}, "db": {"type": "mcp"}},
    "steps": [{"agent": "a", "branch": {"default": "c"}},
              {"parallel": ["b", "c"], "hitl": True, "gateId": "g"}]},
    "prompts": {"b": {"systemPrompt": "x", "schema": "{}"}}}


def test_removing_an_agent_takes_it_out_of_its_stage_and_a_group_of_one_becomes_a_stage(env):
    new, changed = ops(env)(BASE, [{"op": "remove_agent", "id": "b"}])
    assert new["workflow"]["steps"][1] == {"agent": "c", "hitl": True}
    assert "b" not in new["prompts"] and changed["removed"] == ["b"]
    assert "b" in BASE["workflow"]["agents"]                 # the input is not mutated


def test_renaming_an_agent_follows_every_reference(env):
    new, _ = ops(env)(BASE, [{"op": "rename_agent", "id": "c", "to": "checker"}])
    assert new["workflow"]["steps"][0]["branch"]["default"] == "checker"
    assert new["workflow"]["steps"][1]["parallel"] == ["b", "checker"]


def test_removing_a_tool_unbinds_it_and_merging_a_block_keeps_the_rest(env):
    new, changed = ops(env)(BASE, [{"op": "remove_tool", "key": "kb"},
                                   {"op": "set_block", "name": "ui", "value": {"title": "T"}}])
    assert new["workflow"]["agents"]["a"]["tool"] == "db" and changed["agents"] == ["a"]
    new2, _ = ops(env)(new, [{"op": "set_block", "name": "ui", "value": {"heading": "H"}}])
    assert new2["workflow"]["ui"] == {"title": "T", "heading": "H"}


@pytest.mark.parametrize("bad,why", [
    ([], "non-empty"),
    ([{"op": "set_agent", "id": "has-hyphen", "agent": {}}], "invalid agent id"),
    ([{"op": "update_agent", "id": "nope", "set": {}}], "no agent"),
    ([{"op": "set_tool", "key": "bad_key", "tool": {}}], "invalid tool key"),
    ([{"op": "set_block", "name": "agents", "value": {}}], "must be one of"),
    ([{"op": "deploy"}], "unknown op"),
])
def test_an_edit_that_cannot_apply_is_named(env, bad, why):
    with pytest.raises(env.handler.designer.OpError, match=why):
        ops(env)(BASE, bad)


def test_the_spec_the_model_reads_is_every_key_the_framework_has(env):
    designer = env.handler.designer
    text = designer.system_prompt()[0]["text"]
    for block, spec in designer.validate_build.KEYS.items():
        assert f"### `{block}`" in text
        for key in spec["keys"]:
            assert f"`{key}`" in text, f"{block}.{key} is missing from the designer's spec"
    assert json.dumps(designer.TOOLS)                        # a valid Converse toolConfig


# --- attachments: files and s3:// paths a message brings -------------------------------

def _upload(e, name, body, bid="pclaims01"):
    """What the page does: ask for a presigned POST, then put the file where it says."""
    status, post = call(e, "POST /api/builds/{id}/design/attachments", params={"id": bid},
                        body={"name": name})
    assert status == 200 and post["key"].startswith(f"builds/{bid}/attachments/")
    e.s3.put_object(Bucket="console-builds", Key=post["key"], Body=body)
    return {"key": post["key"], "name": post["name"]}


def test_an_uploaded_file_goes_to_the_model_with_its_message_and_is_named_later(env, model):
    save(env)
    script = model(says("Read it."), says("Still here."))
    spec = _upload(env, "claims spec (v2).md", b"# Claims\nTriage within 24 hours.")
    status, doc = call(env, "POST /api/builds/{id}/design", params={"id": "pclaims01"},
                       body={"message": "Use this spec", "attachments": [spec]})
    assert status == 202 and doc["turns"][-2]["attachments"][0]["name"] == "claims spec (v2).md"
    run_background(env)
    sent = script.calls[0]["messages"][-1]["content"]
    doc_block = next(b["document"] for b in sent if "document" in b)
    assert doc_block["format"] == "md" and doc_block["name"] == "claims spec (v2)"
    assert doc_block["source"]["bytes"] == b"# Claims\nTriage within 24 hours."
    # The next turn names it but does not send it again.
    send(env, "and now?")
    run_background(env)
    later = script.calls[1]["messages"]
    assert not any("document" in b for m in later for b in m["content"])
    assert any("[Attached then: claims spec (v2).md]" in b.get("text", "") for m in later for b in m["content"])
    got = conversation(env)[1]
    assert got["attach"]["maxFiles"] == 5 and got["attach"]["s3"] == []


def test_what_a_message_may_attach_is_checked_before_the_turn_starts(env, model):
    save(env)
    model(says("ok"))
    d = env.handler.designer

    def post(**body):
        return call(env, "POST /api/builds/{id}/design", params={"id": "pclaims01"},
                    body={"message": "x", **body})
    assert call(env, "POST /api/builds/{id}/design/attachments", params={"id": "pclaims01"},
                body={"name": "tool.exe"})[1]["error"].startswith("tool.exe: not a file type")
    status, err = post(attachments=[{"key": "builds/other/attachments/ab-x.md", "name": "x.md"}])
    assert status == 400 and "not one of this build's uploads" in err["error"]
    status, err = post(attachments=[{"key": "builds/pclaims01/attachments/ab-x.md", "name": "x.md"}])
    assert status == 400 and "has not finished uploading" in err["error"]
    big = _upload(env, "big.pdf", b"x" * (d.DOC_MAX_BYTES + 1))
    assert "the most is 4.50 MB" in post(attachments=[big])[1]["error"]
    six = [_upload(env, f"f{i}.txt", b"hi") for i in range(6)]
    assert "at most 5 files" in post(attachments=six)[1]["error"]
    # s3:// needs an allowed bucket; none is allowed by default.
    status, err = post(message="read s3://data-bucket/spec.md")
    assert status == 400 and "reads no S3 paths" in err["error"]
    assert env.invoked == []


def test_an_s3_path_in_an_allowed_bucket_is_read_object_or_prefix(env, model, monkeypatch):
    save(env)
    d = env.handler.designer
    env.s3.create_bucket(Bucket="data-bucket")
    env.s3.put_object(Bucket="data-bucket", Key="specs/a.csv", Body=b"id,name\n1,x")
    env.s3.put_object(Bucket="data-bucket", Key="specs/b.png", Body=b"\x89PNG")
    env.s3.put_object(Bucket="data-bucket", Key="other/c.md", Body=b"no")
    monkeypatch.setattr(d, "S3_ALLOWED", ["data-bucket/specs"])
    monkeypatch.setattr(d, "_s3_any", env.s3)
    script = model(says("ok"))

    def post(message):
        return call(env, "POST /api/builds/{id}/design", params={"id": "pclaims01"}, body={"message": message})
    status, err = post("see s3://data-bucket/other/c.md")
    assert status == 400 and "not in a bucket this console may read" in err["error"]
    status, doc = post("Use the files in s3://data-bucket/specs/ please.")
    assert status == 202
    assert [a["uri"] for a in doc["turns"][-2]["attachments"]] == [
        "s3://data-bucket/specs/a.csv", "s3://data-bucket/specs/b.png"]
    run_background(env)
    sent = script.calls[0]["messages"][-1]["content"]
    assert next(b for b in sent if "document" in b)["document"]["format"] == "csv"
    assert next(b for b in sent if "image" in b)["image"] == {"format": "png", "source": {"bytes": b"\x89PNG"}}
    audit = [a for a in call(env, "GET /api/audit")[1] if a["action"] == "design.message"]
    assert audit[0]["detail"]["attachments"] == ["s3://data-bucket/specs/a.csv", "s3://data-bucket/specs/b.png"]


def test_a_change_cut_off_at_the_output_limit_is_asked_for_again_in_parts(env, model):
    """Live: a 9-agent draft in one apply_changes hit maxTokens; the turn ended "Done."
    with nothing applied and no reason. Now the model is told, and tries again smaller."""
    save(env)
    cut = tool_use(add_fraud())
    cut["stopReason"] = "max_tokens"
    script = model(cut, tool_use(add_fraud()), says("Added it in parts."))
    send(env, "add a fraud check")
    run_background(env)
    reply = conversation(env)[1]["turns"][-1]
    assert reply["status"] == "done" and reply["text"] == "Added it in parts."
    assert [c["summary"] for c in reply["changes"]] == ["Added a fraud check after intake"]
    retry = script.calls[1]["messages"]
    assert "cut off at the output limit" in retry[-1]["content"][0]["text"]
    assert not any("toolUse" in b for b in retry[-2]["content"])


def test_a_named_identitys_secret_is_asked_for_in_the_secure_field(env):
    """An `identities` entry holds no secret, so the page must ask for it: before, the
    designer could only ask for a tool's or a remote agent's, and an apikey identity was
    left with no way to set its key in the UI."""
    designer = env.handler.designer
    wf = {"identities": {"cmsApi": {"type": "apikey"}, "crmOauth": {"type": "oauth2", "clientId": "c"}},
          "tools": {}, "agents": {}}
    got = designer._secrets_needed([{"identity": "cmsApi", "why": "CMS key"},
                                    {"identity": "crmOauth", "why": "CRM client"},
                                    {"identity": "nosuch", "why": "not an identity"}], wf)
    assert got == [{"kind": "identitySecrets", "name": "cmsApi", "why": "CMS key", "label": "API key"},
                   {"kind": "identitySecrets", "name": "crmOauth", "why": "CRM client",
                    "label": "OAuth client secret"}]


# --- a long turn: it streams what it is doing, and does not stop half-way ---------------

def test_a_change_being_written_names_each_edit_as_it_streams(env):
    designer = env.handler.designer
    raw = json.dumps({"summary": "s", "ops": [
        {"op": "set_tool", "key": "webSearch", "tool": {"type": "websearch"}},
        {"op": "set_agent", "id": "trip_intake", "agent": {"name": "Trip Intake"}},
        {"op": "set_steps", "steps": []}]})
    assert designer.progress_of(raw) == ["Adding tool webSearch", "Adding agent trip_intake",
                                         "Wiring the stages"]
    # Mid-stream: only what has arrived so far, and never the previous op's target.
    half = raw[:raw.index("trip_intake")]
    assert designer.progress_of(half) == ["Adding tool webSearch"]


def test_the_conversation_is_cached_up_to_the_newest_message(env):
    designer = env.handler.designer
    msgs = [{"role": "user", "content": [{"text": "a"}, {"cachePoint": {"type": "default"}}]},
            {"role": "assistant", "content": [{"text": "b"}]},
            {"role": "user", "content": [{"text": "c"}]}]
    out = designer._cached(msgs)
    points = [i for i, m in enumerate(out) for b in m["content"] if "cachePoint" in b]
    assert points == [2], "exactly one cache point, after the newest message"
    assert msgs[2]["content"] == [{"text": "c"}], "the caller's messages are not changed"


def test_a_turn_out_of_time_carries_on_in_a_fresh_invocation(env, model, monkeypatch):
    """It used to stop at the budget with "That took too long" and half a workflow built.
    Now the next invocation picks up from the saved draft and finishes the request."""
    designer = env.handler.designer
    save(env)
    script = model(tool_use(add_fraud()), says("Done: the rest is in place."))
    continued: list[dict] = []
    monkeypatch.setattr(designer, "_continue", lambda ev: continued.append(ev))
    # The clock runs out after the first model call (the one that applies the change).
    now = [0.0]
    stream = script.converse_stream

    def slow(**kw):
        out = stream(**kw)
        now[0] = designer.TURN_BUDGET_S + 1
        return out
    monkeypatch.setattr(script, "converse_stream", slow)
    monkeypatch.setattr(designer.time, "monotonic", lambda: now[0])
    send(env, "Add a fraud check after intake, and then a final report")
    assert run_background(env) == {"ok": True, "continued": 1}
    doc = conversation(env)[1]
    assert doc["status"] == "thinking", "the turn is still running, not stopped"
    first = doc["turns"][-1]
    assert first["changes"][0]["summary"] == "Added a fraud check after intake"
    assert continued and continued[0]["cont"] == 1

    # The continuation: a fresh clock, the same turn.
    monkeypatch.setattr(script, "converse_stream", stream)
    now[0] = 0.0
    assert env.handler.handler(continued[0], CTX) == {"ok": True}
    told = json.dumps(script.calls[-1]["messages"][-1])
    assert "continuing YOUR OWN reply" in told and "Added a fraud check after intake" in told
    reply = conversation(env)[1]["turns"][-1]
    assert reply["status"] == "done" and "That took too long" not in reply["text"]
    assert [c["summary"] for c in reply["changes"]] == ["Added a fraud check after intake"]
    # A duplicate delivery of the first part does nothing now.
    assert env.handler.handler({**continued[0], "cont": 0}, CTX)["skipped"] is True


def test_the_model_is_asked_for_medium_effort_and_a_model_without_it_still_works(env, model, monkeypatch):
    """Claude Sonnet 5's default effort thought for ~135 s, hidden, and was cut off at the
    output limit with nothing applied. Sonnet 5.5 at medium wrote it in ~31 s."""
    designer = env.handler.designer
    save(env)
    script = model(says("Hello."), says("Hello again."))
    send(env, "Hi")
    run_background(env)
    assert script.calls[-1]["additionalModelRequestFields"] == {"output_config": {"effort": "medium"}}
    # A model that rejects the field: one retry without it, then never sent again.
    stream = script.converse_stream

    def picky(**kw):
        if "additionalModelRequestFields" in kw:
            raise RuntimeError("ValidationException: output_config.effort: Extra inputs are not permitted")
        return stream(**kw)
    monkeypatch.setattr(script, "converse_stream", picky)
    monkeypatch.setattr(designer, "_no_effort", set())
    send(env, "Hi again")
    assert run_background(env) == {"ok": True}
    assert conversation(env)[1]["turns"][-1]["text"] == "Hello again."
    assert designer._no_effort == {designer.current_model()[0]}


def test_a_model_the_account_is_refused_falls_back_to_the_next(env, model, monkeypatch):
    """A workshop account's private marketplace had Sonnet 5 but not Sonnet 5.5, and every
    turn failed with AccessDeniedException. Now the next model in the list is used."""
    designer = env.handler.designer
    assert [m for m, _ in designer.MODELS] == ["us.anthropic.claude-sonnet-5-5",
                                              "us.anthropic.claude-sonnet-5"]
    assert all(e == "medium" for _, e in designer.MODELS)
    monkeypatch.setattr(designer, "_model_at", [0])
    save(env)
    script = model(says("Hello from the fallback."))
    stream = script.converse_stream

    def marketplace(**kw):
        if kw["modelId"].endswith("sonnet-5-5"):
            raise RuntimeError("AccessDeniedException: Not authorized to perform action due "
                               "to private marketplace eligibility")
        return stream(**kw)
    monkeypatch.setattr(script, "converse_stream", marketplace)
    send(env, "Hi")
    assert run_background(env) == {"ok": True}
    doc = conversation(env)[1]
    assert doc["turns"][-1]["text"] == "Hello from the fallback."
    assert doc["model"] == "us.anthropic.claude-sonnet-5"
    assert script.calls[-1]["modelId"] == "us.anthropic.claude-sonnet-5"
    assert script.calls[-1]["additionalModelRequestFields"] == {"output_config": {"effort": "medium"}}


def test_the_model_list_reads_per_model_effort(env):
    designer = env.handler.designer
    assert designer._model_list("us.anthropic.claude-sonnet-5-5:low, anthropic.claude-x-v1:0") == [
        ("us.anthropic.claude-sonnet-5-5", "low"), ("anthropic.claude-x-v1:0", designer.EFFORT)]


def test_renaming_the_build_carries_the_apps_title_and_heading(env):
    """The page keeps the build's name and ui.title/ui.heading in step; so does set_name."""
    designer = env.handler.designer
    p = {"name": "Trip Planner", "workflow": {"agents": {}, "steps": [], "tools": {},
                                              "ui": {"title": "Trip Planner", "heading": "Acme"}},
         "prompts": {}}
    new, changed = designer.apply_ops(p, [{"op": "set_name", "name": "Travel Brief"}])
    assert new["name"] == "Travel Brief"
    assert new["workflow"]["ui"] == {"title": "Travel Brief", "heading": "Acme"}
    assert "ui" in changed["blocks"]


def test_an_allowed_s3_prefix_is_a_folder_not_a_string_prefix(env, monkeypatch):
    designer = env.handler.designer
    monkeypatch.setattr(designer, "S3_ALLOWED", ["docs-bucket/reports"])
    assert designer._s3_allowed("docs-bucket", "reports/q3.pdf")
    assert designer._s3_allowed("docs-bucket", "reports")
    assert not designer._s3_allowed("docs-bucket", "reports-private/payroll.pdf")
    assert not designer._s3_allowed("docs-bucket", "reportsX")
    assert not designer._s3_allowed("other-bucket", "reports/q3.pdf")
    monkeypatch.setattr(designer, "S3_ALLOWED", ["docs-bucket"])
    assert designer._s3_allowed("docs-bucket", "anything/at/all")


# --- library items: kept, and reused --------------------------------------------------

DOCS_TOOL = {"type": "mcp", "description": "Docs server.", "endpoint": "https://docs.example.com/mcp"}


def _published(env, sub="u1", name="docs"):
    """A tool in the caller's library, and the build using it live (Publish to library)."""
    _, it = call(env, "POST /api/library", sub=sub,
                 body={"kind": "tool", "name": name, "definition": DOCS_TOOL})
    return it


def test_an_edit_to_a_build_with_a_library_tool_is_applied_not_rejected(env, model):
    """Observed live: after Publish to library, every Assistant edit came back
    "a shared tool that could not be loaded", because the draft was checked with its
    library links unresolved; the Assistant then said it could not reach the library
    tools, or re-created them."""
    it = _published(env)
    p = project()
    p["workflow"]["tools"] = {"docs": {"library": it["id"]}}
    p["workflow"]["agents"]["claims_intake"]["tool"] = "docs"
    assert call(env, "PUT /api/builds/{id}", params={"id": "pclaims01"}, body={"project": p})[0] == 200
    script = model(tool_use(add_fraud()), says("Added the fraud check."))
    send(env, "Add a fraud check after intake")
    run_background(env)
    result = script.calls[1]["messages"][-1]["content"][0]["toolResult"]["content"][0]["json"]
    assert result["status"] == "applied" and result["remaining_problems"] == []
    wf = draft(env)["workflow"]
    assert wf["tools"] == {"docs": {"library": it["id"]}} and "fraud_check" in wf["agents"]
    # The model saw what the library tool is, and that it must leave it alone.
    context = script.calls[0]["messages"][-1]["content"][0]["text"]
    assert '"tools.docs": {"type": "mcp"' in context and "never edit, re-create or remove it" in context


def test_the_users_library_is_offered_and_reused_by_its_id(env, model):
    it = _published(env)
    _, theirs = call(env, "POST /api/library", sub="u2",
                     body={"kind": "tool", "name": "secret", "definition": DOCS_TOOL})
    save(env)
    use = ("apply_changes", {"summary": "Intake reads the docs server from your library", "ops": [
        {"op": "set_tool", "key": "docs", "tool": {"library": it["id"]}},
        {"op": "update_agent", "id": "claims_intake", "set": {"tool": "docs"}}]})
    script = model(tool_use(use), says("Done."))
    send(env, "Let intake use my docs tool")
    run_background(env)
    context = script.calls[0]["messages"][-1]["content"][0]["text"]
    assert f'"id": "{it["id"]}"' in context and '"type": "mcp"' in context
    assert theirs["id"] not in context                       # someone else's, not shared
    result = script.calls[1]["messages"][-1]["content"][0]["toolResult"]["content"][0]["json"]
    assert result["status"] == "applied"
    assert draft(env)["workflow"]["tools"]["docs"] == {"library": it["id"]}


def test_a_library_item_not_shared_with_the_caller_is_not_loaded(env, model):
    _, theirs = call(env, "POST /api/library", sub="u2",
                     body={"kind": "tool", "name": "secret", "definition": DOCS_TOOL})
    save(env)
    sneak = ("apply_changes", {"summary": "x", "ops": [
        {"op": "set_tool", "key": "docs", "tool": {"library": theirs["id"]}},
        {"op": "update_agent", "id": "claims_intake", "set": {"tool": "docs"}}]})
    script = model(tool_use(sneak), says("Could not."))
    send(env, "Use that tool")
    run_background(env)
    result = script.calls[1]["messages"][-1]["content"][0]["toolResult"]["content"][0]["json"]
    assert result["status"] == "rejected"
    assert any(e["path"] == "tools.docs" and "could not be loaded" in e["message"] for e in result["errors"])
