"""A gate the Run Assistant decides is decided like one on the run page.

Seen on a live run: asked to approve a run's review gate, the Run Assistant
(bff/chatbot.py submit_review) resumed the run itself, so the decision never went through
the route the run page uses: no run.decided row in the activity log, and nothing stopped a
second decision of the same pause. Every action the Assistant takes now goes through
handler._chat_invoke, which for a gate decision claims the pause once (_claim_decision) and
logs run.decided for the person who asked, marked as made through the Assistant.

Scenarios: an invoice-approval run and a staff-onboarding run; the model is scripted.
"""
from __future__ import annotations

import importlib
import json
import sys
import types

import pytest
from botocore.exceptions import ClientError

WF = {"agents": {"approver": {"name": "Approver", "runtime": "main"}},
      "steps": [{"agent": "approver", "hitl": True}],
      "authorization": {"actions": {"decision": ["approvers"]}},
      "orchestrator": {"chatbot": {"enabled": True}}}
EMAIL = "reviewer@example.com"


class StatusTable:
    """One run's status item, with the conditional claim DynamoDB makes."""

    def __init__(self, items: dict[str, dict]):
        self.items = items

    def get_item(self, Key, **kw):
        it = self.items.get(Key["session_id"])
        return {"Item": json.loads(json.dumps(it))} if it else {}

    def update_item(self, Key, UpdateExpression, ConditionExpression="", ExpressionAttributeValues=None, **kw):
        it = self.items[Key["session_id"]]
        hitl = it.get("hitl") or {}
        if "decidedAt" in hitl or it.get("overall") != "waiting_human":
            raise ClientError({"Error": {"Code": "ConditionalCheckFailedException", "Message": "taken"}},
                              "UpdateItem")
        hitl["decidedAt"] = ExpressionAttributeValues[":t"]
        it["hitl"] = hitl
        return {}


@pytest.fixture()
def bff(monkeypatch):
    monkeypatch.setenv("WORKFLOW_JSON", json.dumps(WF))
    monkeypatch.setenv("STATUS_TABLE", "t-status")
    monkeypatch.setenv("EVENTS_TABLE", "t-events")
    monkeypatch.setenv("RUNTIME_ARN", "arn:aws:bedrock-agentcore:us-east-1:1:runtime/x")
    for mod in ("workflow", "authz", "chatbot", "handler"):
        sys.modules.pop(mod, None)
    handler = importlib.import_module("handler")
    invoked: list[dict] = []
    audited: list[tuple] = []
    monkeypatch.setattr(handler, "_self_invoke", lambda fn, payload: invoked.append(payload))
    monkeypatch.setattr(handler, "_audit_run", lambda event, action, sid, **d: audited.append((action, sid, d)))
    monkeypatch.setattr(handler.chatbot, "is_enabled", lambda: True)
    handler._test = types.SimpleNamespace(invoked=invoked, audited=audited)
    yield handler
    for mod in ("workflow", "authz", "chatbot", "handler"):
        sys.modules.pop(mod, None)


def waiting(bff, monkeypatch, sid: str, gate: str, user: str) -> StatusTable:
    tbl = StatusTable({sid: {"session_id": sid, "overall": "waiting_human", "owner": EMAIL,
                             "user": user, "hitl": {"node": gate}}})
    monkeypatch.setattr(bff, "status_tbl", tbl)
    monkeypatch.setattr(bff.chatbot, "_status", tbl)
    return tbl


def model_says(bff, monkeypatch, *replies):
    """A scripted Run Assistant model: each Converse call returns the next reply."""
    queue = list(replies)
    monkeypatch.setattr(bff.chatbot, "_converse", lambda model, system, messages, tools: queue.pop(0))


def approve(sid: str, decision: str = "approve", comment: str = "") -> dict:
    return {"stopReason": "tool_use", "output": {"message": {"role": "assistant", "content": [
        {"toolUse": {"toolUseId": "t1", "name": "submit_review",
                     "input": {"session_id": sid, "decision": decision, "comment": comment}}}]}}}


def done(text: str) -> dict:
    return {"stopReason": "end_turn", "output": {"message": {"role": "assistant", "content": [{"text": text}]}}}


def chat(bff, message: str, sid: str) -> tuple[int, dict]:
    claims = {"email": EMAIL, "cognito:groups": ["approvers"]}
    event = {"requestContext": {"http": {"method": "POST"}, "authorizer": {"jwt": {"claims": claims}}},
             "rawPath": "/api/chat", "routeKey": "POST /api/chat",
             "body": json.dumps({"message": message, "session_id": sid})}
    resp = bff._api(event, types.SimpleNamespace(function_name="bff-fn"))
    return resp["statusCode"], json.loads(resp["body"])


@pytest.mark.parametrize("sid,gate,user,decision,comment", [
    ("inv-0001-run", "approver", "ap-clerk@example.com", "approve", ""),            # invoice approval
    ("hr-0042-run", "approver", "hr-partner@example.com", "revise", "Add the start date."),  # onboarding
])
def test_a_gate_the_assistant_decides_is_claimed_and_logged_as_run_decided(bff, monkeypatch, sid, gate, user,
                                                                          decision, comment):
    tbl = waiting(bff, monkeypatch, sid, gate, user)
    model_says(bff, monkeypatch, approve(sid, decision, comment), done("Done."))
    status, body = chat(bff, f"{decision} the review for {sid}", sid)
    assert status == 200 and body["actions"]
    # Logged as the run page logs it, for the person who asked, marked as the Assistant's.
    assert bff._test.audited == [("run.decided", sid, {"decision": decision, "comment": comment,
                                                       "via": "assistant"})]
    # Claimed once, and the runtime resumed with the run's user for cost attribution.
    assert "decidedAt" in tbl.items[sid]["hitl"]
    sent = bff._test.invoked[-1]
    assert (sent["action"], sent["session_id"], sent["decision"], sent["user"]) == ("resume", sid, decision, user)


@pytest.mark.parametrize("sid", ["inv-0002-run", "hr-0043-run"])
def test_a_pause_already_decided_is_not_decided_again_through_the_assistant(bff, monkeypatch, sid):
    tbl = waiting(bff, monkeypatch, sid, "approver", "someone@example.com")
    tbl.items[sid]["hitl"]["decidedAt"] = "2026-01-01 10:00:00"      # the run page got there first
    model_says(bff, monkeypatch, approve(sid), done("It was already decided."))
    status, body = chat(bff, "approve it", sid)
    assert status == 200 and body["actions"] == []
    assert bff._test.invoked == [] and bff._test.audited == []


def test_other_assistant_actions_are_sent_as_before(bff, monkeypatch):
    waiting(bff, monkeypatch, "inv-0003-run", "approver", "u@example.com")
    model_says(bff, monkeypatch, {"stopReason": "tool_use", "output": {"message": {"role": "assistant", "content": [
        {"toolUse": {"toolUseId": "t1", "name": "rerun_agents",
                     "input": {"session_id": "inv-0003-run", "agents": [{"agent_id": "approver"}]}}}]}}},
        done("Re-running."))
    monkeypatch.setattr(bff.authz, "permitted_actions", lambda event: ["decision", "rerun"])
    chat(bff, "re-run the approver", "inv-0003-run")
    assert bff._test.invoked[-1]["action"] == "rerun_from" and bff._test.audited == []
