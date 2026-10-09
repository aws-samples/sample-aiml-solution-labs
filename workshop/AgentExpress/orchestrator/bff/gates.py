"""Review gates answered without the run page (steps[].hitl, app/common/gates.py).

Two ways, both arriving at this function from EventBridge (the IaC wires them only when
a step asks for them):

  a decision   an "AgentExpress Approval Decision" event on the account's default bus,
               for a gate with `approval: "event"`:
                   {"detail-type": "AgentExpress Approval Decision",
                    "detail": {"app": "<agentName>", "session": "<run id>",
                               "gate": "<gate>", "decision": "approve" | "deny" | "revise",
                               "comment": "...", "by": "who decided"}}
               Whoever may put events on that bus may decide, so that is the permission
               to guard. A decision for a run that is not waiting at that gate, or for a
               gate that does not take events, is ignored and logged.
  a timeout    every five minutes a sweep finds runs waiting past their gate's
               `timeout.after` and decides them with its `timeout.action`.

Both are in the run owner's activity log as run.decided, with who (or what) decided.
"""

from __future__ import annotations

import os

import workflow
from boto3.dynamodb.conditions import Attr

APP_NAME = os.environ.get("APP_NAME", "")
DECISIONS = ("approve", "deny", "revise")


def gate_spec(node: str) -> dict:
    """The `hitl` object of the step whose gate is `node` ({} for `true` or none). A
    single agent's gate is named by the agent; a group's by its gateId, else group<i>
    (graph_builder._gate_id)."""
    for i, step in enumerate(workflow.RAW.get("steps") or []):
        if not isinstance(step, dict):
            continue
        name = step.get("agent") or step.get("gateId") or f"group{i}"
        if name == node:
            h = step.get("hitl")
            return h if isinstance(h, dict) else {}
    return {}


def _waiting(table, sid: str) -> dict | None:
    item = table.get_item(Key={"session_id": sid}, ProjectionExpression="overall, hitl").get("Item")
    return item if item and item.get("overall") == "waiting_human" else None


def decide(detail: dict, table, resume) -> dict:
    """One "AgentExpress Approval Decision" event."""
    sid = str(detail.get("session") or "")
    decision = str(detail.get("decision") or "").lower()
    if APP_NAME and detail.get("app") != APP_NAME:
        return {"decided": False, "reason": "for another app"}
    if decision not in DECISIONS:
        return {"decided": False, "reason": f"decision must be one of {', '.join(DECISIONS)}"}
    item = _waiting(table, sid) if sid else None
    if not item:
        print(f"[gates] decision for {sid!r} ignored: it is not waiting for one")
        return {"decided": False, "reason": "the run is not waiting for a decision"}
    node = str((item.get("hitl") or {}).get("node") or "")
    if detail.get("gate") and detail["gate"] != node:
        return {"decided": False, "reason": f"the run is waiting at {node}, not {detail['gate']}"}
    if gate_spec(node).get("approval") != "event":
        print(f"[gates] decision for {sid} at {node} ignored: that gate is decided in the app")
        return {"decided": False, "reason": "that gate does not take decisions by event"}
    by = f"event:{str(detail.get('by') or 'unnamed')[:100]}"
    resume(sid, decision, str(detail.get("comment") or "")[:2000], by)
    return {"decided": True, "session": sid, "gate": node, "decision": decision}


def sweep(table, resume) -> dict:
    """Decide every run waiting past its gate's timeout."""
    now = _utc_now()   # timeoutAt is UTC, written by app/common/gates.py
    kwargs = {"ProjectionExpression": "session_id, hitl",
              "FilterExpression": Attr("overall").eq("waiting_human") & Attr("hitl.timeoutAt").exists()}
    decided = []
    while True:
        page = table.scan(**kwargs)
        for item in page.get("Items", []):
            h = item.get("hitl") or {}
            due, action = str(h.get("timeoutAt") or ""), str(h.get("timeoutAction") or "")
            if not due or due > now or action not in ("approve", "deny"):
                continue
            sid = item["session_id"]
            try:
                # Once: whichever sweep claims it first decides it.
                table.update_item(Key={"session_id": sid}, UpdateExpression="REMOVE hitl.timeoutAt",
                                  ConditionExpression="hitl.timeoutAt = :t AND overall = :w",
                                  ExpressionAttributeValues={":t": due, ":w": "waiting_human"})
            except Exception as e:  # noqa: BLE001 - decided meanwhile
                print(f"[gates] {sid}: not timed out after all ({type(e).__name__})")
                continue
            resume(sid, action, f"No decision by {due}: {action}d by the gate's timeout.", "timeout")
            decided.append(sid)
        if not page.get("LastEvaluatedKey"):
            break
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    return {"timedOut": decided}


def _utc_now() -> str:
    import time
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def is_gate_event(event: dict) -> bool:
    return event.get("axGate") in ("decision", "sweep")


def dispatch(event: dict, table, resume) -> dict:
    if event.get("axGate") == "sweep":
        return sweep(table, resume)
    ev = event.get("event") if isinstance(event.get("event"), dict) else {}
    return decide(ev.get("detail") or {}, table, resume)
