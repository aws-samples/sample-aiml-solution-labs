"""How a review gate decides (steps[].hitl).

`"hitl": true` is a person, every time, in the console. The object form says more:

    "hitl": {
      "mode": "always" | "threshold" | "auto",
      "when": [{"field": "riskScore", "gte": 80}],     # threshold: a person only when one matches
      "approval": "console" | "event",                 # event: also asked for, and answerable, on EventBridge
      "timeout": {"after": "24h", "action": "deny"}     # no decision by then: approve or deny
    }

  always     the run waits for a decision (the default).
  threshold  the run waits only when a `when` rule matches the gated output (the rules
             are branch rules: `field`, a dot path into the output JSON, or the raw text
             without one; equals / notEquals / in / contains / exists / gt / gte / lt /
             lte). No match: the gate approves itself and says which rules it checked.
  auto       the gate approves itself. So does every gate of a run a trigger started
             with `gates: "auto"`.

Every gate that approves itself says so on the run's timeline and in the activity log.

`approval: "event"` publishes "AgentExpress Approval Requested" to the account's default
event bus as the run starts waiting, so Slack, ServiceNow or your own code can ask a
person; their answer comes back as an "AgentExpress Approval Decision" event (bff/gates.py).
The console can still decide it. A `timeout` is applied by the BFF's sweep, every five
minutes, with the action as the decision.
"""

from __future__ import annotations

import json
import os
import re
import time

from app.common import assets, branching, clock

MODES = ("always", "threshold", "auto")
APPROVALS = ("console", "event")
AFTER_RE = re.compile(r"^([1-9][0-9]*)(m|h|d)$")
UNIT_S = {"m": 60, "h": 3600, "d": 86400}
MAX_AFTER_S = 30 * 86400
#: What an approval request and decision are called on EventBridge.
SOURCE = "agentexpress"
REQUESTED = "AgentExpress Approval Requested"
DECISION = "AgentExpress Approval Decision"
APP_NAME = os.environ.get("APP_NAME", "")


def spec_of(hitl) -> dict:
    """The gate's spec, with every default filled in."""
    h = hitl if isinstance(hitl, dict) else {}
    return {"mode": h.get("mode") or "always", "when": list(h.get("when") or []),
            "approval": h.get("approval") or "console", "timeout": h.get("timeout")}


def after_seconds(text: str) -> int:
    m = AFTER_RE.match(str(text or ""))
    if not m:
        raise ValueError(f"`timeout.after` is <n>m, <n>h or <n>d (e.g. 30m, 24h, 2d), not {text!r}")
    return int(m.group(1)) * UNIT_S[m.group(2)]


def validate(hitl, where: str) -> None:
    """Raise ValueError naming `where` for a malformed gate. `true`/`false` is always fine."""
    if isinstance(hitl, bool) or hitl is None:
        return
    if not isinstance(hitl, dict):
        raise ValueError(f"{where}: `hitl` is true, false or an object {{mode, when, approval, timeout}}.")
    bad = sorted(set(hitl) - {"mode", "when", "approval", "timeout"})
    if bad:
        raise ValueError(f"{where}: `hitl` has unknown key(s) {bad}: it takes mode, when, approval, timeout.")
    s = spec_of(hitl)
    if s["mode"] not in MODES:
        raise ValueError(f"{where}: `hitl.mode` is one of {list(MODES)}.")
    if s["mode"] == "threshold" and not s["when"]:
        raise ValueError(f"{where}: `hitl.mode` threshold needs `when`: the rules that call for a person.")
    if s["when"]:
        branching.validate_spec({"when": [{**r, "goto": "END"} if isinstance(r, dict) else r for r in s["when"]]},
                                f"{where} hitl")
    if s["approval"] not in APPROVALS:
        raise ValueError(f"{where}: `hitl.approval` is one of {list(APPROVALS)}.")
    t = s["timeout"]
    if t is not None:
        if not isinstance(t, dict) or t.get("action") not in ("approve", "deny"):
            raise ValueError(f"{where}: `hitl.timeout` is {{\"after\": \"24h\", \"action\": \"approve\" | \"deny\"}}.")
        if after_seconds(t.get("after")) > MAX_AFTER_S:
            raise ValueError(f"{where}: `hitl.timeout.after` is at most 30d.")


def auto_reason(spec: dict, state: dict, outputs: list[str]) -> str:
    """Why this gate approves itself now, or "" when a person decides."""
    if state.get("gates") == "auto":
        return "the trigger that started this run approves every gate"
    if spec["mode"] == "auto":
        return "this gate approves itself"
    if spec["mode"] == "threshold":
        for out in outputs:
            data = assets.extract_json(out or "") or {}
            for rule in spec["when"]:
                if branching._matches(rule, data, out or ""):
                    return ""
        rules = "; ".join(branching._describe(r) for r in spec["when"])
        return f"no rule called for a person ({rules})"
    return ""


def request_extra(spec: dict) -> dict:
    """What the waiting run's `hitl` carries besides node and question: how it is
    answered, and when it times out (read by bff/gates.py and the run page)."""
    out = {"approval": spec["approval"]}
    t = spec["timeout"]
    if isinstance(t, dict):
        due = time.time() + after_seconds(t.get("after"))
        out.update(timeoutAt=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(due)),
                   timeoutAction=t.get("action"))
    return out


def publish_request(session_id: str, hitl: dict, output: str = "") -> None:
    """Ask on EventBridge too (approval "event"), once, as the run starts waiting: the
    sink calls this when the run first reaches waiting_human. Best effort: the console
    still works."""
    if hitl.get("approval") != "event":
        return
    try:
        import boto3
        node = str(hitl.get("node") or "")
        detail = {"app": APP_NAME, "session": session_id, "gate": node,
                  "question": str(hitl.get("question") or "")[:2000], "output": str(output or "")[:4000],
                  "at": clock.now_str(),
                  **{k: hitl[k] for k in ("timeoutAt", "timeoutAction") if hitl.get(k)},
                  "answerWith": {"detail-type": DECISION, "detail": {
                      "app": APP_NAME, "session": session_id, "gate": node,
                      "decision": "approve | deny | revise", "comment": "", "by": ""}}}
        boto3.client("events").put_events(Entries=[{"Source": SOURCE, "DetailType": REQUESTED,
                                                    "Detail": json.dumps(detail)}])
    except Exception as e:  # noqa: BLE001 - a missed notification never fails the run
        print(f"[gates] could not publish the approval request for {session_id}: {type(e).__name__}: {e}")


def audit_auto(session_id: str, node: str, why: str) -> None:
    """The self-approval in the activity log, for the run's owner."""
    from app.common import audit
    table = os.environ.get("STATUS_TABLE", "")
    if not (audit.AUDIT_TABLE and table):
        return
    try:
        import boto3
        res = boto3.resource("dynamodb")
        run = res.Table(table).get_item(Key={"session_id": session_id},
                                        ProjectionExpression="#o, #u", ExpressionAttributeNames={
                                            "#o": "owner", "#u": "user"}).get("Item") or {}
        if not run.get("owner"):
            return
        for item in audit.audit_items(str(run["owner"]), str(run.get("user") or ""), "run.auto_approved",
                                      {"session": session_id, "gate": node, "why": why[:300]}):
            res.Table(audit.AUDIT_TABLE).put_item(Item=item)
    except Exception as e:  # noqa: BLE001 - never fails the run
        print(f"[gates] could not record the self-approval of {session_id}: {type(e).__name__}: {e}")
