"""In-app assistant — a Bedrock Converse tool-use loop scoped to THIS app.

Answers questions about workflow runs (status, cost, latency, outputs, guardrails,
evaluations) and takes actions (approve/revise/deny a review gate, re-run agents,
run an evaluation) by triggering the SAME runtime actions the UI buttons use.
Anything outside this app is politely declined.

Config (workflow.json -> orchestrator.chatbot, shipped compact in WORKFLOW_JSON):
  {enabled, model, greeting, placeholder, tools:{status,sessions,outputs,costs,
   latency,guardrails,evals,runEval,rerun,review}}
Every tools.<name> flag toggles ONE capability, so the assistant is fully
config-driven — turn a capability off and it disappears from the tool set. Any
tool not mentioned defaults to ON.

This is deliberately a plain Bedrock Converse tool loop in the BFF Lambda, not
another agent: it answers ABOUT runs and must stay available while a run is
paused, so it does not depend on the orchestrator runtime being warm.
"""

import os
import time
from decimal import Decimal

import boto3
import workflow
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

REGION = os.environ.get("AWS_REGION", "us-east-1")
# The projection, not the raw workflow: the assistant answers questions a UI user
# asks, so it should see exactly what that user's page sees and nothing more. See
# bff/workflow.py.
WORKFLOW = workflow.VIEW
CHATBOT = WORKFLOW.get("chatbot") or {}
AGENTS = WORKFLOW.get("agents") or {}
TOOLS_CFG = CHATBOT.get("tools") or {}
UI = WORKFLOW.get("ui") or {}
# The deployment's own model (MODEL_ID, injected by both IaC paths), else the workflow's
# defaultModel, else the framework default — never a literal of its own, which is how
# this drifted from var.model_id / the CDK context default.
DEFAULT_MODEL = workflow.default_model(REGION)
MAX_TURNS = 6

_ddb = boto3.resource("dynamodb", region_name=REGION)
# None on a control-plane console, which has no runs of its own (and no assistant).
_status = _ddb.Table(os.environ["STATUS_TABLE"]) if os.environ.get("STATUS_TABLE") else None
_tel = _ddb.Table(os.environ["TELEMETRY_TABLE"]) if os.environ.get("TELEMETRY_TABLE") else None
_bedrock = boto3.client("bedrock-runtime", region_name=REGION)


def is_enabled() -> bool:
    return bool(CHATBOT.get("enabled"))


# --- helpers ---------------------------------------------------------------

def _num(x) -> float:
    try:
        return float(x)
    except Exception:  # noqa: BLE001
        return 0.0


def _query_all(table, **kw) -> list:
    out = []
    while True:
        page = table.query(**kw)
        out += page.get("Items", [])
        lek = page.get("LastEvaluatedKey")
        if not lek:
            break
        kw["ExclusiveStartKey"] = lek
    return out


def _scan_all(table, **kw) -> list:
    out = []
    while True:
        page = table.scan(**kw)
        out += page.get("Items", [])
        lek = page.get("LastEvaluatedKey")
        if not lek:
            break
        kw["ExclusiveStartKey"] = lek
    return out


def _sid(args, ctx) -> str:
    return (args.get("session_id") or ctx or "").strip()


#: Whose runs this chat turn may touch (the caller's JWT `sub`), set by handle_chat.
#: None only when there is no caller at all (the unit tests of the tool loop). A Lambda
#: serves one request at a time, so a module value is per-request here.
_OWNER: str | None = None


def _owns(sid: str) -> bool:
    """Whether the caller started run `sid`. Every tool naming a run goes through this,
    so the assistant is no way round the per-user privacy the routes enforce."""
    if _OWNER is None:
        return True
    it = _status.get_item(Key={"session_id": sid}, ProjectionExpression="#o",
                          ExpressionAttributeNames={"#o": "owner"}).get("Item") or {}
    return it.get("owner") == _OWNER


def _tel_rows(sid: str) -> list:
    return _query_all(_tel, KeyConditionExpression=Key("session_id").eq(sid)) if (_tel and sid) else []


# --- tool implementations --------------------------------------------------
# Every tool: fn(args, ctx_session, self_invoke) -> (result_dict, action_label|None)

def _t_status(args, ctx, si):
    sid = _sid(args, ctx)
    if not sid:
        return {"error": "no run specified — use find_runs or ask the user which one"}, None
    it = _status.get_item(Key={"session_id": sid}).get("Item")
    if not it:
        return {"error": f"no run '{sid}'"}, None
    nodes = {k: (v or {}).get("status") for k, v in (it.get("nodes") or {}).items()}
    return {"session_id": sid, "topic": it.get("topic"), "overall": it.get("overall"),
            "awaiting_review_at": (it.get("hitl") or {}).get("node"),
            "created": it.get("created"), "updated_at": it.get("updated_at"),
            "agents": nodes}, None


def _t_sessions(args, ctx, si):
    q = (args.get("query") or "").lower().strip()
    limit = int(args.get("limit") or 5)
    kw = {"ProjectionExpression": "session_id, topic, overall, created"}
    if _OWNER is not None:
        kw["FilterExpression"] = Attr("owner").eq(_OWNER)
    items = _scan_all(_status, **kw)
    if q:
        items = [it for it in items if q in str(it.get("topic", "")).lower()]
    items.sort(key=lambda x: str(x.get("created", "")), reverse=True)
    return {"count": len(items), "runs": [
        {"session_id": it.get("session_id"), "topic": it.get("topic"),
         "overall": it.get("overall"), "created": it.get("created")}
        for it in items[:limit]]}, None


def _t_output(args, ctx, si):
    sid, aid = _sid(args, ctx), args.get("agent_id", "")
    if not sid or not aid:
        return {"error": "session_id and agent_id required"}, None
    it = _status.get_item(Key={"session_id": sid}).get("Item") or {}
    hist = (it.get("history") or {}).get(aid) or []
    if hist:
        rec = hist[-1]
        return {"agent_id": aid, "version": rec.get("version"), "versions": len(hist),
                "output": str(rec.get("output", ""))[:4000]}, None
    out = ((it.get("nodes") or {}).get(aid) or {}).get("output", "")
    return {"agent_id": aid, "output": str(out)[:4000] or "(no output yet)"}, None


def _t_costs(args, ctx, si):
    sid = _sid(args, ctx)
    rows = _tel_rows(sid)
    if not rows:
        return {"error": "no telemetry for this run"}, None
    per, total = {}, 0.0
    for r in rows:
        c = _num(r.get("cost_usd"))
        per[r.get("agent_id", "?")] = per.get(r.get("agent_id", "?"), 0.0) + c
        total += c
    return {"session_id": sid, "totalCostUsd": round(total, 4),
            "byAgent": {k: round(v, 4) for k, v in sorted(per.items(), key=lambda x: -x[1])}}, None


def _t_latency(args, ctx, si):
    sid, aid = _sid(args, ctx), args.get("agent_id")
    rows = _tel_rows(sid)
    if not rows:
        return {"error": "no telemetry for this run"}, None
    per = {}
    for r in rows:
        a = r.get("agent_id", "?")
        if aid and a != aid:
            continue
        per[a] = per.get(a, 0) + int(_num(r.get("latency_ms")))
    return {"session_id": sid, "latencyMsByAgent": per, "totalMs": sum(per.values())}, None


def _t_guardrails(args, ctx, si):
    sid, aid = _sid(args, ctx), args.get("agent_id")
    rows = [r for r in _tel_rows(sid)
            if r.get("kind") == "guardrail" and (not aid or r.get("agent_id") == aid)]
    return {"session_id": sid, "count": len(rows), "checks": [
        {"agent_id": r.get("agent_id"), "source": r.get("label"), "action": r.get("status"),
         "detail": str(r.get("output_text", ""))[:300]} for r in rows[-20:]]}, None


def _t_evals(args, ctx, si):
    sid, aid, prm = _sid(args, ctx), args.get("agent_id"), args.get("prompt")
    rows = [r for r in _tel_rows(sid)
            if r.get("kind") == "eval" and (not aid or r.get("agent_id") == aid)
            and (not prm or r.get("prompt") == prm)]
    rows.sort(key=lambda x: str(x.get("sk", "")))
    return {"session_id": sid, "count": len(rows), "evals": [
        {"agent_id": r.get("agent_id"), "prompt": r.get("prompt"),
         "version": int(_num(r.get("version"))), "evaluator": r.get("label"),
         "score": _num(r.get("value")), "explanation": str(r.get("output_text", ""))[:300]}
        for r in rows[-25:]]}, None


def _t_run_eval(args, ctx, si):
    sid, aid = _sid(args, ctx), args.get("agent_id", "")
    if not sid or not aid:
        return {"error": "session_id and agent_id required"}, None
    # Never tell the user "Started evaluation" for an agent that has not enabled it.
    # The runtime declines it (the gate is in evaluations.evaluate_agent), so without
    # this the assistant confidently reported starting work that never ran.
    if aid not in (WORKFLOW.get("evalAgents") or []):
        return ({"error": f"evaluations are not enabled for {aid}",
                 "hint": "agentcore.evaluations.enabled in workflow.json"},
                None)
    si({"action": "evaluate", "session_id": sid, "agent_id": aid, "prompt": args.get("prompt", "")})
    return {"status": "started", "agent_id": aid}, f"Started evaluation for {aid}"


def _t_review(args, ctx, si):
    sid = _sid(args, ctx)
    decision = (args.get("decision") or "approve").lower()
    comment = args.get("comment", "")
    if not sid:
        return {"error": "no run specified — use find_runs or ask which one"}, None
    if decision not in ("approve", "deny", "revise"):
        return {"error": "decision must be 'approve', 'deny', or 'revise'"}, None
    if decision == "revise" and not comment:
        return {"error": "a 'revise' decision needs a comment describing what to change"}, None
    it = _status.get_item(Key={"session_id": sid}).get("Item") or {}
    if it.get("overall") != "waiting_human":
        return {"error": f"run is '{it.get('overall')}', not awaiting review — nothing to approve"}, None
    gate = (it.get("hitl") or {}).get("node")
    si({"action": "resume", "session_id": sid, "decision": decision, "comment": comment})
    verb = {"approve": "Approved", "deny": "Denied", "revise": "Sent revision to"}[decision]
    return {"status": "submitted", "decision": decision, "gate": gate}, f"{verb} the {gate} review gate"


def _t_rerun(args, ctx, si):
    sid = _sid(args, ctx)
    raw = args.get("agents") or ([{"agent_id": args["agent_id"]}] if args.get("agent_id") else [])
    agents = [{"agentId": a.get("agent_id") or a.get("agentId"), "comment": a.get("comment", "")}
              for a in raw if (a.get("agent_id") or a.get("agentId"))]
    if not sid or not agents:
        return {"error": "session_id and at least one agent required"}, None
    if len(agents) == 1:
        si({"action": "rerun_from", "session_id": sid,
            "agent_id": agents[0]["agentId"], "comment": agents[0]["comment"]})
    else:
        si({"action": "rerun_from", "session_id": sid, "agents": agents})
    names = ", ".join(a["agentId"] for a in agents)
    return {"status": "started", "agents": agents}, f"Re-running {names}"


# --- tool registry (config key -> Converse toolSpec + impl) ----------------

_SID_PROP = {"session_id": {"type": "string",
                            "description": "Run session id. Omit to use the run currently open in the UI."}}

_REGISTRY = {
    "status": {"name": "get_status", "fn": _t_status,
               "description": "Get a run's overall status, which gate it's waiting at "
                              "(if any), and each agent's status.",
               "schema": {"type": "object", "properties": _SID_PROP}},
    "sessions": {"name": "find_runs", "fn": _t_sessions,
                 "description": "List/search past workflow runs (most recent first). Use 'query' to match a topic.",
                 "schema": {"type": "object", "properties": {
                     "query": {"type": "string", "description": "Optional text to match against the run topic."},
                     "limit": {"type": "integer", "description": "Max results (default 5)."}}}},
    "outputs": {"name": "get_agent_output", "fn": _t_output,
                "description": "Get the latest output an agent produced in a run.",
                "schema": {"type": "object", "properties": {**_SID_PROP,
                           "agent_id": {"type": "string",
                                        "description": "Agent id (see the agent list "
                                                       "in the system prompt)."}},
                           "required": ["agent_id"]}},
    "costs": {"name": "get_costs", "fn": _t_costs,
              "description": "Get the cost (USD) of a run, total and per agent.",
              "schema": {"type": "object", "properties": _SID_PROP}},
    "latency": {"name": "get_latency", "fn": _t_latency,
                "description": "Get latency (ms) for a run, total and per agent (optionally one agent).",
                "schema": {"type": "object", "properties": {**_SID_PROP,
                           "agent_id": {"type": "string", "description": "Optional single agent id."}}}},
    "guardrails": {"name": "get_guardrails", "fn": _t_guardrails,
                   "description": "Get guardrail checks for a run — whether input/output passed or was blocked.",
                   "schema": {"type": "object", "properties": {**_SID_PROP,
                              "agent_id": {"type": "string", "description": "Optional single agent id."}}}},
    "evals": {"name": "get_evals", "fn": _t_evals,
              "description": "Get AgentCore evaluation results (evaluator scores + "
                             "explanations) for a run, optionally by agent/prompt.",
              "schema": {"type": "object", "properties": {**_SID_PROP,
                         "agent_id": {"type": "string", "description": "Optional single agent id."},
                         "prompt": {"type": "string", "description": "Optional named prompt id."}}}},
    "runEval": {"name": "run_evaluation", "fn": _t_run_eval, "action": True,
                "authz": "evaluate",
                "description": "Trigger an AgentCore evaluation for one agent's run (scores all its prompts).",
                "schema": {"type": "object", "properties": {**_SID_PROP,
                           "agent_id": {"type": "string", "description": "Agent id to evaluate."},
                           "prompt": {"type": "string", "description": "Optional single named prompt to evaluate."}},
                           "required": ["agent_id"]}},
    "review": {"name": "submit_review", "fn": _t_review, "action": True,
               "authz": "decision",
               "description": "Approve, revise, or deny the human-review gate a run is "
                              "currently paused at. approve = continue the workflow; "
                              "revise (needs a comment) = re-run the gated agent with "
                              "that feedback; deny = halt the run.",
               "schema": {"type": "object", "properties": {**_SID_PROP,
                          "decision": {"type": "string", "enum": ["approve", "deny", "revise"],
                                       "description": "approve | revise | deny"},
                          "comment": {"type": "string",
                                      "description": "Feedback — required when the "
                                                     "decision is 'revise'."}},
                          "required": ["decision"]}},
    "rerun": {"name": "rerun_agents", "fn": _t_rerun, "action": True,
              "authz": "rerun",
              "description": "Re-run one or more agents of a run with optional feedback; "
                             "downstream stages regenerate. Several agents can only be "
                             "re-run together when they belong to the same parallel stage.",
              "schema": {"type": "object", "properties": {**_SID_PROP,
                         "agents": {"type": "array", "description": "Agents to re-run.",
                                    "items": {"type": "object", "properties": {
                                        "agent_id": {"type": "string"},
                                        "comment": {"type": "string",
                                                    "description": "Optional feedback "
                                                                   "for that agent."}},
                                        "required": ["agent_id"]}}},
                         "required": ["agents"]}},
}


def _enabled_tools(permitted: list[str] | None = None) -> dict:
    """The registry, minus tools config disabled and minus action tools the caller
    is not authorized for.

    Two independent filters:
      * TOOLS_CFG  — what this deployment offers at all (default: everything).
      * permitted  — what THIS caller may do, from bff/authz.py. An entry carrying
        an "authz" key is a mutating action; it is withheld unless that action is
        in `permitted`. Withholding beats refusing in the prompt: a tool the model
        was never given cannot be talked into being called.
    `permitted = None` means "no authorization context" and keeps every enabled
    tool, which is the behaviour when authorization is not configured.
    """
    out = {k: v for k, v in _REGISTRY.items() if TOOLS_CFG.get(k, True)}
    if permitted is None:
        return out
    return {k: v for k, v in out.items()
            if not v.get("authz") or v["authz"] in permitted}


def _tool_config(enabled: dict) -> dict:
    return {"tools": [{"toolSpec": {"name": v["name"], "description": v["description"],
                                    "inputSchema": {"json": v["schema"]}}} for v in enabled.values()]}


def _by_tool_name(enabled: dict) -> dict:
    return {v["name"]: v for v in enabled.values()}


def _authz_rule(permitted: list[str] | None) -> str:
    """A prompt line naming the actions withheld from this caller.

    The withholding in _enabled_tools() is the control (the tool is simply absent).
    This only stops the assistant guessing at WHY it cannot act — without it, a
    reader with no permissions is told "I can't do that" with no reason. Derived
    from `permitted`, not from the enabled set, so a capability switched off in
    config is never mis-reported as an authorization problem.
    """
    if permitted is None:
        return ""
    wanted = {v["authz"] for k, v in _REGISTRY.items()
              if v.get("authz") and TOOLS_CFG.get(k, True)}
    missing = sorted(wanted - set(permitted))
    if not missing:
        return ""
    phrase = {"decision": "approve, revise or deny a review gate",
              "rerun": "re-run agents", "evaluate": "start an evaluation"}
    return ("- This user is NOT authorized to " +
            "; ".join(phrase.get(m, m) for m in missing) +
            ". You have no tool for that; if asked, say they lack permission and should "
            "contact whoever administers access. Never claim you did it.\n")


def _system_prompt(ctx_session: str, enabled: dict,
                   permitted: list[str] | None = None) -> str:
    agents = "\n".join(f"  - {aid}: {a.get('name', aid)}" for aid, a in AGENTS.items())
    caps = ", ".join(v["name"] for v in enabled.values())
    ctx_line = ""
    if ctx_session and _owns(ctx_session):
        it = _status.get_item(Key={"session_id": ctx_session}).get("Item") or {}
        if it:
            ctx_line = (f"\nThe run currently open in the UI is session_id='{ctx_session}' "
                        f"(topic: {it.get('topic', '?')}, status: {it.get('overall', '?')}). "
                        f"Use it when the user says 'this/the run' and gives no other id.")
    return (
        # The product name comes from workflow.json `ui.title` — the same key the page
        # title and header use. Hardcoding it told a customer's users they were talking
        # to something the deployment isn't called.
        f"You are the in-app assistant for {UI.get('title') or 'this multi-agent workflow'}, "
        "built on Amazon Bedrock AgentCore. You help users understand and operate their "
        "workflow runs.\n\n"
        "You can ONLY use the provided tools to answer; never invent numbers, statuses, or outputs. "
        f"Available tools: {caps}.\n\n"
        # The ids AND the display names both come from workflow.json, so the mapping
        # instruction stays generic: naming this sample's agents here would tell a
        # customer's deployment to look for agents it does not have.
        "Agents in this workflow (id: name). Map whatever the user calls an agent — its "
        "display name, a shortened form, or the step it belongs to — to the matching "
        "id below:\n" + agents + "\n"
        + ctx_line + "\n\n"
        "Rules:\n"
        "- Answer ONLY questions about THIS application: its runs, agents, outputs, costs, latency, "
        "guardrails, evaluations, and the actions you can take.\n"
        "- If asked anything outside this scope (general knowledge, coding help, world facts, chit-chat, "
        "other products), politely decline in one sentence and say what you can help with instead.\n"
        "- When the user asks you to run, re-run, evaluate, or to approve/revise/deny a run's review "
        "gate, CALL the matching action tool, then confirm exactly what you did (actions run "
        "asynchronously; results appear in the UI shortly).\n"
        + _authz_rule(permitted) +
        "- If no run is specified and none is open, use find_runs or ask which run.\n"
        "- Be concise. Prefer plain sentences; use short bullet lists for per-agent breakdowns.")


def _to_messages(history: list, message: str) -> list:
    msgs = []
    for m in (history or [])[-10:]:
        role = "assistant" if m.get("role") == "assistant" else "user"
        text = str(m.get("text") or m.get("content") or "").strip()
        if text:
            msgs.append({"role": role, "content": [{"text": text[:4000]}]})
    msgs.append({"role": "user", "content": [{"text": message[:4000]}]})
    return msgs


def _to_native(obj):
    """Recursively convert DynamoDB Decimals to int/float.

    Tool results are DynamoDB items fed back to Bedrock Converse as a toolResult
    `json` block. botocore rejects Decimal (only str/int/bool/float/list/dict are
    valid document types), so an unconverted Decimal (e.g. a `version` or cost
    field) would raise ParamValidationError on the NEXT converse call."""
    if isinstance(obj, Decimal):
        return int(obj) if obj == obj.to_integral_value() else float(obj)
    if isinstance(obj, dict):
        return {k: _to_native(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_native(v) for v in obj]
    return obj


def _converse(model, system, messages, tool_config):
    """Bedrock Converse with a light retry on transient errors. Raises on final
    failure; the caller turns that into a friendly reply rather than a blank 500."""
    transient = {"ThrottlingException", "TooManyRequestsException",
                 "ServiceUnavailableException", "ModelTimeoutException",
                 "InternalServerException"}
    last: Exception | None = None
    # Converse rejects an EMPTY tools list, which is reachable if a deployment
    # disables every read capability and the caller is authorized for no actions.
    # Omit toolConfig entirely in that case so the assistant still answers (it just
    # has nothing to look anything up with) instead of returning a ValidationError.
    extra = {"toolConfig": tool_config} if tool_config.get("tools") else {}
    for attempt in range(3):
        try:
            return _bedrock.converse(
                modelId=model, system=system, messages=messages, **extra,
                inferenceConfig={"maxTokens": 1500, "temperature": 0},
            )
        except ClientError as e:
            last = e
            if e.response.get("Error", {}).get("Code", "") in transient:
                time.sleep(0.6 * (attempt + 1))
                continue
            raise
    raise last  # type: ignore[misc]


def handle_chat(body: dict, ctx_session: str, self_invoke,
                permitted: list[str] | None = None, owner: str | None = None,
                target: dict | None = None) -> dict:
    """One chat turn. `target` (a deployed build's tables and workflow view, from
    bff/handler.py) points every tool at that build instead of this deployment."""
    global _status, _tel, WORKFLOW, AGENTS
    saved = (_status, _tel, WORKFLOW, AGENTS)
    if target:
        _status, _tel = target["status"], target["telemetry"]
        WORKFLOW = target["view"]
        AGENTS = WORKFLOW.get("agents") or {}
    try:
        return _chat_turn(body, ctx_session, self_invoke, permitted, owner)
    finally:
        _status, _tel, WORKFLOW, AGENTS = saved


def _chat_turn(body: dict, ctx_session: str, self_invoke,
               permitted: list[str] | None, owner: str | None) -> dict:
    """Run one chat turn (may involve several tool calls). Returns {reply, actions}.

    `permitted` is the caller's authorized actions (bff/authz.py). The caller always
    supplies it; None means "unauthenticated context, no restrictions" and exists so
    this loop stays testable without a JWT.
    """
    global _OWNER
    _OWNER = owner
    message = (body.get("message") or "").strip()
    if not message:
        return {"error": "message required"}
    enabled = _enabled_tools(permitted)
    tool_config = _tool_config(enabled)
    by_name = _by_tool_name(enabled)
    model = CHATBOT.get("model") or DEFAULT_MODEL
    system = [{"text": _system_prompt(ctx_session, enabled, permitted)}]
    messages = _to_messages(body.get("history"), message)
    actions: list[str] = []

    for _ in range(MAX_TURNS):
        try:
            resp = _converse(model, system, messages, tool_config)
        except Exception as e:  # noqa: BLE001 - return a clean reply, never a blank 500
            return {"reply": "The assistant couldn't reach the model right now "
                             f"({type(e).__name__}). Please try again in a moment.",
                    "actions": actions}
        out = resp["output"]["message"]
        messages.append(out)
        if resp.get("stopReason") == "tool_use":
            results = []
            for blk in out.get("content", []):
                tu = blk.get("toolUse")
                if not tu:
                    continue
                spec = by_name.get(tu["name"])
                try:
                    named = _sid(tu.get("input") or {}, ctx_session)
                    if not spec:
                        result, action = {"error": f"unknown tool {tu['name']}"}, None
                    elif spec["fn"] is not _t_sessions and named and not _owns(named):
                        # Someone else's run reads exactly like a missing one.
                        result, action = {"error": f"no run '{named}'"}, None
                    else:
                        result, action = spec["fn"](tu.get("input") or {}, ctx_session, self_invoke)
                except Exception as e:  # noqa: BLE001 - never crash the chat
                    result, action = {"error": f"{type(e).__name__}: {e}"}, None
                if action:
                    actions.append(action)
                results.append({"toolResult": {"toolUseId": tu["toolUseId"],
                                               "content": [{"json": _to_native(result)}]}})
            messages.append({"role": "user", "content": results})
            continue
        text = "".join(b.get("text", "") for b in out.get("content", []) if "text" in b)
        return {"reply": text.strip() or "(no response)", "actions": actions}

    return {"reply": "I did a few lookups but couldn't wrap that up — try narrowing the question.",
            "actions": actions}
