"""This deployment's workflow, served over A2A (Agent2Agent) to another team's agent.

AgentExpress already CALLS A2A agents (`runtime: "a2a"`, app/common/a2a_agent.py). This is
the other direction: an agent this deployment does not own discovers the workflow from an
Agent Card, sends it a message, polls the task and reads the result. A run maps onto an
A2A Task almost one for one, so nothing new runs here: a message starts an ordinary run,
the same one Start run starts, and a task is read off its status row.

    GET  /api/a2a/.well-known/agent-card.json   discovery
    POST /api/a2a                                JSON-RPC 2.0: message/send, tasks/get, tasks/cancel

The protocol is A2A v0.3.0 (https://a2a-protocol.org/v0.3.0/specification/), the version
a2a_lambda/handler.py serves and app/common/a2a_agent.py speaks, so this deployment's own
client can call it (tests/test_a2a_server.py does exactly that).

OFF UNLESS THE DEPLOYMENT EXPOSES IT. `expose_a2a` / `-c exposeA2a=true` creates the two
routes and sets A2A_ENABLED. Without it the routes do not exist, and if they somehow did
this module would still answer 404, because it has no client ids to accept.

WHO MAY CALL. Machines, signed in with the deployment's own identity provider: never a key
minted here. With a Cognito pool this deployment creates, the IaC adds an app client that
uses OAuth2 client credentials with the custom scope `a2a/invoke`. The two routes sit
behind their OWN JWT authorizer, whose audience is that client and nothing else, and they
require the scope. So the people's sign-in client cannot reach these routes (its tokens
have the wrong audience and no such scope) and the A2A client cannot reach any other route
(the main authorizer's audience is the sign-in client). This module checks the same two
things again from the claims, and handler.py refuses an A2A client's token on every other
route, so one IaC mistake does not open either door.

WHAT AN A2A RUN IS
  * Owned by `a2a:<client id>`, so it is private to that client, like any run is private to
    its starter. It is held to `orchestrator.limits` under that name (a 429 becomes a
    JSON-RPC error) and its start, end and cancel are in the activity log.
  * Its review gates WAIT for a person. A2A has `input-required` for "the agent needs more
    from the caller", but answering a gate is a reviewer's decision about the work, not more
    input from whoever asked for it, so the task reports `input-required` with a message
    saying a person decides in the console, and a message on that task is refused. The
    people who decide are the deployment's `a2a_approvers` groups: they see A2A runs on the
    Runs page the way a service trigger's approvers see its runs. With none set, the run
    waits until the gate's own timeout, if the workflow gives it one.
"""
from __future__ import annotations

import base64
import json
import os
import re

import audit
import builds
import clock

#: The A2A protocol version, as the stand-in server and our own client declare it.
PROTOCOL_VERSION = "0.3.0"
#: Where the RPC endpoint is. Under /api/ so CloudFront's existing /api/* behaviour reaches
#: it, and the app's own URL is the agent's URL: no second origin or distribution.
RPC_PATH = "/api/a2a"
CARD_PATH = "/.well-known/agent-card.json"
#: The route keys, exactly as both IaC paths declare them (tests pin all three together).
ROUTES = (f"GET {RPC_PATH}{CARD_PATH}", f"POST {RPC_PATH}")


def _json_list(name: str) -> list[str]:
    try:
        got = json.loads(os.environ.get(name) or "[]")
    except ValueError:
        return []
    return [str(v) for v in got if str(v).strip()] if isinstance(got, list) else []


ENABLED = os.environ.get("A2A_ENABLED", "") == "true"
#: The app clients whose tokens may call. From the IaC, never from a request.
CLIENT_IDS = frozenset(_json_list("A2A_CLIENT_IDS"))
#: The scope a token must carry, e.g. "a2a/invoke".
SCOPE = os.environ.get("A2A_SCOPE", "")
#: Where a caller gets a token, for the card's securitySchemes. "" when the IaC does not know it.
TOKEN_URL = os.environ.get("A2A_TOKEN_URL", "")
#: Groups whose members read and decide A2A runs in the console.
APPROVERS = frozenset(_json_list("A2A_APPROVERS"))

#: The largest request body accepted. A request is at most TOPIC_MAX characters of text,
#: and a JSON-RPC envelope around it does not need more than this.
MAX_BODY = 64 * 1024
#: bff/handler.py TOPIC_MAX: an A2A request is held to what the Start run box accepts.
TOPIC_MAX = 10000
TASK_ID_RE = re.compile(r"^[0-9a-f]{12}$")
CONTEXT_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
CLIENT_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
METHODS = ("message/send", "tasks/get", "tasks/cancel")

# JSON-RPC 2.0 codes, and A2A's own (spec section 8.2).
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603
TASK_NOT_FOUND = -32001
TASK_NOT_CANCELABLE = -32002
UNSUPPORTED_OPERATION = -32004
CONTENT_TYPE_NOT_SUPPORTED = -32005
#: Server-defined (the spec reserves -32000..-32099 for these): orchestrator.limits refused
#: the run. `data.httpStatus` is 429, what the console gets for the same refusal.
LIMIT_REACHED = -32010

#: Run status (`overall`) -> A2A TaskState. `running` is split below into submitted/working.
STATES = {
    "waiting_human": "input-required",
    "done": "completed",
    # A reviewer said no: the agent declined to finish the task, which is A2A's `rejected`.
    "denied": "rejected",
    "failed": "failed",
    "cancelled": "canceled",
}
#: The run statuses after which nothing changes (bff/suites.py RUN_OUTCOMES).
TERMINAL = frozenset({"done", "denied", "failed", "cancelled"})
GATE_NOTE = ("This run is waiting at a review gate for a person to decide in the AgentExpress "
             "console. This agent does not take that decision over A2A; poll tasks/get until it "
             "moves on.")


# --- who is calling --------------------------------------------------------------------

def _claims(event: dict) -> dict:
    return (((event.get("requestContext") or {}).get("authorizer") or {}).get("jwt") or {}).get("claims") or {}


def client_of(claims: dict) -> str:
    """The OAuth client a token was issued to: `client_id` (Cognito access tokens), `azp`
    (Auth0, Entra v2), `cid` (Okta) or `appid` (Entra v1)."""
    for name in ("client_id", "azp", "cid", "appid"):
        if claims.get(name):
            return str(claims[name])
    return ""


def _scopes(claims: dict) -> set[str]:
    """`scope` (space-separated) or `scp`. API Gateway hands an array claim over as the
    string "[a b]", so the brackets are dropped before splitting."""
    raw = claims.get("scope") or claims.get("scp") or ""
    if isinstance(raw, list):
        return {str(s) for s in raw}
    return set(str(raw).strip().strip("[]").replace(",", " ").split())


def configured() -> bool:
    return ENABLED and bool(CLIENT_IDS) and bool(SCOPE)


def caller(event: dict) -> str:
    """The calling client's id when it may use A2A here, else "". The authorizer has
    verified the token; this checks it is one of OUR A2A clients and carries the scope."""
    if not configured():
        return ""
    claims = _claims(event)
    cid = client_of(claims)
    if not CLIENT_ID_RE.match(cid) or cid not in CLIENT_IDS or SCOPE not in _scopes(claims):
        return ""
    return cid


def is_a2a_token(event: dict) -> bool:
    """Is this request signed in as one of the A2A clients? handler.py refuses those on
    every route but the two here."""
    return configured() and client_of(_claims(event)) in CLIENT_IDS


def owner_of(client_id: str) -> str:
    return f"a2a:{client_id}"


def owners_for(groups: list[str]) -> list[str]:
    """The A2A run owners a person in `groups` may read and decide: every A2A client's,
    for a member of an approver group, else none."""
    if not configured() or not (APPROVERS & set(groups or [])):
        return []
    return sorted(owner_of(c) for c in CLIENT_IDS)


# --- the Agent Card --------------------------------------------------------------------

def agent_card(view: dict, base_url: str, version: int | None = None) -> dict:
    """Discovery, from the deployed workflow: one skill, which is running it.

    Built from the browser projection (bff/workflow.py VIEW), not the raw file: what a
    signed-in page may see is the most a signed-in machine is shown."""
    ui = view.get("ui") or {}
    title = str(ui.get("title") or "AgentExpress workflow")
    agents = view.get("agents") or {}
    names = [str((a or {}).get("name") or aid) for aid, a in agents.items() if isinstance(a, dict)]
    description = (f"Runs the {title} workflow: {', '.join(names) or 'its agents'}. A run may "
                   "pause at a review gate for a person in the console (task state "
                   "input-required); poll tasks/get for the result.")
    skill = {"id": "run-workflow", "name": title, "description": description,
             "tags": ["workflow", "multi-agent"],
             "inputModes": ["text/plain"], "outputModes": ["text/plain"]}
    if ui.get("defaultTopic"):
        skill["examples"] = [str(ui["defaultTopic"])]
    if TOKEN_URL:
        scheme = {"type": "oauth2",
                  "description": "OAuth2 client credentials from this deployment's identity provider.",
                  "flows": {"clientCredentials": {"tokenUrl": TOKEN_URL,
                                                  "scopes": {SCOPE: "Start, read and cancel runs of this workflow"}}}}
    else:
        scheme = {"type": "http", "scheme": "bearer", "bearerFormat": "JWT",
                  "description": f"An access token from this deployment's identity provider, with scope {SCOPE}."}
    return {
        "protocolVersion": PROTOCOL_VERSION,
        "name": title,
        "description": description,
        "url": base_url,
        "preferredTransport": "JSONRPC",
        # v0.3.0 names this list `additionalInterfaces`; later drafts and the stand-in server
        # say `supportedInterfaces`, which is what our own client reads first. Both, same URL.
        "additionalInterfaces": [{"transport": "JSONRPC", "url": base_url}],
        "supportedInterfaces": [{"transport": "JSONRPC", "url": base_url}],
        "provider": {"organization": title, "url": base_url},
        "version": str(version) if version is not None else "1.0.0",
        "capabilities": {"streaming": False, "pushNotifications": False, "stateTransitionHistory": False},
        "securitySchemes": {"oauth2": scheme},
        "security": [{"oauth2": [SCOPE]}],
        "defaultInputModes": ["text/plain"],
        "defaultOutputModes": ["text/plain"],
        "skills": [skill],
    }


# --- tasks -----------------------------------------------------------------------------

def state_of(item: dict) -> str:
    """The A2A state of a run's status row. Anything this code does not know is `unknown`,
    never an error: a status added later must not break every poll."""
    overall = str(item.get("overall") or "")
    if overall == "running":
        nodes = item.get("nodes") if isinstance(item.get("nodes"), dict) else {}
        started = any(isinstance(n, dict) and str(n.get("status") or "pending") != "pending"
                      for n in nodes.values())
        return "working" if started else "submitted"
    return STATES.get(overall, "unknown")


def _text_message(text: str, task_id: str, context_id: str) -> dict:
    return {"kind": "message", "role": "agent", "messageId": f"{task_id}-status",
            "taskId": task_id, "contextId": context_id, "parts": [{"kind": "text", "text": text}]}


def task_of(item: dict) -> dict:
    """A run as an A2A Task. On completion the run's result (the last agent's output) is
    the one artifact."""
    sid = str(item.get("session_id") or "")
    context_id = str((item.get("a2a") or {}).get("contextId") or sid)
    state = state_of(item)
    status: dict = {"state": state}
    result = item.get("result") if isinstance(item.get("result"), str) else ""
    if state == "input-required":
        gate = str((item.get("hitl") or {}).get("node") or "")
        status["message"] = _text_message(GATE_NOTE + (f" (gate: {gate})" if gate else ""), sid, context_id)
    elif state in ("failed", "rejected", "canceled") and result:
        status["message"] = _text_message(result[:1000], sid, context_id)
    task = {"kind": "task", "id": sid, "contextId": context_id, "status": status}
    if state == "completed" and result:
        task["artifacts"] = [{"artifactId": f"{sid}-result", "name": "result",
                              "parts": [{"kind": "text", "text": result}]}]
    return task


# --- JSON-RPC --------------------------------------------------------------------------

class RpcError(Exception):
    def __init__(self, code: int, message: str, data: dict | None = None):
        super().__init__(message)
        self.code, self.data = code, data


def _http(status: int, body: dict) -> dict:
    return {"statusCode": status,
            "headers": {"content-type": "application/json", "A2A-Version": PROTOCOL_VERSION},
            "body": json.dumps(body, default=str)}


def _ok(rpc_id, result: dict) -> dict:
    return _http(200, {"jsonrpc": "2.0", "id": rpc_id, "result": result})


def _err(rpc_id, e: RpcError) -> dict:
    # HTTP 200, as A2A says a JSON-RPC error is delivered. Our client reads both.
    error = {"code": e.code, "message": str(e)}
    if e.data:
        error["data"] = e.data
    return _http(200, {"jsonrpc": "2.0", "id": rpc_id, "error": error})


def _raw_body(event: dict) -> bytes:
    raw = event.get("body") or ""
    if event.get("isBase64Encoded"):
        try:
            return base64.b64decode(raw)
        except ValueError:
            raise RpcError(PARSE_ERROR, "Parse error - the body is not valid base64") from None
    return raw.encode() if isinstance(raw, str) else bytes(raw)


def _request_text(message: dict) -> str:
    """The run's request, from the message's parts: text as is, a data part as JSON (the
    same reading as a2a_lambda/handler.py). A file part is refused rather than dropped."""
    out: list[str] = []
    for part in message.get("parts") if isinstance(message.get("parts"), list) else []:
        if not isinstance(part, dict):
            raise RpcError(INVALID_PARAMS, "Invalid params - every part must be an object")
        kind = str(part.get("kind") or part.get("type") or "").lower()
        if kind in ("", "text") and isinstance(part.get("text"), str):
            out.append(part["text"])
        elif kind == "data" and part.get("data") is not None:
            out.append(json.dumps(part["data"], ensure_ascii=False))
        else:
            raise RpcError(CONTENT_TYPE_NOT_SUPPORTED,
                           "Incompatible content types - this agent takes text and data parts, "
                           f"not {kind or 'that part'}")
    return "\n".join(out).strip()


def _send(params: dict, owner: str, ip: str, start) -> dict:
    message = params.get("message")
    if not isinstance(message, dict) or not isinstance(message.get("parts"), list):
        raise RpcError(INVALID_PARAMS, "Invalid params - params.message with a parts list is required")
    if message.get("role") not in (None, "user"):
        raise RpcError(INVALID_PARAMS, 'Invalid params - message.role must be "user"')
    if message.get("taskId"):
        # The only reason to message an existing task here is to answer its gate, and that is
        # a reviewer's decision (see the module docstring).
        raise RpcError(UNSUPPORTED_OPERATION,
                       "This operation is not supported - a task takes no further messages; a person answers "
                       "its review gates in the console. Send a new message without taskId to start another run.")
    context_id = params.get("contextId") or message.get("contextId") or ""
    if context_id and (not isinstance(context_id, str) or not CONTEXT_ID_RE.match(context_id)):
        raise RpcError(INVALID_PARAMS, "Invalid params - contextId must be 1-128 of A-Z a-z 0-9 . _ : -")
    topic = _request_text(message)
    if not topic:
        raise RpcError(INVALID_PARAMS, "Invalid params - the message carries no text")
    if len(topic) > TOPIC_MAX:
        raise RpcError(INVALID_PARAMS, f"Invalid params - a request is at most {TOPIC_MAX} characters")
    extra = {"a2a": {"client": owner.removeprefix("a2a:"), **({"contextId": context_id} if context_id else {})}}
    try:
        sid = start(topic, owner, ip, extra)
    except builds.BuildError as e:
        if e.status == 429:
            raise RpcError(LIMIT_REACHED, str(e), {"httpStatus": 429}) from None
        raise RpcError(INTERNAL_ERROR, f"Internal error - {e}") from None
    if not sid:
        raise RpcError(INTERNAL_ERROR, "Internal error - the run could not be created")
    return {"kind": "task", "id": sid, "contextId": context_id or sid, "status": {"state": "submitted"}}


def _own_task(params: dict, owner: str, table) -> dict:
    task_id = params.get("id")
    if not isinstance(task_id, str) or not TASK_ID_RE.match(task_id):
        raise RpcError(INVALID_PARAMS, "Invalid params - id must be a task id this agent returned")
    item = table.get_item(Key={"session_id": task_id}).get("Item")
    # Someone else's run reads exactly like a missing one, as on every run route.
    if not item or item.get("owner") != owner:
        raise RpcError(TASK_NOT_FOUND, "Task not found")
    return item


def _cancel(params: dict, owner: str, ip: str, table) -> dict:
    item = _own_task(params, owner, table)
    if item.get("overall") in TERMINAL:
        raise RpcError(TASK_NOT_CANCELABLE, f"Task cannot be canceled - it is already {state_of(item)}")
    # What the console's Cancel does (handler.py): the running workflow polls the flag.
    table.update_item(
        Key={"session_id": item["session_id"]},
        UpdateExpression="SET cancel_requested = :c, overall = :o, hitl = :z, updated_at = :u",
        ExpressionAttributeValues={":c": True, ":o": "cancelled", ":z": None, ":u": clock.now_str()})
    if audit.enabled():
        try:
            audit.record(owner, "", "run.cancelled", session=item["session_id"], ip=ip, via="a2a")
        except Exception as e:  # noqa: BLE001 - never blocks the cancel
            print(f"[a2a] audit run.cancelled: {type(e).__name__}: {e}")
    return task_of({**item, "overall": "cancelled", "hitl": None})


def handle(event: dict, table, start, view: dict, version: int | None = None) -> dict:
    """Answer one request on ROUTES. `table` is this deployment's status table (None on a
    console that runs nothing), `start(topic, owner, ip, item_extra)` starts a run the way
    bff/handler.py _begin_run does and returns its id."""
    if not configured() or table is None:
        return _http(404, {"error": "A2A is not enabled on this deployment"})
    cid = caller(event)
    if not cid:
        # The token passed an authorizer but is not one of the A2A clients, or lacks the scope.
        print(f"[a2a] DENY client={client_of(_claims(event))[:128]!r}")
        return _http(403, {"error": f"an A2A client of this deployment with scope {SCOPE} is required"})
    ctx = event.get("requestContext") or {}
    base_url = f"https://{ctx.get('domainName') or ''}{RPC_PATH}"
    if event.get("routeKey") == ROUTES[0]:
        return _http(200, agent_card(view, base_url, version))

    rpc_id = None
    try:
        raw = _raw_body(event)
        if len(raw) > MAX_BODY:
            raise RpcError(INVALID_REQUEST, f"Invalid Request - the body is over {MAX_BODY // 1024} KB")
        try:
            body = json.loads(raw)
        except ValueError:
            raise RpcError(PARSE_ERROR, "Parse error - the body is not JSON") from None
        if not isinstance(body, dict):
            raise RpcError(INVALID_REQUEST, "Invalid Request - one JSON-RPC request object (batches are not supported)")
        raw_id = body.get("id")
        rpc_id = raw_id if isinstance(raw_id, (str, int)) and not isinstance(raw_id, bool) else None
        if body.get("jsonrpc") != "2.0":
            raise RpcError(INVALID_REQUEST, 'Invalid Request - "jsonrpc" must be "2.0"')
        method = body.get("method")
        if method not in METHODS:
            raise RpcError(METHOD_NOT_FOUND, f"Method not found - this agent implements {', '.join(METHODS)}")
        params = body.get("params")
        if not isinstance(params, dict):
            raise RpcError(INVALID_PARAMS, "Invalid params - params must be an object")
        owner = owner_of(cid)
        ip = str((ctx.get("http") or {}).get("sourceIp") or "")
        if method == "message/send":
            return _ok(rpc_id, _send(params, owner, ip, start))
        if method == "tasks/get":
            return _ok(rpc_id, task_of(_own_task(params, owner, table)))
        return _ok(rpc_id, _cancel(params, owner, ip, table))
    except RpcError as e:
        return _err(rpc_id, e)
