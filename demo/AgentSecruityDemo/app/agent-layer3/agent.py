"""
Iris - customer-support agent (LAYER 3: Okta OBO + Gateway + Policy) — AG-UI server.

Served over AG-UI so the UI can render the agent's real-time flow, WITHOUT giving
up the on-behalf-of (OBO) token exchange:

  1. The runtime is JWT-authorized (CUSTOM_JWT / Okta iris-agent) and forwards the
     inbound Okta access token to this container in the Authorization header
     (requestHeaderConfiguration allowlist).
  2. On each /invocations, the runtime injects the workload access token in the
     'WorkloadAccessToken' header (same as it does for BedrockAgentCoreApp). We
     read that header and seed it into BedrockAgentCoreContext — the exact
     ContextVar the @requires_access_token decorator reads. So the decorator runs
     UNCHANGED (we do not bypass it, and we do NOT re-fetch the token — runtime
     identities are barred from GetWorkloadAccessTokenForJWT).
  3. @requires_access_token(ON_BEHALF_OF_TOKEN_EXCHANGE) exchanges (RFC 8693) the
     inbound token via the delegate credential provider for a token bound to the
     Gateway audience (aud=iris-gateway), customer_id preserved.
  4. The agent connects to the AgentCore Gateway MCP endpoint with the OBO token.
  5. Gateway validates aud=iris-gateway, runs Cedar policy, the REQUEST interceptor
     injects customer_id from the OBO JWT, then invokes the Lambda.
"""
import base64
import json
import logging
import os

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse
from strands import Agent
from strands.tools.mcp import MCPClient
from mcp.client.streamable_http import streamablehttp_client
from ag_ui_strands import StrandsAgent
from ag_ui.core import RunAgentInput
from ag_ui.encoder import EventEncoder
from bedrock_agentcore.identity.auth import requires_access_token
from bedrock_agentcore.runtime.context import BedrockAgentCoreContext

os.environ.setdefault("BYPASS_TOOL_CONSENT", "true")

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("iris-layer3")

REGION = os.environ.get("AWS_REGION", "us-east-1")
GATEWAY_ID = os.environ.get("GATEWAY_ID", "")
AGENT_TYPE = os.environ.get("AGENT_TYPE", "support")
OBO_PROVIDER_NAME = os.environ.get("OBO_PROVIDER_NAME", "")
GATEWAY_AUDIENCE = os.environ.get("OKTA_GATEWAY_AUDIENCE", "iris-gateway")
TOOL_SCOPE = os.environ.get("TOOL_SCOPE", "tool:read")

# Base rules that apply regardless of which tools this agent was granted.
SYSTEM_PROMPT_BASE = (
    "You are Iris, a customer-support agent. You can ONLY answer using data "
    "returned by the Gateway tools listed below. You MUST call a tool for every "
    "request. NEVER invent, guess, or infer order status, shipping, dates, or any "
    "details that are not present in a tool result. If a tool returns no data, "
    "say so plainly. Do not add narrative like 'being processed'.\n"
    "You have EXACTLY the tools listed below and NO others. If the user asks for "
    "something none of your tools can do, say you don't have a tool for that and "
    "stop — do NOT try to call a tool that isn't in your list, and do NOT invent "
    "tool names. Never set customer_id yourself; the gateway injects it from the "
    "verified token.\n"
)

# Per-tool usage hints — emitted ONLY for tools the agent actually received from
# the Gateway (Cedar filters tools/list by scope), so we never advertise a tool
# the agent can't call. Keyed by the bare tool name.
TOOL_HINTS = {
    "get_record": "- get_record(order_id): look up one of the caller's own orders.",
    "get_my_info": "- get_my_info(): return the caller's own customer record.",
    "get_shipment": ("- get_shipment(order_id): delivery/shipment status (shipped?, "
                     "tracking, carrier, ETA). This is the ONLY source of shipping data."),
    "update_record": ("- update_record(customer_id, field, value): update a customer "
                      "record. When asked to update/change/set a customer's details, "
                      "you MUST call this — never refuse it yourself."),
}


def _system_prompt_for(tool_names):
    """Build a system prompt that describes ONLY the tools this agent holds.

    tool_names are the bare names from the Gateway's tools/list. This guarantees
    the prompt never mentions a capability Cedar filtered out — so the model won't
    hallucinate a call to a tool it doesn't have (e.g. an admin trying get_shipment).
    """
    lines = [TOOL_HINTS[n] for n in tool_names if n in TOOL_HINTS]
    tools_block = "\n".join(lines) if lines else "(no tools available)"
    return SYSTEM_PROMPT_BASE + "Your available tools:\n" + tools_block


def _gateway_mcp_url():
    return f"https://{GATEWAY_ID}.gateway.bedrock-agentcore.{REGION}.amazonaws.com/mcp"


def _decode_jwt(token):
    """Decode a JWT payload (display/scoping only — no signature verification)."""
    try:
        p = token.split(".")[1]
        p += "=" * (-len(p) % 4)
        return json.loads(base64.urlsafe_b64decode(p))
    except Exception:
        return {}


# The runtime injects the workload access token into this header on every
# invocation (same as it does for BedrockAgentCoreApp). We do NOT call
# GetWorkloadAccessTokenForJWT ourselves — runtime-managed identities are barred
# from that ("Workload Identity does not belong to caller account"). We just read
# the injected header and seed the ContextVar the OBO decorator reads.
WORKLOAD_HEADER = "WorkloadAccessToken"


def _seed_workload_token(request):
    """Seed BedrockAgentCoreContext with the workload access token the runtime
    injected in the WorkloadAccessToken header, so @requires_access_token finds
    it — exactly what BedrockAgentCoreApp does internally."""
    wat = request.headers.get(WORKLOAD_HEADER)
    if not wat:
        raise ValueError(
            f"no '{WORKLOAD_HEADER}' header — the runtime did not inject a "
            "workload access token (is the runtime JWT-authorized?)")
    BedrockAgentCoreContext.set_workload_access_token(wat)
    return wat


async def _get_obo_token():
    """Get the OBO token via @requires_access_token — UNCHANGED from the HTTP
    agent. It reads the workload token we seeded above, then performs the
    ON_BEHALF_OF token exchange against the delegate credential provider."""
    holder = {}

    @requires_access_token(
        provider_name=OBO_PROVIDER_NAME,
        scopes=[TOOL_SCOPE],
        auth_flow="ON_BEHALF_OF_TOKEN_EXCHANGE",
        custom_parameters={
            "audience": GATEWAY_AUDIENCE,
            "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
        },
    )
    async def _inner(*, access_token: str):
        holder["token"] = access_token

    await _inner()
    return holder.get("token")


def _build_agui_agent(model_id, tools, system_prompt):
    kwargs = {"system_prompt": system_prompt, "tools": tools}
    if model_id:
        kwargs["model"] = model_id
    strands_agent = Agent(**kwargs)
    return StrandsAgent(
        agent=strands_agent,
        name=f"iris_layer3_{AGENT_TYPE}",
        description=f"Iris Layer 3 ({AGENT_TYPE}) — Gateway MCP tools via OBO",
    )


app = FastAPI()


@app.post("/invocations")
async def invocations(input_data: dict, request: Request):
    """AG-UI endpoint. Seeds the workload token from the forwarded JWT, performs
    the OBO exchange, connects to the Gateway MCP endpoint, and streams AG-UI
    events. forwardedProps carries model_id."""
    fp = (input_data.get("forwardedProps") or {}) if isinstance(input_data, dict) else {}
    model_id = fp.get("model_id") or None

    accept_header = request.headers.get("accept")
    encoder = EventEncoder(accept=accept_header)

    # Hand-crafted AG-UI SSE frames for error paths (bypass the typed encoder).
    import uuid as _uuid
    def _sse_err(message):
        rid = str(_uuid.uuid4()); mid = str(_uuid.uuid4())
        frames = [
            {"type": "RUN_STARTED", "threadId": rid, "runId": rid},
            {"type": "TEXT_MESSAGE_START", "messageId": mid, "role": "assistant"},
            {"type": "TEXT_MESSAGE_CONTENT", "messageId": mid, "delta": message},
            {"type": "TEXT_MESSAGE_END", "messageId": mid},
            {"type": "RUN_ERROR", "message": message},
            {"type": "RUN_FINISHED", "threadId": rid, "runId": rid},
        ]
        return "".join(f"data: {json.dumps(f)}\n\n" for f in frames)

    async def event_generator():
        # 1) seed the workload token (from the runtime-injected header) →
        # 2) OBO exchange (decorator) → 3) Gateway MCP
        try:
            _seed_workload_token(request)
            obo_token = await _get_obo_token()
        except Exception as e:
            log.error("OBO setup failed: %s", e, exc_info=True)
            yield _sse_err(f"OBO token exchange failed: {e}")
            return
        if not obo_token:
            yield _sse_err("OBO exchange returned no token.")
            return

        obo_claims = _decode_jwt(obo_token)
        # Do NOT log customer_id — logs are a downstream sink (CloudWatch, aggregators)
        # read by people not entitled to customer identifiers. Log only that the claim
        # was present; aud/agent_name/scp are not personal data and are what you need
        # to debug the exchange.
        log.info("OBO token: has_customer_id=%s aud=%s agent_name=%s scp=%s",
                 bool(obo_claims.get("customer_id")), obo_claims.get("aud"),
                 obo_claims.get("agent_name"), obo_claims.get("scp"))
        # Emit the exchanged OBO token claims as a CUSTOM AG-UI event so the UI can
        # render the token-flow panel + light the OBO node in the diagram.
        yield "data: " + json.dumps({"type": "CUSTOM", "name": "obo_token", "value": {
            "customer_id": obo_claims.get("customer_id"), "aud": obo_claims.get("aud"),
            "agent_name": obo_claims.get("agent_name"), "cid": obo_claims.get("cid"),
            "scp": obo_claims.get("scp"), "sub": obo_claims.get("sub"), "iss": obo_claims.get("iss"),
        }}) + "\n\n"

        headers = {"Authorization": f"Bearer {obo_token}"}
        mcp_client = MCPClient(lambda: streamablehttp_client(_gateway_mcp_url(), headers=headers))
        with mcp_client:
            tools = mcp_client.list_tools_sync()
            if not tools:
                yield _sse_err("No tools available from the Gateway — check Gateway/policy/OBO.")
                return
            # Cedar filters tools/list BY SCOPE — the Gateway only returns the tools
            # this agent is authorized to call (support→reads, admin→update). We emit
            # those names for the UI, AND build the system prompt from them so the
            # prompt never advertises a tool the agent doesn't hold (otherwise the
            # model would try to call, e.g., get_shipment it can't see).
            def _tool_name(t):
                for attr in ("tool_name", "name"):
                    v = getattr(t, attr, None)
                    if v:
                        return v
                spec = getattr(t, "tool_spec", None) or getattr(t, "mcp_tool", None)
                return getattr(spec, "name", None) or str(t)
            tool_names = [_tool_name(t) for t in tools]
            bare_names = [n.split("___").pop() if "___" in n else n for n in tool_names]
            log.info("Gateway tools/list returned to %s agent: %s", AGENT_TYPE, tool_names)
            yield "data: " + json.dumps({"type": "CUSTOM", "name": "tool_list", "value": {
                "agent": AGENT_TYPE, "scope": TOOL_SCOPE, "tools": tool_names,
            }}) + "\n\n"
            system_prompt = _system_prompt_for(bare_names)
            agui_agent = _build_agui_agent(model_id, tools, system_prompt)
            run_input = RunAgentInput(**input_data)
            async for event in agui_agent.run(run_input):
                yield encoder.encode(event)

    return StreamingResponse(event_generator(), media_type=encoder.get_content_type())


@app.get("/ping")
async def ping():
    return JSONResponse({"status": "Healthy"})


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
