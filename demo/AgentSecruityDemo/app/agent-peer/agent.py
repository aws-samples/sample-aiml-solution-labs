"""
Iris A2A PEER — the "Orders" specialist agent (LAYER 6 · multi-agent / A2A).

This is a SEPARATE AgentCore runtime speaking the Agent-to-Agent (A2A) protocol
(JSON-RPC on port 9000, Agent Card at /.well-known/agent-card.json). The main Iris
agent delegates order lookups to it as a peer.

THE THREAT it demonstrates (OWASP Agentic T14 — trust escalation / confused deputy):
when the main agent is blocked from reading another customer's data directly, a
hijacked agent can just ASK A PEER. If the peer trusts a `customer_id` it receives as
a TOOL ARGUMENT from the caller, the caller can pass someone else's id and the peer
hands the data back — the peer becomes a "confused deputy" acting with its own
authority on the caller's behalf.

TWO MODES (A2A_PEER_MODE env):
  - "vuln" (default): the order_lookup tool TRUSTS the customer_id argument it is
    given. A caller can pass ANY customer_id → cross-customer read. This is the
    confused-deputy vulnerability.
  - "fix": the tool IGNORES any customer_id argument and uses ONLY the identity
    propagated with the request (the signed caller identity, here read from the
    A2A message context / a forwarded claim). It also RE-VALIDATES that the order
    belongs to that identity against the system of record before returning — so a
    forged/■mismatched id returns nothing. (Deck: propagate signed sub+act, drop
    userId as a callable param, re-validate the value from the record.)

Mock data only (no Aurora dependency) so the peer is self-contained; the point is the
identity-trust behaviour, not the datastore.
"""
import base64
import contextvars
import json
import logging
import os

from strands import Agent, tool
from strands.multiagent.a2a import A2AServer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("iris-peer")

# ---- SPIKE: can an A2A runtime read the inbound bearer / workload token in tool code? ----
# Strands' A2A executor drops HTTP headers before the agent runs, so a tool can't reach
# request.headers directly. But A2AServer.to_fastapi_app() returns a real FastAPI app, so
# we attach middleware that stashes the inbound auth headers into a contextvar the tool
# can read. This proves whether the "fix" mode can derive identity from the TOKEN.
_INBOUND_HEADERS = contextvars.ContextVar("inbound_headers", default={})
# The OBO caller token (a real Okta JWT, sub=verified caller) that Iris embeds as a data
# part in the A2A message — the reliable channel, since AgentCore strips custom headers.
_CALLER_TOKEN = contextvars.ContextVar("caller_token", default="")


def _decode_jwt_claims(token):
    """Best-effort decode of a JWT payload (no signature check — the AgentCore authorizer
    already validated it at the door). Returns {} on any failure."""
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return {}
        pad = parts[1] + "=" * (-len(parts[1]) % 4)
        return json.loads(base64.urlsafe_b64decode(pad))
    except Exception:
        return {}


def _token_sub():
    """The caller's identity from the inbound token. AgentCore's JWT authorizer consumes
    the OBO bearer at the door and re-injects it as the `WorkloadAccessToken` header
    (NOT `Authorization`), so we decode THAT first, then fall back to a raw bearer."""
    # Iris forwards its OBO token (a real, decodable Okta JWT with sub=verified caller)
    # as a DATA part in the A2A message — captured by middleware into _CALLER_TOKEN.
    # (AgentCore strips custom headers + the transport WorkloadAccessToken is an opaque
    # KMS blob, so the message body is the reliable channel.)
    tok = _CALLER_TOKEN.get() or ""
    if not tok:
        h = _INBOUND_HEADERS.get() or {}
        auth = h.get("authorization") or ""
        if auth.lower().startswith("bearer "):
            tok = auth[7:]
    if not tok:
        return None, {}
    claims = _decode_jwt_claims(tok)
    return (claims.get("customer_id") or claims.get("sub")), claims

REGION = os.environ.get("AWS_REGION", "us-east-1")
PEER_MODE = os.environ.get("A2A_PEER_MODE", "vuln").lower()   # "vuln" | "fix"
MODEL_ID = os.environ.get("PEER_MODEL_ID", "us.amazon.nova-lite-v1:0")

# The Orders service reads the SHARED Aurora `orders` table (same DB the gateway tools
# use), via the RDS Data API — NOT the gateway. That's the point of the confused-deputy
# beat: the peer is a SEPARATE authorization boundary that makes its OWN identity choice
# (vuln trusts the arg; fix uses the token sub). It just now reads REAL, consistent data
# instead of a hardcoded dict. The peer's scoped role already grants rds-data + secrets.
CLUSTER_ARN = os.environ.get("CLUSTER_ARN", "")
SECRET_ARN = os.environ.get("SECRET_ARN", "")
DATABASE_NAME = os.environ.get("DATABASE_NAME", "irisdb")

_rds = None


def _orders_for(customer_id, order_id=""):
    """Query the shared Aurora `orders` table for a customer's orders. The customer_id
    passed here is the EFFECTIVE identity the peer chose (arg in vuln, token sub in fix) —
    the query is deliberately scoped to whatever the peer decided to trust."""
    global _rds
    if not (CLUSTER_ARN and SECRET_ARN):
        log.error("PEER: no CLUSTER_ARN/SECRET_ARN — orders DB not configured")
        return []
    if _rds is None:
        import boto3
        _rds = boto3.client("rds-data", region_name=REGION)
    sql = "SELECT order_id, item, amount, status FROM orders WHERE customer_id = :cid"
    params = [{"name": "cid", "value": {"stringValue": customer_id}}]
    if order_id:
        sql += " AND order_id = :oid"
        params.append({"name": "oid", "value": {"stringValue": order_id}})
    sql += " ORDER BY order_id"
    resp = _rds.execute_statement(resourceArn=CLUSTER_ARN, secretArn=SECRET_ARN,
                                  database=DATABASE_NAME, sql=sql, parameters=params)
    rows = []
    for rec in resp.get("records", []):
        rows.append({"order_id": rec[0].get("stringValue", ""),
                     "item": rec[1].get("stringValue", ""),
                     "total": rec[2].get("stringValue", ""),
                     "status": rec[3].get("stringValue", "")})
    return rows

# The identity the A2A caller is ACTING FOR. In a full deployment this is the signed
# sub+act propagated via GetWorkloadAccessTokenForJWT and read from the request
# context; here the main agent forwards it and the peer reads it from an env/context
# shim. We keep it simple + explicit so the vuln vs fix behaviour is unambiguous.
def _caller_identity():
    # In "fix" mode the ONLY trusted source of who-we-act-for. (Wired from the
    # forwarded caller identity; falls back to the configured actor for local runs.)
    return os.environ.get("A2A_CALLER_CUSTOMER_ID", "") or "C-1001"


@tool
def order_lookup(customer_id: str = "", order_id: str = "") -> str:
    """Look up orders for a customer. (The main agent delegates order questions here.)

    Args:
        customer_id: the customer whose orders to return.
        order_id: optional specific order id to filter to.
    """
    token_sub, claims = _token_sub()
    # PROBE: confirm we received + decoded the forwarded caller token. We do NOT log the
    # token sub or the customer_id argument — the security-relevant fact is only WHETHER
    # the caller-supplied argument disagrees with the verified token identity, which is a
    # boolean. Claim NAMES (not values) are safe and are what you need to debug decoding.
    _h = _INBOUND_HEADERS.get() or {}
    log.info("PEER[spike] has_token_sub=%s arg_supplied=%s arg_differs_from_token=%s "
             "mode=%s caller_token(datapart)=%s claims=%s",
             bool(token_sub), bool(customer_id),
             bool(customer_id and customer_id != (token_sub or "")), PEER_MODE,
             bool(_CALLER_TOKEN.get()), list(claims.keys()))

    if PEER_MODE == "fix":
        # FIX: derive identity ONLY from the BEARER TOKEN's sub (validated by the
        # AgentCore JWT authorizer). IGNORE the caller-supplied customer_id argument.
        # Fail CLOSED — if there is no token identity, return nothing (do NOT fall back
        # to a default, which would silently act for someone).
        effective = token_sub
        if not effective:
            log.info("PEER[fix] no token identity — refusing (fail closed). arg_supplied=%s",
                     bool(customer_id))
            return "No caller identity in the request token — refusing the lookup. [fix: token sub required]"
        if customer_id and customer_id != effective:
            # The control fired. What matters for audit is THAT a mismatched argument was
            # ignored, not which customer was named.
            log.info("PEER[fix] ignoring caller-supplied customer_id (differs from token "
                     "sub); acting for the verified token identity only")
    else:
        # VULN: trust whatever customer_id the caller passed as the ARG (confused deputy).
        effective = customer_id or token_sub or _caller_identity()
        if customer_id and customer_id != (token_sub or ""):
            # The vulnerability fired. The alertable signal is the mismatch itself — a
            # cross-customer read — so we log that without naming either party.
            log.info("PEER[vuln] TRUSTING caller-supplied customer_id that DIFFERS from the "
                     "token sub — cross-customer read")

    try:
        rows = _orders_for(effective, order_id)
    except Exception as e:
        log.error("PEER: orders query failed: %s", e)
        return f"Orders lookup failed for {effective}: {e}"
    src = "token sub" if PEER_MODE == "fix" else "customer_id argument"
    if not rows:
        return f"No orders found for {effective}. [{PEER_MODE}: identity from {src}]"
    mode_note = (f"[fix: acted for token sub {effective} only]" if PEER_MODE == "fix"
                 else f"[vuln: acted on the customer_id argument {effective} as given]")
    return f"Orders for {effective} {mode_note}: " + "; ".join(
        f"{r['order_id']} — {r['item']} ({r['total']}, {r['status']})" for r in rows)


peer_agent = Agent(
    model=MODEL_ID,
    name="Orders Agent",
    description="A2A peer that looks up customer orders on behalf of the calling agent.",
    system_prompt=(
        "You are the Orders specialist agent. Answer order questions using ONLY the "
        "order_lookup tool. Return exactly what the tool returns; never invent orders."),
    tools=[order_lookup],
)

# A2A server — AgentCore A2A contract: port 9000, mounted at root, Agent Card at
# /.well-known/agent-card.json. serve_at_root=True because AgentCore proxies the
# runtime invocations path to the container root.
a2a = A2AServer(peer_agent, host="0.0.0.0", port=9000, serve_at_root=True)


def _extract_caller_token(body_bytes):
    """Pull the OBO caller token Iris embeds as a DATA part in the A2A message. AgentCore
    strips custom HTTP headers, so the token travels in the JSON-RPC message body:
      params.message.parts[] → {"kind":"data","data":{"iris_caller_token": "<jwt>"}}"""
    try:
        body = json.loads(body_bytes)
        parts = (((body.get("params") or {}).get("message") or {}).get("parts") or [])
        for p in parts:
            d = p.get("data") or {}
            if isinstance(d, dict) and d.get("iris_caller_token"):
                return d["iris_caller_token"]
    except Exception:
        pass
    return ""


def _build_app():
    """Serve via FastAPI so middleware can capture (a) inbound headers and (b) the OBO
    caller token embedded in the A2A message body — both into contextvars the tool reads.
    (Strands' A2A executor drops HTTP headers AND doesn't surface data parts to tools.)"""
    app = a2a.to_fastapi_app()

    @app.middleware("http")
    async def _capture(request, call_next):
        try:
            _INBOUND_HEADERS.set({k.lower(): v for k, v in request.headers.items()})
        except Exception:
            _INBOUND_HEADERS.set({})
        # Read + re-inject the body so we can extract the caller token without consuming
        # the stream downstream.
        try:
            raw = await request.body()
            _CALLER_TOKEN.set(_extract_caller_token(raw))
            async def _receive():
                return {"type": "http.request", "body": raw, "more_body": False}
            request._receive = _receive
        except Exception:
            _CALLER_TOKEN.set("")
        return await call_next(request)

    return app


app = _build_app()

if __name__ == "__main__":
    import uvicorn
    log.info("Iris A2A peer (Orders) starting on :9000 — mode=%s", PEER_MODE)
    uvicorn.run(app, host="0.0.0.0", port=9000)
