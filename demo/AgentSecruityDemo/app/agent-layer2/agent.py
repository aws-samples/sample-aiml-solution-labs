"""
Iris - customer-support agent (LAYER 2: Identity-scoped) — AG-UI server.

Same shape as the baseline/Layer 1 AG-UI agent (run_query + http_request, served
over AG-UI so the UI can render the live flow), with ONE security change: the
runtime is JWT-authorized (Okta iris-agent) and run_query is SCOPED to the
authenticated user's customer_id. No matter what SQL the model emits or what the
prompt asks for, run_query only ever returns the caller's own record.

The caller identity (customer_id) is extracted from the VERIFIED Okta JWT that
the runtime forwards to this container in the Authorization header (enabled via
the runtime's requestHeaderConfiguration allowlist, permitted because the runtime
has a customJWTAuthorizer). The token — not the prompt, not the control plane — is
the source of truth for customer_id. run_query is scoped to that claim.
"""
import base64
import json
import os

import boto3
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse
from strands import Agent, tool
from ag_ui_strands import StrandsAgent
from ag_ui.core import RunAgentInput
from ag_ui.encoder import EventEncoder

os.environ.setdefault("BYPASS_TOOL_CONSENT", "true")

REGION = os.environ.get("AWS_REGION", "us-east-1")
CLUSTER_ARN = os.environ.get("CLUSTER_ARN", "")
SECRET_ARN = os.environ.get("SECRET_ARN", "")
DATABASE = os.environ.get("DATABASE_NAME", "irisdb")
SHIPMENT_URL = os.environ.get("SHIPMENT_URL", "")

_rds = boto3.client("rds-data", region_name=REGION)

# Set per-invocation from the caller's verified identity (forwardedProps).
_current_customer_id = None

SYSTEM_PROMPT = (
    "You are Iris, a helpful customer-support agent. "
    "You have two tools:\n"
    "- run_query: execute SQL against the PostgreSQL customer database\n"
    "- http_request: make an HTTP request to a URL (GET or POST)\n\n"
    "CUSTOMER DATA: the ONLY table is 'customers' with columns: customer_id, name, "
    "email, order_id, phone. There is NO other table, and it has NO shipping or "
    "delivery columns. Use run_query for customer/order details. Access is scoped to "
    "the authenticated user — run_query only ever returns that user's own record.\n"
    "Example queries:\n"
    "  SELECT * FROM customers WHERE order_id = 'A3X7K'\n\n"
    "SHIPMENT / DELIVERY STATUS: the customer database does NOT contain shipping "
    "information. Whenever the user asks anything about shipment, delivery, tracking, "
    "whether an order has shipped, its status, carrier, or ETA, you MUST call "
    "http_request (HTTP GET) against the shipment-tracking service — this is the ONLY "
    "source of shipping data:\n"
    f"  {SHIPMENT_URL}?order_id=<ORDER_ID>\n"
    "It returns JSON with status, carrier, eta, and last_location. NEVER answer a "
    "shipping/delivery question from run_query or from memory — always use http_request.\n\n"
    "Always execute the tools — do not just describe what you would do."
)


@tool
def run_query(sql: str) -> str:
    """Execute a SQL query against the customer database.

    Access is scoped to the authenticated user — only their record is returned,
    regardless of the SQL supplied.

    Args:
        sql: A SQL query (e.g. "SELECT * FROM customers").

    Returns:
        JSON array of rows (scoped to the current user), or error message.
    """
    global _current_customer_id
    if not _current_customer_id:
        return json.dumps({"error": "No authenticated user. Access denied."})
    # Force scoping to the authenticated user — ignore whatever SQL was generated.
    try:
        resp = _rds.execute_statement(
            resourceArn=CLUSTER_ARN, secretArn=SECRET_ARN, database=DATABASE,
            sql="SELECT customer_id, name, email, order_id, phone FROM customers WHERE customer_id = :cid",
            parameters=[{"name": "cid", "value": {"stringValue": _current_customer_id}}],
            includeResultMetadata=True,
        )
        columns = [col["name"] for col in resp.get("columnMetadata", [])]
        rows = []
        for record in resp.get("records", []):
            row = {}
            for i, field in enumerate(record):
                col_name = columns[i] if i < len(columns) else f"col{i}"
                if "stringValue" in field:
                    row[col_name] = field["stringValue"]
                elif "longValue" in field:
                    row[col_name] = field["longValue"]
                elif "booleanValue" in field:
                    row[col_name] = field["booleanValue"]
                elif "isNull" in field and field["isNull"]:
                    row[col_name] = None
                else:
                    row[col_name] = str(field)
            rows.append(row)
        return json.dumps(rows)
    except Exception as e:
        # No customer_id in logs — the error itself is what you need to debug.
        print(f"[run_query ERROR] scoped={bool(_current_customer_id)} error={e}")
        return f"query-error: {e}"


@tool
def http_request(url: str, method: str = "GET", body: str = None) -> str:
    """Make an HTTP request to a URL. Use this to call the shipment-tracking
    service for delivery status, or any other HTTP endpoint.

    Args:
        url: The URL to request.
        method: HTTP method — "GET" (default) or "POST".
        body: Optional request body (for POST), typically JSON.
    """
    import urllib.request
    import botocore.auth, botocore.awsrequest, botocore.session as bcs
    from urllib.parse import urlparse
    method = (method or "GET").upper()
    try:
        data = body.encode() if body else None
        parsed = urlparse(url)
        is_lambda_url = "lambda-url" in (parsed.hostname or "")
        if is_lambda_url:
            session = bcs.get_session()
            creds = session.get_credentials().get_frozen_credentials()
            aws_req = botocore.awsrequest.AWSRequest(
                method=method, url=url, data=data,
                headers={"Content-Type": "application/json"})
            botocore.auth.SigV4Auth(creds, "lambda", REGION).add_auth(aws_req)
            req = urllib.request.Request(url, data=data, method=method,
                                         headers=dict(aws_req.headers))
        else:
            req = urllib.request.Request(url, data=data, method=method,
                                         headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=20) as r:
            return f"{r.status} {r.read(2000).decode(errors='ignore')}"
    except Exception as e:
        print(f"[http_request ERROR] {method} url={url} error={e}")
        return f"request-failed: {e}"


def _customer_id_from_jwt(auth_header):
    """Extract customer_id from the forwarded Authorization: Bearer <jwt>.
    Decode-only (no signature check) — the runtime's customJWTAuthorizer already
    validated the token's issuer/signature/expiry before forwarding it here."""
    if not auth_header:
        return None
    token = auth_header.split(" ", 1)[1] if " " in auth_header else auth_header
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload))
        return claims.get("customer_id") or claims.get("custom:customerId")
    except Exception as e:
        print(f"[jwt decode ERROR] {e}")
        return None


def _build_agui_agent(model_id):
    kwargs = {"system_prompt": SYSTEM_PROMPT, "tools": [run_query, http_request]}
    if model_id:
        kwargs["model"] = model_id
    strands_agent = Agent(**kwargs)
    return StrandsAgent(
        agent=strands_agent,
        name="iris_layer2",
        description="Iris Layer 2 (identity-scoped) customer-support agent",
    )


app = FastAPI()


@app.post("/invocations")
async def invocations(input_data: dict, request: Request):
    """AG-UI endpoint. forwardedProps carries model_id + the caller's customer_id
    (from the validated Okta login) used to scope run_query."""
    global _current_customer_id
    fp = {}
    try:
        fp = input_data.get("forwardedProps") or {}
    except Exception:
        fp = {}
    # Source of truth: the VERIFIED JWT forwarded in the Authorization header.
    # Fall back to forwardedProps.customer_id only for local testing (no header).
    _current_customer_id = _customer_id_from_jwt(request.headers.get("authorization")) or fp.get("customer_id") or None
    model_id = fp.get("model_id") or None
    # Log THAT scoping resolved and from WHICH source (the security-relevant fact),
    # never the identity itself.
    print(f"[layer2] scoping resolved={bool(_current_customer_id)} "
          f"(from {'JWT' if request.headers.get('authorization') else 'forwardedProps'})")

    accept_header = request.headers.get("accept")
    encoder = EventEncoder(accept=accept_header)
    agui_agent = _build_agui_agent(model_id)

    async def event_generator():
        run_input = RunAgentInput(**input_data)
        async for event in agui_agent.run(run_input):
            yield encoder.encode(event)

    return StreamingResponse(event_generator(), media_type=encoder.get_content_type())


@app.get("/ping")
async def ping():
    return JSONResponse({"status": "Healthy"})


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
