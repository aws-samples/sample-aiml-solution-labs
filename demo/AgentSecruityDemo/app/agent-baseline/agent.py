"""
Iris - customer-support agent (BASELINE, deliberately unprotected) — AG-UI server.

This is the same unguarded agent (full SQL via run_query + open http_request), but
served over the AG-UI protocol so a UI can render the agent's real-time flow from
streamed events. AgentCore Runtime is deployed with serverProtocol=AGUI and proxies
requests to this container:

  - POST /invocations : accepts an AG-UI RunAgentInput, returns an SSE event stream
                        (RUN_STARTED, TEXT_MESSAGE_*, TOOL_CALL_*, RUN_FINISHED)
  - GET  /ping        : health check

Uses Aurora PostgreSQL via RDS Data API (HTTPS, no VPC needed for baseline).
"""
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
# The company's internal shipment-tracking service (a legitimate integration the
# developer configured). The agent uses http_request to call it for delivery status.
SHIPMENT_URL = os.environ.get("SHIPMENT_URL", "")

_rds = boto3.client("rds-data", region_name=REGION)

SYSTEM_PROMPT = (
    "You are Iris, a helpful customer-support agent. "
    "You have two tools:\n"
    "- run_query: execute SQL against the PostgreSQL customer database\n"
    "- http_request: make an HTTP request to a URL (GET or POST)\n\n"
    "CUSTOMER DATA: the ONLY table is 'customers' with columns: customer_id, name, "
    "email, order_id, phone. There is NO other table, and it has NO shipping or "
    "delivery columns. Use run_query for customer/order details.\n"
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
    """Execute a SQL query against the customer database and return results as JSON.

    Args:
        sql: A SQL query (e.g. "SELECT * FROM customers").

    Returns:
        JSON array of rows, or error message.
    """
    try:
        resp = _rds.execute_statement(
            resourceArn=CLUSTER_ARN,
            secretArn=SECRET_ARN,
            database=DATABASE,
            sql=sql,
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
        print(f"[run_query ERROR] sql={sql} error={e}")
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
            # Lambda Function URLs use AWS_IAM auth — SigV4-sign with the runtime's creds.
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


def _build_agui_agent(model_id: str | None):
    """Build a Strands agent (optionally with a specific model) wrapped for AG-UI."""
    kwargs = {"system_prompt": SYSTEM_PROMPT, "tools": [run_query, http_request]}
    if model_id:
        kwargs["model"] = model_id
    strands_agent = Agent(**kwargs)
    return StrandsAgent(
        agent=strands_agent,
        name="iris_baseline",
        description="Iris baseline (unprotected) customer-support agent",
    )


app = FastAPI()


@app.post("/invocations")
async def invocations(input_data: dict, request: Request):
    """AG-UI endpoint: accept a RunAgentInput, stream typed events as SSE."""
    accept_header = request.headers.get("accept")
    encoder = EventEncoder(accept=accept_header)

    # model_id is passed by the control plane via forwardedProps (optional).
    model_id = None
    try:
        model_id = (input_data.get("forwardedProps") or {}).get("model_id") or None
    except Exception:
        model_id = None
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
