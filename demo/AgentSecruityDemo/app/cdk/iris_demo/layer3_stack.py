"""
Layer 3 (Tools) stack — Gateway + Policy + Lambda tools.

Creates:
  1. Three tool Lambdas: get_record, get_info, update_record
  2. One REQUEST interceptor Lambda (identity injection — see INTERCEPTOR_CODE)
  3. AgentCore Gateway with CUSTOM_JWT authorizer (validates the OBO token)
  4. Policy Engine with Cedar policies (support read / admin update)
  5. Gateway targets pointing to the tool Lambdas

Identity flow (the key security property):
  - A Gateway Lambda target receives ONLY the tool arguments + gateway metadata.
    It never receives the caller's JWT or claims.
  - The REQUEST interceptor DOES see the verified OBO token (passRequestHeaders)
    and stamps customer_id (from the token) into the tool arguments, overwriting
    anything the model sent. So the tool Lambda scopes rows by an identity that
    came from the verified JWT, never from the prompt.

Requires baseline (Aurora) to be deployed first.
"""
from aws_cdk import (
    Stack, Tags, CfnOutput, Duration,
    aws_lambda as lambda_,
    aws_iam as iam,
)
from constructs import Construct


# ---------------------------------------------------------------------------
# REQUEST interceptor Lambda.
#
# WHY THIS EXISTS: a Gateway *Lambda target* only ever receives the tool input
# arguments (event) + gateway metadata (context) — it NEVER receives the caller's
# JWT or claims. So a tool Lambda cannot, on its own, learn who the caller is and
# would have to trust a customer_id from the prompt (forgeable by a hijacked
# agent). The REQUEST interceptor is the one component that sees BOTH the verified
# OBO token (via passRequestHeaders=true → headers.Authorization) AND the outgoing
# tool call. It decodes customer_id from the token and STAMPS it into the tool
# arguments, overwriting anything the model supplied. Identity therefore comes
# from the verified JWT, never from the prompt.
INTERCEPTOR_CODE = '''
import json, base64, logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DELIM = "___"  # gateway tool name is "<TargetName>___<tool_name>"

def _decode_jwt(token):
    """Decode a JWT payload (no signature check — the Gateway already validated
    the token before this interceptor ran)."""
    try:
        if token.lower().startswith("bearer "):
            token = token[7:]
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(payload))
    except Exception as e:
        logger.warning("jwt decode failed: %s", e)
        return {}

def _get_header(headers, name):
    for k, v in (headers or {}).items():
        if k.lower() == name.lower():
            return v
    return None

def lambda_handler(event, context):
    mcp = event.get("mcp", {})

    # Defensive: if ever wired as a RESPONSE interceptor, pass the response through.
    if mcp.get("gatewayResponse") is not None:
        gr = mcp.get("gatewayResponse", {})
        return {"interceptorOutputVersion": "1.0", "mcp": {"transformedGatewayResponse": {
            "body": gr.get("body", {}), "statusCode": gr.get("statusCode", 200)}}}

    req = mcp.get("gatewayRequest", {})
    body = req.get("body", {}) or {}
    method = body.get("method")

    # Only tool invocations are rewritten; initialize / tools/list / notifications
    # pass through unchanged so the MCP handshake is untouched.
    if method != "tools/call":
        return {"interceptorOutputVersion": "1.0", "mcp": {"transformedGatewayRequest": {"body": body}}}

    token = _get_header(req.get("headers", {}), "Authorization")
    claims = _decode_jwt(token) if token else {}
    customer_id = claims.get("customer_id", "")

    params = body.setdefault("params", {})
    args = params.get("arguments") or {}
    params["arguments"] = args

    full_name = params.get("name", "")
    tool = full_name.split(DELIM, 1)[1] if DELIM in full_name else full_name

    if tool in ("get_record", "get_my_info", "get_shipment", "process_refund"):
        # Stamp the caller's identity from the VERIFIED token, overwriting any
        # model-supplied value. Read tools + process_refund are scoped to the
        # caller's own data (you can only refund your own order).
        args["customer_id"] = customer_id
    elif tool == "update_record":
        # The admin (Cedar-authorized) chooses the target customer_id; record who
        # actually acted for audit. Identity still never comes from the prompt.
        args["acting_customer_id"] = customer_id

    # Do NOT log the caller's customer_id — CloudWatch is a downstream sink read by
    # people not entitled to customer identifiers. The auditable fact is that identity
    # WAS present and injected from the verified token, not which customer it was.
    logger.info("interceptor: tool=%s identity_injected=%s", tool, bool(customer_id))
    return {"interceptorOutputVersion": "1.0", "mcp": {"transformedGatewayRequest": {"body": body}}}
'''


# Lambda code for the three tools
GET_RECORD_CODE = '''
import json, os, boto3

rds = boto3.client("rds-data", region_name=os.environ.get("AWS_REGION", "us-east-1"))
CLUSTER = os.environ["CLUSTER_ARN"]
SECRET = os.environ["SECRET_ARN"]
DB = os.environ["DATABASE_NAME"]

def handler(event, context):
    """Look up a customer record by order_id, scoped to the caller's own records.

    customer_id is INJECTED by the Gateway request interceptor from the verified
    OBO token — it is never taken from the model/prompt. Fail closed if absent.
    """
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    order_id = body.get("order_id", "")
    customer_id = body.get("customer_id", "")  # interceptor-injected, from the JWT

    if not customer_id:
        return {"statusCode": 403, "body": json.dumps({"error": "No verified caller identity. Access denied."})}
    if not order_id:
        return {"statusCode": 400, "body": json.dumps({"error": "order_id required"})}

    # Row-level scoping: the caller can only ever see their OWN record.
    # refund_eligible is the SOFT authoritative signal for whether a refund is
    # allowed (false for everyone here). The agent reads it and is SUPPOSED to
    # honour it — the Layer 4 demo shows a poisoned memory talking the agent into
    # overriding it anyway.
    resp = rds.execute_statement(
        resourceArn=CLUSTER, secretArn=SECRET, database=DB,
        sql="SELECT customer_id, name, email, order_id, refund_eligible FROM customers WHERE order_id = :oid AND customer_id = :cid",
        parameters=[
            {"name": "oid", "value": {"stringValue": order_id}},
            {"name": "cid", "value": {"stringValue": customer_id}},
        ],
        includeResultMetadata=True)
    cols = [c["name"] for c in resp.get("columnMetadata", [])]
    rows = []
    for rec in resp.get("records", []):
        row = {}
        for i, f in enumerate(rec):
            if "booleanValue" in f:
                row[cols[i]] = f["booleanValue"]
            elif "isNull" in f and f["isNull"]:
                row[cols[i]] = None
            else:
                row[cols[i]] = f.get("stringValue") or f.get("longValue")
        rows.append(row)
    return {"statusCode": 200, "body": json.dumps(rows)}
'''

GET_INFO_CODE = '''
import json, os, boto3

rds = boto3.client("rds-data", region_name=os.environ.get("AWS_REGION", "us-east-1"))
CLUSTER = os.environ["CLUSTER_ARN"]
SECRET = os.environ["SECRET_ARN"]
DB = os.environ["DATABASE_NAME"]

def handler(event, context):
    """Return the caller's own record.

    customer_id is INJECTED by the Gateway request interceptor from the verified
    OBO token — never from the model/prompt. Fail closed if absent.
    """
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    customer_id = body.get("customer_id", "")  # interceptor-injected, from the JWT

    if not customer_id:
        return {"statusCode": 403, "body": json.dumps({"error": "No verified caller identity. Access denied."})}

    resp = rds.execute_statement(
        resourceArn=CLUSTER, secretArn=SECRET, database=DB,
        sql="SELECT customer_id, name, email, order_id FROM customers WHERE customer_id = :cid",
        parameters=[{"name": "cid", "value": {"stringValue": customer_id}}],
        includeResultMetadata=True)
    cols = [c["name"] for c in resp.get("columnMetadata", [])]
    rows = []
    for rec in resp.get("records", []):
        row = {}
        for i, f in enumerate(rec):
            row[cols[i]] = f.get("stringValue") or f.get("longValue") or None
        rows.append(row)
    return {"statusCode": 200, "body": json.dumps(rows)}
'''

GET_SHIPMENT_CODE = '''
import json, os, boto3

rds = boto3.client("rds-data", region_name=os.environ.get("AWS_REGION", "us-east-1"))
CLUSTER = os.environ["CLUSTER_ARN"]
SECRET = os.environ["SECRET_ARN"]
DB = os.environ["DATABASE_NAME"]

def handler(event, context):
    """Return delivery/shipment status for an order the CALLER owns.

    customer_id is INJECTED by the Gateway request interceptor from the verified
    OBO token — never from the model/prompt. The order must belong to the caller
    (joined against customers), so a support user can't read another user's shipment.
    """
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    order_id = body.get("order_id", "")
    customer_id = body.get("customer_id", "")  # interceptor-injected, from the JWT

    if not customer_id:
        return {"statusCode": 403, "body": json.dumps({"error": "No verified caller identity. Access denied."})}
    if not order_id:
        return {"statusCode": 400, "body": json.dumps({"error": "order_id required"})}

    # Ownership check + shipment join: only return status if this order is the caller's.
    resp = rds.execute_statement(
        resourceArn=CLUSTER, secretArn=SECRET, database=DB,
        sql=("SELECT s.order_id, s.status, s.carrier, s.eta, s.last_location "
             "FROM shipments s JOIN customers c ON c.order_id = s.order_id "
             "WHERE s.order_id = :oid AND c.customer_id = :cid"),
        parameters=[
            {"name": "oid", "value": {"stringValue": order_id}},
            {"name": "cid", "value": {"stringValue": customer_id}},
        ],
        includeResultMetadata=True)
    cols = [c["name"] for c in resp.get("columnMetadata", [])]
    rows = []
    for rec in resp.get("records", []):
        row = {}
        for i, f in enumerate(rec):
            row[cols[i]] = f.get("stringValue") or f.get("longValue") or None
        rows.append(row)
    if not rows:
        return {"statusCode": 404, "body": json.dumps({"error": "no shipment for that order (or not yours)"})}
    return {"statusCode": 200, "body": json.dumps(rows[0])}
'''

PROCESS_REFUND_CODE = '''
import json, os, boto3

rds = boto3.client("rds-data", region_name=os.environ.get("AWS_REGION", "us-east-1"))
CLUSTER = os.environ["CLUSTER_ARN"]
SECRET = os.environ["SECRET_ARN"]
DB = os.environ["DATABASE_NAME"]

def handler(event, context):
    """Process (issue) a refund for one of the caller's own orders by writing a row
    to the refunds table. This is a REAL side effect — a durable record of money
    moving. The tool itself does NOT re-check eligibility: it trusts the agent to
    have honoured the refund_eligible flag on the customer record first.

    That is the whole Layer 4 point: nothing at the tool layer stops a wrongly-
    approved refund. If a poisoned memory convinces the agent to override the
    (false) refund_eligible flag, the agent calls THIS tool and a refund row is
    written — provable harm. customer_id is INJECTED from the verified OBO token,
    so a caller can only ever refund their own orders.
    """
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    order_id = body.get("order_id", "")
    amount = str(body.get("amount", ""))
    customer_id = body.get("customer_id", "")  # interceptor-injected, from the JWT

    if not customer_id:
        return {"statusCode": 403, "body": json.dumps({"error": "No verified caller identity. Access denied."})}
    if not order_id:
        return {"statusCode": 400, "body": json.dumps({"error": "order_id required"})}

    # Ownership check: only the caller's own order can be refunded.
    owns = rds.execute_statement(
        resourceArn=CLUSTER, secretArn=SECRET, database=DB,
        sql="SELECT order_id FROM customers WHERE order_id = :oid AND customer_id = :cid",
        parameters=[
            {"name": "oid", "value": {"stringValue": order_id}},
            {"name": "cid", "value": {"stringValue": customer_id}},
        ],
        includeResultMetadata=True)
    if not owns.get("records", []):
        return {"statusCode": 404, "body": json.dumps({"error": "no such order for this customer"})}

    rds.execute_statement(
        resourceArn=CLUSTER, secretArn=SECRET, database=DB,
        sql="INSERT INTO refunds (order_id, customer_id, amount) VALUES (:oid, :cid, :amt)",
        parameters=[
            {"name": "oid", "value": {"stringValue": order_id}},
            {"name": "cid", "value": {"stringValue": customer_id}},
            {"name": "amt", "value": {"stringValue": amount or "full"}},
        ])
    return {"statusCode": 200, "body": json.dumps({
        "refund_processed": True, "order_id": order_id, "amount": amount or "full",
        "message": "Refund has been processed and recorded.",
    })}
'''

UPDATE_RECORD_CODE = '''
import json, os, boto3

rds = boto3.client("rds-data", region_name=os.environ.get("AWS_REGION", "us-east-1"))
CLUSTER = os.environ["CLUSTER_ARN"]
SECRET = os.environ["SECRET_ARN"]
DB = os.environ["DATABASE_NAME"]

def handler(event, context):
    """Update a customer record. Only reachable by admin agents (Cedar-gated).

    The admin legitimately targets any customer_id (that's the admin's job); the
    Gateway request interceptor injects acting_customer_id from the verified OBO
    token so the write is attributable to the real acting identity, not the prompt.
    """
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    customer_id = body.get("customer_id", "")
    field = body.get("field", "")
    value = body.get("value", "")
    acting_customer_id = body.get("acting_customer_id", "")  # interceptor-injected, from the JWT

    if not acting_customer_id:
        return {"statusCode": 403, "body": json.dumps({"error": "No verified acting identity. Access denied."})}
    if not customer_id or not field or not value:
        return {"statusCode": 400, "body": json.dumps({"error": "customer_id, field, and value required"})}

    # A column NAME can never be a bound parameter, so the updatable columns are mapped
    # to COMPLETE, CONSTANT statements. Every SQL string here is a literal in source: the
    # request only ever selects one, it never contributes text to one. That removes the
    # injection surface entirely rather than relying on an allowlist to sanitize input
    # before it is interpolated. (Values stay parameterized via :val / :cid below.)
    SQL_BY_FIELD = {
        "name": "UPDATE customers SET name = :val WHERE customer_id = :cid",
        "email": "UPDATE customers SET email = :val WHERE customer_id = :cid",
    }
    statement = SQL_BY_FIELD.get(field)
    if statement is None:
        return {"statusCode": 400, "body": json.dumps({"error": f"Can only update: {list(SQL_BY_FIELD)}"})}

    rds.execute_statement(
        resourceArn=CLUSTER, secretArn=SECRET, database=DB,
        sql=statement,
        parameters=[
            {"name": "val", "value": {"stringValue": value}},
            {"name": "cid", "value": {"stringValue": customer_id}},
        ])
    return {"statusCode": 200, "body": json.dumps({
        "updated": customer_id, "field": field, "value": value, "acting_customer_id": acting_customer_id})}
'''


class Layer3Stack(Stack):
    def __init__(self, scope: Construct, cid: str, **kwargs) -> None:
        super().__init__(scope, cid, **kwargs)

        Tags.of(self).add("demo", "iris-security")
        Tags.of(self).add("demo-layer", "layer3")

        # NOTE: VPC endpoints for Gateway access live in the stable
        # Layer3EndpointsStack (deployed once, never recreated on redeploy) so
        # that recreating this timestamped tools stack never conflicts with the
        # singleton private-DNS endpoints. See layer3_endpoints_stack.py.

        # Environment variables (passed via context from server)
        cluster_arn = self.node.try_get_context("clusterArn") or ""
        secret_arn = self.node.try_get_context("secretArn") or ""
        db_name = self.node.try_get_context("databaseName") or "irisdb"

        env_vars = {
            "CLUSTER_ARN": cluster_arn,
            "SECRET_ARN": secret_arn,
            "DATABASE_NAME": db_name,
        }

        # IAM role for Lambda tools (RDS Data API access)
        tool_role = iam.Role(
            self, "ToolRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
            ],
        )
        if cluster_arn:
            tool_role.add_to_policy(iam.PolicyStatement(
                actions=["rds-data:ExecuteStatement", "rds-data:BatchExecuteStatement"],
                resources=[cluster_arn],
            ))
        if secret_arn:
            tool_role.add_to_policy(iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue"],
                resources=[secret_arn],
            ))

        # Lambda functions
        get_record_fn = lambda_.Function(
            self, "GetRecordFn",
            function_name="iris-get-record",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            timeout=Duration.seconds(30),
            role=tool_role,
            environment=env_vars,
            code=lambda_.Code.from_inline(GET_RECORD_CODE),
        )

        get_info_fn = lambda_.Function(
            self, "GetInfoFn",
            function_name="iris-get-info",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            timeout=Duration.seconds(30),
            role=tool_role,
            environment=env_vars,
            code=lambda_.Code.from_inline(GET_INFO_CODE),
        )

        get_shipment_fn = lambda_.Function(
            self, "GetShipmentFn",
            function_name="iris-get-shipment",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            timeout=Duration.seconds(30),
            role=tool_role,
            environment=env_vars,
            code=lambda_.Code.from_inline(GET_SHIPMENT_CODE),
        )

        process_refund_fn = lambda_.Function(
            self, "ProcessRefundFn",
            function_name="iris-process-refund",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            timeout=Duration.seconds(30),
            role=tool_role,
            environment=env_vars,
            code=lambda_.Code.from_inline(PROCESS_REFUND_CODE),
        )

        update_record_fn = lambda_.Function(
            self, "UpdateRecordFn",
            function_name="iris-update-record",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            timeout=Duration.seconds(30),
            role=tool_role,
            environment=env_vars,
            code=lambda_.Code.from_inline(UPDATE_RECORD_CODE),
        )

        # REQUEST interceptor: injects customer_id from the verified OBO token into
        # tool arguments before the tool Lambda runs. Needs no DB access — just a
        # basic execution role (the default created by from_inline is fine).
        interceptor_fn = lambda_.Function(
            self, "InterceptorFn",
            function_name="iris-interceptor",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.lambda_handler",
            timeout=Duration.seconds(15),
            code=lambda_.Code.from_inline(INTERCEPTOR_CODE),
        )

        # Grant Gateway's service role permission to invoke these Lambdas
        exec_role_arn = self.node.try_get_context("execRoleArn") or ""
        if exec_role_arn:
            gw_role = iam.Role.from_role_arn(self, "GatewayRole", exec_role_arn)
            get_record_fn.grant_invoke(gw_role)
            get_info_fn.grant_invoke(gw_role)
            get_shipment_fn.grant_invoke(gw_role)
            process_refund_fn.grant_invoke(gw_role)
            update_record_fn.grant_invoke(gw_role)
            interceptor_fn.grant_invoke(gw_role)

        # Outputs
        CfnOutput(self, "GetRecordFnArn", value=get_record_fn.function_arn)
        CfnOutput(self, "GetInfoFnArn", value=get_info_fn.function_arn)
        CfnOutput(self, "GetShipmentFnArn", value=get_shipment_fn.function_arn)
        CfnOutput(self, "ProcessRefundFnArn", value=process_refund_fn.function_arn)
        CfnOutput(self, "UpdateRecordFnArn", value=update_record_fn.function_arn)
        CfnOutput(self, "InterceptorFnArn", value=interceptor_fn.function_arn)
        CfnOutput(self, "ToolRoleArn", value=tool_role.role_arn)
