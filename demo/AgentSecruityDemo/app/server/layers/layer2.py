"""Layer 2 (Identity) — per-layer AgentCore deploy module.

Same agent shape as baseline/Layer 1 (run_query + http_request, AG-UI), with ONE
security change vs Layer 1: the runtime is JWT-authorized (Okta iris-agent) and
run_query is SCOPED to the authenticated user's customer_id. Whatever SQL the model
emits or the prompt asks for, run_query only ever returns the caller's own record —
identity comes from the VERIFIED Okta JWT the runtime forwards (Authorization header),
never from the prompt.

Deployment vs Layer 1: VPC mode (keeps L1's network controls) PLUS a customJWTAuthorizer
(allowedAudience = iris-agent) and requestHeaderConfiguration allowlisting Authorization
so the token reaches the container. No allowedScopes here — Layer 2 is "any valid Okta
user"; per-agent scope gating arrives at Layer 3.

Runtime name: iris_layer2 (fixed). Tracked under phase "layer2". Invoked with a Bearer
token (JWT) — SigV4 is rejected by a JWT-authorized runtime.
"""
import os

HERE = os.path.dirname(os.path.abspath(__file__))
AGENT_LAYER2_DIR = os.path.abspath(os.path.join(HERE, "..", "..", "agent-layer2"))

RUNTIME_NAME = "iris_layer2"


async def deploy(A, ac, ctx):
    """Build + deploy the Layer 2 runtime (VPC + Okta JWT authorizer). ctx keys:
    valid_subnets, sg_id, role_arn, cluster_arn, secret_arn, db_name, shipment_url,
    l2_repo. Yields SSE log chunks; ends with a ('__done__',) sentinel."""
    _sse = A._sse
    OKTA = A.OKTA
    yield _sse({"type": "log", "line": "[3/7] Layer 2 agent (VPC + Okta JWT · identity-scoped)..."})

    l2_repo = ctx.get("l2_repo")
    valid_subnets = ctx.get("valid_subnets") or []
    if not l2_repo or not valid_subnets:
        yield _sse({"type": "log", "line": "no Layer 2 ECR repo or subnets — skipping Layer 2."})
        yield ("__done__",)
        return

    try:
        image = A._build_and_push_agent_image(l2_repo, AGENT_LAYER2_DIR)
        yield _sse({"type": "log", "line": f"pushed Layer 2 image: {image}"})
        sg_id = ctx.get("sg_id")
        issuer = OKTA["agent_issuer"]
        audience = OKTA["agent_audience"]
        create_kwargs = dict(
            agentRuntimeName=RUNTIME_NAME,
            roleArn=ctx.get("role_arn"),
            networkConfiguration={"networkMode": "VPC", "networkModeConfig": {
                "subnets": valid_subnets, "securityGroups": [sg_id] if sg_id else []}},
            protocolConfiguration={"serverProtocol": "AGUI"},
            environmentVariables={
                "CLUSTER_ARN": ctx.get("cluster_arn", ""),
                "SECRET_ARN": ctx.get("secret_arn", ""),
                "DATABASE_NAME": ctx.get("db_name", "irisdb"),
                "SHIPMENT_URL": ctx.get("shipment_url", ""),
            },
            # Forward the verified JWT to the container so the agent scopes run_query to
            # the token's customer_id (allowed because the runtime has a JWT authorizer).
            requestHeaderConfiguration={"requestHeaderAllowlist": ["Authorization"]},
            authorizerConfiguration={"customJWTAuthorizer": {
                "discoveryUrl": f"{issuer}/.well-known/openid-configuration",
                "allowedAudience": [audience],
            }},
            agentRuntimeArtifact={"containerConfiguration": {"containerUri": image}},
        )
        yield _sse({"type": "log", "line": f"JWT authorizer: {issuer} (aud={audience})"})
        resp = ac.create_agent_runtime(**create_kwargs)
        rid = resp.get("agentRuntimeId")
        import time as _t
        for _ in range(60):
            st = ac.get_agent_runtime(agentRuntimeId=rid).get("status")
            if st == "READY":
                break
            if "FAILED" in (st or ""):
                yield _sse({"type": "log", "line": f"Layer 2 runtime FAILED: {st}"}); rid = None; break
            _t.sleep(5)
        if rid:
            A.state.add_resource("agentcore-runtime", rid, "layer2",
                                 arn=f"arn:aws:bedrock-agentcore:{A.REGION}:{A.account_id()}:runtime/{rid}")
            yield _sse({"type": "log", "line": f"Layer 2 runtime READY (VPC + JWT): {rid}"})
    except Exception as e:
        yield _sse({"type": "log", "line": f"Layer 2 build/deploy failed (non-fatal): {e}"})
    yield ("__done__",)
