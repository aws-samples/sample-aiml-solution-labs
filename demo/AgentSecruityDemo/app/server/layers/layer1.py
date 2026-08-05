"""Layer 1 (Network controls) — per-layer AgentCore deploy module.

Each demo layer owns its AgentCore resources (runtime, and later gateway/memory/
policy as those layers need them) in its own module under server/layers/. The ONE
AgentCore deploy panel orchestrates them in order, so the whole stack still stands up
from a single action on the Deploy screen.

LAYER 1 is the baseline agent (full SQL via run_query + open http_request) deployed
in **VPC mode**. The only control is the network perimeter: the VPC's egress is
filtered by the Route 53 DNS Firewall (allow ONLY the sanctioned shipment host, deny
everything else, incl. the attacker exfil URL). Same agent as baseline, different
network — so the open http_request reaches the shipment service but is blocked for the
exfil URL. No identity yet (SigV4-invoked, like baseline) — that is the L1 story.

Runtime name: iris_layer1 (fixed). Tracked under phase "layer1". The purge in
layer6_deploy discovers + deletes it by the iris_ name prefix, so redeploys are clean.
"""
import os

HERE = os.path.dirname(os.path.abspath(__file__))
AGENT_LAYER1_DIR = os.path.abspath(os.path.join(HERE, "..", "..", "agent-layer1"))

RUNTIME_NAME = "iris_layer1"


async def deploy(A, ac, ctx):
    """Build + deploy the Layer 1 runtime (VPC mode). ctx keys:
    valid_subnets, sg_id, role_arn, cluster_arn, secret_arn, db_name, shipment_url,
    l1_repo. Yields SSE log chunks (A._sse dicts); ends with a ('__done__',) sentinel."""
    _sse = A._sse
    yield _sse({"type": "log", "line": "[2/7] Layer 1 agent (VPC mode — network controls)..."})

    l1_repo = ctx.get("l1_repo")
    valid_subnets = ctx.get("valid_subnets") or []
    if not l1_repo or not valid_subnets:
        yield _sse({"type": "log", "line": "no Layer 1 ECR repo or subnets — skipping Layer 1."})
        yield ("__done__",)
        return

    try:
        image = A._build_and_push_agent_image(l1_repo, AGENT_LAYER1_DIR)
        yield _sse({"type": "log", "line": f"pushed Layer 1 image: {image}"})
        sg_id = ctx.get("sg_id")
        env = {
            "CLUSTER_ARN": ctx.get("cluster_arn", ""),
            "SECRET_ARN": ctx.get("secret_arn", ""),
            "DATABASE_NAME": ctx.get("db_name", "irisdb"),
            # Same shipment URL as baseline — the DNS Firewall allows this host.
            "SHIPMENT_URL": ctx.get("shipment_url", ""),
        }
        resp = ac.create_agent_runtime(
            agentRuntimeName=RUNTIME_NAME,
            roleArn=ctx.get("role_arn"),   # broad infra role — network is the control, not IAM
            networkConfiguration={"networkMode": "VPC", "networkModeConfig": {
                "subnets": valid_subnets, "securityGroups": [sg_id] if sg_id else []}},
            protocolConfiguration={"serverProtocol": "AGUI"},
            environmentVariables=env,
            agentRuntimeArtifact={"containerConfiguration": {"containerUri": image}},
        )
        rid = resp.get("agentRuntimeId")
        import time as _t
        for _ in range(60):
            st = ac.get_agent_runtime(agentRuntimeId=rid).get("status")
            if st == "READY":
                break
            if "FAILED" in (st or ""):
                yield _sse({"type": "log", "line": f"Layer 1 runtime FAILED: {st}"}); rid = None; break
            _t.sleep(5)
        if rid:
            A.state.add_resource("agentcore-runtime", rid, "layer1",
                                 arn=f"arn:aws:bedrock-agentcore:{A.REGION}:{A.account_id()}:runtime/{rid}")
            yield _sse({"type": "log", "line": f"Layer 1 runtime READY (VPC): {rid}"})
    except Exception as e:
        yield _sse({"type": "log", "line": f"Layer 1 build/deploy failed (non-fatal): {e}"})
    yield ("__done__",)
