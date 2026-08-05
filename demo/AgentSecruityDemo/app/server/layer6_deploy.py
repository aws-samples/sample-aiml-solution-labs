"""
Layer 6 (capstone) — SELF-CONTAINED full-stack deploy.

THE consolidated deploy path: stands up the COMPLETE secure-agent stack from the
cdk/ CDK project + AgentCore control-plane, and runs the goal-fenced agent +
A2A peer. This is the single combined stack — all resources are neutrally named
(Iris* / iris-*).

It composes every security control (Infra + L1..L5) in order, then creates ONE
runtime carrying all of them:
  1. Infra   — cdk deploy IrisInfra + IrisNetwork + IrisEndpoints; seed Aurora;
               build a DNS Firewall ALLOWLIST (only the sanctioned shipment host
               resolves; everything else NXDOMAINs). NO attacker collector.
  2. Tools   — cdk deploy IrisTools; create Gateway (MCP, CUSTOM_JWT, REQUEST
               interceptor) + 5 typed targets + Cedar policy engine (ENFORCE) + OBO
               credential providers (reusing the SAME Okta app).
  3. Memory  — cdk deploy IrisMemory; create_memory (self-managed pre-write gate).
  4. Models  — cdk deploy IrisModels; put_enforced_guardrail_configuration
               (account-level, scoped to the approved Sonnet model).
  5. Agent   — build+push agent-layer6 (superset + goal fence) → ONE runtime on the
               L5 scoped role, VPC, Okta CUSTOM_JWT, all env wired.

All state is tracked under phase="layer6". A matching destroy is provided.

This module intentionally REUSES helpers from server.app (the shared _stream_cmd,
_read_cfn_outputs, _build_and_push_agent_image, _seed_*, OKTA, _sess, account_id,
state, _sse) via a late import, so there is a single source of truth for them and
zero duplication.
"""
import asyncio
import json
import os

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

router = APIRouter()

HERE = os.path.dirname(os.path.abspath(__file__))
CDK_L6_DIR = os.path.abspath(os.path.join(HERE, "..", "cdk"))
AGENT_L6_DIR = os.path.abspath(os.path.join(HERE, "..", "agent-layer6"))
AGENT_PEER_DIR = os.path.abspath(os.path.join(HERE, "..", "agent-peer"))
AGENT_BASELINE_DIR = os.path.abspath(os.path.join(HERE, "..", "agent-baseline"))
AGENT_LAYER1_DIR = os.path.abspath(os.path.join(HERE, "..", "agent-layer1"))


def _valid_subnets(A, subnets):
    """Filter subnets to the AZ ids AgentCore runtimes support (same logic the demo
    per-layer deploys use)."""
    supported = {"use1-az1", "use1-az2", "use1-az4", "usw2-az1", "usw2-az2", "usw2-az3"}
    ec2 = A._sess().client("ec2")
    info = ec2.describe_subnets(SubnetIds=subnets).get("Subnets", []) if subnets else []
    az_map = {az["ZoneName"]: az["ZoneId"] for az in ec2.describe_availability_zones().get("AvailabilityZones", [])}
    return [s["SubnetId"] for s in info if az_map.get(s["AvailabilityZone"]) in supported] or subnets[:1]

# Fixed-name L6 infra stacks (match cdk/app.py).
L6_INFRA = "IrisInfra"
L6_NETWORK = "IrisNetwork"
L6_ENDPOINTS = "IrisEndpoints"


def _app():
    """Late import of the shared server module (avoids circular import at load).
    The server is launched as `uvicorn app:app` from inside server/, so the module
    is the top-level `app`, not `server.app`."""
    import app as A
    return A


async def _l6_cdk_deploy(gen_out, stack_names, ctx=None, output_dir=None):
    """Like app._cdk_deploy_stream but rooted at the LAYER 6 cdk project (cdk/)."""
    A = _app()
    cmd = ["npx", "cdk", "deploy", *stack_names, "--require-approval", "never",
           "--outputs-file", "cdk-outputs.json"]
    if output_dir:
        cmd += ["--output", output_dir]
    for k, v in (ctx or {}).items():
        cmd += ["-c", f"{k}={v}"]
    async for chunk in A._stream_cmd(cmd, cwd=CDK_L6_DIR):
        yield chunk
        try:
            ev = json.loads(chunk[len("data: "):]) if chunk.startswith("data: ") else {}
            if ev.get("type") == "end":
                gen_out["rc"] = ev.get("code")
        except Exception:
            pass


def _l6_read_outputs(stack_name):
    """Read cdk-outputs.json from the L6 cdk dir for a given stack."""
    path = os.path.join(CDK_L6_DIR, "cdk-outputs.json")
    try:
        with open(path) as f:
            return json.load(f).get(stack_name, {})
    except Exception:
        return {}


def _l6_dns_firewall_allowlist(shipment_url, vpc_id):
    """DNS Firewall as a GENERIC default-deny egress ALLOWLIST for Layer 6.

    Unlike the demo's version (allow shipment host + block *.lambda-url), this is the
    production-correct posture: ALLOW only the sanctioned shipment host, then BLOCK
    (NXDOMAIN) everything else via a catch-all '*'. Because the block is a catch-all,
    any non-approved host — including a would-be exfil collector Function URL — is
    denied without naming it. AWS-service DNS (amazonaws.com, the VPC-endpoint traffic)
    is reached via the interface endpoints' private DNS, which resolver rules on the
    associated VPC still evaluate — so we must also ALLOW the AWS service domains the
    agent legitimately needs, or the whole runtime breaks. We allow-list the shipment
    host + '*.amazonaws.com' + '*.on.aws' EXCEPT the block catch-all is scoped to the
    lambda-url pattern to avoid nuking AWS-service DNS.

    Yields human-readable log lines. Tracked under phase='layer6'.
    """
    A = _app()
    r53 = A._sess().client("route53resolver")
    host = A._shipment_host(shipment_url)
    if not host:
        yield "no shipment host — skipping DNS Firewall"; return
    region = A.REGION
    suffix = "l6-" + host.split(".")[0][:10]

    # ALLOW list — the exact sanctioned shipment host.
    allow_name = f"iris-{suffix}-allow"
    allow_id = None
    for dl in r53.list_firewall_domain_lists().get("FirewallDomainLists", []):
        if dl["Name"] == allow_name:
            allow_id = dl["Id"]; break
    if not allow_id:
        allow_id = r53.create_firewall_domain_list(Name=allow_name)["FirewallDomainList"]["Id"]
    r53.update_firewall_domains(FirewallDomainListId=allow_id, Operation="REPLACE", Domains=[host])
    A.state.add_resource("dns-fw-domain-list", allow_id, "layer6")
    yield f"allow-list (only sanctioned host): {host}"

    # BLOCK list — every other Function-URL host (catch-all for the lambda-url space).
    # An exfil collector is a *.lambda-url host too, so this denies it without naming
    # it. AWS-service DNS (*.amazonaws.com) is NOT matched, so VPC-endpoint traffic
    # and Bedrock/Gateway calls flow normally.
    block_name = f"iris-{suffix}-block"
    wildcard = f"*.lambda-url.{region}.on.aws"
    block_id = None
    for dl in r53.list_firewall_domain_lists().get("FirewallDomainLists", []):
        if dl["Name"] == block_name:
            block_id = dl["Id"]; break
    if not block_id:
        block_id = r53.create_firewall_domain_list(Name=block_name)["FirewallDomainList"]["Id"]
    r53.update_firewall_domains(FirewallDomainListId=block_id, Operation="REPLACE", Domains=[wildcard])
    A.state.add_resource("dns-fw-domain-list", block_id, "layer6")
    yield f"block-list (default-deny other egress): {wildcard}"

    rg_name = f"iris-{suffix}-egress-fw"
    rg_id = None
    for rg in r53.list_firewall_rule_groups().get("FirewallRuleGroups", []):
        if rg["Name"] == rg_name:
            rg_id = rg["Id"]; break
    if not rg_id:
        rg_id = r53.create_firewall_rule_group(Name=rg_name)["FirewallRuleGroup"]["Id"]
    A.state.add_resource("dns-fw-rule-group", rg_id, "layer6")

    def _put_rule(name, dl_id, action, priority, block_resp=None):
        kw = dict(FirewallRuleGroupId=rg_id, FirewallDomainListId=dl_id,
                  Priority=priority, Action=action, Name=name)
        if action == "BLOCK":
            kw["BlockResponse"] = block_resp or "NXDOMAIN"
        try:
            r53.create_firewall_rule(**kw)
        except Exception:
            try:
                r53.update_firewall_rule(**kw)
            except Exception:
                pass
    _put_rule("allow-shipment", allow_id, "ALLOW", 1)
    _put_rule("block-egress", block_id, "BLOCK", 2, "NXDOMAIN")
    yield "rule group configured (allow sanctioned host #1, default-deny #2)"

    assoc_name = f"iris-{suffix}-fw-assoc"
    already = False
    for a in r53.list_firewall_rule_group_associations(VpcId=vpc_id).get("FirewallRuleGroupAssociations", []):
        if a.get("FirewallRuleGroupId") == rg_id:
            already = True
            A.state.add_resource("dns-fw-association", a["Id"], "layer6"); break
    if not already:
        assoc = r53.associate_firewall_rule_group(
            CreatorRequestId=f"iris-{suffix}-{vpc_id}"[:60],
            FirewallRuleGroupId=rg_id, VpcId=vpc_id, Priority=101, Name=assoc_name,
        )["FirewallRuleGroupAssociation"]
        A.state.add_resource("dns-fw-association", assoc["Id"], "layer6")
    yield f"DNS Firewall associated with VPC {vpc_id}"


@router.get("/api/code/stack")
def code_stack():
    """PANEL 1 code — the CDK stack (all CloudFormation): the self-contained app plus
    every stack it synthesizes (infra/network/endpoints/tools/memory/models)."""
    A = _app()
    return {
        "sections": [
            {"title": "CDK app — composes every layer's durable resources into one stack set", "lang": "python",
             "file": "cdk/app.py", "code": A._read_src("cdk/app.py")},
            {"title": "Infra stack — VPC + private subnets + Aurora (Data API) + ECR + exec role + shipment", "lang": "python",
             "file": "cdk/iris_demo/baseline_stack.py", "code": A._read_src("cdk/iris_demo/baseline_stack.py")},
            {"title": "Network stack (L1) — agent security group + egress subnets", "lang": "python",
             "file": "cdk/iris_demo/layer1_stack.py", "code": A._read_src("cdk/iris_demo/layer1_stack.py")},
            {"title": "Gateway VPC endpoints stack (L3) — PrivateLink interface endpoints", "lang": "python",
             "file": "cdk/iris_demo/layer3_endpoints_stack.py", "code": A._read_src("cdk/iris_demo/layer3_endpoints_stack.py")},
            {"title": "Tools stack (L3) — typed tool Lambdas + REQUEST interceptor", "lang": "python",
             "file": "cdk/iris_demo/layer3_stack.py", "code": A._read_src("cdk/iris_demo/layer3_stack.py")},
            {"title": "Memory stack (L4) — CMK + memory exec role + self-managed pre-write gate", "lang": "python",
             "file": "cdk/iris_demo/layer4_stack.py", "code": A._read_src("cdk/iris_demo/layer4_stack.py")},
            {"title": "Models stack (L5) — scoped exec role (IAM allowlist) + AIP + default Guardrail", "lang": "python",
             "file": "cdk/iris_demo/layer5_stack.py", "code": A._read_src("cdk/iris_demo/layer5_stack.py")},
        ]
    }


@router.get("/api/code/agentcore")
def code_agentcore():
    """PANEL 2 code — the server-side AgentCore control-plane deploy + the goal-fenced
    agent + the A2A peer."""
    A = _app()
    return {
        "sections": [
            {"title": "AgentCore deploy orchestrator (Gateway + Cedar + OBO + Memory + guardrail enforcement + peer + ONE runtime)", "lang": "python",
             "file": "server/layer6_deploy.py", "code": A._read_src("server/layer6_deploy.py")},
            {"title": "Agent code — superset agent + GOAL FENCE hook + A2A order_lookup delegate", "lang": "python",
             "file": "agent-layer6/agent.py", "code": A._read_src("agent-layer6/agent.py")},
            {"title": "A2A Orders peer — confused-deputy demo (propagate-identity vs trust-arg)", "lang": "python",
             "file": "agent-peer/agent.py", "code": A._read_src("agent-peer/agent.py")},
        ]
    }


@router.get("/api/code/fullstack")
def code_fullstack():
    """Back-compat alias — returns the CDK-stack code sections."""
    return code_stack()


# The deploy is split into TWO panels that share state via this context file:
#   1. /api/deploy/stack     — the CDK stack (all CloudFormation): infra + network +
#      endpoints + tools + memory + models. Persists its CFN outputs here.
#   2. /api/deploy/agentcore — the server-side AgentCore control-plane deploy: Gateway
#      + Cedar + OBO + Memory resource + guardrail enforcement + the A2A peer + the ONE
#      consolidated runtime. Reads the context this file wrote.
# (cdk-outputs.json is overwritten per stack deploy, so we snapshot everything the
#  AgentCore panel needs into one durable file keyed by the deploy suffix.)
STACK_CTX_PATH = os.path.join(HERE, "..", "logs", "iris-stack-context.json")


def _save_stack_ctx(ctx):
    os.makedirs(os.path.dirname(STACK_CTX_PATH), exist_ok=True)
    with open(STACK_CTX_PATH, "w") as f:
        json.dump(ctx, f, indent=2)


def _load_stack_ctx():
    try:
        with open(STACK_CTX_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


@router.post("/api/deploy/stack")
async def deploy_stack():
    """PANEL 1 — the CDK stack (all CloudFormation). Deploys every durable AWS
    resource (infra + network + endpoints + tools + memory + models), seeds Aurora,
    builds the DNS Firewall allowlist, and enforces the account-level guardrail. Writes
    a context snapshot the AgentCore panel consumes. No AgentCore runtimes created here."""
    async def gen():
        A = _app()
        _sse = A._sse

        yield _sse({"type": "log", "line": "=== IRIS · CDK STACK DEPLOY (infra → tools → memory → models) ==="})

        # ---------- clean up any previous timestamped CDK-owned resources ----------
        res_all = A.state.all_resources()
        for r in [r for r in res_all if r.get("kind") == "guardrail-enforcement" and r.get("phase") == "layer6"]:
            try:
                A._sess().client("bedrock").delete_enforced_guardrail_configuration(configId=r["id"])
            except Exception:
                pass
            A.state.remove_resource("guardrail-enforcement", r["id"])
        # LEGACY-ORPHAN SWEEP: every stack is now FIXED-NAME and updates in place
        # (IrisTools / IrisMemory / IrisModels — no suffix). But earlier builds used
        # TIMESTAMPED names (IrisTools615486, …) that a fixed-name deploy won't touch,
        # so they'd linger forever. Discover any suffixed leftovers from CloudFormation
        # by prefix (state.json can't be trusted — it drifts) and delete them, incl. a
        # REVIEW_IN_PROGRESS changeset corpse from a failed old redeploy. The exact
        # fixed names (nm == prefix) are EXCLUDED — those are the live stacks we keep.
        prior_ts = []
        try:
            cfn = A._sess().client("cloudformation")
            live = []
            paginator = cfn.get_paginator("list_stacks")
            active = {"CREATE_COMPLETE", "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE",
                      "ROLLBACK_COMPLETE", "REVIEW_IN_PROGRESS", "CREATE_FAILED",
                      "ROLLBACK_FAILED", "UPDATE_ROLLBACK_FAILED", "IMPORT_COMPLETE"}
            for page in paginator.paginate(StackStatusFilter=list(active)):
                for st in page.get("StackSummaries", []):
                    nm = st.get("StackName", "")
                    if any(nm.startswith(p) and nm != p for p in ("IrisTools", "IrisMemory", "IrisModels")):
                        live.append(nm)
            prior_ts = sorted(set(live))
        except Exception as e:
            yield _sse({"type": "log", "line": f"(could not list prior stacks from CFN: {e})"})
        if prior_ts:
            yield _sse({"type": "log", "line": f"cleaning up {len(prior_ts)} prior timestamped stack(s): {', '.join(prior_ts)}"})
            # Delete via the CloudFormation API (NOT `cdk destroy`): the CDK CLI can only
            # destroy stacks the current app synthesizes, and the app only emits the
            # CURRENT suffix — an old-suffix name (e.g. IrisTools615486) isn't in the
            # app, so `cdk destroy` can't find it. delete_stack works by name directly.
            cfn = A._sess().client("cloudformation")
            for sid in prior_ts:
                try:
                    cfn.delete_stack(StackName=sid)
                    yield _sse({"type": "log", "line": f"  delete_stack {sid} requested"})
                except Exception as e:
                    yield _sse({"type": "log", "line": f"  delete_stack {sid} failed: {e}"})
            # Wait (best-effort) for each to finish deleting so their fixed resources
            # (e.g. the memory-gate table, guardrail) free up before we recreate.
            for sid in prior_ts:
                try:
                    cfn.get_waiter("stack_delete_complete").wait(
                        StackName=sid, WaiterConfig={"Delay": 10, "MaxAttempts": 60})
                    yield _sse({"type": "log", "line": f"  {sid} deleted"})
                except Exception as e:
                    yield _sse({"type": "log", "line": f"  {sid} delete wait ended: {e}"})
                A.state.remove_resource("cdk-stack", sid)

        # =====================================================================
        # 1) INFRA — VPC / Aurora / ECR / exec role / shipment + network + endpoints
        # =====================================================================
        yield _sse({"type": "log", "line": f"[1/4] infra: cdk deploy {L6_INFRA}..."})
        out = {"rc": None}
        async for chunk in _l6_cdk_deploy(out, [L6_INFRA]):
            yield chunk
        if out["rc"] not in (0, None):
            yield _sse({"type": "log", "line": "ERROR: L6 infra CDK deploy failed."})
            yield _sse({"type": "failed", "phase": "layer6"}); return
        infra = _l6_read_outputs(L6_INFRA)
        A.state.add_resource("cdk-stack", L6_INFRA, "layer6")
        vpc_id = infra.get("VpcId", "")
        cluster_arn = infra.get("ClusterArn", "")
        secret_arn = infra.get("SecretArn", "")
        db_name = infra.get("DatabaseName", "irisdb")
        role_arn = infra.get("ExecRoleArn", "")
        shipment_url = infra.get("ShipmentUrl", "")
        if infra.get("VpcId"):
            A.state.add_resource("vpc", infra["VpcId"], "layer6")
        if cluster_arn:
            A.state.add_resource("aurora-cluster", cluster_arn.split(":")[-1], "layer6")
        # Track the attacker collector (baseline exfil target) so the baseline run can
        # resolve {{COLLECTOR_URL}} and the teardown removes it. Tracked under phase
        # "infra" to match the collector-url lookup the baseline run endpoint uses.
        if infra.get("CollectorUrl"):
            A.state.add_resource("collector-url", infra["CollectorUrl"], "infra", url=infra["CollectorUrl"])
        if infra.get("ExfilBucketName"):
            A.state.add_resource("s3-bucket", infra["ExfilBucketName"], "layer6")
        # Track the sanctioned shipment URL so the flow diagram can distinguish the
        # legit shipment call (green) from an exfil POST (red) — the UI reads this via
        # the shipment-url resource kind (window._shipmentUrl). Without it, every
        # http_request misroutes to the collector path.
        if shipment_url:
            A.state.add_resource("shipment-url", shipment_url, "infra", url=shipment_url)
        yield _sse({"type": "log", "line": f"infra ready (VPC {vpc_id}, Aurora, ECR, exec role, collector)"})

        # seed Aurora (reuse the demo's seeders — same schema)
        if cluster_arn and secret_arn:
            for attempt in range(3):
                try:
                    n = A._seed_customers(cluster_arn=cluster_arn, secret_arn=secret_arn, database=db_name)
                    yield _sse({"type": "log", "line": f"seeded {n} customer records"})
                    break
                except Exception:
                    if attempt < 2:
                        await asyncio.sleep(10)
            try:
                A._seed_shipments(cluster_arn=cluster_arn, secret_arn=secret_arn, database=db_name)
                A._seed_orders(cluster_arn=cluster_arn, secret_arn=secret_arn, database=db_name)
                A._seed_refunds(cluster_arn=cluster_arn, secret_arn=secret_arn, database=db_name)
                yield _sse({"type": "log", "line": "seeded shipments + orders + refunds tables"})
            except Exception as e:
                yield _sse({"type": "log", "line": f"seeding warning: {e}"})

        # network controls
        yield _sse({"type": "log", "line": f"network: cdk deploy {L6_NETWORK}..."})
        out = {"rc": None}
        async for chunk in _l6_cdk_deploy(out, [L6_NETWORK], ctx={"vpcId": vpc_id}):
            yield chunk
        if out["rc"] not in (0, None):
            yield _sse({"type": "log", "line": "ERROR: L6 network CDK deploy failed."})
            yield _sse({"type": "failed", "phase": "layer6"}); return
        net = _l6_read_outputs(L6_NETWORK)
        A.state.add_resource("cdk-stack", L6_NETWORK, "layer6")
        sg_id = net.get("SecurityGroupId", "")
        subnets = [s for s in net.get("SubnetIds", "").split(",") if s]
        yield _sse({"type": "log", "line": f"network controls ready (SG {sg_id})"})

        # gateway VPC endpoints
        yield _sse({"type": "log", "line": f"endpoints: cdk deploy {L6_ENDPOINTS}..."})
        out = {"rc": None}
        async for chunk in _l6_cdk_deploy(out, [L6_ENDPOINTS],
                                          ctx={"vpcId": vpc_id, "securityGroupId": sg_id},
                                          output_dir="cdk.out.l6.endpoints"):
            yield chunk
        if out["rc"] in (0, None):
            A.state.add_resource("cdk-stack", L6_ENDPOINTS, "layer6")
            yield _sse({"type": "log", "line": "Gateway VPC endpoints ready"})

        # DNS firewall — allowlist-only egress
        if shipment_url and vpc_id:
            try:
                yield _sse({"type": "log", "line": "building DNS Firewall (allowlist: only sanctioned host; default-deny rest)..."})
                for line in _l6_dns_firewall_allowlist(shipment_url, vpc_id):
                    yield _sse({"type": "log", "line": line})
            except Exception as e:
                yield _sse({"type": "log", "line": f"DNS Firewall build failed (non-fatal): {e}"})

        # =====================================================================
        # 2) TOOLS — tool Lambdas stack + Gateway + Cedar + OBO
        # =====================================================================
        tools_stack = "IrisTools"
        yield _sse({"type": "log", "line": f"[2/4] tools: cdk deploy {tools_stack}..."})
        out = {"rc": None}
        async for chunk in _l6_cdk_deploy(out, [tools_stack], ctx={
            "execRoleArn": role_arn,
            "clusterArn": cluster_arn, "secretArn": secret_arn, "databaseName": db_name,
        }):
            yield chunk
        if out["rc"] not in (0, None):
            yield _sse({"type": "log", "line": "ERROR: tools CDK deploy failed."})
            yield _sse({"type": "failed", "phase": "layer6"}); return
        l3 = _l6_read_outputs(tools_stack)
        A.state.add_resource("cdk-stack", tools_stack, "layer6")

        # =====================================================================
        # 3) MEMORY — CMK + self-managed pre-write gate (CDK durable resources only)
        # =====================================================================
        mem_stack = "IrisMemory"
        yield _sse({"type": "log", "line": f"[3/4] memory: cdk deploy {mem_stack}..."})
        out = {"rc": None}
        async for chunk in _l6_cdk_deploy(out, [mem_stack], ctx={
            "execRoleArn": role_arn,
        }):
            yield chunk
        if out["rc"] not in (0, None):
            yield _sse({"type": "log", "line": "ERROR: memory CDK deploy failed."})
            yield _sse({"type": "failed", "phase": "layer6"}); return
        l4 = _l6_read_outputs(mem_stack)
        A.state.add_resource("cdk-stack", mem_stack, "layer6")
        gate_table = l4.get("MemoryGateTable")
        if gate_table:
            A.state.add_resource("memory-gate-table", gate_table, "layer6")

        # =====================================================================
        # 4) MODELS — scoped role + guardrail + AIP + account enforcement
        # =====================================================================
        mod_stack = "IrisModels"
        yield _sse({"type": "log", "line": f"[4/4] models: cdk deploy {mod_stack}..."})
        out = {"rc": None}
        async for chunk in _l6_cdk_deploy(out, [mod_stack], ctx={
            "execRoleArn": role_arn,
            "memoryCmkArn": l4.get("MemoryCmkArn", ""),
        }):
            yield chunk
        if out["rc"] not in (0, None):
            yield _sse({"type": "log", "line": "ERROR: models CDK deploy failed."})
            yield _sse({"type": "failed", "phase": "layer6"}); return
        l5 = _l6_read_outputs(mod_stack)
        A.state.add_resource("cdk-stack", mod_stack, "layer6")
        scoped_role_arn = l5.get("Layer5ExecRoleArn") or role_arn
        # NO account-level guardrail enforcement. Layer 5 makes the guardrail mandatory
        # via the IAM bedrock:GuardrailIdentifier condition key (in layer5_stack.py): the
        # agent MUST attach the guardrail on every Converse request, so it can also TAG
        # the user turn — which lets the prompt-attack filter evaluate the user input and
        # NOT the agent's own system prompt. Account-level enforcement can't do that (it
        # ignores the input tags and re-scans the whole prompt), so we don't use it here.
        # Clean up any account-level enforced config left over from the old design so it
        # doesn't double-guard / re-scan the system prompt.
        try:
            br = A._sess().client("bedrock")
            for c in br.list_enforced_guardrails_configuration().get("guardrailsConfig", []):
                cid = c.get("configId")
                if cid:
                    br.delete_enforced_guardrail_configuration(configId=cid)
                    yield _sse({"type": "log", "line": f"removed account-level enforced guardrail config {cid} "
                                                       f"(Layer 5 now enforces the guardrail via the IAM condition key + request path)"})
        except Exception as e:
            yield _sse({"type": "log", "line": f"note: could not clear account-level enforced configs (ok if none): {e}"})

        # Persist everything the AgentCore panel needs (cdk-outputs.json is overwritten
        # per stack deploy; this snapshot is durable across the two panels).
        _save_stack_ctx({
            "infra": infra, "l3": l3, "l4": l4, "l5": l5,
            "vpc_id": vpc_id, "cluster_arn": cluster_arn, "secret_arn": secret_arn,
            "db_name": db_name, "role_arn": role_arn, "scoped_role_arn": scoped_role_arn,
            "sg_id": sg_id, "subnets": subnets,
        })
        yield _sse({"type": "log", "line": "=== CDK STACK DEPLOYED — all CloudFormation is live. Now run the AgentCore deploy panel. ==="})
        yield _sse({"type": "done", "phase": "layer6"})

    A = _app()
    return StreamingResponse(A._tracked_stream("stack", gen()), media_type="text/event-stream")


@router.post("/api/deploy/agentcore")
async def deploy_agentcore():
    """PANEL 2 — the server-side AgentCore control-plane deploy. Reads the context the
    CDK-stack panel wrote, then creates: Gateway (MCP + CUSTOM_JWT + interceptor) +
    Cedar policy engine + OBO providers, the Memory resource (self-managed pre-write
    gate), the A2A Orders peer runtime, and the ONE consolidated goal-fenced runtime."""
    async def gen():
        A = _app()
        _sse = A._sse
        # Read/connect timeouts + capped retries so a hung control-plane call (e.g. a
        # same-name create racing an async delete) fails fast instead of blocking the
        # whole deploy forever. Without this a stuck create hangs the generator.
        from botocore.config import Config as _Cfg
        ac = A._sess().client("bedrock-agentcore-control",
                              config=_Cfg(connect_timeout=15, read_timeout=60,
                                          retries={"max_attempts": 3, "mode": "standard"}))

        ctx = _load_stack_ctx()
        if not ctx.get("infra"):
            yield _sse({"type": "log", "line": "ERROR: no CDK stack context — run the CDK stack deploy panel first."})
            yield _sse({"type": "failed", "phase": "layer6"}); return
        infra, l3, l4, l5 = ctx["infra"], ctx["l3"], ctx["l4"], ctx["l5"]
        cluster_arn, secret_arn, db_name = ctx["cluster_arn"], ctx["secret_arn"], ctx["db_name"]
        role_arn, scoped_role_arn = ctx["role_arn"], ctx["scoped_role_arn"]
        sg_id, subnets = ctx["sg_id"], ctx["subnets"]

        yield _sse({"type": "log", "line": "=== IRIS · AGENTCORE DEPLOY (baseline + Gateway + Memory + peer + runtime) ==="})

        # ---------- PURGE the prior AgentCore generation (discovered live from AWS) ----
        # Fixed names + no update-in-place → we must delete-then-create. This removes
        # EVERY Iris runtime (baseline/peer/main — they hold VPC ENIs + cost), gateway
        # + targets, policy engine, OBO provider, and memory, so a redeploy never
        # orphans. Discovery is from AWS (not state.json, which drifts).
        yield _sse({"type": "log", "line": "purging any prior AgentCore resources (runtimes, gateway, policy, OBO, memory)..."})
        async for line in _l6_purge_agentcore(A, ac):
            if not isinstance(line, tuple):   # ('__done__',) sentinel
                yield line

        # =====================================================================
        # 1) BASELINE — the deliberately-unprotected reference agent (PUBLIC mode).
        #    Full SQL via run_query + open http_request, no VPC/identity/tools. This is
        #    the "before" agent for the before/after story; it can reach the collector.
        # =====================================================================
        # Subnets valid for AgentCore VPC runtimes (Layer 1 + peer + main all use these).
        valid_subnets = _valid_subnets(A, subnets)

        yield _sse({"type": "log", "line": "[1/7] baseline agent (PUBLIC mode — unprotected reference)..."})
        base_repo = infra.get("EcrRepoUriBaseline")
        if base_repo:
            try:
                base_image = A._build_and_push_agent_image(base_repo, AGENT_BASELINE_DIR)
                yield _sse({"type": "log", "line": f"pushed baseline image: {base_image}"})
                base_env = {
                    "CLUSTER_ARN": cluster_arn, "SECRET_ARN": secret_arn,
                    "DATABASE_NAME": db_name, "SHIPMENT_URL": infra.get("ShipmentUrl", ""),
                }
                bresp = ac.create_agent_runtime(
                    agentRuntimeName=AC_NAMES["baseline_runtime"],
                    roleArn=role_arn,   # the broad (unscoped) infra exec role
                    networkConfiguration={"networkMode": "PUBLIC"},
                    protocolConfiguration={"serverProtocol": "AGUI"},
                    environmentVariables=base_env,
                    agentRuntimeArtifact={"containerConfiguration": {"containerUri": base_image}},
                )
                brid = bresp.get("agentRuntimeId")
                import time as _tb
                for _ in range(60):
                    st = ac.get_agent_runtime(agentRuntimeId=brid).get("status")
                    if st == "READY":
                        break
                    if "FAILED" in (st or ""):
                        yield _sse({"type": "log", "line": f"baseline runtime FAILED: {st}"}); brid = None; break
                    _tb.sleep(5)
                if brid:
                    A.state.add_resource("agentcore-runtime", brid, "baseline",
                                         arn=f"arn:aws:bedrock-agentcore:{A.REGION}:{A.account_id()}:runtime/{brid}")
                    yield _sse({"type": "log", "line": f"baseline runtime READY (PUBLIC): {brid}"})
            except Exception as e:
                yield _sse({"type": "log", "line": f"baseline build/deploy failed (non-fatal): {e}"})
        else:
            yield _sse({"type": "log", "line": "no baseline ECR repo in stack outputs — skipping baseline."})

        # =====================================================================
        # 2) LAYER 1 — its own AgentCore runtime (per-layer deploy module). Same agent
        #    as baseline, but VPC mode so the DNS Firewall filters egress. Code lives in
        #    server/layers/layer1.py; orchestrated here so the whole stack stands up from
        #    the ONE AgentCore deploy panel.
        # =====================================================================
        from layers import layer1 as _layer1
        async for line in _layer1.deploy(A, ac, {
            "valid_subnets": valid_subnets, "sg_id": sg_id, "role_arn": role_arn,
            "cluster_arn": cluster_arn, "secret_arn": secret_arn, "db_name": db_name,
            "shipment_url": infra.get("ShipmentUrl", ""),
            "l1_repo": infra.get("EcrRepoUriLayer1"),
        }):
            if not isinstance(line, tuple):
                yield line

        # =====================================================================
        # 3) LAYER 2 — identity-scoped runtime (VPC + Okta JWT). server/layers/layer2.py.
        # =====================================================================
        from layers import layer2 as _layer2
        async for line in _layer2.deploy(A, ac, {
            "valid_subnets": valid_subnets, "sg_id": sg_id, "role_arn": role_arn,
            "cluster_arn": cluster_arn, "secret_arn": secret_arn, "db_name": db_name,
            "shipment_url": infra.get("ShipmentUrl", ""),
            "l2_repo": infra.get("EcrRepoUriLayer2"),
        }):
            if not isinstance(line, tuple):
                yield line

        # =====================================================================
        # 4) LAYER 3 — Gateway + Cedar + support/admin OBO + 2 scoped runtimes.
        #    Its own AgentCore gateway/policy/OBO (L3-suffixed names). server/layers/layer3.py.
        # =====================================================================
        from layers import layer3 as _layer3
        async for line in _layer3.deploy(A, ac, {
            "valid_subnets": valid_subnets, "sg_id": sg_id, "role_arn": role_arn,
            "cluster_arn": cluster_arn, "secret_arn": secret_arn, "db_name": db_name,
            "l3": l3, "l3_repo": infra.get("EcrRepoUriLayer3"),
        }):
            if not isinstance(line, tuple):
                yield line

        # =====================================================================
        # 5) LAYER 4 — Memory. Its OWN AgentCore resources (gateway/policy/OBO/memory/
        #    runtime, L4-suffixed) — reuses the shared IrisTools Lambdas + IrisMemory
        #    CDK CMK/role/bucket/topic/gate-table. server/layers/layer4.py.
        # =====================================================================
        from layers import layer4 as _layer4
        async for line in _layer4.deploy(A, ac, {
            "valid_subnets": valid_subnets, "sg_id": sg_id, "role_arn": role_arn,
            "cluster_arn": cluster_arn, "secret_arn": secret_arn, "db_name": db_name,
            "l3": l3, "l4": l4, "l4_repo": infra.get("EcrRepoUriLayer4"),
        }):
            if not isinstance(line, tuple):
                yield line

        # =====================================================================
        # 6) LAYER 5 — Models. Its OWN AgentCore resources; the RUNTIME runs under the
        #    scoped model-allowlist role (guardrail enforcement was registered by the
        #    CDK-stack panel). server/layers/layer5.py.
        # =====================================================================
        from layers import layer5 as _layer5
        async for line in _layer5.deploy(A, ac, {
            "valid_subnets": valid_subnets, "sg_id": sg_id, "role_arn": role_arn,
            "scoped_role_arn": scoped_role_arn,
            "cluster_arn": cluster_arn, "secret_arn": secret_arn, "db_name": db_name,
            "l3": l3, "l4": l4, "l5": l5, "l5_repo": infra.get("EcrRepoUriLayer5"),
        }):
            if not isinstance(line, tuple):
                yield line

        # =====================================================================
        # 7) LAYER 6 — Agents. Its OWN AgentCore resources (gateway/policy/OBO/memory,
        #    L6-suffixed) + the A2A Orders PEER runtime (BEAT A · confused deputy) + the
        #    goal-fenced main runtime (BEAT B · goal fence), on the scoped model role.
        #    server/layers/layer6.py.
        # =====================================================================
        from layers import layer6 as _layer6
        async for line in _layer6.deploy(A, ac, {
            "valid_subnets": valid_subnets, "sg_id": sg_id, "role_arn": role_arn,
            "scoped_role_arn": scoped_role_arn,
            "cluster_arn": cluster_arn, "secret_arn": secret_arn, "db_name": db_name,
            "l3": l3, "l4": l4, "l5": l5,
            "l6_repo": infra.get("EcrRepoUriAgent") or infra.get("EcrRepoUri"),
            "peer_repo": infra.get("EcrRepoUriPeer"),
        }):
            if not isinstance(line, tuple):
                yield line

        # Layer 7 · Observe & Contain — the CloudWatch alarm that detects the rogue-HALT
        # pattern. Created here as part of the deploy (tracked for teardown). The metric
        # itself is manufactured by a Logs metric filter attached to the rogue runtime's
        # log group at launch time (the log group only exists once the rogue is created).
        try:
            import rogue_ops as _rg
            cw = A._sess().client("cloudwatch")
            cw.put_metric_alarm(
                AlarmName=_rg.CW_ALARM, AlarmDescription="Layer 7: >=3 goal-fence blocks (deny + halt) in 60s (rogue detection)",
                Namespace=_rg.CW_NAMESPACE, MetricName=_rg.CW_METRIC, Statistic="Sum",
                Period=60, EvaluationPeriods=1, Threshold=3,
                ComparisonOperator="GreaterThanOrEqualToThreshold", TreatMissingData="notBreaching")
            A.state.add_resource("cloudwatch-alarm", _rg.CW_ALARM, "layer7", name=_rg.CW_ALARM)
            yield _sse({"type": "log", "line": f"Layer 7: CloudWatch alarm '{_rg.CW_ALARM}' created (rogue-block detection)."})
        except Exception as e:  # noqa: BLE001
            yield _sse({"type": "log", "line": f"(Layer 7 alarm not created: {str(e)[:150]})"})

        yield _sse({"type": "log", "line": "=== AGENTCORE DEPLOYED — every control from network to goal fence is live ==="})
        yield _sse({"type": "done", "phase": "layer6"})

    # Deploy-LOG phase is "agentcore" (its own log element + /api/deploy-status/agentcore).
    # Deployed RESOURCES are tracked under phase="layer6" — independent of the log key.
    A = _app()
    return StreamingResponse(A._tracked_stream("agentcore", gen()), media_type="text/event-stream")


@router.get("/api/run/layer6-peer-mode")
async def get_peer_mode():
    """Return the ACTUAL deployed A2A peer mode (+ readiness) so the UI can reflect reality
    instead of a hardcoded default. Without this, a page refresh shows 'vuln' even when the
    peer is really running 'fix' (or mid-restart), so the first prompt behaves unexpectedly."""
    A = _app()
    try:
        ac = A._sess().client("bedrock-agentcore-control")
        from layers.layer6 import PEER_RUNTIME_NAME as _PEER_NAME
        peer_id, tok = None, None
        while True:
            kw = {"maxResults": 100}
            if tok:
                kw["nextToken"] = tok
            r = ac.list_agent_runtimes(**kw)
            for rt in r.get("agentRuntimes", []):
                if rt.get("agentRuntimeName") == _PEER_NAME:
                    peer_id = rt.get("agentRuntimeId"); break
            tok = r.get("nextToken")
            if peer_id or not tok:
                break
        if not peer_id:
            return {"deployed": False}
        info = ac.get_agent_runtime(agentRuntimeId=peer_id)
        env = info.get("environmentVariables") or {}
        st = info.get("status")
        return {"deployed": True, "mode": (env.get("A2A_PEER_MODE") or "vuln").lower(),
                "ready": st == "READY", "status": st}
    except Exception as e:  # noqa: BLE001
        return {"deployed": False, "error": str(e)}


@router.post("/api/run/layer6-peer-mode")
async def set_peer_mode(request: Request):
    """Flip the A2A Orders peer between its two identity-trust modes for the demo:
      - "fix"  → propagate-identity: the peer ignores any caller-supplied customer_id,
                 acts ONLY for the signed caller, and re-validates ownership.
      - "vuln" → trust-arg: the peer trusts the customer_id argument (confused deputy).
    UpdateAgentRuntime requires the artifact/role/network re-supplied, so we read the
    current config with GetAgentRuntime and change only environmentVariables. Returns
    JSON (not SSE) — the caller just toggles a control."""
    A = _app()
    try:
        body = await request.json()
    except Exception:
        body = {}
    mode = (body or {}).get("mode", "").strip().lower()
    if mode not in ("fix", "vuln"):
        return {"error": "mode must be 'fix' or 'vuln'"}
    ac = A._sess().client("bedrock-agentcore-control")
    # Discover the LIVE peer from AWS by name (state can be stale after delete+recreate
    # cycles gave the peer a new runtime id). This guarantees we toggle the SAME peer the
    # main agent actually calls.
    from layers.layer6 import PEER_RUNTIME_NAME as _PEER_NAME
    peer_id = None
    tok = None
    while True:
        kw = {"maxResults": 100}
        if tok:
            kw["nextToken"] = tok
        r = ac.list_agent_runtimes(**kw)
        for rt in r.get("agentRuntimes", []):
            if rt.get("agentRuntimeName") == _PEER_NAME:
                peer_id = rt.get("agentRuntimeId"); break
        tok = r.get("nextToken")
        if peer_id or not tok:
            break
    if not peer_id:
        return {"error": "no A2A peer runtime deployed — run the AgentCore deploy first."}
    peer = {"id": peer_id}
    try:
        # AgentCore rejects UpdateAgentRuntime while the runtime is still UPDATING (a
        # rapid double-toggle hits this). Wait for READY first (short poll) so the second
        # toggle doesn't ConflictException.
        import time as _t
        info = ac.get_agent_runtime(agentRuntimeId=peer["id"])
        for _ in range(24):  # ~2 min max
            st = info.get("status")
            if st in ("READY", "CREATE_FAILED", "UPDATE_FAILED", None) or "FAILED" in (st or ""):
                break
            _t.sleep(5)
            info = ac.get_agent_runtime(agentRuntimeId=peer["id"])
        if info.get("status") and info["status"] not in ("READY",) and "FAILED" not in info["status"]:
            return {"error": f"peer is still {info['status']} — the previous mode change hasn't finished. "
                             f"Wait a few seconds and try again."}
        env = dict(info.get("environmentVariables") or {})
        if env.get("A2A_PEER_MODE") == mode:
            return {"ok": True, "mode": mode, "runtimeId": peer["id"], "note": "already in that mode"}
        env["A2A_PEER_MODE"] = mode
        # Rebuild networkConfiguration from scratch: get_agent_runtime returns read-only
        # fields (e.g. requireServiceS3Endpoint) that UpdateAgentRuntime REJECTS for
        # agents created after 2026-06-11. Keep only subnets + securityGroups.
        net = info.get("networkConfiguration") or {}
        nmc = net.get("networkModeConfig") or {}
        if net.get("networkMode") == "VPC":
            network_cfg = {"networkMode": "VPC", "networkModeConfig": {
                "subnets": nmc.get("subnets", []),
                "securityGroups": nmc.get("securityGroups", [])}}
        else:
            network_cfg = {"networkMode": net.get("networkMode", "PUBLIC")}
        # CRITICAL: re-supply the JWT authorizer. UpdateAgentRuntime does NOT preserve
        # authorizerConfiguration — omitting it drops the peer back to SigV4/IAM, and
        # then Iris's Bearer(OBO) call gets 403. If the live config lost it (a prior
        # toggle stripped it), reconstruct it from the gateway Okta issuer/audience so
        # the peer keeps trusting the OBO bearer.
        OKTA = A.OKTA
        authz = info.get("authorizerConfiguration")
        if not (authz and authz.get("customJWTAuthorizer")):
            authz = {"customJWTAuthorizer": {
                "discoveryUrl": f"{OKTA['gateway_issuer']}/.well-known/openid-configuration",
                "allowedAudience": [OKTA["gateway_audience"]]}}
        ac.update_agent_runtime(
            agentRuntimeId=peer["id"],
            agentRuntimeArtifact=info["agentRuntimeArtifact"],
            roleArn=info["roleArn"],
            networkConfiguration=network_cfg,
            protocolConfiguration=info.get("protocolConfiguration") or {"serverProtocol": "A2A"},
            authorizerConfiguration=authz,
            environmentVariables=env,
        )
        # update_agent_runtime creates a NEW runtime version that must RESTART before it
        # serves the new mode (~1-2 min). Returning immediately lets the user test into the
        # OLD container and see the wrong behavior. Wait for READY so the toggle only
        # reports success once the new mode is actually live.
        import time as _t
        ready = False
        for _ in range(60):   # up to ~3 min
            try:
                st = ac.get_agent_runtime(agentRuntimeId=peer["id"]).get("status")
            except Exception:
                st = None
            if st == "READY":
                ready = True
                break
            if st and "FAILED" in st:
                return {"error": f"peer update failed (status {st})", "mode": mode}
            _t.sleep(3)
        return {"ok": True, "mode": mode, "runtimeId": peer["id"], "ready": ready,
                "note": ("peer is live in the new mode" if ready else
                         "update submitted; peer still restarting — wait a few seconds before invoking")}
    except Exception as e:
        return {"error": str(e)}


@router.post("/api/run/fullstack")
async def run_fullstack(request: Request):
    """Invoke the ONE consolidated Layer 6 runtime. Carries every prior control:
    VPC+DNS-FW (L1), Okta JWT identity (L2), Gateway+Cedar+OBO tools (L3), memory +
    pre-write gate ALWAYS ON (L4), scoped model role + enforced guardrail (L5), goal
    fence (L6). model_id + session ride forwardedProps exactly as the layer runtimes."""
    A = _app()
    _sse = A._sse
    try:
        req_body = await request.json()
    except Exception:
        req_body = {}
    user_prompt = (req_body or {}).get("prompt", "").strip()
    # Layer 6 PINS Sonnet 4.5 + always-on guardrail — no model choice (the runtime env
    # also defaults to Sonnet, so this is belt-and-suspenders). We ignore any client
    # model_id.
    user_model_id = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
    customer_id = (req_body or {}).get("customerId", "")
    access_token = (req_body or {}).get("access_token", "") or (req_body or {}).get("id_token", "")
    mem_session = (req_body or {}).get("session_id", "").strip() or "iris-session-1"
    # Goal fence ON/OFF (default ON). OFF lets Beat 1's confused-deputy peer run without the
    # fence pre-empting the cross-actor order_lookup.
    gf = (req_body or {}).get("goal_fence", True)
    goal_fence = True if gf is None else bool(gf)

    async def gen():
        res_list = A.state.all_resources()
        # The MAIN goal-fenced runtime (NOT the A2A peer, which is also phase=layer6 but
        # role='a2a-peer'). Exclude the peer so we always invoke the user-facing agent.
        runtime_id = next((r.get("id") for r in res_list
                           if r.get("kind") == "agentcore-runtime" and r.get("phase") == "layer6"
                           and r.get("role") != "a2a-peer"), None)
        if not runtime_id:
            yield _sse({"type": "log", "line": "ERROR: no Layer 6 runtime deployed — run the AgentCore deploy first."})
            yield _sse({"type": "failed", "phase": "run"}); return
        prompt = user_prompt or "I'd like a refund on order A3X7K, please process it."
        yield _sse({"type": "log", "line": f"invoking Iris (Layer 6 / full stack · model=Sonnet 4.5 · guardrail ON · goal fence {'ON' if goal_fence else 'OFF'}) as {customer_id} · session={mem_session}..."})
        if not access_token:
            yield _sse({"type": "log", "line": "ERROR: no access token — please log in with Okta first."})
            yield _sse({"type": "failed", "phase": "run"}); return
        # Memory-write gate is a carried-forward control — ALWAYS ON in Layer 6.
        actor = A._decode_jwt_claims(access_token).get("customer_id") or customer_id
        # Authoritative: the live IrisMemory table (= gate Lambda's GATE_TABLE), then state.
        gate_table = A._read_cfn_outputs("IrisMemory").get("MemoryGateTable") \
            or next((r.get("id") for r in res_list if r.get("kind") == "memory-gate-table"), None)
        if gate_table and actor:
            try:
                A._sess().client("dynamodb").put_item(
                    TableName=gate_table,
                    Item={"actorId": {"S": actor}, "governed": {"BOOL": True}})
            except Exception as e:
                yield _sse({"type": "log", "line": f"gate flag set failed: {e}"})
        async for frame in A._agui_relay(runtime_id, prompt, user_model_id, "iris-layer6",
                                         bearer_token=access_token,
                                         extra_props={"session_id": mem_session, "validate_writes": True,
                                                      "goal_fence": goal_fence}):
            yield frame

    return StreamingResponse(gen(), media_type="text/event-stream")


# AgentCore resource names are FIXED (no suffix) — readable + collision-free because
# every deploy PURGES the prior generation first (see _l6_purge_agentcore). The
# AgentCore control plane has no update-in-place for these, so "clean" = delete-then-
# create. All Iris-owned names share this prefix so discovery is unambiguous.
IRIS_AC_PREFIX = "iris-"
AC_NAMES = {
    "baseline_runtime": "iris_baseline",       # runtime names must be [a-zA-Z0-9_]
    "layer1_runtime": "iris_layer1",           # same agent as baseline, VPC mode
    "main_runtime": "iris_agent",
    "peer_runtime": "iris_peer",
    "gateway": "iris-gateway",
    "policy_engine": "iris_policy",
    "memory": "iris_memory",
    "support_obo": "iris_support_obo",
}


async def _l6_purge_agentcore(A, ac):
    """Delete EVERY live Iris-owned AgentCore resource before a (re)deploy, discovered
    from AWS — not state.json, which drifts. This is what lets us use fixed names and
    guarantees no orphans (leftover runtimes hold VPC ENIs + cost; gateways/policy/OBO
    /memory linger silently). Order matters: gateway targets → gateway; runtimes before
    the VPC is reused. Yields SSE log chunks. Idempotent + best-effort per resource."""
    _sse = A._sse

    def _iris(name):
        n = (name or "").lower()
        return n.startswith("iris-") or n.startswith("iris_")

    # 1) Runtimes (baseline + peer + main) — these hold the VPC ENIs.
    deleted_runtimes = []
    try:
        tok = None
        while True:
            kw = {"maxResults": 100}
            if tok:
                kw["nextToken"] = tok
            r = ac.list_agent_runtimes(**kw)
            for rt in r.get("agentRuntimes", []):
                if _iris(rt.get("agentRuntimeName")):
                    rid = rt.get("agentRuntimeId")
                    try:
                        ac.delete_agent_runtime(agentRuntimeId=rid)
                        deleted_runtimes.append(rid)
                        A.state.remove_resource("agentcore-runtime", rid)
                        yield _sse({"type": "log", "line": f"purge: deleting runtime {rt.get('agentRuntimeName')} ({rid})"})
                    except Exception as e:
                        yield _sse({"type": "log", "line": f"purge: runtime {rid} delete: {e}"})
            tok = r.get("nextToken")
            if not tok:
                break
    except Exception as e:
        yield _sse({"type": "log", "line": f"purge: list runtimes failed: {e}"})

    # 2) Gateways (delete their targets first — a gateway with targets won't delete).
    try:
        tok = None
        while True:
            kw = {"maxResults": 100}
            if tok:
                kw["nextToken"] = tok
            r = ac.list_gateways(**kw)
            for gw in r.get("items", []):
                if not _iris(gw.get("name")):
                    continue
                gid = gw.get("gatewayId")
                try:
                    # Delete targets, then WAIT until they're actually gone. These
                    # deletes are async — delete_gateway right after still sees them
                    # ("has targets associated") unless we poll to empty first.
                    def _list_targets():
                        out, ttok = [], None
                        while True:
                            tkw = {"gatewayIdentifier": gid, "maxResults": 100}
                            if ttok:
                                tkw["nextToken"] = ttok
                            tr = ac.list_gateway_targets(**tkw)
                            out += tr.get("items", [])
                            ttok = tr.get("nextToken")
                            if not ttok:
                                return out
                    for tgt in _list_targets():
                        try:
                            ac.delete_gateway_target(gatewayIdentifier=gid, targetId=tgt.get("targetId"))
                        except Exception as te:
                            yield _sse({"type": "log", "line": f"purge: target {tgt.get('targetId')} delete: {te}"})
                    for _ in range(30):
                        if not _list_targets():
                            break
                        await asyncio.sleep(2)
                    ac.delete_gateway(gatewayIdentifier=gid)
                    A.state.remove_resource("gateway", gid)
                    yield _sse({"type": "log", "line": f"purge: deleting gateway {gw.get('name')} ({gid})"})
                except Exception as e:
                    yield _sse({"type": "log", "line": f"purge: gateway {gid} delete: {e}"})
            tok = r.get("nextToken")
            if not tok:
                break
    except Exception as e:
        yield _sse({"type": "log", "line": f"purge: list gateways failed: {e}"})

    # 3) Policy engines.
    try:
        tok = None
        while True:
            kw = {"maxResults": 100}
            if tok:
                kw["nextToken"] = tok
            r = ac.list_policy_engines(**kw)
            for pe in r.get("policyEngines", []):
                if not _iris(pe.get("name")):
                    continue
                pid = pe.get("policyEngineId")
                try:
                    # Delete the engine's policies, then WAIT until it reports empty.
                    # delete_policy is async — delete_policy_engine right after still
                    # sees them ("still contains N policies") unless we poll to empty.
                    def _list_policies():
                        out, ptok = [], None
                        while True:
                            pkw = {"policyEngineId": pid, "maxResults": 100}
                            if ptok:
                                pkw["nextToken"] = ptok
                            pr = ac.list_policies(**pkw)
                            out += pr.get("policies", [])
                            ptok = pr.get("nextToken")
                            if not ptok:
                                return out
                    for pol in _list_policies():
                        try:
                            ac.delete_policy(policyEngineId=pid, policyId=pol.get("policyId"))
                        except Exception as pe_err:
                            yield _sse({"type": "log", "line": f"purge: policy {pol.get('policyId')} delete: {pe_err}"})
                    for _ in range(30):
                        if not _list_policies():
                            break
                        await asyncio.sleep(2)
                    ac.delete_policy_engine(policyEngineId=pid)
                    A.state.remove_resource("policy-engine", pid)
                    yield _sse({"type": "log", "line": f"purge: deleting policy engine {pe.get('name')} ({pid})"})
                except Exception as e:
                    yield _sse({"type": "log", "line": f"purge: policy engine {pid} delete: {e}"})
            tok = r.get("nextToken")
            if not tok:
                break
    except Exception as e:
        yield _sse({"type": "log", "line": f"purge: list policy engines failed: {e}"})

    # 4) OBO / OAuth2 credential providers. (ListOauth2CredentialProviders caps
    #    maxResults at 20 — larger values are a ValidationException.)
    try:
        tok = None
        while True:
            kw = {"maxResults": 20}
            if tok:
                kw["nextToken"] = tok
            r = ac.list_oauth2_credential_providers(**kw)
            for cp in r.get("credentialProviders", []):
                if not _iris(cp.get("name")):
                    continue
                nm = cp.get("name")
                try:
                    ac.delete_oauth2_credential_provider(name=nm)
                    A.state.remove_resource("oauth2-credential-provider", nm)
                    yield _sse({"type": "log", "line": f"purge: deleting OBO provider {nm}"})
                except Exception as e:
                    yield _sse({"type": "log", "line": f"purge: OBO provider {nm} delete: {e}"})
            tok = r.get("nextToken")
            if not tok:
                break
    except Exception as e:
        yield _sse({"type": "log", "line": f"purge: list OBO providers failed: {e}"})

    # 5) Memories.
    try:
        tok = None
        while True:
            kw = {"maxResults": 100}
            if tok:
                kw["nextToken"] = tok
            r = ac.list_memories(**kw)
            for mem in r.get("memories", []):
                mid = mem.get("id") or ""
                if not (mid.lower().startswith("iris") or "iris" in mid.lower()):
                    continue
                try:
                    ac.delete_memory(memoryId=mid)
                    A.state.remove_resource("agentcore-memory", mid)
                    yield _sse({"type": "log", "line": f"purge: deleting memory {mid}"})
                except Exception as e:
                    yield _sse({"type": "log", "line": f"purge: memory {mid} delete: {e}"})
            tok = r.get("nextToken")
            if not tok:
                break
    except Exception as e:
        yield _sse({"type": "log", "line": f"purge: list memories failed: {e}"})

    # Wait for runtimes to finish DELETING so their ENIs release AND their fixed names
    # free up before we recreate with the SAME name (AgentCore rejects a same-name
    # create while the old one is still deleting).
    if deleted_runtimes:
        yield _sse({"type": "log", "line": "purge: waiting for runtime deletion (ENI + name release)..."})
        for rid in deleted_runtimes:
            for _ in range(30):
                try:
                    info = ac.get_agent_runtime(agentRuntimeId=rid)
                    if info.get("status") == "DELETING":
                        await asyncio.sleep(10)
                    else:
                        break
                except Exception:
                    break   # gone
    yield ("__done__",)


async def _l6_build_gateway(A, ac, l3, role_arn):
    """Create Gateway (MCP, CUSTOM_JWT, REQUEST interceptor) + 5 targets + Cedar policy
    engine (ENFORCE) + OBO credential providers, reusing the SAME Okta app. Yields SSE
    log chunks; final yield is a tuple ('__result__', gateway_id, obo_support_name)."""
    _sse = A._sse
    OKTA = A.OKTA
    gw_discovery = f"{OKTA['gateway_issuer']}/.well-known/openid-configuration"
    gw_kwargs = {
        "name": AC_NAMES["gateway"], "protocolType": "MCP", "roleArn": role_arn,
        "authorizerType": "CUSTOM_JWT",
        "authorizerConfiguration": {"customJWTAuthorizer": {
            "discoveryUrl": gw_discovery, "allowedAudience": [OKTA["gateway_audience"]]}},
    }
    interceptor_arn = l3.get("InterceptorFnArn")
    if interceptor_arn:
        gw_kwargs["interceptorConfigurations"] = [{
            "interceptor": {"lambda": {"arn": interceptor_arn}},
            "interceptionPoints": ["REQUEST"],
            "inputConfiguration": {"passRequestHeaders": True},
        }]
    yield _sse({"type": "log", "line": "creating Gateway (MCP, CUSTOM_JWT, REQUEST interceptor)..."})

    async def _delete_gateway_by_name(name):
        """Find a gateway by name, delete its targets (wait empty), delete it (wait
        gone). Used to self-heal a leftover same-named gateway the purge missed —
        create_gateway is async, so a stale name can still be present here."""
        gid_del = None
        gtok = None
        while True:
            gkw = {"maxResults": 100}
            if gtok:
                gkw["nextToken"] = gtok
            lg = ac.list_gateways(**gkw)
            for g in lg.get("items", []):
                if g.get("name") == name:
                    gid_del = g.get("gatewayId"); break
            gtok = lg.get("nextToken")
            if gid_del or not gtok:
                break
        if not gid_del:
            return

        def _tgts():
            out, ttok = [], None
            while True:
                tkw = {"gatewayIdentifier": gid_del, "maxResults": 100}
                if ttok:
                    tkw["nextToken"] = ttok
                tr = ac.list_gateway_targets(**tkw)
                out += tr.get("items", [])
                ttok = tr.get("nextToken")
                if not ttok:
                    return out
        for tgt in _tgts():
            try:
                ac.delete_gateway_target(gatewayIdentifier=gid_del, targetId=tgt.get("targetId"))
            except Exception:
                pass
        for _ in range(30):
            if not _tgts():
                break
            await asyncio.sleep(2)
        try:
            ac.delete_gateway(gatewayIdentifier=gid_del)
        except Exception:
            pass
        A.state.remove_resource("gateway", gid_del)
        for _ in range(30):
            try:
                ac.get_gateway(gatewayIdentifier=gid_del)
                await asyncio.sleep(2)
            except Exception:
                break   # gone

    try:
        gw = ac.create_gateway(**gw_kwargs)
    except ac.exceptions.ConflictException:
        yield _sse({"type": "log", "line": f"gateway '{gw_kwargs['name']}' still exists — deleting the leftover and retrying..."})
        await _delete_gateway_by_name(gw_kwargs["name"])
        gw = ac.create_gateway(**gw_kwargs)
    gateway_id = gw.get("gatewayId")
    for _ in range(30):
        gi = ac.get_gateway(gatewayIdentifier=gateway_id)
        if gi.get("status") == "READY":
            break
        await asyncio.sleep(5)
    A.state.add_resource("gateway", gateway_id, "layer6")
    yield _sse({"type": "log", "line": f"Gateway READY: {gateway_id}"})

    targets = [
        ("GetRecord", l3.get("GetRecordFnArn"), [{"name": "get_record", "description": "Look up one of the caller's own orders by order ID.",
            "inputSchema": {"type": "object", "properties": {"order_id": {"type": "string"}, "customer_id": {"type": "string", "description": "DO NOT SET. Injected from the verified token."}}, "required": ["order_id"]}}]),
        ("GetInfo", l3.get("GetInfoFnArn"), [{"name": "get_my_info", "description": "Get the caller's own customer record.",
            "inputSchema": {"type": "object", "properties": {"customer_id": {"type": "string", "description": "DO NOT SET. Injected from the verified token."}}}}]),
        ("GetShipment", l3.get("GetShipmentFnArn"), [{"name": "get_shipment", "description": "Delivery/shipment status for one of the caller's own orders.",
            "inputSchema": {"type": "object", "properties": {"order_id": {"type": "string"}, "customer_id": {"type": "string", "description": "DO NOT SET. Injected from the verified token."}}, "required": ["order_id"]}}]),
        ("ProcessRefund", l3.get("ProcessRefundFnArn"), [{"name": "process_refund", "description": "Issue a refund for one of the caller's own orders once eligibility is confirmed.",
            "inputSchema": {"type": "object", "properties": {"order_id": {"type": "string"}, "amount": {"type": "string"}, "customer_id": {"type": "string", "description": "DO NOT SET. Injected from the verified token."}}, "required": ["order_id"]}}]),
        ("UpdateRecord", l3.get("UpdateRecordFnArn"), [{"name": "update_record", "description": "Update a customer record field (name/email).",
            "inputSchema": {"type": "object", "properties": {"customer_id": {"type": "string"}, "field": {"type": "string"}, "value": {"type": "string"}, "acting_customer_id": {"type": "string", "description": "DO NOT SET. Injected from the verified token."}}, "required": ["customer_id", "field", "value"]}}]),
    ]
    for name, arn, tools in targets:
        ac.create_gateway_target(
            gatewayIdentifier=gateway_id, name=name,
            targetConfiguration={"mcp": {"lambda": {"lambdaArn": arn, "toolSchema": {"inlinePayload": tools}}}},
            credentialProviderConfigurations=[{"credentialProviderType": "GATEWAY_IAM_ROLE"}])
    yield _sse({"type": "log", "line": "Gateway targets created (5 typed tools)"})

    async def _delete_policy_engine_by_name(name):
        """Self-heal a leftover same-named policy engine the purge missed: delete its
        policies (wait empty), delete the engine (wait gone)."""
        pid_del = None
        etok = None
        while True:
            ekw = {"maxResults": 100}
            if etok:
                ekw["nextToken"] = etok
            le = ac.list_policy_engines(**ekw)
            for e in le.get("policyEngines", []):
                if e.get("name") == name:
                    pid_del = e.get("policyEngineId"); break
            etok = le.get("nextToken")
            if pid_del or not etok:
                break
        if not pid_del:
            return

        def _pols():
            out, ptok = [], None
            while True:
                pkw = {"policyEngineId": pid_del, "maxResults": 100}
                if ptok:
                    pkw["nextToken"] = ptok
                pr = ac.list_policies(**pkw)
                out += pr.get("policies", [])
                ptok = pr.get("nextToken")
                if not ptok:
                    return out
        for pol in _pols():
            try:
                ac.delete_policy(policyEngineId=pid_del, policyId=pol.get("policyId"))
            except Exception:
                pass
        for _ in range(30):
            if not _pols():
                break
            await asyncio.sleep(2)
        try:
            ac.delete_policy_engine(policyEngineId=pid_del)
        except Exception:
            pass
        A.state.remove_resource("policy-engine", pid_del)
        for _ in range(30):
            try:
                ac.get_policy_engine(policyEngineId=pid_del)
                await asyncio.sleep(2)
            except Exception:
                break

    try:
        pe = ac.create_policy_engine(name=AC_NAMES["policy_engine"])
    except ac.exceptions.ConflictException:
        yield _sse({"type": "log", "line": f"policy engine '{AC_NAMES['policy_engine']}' still exists — deleting the leftover and retrying..."})
        await _delete_policy_engine_by_name(AC_NAMES["policy_engine"])
        pe = ac.create_policy_engine(name=AC_NAMES["policy_engine"])
    pe_id = pe.get("policyEngineId"); pe_arn = pe.get("policyEngineArn")
    for _ in range(20):
        pei = ac.get_policy_engine(policyEngineId=pe_id)
        if pei.get("status") == "ACTIVE":
            pe_arn = pe_arn or pei.get("policyEngineArn"); break
        await asyncio.sleep(3)
    A.state.add_resource("policy-engine", pe_id, "layer6")
    gw_arn = ac.get_gateway(gatewayIdentifier=gateway_id).get("gatewayArn", f"arn:aws:bedrock-agentcore:{A.REGION}:{A.account_id()}:gateway/{gateway_id}")
    read_pol = ('permit(\n  principal is AgentCore::OAuthUser,\n  action in [\n'
                '    AgentCore::Action::"GetRecord___get_record",\n    AgentCore::Action::"GetInfo___get_my_info",\n'
                '    AgentCore::Action::"GetShipment___get_shipment",\n    AgentCore::Action::"ProcessRefund___process_refund"\n  ],\n'
                f'  resource == AgentCore::Gateway::"{gw_arn}"\n)\nwhen {{\n  principal.hasTag("scp") &&\n  principal.getTag("scp") like "*tool:read*"\n}};')
    upd_pol = ('permit(\n  principal is AgentCore::OAuthUser,\n  action == AgentCore::Action::"UpdateRecord___update_record",\n'
               f'  resource == AgentCore::Gateway::"{gw_arn}"\n)\nwhen {{\n  principal.hasTag("scp") &&\n  principal.getTag("scp") like "*tool:update*"\n}};')
    # Policy names are ACCOUNT-global (not per-engine); L6 uses L6-prefixed names so
    # they never collide with Layer 3's (L3SupportReadOnly/L3AdminUpdate).
    for base, stmt in (("L6SupportReadOnly", read_pol), ("L6AdminUpdate", upd_pol)):
        try:
            ac.create_policy(policyEngineId=pe_id, name=base,
                             definition={"cedar": {"statement": stmt}}, validationMode="IGNORE_ALL_FINDINGS")
        except ac.exceptions.ConflictException:
            yield _sse({"type": "log", "line": f"policy '{base}' exists — reusing/skipping"})
    upd = dict(gatewayIdentifier=gateway_id, name=gw_kwargs["name"], roleArn=gw_kwargs["roleArn"],
               protocolType=gw_kwargs["protocolType"], authorizerType=gw_kwargs["authorizerType"],
               authorizerConfiguration=gw_kwargs["authorizerConfiguration"],
               policyEngineConfiguration={"arn": pe_arn, "mode": "ENFORCE"})
    if gw_kwargs.get("interceptorConfigurations"):
        upd["interceptorConfigurations"] = gw_kwargs["interceptorConfigurations"]
    ac.update_gateway(**upd)
    yield _sse({"type": "log", "line": "Cedar policy engine attached (ENFORCE); interceptor re-passed"})

    gw_disc = f"{OKTA['gateway_issuer']}/.well-known/openid-configuration"
    obo_support = None
    if OKTA.get("support_delegate_client_secret"):
        name = AC_NAMES["support_obo"]
        obo_kwargs = dict(
            name=name, credentialProviderVendor="CustomOauth2",
            oauth2ProviderConfigInput={"customOauth2ProviderConfig": {
                "oauthDiscovery": {"discoveryUrl": gw_disc},
                "clientId": OKTA["support_delegate_client_id"],
                "clientSecret": OKTA["support_delegate_client_secret"],
                "clientAuthenticationMethod": "CLIENT_SECRET_POST",
                "onBehalfOfTokenExchangeConfig": {"grantType": "TOKEN_EXCHANGE",
                    "tokenExchangeGrantTypeConfig": {"actorTokenContent": "NONE"}}}})
        try:
            ac.create_oauth2_credential_provider(**obo_kwargs)
        except ac.exceptions.ConflictException:
            # Self-heal a leftover same-named provider the purge missed — delete it,
            # wait until it's gone (get_* throws), then retry the create.
            yield _sse({"type": "log", "line": f"OBO provider '{name}' still exists — deleting the leftover and retrying..."})
            try:
                ac.delete_oauth2_credential_provider(name=name)
            except Exception:
                pass
            for _ in range(30):
                try:
                    ac.get_oauth2_credential_provider(name=name)
                    await asyncio.sleep(2)
                except Exception:
                    break   # gone
            ac.create_oauth2_credential_provider(**obo_kwargs)
        obo_support = name
        A.state.add_resource("oauth2-credential-provider", name, "layer6")
        yield _sse({"type": "log", "line": f"OBO credential provider created: {name}"})
    yield ("__result__", gateway_id, obo_support)
