"""Layer 4 (Memory) — per-layer AgentCore deploy module.

Adds per-actor MEMORY governance on top of the Layer 3 tool controls. Per the design
rule that each layer owns its OWN AgentCore resources, Layer 4 does NOT reuse Layer
3's gateway/OBO — it stands up its own:
  - Gateway  iris-gateway-l4  (MCP · CUSTOM_JWT · REQUEST interceptor)  + Cedar policy
    engine iris_policy_l4 (support-read/admin-update) + OBO provider iris_support_obo_l4
  - AgentCore Memory iris_memory_l4 — SELF-MANAGED strategy: AgentCore does no
    extraction; on a trigger it delivers the raw turn to our S3 bucket + pings SNS, and
    our pre-write gate Lambda (from the IrisMemory CDK stack) extracts + grounds facts
    and DROPS ungrounded self-asserted entitlements before BatchCreateMemoryRecords.
    Namespace /facts/{actorId}/ keyed by the verified OBO customer_id.
  - Runtime iris_layer4 — the memory agent (process_refund + automatic per-actor memory).

Reuses the SHARED infra (IrisTools tool Lambdas via ctx.l3; IrisMemory CDK CMK/role/
bucket/topic/gate-table via ctx.l4; VPC/VPCEs/DNS-FW). Tracked under phase "layer4".
The memory-poisoning demo: OFF → poison persists; ON (validate writes) → gate drops it.
"""
import asyncio
import os
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
AGENT_LAYER4_DIR = os.path.abspath(os.path.join(HERE, "..", "..", "agent-layer4"))

GATEWAY_NAME = "iris-gateway-l4"
POLICY_ENGINE_NAME = "iris_policy_l4"
SUPPORT_OBO = "iris_support_obo_l4"
MEMORY_NAME = "iris_memory_l4"
RUNTIME_NAME = "iris_layer4"


def _targets(l3):
    return [
        ("GetRecord", l3.get("GetRecordFnArn"), [{"name": "get_record", "description": "Look up one of the caller's own orders (incl. refund_eligible).",
            "inputSchema": {"type": "object", "properties": {"order_id": {"type": "string"}, "customer_id": {"type": "string", "description": "DO NOT SET. Injected from the verified token."}}, "required": ["order_id"]}}]),
        ("GetInfo", l3.get("GetInfoFnArn"), [{"name": "get_my_info", "description": "Get the caller's own customer record.",
            "inputSchema": {"type": "object", "properties": {"customer_id": {"type": "string", "description": "DO NOT SET. Injected from the verified token."}}}}]),
        ("GetShipment", l3.get("GetShipmentFnArn"), [{"name": "get_shipment", "description": "Delivery/shipment status for one of the caller's own orders.",
            "inputSchema": {"type": "object", "properties": {"order_id": {"type": "string"}, "customer_id": {"type": "string", "description": "DO NOT SET. Injected from the verified token."}}, "required": ["order_id"]}}]),
        ("ProcessRefund", l3.get("ProcessRefundFnArn"), [{"name": "process_refund", "description": "Issue a refund for one of the caller's own orders once eligibility is confirmed.",
            "inputSchema": {"type": "object", "properties": {"order_id": {"type": "string"}, "amount": {"type": "string"}, "customer_id": {"type": "string", "description": "DO NOT SET. Injected from the verified token."}}, "required": ["order_id"]}}]),
        ("UpdateRecord", l3.get("UpdateRecordFnArn"), [{"name": "update_record", "description": "Update a customer record field (name/email). Admin only.",
            "inputSchema": {"type": "object", "properties": {"customer_id": {"type": "string"}, "field": {"type": "string"}, "value": {"type": "string"}, "acting_customer_id": {"type": "string", "description": "DO NOT SET. Injected from the verified token."}}, "required": ["customer_id", "field", "value"]}}]),
    ]


async def _delete_gateway_by_name(ac, A, name):
    gid=None; tok=None
    while True:
        kw={"maxResults":100}
        if tok: kw["nextToken"]=tok
        lg=ac.list_gateways(**kw)
        for g in lg.get("items",[]):
            if g.get("name")==name: gid=g.get("gatewayId"); break
        tok=lg.get("nextToken")
        if gid or not tok: break
    if not gid: return
    def _tgts():
        out=[]; t=None
        while True:
            kw={"gatewayIdentifier":gid,"maxResults":100}
            if t: kw["nextToken"]=t
            r=ac.list_gateway_targets(**kw); out+=r.get("items",[]); t=r.get("nextToken")
            if not t: return out
    for tg in _tgts():
        try: ac.delete_gateway_target(gatewayIdentifier=gid, targetId=tg.get("targetId"))
        except Exception: pass
    for _ in range(30):
        if not _tgts(): break
        await asyncio.sleep(2)
    try: ac.delete_gateway(gatewayIdentifier=gid)
    except Exception: pass
    A.state.remove_resource("gateway", gid)
    for _ in range(30):
        try: ac.get_gateway(gatewayIdentifier=gid); await asyncio.sleep(2)
        except Exception: break


async def _delete_policy_engine_by_name(ac, A, name):
    pid=None; tok=None
    while True:
        kw={"maxResults":100}
        if tok: kw["nextToken"]=tok
        le=ac.list_policy_engines(**kw)
        for e in le.get("policyEngines",[]):
            if e.get("name")==name: pid=e.get("policyEngineId"); break
        tok=le.get("nextToken")
        if pid or not tok: break
    if not pid: return
    def _pols():
        out=[]; t=None
        while True:
            kw={"policyEngineId":pid,"maxResults":100}
            if t: kw["nextToken"]=t
            r=ac.list_policies(**kw); out+=r.get("policies",[]); t=r.get("nextToken")
            if not t: return out
    for pol in _pols():
        try: ac.delete_policy(policyEngineId=pid, policyId=pol.get("policyId"))
        except Exception: pass
    for _ in range(30):
        if not _pols(): break
        await asyncio.sleep(2)
    try: ac.delete_policy_engine(policyEngineId=pid)
    except Exception: pass
    A.state.remove_resource("policy-engine", pid)
    for _ in range(30):
        try: ac.get_policy_engine(policyEngineId=pid); await asyncio.sleep(2)
        except Exception: break


async def _make_obo(ac, A, name, cid, secret, gw_disc):
    kwargs=dict(name=name, credentialProviderVendor="CustomOauth2",
        oauth2ProviderConfigInput={"customOauth2ProviderConfig":{
            "oauthDiscovery":{"discoveryUrl":gw_disc}, "clientId":cid, "clientSecret":secret,
            "clientAuthenticationMethod":"CLIENT_SECRET_POST",
            "onBehalfOfTokenExchangeConfig":{"grantType":"TOKEN_EXCHANGE",
                "tokenExchangeGrantTypeConfig":{"actorTokenContent":"NONE"}}}})
    try:
        ac.create_oauth2_credential_provider(**kwargs)
    except ac.exceptions.ConflictException:
        try: ac.delete_oauth2_credential_provider(name=name)
        except Exception: pass
        for _ in range(30):
            try: ac.get_oauth2_credential_provider(name=name); await asyncio.sleep(2)
            except Exception: break
        ac.create_oauth2_credential_provider(**kwargs)
    A.state.add_resource("oauth2-credential-provider", name, "layer4")


async def deploy(A, ac, ctx):
    """Build + deploy Layer 4: own Gateway + Cedar + OBO + Memory + runtime. ctx keys:
    valid_subnets, sg_id, role_arn, cluster_arn, secret_arn, db_name, l3 (IrisTools
    outputs), l4 (IrisMemory outputs), l4_repo. Yields SSE; ends with ('__done__',)."""
    _sse = A._sse
    OKTA = A.OKTA
    yield _sse({"type": "log", "line": "[5/7] Layer 4 agent (own Gateway + Memory · per-actor pre-write gate)..."})

    l3 = ctx.get("l3") or {}       # shared tool Lambdas + interceptor
    l4 = ctx.get("l4") or {}       # IrisMemory CDK: CMK/role/bucket/topic/gate-table
    l4_repo = ctx.get("l4_repo")
    valid_subnets = ctx.get("valid_subnets") or []
    role_arn = ctx.get("role_arn"); sg_id = ctx.get("sg_id")
    if not l4_repo or not valid_subnets:
        yield _sse({"type": "log", "line": "no Layer 4 ECR repo or subnets — skipping Layer 4."})
        yield ("__done__",); return
    if not OKTA.get("support_delegate_client_secret"):
        yield _sse({"type": "log", "line": "no Okta support delegate secret — skipping Layer 4."})
        yield ("__done__",); return

    # ---- Gateway + Cedar + OBO (L4's own) ----
    gateway_id = None
    try:
        gw_discovery = f"{OKTA['gateway_issuer']}/.well-known/openid-configuration"
        gw_kwargs = {"name": GATEWAY_NAME, "protocolType": "MCP", "roleArn": role_arn,
            "authorizerType": "CUSTOM_JWT",
            "authorizerConfiguration": {"customJWTAuthorizer": {
                "discoveryUrl": gw_discovery, "allowedAudience": [OKTA["gateway_audience"]]}}}
        interceptor_arn = l3.get("InterceptorFnArn")
        if interceptor_arn:
            gw_kwargs["interceptorConfigurations"] = [{
                "interceptor": {"lambda": {"arn": interceptor_arn}},
                "interceptionPoints": ["REQUEST"], "inputConfiguration": {"passRequestHeaders": True}}]
        yield _sse({"type": "log", "line": "creating Layer 4 Gateway (MCP · CUSTOM_JWT · interceptor)..."})
        try:
            gw = ac.create_gateway(**gw_kwargs)
        except ac.exceptions.ConflictException:
            yield _sse({"type": "log", "line": f"gateway '{GATEWAY_NAME}' exists — deleting leftover + retrying..."})
            await _delete_gateway_by_name(ac, A, GATEWAY_NAME)
            gw = ac.create_gateway(**gw_kwargs)
        gateway_id = gw.get("gatewayId")
        for _ in range(30):
            if ac.get_gateway(gatewayIdentifier=gateway_id).get("status") == "READY":
                break
            await asyncio.sleep(5)
        A.state.add_resource("gateway", gateway_id, "layer4")
        yield _sse({"type": "log", "line": f"Gateway READY: {gateway_id}"})

        for name, arn, tools in _targets(l3):
            ac.create_gateway_target(gatewayIdentifier=gateway_id, name=name,
                targetConfiguration={"mcp": {"lambda": {"lambdaArn": arn, "toolSchema": {"inlinePayload": tools}}}},
                credentialProviderConfigurations=[{"credentialProviderType": "GATEWAY_IAM_ROLE"}])
        yield _sse({"type": "log", "line": "Gateway targets created (5 typed tools)"})

        try:
            pe = ac.create_policy_engine(name=POLICY_ENGINE_NAME)
        except ac.exceptions.ConflictException:
            yield _sse({"type": "log", "line": f"policy engine '{POLICY_ENGINE_NAME}' exists — deleting leftover + retrying..."})
            await _delete_policy_engine_by_name(ac, A, POLICY_ENGINE_NAME)
            pe = ac.create_policy_engine(name=POLICY_ENGINE_NAME)
        pe_id = pe.get("policyEngineId"); pe_arn = pe.get("policyEngineArn")
        for _ in range(20):
            pei = ac.get_policy_engine(policyEngineId=pe_id)
            if pei.get("status") == "ACTIVE":
                pe_arn = pe_arn or pei.get("policyEngineArn"); break
            await asyncio.sleep(3)
        A.state.add_resource("policy-engine", pe_id, "layer4")
        gw_arn = ac.get_gateway(gatewayIdentifier=gateway_id).get("gatewayArn", f"arn:aws:bedrock-agentcore:{A.REGION}:{A.account_id()}:gateway/{gateway_id}")
        read_pol = ('permit(\n  principal is AgentCore::OAuthUser,\n  action in [\n'
                    '    AgentCore::Action::"GetRecord___get_record",\n    AgentCore::Action::"GetInfo___get_my_info",\n'
                    '    AgentCore::Action::"GetShipment___get_shipment",\n    AgentCore::Action::"ProcessRefund___process_refund"\n  ],\n'
                    f'  resource == AgentCore::Gateway::"{gw_arn}"\n)\nwhen {{\n  principal.hasTag("scp") &&\n  principal.getTag("scp") like "*tool:read*"\n}};')
        upd_pol = ('permit(\n  principal is AgentCore::OAuthUser,\n  action == AgentCore::Action::"UpdateRecord___update_record",\n'
                   f'  resource == AgentCore::Gateway::"{gw_arn}"\n)\nwhen {{\n  principal.hasTag("scp") &&\n  principal.getTag("scp") like "*tool:update*"\n}};')
        for base, stmt in (("L4SupportReadOnly", read_pol), ("L4AdminUpdate", upd_pol)):
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

        await _make_obo(ac, A, SUPPORT_OBO, OKTA["support_delegate_client_id"], OKTA["support_delegate_client_secret"],
                        f"{OKTA['gateway_issuer']}/.well-known/openid-configuration")
        yield _sse({"type": "log", "line": f"OBO provider created: {SUPPORT_OBO} (support)"})
    except Exception as e:
        yield _sse({"type": "log", "line": f"Layer 4 Gateway/Policy/OBO failed: {e}"})
        yield _sse({"type": "log", "line": traceback.format_exc().splitlines()[-1]})
        yield ("__done__",); return
    if not gateway_id:
        yield ("__done__",); return

    # ---- Memory (self-managed pre-write gate) ----
    memory_id, strategy_id = None, ""
    try:
        yield _sse({"type": "log", "line": "creating Layer 4 Memory (self-managed pre-write gate)..."})
        create_kwargs = dict(
            name=MEMORY_NAME, eventExpiryDuration=30,
            memoryStrategies=[{"customMemoryStrategy": {
                "name": "IrisPerActorFacts",
                "description": "Self-managed pre-write grounding gate (per-actor /facts/)",
                "configuration": {"selfManagedConfiguration": {
                    "triggerConditions": [{"messageBasedTrigger": {"messageCount": 2}},
                                          {"timeBasedTrigger": {"idleSessionTimeout": 60}}],
                    "historicalContextWindowSize": 2,
                    "invocationConfiguration": {
                        "payloadDeliveryBucketName": l4.get("MemoryPayloadBucket"),
                        "topicArn": l4.get("MemoryJobsTopicArn")}}}}}])
        if l4.get("MemoryCmkArn"): create_kwargs["encryptionKeyArn"] = l4["MemoryCmkArn"]
        if l4.get("MemoryExecRoleArn"): create_kwargs["memoryExecutionRoleArn"] = l4["MemoryExecRoleArn"]
        mem_resp = ac.create_memory(**create_kwargs)
        memory = mem_resp.get("memory", mem_resp)
        memory_id = memory.get("id") or memory.get("memoryId")
        yield _sse({"type": "log", "line": f"Memory creating {memory_id} — waiting ACTIVE..."})
        for _ in range(40):
            info = ac.get_memory(memoryId=memory_id).get("memory", {})
            st = info.get("status")
            if st == "ACTIVE":
                strats = info.get("strategies") or info.get("memoryStrategies") or []
                if strats:
                    strategy_id = strats[0].get("strategyId") or strats[0].get("memoryStrategyId") or ""
                break
            if "FAILED" in (st or ""):
                yield _sse({"type": "log", "line": f"Memory FAILED: {info.get('failureReason')}"}); memory_id = None; break
            await asyncio.sleep(3)
        if memory_id:
            A.state.add_resource("agentcore-memory", memory_id, "layer4", strategy_id=strategy_id)
            yield _sse({"type": "log", "line": f"Memory ACTIVE: {memory_id} (self-managed gate · /facts/{{actorId}}/)"})
    except Exception as e:
        yield _sse({"type": "log", "line": f"Layer 4 Memory failed: {e}"})
    if not memory_id:
        yield ("__done__",); return

    # ---- Runtime (memory agent) ----
    try:
        image = A._build_and_push_agent_image(l4_repo, AGENT_LAYER4_DIR)
        yield _sse({"type": "log", "line": f"pushed Layer 4 image: {image}"})
        discovery = f"{OKTA['agent_issuer']}/.well-known/openid-configuration"
        yield _sse({"type": "log", "line": "creating Layer 4 runtime (VPC + JWT + Gateway + Memory)..."})
        resp = ac.create_agent_runtime(
            agentRuntimeName=RUNTIME_NAME, roleArn=role_arn,
            networkConfiguration={"networkMode": "VPC", "networkModeConfig": {
                "subnets": valid_subnets, "securityGroups": [sg_id] if sg_id else []}},
            protocolConfiguration={"serverProtocol": "AGUI"},
            authorizerConfiguration={"customJWTAuthorizer": {
                "discoveryUrl": discovery, "allowedAudience": [OKTA["agent_audience"]],
                "allowedScopes": [OKTA["support_agent_scope"]]}},
            requestHeaderConfiguration={"requestHeaderAllowlist": ["Authorization"]},
            environmentVariables={
                "AGENT_TYPE": "support", "GATEWAY_ID": gateway_id, "OBO_PROVIDER_NAME": SUPPORT_OBO,
                "OKTA_GATEWAY_AUDIENCE": OKTA["gateway_audience"], "TOOL_SCOPE": "tool:read",
                "MEMORY_ID": memory_id, "MEMORY_STRATEGY_ID": strategy_id,
                "CLUSTER_ARN": ctx.get("cluster_arn", ""), "SECRET_ARN": ctx.get("secret_arn", ""),
                "DATABASE_NAME": ctx.get("db_name", "irisdb")},
            agentRuntimeArtifact={"containerConfiguration": {"containerUri": image}})
        rid = resp.get("agentRuntimeId")
        import time as _t
        for _ in range(60):
            st = ac.get_agent_runtime(agentRuntimeId=rid).get("status")
            if st == "READY":
                break
            if "FAILED" in (st or ""):
                yield _sse({"type": "log", "line": f"Layer 4 runtime FAILED: {st}"}); rid = None; break
            _t.sleep(5)
        if rid:
            A.state.add_resource("agentcore-runtime", rid, "layer4",
                                 arn=f"arn:aws:bedrock-agentcore:{A.REGION}:{A.account_id()}:runtime/{rid}")
            yield _sse({"type": "log", "line": f"Layer 4 runtime READY (VPC + JWT + Memory): {rid}"})
    except Exception as e:
        yield _sse({"type": "log", "line": f"Layer 4 runtime failed (non-fatal): {e}"})
    yield ("__done__",)
