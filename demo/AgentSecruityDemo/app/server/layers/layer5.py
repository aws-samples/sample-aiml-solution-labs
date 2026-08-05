"""Layer 5 (Models) — per-layer AgentCore deploy module.

The agent is a BYTE-FOR-BYTE copy of Layer 4 (same memory + Gateway tools). Every
Layer 5 control is OUTSIDE the agent — that's the whole point: "choosing the right,
governed model, enforced by IAM + a guardrail, is itself a security control."

Two controls, both deployment-side (the agent can't opt out):
  1. IAM model allowlist — the runtime runs under the SCOPED exec role from the
     IrisModels CDK stack (ctx.scoped_role_arn): bedrock:InvokeModel*/Converse* allowed
     ONLY on the approved model ARNs (qwen + Sonnet 4.5 CRIS/AIP); everything else is
     explicit-deny. A model id typed into the UI that isn't approved → AccessDenied.
  2. Auto-enforced Guardrail — the account-level PutEnforcedGuardrailConfiguration
     (scoped to the approved Sonnet model) is registered by the CDK-stack deploy panel,
     so it applies to every Sonnet call WITHOUT the agent attaching a GuardrailIdentifier.

Per the per-layer rule, L5 owns its own AgentCore resources (gateway iris-gateway-l5,
policy iris_policy_l5, OBO iris_support_obo_l5, memory iris_memory_l5, runtime
iris_layer5 on the scoped role). Reuses shared IrisTools Lambdas + IrisMemory CDK
CMK/role/bucket/topic. Tracked under phase "layer5".
"""
import asyncio
import os
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
AGENT_LAYER5_DIR = os.path.abspath(os.path.join(HERE, "..", "..", "agent-layer5"))

GATEWAY_NAME = "iris-gateway-l5"
POLICY_ENGINE_NAME = "iris_policy_l5"
SUPPORT_OBO = "iris_support_obo_l5"
MEMORY_NAME = "iris_memory_l5"
RUNTIME_NAME = "iris_layer5"


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
    A.state.add_resource("oauth2-credential-provider", name, "layer5")


async def deploy(A, ac, ctx):
    """Build + deploy Layer 5: own Gateway + Cedar + OBO + Memory + runtime on the
    SCOPED model-allowlist role. ctx keys: valid_subnets, sg_id, role_arn,
    scoped_role_arn, cluster_arn, secret_arn, db_name, l3 (IrisTools outputs), l4
    (IrisMemory outputs), l5_repo. Yields SSE; ends with ('__done__',)."""
    _sse = A._sse
    OKTA = A.OKTA
    yield _sse({"type": "log", "line": "[6/7] Layer 5 agent (model allowlist + auto-enforced guardrail · scoped role)..."})

    l3 = ctx.get("l3") or {}
    l4 = ctx.get("l4") or {}
    l5 = ctx.get("l5") or {}
    l5_repo = ctx.get("l5_repo")
    valid_subnets = ctx.get("valid_subnets") or []
    role_arn = ctx.get("role_arn"); sg_id = ctx.get("sg_id")
    scoped_role_arn = ctx.get("scoped_role_arn") or role_arn   # the L5 model-allowlist role
    if not l5_repo or not valid_subnets:
        yield _sse({"type": "log", "line": "no Layer 5 ECR repo or subnets — skipping Layer 5."})
        yield ("__done__",); return
    if not OKTA.get("support_delegate_client_secret"):
        yield _sse({"type": "log", "line": "no Okta support delegate secret — skipping Layer 5."})
        yield ("__done__",); return

    # ---- Gateway + Cedar + OBO (L5's own). Gateway role stays the broad infra role;
    #      only the RUNTIME uses the scoped model-allowlist role (that's the L5 control). ----
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
        yield _sse({"type": "log", "line": "creating Layer 5 Gateway (MCP · CUSTOM_JWT · interceptor)..."})
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
        A.state.add_resource("gateway", gateway_id, "layer5")
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
        A.state.add_resource("policy-engine", pe_id, "layer5")
        gw_arn = ac.get_gateway(gatewayIdentifier=gateway_id).get("gatewayArn", f"arn:aws:bedrock-agentcore:{A.REGION}:{A.account_id()}:gateway/{gateway_id}")
        read_pol = ('permit(\n  principal is AgentCore::OAuthUser,\n  action in [\n'
                    '    AgentCore::Action::"GetRecord___get_record",\n    AgentCore::Action::"GetInfo___get_my_info",\n'
                    '    AgentCore::Action::"GetShipment___get_shipment",\n    AgentCore::Action::"ProcessRefund___process_refund"\n  ],\n'
                    f'  resource == AgentCore::Gateway::"{gw_arn}"\n)\nwhen {{\n  principal.hasTag("scp") &&\n  principal.getTag("scp") like "*tool:read*"\n}};')
        upd_pol = ('permit(\n  principal is AgentCore::OAuthUser,\n  action == AgentCore::Action::"UpdateRecord___update_record",\n'
                   f'  resource == AgentCore::Gateway::"{gw_arn}"\n)\nwhen {{\n  principal.hasTag("scp") &&\n  principal.getTag("scp") like "*tool:update*"\n}};')
        for base, stmt in (("L5SupportReadOnly", read_pol), ("L5AdminUpdate", upd_pol)):
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
        yield _sse({"type": "log", "line": f"Layer 5 Gateway/Policy/OBO failed: {e}"})
        yield _sse({"type": "log", "line": traceback.format_exc().splitlines()[-1]})
        yield ("__done__",); return
    if not gateway_id:
        yield ("__done__",); return

    # ---- Memory (SELF-MANAGED strategy — SAME flow as Layer 4, its OWN L5 resource) ----
    # Layer 5 gets its OWN memory (iris_memory_l5), but on the SAME self-managed pre-write
    # gate pipeline Layer 4 deploys: it reuses L4's payload bucket + SNS topic + gate
    # Lambda + exec role + CMK. The gate Lambda routes by the memoryId in the SNS job, so
    # L5's writes land in L5's memory (not L4's). We DON'T re-deploy any of that infra here.
    #
    # Layer 5 is NOT about memory — its story is the MODEL + GUARDRAIL + IAM. Memory just
    # has to behave correctly and stay out of the way. So L5 runs the gate with validation
    # ON (the L5 run handler always sends validate_writes=true → per-actor governed flag ON),
    # i.e. the L4 "Validate memory writes" CHECKED behaviour: the pre-write gate grounds each
    # fact and drops ungrounded self-asserted entitlements before they're written. The demo
    # UI never surfaces any of this — no session / inspect / delete controls, no poison prompt.
    memory_id, strategy_id = None, ""
    try:
        payload_bucket = l4.get("MemoryPayloadBucket")
        jobs_topic_arn = l4.get("MemoryJobsTopicArn")
        if not (payload_bucket and jobs_topic_arn):
            yield _sse({"type": "log", "line": "ERROR: Layer 4 self-managed gate infra (payload bucket / jobs topic) not found — deploy Layer 4 first (L5 reuses its gate pipeline)."})
            yield ("__done__",); return
        yield _sse({"type": "log", "line": "creating Layer 5 Memory (self-managed pre-write gate · reuses L4 gate pipeline · its own L5 resource)..."})
        create_kwargs = dict(
            name=MEMORY_NAME, eventExpiryDuration=30,
            memoryStrategies=[{"customMemoryStrategy": {
                "name": "IrisPerActorFacts",
                "description": "Self-managed pre-write grounding gate (per-actor /facts/) — Layer 5 own resource",
                "configuration": {"selfManagedConfiguration": {
                    "triggerConditions": [{"messageBasedTrigger": {"messageCount": 2}},
                                          {"timeBasedTrigger": {"idleSessionTimeout": 60}}],
                    "historicalContextWindowSize": 2,
                    "invocationConfiguration": {
                        "payloadDeliveryBucketName": payload_bucket,
                        "topicArn": jobs_topic_arn}}}}}])
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
            A.state.add_resource("agentcore-memory", memory_id, "layer5", strategy_id=strategy_id)
            yield _sse({"type": "log", "line": f"Memory ACTIVE: {memory_id} (self-managed gate · reuses L4 pipeline · /facts/{{actorId}}/)"})
    except Exception as e:
        yield _sse({"type": "log", "line": f"Layer 5 Memory failed: {e}"})
    if not memory_id:
        yield ("__done__",); return

    # ---- Runtime on the SCOPED model-allowlist role (the L5 control) ----
    try:
        image = A._build_and_push_agent_image(l5_repo, AGENT_LAYER5_DIR)
        yield _sse({"type": "log", "line": f"pushed Layer 5 image: {image}"})
        discovery = f"{OKTA['agent_issuer']}/.well-known/openid-configuration"
        yield _sse({"type": "log", "line": f"creating Layer 5 runtime on SCOPED role (model allowlist): {scoped_role_arn.split('/')[-1]}"})
        resp = ac.create_agent_runtime(
            agentRuntimeName=RUNTIME_NAME, roleArn=scoped_role_arn,   # <-- the L5 control
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
                "DATABASE_NAME": ctx.get("db_name", "irisdb"),
                # Mandatory guardrail: the scoped role's IAM condition DENIES any model
                # call that doesn't carry THIS guardrail, so the agent attaches it (id +
                # version) on every Converse request + tags the user turn.
                "GUARDRAIL_ID": l5.get("DefaultGuardrailId", ""),
                "GUARDRAIL_VERSION": l5.get("DefaultGuardrailVersion", "")},
            agentRuntimeArtifact={"containerConfiguration": {"containerUri": image}})
        rid = resp.get("agentRuntimeId")
        import time as _t
        for _ in range(60):
            st = ac.get_agent_runtime(agentRuntimeId=rid).get("status")
            if st == "READY":
                break
            if "FAILED" in (st or ""):
                yield _sse({"type": "log", "line": f"Layer 5 runtime FAILED: {st}"}); rid = None; break
            _t.sleep(5)
        if rid:
            A.state.add_resource("agentcore-runtime", rid, "layer5",
                                 arn=f"arn:aws:bedrock-agentcore:{A.REGION}:{A.account_id()}:runtime/{rid}")
            yield _sse({"type": "log", "line": f"Layer 5 runtime READY (scoped model role + mandatory guardrail via IAM condition): {rid}"})
    except Exception as e:
        yield _sse({"type": "log", "line": f"Layer 5 runtime failed (non-fatal): {e}"})
    yield ("__done__",)
