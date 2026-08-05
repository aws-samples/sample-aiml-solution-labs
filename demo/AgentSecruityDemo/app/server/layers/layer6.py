"""Layer 6 (Agents) — per-layer AgentCore deploy module.

Layer 6 is the AGENTS layer: it governs the agent as a WHOLE across its trajectory
AND its collaboration with other agents. It carries forward every prior control (VPC
+ DNS-FW, Okta JWT identity, Gateway+Cedar+OBO tools, memory + pre-write gate, scoped
model role + enforced guardrail) and adds the two controls none of the earlier layers
could express, demonstrated as TWO SEPARATE problem→fix beats:

  BEAT A · MULTI-AGENT / A2A (collaboration surface — OWASP Agentic T14, confused
  deputy). A second AgentCore runtime — the "Orders" PEER (agent-peer, A2A protocol,
  port 9000) — is stood up alongside the main agent. When the main agent is blocked
  from reading another customer's data directly, it can DELEGATE to the peer. If the
  peer TRUSTS a customer_id passed as a tool ARGUMENT ("vuln" mode) a hijacked caller
  reads another customer's orders. The FIX ("fix" mode): the peer ignores the argument,
  acts only on the propagated caller identity, and re-validates ownership. The UI flips
  A2A_PEER_MODE on the peer runtime (see /api/run/layer6-peer-mode).

  BEAT B · GOAL FENCE (trajectory surface — OWASP Agentic T6 goal hijack). The main
  runtime uses agent-layer6, whose Strands GoalFence HookProvider holds a frozen CHARTER
  (goal + data_scope + max_records) as DATA and, on BeforeToolCallEvent, CANCELS any
  action that breaches the charter (bulk/all-records read, or a numeric limit over the
  cap). Cedar allow-lists WHICH tool; the fence vetoes an allowed tool used toward a
  drifted goal or at bulk scale. It emits "goal_fence" CUSTOM events for the flow panel.

Per the per-layer rule, L6 owns its own AgentCore resources (gateway iris-gateway-l6,
policy iris_policy_l6, OBO iris_support_obo_l6, memory iris_memory_l6, the A2A peer
runtime iris_peer_l6, and the goal-fenced main runtime iris_layer6 on the scoped model
role). Reuses shared IrisTools Lambdas + IrisMemory CDK CMK/role/bucket/topic. Tracked
under phase "layer6".
"""
import asyncio
import os
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
AGENT_LAYER6_DIR = os.path.abspath(os.path.join(HERE, "..", "..", "agent-layer6"))
AGENT_PEER_DIR = os.path.abspath(os.path.join(HERE, "..", "..", "agent-peer"))

GATEWAY_NAME = "iris-gateway-l6"
POLICY_ENGINE_NAME = "iris_policy_l6"
SUPPORT_OBO = "iris_support_obo_l6"
MEMORY_NAME = "iris_memory_l6"
RUNTIME_NAME = "iris_layer6"
PEER_RUNTIME_NAME = "iris_peer_l6"

CHARTER_MAX_RECORDS = "5"


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
    A.state.add_resource("oauth2-credential-provider", name, "layer6")


async def deploy(A, ac, ctx):
    """Build + deploy Layer 6: own Gateway + Cedar + OBO + Memory + the A2A Orders peer
    runtime + the goal-fenced main runtime (on the SCOPED model-allowlist role). ctx keys:
    valid_subnets, sg_id, role_arn, scoped_role_arn, cluster_arn, secret_arn, db_name,
    l3 (IrisTools outputs), l4 (IrisMemory outputs), l6_repo, peer_repo. Yields SSE;
    ends with ('__done__',)."""
    _sse = A._sse
    OKTA = A.OKTA
    yield _sse({"type": "log", "line": "[Layer 6] Agents — goal fence + A2A peer (multi-agent) · scoped role..."})

    l3 = ctx.get("l3") or {}
    l4 = ctx.get("l4") or {}
    l6_repo = ctx.get("l6_repo")
    peer_repo = ctx.get("peer_repo")
    valid_subnets = ctx.get("valid_subnets") or []
    role_arn = ctx.get("role_arn"); sg_id = ctx.get("sg_id")
    scoped_role_arn = ctx.get("scoped_role_arn") or role_arn   # the model-allowlist role
    # Aurora Data API config for the A2A peer (reads the shared orders table directly).
    # These MUST be unpacked here — the peer block below references them by bare name;
    # without this they raise NameError ("name 'cluster_arn' is not defined") and the
    # peer silently fails to deploy (Beat A / cross-customer scenario won't work).
    cluster_arn = ctx.get("cluster_arn", "")
    secret_arn = ctx.get("secret_arn", "")
    db_name = ctx.get("db_name", "irisdb")
    if not l6_repo or not valid_subnets:
        yield _sse({"type": "log", "line": "no Layer 6 ECR repo or subnets — skipping Layer 6."})
        yield ("__done__",); return
    if not OKTA.get("support_delegate_client_secret"):
        yield _sse({"type": "log", "line": "no Okta support delegate secret — skipping Layer 6."})
        yield ("__done__",); return

    # ---- Gateway + Cedar + OBO (L6's own, L6-suffixed names). ----
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
        yield _sse({"type": "log", "line": "creating Layer 6 Gateway (MCP · CUSTOM_JWT · interceptor)..."})
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
        A.state.add_resource("gateway", gateway_id, "layer6")
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
        A.state.add_resource("policy-engine", pe_id, "layer6")
        gw_arn = ac.get_gateway(gatewayIdentifier=gateway_id).get("gatewayArn", f"arn:aws:bedrock-agentcore:{A.REGION}:{A.account_id()}:gateway/{gateway_id}")
        read_pol = ('permit(\n  principal is AgentCore::OAuthUser,\n  action in [\n'
                    '    AgentCore::Action::"GetRecord___get_record",\n    AgentCore::Action::"GetInfo___get_my_info",\n'
                    '    AgentCore::Action::"GetShipment___get_shipment",\n    AgentCore::Action::"ProcessRefund___process_refund"\n  ],\n'
                    f'  resource == AgentCore::Gateway::"{gw_arn}"\n)\nwhen {{\n  principal.hasTag("scp") &&\n  principal.getTag("scp") like "*tool:read*"\n}};')
        upd_pol = ('permit(\n  principal is AgentCore::OAuthUser,\n  action == AgentCore::Action::"UpdateRecord___update_record",\n'
                   f'  resource == AgentCore::Gateway::"{gw_arn}"\n)\nwhen {{\n  principal.hasTag("scp") &&\n  principal.getTag("scp") like "*tool:update*"\n}};')
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

        await _make_obo(ac, A, SUPPORT_OBO, OKTA["support_delegate_client_id"], OKTA["support_delegate_client_secret"],
                        f"{OKTA['gateway_issuer']}/.well-known/openid-configuration")
        yield _sse({"type": "log", "line": f"OBO provider created: {SUPPORT_OBO} (support)"})
    except Exception as e:
        yield _sse({"type": "log", "line": f"Layer 6 Gateway/Policy/OBO failed: {e}"})
        yield _sse({"type": "log", "line": traceback.format_exc().splitlines()[-1]})
        yield ("__done__",); return
    if not gateway_id:
        yield ("__done__",); return

    # ---- Memory (self-managed pre-write gate — carried forward from L4) ----
    memory_id, strategy_id = None, ""
    try:
        yield _sse({"type": "log", "line": "creating Layer 6 Memory (self-managed pre-write gate)..."})
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
            A.state.add_resource("agentcore-memory", memory_id, "layer6", strategy_id=strategy_id)
            yield _sse({"type": "log", "line": f"Memory ACTIVE: {memory_id}"})
    except Exception as e:
        yield _sse({"type": "log", "line": f"Layer 6 Memory failed: {e}"})
    if not memory_id:
        yield ("__done__",); return

    # ---- A2A Orders PEER runtime (BEAT A — the multi-agent surface) ----
    # A separate AgentCore runtime speaking A2A (port 9000). Deployed in "fix" mode
    # (identity-safe) by default; the UI toggle (/api/run/layer6-peer-mode) flips it to
    # "vuln" to demonstrate the confused deputy. Non-fatal if the peer fails — the goal
    # fence beat still works without it.
    peer_arn = ""
    if peer_repo:
        try:
            yield _sse({"type": "log", "line": "building A2A Orders peer image (Strands A2A server)..."})
            peer_image = A._build_and_push_agent_image(peer_repo, AGENT_PEER_DIR)
            yield _sse({"type": "log", "line": f"pushed peer image: {peer_image}"})
            yield _sse({"type": "log", "line": "creating Orders peer runtime (A2A · port 9000 · VPC · JWT-authorized · mode=vuln)..."})
            # JWT authorizer on the peer: it trusts the SAME Okta IDP/audience as the
            # Gateway (iris-gateway). Iris calls the peer with a Bearer = the OBO token it
            # already minted (sub/customer_id preserved). This lets the peer's FIX mode
            # derive identity from the TOKEN'S sub — a real OAuth A2A edge, not SigV4.
            peer_discovery = f"{OKTA['gateway_issuer']}/.well-known/openid-configuration"
            presp = ac.create_agent_runtime(
                agentRuntimeName=PEER_RUNTIME_NAME, roleArn=scoped_role_arn,
                networkConfiguration={"networkMode": "VPC", "networkModeConfig": {
                    "subnets": valid_subnets, "securityGroups": [sg_id] if sg_id else []}},
                protocolConfiguration={"serverProtocol": "A2A"},
                authorizerConfiguration={"customJWTAuthorizer": {
                    "discoveryUrl": peer_discovery, "allowedAudience": [OKTA["gateway_audience"]]}},
                # The peer runs under the SAME scoped model-allowlist role, so its own
                # model MUST be approved too (else its ConverseStream → AccessDenied). Pin
                # qwen-80b (approved; not the guardrail-mandatory one).
                # Default to "vuln" so the demo opens on the PROBLEM (confused deputy),
                # then the operator toggles to "fix" (propagate identity).
                # DB env: the peer reads the SHARED Aurora `orders` table directly (RDS
                # Data API) — its scoped role already grants rds-data + secretsmanager.
                environmentVariables={"A2A_PEER_MODE": "vuln",
                                      "PEER_MODEL_ID": "qwen.qwen3-next-80b-a3b",
                                      "CLUSTER_ARN": cluster_arn or "",
                                      "SECRET_ARN": secret_arn or "",
                                      "DATABASE_NAME": db_name or "irisdb"},
                agentRuntimeArtifact={"containerConfiguration": {"containerUri": peer_image}})
            prid = presp.get("agentRuntimeId")
            peer_arn = f"arn:aws:bedrock-agentcore:{A.REGION}:{A.account_id()}:runtime/{prid}"
            import time as _t
            for _ in range(60):
                st = ac.get_agent_runtime(agentRuntimeId=prid).get("status")
                if st == "READY":
                    break
                if "FAILED" in (st or ""):
                    yield _sse({"type": "log", "line": f"peer runtime FAILED: {st}"}); peer_arn = ""; break
                _t.sleep(5)
            if peer_arn:
                A.state.add_resource("agentcore-runtime", prid, "layer6", arn=peer_arn, role="a2a-peer")
                yield _sse({"type": "log", "line": f"Orders peer READY (A2A · mode=vuln): {peer_arn}"})
        except Exception as e:
            yield _sse({"type": "log", "line": f"A2A peer build/deploy failed (non-fatal): {e}"})
    else:
        yield _sse({"type": "log", "line": "no peer ECR repo — skipping A2A peer (goal-fence beat still deploys)."})

    # ---- Main runtime on the SCOPED model-allowlist role + GOAL FENCE (BEAT B) ----
    try:
        image = A._build_and_push_agent_image(l6_repo, AGENT_LAYER6_DIR)
        yield _sse({"type": "log", "line": f"pushed Layer 6 image (superset + goal fence): {image}"})
        discovery = f"{OKTA['agent_issuer']}/.well-known/openid-configuration"
        yield _sse({"type": "log", "line": "creating Layer 6 runtime on SCOPED role + goal fence"
                    + (" + A2A peer" if peer_arn else "") + f": {scoped_role_arn.split('/')[-1]}"})
        l5 = ctx.get("l5") or {}
        env = {
            "AGENT_TYPE": "support", "GATEWAY_ID": gateway_id, "OBO_PROVIDER_NAME": SUPPORT_OBO,
            "OKTA_GATEWAY_AUDIENCE": OKTA["gateway_audience"], "TOOL_SCOPE": "tool:read",
            "MEMORY_ID": memory_id, "MEMORY_STRATEGY_ID": strategy_id,
            "CLUSTER_ARN": ctx.get("cluster_arn", ""), "SECRET_ARN": ctx.get("secret_arn", ""),
            "DATABASE_NAME": ctx.get("db_name", "irisdb"),
            "CHARTER_MAX_RECORDS": CHARTER_MAX_RECORDS,
            # Layer 6 pins Sonnet + ALWAYS applies the guardrail (mandatory, no opt-out).
            "MODEL_ID": "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
            "GUARDRAIL_ID": l5.get("DefaultGuardrailId", ""),
            "GUARDRAIL_VERSION": l5.get("DefaultGuardrailVersion", "")}
        if peer_arn:
            env["PEER_RUNTIME_ARN"] = peer_arn   # enables the A2A order_lookup delegate tool
        resp = ac.create_agent_runtime(
            agentRuntimeName=RUNTIME_NAME, roleArn=scoped_role_arn,
            networkConfiguration={"networkMode": "VPC", "networkModeConfig": {
                "subnets": valid_subnets, "securityGroups": [sg_id] if sg_id else []}},
            protocolConfiguration={"serverProtocol": "AGUI"},
            authorizerConfiguration={"customJWTAuthorizer": {
                "discoveryUrl": discovery, "allowedAudience": [OKTA["agent_audience"]],
                "allowedScopes": [OKTA["support_agent_scope"]]}},
            requestHeaderConfiguration={"requestHeaderAllowlist": ["Authorization"]},
            environmentVariables=env,
            agentRuntimeArtifact={"containerConfiguration": {"containerUri": image}})
        rid = resp.get("agentRuntimeId")
        import time as _t2
        for _ in range(60):
            st = ac.get_agent_runtime(agentRuntimeId=rid).get("status")
            if st == "READY":
                break
            if "FAILED" in (st or ""):
                yield _sse({"type": "log", "line": f"Layer 6 runtime FAILED: {st}"}); rid = None; break
            _t2.sleep(5)
        if rid:
            A.state.add_resource("agentcore-runtime", rid, "layer6",
                                 arn=f"arn:aws:bedrock-agentcore:{A.REGION}:{A.account_id()}:runtime/{rid}")
            yield _sse({"type": "log", "line": "Layer 6 runtime READY (goal fence + scoped role"
                        + (" + A2A peer" if peer_arn else "") + f"): {rid}"})
    except Exception as e:
        yield _sse({"type": "log", "line": f"Layer 6 runtime failed (non-fatal): {e}"})
    yield ("__done__",)
