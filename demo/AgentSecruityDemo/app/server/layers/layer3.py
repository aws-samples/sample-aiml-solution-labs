"""Layer 3 (Tools) — per-layer AgentCore deploy module.

The agent no longer holds raw SQL / open HTTP. Instead it reaches TYPED tools through
an AgentCore Gateway (MCP), and every call is:
  1. JWT-authorized  — the runtime's CUSTOM_JWT authorizer validates the inbound Okta
     token; allowedScopes isolates agents (support user's `customer` scope vs admin's
     `admin` scope), so a customer can't even reach the admin runtime.
  2. OBO-exchanged    — the agent exchanges its inbound token for an OBO token
     (aud=iris-gateway) via an AgentCore Identity credential provider (RFC 8693).
  3. Cedar-authorized — the Gateway's policy engine authorizes by scope (read tools
     need scp~tool:read, update_record needs scp~tool:update); no permit → default-deny.
  4. Identity-injected — a REQUEST interceptor stamps customer_id from the verified OBO
     token into the tool args, overwriting anything the model sent.

This module owns its OWN AgentCore resources (fixed names, L3-suffixed so they never
collide with the consolidated L6 gateway): gateway iris-gateway-l3, policy engine
iris_policy_l3, OBO providers iris_support_obo_l3 / iris_admin_obo_l3, and TWO runtimes
iris_layer3_support (scp tool:read) + iris_layer3_admin (scp tool:update). Tracked
under phases layer3 / layer3-admin. The tool Lambdas + interceptor come from the shared
IrisTools CDK stack (ctx.l3 outputs). Reuses the same Okta app.
"""
import asyncio
import os
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
AGENT_LAYER3_DIR = os.path.abspath(os.path.join(HERE, "..", "..", "agent-layer3"))

GATEWAY_NAME = "iris-gateway-l3"
POLICY_ENGINE_NAME = "iris_policy_l3"
SUPPORT_OBO = "iris_support_obo_l3"
ADMIN_OBO = "iris_admin_obo_l3"
SUPPORT_RUNTIME = "iris_layer3_support"
ADMIN_RUNTIME = "iris_layer3_admin"

# The 5 typed Gateway tools (same schema the consolidated build uses).
def _targets(l3):
    return [
        ("GetRecord", l3.get("GetRecordFnArn"), [{"name": "get_record", "description": "Look up a customer record by order ID. Returns it only if it belongs to the authenticated caller.",
            "inputSchema": {"type": "object", "properties": {"order_id": {"type": "string"}, "customer_id": {"type": "string", "description": "DO NOT SET. Injected from the verified token."}}, "required": ["order_id"]}}]),
        ("GetInfo", l3.get("GetInfoFnArn"), [{"name": "get_my_info", "description": "Get the authenticated caller's own customer record.",
            "inputSchema": {"type": "object", "properties": {"customer_id": {"type": "string", "description": "DO NOT SET. Injected from the verified token."}}}}]),
        ("GetShipment", l3.get("GetShipmentFnArn"), [{"name": "get_shipment", "description": "Delivery/shipment status for one of the caller's own orders.",
            "inputSchema": {"type": "object", "properties": {"order_id": {"type": "string"}, "customer_id": {"type": "string", "description": "DO NOT SET. Injected from the verified token."}}, "required": ["order_id"]}}]),
        ("ProcessRefund", l3.get("ProcessRefundFnArn"), [{"name": "process_refund", "description": "Issue a refund for one of the caller's own orders once eligibility is confirmed.",
            "inputSchema": {"type": "object", "properties": {"order_id": {"type": "string"}, "amount": {"type": "string"}, "customer_id": {"type": "string", "description": "DO NOT SET. Injected from the verified token."}}, "required": ["order_id"]}}]),
        ("UpdateRecord", l3.get("UpdateRecordFnArn"), [{"name": "update_record", "description": "Update a customer record field (name/email). Admin only.",
            "inputSchema": {"type": "object", "properties": {"customer_id": {"type": "string"}, "field": {"type": "string"}, "value": {"type": "string"}, "acting_customer_id": {"type": "string", "description": "DO NOT SET. Injected from the verified token."}}, "required": ["customer_id", "field", "value"]}}]),
    ]


async def _delete_gateway_by_name(ac, A, name):
    """Self-heal a leftover same-named gateway (delete targets, wait empty, delete, wait gone)."""
    gid = None
    tok = None
    while True:
        kw = {"maxResults": 100}
        if tok:
            kw["nextToken"] = tok
        lg = ac.list_gateways(**kw)
        for g in lg.get("items", []):
            if g.get("name") == name:
                gid = g.get("gatewayId"); break
        tok = lg.get("nextToken")
        if gid or not tok:
            break
    if not gid:
        return

    def _tgts():
        out, t = [], None
        while True:
            kw = {"gatewayIdentifier": gid, "maxResults": 100}
            if t:
                kw["nextToken"] = t
            r = ac.list_gateway_targets(**kw)
            out += r.get("items", [])
            t = r.get("nextToken")
            if not t:
                return out
    for tgt in _tgts():
        try: ac.delete_gateway_target(gatewayIdentifier=gid, targetId=tgt.get("targetId"))
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
    pid = None
    tok = None
    while True:
        kw = {"maxResults": 100}
        if tok:
            kw["nextToken"] = tok
        le = ac.list_policy_engines(**kw)
        for e in le.get("policyEngines", []):
            if e.get("name") == name:
                pid = e.get("policyEngineId"); break
        tok = le.get("nextToken")
        if pid or not tok:
            break
    if not pid:
        return

    def _pols():
        out, t = [], None
        while True:
            kw = {"policyEngineId": pid, "maxResults": 100}
            if t:
                kw["nextToken"] = t
            r = ac.list_policies(**kw)
            out += r.get("policies", [])
            t = r.get("nextToken")
            if not t:
                return out
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


async def _make_obo(ac, A, name, client_id, client_secret, gw_disc):
    """Create an OBO credential provider, self-healing a leftover same-named one."""
    kwargs = dict(
        name=name, credentialProviderVendor="CustomOauth2",
        oauth2ProviderConfigInput={"customOauth2ProviderConfig": {
            "oauthDiscovery": {"discoveryUrl": gw_disc},
            "clientId": client_id, "clientSecret": client_secret,
            "clientAuthenticationMethod": "CLIENT_SECRET_POST",
            "onBehalfOfTokenExchangeConfig": {"grantType": "TOKEN_EXCHANGE",
                "tokenExchangeGrantTypeConfig": {"actorTokenContent": "NONE"}}}})
    try:
        ac.create_oauth2_credential_provider(**kwargs)
    except ac.exceptions.ConflictException:
        try: ac.delete_oauth2_credential_provider(name=name)
        except Exception: pass
        for _ in range(30):
            try: ac.get_oauth2_credential_provider(name=name); await asyncio.sleep(2)
            except Exception: break
        ac.create_oauth2_credential_provider(**kwargs)
    A.state.add_resource("oauth2-credential-provider", name, "layer3")


async def deploy(A, ac, ctx):
    """Build + deploy Layer 3: Gateway + Cedar + support/admin OBO + two scoped runtimes.
    ctx keys: valid_subnets, sg_id, role_arn, cluster_arn, secret_arn, db_name, l3
    (the IrisTools stack outputs w/ tool + interceptor ARNs), l3_repo. Yields SSE log
    chunks; ends with a ('__done__',) sentinel."""
    _sse = A._sse
    OKTA = A.OKTA
    yield _sse({"type": "log", "line": "[4/7] Layer 3 agent (Gateway + Cedar + OBO · typed tools)..."})

    l3 = ctx.get("l3") or {}
    l3_repo = ctx.get("l3_repo")
    valid_subnets = ctx.get("valid_subnets") or []
    role_arn = ctx.get("role_arn")
    sg_id = ctx.get("sg_id")
    if not l3_repo or not valid_subnets:
        yield _sse({"type": "log", "line": "no Layer 3 ECR repo or subnets — skipping Layer 3."})
        yield ("__done__",); return
    if not (OKTA.get("support_delegate_client_secret") and OKTA.get("admin_delegate_client_secret")):
        yield _sse({"type": "log", "line": "no Okta delegate secrets (support/admin) — skipping Layer 3 (set OKTA_*_DELEGATE_CLIENT_SECRET)."})
        yield ("__done__",); return

    gateway_id = None
    try:
        gw_discovery = f"{OKTA['gateway_issuer']}/.well-known/openid-configuration"
        gw_kwargs = {
            "name": GATEWAY_NAME, "protocolType": "MCP", "roleArn": role_arn,
            "authorizerType": "CUSTOM_JWT",
            "authorizerConfiguration": {"customJWTAuthorizer": {
                "discoveryUrl": gw_discovery, "allowedAudience": [OKTA["gateway_audience"]]}},
        }
        interceptor_arn = l3.get("InterceptorFnArn")
        if interceptor_arn:
            gw_kwargs["interceptorConfigurations"] = [{
                "interceptor": {"lambda": {"arn": interceptor_arn}},
                "interceptionPoints": ["REQUEST"],
                "inputConfiguration": {"passRequestHeaders": True}}]
        yield _sse({"type": "log", "line": "creating Layer 3 Gateway (MCP · CUSTOM_JWT · REQUEST interceptor)..."})
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
        A.state.add_resource("gateway", gateway_id, "layer3")
        yield _sse({"type": "log", "line": f"Gateway READY: {gateway_id}"})

        for name, arn, tools in _targets(l3):
            ac.create_gateway_target(
                gatewayIdentifier=gateway_id, name=name,
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
        A.state.add_resource("policy-engine", pe_id, "layer3")
        gw_arn = ac.get_gateway(gatewayIdentifier=gateway_id).get("gatewayArn", f"arn:aws:bedrock-agentcore:{A.REGION}:{A.account_id()}:gateway/{gateway_id}")

        read_pol = ('permit(\n  principal is AgentCore::OAuthUser,\n  action in [\n'
                    '    AgentCore::Action::"GetRecord___get_record",\n    AgentCore::Action::"GetInfo___get_my_info",\n'
                    '    AgentCore::Action::"GetShipment___get_shipment",\n    AgentCore::Action::"ProcessRefund___process_refund"\n  ],\n'
                    f'  resource == AgentCore::Gateway::"{gw_arn}"\n)\nwhen {{\n  principal.hasTag("scp") &&\n  principal.getTag("scp") like "*tool:read*"\n}};')
        upd_pol = ('permit(\n  principal is AgentCore::OAuthUser,\n  action == AgentCore::Action::"UpdateRecord___update_record",\n'
                   f'  resource == AgentCore::Gateway::"{gw_arn}"\n)\nwhen {{\n  principal.hasTag("scp") &&\n  principal.getTag("scp") like "*tool:update*"\n}};')
        # Policy names are ACCOUNT-global (not per-engine) — prefix with the layer so
        # L3's policies never collide with the L6 gateway's SupportReadOnly/AdminUpdate.
        for base, stmt in (("L3SupportReadOnly", read_pol), ("L3AdminUpdate", upd_pol)):
            try:
                ac.create_policy(policyEngineId=pe_id, name=base,
                                 definition={"cedar": {"statement": stmt}}, validationMode="IGNORE_ALL_FINDINGS")
            except ac.exceptions.ConflictException:
                # Leftover from a prior run in another engine — the purge deletes engines
                # (and their policies), but be defensive if one lingers.
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
        await _make_obo(ac, A, SUPPORT_OBO, OKTA["support_delegate_client_id"], OKTA["support_delegate_client_secret"], gw_disc)
        yield _sse({"type": "log", "line": f"OBO provider created: {SUPPORT_OBO} (support)"})
        await _make_obo(ac, A, ADMIN_OBO, OKTA["admin_delegate_client_id"], OKTA["admin_delegate_client_secret"], gw_disc)
        yield _sse({"type": "log", "line": f"OBO provider created: {ADMIN_OBO} (admin)"})
    except Exception as e:
        yield _sse({"type": "log", "line": f"Layer 3 Gateway/Policy/OBO failed: {e}"})
        yield _sse({"type": "log", "line": traceback.format_exc().splitlines()[-1]})
        yield ("__done__",); return

    if not gateway_id:
        yield _sse({"type": "log", "line": "Layer 3 gateway missing — aborting L3."})
        yield ("__done__",); return

    # Build the image once; deploy TWO runtimes (support/tool:read, admin/tool:update).
    try:
        image = A._build_and_push_agent_image(l3_repo, AGENT_LAYER3_DIR)
        yield _sse({"type": "log", "line": f"pushed Layer 3 image: {image}"})
    except Exception as e:
        yield _sse({"type": "log", "line": f"Layer 3 image build failed: {e}"})
        yield ("__done__",); return

    discovery = f"{OKTA['agent_issuer']}/.well-known/openid-configuration"
    common_env = {
        "GATEWAY_ID": gateway_id, "OKTA_GATEWAY_AUDIENCE": OKTA["gateway_audience"],
        "CLUSTER_ARN": ctx.get("cluster_arn", ""), "SECRET_ARN": ctx.get("secret_arn", ""),
        "DATABASE_NAME": ctx.get("db_name", "irisdb"),
    }
    net = {"networkMode": "VPC", "networkModeConfig": {
        "subnets": valid_subnets, "securityGroups": [sg_id] if sg_id else []}}

    for kind, rt_name, obo, scope, agent_scope, phase in (
        ("support", SUPPORT_RUNTIME, SUPPORT_OBO, "tool:read", OKTA["support_agent_scope"], "layer3"),
        ("admin", ADMIN_RUNTIME, ADMIN_OBO, "tool:update", OKTA["admin_agent_scope"], "layer3-admin"),
    ):
        try:
            yield _sse({"type": "log", "line": f"creating Layer 3 {kind} runtime (VPC + JWT scope={agent_scope})..."})
            env = dict(common_env, AGENT_TYPE=kind, OBO_PROVIDER_NAME=obo, TOOL_SCOPE=scope)
            resp = ac.create_agent_runtime(
                agentRuntimeName=rt_name, roleArn=role_arn, networkConfiguration=net,
                protocolConfiguration={"serverProtocol": "AGUI"},
                authorizerConfiguration={"customJWTAuthorizer": {
                    "discoveryUrl": discovery, "allowedAudience": [OKTA["agent_audience"]],
                    "allowedScopes": [agent_scope]}},
                requestHeaderConfiguration={"requestHeaderAllowlist": ["Authorization"]},
                environmentVariables=env,
                agentRuntimeArtifact={"containerConfiguration": {"containerUri": image}})
            rid = resp.get("agentRuntimeId")
            import time as _t
            for _ in range(60):
                st = ac.get_agent_runtime(agentRuntimeId=rid).get("status")
                if st == "READY":
                    break
                if "FAILED" in (st or ""):
                    yield _sse({"type": "log", "line": f"Layer 3 {kind} runtime FAILED: {st}"}); rid = None; break
                _t.sleep(5)
            if rid:
                A.state.add_resource("agentcore-runtime", rid, phase,
                                     arn=f"arn:aws:bedrock-agentcore:{A.REGION}:{A.account_id()}:runtime/{rid}")
                yield _sse({"type": "log", "line": f"Layer 3 {kind} runtime READY: {rid}"})
        except Exception as e:
            yield _sse({"type": "log", "line": f"Layer 3 {kind} runtime failed (non-fatal): {e}"})
    yield ("__done__",)
