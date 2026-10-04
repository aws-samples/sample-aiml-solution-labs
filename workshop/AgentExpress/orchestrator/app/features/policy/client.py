"""AgentCore Policy — Cedar authorization.

HOW POLICY REALLY WORKS (read this first)
-----------------------------------------
AgentCore Policy is enforced AUTOMATICALLY and SERVER-SIDE at the Gateway. You do
NOT call an authorization API from agent code for the primary path:

    1. You provision a Policy Engine and write Cedar `permit`/`forbid` rules, then
       attach the engine to a Gateway.  (See terraform/policy.tf and gateway.tf.)
    2. When an agent calls a tool THROUGH the Gateway (e.g. ctx.retrieve -> the KB
       "kb___retrieve" tool), the Gateway evaluates Cedar against that request and
       ALLOWS or DENIES the tool invocation on its own.
    3. The attachment has a MODE (workflow.json -> orchestrator.policy.mode):
         * LOG_ONLY  - evaluate + log the ALLOW/DENY to CloudWatch, but never block.
         * ENFORCE   - actually block DENY (default-deny: anything without a
                       matching permit is denied).
       The mode is an enforcement switch; it does NOT change the ALLOW/DENY the
       Cedar rules produce — it only decides whether a DENY is logged or blocked.

Relationship: 1 Gateway -> 1 Policy Engine -> many Cedar policies (permit/forbid).

WHAT THIS MODULE IS
-------------------
`is_authorized()` below is an OPTIONAL, app-level authorization check you can call
explicitly from agent code — for a decision that does NOT go through the Gateway
(e.g. gating some in-process action). It is NOT the primary enforcement path and
is NOT required: Gateway tool calls are already governed by the attached engine
above. It's provided so an agent can ask "am I allowed to do X?" against a policy
engine when there's no Gateway hop to enforce it.

It is best-effort and fail-OPEN (returns True) when policy is not configured or
the control-plane call is unavailable, so it can never break a run. For hard
security guarantees, rely on the Gateway/ENFORCE path, not this helper.
"""


async def is_authorized(engine_id: str, agent_id: str, action: str, resource: str) -> bool:
    """Optionally evaluate a Cedar decision from agent code (see module docstring).

    Args:
        engine_id: the Policy Engine id (from the POLICY_ENGINE_ID env, set by
                   Terraform). Empty -> policy not configured -> allow.
        agent_id:  the calling agent (Cedar principal).
        action:    the action being attempted (Cedar action).
        resource:  the resource being accessed (Cedar resource).

    Returns True (ALLOW) when:
      * no engine is configured (feature off), or
      * the control-plane authorization call fails/does not exist in this SDK.
    Returns True otherwise only when the engine explicitly returns ALLOW.

    NOTE: This is the SECONDARY, app-level check. The authoritative enforcement
    for Gateway tool calls happens at the Gateway (see module docstring); this
    helper does not affect that.
    """
    # Feature disabled (no engine wired) -> allow, so the workflow still runs.
    if not engine_id:
        return True

    import asyncio

    import boto3

    from app.common.config import REGION

    client = boto3.client("bedrock-agentcore-control", region_name=REGION)
    try:
        resp = await asyncio.to_thread(
            client.is_authorized,
            policyEngineId=engine_id,
            principal={"entityType": "Agent", "entityId": agent_id},
            action={"actionType": "Action", "actionId": action},
            resource={"entityType": "Resource", "entityId": resource},
        )
        return resp.get("decision") == "ALLOW"
    except Exception:  # noqa: BLE001 - fail-open; primary enforcement is Gateway-side
        return True
