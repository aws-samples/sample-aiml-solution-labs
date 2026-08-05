"""iris_demo CDK constructs — shared constants."""

# Explicit, least-privilege AgentCore action set for the agent execution roles (replaces
# the AWS-managed bedrock-agentcore:* BedrockAgentCoreFullAccess policy — cdk_nag IAM4).
# Scoped by resource to the account+region at each use site. Covers what the role is used
# for while AgentCore assumes it: runtime data-plane + the control-plane READS the Gateway
# needs to resolve its Cedar policy engine and targets on create/update (UpdateGateway ->
# GetPolicyEngine). Create/delete are run by the deploy's own admin creds, not this role.
AGENTCORE_ROLE_ACTIONS = [
    # runtime data-plane
    "bedrock-agentcore:GetWorkloadAccessToken",
    "bedrock-agentcore:GetWorkloadAccessTokenForJWT",
    "bedrock-agentcore:GetWorkloadAccessTokenForUserId",
    "bedrock-agentcore:GetResourceOauth2Token",
    "bedrock-agentcore:GetResourceApiKey",
    "bedrock-agentcore:GetTokenVault",
    "bedrock-agentcore:CreateEvent",
    "bedrock-agentcore:GetEvent",
    "bedrock-agentcore:ListEvents",
    "bedrock-agentcore:DeleteEvent",
    "bedrock-agentcore:GetSession",
    "bedrock-agentcore:ListSessions",
    "bedrock-agentcore:RetrieveMemoryRecords",
    "bedrock-agentcore:ListMemoryRecords",
    "bedrock-agentcore:GetMemoryRecord",
    "bedrock-agentcore:GetMemory",
    "bedrock-agentcore:ListMemories",
    # control-plane reads used during gateway<->policy association
    "bedrock-agentcore:GetGateway",
    "bedrock-agentcore:ListGateways",
    "bedrock-agentcore:GetGatewayTarget",
    "bedrock-agentcore:ListGatewayTargets",
    "bedrock-agentcore:SynchronizeGatewayTargets",
    "bedrock-agentcore:GetPolicyEngine",
    "bedrock-agentcore:ListPolicyEngines",
    "bedrock-agentcore:GetPolicy",
    "bedrock-agentcore:ListPolicies",
    # The Gateway Execution Role MUST hold these two Cedar-authorization actions plus
    # GetPolicyEngine (above) to attach a policy engine — per AWS docs
    # (bedrock-agentcore/latest/devguide/policy-permissions.html). Without them, UpdateGateway
    # fails and all tool invocations default-deny. This is the complete required set.
    "bedrock-agentcore:AuthorizeAction",
    "bedrock-agentcore:PartiallyAuthorizeActions",
    "bedrock-agentcore:GetAgentRuntime",
    "bedrock-agentcore:ListAgentRuntimes",
    "bedrock-agentcore:InvokeAgentRuntime",
]
