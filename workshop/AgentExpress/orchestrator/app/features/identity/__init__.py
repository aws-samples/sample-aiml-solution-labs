"""AgentCore Workload Identity — outbound OAuth token for external APIs.

Two directions, both declared per agent in workflow.json:
  * inbound  — which identity the CALLER must present. Documentation only here;
               enforcement is at the API Gateway JWT authorizer (for the UI) and
               the Gateway's CUSTOM_JWT authorizer (for agent -> tool calls).
  * outbound — when an agent needs to call an external API directly, it uses
               Workload Identity to fetch an OAuth token from a configured
               credential provider. The provider is set up in Terraform; this
               module just calls IdentityClient.

Config in workflow.json (per agent):
  "identity": { "inbound": "cognito-jwt", "outbound": ["my-api-oauth"] }

Environment:
  (none — IdentityClient authenticates with the runtime's own IAM role)

Key files:
  app/features/identity/client.py  — get_token() implementation
  app/common/context.py            — ctx.get_identity_token(provider)
"""

from app.features.identity.client import get_api_key, get_token  # noqa: F401
