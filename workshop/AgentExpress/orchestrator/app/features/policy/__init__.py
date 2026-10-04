"""AgentCore Policy Engine — Cedar authorization (enforced at the Gateway).

Policy is enforced at the AgentCore Gateway level, NOT inline in agent code.
When an agent calls an MCP tool via the Gateway, the Gateway evaluates the
attached Cedar policies BEFORE forwarding the request to the target. If denied,
the tool call returns an authorization error (which the gateway client surfaces
as mode="denied").

This means:
  - Policies are authored in Cedar (permit / forbid rules)
  - The policy engine is attached to the Gateway in Terraform
  - Agent code does NOT need to call any policy API
  - The Gateway handles enforcement transparently

Relationship: 1 Gateway -> 1 Policy Engine -> many Cedar policies.

Modes (workflow.json -> orchestrator.policy):
  enabled=false          -> no engine created or attached; no evaluation at all
  enabled=true           -> engine attached in ENFORCE (default-deny; a DENY blocks)
  enabled + mode=LOG_ONLY -> evaluate + log to CloudWatch, never block (rollout/testing)

Config in workflow.json (per agent — declarative, for documentation/lineage):
  "policy": { "enabled": true, "policyRef": "kb-corpus-guard" }

Key files:
  app/features/policy/client.py  — optional is_authorized() for rare agent-side checks
  terraform/policy.tf            — the policy engine + Cedar rules
  terraform/gateway.tf           — attaches the engine to the Gateway
"""

from app.features.policy.client import is_authorized  # noqa: F401
