"""AgentCore Gateway — MCP tool access for agents.

The Gateway converts external APIs (Lambda functions, remote MCP servers, REST
APIs via OpenAPI) into MCP-compatible tools that agents can call. It handles
authentication, routing, and protocol translation, so an agent only ever talks to
ONE endpoint.

How it works:
  1. The Gateway is provisioned in Terraform with targets (e.g. "knowledge", "kb")
  2. Each target has a name and a protocol (remote MCP server, Lambda, OpenAPI)
  3. An agent calls ctx.call_tool("<label>", query) -> the Gateway routes to the target
  4. Auth: the runtime fetches a Cognito M2M token -> Gateway validates it (CUSTOM_JWT)

CRITICAL: a Gateway target `name` MUST equal the agent's `mcp` label in
workflow.json, or the call raises ToolUnavailable (nothing is simulated).

Config in workflow.json (per agent):
  "gateway": { "targets": ["knowledge"] }

Environment (set by Terraform):
  GATEWAY_URL, GATEWAY_TOKEN_URL, GATEWAY_CLIENT_ID,
  GATEWAY_CLIENT_SECRET, GATEWAY_AUDIENCE
  GATEWAY_POLICY_MODE  - the Cedar policy mode in effect (display-only)

Key files:
  app/features/gateway/client.py  — MCP client (Cognito token + Streamable HTTP)
  terraform/gateway.tf            — Gateway + targets + Cognito JWT authorizer
  terraform/kb.tf                 — the Knowledge Base retrieve target

At runtime, agents reach it through AgentContext:
  evidence, mode = await ctx.call_tool("docs", query)   # "docs" = a workflow.json tools key
  context,  mode = await ctx.retrieve(query, doc_type="reference")
"""

from app.features.gateway.client import query_tool, retrieve  # noqa: F401
