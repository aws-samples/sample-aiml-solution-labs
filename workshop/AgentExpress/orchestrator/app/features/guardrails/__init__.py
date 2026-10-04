"""Bedrock Guardrails — content filtering on agent input/output.

Calls the Bedrock ApplyGuardrail API. When enabled for an agent (via
workflow.json agentcore.guardrails), it validates text before and/or after the
LLM call. If content is blocked, GuardrailBlocked is raised and the node wrapper
reports the agent as failed with the guardrail's message.

Config in workflow.json (per agent):
  "guardrails": { "input": true, "output": true }
  Optional per-agent override: "guardrailId": "abc123" — but note that id is
  account-specific, which pins the committed config to one AWS account. Prefer
  the GUARDRAIL_ID env default that Terraform injects per account.

Environment:
  GUARDRAIL_ID      - the Bedrock Guardrail identifier (terraform/guardrail.tf)
  GUARDRAIL_VERSION - version to apply (default: "DRAFT", tracks the latest config)

Key files:
  app/features/guardrails/client.py  — check() and GuardrailBlocked
  app/common/context.py              — ctx.guardrail(text, "INPUT"|"OUTPUT")
  terraform/guardrail.tf             — the guardrail definition
"""

from app.features.guardrails.client import GuardrailBlocked, check  # noqa: F401
