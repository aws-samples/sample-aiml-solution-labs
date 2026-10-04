"""AgentCore Features — one subfolder per feature, config-driven.

Each agent declares the features it uses in workflow.json under its 'agentcore'
key. AgentContext reads that config and exposes simple methods (ctx.guardrail,
ctx.memory_recall, …). If a feature is disabled for an agent, the method is a
no-op — so turning a capability on or off for an agent is a config change, never
a code change.

Structure (one subfolder per AgentCore capability):
  runtime/       - Runtime hosting. No module needed: the hosting IS
                   app/orchestrator/runtime.py (orchestrator) and
                   app/subagent_runtime.py (one dedicated agent), both
                   BedrockAgentCoreApp entrypoints with async-task support.
  gateway/       - MCP Gateway (tool routing, targets, Cognito M2M auth)
  memory/        - AgentCore Memory (long-term semantic recall and store)
  identity/      - Workload Identity (outbound OAuth token for external APIs)
  observability/ - GenAI Observability (OTEL spans, metering, cost tracking)
  guardrails/    - Bedrock Guardrails (content filtering on input/output)
  evaluations/   - Evaluations (LLM-as-judge over real traces)
  policy/        - Policy Engine (Cedar authorization, enforced at the Gateway)
  optimization/  - Insights (cross-run failure/intent/summary batch analysis)

Everything here is best-effort: a feature failure is logged and swallowed, never
allowed to break a workflow run.
"""
