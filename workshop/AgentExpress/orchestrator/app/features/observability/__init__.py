"""AgentCore GenAI Observability — OTEL spans, metering, and cost tracking.

Observability is handled by ADOT auto-instrumentation (aws-opentelemetry-distro
in the Dockerfile) plus the modules in this folder:

  otel.py     — Session grouping, spans, trace propagation across runtimes
  meter.py    — Record LLM/tool/memory/guardrail/policy/eval events
  scope.py    — Per-agent run context (session_id, agent_id, user, version)
  records.py  — Telemetry record schema (one DynamoDB row per event)
  store.py    — DynamoDB writer (best-effort, size-safe)
  pricing.py  — Price book and cost math
  costs.py    — Non-per-call costs (AgentCore Runtime compute)
  tokens.py   — Exact token counting via the Bedrock CountTokens API

How it works:
  1. The Dockerfile wraps the process with `opentelemetry-instrument`
  2. ADOT auto-instruments boto3 calls (including bedrock-runtime:Converse)
  3. otel.set_session(session_id) groups all spans under one session
  4. otel.span("agent.X") creates readable per-agent spans
  5. otel.carrier() / attach_carrier() propagate traces across runtimes

Config in workflow.json (per agent):
  "observability": { "enabled": true }

Environment (set by Terraform on each runtime — see terraform/main.tf):
  AGENT_OBSERVABILITY_ENABLED           = "true"        # master switch; AgentCore
                                                        # injects the ADOT distro,
                                                        # configurator and OTLP endpoint
  OTEL_TRACES_SAMPLER                   = "always_on"   # export every span (the UI
                                                        # path propagates sampled=0)
  OTEL_PYTHON_DISABLED_INSTRUMENTATIONS = "aws_langchain"  # drop the duplicate,
                                                           # token-less LangChain span

Isolated on purpose: the rest of the app touches it only through
scope.set_scope(...) and the meter.record_* calls at a few chokepoints. Remove
this folder plus those calls and the app runs exactly as before.
"""
