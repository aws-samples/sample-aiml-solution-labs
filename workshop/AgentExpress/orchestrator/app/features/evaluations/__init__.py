"""AgentCore Evaluations (LLM-as-judge) over an agent's real run.

Scores ACTUAL session data — not synthetic test cases. An agent runs, its model
calls are persisted with their input/output (app/features/observability), its OTEL
spans land in the runtime's CloudWatch log group, and we score the target agent
with the AWS bedrock-agentcore:Evaluate API.

Config in workflow.json (agents.<id>.agentcore.evaluations):
  {"enabled": true, "auto": true, "evaluators": ["Builtin.Faithfulness", ...]}
  - enabled=false        -> never evaluated
  - enabled, auto=false  -> on-demand only (the UI "Evaluate" button)
  - enabled, auto=true   -> also evaluated automatically at session completion

The 13 built-in evaluators (pass as "Builtin.<Name>"): Coherence, Conciseness,
Correctness, Faithfulness, GoalSuccessRate, Harmfulness, Helpfulness,
InstructionFollowing, Refusal, ResponseRelevance, Stereotyping,
ToolParameterAccuracy, ToolSelectionAccuracy.

Why we pass spans to Evaluate INLINE (rather than using managed Online
Evaluation): the managed path sources spans from the "aws/spans" log group
(CloudWatch Transaction Search). Spans land reliably in the runtime's OWN log
group but are not always delivered to "aws/spans", so building the span payload
ourselves and calling Evaluate(sessionSpans=[...]) is the robust path. Evaluate is
designed to accept spans inline, so this still uses the AWS-prescribed API.
"""
