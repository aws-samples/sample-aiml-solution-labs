"""AgentCore Optimization — cross-run Insights.

Optimization is used here via INSIGHTS (see insights.py): a periodic, cross-run
bedrock-agentcore:StartBatchEvaluation with the built-in insight analyzers
(FailureAnalysis / UserIntent / ExecutionSummary). It surfaces recurring failure
patterns, user intents, and execution summaries across ALL runs in the
Observability -> Insights tab.

Unlike per-agent Evaluations (tied to a single run), Insights is manual/periodic
and spans every run in a lookback window, giving a quality-trend view.

Note on per-prompt Recommendations (StartRecommendation): not wired here. Its
session reconstruction does not accept a multi-agent orchestrator's trace
structure — it reports "no sessions identified from input agent traces" even with
correctly-formatted, X-Ray-indexed traces. Insights (batch evaluation) works, so
that is what this folder implements.
"""
