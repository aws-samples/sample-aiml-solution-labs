# --- AgentCore Optimization: Insights ---------------------------------------
#
# AgentCore Optimization is used here via INSIGHTS: a cross-run
# bedrock-agentcore:StartBatchEvaluation with the built-in insight analyzers
# (FailureAnalysis / UserIntent / ExecutionSummary). Findings are stored in the
# DynamoDB table below (a single reserved item, id="latest") and surfaced in the
# Observability -> Insights tab.
#
# Requires CloudWatch Transaction Search (transaction_search.tf) so the runtime
# traces the analyzers read are indexed.
#
# Not wired: per-prompt Recommendations (StartRecommendation). Its session
# reconstruction does not accept a multi-agent orchestrator's trace structure —
# it reports "no sessions identified from input agent traces" even with
# correctly-formatted, X-Ray-indexed traces. Insights works, so that is what this
# sample implements.

# Small key-value store for the latest Insights run (id/arn/status + findings
# JSON, keyed id="latest").
resource "aws_dynamodb_table" "insights" {
  count        = local.workflow_plane ? 1 : 0
  name         = "${var.agent_name}_insights"
  billing_mode = "PAY_PER_REQUEST"
  point_in_time_recovery {
    enabled = true
  }
  hash_key = "id"

  attribute {
    name = "id"
    type = "S"
  }
}

# Runtime permissions for Insights: the findings store, the managed batch
# evaluation (Insights) APIs, and CloudWatch Logs reads for the trace sources.
# Batch-evaluation resources are created dynamically, so (like Evaluate in
# main.tf) they are granted on "*".
resource "aws_iam_role_policy" "runtime_optimization" {
  count = local.workflow_plane ? 1 : 0
  name  = "Optimization-${var.agent_name}"
  role  = aws_iam_role.runtime[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "InsightsFindingsStore"
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem"]
        Resource = [aws_dynamodb_table.insights[0].arn]
      },
      {
        Sid    = "InsightsBatchEvaluation"
        Effect = "Allow"
        Action = [
          "bedrock-agentcore:StartBatchEvaluation",
          "bedrock-agentcore:GetBatchEvaluation",
          "bedrock-agentcore:ListBatchEvaluations"
        ]
        Resource = ["*"]
      },
      {
        # Insights analyzes the traces the caller points it at in CloudWatch
        # Logs. DescribeLogGroups/StartQuery/GetQueryResults are also granted for
        # evaluations in main.tf; this adds the remaining log-read verbs the
        # optimization APIs use. Query operations are not resource-scopable.
        Sid    = "OptimizationLogsReadWrite"
        Effect = "Allow"
        Action = [
          "logs:GetLogEvents", "logs:FilterLogEvents", "logs:DescribeLogGroups",
          "logs:StartQuery", "logs:GetQueryResults",
          # StartBatchEvaluation writes its output to a CloudWatch log group it
          # creates on the caller's behalf (forward access session), so the
          # runtime role needs the log-group/stream create + put verbs too.
          "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents",
          "logs:PutRetentionPolicy"
        ]
        Resource = ["*"]
      }
    ]
  })
}
