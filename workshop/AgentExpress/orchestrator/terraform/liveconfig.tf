# --- Live config (orchestrator.liveConfig; bff/liveconfig.py) --------------------------------
# Versions of agents' prompts and model settings, and the pointer to the active one, are
# reserved rows in the telemetry table. Only the BFF reads or writes them, and only rows whose
# key starts _liveconfig#: it cannot read or change a telemetry row through this. The runtimes
# get no grant at all, because the run's version rides in its start payload and graph state.
# Absent unless the workflow turns the feature on, so a deployment without it is unchanged.
# Mirrors LiveConfig in cdk/lib/orchestrator-stack.ts.
locals {
  live_config = local.workflow_plane && try(local.workflow_def.orchestrator.liveConfig, false) == true
}

resource "aws_iam_role_policy" "bff_liveconfig" {
  count = local.live_config ? 1 : 0
  name  = "LiveConfig-${var.agent_name}"
  role  = aws_iam_role.bff.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "LiveConfig"
      Effect   = "Allow"
      Action   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:Query"]
      Resource = [aws_dynamodb_table.telemetry[0].arn]
      Condition = {
        "ForAllValues:StringLike" = { "dynamodb:LeadingKeys" = ["_liveconfig#*"] }
      }
    }]
  })
}
