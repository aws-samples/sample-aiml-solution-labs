# --- The activity log of a build's own app ------------------------------------------
# Who signed in, and who started, decided, cancelled, re-ran or deleted a run
# (bff/audit.py, signup_lambda/handler.py). A console keeps its log in the builds table,
# next to the builds; a deployment without the Builder — every build's app — keeps it
# here, laid out the same way (pk AUDIT#<sub> / AUDIT_DAY#<day>, sk <ts>#<rand>).
# Mirrors the AuditTable in cdk/lib/orchestrator-stack.ts.
resource "aws_dynamodb_table" "audit" {
  count        = local.builder_enabled ? 0 : 1
  name         = "${var.agent_name}_audit"
  billing_mode = "PAY_PER_REQUEST"
  point_in_time_recovery {
    enabled = true
  }
  hash_key  = "pk"
  range_key = "sk"
  attribute {
    name = "pk"
    type = "S"
  }
  attribute {
    name = "sk"
    type = "S"
  }
}

resource "aws_iam_role_policy" "bff_audit" {
  count = local.builder_enabled ? 0 : 1
  name  = "AgentCoreBFFAudit-${var.agent_name}"
  role  = aws_iam_role.bff.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "AuditLog"
      Effect   = "Allow"
      Action   = ["dynamodb:PutItem", "dynamodb:Query"]
      Resource = aws_dynamodb_table.audit[0].arn
    }]
  })
}

locals {
  audit_table_name = local.builder_enabled ? "${var.agent_name}_builds" : "${var.agent_name}_audit"
  audit_table_arn  = local.builder_enabled ? aws_dynamodb_table.builds[0].arn : aws_dynamodb_table.audit[0].arn
  audit_bff_env = merge(local.console_mode_env,
  local.builder_enabled ? {} : { AUDIT_TABLE = aws_dynamodb_table.audit[0].name })
}
