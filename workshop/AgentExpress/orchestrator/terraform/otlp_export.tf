# --- Optional second trace destination: any OTLP/HTTP endpoint -------------------------
# Off unless var.otlp_endpoint is set. When it is, every runtime (the orchestrator and each
# dedicated agent; they run the same image and entrypoint) adds one span processor beside
# ADOT's and sends a copy of each span there, without prompt or answer content unless
# var.otlp_include_content (app/features/observability/export.py). AgentCore Observability
# in CloudWatch is unchanged. Mirrors cdk/lib/otlp-export.ts.
#
# Off, nothing here exists and the runtimes' environment is exactly what it was: the
# variables are merged in only when on, so an upgrade without this setting plans no change.
locals {
  # Off when workflow.json orchestrator.export is set: that wins (export.tf).
  otlp_export_enabled = var.otlp_endpoint != "" && !local.export_on
  otlp_export_env = local.otlp_export_enabled ? {
    OTLP_EXPORT_ENDPOINT           = var.otlp_endpoint
    OTLP_EXPORT_HEADERS_SECRET_ARN = var.otlp_headers_secret_arn
    OTLP_EXPORT_INCLUDE_CONTENT    = var.otlp_include_content ? "true" : "false"
  } : {}
  # arn:aws:secretsmanager:<region>:<account>:secret:<name>
  otlp_secret_arn_parts = split(":", var.otlp_headers_secret_arn)
  otlp_export_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "ReadOtlpHeadersSecret"
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = var.otlp_headers_secret_arn
      }, {
      # Only needed when the secret uses a customer managed key (the aws/secretsmanager key
      # policy already allows use through Secrets Manager). The key is not a setting, so the
      # grant names the secret's account and region and is held to decrypting THIS secret
      # through Secrets Manager: its encryption context carries the secret's ARN.
      Sid      = "DecryptOtlpHeadersSecret"
      Effect   = "Allow"
      Action   = ["kms:Decrypt"]
      Resource = local.otlp_export_enabled ? "arn:aws:kms:${local.otlp_secret_arn_parts[3]}:${local.otlp_secret_arn_parts[4]}:key/*" : ""
      Condition = {
        StringEquals = {
          "kms:ViaService"                  = local.otlp_export_enabled ? "secretsmanager.${local.otlp_secret_arn_parts[3]}.amazonaws.com" : ""
          "kms:EncryptionContext:SecretARN" = var.otlp_headers_secret_arn
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "runtime_otlp_export" {
  count  = local.workflow_plane && local.otlp_export_enabled ? 1 : 0
  name   = "OtlpExportHeaders-${var.agent_name}"
  role   = aws_iam_role.runtime[0].id
  policy = local.otlp_export_policy
}

resource "aws_iam_role_policy" "subagent_otlp_export" {
  for_each = local.otlp_export_enabled ? local.dedicated_agents : {}
  name     = "OtlpExportHeaders-${var.agent_name}-${each.key}"
  role     = aws_iam_role.subagent[each.key].id
  policy   = local.otlp_export_policy
}
