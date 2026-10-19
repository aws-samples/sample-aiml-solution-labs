# --- Send each run to an observability tool (workflow.json orchestrator.export) ----------
# Off unless app/workflow.json has orchestrator.export. When it does, the ORCHESTRATOR runtime
# sends each run, once it ends, to Langfuse, Datadog, Grafana Cloud, Honeycomb or an OTLP
# backend (app/features/observability/runexport.py). Only the orchestrator: it rebuilds the
# run from the telemetry table, which every runtime (dedicated agents too) already writes,
# so a dedicated runtime needs nothing here. Mirrors cdk/lib/export.ts.
#
# The headers that carry the tool's API key are a Secrets Manager secret the runtime reads by
# ARN: either one this file creates from var.export_headers[<secret>] (the Builder's
# exportHeaders build secret, by the name orchestrator.export.secret gives), or an existing
# one when orchestrator.export.secret is an ARN.
#
# Off, nothing here exists and the runtime's environment is unchanged (the variables are
# merged in only when on). The otlp_* deploy settings (otlp_export.tf) keep working; with
# both, this wins and they are left out (the check below says so).
locals {
  export_cfg            = try(local.workflow_def.orchestrator.export, null)
  export_on             = local.workflow_plane && local.export_cfg != null
  export_secret         = local.export_on ? try(tostring(local.export_cfg.secret), "") : ""
  export_by_arn         = startswith(local.export_secret, "arn:")
  export_creates_secret = local.export_on && !local.export_by_arn
  export_destinations   = ["langfuse", "datadog", "grafana", "honeycomb", "otlp"]
  export_destination    = local.export_on ? try(tostring(local.export_cfg.destination), "") : ""
  export_secret_arn     = local.export_creates_secret ? aws_secretsmanager_secret.export_headers[0].arn : local.export_secret
  export_env = local.export_on ? {
    OTLP_EXPORT_DESTINATION        = local.export_destination
    OTLP_EXPORT_HEADERS_SECRET_ARN = local.export_secret_arn
    OTLP_EXPORT_INCLUDE_CONTENT    = try(local.export_cfg.send.content, false) == true ? "true" : "false"
  } : {}
  # arn:aws:secretsmanager:<region>:<account>:secret:<name>, when the secret is given by ARN.
  export_arn_parts = split(":", local.export_secret)
}

check "export_replaces_otlp_settings" {
  assert {
    condition     = !(local.export_on && var.otlp_endpoint != "")
    error_message = "app/workflow.json orchestrator.export is set, so runs go where it says and the deploy settings otlp_endpoint / otlp_headers_secret_arn are not used. Remove one of the two."
  }
}

# The headers, from the build's exportHeaders secret. Named under the runtime-secret prefix,
# which the Builder's deploy role may create (deploy-role-policy.json RuntimeSecrets); no
# recovery window, as for the runtime secrets (secrets.tf).
resource "aws_secretsmanager_secret" "export_headers" {
  count                   = local.export_creates_secret ? 1 : 0
  name_prefix             = "${local.runtime_secret_prefix}export-headers-"
  description             = "HTTP headers for sending runs from ${var.agent_name} (app/features/observability/runexport.py)"
  recovery_window_in_days = 0
  kms_key_id              = aws_kms_key.data.arn
}
resource "aws_secretsmanager_secret_version" "export_headers" {
  count         = local.export_creates_secret ? 1 : 0
  secret_id     = aws_secretsmanager_secret.export_headers[0].id
  secret_string = try(var.export_headers[local.export_secret], "")
  lifecycle {
    precondition {
      condition     = nonsensitive(try(var.export_headers[local.export_secret], "") != "")
      error_message = "app/workflow.json orchestrator.export.secret names \"${local.export_secret}\", but the build has no exportHeaders secret by that name (var.export_headers). Save the tool's key in the console, or give a Secrets Manager secret ARN."
    }
    precondition {
      condition     = nonsensitive(can(keys(jsondecode(try(var.export_headers[local.export_secret], "")))))
      error_message = "The exportHeaders secret \"${local.export_secret}\" must be a JSON object of HTTP headers."
    }
  }
}

resource "aws_iam_role_policy" "runtime_export" {
  count = local.export_on ? 1 : 0
  name  = "ExportHeaders-${var.agent_name}"
  role  = aws_iam_role.runtime[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([{
      Sid      = "ReadExportHeadersSecret"
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = local.export_secret_arn
      }], local.export_by_arn ? [{
      # A secret given by ARN may use a customer managed key, which is not a setting: the
      # grant names the secret's account and region and is held to decrypting THIS secret
      # through Secrets Manager (as otlp_export.tf does). A secret created above uses the
      # deployment's data key, whose policy already allows that (kms.tf).
      Sid      = "DecryptExportHeadersSecret"
      Effect   = "Allow"
      Action   = ["kms:Decrypt"]
      Resource = "arn:aws:kms:${local.export_arn_parts[3]}:${local.export_arn_parts[4]}:key/*"
      Condition = {
        StringEquals = {
          "kms:ViaService"                  = "secretsmanager.${local.export_arn_parts[3]}.amazonaws.com"
          "kms:EncryptionContext:SecretARN" = local.export_secret
        }
      }
    }] : [])
  })
  lifecycle {
    precondition {
      condition     = contains(local.export_destinations, local.export_destination)
      error_message = "app/workflow.json orchestrator.export.destination must be one of: ${join(", ", local.export_destinations)}."
    }
    precondition {
      condition     = local.export_secret != "" && (!local.export_by_arn || can(regex("^arn:aws:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@!-]+-[A-Za-z0-9]{6}$", local.export_secret)))
      error_message = "app/workflow.json orchestrator.export.secret must name the build's exportHeaders secret, or be the full ARN of a Secrets Manager secret, including the six-character suffix."
    }
  }
}
