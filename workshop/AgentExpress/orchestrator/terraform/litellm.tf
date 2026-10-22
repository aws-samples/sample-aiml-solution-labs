# --- Optional: non-Bedrock models through the customer's LiteLLM proxy -------------------
# Off unless var.litellm_base_url is set. When it is, every runtime (the orchestrator and each
# dedicated agent) can answer an agent whose model is litellm/<model> by calling the proxy's
# /v1/chat/completions with the virtual key in var.litellm_key_secret_arn
# (app/common/litellm_proxy.py), and the BFF lists the proxy's models for the Build view's
# picker (GET /api/models/litellm, bff/litellm_models.py). Mirrors cdk/lib/litellm.ts.
#
# Off, nothing here exists and the runtimes' and the BFF's environment is exactly what it
# was: the variables are merged in only when on, so an upgrade without them plans no change.
locals {
  litellm_enabled = var.litellm_base_url != ""
  litellm_env = local.litellm_enabled ? {
    LITELLM_BASE_URL       = var.litellm_base_url
    LITELLM_KEY_SECRET_ARN = var.litellm_key_secret_arn
  } : {}
  # A build deployed from this console's Builder gets the same two settings (deployer/runner.py).
  litellm_build_env = local.litellm_env
  # arn:aws:secretsmanager:<region>:<account>:secret:<name>
  litellm_secret_arn_parts = split(":", var.litellm_key_secret_arn)
  litellm_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "ReadLitellmKeySecret"
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = var.litellm_key_secret_arn
      }, {
      # Only needed when the secret uses a customer managed key (the aws/secretsmanager key
      # policy already allows use through Secrets Manager). The key is not a setting, so the
      # grant names the secret's account and region and is held to decrypting THIS secret
      # through Secrets Manager: its encryption context carries the secret's ARN.
      Sid      = "DecryptLitellmKeySecret"
      Effect   = "Allow"
      Action   = ["kms:Decrypt"]
      Resource = local.litellm_enabled ? "arn:aws:kms:${local.litellm_secret_arn_parts[3]}:${local.litellm_secret_arn_parts[4]}:key/*" : ""
      Condition = {
        StringEquals = {
          "kms:ViaService"                  = local.litellm_enabled ? "secretsmanager.${local.litellm_secret_arn_parts[3]}.amazonaws.com" : ""
          "kms:EncryptionContext:SecretARN" = var.litellm_key_secret_arn
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "runtime_litellm" {
  count  = local.workflow_plane && local.litellm_enabled ? 1 : 0
  name   = "LitellmKey-${var.agent_name}"
  role   = aws_iam_role.runtime[0].id
  policy = local.litellm_policy
}

resource "aws_iam_role_policy" "subagent_litellm" {
  # A filtered for, not a conditional between local.dedicated_agents and {}: dedicated agents with different keys
  # make that an object type the empty `{}` cannot match ("Inconsistent conditional result types").
  for_each = { for id, a in local.dedicated_agents : id => id if local.litellm_enabled }
  name     = "LitellmKey-${var.agent_name}-${each.key}"
  role     = aws_iam_role.subagent[each.key].id
  policy   = local.litellm_policy
}

# GET /api/models/litellm reads the key to list the proxy's models.
resource "aws_iam_role_policy" "bff_litellm" {
  count  = local.litellm_enabled ? 1 : 0
  name   = "LitellmKey-${var.agent_name}-bff"
  role   = aws_iam_role.bff.id
  policy = local.litellm_policy
}
