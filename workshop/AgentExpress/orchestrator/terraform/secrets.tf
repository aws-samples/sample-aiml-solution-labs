# --- Runtime credentials, in Secrets Manager -------------------------------------------
# The Gateway client secret(s) and the A2A bearer tokens used to be runtime environment
# variables, which anyone allowed bedrock-agentcore:GetAgentRuntime could read. Each
# runtime now gets one secret and only its ARN (RUNTIME_SECRET_ARN); the container copies
# the keys into its environment at start (app/common/runtime_secret.py). Mirrors the CDK
# OrchestratorSecret / SubagentSecret-<id>.
#
# name_prefix, and no recovery window: a destroyed secret otherwise waits out 7-30 days
# under its name, and the next deploy of the same build would fail on it.

locals {
  runtime_secret_prefix = "agentexpress/${var.agent_name}/runtime-"
  # Only for an agent that declares auth "bearer"; a token for an agent that no longer
  # exists is not stored.
  a2a_tokens_env = jsonencode({
    for id in local.a2a_agent_ids : id => var.a2a_tokens[id]
    if lower(try(local.workflow_def.agents[id].auth, local.agent_defaults.auth)) == "bearer"
  })
}

resource "aws_secretsmanager_secret" "runtime" {
  count                   = local.workflow_plane ? 1 : 0
  name_prefix             = local.runtime_secret_prefix
  description             = "Runtime credentials for ${var.agent_name} (app/common/runtime_secret.py)"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "runtime" {
  count     = local.workflow_plane ? 1 : 0
  secret_id = aws_secretsmanager_secret.runtime[0].id
  secret_string = jsonencode({
    GATEWAY_CLIENT_SECRET = local.gateway_enabled ? local.gateway_client_secret : ""
    # orchestrator.gatewayIdentity "perAgent": each in-process agent's own client.
    GATEWAY_AGENT_CLIENTS = local.agent_clients_env
    A2A_TOKENS            = local.a2a_tokens_env
  })
}

resource "aws_iam_role_policy" "runtime_secret" {
  count = local.workflow_plane ? 1 : 0
  name  = "runtime-secret"
  role  = aws_iam_role.runtime[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "ReadOwnRuntimeSecret"
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
      Resource = aws_secretsmanager_secret.runtime[0].arn
    }]
  })
}

# A dedicated agent's: its own Gateway client's secret when gatewayIdentity is "perAgent",
# else the shared one. Never another agent's.
resource "aws_secretsmanager_secret" "subagent" {
  for_each                = local.gateway_enabled ? local.dedicated_agents : {}
  name_prefix             = "${local.runtime_secret_prefix}${each.key}-"
  description             = "Runtime credentials for ${var.agent_name}_${each.key} (app/common/runtime_secret.py)"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "subagent" {
  for_each  = aws_secretsmanager_secret.subagent
  secret_id = each.value.id
  secret_string = jsonencode({
    GATEWAY_CLIENT_SECRET = (contains(local.agent_client_ids, each.key)
      ? aws_cognito_user_pool_client.agent[each.key].client_secret
    : local.gateway_client_secret)
  })
}

resource "aws_iam_role_policy" "subagent_secret" {
  for_each = aws_secretsmanager_secret.subagent
  name     = "runtime-secret"
  role     = aws_iam_role.subagent[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "ReadOwnRuntimeSecret"
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
      Resource = each.value.arn
    }]
  })
}
