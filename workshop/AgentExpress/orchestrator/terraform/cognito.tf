# --- Cognito resources -----------------------------------------------------
# Created only when idp = "cognito" AND cognito.create = true (local.create_cognito).
# With idp = "auth0" or "none", or when you bring your own pool, none of this is
# provisioned. The provider-agnostic wiring lives in identity.tf.

resource "aws_cognito_user_pool" "this" {
  count = local.create_cognito ? 1 : 0
  name  = "${var.agent_name}-users"

  auto_verified_attributes = ["email"]
  username_attributes      = ["email"]

  # Admin-only user creation. The UI sits behind a PUBLIC CloudFront URL, so
  # self-signup would let anyone register and spend your Bedrock budget. Create
  # users deliberately:
  #   aws cognito-idp admin-create-user   --user-pool-id <id> --username you@example.com \
  #       --user-attributes Name=email,Value=you@example.com Name=email_verified,Value=true \
  #       --message-action SUPPRESS
  #   aws cognito-idp admin-set-user-password --user-pool-id <id> --username you@example.com \
  #       --password '<pw>' --permanent
  # cognito.self_signup = true opens it deliberately: email-verified registration, and
  # the new user joins cognito.self_signup_group (signup.tf). Every build and run is
  # private to its creator, so that group reaches only their own.
  admin_create_user_config {
    allow_admin_create_user_only = !local.self_signup
  }
  dynamic "lambda_config" {
    for_each = local.cognito_trigger ? [1] : []
    content {
      post_confirmation   = local.self_signup ? aws_lambda_function.signup[0].arn : null
      post_authentication = local.audit_sign_ins ? aws_lambda_function.signup[0].arn : null
    }
  }

  password_policy {
    minimum_length    = 12
    require_lowercase = true
    require_uppercase = true
    require_numbers   = true
    require_symbols   = true
  }
  # Users may enrol an authenticator app (TOTP). SMS is off: it needs an SNS role and
  # is the weaker factor. Matches the CDK UserPool.
  mfa_configuration = "OPTIONAL"
  software_token_mfa_configuration {
    enabled = true
  }

  schema {
    name                = "email"
    attribute_data_type = "String"
    required            = true
    mutable             = true
    string_attribute_constraints {
      min_length = 1
      max_length = 256
    }
  }
}

# --- Groups named by workflow.json -> authorization ------------------------
# Created from config so that declaring RBAC provisions the groups it needs; a
# group that does not exist cannot be joined, and Cognito would simply omit the
# claim, leaving every gated action denied with nothing to point at.
#
# Membership is deliberately NOT managed here — it is per-person and changes far
# more often than a deploy:
#   aws cognito-idp admin-add-user-to-group --user-pool-id <id> \
#       --username you@example.com --group-name approvers
resource "aws_cognito_user_group" "authz" {
  for_each     = local.create_cognito ? toset(local.authz_groups) : toset([])
  name         = each.value
  user_pool_id = aws_cognito_user_pool.this[0].id
  description  = "Grants: ${join(", ", [for a, gs in local.authz_actions : a if contains(gs, each.value)])} (from app/workflow.json authorization.actions)."
}

resource "aws_cognito_user_pool_domain" "this" {
  count        = local.create_cognito ? 1 : 0
  domain       = "${replace(var.agent_name, "_", "-")}-${local.account_id}"
  user_pool_id = aws_cognito_user_pool.this[0].id
}

resource "aws_cognito_user_pool_client" "spa" {
  count        = local.create_cognito ? 1 : 0
  name         = "${var.agent_name}-spa"
  user_pool_id = aws_cognito_user_pool.this[0].id

  generate_secret              = false
  explicit_auth_flows          = ["ALLOW_USER_SRP_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"]
  allowed_oauth_flows          = ["code"]
  allowed_oauth_scopes         = ["openid", "email", "profile"]
  supported_identity_providers = var.hosted_ui_providers

  allowed_oauth_flows_user_pool_client = true

  # Wire the real CloudFront URL in directly, so login works on the first apply
  # and STAYS working. Setting these by CLI after apply (the old instructions)
  # created permanent drift: the next `terraform apply` reverted them and broke
  # login. Nothing else by default: a localhost redirect lets any local listener receive
  # a sign-in code. var.extra_callback_urls adds one for local development.
  #
  # No dependency cycle: CloudFront depends on the UI *bucket*, not on this
  # client; the auth-config.js *object* depends on this client.
  callback_urls = concat(["https://${aws_cloudfront_distribution.ui.domain_name}"], var.extra_callback_urls)
  logout_urls   = concat(["https://${aws_cloudfront_distribution.ui.domain_name}"], var.extra_callback_urls)
}

# --- Cognito machine-to-machine identity (agent -> AgentCore Gateway) -------
# The SPA client above logs USERS in. Gateway calls are a different boundary: the
# agent runtime needs a client-credentials token, which requires a Resource Server
# (to define the custom scope) plus a CONFIDENTIAL client (with a secret).
#
# Created only when we're also auto-creating the pool AND the Gateway is enabled
# (local.create_m2m) — otherwise you bring your own via var.gateway_identity,
# which is also the Auth0 path.
resource "aws_cognito_resource_server" "gateway" {
  count        = local.create_m2m ? 1 : 0
  identifier   = "gateway"
  name         = "${var.agent_name}-gateway"
  user_pool_id = aws_cognito_user_pool.this[0].id

  scope {
    scope_name        = "invoke"
    scope_description = "Invoke tools through the AgentCore Gateway"
  }
}

resource "aws_cognito_user_pool_client" "m2m" {
  count        = local.create_m2m ? 1 : 0
  name         = "${var.agent_name}-m2m"
  user_pool_id = aws_cognito_user_pool.this[0].id

  # Confidential client: client_credentials REQUIRES a secret. The secret is read
  # back from state into the runtime env (never written to a committed file).
  generate_secret                      = true
  allowed_oauth_flows                  = ["client_credentials"]
  allowed_oauth_scopes                 = aws_cognito_resource_server.gateway[0].scope_identifiers
  allowed_oauth_flows_user_pool_client = true
  supported_identity_providers         = ["COGNITO"]
  # No explicit_auth_flows: this client never authenticates a human.
}

# --- One machine client per agent (orchestrator.gatewayIdentity = "perAgent") ---------
# Each agent with a tool signs in to the Gateway as ITSELF, so the Gateway's logs and
# policy evaluations name the agent. Only with the pool this deployment creates: with your
# own IdP there is nothing here to create a client in (identity.tf refuses that).
locals {
  per_agent_identity = lower(try(local.workflow_def.orchestrator.gatewayIdentity, local.key_defaults.orchestrator.gatewayIdentity)) == "peragent"
  agent_client_ids = toset(local.create_m2m && local.per_agent_identity ? [
    for id, a in try(local.workflow_def.agents, {}) : id
    if lower(try(a.runtime, local.agent_defaults.runtime)) != "a2a" && length([for t in flatten([try(a.tool, [])]) : t if t != ""]) > 0
  ] : [])
  agent_clients_env = jsonencode({ for id, c in aws_cognito_user_pool_client.agent : id => {
    id = c.id, secret = c.client_secret
  } })
}

resource "aws_cognito_user_pool_client" "agent" {
  for_each     = local.agent_client_ids
  name         = "${var.agent_name}-a-${each.key}"
  user_pool_id = aws_cognito_user_pool.this[0].id

  generate_secret                      = true
  allowed_oauth_flows                  = ["client_credentials"]
  allowed_oauth_scopes                 = aws_cognito_resource_server.gateway[0].scope_identifiers
  allowed_oauth_flows_user_pool_client = true
  supported_identity_providers         = ["COGNITO"]
}

resource "terraform_data" "agent_identity_validation" {
  lifecycle {
    precondition {
      condition     = !local.per_agent_identity || !local.gateway_enabled || local.create_m2m
      error_message = "orchestrator.gatewayIdentity \"perAgent\" needs the Cognito pool this deployment creates (idp = \"cognito\" with create = true): with your own IdP there is no pool to create a client per agent in. Use \"shared\"."
    }
  }
}
