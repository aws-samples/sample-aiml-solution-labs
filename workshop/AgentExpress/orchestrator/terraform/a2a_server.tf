# --- This workflow, served over A2A to other teams' agents (bff/a2a_server.py) ---------
# Off unless var.expose_a2a. a2a.tf is the other direction (a stand-in agent this
# workflow CALLS); this file is the endpoint other agents call. Mirrors
# cdk/lib/a2a-server.ts.
#
# Callers are machines and sign in with the deployment's identity provider, never with a
# key minted here. The two routes have their OWN JWT authorizer, whose audience is the A2A
# client(s) only, and they require the A2A scope:
#   * a person's token (the SPA client's) has the wrong audience and no such scope, so it
#     is refused on these routes;
#   * an A2A client's token has the wrong audience for the main authorizer, so it is
#     refused on every other route (and bff/handler.py refuses it there too).

locals {
  a2a_routes = [
    "GET /api/a2a/.well-known/agent-card.json",
    "POST /api/a2a",
  ]
  # A Cognito pool this deployment creates gets an A2A app client; any other provider is
  # configured through var.a2a_identity.
  a2a_create = var.expose_a2a && local.create_cognito
  a2a_scope  = local.a2a_create ? "a2a/invoke" : var.a2a_identity.scope
  a2a_audience = (local.a2a_create ? aws_cognito_user_pool_client.a2a[0].id
  : var.a2a_identity.audience)
  a2a_client_ids = local.a2a_create ? [aws_cognito_user_pool_client.a2a[0].id] : var.a2a_identity.client_ids
  a2a_issuer     = !local.a2a_create && var.a2a_identity.issuer != "" ? var.a2a_identity.issuer : local.jwt_issuer
  a2a_token_url = (local.a2a_create
    ? "https://${aws_cognito_user_pool_domain.this[0].domain}.auth.${var.region}.amazoncognito.com/oauth2/token"
  : var.a2a_identity.token_url)
  a2a_configured = local.a2a_create || (var.a2a_identity.audience != "" && var.a2a_identity.scope != "" && length(var.a2a_identity.client_ids) > 0)
  # Read by bff/a2a_server.py. Nothing at all when off.
  a2a_bff_env = !var.expose_a2a ? {} : {
    A2A_ENABLED    = "true"
    A2A_CLIENT_IDS = jsonencode(local.a2a_client_ids)
    A2A_SCOPE      = local.a2a_scope
    A2A_TOKEN_URL  = local.a2a_token_url
    A2A_APPROVERS  = jsonencode(var.a2a_approvers)
  }
}

resource "aws_cognito_resource_server" "a2a" {
  count        = local.a2a_create ? 1 : 0
  identifier   = "a2a"
  name         = "${var.agent_name}-a2a"
  user_pool_id = aws_cognito_user_pool.this[0].id

  scope {
    scope_name        = "invoke"
    scope_description = "Start, read and cancel runs of this workflow over A2A"
  }
}

# A CONFIDENTIAL client: client credentials require a secret. The secret stays in Cognito
# (read it with `aws cognito-idp describe-user-pool-client`); it is not an output and is
# not given to the BFF, which only needs the client id.
resource "aws_cognito_user_pool_client" "a2a" {
  count        = local.a2a_create ? 1 : 0
  name         = "${var.agent_name}-a2a"
  user_pool_id = aws_cognito_user_pool.this[0].id

  generate_secret                      = true
  allowed_oauth_flows                  = ["client_credentials"]
  allowed_oauth_scopes                 = aws_cognito_resource_server.a2a[0].scope_identifiers
  allowed_oauth_flows_user_pool_client = true
  supported_identity_providers         = ["COGNITO"]
  # No explicit_auth_flows: this client never authenticates a person.
}

resource "aws_apigatewayv2_authorizer" "a2a" {
  count            = var.expose_a2a ? 1 : 0
  api_id           = aws_apigatewayv2_api.bff.id
  authorizer_type  = "JWT"
  identity_sources = ["$request.header.Authorization"]
  name             = "a2a-${var.agent_name}"

  # API Gateway matches `audience` against the token's `aud` or, for an access token
  # without one (Cognito's), its `client_id`.
  jwt_configuration {
    audience = [local.a2a_audience]
    issuer   = local.a2a_issuer
  }

  # Fail the plan rather than deploy an endpoint nobody can call, or one that is open.
  lifecycle {
    precondition {
      condition     = local.auth_enabled
      error_message = "expose_a2a needs a sign-in provider: with idp = \"none\" the A2A routes would have no authorizer. Use idp = \"cognito\" (or another provider with a2a_identity)."
    }
    precondition {
      condition     = local.workflow_plane
      error_message = "expose_a2a serves this deployment's workflow, and a control-plane console (console_mode = \"builder\") runs none. Expose it from the build's own app."
    }
    precondition {
      condition     = local.a2a_configured
      error_message = "expose_a2a without a Cognito pool this deployment creates needs a2a_identity = { audience, scope, client_ids } from your identity provider: the audience and scope of the machine tokens your A2A callers get, and their client ids."
    }
    precondition {
      condition     = local.a2a_create ? true : var.a2a_identity.audience != local.jwt_audience
      error_message = "a2a_identity.audience must not be the sign-in client's audience: people's tokens would then pass the A2A routes' authorizer."
    }
  }
}

resource "aws_apigatewayv2_route" "a2a" {
  for_each             = toset(var.expose_a2a ? local.a2a_routes : [])
  api_id               = aws_apigatewayv2_api.bff.id
  route_key            = each.value
  target               = "integrations/${aws_apigatewayv2_integration.bff.id}"
  authorization_type   = "JWT"
  authorizer_id        = aws_apigatewayv2_authorizer.a2a[0].id
  authorization_scopes = [local.a2a_scope]
}

output "a2a_endpoint" {
  description = "The A2A agent's URL (its card is at <url>/.well-known/agent-card.json); null when expose_a2a is off."
  value       = var.expose_a2a ? "${aws_apigatewayv2_api.bff.api_endpoint}/api/a2a" : null
}
output "a2a_token_url" {
  description = "Where an A2A caller gets a client-credentials token; null when expose_a2a is off."
  value       = var.expose_a2a ? local.a2a_token_url : null
}
output "a2a_client_id" {
  description = "The A2A app client this deployment created (its secret stays in Cognito); null otherwise."
  value       = local.a2a_create ? aws_cognito_user_pool_client.a2a[0].id : null
}
