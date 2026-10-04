# ===========================================================================
# IDENTITY ABSTRACTION — the ONE place that knows how each IdP differs
# ===========================================================================
# `var.idp` ("cognito" | "auth0" | "none") selects the provider; everything else
# in the stack consumes the provider-agnostic locals below:
#
#   local.auth_enabled           is there any auth at all?
#   local.jwt_issuer             API Gateway JWT authorizer issuer
#   local.jwt_audience           API Gateway JWT authorizer audience
#   local.gateway_discovery_url  OIDC discovery for the Gateway's CUSTOM_JWT authorizer
#   local.gateway_token_url      OAuth2 token endpoint the runtime posts to
#   local.gateway_auth_flow      "cognito" | "auth0" — how the app builds that request
#   local.gateway_client_id      M2M client id
#   local.gateway_client_secret  M2M client secret
#   local.gateway_audience       OAuth2 scope (Cognito) / API identifier (Auth0)
#   local.ui_auth                what gets rendered into web/auth-config.js
#
# To add another OIDC provider: add a branch to each local below plus a case in
# app/features/gateway/client.py and the UI's initAuth(). Nothing else changes.

locals {
  is_cognito = var.idp == "cognito"
  is_auth0   = var.idp == "auth0"
  # "none" -> the UI and /api/* deploy OPEN.
  auth_enabled = var.idp != "none"

  # --- Cognito: created here, or supplied ---------------------------------
  # (the resources themselves live in cognito.tf, gated on local.create_cognito)
  create_cognito = local.is_cognito && try(var.cognito.create, true)

  cognito_pool_id = local.is_cognito ? (
    local.create_cognito ? aws_cognito_user_pool.this[0].id : try(var.cognito.user_pool_id, "")
  ) : ""
  cognito_client_id = local.is_cognito ? (
    local.create_cognito ? aws_cognito_user_pool_client.spa[0].id : try(var.cognito.client_id, "")
  ) : ""
  cognito_domain = local.is_cognito ? (
    local.create_cognito ? aws_cognito_user_pool_domain.this[0].domain : try(var.cognito.domain_prefix, "")
  ) : ""

  # --- Auth0: always external --------------------------------------------
  auth0_domain    = local.is_auth0 ? try(var.auth0.domain, "") : ""
  auth0_client_id = local.is_auth0 ? try(var.auth0.client_id, "") : ""

  # --- API Gateway JWT authorizer ----------------------------------------
  # Cognito's issuer is the user-pool endpoint; Auth0's is the tenant domain WITH
  # a trailing slash (Auth0 mints `iss` that way — omitting it fails validation).
  jwt_issuer = (local.is_cognito
    ? "https://cognito-idp.${var.region}.amazonaws.com/${local.cognito_pool_id}"
  : (local.is_auth0 ? "https://${local.auth0_domain}/" : ""))
  # Both providers put the SPA client id in the ID token's `aud`.
  jwt_audience = local.is_cognito ? local.cognito_client_id : local.auth0_client_id

  # --- Agent -> Gateway machine identity ---------------------------------
  # Cognito with create=true provisions its own M2M client (cognito.tf); anything
  # else is bring-your-own via var.gateway_identity.
  create_m2m = local.create_cognito && local.gateway_enabled

  gateway_client_id = (local.create_m2m
    ? aws_cognito_user_pool_client.m2m[0].id
  : try(var.gateway_identity.client_id, ""))
  gateway_client_secret = (local.create_m2m
    ? aws_cognito_user_pool_client.m2m[0].client_secret
  : var.gateway_client_secret)
  # Cognito -> the OAuth2 scope; Auth0 -> the API identifier.
  gateway_audience = (local.create_m2m
    ? "gateway/invoke"
  : try(var.gateway_identity.audience, ""))

  # OIDC discovery the Gateway uses to validate inbound tokens.
  gateway_discovery_url = (local.is_cognito
    ? "https://cognito-idp.${var.region}.amazonaws.com/${local.cognito_pool_id}/.well-known/openid-configuration"
  : (local.is_auth0 ? "https://${local.auth0_domain}/.well-known/openid-configuration" : ""))

  # Token endpoints differ in path AND in request shape — see gateway_auth_flow.
  gateway_token_url = (local.is_cognito
    ? "https://${local.cognito_domain}.auth.${var.region}.amazoncognito.com/oauth2/token"
  : (local.is_auth0 ? "https://${local.auth0_domain}/oauth/token" : ""))

  # Consumed by app/features/gateway/client.py to pick the request shape:
  #   cognito -> HTTP Basic (client_id:secret) + form body with `scope`
  #   auth0   -> form body with client_id + client_secret + `audience`
  gateway_auth_flow = var.idp

  # --- RBAC on run actions (workflow.json -> authorization) ---------------
  # Which human actions are restricted, and to which groups. This is an IDENTITY
  # concern, not a tool concern: it maps a JWT group claim to permission, so it
  # lives beside the rest of the IdP wiring even though bff/authz.py enforces it.
  #
  # authz_actions is the raw action -> [groups] map from config. An action absent
  # from it is UNRESTRICTED, so an empty/missing block leaves behaviour unchanged.
  # authz_groups is every distinct group named anywhere in it — the set Cognito
  # needs to exist (cognito.tf creates them, so a customer never hand-creates a
  # group the config already names).
  authz_actions = try(local.workflow_def.authorization.actions, {})
  authz_groups  = sort(distinct(flatten([for _, gs in local.authz_actions : gs])))
  # Mirrors ACTIONS in bff/authz.py. Duplicated deliberately: Terraform cannot
  # read the Python, and a silently-ignored typo in `actions` is the failure this
  # list exists to catch.
  # From app/vocabulary.json — the same file bff/authz.py and cdk/lib/vocabulary.ts
  # read. This was a third hand-written copy of the action names.
  authz_known_actions = jsondecode(file("${path.module}/../app/vocabulary.json")).authorizationActions.values

  # --- What the SPA needs (rendered into web/auth-config.js) --------------
  # One template serves every provider; unused fields are empty strings.
  ui_auth = {
    enabled       = local.auth_enabled ? "true" : "false"
    provider      = var.idp
    region        = var.region
    user_pool_id  = local.cognito_pool_id
    client_id     = local.is_cognito ? local.cognito_client_id : local.auth0_client_id
    domain_prefix = local.cognito_domain
    domain        = local.auth0_domain
  }
}

# --- Fail fast, with a precise message -------------------------------------
# A misconfigured IdP otherwise surfaces as a confusing downstream failure: an
# authorizer with an empty audience that rejects every request, or a UI that
# redirects to nowhere. These run at PLAN time and name exactly what's missing.
#
# Why preconditions on terraform_data rather than a `check` block: `check`
# assertions only emit WARNINGS and let the apply proceed, which is exactly the
# broken deploy we're trying to prevent. Preconditions are a hard error.
# Why not `variable validation`: these rules span several variables and depend on
# the locals above, which validation blocks cannot reference.
resource "terraform_data" "idp_validation" {
  input = var.idp

  lifecycle {
    precondition {
      condition = !(local.is_cognito && !local.create_cognito && (
        try(var.cognito.user_pool_id, "") == "" ||
        try(var.cognito.client_id, "") == "" ||
        try(var.cognito.domain_prefix, "") == ""
      ))
      error_message = "idp = \"cognito\" with cognito.create = false requires cognito.user_pool_id, cognito.client_id and cognito.domain_prefix."
    }

    precondition {
      condition     = !(local.is_auth0 && (local.auth0_domain == "" || local.auth0_client_id == ""))
      error_message = "idp = \"auth0\" requires auth0.domain and auth0.client_id (from your Auth0 SPA application)."
    }

    precondition {
      condition = !(local.gateway_enabled && !local.create_m2m && (
        local.gateway_client_id == "" || local.gateway_audience == ""
      ))
      error_message = "enable_gateway = true requires gateway_identity.client_id and gateway_identity.audience (the OAuth2 scope for Cognito, or the API identifier for Auth0), unless idp = \"cognito\" with cognito.create = true."
    }

    precondition {
      condition     = !(local.gateway_enabled && !local.create_m2m && var.gateway_client_secret == "")
      error_message = "enable_gateway = true requires the M2M secret: export TF_VAR_gateway_client_secret=... (not needed when idp = \"cognito\" and cognito.create = true)."
    }

    precondition {
      condition     = !(var.idp == "none" && local.gateway_enabled)
      error_message = "idp = \"none\" cannot be combined with enable_gateway = true: the Gateway's CUSTOM_JWT authorizer needs an OIDC provider. Set enable_gateway = false (no tool plane is then provisioned, so agents with a `tool` fail loudly), or pick an idp."
    }
  }
}

# --- Outputs (provider-agnostic) -------------------------------------------

output "idp" {
  description = "Identity provider in effect."
  value       = var.idp
}

output "auth_enabled" {
  description = "Whether the UI/API require authentication."
  value       = local.auth_enabled
}

output "cognito_user_pool_id" {
  description = "Cognito User Pool ID (empty unless idp = \"cognito\")."
  value       = local.cognito_pool_id
}

output "cognito_domain_prefix" {
  description = "Cognito Hosted UI domain prefix (empty unless idp = \"cognito\")."
  value       = local.cognito_domain
}

output "login_client_id" {
  description = "SPA/app client id the UI logs in with (Cognito App Client, or Auth0 SPA client)."
  value       = local.jwt_audience
}
