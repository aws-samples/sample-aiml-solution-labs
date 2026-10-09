# ===========================================================================
# IDENTITY ABSTRACTION — the ONE place that knows how each IdP differs
# ===========================================================================
# `var.idp` ("cognito" | "auth0" | "okta" | "entra" | "none") selects the
# provider; everything else in the stack consumes the provider-agnostic locals below:
#
#   local.auth_enabled           is there any auth at all?
#   local.jwt_issuer             API Gateway JWT authorizer issuer
#   local.jwt_audience           API Gateway JWT authorizer audience
#   local.gateway_discovery_url  OIDC discovery for the Gateway's CUSTOM_JWT authorizer
#   local.gateway_token_url      OAuth2 token endpoint the runtime posts to
#   local.gateway_auth_flow      the idp — how the app builds that request
#   local.gateway_client_id      M2M client id
#   local.gateway_client_secret  M2M client secret
#   local.gateway_audience       OAuth2 scope (Cognito) / API identifier (Auth0) /
#                                the token's `aud` (Okta, Entra)
#   local.gateway_scope          the scope requested where it is not the audience
#   local.ui_auth                what gets rendered into web/auth-config.js
#
# To add another OIDC provider: add a branch to each local below plus a case in
# app/features/gateway/client.py and the UI's initAuth(). Nothing else changes.

locals {
  is_cognito = var.idp == "cognito"
  # What the workflow says signs people in (authorization.signIn.provider): a console
  # build deploys with it; a deployment of your own must agree (checked below).
  workflow_sign_in = try(local.workflow_def.authorization.signIn.provider, "")
  is_auth0         = var.idp == "auth0"
  is_okta          = var.idp == "okta"
  is_entra         = var.idp == "entra"
  # Okta and Entra sign in through the same standard OIDC code + PKCE flow in the UI.
  is_oidc = local.is_okta || local.is_entra
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

  # --- Okta: always external, through a CUSTOM authorization server -------
  # The org authorization server (https://<domain>) signs users in but cannot issue
  # client-credentials tokens with a custom scope, so the Gateway could not be
  # reached: one custom server serves both boundaries.
  okta_domain    = local.is_okta ? try(var.okta.domain, "") : ""
  okta_client_id = local.is_okta ? try(var.okta.client_id, "") : ""
  okta_server    = local.is_okta ? coalesce(try(var.okta.authorization_server, ""), "default") : ""
  okta_issuer    = local.is_okta ? "https://${local.okta_domain}/oauth2/${local.okta_server}" : ""

  # --- Microsoft Entra ID: always external, ONE tenant ---------------------
  # The v2.0 endpoints. The tenant must be its id: `iss` carries the GUID whatever
  # name signed in, and common/organizations would admit every tenant's users.
  entra_tenant    = local.is_entra ? try(var.entra.tenant_id, "") : ""
  entra_client_id = local.is_entra ? try(var.entra.client_id, "") : ""
  entra_login     = "https://login.microsoftonline.com/${local.entra_tenant}"
  entra_issuer    = local.is_entra ? "${local.entra_login}/v2.0" : ""

  # --- API Gateway JWT authorizer ----------------------------------------
  # Cognito's issuer is the user-pool endpoint; Auth0's is the tenant domain WITH
  # a trailing slash (Auth0 mints `iss` that way — omitting it fails validation).
  # Okta's is the authorization server and Entra's the tenant's v2.0 issuer, both
  # without one.
  jwt_issuer = (local.is_cognito
    ? "https://cognito-idp.${var.region}.amazonaws.com/${local.cognito_pool_id}"
    : local.is_auth0 ? "https://${local.auth0_domain}/"
    : local.is_okta ? local.okta_issuer
  : local.is_entra ? local.entra_issuer : "")
  # Every provider puts the SPA client id in the ID token's `aud`.
  jwt_audience = (local.is_cognito ? local.cognito_client_id
    : local.is_auth0 ? local.auth0_client_id
    : local.is_okta ? local.okta_client_id
  : local.entra_client_id)

  # --- Agent -> Gateway machine identity ---------------------------------
  # The agents sign in to the Gateway as a MACHINE, not as the person, so this need
  # not be the sign-in provider. A pool this deployment creates gives them a client:
  # the sign-in pool when Cognito is created here, else a pool of its own for
  # machines only (no users, no sign-in page), so signing people in with Okta, Auth0
  # or Entra needs no machine app set up there. Bring your own client through
  # var.gateway_identity to use your provider's instead.
  machine_pool = (local.gateway_enabled && !local.create_cognito && var.idp != "none"
  && try(var.gateway_identity.client_id, "") == "")
  make_pool   = local.create_cognito || local.machine_pool
  create_m2m  = local.make_pool && local.gateway_enabled
  pool_id     = local.make_pool ? aws_cognito_user_pool.this[0].id : ""
  pool_domain = local.make_pool ? aws_cognito_user_pool_domain.this[0].domain : ""

  gateway_client_id = (local.create_m2m
    ? aws_cognito_user_pool_client.m2m[0].id
  : try(var.gateway_identity.client_id, ""))
  gateway_client_secret = (local.create_m2m
    ? aws_cognito_user_pool_client.m2m[0].client_secret
  : var.gateway_client_secret)
  # Cognito -> the OAuth2 scope; Auth0 -> the API identifier; Okta -> the
  # authorization server's audience; Entra -> the API app's client id.
  gateway_audience = (local.create_m2m
    ? "gateway/invoke"
  : try(var.gateway_identity.audience, ""))
  # Which request shape and token the Gateway expects: a created pool's is Cognito's.
  gateway_auth_flow = local.create_m2m ? "cognito" : var.idp
  # What to ask for where the audience is not it: Okta's custom scope, Entra's
  # "<api>/.default". Empty for Cognito and Auth0, which do not read it.
  gateway_scope = (local.gateway_auth_flow == "okta" ? try(var.gateway_identity.scope, "")
    : local.gateway_auth_flow == "entra" ? coalesce(try(var.gateway_identity.scope, ""), "${local.gateway_audience}/.default")
  : "")

  # OIDC discovery the Gateway uses to validate inbound tokens.
  gateway_discovery_url = (local.create_m2m
    ? "https://cognito-idp.${var.region}.amazonaws.com/${local.pool_id}/.well-known/openid-configuration"
    : local.is_cognito ? "https://cognito-idp.${var.region}.amazonaws.com/${local.cognito_pool_id}/.well-known/openid-configuration"
    : local.is_auth0 ? "https://${local.auth0_domain}/.well-known/openid-configuration"
    : local.is_okta ? "${local.okta_issuer}/.well-known/openid-configuration"
  : local.is_entra ? "${local.entra_issuer}/.well-known/openid-configuration" : "")

  # Token endpoints differ in path AND in request shape — see gateway_auth_flow.
  gateway_token_url = (local.create_m2m
    ? "https://${local.pool_domain}.auth.${var.region}.amazoncognito.com/oauth2/token"
    : local.is_cognito ? "https://${local.cognito_domain}.auth.${var.region}.amazoncognito.com/oauth2/token"
    : local.is_auth0 ? "https://${local.auth0_domain}/oauth/token"
    : local.is_okta ? "${local.okta_issuer}/v1/token"
  : local.is_entra ? "${local.entra_login}/oauth2/v2.0/token" : "")

  # Consumed by app/features/gateway/client.py to pick the request shape:
  #   cognito -> HTTP Basic (client_id:secret) + form body with `scope`
  #   auth0   -> form body with client_id + client_secret + `audience`
  #   okta    -> HTTP Basic + form body with `scope` (gateway_scope)
  #   entra   -> form body with client_id + client_secret + `scope` (gateway_scope)
  # (gateway_auth_flow itself is set above, beside the machine pool.)

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
  # One template serves every provider; unused fields are empty strings. Okta and
  # Entra get their endpoints spelled out, so the UI's one OIDC strategy serves both.
  ui_auth = {
    enabled       = local.auth_enabled ? "true" : "false"
    provider      = var.idp
    region        = var.region
    user_pool_id  = local.cognito_pool_id
    client_id     = local.jwt_audience
    domain_prefix = local.cognito_domain
    domain        = local.auth0_domain
    authorize_url = (local.is_okta ? "${local.okta_issuer}/v1/authorize"
    : local.is_entra ? "${local.entra_login}/oauth2/v2.0/authorize" : "")
    token_url = (local.is_okta ? "${local.okta_issuer}/v1/token"
    : local.is_entra ? "${local.entra_login}/oauth2/v2.0/token" : "")
    logout_url = (local.is_okta ? "${local.okta_issuer}/v1/logout"
    : local.is_entra ? "${local.entra_login}/oauth2/v2.0/logout" : "")
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
      condition     = !local.is_okta || (can(regex("^[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$", local.okta_domain)) && local.okta_client_id != "")
      error_message = "idp = \"okta\" requires okta.domain (the org's host, e.g. acme.okta.com: no https://, no path) and okta.client_id (the Single-Page App integration)."
    }

    precondition {
      condition     = !local.is_okta || can(regex("^[A-Za-z0-9]+$", local.okta_server))
      error_message = "okta.authorization_server is the custom authorization server's id (\"default\", or one like aus1a2b3c4d), letters and digits only."
    }

    precondition {
      condition     = !local.is_entra || (can(regex("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$", local.entra_tenant)) && local.entra_client_id != "")
      error_message = "idp = \"entra\" requires entra.tenant_id (the Directory (tenant) ID, a GUID: not a domain name, common or organizations) and entra.client_id (the app registration with the Single-page application platform)."
    }

    precondition {
      condition     = !(local.gateway_enabled && local.gateway_auth_flow == "okta" && local.gateway_scope == "")
      error_message = "idp = \"okta\" with your own gateway_identity requires gateway_identity.scope: a custom scope on the authorization server (e.g. gateway.invoke). Okta issues no client-credentials token without one. (Leave gateway_identity out and the deployment makes the agents' client itself.)"
    }

    precondition {
      condition     = !(local.gateway_enabled && local.gateway_auth_flow == "entra" && !can(regex("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$", local.gateway_audience)))
      error_message = "idp = \"entra\" with enable_gateway = true requires gateway_identity.audience to be the client id (GUID) of the app registration that exposes the API: a v2 token carries that as `aud`, not its api:// URI."
    }

    precondition {
      condition = !(local.gateway_enabled && !local.create_m2m && (
        local.gateway_client_id == "" || local.gateway_audience == ""
      ))
      error_message = "gateway_identity needs both client_id and audience (the OAuth2 scope for Cognito, the API identifier for Auth0, the authorization server's audience for Okta, the API app's client id for Entra). Leave gateway_identity out entirely and the deployment makes the agents' client itself."
    }

    precondition {
      condition     = !(local.gateway_enabled && !local.create_m2m && var.gateway_client_secret == "")
      error_message = "Your own gateway_identity needs its secret: export TF_VAR_gateway_client_secret=... (not needed when the deployment makes the agents' client itself)."
    }

    precondition {
      condition     = local.workflow_sign_in == "" || local.workflow_sign_in == var.idp
      error_message = "app/workflow.json authorization.signIn.provider is \"${local.workflow_sign_in}\", but this deployment's idp is \"${var.idp}\". Set idp = \"${local.workflow_sign_in}\" and its settings (see DEPLOYMENT.md, Identity provider), or change the workflow's sign-in."
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
  description = "SPA/app client id the UI logs in with (Cognito App Client, Auth0 SPA, Okta SPA integration, or Entra app registration)."
  value       = local.jwt_audience
}

output "login_issuer" {
  description = "The issuer the API authorizer trusts: register the UI URL as a redirect URI on that provider's SPA client."
  value       = local.jwt_issuer
}
