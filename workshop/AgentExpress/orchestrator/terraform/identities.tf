# --- identities: credentials this build's tools and agents sign in with ---------------
# workflow.json `identities.<name>`: an oauth2 client (its secret in the build's secrets)
# or an API key. Two consumers:
#
#   * a TOOL with `identity` inherits it (tools.tf): the Gateway gets its token or key
#     through that tool's own credential provider, exactly as with `auth` + `oauth`;
#   * an AGENT that lists it in agentcore.identity.outbound calls an API DIRECTLY, so
#     the provider is created here, and the runtime gets a credential from it through
#     this build's workload identity (app/features/identity/client.py).
#
# Mirrors identitiesOf in cdk/lib/orchestrator-stack.ts.
locals {
  identity_defs = try(local.workflow_def.identities, {})
  # Only the identities some agent reaches directly: a tool's is created per tool.
  agent_identity_names = toset(local.workflow_plane ? distinct(flatten([
    for id, a in try(local.workflow_def.agents, {}) : [
      for n in try(a.agentcore.identity.outbound, []) : n if contains(keys(local.identity_defs), n)
    ]
  ])) : [])
  oauth_identity_names  = toset([for n in local.agent_identity_names : n if lower(try(local.identity_defs[n].type, "")) == "oauth2"])
  apikey_identity_names = toset([for n in local.agent_identity_names : n if lower(try(local.identity_defs[n].type, "")) == "apikey"])
  identity_provider_name = { for n in local.agent_identity_names :
  n => "bedrock-agentcore-${replace(var.agent_name, "_", "-")}-id-${n}" }
  # What the runtime reads (IDENTITY_PROVIDERS): name -> provider, kind and scopes.
  identity_providers_env = jsonencode({ for n in local.agent_identity_names : n => {
    provider = local.identity_provider_name[n]
    type     = lower(local.identity_defs[n].type)
    scopes   = try(local.identity_defs[n].scopes, [])
  } })
  # The workload identity the runtimes exchange for a credential. Named inside the
  # `<agent_name>-*` pattern the runtime roles' GetWorkloadAccessToken grant covers.
  workload_identity_name = "${var.agent_name}-agents"
}

resource "terraform_data" "identity_validation" {
  lifecycle {
    precondition {
      condition     = alltrue([for n in local.agent_identity_names : nonsensitive(try(var.identity_secrets[n], "") != "")])
      error_message = "An identity an agent uses needs its secret in var.identity_secrets (the build's secrets): ${join(", ", [for n in local.agent_identity_names : n if nonsensitive(try(var.identity_secrets[n], "") == "")])}."
    }
  }
}

resource "aws_bedrockagentcore_oauth2_credential_provider" "identity" {
  for_each                   = local.oauth_identity_names
  name                       = local.identity_provider_name[each.key]
  credential_provider_vendor = "CustomOauth2"
  oauth2_provider_config {
    custom_oauth2_provider_config {
      client_id     = local.identity_defs[each.key].clientId
      client_secret = var.identity_secrets[each.key]
      oauth_discovery {
        discovery_url = try(local.identity_defs[each.key].discoveryUrl, "") != "" ? local.identity_defs[each.key].discoveryUrl : null
        dynamic "authorization_server_metadata" {
          for_each = try(local.identity_defs[each.key].discoveryUrl, "") == "" ? [1] : []
          content {
            issuer                 = try(local.identity_defs[each.key].issuer, "") != "" ? local.identity_defs[each.key].issuer : regex("^https://[^/]+", local.identity_defs[each.key].tokenUrl)
            token_endpoint         = local.identity_defs[each.key].tokenUrl
            authorization_endpoint = local.identity_defs[each.key].tokenUrl
          }
        }
      }
    }
  }
  depends_on = [terraform_data.identity_validation]
}

resource "aws_bedrockagentcore_api_key_credential_provider" "identity" {
  for_each   = local.apikey_identity_names
  name       = local.identity_provider_name[each.key]
  api_key    = var.identity_secrets[each.key]
  depends_on = [terraform_data.identity_validation]
}

resource "awscc_bedrockagentcore_workload_identity" "agents" {
  count = length(local.agent_identity_names) > 0 ? 1 : 0
  name  = local.workload_identity_name
}
