variable "region" {
  type    = string
  default = "us-east-1"
}

variable "tags" {
  description = "Tags on every resource the AWS provider creates, on top of agentexpress:app. The deploy runner adds agentexpress:build / :version / :owner for a build. Mirrors CDK -c tags='{...}'."
  type        = map(string)
  default     = {}
}

variable "agent_name" {
  description = "AgentCore runtime name (letters, numbers, underscores)."
  type        = string
  default     = "multiagent_orchestrator"
}

variable "model_id" {
  description = "Default model for agents that name none. Empty: the workflow's orchestrator.defaultModel, else the framework default (app/defaults.json). Moved to this region's inference-profile geography either way (models.tf)."
  type        = string
  default     = ""
}

variable "container_engine" {
  description = "Container CLI used to build/push images (docker, finch, or podman)."
  type        = string
  default     = "docker"
}

variable "memory_event_expiry_days" {
  description = "Retention for AgentCore Memory short-term events."
  type        = number
  default     = 30
}

variable "log_retention_days" {
  description = <<-EOT
    Retention for the log groups of the Lambdas this stack creates.

    Without an explicit log group, Lambda creates `/aws/lambda/<name>` itself with
    NEVER-EXPIRE retention and no owner — so `terraform destroy` leaves it behind
    accruing cost forever. Verified: a full destroy of this stack orphaned eight such
    groups. Declaring them here makes them stack-owned, so they are retained for this
    many days and removed with everything else.
  EOT
  type        = number
  default     = 30
}

variable "transaction_search_indexing_percentage" {
  description = "Percentage of spans indexed for CloudWatch Transaction Search (0-100). Enabling Transaction Search is what delivers agent spans to the aws/spans log group, which AgentCore Observability and the Evaluations/Insights features read. 1% is free; the default 100 gives full trace coverage for a low-volume deployment — lower it to reduce cost at higher volume."
  type        = number
  default     = 100
}

# ===========================================================================
# IDENTITY PROVIDER (IdP) — one switch selects the whole auth stack
# ===========================================================================
# `idp` picks the provider for BOTH boundaries at once:
#   * end-user login on the UI + the API Gateway JWT authorizer on /api/*
#   * the machine-to-machine (client-credentials) token agents use to call the
#     AgentCore Gateway
#
# Everything downstream (authorizer issuer/audience, the Gateway's CUSTOM_JWT
# config, the OAuth token URL, the shape of auth-config.js the SPA reads) is
# derived in identity.tf — you set ONE value here.
#
#   "cognito" — Amazon Cognito. Terraform can create the whole pool for you
#               (cognito.create = true), including the confidential M2M client.
#   "auth0"   — an existing Auth0 tenant. Terraform creates nothing in Auth0;
#               you supply the tenant domain + client ids.
#   "none"    — NO authentication. The UI and /api/* are OPEN. Local trials and
#               throwaway sandboxes only — never anywhere shared.
#
# See identity.tf for the per-provider requirements (it fails the plan with a
# precise message if something required for your chosen provider is missing).

variable "idp" {
  description = "Identity provider for UI login + API auth + agent->Gateway M2M: \"cognito\", \"auth0\", or \"none\"."
  type        = string
  default     = "cognito"

  validation {
    condition     = contains(["cognito", "auth0", "none"], var.idp)
    error_message = "idp must be one of: \"cognito\", \"auth0\", \"none\"."
  }
  # idp = "none" deploys the UI and /api/* with NO sign-in on a public CloudFront URL,
  # where anyone who finds it can run agents on this account's Bedrock budget.
  validation {
    condition     = var.idp != "none" || var.allow_unauthenticated
    error_message = "idp = \"none\" deploys the console with no authentication on a public URL. Set allow_unauthenticated = true to deploy it anyway, or use idp = \"cognito\"."
  }
}

variable "allow_unauthenticated" {
  description = "Required, as true, alongside idp = \"none\": the explicit acknowledgement that the console deploys with no sign-in. Mirrors CDK -c allowUnauthenticated=true."
  type        = bool
  default     = false
}

variable "cognito" {
  description = "Cognito settings (used when idp = \"cognito\"). Set create = true to have Terraform provision the User Pool, Hosted UI domain, SPA client and — when enable_gateway is true — the confidential M2M client + resource server. Otherwise supply your existing ids."
  type = object({
    create        = optional(bool, true)
    user_pool_id  = optional(string, "")
    client_id     = optional(string, "")
    domain_prefix = optional(string, "")
    # Open registration on the Hosted UI (email verified), with each new user put in
    # self_signup_group. Off by default: the UI is on a public URL, and every user can
    # spend the account's Bedrock budget. Only with create = true. Mirrors CDK
    # -c selfSignUp=true / -c selfSignUpGroup=...
    self_signup       = optional(bool, false)
    self_signup_group = optional(string, "members")
  })
  default = {}
}

variable "auth0" {
  description = "Auth0 settings (used when idp = \"auth0\"). Terraform creates nothing in Auth0 — set these from your tenant. `client_id` is the SPA application (its id is also the ID-token audience the API authorizer checks)."
  type = object({
    domain    = optional(string, "")
    client_id = optional(string, "")
  })
  default = {}
}

# Machine-to-machine identity for agent -> AgentCore Gateway. Required when
# enable_gateway = true, EXCEPT for cognito with create = true (Terraform makes
# the client itself and reads the secret from state).
#
#   Cognito: a Resource Server defines the scope; `audience` is that OAuth2
#            scope, e.g. "gateway/invoke". The token has a `client_id` claim.
#   Auth0:   an API's Identifier is the `audience`. The token has an `aud` claim
#            and no `client_id`, so the Gateway pins the caller on `azp` instead.
variable "gateway_identity" {
  description = "M2M client the runtime uses to obtain a Gateway token. `audience` = the OAuth2 scope (Cognito) or the API identifier (Auth0)."
  type = object({
    client_id = optional(string, "")
    audience  = optional(string, "")
  })
  default = {}
}

variable "gateway_client_secret" {
  description = "M2M client secret. Pass via TF_VAR_gateway_client_secret; never commit it. Not needed when idp = \"cognito\" and cognito.create = true."
  type        = string
  default     = ""
  sensitive   = true
}

# --- AgentCore Gateway (MCP tool plane) -----------------------------------

variable "enable_gateway" {
  description = "Create an AgentCore Gateway fronting an MCP server. Set false to skip all Gateway/Cognito resources."
  type        = bool
  default     = true
}

# --- Tool secrets ----------------------------------------------------------
# API keys for tools declared in app/workflow.json, keyed by the TOOL NAME
# (the key in the workflow.json `tools` block). Only needed for a tool whose
# endpoint requires a key; everything else about the tool is declared in
# workflow.json, which stays free of secrets.
#
# Pass at apply time rather than committing:
#   export TF_VAR_tool_api_keys='{"billing":"sk-live-..."}'
#
# Each key is vaulted in an AgentCore API-key credential provider and sent by
# the Gateway as an X-API-Key header, so it never reaches the agent.
variable "tool_api_keys" {
  description = "Map of workflow.json tool name -> API key, for tools that need one."
  type        = map(string)
  default     = {}
  sensitive   = true
}

# The secret of each identity in workflow.json `identities` (identities.tf): an OAuth
# client secret or an API key, keyed by the identity's name. Like tool_api_keys, never in
# workflow.json:
#   export TF_VAR_identity_secrets='{"partner":"..."}'
variable "identity_secrets" {
  description = "Map of workflow.json identity name -> its client secret or API key."
  type        = map(string)
  default     = {}
  sensitive   = true
}
variable "a2a_tokens" {
  description = <<-EOT
    Map of workflow.json agent id -> bearer token, for each `runtime = "a2a"` agent
    declaring `auth = "bearer"`. These are credentials for somebody ELSE's agent, so
    they are deliberately not in workflow.json (which is committed):
      export TF_VAR_a2a_tokens='{"credit_check":"..."}'
    For an OAuth-protected remote agent use `auth = "oauth2"` instead and let
    AgentCore Workload Identity mint the token, so no long-lived secret exists here.
  EOT
  type        = map(string)
  default     = {}
  sensitive   = true
}

variable "extra_callback_urls" {
  description = "Further Hosted UI callback/logout URLs on the SPA client, for local development (e.g. [\"https://localhost:5173\"]). Only the CloudFront URL by default."
  type        = list(string)
  default     = []
}

variable "api_throttle_rate" {
  description = "Steady-state requests per second on the BFF API's $default stage. Keep in step with API_THROTTLE_RATE in cdk/lib/orchestrator-stack.ts."
  type        = number
  default     = 500
}

variable "api_throttle_burst" {
  description = "Burst limit on the BFF API's $default stage. Keep in step with API_THROTTLE_BURST in cdk/lib/orchestrator-stack.ts."
  type        = number
  default     = 1000
}

variable "hosted_ui_providers" {
  description = "Sign-in options the Hosted UI offers on the SPA client: \"COGNITO\" (the pool's own users) and/or the name of a federated identity provider you added to the pool (e.g. an OIDC IdP). Set here rather than by hand: a manual change is reverted when the client updates. Mirrors CDK -c hostedUiProviders."
  type        = list(string)
  default     = ["COGNITO"]
}
