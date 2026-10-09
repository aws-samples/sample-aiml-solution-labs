# --- AgentCore Gateway (the tool plane's front door) -----------------------
#
# ONE Gateway fronts every tool your agents can call. Inbound auth is a
# CUSTOM_JWT authorizer backed by the configured IdP (see identity.tf): the agent
# runtime fetches a short-lived client-credentials token and calls the Gateway's
# MCP URL.
#
# The TARGETS behind it are generated from the `tools` block in
# app/workflow.json — see terraform/tools.tf. Add a data source there, not here.
#
# A Cedar policy engine (policy.tf) is attached below, so every tool call is
# authorized server-side against permits generated from that same config.

# --- Gateway service role -------------------------------------------------

resource "aws_iam_role" "gateway" {
  count = local.gateway_enabled ? 1 : 0
  name  = "AgentCoreGateway-${var.agent_name}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "bedrock-agentcore.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = {
        StringEquals = { "aws:SourceAccount" = local.account_id }
        ArnLike      = { "aws:SourceArn" = "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:*" }
      }
    }]
  })
}

resource "aws_iam_role_policy" "gateway" {
  count = local.gateway_enabled ? 1 : 0
  name  = "GatewayPolicy-${var.agent_name}"
  role  = aws_iam_role.gateway[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:${var.region}:${local.account_id}:*"
      },
      {
        # Needed when a target uses an API-key / OAuth credential provider.
        # Harmless when every target is public or SigV4.
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = "arn:aws:secretsmanager:${var.region}:${local.account_id}:secret:bedrock-agentcore*"
      },
      {
        # Lets the Gateway read + evaluate the attached Cedar Policy Engine on
        # each tool call (required to attach a policy_engine_configuration).
        Sid    = "PolicyEngineEvaluate"
        Effect = "Allow"
        Action = [
          "bedrock-agentcore:GetPolicyEngine",
          "bedrock-agentcore:ListPolicies",
          "bedrock-agentcore:GetPolicy",
          "bedrock-agentcore:*Authorize*"
        ]
        # AuthorizeAction is checked against BOTH the policy engine and the
        # gateway resource, so grant on both ARNs.
        Resource = [
          "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:policy-engine/*",
          "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:gateway/*"
        ]
      }
      # OUTBOUND permission for the managed web-search connector. The connector
      # runs inside AWS and the Gateway reaches it as ITSELF, so without this a
      # call fails at INVOKE time (not at apply time) with
      #   -32002 "Execution role is not authorized for connector web-search"
      # Generated from config: present only when a tools entry declares
      # type=websearch.
      ], length(local.websearch_tools) == 0 ? [] : [
      {
        # Scoped to the AWS-OWNED tool ARN exactly as documented (note the
        # literal "aws" where the account id would normally be — authorization is
        # enforced per invocation against that ARN). This was "*" before, which
        # worked but granted more than the docs call for.
        Sid      = "InvokeWebSearch"
        Effect   = "Allow"
        Action   = ["bedrock-agentcore:InvokeWebSearch"]
        Resource = "arn:aws:bedrock-agentcore:${var.region}:aws:tool/web-search.v1"
      },
      {
        # The documented Web Search service-role policy pairs InvokeWebSearch with
        # InvokeGateway on the gateway.
        #
        # Scoped to gateway/* rather than the concrete ARN, to stay identical to
        # the CDK path: there, referencing the Gateway from this role's policy is a
        # CloudFormation circular dependency (the Gateway waits for the policy).
        # The PolicyEngineEvaluate statement above is scoped the same way.
        Sid      = "InvokeGateway"
        Effect   = "Allow"
        Action   = ["bedrock-agentcore:InvokeGateway"]
        Resource = "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:gateway/*"
      }
      # An OpenAPI target's schema is loaded from S3 BY THE GATEWAY, using this role.
      # Without this statement `type: "openapi"` cannot work at all: target creation
      # fails on a schema it may not read, and the message names neither the bucket
      # nor the permission. Generated from config, and scoped to the exact objects
      # the tools block declares — whether the framework uploaded the schema or the
      # customer hosts it themselves.
      # (An inline `schema` is sent with the target itself, so it needs no read.)
      ], length(local.openapi_source_tools) + length(local.external_openapi_tools) == 0 ? [] : [
      {
        Sid    = "ReadOpenApiSchemas"
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = concat(
          length(local.openapi_source_tools) == 0 ? [] : [
            "${aws_s3_bucket.tool_schemas[0].arn}/*"
          ],
          local.external_openapi_arns,
        )
      }
      # OAuth client credentials (auth = "oauth2"): the Gateway asks AgentCore Identity
      # for a token as its own workload identity. Both ARN forms of the token vault are
      # listed (the provider ARN comes back in the acps namespace). Mirrors
      # GatewayOAuthTokens in cdk/lib/tool-plane.ts.
      ], length(local.oauth_tool_names) == 0 ? [] : [
      {
        Sid    = "GatewayOAuthTokens"
        Effect = "Allow"
        Action = ["bedrock-agentcore:GetResourceOauth2Token", "bedrock-agentcore:GetWorkloadAccessToken"]
        Resource = [
          "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:token-vault/*",
          "arn:aws:acps:${var.region}:${local.account_id}:token-vault/*",
          "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:workload-identity-directory/*",
        ]
      }
      # An API-key provider (auth = "apikey", or an apikey identity) is read the same way;
      # without it every call answers "An internal error occurred". Mirrors
      # GatewayApiKeys in cdk/lib/tool-plane.ts.
      ], length(local.apikey_tool_names) == 0 ? [] : [
      {
        Sid    = "GatewayApiKeys"
        Effect = "Allow"
        Action = ["bedrock-agentcore:GetResourceApiKey", "bedrock-agentcore:GetWorkloadAccessToken"]
        Resource = [
          "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:token-vault/*",
          "arn:aws:acps:${var.region}:${local.account_id}:token-vault/*",
          "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:workload-identity-directory/*",
        ]
      }
      # Tools that act as the person (auth "user" / "obo"): the person Gateway gets each
      # person's own token, or exchanges their sign-in, as that person. Mirrors
      # PersonGatewayTokens in cdk/lib/tool-plane.ts.
      ], length(local.person_tool_names) == 0 ? [] : [
      {
        Sid    = "PersonGatewayTokens"
        Effect = "Allow"
        Action = ["bedrock-agentcore:GetResourceOauth2Token", "bedrock-agentcore:GetWorkloadAccessToken",
        "bedrock-agentcore:GetWorkloadAccessTokenForJWT"]
        Resource = [
          "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:token-vault/*",
          "arn:aws:acps:${var.region}:${local.account_id}:token-vault/*",
          "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:workload-identity-directory/*",
        ]
      }
      # A SigV4 API Gateway target: invoke exactly that API and stage, nothing else.
      ], length(local.signed_rest_api_arns) == 0 ? [] : [
      {
        Sid      = "GatewayInvokeRestApis"
        Effect   = "Allow"
        Action   = ["execute-api:Invoke"]
        Resource = local.signed_rest_api_arns
      }
      # The interceptors (interceptors.tf): here, not in a separate policy, so the IAM
      # wait before the Gateway is created covers them too. Mirrors InvokeInterceptors
      # in cdk/lib/tool-plane.ts.
    ], local.interceptor_statements)
  })
}

# --- The Gateway ----------------------------------------------------------

resource "aws_bedrockagentcore_gateway" "mcp" {
  count           = local.gateway_enabled ? 1 : 0
  name            = "${replace(var.agent_name, "_", "-")}-gw"
  role_arn        = aws_iam_role.gateway[0].arn
  protocol_type   = "MCP"
  authorizer_type = "CUSTOM_JWT"

  # Inbound auth is provider-specific, because the two token formats differ:
  #
  #   Cognito client-credentials tokens carry `client_id` + `scope` and NO `aud`
  #   claim, so the caller must be pinned with allowed_clients — an
  #   allowed_audience check could never match.
  #
  #   Auth0 M2M tokens are the mirror image: they carry `aud` (the API
  #   identifier) and no `client_id`, so we pin the audience and additionally
  #   constrain the calling application via its `azp` claim.
  #
  #   Okta tokens carry the authorization server's `aud` and the client as `cid`;
  #   Entra v2 tokens the API app's client id as `aud` and the client as `azp`.
  #   Same shape as Auth0: the audience, then the client by that claim.
  authorizer_configuration {
    custom_jwt_authorizer {
      discovery_url = local.gateway_discovery_url

      allowed_clients = local.gateway_auth_flow == "cognito" ? concat([local.gateway_client_id],
      [for c in aws_cognito_user_pool_client.agent : c.id]) : null
      allowed_audience = local.gateway_auth_flow == "cognito" ? null : [local.gateway_audience]

      dynamic "custom_claim" {
        for_each = local.gateway_auth_flow == "cognito" ? [] : [local.gateway_auth_flow == "okta" ? "cid" : "azp"]
        content {
          inbound_token_claim_name       = custom_claim.value
          inbound_token_claim_value_type = "STRING"
          authorizing_claim_match_value {
            claim_match_operator = "EQUALS"
            claim_match_value {
              match_value_string = local.gateway_client_id
            }
          }
        }
      }
    }
  }

  # AgentCore Policy: attach the Cedar policy engine (terraform/policy.tf) so the
  # Gateway evaluates policies on every tool call. `mode` comes from workflow.json
  # (orchestrator.policy.mode): LOG_ONLY = log decisions only; ENFORCE = block.
  # When policy.enabled is false, local.policy_enabled is false and this block is
  # omitted entirely — the Gateway does no policy evaluation.
  dynamic "policy_engine_configuration" {
    for_each = local.policy_enabled ? [1] : []
    content {
      arn  = aws_bedrockagentcore_policy_engine.main[0].policy_engine_arn
      mode = local.policy_mode
    }
  }
  # Interceptors (interceptors.tf): one REQUEST and/or one RESPONSE Lambda. Headers
  # (the caller's token and the runtime's x-ax-* headers) only with passRequestHeaders.
  dynamic "interceptor_configuration" {
    for_each = [for p in ["request", "response"] : p if contains(keys(local.interceptor_arns), p)]
    content {
      interception_points = [upper(interceptor_configuration.value)]
      interceptor {
        lambda {
          arn = local.interceptor_arns[interceptor_configuration.value]
        }
      }
      input_configuration {
        pass_request_headers = try(local.interceptor_specs[interceptor_configuration.value].passRequestHeaders, false) == true
      }
    }
  }
  # CreateGateway checks, with the Gateway's OWN role, that it may use the policy engine
  # (bedrock-agentcore:AuthorizeAction). Ordering after the inline policy is not enough:
  # IAM had not propagated it yet when Terraform created the Gateway a moment later, and
  # CreateGateway failed with AccessDeniedException (seen on a fresh workshop account;
  # an IAM simulation of the same role allowed the action seconds afterwards). CDK does
  # not hit it because CloudFormation is slower between the two. Same wait as the runtimes.
  depends_on = [aws_iam_role_policy.gateway, time_sleep.gateway_iam_propagation]
}

resource "time_sleep" "gateway_iam_propagation" {
  count           = local.gateway_enabled ? 1 : 0
  depends_on      = [aws_iam_role_policy.gateway]
  create_duration = "25s"
}

# --- The person Gateway --------------------------------------------------------
# Tools that act as the PERSON using the app (auth "user": each person's own account,
# OAuth authorization code; auth "obo": the person's sign-in exchanged on their behalf)
# need the PERSON's token at the Gateway: AgentCore Identity keeps a 3LO grant per
# person, keyed by that token's subject, and exchanges that token for OBO. The Gateway
# above only ever sees the agents' machine client, so these tools get one of their own
# that trusts the app's sign-in (the ID token, whose audience is the app's client).
# MCP 2025-11-25 is what lets it answer "connect your account first" (URL elicitation).
# No Cedar engine or interceptors here: each person is authorized by the tool's own
# provider. Mirrors PersonGateway in cdk/lib/tool-plane.ts.
resource "aws_bedrockagentcore_gateway" "person" {
  count           = local.gateway_enabled && length(local.person_tool_names) > 0 ? 1 : 0
  name            = "${replace(var.agent_name, "_", "-")}-gwu"
  role_arn        = aws_iam_role.gateway[0].arn
  protocol_type   = "MCP"
  authorizer_type = "CUSTOM_JWT"
  protocol_configuration {
    mcp {
      supported_versions = ["2025-11-25"]
    }
  }
  authorizer_configuration {
    custom_jwt_authorizer {
      discovery_url    = "${trimsuffix(local.jwt_issuer, "/")}/.well-known/openid-configuration"
      allowed_audience = [local.jwt_audience]
    }
  }
}
