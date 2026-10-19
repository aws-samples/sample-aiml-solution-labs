# --- AgentCore Harness agents (agents with runtime "harness", app/common/harness_agent.py) --
# A harness is an agent with no code: AgentCore runs the model loop and its session from
# configuration. One per harness agent, built from its workflow.json entry:
#   system prompt  <- instructions
#   model          <- model (else the deployment default), temperature, topP, maxTokens, on
#                     harness.provider: Bedrock (default), or OpenAI / Gemini / LiteLLM with
#                     harness.apiKey (an apikey identity -> a credential provider made here,
#                     or a credential provider ARN)
#   limits         <- harness.maxIterations, harness.timeoutSeconds
#   tools          <- `tool` (the build's Gateway, signed in with the agent's Gateway machine
#                     client through an OAuth2 credential provider made here), harness.mcp,
#                     harness.browser, harness.codeInterpreter
#   allowedTools   <- exactly those, plus the built-in shell and file tools with harness.shell;
#                     none at all otherwise
#   skills         <- harness.skills (AWS catalog, Git, S3)
#   memory         <- disabled: CreateHarness provisions a managed memory (billed) when the
#                     field is omitted (devguide/harness-memory.html); or harness.memory, one
#                     of the build's `memories`. The framework's own memory, if the agent
#                     declares agentcore.memory instead, is recalled and stored by the orchestrator.
#   environment    <- harness.container, harness.mounts, harness.network
#   endpoint       <- harness.endpoint: a named endpoint, moved to each new version
# Each gets its own execution role, scoped to the documented sample for a harness on the
# public network (devguide/harness-security.html, "Execution role policy") plus, only for
# what the agent uses, that page's optional statements narrowed to its own resources. The
# orchestrator's role may invoke these harnesses and nothing else of the kind.
#
# harness.arn brings a harness made elsewhere: nothing is created for that agent, and the
# orchestrator may invoke exactly that ARN (and its endpoint). A workflow with no harness
# agent gets nothing here. Mirrors cdk/lib/harness.ts.
locals {
  harness_all = {
    for id, a in try(local.workflow_def.agents, {}) : id => a
    if local.workflow_plane && try(a.runtime, "") == "harness"
  }
  # A harness you bring (harness.arn): invoked, never created.
  harness_byo = { for id, a in local.harness_all : id => tostring(a.harness.arn) if try(a.harness.arn, "") != "" }
  # The harnesses this deployment creates.
  harness_agents = { for id, a in local.harness_all : id => a if !contains(keys(local.harness_byo), id) }
  # CreateHarness harnessName is [a-zA-Z][a-zA-Z0-9_]{0,39}.
  harness_names      = { for id in keys(local.harness_agents) : id => "${var.agent_name}_${id}" }
  harness_role_names = { for id in keys(local.harness_agents) : id => "AgentCoreHarness-${var.agent_name}-${id}" }
  harness_dash       = replace(var.agent_name, "_", "-")

  # Each harness agent's settings, in one shape whatever the entry holds.
  harness_spec = { for id, a in local.harness_agents : id => {
    provider = lower(try(tostring(a.harness.provider), "bedrock"))
    tools    = [for k in flatten([try(a.tool, [])]) : tostring(k) if k != ""]
    mcp      = [for m in try(a.harness.mcp, []) : { name = tostring(m.name), url = tostring(m.url) }]
    # true for the managed one, or a custom one's ARN.
    browser_arn = try(regex("^arn:.*", a.harness.browser), "")
    browser     = try(a.harness.browser == true, false) || try(regex("^arn:.*", a.harness.browser), "") != ""
    code_arn    = try(regex("^arn:.*", a.harness.codeInterpreter), "")
    code        = try(a.harness.codeInterpreter == true, false) || try(regex("^arn:.*", a.harness.codeInterpreter), "") != ""
    skills = [for s in try(a.harness.skills, []) : {
      kind       = can(s.aws) ? "aws" : can(s.git) ? "git" : "s3"
      paths      = try([for p in s.aws : tostring(p)], [])
      url        = try(tostring(s.git), "")
      path       = try(tostring(s.path), "")
      credential = try(tostring(s.credential), "")
      uri        = try(tostring(s.s3), "")
    }]
    memory    = try(tostring(a.harness.memory), "")
    container = try(tostring(a.harness.container), "")
    mounts = [for m in try(a.harness.mounts, []) : {
      kind = can(m.session) ? "session" : can(m.efs) ? "efs" : "s3Files"
      path = try(tostring(m.session), tostring(m.path), "")
      arn  = try(tostring(m.efs), tostring(m.s3Files), "")
    }]
    subnets  = try([for s in a.harness.network.subnetIds : tostring(s)], [])
    groups   = try([for s in a.harness.network.securityGroupIds : tostring(s)], [])
    endpoint = try(tostring(a.harness.endpoint), "")
    api_key  = try(tostring(a.harness.apiKey), "")
    api_base = try(tostring(a.harness.apiBase), "")
  } }

  # allowedTools, in the order app/common/harness_agent.allowed_tools gives them. Omitted,
  # every tool is allowed, the built-in shell included; "@no_tools" names an MCP server the
  # harness does not have, so it matches nothing. Same literals in harness.ts.
  harness_tools = { for id, s in local.harness_spec : id => coalescelist(concat(
    try(local.harness_agents[id].harness.shell, false) == true ? ["@builtin"] : [],
    flatten([for k in s.tools : distinct(["@gateway/${k}___*", "@gateway/${lower(k)}___*"])]),
    [for m in s.mcp : "@${m.name}"],
    s.browser ? ["browser", "@browser"] : [],
    s.code ? ["code_interpreter", "@code_interpreter"] : [],
    length(s.skills) > 0 ? ["skills", "@builtin/skills"] : [],
  ), ["@no_tools"]) }

  # Harness agents that call the Gateway, and the client each signs in with: its own with
  # orchestrator.gatewayIdentity "perAgent" (cognito.tf makes one for every agent with a
  # `tool`), else the shared machine client.
  harness_gateway_ids = toset([for id, s in local.harness_spec : id if length(s.tools) > 0])
  # API keys made here from the build's secrets: apikey identities named by harness.apiKey
  # or a Git skill's credential. An ARN is a credential provider that already exists.
  harness_key_names = toset(distinct(flatten([for id, s in local.harness_spec : concat(
    s.api_key != "" && !startswith(s.api_key, "arn:") ? [s.api_key] : [],
    [for k in s.skills : k.credential if k.credential != "" && !startswith(k.credential, "arn:")],
  )])))
  # Every API key credential provider ARN a harness reads, by agent.
  harness_key_refs = { for id, s in local.harness_spec : id => distinct(concat(
  s.api_key != "" ? [s.api_key] : [], [for k in s.skills : k.credential if k.credential != ""])) }

  # What a harness agent cannot have: no instructions, a bad limit.
  bad_harness = [for id, a in local.harness_agents : id if(
    trimspace(try(tostring(a.instructions), "")) == ""
    || !(try(a.harness.maxIterations, 1) >= 1 && try(a.harness.maxIterations, 1) <= 100)
    || !(try(a.harness.timeoutSeconds, 1) >= 1 && try(a.harness.timeoutSeconds, 1) <= 900)
  )]
  bad_harness_names = [for id, n in local.harness_names : id if !can(regex("^[a-zA-Z][a-zA-Z0-9_]{0,39}$", n)) || length(local.harness_role_names[id]) > 64]
  # Gateway tools need the Gateway, Cognito's client-credentials token (what the OAuth2
  # credential provider below fetches), and tools that do not act as the person.
  bad_harness_gateway = [for id in local.harness_gateway_ids : id if(
    !local.gateway_enabled || local.gateway_auth_flow != "cognito"
    || anytrue([for k in local.harness_spec[id].tools : !contains(keys(local.tools), k) || contains(["user", "obo"], try(local.tools[k].auth, ""))])
  )]
  bad_harness_refs = [for id, s in local.harness_spec : id if(
    (s.memory != "" && !contains(keys(local.named_memories), s.memory))
    || anytrue([for n in local.harness_key_refs[id] : !startswith(n, "arn:") && nonsensitive(try(var.identity_secrets[n], "") == "")])
  )]
  # A harness you bring: this account and region (InvokeHarness is regional, and a harness
  # has no resource policy that would let another account's role call it).
  bad_harness_byo = [for id, arn in local.harness_byo : id if(
    !can(regex("^arn:aws[a-z-]*:bedrock-agentcore:${var.region}:${local.account_id}:harness/[a-zA-Z][a-zA-Z0-9_]{0,39}-[a-zA-Z0-9]{10}$", arn))
  )]

  # agent id -> harness ARN for the orchestrator (HARNESS_ARNS). Only set when there is one,
  # so a deployment without harness agents keeps its runtime's environment as it was. A
  # harness you bring is read from workflow.json (harness.arn), not from here.
  harness_env = length(local.harness_agents) > 0 ? {
    HARNESS_ARNS = jsonencode({ for id, h in aws_bedrockagentcore_harness.agent : id => h.arn })
  } : {}
  # Every harness ARN the orchestrator invokes, and the endpoint ARN of each named endpoint
  # (devguide/harness-security.html: invoking a custom endpoint needs both).
  harness_invoke_arns = concat(
    [for h in aws_bedrockagentcore_harness.agent : h.arn],
    [for id, s in local.harness_spec : "${aws_bedrockagentcore_harness.agent[id].arn}/harness-endpoint/${s.endpoint}" if s.endpoint != ""],
    values(local.harness_byo),
    [for id, arn in local.harness_byo : "${arn}/harness-endpoint/${local.harness_all[id].harness.endpoint}" if try(local.harness_all[id].harness.endpoint, "") != ""],
  )
}

resource "terraform_data" "harness_agents" {
  count = length(local.harness_all) > 0 ? 1 : 0
  lifecycle {
    precondition {
      condition     = length(local.bad_harness) == 0
      error_message = "app/workflow.json harness agent(s) ${join(", ", local.bad_harness)}: a harness agent needs `instructions`, and harness.maxIterations is 1-100, harness.timeoutSeconds 1-900."
    }
    precondition {
      condition     = length(local.bad_harness_names) == 0
      error_message = "app/workflow.json harness agent(s) ${join(", ", local.bad_harness_names)}: the harness is named <agent_name>_<agent id>, which must be at most 40 letters, digits and underscores, starting with a letter (and its role AgentCoreHarness-<agent_name>-<id> at most 64 characters). Shorten var.agent_name or the agent id."
    }
    precondition {
      condition     = length(local.bad_harness_gateway) == 0
      error_message = "app/workflow.json harness agent(s) ${join(", ", local.bad_harness_gateway)}: a harness agent's `tool` needs the Gateway (enable_gateway = true) signed in with the Cognito machine client this deployment makes or is given (idp = \"cognito\", or a machine pool with Okta, Auth0 or Entra), and tools of `tools` that do not act as the person (auth \"user\" / \"obo\")."
    }
    precondition {
      condition     = length(local.bad_harness_refs) == 0
      error_message = "app/workflow.json harness agent(s) ${join(", ", local.bad_harness_refs)}: harness.memory must name one of `memories`, and an apikey identity named by harness.apiKey or a skill's credential needs its key in var.identity_secrets (the build's secrets)."
    }
    precondition {
      condition     = length(local.bad_harness_byo) == 0
      error_message = "app/workflow.json harness agent(s) ${join(", ", local.bad_harness_byo)}: harness.arn must be a harness in this account (${local.account_id}) and region (${var.region}); a harness in another account or region cannot be invoked from here."
    }
  }
}

# The Gateway, for harness agents with a `tool`: an OAuth2 credential provider holding the
# client the agent signs in with, so the harness gets the same Cognito token the other
# agents present and the Gateway's Cedar policy and interceptors apply to it as to them.
resource "aws_bedrockagentcore_oauth2_credential_provider" "harness_gateway" {
  for_each                   = local.harness_gateway_ids
  name                       = "bedrock-agentcore-${local.harness_dash}-hgw-${each.key}"
  credential_provider_vendor = "CustomOauth2"
  oauth2_provider_config {
    custom_oauth2_provider_config {
      client_id     = try(aws_cognito_user_pool_client.agent[each.key].id, local.gateway_client_id)
      client_secret = try(aws_cognito_user_pool_client.agent[each.key].client_secret, local.gateway_client_secret)
      oauth_discovery {
        authorization_server_metadata {
          issuer                 = trimsuffix(local.gateway_discovery_url, "/.well-known/openid-configuration")
          token_endpoint         = local.gateway_token_url
          authorization_endpoint = local.gateway_token_url
        }
      }
    }
  }
  depends_on = [terraform_data.harness_agents]
}

# Model and private Git skill API keys made from the build's secrets (harness_key_names).
resource "aws_bedrockagentcore_api_key_credential_provider" "harness" {
  for_each   = local.harness_key_names
  name       = "bedrock-agentcore-${local.harness_dash}-hk-${each.key}"
  api_key    = var.identity_secrets[each.key]
  depends_on = [terraform_data.harness_agents]
}

locals {
  # A key reference -> its credential provider ARN.
  harness_key_arn     = { for n in local.harness_key_names : n => aws_bedrockagentcore_api_key_credential_provider.harness[n].credential_provider_arn }
  harness_secret_base = "arn:aws:secretsmanager:${var.region}:${local.account_id}:secret:bedrock-agentcore-identity!default"
  # The optional statements of devguide/harness-security.html, per agent, only for what it
  # uses and narrowed to its own resources. In the order cdk/lib/harness.ts gives them.
  harness_extra_statements = { for id, s in local.harness_spec : id => concat(
    contains(local.harness_gateway_ids, id) ? [
      jsonencode({
        Sid    = "HarnessGatewayToken"
        Effect = "Allow"
        Action = "bedrock-agentcore:GetResourceOauth2Token"
        Resource = [
          "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:token-vault/default",
          aws_bedrockagentcore_oauth2_credential_provider.harness_gateway[id].credential_provider_arn,
          "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:workload-identity-directory/default",
          "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:workload-identity-directory/default/workload-identity/harness_${local.harness_names[id]}-*"
        ]
      }),
      jsonencode({
        Sid      = "HarnessGatewaySecret"
        Effect   = "Allow"
        Action   = "secretsmanager:GetSecretValue"
        Resource = "${local.harness_secret_base}/oauth2/bedrock-agentcore-${local.harness_dash}-hgw-${id}-*"
      })
    ] : [],
    length(local.harness_key_refs[id]) > 0 ? [
      jsonencode({
        Sid    = "HarnessApiKeys"
        Effect = "Allow"
        Action = "bedrock-agentcore:GetResourceApiKey"
        Resource = concat([
          "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:token-vault/default",
          "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:workload-identity-directory/default",
          "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:workload-identity-directory/default/workload-identity/harness_${local.harness_names[id]}-*"
        ], [for n in local.harness_key_refs[id] : startswith(n, "arn:") ? n : local.harness_key_arn[n]])
      }),
      jsonencode({
        # The key's secret, named after its provider (the last part of the provider ARN).
        Sid    = "HarnessApiKeySecrets"
        Effect = "Allow"
        Action = "secretsmanager:GetSecretValue"
        Resource = [for n in local.harness_key_refs[id] : startswith(n, "arn:")
          ? "arn:aws:secretsmanager:${split(":", n)[3]}:${split(":", n)[4]}:secret:bedrock-agentcore-identity!default/apikey/${element(split("/", n), length(split("/", n)) - 1)}-*"
        : "${local.harness_secret_base}/apikey/bedrock-agentcore-${local.harness_dash}-hk-${n}-*"]
      })
    ] : [],
    s.browser ? [jsonencode({
      Sid    = "HarnessBrowser"
      Effect = "Allow"
      Action = ["bedrock-agentcore:StartBrowserSession", "bedrock-agentcore:StopBrowserSession", "bedrock-agentcore:GetBrowserSession",
        "bedrock-agentcore:ListBrowserSessions", "bedrock-agentcore:UpdateBrowserStream", "bedrock-agentcore:ConnectBrowserAutomationStream",
      "bedrock-agentcore:ConnectBrowserLiveViewStream"]
      Resource = s.browser_arn != "" ? s.browser_arn : "arn:aws:bedrock-agentcore:${var.region}:aws:browser/*"
    })] : [],
    s.code ? [jsonencode({
      Sid    = "HarnessCodeInterpreter"
      Effect = "Allow"
      Action = ["bedrock-agentcore:StartCodeInterpreterSession", "bedrock-agentcore:StopCodeInterpreterSession",
        "bedrock-agentcore:GetCodeInterpreterSession", "bedrock-agentcore:ListCodeInterpreterSessions",
      "bedrock-agentcore:InvokeCodeInterpreter"]
      Resource = s.code_arn != "" ? s.code_arn : "arn:aws:bedrock-agentcore:${var.region}:aws:code-interpreter/*"
    })] : [],
    s.memory != "" ? [jsonencode({
      Sid      = "HarnessMemory"
      Effect   = "Allow"
      Action   = ["bedrock-agentcore:CreateEvent", "bedrock-agentcore:DeleteEvent", "bedrock-agentcore:GetEvent", "bedrock-agentcore:ListEvents", "bedrock-agentcore:RetrieveMemoryRecords"]
      Resource = awscc_bedrockagentcore_memory.named[s.memory].memory_arn
    })] : [],
    length([for k in s.skills : k if k.kind == "s3"]) > 0 ? [
      jsonencode({
        # The skill objects under each prefix, and the listing of their buckets.
        Sid      = "HarnessSkillObjects"
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = [for k in s.skills : "arn:aws:s3:::${trimprefix(k.uri, "s3://")}*" if k.kind == "s3"]
      }),
      jsonencode({
        Sid      = "HarnessSkillBuckets"
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = distinct([for k in s.skills : "arn:aws:s3:::${split("/", trimprefix(k.uri, "s3://"))[0]}" if k.kind == "s3"])
      })
    ] : [],
    can(regex("^[0-9]{12}\\.dkr\\.ecr\\.", s.container)) ? [jsonencode({
      # Your image's repository only.
      Sid      = "HarnessContainerPull"
      Effect   = "Allow"
      Action   = ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"]
      Resource = "arn:aws:ecr:${regex("^([0-9]{12})\\.dkr\\.ecr\\.([a-z0-9-]+)\\.", s.container)[1]}:${regex("^([0-9]{12})\\.", s.container)[0]}:repository/${regex("^[^/]+/([^:@]+)", s.container)[0]}"
    })] : [],
    length(s.subnets) > 0 ? [jsonencode({
      # In a VPC the harness pulls its managed image from the region's private
      # repository, owned by an AWS service account (devguide/harness-security.html).
      Sid      = "HarnessVpcImagePull"
      Effect   = "Allow"
      Action   = ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer", "ecr:BatchCheckLayerAvailability"]
      Resource = "arn:aws:ecr:${var.region}:*:repository/harness-*"
    })] : [],
    can(regex("^[0-9]{12}\\.dkr\\.ecr\\.", s.container)) || length(s.subnets) > 0 ? [jsonencode({
      # ecr:GetAuthorizationToken has no resource type.
      Sid      = "HarnessEcrToken"
      Effect   = "Allow"
      Action   = "ecr:GetAuthorizationToken"
      Resource = "*"
    })] : [],
    length([for m in s.mounts : m if m.kind == "efs"]) > 0 ? [jsonencode({
      # Mount through exactly these access points (the file system is the access point's).
      Sid       = "HarnessEfsMounts"
      Effect    = "Allow"
      Action    = ["elasticfilesystem:ClientMount", "elasticfilesystem:ClientWrite"]
      Resource  = distinct([for m in s.mounts : "arn:aws:elasticfilesystem:${split(":", m.arn)[3]}:${split(":", m.arn)[4]}:file-system/*" if m.kind == "efs"])
      Condition = { StringEquals = { "elasticfilesystem:AccessPointArn" = [for m in s.mounts : m.arn if m.kind == "efs"] } }
    })] : [],
    length([for m in s.mounts : m if m.kind == "s3Files"]) > 0 ? [jsonencode({
      Sid       = "HarnessS3FilesMounts"
      Effect    = "Allow"
      Action    = ["s3files:ClientMount", "s3files:ClientWrite", "s3files:GetAccessPoint"]
      Resource  = distinct([for m in s.mounts : split("/access-point/", m.arn)[0] if m.kind == "s3Files"])
      Condition = { StringEquals = { "s3files:AccessPointArn" = [for m in s.mounts : m.arn if m.kind == "s3Files"] } }
    })] : [],
  ) }
}

resource "aws_iam_role" "harness" {
  for_each = local.harness_agents
  name     = local.harness_role_names[each.key]

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

  path                 = var.iam_role_path
  permissions_boundary = local.iam_permissions_boundary
  depends_on           = [terraform_data.harness_agents]
}

resource "aws_iam_role_policy" "harness" {
  for_each = local.harness_agents
  name     = "AgentCoreHarnessPolicy-${var.agent_name}-${each.key}"
  role     = aws_iam_role.harness[each.key].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Sid    = "HarnessModelInvocation"
        Effect = "Allow"
        Action = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"]
        # Models and inference profiles only, as for a dedicated agent's role.
        Resource = [
          "arn:aws:bedrock:*::foundation-model/*",
          "arn:aws:bedrock:${var.region}:${local.account_id}:inference-profile/*",
          "arn:aws:bedrock:${var.region}:${local.account_id}:application-inference-profile/*"
        ]
      },
      {
        # On the public network the harness pulls its managed image from Amazon ECR Public.
        # Neither action has a resource type, so "*"; the bearer token only for ECR Public.
        Sid       = "HarnessEcrPublicPull"
        Effect    = "Allow"
        Action    = ["ecr-public:GetAuthorizationToken", "sts:GetServiceBearerToken"]
        Resource  = "*"
        Condition = { StringEquals = { "sts:AWSServiceName" = "ecr-public.amazonaws.com" } }
      },
      {
        Sid      = "HarnessLogGroup"
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:DescribeLogStreams"]
        Resource = ["arn:aws:logs:${var.region}:${local.account_id}:log-group:/aws/bedrock-agentcore/runtimes/*"]
      },
      {
        Sid      = "HarnessDescribeLogGroups"
        Effect   = "Allow"
        Action   = ["logs:DescribeLogGroups"]
        Resource = ["arn:aws:logs:${var.region}:${local.account_id}:log-group:*"]
      },
      {
        Sid      = "HarnessLogStream"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["arn:aws:logs:${var.region}:${local.account_id}:log-group:/aws/bedrock-agentcore/runtimes/*:log-stream:*"]
      },
      {
        # Span delivery to its own log group; no resource type (see main.tf).
        Sid      = "HarnessSpanDelivery"
        Effect   = "Allow"
        Action   = ["logs:PutResourcePolicy"]
        Resource = "*"
      },
      {
        # X-Ray has no resource types for these.
        Sid      = "HarnessTracing"
        Effect   = "Allow"
        Action   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords", "xray:GetSamplingRules", "xray:GetSamplingTargets"]
        Resource = "*"
      },
      {
        Sid       = "HarnessMetrics"
        Effect    = "Allow"
        Action    = "cloudwatch:PutMetricData"
        Resource  = "*"
        Condition = { StringEquals = { "cloudwatch:namespace" = "bedrock-agentcore" } }
      },
      {
        Sid    = "HarnessWorkloadIdentity"
        Effect = "Allow"
        Action = ["bedrock-agentcore:GetWorkloadAccessToken", "bedrock-agentcore:GetWorkloadAccessTokenForJWT"]
        Resource = [
          "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:workload-identity-directory/default",
          "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:workload-identity-directory/default/workload-identity/harness_${local.harness_names[each.key]}-*"
        ]
      }
    ], [for s in local.harness_extra_statements[each.key] : jsondecode(s)])
  })
}

# The harness validates its role when it is created; wait for the policies to propagate.
resource "time_sleep" "harness_iam_propagation" {
  count           = length(local.harness_agents) > 0 ? 1 : 0
  depends_on      = [aws_iam_role_policy.harness]
  create_duration = "25s"
}

resource "aws_bedrockagentcore_harness" "agent" {
  for_each           = local.harness_agents
  harness_name       = local.harness_names[each.key]
  execution_role_arn = aws_iam_role.harness[each.key].arn

  system_prompt {
    text = each.value.instructions
  }
  model {
    dynamic "bedrock_model_config" {
      for_each = local.harness_spec[each.key].provider == "bedrock" ? [1] : []
      content {
        model_id   = local.regional_model[try(each.value.model, local.model_requested)]
        max_tokens = try(each.value.maxTokens, local.agent_defaults.maxTokens)
        # The framework's default temperature, except with topP alone: several Claude models
        # refuse both, and an in-process agent drops the temperature in that case too.
        temperature = try(each.value.temperature, try(each.value.topP, null) == null ? local.agent_defaults.temperature : null)
        top_p       = try(each.value.topP, null)
      }
    }
    # Another provider's model: its own id, the key from AgentCore Identity, and only the
    # sampling settings the entry gives (their defaults are the provider's).
    dynamic "openai_model_config" {
      for_each = local.harness_spec[each.key].provider == "openai" ? [1] : []
      content {
        model_id    = each.value.model
        api_key_arn = try(local.harness_key_arn[local.harness_spec[each.key].api_key], local.harness_spec[each.key].api_key)
        max_tokens  = try(each.value.maxTokens, local.agent_defaults.maxTokens)
        temperature = try(each.value.temperature, null)
        top_p       = try(each.value.topP, null)
      }
    }
    dynamic "gemini_model_config" {
      for_each = local.harness_spec[each.key].provider == "gemini" ? [1] : []
      content {
        model_id    = each.value.model
        api_key_arn = try(local.harness_key_arn[local.harness_spec[each.key].api_key], local.harness_spec[each.key].api_key)
        max_tokens  = try(each.value.maxTokens, local.agent_defaults.maxTokens)
        temperature = try(each.value.temperature, null)
        top_p       = try(each.value.topP, null)
      }
    }
    dynamic "litellm_model_config" {
      for_each = local.harness_spec[each.key].provider == "litellm" ? [1] : []
      content {
        model_id    = each.value.model
        api_key_arn = local.harness_spec[each.key].api_key == "" ? null : try(local.harness_key_arn[local.harness_spec[each.key].api_key], local.harness_spec[each.key].api_key)
        api_base    = local.harness_spec[each.key].api_base == "" ? null : local.harness_spec[each.key].api_base
        max_tokens  = try(each.value.maxTokens, local.agent_defaults.maxTokens)
        temperature = try(each.value.temperature, null)
        top_p       = try(each.value.topP, null)
      }
    }
  }
  memory {
    dynamic "disabled" {
      for_each = local.harness_spec[each.key].memory == "" ? [1] : []
      content {}
    }
    # One of the build's memories; the harness derives retrieval from its strategies.
    dynamic "agentcore_memory_configuration" {
      for_each = local.harness_spec[each.key].memory != "" ? [local.harness_spec[each.key].memory] : []
      content {
        arn = awscc_bedrockagentcore_memory.named[agentcore_memory_configuration.value].memory_arn
      }
    }
  }
  allowed_tools   = local.harness_tools[each.key]
  max_iterations  = try(each.value.harness.maxIterations, null)
  timeout_seconds = try(each.value.harness.timeoutSeconds, null)

  # Tools, in the order harness.ts gives them: the Gateway, MCP servers, browser, code interpreter.
  dynamic "tool" {
    for_each = contains(local.harness_gateway_ids, each.key) ? [1] : []
    content {
      type = "agentcore_gateway"
      name = "gateway"
      config {
        agentcore_gateway {
          gateway_arn = aws_bedrockagentcore_gateway.mcp[0].gateway_arn
          outbound_auth {
            oauth {
              provider_arn = aws_bedrockagentcore_oauth2_credential_provider.harness_gateway[each.key].credential_provider_arn
              scopes       = [local.gateway_audience]
              grant_type   = "CLIENT_CREDENTIALS"
            }
          }
        }
      }
    }
  }
  dynamic "tool" {
    for_each = local.harness_spec[each.key].mcp
    content {
      type = "remote_mcp"
      name = tool.value.name
      config {
        remote_mcp {
          url = tool.value.url
        }
      }
    }
  }
  dynamic "tool" {
    for_each = local.harness_spec[each.key].browser ? [local.harness_spec[each.key].browser_arn] : []
    content {
      type = "agentcore_browser"
      name = "browser"
      config {
        agentcore_browser {
          browser_arn = tool.value == "" ? null : tool.value
        }
      }
    }
  }
  dynamic "tool" {
    for_each = local.harness_spec[each.key].code ? [local.harness_spec[each.key].code_arn] : []
    content {
      type = "agentcore_code_interpreter"
      name = "code_interpreter"
      config {
        agentcore_code_interpreter {
          code_interpreter_arn = tool.value == "" ? null : tool.value
        }
      }
    }
  }

  dynamic "skill" {
    for_each = local.harness_spec[each.key].skills
    content {
      dynamic "aws_skills" {
        for_each = skill.value.kind == "aws" ? [1] : []
        content {
          # No paths: every skill in the AWS catalog.
          paths = length(skill.value.paths) > 0 ? skill.value.paths : null
        }
      }
      dynamic "git" {
        for_each = skill.value.kind == "git" ? [1] : []
        content {
          url  = skill.value.url
          path = skill.value.path == "" ? null : skill.value.path
          dynamic "auth" {
            for_each = skill.value.credential == "" ? [] : [skill.value.credential]
            content {
              credential_arn = try(local.harness_key_arn[auth.value], auth.value)
            }
          }
        }
      }
      dynamic "s3" {
        for_each = skill.value.kind == "s3" ? [1] : []
        content {
          uri = skill.value.uri
        }
      }
    }
  }

  dynamic "environment" {
    for_each = length(local.harness_spec[each.key].mounts) > 0 || length(local.harness_spec[each.key].subnets) > 0 ? [1] : []
    content {
      agentcore_runtime_environment {
        dynamic "network_configuration" {
          for_each = length(local.harness_spec[each.key].subnets) > 0 ? [1] : []
          content {
            network_mode = "VPC"
            network_mode_config {
              security_groups = local.harness_spec[each.key].groups
              subnets         = local.harness_spec[each.key].subnets
            }
          }
        }
        dynamic "filesystem_configuration" {
          for_each = local.harness_spec[each.key].mounts
          content {
            dynamic "session_storage" {
              for_each = filesystem_configuration.value.kind == "session" ? [1] : []
              content {
                mount_path = filesystem_configuration.value.path
              }
            }
            dynamic "efs_access_point" {
              for_each = filesystem_configuration.value.kind == "efs" ? [1] : []
              content {
                access_point_arn = filesystem_configuration.value.arn
                mount_path       = filesystem_configuration.value.path
              }
            }
            dynamic "s3_files_access_point" {
              for_each = filesystem_configuration.value.kind == "s3Files" ? [1] : []
              content {
                access_point_arn = filesystem_configuration.value.arn
                mount_path       = filesystem_configuration.value.path
              }
            }
          }
        }
      }
    }
  }
  dynamic "environment_artifact" {
    for_each = local.harness_spec[each.key].container != "" ? [1] : []
    content {
      container_configuration {
        container_uri = local.harness_spec[each.key].container
      }
    }
  }

  depends_on = [time_sleep.harness_iam_propagation]
}

# harness.endpoint: a named endpoint the orchestrator calls. Created on the latest version
# (no target_version: devguide/harness-versioning.html), and made again whenever the
# harness changes, because this provider exposes no harness version to point it at: so it
# moves to each version a deploy makes. (The CDK path updates it in place to the version
# CloudFormation reports.) While it is re-created, a step calling it fails.
resource "awscc_bedrockagentcore_harness_endpoint" "agent" {
  for_each      = { for id, s in local.harness_spec : id => s.endpoint if s.endpoint != "" }
  harness_id    = aws_bedrockagentcore_harness.agent[each.key].harness_id
  endpoint_name = each.value
  description   = "Follows each version this deployment makes"
  lifecycle {
    replace_triggered_by = [aws_bedrockagentcore_harness.agent[each.key]]
  }
}

# The orchestrator invokes them: InvokeHarness needs InvokeHarness AND InvokeAgentRuntime
# on the harness ARN, and the endpoint ARN too for a named endpoint
# (devguide/harness-security.html, "Required IAM permissions for callers").
resource "aws_iam_role_policy" "runtime_harness" {
  count = length(local.harness_all) > 0 ? 1 : 0
  name  = "AgentCoreRuntimeHarness-${var.agent_name}"
  role  = aws_iam_role.runtime[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "InvokeHarnessAgents"
      Effect   = "Allow"
      Action   = ["bedrock-agentcore:InvokeHarness", "bedrock-agentcore:InvokeAgentRuntime"]
      Resource = local.harness_invoke_arns
    }]
  })
  depends_on = [terraform_data.harness_agents]
}
