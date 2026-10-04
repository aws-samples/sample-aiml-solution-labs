# --- Per-agent (dedicated) AgentCore Runtimes -----------------------------
# Any agent marked `"runtime": "dedicated"` in workflow.json gets its OWN
# AgentCore Runtime here (same image; AGENT_ID selects which agent it hosts).
# Agents left as "main" run in-process in the orchestrator runtime. This is
# purely config-driven: add/remove a dedicated agent by editing workflow.json.

locals {
  workflow_def = jsondecode(file("${path.module}/../app/workflow.json"))
  dedicated_agents = {
    for id, a in local.workflow_def.agents : id => a
    if local.workflow_plane && lookup(a, "runtime", local.agent_defaults.runtime) == "dedicated"
  }

  subagent_runtime_arns = [for r in awscc_bedrockagentcore_runtime.subagent : r.agent_runtime_arn]
  # Non-empty resource list even when there are no dedicated agents (an empty
  # IAM Resource is invalid).
  invoke_runtime_resources = length(local.subagent_runtime_arns) > 0 ? concat(
    local.subagent_runtime_arns,
    [for a in local.subagent_runtime_arns : "${a}/*"],
  ) : ["arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:runtime/none"]
}

# ONE EXECUTION ROLE PER DEDICATED AGENT, scoped to what that agent's workflow.json
# entry actually asks for. This was a single shared role, which meant an agent that
# enables nothing carried the union of every other agent's permissions — in the shipped
# workflow, `knowledge_research` does not use guardrails and no dedicated agent uses
# long-term memory, yet every one of them could call ApplyGuardrail and read the
# semantic memory store.
#
# Nothing here is for the customer to write. The grants are DERIVED from the same
# config that switches the feature on, so enabling memory for an agent grants that
# agent memory access and enabling it for no one grants it to no one.
#
# Still narrower than the orchestrator's role in every case: no Memory checkpointer, no
# DynamoDB progress store, no Evaluations. And tool access needs no IAM at all — a
# Gateway call carries the IdP's client-credentials token from env (Cognito or Auth0).
locals {
  # Per-agent feature flags, read once so the role and its policy agree by construction.
  subagent_features = {
    for id, a in local.dedicated_agents : id => {
      guardrails = try(a.agentcore.guardrails.input, false) || try(a.agentcore.guardrails.output, false)
      memory     = length(try(a.agentcore.memory.longTerm, [])) > 0 || try(a.agentcore.memory.use, "") != ""
      # Identities it calls an API with directly (identities.tf).
      identity = length([for n in try(a.agentcore.identity.outbound, []) : n if contains(keys(local.identity_defs), n)]) > 0
      # Workload identity is the outbound-token plumbing an agent uses to reach the
      # Gateway, so it follows from having a tool at all rather than from a feature flag.
      # `tool` is one key or a list of them.
      tool = length([for t in flatten([try(a.tool, [])]) : t if t != ""]) > 0
    }
  }
  # IAM role names cap at 64 characters and this one carries the agent id, which the
  # shared name did not. Checked rather than truncated: a silently shortened name can
  # collide with another agent's, and two runtimes sharing a role is the thing this
  # change exists to stop.
  subagent_role_names = {
    for id in keys(local.dedicated_agents) : id => "AgentCoreSubagent-${var.agent_name}-${id}"
  }
}

resource "aws_iam_role" "subagent" {
  for_each = local.dedicated_agents
  name     = local.subagent_role_names[each.key]

  lifecycle {
    precondition {
      condition     = length(local.subagent_role_names[each.key]) <= 64
      error_message = "The execution role name for dedicated agent \"${each.key}\" would be \"${local.subagent_role_names[each.key]}\" (${length(local.subagent_role_names[each.key])} chars), over IAM's 64-character limit. Shorten var.agent_name or the agent id."
    }
  }

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

resource "aws_iam_role_policy" "subagent" {
  for_each = local.dedicated_agents
  name     = "AgentCoreSubagentPolicy-${var.agent_name}-${each.key}"
  role     = aws_iam_role.subagent[each.key].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Sid    = "ECRImageAccess"
        Effect = "Allow"
        # The full documented pull set — see the same statement in main.tf for why
        # BatchCheckLayerAvailability is not optional.
        Action = ["ecr:BatchCheckLayerAvailability", "ecr:BatchGetImage",
        "ecr:GetDownloadUrlForLayer"]
        Resource = [aws_ecr_repository.orchestrator[0].arn]
      },
      {
        Sid      = "ECRTokenAccess"
        Effect   = "Allow"
        Action   = ["ecr:GetAuthorizationToken"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams", "logs:DescribeLogGroups"]
        Resource = ["arn:aws:logs:${var.region}:${local.account_id}:log-group:/aws/bedrock-agentcore/runtimes/*"]
      },
      {
        # Required for AgentCore "unified" telemetry span delivery to the agent's
        # own log group (see main.tf for details).
        Sid      = "AgentCoreUnifiedSpanDelivery"
        Effect   = "Allow"
        Action   = ["logs:PutResourcePolicy"]
        Resource = ["*"]
      },
      {
        Effect   = "Allow"
        Action   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords", "xray:GetSamplingRules", "xray:GetSamplingTargets"]
        Resource = ["*"]
      },
      {
        Effect    = "Allow"
        Action    = "cloudwatch:PutMetricData"
        Resource  = "*"
        Condition = { StringEquals = { "cloudwatch:namespace" = "bedrock-agentcore" } }
      },
      {
        # The cost telemetry prices each model call from the AWS Price List
        # (app/features/observability/pricelist.py). Read-only public price data; the
        # API has no resource-level permissions. Mirrors priceListRead in the CDK stack.
        Sid      = "PriceListRead"
        Effect   = "Allow"
        Action   = ["pricing:GetProducts"]
        Resource = "*"
      },
      {
        Sid    = "BedrockModelInvocation"
        Effect = "Allow"
        Action = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream", "bedrock:CountTokens"]
        # Inference profiles, NOT the account-wide "arn:aws:bedrock:<region>:<acct>:*"
        # this used to carry. That wildcard also covered custom models, provisioned
        # throughput, agents, guardrails and prompts, none of which a sub-agent calls —
        # and it was broader than both the CDK sub-agent role and Terraform's own
        # orchestrator role, so it was a drift rather than a decision.
        Resource = [
          "arn:aws:bedrock:*::foundation-model/*",
          "arn:aws:bedrock:${var.region}:${local.account_id}:inference-profile/*",
          "arn:aws:bedrock:${var.region}:${local.account_id}:application-inference-profile/*"
        ]
      },
      ],
      # --- Below: granted only to agents whose own config asks for it ---------
      # Outbound-token plumbing for reaching the Gateway. Follows from HAVING a tool,
      # because an agent with none never calls one.
      !local.subagent_features[each.key].tool ? [] : [
        {
          Sid    = "AgentCoreWorkloadIdentity"
          Effect = "Allow"
          Action = ["bedrock-agentcore:GetWorkloadAccessToken", "bedrock-agentcore:GetWorkloadAccessTokenForJWT", "bedrock-agentcore:GetWorkloadAccessTokenForUserId"]
          Resource = [
            "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:workload-identity-directory/default",
            "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:workload-identity-directory/default/workload-identity/${var.agent_name}*"
          ]
        }
      ],
      # Long-term memory: only for an agent that declares agentcore.memory.longTerm.
      # No dedicated agent in the shipped workflow does, so this statement now appears
      # on no sub-agent role at all — it was previously on every one of them.
      !local.subagent_features[each.key].memory ? [] : [
        {
          Sid    = "AgentCoreLongTermMemory"
          Effect = "Allow"
          Action = [
            "bedrock-agentcore:CreateEvent",
            "bedrock-agentcore:RetrieveMemories",
            "bedrock-agentcore:RetrieveMemoryRecords",
            "bedrock-agentcore:ListMemoryRecords",
            "bedrock-agentcore:GetMemoryRecord"
          ]
          Resource = concat([
            awscc_bedrockagentcore_memory.semantic[0].memory_arn,
            "${awscc_bedrockagentcore_memory.semantic[0].memory_arn}/*"
          ], local.named_memory_arns)
        }
      ],
      !local.subagent_features[each.key].identity ? [] : [
        {
          Sid    = "AgentCoreIdentityCredentials"
          Effect = "Allow"
          Action = ["bedrock-agentcore:GetResourceOauth2Token", "bedrock-agentcore:GetResourceApiKey",
          "bedrock-agentcore:GetWorkloadAccessToken"]
          Resource = [
            "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:token-vault/default",
            "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:token-vault/default/*",
            "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:workload-identity-directory/default",
            "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:workload-identity-directory/default/workload-identity/${var.agent_name}-*"
          ]
        },
        {
          Sid      = "AgentCoreIdentitySecrets"
          Effect   = "Allow"
          Action   = ["secretsmanager:GetSecretValue"]
          Resource = ["arn:aws:secretsmanager:${var.region}:${local.account_id}:secret:bedrock-agentcore-identity!*"]
        }
      ],
      # Content safety: only for an agent with agentcore.guardrails.input or .output.
      # Without it the GUARDRAIL_ID env resolves and ApplyGuardrail is denied — which is
      # the correct outcome for an agent that never calls it.
      !local.subagent_features[each.key].guardrails ? [] : [
        {
          Sid      = "BedrockGuardrails"
          Effect   = "Allow"
          Action   = ["bedrock:ApplyGuardrail"]
          Resource = local.guardrail_arns
        }
    ])
  })
}

# Waits on EVERY per-agent policy, not one. With `for_each` roles a single-resource
# depends_on would let a runtime validate against a role whose policy had not propagated,
# which fails the create with an unhelpful "role cannot be assumed".
resource "time_sleep" "subagent_iam_propagation" {
  count           = length(local.dedicated_agents) > 0 ? 1 : 0
  depends_on      = [aws_iam_role_policy.subagent, aws_iam_role_policy.subagent_secret, aws_secretsmanager_secret_version.subagent]
  create_duration = "25s"
}

resource "awscc_bedrockagentcore_runtime" "subagent" {
  for_each = local.dedicated_agents

  # The agent id is the module name — one convention, no override (see
  # app/orchestrator/registry.py). app/features/evaluations/service.py finds this
  # runtime's traces by matching the "_<agent id>" suffix, so the two must agree.
  agent_runtime_name = "${var.agent_name}_${each.key}"
  description        = "Dedicated runtime for agent ${each.key} (${each.value.name})"
  role_arn           = aws_iam_role.subagent[each.key].arn

  agent_runtime_artifact = {
    container_configuration = {
      container_uri = local.image_uri
    }
  }

  network_configuration = {
    network_mode = "PUBLIC"
  }

  environment_variables = {
    AWS_REGION         = var.region
    BEDROCK_MODEL_ID   = local.model_id
    AGENT_ID           = each.key
    TELEMETRY_TABLE    = aws_dynamodb_table.telemetry[0].name
    SEMANTIC_MEMORY_ID = awscc_bedrockagentcore_memory.semantic[0].memory_id
    CUSTOM_EVALUATORS  = local.custom_evaluator_ids

    # --- AgentCore GenAI Observability ---
    # Master switch; AgentCore injects the ADOT config + managed OTLP endpoint.
    AGENT_OBSERVABILITY_ENABLED = "true"
    # Always sample (ignore the inherited sampled=0 from the cross-runtime parent
    # context) so spans are exported. See the note in main.tf.
    OTEL_TRACES_SAMPLER = "always_on"
    # Drop ADOT's duplicate, token-less LangChain LLM span so tokens show. See main.tf.
    OTEL_PYTHON_DISABLED_INSTRUMENTATIONS = "aws_langchain"

    # Default guardrail, same as the orchestrator runtime (main.tf), so a
    # dedicated agent that enables guardrails in workflow.json enforces the SAME
    # account-provisioned guardrail. Injected per account; nothing hardcoded.
    GUARDRAIL_ID       = local.guardrail_id
    GUARDRAIL_VERSION  = "DRAFT"
    GUARDRAILS         = local.guardrails_env
    MEMORIES           = local.memories_env
    IDENTITY_PROVIDERS = local.identity_providers_env
    WORKLOAD_IDENTITY  = length(local.agent_identity_names) > 0 ? local.workload_identity_name : ""

    # Gateway-backed MCP access (same as the orchestrator) for agents that use it.
    GATEWAY_URL       = local.gateway_enabled ? aws_bedrockagentcore_gateway.mcp[0].gateway_url : ""
    GATEWAY_TOKEN_URL = local.gateway_enabled ? local.gateway_token_url : ""
    # This agent's own client when orchestrator.gatewayIdentity is "perAgent".
    GATEWAY_CLIENT_ID = contains(local.agent_client_ids, each.key) ? aws_cognito_user_pool_client.agent[each.key].id : local.gateway_client_id
    # Its Gateway client secret: in Secrets Manager (secrets.tf), not here.
    RUNTIME_SECRET_ARN = local.gateway_enabled ? aws_secretsmanager_secret.subagent[each.key].arn : ""
    # Which client-credentials request shape to build ("cognito" | "auth0").
    GATEWAY_AUTH_FLOW = local.gateway_enabled ? local.gateway_auth_flow : ""
    GATEWAY_AUDIENCE  = local.gateway_audience
    # Cedar policy mode (display-only, so the observability UI can label decisions).
    GATEWAY_POLICY_MODE = local.policy_enabled ? local.policy_mode : ""
    # The `tools` block from workflow.json: how each tool is called (its type,
    # and per-type call options like the KB corpora or WebSearch maxResults).
    TOOLS_JSON = local.tools_env
    # Where image agents store what they render (images.tf); "" when none draws.
    ASSETS_BUCKET = local.assets_bucket
  }
  depends_on = [null_resource.build_push, time_sleep.subagent_iam_propagation]
}
