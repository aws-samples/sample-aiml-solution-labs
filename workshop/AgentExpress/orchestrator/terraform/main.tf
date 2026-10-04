data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  ecr_repo   = "agentcore-${var.agent_name}"

  # Rebuild the image whenever the Dockerfile, deps, or any app/ file changes.
  # Exclude Python bytecode caches so the image tag is deterministic regardless of
  # locally-generated __pycache__/*.pyc (which are not shipped in the image anyway).
  app_files = [
    for f in fileset("${path.module}/../app", "**") :
    f if !can(regex("(^|/)__pycache__/", f)) && !endswith(f, ".pyc")
  ]
  source_hash = substr(sha1(join("", concat(
    [filesha1("${path.module}/../Dockerfile"), filesha1("${path.module}/../requirements.txt"),
    filesha1("${path.module}/../requirements.lock")],
    [for f in sort(local.app_files) : filesha1("${path.module}/../app/${f}")]
  ))), 0, 12)
  image_uri = local.workflow_plane ? "${aws_ecr_repository.orchestrator[0].repository_url}:${local.source_hash}" : ""
}

# --- AgentCore Memory (durable checkpointer backend) ----------------------

resource "awscc_bedrockagentcore_memory" "orchestrator" {
  count                 = local.workflow_plane ? 1 : 0
  name                  = "${var.agent_name}_memory"
  event_expiry_duration = var.memory_event_expiry_days
  description           = "Checkpoint/state store for the multi-agent orchestrator"
}

# --- AgentCore Memory: long-term semantic (cross-session, per-subject) ------
# SEPARATE from the checkpointer above. This one has LONG-TERM strategies:
# AgentCore asynchronously extracts durable insights from what agents store, and
# groups them under a namespace template. The app sets
# actorId = "<agentId>-<subjectSlug>", so each agent's insights are isolated per
# subject and recalled across runs. Adding a new agent or subject needs NO
# Terraform change — the namespace is derived from actorId at run time.
resource "awscc_bedrockagentcore_memory" "semantic" {
  count                 = local.workflow_plane ? 1 : 0
  name                  = "${var.agent_name}_semantic"
  event_expiry_duration = var.memory_event_expiry_days
  description           = "Long-term semantic memory (per-agent, per-subject insights)"

  # Two long-term strategies, so more than one is exercised:
  #   * SEMANTIC — extracts discrete facts/insights  -> namespace insights/{actorId}
  #   * SUMMARY  — maintains a running summary        -> namespace summary/{actorId}/{sessionId}
  # An agent's workflow.json memory.longTerm list (["semantic","summary"]) selects
  # which namespaces it reads; both extract from the same stored turns.
  # userPreference, episodic and each agent's custom strategy are added only when an
  # agent names them (local.memory_strategies in evaluations.tf): each strategy is a
  # model call per stored turn. Mirrors memoryStrategiesFor in cdk/lib/orchestrator-stack.ts.
  memory_strategies = local.memory_strategies
  # A custom strategy's extraction runs a model on your behalf.
  memory_execution_role_arn = length(local.memory_customs) > 0 ? aws_iam_role.memory[0].arn : null
}

# --- Container registry ---------------------------------------------------

resource "aws_ecr_repository" "orchestrator" {
  count = local.workflow_plane ? 1 : 0
  name  = local.ecr_repo
  # Immutable: a tag is the hash of the source it was built from (local.source_hash), so
  # a deployed runtime always runs the code its tag names. build_push skips a tag that
  # is already there, which is what makes a retried apply safe.
  image_tag_mutability = "IMMUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

# --- Build & push the ARM64 image -----------------------------------------

resource "null_resource" "build_push" {
  count = local.workflow_plane ? 1 : 0
  triggers = {
    image_uri = local.image_uri
  }

  provisioner "local-exec" {
    working_dir = path.module
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      set -euo pipefail
      REG="${local.account_id}.dkr.ecr.${var.region}.amazonaws.com"
      aws ecr get-login-password --region ${var.region} \
        | ${var.container_engine} login --username AWS --password-stdin "$REG"
      if aws ecr describe-images --region ${var.region} --repository-name "${local.ecr_repo}" \
          --image-ids imageTag="${local.source_hash}" >/dev/null 2>&1; then
        echo "image ${local.source_hash} is already in ${local.ecr_repo}; not rebuilding"
        exit 0
      fi
      ${var.container_engine} build --platform linux/arm64 \
        -t "${local.image_uri}" -f ../Dockerfile ..
      ${var.container_engine} push "${local.image_uri}"
    EOT
  }

  # No depends_on needed: triggers.image_uri -> local.image_uri -> the repository's
  # own repository_url, which is an implicit dependency.
}

# --- IAM execution role ----------------------------------------------------

resource "aws_iam_role" "runtime" {
  count = local.workflow_plane ? 1 : 0
  name  = "AgentCoreRuntime-${var.agent_name}"

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

resource "aws_iam_role_policy" "runtime" {
  count = local.workflow_plane ? 1 : 0
  name  = "AgentCoreRuntimeExecutionPolicy-${var.agent_name}"
  role  = aws_iam_role.runtime[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ECRImageAccess"
        Effect = "Allow"
        # The full documented pull set. BatchCheckLayerAvailability is the one that
        # looks droppable and is not: a pull that has to verify layers rather than
        # take them from cache calls it, so omitting it buys nothing and risks a
        # cold start that fails only sometimes. Matches the CDK path's grantPull().
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
        # Required for AgentCore "unified" telemetry: AgentCore uses this to add a
        # CloudWatch Logs resource policy so X-Ray can deliver spans to the agent's
        # own log group. Without it, unified spans never reach CloudWatch. (Docs:
        # observability-configure — "Span destination for agents hosted in
        # AgentCore runtime".) PutResourcePolicy is account-scoped -> Resource "*".
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
        # AgentCore Evaluations: the runtime scores an agent's run (LLM-as-judge
        # over its OTEL spans / persisted prompt I/O). Evaluate uses the built-in
        # evaluators (public ARNs); the Logs Insights query downloads the session's
        # spans from the runtime log group. GetQueryResults/StopQuery are not
        # resource-scopable, so this statement uses Resource "*".
        Sid    = "AgentCoreEvaluations"
        Effect = "Allow"
        Action = [
          "bedrock-agentcore:Evaluate",
          "logs:DescribeLogGroups", "logs:StartQuery", "logs:GetQueryResults", "logs:StopQuery"
        ]
        Resource = ["*"]
      },
      {
        Effect    = "Allow"
        Action    = "cloudwatch:PutMetricData"
        Resource  = "*"
        Condition = { StringEquals = { "cloudwatch:namespace" = "bedrock-agentcore" } }
      },
      {
        Sid    = "AgentCoreMemoryDataPlane"
        Effect = "Allow"
        Action = [
          "bedrock-agentcore:CreateEvent",
          "bedrock-agentcore:ListEvents",
          "bedrock-agentcore:GetEvent",
          "bedrock-agentcore:ListSessions",
          "bedrock-agentcore:RetrieveMemories",
          # Long-term semantic recall/store (the SDK's search_long_term_memories
          # calls RetrieveMemoryRecords; list/get for inspection).
          "bedrock-agentcore:RetrieveMemoryRecords",
          "bedrock-agentcore:ListMemoryRecords",
          "bedrock-agentcore:GetMemoryRecord"
        ]
        Resource = concat([
          awscc_bedrockagentcore_memory.orchestrator[0].memory_arn,
          "${awscc_bedrockagentcore_memory.orchestrator[0].memory_arn}/*",
          awscc_bedrockagentcore_memory.semantic[0].memory_arn,
          "${awscc_bedrockagentcore_memory.semantic[0].memory_arn}/*"
        ], local.named_memory_arns)
      },
      {
        # Bedrock Guardrails: apply content safety to agent input/output — the
        # deployment's own and every named one (workflow.json `guardrails`).
        Sid      = "BedrockGuardrails"
        Effect   = "Allow"
        Action   = ["bedrock:ApplyGuardrail"]
        Resource = local.guardrail_arns
      },
      {
        # Credentials for an API an agent calls directly (agentcore.identity.outbound ->
        # identities.tf): exchanged through the workload identity, from the token vault.
        Sid    = "AgentCoreIdentityCredentials"
        Effect = "Allow"
        Action = ["bedrock-agentcore:GetResourceOauth2Token", "bedrock-agentcore:GetResourceApiKey"]
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
      },
      {
        Sid    = "AgentCoreWorkloadIdentity"
        Effect = "Allow"
        Action = ["bedrock-agentcore:GetWorkloadAccessToken", "bedrock-agentcore:GetWorkloadAccessTokenForJWT", "bedrock-agentcore:GetWorkloadAccessTokenForUserId"]
        Resource = [
          "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:workload-identity-directory/default",
          "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:workload-identity-directory/default/workload-identity/${var.agent_name}-*"
        ]
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
        # Inference profiles, not account-wide bedrock:* — that also covered custom
        # models, provisioned throughput, agents, guardrails and prompts, none of which
        # the runtime invokes. bff.tf already used this narrower form.
        Resource = [
          "arn:aws:bedrock:*::foundation-model/*",
          "arn:aws:bedrock:${var.region}:${local.account_id}:inference-profile/*",
          "arn:aws:bedrock:${var.region}:${local.account_id}:application-inference-profile/*"
        ]
      },
      {
        Sid      = "ProgressStoreWrite"
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:GetItem"]
        Resource = [aws_dynamodb_table.status[0].arn, aws_dynamodb_table.events[0].arn]
      },
      {
        # The activity log: the runtime records how each run ended (app/common/audit.py);
        # the BFF logged it starting. Mirrors RunOutcomesToActivityLog in the CDK stack.
        Sid      = "RunOutcomesToActivityLog"
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem"]
        Resource = [local.audit_table_arn]
      },
      {
        # Scan is needed on the STATUS table only: Insights flags whether each
        # analyzed session still exists, so the UI can disable dead links. The
        # comment used to say "status only" while the grant covered both tables.
        Sid      = "ProgressStoreScanStatus"
        Effect   = "Allow"
        Action   = ["dynamodb:Scan"]
        Resource = [aws_dynamodb_table.status[0].arn]
      },
      {
        # Invoke the dedicated per-agent runtimes (runtime: "dedicated").
        Sid      = "InvokeDedicatedAgentRuntimes"
        Effect   = "Allow"
        Action   = ["bedrock-agentcore:InvokeAgentRuntime"]
        Resource = local.invoke_runtime_resources
      },
    ]
  })
}

# Give the execution role a moment to propagate before the runtime validates it.
resource "time_sleep" "iam_propagation" {
  count           = local.workflow_plane ? 1 : 0
  depends_on      = [aws_iam_role_policy.runtime, aws_iam_role_policy.runtime_secret, aws_secretsmanager_secret_version.runtime]
  create_duration = "25s"
}

# --- AgentCore Runtime -----------------------------------------------------

resource "awscc_bedrockagentcore_runtime" "orchestrator" {
  count              = local.workflow_plane ? 1 : 0
  agent_runtime_name = var.agent_name
  description        = "Multi-agent LangGraph orchestrator (Terraform-managed)"
  role_arn           = aws_iam_role.runtime[0].arn

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
    MEMORY_ID          = awscc_bedrockagentcore_memory.orchestrator[0].memory_id
    SEMANTIC_MEMORY_ID = awscc_bedrockagentcore_memory.semantic[0].memory_id
    CUSTOM_EVALUATORS  = local.custom_evaluator_ids
    STATUS_TABLE       = aws_dynamodb_table.status[0].name
    EVENTS_TABLE       = aws_dynamodb_table.events[0].name
    TELEMETRY_TABLE    = aws_dynamodb_table.telemetry[0].name
    AUDIT_TABLE        = local.audit_table_name

    # Default guardrail for agents that enable guardrails in workflow.json but
    # don't specify their own guardrailId. DRAFT tracks the latest config.
    GUARDRAIL_ID      = local.guardrail_id
    GUARDRAIL_VERSION = "DRAFT"
    # The named ones (workflow.json `guardrails`), for agentcore.guardrails.use.
    GUARDRAILS = local.guardrails_env
    # Named memories (workflow.json `memories`), for agentcore.memory.use.
    MEMORIES = local.memories_env
    # Identities agents reach directly (identities.tf), and the workload identity the
    # runtime exchanges for their credentials.
    IDENTITY_PROVIDERS = local.identity_providers_env
    WORKLOAD_IDENTITY  = length(local.agent_identity_names) > 0 ? local.workload_identity_name : ""

    # AgentCore Optimization: the cross-run Insights findings store.
    INSIGHTS_TABLE = aws_dynamodb_table.insights[0].name

    # AgentCore Evaluations reads this app's OTEL spans from the runtime log
    # group(s). We inject the stable name PREFIX; the exact id-suffixed group
    # name is discovered at query time (it can't be referenced here without a
    # self-dependency cycle on the runtime resource).
    SPAN_LOG_GROUP_PREFIX = "/aws/bedrock-agentcore/runtimes/${var.agent_name}-"

    # --- AgentCore GenAI Observability ---
    # Master switch; AgentCore injects the ADOT config + managed OTLP endpoint.
    AGENT_OBSERVABILITY_ENABLED = "true"
    # Always sample. The UI path (API Gateway -> BFF Lambda -> InvokeAgentRuntime)
    # propagates an X-Ray trace context with sampled=0; the default parent-based
    # sampler then marks every span not-recording and NOTHING is exported. Direct
    # SDK invokes have no parent, so they sampled fine — which is why UI runs
    # showed no traces while direct tests did. always_on ignores the inherited
    # decision.
    OTEL_TRACES_SAMPLER = "always_on"
    # Disable ADOT's bundled LangChain instrumentation (entry point "aws_langchain").
    # It emits a SECOND "chat <model>" span per LLM call (aws.genai.span_kind=LLM)
    # that carries NO token usage, while the botocore bedrock-runtime span carries
    # the real gen_ai.usage.* counts. The GenAI Observability dashboard reads the
    # LangChain framework span for its token metric, so tokens showed as 0.
    # Disabling it leaves exactly one LLM span per call (botocore, with tokens).
    # Trade-off: also drops the LangChain-only gen_ai.input/output.messages and
    # langgraph.node/step attributes (the prompt/response content view) —
    # app/features/observability captures that itself. Reversible.
    OTEL_PYTHON_DISABLED_INSTRUMENTATIONS = "aws_langchain"
    # Where image agents store what they render (images.tf); "" when none draws.
    ASSETS_BUCKET = local.assets_bucket
    # Map of agent_id -> its dedicated runtime ARN, consumed by
    # AgentCoreRuntimeAgent to invoke a "dedicated" agent cross-runtime.
    AGENT_RUNTIME_ARNS = jsonencode({
      for id, r in awscc_bedrockagentcore_runtime.subagent : id => r.agent_runtime_arn
    })
    # The Gateway client secret(s) and the A2A bearer tokens: in Secrets Manager, not
    # here (secrets.tf, app/common/runtime_secret.py).
    RUNTIME_SECRET_ARN = aws_secretsmanager_secret.runtime[0].arn
    # Map of agent_id -> endpoint, for a `runtime = "a2a"` agent whose URL only exists
    # AFTER a deploy (the stand-in's Function URL). Injected rather than committed, for
    # the same reason AGENT_RUNTIME_ARNS is. Empty when no agent uses `source`.
    A2A_ENDPOINTS = jsonencode(local.a2a_endpoints)

    # Gateway-backed MCP access (agent -> Gateway via IdP client-credentials).
    # Empty when local.gateway_enabled is false, in which case an agent with a `tool`
    # fails loudly rather than inventing an answer (app/common/errors.py).
    GATEWAY_URL       = local.gateway_enabled ? aws_bedrockagentcore_gateway.mcp[0].gateway_url : ""
    GATEWAY_TOKEN_URL = local.gateway_enabled ? local.gateway_token_url : ""
    GATEWAY_CLIENT_ID = local.gateway_client_id
    # Which client-credentials request shape to build ("cognito" | "auth0").
    GATEWAY_AUTH_FLOW = local.gateway_enabled ? local.gateway_auth_flow : ""
    # The OAuth2 `scope` the runtime requests (Cognito's client-credentials
    # equivalent of an audience) — see app/features/gateway/client.py.
    GATEWAY_AUDIENCE = local.gateway_audience
    # Cedar policy mode in effect at the Gateway (LOG_ONLY|ENFORCE), so the app
    # can label policy decisions in the observability UI. Enforcement itself is
    # server-side at the Gateway; this is display-only.
    GATEWAY_POLICY_MODE = local.policy_enabled ? local.policy_mode : ""
    # The `tools` block from workflow.json: how each tool is called (its type,
    # and per-type call options like the KB corpora or WebSearch maxResults).
    # Lets an agent invoke its configured tool without hardcoding anything.
    TOOLS_JSON = local.tools_env
  }

  depends_on = [null_resource.build_push, time_sleep.iam_propagation]
}
