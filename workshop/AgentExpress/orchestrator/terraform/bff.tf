# --- BFF Lambda + API Gateway (HTTP API) ----------------------------------

# The BFF's deployment package: bff/*.py PLUS app/workflow.json, so the API reads
# the same file the customer edits and the same file the runtime reads.
#
# It used to be source_dir = ../bff, with a projection of the workflow built here in
# HCL (and a second time in TypeScript for the CDK path) and shipped in a
# WORKFLOW_JSON environment variable. That was a CEILING: Lambda caps the whole
# environment at 4 KB and the quota cannot be raised, so this file carried a
# 3400-byte precondition and the shipped ten-agent workflow measured 3153 bytes —
# about eleven agents before a customer's deploy failed with "shorten your agent
# names". bff/workflow.py now does the projection, once, in Python.
#
# `source` blocks rather than `source_dir` because the two inputs live in different
# directories. bff/ is pure UTF-8 Python, which is why `file()` is safe here; a
# binary asset would need `filebase64` and a different archive strategy.
#
# `**/*.py` rather than `*.py` so a future subpackage under bff/ is included, and
# rather than `**` so __pycache__ is not: source_dir used to ship every stale .pyc
# in the working tree — including ones built by a different Python minor version —
# into the deployment package.
data "archive_file" "bff" {
  type        = "zip"
  output_path = "${path.module}/.build/bff.zip"

  dynamic "source" {
    for_each = fileset("${path.module}/../bff", "**/*.py")
    content {
      content  = file("${path.module}/../bff/${source.value}")
      filename = source.value
    }
  }

  # AWS Agent Registry's API models (bff/registry.py MODELS): the Lambda runtime's boto3
  # predates them. Mirrors the bff/ subdirectories stageBffPackage copies.
  dynamic "source" {
    for_each = fileset("${path.module}/../bff", "botocore_data/**/*.json")
    content {
      content  = file("${path.module}/../bff/${source.value}")
      filename = source.value
    }
  }

  # The Gateway interceptor templates, one copy shared with the Interceptors tab:
  # bff/interceptor_code.py generates an interceptor's files from them when the
  # Assistant sets its templates. Mirrors stageBffPackage in the CDK stack.
  dynamic "source" {
    for_each = toset(["request", "response"])
    content {
      content  = file("${path.module}/../web/src/builder/interceptor-templates/${source.value}.py")
      filename = "interceptor_templates/${source.value}.py"
    }
  }
  source {
    content  = file("${path.module}/../app/workflow.json")
    filename = "workflow.json"
  }

  # The framework's closed value sets. bff/authz.py reads the RBAC action names from
  # here rather than keeping a third copy of them.
  source {
    content  = file("${path.module}/../app/vocabulary.json")
    filename = "vocabulary.json"
  }
  # Every key's DEFAULT (generated from app/keys.json by build_schema.py). bff/authz.py
  # reads groupsClaim's default from here rather than keeping a fourth copy of it.
  source {
    content  = file("${path.module}/../app/defaults.json")
    filename = "defaults.json"
  }
  # The key spec. bff/validate_build.py checks a build against it before a deploy,
  # the same rules the Build view shows as you type.
  source {
    content  = file("${path.module}/../app/keys.json")
    filename = "keys.json"
  }
  # The framework version, recorded in every bundle the BFF freezes for a deploy.
  source {
    content  = file("${path.module}/../VERSION")
    filename = "VERSION"
  }
  # The Terraform path's deploy permissions: bff/accounts.py builds a connected
  # account's deploy role from them instead of granting AdministratorAccess.
  source {
    content  = file("${path.module}/deploy-role-policy.json")
    filename = "deploy-role-policy.json"
  }
}

resource "aws_iam_role" "bff" {
  name = "AgentCoreBFF-${var.agent_name}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "bff" {
  name = "AgentCoreBFFPolicy-${var.agent_name}"
  role = aws_iam_role.bff.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:${var.region}:${local.account_id}:*"
      },
      ], !local.workflow_plane ? [] : [
      # This deployment's own runs: none on a control-plane console.
      {
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem", "dynamodb:Query", "dynamodb:Scan"]
        Resource = [aws_dynamodb_table.status[0].arn, aws_dynamodb_table.events[0].arn]
      },
      {
        Effect = "Allow"
        Action = ["bedrock-agentcore:InvokeAgentRuntime"]
        Resource = [
          awscc_bedrockagentcore_runtime.orchestrator[0].agent_runtime_arn,
          "${awscc_bedrockagentcore_runtime.orchestrator[0].agent_runtime_arn}/*"
        ]
      },
      ], length(local.person_tool_names) == 0 || !local.gateway_enabled ? [] : [
      # A person who connected their account for a tool (auth "user") comes back to the
      # app, which binds that grant to them (POST /api/connect). Mirrors orchestrator-stack.ts.
      {
        Sid    = "BindPersonConsent"
        Effect = "Allow"
        Action = ["bedrock-agentcore:CompleteResourceTokenAuth"]
        Resource = [
          "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:workload-identity-directory/*",
          "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:token-vault/*",
        ]
      },
      {
        # Binding makes AgentCore Identity exchange the person's code with the CALLER's
        # credentials, so it reads the tool's client secret as the BFF. This build's only.
        Sid      = "BindPersonConsentSecret"
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = ["arn:aws:secretsmanager:${var.region}:${local.account_id}:secret:bedrock-agentcore-identity!default/oauth2/bedrock-agentcore-${replace(var.agent_name, "_", "-")}-*"]
      },
      ], [
      {
        Effect   = "Allow"
        Action   = ["lambda:InvokeFunction"]
        Resource = "arn:aws:lambda:${var.region}:${local.account_id}:function:AgentCoreBFF-${var.agent_name}"
      },
      {
        # In-app assistant: the chat tool loop calls Bedrock Converse; AgentExpress Assistant
        # streams its reply (ConverseStream).
        # (Telemetry Query for its read tools is already granted by the
        # bff_telemetry policy in observability.tf.)
        Sid    = "BedrockChatModel"
        Effect = "Allow"
        Action = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"]
        Resource = [
          "arn:aws:bedrock:*::foundation-model/*",
          "arn:aws:bedrock:${var.region}:${local.account_id}:inference-profile/*"
        ]
      },
      {
        # GET /api/models: list what this account can invoke, for the Build view's
        # model picker. Read-only list calls, which take no resource ARN.
        Sid      = "BedrockListModels"
        Effect   = "Allow"
        Action   = ["bedrock:ListFoundationModels", "bedrock:ListInferenceProfiles"]
        Resource = "*"
      }
    ])
  })
}

# Owned explicitly so `destroy` removes it. See var.log_retention_days.
resource "aws_cloudwatch_log_group" "bff" {
  name              = "/aws/lambda/AgentCoreBFF-${var.agent_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "bff" {
  function_name    = "AgentCoreBFF-${var.agent_name}"
  role             = aws_iam_role.bff.arn
  runtime          = "python3.13"
  handler          = "handler.handler"
  filename         = data.archive_file.bff.output_path
  source_code_hash = data.archive_file.bff.output_base64sha256
  # 300 s, not 60: a Design-with-AI turn (bff/designer.py) runs in a background
  # invocation of this function. API requests are still cut off by API Gateway at 30 s.
  timeout     = 300
  memory_size = 256

  environment {
    variables = merge({
      # The assistant's tool-use loop runs in this Lambda, so it needs the
      # deployment's model rather than its own copy of the id.
      MODEL_ID = local.model_id
      # No WORKFLOW_JSON. The workflow is in the deployment package (see the
      # archive_file above) because a 4 KB environment could not hold more than
      # about eleven agents' worth of it.
      # Where image agents' images are (images.tf), for GET /api/images.
      ASSETS_BUCKET = local.assets_bucket
    }, local.bff_run_env, local.builder_bff_env, local.audit_bff_env, local.trigger_bff_env, local.gate_bff_env) # builder.tf, audit.tf, triggers.tf
  }
  # The log group must exist BEFORE the function, or Lambda creates
  # /aws/lambda/<name> itself and Terraform's CreateLogGroup then fails with
  # ResourceAlreadyExistsException. Nothing in the function's arguments references the
  # group, so without this they are created in parallel and the apply is a race — which
  # is exactly how it failed on the first real apply, for two of the four functions.
  # (The CDK path gets this ordering for free by passing the group as `logGroup:`.)
  depends_on = [aws_cloudwatch_log_group.bff]
}

resource "aws_apigatewayv2_api" "bff" {
  name          = "AgentCoreBFF-${var.agent_name}"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "bff" {
  api_id                 = aws_apigatewayv2_api.bff.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.bff.invoke_arn
  payload_format_version = "2.0"
}

# JWT authorizer on /api/* — provider-agnostic. The UI sends its ID token; the
# issuer + audience for the configured IdP are derived in identity.tf. Skipped
# entirely when idp = "none", which leaves the API OPEN.
resource "aws_apigatewayv2_authorizer" "jwt" {
  count            = local.auth_enabled ? 1 : 0
  api_id           = aws_apigatewayv2_api.bff.id
  authorizer_type  = "JWT"
  identity_sources = ["$request.header.Authorization"]
  # Deliberately NOT derived from var.idp: the name is an identity, not config.
  # Keeping it stable means switching provider updates issuer/audience in place
  # instead of replacing an authorizer that live routes still reference.
  name = "jwt-${var.agent_name}"

  jwt_configuration {
    audience = [local.jwt_audience]
    issuer   = local.jwt_issuer
  }

  # If a future change ever does force replacement, the new authorizer must exist
  # before the old one goes away — API Gateway refuses to delete an authorizer
  # that any route still references (409 ConflictException).
  lifecycle {
    create_before_destroy = true
  }
}

# Renamed from .cognito when auth became provider-agnostic. Without this,
# Terraform would try to create a second authorizer and delete the in-use one.
moved {
  from = aws_apigatewayv2_authorizer.cognito
  to   = aws_apigatewayv2_authorizer.jwt
}

resource "aws_apigatewayv2_route" "routes" {
  for_each = toset([
    "GET /api/workflow",
    # Who the caller is and which run actions they may take, so the UI can disable
    # controls instead of offering buttons that 403.
    "GET /api/me",
    # The Bedrock text models this account can invoke, for the Build view's picker.
    "GET /api/models",
    "POST /api/sessions",
    # One file a run will bring, for agents with `attachments` (bff/runfiles.py).
    "POST /api/sessions/attachments",
    "GET /api/sessions",
    "GET /api/sessions/{id}",
    "DELETE /api/sessions/{id}",
    "POST /api/sessions/{id}/decision",
    "POST /api/sessions/{id}/cancel",
    "POST /api/sessions/{id}/rerun",
    "POST /api/sessions/{id}/evaluate",
    "POST /api/insights/run",
    "GET /api/insights",
    "GET /api/sessions/{id}/telemetry",
    "GET /api/telemetry/aggregate",
    "POST /api/chat",
    # Builder builds (bff/builds.py). Declared even with the builder plane off, where
    # the BFF answers 404 — so the route set is the same on every deployment.
    "GET /api/builds",
    "GET /api/builds/{id}",
    "PUT /api/builds/{id}",
    "DELETE /api/builds/{id}",
    "POST /api/builds/{id}/deploy",
    "POST /api/builds/{id}/destroy",
    "GET /api/builds/{id}/log",
    "GET /api/builds/{id}/login",
    # AgentExpress Assistant: the conversation that edits a build (bff/designer.py).
    "GET /api/builds/{id}/design",
    "POST /api/builds/{id}/design",
    "DELETE /api/builds/{id}/design",
    "POST /api/builds/{id}/design/attachments",
    # A link to an image an image agent rendered (images.tf).
    "GET /api/images",
    # A build's write-only secrets, and its knowledge-base documents.
    "GET /api/builds/{id}/secrets",
    "PUT /api/builds/{id}/secrets",
    "GET /api/builds/{id}/docs",
    "POST /api/builds/{id}/docs",
    "DELETE /api/builds/{id}/docs",
    # Connected AWS accounts (bff/accounts.py).
    "GET /api/accounts",
    "POST /api/accounts",
    "PUT /api/accounts/{id}",
    "DELETE /api/accounts/{id}",
    "POST /api/accounts/{id}/verify",
    "GET /api/accounts/{id}/launch",
    # A tool written in the build: checked (bff/codecheck.py), and tested once deployed.
    "POST /api/code/check",
    "POST /api/builds/{id}/test-tool",
    # The policy library (bff/policies.py), and plain English -> Cedar.
    "GET /api/policies",
    "POST /api/policies",
    "POST /api/policies/generate",
    "GET /api/policies/generate/{id}",
    "PUT /api/policies/{id}",
    "DELETE /api/policies/{id}",
    # Sharing, the library and admin-defined groups (bff/sharing.py, bff/library.py).
    "PUT /api/builds/{id}/shares",
    "GET /api/library",
    "POST /api/library",
    "GET /api/library/{id}",
    "PUT /api/library/{id}",
    "DELETE /api/library/{id}",
    "PUT /api/library/{id}/shares",
    "GET /api/registry",
    "GET /api/registry/search",
    "GET /api/builds/{id}/registry",
    "POST /api/builds/{id}/publish",
    "GET /api/groups",
    "PUT /api/groups/{id}",
    "DELETE /api/groups/{id}",
    # The audit log, and the sign-out the page reports before it signs out.
    "GET /api/audit",
    "POST /api/audit/logout",
    # External triggers (bff/triggers.py): the app's Triggers page.
    "GET /api/triggers",
    # A person back from connecting their account for a tool (auth "user").
    "POST /api/connect",
    "POST /api/triggers/{name}/secret",
    "POST /api/triggers/{name}/test",
  ])
  api_id             = aws_apigatewayv2_api.bff.id
  route_key          = each.value
  target             = "integrations/${aws_apigatewayv2_integration.bff.id}"
  authorization_type = local.auth_enabled ? "JWT" : "NONE"
  authorizer_id      = local.auth_enabled ? aws_apigatewayv2_authorizer.jwt[0].id : null
}

# A webhook trigger's delivery: the ONE route with no JWT authorizer. The BFF checks the
# signature over the body with the trigger's secret before reading anything else, and the
# route has its own, lower throttle (the stage below). Mirrors the CDK HttpApi.
resource "aws_apigatewayv2_route" "hook" {
  # checkov:skip=CKV_AWS_309:A webhook sender holds no user token; its proof is an HMAC signature of the body with the trigger's secret, checked by the BFF before anything else is read, on a route with its own throttle.
  api_id             = aws_apigatewayv2_api.bff.id
  route_key          = "POST /api/hooks/{name}"
  target             = "integrations/${aws_apigatewayv2_integration.bff.id}"
  authorization_type = "NONE"
}

# Access logs and a throttle on the $default stage. Mirrors the CDK HttpApi stage. The
# throttle is generous for a console: the UI polls a run every few seconds.
resource "aws_cloudwatch_log_group" "api_access" {
  name              = "/aws/apigateway/AgentCoreBFF-${var.agent_name}"
  retention_in_days = var.log_retention_days
}
resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.bff.id
  name        = "$default"
  auto_deploy = true
  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_access.arn
    # One JSON line per request: who, what, how it ended. No bodies, no headers.
    format = jsonencode({
      requestId        = "$context.requestId"
      ip               = "$context.identity.sourceIp"
      requestTime      = "$context.requestTime"
      routeKey         = "$context.routeKey"
      status           = "$context.status"
      responseLength   = "$context.responseLength"
      latency          = "$context.responseLatency"
      integrationError = "$context.integrationErrorMessage"
      authorizerError  = "$context.authorizer.error"
      sub              = "$context.authorizer.claims.sub"
    })
  }
  default_route_settings {
    throttling_rate_limit  = var.api_throttle_rate
    throttling_burst_limit = var.api_throttle_burst
  }
  route_settings {
    route_key              = aws_apigatewayv2_route.hook.route_key
    throttling_rate_limit  = var.hook_throttle_rate
    throttling_burst_limit = var.hook_throttle_burst
  }
}

resource "aws_lambda_permission" "apigw" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.bff.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.bff.execution_arn}/*/*"
}

# This deployment's own runs, for the BFF: none on a control-plane console.
locals {
  bff_run_env = !local.workflow_plane ? {} : {
    STATUS_TABLE    = aws_dynamodb_table.status[0].name
    EVENTS_TABLE    = aws_dynamodb_table.events[0].name
    TELEMETRY_TABLE = aws_dynamodb_table.telemetry[0].name
    RUNTIME_ARN     = awscc_bedrockagentcore_runtime.orchestrator[0].agent_runtime_arn
  }
}
