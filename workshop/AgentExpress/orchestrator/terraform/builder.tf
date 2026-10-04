# The Builder's control plane: where builds are stored, and the project that deploys and
# destroys them. Mirrors cdk/lib/builder-plane.ts; cdk/test/parity.test.ts and
# tests/test_builder_plane.py keep the two in step.
#
#   * a DynamoDB table of builds (layout in bff/buildstore.py) with an owner index
#   * a versioned S3 bucket: drafts, the immutable version each deploy used, and the
#     Terraform state of builds deployed with Terraform
#   * the framework source, zipped and uploaded, which every deploy starts from
#   * a CodeBuild project running deployer/runner.py — `cdk deploy|destroy` or
#     `terraform apply|destroy` for ONE build, as its own stack
#
# THE DEPLOY PROJECT CAN CREATE IAM ROLES. It deploys whole stacks: on the CDK path it
# assumes the CDK bootstrap roles, on the Terraform path it carries
# deploy-role-policy.json. That is why starting it is behind its own permissions,
# `deploy` and `destroy`, in workflow.json -> authorization.actions.

variable "enable_builder" {
  description = "Deploy the Builder's control plane (builds store + the project that deploys each build as its own stack). The deploy runner sets false for the build stacks it creates."
  type        = bool
  default     = true
}

# "builder": this console is a control plane. People design, build and deploy here, and
# each build's runs, observability and assistant live in that build's own app, so the
# BFF refuses every run route and the UI shows no Runs or Observability. Needs the
# Builder. "app" (the default): a workflow's own app, with the Builder beside it.
# Mirrors -c consoleMode in cdk/bin/orchestrator.ts.
variable "console_mode" {
  description = "\"app\" (default): this workflow's own app. \"builder\": a control-plane console that designs, builds and deploys, and runs nothing itself."
  type        = string
  default     = "app"
  validation {
    condition     = contains(["app", "builder"], var.console_mode)
    error_message = "console_mode must be \"app\" or \"builder\"."
  }
}

# Buckets, or bucket/prefix, the AgentExpress Assistant may read when a message names an
# s3:// path (bff/designer.py). Mirrors -c designerS3Buckets in cdk/bin/orchestrator.ts.
variable "designer_s3_buckets" {
  description = "Buckets (or bucket/prefix) the AgentExpress Assistant may read files from when a message names s3://... Empty: none."
  type        = list(string)
  default     = []
}
# The AgentExpress Assistant's model and the small model that condenses long
# conversations (bff/designer.py). Empty: designer.py's defaults (Claude Sonnet 5.5; the
# deployment's default model), in this region's inference-profile geography. Mirrors
# -c designerModel / -c designerSummaryModel in cdk/bin/orchestrator.ts.
variable "designer_model" {
  description = "The AgentExpress Assistant's models, tried in order: \"model[:effort],...\". Empty: Claude Sonnet 5.5, then Claude Sonnet 5 when the account may not call 5.5."
  type        = string
  default     = ""
}
variable "designer_summary_model" {
  description = "Model id the Assistant uses to condense a long conversation. Empty: the deployment's default model."
  type        = string
  default     = ""
}
variable "designer_effort" {
  description = "How hard the Assistant's model reasons before it answers: medium (default; a whole workflow in about 30 s on Sonnet 5.5), low, high, or model (the model's own default). Empty: medium."
  type        = string
  default     = ""
  validation {
    condition     = contains(["", "low", "medium", "high", "model"], var.designer_effort)
    error_message = "designer_effort must be low, medium, high or model."
  }
}
locals {
  designer_s3 = [for e in var.designer_s3_buckets : {
    bucket = split("/", trimsuffix(trimprefix(e, "s3://"), "/"))[0]
    prefix = join("/", slice(split("/", trimsuffix(trimprefix(e, "s3://"), "/")), 1, length(split("/", trimsuffix(trimprefix(e, "s3://"), "/")))))
  }]
}
resource "aws_iam_role_policy" "bff_designer_s3" {
  count = local.builder_enabled && length(local.designer_s3) > 0 ? 1 : 0
  name  = "AgentCoreBFFAssistantS3-${var.agent_name}"
  role  = aws_iam_role.bff.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [for i, d in local.designer_s3 : {
        Sid      = "AssistantReadsS3Object${i}", Effect = "Allow", Action = ["s3:GetObject"]
        Resource = "arn:aws:s3:::${d.bucket}/${d.prefix == "" ? "" : "${d.prefix}/"}*"
      }],
      [for i, d in local.designer_s3 : merge({
        Sid = "AssistantListsS3${i}", Effect = "Allow", Action = ["s3:ListBucket"], Resource = "arn:aws:s3:::${d.bucket}"
      }, d.prefix == "" ? {} : { Condition = { StringLike = { "s3:prefix" = ["${d.prefix}/*", d.prefix] } } })]
    )
  })
}
resource "terraform_data" "console_mode_check" {
  lifecycle {
    precondition {
      condition     = var.console_mode != "builder" || var.enable_builder
      error_message = "console_mode = \"builder\" needs the Builder: set enable_builder = true."
    }
  }
}

locals {
  # The workflow plane: what RUNS this deployment's workflow (image, runtime, memory,
  # guardrail, Gateway, Knowledge Base, run tables, Transaction Search). None on a
  # control-plane console, which only designs, builds and deploys. Mirrors workflowPlane
  # in cdk/lib/orchestrator-stack.ts.
  workflow_plane  = var.console_mode != "builder"
  gateway_enabled = var.enable_gateway && local.workflow_plane
  builder_enabled = var.enable_builder
  deploy_project  = "${replace(var.agent_name, "_", "-")}-deploy"
  # Every build's resources are named ax_xxxxxxxx (bff/buildstore.py AGENT_PREFIX).
  build_agent_prefix = "ax_"
  # The role a connected account creates (bff/buildstore.py CONNECT_ROLE_PREFIX).
  connect_role_prefix = "AgentExpressDeploy-"
  # Where a build's tool API keys and A2A tokens live: <prefix><build id>.
  secrets_prefix    = "agentexpress/${var.agent_name}/builds/"
  terraform_version = "1.15.8"
  deploy_policy     = jsondecode(file("${path.module}/deploy-role-policy.json"))
  # Same split as cdk/lib/builder-plane.ts DEPLOY_POLICY_CHUNK: one managed policy holds
  # at most 6,144 characters, and the whole file is more than that. Not chunklist(): the
  # statements differ in shape (a Resource string or list, a Condition or none), and
  # chunklist needs elements of one type.
  deploy_policy_chunks = [
    for i in range(0, length(local.deploy_policy.Statement), 7) :
    slice(local.deploy_policy.Statement, i, min(i + 7, length(local.deploy_policy.Statement)))
  ]

  builder_bff_env = local.builder_enabled ? merge({
    BUILDS_TABLE   = aws_dynamodb_table.builds[0].name
    BUILDS_BUCKET  = aws_s3_bucket.builds[0].id
    DEPLOY_PROJECT = local.deploy_project
    SECRETS_PREFIX = local.secrets_prefix
    # The two roles a connected account's deploy role trusts (bff/accounts.py template).
    DEPLOY_ROLE_ARN = aws_iam_role.deploy[0].arn
    BFF_ROLE_ARN    = aws_iam_role.bff.arn
    # s3:// paths an AgentExpress Assistant message may name (bff/designer.py).
    DESIGNER_S3_BUCKETS = join(",", var.designer_s3_buckets)
    }, var.designer_model == "" ? {} : { DESIGNER_MODEL = var.designer_model },
    var.designer_summary_model == "" ? {} : { DESIGNER_SUMMARY_MODEL = var.designer_summary_model },
  var.designer_effort == "" ? {} : { DESIGNER_EFFORT = var.designer_effort }) : {}
  console_mode_env = local.builder_enabled && var.console_mode == "builder" ? { CONSOLE_MODE = "builder" } : {}
}

resource "aws_dynamodb_table" "builds" {
  count        = local.builder_enabled ? 1 : 0
  name         = "${var.agent_name}_builds"
  billing_mode = "PAY_PER_REQUEST"
  point_in_time_recovery {
    enabled = true
  }
  hash_key  = "pk"
  range_key = "sk"

  attribute {
    name = "pk"
    type = "S"
  }
  attribute {
    name = "sk"
    type = "S"
  }
  attribute {
    name = "owner"
    type = "S"
  }
  attribute {
    name = "updated"
    type = "S"
  }
  global_secondary_index {
    name = "by_owner"
    # key_schema, not hash_key/range_key: those are deprecated on a GSI in the AWS
    # provider and print a warning on every plan.
    key_schema {
      attribute_name = "owner"
      key_type       = "HASH"
    }
    key_schema {
      attribute_name = "updated"
      key_type       = "RANGE"
    }
    projection_type = "ALL"
  }
}

# A generated name: agentcore-<agent_name>-builds-<account>-<region> would not fit in 63
# characters for the default agent_name.
resource "aws_s3_bucket" "builds" {
  count         = local.builder_enabled ? 1 : 0
  bucket_prefix = "agentcore-builds-"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "builds" {
  count  = local.builder_enabled ? 1 : 0
  bucket = aws_s3_bucket.builds[0].id
  # Versioned: Terraform state lives here, and a draft overwritten by autosave can be
  # recovered.
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "builds" {
  count                   = local.builder_enabled ? 1 : 0
  bucket                  = aws_s3_bucket.builds[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "builds" {
  count  = local.builder_enabled ? 1 : 0
  bucket = aws_s3_bucket.builds[0].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Knowledge-base documents are uploaded from the browser straight to this bucket (a
# presigned POST), so it must accept a POST from the console's origin.
resource "aws_s3_bucket_cors_configuration" "builds" {
  count  = local.builder_enabled ? 1 : 0
  bucket = aws_s3_bucket.builds[0].id
  cors_rule {
    allowed_methods = ["POST"]
    allowed_origins = ["https://${aws_cloudfront_distribution.ui.domain_name}"]
    allowed_headers = ["*"]
    max_age_seconds = 3000
  }
}

resource "aws_s3_bucket_policy" "builds_tls" {
  count  = local.builder_enabled ? 1 : 0
  bucket = aws_s3_bucket.builds[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.builds[0].arn, "${aws_s3_bucket.builds[0].arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.builds]
}

# The source every deploy starts from. What is left out mirrors
# cdk/lib/builder-plane.ts SOURCE_EXCLUDES: build output, installed packages, local
# state, and a developer's own settings (terraform.tfvars and backend.hcl may carry
# secrets).
data "archive_file" "builder_source" {
  count       = local.builder_enabled ? 1 : 0
  type        = "zip"
  source_dir  = "${path.module}/.."
  output_path = "${path.module}/.build/builder-source.zip"
  excludes = [
    "**/node_modules/**", "**/cdk.out/**", "**/dist/**", "**/.vite/**", "**/.terraform/**",
    "**/.terraform.lock.hcl", "**/.build/**", "**/__pycache__/**", "**/.pytest_cache/**",
    "**/.ruff_cache/**", "**/.venv/**", "**/.DS_Store", "**/terraform.tfvars",
    "**/backend.hcl", "**/tfplan", "**/*.tfstate", "**/*.tfstate.*", "**/.env", "**/.env.*",
    "**/zz_builder_backend_override.tf", "**/builder.auto.tfvars.json",
  ]
}

resource "aws_s3_object" "builder_source" {
  count  = local.builder_enabled ? 1 : 0
  bucket = aws_s3_bucket.builds[0].id
  key    = "source/${data.archive_file.builder_source[0].output_md5}.zip"
  source = data.archive_file.builder_source[0].output_path
  etag   = data.archive_file.builder_source[0].output_md5
}

resource "aws_cloudwatch_log_group" "deploy" {
  count             = local.builder_enabled ? 1 : 0
  name              = "/aws/codebuild/${local.deploy_project}"
  retention_in_days = var.log_retention_days
}

resource "aws_iam_role" "deploy" {
  count = local.builder_enabled ? 1 : 0
  name  = "AgentCoreDeploy-${var.agent_name}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "codebuild.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "deploy" {
  count = local.builder_enabled ? 1 : 0
  name  = "AgentCoreDeployRunner-${var.agent_name}"
  role  = aws_iam_role.deploy[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "Logs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.deploy[0].arn}:*"
      },
      {
        Sid      = "BuildsTable"
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem", "dynamodb:Query"]
        Resource = [aws_dynamodb_table.builds[0].arn, "${aws_dynamodb_table.builds[0].arn}/index/*"]
      },
      {
        Sid      = "BuildsBucket"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion", "s3:ListBucket", "s3:ListBucketVersions"]
        Resource = [aws_s3_bucket.builds[0].arn, "${aws_s3_bucket.builds[0].arn}/*"]
      },
      {
        # A build's tool API keys and A2A tokens, read at deploy and removed with the build.
        Sid    = "BuildSecrets"
        Effect = "Allow"
        # ...and it keeps the owner's first-sign-in password for the build's console there.
        Action   = ["secretsmanager:GetSecretValue", "secretsmanager:DeleteSecret", "secretsmanager:CreateSecret", "secretsmanager:PutSecretValue"]
        Resource = "arn:aws:secretsmanager:${var.region}:${local.account_id}:secret:${local.secrets_prefix}*"
      },
      {
        # Deploying into a CONNECTED account: its deploy role, and only that kind of role.
        Sid      = "ConnectedAccounts"
        Effect   = "Allow"
        Action   = ["sts:AssumeRole"]
        Resource = "arn:aws:iam::*:role/${local.connect_role_prefix}*"
      },
      {
        # Inviting the owner into a build's own console (for builds in this account).
        Sid      = "InviteOwner"
        Effect   = "Allow"
        Action   = ["cognito-idp:AdminCreateUser", "cognito-idp:AdminAddUserToGroup", "cognito-idp:ListGroups"]
        Resource = "arn:aws:cognito-idp:${var.region}:${local.account_id}:userpool/*"
        # Only a build's pool: every stack tags its resources agentexpress:app=<agent_name>,
        # and a build's agent_name is ax_<id>. Not this console's pool, nor anyone else's.
        Condition = { StringLike = { "aws:ResourceTag/agentexpress:app" = "${local.build_agent_prefix}*" } }
      },
      {
        # After a destroy, the runner deletes the log groups neither tool owns (AgentCore's
        # runtime logs, CDK provider Lambdas), matched by the build's unique ax_ id.
        Sid    = "LeftoverLogGroups"
        Effect = "Allow"
        Action = ["logs:DeleteLogGroup"]
        Resource = [
          "arn:aws:logs:${var.region}:${local.account_id}:log-group:/aws/bedrock-agentcore/runtimes/${local.build_agent_prefix}*",
          "arn:aws:logs:${var.region}:${local.account_id}:log-group:/aws/lambda/ax-*",
          "arn:aws:logs:${var.region}:${local.account_id}:log-group:ax-*"
        ]
      },
      {
        # CDK path: the bootstrap roles do the deploying.
        Sid      = "CdkBootstrapRoles"
        Effect   = "Allow"
        Action   = ["sts:AssumeRole"]
        Resource = "arn:aws:iam::${local.account_id}:role/cdk-hnb659fds-*"
      },
      {
        Sid      = "CdkBootstrapVersion"
        Effect   = "Allow"
        Action   = ["ssm:GetParameter"]
        Resource = "arn:aws:ssm:${var.region}:${local.account_id}:parameter/cdk-bootstrap/*"
      },
      {
        # The deploy policy may manage AgentCoreDeploy-* roles and AgentExpressDeploy-*
        # policies (a human deploying a console needs that). This role is one of each, so
        # it must not be able to grant itself more: no edit of its own role or policies.
        Sid    = "DenySelfEscalation"
        Effect = "Deny"
        Action = [
          "iam:PutRolePolicy", "iam:DeleteRolePolicy", "iam:AttachRolePolicy", "iam:DetachRolePolicy",
          "iam:UpdateAssumeRolePolicy", "iam:PutRolePermissionsBoundary", "iam:DeleteRolePermissionsBoundary",
          "iam:DeleteRole", "iam:CreatePolicyVersion", "iam:DeletePolicyVersion", "iam:SetDefaultPolicyVersion",
          "iam:DeletePolicy"
        ]
        Resource = [
          "arn:aws:iam::${local.account_id}:role/AgentCoreDeploy-${var.agent_name}",
          "arn:aws:iam::${local.account_id}:policy/AgentExpressDeploy-${var.agent_name}-*"
        ]
      }
    ]
  })
}

# Terraform path: the documented deploy permissions — plus read-only everywhere, because
# a refresh reads far more of each resource than the write permissions name, and a read
# gap fails the apply.
resource "aws_iam_policy" "deploy" {
  count  = local.builder_enabled ? length(local.deploy_policy_chunks) : 0
  name   = "AgentExpressDeploy-${var.agent_name}-${count.index}"
  policy = jsonencode({ Version = "2012-10-17", Statement = local.deploy_policy_chunks[count.index] })
}

resource "aws_iam_role_policy_attachment" "deploy" {
  count      = local.builder_enabled ? length(local.deploy_policy_chunks) : 0
  role       = aws_iam_role.deploy[0].name
  policy_arn = aws_iam_policy.deploy[count.index].arn
}

resource "aws_iam_role_policy_attachment" "deploy_read_only" {
  count      = local.builder_enabled ? 1 : 0
  role       = aws_iam_role.deploy[0].name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}

resource "aws_codebuild_project" "deploy" {
  count         = local.builder_enabled ? 1 : 0
  name          = local.deploy_project
  description   = "Deploys and destroys AgentExpress Builder builds, one stack each"
  service_role  = aws_iam_role.deploy[0].arn
  build_timeout = 120

  artifacts {
    type = "NO_ARTIFACTS"
  }

  environment {
    # ARM, because the AgentCore runtime image is linux/arm64 and building it natively
    # is far faster than under emulation. Privileged for Docker.
    type            = "ARM_CONTAINER"
    compute_type    = "BUILD_GENERAL1_LARGE"
    image           = "aws/codebuild/amazonlinux-aarch64-standard:3.0"
    privileged_mode = true

    environment_variable {
      name  = "SOURCE_URI"
      value = "s3://${aws_s3_bucket.builds[0].id}/${aws_s3_object.builder_source[0].key}"
    }
    environment_variable {
      name  = "BUILDS_TABLE"
      value = aws_dynamodb_table.builds[0].name
    }
    environment_variable {
      name  = "BUILDS_BUCKET"
      value = aws_s3_bucket.builds[0].id
    }
    environment_variable {
      name  = "TERRAFORM_VERSION"
      value = local.terraform_version
    }
    environment_variable {
      name  = "SECRETS_PREFIX"
      value = local.secrets_prefix
    }
    environment_variable {
      name  = "CONSOLE_NAME"
      value = var.agent_name
    }
  }

  source {
    type      = "NO_SOURCE"
    buildspec = file("${path.module}/../deployer/buildspec.json")
  }

  logs_config {
    cloudwatch_logs {
      group_name = aws_cloudwatch_log_group.deploy[0].name
    }
  }

  depends_on = [aws_iam_role_policy.deploy, aws_iam_role_policy_attachment.deploy]
}

# What the console's BFF needs to serve /api/builds and to run deployed builds.
resource "aws_iam_role_policy" "bff_builder" {
  count = local.builder_enabled ? 1 : 0
  name  = "AgentCoreBFFBuilder-${var.agent_name}"
  role  = aws_iam_role.bff.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Someone a build is shared with gets a login to its app on first ask
        # (bff/builds.py _invite_collaborator), like the owner's from the deploy.
        Sid      = "InviteCollaborator"
        Effect   = "Allow"
        Action   = ["cognito-idp:AdminCreateUser", "cognito-idp:AdminAddUserToGroup", "cognito-idp:ListGroups"]
        Resource = "arn:aws:cognito-idp:${var.region}:${local.account_id}:userpool/*"
        # Only a build's pool: every stack tags its resources agentexpress:app=<agent_name>,
        # and a build's agent_name is ax_<id>. Not this console's pool, nor anyone else's.
        Condition = { StringLike = { "aws:ResourceTag/agentexpress:app" = "${local.build_agent_prefix}*" } }
      },
      {
        # Scan: an admin's list of every user's builds (GET /api/builds?scope=all).
        Sid      = "BuildsTable"
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem", "dynamodb:Query", "dynamodb:Scan"]
        Resource = [aws_dynamodb_table.builds[0].arn, "${aws_dynamodb_table.builds[0].arn}/index/*"]
      },
      {
        Sid      = "BuildsObjects"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion"]
        Resource = "${aws_s3_bucket.builds[0].arn}/*"
      },
      {
        Sid      = "BuildsList"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:ListBucketVersions"]
        Resource = aws_s3_bucket.builds[0].arn
      },
      {
        Sid      = "DeployProject"
        Effect   = "Allow"
        Action   = ["codebuild:StartBuild", "codebuild:BatchGetBuilds"]
        Resource = aws_codebuild_project.deploy[0].arn
      },
      {
        Sid      = "DeployLog"
        Effect   = "Allow"
        Action   = ["logs:GetLogEvents"]
        Resource = "${aws_cloudwatch_log_group.deploy[0].arn}:*"
      },
      {
        Sid      = "BuildSecrets"
        Effect   = "Allow"
        Action   = ["secretsmanager:CreateSecret", "secretsmanager:PutSecretValue", "secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret", "secretsmanager:DeleteSecret"]
        Resource = "arn:aws:secretsmanager:${var.region}:${local.account_id}:secret:${local.secrets_prefix}*"
      },
      {
        # Verifying a connection: assume the connected account's deploy role once.
        Sid      = "VerifyConnectedAccounts"
        Effect   = "Allow"
        Action   = ["sts:AssumeRole"]
        Resource = "arn:aws:iam::*:role/${local.connect_role_prefix}*"
      },
      {
        # Running a deployed build: its tables and its runtime, all named ax_xxxxxxxx.
        Sid    = "BuildTables"
        Effect = "Allow"
        Action = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem", "dynamodb:Query", "dynamodb:Scan"]
        Resource = [
          "arn:aws:dynamodb:${var.region}:${local.account_id}:table/${local.build_agent_prefix}*",
          "arn:aws:dynamodb:${var.region}:${local.account_id}:table/${local.build_agent_prefix}*/index/*"
        ]
      },
      {
        Sid    = "BuildRuntimes"
        Effect = "Allow"
        Action = ["bedrock-agentcore:InvokeAgentRuntime"]
        Resource = [
          "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:runtime/${local.build_agent_prefix}*",
          "arn:aws:bedrock-agentcore:${var.region}:${local.account_id}:runtime/${local.build_agent_prefix}*/*"
        ]
      },
      {
        # A code tool, checked in the AWS-managed Code Interpreter sandbox
        # (bff/codecheck.py), and called by Test tool once a build here deploys it.
        Sid    = "CodeToolChecks"
        Effect = "Allow"
        Action = ["bedrock-agentcore:StartCodeInterpreterSession", "bedrock-agentcore:InvokeCodeInterpreter",
        "bedrock-agentcore:StopCodeInterpreterSession"]
        Resource = ["arn:aws:bedrock-agentcore:${var.region}:aws:code-interpreter/aws.codeinterpreter.v1"]
      },
      {
        # Plain English -> Cedar by AgentCore Policy, against a build's own engine and
        # Gateway in this account (bff/policies.py generate).
        Sid    = "WritePoliciesInPlainEnglish"
        Effect = "Allow"
        Action = ["bedrock-agentcore:StartPolicyGeneration", "bedrock-agentcore:GetPolicyGeneration",
          "bedrock-agentcore:ListPolicyGenerationAssets", "bedrock-agentcore:GetGateway",
          # AgentCore reads the Gateway's tools, as the caller, to write the policy.
          "bedrock-agentcore:ListGatewayTargets", "bedrock-agentcore:GetGatewayTarget", "bedrock-agentcore:GetPolicyEngine",
        "bedrock-agentcore:InvokeGateway"]
        Resource = ["arn:aws:bedrock-agentcore:*:${local.account_id}:policy-engine/${local.build_agent_prefix}*",
        "arn:aws:bedrock-agentcore:*:${local.account_id}:gateway/${replace(local.build_agent_prefix, "_", "-")}*"]
      },
      {
        Sid      = "FindBuildPolicyEngines"
        Effect   = "Allow"
        Action   = ["bedrock-agentcore:ListPolicyEngines", "bedrock-agentcore:ListGateways"]
        Resource = ["*"]
      },
      {
        Sid      = "TestCodeTools"
        Effect   = "Allow"
        Action   = ["lambda:InvokeFunction"]
        Resource = ["arn:aws:lambda:${var.region}:${local.account_id}:function:ToolLambda-${local.build_agent_prefix}*"]
      },
      {
        # ...and the images their image agents render (bff/handler.py _image_link).
        Sid      = "BuildImages"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "arn:aws:s3:::agentcore-${replace(local.build_agent_prefix, "_", "-")}*-assets-${local.account_id}/runs/*"
      }
    ]
  })
}
