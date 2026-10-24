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

# A link to a product demo video, shown on a control-plane console (console_mode =
# "builder") in the About panel and the first-run view. Empty: no link. Mirrors
# -c demoVideoUrl in cdk/bin/orchestrator.ts.
variable "demo_video_url" {
  description = "An https:// link to a product demo video, shown on a control-plane console (console_mode = \"builder\"). Empty (default): no link."
  type        = string
  default     = ""
  validation {
    condition     = var.demo_video_url == "" || can(regex("^https://[^\\s/?#]+[^\\s]*$", var.demo_video_url))
    error_message = "demo_video_url must be an https:// URL or empty."
  }
}

# The documentation base URL the UI links each feature's info icon to, and the top bar's
# Documentation link (web/src/docs/docsLinks.ts). On a control plane and on a workflow's own
# app, and passed on to every build this console deploys. Unset (null, the default): the
# public user guide at the release tag (local.docs_url, from ../docs_default.json). Empty:
# no documentation links anywhere. Mirrors -c docsUrl in cdk/bin/orchestrator.ts.
variable "docs_url" {
  description = "An https:// documentation base URL the UI links each feature to. Unset: the public user guide at the release tag (docs_default.json). Empty: no documentation links."
  type        = string
  default     = null
  nullable    = true
  validation {
    condition     = var.docs_url == null || var.docs_url == "" || can(regex("^https://[^\\s/?#]+[^\\s]*$", var.docs_url))
    error_message = "docs_url must be an https:// URL or empty."
  }
}

# false: no info icons and no Documentation link anywhere, on this console and on every
# build it deploys. Mirrors -c docsLinks in cdk/bin/orchestrator.ts.
variable "docs_links" {
  description = "Show the in-product documentation links (info icons and the Documentation link). false: none anywhere."
  type        = bool
  default     = true
}

# Buckets, or bucket/prefix, the AgentExpress Assistant may read when a message names an
# s3:// path (bff/designer.py). Mirrors -c designerS3Buckets in cdk/bin/orchestrator.ts.
# Buckets an S3 code source (codeSource.s3) may name when a build deploys into this
# console's own account (bff/code_source.py, deployer/runner.py). Empty: S3 code sources
# need a connected account. Mirrors -c codeSourceBuckets in cdk/bin/orchestrator.ts.
variable "code_source_buckets" {
  description = "Buckets an S3 code source may read for builds deployed into this console's own account. Empty: S3 code sources need a connected account."
  type        = list(string)
  default     = []
  validation {
    condition     = alltrue([for b in var.code_source_buckets : can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", b))])
    error_message = "code_source_buckets must be bucket names (no s3://, no prefix)."
  }
}
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
# The model that reviews the Assistant's changes when Review changes is on (bff/review.py).
# Its inference profile and the foundation model in each Region it routes to are covered by
# the BFF's BedrockChatModel grant (bff.tf). Mirrors -c designerReviewModel.
variable "designer_review_model" {
  description = "Model id that reviews the AgentExpress Assistant's changes (Review changes: Auto or Always). Empty: us.openai.gpt-6.1-sol."
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
  # Its role ARNs name roles at "/": with iam_role_path they live under that path instead.
  deploy_policy = jsondecode(replace(file("${path.module}/deploy-role-policy.json"), ":role/", ":role${var.iam_role_path}"))
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
    # S3 code sources a build in this account may read (bff/builds.py deploy).
    CODE_SOURCE_BUCKETS = join(",", var.code_source_buckets)
    }, var.designer_model == "" ? {} : { DESIGNER_MODEL = var.designer_model },
    var.designer_summary_model == "" ? {} : { DESIGNER_SUMMARY_MODEL = var.designer_summary_model },
    var.designer_review_model == "" ? {} : { DESIGNER_REVIEW_MODEL = var.designer_review_model },
  var.designer_effort == "" ? {} : { DESIGNER_EFFORT = var.designer_effort }) : {}
  console_mode_env = local.builder_enabled && var.console_mode == "builder" ? merge({ CONSOLE_MODE = "builder" },
  var.demo_video_url == "" ? {} : { DEMO_VIDEO_URL = var.demo_video_url }) : {}
  # The default documentation: the public user guide at the release tag. The same file
  # cdk/lib/docs-default.ts reads; the release process bumps its `ref`.
  docs_default = jsondecode(file("${path.module}/../docs_default.json"))
  docs_url     = var.docs_url == null ? replace(local.docs_default.urlTemplate, "{ref}", local.docs_default.ref) : var.docs_url
  # The documentation links (GET /api/me docsUrl, or docsLinks false), on any console; and on
  # the deploy project, for every build's app (deployer/runner.py docs_settings). Off
  # (DOCS_LINKS=false) with docs_links = false or an empty docs_url. Mirrors docsEnv in
  # cdk/lib/orchestrator-stack.ts and cdk/lib/builder-plane.ts.
  docs_env = var.docs_links && local.docs_url != "" ? { DOCS_URL = local.docs_url } : { DOCS_LINKS = "false" }
}

variable "keep_builds_table_protected" {
  type        = bool
  default     = false
  description = <<-EOT
    The builds table is deletion-protected while the console runs, so no account clean-up
    tool or stray DeleteTable can take it. By default `terraform destroy` clears that
    protection just before it deletes the table, so a destroy you asked for completes in
    one step. Set this true on a console that holds work you cannot lose: destroy then
    stops at the table, and you clear the protection yourself on purpose
    (`aws dynamodb update-table --table-name <agent_name>_builds --no-deletion-protection-enabled`).
    Mirrors `-c keepBuildsTableProtected=true` on the CDK path.
  EOT
}

resource "aws_dynamodb_table" "builds" {
  count        = local.builder_enabled ? 1 : 0
  name         = "${var.agent_name}_builds"
  billing_mode = "PAY_PER_REQUEST"
  # Every build, its versions, shares and the console's audit log live here, so no
  # account clean-up tool or stray DeleteTable may take it. `terraform destroy` clears
  # it for you at the last moment (terraform_data.builds_table_unprotect, below) unless
  # keep_builds_table_protected is set. Mirrors the CDK BuildsTable.
  deletion_protection_enabled = true
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

  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.data.arn
  }
}

# Clears the table's deletion protection at the last moment of a `terraform destroy`, so
# the destroy you asked for finishes instead of stopping at the table.
#
#   * It depends on the table (through `input`), so Terraform destroys it FIRST.
#   * It acts only on destroy. Creating it, or changing the flag, runs nothing.
#   * It is always in the plan when the Builder is on, and carries the flag as data.
#     Were it `count = keep ? 0 : 1`, turning keep_builds_table_protected on would
#     REMOVE the resource, and removing it runs the destroy step: the opposite of what
#     the flag asks for.
#   * A failure here is not fatal. Terraform goes on to delete the table, and AWS's own
#     "protected against deletion" message is the error you see, with the fix printed.
resource "terraform_data" "builds_table_unprotect" {
  count = local.builder_enabled ? 1 : 0
  input = {
    table     = aws_dynamodb_table.builds[0].name
    region    = var.region
    unprotect = !var.keep_builds_table_protected
  }

  provisioner "local-exec" {
    when        = destroy
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      if [ "${self.input.unprotect}" != "true" ]; then
        echo "builds table ${self.input.table} left protected (keep_builds_table_protected = true)"
        exit 0
      fi
      if aws dynamodb update-table --region ${self.input.region} --table-name ${self.input.table} \
           --no-deletion-protection-enabled >/dev/null; then
        echo "deletion protection cleared on ${self.input.table}"
      else
        echo "could not clear deletion protection on ${self.input.table}; run: aws dynamodb update-table --region ${self.input.region} --table-name ${self.input.table} --no-deletion-protection-enabled   then destroy again" >&2
      fi
    EOT
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
    allowed_origins = local.ui_origins
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
    "**/backend.hcl", "**/tfplan", "**/*.tfplan", "**/*.tfstate", "**/*.tfstate.*", "**/.env", "**/.env.*",
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

  path                 = var.iam_role_path
  permissions_boundary = local.iam_permissions_boundary
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
        # A CDK build's destroy that a late CloudFront log delivery failed: the runner empties
        # that build's UI logs bucket and destroys again (deployer/runner.py
        # empty_late_log_buckets). Only buckets named after a build's stack.
        Sid      = "LateLogBuckets"
        Effect   = "Allow"
        Action   = ["s3:ListBucketVersions", "s3:DeleteObject", "s3:DeleteObjectVersion"]
        Resource = ["arn:aws:s3:::ax-*-stack-uilogsbucket*", "arn:aws:s3:::ax-*-stack-uilogsbucket*/*"]
      },
      {
        # After a destroy, the runner deletes the log groups neither tool owns (AgentCore's
        # runtime logs, CDK provider Lambdas), matched by the build's unique ax_ id.
        Sid    = "LeftoverLogGroups"
        Effect = "Allow"
        Action = ["logs:DeleteLogGroup"]
        Resource = [
          "arn:aws:logs:${var.region}:${local.account_id}:log-group:/aws/bedrock-agentcore/runtimes/${local.build_agent_prefix}*",
          # A harness's runtime logs: AgentCore names them harness_<agent>_<step>-<id>-DEFAULT.
          "arn:aws:logs:${var.region}:${local.account_id}:log-group:/aws/bedrock-agentcore/runtimes/harness_${local.build_agent_prefix}*",
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
          aws_iam_role.deploy[0].arn,
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

# SAMPLE: ReadOnlyAccess is intentionally broad so `terraform plan`/refresh can read every
# resource type a build might declare. For production, replace it with a scoped read policy
# covering only the services your workflows deploy (Bedrock, AgentCore, Lambda, DynamoDB,
# S3, IAM, CloudFront, Cognito, API Gateway, Secrets Manager, CloudWatch Logs).
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
    # S3 code sources a build in this account may read (runner.py code_source_s3).
    environment_variable {
      name  = "CODE_SOURCE_BUCKETS"
      value = join(",", var.code_source_buckets)
    }
    # The organisation's boundary and role path, for every build's roles (hardening.tf).
    dynamic "environment_variable" {
      for_each = local.build_guardrail_env
      content {
        name  = environment_variable.key
        value = environment_variable.value
      }
    }
    # This console's LiteLLM proxy and key, for every build's litellm/ agents (litellm.tf).
    dynamic "environment_variable" {
      for_each = local.litellm_build_env
      content {
        name  = environment_variable.key
        value = environment_variable.value
      }
    }
    # This console's documentation links, for every build's app (local.docs_env).
    dynamic "environment_variable" {
      for_each = local.docs_env
      content {
        name  = environment_variable.key
        value = environment_variable.value
      }
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

  encryption_key = aws_kms_key.data.arn
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
        Sid    = "BuildsObjects"
        Effect = "Allow"
        # Scoped to this deployment's builds bucket objects only.
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion"] # nosemgrep
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
      # AWS Agent Registry (bff/registry.py): list registries, find approved records, and
      # (an admin) publish a build or skill for approval / deprecate it. Records only: the
      # console never creates or deletes a registry. Mirrors cdk/lib/builder-plane.ts.
      {
        Sid      = "AgentRegistryList"
        Effect   = "Allow"
        Action   = ["agent-registry:ListRegistries"]
        Resource = "*"
      },
      {
        Sid    = "AgentRegistryRecords"
        Effect = "Allow"
        Action = ["agent-registry:SearchDiscoverableRegistryRecords", "agent-registry:ListDiscoverableRegistryRecords",
          "agent-registry:GetDiscoverableRegistryRecord", "agent-registry:GetRegistry",
          "agent-registry:CreateRegistryRecord", "agent-registry:UpdateRegistryRecord",
          "agent-registry:GetRegistryRecord", "agent-registry:SubmitRegistryRecordForApproval",
        "agent-registry:UpdateRegistryRecordStatus", "agent-registry:TagResource"]
        Resource = "arn:aws:agent-registry:${var.region}:${local.account_id}:registry/*"
      },
      {
        Sid    = "BuildSecrets"
        Effect = "Allow"
        # Scoped to this deployment's own secret-name prefix.
        Action   = ["secretsmanager:CreateSecret", "secretsmanager:PutSecretValue", "secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret", "secretsmanager:DeleteSecret"] # nosemgrep
        Resource = "arn:aws:secretsmanager:${var.region}:${local.account_id}:secret:${local.secrets_prefix}*"
      },
      {
        # Verifying a connection: assume the connected account's deploy role once.
        Sid    = "VerifyConnectedAccounts"
        Effect = "Allow"
        # Only roles with the connect-role prefix this stack hands out, in connected accounts by design.
        Action   = ["sts:AssumeRole"] # nosemgrep
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
        Sid    = "BuildImages"
        Effect = "Allow"
        # Read-only, this account's build asset buckets, runs/ prefix only.
        Action   = ["s3:GetObject"] # nosemgrep
        Resource = "arn:aws:s3:::agentcore-${replace(local.build_agent_prefix, "_", "-")}*-assets-${local.account_id}/runs/*"
      }
    ]
  })
}
