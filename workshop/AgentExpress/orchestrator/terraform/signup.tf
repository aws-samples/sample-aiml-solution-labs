# The Cognito trigger function (signup_lambda/handler.py), for two jobs. Mirrors the
# SignupTrigger in cdk/lib/orchestrator-stack.ts.
#  * Self sign-up (cognito.self_signup = true): post-confirmation puts each
#    self-registered user in the default group, so they can act on their own builds
#    and runs.
#  * The activity log (any deployment with its own pool): post-authentication records
#    each sign-in — on a console in the builds table, next to the deploys and destroys;
#    in a build's own app in its audit table (audit.tf), next to its runs.
# Nothing here exists without a pool of its own.
locals {
  self_signup       = local.create_cognito && try(var.cognito.self_signup, false)
  self_signup_group = try(var.cognito.self_signup_group, "members")
  audit_sign_ins    = local.create_cognito
  cognito_trigger   = local.self_signup || local.audit_sign_ins
}

data "archive_file" "signup" {
  count       = local.cognito_trigger ? 1 : 0
  type        = "zip"
  source_file = "${path.module}/../signup_lambda/handler.py"
  output_path = "${path.module}/.build/signup.zip"
}

resource "aws_iam_role" "signup" {
  count = local.cognito_trigger ? 1 : 0
  name  = "AgentCoreSignup-${var.agent_name}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "signup" {
  count = local.cognito_trigger ? 1 : 0
  name  = "AgentCoreSignup-${var.agent_name}"
  role  = aws_iam_role.signup[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.signup[0].arn}:*"
        }], local.self_signup ? [{
        # Any pool in this account/region, not the pool's ARN: the pool names this
        # function as its trigger, so naming the pool here would be a dependency cycle.
        Effect   = "Allow"
        Action   = ["cognito-idp:AdminAddUserToGroup"]
        Resource = "arn:aws:cognito-idp:${var.region}:${local.account_id}:userpool/*"
        }] : [], local.audit_sign_ins ? [{
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem"]
        Resource = local.audit_table_arn
    }] : [])
  })
}

resource "aws_cloudwatch_log_group" "signup" {
  count             = local.cognito_trigger ? 1 : 0
  name              = "/aws/lambda/AgentCoreSignup-${var.agent_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "signup" {
  count            = local.cognito_trigger ? 1 : 0
  function_name    = "AgentCoreSignup-${var.agent_name}"
  role             = aws_iam_role.signup[0].arn
  runtime          = "python3.13"
  handler          = "handler.handler"
  filename         = data.archive_file.signup[0].output_path
  source_code_hash = data.archive_file.signup[0].output_base64sha256
  timeout          = 10
  memory_size      = 128
  environment {
    variables = merge(
      local.self_signup ? { DEFAULT_GROUP = local.self_signup_group } : {},
      local.audit_sign_ins ? { AUDIT_TABLE = local.audit_table_name } : {},
    )
  }
  depends_on = [aws_cloudwatch_log_group.signup]
}

resource "aws_lambda_permission" "signup" {
  count         = local.cognito_trigger ? 1 : 0
  statement_id  = "AllowCognitoTriggers"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.signup[0].function_name
  principal     = "cognito-idp.amazonaws.com"
  source_arn    = aws_cognito_user_pool.this[0].arn
}

# The group must exist, or confirmation fails for every new user. And it must hold no
# action that reaches EVERY user's data (admin, audit, insights): anyone can sign up, so
# an admin is only ever made from the backend. Mirrors PRIVILEGED_ACTIONS in
# cdk/lib/orchestrator-stack.ts.
locals {
  privileged_actions = ["admin", "audit", "insights"]
  signup_privileges = [for a in local.privileged_actions : a
  if contains(try(local.authz_actions[a], []), local.self_signup_group)]
}

resource "terraform_data" "signup_group_check" {
  count = local.self_signup ? 1 : 0
  lifecycle {
    precondition {
      condition     = length(local.signup_privileges) == 0
      error_message = "cognito.self_signup puts every new user in group \"${local.self_signup_group}\", and app/workflow.json grants that group ${join(", ", local.signup_privileges)}, which reach every user's builds, runs or activity. Anyone who signs up would get them. Grant those to a group you fill from the backend only (e.g. \"admins\"), or set cognito.self_signup_group to a group without them."
    }
    precondition {
      condition     = contains(local.authz_groups, local.self_signup_group)
      error_message = "cognito.self_signup puts every new user in group \"${local.self_signup_group}\", but no action in app/workflow.json authorization.actions names it, so the group would not exist and sign-up confirmation would fail. Name it on the actions a new user may take (e.g. \"deploy\": [\"operators\", \"${local.self_signup_group}\"]), or set cognito.self_signup_group to a group that is named."
    }
  }
}
