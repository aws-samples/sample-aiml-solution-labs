# --- type = "lambda" with `code`: a function written in the build ---------------------
# Its files are app/tools/_code/<key>/ (scaffold.py apply wrote them from the bundle; the
# deploy runner pip-installed its requirements into the same folder). Its role is logs
# plus ONLY the grants workflow.json names, and the same statements are its permissions
# boundary, with an explicit Deny on the framework's own tables, secrets and buckets — so
# nothing attached to the role later can reach further. Mirrors codeToolStatements and
# CODE_BOUNDARY_DENY in cdk/lib/tool-plane.ts.
locals {
  code_tool_fn     = { for n, t in local.code_lambda_tools : n => "ToolLambda-${var.agent_name}-${n}" }
  code_tool_grants = { for n, t in local.code_lambda_tools : n => try(t.code.grants, {}) }
  # An allowlist, not just the deny below: a code tool reaches a secret, table or bucket
  # only if its owner tagged it agentexpress:code-tools=true (a bucket also needs ABAC
  # turned on: aws s3api put-bucket-abac). Anything else in the account stays out of
  # reach whatever a build names. Mirrors CODE_GRANTABLE in cdk/lib/tool-plane.ts.
  code_grantable = { StringEquals = { "aws:ResourceTag/agentexpress:code-tools" = "true" } }
  code_tool_s3 = { for n, g in local.code_tool_grants : n => {
    bucket = split("/", replace(try(g.s3Prefix, ""), "s3://", ""))[0]
    prefix = join("/", slice(split("/", replace(try(g.s3Prefix, ""), "s3://", "")), 1, length(split("/", replace(try(g.s3Prefix, ""), "s3://", "")))))
  } }
  code_tool_statements = { for n, g in local.code_tool_grants : n => concat(
    [{
      Sid      = "OwnLogs"
      Effect   = "Allow"
      Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
      Resource = ["arn:aws:logs:${var.region}:${local.account_id}:log-group:/aws/lambda/${local.code_tool_fn[n]}:*"]
    }],
    # Only when granted. A for-filter, not a conditional: the two results would
    # be tuples of different types, which Terraform refuses to unify.
    [for s in [{
      Sid       = "ReadItsSecret"
      Effect    = "Allow"
      Action    = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
      Resource  = ["arn:aws:secretsmanager:${var.region}:${local.account_id}:secret:${try(g.secret, "")}-??????"]
      Condition = local.code_grantable
    }] : s if !(try(g.secret, "") == "")],
    # Only when granted. A for-filter, not a conditional: the two results would
    # be tuples of different types, which Terraform refuses to unify.
    [for s in [{
      Sid    = "ItsTable"
      Effect = "Allow"
      Action = concat(["dynamodb:GetItem", "dynamodb:BatchGetItem", "dynamodb:Query", "dynamodb:Scan", "dynamodb:DescribeTable"],
        try(g.tableAccess, "read") == "readwrite" ? ["dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem",
      "dynamodb:BatchWriteItem", "dynamodb:ConditionCheckItem"] : [])
      Resource = ["arn:aws:dynamodb:${var.region}:${local.account_id}:table/${try(g.table, "")}",
      "arn:aws:dynamodb:${var.region}:${local.account_id}:table/${try(g.table, "")}/index/*"]
      Condition = local.code_grantable
    }] : s if !(try(g.table, "") == "")],
    # Only when granted. A for-filter, not a conditional: the two results would
    # be tuples of different types, which Terraform refuses to unify.
    [for s in [{
      Sid       = "ItsPrefix"
      Effect    = "Allow"
      Action    = try(g.s3Access, "read") == "readwrite" ? ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"] : ["s3:GetObject"]
      Resource  = ["arn:aws:s3:::${local.code_tool_s3[n].bucket}/${local.code_tool_s3[n].prefix}*"]
      Condition = local.code_grantable
      }, {
      Sid       = "ListItsPrefix"
      Effect    = "Allow"
      Action    = ["s3:ListBucket"]
      Resource  = ["arn:aws:s3:::${local.code_tool_s3[n].bucket}"]
      Condition = merge(local.code_grantable, { StringLike = { "s3:prefix" = ["${local.code_tool_s3[n].prefix}*"] } })
    }] : s if !(try(g.s3Prefix, "") == "")],
    # Only when granted. A for-filter, not a conditional: the two results would
    # be tuples of different types, which Terraform refuses to unify.
    [for s in [{
      Sid    = "AttachToItsVpc"
      Effect = "Allow"
      Action = ["ec2:CreateNetworkInterface", "ec2:DescribeNetworkInterfaces", "ec2:DescribeSubnets",
      "ec2:DeleteNetworkInterface", "ec2:AssignPrivateIpAddresses", "ec2:UnassignPrivateIpAddresses"]
      Resource = ["*"]
    }] : s if !(try(g.vpc, null) == null)],
  ) }
  code_boundary_deny = [
    {
      Sid    = "NeverFrameworkTables"
      Effect = "Deny"
      Action = ["dynamodb:*"]
      Resource = flatten([for p in ["ax_*", "*_builds", "*_audit", "*_status", "*_events", "*_telemetry", "*_insights"] :
      ["arn:aws:dynamodb:*:*:table/${p}", "arn:aws:dynamodb:*:*:table/${p}/*"]])
    },
    {
      Sid      = "NeverFrameworkSecrets"
      Effect   = "Deny"
      Action   = ["secretsmanager:*"]
      Resource = ["arn:aws:secretsmanager:*:*:secret:agentexpress/*", "arn:aws:secretsmanager:*:*:secret:bedrock-agentcore*"]
    },
    {
      Sid      = "NeverFrameworkBuckets"
      Effect   = "Deny"
      Action   = ["s3:*"]
      Resource = ["arn:aws:s3:::agentcore-*", "arn:aws:s3:::*builderbuildsbucket*"]
    },
  ]
}

resource "aws_iam_policy" "tool_code_boundary" {
  for_each    = local.code_lambda_tools
  name        = "${local.code_tool_fn[each.key]}-boundary"
  description = "The most ${local.code_tool_fn[each.key]} may ever do: its grants in workflow.json, and never the framework's own data."
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = concat(local.code_tool_statements[each.key], local.code_boundary_deny)
  })
}

resource "aws_iam_role" "tool_code" {
  for_each             = local.code_lambda_tools
  name                 = local.code_tool_fn[each.key]
  permissions_boundary = aws_iam_policy.tool_code_boundary[each.key].arn
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "tool_code" {
  for_each = local.code_lambda_tools
  name     = "grants"
  role     = aws_iam_role.tool_code[each.key].id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = local.code_tool_statements[each.key]
  })
}

resource "aws_cloudwatch_log_group" "tool_code" {
  for_each          = local.code_lambda_tools
  name              = "/aws/lambda/${local.code_tool_fn[each.key]}"
  retention_in_days = var.log_retention_days
}

data "archive_file" "tool_code" {
  for_each    = local.code_lambda_tools
  type        = "zip"
  source_dir  = "${path.module}/../app/tools/_code/${each.key}"
  output_path = "${path.module}/.build/code-${each.key}.zip"
  excludes    = ["__pycache__", ".from-builder", "events.json"]
}

resource "aws_lambda_function" "tool_code" {
  for_each         = local.code_lambda_tools
  function_name    = local.code_tool_fn[each.key]
  role             = aws_iam_role.tool_code[each.key].arn
  runtime          = "python3.12"
  handler          = "handler.lambda_handler"
  filename         = data.archive_file.tool_code[each.key].output_path
  source_code_hash = data.archive_file.tool_code[each.key].output_base64sha256
  timeout          = try(each.value.code.timeoutSeconds, 30)
  memory_size      = try(each.value.code.memoryMB, 256)
  environment {
    variables = merge(
      try(each.value.code.environment, {}),
      try(local.code_tool_grants[each.key].secret, "") == "" ? {} : { SECRET_NAME = local.code_tool_grants[each.key].secret },
      try(local.code_tool_grants[each.key].table, "") == "" ? {} : { TABLE_NAME = local.code_tool_grants[each.key].table },
      try(local.code_tool_grants[each.key].s3Prefix, "") == "" ? {} : { S3_PREFIX = local.code_tool_grants[each.key].s3Prefix },
    )
  }
  dynamic "vpc_config" {
    for_each = try(local.code_tool_grants[each.key].vpc, null) == null ? [] : [local.code_tool_grants[each.key].vpc]
    content {
      subnet_ids         = vpc_config.value.subnetIds
      security_group_ids = vpc_config.value.securityGroupIds
    }
  }
  depends_on = [aws_cloudwatch_log_group.tool_code, aws_iam_role_policy.tool_code]
}
