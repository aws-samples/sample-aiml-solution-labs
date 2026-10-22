# --- Hosted MCP servers (tools.<key>.hosted) ----------------------------------------
# A tool `{ type = "mcp", hosted = { code?, network? } }` is an MCP server AgentExpress runs on
# its own AgentCore Runtime (protocol MCP) and registers as an MCP server target of the
# build's Gateway (tools.tf mcp_server: endpoint derived here, auth "sigv4" as the gateway role
# with service "bedrock-agentcore"). Mirrors cdk/lib/hosted-mcp.ts; the AWS facts and their
# documentation URLs are in that file's header. In short:
#   * awscc_bedrockagentcore_runtime (awscc 1.101.0): protocol_configuration = "MCP" (a String;
#     values MCP | HTTP | A2A | AGUI, https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-bedrockagentcore-runtime.html),
#     agent_runtime_artifact.container_configuration.container_uri
#     (https://registry.terraform.io/providers/hashicorp/awscc/1.101.0/docs/resources/bedrockagentcore_runtime).
#   * A container on ARM64, server on 0.0.0.0:8000/mcp, streamable HTTP
#     (https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-mcp-protocol-contract.html).
#   * Endpoint https://bedrock-agentcore.{region}.amazonaws.com/runtimes/{URL-encoded ARN}/invocations?qualifier=DEFAULT
#     (https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-mcp.html).
#   * The gateway role invokes the runtime: bedrock-agentcore:InvokeAgentRuntime
#     (https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-oauth.html).
locals {
  # Only when the Gateway exists: local.mcp_tools is empty without it (tools.tf).
  hosted_mcp_tools = { for n, t in local.mcp_tools : n => local.tools_raw[n] if t.hosted }
  # An image the build brings (hosted.code.image), used as is; else built from
  # app/tools/_code/<key>/ (written in the build, or fetched there by the deployer).
  hosted_mcp_images = { for n, t in local.hosted_mcp_tools : n => tostring(t.hosted.code.image) if can(t.hosted.code.image) }
  hosted_mcp_built  = { for n, t in local.hosted_mcp_tools : n => t if !can(t.hosted.code.image) }
  hosted_mcp_dirs   = { for n, t in local.hosted_mcp_built : n => "${path.module}/../app/tools/_code/${n}" }
  hosted_mcp_hashes = { for n, d in local.hosted_mcp_dirs : n => substr(sha1(join("", [
    for f in sort(fileset(d, "**")) : filesha1("${d}/${f}")
    if !can(regex("(^|/)__pycache__/", f)) && !endswith(f, ".pyc") && f != ".from-builder"
  ])), 0, 12) }
  hosted_mcp_uris = merge(local.hosted_mcp_images, {
    for n, _ in local.hosted_mcp_built : n => "${aws_ecr_repository.hosted_mcp[n].repository_url}:${local.hosted_mcp_hashes[n]}"
  })
  # Each hosted server's network, in runtime_network.tf's shape and checks.
  hosted_mcp_networks = { for n, t in local.hosted_mcp_tools : n => {
    mode    = try(t.hosted.network, null) == null ? "PUBLIC" : tostring(try(t.hosted.network.mode, ""))
    subnets = [for s in try(t.hosted.network.subnetIds, []) : tostring(s)]
    groups  = [for s in try(t.hosted.network.securityGroupIds, []) : tostring(s)]
  } }
  # The endpoint the Gateway target calls: the runtime ARN URL-encoded (":" %3A, "/" %2F).
  hosted_mcp_endpoints = { for n, r in awscc_bedrockagentcore_runtime.hosted_mcp : n =>
    "https://bedrock-agentcore.${var.region}.amazonaws.com/runtimes/${urlencode(r.agent_runtime_arn)}/invocations?qualifier=DEFAULT"
  }
}

resource "aws_ecr_repository" "hosted_mcp" {
  for_each             = local.hosted_mcp_built
  name                 = "${local.ecr_repo}-mcp-${lower(each.key)}"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = true
  image_scanning_configuration {
    scan_on_push = true
  }
  encryption_configuration {
    encryption_type = "KMS"
    kms_key         = aws_kms_key.data.arn
  }
}

# Built and pushed like the orchestrator image (main.tf build_push), with var.container_engine.
resource "null_resource" "hosted_mcp_build_push" {
  for_each = local.hosted_mcp_built
  triggers = {
    image_uri = local.hosted_mcp_uris[each.key]
  }
  provisioner "local-exec" {
    working_dir = local.hosted_mcp_dirs[each.key]
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      set -euo pipefail
      REG="${local.account_id}.dkr.ecr.${var.region}.amazonaws.com"
      aws ecr get-login-password --region ${var.region} \
        | ${var.container_engine} login --username AWS --password-stdin "$REG"
      if aws ecr describe-images --region ${var.region} --repository-name "${aws_ecr_repository.hosted_mcp[each.key].name}" \
          --image-ids imageTag="${local.hosted_mcp_hashes[each.key]}" >/dev/null 2>&1; then
        echo "image ${local.hosted_mcp_hashes[each.key]} is already there; not rebuilding"
        exit 0
      fi
      ${var.container_engine} build --platform linux/arm64 -t "${local.hosted_mcp_uris[each.key]}" .
      ${var.container_engine} push "${local.hosted_mcp_uris[each.key]}"
    EOT
  }
}

resource "aws_iam_role" "hosted_mcp" {
  for_each = local.hosted_mcp_tools
  name     = substr("AgentCoreMcp-${var.agent_name}-${each.key}", 0, 64)
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
}

# Pull its image, and logs, spans and metrics as every Runtime of the build. The server's own
# AWS calls are not in v1.
resource "aws_iam_role_policy" "hosted_mcp" {
  for_each = local.hosted_mcp_tools
  name     = "HostedMcp-${each.key}"
  role     = aws_iam_role.hosted_mcp[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["ecr:GetAuthorizationToken"], Resource = "*" },
      # The ECR pull actions of the AgentCore Runtime execution role, as CDK grants them
      # (https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-permissions.html).
      {
        Effect = "Allow", Action = ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"]
        Resource = contains(keys(local.hosted_mcp_images), each.key) ? (
          "arn:aws:ecr:${regex("^\\d{12}\\.dkr\\.ecr\\.([a-z0-9-]+)\\.", local.hosted_mcp_images[each.key])[0]}:${regex("^(\\d{12})\\.", local.hosted_mcp_images[each.key])[0]}:repository/${regex("^[^/]+/([a-z0-9._/-]+?)(?::[\\w.-]+|@sha256:[0-9a-f]{64})?$", local.hosted_mcp_images[each.key])[0]}"
        ) : aws_ecr_repository.hosted_mcp[each.key].arn
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams", "logs:DescribeLogGroups"]
        Resource = "arn:aws:logs:${var.region}:${local.account_id}:log-group:/aws/bedrock-agentcore/runtimes/*"
      },
      { Effect = "Allow", Action = ["xray:PutTraceSegments", "xray:PutTelemetryRecords", "xray:GetSamplingRules", "xray:GetSamplingTargets"], Resource = "*" },
      {
        Effect    = "Allow", Action = ["cloudwatch:PutMetricData"], Resource = "*"
        Condition = { StringEquals = { "cloudwatch:namespace" = "bedrock-agentcore" } }
      },
    ]
  })
}

resource "time_sleep" "hosted_mcp_iam_propagation" {
  count           = length(local.hosted_mcp_tools) > 0 ? 1 : 0
  depends_on      = [aws_iam_role_policy.hosted_mcp]
  create_duration = "25s"
}

resource "awscc_bedrockagentcore_runtime" "hosted_mcp" {
  for_each           = local.hosted_mcp_tools
  agent_runtime_name = "${var.agent_name}_mcp_${each.key}"
  description        = "Hosted MCP server for tool ${each.key}"
  role_arn           = aws_iam_role.hosted_mcp[each.key].arn
  agent_runtime_artifact = {
    container_configuration = {
      container_uri = local.hosted_mcp_uris[each.key]
    }
  }
  protocol_configuration = "MCP"
  network_configuration = {
    network_mode = local.hosted_mcp_networks[each.key].mode
    network_mode_config = local.hosted_mcp_networks[each.key].mode == "VPC" ? {
      subnets         = local.hosted_mcp_networks[each.key].subnets
      security_groups = local.hosted_mcp_networks[each.key].groups
    } : null
  }
  environment_variables = { AWS_REGION = var.region }
  depends_on            = [null_resource.hosted_mcp_build_push, time_sleep.hosted_mcp_iam_propagation]
  lifecycle {
    precondition {
      condition     = can(regex("^[a-zA-Z][a-zA-Z0-9_]{0,47}$", "${var.agent_name}_mcp_${each.key}"))
      error_message = "workflow.json tools.${each.key}.hosted: the runtime name ${var.agent_name}_mcp_${each.key} must match [a-zA-Z][a-zA-Z0-9_]{0,47} (AgentRuntimeName); use a shorter tool key."
    }
    precondition {
      condition = (contains(local.network_vocab.values, local.hosted_mcp_networks[each.key].mode)
        && (local.hosted_mcp_networks[each.key].mode == "VPC"
          ? alltrue([for k, ids in { s = local.hosted_mcp_networks[each.key].subnets, g = local.hosted_mcp_networks[each.key].groups } :
            length(ids) >= 1 && length(ids) <= local.network_vocab.maxIds && alltrue([for x in ids :
          can(regex(k == "s" ? local.network_vocab.subnetPattern : local.network_vocab.securityGroupPattern, x))])])
      : length(concat(local.hosted_mcp_networks[each.key].subnets, local.hosted_mcp_networks[each.key].groups)) == 0))
      error_message = "workflow.json tools.${each.key}.hosted.network: mode must be one of ${join(", ", local.network_vocab.values)}; mode \"VPC\" needs 1-${local.network_vocab.maxIds} subnetIds and securityGroupIds, and they apply to VPC only."
    }
  }
}

# The gateway role is the principal that invokes the runtime.
resource "aws_iam_role_policy" "gateway_hosted_mcp" {
  count = length(local.hosted_mcp_tools) > 0 ? 1 : 0
  name  = "GatewayHostedMcp-${var.agent_name}"
  role  = aws_iam_role.gateway[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "InvokeHostedMcp"
      Effect   = "Allow"
      Action   = ["bedrock-agentcore:InvokeAgentRuntime"]
      Resource = flatten([for r in awscc_bedrockagentcore_runtime.hosted_mcp : [r.agent_runtime_arn, "${r.agent_runtime_arn}/*"]])
    }]
  })
}
