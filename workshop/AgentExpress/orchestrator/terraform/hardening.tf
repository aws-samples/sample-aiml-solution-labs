# Organisation guardrails, for accounts whose rules require them. Every one is OFF by default
# (variables.tf), and unset it changes nothing: no boundary, path "/", the CloudFront
# certificate and URL, no web ACL. Mirrors cdk/lib/hardening.ts; tests/test_hardening.py and
# cdk/test/hardening.test.ts keep the two in step.
#
#   * iam_permissions_boundary_arn: the boundary of EVERY aws_iam_role here (each role block
#     names local.iam_permissions_boundary), passed to the deploy runner so a build's roles get
#     it too, and REQUIRED by the deploy role's own policy (below).
#   * iam_role_path: the path of every role. Changing it replaces the roles.
#   * custom_domain + acm_certificate_arn: a CloudFront alias with TLS 1.2 (2021 policy) and SNI;
#     the Hosted UI callbacks and the buckets' CORS accept it alongside the CloudFront URL.
#   * enable_waf: a CLOUDFRONT web ACL on the distribution (us-east-1 stacks only).
locals {
  iam_permissions_boundary = var.iam_permissions_boundary_arn == "" ? null : var.iam_permissions_boundary_arn
  # Where people open the console: the custom domain when there is one. The CloudFront URL
  # keeps working, so every allow-list takes both (local.ui_origins).
  ui_origin  = var.custom_domain == "" ? "https://${aws_cloudfront_distribution.ui.domain_name}" : "https://${var.custom_domain}"
  ui_origins = concat(["https://${aws_cloudfront_distribution.ui.domain_name}"], var.custom_domain == "" ? [] : ["https://${var.custom_domain}"])
  # The oldest TLS a viewer may use on the custom domain. CloudFront's own certificate is
  # fixed by CloudFront and cannot be raised. Mirrors MIN_VIEWER_TLS in cdk/lib/hardening.ts.
  min_viewer_tls = "TLSv1.2_2021"
  # The deploy role may create and change roles only with the boundary on them, and may not
  # take it off or edit the boundary policy. Deny statements, so they hold whatever else the
  # role is granted. Resource "*" because the condition is the scope: any role, in any account
  # the role can reach, that lacks the boundary. Mirrors boundaryEnforcementStatements.
  # A for-filter, not a conditional: [] and these three objects are different types.
  boundary_enforcement = [for s in [
    {
      Sid       = "RolesOnlyWithTheBoundary"
      Effect    = "Deny"
      Action    = ["iam:CreateRole", "iam:PutRolePolicy", "iam:AttachRolePolicy", "iam:PutRolePermissionsBoundary"]
      Resource  = "*"
      Condition = { ArnNotEquals = { "iam:PermissionsBoundary" = local.iam_permissions_boundary } }
    },
    {
      Sid      = "KeepTheBoundary"
      Effect   = "Deny"
      Action   = ["iam:DeleteRolePermissionsBoundary"]
      Resource = "*"
    },
    {
      Sid      = "LeaveTheBoundaryPolicyAlone"
      Effect   = "Deny"
      Action   = ["iam:CreatePolicyVersion", "iam:DeletePolicyVersion", "iam:SetDefaultPolicyVersion", "iam:DeletePolicy"]
      Resource = local.iam_permissions_boundary
    },
  ] : s if local.iam_permissions_boundary != null]
  # What the deploy runner passes on to every build it deploys (deployer/runner.py).
  build_guardrail_env = merge(
    local.iam_permissions_boundary == null ? {} : { PERMISSIONS_BOUNDARY_ARN = local.iam_permissions_boundary },
    var.iam_role_path == "/" ? {} : { IAM_ROLE_PATH = var.iam_role_path },
  )
  # AWS managed rule groups on the web ACL, in priority order. Mirrors WAF_MANAGED_RULE_GROUPS.
  waf_managed_rule_groups = ["AWSManagedRulesAmazonIpReputationList", "AWSManagedRulesCommonRuleSet", "AWSManagedRulesKnownBadInputsRuleSet"]
}

resource "aws_iam_role_policy" "deploy_boundary" {
  count  = local.builder_enabled && local.iam_permissions_boundary != null ? 1 : 0
  name   = "AgentCoreDeployBoundary-${var.agent_name}"
  role   = aws_iam_role.deploy[0].id
  policy = jsonencode({ Version = "2012-10-17", Statement = local.boundary_enforcement })
}

# --- AWS WAF on the console's CloudFront distribution ----------------------
# A web ACL for CloudFront has scope CLOUDFRONT, which exists only in us-east-1; enable_waf
# refuses any other region (variables.tf). The HTTP API behind /api/* cannot take a web ACL
# (API Gateway associates WAF with REST APIs only), so /api/* is covered here, on its way
# through CloudFront. Calls straight to the execute-api URL are not, and still need a JWT.
resource "aws_wafv2_web_acl" "ui" {
  count       = var.enable_waf ? 1 : 0
  name        = "agentcore-${replace(var.agent_name, "_", "-")}-ui"
  description = "AgentExpress console: AWS managed rule groups and a per-IP rate limit"
  scope       = "CLOUDFRONT"

  default_action {
    allow {}
  }

  rule {
    name     = "RateLimitPerIp"
    priority = 0
    action {
      block {}
    }
    statement {
      rate_based_statement {
        limit              = var.waf_rate_limit
        aggregate_key_type = "IP"
      }
    }
    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "RateLimitPerIp"
      sampled_requests_enabled   = true
    }
  }

  dynamic "rule" {
    for_each = local.waf_managed_rule_groups
    content {
      name     = rule.value
      priority = rule.key + 1
      override_action {
        none {}
      }
      statement {
        managed_rule_group_statement {
          vendor_name = "AWS"
          name        = rule.value
          # The Common rule set blocks request bodies over 8 KB, and saving a build or a
          # workflow sends more than that. Counted instead of blocked, so the console works.
          dynamic "rule_action_override" {
            for_each = rule.value == "AWSManagedRulesCommonRuleSet" ? ["SizeRestrictions_BODY"] : []
            content {
              name = rule_action_override.value
              action_to_use {
                count {}
              }
            }
          }
        }
      }
      visibility_config {
        cloudwatch_metrics_enabled = true
        metric_name                = rule.value
        sampled_requests_enabled   = true
      }
    }
  }

  visibility_config {
    cloudwatch_metrics_enabled = true
    metric_name                = "agentcore-${replace(var.agent_name, "_", "-")}-ui"
    sampled_requests_enabled   = true
  }
}
