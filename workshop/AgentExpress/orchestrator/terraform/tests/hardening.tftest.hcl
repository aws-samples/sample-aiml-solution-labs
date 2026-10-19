# The organisation guardrails (hardening.tf), planned offline: every provider is mocked, so
# no credentials and no AWS call. `terraform test` from orchestrator/terraform/ (about 3 s);
# tests/test_hardening.py runs it as part of pytest.
#
# Each run plans a TARGETED set: the UI upload (ui.tf) is a for_each over files a build step
# makes at apply time, which is why a real first deploy is two applies, and no plan of the
# whole stack can be made before it.
#
# What it proves: unset, the settings leave every role at path "/" with no boundary but its
# own, CloudFront on its own certificate with no alias and no web ACL. Set, every role
# carries them, and each variable refuses a bad value.

mock_provider "aws" {
  # The ARNs the deploy role's policy names, known at PLAN time so it can be read in an
  # assertion (a mocked value is otherwise made up at apply).
  override_during = plan
  mock_resource "aws_iam_role" {
    defaults = { arn = "arn:aws:iam::123456789012:role/mock" }
  }
  mock_resource "aws_cloudwatch_log_group" {
    defaults = { arn = "arn:aws:logs:us-east-1:123456789012:log-group:mock" }
  }
  mock_resource "aws_dynamodb_table" {
    defaults = { arn = "arn:aws:dynamodb:us-east-1:123456789012:table/mock" }
  }
  mock_resource "aws_s3_bucket" {
    defaults = { arn = "arn:aws:s3:::mock", id = "mock" }
  }
  mock_data "aws_caller_identity" {
    defaults = { account_id = "123456789012" }
  }
  mock_data "aws_iam_policy_document" {
    defaults = { json = "{}" }
  }
}
mock_provider "awscc" {}
mock_provider "null" {}
mock_provider "external" {
  mock_data "external" {
    defaults = { result = {} }
  }
}
mock_provider "archive" {}
mock_provider "time" {}

variables {
  region = "us-east-1"
}

run "unset_changes_nothing" {
  command = plan
  plan_options {
    target = [
      aws_iam_role.a2a_lambda, aws_iam_role.bff, aws_iam_role.deploy, aws_iam_role.memory,
      aws_iam_role.gateway, aws_iam_role.kb, aws_iam_role.kb_lambda, aws_iam_role.runtime,
      aws_iam_role.signup, aws_iam_role.subagent, aws_iam_role.tool_lambda, aws_iam_role.tool_code,
      aws_iam_role.trigger_scheduler, aws_iam_role_policy.deploy, aws_iam_role_policy.deploy_boundary,
      aws_cloudfront_distribution.ui, aws_wafv2_web_acl.ui, aws_codebuild_project.deploy,
      aws_cognito_user_pool_client.spa, aws_s3_bucket_cors_configuration.builds,
    ]
  }
  assert {
    condition = alltrue(flatten([
      [for r in aws_iam_role.a2a_lambda : r.path == "/" && r.permissions_boundary == null],
      [aws_iam_role.bff.path == "/" && aws_iam_role.bff.permissions_boundary == null],
      [for r in aws_iam_role.deploy : r.path == "/" && r.permissions_boundary == null],
      [for r in aws_iam_role.memory : r.path == "/" && r.permissions_boundary == null],
      [for r in aws_iam_role.gateway : r.path == "/" && r.permissions_boundary == null],
      [for r in aws_iam_role.kb : r.path == "/" && r.permissions_boundary == null],
      [for r in aws_iam_role.kb_lambda : r.path == "/" && r.permissions_boundary == null],
      [for r in aws_iam_role.runtime : r.path == "/" && r.permissions_boundary == null],
      [for r in aws_iam_role.signup : r.path == "/" && r.permissions_boundary == null],
      [for r in values(aws_iam_role.subagent) : r.path == "/" && r.permissions_boundary == null],
      [for r in values(aws_iam_role.tool_lambda) : r.path == "/" && r.permissions_boundary == null],
      [for r in aws_iam_role.trigger_scheduler : r.path == "/" && r.permissions_boundary == null],
    ]))
    error_message = "With the settings unset, a role got a path other than / or a boundary."
  }
  assert {
    condition     = alltrue([for k, r in aws_iam_role.tool_code : r.path == "/" && r.permissions_boundary == aws_iam_policy.tool_code_boundary[k].arn])
    error_message = "A code tool's role lost its own boundary."
  }
  assert {
    condition     = length(aws_iam_role_policy.deploy_boundary) == 0 && length(aws_wafv2_web_acl.ui) == 0
    error_message = "A guardrail resource exists with its setting unset."
  }
  assert {
    condition     = aws_cloudfront_distribution.ui.aliases == null && aws_cloudfront_distribution.ui.web_acl_id == null
    error_message = "The distribution got an alias or a web ACL with the settings unset."
  }
  assert {
    condition = alltrue([for v in aws_cloudfront_distribution.ui.viewer_certificate :
      v.cloudfront_default_certificate && v.acm_certificate_arn == null && v.minimum_protocol_version == null && v.ssl_support_method == null
    ])
    error_message = "The viewer certificate changed with custom_domain unset."
  }
  assert {
    condition     = length(aws_cognito_user_pool_client.spa[0].callback_urls) == 1
    error_message = "The Hosted UI callbacks gained a URL with custom_domain unset."
  }
  assert {
    condition = alltrue([for p in aws_codebuild_project.deploy : alltrue([for e in p.environment : alltrue([for v in e.environment_variable :
    !contains(["PERMISSIONS_BOUNDARY_ARN", "IAM_ROLE_PATH"], v.name)])])])
    error_message = "The deploy project passes a guardrail to builds that was never set."
  }
  assert {
    condition     = !strcontains(aws_iam_role_policy.deploy[0].policy, "iam:PermissionsBoundary") && strcontains(aws_iam_role_policy.deploy[0].policy, "DenySelfEscalation")
    error_message = "The deploy role's own policy changed with the settings unset."
  }
}

run "boundary_and_path_on_every_role" {
  command = plan
  variables {
    iam_permissions_boundary_arn = "arn:aws:iam::123456789012:policy/OrgBoundary"
    iam_role_path                = "/agentexpress/"
  }
  plan_options {
    target = [
      aws_iam_role.a2a_lambda, aws_iam_role.bff, aws_iam_role.deploy, aws_iam_role.memory,
      aws_iam_role.gateway, aws_iam_role.kb, aws_iam_role.kb_lambda, aws_iam_role.runtime,
      aws_iam_role.signup, aws_iam_role.subagent, aws_iam_role.tool_lambda, aws_iam_role.tool_code,
      aws_iam_role.trigger_scheduler, aws_iam_role_policy.deploy_boundary, aws_iam_policy.deploy,
      aws_codebuild_project.deploy,
    ]
  }
  assert {
    condition = alltrue(flatten([
      [for r in aws_iam_role.a2a_lambda : r.path == "/agentexpress/" && r.permissions_boundary == var.iam_permissions_boundary_arn],
      [aws_iam_role.bff.path == "/agentexpress/" && aws_iam_role.bff.permissions_boundary == var.iam_permissions_boundary_arn],
      [for r in aws_iam_role.deploy : r.path == "/agentexpress/" && r.permissions_boundary == var.iam_permissions_boundary_arn],
      [for r in aws_iam_role.memory : r.path == "/agentexpress/" && r.permissions_boundary == var.iam_permissions_boundary_arn],
      [for r in aws_iam_role.gateway : r.path == "/agentexpress/" && r.permissions_boundary == var.iam_permissions_boundary_arn],
      [for r in aws_iam_role.kb : r.path == "/agentexpress/" && r.permissions_boundary == var.iam_permissions_boundary_arn],
      [for r in aws_iam_role.kb_lambda : r.path == "/agentexpress/" && r.permissions_boundary == var.iam_permissions_boundary_arn],
      [for r in aws_iam_role.runtime : r.path == "/agentexpress/" && r.permissions_boundary == var.iam_permissions_boundary_arn],
      [for r in aws_iam_role.signup : r.path == "/agentexpress/" && r.permissions_boundary == var.iam_permissions_boundary_arn],
      [for r in values(aws_iam_role.subagent) : r.path == "/agentexpress/" && r.permissions_boundary == var.iam_permissions_boundary_arn],
      [for r in values(aws_iam_role.tool_lambda) : r.path == "/agentexpress/" && r.permissions_boundary == var.iam_permissions_boundary_arn],
      [for r in values(aws_iam_role.tool_code) : r.path == "/agentexpress/" && r.permissions_boundary == var.iam_permissions_boundary_arn],
      [for r in aws_iam_role.trigger_scheduler : r.path == "/agentexpress/" && r.permissions_boundary == var.iam_permissions_boundary_arn],
    ]))
    error_message = "A role is missing the boundary or the path."
  }
  assert {
    condition     = length(aws_iam_role_policy.deploy_boundary) == 1 && strcontains(aws_iam_role_policy.deploy_boundary[0].policy, "\"ArnNotEquals\":{\"iam:PermissionsBoundary\":\"arn:aws:iam::123456789012:policy/OrgBoundary\"}")
    error_message = "The deploy role does not require the boundary on the roles it creates."
  }
  assert {
    condition     = alltrue([for p in aws_iam_policy.deploy : !strcontains(p.policy, ":role/AgentCore")]) && anytrue([for p in aws_iam_policy.deploy : strcontains(p.policy, ":role/agentexpress/AgentCoreRuntime-*")])
    error_message = "The deploy policy still names roles at / rather than under iam_role_path."
  }
  assert {
    condition = alltrue([for p in aws_codebuild_project.deploy : alltrue([for e in p.environment :
    length([for v in e.environment_variable : v if contains(["PERMISSIONS_BOUNDARY_ARN", "IAM_ROLE_PATH"], v.name)]) == 2])])
    error_message = "The deploy project does not pass the boundary and path on to builds."
  }
}

run "custom_domain_with_tls_1_2" {
  command = plan
  variables {
    custom_domain       = "agents.example.com"
    acm_certificate_arn = "arn:aws:acm:us-east-1:123456789012:certificate/0f1e2d3c-4b5a-6978-8a9b-0c1d2e3f4a5b"
  }
  plan_options {
    target = [aws_cloudfront_distribution.ui, aws_cognito_user_pool_client.spa, aws_s3_bucket_cors_configuration.builds]
  }
  assert {
    condition     = aws_cloudfront_distribution.ui.aliases == toset(["agents.example.com"])
    error_message = "The custom domain is not an alias of the distribution."
  }
  assert {
    condition = alltrue([for v in aws_cloudfront_distribution.ui.viewer_certificate :
      !v.cloudfront_default_certificate && v.acm_certificate_arn == var.acm_certificate_arn && v.minimum_protocol_version == "TLSv1.2_2021" && v.ssl_support_method == "sni-only"
    ])
    error_message = "The custom domain is not served with its certificate, SNI and TLS 1.2 (2021)."
  }
  assert {
    condition     = contains(aws_cognito_user_pool_client.spa[0].callback_urls, "https://agents.example.com") && contains(aws_cognito_user_pool_client.spa[0].logout_urls, "https://agents.example.com")
    error_message = "Sign-in on the custom domain would be refused: it is not a Hosted UI callback."
  }
  assert {
    condition     = alltrue([for c in aws_s3_bucket_cors_configuration.builds : anytrue([for r in c.cors_rule : contains(r.allowed_origins, "https://agents.example.com")])])
    error_message = "Uploads from the custom domain would be refused by the builds bucket's CORS."
  }
}

run "waf_on_the_distribution" {
  command = plan
  variables {
    enable_waf     = true
    waf_rate_limit = 5000
  }
  plan_options {
    target = [aws_cloudfront_distribution.ui, aws_wafv2_web_acl.ui]
  }
  assert {
    condition     = aws_wafv2_web_acl.ui[0].scope == "CLOUDFRONT" && length(aws_wafv2_web_acl.ui[0].rule) == 4
    error_message = "The web ACL is not a CLOUDFRONT ACL with the rate rule and three managed groups."
  }
  assert {
    condition     = anytrue([for r in aws_wafv2_web_acl.ui[0].rule : anytrue([for s in r.statement : anytrue([for b in s.rate_based_statement : b.limit == 5000])])])
    error_message = "The rate-based rule does not use waf_rate_limit."
  }
}

run "waf_refused_outside_us_east_1" {
  command = plan
  variables {
    region     = "eu-west-1"
    enable_waf = true
  }
  # A -target plan checks only the variables its targets read, so target readers of each.
  plan_options {
    target = [aws_cloudfront_distribution.ui, aws_iam_role.bff]
  }
  expect_failures = [var.enable_waf]
}

run "boundary_must_be_a_policy_arn" {
  command = plan
  variables {
    iam_permissions_boundary_arn = "arn:aws:iam::123456789012:role/NotAPolicy"
  }
  # A -target plan checks only the variables its targets read, so target readers of each.
  plan_options {
    target = [aws_cloudfront_distribution.ui, aws_iam_role.bff]
  }
  expect_failures = [var.iam_permissions_boundary_arn]
}

run "path_must_be_slashed_and_plain" {
  command = plan
  variables {
    iam_role_path = "/a*b/"
  }
  # A -target plan checks only the variables its targets read, so target readers of each.
  plan_options {
    target = [aws_cloudfront_distribution.ui, aws_iam_role.bff]
  }
  expect_failures = [var.iam_role_path]
}

run "domain_needs_a_certificate" {
  command = plan
  variables {
    custom_domain = "agents.example.com"
  }
  # A -target plan checks only the variables its targets read, so target readers of each.
  plan_options {
    target = [aws_cloudfront_distribution.ui, aws_iam_role.bff]
  }
  expect_failures = [var.acm_certificate_arn]
}

run "certificate_must_be_in_us_east_1" {
  command = plan
  variables {
    custom_domain       = "agents.example.com"
    acm_certificate_arn = "arn:aws:acm:eu-west-1:123456789012:certificate/0f1e2d3c-4b5a-6978-8a9b-0c1d2e3f4a5b"
  }
  # A -target plan checks only the variables its targets read, so target readers of each.
  plan_options {
    target = [aws_cloudfront_distribution.ui, aws_iam_role.bff]
  }
  expect_failures = [var.acm_certificate_arn]
}

run "domain_must_be_a_host_name" {
  command = plan
  variables {
    custom_domain       = "https://agents.example.com/"
    acm_certificate_arn = "arn:aws:acm:us-east-1:123456789012:certificate/0f1e2d3c-4b5a-6978-8a9b-0c1d2e3f4a5b"
  }
  # A -target plan checks only the variables its targets read, so target readers of each.
  plan_options {
    target = [aws_cloudfront_distribution.ui, aws_iam_role.bff]
  }
  expect_failures = [var.custom_domain]
}
