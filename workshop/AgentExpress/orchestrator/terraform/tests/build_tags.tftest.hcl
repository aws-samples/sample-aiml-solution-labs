# build_tags (builder.tf), planned offline: every provider is mocked, so no credentials and
# no AWS call. `terraform test -filter=tests/build_tags.tftest.hcl` from
# orchestrator/terraform/; tests/test_build_tags.py runs it as part of pytest.
#
# What it proves: unset, the deploy project passes no BUILD_EXTRA_TAGS; set, it passes the
# tags as JSON with sorted keys (the bytes buildTagsEnv in cdk/lib/build-tags.ts writes),
# and each of the variable's rules refuses a bad value at plan. Each run targets the deploy
# project only: a -target plan checks only the variables its targets read, and it reads
# var.build_tags.

mock_provider "aws" {
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

run "unset_passes_nothing" {
  command = plan
  plan_options {
    target = [aws_codebuild_project.deploy]
  }
  assert {
    condition     = length(aws_codebuild_project.deploy) == 1
    error_message = "The deploy project is not planned."
  }
  assert {
    condition = alltrue([for p in aws_codebuild_project.deploy : alltrue([for e in p.environment : alltrue([for v in e.environment_variable :
    v.name != "BUILD_EXTRA_TAGS"])])])
    error_message = "The deploy project passes BUILD_EXTRA_TAGS with build_tags unset."
  }
}

run "set_passes_sorted_json" {
  command = plan
  variables {
    build_tags = { team = "data platform", "cost-center" = "1234" }
  }
  plan_options {
    target = [aws_codebuild_project.deploy]
  }
  assert {
    condition = length([for v in aws_codebuild_project.deploy[0].environment[0].environment_variable : v
    if v.name == "BUILD_EXTRA_TAGS" && v.value == "{\"cost-center\":\"1234\",\"team\":\"data platform\"}"]) == 1
    error_message = "The deploy project does not pass build_tags as one sorted JSON BUILD_EXTRA_TAGS."
  }
}

run "forty_is_the_limit" {
  command = plan
  variables {
    build_tags = { for i in range(40) : format("k%02d", i) => "v" }
  }
  plan_options {
    target = [aws_codebuild_project.deploy]
  }
}

run "aws_prefix" {
  command = plan
  variables {
    build_tags = { "aws:x" = "y" }
  }
  plan_options {
    target = [aws_codebuild_project.deploy]
  }
  expect_failures = [var.build_tags]
}

run "framework_prefix_any_case" {
  command = plan
  variables {
    build_tags = { "AgentExpress:build" = "y" }
  }
  plan_options {
    target = [aws_codebuild_project.deploy]
  }
  expect_failures = [var.build_tags]
}

run "key_too_long" {
  command = plan
  variables {
    build_tags = { (format("%0129d", 0)) = "v" }
  }
  plan_options {
    target = [aws_codebuild_project.deploy]
  }
  expect_failures = [var.build_tags]
}

run "empty_key" {
  command = plan
  variables {
    build_tags = { "" = "v" }
  }
  plan_options {
    target = [aws_codebuild_project.deploy]
  }
  expect_failures = [var.build_tags]
}

run "value_too_long" {
  command = plan
  variables {
    build_tags = { k = format("%0257d", 0) }
  }
  plan_options {
    target = [aws_codebuild_project.deploy]
  }
  expect_failures = [var.build_tags]
}

run "bad_key_char" {
  command = plan
  variables {
    build_tags = { "a#b" = "v" }
  }
  plan_options {
    target = [aws_codebuild_project.deploy]
  }
  expect_failures = [var.build_tags]
}

run "bad_value_char" {
  command = plan
  variables {
    build_tags = { k = "a<b" }
  }
  plan_options {
    target = [aws_codebuild_project.deploy]
  }
  expect_failures = [var.build_tags]
}

run "non_ascii" {
  command = plan
  variables {
    build_tags = { "équipe" = "v" }
  }
  plan_options {
    target = [aws_codebuild_project.deploy]
  }
  expect_failures = [var.build_tags]
}

run "case_duplicate" {
  command = plan
  variables {
    build_tags = { Team = "a", team = "b" }
  }
  plan_options {
    target = [aws_codebuild_project.deploy]
  }
  expect_failures = [var.build_tags]
}

run "too_many" {
  command = plan
  variables {
    build_tags = { for i in range(41) : format("k%02d", i) => "v" }
  }
  plan_options {
    target = [aws_codebuild_project.deploy]
  }
  expect_failures = [var.build_tags]
}
