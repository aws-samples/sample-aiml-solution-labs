# docs_url and docs_links (builder.tf), planned offline: every provider is mocked, so no
# credentials and no AWS call. `terraform test -filter=tests/docs_url.tftest.hcl` from
# orchestrator/terraform/.
#
# What it proves: unset, the BFF gets the public user guide at the release tag from
# ../docs_default.json (the file cdk/lib/docs-default.ts reads); set, the URL given; empty
# or docs_links = false, no DOCS_URL and DOCS_LINKS=false. The Builder's deploy project
# passes the same on to every build, and anything but an https:// URL is refused at plan. Planned on a control-plane console, as tests/demo_video.tftest.hcl is: a
# workflow's own app's BFF reads every dedicated agent, which this offline plan cannot
# evaluate. builder.tf merges DOCS_URL into the BFF in either console mode.

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

run "set_gives_the_links" {
  command = plan
  variables {
    console_mode = "builder"
    docs_url     = "https://docs.example.com/product/"
  }
  plan_options {
    target = [aws_lambda_function.bff, aws_codebuild_project.deploy]
  }
  assert {
    condition     = aws_lambda_function.bff.environment[0].variables["DOCS_URL"] == "https://docs.example.com/product/"
    error_message = "The BFF does not get docs_url."
  }
  assert {
    condition = anytrue([for v in aws_codebuild_project.deploy[0].environment[0].environment_variable :
    v.name == "DOCS_URL" && v.value == "https://docs.example.com/product/"])
    error_message = "The deploy project does not pass docs_url on to builds."
  }
}

run "unset_gives_the_public_guide_at_the_release_tag" {
  command = plan
  variables {
    console_mode = "builder"
  }
  plan_options {
    target = [aws_lambda_function.bff, aws_codebuild_project.deploy]
  }
  assert {
    condition = aws_lambda_function.bff.environment[0].variables["DOCS_URL"] == replace(
    jsondecode(file("../docs_default.json")).urlTemplate, "{ref}", jsondecode(file("../docs_default.json")).ref)
    error_message = "Unset, the BFF does not get the default from docs_default.json."
  }
  assert {
    condition     = startswith(local.docs_url, "https://github.com/aws-samples/sample-aiml-solution-labs/blob/agentexpress-v")
    error_message = "The default is not the public user guide at a release tag."
  }
  assert {
    condition     = !contains(keys(aws_lambda_function.bff.environment[0].variables), "DOCS_LINKS")
    error_message = "DOCS_LINKS is set with the links on."
  }
  assert {
    condition = anytrue([for v in aws_codebuild_project.deploy[0].environment[0].environment_variable :
    v.name == "DOCS_URL" && v.value == local.docs_url])
    error_message = "The deploy project does not pass the default on to builds."
  }
}

run "empty_gives_no_links" {
  command = plan
  variables {
    console_mode = "builder"
    docs_url     = ""
  }
  plan_options {
    target = [aws_lambda_function.bff, aws_codebuild_project.deploy]
  }
  assert {
    condition     = !contains(keys(aws_lambda_function.bff.environment[0].variables), "DOCS_URL")
    error_message = "DOCS_URL is set with docs_url empty."
  }
  assert {
    condition     = aws_lambda_function.bff.environment[0].variables["DOCS_LINKS"] == "false"
    error_message = "DOCS_LINKS is not false with docs_url empty."
  }
}

run "docs_links_false_gives_no_links" {
  command = plan
  variables {
    console_mode = "builder"
    docs_links   = false
    docs_url     = "https://docs.example.com/product/"
  }
  plan_options {
    target = [aws_lambda_function.bff, aws_codebuild_project.deploy]
  }
  assert {
    condition     = aws_lambda_function.bff.environment[0].variables["DOCS_LINKS"] == "false"
    error_message = "The BFF does not get DOCS_LINKS=false."
  }
  assert {
    condition     = !contains(keys(aws_lambda_function.bff.environment[0].variables), "DOCS_URL")
    error_message = "DOCS_URL is set with docs_links = false."
  }
  assert {
    condition = anytrue([for v in aws_codebuild_project.deploy[0].environment[0].environment_variable :
    v.name == "DOCS_LINKS" && v.value == "false"])
    error_message = "The deploy project does not pass docs_links = false on to builds."
  }
  assert {
    condition     = !anytrue([for v in aws_codebuild_project.deploy[0].environment[0].environment_variable : v.name == "DOCS_URL"])
    error_message = "The deploy project carries DOCS_URL with docs_links = false."
  }
}

run "must_be_https" {
  command = plan
  variables {
    console_mode = "builder"
    docs_url     = "http://docs.example.com/product/"
  }
  plan_options {
    target = [aws_lambda_function.bff]
  }
  expect_failures = [var.docs_url]
}
