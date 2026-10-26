# v1.3.9 features together, planned offline (tests/test_v139_wiring.py runs it on a copy whose
# workflow.json creates build storage, has a Document generator reading its template from
# and writing to ${storage}, and a deliver step with an s3 target in ${storage}): the token
# resolves to the build's bucket in the docgen function's settings and grants and in the
# runtime's deliver grant, and nothing refuses it.
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
  mock_resource "aws_kms_key" {
    defaults = { arn = "arn:aws:kms:us-east-1:123456789012:key/mock" }
  }
  mock_data "aws_caller_identity" {
    defaults = { account_id = "123456789012" }
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
  region         = "us-east-1"
  enable_builder = false
}
run "the_storage_token_resolves_for_docgen_and_deliver" {
  command = plan
  plan_options {
    target = [aws_iam_role_policy.tool_lambda_docgen, aws_iam_role_policy.runtime_deliver, terraform_data.deliver_steps, terraform_data.storage_token]
  }
  assert {
    condition = (local.builtin_lambda_docgen_tools["reportDoc"].template_s3_uri == "s3://multiagent-orchestrator-storage-123456789012/templates/"
    && local.builtin_lambda_docgen_tools["reportDoc"].output_prefix == "s3://multiagent-orchestrator-storage-123456789012/outputs/")
    error_message = "The Document generator's template and output locations did not resolve to the build's bucket."
  }
  assert {
    condition = (strcontains(aws_iam_role_policy.tool_lambda_docgen["reportDoc"].policy, "arn:aws:s3:::multiagent-orchestrator-storage-123456789012/templates/*")
      && strcontains(aws_iam_role_policy.tool_lambda_docgen["reportDoc"].policy, "arn:aws:s3:::multiagent-orchestrator-storage-123456789012/outputs/*")
    && !strcontains(aws_iam_role_policy.tool_lambda_docgen["reportDoc"].policy, "storage}"))
    error_message = "The Document generator's role is not scoped to its folders of the build's bucket."
  }
  assert {
    condition     = jsonencode(local.deliver_s3_resources) == jsonencode(["arn:aws:s3:::multiagent-orchestrator-storage-123456789012/delivered/*"]) && length(local.deliver_storage_uris) == 0
    error_message = "The deliver step's s3 target did not resolve to the build's bucket."
  }
  assert {
    condition     = jsondecode(aws_iam_role_policy.runtime_deliver[0].policy).Statement[2].Resource == ["arn:aws:s3:::multiagent-orchestrator-storage-123456789012/delivered/*"]
    error_message = "The runtime's deliver grant is not on the target's prefix of the build's bucket."
  }
  assert {
    condition     = local.storage_on && local.storage_token_used && length(terraform_data.storage_token) == 1
    error_message = "The deliver target's token is not seen where the token is checked (storage.tf storage_token_used)."
  }
}
