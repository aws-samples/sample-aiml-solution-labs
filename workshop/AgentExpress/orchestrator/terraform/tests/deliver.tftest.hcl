# Deliver steps (v139 Track C deliver; deliver.tf), planned offline: every provider is mocked,
# so no credentials and no AWS call. `terraform test -filter=tests/deliver.tftest.hcl` from
# orchestrator/terraform/ (tests/test_deliver_tf.py runs it).
#
# What it proves: app/workflow.json has no deliver step, so the build plans no deliver policy
# and no deliver check, and its runtime secret holds exactly the keys it held before
# (no DELIVER_SECRETS), even with a deliverSecrets secret saved. A build WITH a deliver step
# is planned by tests/test_deliver_tf.py on a copy (tests/fixtures/deliver_on.tftest.hcl).
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
  region          = "us-east-1"
  enable_builder  = false
  deliver_secrets = { unused = "x" }
}
run "a_build_without_deliver_is_unchanged" {
  command = plan
  plan_options {
    target = [aws_iam_role_policy.runtime_deliver, terraform_data.deliver_steps, aws_secretsmanager_secret_version.runtime]
  }
  assert {
    condition     = length(aws_iam_role_policy.runtime_deliver) == 0 && length(terraform_data.deliver_steps) == 0
    error_message = "A build with no deliver step plans a deliver policy or check."
  }
  # The runtime secret is jsonencode(merge(<the keys it had>, local.deliver_secrets_env)):
  # with nothing merged in, it is the same string as before. (Its other values come from the
  # Gateway's mocked client, unknown at plan, so the string itself cannot be compared here.)
  assert {
    condition     = length(keys(local.deliver_secrets_env)) == 0 && length(local.deliver_secret_names) == 0
    error_message = "A build with no deliver step adds DELIVER_SECRETS to its runtime secret."
  }
  assert {
    condition     = jsonencode(merge({ A = "1", B = { c = 2 } }, local.deliver_secrets_env)) == jsonencode({ A = "1", B = { c = 2 } })
    error_message = "Merging the empty deliver keys changes the secret's JSON."
  }
}
