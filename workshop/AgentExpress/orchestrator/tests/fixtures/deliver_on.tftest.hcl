# v139 Track C deliver: a build WITH a deliver step, planned offline by tests/test_deliver_tf.py
# on a copy of orchestrator/ whose app/workflow.json has one (s3, email and a signed webhook
# target) and keeps run files. Every provider is mocked: no credentials, no AWS call.
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
  deliver_secrets = { hook = "s3cret", other = "nope" }
}
run "the_runtime_gets_only_what_the_targets_need" {
  command = plan
  plan_options {
    target = [aws_iam_role_policy.runtime_deliver, terraform_data.deliver_steps, aws_secretsmanager_secret_version.runtime]
  }
  assert {
    condition     = length(aws_iam_role_policy.runtime_deliver) == 1 && length(terraform_data.deliver_steps) == 1
    error_message = "A build with a deliver step plans no deliver policy."
  }
  assert {
    condition     = [for s in jsondecode(aws_iam_role_policy.runtime_deliver[0].policy).Statement : s.Sid] == ["DeliverReadRunFiles", "DeliverListRunFiles", "DeliverS3Put", "DeliverSesSend"]
    error_message = "The deliver policy does not have exactly the four statements."
  }
  assert {
    condition = (jsondecode(aws_iam_role_policy.runtime_deliver[0].policy).Statement[0].Resource == ["arn:aws:s3:::mock/runs/*/outputs/*"]
      && jsondecode(aws_iam_role_policy.runtime_deliver[0].policy).Statement[1].Resource == ["arn:aws:s3:::mock"]
    && jsondecode(aws_iam_role_policy.runtime_deliver[0].policy).Statement[1].Condition == { StringLike = { "s3:prefix" = ["runs/*/outputs/*"] } })
    error_message = "The runtime reads or lists more of the assets bucket than the run's saved files."
  }
  assert {
    condition     = jsondecode(aws_iam_role_policy.runtime_deliver[0].policy).Statement[2].Resource == ["arn:aws:s3:::amzn-s3-demo-bucket/reports/*", "arn:aws:s3:::amzn-s3-demo-bucket/*"]
    error_message = "The runtime may put outside the s3 targets' prefixes."
  }
  assert {
    condition = (jsondecode(aws_iam_role_policy.runtime_deliver[0].policy).Statement[3].Action == ["ses:SendEmail"]
      && jsondecode(aws_iam_role_policy.runtime_deliver[0].policy).Statement[3].Resource == ["arn:aws:ses:us-east-1:123456789012:identity/example.com"]
    && jsondecode(aws_iam_role_policy.runtime_deliver[0].policy).Statement[3].Condition == { StringEquals = { "ses:FromAddress" = ["reports@example.com"] } })
    error_message = "The runtime may send from more than the email target's identity and From address."
  }
  assert {
    # Merged into the runtime secret (secrets.tf); the secret's string is unknown at plan
    # (the Gateway's mocked client), so its deliver part is read where it is built.
    condition     = keys(local.deliver_secrets_env) == ["DELIVER_SECRETS"] && nonsensitive(jsondecode(local.deliver_secrets_env.DELIVER_SECRETS)) == { hook = "s3cret" }
    error_message = "The runtime secret does not hold exactly the signing secret a webhook target names."
  }
}
run "a_missing_signing_secret_fails_the_plan" {
  command = plan
  variables {
    deliver_secrets = {}
  }
  plan_options {
    target = [terraform_data.deliver_steps]
  }
  expect_failures = [terraform_data.deliver_steps]
}
