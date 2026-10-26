# Build storage (storage.tf, Track E), planned offline: every provider is mocked, so no
# credentials and no AWS call. `terraform test -filter=tests/storage.tftest.hcl` from
# orchestrator/terraform/.
#
# What it proves: app/workflow.json does not set orchestrator.storage, so nothing of it is
# planned, the token resolves to itself and the values it could be in are as written. The build WITH storage (and with retain) is planned
# by tests/test_storage.py, on a copy whose workflow.json sets it
# (tests/fixtures/storage_on.tftest.hcl).
mock_provider "aws" {
  override_during = plan
  mock_resource "aws_iam_role" {
    defaults = { arn = "arn:aws:iam::123456789012:role/mock" }
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
  # As the deploy runner plans a build: no control plane of its own.
  enable_builder = false
}
run "a_workflow_without_storage_plans_none" {
  command = plan
  plan_options {
    target = [
      aws_s3_bucket.storage, aws_s3_bucket_abac.storage, aws_s3_bucket_public_access_block.storage,
      aws_s3_bucket_versioning.storage, aws_s3_bucket_server_side_encryption_configuration.storage,
      aws_s3_bucket_lifecycle_configuration.storage, aws_s3_bucket_policy.storage_tls,
      terraform_data.storage_token, terraform_data.storage_name, aws_iam_role_policy.kb, aws_iam_role_policy.tool_code,
    ]
  }
  assert {
    condition = (length(aws_s3_bucket.storage) + length(aws_s3_bucket_abac.storage) + length(aws_s3_bucket_public_access_block.storage)
      + length(aws_s3_bucket_versioning.storage) + length(aws_s3_bucket_server_side_encryption_configuration.storage)
      + length(aws_s3_bucket_lifecycle_configuration.storage) + length(aws_s3_bucket_policy.storage_tls)
    + length(terraform_data.storage_token) + length(terraform_data.storage_name)) == 0
    error_message = "A workflow without orchestrator.storage plans a storage resource."
  }
  assert {
    condition     = !local.storage_on && local.storage_uri == local.storage_token && !local.storage_kms
    error_message = "Without orchestrator.storage the token must resolve to itself."
  }
  assert {
    condition     = local.kb_s3_uri == try(local.workflow_def.tools[local.kb_tool_name].s3Uri, "")
    error_message = "Without orchestrator.storage the kb tool's s3Uri must be as written."
  }
  assert {
    condition     = alltrue([for n, g in local.code_tool_grants : local.code_tool_s3_prefix[n] == try(g.s3Prefix, "")])
    error_message = "Without orchestrator.storage a code tool's s3Prefix must be as written."
  }
}
