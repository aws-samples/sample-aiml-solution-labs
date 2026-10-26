# Build storage (terraform/storage.tf) WITH orchestrator.storage, planned offline with every
# provider mocked. Not in terraform/tests/: tests/test_storage.py copies it into a copy of the
# tree whose workflow.json sets {"create": true, "retain": __RETAIN__}, a code tool
# `renderer` granted "${storage}/templates/" readwrite, and the kb tool's s3Uri
# "${storage}/kb/".
mock_provider "aws" {
  override_during = plan
  mock_resource "aws_iam_role" {
    defaults = { arn = "arn:aws:iam::123456789012:role/mock" }
  }
  mock_resource "aws_iam_policy" {
    defaults = { arn = "arn:aws:iam::123456789012:policy/mock" }
  }
  mock_resource "aws_s3_bucket" {
    defaults = { arn = "arn:aws:s3:::multiagent-orchestrator-storage-123456789012", id = "multiagent-orchestrator-storage-123456789012" }
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
  region         = "us-east-1"
  enable_builder = false
}
run "a_workflow_with_storage_plans_the_bucket_and_resolves_the_token" {
  command = plan
  plan_options {
    target = [
      aws_s3_bucket.storage, aws_s3_bucket_abac.storage, aws_s3_bucket_public_access_block.storage,
      aws_s3_bucket_versioning.storage, aws_s3_bucket_server_side_encryption_configuration.storage,
      aws_s3_bucket_lifecycle_configuration.storage, aws_s3_bucket_policy.storage_tls,
      terraform_data.storage_token, terraform_data.storage_name, aws_iam_role_policy.tool_code, aws_iam_role_policy.kb,
    ]
  }
  assert {
    condition     = local.storage_on && local.storage_retain == __RETAIN__ && local.storage_kms == !__RETAIN__
    error_message = "orchestrator.storage was not read as written."
  }
  assert {
    condition = (aws_s3_bucket.storage[0].bucket == "multiagent-orchestrator-storage-123456789012"
      && aws_s3_bucket.storage[0].force_destroy == !__RETAIN__
    && aws_s3_bucket.storage[0].tags["agentexpress:code-tools"] == "true")
    error_message = "The bucket is not <build name>-storage-<account>, emptied on destroy unless kept, tagged for code tools."
  }
  assert {
    condition     = aws_s3_bucket_abac.storage[0].abac_status[0].status == "Enabled"
    error_message = "ABAC is not on, so a code tool's tag-conditioned grant would not reach the bucket."
  }
  assert {
    condition = (aws_s3_bucket_public_access_block.storage[0].block_public_acls && aws_s3_bucket_public_access_block.storage[0].block_public_policy
    && aws_s3_bucket_public_access_block.storage[0].ignore_public_acls && aws_s3_bucket_public_access_block.storage[0].restrict_public_buckets)
    error_message = "Not all four Block Public Access settings are on."
  }
  assert {
    condition     = aws_s3_bucket_versioning.storage[0].versioning_configuration[0].status == "Enabled"
    error_message = "The bucket is not versioned."
  }
  assert {
    condition = (tolist(aws_s3_bucket_server_side_encryption_configuration.storage[0].rule)[0].apply_server_side_encryption_by_default[0].sse_algorithm
    == (__RETAIN__ ? "AES256" : "aws:kms"))
    error_message = "The bucket is not encrypted with the build's key (S3-managed keys when kept)."
  }
  assert {
    condition = (tolist(aws_s3_bucket_lifecycle_configuration.storage[0].rule)[0].noncurrent_version_expiration[0].noncurrent_days == 30
    && tolist(aws_s3_bucket_lifecycle_configuration.storage[0].rule)[0].abort_incomplete_multipart_upload[0].days_after_initiation == 7)
    error_message = "The lifecycle rule does not keep old versions 30 days and abort uploads after 7."
  }
  assert {
    condition     = strcontains(aws_s3_bucket_policy.storage_tls[0].policy, "aws:SecureTransport")
    error_message = "The bucket does not refuse plain HTTP."
  }
  assert {
    condition = ((local.kb_s3_bucket == "multiagent-orchestrator-storage-123456789012" && local.kb_s3_prefix == "kb/")
    && local.code_tool_s3_prefix["renderer"] == "s3://multiagent-orchestrator-storage-123456789012/templates/")
    error_message = "The token did not resolve to s3://<the bucket> where it is read."
  }
  assert {
    condition = (strcontains(aws_iam_role_policy.tool_code["renderer"].policy, "arn:aws:s3:::multiagent-orchestrator-storage-123456789012/templates/*")
    && strcontains(aws_iam_role_policy.tool_code["renderer"].policy, "UseStorageKey") == !__RETAIN__)
    error_message = "The code tool's grant is not on its prefix of the bucket, with the build's key only when it encrypts it."
  }
}
