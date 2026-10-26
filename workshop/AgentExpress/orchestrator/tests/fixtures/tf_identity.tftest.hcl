# The whole plan of a build, offline (tests/test_v139_identity.py writes this file into a copy
# of terraform/ and fills in TARGETS). Every provider is mocked, and every value a mock would
# otherwise make up at random is fixed, so the same configuration plans the same text.
mock_provider "aws" {
  override_during = plan
  mock_data "aws_caller_identity" {
    defaults = { account_id = "123456789012" }
  }
}
mock_provider "awscc" {
  override_during = plan
}
mock_provider "null" {
  override_during = plan
}
mock_provider "external" {
  override_during = plan
  mock_data "external" {
    defaults = { result = {} }
  }
}
mock_provider "archive" {
  override_during = plan
  mock_data "archive_file" {
    defaults = {
      output_base64sha256 = "mock-archive-sha256"
      output_md5          = "mock-archive-md5"
      output_path         = "mock-archive.zip"
      output_size         = 0
    }
  }
}
mock_provider "time" {
  override_during = plan
}
variables {
  region = "us-east-1"
  ENABLE_BUILDER
}
run "plan" {
  command = plan
  # Every resource in the configuration except the UI's files (aws_s3_object.ui_asset), whose
  # for_each is the list of files the UI build writes, known only after it runs.
  plan_options {
    target = [
      TARGETS
    ]
  }
}
