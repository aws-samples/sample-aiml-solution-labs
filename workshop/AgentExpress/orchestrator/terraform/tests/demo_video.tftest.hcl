# demo_video_url (builder.tf), planned offline: every provider is mocked, so no credentials
# and no AWS call. `terraform test -filter=tests/demo_video.tftest.hcl` from
# orchestrator/terraform/.
#
# What it proves: a control-plane console's BFF gets DEMO_VIDEO_URL when it is set and not
# when it is empty, and anything but an https:// URL is refused at plan. A workflow's own
# app is not planned here: its BFF reads every dedicated agent, which this offline plan
# cannot evaluate. builder.tf sets DEMO_VIDEO_URL only with console_mode = "builder".

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

run "console_gets_the_link" {
  command = plan
  variables {
    console_mode   = "builder"
    demo_video_url = "https://videos.example.com/demo"
  }
  plan_options {
    target = [aws_lambda_function.bff]
  }
  assert {
    condition     = aws_lambda_function.bff.environment[0].variables["DEMO_VIDEO_URL"] == "https://videos.example.com/demo"
    error_message = "A control-plane console's BFF does not get demo_video_url."
  }
}

run "unset_gives_no_link" {
  command = plan
  variables {
    console_mode = "builder"
  }
  plan_options {
    target = [aws_lambda_function.bff]
  }
  assert {
    condition     = !contains(keys(aws_lambda_function.bff.environment[0].variables), "DEMO_VIDEO_URL")
    error_message = "DEMO_VIDEO_URL is set with demo_video_url empty."
  }
}

run "must_be_https" {
  command = plan
  variables {
    console_mode   = "builder"
    demo_video_url = "http://videos.example.com/demo"
  }
  plan_options {
    target = [aws_lambda_function.bff]
  }
  expect_failures = [var.demo_video_url]
}
