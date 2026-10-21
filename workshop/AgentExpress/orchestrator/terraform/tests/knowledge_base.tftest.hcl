# The Knowledge Base resources (kb.tf), planned offline: every provider is mocked, so no
# credentials and no AWS call. `terraform test -filter=tests/knowledge_base.tftest.hcl`
# from orchestrator/terraform/.
#
# What it proves: a build with no Knowledge Base tool plans no Knowledge Base resource, and
# a build with one plans the S3 Vectors index, the Knowledge Base pointing at it and its data
# source reading the documents bucket.
#
# app/workflow.json declares a kb tool. The run with none sets enable_gateway = false:
# local.tools is empty with no Gateway, so kb_create is false and every Knowledge Base
# resource has count = 0, as in a build whose workflow declares no kb tool.
#
# Terraform runs the provider's validators on a count = 0 resource too, with every value it
# can compute at validate time. Here the Knowledge Base depends on var.enable_gateway, which
# validation treats as unknown, so those validators skip it: tests/test_tf_no_kb.py
# validates a copy whose workflow.json has no tools, the case that failed with "Invalid
# Attribute Combination".
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
  mock_resource "aws_s3vectors_index" {
    defaults = { index_arn = "arn:aws:s3vectors:us-east-1:123456789012:bucket/mock/index/mock" }
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
run "build_without_knowledge_base" {
  command = plan
  variables {
    enable_gateway = false
  }
  plan_options {
    target = [
      aws_bedrockagent_knowledge_base.kb, aws_bedrockagent_data_source.kb,
      awscc_bedrock_data_source.kb_graph,
    ]
  }
  assert {
    condition = (length(aws_bedrockagent_knowledge_base.kb) == 0 && length(aws_bedrockagent_data_source.kb) == 0
    && length(awscc_bedrock_data_source.kb_graph) == 0 && length(aws_s3vectors_index.kb) == 0)
    error_message = "A build with no Knowledge Base tool plans a Knowledge Base resource."
  }
}
run "build_with_knowledge_base" {
  command = plan
  plan_options {
    target = [
      aws_bedrockagent_knowledge_base.kb, aws_bedrockagent_data_source.kb,
      awscc_bedrock_data_source.kb_graph,
    ]
  }
  assert {
    condition     = length(aws_bedrockagent_knowledge_base.kb) == 1 && length(aws_bedrockagent_data_source.kb) == 1
    error_message = "A build with a Knowledge Base tool does not plan the Knowledge Base and its data source."
  }
  assert {
    condition = (aws_bedrockagent_knowledge_base.kb[0].storage_configuration[0].type == "S3_VECTORS"
      && aws_bedrockagent_knowledge_base.kb[0].storage_configuration[0].s3_vectors_configuration[0].index_arn
    == "arn:aws:s3vectors:us-east-1:123456789012:bucket/mock/index/mock")
    error_message = "The Knowledge Base does not point at its S3 Vectors index."
  }
  assert {
    condition     = aws_bedrockagent_data_source.kb[0].data_source_configuration[0].s3_configuration[0].bucket_arn == "arn:aws:s3:::mock"
    error_message = "The data source does not read the documents bucket."
  }
}
