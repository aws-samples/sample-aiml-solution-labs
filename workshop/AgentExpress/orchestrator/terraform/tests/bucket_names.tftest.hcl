# Bucket names (bucket_names.tf), planned offline: every provider is mocked, so no credentials
# and no AWS call. `terraform test -filter=tests/bucket_names.tftest.hcl` from
# orchestrator/terraform/. tests/test_bucket_names.py runs it, and holds every name expected
# here to tests/fixtures/bucket_names.json, the vectors cdk/lib/bucket-names.ts and
# bff/bucket_names.py are held to.
#
# What it proves: the default is "legacy", which names every bucket as before (no rename of a
# deployed bucket); "regional" ends each name with the Region and cuts a long agent name (with
# a digest of it) to 63 characters; a name S3 reserves fails the plan.
mock_provider "aws" {
  override_during = plan
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
run "the_default_is_the_legacy_names" {
  command = plan
  plan_options {
    target = [terraform_data.bucket_names, aws_s3_bucket.ui]
  }
  variables {
    agent_name = "multiagent_orchestrator"
  }
  assert {
    condition = local.bucket_names == {
      assets    = "agentcore-multiagent-orchestrator-assets-123456789012"
      kbdocs    = "agentcore-multiagent-orchestrator-kbdocs-123456789012"
      schemas   = "agentcore-multiagent-orchestrator-schemas-123456789012"
      storage   = "multiagent-orchestrator-storage-123456789012"
      ui        = "agentcore-multiagent-orchestrator-ui-123456789012"
      "ui-logs" = "agentcore-multiagent-orchestrator-ui-logs-123456789012"
    }
    error_message = "The legacy names changed: ${jsonencode(local.bucket_names)}"
  }
  assert {
    condition     = length(terraform_data.bucket_names) == 0 && aws_s3_bucket.ui.bucket == local.bucket_names.ui
    error_message = "Legacy names are checked, or the UI bucket is not named here."
  }
}
run "a_console_build_in_the_longest_region" {
  command = plan
  plan_options {
    target = [terraform_data.bucket_names, aws_s3_bucket.ui]
  }
  variables {
    agent_name    = "ax_1a2b3c4d"
    region        = "us-isof-south-1"
    bucket_naming = "regional"
  }
  assert {
    condition = local.bucket_names == {
      assets    = "agentcore-ax-1a2b3c4d-assets-123456789012-us-isof-south-1"
      kbdocs    = "agentcore-ax-1a2b3c4d-kbdocs-123456789012-us-isof-south-1"
      schemas   = "agentcore-ax-1a2b3c4d-schemas-123456789012-us-isof-south-1"
      storage   = "ax-1a2b3c4d-storage-123456789012-us-isof-south-1"
      ui        = "agentcore-ax-1a2b3c4d-ui-123456789012-us-isof-south-1"
      "ui-logs" = "agentcore-ax-1a2b3c4d-ui-logs-123456789012-us-isof-south-1"
    }
    error_message = "Regional names: ${jsonencode(local.bucket_names)}"
  }
}
run "the_default_agent_name_is_cut_to_fit" {
  command = plan
  plan_options {
    target = [terraform_data.bucket_names, aws_s3_bucket.ui]
  }
  variables {
    agent_name    = "multiagent_orchestrator"
    region        = "ap-southeast-3"
    bucket_naming = "regional"
  }
  assert {
    condition = local.bucket_names == {
      assets    = "agentcore-multiagen-4c53c88c-assets-123456789012-ap-southeast-3"
      kbdocs    = "agentcore-multiagen-4c53c88c-kbdocs-123456789012-ap-southeast-3"
      schemas   = "agentcore-multiage-4c53c88c-schemas-123456789012-ap-southeast-3"
      storage   = "multiagent-orchestrator-storage-123456789012-ap-southeast-3"
      ui        = "agentcore-multiagent-or-4c53c88c-ui-123456789012-ap-southeast-3"
      "ui-logs" = "agentcore-multiage-4c53c88c-ui-logs-123456789012-ap-southeast-3"
    }
    error_message = "Regional names: ${jsonencode(local.bucket_names)}"
  }
}
run "the_longest_agent_name_is_cut_to_fit" {
  command = plan
  plan_options {
    target = [terraform_data.bucket_names, aws_s3_bucket.ui]
  }
  variables {
    agent_name    = "abbbbbbbbbbbbbbbbbbbb_cccccccccccccccccccccccccc"
    region        = "us-gov-west-1"
    bucket_naming = "regional"
  }
  assert {
    condition = local.bucket_names == {
      assets    = "agentcore-abbbbbbbbb-c6c2e1f6-assets-123456789012-us-gov-west-1"
      kbdocs    = "agentcore-abbbbbbbbb-c6c2e1f6-kbdocs-123456789012-us-gov-west-1"
      schemas   = "agentcore-abbbbbbbb-c6c2e1f6-schemas-123456789012-us-gov-west-1"
      storage   = "abbbbbbbbbbbbbbbbbb-c6c2e1f6-storage-123456789012-us-gov-west-1"
      ui        = "agentcore-abbbbbbbbbbbbb-c6c2e1f6-ui-123456789012-us-gov-west-1"
      "ui-logs" = "agentcore-abbbbbbbb-c6c2e1f6-ui-logs-123456789012-us-gov-west-1"
    }
    error_message = "Regional names: ${jsonencode(local.bucket_names)}"
  }
}
run "a_name_s3_reserves_fails_the_plan" {
  command = plan
  plan_options {
    target = [terraform_data.bucket_names, aws_s3_bucket.ui]
  }
  variables {
    agent_name    = "xn__a"
    bucket_naming = "regional"
  }
  expect_failures = [terraform_data.bucket_names]
}
