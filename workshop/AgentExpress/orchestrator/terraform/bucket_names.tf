# --- Bucket names (var.bucket_naming) ------------------------------------------------------
# The names of this stack's own S3 buckets, in both naming schemes. Mirrors
# cdk/lib/bucket-names.ts and bff/bucket_names.py; tests/fixtures/bucket_names.json holds all
# three to the same names (tests/test_bucket_names.py, tests/bucket_names.tftest.hcl).
#
# A general purpose bucket name is unique across a partition, and a deleted name "might not
# become available immediately"
# (https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html), so a build
# destroyed in one Region and deployed in another minutes later failed to create its bucket.
#   * "legacy" (the default): the names every deployment had before. Upgrading never renames
#     a bucket: Terraform REPLACES a bucket whose name changes, with what it holds.
#   * "regional": the Region ends every name. The console's deploy runner chooses it only for
#     a build with no Terraform state yet, and records it on the build (deployer/runner.py).
# A regional name over 63 characters keeps its prefix, -<kind>-<account>-<region> and an
# 8-character digest of the agent name, and cuts the agent name to fit, so every IAM pattern
# on these names still matches.
locals {
  # kind -> the prefix before the agent name. Not agentcore-* for storage: that prefix is the
  # framework's own, which every code tool's boundary denies (storage.tf).
  bucket_kinds = {
    assets    = "agentcore-"
    kbdocs    = "agentcore-"
    schemas   = "agentcore-"
    ui        = "agentcore-"
    "ui-logs" = "agentcore-"
    storage   = ""
  }
  bucket_names_legacy = {
    for k, pre in local.bucket_kinds : k => "${pre}${replace(var.agent_name, "_", "-")}-${k}-${local.account_id}"
  }
  bucket_dashed = lower(replace(var.agent_name, "_", "-"))
  bucket_digest = substr(sha256(var.agent_name), 0, 8)
  bucket_tails  = { for k, pre in local.bucket_kinds : k => "-${k}-${local.account_id}-${var.region}" }
  # The agent name cut to what fits beside the prefix, the digest and the tail, with no
  # trailing dash. max(0, ...): substr's length -1 means "to the end".
  bucket_cut = {
    for k, pre in local.bucket_kinds : k => replace(
      substr(local.bucket_dashed, 0, max(0, 63 - length(pre) - length(local.bucket_tails[k]) - 9)), "/-+$/", ""
    )
  }
  bucket_names_regional = {
    for k, pre in local.bucket_kinds : k => (
      length("${pre}${local.bucket_dashed}${local.bucket_tails[k]}") <= 63
      ? "${pre}${local.bucket_dashed}${local.bucket_tails[k]}"
      : local.bucket_cut[k] == ""
      ? "${pre}${local.bucket_digest}${local.bucket_tails[k]}"
      : "${pre}${local.bucket_cut[k]}-${local.bucket_digest}${local.bucket_tails[k]}"
    )
  }
  bucket_names = var.bucket_naming == "regional" ? local.bucket_names_regional : local.bucket_names_legacy
}

# A regional name S3 would refuse fails the plan, before anything is created. Legacy names
# are the ones deployed stacks already have, so nothing new is checked for them.
resource "terraform_data" "bucket_names" {
  count = var.bucket_naming == "regional" ? 1 : 0
  lifecycle {
    precondition {
      condition     = alltrue([for n in values(local.bucket_names_regional) : length(n) <= 63])
      error_message = "A bucket name is over S3's 63 characters: ${join(", ", values(local.bucket_names_regional))}."
    }
    precondition {
      condition = alltrue([for n in values(local.bucket_names_regional) :
      !startswith(n, "xn--") && !startswith(n, "sthree-") && !startswith(n, "amzn-s3-demo-")])
      error_message = "A bucket name starts with a prefix S3 reserves (xn--, sthree-, amzn-s3-demo-): ${join(", ", values(local.bucket_names_regional))}. Use another agent name."
    }
  }
}
