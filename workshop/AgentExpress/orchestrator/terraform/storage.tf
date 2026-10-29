# --- begin Track E storage ---
# Build storage (workflow.json `orchestrator.storage`): an S3 bucket the deploy creates for
# the build, which tools reach through the `${storage}` token (app/vocabulary.json
# storageToken). Mirrors cdk/lib/storage.ts; cdk/test/storage.test.ts holds the two together.
#
# Opt-in. {"create": true} creates <build name>-storage-<account>: all four Block Public
# Access settings and an HTTPS-only policy; encrypted by default with the build's KMS key
# (kms.tf) and an S3 Bucket Key, or S3-managed keys with `retain` (the build's key is deleted
# with the build, which would leave what the bucket keeps unreadable); versioned, a version
# deleted 30 days after it is replaced; tagged agentexpress:code-tools=true with ABAC on, so a
# code tool's grant (tools_code.tf code_grantable) reaches it.
#   https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-control-block-public-access.html
#   https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html
#   https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucket-key.html
#   https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-configuration-examples.html
#   https://docs.aws.amazon.com/AmazonS3/latest/userguide/buckets-tagging-enable-abac.html
#
# Nothing is granted on the bucket here: a tool that names it gets its own scoped grant where
# tools get theirs (a code tool's s3Prefix in tools_code.tf, a kb tool's s3Uri in kb.tf).
#
# Destroy: by default the bucket is emptied (force_destroy, every version) and deleted.
# With `retain`, deployer/runner.py and teardown.sh take these resources out of the state
# before `terraform destroy`, so the bucket and its contents stay; a later deploy of the same
# build then finds the name taken. A workflow without orchestrator.storage plans nothing here.
locals {
  storage_cfg    = try(local.workflow_def.orchestrator.storage, null)
  storage_on     = local.workflow_plane && try(local.storage_cfg.create, local.key_defaults.orchestrator.storage.create) == true
  storage_retain = try(local.storage_cfg.retain, local.key_defaults.orchestrator.storage.retain) == true
  storage_token  = local.vocab.storageToken.values[0]
  # Dashed like the build's other buckets (no underscore in a bucket name); not agentcore-*,
  # the framework's own prefix, which every code tool's boundary denies. bucket_names.tf:
  # <name>-storage-<account>, with -<region> after it under bucket_naming = "regional".
  storage_bucket_name = local.bucket_names.storage
  # What the token stands for. Without storage it stands for itself, so a value carrying it
  # is left as written, and terraform_data.storage_token refuses it.
  storage_uri = local.storage_on ? "s3://${local.storage_bucket_name}" : local.storage_token
  # The build's KMS key encrypts it, unless it is kept past the build.
  storage_kms = local.storage_on && !local.storage_retain
  # Where the token points, for a step that resolves it at run time (app/common/storage.py).
  storage_env = local.storage_on ? { STORAGE_BUCKET = local.storage_bucket_name } : {}
  # Whether a tool, an interceptor or a deliver step's target carries the token (where
  # storageToken.fields are; deliver.tf deliver_targets).
  storage_token_used = strcontains(jsonencode([try(local.workflow_def.tools, {}), try(local.workflow_def.orchestrator.interceptors, {}), local.deliver_targets]), local.storage_token)
}

resource "terraform_data" "storage_token" {
  count = local.storage_token_used ? 1 : 0
  lifecycle {
    precondition {
      condition     = local.storage_on
      error_message = "workflow.json uses ${local.storage_token}, but this build creates no storage: set orchestrator.storage.create to true."
    }
  }
}

resource "terraform_data" "storage_name" {
  count = local.storage_on ? 1 : 0
  lifecycle {
    precondition {
      # https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html: 3 to 63.
      condition     = length(local.storage_bucket_name) <= 63
      error_message = "orchestrator.storage: the bucket name ${local.storage_bucket_name} is over S3's 63 characters; use a shorter agent name."
    }
  }
}

resource "aws_s3_bucket" "storage" {
  count         = local.storage_on ? 1 : 0
  bucket        = local.storage_bucket_name
  force_destroy = !local.storage_retain
  tags          = { "agentexpress:code-tools" = "true" }
}

resource "aws_s3_bucket_abac" "storage" {
  count  = local.storage_on ? 1 : 0
  bucket = aws_s3_bucket.storage[0].id
  abac_status {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "storage" {
  count                   = local.storage_on ? 1 : 0
  bucket                  = aws_s3_bucket.storage[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "storage" {
  count  = local.storage_on ? 1 : 0
  bucket = aws_s3_bucket.storage[0].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "storage" {
  count  = local.storage_on ? 1 : 0
  bucket = aws_s3_bucket.storage[0].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = local.storage_kms ? "aws:kms" : "AES256"
      kms_master_key_id = local.storage_kms ? aws_kms_key.data.arn : null
    }
    bucket_key_enabled = local.storage_kms ? true : null
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "storage" {
  count  = local.storage_on ? 1 : 0
  bucket = aws_s3_bucket.storage[0].id
  rule {
    id     = "storage-housekeeping"
    status = "Enabled"
    filter {}
    expiration {
      expired_object_delete_marker = true
    }
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
  depends_on = [aws_s3_bucket_versioning.storage]
}

resource "aws_s3_bucket_policy" "storage_tls" {
  count  = local.storage_on ? 1 : 0
  bucket = aws_s3_bucket.storage[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.storage[0].arn, "${aws_s3_bucket.storage[0].arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.storage]
}
# --- end Track E storage ---
