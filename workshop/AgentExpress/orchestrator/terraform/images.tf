# Image agents (workflow.json `output: "image"`, app/common/images.py) store what they
# render in this bucket, and the BFF hands out short-lived links to it (GET /api/images).
# Only when some agent draws; emptied and deleted with the stack (force_destroy).
# Mirrors the AssetsBucket in cdk/lib/orchestrator-stack.ts.
locals {
  image_agents = [
    for id, a in local.workflow_def.agents : id if try(a.output, "") == "image"
  ]
  assets_enabled = local.workflow_plane && length(local.image_agents) > 0
  # Agents whose model reads earlier agents' images (workflow.json `vision`, app/common/
  # images.load_for): they read the bucket as well as write it.
  vision_agents = [
    for id, a in local.workflow_def.agents : id
    if try(a.runtime, local.key_defaults.agent.runtime) != "a2a" && length(try(a.vision.from, [])) > 0
  ]
  main_reads_images = length([for id in local.vision_agents : id if !contains(keys(local.dedicated_agents), id)]) > 0
  assets_bucket     = local.assets_enabled ? aws_s3_bucket.assets[0].id : ""
}

# An image agent whose model is served only outside this region's geography sends its
# brief abroad. Refused unless the agent sets image.allowCrossRegion = true. Same rule as
# app/common/images.residency_error and imageResidencyError in cdk/lib/vocabulary.ts.
locals {
  image_model_regions = local.vocab.imageModels.regionsByModel
  image_residency_violations = [
    for id in local.image_agents : id
    if !try(local.workflow_def.agents[id].image.allowCrossRegion == true, false)
    && length([
      for r in local.image_model_regions[try(local.workflow_def.agents[id].image.model, local.vocab.imageModels.values[0])] : r
      if r == var.region || try([for g in local.model_geos.byRegionPrefix : g[1] if startswith(r, g[0])][0], local.model_geos.fallback) == local.region_geo
    ]) == 0
  ]
}

resource "terraform_data" "image_residency" {
  count = local.assets_enabled ? 1 : 0
  lifecycle {
    precondition {
      condition     = length(local.image_residency_violations) == 0
      error_message = "Image agent(s) ${join(", ", local.image_residency_violations)}: the image model is served only outside this deployment's geography (${var.region}; see imageModels.regionsByModel in app/vocabulary.json), so the image brief would leave it. Set the agent's image.allowCrossRegion to true to allow that, or deploy in a region of that geography."
    }
  }
}

resource "aws_s3_bucket" "assets" {
  count = local.assets_enabled ? 1 : 0
  # Dashed like the other buckets: an S3 bucket name may not contain an underscore.
  bucket        = "agentcore-${replace(var.agent_name, "_", "-")}-assets-${local.account_id}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "assets" {
  count                   = local.assets_enabled ? 1 : 0
  bucket                  = aws_s3_bucket.assets[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "assets" {
  count  = local.assets_enabled ? 1 : 0
  bucket = aws_s3_bucket.assets[0].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_policy" "assets_tls" {
  count  = local.assets_enabled ? 1 : 0
  bucket = aws_s3_bucket.assets[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.assets[0].arn, "${aws_s3_bucket.assets[0].arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.assets]
}
# The orchestrator runs every in-process (runtime "main") agent, so it may write images.
resource "aws_iam_role_policy" "runtime_assets" {
  count = local.assets_enabled ? 1 : 0
  name  = "AgentCoreRuntimeAssets-${var.agent_name}"
  role  = aws_iam_role.runtime[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    # Read too, only when an in-process agent reads images (`vision`).
    Statement = [{ Effect = "Allow", Action = concat(["s3:PutObject"], local.main_reads_images ? ["s3:GetObject"] : []), Resource = "${aws_s3_bucket.assets[0].arn}/runs/*" }]
  })
}
# A dedicated agent that reads images (`vision`) reads them from its own runtime.
# Mirrors ReadRunImages in cdk/lib/orchestrator-stack.ts.
resource "aws_iam_role_policy" "subagent_vision" {
  for_each = local.assets_enabled ? { for id in local.vision_agents : id => id if contains(keys(local.dedicated_agents), id) } : {}
  name     = "AgentCoreSubagentVision-${each.key}"
  role     = aws_iam_role.subagent[each.key].id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Sid = "ReadRunImages", Effect = "Allow", Action = ["s3:GetObject"], Resource = "${aws_s3_bucket.assets[0].arn}/runs/*" }]
  })
}

# A dedicated image agent writes from its own runtime, under its own role.
resource "aws_iam_role_policy" "subagent_assets" {
  for_each = { for id in local.image_agents : id => id if contains(keys(local.dedicated_agents), id) }
  name     = "AgentCoreSubagentAssets-${each.key}"
  role     = aws_iam_role.subagent[each.key].id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = ["s3:PutObject"], Resource = "${aws_s3_bucket.assets[0].arn}/runs/*" }]
  })
}

resource "aws_iam_role_policy" "bff_assets" {
  count = local.assets_enabled ? 1 : 0
  name  = "AgentCoreBFFAssets-${var.agent_name}"
  role  = aws_iam_role.bff.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = ["s3:GetObject"], Resource = "${aws_s3_bucket.assets[0].arn}/runs/*" }]
  })
}
