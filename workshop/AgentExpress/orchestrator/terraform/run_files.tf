# Keeping the files runs make (workflow.json `orchestrator.files`, app/common/artifacts.py):
# what agents save under runs/<session>/outputs/ and the code the codeInterpreter tool
# ran under runs/<session>/code/, beside the inputs and images already kept there. Shown on
# a run's Files tab and the app's Files page (bff/runartifacts.py).
#
# Opt-in. Setting `orchestrator.files` (even {}) creates the assets bucket (images.tf) when
# no agent needed it yet, lets every dedicated runtime write outputs and code, lets the page
# read text files through a link (CORS GET), and expires everything under runs/ after
# retentionDays (default 90). A deployment without it keeps exactly what it kept before,
# with no expiry. Mirrors filesRetentionDays in cdk/lib/run-files.ts.
locals {
  files_on             = local.workflow_plane && try(local.workflow_def.orchestrator.files, null) != null
  files_retention_days = try(local.workflow_def.orchestrator.files.retentionDays, local.key_defaults.orchestrator.files.retentionDays)
}

resource "terraform_data" "files_retention" {
  count = local.files_on ? 1 : 0
  lifecycle {
    precondition {
      condition     = try(local.files_retention_days == floor(local.files_retention_days) && local.files_retention_days >= 1 && local.files_retention_days <= 3650, false)
      error_message = "orchestrator.files.retentionDays must be a whole number of days, 1 to 3650."
    }
  }
}

# A dedicated agent saves its files (ctx.save_file) and keeps its sandbox code from its own
# runtime. Only those two folders: it never overwrites a run's inputs or another agent's
# images. The orchestrator's own role already writes runs/* (images.tf runtime_assets).
# Mirrors SaveRunFiles in cdk/lib/orchestrator-stack.ts.
resource "aws_iam_role_policy" "subagent_save_files" {
  for_each = { for id, a in local.dedicated_agents : id => id if local.files_on }
  name     = "AgentCoreSubagentSaveFiles-${each.key}"
  role     = aws_iam_role.subagent[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "SaveRunFiles", Effect = "Allow", Action = ["s3:PutObject"],
      Resource = ["${aws_s3_bucket.assets[0].arn}/runs/*/outputs/*", "${aws_s3_bucket.assets[0].arn}/runs/*/code/*"]
    }]
  })
}

# The Files tab lists a run's folder; the BFF already reads (and signs links to) runs/*
# (images.tf bff_assets). Only keys under runs/. Mirrors ListRunFiles in the CDK.
resource "aws_iam_role_policy" "bff_list_run_files" {
  count = local.assets_enabled ? 1 : 0
  name  = "AgentCoreBFFListRunFiles-${var.agent_name}"
  role  = aws_iam_role.bff.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "ListRunFiles", Effect = "Allow", Action = ["s3:ListBucket"],
      Resource  = aws_s3_bucket.assets[0].arn,
      Condition = { StringLike = { "s3:prefix" = ["runs/*"] } }
    }]
  })
}
