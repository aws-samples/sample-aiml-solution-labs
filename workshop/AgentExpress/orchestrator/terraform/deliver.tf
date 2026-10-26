# --- Deliver steps (v139 Track C deliver; app/common/deliver.py) ------------------------
# A task step with task.kind "deliver" sends the run's files to S3, by email with Amazon
# SES, or to a webhook. It runs in the orchestrator's own runtime, so only that role gets
# anything, and only when a deliver step exists:
#   DeliverReadRunFiles  s3:GetObject on the assets bucket's runs/<session>/outputs/ only;
#   DeliverListRunFiles  s3:ListBucket on the assets bucket, for prefixes under runs/*/outputs/;
#   DeliverS3Put         s3:PutObject under each s3 target's prefix only;
#   DeliverSesSend       ses:SendEmail on each email target's identity ARN only, with
#                        ses:FromAddress pinned to the configured From addresses
#                        (https://docs.aws.amazon.com/service-authorization/latest/reference/list_sesv2.html,
#                        https://docs.aws.amazon.com/ses/latest/dg/control-user-access.html).
# A webhook target needs no AWS access; its optional signing secret is a deliverSecrets
# build secret (var.deliver_secrets), put in the runtime secret as DELIVER_SECRETS only when
# a target names one (secrets.tf). A workflow with no deliver step gets nothing here.
# Mirrors cdk/lib/deliver.ts.
locals {
  deliver_on      = local.workflow_plane && length([for id, t in local.task_specs : id if try(t.kind, "") == "deliver"]) > 0
  deliver_targets = flatten([for id, t in local.task_specs : try(t.targets, []) if try(t.kind, "") == "deliver"])
  # An s3 target's uri with the ${storage} token resolved to the build's bucket (storage.tf);
  # as written otherwise. The runtime resolves the same uri from STORAGE_BUCKET.
  deliver_s3    = [for t in local.deliver_targets : merge(t, { uri = replace(tostring(t.uri), local.storage_token, local.storage_uri) }) if try(t.type, "") == "s3" && can(tostring(t.uri))]
  deliver_email = [for t in local.deliver_targets : t if try(t.type, "") == "email" && can(tostring(t.identityArn))]
  # arn:aws:s3:::<bucket>/<prefix>* for s3://<bucket>/<prefix>/.
  deliver_s3_resources = distinct([for t in local.deliver_s3 : "arn:aws:s3:::${trimprefix(t.uri, "s3://")}*"])
  deliver_identities   = distinct([for t in local.deliver_email : tostring(t.identityArn)])
  deliver_from         = distinct([for t in local.deliver_email : tostring(try(t.fromAddress, ""))])
  # Left as written only when the build creates no storage.
  deliver_storage_uris = [for t in local.deliver_s3 : t.uri if startswith(t.uri, "$${storage}")]
  deliver_secret_names = sort(distinct([for t in local.deliver_targets : tostring(t.secret) if try(t.type, "") == "webhook" && try(tostring(t.secret), "") != ""]))
  # Only the names a webhook target signs with; no key at all without one, so the runtime
  # secret of a build with no deliver step is what it was.
  deliver_secrets_env = { for k, v in { DELIVER_SECRETS = jsonencode({ for n in local.deliver_secret_names : n => try(var.deliver_secrets[n], "") }) } : k => v if length(local.deliver_secret_names) > 0 }
}
resource "terraform_data" "deliver_steps" {
  count = local.deliver_on ? 1 : 0
  lifecycle {
    precondition {
      condition     = local.assets_enabled
      error_message = "app/workflow.json has a deliver step, which sends the run's files: set orchestrator.files so the run keeps them."
    }
    precondition {
      condition     = length(local.deliver_storage_uris) == 0
      error_message = "app/workflow.json deliver target uri ${try(local.deliver_storage_uris[0], "")} uses $${storage}, which needs the build's storage (orchestrator.storage)."
    }
    precondition {
      condition     = length(setsubtract(local.deliver_secret_names, nonsensitive(keys(var.deliver_secrets)))) == 0
      error_message = "app/workflow.json deliver target(s) sign with a deliverSecrets secret the build does not have (var.deliver_secrets): ${join(", ", local.deliver_secret_names)}. Save it on the step's panel, or remove `secret`."
    }
  }
}
resource "aws_iam_role_policy" "runtime_deliver" {
  count = local.deliver_on && local.assets_enabled ? 1 : 0
  name  = "AgentCoreRuntimeDeliver-${var.agent_name}"
  role  = aws_iam_role.runtime[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [{
        Sid      = "DeliverReadRunFiles", Effect = "Allow", Action = ["s3:GetObject"],
        Resource = ["${aws_s3_bucket.assets[0].arn}/runs/*/outputs/*"]
        }, {
        Sid       = "DeliverListRunFiles", Effect = "Allow", Action = ["s3:ListBucket"],
        Resource  = [aws_s3_bucket.assets[0].arn],
        Condition = { StringLike = { "s3:prefix" = ["runs/*/outputs/*"] } }
      }],
      length(local.deliver_s3_resources) > 0 ? [{
        Sid      = "DeliverS3Put", Effect = "Allow", Action = ["s3:PutObject"],
        Resource = local.deliver_s3_resources
      }] : [],
      length(local.deliver_identities) > 0 ? [{
        Sid       = "DeliverSesSend", Effect = "Allow", Action = ["ses:SendEmail"],
        Resource  = local.deliver_identities,
        Condition = { StringEquals = { "ses:FromAddress" = local.deliver_from } }
      }] : [],
    )
  })
}
