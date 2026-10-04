# --- CloudWatch Transaction Search (account-wide, one-time) ----------------
#
# AgentCore Observability and the Evaluations/Insights features read agent OTEL
# spans from the CloudWatch "aws/spans" log group. Those spans only land there
# once Transaction Search is enabled for the account (it switches X-Ray span
# ingestion into CloudWatch Logs). That is normally a manual console step per new
# account; we provision it here so a fresh-account deploy is self-contained.
#
# Transaction Search is an ACCOUNT-WIDE SINGLETON, so the native
# AWS::XRay::TransactionSearchConfig resource can only ever CREATE it and errors
# with "AlreadyExists" on any account where it is already on. To make the deploy
# idempotent (never fail when it's already enabled), we enable it via an
# idempotent CLI step that checks the current state first — mirroring the
# null_resource + local-exec pattern used for the image build and KB ingestion.

# Resource policy letting X-Ray deliver spans to the aws/spans (and
# application-signals) log groups. This one IS safe to manage in Terraform: it is
# keyed by name and updates in place, so it never hits the singleton problem.
resource "aws_cloudwatch_log_resource_policy" "transaction_search" {
  count       = local.workflow_plane ? 1 : 0
  policy_name = "${var.agent_name}-transaction-search"

  policy_document = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "TransactionSearchXRayAccess"
      Effect    = "Allow"
      Principal = { Service = "xray.amazonaws.com" }
      Action    = "logs:PutLogEvents"
      Resource = [
        "arn:aws:logs:${var.region}:${local.account_id}:log-group:aws/spans:*",
        "arn:aws:logs:${var.region}:${local.account_id}:log-group:/aws/application-signals/data:*"
      ]
      Condition = {
        ArnLike      = { "aws:SourceArn" = "arn:aws:xray:${var.region}:${local.account_id}:*" }
        StringEquals = { "aws:SourceAccount" = local.account_id }
      }
    }]
  })
}

# Idempotent enablement: only flips the trace-segment destination to CloudWatch
# Logs if it isn't already, so re-running (or an account where it's already on)
# is a no-op instead of an error. The indexing-percentage update is best-effort
# and never fails the apply.
resource "null_resource" "transaction_search" {
  count = local.workflow_plane ? 1 : 0
  # `always_run` changes every apply, so the idempotent check below RE-RUNS on
  # each `terraform apply` and reconciles drift — e.g. if someone disabled
  # Transaction Search in the console, the next apply turns it back on. (A
  # null_resource has no drift detection, so without this it would run only
  # once.) The check itself is a no-op when already enabled, so this is cheap.
  triggers = {
    always_run          = timestamp()
    region              = var.region
    indexing_percentage = var.transaction_search_indexing_percentage
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      set -euo pipefail
      REGION="${var.region}"
      PCT="${var.transaction_search_indexing_percentage}"

      # A change still in progress (Status PENDING, for up to a few minutes) refuses
      # another with "Updates are not allowed while the current status is PENDING".
      for _ in $(seq 1 30); do
        STATUS="$(aws xray get-trace-segment-destination --region "$REGION" \
                    --query Status --output text 2>/dev/null || echo UNKNOWN)"
        [ "$STATUS" != "PENDING" ] && break
        echo "Trace segment destination is PENDING; waiting..."
        sleep 10
      done
      DEST="$(aws xray get-trace-segment-destination --region "$REGION" \
                --query Destination --output text 2>/dev/null || echo None)"
      if [ "$DEST" != "CloudWatchLogs" ]; then
        echo "Enabling Transaction Search (trace segment destination -> CloudWatchLogs)"
        aws xray update-trace-segment-destination --region "$REGION" --destination CloudWatchLogs
      else
        echo "Transaction Search already enabled; leaving as-is."
      fi

      # Best-effort: set the indexed percentage. Never fail the apply on this.
      if aws xray update-indexing-rule --region "$REGION" --name Default \
           --rule "Probabilistic={DesiredSamplingPercentage=$PCT}" >/dev/null 2>&1; then
        echo "Set Transaction Search indexing percentage to $PCT%."
      else
        echo "Skipped indexing-rule update (non-fatal)."
      fi
    EOT
  }

  depends_on = [aws_cloudwatch_log_resource_policy.transaction_search]
}
