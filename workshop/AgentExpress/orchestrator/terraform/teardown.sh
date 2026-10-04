#!/usr/bin/env bash
#
# Tear down EVERYTHING this stack created in the CURRENT AWS account, in the
# right order, then clean up the pieces Terraform can't:
#
#   1. terraform destroy            (all TF-managed resources)
#   2. revert CloudWatch Transaction Search  (enabled via a null_resource, so
#                                             `destroy` does NOT undo it)
#   3. empty + delete the remote-state S3 bucket (created by bootstrap-state.sh,
#                                                 outside Terraform)
#   4. remove local backend/init artifacts
#
# Derives the account id + bucket name from your active creds — nothing hardcoded.
# Mirrors bootstrap-state.sh. Safe to re-run; each step is best-effort/idempotent.
#
# NOTE on step 2: Transaction Search is an ACCOUNT-WIDE setting. If other
# workloads in this account rely on it, skip that step (KEEP_TRANSACTION_SEARCH=1).
#
# Usage:
#   ./teardown.sh                           # prompts for confirmation
#   FORCE=1 ./teardown.sh                   # skip the confirmation prompt
#   KEEP_TRANSACTION_SEARCH=1 ./teardown.sh # leave Transaction Search enabled
#   KEEP_STATE_BUCKET=1 ./teardown.sh       # destroy resources but keep the state bucket
#
# Optional env overrides:
#   AWS_REGION / AWS_DEFAULT_REGION   region              (default us-east-1)
#   STATE_NAME                        bucket infix        (default multiagent-orchestrator)
#
set -euo pipefail
cd "$(dirname "$0")"

REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
export AWS_DEFAULT_REGION="$REGION"
NAME="${STATE_NAME:-multiagent-orchestrator}"

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
if [[ -z "$ACCOUNT_ID" || "$ACCOUNT_ID" == "None" ]]; then
  echo "ERROR: could not resolve your AWS account (aws sts get-caller-identity)." >&2
  exit 1
fi
BUCKET="agentcore-${NAME}-tfstate-${ACCOUNT_ID}"

echo "About to DESTROY the stack and delete state for:"
echo "  Account : ${ACCOUNT_ID}"
echo "  Region  : ${REGION}"
echo "  State   : s3://${BUCKET}"
echo
if [[ "${FORCE:-0}" != "1" ]]; then
  read -r -p "Type the account id (${ACCOUNT_ID}) to confirm: " CONFIRM
  [[ "$CONFIRM" == "$ACCOUNT_ID" ]] || { echo "Aborted."; exit 1; }
fi

# The Gateway M2M secret must be resolvable for `destroy` to evaluate variables.
# Empty is fine for destroy — nothing is being created. (When create_cognito is
# true Terraform manages that client itself, so this is usually unset anyway.)
# The declared variable is `gateway_client_secret` (variables.tf). This used to name
# a variable that does not exist, and Terraform silently ignores TF_VAR_ for an
# undeclared variable — so the safeguard did nothing and only worked because the real
# variable defaults to "".
export TF_VAR_gateway_client_secret="${TF_VAR_gateway_client_secret:-}"

# --- 1. terraform destroy --------------------------------------------------
if [[ ! -f backend.hcl ]]; then
  echo "NOTE: backend.hcl not found — running ./bootstrap-state.sh first to wire the backend."
  ./bootstrap-state.sh
fi
echo "==> terraform destroy"
terraform destroy -auto-approve

# --- 2. revert Transaction Search -----------------------------------------
if [[ "${KEEP_TRANSACTION_SEARCH:-0}" == "1" ]]; then
  echo "==> leaving CloudWatch Transaction Search enabled (KEEP_TRANSACTION_SEARCH=1)"
else
  echo "==> reverting CloudWatch Transaction Search (trace segment destination -> XRay)"
  aws xray update-trace-segment-destination --region "$REGION" --destination XRay >/dev/null 2>&1 \
    && echo "   done." || echo "   skipped (was not enabled or already reverted)."
fi

# --- 3. empty + delete the remote-state bucket -----------------------------
if [[ "${KEEP_STATE_BUCKET:-0}" == "1" ]]; then
  echo "==> keeping state bucket ${BUCKET} (KEEP_STATE_BUCKET=1)"
elif aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "==> emptying + deleting state bucket ${BUCKET}"
  # Delete all object versions, then all delete markers, then the bucket.
  vers="$(aws s3api list-object-versions --bucket "$BUCKET" \
            --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' --output json 2>/dev/null || echo '{}')"
  if [[ "$vers" != '{}' && "$vers" != '{"Objects":null}' ]]; then
    aws s3api delete-objects --bucket "$BUCKET" --delete "$vers" >/dev/null 2>&1 || true
  fi
  dm="$(aws s3api list-object-versions --bucket "$BUCKET" \
          --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' --output json 2>/dev/null || echo '{}')"
  if [[ "$dm" != '{}' && "$dm" != '{"Objects":null}' ]]; then
    aws s3api delete-objects --bucket "$BUCKET" --delete "$dm" >/dev/null 2>&1 || true
  fi
  aws s3api delete-bucket --bucket "$BUCKET" --region "$REGION" >/dev/null 2>&1 \
    && echo "   deleted." || echo "   could not delete bucket (may be non-empty or already gone)."
else
  echo "==> state bucket ${BUCKET} not found; skipping."
fi

# --- 4. local cleanup ------------------------------------------------------
echo "==> removing local backend/init artifacts (.terraform, backend.hcl)"
rm -rf .terraform backend.hcl

echo
echo "Teardown complete for account ${ACCOUNT_ID}."
echo "To deploy into another account: switch credentials, then"
echo "  ./bootstrap-state.sh && terraform apply"
