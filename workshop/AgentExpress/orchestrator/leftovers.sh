#!/usr/bin/env bash
#
# List what a destroy left behind. REPORTS ONLY: it never deletes anything.
#
#   ./leftovers.sh <agent_name> [region]
#
# Every resource this project creates carries the tag  agentexpress:app = <agent_name>
# (terraform/versions.tf default_tags; bin/orchestrator.ts). After a destroy nothing should
# carry it, so whatever the Resource Groups Tagging API still returns for that exact value
# is a leftover of THIS stack and no one else's: another deployment has another agent_name.
# That is the whole ownership rule, and why this can be run in a shared account.
#
# Why it does not delete: what removes a leftover differs by resource type (a log group, a
# Cognito domain and a Runtime each have their own call), and a wrong delete in a shared
# account is worse than a listed leftover. It prints the ARNs; you decide.
#
# Honest limits:
#   * only resources of types the Tagging API covers, and only those that were tagged;
#   * a resource deleted seconds ago can still be listed for a few minutes, so run it again
#     before acting on a surprising line;
#   * IAM roles and CloudFront are global and are read from us-east-1;
#   * a KMS key a destroy has scheduled for deletion keeps its tag until the waiting period ends.
#     It is listed apart ("Scheduled for deletion") and does not make the exit status 10. To know
#     that, the only other call made is the read-only `kms describe-key`, once per tagged key.
#
# Exit status: 0 nothing found, 10 something found, 1 it could not look (bad usage or no
# access). `terraform/teardown.sh` calls it and carries on either way.
#
set -uo pipefail

AGENT="${1:-}"
REGION="${2:-${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}}"
if [[ -z "$AGENT" ]]; then
  echo "usage: $0 <agent_name> [region]" >&2
  exit 1
fi

# Plain strings and `while read`, not arrays or `mapfile`: this runs under the bash 3.2 that
# ships with macOS, where an empty array is an error under `set -u` and `mapfile` is absent.
regions="$REGION"
[[ "$REGION" != "us-east-1" ]] && regions="$regions us-east-1"

found=""
looked=0
for r in $regions; do
  if ! out="$(aws resourcegroupstaggingapi get-resources --region "$r" \
        --tag-filters "Key=agentexpress:app,Values=${AGENT}" \
        --query 'ResourceTagMappingList[].ResourceARN' --output text 2>&1)"; then
    echo "could not look in ${r}: ${out}" >&2
    continue
  fi
  looked=$((looked + 1))
  found="${found}
$(printf '%s' "$out" | tr '\t' '\n')"
done

if [[ "$looked" -eq 0 ]]; then
  exit 1
fi

# The same ARN can come back from two regions (global services); list it once.
unique="$(printf '%s\n' "$found" | sed -e '/^$/d' -e '/^None$/d' | sort -u)"

# A KMS key cannot be deleted at once: a destroy schedules it (7 to 30 days) and it stays tagged
# until then. A key in that state is on its way out and nothing can be done about it, so it is
# reported on its own line and is not a leftover. A key whose state cannot be read stays a leftover.
leftover=""
scheduled=""
while IFS= read -r arn; do
  [[ -z "$arn" ]] && continue
  if [[ "$arn" =~ ^arn:aws[a-z-]*:kms:([a-z0-9-]+):[0-9]+:key/ ]]; then
    state="$(aws kms describe-key --region "${BASH_REMATCH[1]}" --key-id "$arn" \
               --query KeyMetadata.KeyState --output text 2>/dev/null || true)"
    if [[ "$state" == "PendingDeletion" ]]; then
      scheduled="${scheduled}
${arn}"
      continue
    fi
  fi
  leftover="${leftover}
${arn}"
done <<EOF
$unique
EOF
leftover="$(printf '%s\n' "$leftover" | sed '/^$/d')"
scheduled="$(printf '%s\n' "$scheduled" | sed '/^$/d')"

if [[ -n "$scheduled" ]]; then
  echo "Scheduled for deletion by KMS (not a leftover; the tag stays until the waiting period ends):"
  printf '%s\n' "$scheduled" | sed 's/^/  /'
fi

if [[ -z "$leftover" ]]; then
  echo "Nothing tagged agentexpress:app=${AGENT} remains."
  exit 0
fi

count="$(printf '%s\n' "$leftover" | wc -l | tr -d ' ')"
echo "Still tagged agentexpress:app=${AGENT} (${count}). These belong to this stack and nothing else; none of them was deleted:"
printf '%s\n' "$leftover" | sed 's/^/  /'
echo "If a line was deleted in the last few minutes it may still be listed; run this again before you act."
exit 10
