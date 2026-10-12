#!/bin/bash
# Block writes outside the four surfaces a customer owns.
#
# WHY A HOOK AND NOT A CONSTRAINT IN THE SKILL
# The skill says "You MUST NOT edit terraform/, cdk/, app/common/ ...". That is a request
# to a model, and a model under pressure to make a test pass will edit a framework file
# and explain why it was justified. An exit code is not negotiable.
#
# WHAT IT DOES NOT DO
# It is a DEV-TIME GUARD, not a security boundary. It sees only the write tools, so an
# agent determined to route around it can use `execute_bash` with a heredoc. That is
# deliberate: closing that path means policing every shell command, which breaks the
# build, the formatters and the generators. The backstop is
# `orchestrator/tests/test_edit_boundary.py`, which inspects the working tree rather than
# the call — so if this hook is bypassed or disabled, the gates still catch it.
#
# FAILS OPEN, LOUDLY. If the payload cannot be parsed, this warns and allows, because the
# alternative is an agent that cannot write anything at all the first time Kiro changes a
# field name. The unparsed payload is appended to /tmp/agentexpress-hook-unparsed.jsonl so
# the key can be corrected rather than guessed at twice.
set -uo pipefail

EVENT=$(cat)

if ! command -v jq >/dev/null 2>&1; then
    echo "guard-edit-boundary: jq is not installed, so the edit boundary is NOT being" \
         "enforced. Install jq, or rely on orchestrator/tests/test_edit_boundary.py." >&2
    exit 1                      # not 2: warn, do not block
fi

TOOL=$(jq -r '.tool_name // empty' <<<"$EVENT")

# The path's key is not documented for the write tools, so try every plausible name. The
# first non-empty string wins.
FILE=$(jq -r '[.tool_input.path, .tool_input.file_path, .tool_input.filePath,
               .tool_input.targetFile, .tool_input.file, .tool_input.target_file]
              | map(select(type == "string" and length > 0)) | first // empty' <<<"$EVENT")

if [ -z "$FILE" ]; then
    printf '%s\n' "$EVENT" >> /tmp/agentexpress-hook-unparsed.jsonl
    echo "guard-edit-boundary: could not find a file path in the ${TOOL:-unknown} payload," \
         "so this write is NOT being checked. The payload is in" \
         "/tmp/agentexpress-hook-unparsed.jsonl — add its path key to the jq chain in" \
         ".kiro/hooks/guard-edit-boundary.sh." >&2
    exit 1                      # not 2: warn, do not block
fi

CWD=$(jq -r '.cwd // empty' <<<"$EVENT")
ROOT=${CWD:-$PWD}

# Compare repo-relative, so an absolute path and a relative one are judged the same way.
REL=${FILE#"$ROOT"/}
REL=${REL#./}

# THE FOUR SURFACES, plus DEPLOY CONFIG and the files the framework's generators write.
#   workflow.json                  the configuration
#   app/subagents/**               one folder per agent: prompt, contract, run()
#   app/tools/**                   a tool's handler.py or openapi.json
#   kb_docs/**                     the customer's documents
#
# DEPLOY CONFIG IS NOT A FRAMEWORK EDIT, and leaving it out was a real defect in the first
# version of this hook: it would have blocked a customer setting their own region. Which
# account, region, model and identity provider to deploy into is theirs to choose, and the
# two files that carry it live inside otherwise off-limits directories:
#   terraform/terraform.tfvars     region, model, idp, gateway on/off (gitignored)
#   cdk/cdk.json                   the same, as CDK context
# .env and *.creds.env are local secrets, never committed, and nothing here reads them at
# build time — blocking a write to one would only stop someone configuring their own shell.
#
# workflow.schema.json and defaults.json are GENERATED from app/keys.json by
# build_schema.py, which scaffold.py runs — blocking them would block the framework's own
# scripts and look like the hook was broken.
case "$REL" in
    orchestrator/app/workflow.json|\
    orchestrator/app/workflow.schema.json|\
    orchestrator/app/defaults.json|\
    orchestrator/app/subagents/*|\
    orchestrator/app/tools/*|\
    orchestrator/kb_docs/*|\
    orchestrator/terraform/terraform.tfvars|\
    orchestrator/cdk/cdk.json|\
    orchestrator/.agentexpress-customer|\
    .env|*.env|\
    .kiro/*)
        exit 0 ;;
esac

# Outside the repo entirely — a scratch file in /tmp, say. Not our business.
case "$REL" in
    /*) exit 0 ;;
esac

cat >&2 <<EOF
guard-edit-boundary: BLOCKED a write to a framework file.

  $REL

A workflow implementation touches only these four places:

  orchestrator/app/workflow.json     agents, topology, tools, gates, RBAC, UI strings
  orchestrator/app/subagents/<id>/   one folder per agent
  orchestrator/app/tools/<name>/     a tool's handler.py or openapi.json
  orchestrator/kb_docs/<corpus>/     your documents

plus your deploy configuration, which is also yours to set:

  orchestrator/terraform/terraform.tfvars    region, model, identity provider
  orchestrator/cdk/cdk.json                  the same, as CDK context

Everything else — the IaC, the Gateway targets, the Cedar policies, the guardrail, the
IAM, the BFF and the UI — is GENERATED from those four. Editing it directly is either
overwritten on the next generate or left disagreeing with the config that produced its
neighbours.

If a framework file genuinely has to change, that is a framework change rather than a
workflow one: make it deliberately, on its own, and say so. Disable this hook in
.kiro/hooks/guard-edit-boundary.json while you do.
EOF
exit 2                          # 2 blocks the tool call; stderr goes back to the agent
