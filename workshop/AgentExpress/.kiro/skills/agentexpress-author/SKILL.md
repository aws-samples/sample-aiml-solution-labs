---
name: agentexpress-author
description: Author a new AgentExpress multi-agent workflow from a described use case — the workflow.json config, the agent packages and their system prompts, tool definitions (kb / websearch / mcp / openapi / lambda / apigateway / smithy), review gates, named guardrails, memories, evaluators, identities and Cedar policies, and UI strings. Also use to edit a bundle exported from the AgentExpress console before importing it back. Use when someone wants to put their own domain on this framework, add or change an agent, add a data source, or asks what they need to edit to implement a use case. Also use when a workflow.json change is rejected at plan/synth, a scaffolded agent fails to import, or a run shows no cost and no prompts for an agent.
---

# AgentExpress Workflow Author

## Overview

AgentExpress turns a declared config into a deployed multi-agent system: durable
human-review gates, re-run from any step, grounded output, per-call cost and token
telemetry, and RBAC enforced server-side. This skill covers authoring a customer's own
workflow on it.

**The whole value is the small edit surface.** A customer owns four directories. The
framework generates everything else from them — IaC in both Terraform and CDK, Gateway
targets, Cedar policies, the Bedrock Guardrail, IAM roles, the console UI and the graph.

## The four surfaces a customer owns

| Surface | When it is touched | What it holds |
|---|---|---|
| `orchestrator/app/workflow.json` | Always | Agents, topology, tools, gates, RBAC, guardrail policy, UI strings |
| `orchestrator/app/subagents/<id>/` | Always | One folder per agent: `agent.py`, `prompts.py`, `__init__.py` |
| `orchestrator/app/tools/<name>/` | Only for a tool shipping code | `openapi.json` for `type: openapi`; a code tool's `handler.py` under `app/tools/_code/<key>/` (`tools.<key>.code`); a hosted MCP server's `server.py` there too (`tools.<key>.hosted`) |
| `orchestrator/kb_docs/<corpus>/` | Only for a Knowledge Base tool | The customer's documents; each top-level folder is one corpus |

A reasoning-only workflow touches two. A workflow with its own data source touches three
or four. Nothing else is a customer edit.

## What you MUST NOT edit

You **MUST NOT** hand-edit `orchestrator/terraform/`, `orchestrator/cdk/`,
`orchestrator/app/workflow.schema.json`, `orchestrator/app/defaults.json`, or anything
under `orchestrator/app/common/`, `app/features/`, `app/orchestrator/`, `bff/` or
`web/src/` while authoring a workflow. All of it is either generated from config or is
framework code shared by every deployment. Framework code contains **zero** references to
any sample agent id or asset type, which is the property that keeps it generic — adding
one breaks it for the next customer.

## Authoring procedure

You **MUST** follow [author-workflow.sop.md](author-workflow.sop.md). It sequences the
work, names the script for each step, and validates before advancing.

[SAMPLE-PROMPTS.md](SAMPLE-PROMPTS.md) holds ready-to-paste prompts a customer can start
from, including one that rebuilds this project's own workflow.

Load [references/checklist.md](references/checklist.md) first — the ten things the
framework needs from a customer, how to tell an answer from a gesture at one, and which two
items get a stated default rather than a question. A vague brief does not fail; it produces
something confidently wrong, which is worse.

Load [references/contract.md](references/contract.md) for the exact legal shapes — tool
types and their required keys, the step forms, the agent entry keys, and the `ctx` API an
agent may call.

## The two rules that break deployments

1. **Every external call goes through `ctx`.** `ctx.llm`, `ctx.call_tool`,
   `ctx.retrieve`, `ctx.memory_recall` and the rest are where cost metering, token
   counting, guardrails, OTEL spans and policy checks are applied. A direct `boto3` call
   works and produces no telemetry, no guardrail check and no evaluation — the agent looks
   free and unmonitored in Observability.
2. **No figure without a source.** `app/common/grounding.py` flags any number in an
   agent's output that appears in no upstream asset and no tool result, and the run is sent
   back. Agents bound to a `tool` are exempt, because their figures come from the tool.

## Verifying the work

The framework ships its own gates, and they derive their expectations from the
customer's `workflow.json` rather than from this sample:

```bash
cd orchestrator
python3 -m pytest                    # config plane
ruff check .
python3 format_workflow.py --check    # canonical key order
python3 build_schema.py --check       # schema and defaults in sync

cd web    && npx tsc --noEmit && npx vitest run && npm run build
cd ../cdk && npx tsc --noEmit -p . && npx jest
cd ../terraform && terraform fmt -check -recursive
```

A failure here means the customer's config is wrong, not that a test is stale.

## Known sharp edges

- **Tool keys must match `^[A-Za-z][A-Za-z0-9]*$`** — letters and digits, starting with a
  letter. The key builds both the Gateway target name, which allows no underscores, and the
  Cedar policy name `permit_<key>`, which allows no hyphens, so neither separator survives
  both. `claims_history` is rejected at synth; `claimsHistory` is fine. **Agent ids are
  different and may contain underscores**, which is why `cost_research` is legal.
- **Seven tool types are framework code**, not config: `kb`, `websearch`, `mcp`,
  `openapi`, `lambda`, `apigateway`, `smithy`. Another would mean editing both IaC paths, so
  `lambda` is the escape hatch for anything else — a warehouse, an RDBMS, a VPC resource,
  a control-plane API. A code tool (`tools.<key>.code`) is written in the build and
  deployed by the framework with only the grants it names.
- **Cedar conditions read `context.input.<argument>` only**, on one tool
  (`action == AgentCore::Action::"<key>___<tool>"`). `context.arguments` matches nothing and
  `context.query` fails the deploy. A web search tool is `<key>___WebSearch`, a knowledge base
  `<key>___retrieve`. Both validators reject the wrong forms, in `orchestrator.policy.custom`
  too. A policy on a shared `{"library": …}` tool goes in `orchestrator.policy.custom`
  (`{name, description, statement}`), because the library entry itself is never edited.
- **Denied topics** are matched on their definition: `name` 1–100 of letters, digits,
  spaces and `- _ ! ? .`; `definition` ≤ 200 characters; ≤ 5 `examples` of ≤ 100.
- **Region.** `websearch` runs in us-east-1, eu-west-1 and ap-northeast-1 only; image agents
  render where Bedrock serves the model (us-west-2 today): fine for a US deployment, refused
  elsewhere unless the agent sets `image.allowCrossRegion: true`.
- **Capabilities follow the agent's settings.** An agent with `vision` needs a model that
  takes images; one with `toolMode: "model"` needs a model that calls tools. Problems flags a
  mismatch.
- **Knowledge Base storage** is S3 Vectors unless the `kb` tool sets `vectorStore` (an existing
  OpenSearch, Aurora, Neptune Analytics, Pinecone or MongoDB Atlas store the customer owns).
- **Steps without agent code:** `runtime: "task"` (tool, code, extract, output) and
  `runtime: "harness"` (an AgentCore Harness from `instructions`, or `harness.arn`) need no folder
  under `app/subagents/`. See [references/contract.md](references/contract.md).
- **Hosted MCP servers** (`tools.<key>.hosted` instead of `endpoint`) run on their own AgentCore
  Runtime behind the Gateway: the key is at most 32 characters, `auth` stays unset, and the starter
  `requirements.txt` pins `mcp==1.30.0` and `pydantic==2.13.5` (an older MCP SDK fails to import).
- **Code sources** (`code.codeSource`, `task.codeSource`, `hosted.code`) take git or S3 instead of
  files in the bundle; a git `token` is a secret's NAME. S3 in the console's own account works only
  for buckets the operator listed.
- **Prompt caching** (`agents.<id>.promptCache`) is per agent, `main` and `dedicated` only, and
  limited to what the model's row in `promptCacheModels` (`app/vocabulary.json`) lists.
- **Runtime network**: `orchestrator.network` for `main` and `task` agents, `agents.<id>.network`
  for `dedicated` ones, `harness.network` for harness agents; `a2a` agents take none. The default is
  `PUBLIC`.
- **The topology is a declared DAG.** Dynamic agent-to-agent routing is not expressible;
  `branch` covers conditional paths.
- **`app/keys.json` is the source of truth**, not `workflow.schema.json` — the schema is
  generated from it by `build_schema.py`.
