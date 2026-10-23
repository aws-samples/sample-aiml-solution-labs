# workflow.json settings

Everything about a workflow lives in one file, `workflow.json`: the agents, how they connect, their tools, the review gates, who may do what, and the AgentCore features each agent uses. Read it when you edit an exported file or review a change: it walks through the file block by block, with short examples. For every key, its default and what reads it, see [WORKFLOW_REFERENCE.md](https://github.com/aws-samples/sample-aiml-solution-labs/tree/main/workshop/AgentExpress/orchestrator/docs/WORKFLOW_REFERENCE.md) in the code repository.

# Start with the smallest workflow

`workflow.json` is the single source of truth. AWS CDK and Terraform both read it and create the same resources from it: runtimes, the Gateway and its targets, Cedar permits, guardrails, memories, evaluators and IAM grants. You never edit Terraform, CDK or Cedar to change a workflow.

Two keys are required on an agent (`name`, `maxTokens`) and two blocks at the top (`agents`, `steps`). This deploys and runs: two agents, with a person reviewing the first.

```json
{
  "$schema": "./workflow.schema.json",
  "agents": {
    "intake": { "name": "Intake", "maxTokens": 2000, "produces": "brief" },
    "report": { "name": "Report", "maxTokens": 4000, "produces": "report" }
  },
  "steps": [ { "agent": "intake", "hitl": true }, { "agent": "report" } ]
}
```

Everything else is optional. In the control plane, the file is on the [workflow.json tab](Workflow_JSON_Tab.md). In the code repository it is `orchestrator/app/workflow.json`.

## Check a change

The same rules run in four places: your editor (from `workflow.schema.json`), the control plane as you edit, the control plane's deploy before CodeBuild starts, and `terraform plan` or `cdk synth` when you deploy yourself. An unknown key is kept, not dropped, so the validator can name it. If a key is not in WORKFLOW_REFERENCE.md, nothing reads it. `python3 format_workflow.py` puts every agent, tool and step in the canonical key order.

# Know the top-level blocks

| Block | Holds | Control plane tab |
|---|---|---|
| `orchestrator` | Engine-wide settings: default model, policy, limits, files, triggers, interceptors, export, live config | [Settings](Settings.md) and others |
| `ui` | The strings people see | [Settings](Settings.md) |
| `guardrail` | The deployment guardrail | [Guardrails](Guardrails.md) |
| `authorization` | Which groups may take which actions, and who signs people in | [Settings](Settings.md), [Identity](Identity.md) |
| `tools` | Every data source an agent may call | [Tools](Tools_and_Gateway.md) |
| `skills` | Know-how agents open when needed | [Skills](Skills.md) |
| `agents` | One entry per agent or step | Design (the canvas) |
| `steps` | The order, the review gates and the branches | Design (the canvas) |
| `guardrails`, `memories`, `evaluators`, `identities`, `policies` | Named blocks, defined once and used by name | One tab each |

Account-level values, such as the Region, the sign-in provider and whether the Gateway is on, are deploy settings, not in this file. So the same file deploys in any account.

# Set engine-wide options

The `orchestrator` block. Every key is optional.

```json
"orchestrator": {
  "defaultModel": "<a Bedrock model or inference profile id>",
  "policy": { "enabled": true, "mode": "LOG_ONLY" },
  "models": { "overrides": ["us.amazon.nova-pro-v1:0", "us.amazon.nova-lite-v1:0"] },
  "limits": { "userDailyUsd": 5, "userRunsPerHour": 20, "monthlyUsd": 200 },
  "files": { "retentionDays": 30 },
  "liveConfig": true
}
```

Other keys: `network` (where the orchestrator's AgentCore Runtime runs: Public or your VPC; see [Runtimes](Runtimes.md)), `gatewayIdentity`, `interceptors`, `triggers`, `attachments.s3`, `export`, `chatbot`, `modelRates` and `priceList`, `runtimeInvoke`, `a2aInvoke` and `insights`. Each is explained on the page of the feature it sets, linked from the [Settings tab](Settings.md).

# Describe agents

One entry per step, keyed by the agent's id: letters, digits and underscores, starting with a letter, with no hyphens.

```json
"contract_review": {
  "name": "Contract Review", "runtime": "dedicated", "tool": "kb", "corpus": "policies",
  "maxTokens": 4000, "toolMode": "model",
  "agentcore": {
    "memory": { "longTerm": ["semantic"] },
    "guardrails": { "output": true },
    "evaluations": { "enabled": true, "auto": true, "evaluators": ["Builtin.Faithfulness"] }
  }
}
```

- Common keys: `name`, `runtime`, `model`, `maxTokens`, `produces`, `tool`, `corpus`, `toolMode`, `framework`, `skills`, `attachments`, `askPerson`, `models` (routing and fallbacks).
- The `agentcore` block holds the agent's switches: `memory`, `guardrails`, `evaluations` and `identity`. There is no per-agent policy switch: Cedar applies at the Gateway to every agent.
- `promptCache`: what Bedrock caches for the agent's model calls (`system`, `tools`, `messages`, `ttl`). Off when absent. See [Prompt caching](Prompt_Caching.md).
- Runtime-only keys: `task`, `instructions` and `harness`, `agentCard` and `auth`, and `network` for a `dedicated` agent's own Runtime. See [Runtimes](Runtimes.md).

# Declare tools

One entry per tool, keyed by a name of letters and digits starting with a letter (`policyDocs`, not `policy_docs`).

```json
"tools": {
  "kb": { "type": "kb", "corpora": ["policies"] },
  "websearch": { "type": "websearch", "maxResults": 10 },
  "docs": { "type": "mcp", "endpoint": "https://knowledge-mcp.global.api.aws", "call": "aws___search_documentation" }
}
```

Each type and its keys are on [Tool types in depth](Tool_Types.md), including `hosted` (an MCP server AgentExpress runs for you) and `codeSource` (code from Git or S3). Secrets never go in this file.

# Order the steps

Each entry in `steps` is `{ "agent": "id" }`, `{ "parallel": [...] }` or `{ "sequence": [...] }`, and may add `hitl`, `gateId`, `gateName` and `branch`.

```json
"steps": [
  { "agent": "intake", "hitl": true,
    "branch": { "when": [{ "field": "objective", "exists": false, "goto": "END" }] } },
  { "parallel": ["policyCheck", "research"], "hitl": true, "gateId": "evidence", "gateName": "Evidence" },
  { "sequence": ["drafter", "reviewer"], "hitl": true, "gateId": "signoff", "gateName": "Sign-off" },
  { "agent": "report" }
]
```

Reorder, add or remove steps freely: the graph, the UI and the deployed resources follow. See [Stages and steps](Stages_and_Steps.md).

# Grant actions and name shared blocks

The `authorization` block maps actions to groups:

```json
"authorization": {
  "groupsClaim": "cognito:groups",
  "actions": { "decision": ["approvers"], "rerun": ["approvers"], "admin": ["admins"] }
}
```

An action not listed is open to anyone signed in; one listed with groups needs one of them; one with an empty list is denied to everyone. `admin`, `audit` and `override` are closed unless granted. See [Users, groups and RBAC](Users_Groups_and_RBAC.md).

Named blocks are defined once and used by name. A name is letters and digits, starting with a letter, up to 32 characters.

| Block | Becomes | Used by |
|---|---|---|
| `guardrails.<name>` | Its own Bedrock Guardrail | `agentcore.guardrails.use` |
| `memories.<name>` | Its own AgentCore Memory | `agentcore.memory.use` |
| `evaluators.<name>` | An AgentCore evaluator | `Custom.<name>` in `agentcore.evaluations.evaluators` |
| `identities.<name>` | An AgentCore Identity credential provider | A tool's `identity`; `agentcore.identity.outbound` |
| `policies.<name>` | One Cedar policy per tool it is attached to | A tool's `policies` |

# Ask the Assistant

The AgentExpress Assistant edits this same file, so you can ask for any key on this page by name or in plain words. Each change is checked by the Build view's own rules, and the Assistant fixes what fails. Requests you can paste:

- "Set orchestrator.limits to 5 dollars a day and 20 runs an hour per user"
- "Give the parallel stage the gate id evidence and the gate name Evidence"
- "Add a named memory called notes, with the summary strategy, kept for 30 days per user, and use it on the report agent"
- "Build this workflow based on the attached spec"

How its edits land in the file:

- **Agents**: it writes a whole agent, or changes some of its keys. A nested block such as `agentcore` or `harness` is written whole.
- **Steps**: it writes the whole `steps` list, in order.
- **Blocks**: `orchestrator`, `ui`, `guardrail`, `authorization` and the named blocks take only the keys it names; the rest stay. A map inside a block, such as `orchestrator.triggers`, is written whole. Setting a named entry to `null` removes it.
- Paste a link to docs or a spec ("Build this based on &lt;url>"), or attach files, up to 5 of each per message.
- It never writes a secret into the file, and it can't import or export the file. Use **Project → Import a file…** and the **Export** menu.

More requests are on [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [workflow.json tab](Workflow_JSON_Tab.md): the file inside the control plane
- [Export, edit and import](Export_and_Import.md): download, edit and import it back
- [Deploy from your laptop](Deploy_from_Your_Laptop.md): deploy settings outside the file
- [WORKFLOW_REFERENCE.md](https://github.com/aws-samples/sample-aiml-solution-labs/tree/main/workshop/AgentExpress/orchestrator/docs/WORKFLOW_REFERENCE.md): every key, in full
