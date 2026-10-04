# AgentExpress: Multi-Agent Orchestrator (LangGraph + Amazon Bedrock AgentCore)

AgentExpress is a configuration-driven multi-agent orchestrator built with **LangGraph**
and deployed on **Amazon Bedrock AgentCore Runtime**. One file, `workflow.json`, declares
the agents, their topology, tools, human review gates, access rules and the AgentCore
features each agent uses. Terraform or CDK turns that file into a running system with a
web console.

It ships with a generic **intake → research → analysis → report** pipeline of nine agents
that exercises every pattern: sequential and parallel stages, six tool types,
human-in-the-loop review, content-based branching, durable state and agents you don't own.

**Who it's for:** teams that want their own multi-agent workflow on AWS without writing
the orchestration, infrastructure or UI. This is a **reference pattern**, not a finished
product: replace the prompts, contracts and Knowledge Base corpus with your own.

## What you edit

**The fastest path is to ask.** Open this repository in Kiro and describe your use case:

> Use the agentexpress-author skill to build this workflow. Settle household insurance
> claims: read the FNOL, check the policy wording for coverage, price the repair, and
> produce a settlement decision a human approves.

The skill in [`.kiro/skills/agentexpress-author/`](.kiro/skills/agentexpress-author/)
checks your description against a ten-item readiness checklist, asks for what is missing,
confirms the topology with you, then clears this sample, scaffolds your agents, writes
their prompts and runs the test suite. Paste-ready prompts are in
[`SAMPLE-PROMPTS.md`](.kiro/skills/agentexpress-author/SAMPLE-PROMPTS.md). A `PreToolUse`
hook ([`.kiro/hooks/guard-edit-boundary.sh`](.kiro/hooks/guard-edit-boundary.sh)) blocks
writes to framework files while you implement a workflow, and
`tests/test_edit_boundary.py` catches anything that gets past it.

To do it by hand, follow [GETTING_STARTED.md](GETTING_STARTED.md). Either way, this is
your whole surface:

| You edit | For |
|---|---|
| `orchestrator/app/workflow.json` | agents, topology, tools, HITL gates, RBAC, guardrail policy, UI strings |
| `orchestrator/app/subagents/<id>/` | one folder per agent: its prompt, its contract, its `run()`. `python3 scaffold.py agent <id>` writes the folder and the config entry together |
| `orchestrator/app/tools/<name>/` | only for a tool that declares `source`: the handler or schema the framework packages and deploys |
| `orchestrator/kb_docs/` | your documents; each top-level folder becomes a corpus |
| `orchestrator/terraform/terraform.tfvars` | deploy-time only: region, login, model, Gateway on/off (copy from `.example`) |

No Terraform or CDK edits, no Cedar policy to write and no tests to fix:

- Both IaC paths generate the Gateway targets, authorization rules and Guardrail from
  `workflow.json`, and the tests derive their expectations from it.
- A mistake in `workflow.json` fails at `terraform plan` / `cdk synth` with a message
  naming the entry, not at deploy or run time.
- `workflow.json` points at `workflow.schema.json`, so your editor completes legal keys,
  lists allowed values and shows each key's default on hover.
  `python3 format_workflow.py` enforces a canonical key order.
- Defaults live in one place (`app/keys.json`, `app/vocabulary.json`; the schema and
  `defaults.json` are generated). A test fails if any Python, HCL or TypeScript code
  restates a default.
- Portability is tested: swapping in a four-agent workflow from another domain, and
  renaming a shipped agent, both pass the full suite, `terraform validate` and `cdk synth`
  with no other change.

**Two couplings remain.** The six tool *types* (`kb`, `websearch`, `mcp`, `openapi`,
`lambda`, `apigateway`) are code, and Knowledge Base storage is S3 Vectors. The `lambda`
type is the escape hatch: anything the Gateway cannot reach directly (a warehouse, an
RDBMS, a VPC resource) is reachable through a function. Your own vocabulary is not a
coupling: `assetType`, `sourceType` and `sectionType` are open strings.

## Capabilities at a glance

### Patterns

- **Sequential and parallel.** A `parallel` group (agents run concurrently, one gate after
  all) and a `sequence` group (agents in order, one gate after the chain).
- **Human-in-the-loop.** Approve / revise / deny gates after a single agent, a parallel
  group (revise re-runs only the agents you flag) or a sequence (revise re-runs the chain).
- **Content-based branching.** A step's `branch` block lets its output choose what runs
  next: jump ahead, skip a stage or end the run. Rules compare a field of the output
  (`equals`, `in`, `contains`, `exists`, `gt`/`lt`, ...); first match wins; with no match
  and no `default` the run continues. Bypassed agents show as **skipped** and the rule
  that fired is written to the timeline. Full syntax:
  [WORKFLOW_REFERENCE.md](orchestrator/docs/WORKFLOW_REFERENCE.md).
- **Rewind and re-run.** After a run settles, re-run one agent (and everything downstream)
  or part of a parallel stage with a note as feedback. Each run is kept as a numbered
  version.
- **Structured, verified output.** Every agent emits a Pydantic contract asset with an
  evidence classification and claim tracing. Invented citation URLs are dropped, and
  figures that appear in no upstream asset are flagged for the reviewer.
- **Agents you don't own (A2A).** `runtime: "a2a"` plus an Agent Card URL delegates a step
  over the Agent2Agent protocol. Gates, branching, re-run, guardrails and memory still
  work; evaluations and Cedar cannot reach inside the other service.
- **Per-agent placement.** `runtime: "main"` runs in-process; `"dedicated"` gives the agent
  its own AgentCore Runtime, with trace context propagated.
- **Any agentic framework inside `run()`.** The sample has a Strands agent and a nested
  LangGraph. The model call must go through `ctx.llm` to keep guardrails, telemetry and
  cancellation.
- **Identity and RBAC.** `idp` selects Cognito, Auth0 or no login for both the UI and
  agent-to-Gateway calls. The `authorization` block maps JWT groups to actions on runs
  (`start`, `decision`, `rerun`, `cancel`, `evaluate`, `insights`, `delete`), builds
  (`deploy`, `destroy`) and users (`audit`, `admin`). Unlisted actions stay open, except
  `audit` and `admin` (closed unless granted) and `insights` (closed once any action is
  restricted).
- **Run management.** Cancel a running pipeline or delete a past run. Runs are private to
  whoever started them; an admin can read them but not act on them.
- **Activity log.** Sign-ins (a Cognito trigger), sign-outs and every run action from the
  BFF (`run.started`, `run.decided`, `run.rerun`, ...), plus `run.completed`,
  `run.failed` and `run.denied` written by the runtime (`app/common/audit.py`). Shown on
  the **Activity** page; an admin sees everyone's over a date range of up to 31 days.
- **Run Assistant.** An in-app chat that answers questions about your runs and takes
  actions (approve a gate, re-run, evaluate) through the same APIs as the buttons, under
  the same RBAC. It is on unless `orchestrator.chatbot.enabled` is `false`, its title
  comes from `ui.assistantTitle`, and replies render Markdown.
- **Durable state and live progress.** LangGraph checkpoints in AgentCore Memory survive
  HITL pauses; status is mirrored to DynamoDB and the UI polls it. Timestamps are stored
  in US Eastern (`YYYY-MM-DD HH:MM:SS`) and shown in each viewer's own timezone.

### Tool types

Every data source is declared in `tools`; both IaC paths generate the Gateway target and
the Cedar permit from it. The sample uses the first five, one per research agent.

| Type | What it reaches |
|---|---|
| `kb` | Bedrock Knowledge Base on S3 Vectors, scoped to a corpus by metadata filter |
| `websearch` | the AWS-managed AgentCore Web Search connector |
| `mcp` | any remote MCP server (default: AWS's managed AWS Knowledge MCP Server, no key needed) |
| `openapi` | a REST API from an OpenAPI schema; each `operationId` becomes a tool |
| `lambda` | your function (`lambdaArn`), or one built from `app/tools/<source>/` |
| `apigateway` | an API Gateway REST API stage in the same account and region (unused here) |

### AgentCore features

Declared per agent under `agentcore` in `workflow.json`; each lives in its own folder
under [`orchestrator/app/features/`](orchestrator/app/features/).

| Feature | What it does here | Config key |
|---|---|---|
| Runtime | hosts the orchestrator and each `dedicated` agent; async tasks survive HITL pauses (8h limit) | `runtime` |
| Gateway | one OAuth-authed MCP endpoint in front of every tool | the agent's `tool` |
| Memory | short-term = LangGraph checkpointer; long-term = semantic + summary, recalled before and stored after an agent runs | `memory.longTerm` |
| Identity | outbound Workload Identity tokens for calling an API directly | `identity.outbound` |
| Observability | OTEL spans per agent and tool, cross-runtime traces, metered tokens, latency and cost | always on |
| Guardrails | Bedrock `ApplyGuardrail` on input and/or output; a block halts the run | `guardrails.input`, `guardrails.output` |
| Evaluations | LLM-as-judge over the real run, per prompt per version; auto or on demand | `evaluations` |
| Policy | Cedar authorization enforced at the Gateway (`ENFORCE` or `LOG_ONLY`) | `orchestrator.policy`, `policy.enabled` |
| Optimization | cross-run Insights: failure patterns, user intents, execution summaries | `orchestrator.insights` |

The **Observability** tab shows cost and latency by date, model and user, a per-run
breakdown with a Prompts & I/O inspector (exact prompts, tool results, memory,
guardrail and policy decisions), evaluation scores and Insights.

### Image generation and image reading

- **Image agents.** `output: "image"` makes an agent's model write an image brief that an
  image model renders; the reviewer sees the images at the gate. The `image` block sets
  model, aspect ratio, format, seed and a negative prompt. Images stay in the
  deployment's own assets bucket.
- **Vision.** `vision: {"from": ["<agent>", ...], "maxImages": N}` lets a later agent's
  model see images produced earlier in the same run (default 4, max 20). The model must
  be a Bedrock model that accepts image input (Claude, Nova Pro/Lite, Llama 3.2 Vision).
  A typical use: one agent generates, the next validates what it produced.
- Bedrock's text guardrails and evaluations do not inspect images. The sample workflow
  does not use either feature.

### The console: Builder, library, sharing

With the Builder on (the default; `enable_builder = false` / `-c builder=false` turns it
off), the deployment is also a console for designing workflows and deploying each as its
own stack.

- **Build view** has two tabs over the same autosaved draft: **AgentExpress Assistant**, a
  chat that drafts and edits the workflow as you talk, and **Build manually**, a canvas
  plus Tools, Policies, Guardrails, Memory, Evals, Identity, Settings and `workflow.json`
  tabs. Validation status shows in the header. **Export** gives a bundle or
  `workflow.json` only; **Import a file…** loads either.
- **Deploy.** Each build deploys with CDK or Terraform from the console's CodeBuild
  project. A build's own app (**Open app**) has no Build view, library or admin; it runs
  and observes that build only and is titled "🧭 AgentExpress - \<ui.title\>".
- **Console modes.** `console_mode` / `-c consoleMode` is `app` (default: this workflow's
  app with the Builder beside it) or `builder` (a control plane with no runs of its own).
- **Library** (Tools, Identity, Memory, Evals, Policies, Guardrails). Items are private
  until shared with emails, groups or everyone. Builds use items live, so a change shows
  everywhere at once (a deployed build picks it up on its next deploy). From a build's
  tabs you can **Publish**, **Unpublish**, **Make a copy for this build** and **Share**.
  Deleting an item leaves each build that used it with its own copy. Identity secrets
  never go into the library.
- **Sharing a build.** Share with emails, groups or everyone. A save based on an older
  copy is refused (409) so collaborators cannot overwrite each other.
- **Moving a build.** Import the bundle into another console, or run
  `python3 scaffold.py apply <bundle> --exact` in a clone and deploy with either IaC path.
  Secrets are not in the bundle. Details in [DEPLOYMENT.md](DEPLOYMENT.md).

## The sample workflow

Four stages, nine agents, defined in
[`orchestrator/app/workflow.json`](orchestrator/app/workflow.json):

```
1 intake ─(HITL)─▶ 2 [ knowledge_research ‖ web_search ‖ documentation_search
     │                 ‖ cost_research ‖ lifecycle_research ]
     │                 ─(HITL)─▶ 3 [ analysis → recommendation ] ─(HITL)─▶ 4 report
     ├──(branch)──▶ 3, skipping research, if the brief has no research questions
     └──(branch)──▶ END, if the brief has no objective at all
```

| Agent | Stage / pattern | Runtime | Tool | Reasons with | Notable features |
|---|---|---|---|---|---|
| `intake` | 1, single + gate + branch | main | none | `ctx.llm` | input guardrail, auto eval |
| `knowledge_research` | 2, parallel | dedicated | `kb` (corpus `reference`) | nested LangGraph | policy |
| `web_search` | 2, parallel | dedicated | `websearch` | Strands | output guardrail, policy |
| `documentation_search` | 2, parallel | dedicated | `docs` (MCP) | `ctx.llm` | output guardrail, policy |
| `cost_research` | 2, parallel | main | `pricing` (Lambda from `app/tools/pricing/`) | `ctx.llm` + code | policy |
| `lifecycle_research` | 2, parallel | main | `lifecycle` (OpenAPI from `app/tools/lifecycle/`) | `ctx.llm` + code | policy |
| `analysis` | 3, sequence | a2a | its own | someone else's | semantic memory, output guardrail |
| `recommendation` | 3, sequence | a2a | its own | someone else's | semantic + summary memory, output guardrail |
| `report` | 4, terminal (no gate) | main | none | `ctx.llm` | input + output guardrail, auto eval |

`analysis` and `recommendation` have no folder under `app/subagents/`; out of the box they
run on a stand-in Lambda (`a2a_lambda/`) so the A2A path works end to end. For them,
memory and guardrails still apply, evaluations score a role descriptor plus the real
output (on demand only), and Cedar cannot see their tool calls.

## Architecture

![Architecture diagram](architecture.png)

> [`architecture.drawio`](architecture.drawio) is the editable source. Open it at
> [diagrams.net](https://app.diagrams.net) or the VS Code Draw.io extension and re-export
> to `architecture.png` after any change.

In short: Browser → CloudFront (S3 UI) and API Gateway (JWT authorizer) → a Lambda BFF →
the orchestrator on AgentCore Runtime (LangGraph), which calls dedicated runtimes, A2A
agents and the AgentCore Gateway (Cedar-enforced tool targets), with state in AgentCore
Memory and DynamoDB. The UI is Vite + React + TypeScript on Cloudscape, built from source
at deploy time. Components, data flow and the security model are in
[ARCHITECTURE.md](ARCHITECTURE.md).

## Repository map

```
.kiro/
├── skills/agentexpress-author/   describe a use case, get a workflow (Kiro skill)
└── hooks/guard-edit-boundary.*   blocks framework edits while you implement a workflow
orchestrator/
├── app/
│   ├── workflow.json             the single source of truth (yours)
│   ├── subagents/<id>/           one package per agent whose code you ship (yours)
│   ├── tools/<source>/           code or schema for tools that declare `source` (yours)
│   ├── keys.json, vocabulary.json   framework-owned key and value specs
│   ├── workflow.schema.json, defaults.json   generated from those
│   ├── common/                   shared framework: Agent base, context, assets, branching
│   ├── features/                 one folder per AgentCore capability
│   └── orchestrator/             graph builder, nodes, registry, runtime, local server
├── web/                          the console UI (Vite + React + TypeScript, Cloudscape)
├── bff/                          Lambda BFF: API, RBAC, assistant, Builder, library
├── deployer/                     CodeBuild runner that deploys builds from the console
├── kb_lambda/, a2a_lambda/, signup_lambda/   Gateway KB target, A2A stand-in, Cognito triggers
├── kb_docs/                      Knowledge Base corpora (yours)
├── scaffold.py                   reset the sample, add an agent, apply a bundle
├── format_workflow.py, build_schema.py   formatting and generated-file checks
├── docs/WORKFLOW_REFERENCE.md    every workflow.json key, what reads it, what it does
├── tests/                        pytest config-plane suite (no AWS, no model)
├── cdk/                          CDK / TypeScript IaC (parity with terraform/)
└── terraform/                    Terraform IaC
```

## Run, test and deploy

- **Deploy:** Terraform (`orchestrator/terraform/`) or CDK (`orchestrator/cdk/`), with full
  parity and the same `idp` switch. Both build the container image and the UI from source
  (a container engine, plus Node.js ≥ 20 for the UI). Variables, login modes, IAM, users and groups,
  teardown and troubleshooting are in [DEPLOYMENT.md](DEPLOYMENT.md).
- **Run locally:** `uvicorn app.orchestrator.server:app` serves the same API and UI; it
  needs AWS credentials with Bedrock access. There is no offline mode: a failed model or
  tool call fails the run rather than inventing data. Steps in
  [GETTING_STARTED.md](GETTING_STARTED.md).
- **Tests:** pytest (1296), CDK jest (269 in 6 files) and web vitest (265 in 30 files).
  The pytest suite needs no AWS account or model.

## Security summary

This is a reference sample, not a hardened product. Review it before any non-sandbox use.
The full model is in [ARCHITECTURE.md](ARCHITECTURE.md).

**Built in**
- SPA login (Cognito or Auth0) and an API Gateway JWT authorizer on `/api/*`.
- M2M client-credentials tokens for agent → Gateway calls, pinned by `client_id`.
- Cedar policy enforced at the Gateway (default-deny in `ENFORCE`), so a prompt-injected
  attempt to widen tool access is refused by infrastructure.
- RBAC on run and build actions (`bff/authz.py`); the assistant is held to the same rules.
- Bedrock Guardrails on agent input and output, per agent.
- SigV4/IAM for internal calls; scoped IAM roles per component and per dedicated agent.
- No secrets in the repo; secrets are passed at deploy time. A runtime reads its Gateway client
  secret and A2A tokens from its own Secrets Manager secret, not its environment.
- Baseline on both IaC paths: point-in-time recovery on every table, TLS-only encrypted buckets,
  API access logs and a throttle, CloudFront security headers (HSTS, frame-deny, nosniff), Cognito
  passwords of 12+ characters with symbols and optional TOTP MFA, pinned dependencies and base
  image, an immutable ECR repository.
- Scanned with cdk-nag (`-c nag=true`, and in `npm test`) and checkov (`terraform/.checkov.yaml`);
  each accepted finding carries its reason in `cdk/lib/nag.ts` or the checkov config.

**Harden before production**
- `idp = "none"` leaves the UI and API open to anyone with the URL; both IaC paths refuse it without `allow_unauthenticated = true` (`-c allowUnauthenticated=true`).
- Ownership of runs and builds is enforced in the BFF, not the DynamoDB tables.
- The console's deploy project can create IAM roles: restrict `deploy` and `destroy`. It cannot
  edit its own role or policies, and attaches only its own policies and `ReadOnlyAccess`, but a
  CDK deploy runs as the bootstrap's CloudFormation execution role (AdministratorAccess unless
  bootstrapped with `--cloudformation-execution-policies`).
- No WAF on CloudFront, and the default `*.cloudfront.net` certificate; add both with a domain.
- A code tool's grants reach only resources tagged `agentexpress:code-tools=true`.
- Buckets use `force_destroy = true`; enable versioning and retention for real data.
- The shipped guardrail and Cedar rules are samples; replace them in `workflow.json`.
- Telemetry captures prompt and output content; lower `OBS_MAX_CAPTURE_CHARS` and restrict
  table access if inputs are sensitive.
- The assistant can take actions; turn off its action tools in `orchestrator.chatbot.tools`
  to stop that.

**Cost and scale.** Serverless and scale-to-zero (AgentCore, Lambda, on-demand DynamoDB, S3
Vectors). Claude Haiku is the default model. Evaluations, Insights, guardrails and
Transaction Search above 1% cost extra, and all are metered in the Observability tab. The
UI polls rather than receiving push, and calls to dedicated agents are synchronous.

## Where to go next

- [GETTING_STARTED.md](GETTING_STARTED.md): deploy the sample, point it at your data, add
  agents, walk through the Build console, customize, run tests.
- [DEPLOYMENT.md](DEPLOYMENT.md): Terraform and CDK, identity providers, IAM, users and
  groups, Builder console deploy, teardown, troubleshooting.
- [ARCHITECTURE.md](ARCHITECTURE.md): components, data flow, runtime, BFF, IaC parity,
  stores, security model.
- [orchestrator/docs/WORKFLOW_REFERENCE.md](orchestrator/docs/WORKFLOW_REFERENCE.md):
  every `workflow.json` key, including the full `branch` operator reference.

## Security and license

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

This library is licensed under the MIT-0 License. See the [LICENSE](LICENSE) file.
