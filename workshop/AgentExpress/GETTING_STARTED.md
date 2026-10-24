# Getting started

The framework is configuration driven. To run your own multi-agent system on your own data, you
edit `workflow.json` and add one folder per agent (plus one per tool, only when you ask the
framework to supply that tool's artifact):

| What | Where |
|---|---|
| Tools, agents, topology, gates, RBAC, guardrail, UI strings | `orchestrator/app/workflow.json` |
| Each agent's prompt, contract and `run()` | `orchestrator/app/subagents/<agent_id>/` |
| A tool the framework supplies (entry declares `source`) | `orchestrator/app/tools/<source>/` |
| Your documents: each top-level folder is a corpus | `orchestrator/kb_docs/` |
| Your account: region, login, model, Gateway on/off (gitignored) | `orchestrator/terraform/terraform.tfvars` |

No Terraform, CDK, Cedar or UI edits. Both IaC paths read `workflow.json`, so declaring a tool or
agent provisions it. Every key is in
[orchestrator/docs/WORKFLOW_REFERENCE.md](orchestrator/docs/WORKFLOW_REFERENCE.md).

## 1. Deploy the sample

Get the reference workflow running first as a known-good baseline (about 20 minutes).

```bash
cd orchestrator/terraform
cp terraform.tfvars.example terraform.tfvars      # defaults are fine to start
./bootstrap-state.sh                              # S3 remote state, once per account
terraform apply -target=null_resource.ui_build    # first apply of a new stack only
terraform apply                                   # builds the image, provisions everything
```

You need Terraform, a container engine that builds `linux/arm64` (Finch, Docker or Podman), AWS
credentials, and Bedrock access to the model in `terraform.tfvars` and the embedding model (Titan
Text Embeddings V2 by default; override with `embeddingModel` on the `kb` tool).

Create a login, add yourself to a group so you can approve gates, and open `terraform output
ui_url`. Type a request, approve the gates, and watch the run. User and group setup, CDK, Auth0, Okta, Entra ID,
no-login, IAM, the Builder console and teardown are in [DEPLOYMENT.md](DEPLOYMENT.md).

## 2. Point it at your data

Everything an agent can call is declared in the `tools` block. Each key is both the Gateway
target name and the label agents reference.

> **Tool naming and `tools/list`.**
> - A key is letters and digits starting with a letter: `internalTools`, not `internal_tools` or
>   `internal-tools` (Gateway targets forbid underscores, Cedar permits forbid hyphens).
> - The Gateway publishes each tool as `<targetName>___<toolName>`; AWS-managed servers add their
>   own `___` prefix. Set `call` to the published name minus `<targetName>___`.
> - `tools/list` is paginated. Follow `nextCursor`, or a target looks empty when its tools are on
>   page two. The app does; ad hoc scripts often don't.

To see what was published after a deploy:

```bash
TOKEN=$(curl -s -X POST "https://$(terraform output -raw cognito_domain_prefix).auth.${AWS_REGION:-us-east-1}.amazoncognito.com/oauth2/token" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -u "<m2m-client-id>:<secret>" -d 'grant_type=client_credentials&scope=gateway/invoke' | jq -r .access_token)
GW=$(terraform output -raw gateway_url)
CURSOR=""
while : ; do
  BODY=$(jq -nc --arg c "$CURSOR" '{jsonrpc:"2.0",id:1,method:"tools/list",
           params: (if $c == "" then {} else {cursor:$c} end)}')
  RESP=$(curl -s -X POST "$GW" -H "Authorization: Bearer $TOKEN" \
    -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
    -d "$BODY")
  echo "$RESP" | jq -r '.result.tools[].name'
  CURSOR=$(echo "$RESP" | jq -r '.result.nextCursor // empty')
  [ -z "$CURSOR" ] && break
done
```

If a target really lists nothing, the agent raises `ToolUnavailable` and the run fails with that
reason. The framework never substitutes invented data.

Secrets never go in `workflow.json`. An API key or client secret goes in `TF_VAR_tool_api_keys`
(CDK: `TOOL_API_KEYS`), a JSON map keyed by tool, or in a build's Secrets in the console. It is
vaulted in an AgentCore credential provider; the agent never sees it.

### `kb`: your documents

```json
"kb": { "type": "kb", "corpora": ["policies", "runbooks"],
        "policy": { "tool": "retrieve", "restrictTo": { "filter": ["policies", "runbooks"] } } }
```

Each top-level folder of `kb_docs/` is a corpus; an agent is scoped to one with `corpus`. The next
apply uploads, re-ingests and updates the Cedar permit, which allows retrieval only with a filter
naming a declared corpus. A build can instead use Inspector uploads per corpus, your own bucket
(`s3Uri`, optional `kmsKeyArn`) or an existing Knowledge Base (`knowledgeBaseId`, never deleted).
A console deploy of a build whose uploaded corpus has no document is refused before it starts,
naming the corpus and the tool; a bundle never carries documents, so put them under
`kb_docs/<corpus>/` when you apply one yourself.

### `websearch`: the live web

```json
"websearch": { "type": "websearch", "maxResults": 10, "domains": { "exclude": ["example.com"] } }
```

The AWS-managed AgentCore Web Search connector: no key or endpoint, available in `us-east-1`,
`eu-west-1` and `ap-northeast-1`. `domains` is set on the Gateway target, so it is hidden from the
agent and applies to every request. Web Search's terms require displaying citations: the framework
keeps each result's title, URL and date, drops cited URLs not in the evidence, and renders sources
as links. Keep that in your own agents.

### `mcp`: your MCP server

```json
"wiki": { "type": "mcp", "endpoint": "https://mcp.deepwiki.com/mcp",
          "call": "ask_question", "arg": "question", "args": { "repoName": "aws/aws-cdk" } }
```

`call` picks the tool (required when the server publishes several), `arg` is the parameter the
query goes into (default `query`), `args` are fixed extras. `listingMode: "DYNAMIC"` fetches the
catalogue per call instead of caching it. `auth` is `none`, `apikey` (sent as `X-API-Key`),
`sigv4` (the Gateway signs with its own role) or `oauth2`. The sample's `docs` tool is the AWS
Knowledge MCP Server (`https://knowledge-mcp.global.api.aws`), which needs no credentials.

**No server yet? Host it here.** Set `hosted` instead of `endpoint` (Builder: **Host it here** in
the tool's MCP server panel), and write, upload or link the code (Git, S3 or a private ECR image).
The deploy runs it on its own AgentCore Runtime and registers it on the Gateway, so its tools show
up as `<key>___<tool>`. The canvas starts you with a `server.py` and a `requirements.txt` pinned
to `mcp==1.30.0` and `pydantic==2.13.5`; the Assistant can write them for you too. Details:
[DEPLOYMENT](DEPLOYMENT.md#hosted-mcp-servers).

```json
"facts": { "type": "mcp", "description": "Look up reference facts.", "call": "lookup_fact", "arg": "query", "hosted": {} }
```

### `openapi`: any REST API

```json
"billing": { "type": "openapi", "schemaS3Uri": "s3://your-bucket/billing-openapi.yaml" }
```

The OpenAPI 3 document can be in S3 (`schemaS3Uri`), in `app/tools/<source>/openapi.json`
(`source`) or inline (`schema`); in the Builder you can also upload it or have the designer write
it. Tool names come from each `operationId`. An operation an agent calls should accept `query`.

### `apigateway`: an API Gateway REST API

```json
"orders": { "type": "apigateway", "restApiId": "a1b2c3d4e5", "stage": "prod",
            "toolFilters": [{ "path": "/orders/*", "methods": ["GET"] }], "auth": "sigv4" }
```

A stage in the deploy's account and region. `toolFilters` is an allow list: a path is explicit
(`/orders/{id}`) or a prefix ending in `/*`. With `sigv4` the Gateway gets `execute-api:Invoke` on
exactly that API and stage.

### `lambda`: your database, or anything else

A Lambda that can reach your warehouse, RDBMS or VPC resource becomes a tool. It has no
`tools/list`, so `toolSchema` declares each tool; a malformed ARN, empty schema, unknown property
type, or a `call`/`arg` that names nothing fails plan/synth. `call` is required when one function
publishes several tools.

```json
"claims": {
  "type": "lambda",
  "lambdaArn": "arn:aws:lambda:us-east-1:123456789012:function:query-claims",
  "call": "query_claims", "arg": "question",
  "toolSchema": [{ "name": "query_claims", "description": "Answer a question from the claims warehouse.",
    "properties": { "question": { "type": "string", "required": true, "description": "The question." } } }]
}
```

- **`lambdaArn`: yours.** You deploy it; the framework registers the target, grants the Gateway
  `lambda:InvokeFunction` on that ARN and (same account) adds the resource-policy statement. No
  secret involved. Cross-account works if the owning account adds that statement.
- **`source`: shipped.** `"source": "pricing"` deploys `app/tools/pricing/` (the sample's `pricing`
  tool: real AWS on-demand unit rates, never totals). Its role is fixed at CloudWatch Logs plus
  read-only price list access, so `pricing` is the only accepted value. For your own connector,
  which usually needs a VPC, secret or table grant, use `lambdaArn`.
- **`code`: written in the build.** **Write it here** on a Lambda tool in the Builder (or ask the
  AgentExpress Assistant). Its files (`handler.py` with `lambda_handler(event, context)`,
  `requirements.txt`, other `*.py`, `events.json` test events) travel with the build and deploy in
  its account as `ToolLambda-<agentName>-<key>`. **Where the code comes from** can also be an
  upload, a Git repository (a private one's token goes in the build's secrets) or Amazon S3,
  fetched at deploy ([code sources](DEPLOYMENT.md#code-from-files-git-or-s3-code-sources)).

For `code`, grants are a fixed menu: one secret (`SECRET_NAME`), one DynamoDB table read or
read/write (`TABLE_NAME`), one S3 prefix (`S3_PREFIX`), a VPC, e.g. `"code": { "grants": {
"secret": "payments/api-key", "table": "orders", "tableAccess": "readwrite" } }`. None means no AWS
access, and a permissions boundary denies the framework's own resources. A grant reaches only a
secret, table or bucket tagged `agentexpress:code-tools=true` (for a bucket, also turn on S3 ABAC:
`aws s3api put-bucket-abac`); an untagged one is AccessDenied. The deploy is refused
unless the code parses and defines the handler, requirements are `name==version` lines, and no key
is hard-coded; lint and security findings are warnings. **Check and run in the sandbox** runs each
test event in an AgentCore Code Interpreter; after deploy, **Test tool** calls the real function.

### More tool kinds

| Kind | Entry | Details |
|---|---|---|
| AWS API | `"type": "smithy"` with the service's Smithy model and an `allow` list of actions on ARNs | [DEPLOYMENT](DEPLOYMENT.md#tools-that-call-an-aws-api-smithy) |
| Code Interpreter | `"type": "lambda", "source": "codeInterpreter"` (Builder: **Add a tool → Type → Code Interpreter**) | [DEPLOYMENT](DEPLOYMENT.md#running-code-in-a-sandbox) |
| Browser | `"type": "lambda", "source": "browser"`, optional `allowDomains` (**Add a tool → Type → Browser**) | [DEPLOYMENT](DEPLOYMENT.md#opening-web-pages-with-a-browser) |
| Connector | **Tools → Add a connector**: an `openapi` tool for Salesforce, Slack, Jira, GitHub and others | [DEPLOYMENT](DEPLOYMENT.md#connectors) |

## 3. Add your own agent

**Fast path: the Kiro skill.** Open the repository in Kiro and say *"Use the agentexpress-author
skill to build \<your use case\>"*. It scores your description against a checklist, asks for the
gaps, confirms the topology, then does everything below. Paste-ready briefs are in
[SAMPLE-PROMPTS.md](.kiro/skills/agentexpress-author/SAMPLE-PROMPTS.md). A `PreToolUse` hook blocks
writes outside the files you own.

**`scaffold.py`.** `agent` writes the `workflow.json` entry and a complete folder whose `run()`
really calls the model; placing it in `steps` is left to you.

```bash
cd orchestrator
python3 scaffold.py reset --dry-run     # replacing the sample: see what it removes
python3 scaffold.py reset               # leaves one working agent in one gated step
python3 scaffold.py agent contract_review --tool contracts_kb   # --dry-run to preview
```

### The agent folder contract

The folder name is the agent id: `app/subagents/contract_review/` holds `__init__.py` (`from
.agent import agent`), `agent.py` and `prompts.py`. The framework asks for three things: a package
exporting `agent`, a subclass of `Agent`, and a `run()`. Miss one and `registry.check_agent_module`
names the file and the fix, including a class that never overrode `run()`; `pytest` catches it.

```python
# agent.py
from app.common.base import Agent
from app.common.context import AgentContext
from app.subagents._shared import research
from .prompts import SYSTEM_PROMPT

class ContractReviewAgent(Agent):
    system_prompt = SYSTEM_PROMPT

    async def run(self, ctx: AgentContext) -> str:
        # Gathers evidence from the tool bound in workflow.json, returns a validated asset.
        return await research.synthesize(ctx, system_prompt=SYSTEM_PROMPT)

agent = ContractReviewAgent()
```

`run()` has `ctx.llm(...)`, `ctx.call_tool(label, query)`, `ctx.retrieve(query, doc_type=...)`,
`ctx.input(other_agent_id)`, `ctx.heartbeat(pct)` and `ctx.log(msg)`. You can drive another
framework per agent (`web_search` uses Strands, `knowledge_research` a nested LangGraph; see
`think=` on `research.synthesize`). Keep model calls on `ctx.llm`: that is where guardrails, cost
telemetry, memory and truncation detection live.

Declare it and place it in the topology:

```json
"agents": {
  "contract_review": {
    "name": "Contract Review", "runtime": "dedicated", "tool": "kb", "corpus": "policies",
    "maxTokens": 4000,
    "agentcore": { "memory": { "longTerm": ["semantic"] }, "guardrails": { "output": true },
      "evaluations": { "enabled": true, "auto": true, "evaluators": ["Builtin.Faithfulness"] },
      "policy": { "enabled": true } }
  }
},
"steps": [
  { "agent": "intake", "hitl": true },
  { "parallel": ["knowledge_research", "contract_review"], "hitl": true, "gateId": "research", "gateName": "Research" },
  { "agent": "report" }
]
```

`"dedicated"` gives the agent its own AgentCore Runtime; `"main"` runs it in-process. Downstream
agents read inputs from the topology, so `report` picks it up without a code change. `hitl: true`
adds an approve / revise / deny gate; `branch` lets an agent's output pick the next step.

**An agent you don't own.** `"runtime": "a2a"` with an `agentCard` URL delegates the step over
A2A, with no folder; gates, branches and re-runs work unchanged. `auth` is `none`, `bearer` (token
in `TF_VAR_a2a_tokens` / `A2A_TOKENS`), `oauth2` or `sigv4`; `model`, `maxTokens`, `tool` and
`corpus` are rejected. Guardrails and memory apply; evaluations drop to role level, and its tool
calls are outside your Cedar policy. The sample's `analysis` and `recommendation` are A2A agents
backed by a shipped Lambda (`"source": "a2a_lambda"` plus `skill`).

**Agents with no folder.** `"runtime": "harness"` with `instructions` is run by an AgentCore
Harness (or `harness.arn` for one you already have); `"runtime": "task"` with a `task` block is a
step with no model. See [harness](DEPLOYMENT.md#agents-run-by-agentcore-harness) and
[task steps](DEPLOYMENT.md#steps-with-no-model-task-steps).

## 4. The console and the Builder

Open **Build** (marked *Preview*) in the side navigation. Builds save to your account as you type
and follow you to any browser. Builds and runs are private to their creator unless shared. Under
a build's name, two lines say when it was created and by whom, and when it was last edited and by
whom.

### AgentExpress Assistant

A build opens on the **AgentExpress Assistant** tab. Describe what you want.

- **Questions first, or not.** A brief with at least three of input, output, sources and steps or
  sign-off is drafted at once. A thinner one (`I want a trip planner`) gets up to three questions,
  each with the answer it would assume; `go` builds with those. "Just build it" or "ask me first"
  overrides that.
- **What it edits.** Agents, steps, tools and their code, skills, interceptors (from the templates),
  triggers and the named blocks. It also turns prompt caching on or off per agent, sets a
  Runtime's network, writes a hosted MCP server, and points code at Git or S3. It reuses your
  library items as they are, and imports from your AWS Agent Registry when asked. It cannot deploy
  or publish; you do that from the Deployment panel.
- **Links.** Give it any public page: `build this based on <url>`. It reads the page before it
  changes anything (text, title and links; scripts are not run). It refuses private or internal
  addresses and pages that need a sign-in, waits about 10 seconds per page, keeps the first
  40,000 characters, and reads up to 5 pages per message. Page text is treated as untrusted data,
  never as instructions. PDFs are not read from a link yet.
- **Review changes.** Off, Auto (the default) or Always, under the message box; your choice is
  remembered in this browser. A second model checks a change before it is saved, once, and only
  advises: the Assistant takes the notes that serve your request. Auto reviews larger changes: a
  new build, agents, stages or tools added or removed, code, security settings, or more than 8
  items. Each reply says how it went: *Reviewed: no issues*, *Reviewed: N suggestions applied* (or
  *noted*), *Review skipped (small change)* or *Review unavailable* (the change is saved anyway).
- **What you see.** Replies stream (Enter sends, Shift+Enter adds a line); changed agents are
  highlighted. **Defaults used** lists what it filled in; **Needs your input** lists what only you
  can give (endpoint, ARN, key), and the deploy waits for them. Secrets go in the **Secrets** card,
  never the chat. **Undo** / **Redo** cover its changes, canvas edits and uploads.
- **Model.** Claude Sonnet 5.5 at medium effort (Claude Sonnet 5 if the account may not use 5.5);
  change with `-c designerModel` / `-c designerEffort` (Terraform `designer_model` /
  `designer_effort`). It reads the latest 24 messages plus a running summary of older ones.

### Build manually

The canvas. Drag agents onto a stage (parallel) or between stages (a new step), drag a tool onto
an agent, and gate the stages a human signs off. Forms are generated from `app/keys.json`, and
problems are named as you make them, in the deploy's own words.

- An agent can hold several tools. With `toolMode: "model"` (the default here) its model picks
  which to call, reads each result, and stops when done or at `maxToolCalls`; `"direct"` queries
  every tool once up front. Every call goes through the Gateway either way. The tool-calling turn
  sees the agent's own instructions and the earlier steps' approved output, so a tool can act on a
  draft; a call Cedar refuses shows on the run timeline as *Refused by Cedar policy*, for a
  `dedicated` agent too (its runtime returns its timeline lines with its output).
- Models: any Bedrock text model your account can invoke, read live from Bedrock; the call adapts
  to models that refuse a setting. The picker offers only models that can do what the agent asks:
  image input for `vision`, tool calling for `toolMode: "model"`. Each model is tagged, and a
  mismatch shows in Problems. **Agent framework** (none, Strands or LangGraph) decides how
  `apply` writes `agent.py`. New agents start with the shared guardrail on. Every agent is told
  today's date.
- The header has **Project** (New build, Import a file…, Delete this build, Move to team…),
  **Export**, **Share**, and the status (*Valid*, or the error and warning count), which opens the
  problems list.
- The palette has **+ New generic step** (a [task step](DEPLOYMENT.md#steps-with-no-model-task-steps):
  tool, code, extract or output, picked in its panel) and **Tools → Add a connector**
  ([Connectors](DEPLOYMENT.md#connectors)). Each agent card lists its tools, guardrails, memory,
  Cedar policies, skills, evaluations, identity and HITL; **Show** hides kinds and turns on the
  AgentCore rail ([canvas](DEPLOYMENT.md#the-builder-canvas)).

**Tabs and named blocks.** A build keeps named `guardrails`, `memories`, `evaluators`,
`identities` (an OAuth client or API key), `policies` (Cedar) and `skills`, each on its own tab. Agents pick
them by name (`agentcore.guardrails.use`, `agentcore.memory.use`, `Custom.<name>` in
`evaluations.evaluators`, `agentcore.identity.outbound`, the agent's `skills`); a tool attaches `policies` and signs in
with `identity`. `orchestrator.gatewayIdentity: "perAgent"` gives each agent its own Gateway client.
Every row has **Edit** and **Delete**. The Identity tab's **Secrets** section takes each identity's
API key or OAuth client secret (write-only); an `apikey` tool sends it as `X-API-Key` from
AgentCore Identity, so the agent never sees it.

**Skills.** A skill is know-how an agent opens only when a task needs it: a description (when to
use it), instructions (the SKILL.md body) and up to 20 text reference files (200 KB in all). On the
**Skills** tab write one or import a SKILL.md, then pick it in an agent's **Skills**. An agent with
tools and `toolMode: "model"` opens a skill, and any of its files, itself (`use_skill`); otherwise
its skills are added to its instructions in full. Skills never run code.

**AWS Agent Registry.** **Add from registry** (Tools tab, Skills tab, Agents palette) lists what
your organization approved: an MCP server becomes an `mcp` tool, an A2A agent card a remote agent,
a SKILL.md a skill. **Keep in sync** takes each newer approved version when the build opens and
before each deploy; otherwise press **Update**. Admins **Publish to registry** a deployed build or
its skills for a curator to approve. Details: [DEPLOYMENT.md](DEPLOYMENT.md#aws-agent-registry).

### Library and sharing

The library pages hold items usable in any build, grouped as the Builder's tabs are: **Agent library** (Tools,
Skills, Memory, Guardrails), **Gateway and access library** (Identity, Policies, Interceptors) and **Quality library** (Evals).
An item is private until shared, and builds use it live: change it and every build shows the
change (a deployed build picks it up on its next deploy).

- Library page: tick items and press **Share (n)**. Deleting an item leaves each build a copy.
- Build tab: **Publish to library (n)** for ticked entries, or **Publish** on one row (an
  identity's secret stays with each build). A row using a library item offers **Make a copy for
  this build** and **Unpublish** (builds keep copies). **Share (n)** publishes ticked local
  entries first, then opens the share dialog.
- **Share** in the build header shares the build with emails, admin-defined groups or everyone.
  They can do everything you can, including deploy and delete, and get their own login to the
  deployed app. The later of two concurrent saves is refused and offers **Reload now**.

### Deploy a build

**Deploy** in the *Deployment* panel asks **AWS CDK or Terraform**, freezes the build as its next
version, and deploys it as its own stack (`ax-xxxxxxxx-stack`), so builds sit side by side. It
runs in CodeBuild (2-hour limit), usually takes 5 to 15 minutes, and continues if you sign out. The panel
shows progress, errors and the log. It folds down to one line (its status and buttons) to give the
canvas and the chat more room, and opens again by itself when a deploy starts or fails. **History**
lists every deploy and destroy: when, which version, the tool, where, who started it and how it
ended, with the reason for a failure. Someone the build is shared with sees it too.

- **Deploy to** is this console's account (its region) or a connected account (any region).
  *Connect an AWS account* launches one CloudFormation stack creating a deploy role only this
  console can assume, with an External ID unique to you. **AWS accounts** lists connections to
  rename, re-region, check, update or disconnect (refused while a build is deployed there). A
  region that can't run something the build uses is refused before the deploy starts.
- The server checks the build with the Build view's rules. Versions count successful deploys
  only. Secrets (tool keys, OAuth client secrets, identity secrets, A2A tokens) go in the
  Inspector, the Identity tab or the **Secrets** card, stored write-only in Secrets Manager.
- **Destroy** removes the stack with the tool it was deployed with. `deploy` and `destroy` are
  gated actions; treat them as admin actions. Role permissions, CDK bootstrap,
  `consoleMode=builder` and tagging are in [DEPLOYMENT.md](DEPLOYMENT.md).

**The build's own app.** Every deployed build gets one (**Open app**). The build page shows its
URL, your user name and a temporary password (also emailed; first sign-in sets your own). It has
runs, observability, the Run Assistant and its own Activity, but no Build view, AWS accounts,
library or Admin page, and is titled "🧭 AgentExpress - <ui.title>". Each run records the workflow
it ran with, so old runs keep their own graph. A run that ends early or fails says why at the top:
the branch rule (or the branch's default) that sent it to END, the gate a reviewer denied it at
(with their note), the guardrail rule that blocked it, or the agent that failed and how. A run page
has the **Graph** (numbered steps, each component's outcome and deployed name), **Flow** (**Story**
step by step, or **Summary**, with replay), **Timeline** (**Chart**, a Gantt, or **Log**, up to 1,000
events with each reviewer's note), **Outputs** and **Files**; see
[DEPLOYMENT.md](DEPLOYMENT.md#the-flow-tab-on-a-run). On **Observability**, the Run Assistant's
*this run* is the run shown in Run detail.

### Change a prompt without redeploying, and check a change
- **Live config** (build app, admins): set `"orchestrator": { "liveConfig": true }` and deploy once; then
  edit an agent's prompt or model settings, save a version and **Activate** it, or roll back to version 0.
  See [DEPLOYMENT.md](DEPLOYMENT.md#changing-a-prompt-or-model-without-redeploying-live-config).
- **Send runs to Langfuse, Datadog and others** (Builder → **Settings** → **Export**): pick the tool, enter its
  key, **Test connection**, save and deploy. Each run page then links to its trace. See
  [DEPLOYMENT.md](DEPLOYMENT.md#send-runs-to-langfuse-datadog-and-others).
- **Tests tab** (Builder): write cases (a request plus fixed checks), run them on the deployed build, and
  read the pass rate and what broke. See [DEPLOYMENT.md](DEPLOYMENT.md#test-suites).

### Activity and admins

**Activity** lists sign-ins and sign-outs, builds created and deleted, designer prompts, secrets
changed (names only), app passwords viewed, deploys and destroys (build, version, tool, account,
region), account changes and run actions, with time and source address. The runtime logs how each
run ended: *Run completed*, *Run failed* (with the error) or *Run ended (denied)*. Library items,
groups and sharing are logged too. Filter by kind or any text. With the `audit` permission,
**Everyone** shows all users' activity over a date range of up to 31 days (the last 7 by default).

A member of a group granted `admin`, `audit` and `insights` (`admins` in the sample) gets an
**Admin** page: every user's builds (with when each was last updated) and runs read-only, everyone's activity, and **destroy** on any
build. Admins can't edit, deploy, chat, decide, re-run, cancel or delete for another user, or read
their app password; each look is logged as `admin.viewed`. Admins are made only from the backend
(the deploy refuses a self-sign-up group holding those actions):

```bash
aws cognito-idp admin-add-user-to-group --user-pool-id <pool> --username you@example.com --group-name admins
```

### Export, import and moving a build

**Export → Bundle (workflow, prompts and code)** downloads one JSON file; **Export → workflow.json
only** downloads just the workflow. **Project → Import a file…** reads either back, as a new build
or replacing this one. To work in your own repo:

```bash
cd orchestrator
python3 scaffold.py apply ~/Downloads/my-workflow.agentexpress.json   # --dry-run to preview
```
Then deploy it with CDK or Terraform as in [DEPLOYMENT.md](DEPLOYMENT.md); `cdk synth` or
`terraform validate` checks the workflow before a deploy. A coding agent can edit a bundle too (Kiro,
Claude Code, with the `agentexpress-author` skill in `.kiro/`) before you import it back.

`apply` writes `workflow.json` and a folder per new agent, never overwriting code you edited: an
existing `agent.py` is kept, and `prompts.py` is rewritten only while it carries the Builder's
marker line. It warns when a bundle came from a different framework release
(`orchestrator/VERSION`). `apply --exact` makes the tree mirror the bundle: Builder prompts are
regenerated, and agent, `app/tools/` and `kb_docs/` folders the bundle doesn't name are deleted. A
console deploy runs this on a fresh framework copy.

To move a build to another console or account, import the bundle there, or run `python3
scaffold.py apply <bundle> --exact` (with `--dry-run` first) on a clone and deploy it yourself.
Bundles carry no KB uploads (put them under `kb_docs/<corpus>/`) and no secrets. Pass secrets as
JSON maps keyed by tool, identity or agent id:

| | Tool keys | Identity secrets | A2A tokens |
|---|---|---|---|
| CDK | `TOOL_API_KEYS` | `IDENTITY_SECRETS` | `A2A_TOKENS` |
| Terraform | `TF_VAR_tool_api_keys` | `TF_VAR_identity_secrets` | `TF_VAR_a2a_tokens` |

## 5. Images

**Generate.** `output: "image"` makes an image agent: its model writes an image brief and a
Stability model (Stable Diffusion 3.5 Large by default) renders it. Bedrock serves those models
in us-west-2 only today, so a US deployment renders there; a deployment outside the US is refused
unless the agent sets `image.allowCrossRegion: true`, because the brief would leave its geography. Images are stored in the deployment's assets
bucket, deleted with it, and shown at the gate. Guardrails and evaluations don't check images; the
model's own filter does.

**Validate.** A later agent reads them with `"vision": { "from": ["poster"], "maxImages": 4 }`: its
model gets the images next to its text. Give it a model that reads images (Claude, Amazon Nova Pro
or Lite, Llama 3.2 Vision) and a verdict schema such as `{"score": "0-100", "issues": ["..."]}`.
Only this run's images are read. The validator requires each agent in `from` to run in an earlier
step (not a parallel peer, not itself) and warns if it isn't an image agent.

## 6. Files a run starts with

Set `"attachments": true` on the agents that should read the files a run is started with.
**Start run** then shows **Attach files** (up to 5; documents 4.5 MB, images 3.75 MB), and the
request may name S3 objects or folders as `s3://bucket/key` or `s3://bucket/folder/` inside a
location `orchestrator.attachments.s3` lists (name a bucket you own):

```json
"orchestrator": { "attachments": { "s3": ["amzn-s3-demo-bucket/contracts"] } },
"agents": { "intake": { "name": "Intake", "attachments": true } }
```

The files are checked and copied into the run's own folder when it starts (so a re-run reads
the same ones), listed on the run page, and sent to those agents' models as document or image
content. Uploads not used within a day are deleted.

## 7. Gateway interceptors
An interceptor is a Lambda the AgentCore Gateway calls on every tool call: one before the
request reaches the tool, one after the tool answers. A Cedar policy allows or denies a call;
an interceptor can also change it, redact it or log it. On the Builder's **Interceptors** tab,
turn either on and check what it should do; each checked template becomes a section of its
`handler.py`, which you may then edit (or point it at a Lambda of yours by ARN):

| Before each request | After each answer |
|---|---|
| Audit log (no argument values, no headers) | Audit log |
| Block tools, for every agent or the ones listed | Redact email, phone, SSN, card numbers |
| Argument guard: a size cap, denied patterns | Hide tools from `tools/list` |
| Inject run context (session, agent, user) into arguments | Cap result size |
| Your own check | Your own change |

```json
"orchestrator": { "interceptors": {
  "request":  { "code": {}, "passRequestHeaders": true,
                "templates": { "audit": {}, "blockTools": { "tools": ["refunds___issueRefund"] } } },
  "response": { "lambdaArn": "arn:aws:lambda:us-east-1:123456789012:function:my-redactor" } } }
```
With `passRequestHeaders` the runtime's `x-ax-session`, `x-ax-agent` and `x-ax-user` headers
reach it (the agent's Gateway token does too: never log headers). A refusal shows on the run's
timeline as "Refused by interceptor". The Gateway may call an interceptor twice for one
request, so keep it free of side effects you would not want repeated. Both need the Gateway.

## 8. Triggers: runs nobody typed
`orchestrator.triggers` starts runs from outside, on the Builder's **Triggers** tab:

| Type | From | Set |
|---|---|---|
| `webhook` | GitHub, Jira, ServiceNow, Slack, Stripe, your system | `signature`: `agentexpress` (default), `github`, `slack`, `stripe`, `token` |
| `schedule` | EventBridge Scheduler | `expression` cron(...) or rate(...), `timezone` |
| `eventbridge` | AWS services, your applications, SaaS partners | `pattern`, `bus` |
| `s3` | an object created in a bucket | `bucket`, `prefix` |
| `sqs` | a message on a queue | `queueArn`, or none for a queue made for you, with a dead-letter queue |

```json
"triggers": {
  "jira":    { "type": "webhook", "prompt": "Triage {{body.issue.key}}: {{body.issue.fields.summary}}",
               "attachPayload": true, "maxRunsPerHour": 30 },
  "alarms":  { "type": "eventbridge", "pattern": { "source": ["aws.cloudwatch"] },
               "prompt": "Investigate {{detail.alarmName}}" },
  "nightly": { "type": "schedule", "expression": "cron(0 6 * * ? *)", "prompt": "Daily summary",
               "runAs": "service", "approvers": ["reviewers"], "gates": "auto" } }
```
`prompt` is the run's request, filled from the delivery (`{{body.x}}`, `{{headers.x}}`,
`{{detail.x}}`, `{{event.x}}`, `{{time}}`); the delivery is untrusted text and never runs as
code. A delivery seen in the last day starts no second run (`idempotencyKey` overrides the key).
`runAs` is the build's owner (default) or `service`, whose runs its `approvers` groups read and
decide. Once deployed, the app's **Triggers** page (admins) shows each webhook's URL, generates
or stores its secret (shown once), gives a signed `curl`, sends test deliveries and lists the
last ones. An S3 bucket must send its events to EventBridge; its object is attached to the run
when `orchestrator.attachments.s3` allows the bucket. A queue of yours needs a visibility timeout
above 300 seconds.

## 9. Review gates that decide for themselves
`"hitl": true` waits for a person in the app. The object form says more:
```json
{ "agent": "triage", "hitl": {
    "mode": "threshold", "when": [{ "field": "riskScore", "gte": 80 }],
    "approval": "event", "timeout": { "after": "24h", "action": "deny" } } }
```
`mode` is `always` (default), `threshold` (a person only when a `when` rule, a branch rule
without `goto`, matches the output) or `auto`. `approval: "event"` also puts an "AgentExpress
Approval Requested" event on the account's default bus and accepts an "AgentExpress Approval
Decision" event (`detail`: `app`, `session`, `gate`, `decision`, `comment`, `by`) as the answer,
so Slack, ServiceNow or your code can decide; whoever may put events on that bus may decide.
`timeout` decides a gate nobody decided in time (checked every five minutes). Every
self-approval, event decision and timeout is on the timeline and in the activity log.

## 10. Tune features

Each capability is a flag under an agent's `agentcore` (see the example in section 3), applied
around your `run()`: `guardrails.input`/`.output` (Bedrock `ApplyGuardrail` before/after the model
call), `memory.longTerm`, `evaluations.enabled`/`.auto` (LLM-as-judge per prompt version) and
`policy.enabled` (Cedar on every tool call at the Gateway). LangGraph checkpointing and
observability (OTEL spans, tokens, cost, latency) are always on.

**Guardrails.** The top-level `guardrail` block defines what is enforced; omit a key and that
policy isn't created.

```json
"guardrail": {
  "contentFilters": { "HATE": "HIGH", "VIOLENCE": "MEDIUM", "PROMPT_ATTACK": "HIGH" },
  "deniedWords": ["SETTLEMENT_OFFER"], "managedWordLists": ["PROFANITY"],
  "deniedTopics": [{ "name": "LegalAdvice", "definition": "Statements that give specific legal advice." }],
  "piiEntities": { "US_SOCIAL_SECURITY_NUMBER": "BLOCK", "EMAIL": "ANONYMIZE" }
}
```

> **Regulated data.** A guardrail's PII filters are a control, not a compliance programme. If your
> workflow handles protected health information (HIPAA), cardholder data (PCI DSS) or personal data
> (GDPR, CCPA), meeting those obligations is your responsibility under the
> [AWS shared responsibility model](https://aws.amazon.com/compliance/shared-responsibility-model/);
> see [AWS Compliance Programs](https://aws.amazon.com/compliance/programs/). This sample uses
> synthetic data only.

Strengths are `NONE|LOW|MEDIUM|HIGH` (`PROMPT_ATTACK` is input only); PII actions are `BLOCK` or
`ANONYMIZE`. A check whose only finding is anonymized PII does not stop the agent: it carries on
with the masked text (input) or hands it on masked (output). Anything blocked stops it, and the
timeline names what blocked it (`denied topic LegalAdvice`, `content filter INSULTS`). Bedrock
matches a denied topic on its `definition`, so keep it narrow (advice to one person about their
own case, not the subject in general): at most 200 characters, with at most 5 `examples` of up to
100 each, and a `name` of letters, digits, spaces and `-_!?.`; the validator flags anything past
those limits. The sample's `BLOCKED_DEMO_TERM` and `LegalAdvice` exist to prove blocking works;
replace them. The `ui` block (`title`, `heading`, `defaultTopic`, `topicPlaceholder`,
`assistantTitle`) sets presentation strings. The build's name (renamed at the top of the Build
view) and `ui.title`/`ui.heading` stay in step: renaming the build updates whichever of the two
still showed the old name, and changing the title in Settings renames the build. A heading set to
something of its own is kept.

**Memory.** Strategies are semantic, summary, user preference and episodic, or a custom one (a
built-in with your own extraction instructions and model). Scope is per user (default), per
subject, shared by everyone using the agent, or one run. Each strategy is a model call per stored
turn, so extra ones are provisioned only when used. One user's runs never feed another's. Each
stored turn is the run's request (as the user's words) and the agent's output, so a user
preference strategy picks up what the request asked for ("tone casual, short sentences") and a
later run recalls it. It extracts what it judges a preference, which can include the run's own
details; say in a custom strategy's instructions if only style should be kept.

**Evaluations.** Pick from the built-in evaluators (Coherence, Conciseness, Correctness,
Faithfulness, Harmfulness, Helpfulness, InstructionFollowing, Refusal, ResponseRelevance,
Stereotyping), or write your own judge with a name and instructions. The deploy creates it as an AgentCore evaluator and deletes it on destroy.

**Policy (Cedar).** Declaring a tool permits it; anything undeclared is denied, including a tool
name an injected prompt invents. `orchestrator.policy` takes `enabled` and `mode` (`ENFORCE`, or
`LOG_ONLY` to log without blocking for a safe rollout). `"policy": { "permit": false }` on a tool
registers it but denies it.

Custom policies are optional. The **Policies** tab writes one from plain English ("never refund
more than 500 dollars"), a guided form, or Cedar you type, and checks it as you go. Plain English
goes to AgentCore Policy's generator (`StartPolicyGeneration`), so the build must be deployed once
with the engine on. Each goes in `orchestrator.policy.custom`, and a `forbid` always wins:

```json
"custom": [{ "name": "noBigRefunds",
  "statement": "forbid(principal, action == AgentCore::Action::\"refunds___issueRefund\", resource == AgentCore::Gateway::\"{{gateway}}\") when { context.input.amount > 500 };" }]
```

`{{gateway}}` is filled in at deploy. Keep reusable ones on the **Policies** library page. The
checker flags, before deploy:
- a condition on anything but `context.input` (AgentCore passes a call's arguments there;
  `context.arguments` or `context.query` matches nothing or fails the deploy);
- a `context.input` condition on a whole target (`action in …`) rather than one tool (`action == …`);
- a web search or knowledge-base tool named by anything but its real tool, `<key>___WebSearch` or
  `<key>___retrieve`, in a tool's policies or in `orchestrator.policy.custom`;
- an unconditional `forbid`, which hides the tool from agents (an agent whose tools are all hidden
  fails the run with the reason). Who may start runs, approve
gates, re-run or deploy is the `authorization` block; see [DEPLOYMENT.md](DEPLOYMENT.md).

**Prompt caching.** Per agent (`main` or `dedicated`), in the agent panel's **Prompt caching**
section or by asking the Assistant: cache the system prompt, the tool definitions and the
conversation so far, so a repeated prefix is billed at the cache-read rate.

```json
"triage": { "promptCache": { "system": true, "ttl": "5m" } }
```

Each model allows its own fields, TTLs and a minimum of tokens per checkpoint (Claude: all three;
Amazon Nova: system and messages; the listed OpenAI models, GPT-5.6 Sol, Terra, Luna, GPT-6 Astra
and GPT-6.1 Sol: the system prompt only). The panel shows the rules for the agent's model and the
checker flags what it cannot do. A cache write can cost more than plain input, so caching pays
when the same prefix comes back within the TTL.
Observability shows cache reads, writes, cost and saving per call and per run, and the
**Insights** tab says, per agent, whether caching is paying off, rarely read, or caching nothing
(usually a prefix under the model's minimum). A model with no published rate is priced at an
estimated fallback rate (`rates_known=false`); cache tokens with no published rate are left out of
the cost and shown as unknown, never guessed. Rules:
[WORKFLOW_REFERENCE](orchestrator/docs/WORKFLOW_REFERENCE.md#promptcache-prompt-caching).

**Your VPC.** Runtimes are `PUBLIC` unless you say otherwise. **Settings → Network** (or
`orchestrator.network`) moves the orchestrator Runtime, and with it every `main` and task agent;
a `dedicated` agent's panel has its own **Network** section (`agents.<id>.network`), and so does a
hosted MCP server. Harness agents use their Harness settings, and remote (`a2a`) agents run on
their owner's network. What the VPC needs:
[DEPLOYMENT](DEPLOYMENT.md#running-agentcore-runtimes-in-your-vpc).

```json
"orchestrator": { "network": { "mode": "VPC", "subnetIds": ["subnet-0123456789abcdef0"], "securityGroupIds": ["sg-0123456789abcdef0"] } }
```

**Run Assistant.** The in-app assistant (titled from `ui.assistantTitle`, *Assistant* when unset)
is on unless `orchestrator.chatbot.enabled` is `false`, so a Builder build gets one by default. It
answers questions about runs and acts on them within your `authorization` rules; replies render
Markdown.

## 11. Check your changes and redeploy

Both IaC paths check `workflow.json` at plan or synth, so a typo fails `terraform plan` or
`cdk synth` with a message naming the entry, before anything deploys. After editing agents or
tools, the project's checks confirm the workflow and agent folders fit together; they need no AWS
account:

```bash
cd orchestrator     && pytest
cd orchestrator/cdk && npm test
cd orchestrator/web && npm test
```

Run locally (needs AWS credentials and Bedrock access):

```bash
cd orchestrator
pip install -r requirements.txt -r requirements-dev.txt
(cd web && npm ci && npm run build)
uvicorn app.orchestrator.server:app --port 8090
# UI hot reload: (cd web && VITE_API_BASE=http://127.0.0.1:8090 npm run dev)
```

There is no offline mode: model, tool and Cedar failures raise `ModelUnavailable`,
`ToolUnavailable` and `ToolDenied` ([errors.py](orchestrator/app/common/errors.py)). To iterate
without a Gateway, drop the `tool` binding from agents you aren't exercising. Redeploy with
`terraform apply` (rebuilds the image, updates runtimes); editing `kb_docs/` re-ingests only that
corpus.

Before anything beyond a sandbox, read the security and cost notes in
[README.md](README.md#security-summary). Internals are in [ARCHITECTURE.md](ARCHITECTURE.md).
