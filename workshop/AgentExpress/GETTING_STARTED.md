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
agent provisions it, and the tests derive their expectations from your file. Every key is in
[orchestrator/docs/WORKFLOW_REFERENCE.md](orchestrator/docs/WORKFLOW_REFERENCE.md).

## 1. Deploy the sample

Get the reference workflow running first as a known-good baseline (about 20 minutes).

```bash
cd orchestrator/terraform
cp terraform.tfvars.example terraform.tfvars      # defaults are fine to start
./bootstrap-state.sh                              # S3 remote state, once per account
terraform apply                                   # builds the image, provisions everything
```

You need Terraform, a container engine that builds `linux/arm64` (Finch, Docker or Podman), AWS
credentials, and Bedrock access to the model in `terraform.tfvars` and the embedding model (Titan
Text Embeddings V2 by default; override with `embeddingModel` on the `kb` tool).

Create a login, add yourself to a group so you can approve gates, and open `terraform output
ui_url`. Type a request, approve the gates, and watch the run. User and group setup, CDK, Auth0,
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
  its account as `ToolLambda-<agentName>-<key>`.

For `code`, grants are a fixed menu: one secret (`SECRET_NAME`), one DynamoDB table read or
read/write (`TABLE_NAME`), one S3 prefix (`S3_PREFIX`), a VPC, e.g. `"code": { "grants": {
"secret": "payments/api-key", "table": "orders", "tableAccess": "readwrite" } }`. None means no AWS
access, and a permissions boundary denies the framework's own resources. A grant reaches only a
secret, table or bucket tagged `agentexpress:code-tools=true` (for a bucket, also turn on S3 ABAC:
`aws s3api put-bucket-abac`); an untagged one is AccessDenied. The deploy is refused
unless the code parses and defines the handler, requirements are `name==version` lines, and no key
is hard-coded; lint and security findings are warnings. **Check and run in the sandbox** runs each
test event in an AgentCore Code Interpreter; after deploy, **Test tool** calls the real function.

## 3. Add your own agent

**Fast path: the Kiro skill.** Open the repository in Kiro and say *"Use the agentexpress-author
skill to build \<your use case\>"*. It scores your description against a checklist, asks for the
gaps, confirms the topology, then does everything below. Paste-ready briefs are in
[SAMPLE-PROMPTS.md](.kiro/skills/agentexpress-author/SAMPLE-PROMPTS.md). A `PreToolUse` hook blocks
writes outside the surfaces you own, and `pytest` catches anything else.

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
telemetry, memory and truncation detection live. Caveats are in [README.md](README.md).

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

## 4. The console and the Builder

Open **Build** (marked *Preview*) in the side navigation. Builds save to your account as you type
and follow you to any browser. Builds and runs are private to their creator unless shared.

### AgentExpress Assistant

A build opens on the **AgentExpress Assistant** tab. Describe what you want; the designer drafts a
complete workflow after an answer or two, then asks a few questions at a time. The default model
is Claude Sonnet 5.5 at medium reasoning effort (Claude Sonnet 5 if the account may not use 5.5), through the console region's inference profile (change
them with `-c designerModel` / `-c designerEffort`, Terraform `designer_model` / `designer_effort`); the chat doesn't
show the model. Replies stream with what it is doing (Enter sends, Shift+Enter adds a line). The conversation is
saved with the build; it reads the latest 24 messages verbatim plus a running summary of older
ones. It edits the build as you talk, highlights changed agents on the preview, writes prompts,
and cannot deploy. **Defaults used** lists what it filled in; **Needs your input** lists values
only you can give (endpoint, ARN, key), and the deploy waits for them. Secrets go in the
**Secrets** card, never the chat. **Undo** / **Redo** cover its changes, canvas edits and uploads.

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
- The header has **Project** (New / Import a file… / Delete), **Export**, **Share**, and the
  status (*Valid*, or the error and warning count), which opens the problems list.

**Tabs and named blocks.** A build keeps named `guardrails`, `memories`, `evaluators`,
`identities` (an OAuth client or API key) and `policies` (Cedar), each on its own tab. Agents pick
them by name (`agentcore.guardrails.use`, `agentcore.memory.use`, `Custom.<name>` in
`evaluations.evaluators`, `agentcore.identity.outbound`); a tool attaches `policies` and signs in
with `identity`. `orchestrator.gatewayIdentity: "perAgent"` gives each agent its own Gateway client.
Every row has **Edit** and **Delete**. The Identity tab's **Secrets** section takes each identity's
API key or OAuth client secret (write-only); an `apikey` tool sends it as `X-API-Key` from
AgentCore Identity, so the agent never sees it.

### Library and sharing

The Tools, Identity, Memory, Evals, Policies and Guardrails pages hold items usable in any build.
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
shows progress, errors and the log.

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
it ran with, so old runs keep their own graph.

### Activity and admins

**Activity** lists sign-ins and sign-outs, builds created and deleted, designer prompts, secrets
changed (names only), app passwords viewed, deploys and destroys (build, version, tool, account,
region), account changes and run actions, with time and source address. The runtime logs how each
run ended: *Run completed*, *Run failed* (with the error) or *Run ended (denied)*. Library items,
groups and sharing are logged too. Filter by kind or any text. With the `audit` permission,
**Everyone** shows all users' activity over a date range of up to 31 days (the last 7 by default).

A member of a group granted `admin`, `audit` and `insights` (`admins` in the sample) gets an
**Admin** page: every user's builds and runs read-only, everyone's activity, and **destroy** on any
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
pytest
```

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

## 6. Tune features

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

Strengths are `NONE|LOW|MEDIUM|HIGH` (`PROMPT_ATTACK` is input only); PII actions are `BLOCK` or
`ANONYMIZE`. The sample's `BLOCKED_DEMO_TERM` and `LegalAdvice` exist to prove blocking works;
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

**Evaluations.** Pick from the 13 built-in evaluators (GoalSuccessRate, ToolSelectionAccuracy and
ToolParameterAccuracy are listed but not offered yet), or write your own judge with a name and
instructions. The deploy creates it as an AgentCore evaluator and deletes it on destroy.

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
  `<key>___retrieve`;
- an unconditional `forbid`, which hides the tool from agents (an agent whose tools are all hidden
  fails the run with the reason). Who may start runs, approve
gates, re-run or deploy is the `authorization` block; see [DEPLOYMENT.md](DEPLOYMENT.md).

**Run Assistant.** The in-app assistant (titled from `ui.assistantTitle`, *Assistant* when unset)
is on unless `orchestrator.chatbot.enabled` is `false`, so a Builder build gets one by default. It
answers questions about runs and acts on them within your `authorization` rules; replies render
Markdown.

## 7. Tests and redeploy

```bash
cd orchestrator     && pytest      # runtime side: 1310 tests
cd orchestrator/cdk && npm test    # IaC + Terraform/CDK parity + cdk-nag: 282 tests in 7 files
cd orchestrator/web && npm test    # UI and Builder: 265 tests in 30 files
```

None needs AWS credentials, a model or a container builder. They test the config plane
(topology, tool-call arguments, citations, Cedar permits, RBAC, Terraform/CDK parity), not prompt
quality. Both IaC paths also validate `workflow.json` at plan/synth, so a typo fails `terraform
plan` or `cdk synth`, not the deployment. See [orchestrator/tests/README.md](orchestrator/tests/README.md)
and [orchestrator/cdk/test/README.md](orchestrator/cdk/test/README.md).

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

Before anything beyond a sandbox, read the security, cost and scalability notes in
[README.md](README.md). Internals are in [ARCHITECTURE.md](ARCHITECTURE.md).
