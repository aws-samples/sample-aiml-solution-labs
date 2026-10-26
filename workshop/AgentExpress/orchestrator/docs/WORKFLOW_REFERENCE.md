# `workflow.json` reference

Every key, what reads it, and what happens if you change it. This file holds the
explanation so `workflow.json` can stay short enough to read as configuration.

**Rule of the file:** if a key isn't listed here, nothing reads it. An unknown key is
named at `terraform plan` / `cdk synth`, before anything deploys.

## Before you read any of this: your editor already knows

`workflow.json` starts with `"$schema": "./workflow.schema.json"`, so any mainstream
editor will, as you type, complete the keys legal **for the `runtime` or `type` you
chose**, underline a value outside its allowed set and list the alternatives, mark a
missing required key, and show on hover both what a key does and which code reads it.
Most of what follows is the same information in prose, for when you want the reasoning
rather than the rule.

Three framework-owned files sit behind that, none of which you edit:

| File | Holds |
|---|---|
| `app/keys.json` | which keys exist, where each one is legal, and what reads it |
| `app/vocabulary.json` | the closed value sets (`runtime`, tool `type`, auth modes, …) |
| `app/workflow.schema.json` | **generated** from the two above — what your editor uses |

## The smallest workflow that works

Everything else in this file is optional. **Two keys are required on an agent**
(`name`, `maxTokens`) and two blocks at the top level (`agents`, `steps`). This
deploys and runs:

```json
{
  "$schema": "./workflow.schema.json",
  "agents": {
    "intake": { "name": "Intake", "maxTokens": 2000, "produces": "brief" },
    "report": { "name": "Report", "maxTokens": 4000, "produces": "report" }
  },
  "steps": [
    { "agent": "intake", "hitl": true },
    { "agent": "report" }
  ]
}
```

Two folders under `app/subagents/` — `python3 scaffold.py agent intake` writes each —
and that is a working human-reviewed pipeline. No tools, no guardrail, no `runtime` (it
defaults to `main`), no `agentcore` block. Add each of those when you want what it does,
not before.

For scale: the shipped nine-agent sample uses **6 keys per agent** on average and 11
distinct agent keys in total. The full tables below are a reference, not a checklist.

**You do not have to read this file to find a default.** Every optional key's default is
declared once in `app/keys.json`, and because `default` is a real JSON Schema keyword the
generated schema carries it — so your editor shows the value on hover and offers it in
completion. Where one key legitimately defaults differently per variant it says so per
variant: `maxResults` shows **5** on a `type: "kb"` tool (retrieval depth) and **10** on a
`type: "websearch"` one (page size).

Every plane reads that one home: the values reach the running code through
`app/defaults.json`, which `python3 build_schema.py` generates from `keys.json`.

The reason for the rule: a drifted default is quieter than a drifted value. A value this
framework does not allow is rejected at plan or synth with a message naming it, whereas a
default that differs between the Terraform and CDK paths deploys cleanly on both and
behaves differently — indistinguishable from the feature working.

## Two commands

```bash
python3 format_workflow.py    # canonical key order + readable formatting (--check to verify)
python3 build_schema.py       # regenerate the schema after a framework upgrade
```

**Every agent, tool and step is written in the same key order**, and the formatter is
what guarantees it rather than your discipline. An agent reads: who it is (`name`,
`kind`, `runtime`, `produces`) → how it reasons (`model`, `temperature`, `maxTokens`)
*or*, for `runtime: "a2a"`, where it lives (`source`/`agentCard`, `skill`, `auth`) →
what it may read (`tool`, `corpus`, `access`) → `agentcore`. A tool reads: `type`,
`description` → where it lives → how it is called → filters → `auth` → `policy`. Hand
edits are re-ordered on the next format, so the
shape is learned once instead of re-read per entry.

An unrecognised key is **kept**, not silently dropped — it has to survive to the
validator that can name it.

---

## Top level

| Key | Purpose |
|---|---|
| `$comment` | Orientation for whoever opens the file. Not read by anything. |
| `orchestrator` | Engine-wide settings. Not an agent — it has no model or prompt of its own. |
| `ui` | Presentation strings, so re-branding is a config edit rather than a UI code change. |
| `guardrail` | The Bedrock Guardrail *policy* — what is enforced. Agents opt in per agent. |
| `authorization` | Which JWT groups may approve, re-run, cancel, evaluate, run insights, delete, deploy, destroy, see every user's activity (`audit`), and read every user's builds and runs and destroy any build (`admin`), and pick a run's models on Start run (`override`, from `orchestrator.models.overrides`). `audit`, `admin` and `override` are closed unless a group is named; `insights` is too whenever anything is restricted. Grant all three to a group filled from the backend only (the sample's `admins`) — a deploy refuses a self-sign-up group holding any of them. |
| `tools` | Every data source an agent may call. |
| `skills` | Know-how an agent opens when a task needs it: a SKILL.md with optional reference files. Optional. See [`skills`](#skills). |
| `agents` | One entry per folder under `app/subagents/<id>/`. |
| `steps` | The topology and the human-review gates. |
| `guardrails`, `memories`, `evaluators`, `identities`, `policies` | Optional named blocks, defined once and used by name. See [named blocks](#named-blocks-guardrails-memories-evaluators-identities-policies). |

Account-level values — region, IdP, whether the Gateway is on — live in
`terraform.tfvars` or CDK context, **not** here, so this file stays portable
between accounts.

### What else you may edit

This file plus `app/subagents/<id>/` and `kb_docs/` is the whole surface. Nothing outside
it should need changing:

| Yours to change | Where it lives |
|---|---|
| An agent's identity and behaviour | `app/subagents/<id>/prompts.py` and `agent.py` |
| How an evidence-gathering agent should use its inputs | pass `instructions=` to `research.synthesize` from your `agent.py` |
| Which agentic framework an agent reasons with | pass `think=` to `research.synthesize`, or just write it in `run()`. Per agent — `web_search` uses Strands, `knowledge_research` a nested LangGraph, the others none. Route the model call through `ctx.llm` or you lose guardrails, cost telemetry, memory and truncation detection silently; see the README |
| Your terminal asset's sections | a `SECTIONS` tuple in that agent's `prompts.py` — it builds the prompt, the schema and the completeness check |
| Framework value sets (runtimes, tool types, memory strategies, RBAC action names, …) | `app/vocabulary.json` — framework-owned, NOT yours. Declared once and read by all three planes (Python, CDK, Terraform), each set with a comment saying why it is closed. Listed here so you know where an "invalid value" message comes from |
| Your own asset shape | a Pydantic model in your agent's folder. `assetType`, `sourceType`, `sectionType` and `artifactType` are open strings, so your vocabulary survives into the UI. Narrow any of them on your own model (`artifact_type: Literal["dicom-study"]`) if you want the check — that is the place that knows. The UI chooses a layout by the SHAPE of each field, not by its name, so a contract the framework has never seen still renders as a first-class one |

Renaming an agent means: its key in `agents`, its ids in `steps`, its folder under
`app/subagents/`, and the id it passes to `upstream_of()` in its own `agent.py`. If
you miss the last one the container fails at start with a message naming the agent
and listing the ids in the topology — it does not silently synthesize from its own
previous output. The test suite derives its expectations from this file, so a rename
does not turn tests red.

## `orchestrator`

| Key | Read by | Notes |
|---|---|---|
| `defaultModel` | `config.py` | Model for any agent that doesn't name its own. `-c modelId` (CDK) / `model_id` (Terraform) override it; empty uses this, else the framework default in `defaults.json`. A cross-region profile prefix (`us.`, `eu.`, `apac.`) is moved to the deployment region's geography (`vocabulary.json` `inferenceProfileGeos`), as is each agent's `model`: a `us.` id deployed in eu-west-1 calls `eu.`. `global.` and bare model ids are left alone. |
| `runtimeInvoke.maxAttempts` | `config.py` → `agentcore_agent._agentcore` | **Total** attempts per call to a `dedicated` agent's runtime, not retries-after-the-first. Default **1, i.e. no retrying**, which is deliberate: `InvokeAgentRuntime` is synchronous, slow and **not idempotent**, so a retry does not replace the attempt it followed — the remote container is already working and cannot tell the caller stopped listening. boto3's own default (`legacy` mode, up to 5 attempts) therefore lets one transient blip run a research agent twice, bill both model calls, and return whichever answered last, with nothing in the timeline to show it: the node logs "Invoking dedicated AgentCore Runtime" once, before any retry exists. For a call this long the failure that matters is a lost response to work that already succeeded, and retrying that is strictly worse than failing: an error reaches the reviewer, a duplicate just inflates the bill. Raise it only if you have made the call idempotent. |
| `runtimeInvoke.readTimeoutSeconds` | same | How long to wait for a dedicated agent's response. Default **600**: a dedicated agent that makes several tool calls and writes a long answer takes minutes (120 timed out such an agent mid-answer). With retrying off this timeout is fatal to the run, so keep it comfortably above your slowest agent — otherwise you trade a duplicate for a truncated run, which is not the trade being made here. |
| `a2aInvoke.timeoutSeconds` | `config.py` → `a2a_agent` | Per HTTP request to a `runtime: "a2a"` agent (the card fetch, each RPC call). Default **30**; the shipped sample sets **150**, because two of its remote agents synthesize a full asset rather than answering a short question. Keep it **above** the remote agent's own timeout (the shipped stand-in's Lambda is 120s) so the side that gives up first is the side that can say why — a client that abandons the request leaves no diagnosis and cannot tell a slow agent from a dead one. |
| `a2aInvoke.pollIntervalSeconds` | same | Gap between `tasks/get` calls while a remote task is still working. Default **2**. Their rate limit is unknown to you; a busy loop is rude and may be throttled, which then looks like their agent failing. |
| `a2aInvoke.maxPollSeconds` | same | Total wall-clock before giving up on a remote task, failing the run with the last state seen. Default **300**. Separate from `runtimeInvoke` on purpose: a dedicated runtime is yours and you know how slow it is, whereas A2A models work as a *Task* precisely so it can take minutes — so the budget that matters is not one request timeout but how long you are willing to wait overall. Bounded rather than open-ended, because a task that never leaves `working` would otherwise hold the workflow until the runtime's own 8-hour ceiling. |
| `policy.enabled` | `policy.tf`, `tool-plane.ts` | Creates the Cedar policy engine and attaches it to the Gateway. |
| `policy.mode` | same | `ENFORCE` obeys a DENY and blocks the call; `LOG_ONLY` evaluates and logs without blocking — the safe way to roll out. |
| `policy.custom` | same (one AgentCore policy each) | Your own Cedar statements beside the generated permits: `[{"name", "description", "statement"}]`. Actions are `<toolKey>___<toolName>` (one tool) or `<toolKey>` with `in`; the resource is always `AgentCore::Gateway::"{{gateway}}"`. A `forbid` always wins. To replace a tool's generated permit, set its `policy.permit` to `false`. |
| `gatewayIdentity` | `cognito.tf` / `orchestrator-stack.ts`; `gateway/client.py` | `shared` (default: one machine client for all agents) or `perAgent` (one each, so the Gateway knows which agent called). `perAgent` needs the Cognito pool the deployment creates. |
| `modelRates` | `config.py:MODEL_RATES` → `observability/pricing.py` | OPTIONAL per-model token rates, keyed by a substring of the model id: `{"nova-pro": {"input": 0.80, "output": 3.20}}` (USD per 1M tokens). First of four price sources: `modelRates`, then the AWS Price List for the deployment's region (`priceList`), then a built-in snapshot, then a fallback guess (row marked `rates_known=false`, shown as `~$`). Each llm row records which one priced it (`rates_source`), shown in the rate tooltip and the CSV/JSON export. A longer key wins over a shorter one. |
| `priceList` | `config.py:PRICE_LIST` → `observability/pricelist.py` | Default **true**: price models from the AWS Price List API (`pricing:GetProducts`, granted by both IaC paths) for this region — standard on-demand text-token rates, regional vs `global.` profile told apart, cached for a day. `false` keeps pricing offline on the built-in snapshot. |
| `imageRates` | `config.py:IMAGE_RATES` → `observability/pricing.py` | OPTIONAL per-image prices for image models, keyed by a substring of the model id: `{"sd3-5-large": 0.08}`. Wins over the AWS Price List and the `imageModels.pricePerImageUsd` snapshot in `vocabulary.json`. |
| `serviceRates` | `config.py:SERVICE_RATES` → `observability/pricing.py` | OPTIONAL overrides for the non-model rates: `embeddingPerMillionTokens`, `runtimeVcpuHour`, `runtimeGbHour`, `gatewayPerThousandInvocations`, `gatewayInvocationsPerToolCall`, and the assumed runtime footprint `runtimeVcpu` / `runtimeGb`. |
| `insights.lookbackHours` | `config.py:INSIGHTS` → `features/optimization/insights.py` | How far back the cross-run AgentCore Insights analysis looks. Default **168** (7 days). Used whenever a run request gives no window of its own. |
| `insights.pollTimeoutSeconds` | same | How long to wait for the batch analysis before reporting that it is still running AWS-side. Default **900**. A timeout is now RECORDED with its reason rather than being stored as a permanent "in progress". |
| `insights.pollIntervalSeconds` | same | Gap between `GetBatchEvaluation` polls. Default **20**. |
| `chatbot.enabled` | `bff/chatbot.py`, UI | Shows the chat icon. |
| `chatbot.model` | `bff/chatbot.py` | Model for the assistant's tool-use loop. |
| `chatbot.greeting` / `chatbot.placeholder` | UI | The assistant's opening message and input placeholder. Shipped in the BFF projection (`orchestrator/bff/workflow.py`); the page keeps a short generic fallback if you omit them. |
| `chatbot.tools.<name>` | `bff/chatbot.py` | One flag per assistant capability: `status`, `sessions`, `outputs`, `costs`, `latency`, `guardrails`, `evals`, `runEval`, `rerun`, `review`. Anything unlisted defaults to **on**. |
| `attachments.s3` | `bff/runfiles.py`; `images.tf` / `orchestrator-stack.ts` | Buckets, or `bucket/folder` locations, a run's request may name as `s3://...` for agents with `attachments`, e.g. `["amzn-s3-demo-bucket/contracts"]`; name a bucket you own. The deployment grants **the BFF** read (and list) on exactly these; it copies each file into the run, so the runtimes only read the run's own copies. None by default: uploads only. A framework bucket (`agentcore-*`) is refused. |
| `files.retentionDays` | `run_files.tf` / `lib/run-files.ts`; `app/common/artifacts.py`; `bff/runartifacts.py` | OPTIONAL, off unless `orchestrator.files` is set (even `{}`). Keeps the files runs make: agents save them with `ctx.save_file(name, data, content_type)` (to `runs/<session>/outputs/<name>`, at most 25 MB each), and the code the `codeInterpreter` tool runs is kept with what it printed (`runs/<session>/code/<agent>-<n>.<ext>` and `.log`). Setting it creates the assets bucket if no agent needed it, grants each dedicated runtime write on those two folders only, and deletes everything under `runs/` (inputs and images too) after **retentionDays**: 1 to 3650, default **90**. A run's files show on its **Files** tab and on the app's **Files** page. Not set: nothing new is created and nothing expires, as before. The same name saved twice in a run replaces the first. |
| `limits.userDailyUsd` | `bff/limits.py`; `features/observability/spend.py` | OPTIONAL, off unless set. The most one user may spend in a day (US dollars, Eastern time; `0` = no limit). Once a user has spent that much, starting a run **or re-running one** is refused with HTTP 429 and a message that says what was used and when it resets. "Spent" is the same cost figure the observability views add up, kept as a running total per user per day (one small row in the telemetry table, written by the runtime as each call is metered) so a start does not have to read whole telemetry rows. It also stops a run that was let in under the limit and has since crossed it, at its next step: the runtime checks the same total before each agent starts, and that step fails with the limit and its reset time (`BudgetReached`) having done no work, so it can simply be run again after the reset. A step already going finishes, so a user can overshoot by about one step's cost. It **fails open**: if the counters cannot be read the run starts, and the log says so. With `idp: "none"` nobody is identified and everyone shares one budget. Turning it on starts that day's total from that moment. |
| `limits.userRunsPerHour` | `bff/limits.py` | OPTIONAL, off unless set. The most runs one user may start in a clock hour (Eastern time; `0` = no limit). The run past it is refused with HTTP 429 and the time to come back. Counted with one conditional write, so two requests cannot both take the last slot. A re-run is not a new run and is not counted. It is a fixed hour, not a sliding window, so a user can start the limit's worth either side of the hour boundary. This is what bounds how many runs start before spend catches up with `userDailyUsd`. Triggers count too, as whoever they run as: `runAs: "service"` is the trigger's own user, `runAs: "owner"` spends from the build owner's budget. This is on top of the trigger's own `maxRunsPerHour`. |
| `liveConfig` | `bff/liveconfig.py`; `app/common/liveconfig.py`; `liveconfig.tf` / `orchestrator-stack.ts` | OPTIONAL, default `false`. Lets an admin change each agent's system prompt and model settings (`model`, `temperature`, `topP`, `maxTokens`, `stopSequences`) on the running deployment, without a redeploy, on the **Live config** page: edits are saved as numbered versions, and activating one points new runs at it (activating an older one, or version 0, is the rollback). A run uses the version that was active when it started for every step, through gate resumes, re-runs and dedicated runtimes, and shows it as `config vN`. Nothing else can change live: tools, gates, guardrails, memory, Cedar, identity and topology are provisioned by the deploy. A model must be one the runtimes' IAM grant covers (foundation models, and inference profiles in this region and account). Needs a group granted `admin`. When `false`, nothing is read, no IAM statement is added and no page is shown. Versions are reserved rows in the telemetry table that only the BFF may touch (`_liveconfig#`). |
| `models.overrides` | `bff/model_override.py`; `app/common/model_routing.py` | OPTIONAL. Up to 20 model ids a person may pick on **Start run** (Advanced), for every agent or per agent, for that run only. Needs a group granted `override` in `authorization.actions` (closed unless granted); the server refuses a pick from anyone else, and any model not on this list. The run stores its pick (`modelOverride`, shown as "Model override" on the run page), keeps it through every gate and re-run, and sends a dedicated agent only its own model. An overridden agent uses that model only: no `route`, no `fallback`. Harness agents, remote agents and task steps keep their own models. Not for a Builder build started from the console. |
| `limits.monthlyUsd` | `bff/limits.py`; `features/observability/spend.py` | OPTIONAL, off unless set. The most this **deployment** may spend in a calendar month, for everyone together (US dollars, Eastern time; `0` = no limit). Once reached, every new run is refused with HTTP 429 until the 1st, and a run in progress stops at its next step, as with `userDailyUsd`. It is one running total for the app, kept like the daily one, and it is cut by the date each cost was incurred. It is a brake on a runaway app or trigger, not a bill: it counts what this app's own observability meters, which is as exact as the model prices behind it. |
| `export` | `app/features/observability/runexport.py`; `export.tf` / `cdk/lib/export.ts`; `bff/export_check.py` | OPTIONAL, off unless set. Sends each run, once it ends, to an observability tool: `destination` `langfuse`, `datadog`, `grafana` (Grafana Cloud), `honeycomb` or `otlp` (any OTLP/HTTP backend). `region`: Langfuse `us`, `eu` (default) or `self-hosted`; your Datadog site (default `datadoghq.com`); Honeycomb `us` (default) or `eu`. `endpoint`: the https address, only for `otlp`, `grafana` and self-hosted Langfuse. `secret`: the NAME of the build secret of kind `exportHeaders` holding the HTTP headers with the key (a JSON object), or the full ARN of a Secrets Manager secret; never the key itself. `project`: optional, the Langfuse project id or the Datadog service name, for the console's links (Datadog also gets it as `service.name`). `send`: `content` (default `false`: prompts, answers, tool input and output, the request and result), `evals` (default `true`), `metrics` (default `true`; Datadog, Honeycomb and `otlp` only). One trace per run (agents, model calls with tokens and cost, tool calls, guardrail, Cedar, memory and HITL events, evaluation scores); the run records `export.status`. Not set: nothing is sent, read or created. With the `otlp_*` deploy settings too, this wins. See [DEPLOYMENT](../../DEPLOYMENT.md#send-runs-to-langfuse-datadog-and-others). Not run live yet. |
| `network` | `main.tf` / `cdk/lib/runtime-network.ts`; `bff/network_check.py`, `networkCheck.ts` | OPTIONAL. The shared orchestrator AgentCore Runtime's network, which every `main` and `task` agent runs in: `{"mode": "VPC", "subnetIds": ["subnet-..."], "securityGroupIds": ["sg-..."]}` or `{"mode": "PUBLIC"}`. `mode` is required, `PUBLIC` or `VPC` (the API's `networkMode`); with `VPC`, 1-16 subnet ids (`subnet-` + 8-17 letters or digits) and 1-16 security group ids (`sg-` + 8-17), and neither with `PUBLIC`. Not set: `PUBLIC`, and the deploy is what it was. A `dedicated` agent does not inherit it: it sets its own `agents.<id>.network`. What the VPC needs: [DEPLOYMENT](../../DEPLOYMENT.md#running-agentcore-runtimes-in-your-vpc). |
| `interceptors.request` / `interceptors.response` | `tool-plane.ts` / `interceptors.tf`; `app/tools/_code/interceptor-<point>/` | A Lambda the Gateway calls before each MCP request and/or after each answer (at most one each). Exactly one of `code` (written in the build, generated from `templates`: request `audit`, `blockTools`, `argumentGuard`, `injectContext`, `custom`; response `audit`, `redactPii`, `hideTools`, `capResult`, `custom`; or fetched at deploy with `code.codeSource`, see [code sources](#code-sources-git-s3-or-an-image)) or `lambdaArn`. `passRequestHeaders` (default false) hands it the headers, with `x-ax-session` / `x-ax-agent` / `x-ax-user`. A refusal is `Refused by interceptor: <reason>` on the timeline. Needs the Gateway. |
| `triggers.<name>` | `bff/triggers.py`; `triggers.tf` / `orchestrator-stack.ts` | What starts a run without anyone typing it: `type` `webhook` (`signature`), `schedule` (`expression`, `timezone`), `eventbridge` (`pattern`, `bus`), `s3` (`bucket`, `prefix`) or `sqs` (`queueArn`, or none for a created queue with a dead-letter queue). `prompt` with `{{placeholders}}` from the delivery; `enabled`, `runAs` (`owner` or `service`), `approvers`, `gates` (`inherit` or `auto`), `maxRunsPerHour` (60), `attachPayload`, `idempotencyKey`. Webhooks arrive at `POST /api/hooks/<name>`, the one route without a JWT, proven by the signature. |

You never write Cedar by hand — the rules are generated from the `tools` block.
Every tool you declare is permitted; anything not declared is refused by Cedar's
default-deny, including a tool name a prompt-injected instruction invents.

<!-- begin Track E storage -->
### `orchestrator.storage` — a bucket created for the build

OPTIONAL, off unless `create` is `true`. Set it on the Builder's **Settings → Storage**, or:

```json
"orchestrator": { "storage": { "create": true } },
"tools": {
  "renderer": { "type": "lambda", "...": "...",
    "code": { "grants": { "s3Prefix": "${storage}/templates/", "s3Access": "readwrite" } } }
}
```

| Key | Read by | Notes |
|---|---|---|
| `storage.create` | `storage.tf` / `cdk/lib/storage.ts`; `bff/storage_check.py`, `storageCheck.ts` | `true`: the deploy creates `<build name>-storage-<account>` (underscores as dashes). All four S3 Block Public Access settings on, a bucket policy refusing plain HTTP, versioned, encrypted by default with the build's own KMS key (with an S3 Bucket Key), tagged `agentexpress:code-tools=true` with ABAC on, so a code tool's grant (which needs that tag) reaches it. A lifecycle rule deletes a version 30 days after it is replaced, clears delete markers left with no versions, and aborts uploads unfinished after 7 days. Nothing is granted on it but what each tool that names it gets (below). Default `false`. Not set or `false`: nothing is created and the deploy is byte for byte what it was (`cdk/test/storage-identity.test.ts`, `tests/test_v139_identity.py`). |
| `storage.retain` | `storage.tf` / `cdk/lib/storage.ts`; `deployer/runner.py`, `terraform/teardown.sh` | Default **`false`: destroying the build empties the bucket (every version) and deletes it**, like the build's other buckets. `true`: the destroy keeps the bucket and its contents (CDK `RemovalPolicy.RETAIN`; Terraform: the runner and `teardown.sh` take it out of the state before `terraform destroy`). A kept bucket is encrypted with S3-managed keys instead, because the build's KMS key is deleted with the build (after 7 days) and would leave what it keeps unreadable; objects written before you turned `retain` on keep the key they were written with. A later deploy of the same build finds the name taken: delete or import the kept bucket first. Without `create` it keeps nothing (a warning). |

**The `${storage}` token** stands for `s3://<the bucket>` and must start the value, once:
`${storage}/` is the whole bucket, `${storage}/templates/` a folder. It is read in exactly these
fields (`app/vocabulary.json` `storageToken.fields`), where the IaC resolves it at synth or plan:

- `tools.<key>.code.grants.s3Prefix` and `orchestrator.interceptors.<point>.code.grants.s3Prefix`:
  the function gets its usual grant on that prefix (and `S3_PREFIX` resolved), plus
  `kms:Decrypt` (and `kms:GenerateDataKey` with `s3Access: "readwrite"`) on the build's key,
  through S3 only, while the bucket uses it.
- `tools.<kb>.s3Uri`: the Knowledge Base reads its documents from that folder of the bucket.
- `tools.<key>.templateS3Uri` and `tools.<key>.outputPrefix` (a Document generator): its role
  reads the templates from, and writes the files to, those folders of the bucket. That role
  has no permissions boundary of its own, so the build key's policy (any role in the account,
  through S3 only) covers the KMS side.
- `agents.<id>.task.targets[].uri` (a deliver step's `s3` target): the runtime role may put under
  that folder of the bucket, and the runtime resolves the same uri from `STORAGE_BUCKET`.

Anywhere else, or with no storage, the token is refused by both validators and by both IaC
paths rather than deployed as text. The orchestrator runtime gets `STORAGE_BUCKET` (only when
the build creates storage) for a step that resolves the token while it runs
(`app/common/storage.py`).
<!-- end Track E storage -->

## `ui`

Every user-facing string in the shell, so re-branding is a config edit rather than a
change to the UI's source. All optional — the page has a neutral fallback for each.

| Key | Where it shows |
|---|---|
| `title` | Browser tab title |
| `heading` | The header, top left |
| `defaultTopic` | Pre-filled topic, AND the server-side default when a run is started with no topic (`config.py`, `bff/handler.py`) |
| `topicPlaceholder` | The topic input's placeholder and accessible name |
| `subjectPlaceholder` | The subject input's placeholder and accessible name |
| `subjectHint` | The subject input's tooltip |
| `assistantTitle` | The assistant panel's title |
| `assistantSubtitle` | The line under it |

## `agents.<id>`

The key **is** the agent id, and — for an agent whose code you ship — the folder name
under `app/subagents/`. It must match `^[a-zA-Z][a-zA-Z0-9_]*$` — no hyphens, because
the id becomes part of an AgentCore Runtime name.

| Key | Read by | Notes |
|---|---|---|
| `name` | `registry.py`, UI | Display name. |
| `runtime` | `registry.py`, `subagent_runtimes.tf` | `main` \| `dedicated` \| `a2a` \| `task` \| `harness`. See below — the first two are placements of *your* code, the third is an agent you don't operate, `task` is a step with no model ([task steps](#runtime-task--a-step-with-no-model)), and `harness` is an agent with no code, run by a managed AgentCore Harness ([harness agents](#runtime-harness--an-agent-run-by-agentcore-harness)). |
| `task` | `registry.py` → `app/common/task_agent.py`; `bff/task_check.py`, `taskCheck.ts`; `tasks.tf` / `cdk/lib/tasks.ts` | **`runtime: "task"` only, and required there.** What the step does: `{"kind": "tool" \| "code" \| "extract" \| "output" \| "deliver", ...}`. See [task steps](#runtime-task--a-step-with-no-model). |
| `model` | `registry.py` | **Optional.** Omit to use `orchestrator.defaultModel`. No shipped agent sets it; the mechanism is there when you want a bigger model for one step. `litellm/<name>` (e.g. `litellm/gpt-4o`) is a non-Bedrock model served by the deployment's LiteLLM proxy (`litellm_base_url` / `-c litellmBaseUrl`; DEPLOYMENT.md "Using non-Bedrock models through LiteLLM"). Not for `orchestrator.defaultModel` or an evaluator's judge `model`. |
| `models` | `app/common/model_routing.py` (from `ctx.llm` and the tool-calling turn); `bff/model_routing_check.py`, `modelRoutingCheck.ts` | **Optional; `main` and `dedicated` only.** `{"route": [...], "fallback": [...]}`. `route`: up to 10 rules, first match wins, else the agent's model: `{"model": m, "inputTokensOver": N}` (the call's system + user text, estimated at 4 characters per token, is over N, 1-1000000) or `{"model": m, "matches": "invoice\|refund"}` (a case-insensitive regular expression, 1-200 characters, searched in the run's request and its subject). `fallback`: up to 3 models tried in order when the call fails with Bedrock `ThrottlingException`, `ServiceUnavailableException` or `ModelNotReadyException` (or HTTP 429/503 from a LiteLLM proxy); never on any other error (validation, access, timeout, guardrail). A run with a model override uses neither. Each call's telemetry row records the model and `chosen_by` (`route N`, `fallback N`, `override`). No IAM change: the runtimes may already call any foundation model and any inference profile in this region and account. |
| `promptCache` | `registry.py` → `app/common/prompt_cache.py` → `app/common/llm.py`; `bff/prompt_cache_check.py`, `promptCacheCheck.ts` | **Optional; `main` and `dedicated` only.** Bedrock prompt caching for this agent's model calls: `{"system": true, "tools": true, "messages": true, "ttl": "5m"}`. Off unless set. See [prompt caching](#promptcache-prompt-caching). |
| `instructions` | `harness.tf` / `cdk/lib/harness.ts` → the harness's system prompt; `harness_agent.py` | **`runtime: "harness"` only, and required there (except with `harness.arn`).** The harness agent's system prompt. Set on the harness at deploy time, so a change needs a deploy (live config does not change it). |
| `harness` | `harness.tf` / `cdk/lib/harness.ts`; `harness_agent.py`; `bff/harness_check.py`, `harnessCheck.ts` | **`runtime: "harness"` only.** `{"maxIterations": 1-100, "timeoutSeconds": 1-900, "shell": false, "provider", "apiKey", "apiBase", "browser", "codeInterpreter", "mcp", "skills", "memory", "container", "mounts", "network", "endpoint"}`, or `{"arn": "<harness ARN>", "endpoint", "maxIterations", "timeoutSeconds"}` to use a harness made elsewhere. A harness agent's `tool` is the Gateway tools its harness calls. See [harness agents](#runtime-harness--an-agent-run-by-agentcore-harness). |
| `network` | `subagent_runtimes.tf` / `cdk/lib/runtime-network.ts`; `bff/network_check.py`, `networkCheck.ts` | **`runtime: "dedicated"` only.** This agent's own AgentCore Runtime network, the same shape as [`orchestrator.network`](#orchestrator). Not set: `PUBLIC`; it is not inherited from `orchestrator.network`. On a `main` or `task` agent it is rejected (they run in the shared orchestrator Runtime: set `orchestrator.network`, or make the agent `dedicated`); on a `harness` agent use `harness.network`; on an `a2a` agent it is rejected, since a remote agent runs on its owner's network, outside this build. |
| `maxTokens` | `registry.py` → `agent.max_tokens` | Output budget in tokens for this agent's model calls. A call may take up to 300 s to answer (`BEDROCK_READ_TIMEOUT_SECONDS` on the runtime; boto3's own 60 s cut off an 8000-token report), with one retry. |
| `temperature` | `registry.py` | **Optional**, defaults to 0. |
| `tool` | `registry.py` | A key in the `tools` block, or a list of keys (`["policyDocs", "claimsDb"]`). Each tool is queried before the agent reasons and its results reach the model as labelled evidence (`_shared/evidence.gather`). Validated — a name that doesn't resolve fails at plan/synth. Omit and the agent reasons purely over upstream outputs. On a `runtime: "harness"` agent: the Gateway tools its harness calls itself ([harness agents](#runtime-harness--an-agent-run-by-agentcore-harness)). |
| `corpus` | `registry.py` | For a `type: "kb"` tool: which corpus to retrieve from. Must be one of that tool's declared `corpora`. |
| `produces` | `nodes.py` | The deliverable name, injected into the agent's task prompt. |
| `access` | UI chip | A short human label for the data source. **Only read when the agent has no `tool`** — with a tool, the chip is derived from the tool's type. Don't set both. |
| `attachments` | `context.py` → `app/common/attachments.py`; `bff/runfiles.py` | **`main` / `dedicated` / `task`.** On a task step: a code step's script gets the run's files in `in/`, and an extract step needs it. `true`: this agent's model reads the files a run is started with — uploaded on **Start run**, or named in the request as `s3://bucket/key` (or `s3://bucket/folder/`) inside a location `orchestrator.attachments.s3` allows. Every model call of the agent gets them as Converse document or image content (at most 5 files; documents 4.5 MB, images 3.75 MB; PDF, Word, Excel, CSV, HTML, text, Markdown, JSON, code, PNG, JPEG, GIF, WebP). They are checked and copied into the run's own folder when it starts, so a re-run reads the same files. The model must accept documents (Claude, Amazon Nova). Default `false`; the Start run form offers no files unless some agent reads them. |
| `agentCard` | `a2a_agent.py` | **`runtime: "a2a"` only.** The remote agent's base URL or Agent Card URL. Must be `https://`. |
| `auth` | `a2a_agent.py` | **`runtime: "a2a"` only.** `none` \| `bearer` \| `oauth2` \| `sigv4`. Defaults to `none`. Required to be `sigv4` with `source` — see the stand-in, below. |
| `source` | `a2a.tf`, `orchestrator-stack.ts` | **`runtime: "a2a"` only.** The framework-deployed stand-in to reach instead of a committed URL. `a2a_lambda` is the only value. Mutually exclusive with `agentCard`. |
| `skill` | `a2a_agent.py` (endpoint path), IaC | **With `source` only.** Which of the stand-in's published skills this agent is. An external agent advertises its skills in its own Agent Card, so this means nothing without `source` and is rejected there. |
| `skills` | `context.py` → `app/common/skills.py` | Names in the top-level [`skills`](#skills) map this agent may use. With `toolMode: "model"` and tools it opens them itself (`use_skill`); otherwise they are added to its instructions in full. |
| `registry` | Console only | Where an agent card came from in an AWS Agent Registry (`runtime: "a2a"`). Set by the console, never by hand; see [`skills`](#skills) for its fields. |
| `askPerson` | `app/common/asks.py` → `tool_loop.py`; `bff/asks.py` | **`main` only, with `toolMode: "model"`.** `true`, or `{"maxQuestions": 3, "timeout": "24h"}`. The agent's model is offered an `ask_person(question)` tool: the run waits, the question is shown on the run page with a text box, and the answer comes back to the model as the tool's result, labelled as the person's own (untrusted) words. At most `maxQuestions` (1–10, default 3) per step; a question is cut to 500 characters and an answer to 4,000. With `timeout`, an unanswered question goes unanswered after that long (the five-minute sweep, as for a gate). A run started unattended (`gates: "auto"`) gets "no person is available". Model and tool calls made before the question are replayed from the checkpoint on resume, not made again. |
| `kind` | `registry.py`, UI | `sync` (default) or long-running. Presentation only: the UI shows a chip when it is not `sync`. |
| `framework` | `scaffold.py apply` → `agent.py` | `plain` (default), `strands` or `langgraph`: how the generated `agent.py` reasons. All call the model through `ctx.llm`. An `agent.py` you wrote is kept. |
| `toolMode` | `registry._configure` → `evidence.gather` / `tool_loop.py` | `direct` (default): every bound tool is queried once before the model reasons. `model`: the model picks tools and arguments, reading each result, up to `maxToolCalls`. Every call goes through the Gateway either way. |
| `maxToolCalls` | `tool_loop.py` | With `toolMode: "model"`: the most tool calls before the model must answer. Default **6**. |
| `topP`, `stopSequences` | `registry.py` → `ctx.llm` | Optional sampling settings. Several Claude models take `temperature` or `topP`, not both. Up to 4 stop sequences; one that occurs inside a JSON asset cuts it off. |
| `output` | `scaffold.py apply`; `registry.py` | `text` (default, a JSON asset) or `image`: the model writes an image brief and an image model renders it; the reviewer sees the images at the gate. |
| `image` | `registry.py` → `ctx.image` | With `output: "image"`: `model` (default Stable Diffusion 3.5 Large), `aspectRatio` (`1:1`), `outputFormat` (`png` or `jpeg`), `seed`, `negativePrompt`, `allowCrossRegion` (needed when the model is not served in this geography; a US deployment renders in us-west-2 without it). |
| `vision` | `registry.py` → `ctx.llm` | `{"from": ["<agent>"], "maxImages": 4}`: this agent's model reads images earlier agents made in this run (max 20). The model must accept images. |

### `runtime` — where the agent actually runs

Three placements. The first two differ only in *where your code executes*; the third
is a different thing entirely.

| | What it is | Needs a folder under `app/subagents/`? |
|---|---|---|
| `main` (default) | a LangGraph node inside the orchestrator container | yes |
| `dedicated` | its **own** AgentCore Runtime, called with `InvokeAgentRuntime` | yes |
| `a2a` | an agent **you do not operate**, called over the Agent2Agent protocol at its Agent Card URL | **no** |

`main` and `dedicated` run the same `run()` code — placement is config, so moving an
agent to its own container and its own scaling is a one-word edit.

`a2a` is not a placement, it is a **trust boundary.** The agent is somebody else's
service: a third party's, another team's, or a managed one. Nothing of it lives in this
repo, which is the point — it turns a multi-agent workflow into a multi-*organisation*
one. That is also why it is a `runtime` value and not a `tool`: a tool returns data
for one of *your* agents to reason over, whereas this replaces the agent.

```json
"credit_check": {
  "name": "Vendor Credit Check",
  "runtime": "a2a",
  "agentCard": "https://agents.vendor.example/credit",
  "auth": "bearer",
  "produces": "credit-assessment"
}
```

It is an ordinary node in the topology: put it in `steps` anywhere, gate it with
`hitl`, route to it with `branch`, re-run it, read its version history. The framework
sends it the same inputs an in-process agent reads — the request, the approved
upstream outputs, any reviewer feedback — as one opaque text part, because A2A carries
text and the remote agent has its own output contract. Demanding yours would make it
un-integrable.

**Discovery.** `agentCard` may be the agent's base URL (the card is read from
`<url>/.well-known/agent-card.json`) or the card URL itself. The card is *read*, not
assumed: if it declares `supportedInterfaces`, the first JSON-RPC entry wins, in the
card's preference order. So an agent may serve its RPC endpoint on a different host
from its card and still work.

**Auth.**

| `auth` | What happens | Where the credential comes from |
|---|---|---|
| `none` | no `Authorization` header | — |
| `bearer` | `Authorization: Bearer <token>` | `var.a2a_tokens` / `$A2A_TOKENS`, keyed by agent id |
| `oauth2` | a token minted per call | the agent's existing `agentcore.identity.outbound` provider |
| `sigv4` | every request signed with SigV4 | the orchestrator's own execution role. Nothing in config, nothing to rotate, nothing to leak — the mode for an AWS-hosted agent behind IAM, which is what the shipped stand-in is |

Tokens are **never** written in `workflow.json` — it is committed, and these are
credentials for somebody else's service. A `bearer` agent with no token supplied fails
at plan/synth, not at runtime, because the alternative is a 401 from a service you do
not control.

**Keys that do not apply.** `model`, `temperature`, `maxTokens`, `tool` and `corpus`
are all rejected on an `a2a` agent. A remote agent makes its own model call and reaches
its own data sources, so those would read as governing its cost and its access while
doing nothing. Setting them fails validation rather than misleading you. Its output
budget is its own: in the shipped stand-in each skill declares one
(`a2a_lambda/handler.py` `maxTokens`), which is why a reviewer skill and a full
synthesis skill can be backed by the same function without one starving the other.

**What you keep.** The framework wraps the call in *your* container, so **guardrails**
apply to it. **Long-term memory works in both directions**: `agentcore.memory.longTerm`
recalls this agent's past insights and carries them to the remote agent inside the A2A
task, under `recalledContext`, with the same caveat `ctx.llm` attaches locally — that
they are unverified recollections and never evidence. Its reply is then stored as a new
insight. This differs from `dedicated` on purpose: `InvokeAgentRuntime` has a fixed
payload that cannot carry a recollection, so recalling for one would be billed and
thrown away, whereas A2A carries opaque text and can. Worth knowing before you declare
it: this sends your deployment's accumulated recollections to an agent outside it, which
is why it follows `agentcore.memory` rather than happening unconditionally.

**What you lose.** **Evaluations** fall back to a role descriptor: there is no local
model call to capture a prompt from, because the reasoning happened on their side. The
descriptor plus the real output still supports Coherence, Helpfulness and
InstructionFollowing; it does not support **Faithfulness**, because there is no source
context to be faithful to — so prefer `auto: false` and a narrower evaluator list on a
remote agent, as the two shipped ones do. A number computed from a descriptor reads
exactly like a measured one. And any **tool** the remote agent uses is outside your
Cedar policy: you are trusting their boundary, not enforcing yours. The UI marks it with
an `a2a` chip and labels its source `A2A · <host>` (or `A2A · <source>` for the shipped
stand-in) so a reviewer can see which parts of a deliverable came from outside.

**When it fails** it raises `RemoteAgentUnavailable` with whatever the remote side
said, and the run fails with that reason on that node. There is no fallback: an empty
answer would flow into every downstream agent looking exactly like a real finding of
nothing. A SigV4 403 additionally reports which principal signed, because that is the
only question a rejected signature raises and the status alone does not answer it.

**What you get back, and who builds it.** If the remote agent replies with a JSON
object and the agent declares `produces`, the framework wraps that object in the asset
envelope — `assetId`, `assetType`, `version`, `status`, `createdAt`, `createdByAgent`,
`sourceAssetIds` — so a remote step is a first-class contract producer and a downstream
agent can cite it.

The framework does this rather than asking the remote agent to, because the envelope is
bookkeeping **about your run**: `version` is how many times that step has run in this
session, `createdByAgent` is the id *you* gave it, `assetType` is *your* `produces`,
and `sourceAssetIds` are the upstream assets *your* graph handed over. A remote agent
asked to invent them gets `version` wrong on every re-run — silently, because a wrong
integer still validates.

Fields the remote DID send win; the envelope only fills gaps. A **prose** reply is
returned exactly as written: a remote reviewer that answers in sentences is a
legitimate remote agent, and wrapping its text in a JSON envelope would make the
timeline and the report worse. Only a JSON object is treated as contract content.

> This was a real gap, not a hypothetical one. Without the envelope a remote reply had
> no `assetId`, and `synthesis.upstream_context` — which collects exactly that field for
> claim tracing — took the reply in as a block with nothing to cite. Measured on a live
> run: the remote output reached the analysis and the final report and was quoted there,
> but never appeared in any `sourceAssetIds`. Used and attributed in prose; not
> machine-traceable.

**The one thing that cannot be checked.** The merged asset is NOT validated against a
pydantic contract, because there is no local contract class for an agent whose code is
somebody else's — `produces` names the type, it does not import a model. That is the
real cost of the boundary, and it is why the UI renders an asset by *shape* rather than
by field name. If you need schema enforcement, put the remote agent behind a
`dedicated` wrapper that validates its reply, where the contract is yours to declare.

### The shipped stand-in (`source: "a2a_lambda"`)

`runtime: "a2a"` needs an agent you do not operate, so a committed placeholder URL would
fail every run. The framework therefore ships one: a real A2A server
(`a2a_lambda/handler.py`) in its own Lambda behind its own Function URL, deployed only
when an agent asks for it with `source: "a2a_lambda"`. `skill` picks which of its
published skills you are reaching, selected by PATH (`/analysis`), so one function backs
several genuinely different agents.

It publishes four, and they are a **catalogue rather than a roster** — `workflow.json`
decides which become agents, and a skill nobody names is simply never reached:

| `skill` | What it is | Its output shape |
|---|---|---|
| `analysis` | synthesizes the approved research into one analysis, tracing claims | the CONTENT of this sample's `Analysis` contract |
| `recommendation` | turns the approved analysis into prioritized actions | the CONTENT of its `Recommendation` contract |
| `compliance` | an outside reviewer on regulatory and data-handling exposure | its own |
| `resilience` | an outside reviewer on failure modes, blast radius and recovery | its own |

The shipped sample wires up the first two, as its Analysis → Recommendation stage. Add
`"skill": "compliance"` to a new `a2a` agent and you have a third with no other edit.

The split between those two groups is the interesting part. A reviewer's shape is *its
own*, which is the normal case for a third party and the reason A2A carries opaque text.
The two synthesis skills are asked for **this sample's contract content**, because they
are steps in the deliverable rather than commentary on it — and note what their shapes do
*not* contain: no `assetId`, `version`, `status`, `createdAt` or `sourceAssetIds`. Those
are your run's bookkeeping, and the framework stamps them (see above).

A skill that hits its output ceiling is **refused** as a failed task rather than
returned. A local agent detects its own truncation and stamps a `limitations` entry; that
signal cannot cross the boundary — from the client's side a cut-off reply is just a
reply, and JSON repair turns it into a complete-*looking* asset with the end of the
longest list missing. Silent partial content in front of an approver is worse than a
failed step.

The stand-in's Lambda timeout is deliberately **shorter** than
`orchestrator.a2aInvoke.timeoutSeconds` (120s against the sample's 150s): the side that
gives up first should be the side that can say why. A client that abandons the request
first leaves no diagnosis and cannot tell a slow agent from a dead one.

It requires `auth: "sigv4"`, and that is validated rather than defaulted: the endpoint is
an `AWS_IAM` Function URL, so an unsigned request is a guaranteed 403 that surfaces as
"could not read the Agent Card … HTTP 403" — which reads like a missing agent instead of
like wrong config.

Point an agent at a real third party's `agentCard` instead and none of that infrastructure
is provisioned.

> **There is no `repair` key.** An earlier output-rules engine that re-asked an agent to
> fix its own violations was removed: it encoded one editorial standard (`document`,
> `record`, `evidence`, `transcript` treated as artifact nouns) that misfires in a legal or
> medical domain, and a framework has no business holding that opinion. If you want output
> checks, put them in your own agent under `app/subagents/<id>/`.

### `runtime: "task"` — a step with no model

A task step sits in `steps` like an agent (alone, in a `parallel` group or a `sequence`,
gated, re-run from a gate, a `branch` decider) and runs in the orchestrator's own runtime.
It makes no model call, has no folder under `app/subagents/` and no prompt, and its output
is JSON that a later agent reads with `ctx.input("<id>")`. Only `name`, `kind`, `runtime`,
`produces`, `attachments`, `task` and `agentcore.guardrails` apply.

```json
"price_lookup": {
  "name": "Look up the price",
  "runtime": "task",
  "task": {"kind": "tool", "tool": "pricing",
           "arguments": {"service": "{{outputs.intake.service}}", "query": "{{request}}"}}
},
"crunch": {
  "name": "Crunch the sales file",
  "runtime": "task",
  "attachments": true,
  "task": {"kind": "code", "language": "python",
           "code": "import json, csv\nrows = list(csv.DictReader(open('in/sales.csv')))\nprint(json.dumps({'rows': len(rows)}))"}
},
"read_invoice": {"name": "Read the invoice", "runtime": "task", "attachments": true,
                 "task": {"kind": "extract", "features": ["TABLES", "FORMS"]}},
"export": {"name": "Export", "runtime": "task",
           "task": {"kind": "output", "format": "csv", "from": ["price_lookup.result"], "name": "prices-{{session}}"}}
```

| `task.kind` | Keys | What it does |
|---|---|---|
| `tool` | `tool` (required), `arguments` | Calls the tool through the Gateway with `arguments`, as one call. The Cedar policy, interceptors and the tool's `approval` apply as for an agent's call; the tool's fixed `args` win over `arguments`. A target that publishes several tools needs `call` on the tool. Output: `{"result": <the tool's answer as data>, "text": ...}`. |
| `code` | `code` (≤ 50,000 bytes) or `codeSource` (git or S3, read at deploy: [code sources](#code-sources-git-s3-or-an-image)), `language` (`python` default, `javascript`, `typescript`) | Runs the script once in the AWS-managed AgentCore Code Interpreter (`aws.codeinterpreter.v1`), a fresh session that is always stopped. The script reads `in/request.txt`, `in/outputs.json` (earlier outputs, parsed when they are JSON) and, with `attachments`, the run's files as `in/<name>`. What it prints is the output (`output`, parsed when it is JSON); files it writes to `out/` are kept as run files (up to 20); the script and its log go to `runs/<session>/code/` when the deployment keeps files. A non-zero exit fails the step with the end of stderr. Up to 120 s. |
| `extract` | `file`, `features` (`TABLES`, `FORMS`; default both) | Reads the run's PDF, PNG and JPEG files (or the one named `file`) with Amazon Textract: an image with `AnalyzeDocument`, a PDF with the asynchronous `StartDocumentAnalysis` (the synchronous call reads one page only), polled for up to 300 s. Output per file: `pages`, `text` (cut at 50,000 characters), `tables` (rows of cell text) and `forms` (`key`/`value`). Needs `attachments: true`. |
| `output` | `format` (required: `csv`, `json`, `markdown`, `txt`), `name`, `from` | Writes a file from earlier outputs and keeps it with the run (`runs/<session>/outputs/`, on the run's Files tab). `from` lists step ids or `<step>.<field>` (default: every earlier output); a CSV takes the rows of a list of objects. Needs `orchestrator.files`. No Excel or PDF writer: that is a code step's job. |
| `deliver` | `targets` (required, 1-10) | Sends the files the run's steps saved (`runs/<session>/outputs/`) to each target in order: see [deliver targets](#deliver-targets). Every delivery, skip and failure is a timeline line, and each delivery or failure an activity-log event (`run.delivered`, `run.delivery_failed`). A failed target fails the step after the others were tried. Output: `{"deliveries": [{"target", "type", "to", "status": "delivered" \| "skipped" \| "failed", "files", ...}]}`. Needs `orchestrator.files`. |

<!-- begin v139 Track C deliver -->
#### Deliver targets

```json
"send": {"name": "Send the reports", "runtime": "task",
         "task": {"kind": "deliver", "targets": [
           {"type": "s3", "uri": "s3://amzn-s3-demo-bucket/reports/", "files": ["*.pdf", "*.pptx"]},
           {"type": "email", "identityArn": "arn:aws:ses:us-east-1:111122223333:identity/example.com",
            "fromAddress": "reports@example.com", "to": ["team@example.com"],
            "subject": "Report {{session}}", "when": {"field": "outputs.check.passed", "equals": true}},
           {"type": "webhook", "url": "https://hooks.example.com/in", "secret": "hook"}]}}
```

| Key | Targets | Meaning |
|---|---|---|
| `type` | all | Required: `s3`, `email` or `webhook` (`deliverTargetTypes`). |
| `files` | all | File-name patterns (`*`, `?`, case-insensitive), no folder. Default: every file the run saved. |
| `producedBy` | all | Step ids, each earlier: only the files those steps saved (each file records its step). |
| `when` | all | One condition: `field` (`request`, or `outputs.<step>.<field>`, the step earlier) and one or more of `branch.when`'s operators (`equals`, `notEquals`, `in`, `contains`, `exists`, `gt`, `gte`, `lt`, `lte`). When it does not hold the target is skipped, and the skip is on the timeline. |
| `uri` | `s3` | Required: `s3://<bucket>/` or `s3://<bucket>/<prefix>/` (ending in `/`), or `${storage}/<prefix>/` for the build's storage (`orchestrator.storage`). Each file is copied (`CopyObject`) to `<prefix><session>/<name>`. Not one of the framework's own buckets. The runtime role gets `s3:PutObject` on `arn:aws:s3:::<bucket>/<prefix>*` only. A bucket encrypted with your own KMS key also needs that key to allow the runtime role. |
| `identityArn` | `email` | Required: the SES identity, `arn:aws:ses:<region>:<account>:identity/<domain or address>`, verified in SES in that Region. The runtime role gets `ses:SendEmail` on it only, with `ses:FromAddress` pinned to the `fromAddress` values. |
| `fromAddress` | `email` | Required: the identity's own address, or an address at its domain. |
| `to` | `email` | Required: 1-50 addresses (SES takes 50 per message). Written by the author; a placeholder here is refused. |
| `recipientsFrom` | `email` | Optional, the explicit opt-in to more recipients chosen by a step: one `{{outputs.<step>.<field>}}` (a list or a comma-separated text of addresses), added to `to`, still at most 50 together. |
| `subject`, `body` | `email` | Optional text, with the placeholders of a task step. The files are listed under the body. |
| `attach` | `email` | Default `true`: the files are attached while they are at most 25 MB together (SES v2 takes 40 MB per message after base64); above that, or with `false`, they go as links. |
| `url` | `webhook` | Required: `https://`, no `user:password@`. Posted through the SSRF guard (`app/common/netguard.py`): private, loopback and link-local addresses are refused, each redirect is checked again. 10 s timeout; any answer but 2xx fails the target. The body: `{"event": "agentexpress.delivery", "app", "session", "step", "deliveredAt", "files": [{"name", "bytes", "contentType", "producedBy", "url", "expiresAt"}]}`. |
| `secret` | `webhook` | Optional: the NAME of a build secret of kind `deliverSecrets`, never the secret. The body is signed as the app's own webhooks are: `X-AX-Timestamp` and `X-AX-Signature: sha256=<HMAC-SHA256 of "<timestamp>.<body>">`. The deploy fails when the build has no secret by that name. |
| `linkExpiresMinutes` | `email`, `webhook` | How long a link lasts, 1-10080 (default 60). Links are presigned with the runtime role's credentials, so they end when those do, even sooner. |

Good to know: the SES sandbox lets you send only to verified addresses (200 a day, 1 a second); SES's error is on the timeline as SES wrote it. The runtime also gets `s3:GetObject` on `runs/*/outputs/*` and `s3:ListBucket` for `runs/*/outputs/*` prefixes of the assets bucket, only with a deliver step. A workflow without one deploys exactly as before.
<!-- end v139 Track C deliver -->

**Placeholders** in `arguments` and an output's `name`: `{{request}}`, `{{session}}`,
`{{outputs.<step>}}` and `{{outputs.<step>.<field>}}` (`.<n>` for a list item). A string that
is only one placeholder takes the value as it is (a number stays a number); inside other text
it becomes text. Each `<step>` must run before this one; the validators say so before a deploy.

Good to know: add task steps from the palette (**+ New generic step**). Renaming a step does not
rewrite placeholders that name it; the validator flags them. A run does not accept TIFF files, so
Textract reads PDFs, PNG and JPEG.

### `runtime: "harness"` — an agent run by AgentCore Harness

A harness agent has no folder under `app/subagents/` and no code: the deploy creates one
[AgentCore Harness](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/harness.html)
for it, and the orchestrator calls `InvokeHarness` when the step runs. It sits in `steps` like
any agent (gated, re-run with a reviewer's note, in a `parallel` group or a `sequence`). It is
given the same inputs an A2A agent is (the request, earlier outputs, the reviewer's note,
recalled insights) as one message, and the last thing it says is the step's output; a JSON
answer gets the asset envelope of its `produces`.

```json
"writer": {
  "name": "Writer",
  "runtime": "harness",
  "produces": "draft",
  "instructions": "You write the customer summary from the earlier steps' outputs. Return JSON: {\"summary\": ...}.",
  "model": "us.anthropic.claude-haiku-4-5-20251001-v1:0",
  "maxTokens": 2000,
  "harness": {"maxIterations": 10, "timeoutSeconds": 300, "shell": true}
}
```

A fuller one, with tools, skills, memory and an endpoint:

```json
"researcher": {
  "name": "Researcher",
  "runtime": "harness",
  "instructions": "Research the request with your tools and return JSON: {\"findings\": [...]}",
  "model": "gemini/gemini-2.5-pro",
  "tool": ["docs", "pricing"],
  "harness": {
    "provider": "litellm", "apiKey": "geminiKey",
    "browser": true, "codeInterpreter": true,
    "mcp": [{"name": "wiki", "url": "https://wiki.example.com/mcp"}],
    "skills": [{"aws": ["core-skills/*"]}, {"git": "https://github.com/anthropics/skills", "path": "skills/docx"}],
    "memory": "research",
    "mounts": [{"session": "/mnt/work"}],
    "endpoint": "live"
  }
}
```

| Key | What it sets on the harness |
|---|---|
| `instructions` | The system prompt. Required (except with `harness.arn`). |
| `model`, `temperature`, `topP`, `maxTokens` | The model and its settings. Bedrock by default (`orchestrator.defaultModel` if omitted, moved to the region's inference-profile geography; with `topP` and no `temperature`, no temperature is sent). Another provider's model is its own id, sent as given, with only the settings you write. Not `litellm/<name>`: that is this deployment's LiteLLM proxy, which a harness does not call. |
| `harness.provider` | `"bedrock"` (default), `"openai"`, `"gemini"` or `"litellm"` (any LiteLLM provider; ids like `gemini/gemini-2.5-pro`, `anthropic/claude-sonnet-4-6`). Anything but Bedrock needs `model`. |
| `harness.apiKey` | The provider's key for `openai` and `gemini` (optional for `litellm`): the name of an `identities` entry of type `apikey` (its key in the build's secrets; the deploy makes an AgentCore API key credential provider `bedrock-agentcore-<agent>-hk-<name>` from it) or the ARN of an API key credential provider you already have. Never the key. |
| `harness.apiBase` | `litellm` only: an OpenAI-compatible endpoint (a proxy, a gateway), `https://` on a public host. The key is sent to it. |
| `tool` | The build's tools (keys of `tools`) the harness may call through the Gateway. The harness gets one `agentcore_gateway` tool named `gateway`, signed in through an OAuth2 credential provider (`bedrock-agentcore-<agent>-hgw-<id>`) that holds the agent's Gateway machine client: the shared one, or its own with `orchestrator.gatewayIdentity: "perAgent"`. So Cedar policy and the interceptors apply to it as to every agent. Only the named tools are allowed (`@gateway/<tool>___*`). Needs the Gateway, signed in with Cognito's client-credentials token (the pool this deployment makes, or `idp: "cognito"` with your own); not tools with `auth` `"user"` or `"obo"`, which act as the person, nor the Document generator (`source: "docgen"`), whose file is saved with the run the runtime sets on each call. Interceptors with `passRequestHeaders` see no `x-ax-*` headers from a harness. |
| `harness.mcp` | Remote MCP servers, up to 10: `[{"name": "wiki", "url": "https://..."}]`, `https://` on a public host (not localhost, a private address, `.local` or `.internal`). No headers: nothing secret goes in workflow.json. |
| `harness.browser`, `harness.codeInterpreter` | `true` for the managed AgentCore Browser / Code Interpreter, or a custom one's ARN. The role gets the session actions on exactly that resource. |
| `harness.shell` | `true` lets the agent use the built-in `shell` and `file_operations` tools in its own isolated session, which can reach the internet. Without it, and with no other tool, `allowedTools` is set so that it can use no tool at all, on the harness and again on every call. With tools or an API key, shell commands run in the session that holds their credentials: the check warns. |
| `harness.skills` | Up to 20, each one of `{"aws": ["core-skills/*"]}` (the AWS skills catalog, `[]` for all), `{"git": "https://...", "path": "folder", "credential": "<apikey identity or ARN>"}` (a private repository's token), `{"s3": "s3://bucket/prefix/"}` (the role may read that prefix and list that bucket). The skill tool (`skills`) is allowed when there are skills. The build's own `skills` are not uploaded for a harness. |
| `harness.memory` | One of the build's `memories`: the harness keeps its conversation there (`agentCoreMemoryConfiguration`) and recalls long-term records itself, as `actorId` the same partition the framework uses (the memory's `scope`, default per user). Default: memory **off** on the harness (the service would otherwise create a billed managed memory). Not with `agentcore.memory`, which would store the same turns again: use one. |
| `harness.container` | Your image for the session, `linux/arm64`: `<account>.dkr.ecr.<region>.amazonaws.com/<repo>:<tag>` (the role may pull that repository only) or `public.ecr.aws/...`. |
| `harness.mounts` | Up to 5 under `/mnt`: `{"session": "/mnt/data"}` (per-session storage, kept across stop and resume, no VPC), `{"efs": "<access point ARN>", "path": "/mnt/efs"}`, `{"s3Files": "<access point ARN>", "path": "/mnt/s3"}`. The role may mount through exactly those access points. EFS and S3 Files need `network`. |
| `harness.network` | `{"subnetIds": [...], "securityGroupIds": [...]}`: the session runs in your VPC. The VPC needs interface endpoints for `ecr.api`, `ecr.dkr` (and `bedrock-runtime` for a Bedrock model) and a gateway endpoint for S3; the role may pull the managed image from the region's `harness-*` repository. |
| `harness.endpoint` | A named endpoint (letters, digits, `_`; not `DEFAULT`) the orchestrator calls with `qualifier`. CDK points it at the harness's `Version`, so a deploy that changes the harness moves it in place; Terraform re-creates it when the harness changes (the provider reports no version), so a step calling it during that apply fails. |
| `harness.arn` | A harness made elsewhere, in this account and region: the deploy creates no harness, role or credential provider for the agent, and the orchestrator's role may invoke exactly that ARN (and `harness.endpoint` on it). With it, `instructions`, `model`, `temperature`, `topP`, `maxTokens`, `tool` and every other `harness` key are refused: they are set on that harness. `maxIterations` and `timeoutSeconds` are sent on each call. Its own tools are kept (no `allowedTools` is sent). Its model is not known here, so its tokens are recorded with `rates_known: false`. |
| `harness.maxIterations`, `harness.timeoutSeconds` | How many model turns and how many seconds one step may take. Omitted: the harness's own defaults. |

A model of another provider is recorded with its tokens and `rates_known: false` (no price
table here covers it). `agentcore.memory` works as for an A2A agent: the orchestrator recalls before the step,
sends the insights with their caveat, and stores the answer. `agentcore.guardrails` and
`agentcore.evaluations` work as for any agent, in the orchestrator. `agentcore.identity` is refused.

Good to know: a harness agent's prompt and model are set on the harness at deploy, so live
config does not change them. For memory, set `harness.memory` or `agentcore.memory`, not
both. Each tool call the harness makes is a timeline line; its own spans are in
AgentCore observability under the harness.

### `maxTokens` — why it is per agent

Different agents emit very differently sized payloads. A research agent returns a
full structured JSON — summary, several classified findings, sources, limitations —
and the report agent returns a whole sectioned document.

Set it too low and the model's output is **truncated mid-JSON**, which does not
surface as "budget exceeded". It surfaces as *"the model returned no parseable
research JSON"*, because the framework refuses to emit a half-parsed asset that
downstream agents would treat as real findings. If you see that error, this is the
first thing to raise.

Current values: `intake` 3000, the three evidence-gathering research agents 4000,
`cost_research` 1500 (it emits a small rates table, not prose), `report` 8000.

`analysis` and `recommendation` have none, because they are `runtime: "a2a"` and a remote
agent's budget is its own — for the shipped stand-in it is 8000 each, declared per skill
in `a2a_lambda/handler.py`, with the reviewer skills left on the 2000 default.

Across the A2A boundary this number matters more than it does locally, and in a different
way. A local agent that runs out of room repairs the JSON and stamps a `limitations`
entry, so the asset is usable and the reviewer is told. The remote agent cannot tell you
that — from the client's side a cut-off reply is just a reply — so the shipped stand-in
**refuses** a truncated answer as a failed task instead, and the run stops with the skill
and the budget named in the error. Correct, but it means the budget wants headroom rather
than a fit: 6000 was enough for these two until one prompt change asked each item to
carry an extra assetId, and the next run failed.

These were literals buried at each call site until they were moved here. Nothing in
`app/subagents/` passes `max_tokens` any more, and a test enforces that — otherwise
the number in this file would be quietly overridden by code.

### `promptCache`: prompt caching

Per agent, off unless set; there is no build-wide default. `main` and `dedicated` agents only
(a harness, task or `a2a` step has no setting here). In the Builder: the agent panel's
**Prompt caching** section; the AgentExpress Assistant can also turn it on or off.

```json
"triage": { "name": "Triage", "toolMode": "model", "tool": ["policyDocs"],
            "promptCache": { "system": true, "tools": true, "messages": true, "ttl": "5m" } }
```

| Key | Notes |
|---|---|
| `system` | Cache the system prompt (instructions and skills). Recalled memory and today's date come after the cache point, uncached. |
| `tools` | Cache the tool definitions of a tool-calling turn (`toolMode: "model"`). |
| `messages` | Cache the conversation so far in tool-calling turns (`toolMode: "model"`). |
| `ttl` | `"5m"` (default), `"1h"` or `"30m"`, and only a value the model's row lists. |

What each model supports comes from `promptCacheModels` in `app/vocabulary.json`, copied from the
[Bedrock prompt caching](https://docs.aws.amazon.com/bedrock/latest/userguide/prompt-caching.html)
table, by base model id (an inference profile's geography prefix is dropped):

- `byModel`: per model `fields` (which of `tools`, `system`, `messages` it caches), `ttls`,
  `minTokens` / `minTokensText` (tokens per checkpoint, counted over tools, system and messages
  together) and `maxCheckpoints`. Claude models take all three fields; Amazon Nova takes `system`
  and `messages` with a 5-minute TTL.
- `openaiExplicit`: the OpenAI rows marked `"api": "InvokeModel"` (GPT-5.6 Sol, Terra and Luna,
  GPT-6 Astra, GPT-6.1 Sol). Only the system prompt of a single model call is cached, through
  InvokeModel with an explicit breakpoint, TTL `30m`; tool-calling turns stay on Converse,
  uncached.
- A model in no row gets no cache point and the Build view warns. Other OpenAI models cache
  implicitly (`implicitOnly`); any cache tokens they report are still recorded.

`tools` or `messages` on a model whose row lacks that field, or a `ttl` it does not list, is an
error. Below the minimum the call still runs, uncached. If Bedrock refuses the cache point, the
call is retried once without it. Each call's telemetry row records the cache read and write
tokens and their cost; Observability shows them per call, per run and on the Insights tab.

### `agents.<id>.agentcore` — the AgentCore feature switches

The framework applies these around the agent's `run()`, so turning a capability on
or off is a config change. Omit a block to leave the capability off.

| Key | Read by | Notes |
|---|---|---|
| `memory.longTerm` | `context.py` | List of strategies: `semantic` extracts discrete insights into `insights/{actor}` (cross-run), `summary` maintains a running summary in `summary/{actor}/{session}` (session-scoped), `userPreference` learns the requester's stated preferences into `preferences/{actor}`, and `episodic` records each run in `episodes/{actor}/{session}` with cross-run reflections in `episodes/{actor}` (what recall reads). `true` is shorthand for `["semantic"]`. Before `run()` the framework recalls past insights into the system prompt; after, it stores new ones. The actor is the agent plus `memory.scope` (default `user`: a digest of the user, and the subject when the run has one), so insights don't leak between users or subjects. `semantic` and `summary` are always provisioned; `userPreference` and `episodic` only when some agent names them (each strategy is a model call per stored event). Any other name fails the schema (`workflow.schema.json`) and is rejected at container start (`app/orchestrator/registry.py`) rather than silently searched as a namespace nothing writes to. **Where the recall/store runs depends on the placement:** for `main` it is the orchestrator; for `dedicated` it is inside that agent's own container, because that is where `ctx.llm` injects the insights; for `a2a` it is the orchestrator, which carries the recalled insights to the remote agent inside the A2A task (`recalledContext`, with the caveat attached) and stores its reply afterwards. |
| `memory.scope` | `context.py` | Whose memories: `user` (default, each user's own), `subject` (shared per `subject_id`), `agent` (everyone who runs the agent) or `run`. |
| `memory.custom` | IaC custom strategy; `context.py` | `{"base": "semantic" \| "summary" \| "userPreference", "instructions": "...", "model": "..."}`: a built-in strategy that follows your extraction instructions. |
| `memory.use` | `context.py`; IaC named memories | Use a memory from [`memories`](#named-blocks-guardrails-memories-evaluators-identities-policies) instead of setting strategies here. Agents naming the same memory share it, within its scope. |
| `identity.outbound` | `context.py` | Credential providers this agent may fetch an OAuth token from, to call an external API **directly** via `ctx.get_identity_token`. Not needed for anything reached through the Gateway. |
| `guardrails.input` | `context.py` | Run the Bedrock guardrail on the agent's input, before the model sees it. |
| `guardrails.output` | `context.py` | Run it on the agent's output. Use this on any agent handling untrusted text. |
| `guardrails.guardrailId` | `context.py` → the `GuardrailIdentifier` on the Bedrock call | Use a **different** guardrail for this one agent instead of the deployment's own — for a step whose content policy genuinely differs, such as a medical or legal stage inside a general pipeline. Omit and the deployment guardrail applies. |
| `guardrails.use` | `context.py`; IaC named guardrails | Apply a guardrail from [`guardrails`](#named-blocks-guardrails-memories-evaluators-identities-policies) instead of the deployment's own; `input` and/or `output` say where. |
| `evaluations.enabled` | `evaluations/service.py` | Enables AgentCore Evaluations (LLM-as-judge) and shows the Evaluate button. |
| `evaluations.auto` | same | `true` scores the agent automatically at run completion; `false` means on demand only. Each evaluator is a billed model call, so `false` is the cheaper default. |
| `evaluations.evaluators` | same | Built-ins, `Builtin.<Name>`: `Coherence`, `Conciseness`, `Correctness`, `Faithfulness`, `GoalSuccessRate`, `Harmfulness`, `Helpfulness`, `InstructionFollowing`, `Refusal`, `ResponseRelevance`, `SkillInstructionFollowing`, `SkillSelectionAccuracy`, `Stereotyping`, `ToolParameterAccuracy`, `ToolSelectionAccuracy`, `TrajectoryAnyOrderMatch`, `TrajectoryExactOrderMatch`, `TrajectoryInOrderMatch` (scored by this framework: Coherence, Conciseness, Correctness, Faithfulness, Harmfulness, Helpfulness, InstructionFollowing, Refusal, ResponseRelevance, Stereotyping). Validated on SHAPE (`Builtin.<Name>` or `Custom.<Name>`) rather than against this list, so an evaluator AWS adds later needs no framework edit, while a typo like `Faithfullness` is still caught at container start rather than as an API error mid-run. |
| `evaluations.custom` | IaC evaluator; `evaluations/service.py` | Your own LLM judges for this agent: `[{"name", "instructions", "model", "scale"}]`. Each becomes an AgentCore evaluator, scored like a built-in and deleted on destroy. Or list `Custom.<name>` from [`evaluators`](#named-blocks-guardrails-memories-evaluators-identities-policies) in `evaluators`. |
| `policy.enabled` | nothing | **Has no effect** (a validation warning says so); kept only so older files still load. AgentCore Policy evaluates Cedar at the Gateway, on every tool call of every agent: set `orchestrator.policy`. AgentCore has no API for a Cedar decision from agent code, so `ctx.policy_check` always returns true. |

**Deliberately not per-agent**, though it might look like it should be:

- **Short-term memory / the checkpointer.** One LangGraph checkpointer per
  deployment, in AgentCore Memory. There is no per-agent switch.
- **Inbound authentication.** One API Gateway JWT authorizer per deployment.
- **Observability.** Telemetry is captured for every agent, always. There is no
  off switch, per agent or otherwise.
- **Which Gateway targets an agent may reach.** Controlled by the agent's `tool`
  binding plus the generated Cedar permit — enforced server-side, not by a list in
  this file.

Each of those had a key here at one point. They were removed because they implied
control that didn't exist.

## `tools.<name>`

The key is **both** the Gateway target name **and** the label an agent binds to via
`tool`, so the two can never drift.

> **The key must be letters and digits, starting with a letter** — `policyDocs`, not
> `policy_docs` and not `policy-docs`. Narrower than an agent id, which *may* contain
> underscores, because the key builds two AWS names with contradictory constraints: the
> Gateway target is `<key>` and forbids underscores, the Cedar policy is `permit_<key>`
> and forbids hyphens. Neither separator survives both, and camelCase matches the rest of
> this file (`maxTokens`, `agentCard`, `gateId`). All four planes reject a bad key up
> front, rather than letting CloudFormation refuse the change set after a clean synth.

Seven types:

| `type` | Backend | Required keys |
|---|---|---|
| `kb` | Bedrock Knowledge Base: over `kb_docs/` / uploads (S3 Vectors), over your own bucket (`s3Uri`, optional `kmsKeyArn`), or your existing one (`knowledgeBaseId`) | `corpora` |
| `websearch` | The AWS-managed AgentCore Web Search connector | — |
| `mcp` | Any remote MCP server over Streamable HTTP, or one the build hosts on its own AgentCore Runtime | `endpoint` **or** `hosted` |
| `openapi` | Any REST API, from an OpenAPI 3 document: in S3, in `app/tools/`, or inline | `schemaS3Uri` **or** `source` **or** `schema` |
| `apigateway` | A stage of an API Gateway REST API in the deploy's account and region | `restApiId`, `stage`, `toolFilters` |
| `lambda` | A Lambda — yours, or one the framework deploys | `lambdaArn` **or** `source`, plus `toolSchema` |
| `smithy` | An AWS service's API, from its Smithy JSON model: in S3 or inline | `schemaS3Uri` **or** `schema`, plus `allow` |

Outbound `auth` for `mcp`, `openapi` and `apigateway`: `apikey` (vaulted, sent as X-API-Key),
`sigv4` (signed with the Gateway's role; for `apigateway`, `execute-api:Invoke` on exactly
that API and stage), `oauth2` (`mcp`/`openapi` only: OAuth client credentials from an
AgentCore Identity provider the framework creates from `oauth` — `clientId`, `scopes`, and
`discoveryUrl` or `tokenUrl`) or `none`. A key or client secret goes in `tool_api_keys` (a
build's secrets), never in workflow.json.

Two more modes act as **the person using the app**, not as the app (`mcp`/`openapi` only, and
they need a sign-in provider: not `idp=none`):

| `auth` | In the Builder | What happens |
|---|---|---|
| `user` | User login (3LO) | OAuth authorization code. The first call for a person fails the step with a **Connect** button; they sign in at the tool's provider once, come back to the app (which binds the grant to them, `POST /api/connect`), and run the step again. AgentCore Identity keeps one grant per person. |
| `obo` | On behalf of user (OBO) | The person's sign-in token is exchanged on their behalf (RFC 8693 token exchange) for a token to the tool. `oauth.audience` names the tool's API, if its provider needs one. **Not with Entra ID sign-in**: Entra's exchange needs an access token for the app, which sign-in does not request. |

Both use `oauth` like `oauth2` (`clientId`, `scopes`, `discoveryUrl` or `tokenUrl`, the client
secret in `tool_api_keys`). With `user` and a `tokenUrl`, also give `oauth.authorizationUrl`
(where people sign in). After the first deploy, the Builder shows the tool's **callback URL**:
register it as a redirect URL at the tool's provider. An `mcp` tool in either mode must list its
tools in `toolSchema`, because the Gateway cannot ask the server before anyone has connected.

These tools sit on a second Gateway (`<agent>-gwu`) that trusts the app's sign-in, so each call
carries the person's own token; the agents' machine client never reaches them, and the Cedar
permits do not cover them (the tool's provider authorizes each person). The token reaches the
runtime only for the person who started the run, in the build's own app: a step approved by a
reviewer, started by a trigger, or run from the Builder console fails with a message to run it
there instead.

Shared keys:

| Key | Notes |
|---|---|
| `description` | Becomes the Gateway target description. Truncated to 190 characters on deploy. |
| `call` | Which tool on the target to invoke. **Required** when the target publishes more than one — the app refuses to guess, because calling the wrong tool returns real-looking data for a question nobody asked. |
| `arg` | The parameter the agent's query goes into. Default `query`. This is what makes an arbitrary MCP server reachable from config: every server names its parameters differently. |
| `args` | Fixed extra arguments sent on every call. |
| `approval` | `mcp`, `openapi`, `lambda`, `apigateway`, `smithy`. `"person"` or `"event"`: every call of this tool waits for a person, who sees the tool and the exact arguments it will be sent (the fixed `args` included) and approves or denies. Approve makes the call; deny tells the agent it was refused, and it carries on (in `direct` mode the refusal becomes one of its limitations). `"event"` also publishes an "AgentExpress Approval Requested" event, answerable with an "AgentExpress Approval Decision" event, as a gate's `approval: "event"` is. A tool its Cedar policy hides is never put to a person; a **conditional** Cedar rule is evaluated by the Gateway when the call is made, after the approval. Refused at a `dedicated` agent, which cannot wait. An unattended run refuses the call. |
| `approvalTimeout` | With `approval`: `{"after": "1h", "action": "approve" \| "deny"}`. A call nobody decides by then takes the action. |
| `rowFields` | Maps the roles an agent needs to the field names your payload actually uses, so an agent that *computes* from tool output stays config-driven. See [`rowFields`](#rowfields--for-an-agent-that-computes-rather-than-narrates) below. |
| `rowPath` | Dot path to the **list** of records in the response, e.g. `result.releases`. Needed only when the list is somewhere the framework does not already probe for (`results`, `result`, `items`, `documents`, `content.result`, `content.results`) — common for a REST API you did not design. Pairs with `rowFields`: this says *where* the rows are, that says what they are called. Without it an agent that computes sees **zero** rows and reports "nothing found" for a response that was full of data, which is exactly how it was first found. |
| `auth` | Outbound auth to the endpoint: `none`, `apikey` (vaulted, sent as `X-API-Key`), `sigv4` (the Gateway signs with its own role — no secret), `oauth2`, `user` or `obo` (above). |
| `policy.tool` | Narrow the Cedar permit to one tool name. Omit for a target-level permit, which is what a remote MCP server needs since its tool names aren't known at deploy time. |
| `policy.restrictTo` | Argument allow-lists, e.g. `{ "filter": ["reference"] }`. Enforced at the Gateway, so a prompt-injected attempt to widen access is refused by infrastructure rather than by prompt wording. |
| `policy.permit` | `false` registers the target but deliberately does not permit it — useful for demonstrating default-deny. |
| `policies` | Names in [`policies`](#named-blocks-guardrails-memories-evaluators-identities-policies) that govern this tool. A statement written with `AgentCore::Action::"{{tool}}"` is deployed once per tool it is attached to. |
| `identity` | A name in `identities`, instead of `auth` + `oauth`: an `oauth2` identity makes the tool `auth: "oauth2"` (`mcp`, `openapi`), an `apikey` one `auth: "apikey"`. The secret is the identity's. |
| `toolFilters`, `toolOverrides` | `apigateway` only. `toolFilters` is the allow list of operations (`[{"path": "/orders/*", "methods": ["GET"]}]`; a path is explicit or a prefix ending `/*`). `toolOverrides` renames or describes a selected operation (`[{"path", "method", "name", "description"}]`), required for one with no `operationId`. |
| `registry` | Where an `mcp` tool came from in an AWS Agent Registry. Set by the console, never by hand; with `sync: true` its `description`, `endpoint` and `toolSchema` follow the newest approved version. The runtime ignores it. |

**Secrets never live here.** An API key goes in `tool_api_keys` in
`terraform.tfvars`, or `$TOOL_API_KEYS` for CDK, keyed by the tool name.

### `type: "kb"`

One per deployment. `corpora` lists the top-level folders under `kb_docs/`; each
becomes a `doc_type` tag, so different agents can be scoped to different document
sets. A corpus naming a folder that doesn't exist is rejected at plan time — before
that check existed, it produced a Cedar filter on a `doc_type` no chunk carried and
retrieval quietly returned nothing.

**The embedding model and its dimension are a validated pair.**

```json
"kb": {
  "type": "kb",
  "corpora": ["reference"],
  "embeddingModel": "amazon.titan-embed-text-v2:0",
  "dimensions": 512
}
```

Both are optional: `dimensions` defaults to the model's first supported size, and the
model defaults to Titan v2 at 1024, which is what was hardcoded before either key
existed. Declaring a size the model does not support is rejected at plan/synth, and that
check matters more than most — Bedrock does **not** reject a mismatch when you deploy. It
fails later at *ingestion*, so the stack reports success and the corpus is silently empty.

Changing either key **replaces** the vector index and the Knowledge Base, because neither
property can be altered in place. Both IaC paths derive those resource names from a digest
of the pair, which is what turns the replacement into a working deploy instead of
CloudFormation refusing to replace a custom-named resource.

**Your own vector store: `vectorStore`.** Optional; leave it out and the deploy creates an S3
Vectors bucket and index, as before. With it, the Knowledge Base uses your store. Every setting
listed is required unless marked optional; the store and its index must exist first.

```json
"kb": {
  "type": "kb",
  "corpora": ["reference"],
  "vectorStore": {
    "type": "opensearchServerless",
    "collectionArn": "arn:aws:aoss:us-east-1:111122223333:collection/abc123",
    "indexName": "kb-index",
    "vectorField": "embedding", "textField": "text", "metadataField": "metadata"
  }
}
```

| `type` | Settings |
|---|---|
| `opensearchServerless` | `collectionArn`, `indexName`, `vectorField`, `textField`, `metadataField` |
| `opensearchManaged` | `domainArn`, `domainEndpoint`, `indexName`, `vectorField`, `textField`, `metadataField` |
| `aurora` | `resourceArn` (cluster), `credentialsSecretArn`, `databaseName`, `tableName`, `primaryKeyField`, `vectorField`, `textField`, `metadataField`, optional `customMetadataField` |
| `neptuneAnalytics` | `graphArn`, `graphModel` (builds the graph; a model or inference profile id), `textField`, `metadataField` |
| `pinecone` | `connectionString`, `credentialsSecretArn`, `textField`, `metadataField`, optional `namespace` |
| `mongodbAtlas` | `endpoint`, `credentialsSecretArn`, `databaseName`, `collectionName`, `indexName`, `vectorField`, `textField`, `metadataField`, optional `endpointServiceName`, `textIndexName` |

ARNs must be full, with their account (they go into the Knowledge Base role's grants), and the
shapes are Bedrock's. Not with `knowledgeBaseId`. What each store grants, and what
you set up first, is in `DEPLOYMENT.md`, "Knowledge Base on your own vector store".

**Documents from a connector: `dataSource`.** Optional; leave it out and the documents are
your uploads or `s3Uri`. Needs `vectorStore.type: "opensearchServerless"` (AWS supports these
connectors only there). Not with `s3Uri` or `knowledgeBaseId`.

```json
"dataSource": { "type": "web", "seedUrls": ["https://docs.example.com/"], "scope": "hostOnly", "maxPages": 500 }
```

| `type` | Settings |
|---|---|
| `web` | `seedUrls` (1–100 http(s) URLs); optional `scope` (`hostOnly` or `subdomains`; default: the seed's host and path), `inclusionFilters`, `exclusionFilters` (up to 25 regexes each), `rateLimit` (1–300 pages a minute per host), `maxPages` (1–25000), `userAgent` (a 15–40 character suffix) |
| `confluence` | `hostUrl`, `authType` (`basic` or `oauth2`), `credentialsSecretArn`; optional `filters` |
| `sharepoint` | `domain`, `siteUrls` (1–100), `credentialsSecretArn`; optional `tenantId`, `filters` |
| `salesforce` | `hostUrl`, `credentialsSecretArn`; optional `filters` |

`filters` is up to 25 `{ "objectType": "Page", "include": [...], "exclude": [...] }` entries,
each a content type and up to 25 regexes. The limits are Bedrock's.

**How documents are split and read: `chunking` and `parsing`.** Both optional; leave them out
and the data source is exactly what it was (Bedrock's default ~300-token chunks, its default
text parser). Not with `knowledgeBaseId`: that Knowledge Base already has its own.

```json
"kb": {
  "type": "kb",
  "corpora": ["reference"],
  "chunking": { "strategy": "fixedSize", "maxTokens": 500, "overlapPercentage": 15 },
  "parsing": { "strategy": "foundationModel", "model": "us.amazon.nova-pro-v1:0" }
}
```

| Key | Settings | What it does |
|---|---|---|
| `chunking.strategy: "fixedSize"` | `maxTokens` (1–8192), `overlapPercentage` (1–99) | Chunks of at most `maxTokens`, each repeating that share of the one before. |
| `chunking.strategy: "hierarchical"` | `parentMaxTokens`, `childMaxTokens` (1–8192, child smaller), `overlapTokens` (≥ 1) | Search matches the small chunks and returns the large one around them. AWS does not recommend it with S3 Vectors, the store this framework creates: large chunks can exceed its per-vector metadata limit (the Build view warns). |
| `chunking.strategy: "semantic"` | `maxTokens` (1–8192), `bufferSize` (0–1), `breakpointPercentileThreshold` (50–99) | Splits where the meaning changes. Costs model calls at ingestion. |
| `chunking.strategy: "none"` | — | Each file is one chunk. No page numbers in citations. |
| `parsing.strategy: "foundationModel"` | `model` (a model id or a system inference profile id), optional `prompt` (1–10000 characters) | The model reads every PDF and image, including tables and figures, at its token price. The Knowledge Base role gets `bedrock:InvokeModel` on that model only (for a profile: the profile, and that model in any region). |

Every setting a strategy lists is required, and the ranges are Bedrock's. Changing either key **replaces the
data source** (neither can be changed in place) and re-ingests every document: the data source
is named `docs-<digest>` of the settings on both IaC paths, so the replacement is clean. Not
offered: Bedrock Data Automation parsing (AWS lists it as a preview in US West (Oregon) only),
custom transformation Lambdas, context enrichment, and multimodal storage (so parsed images are
read as text, not stored as files).

**Retrieval is config, with one deliberate exception.**

| Key | Default | What it does |
|---|---|---|
| `maxResults` | 5 | Retrieval depth. |
| `corpusKey` | `doc_type` | Which metadata attribute an agent's `corpus` is matched against. Set it when your documents are already tagged by customer id, product line or classification, so a corpus need not be a folder name. |
| `corpusOperator` | `equals` | How `corpus` is compared. Single-value operators only — see below. |
| `filter` | none | A Bedrock-shaped filter (`andAll`/`orAll`/…) applied to **every** call, set on the target and invisible to the agent. |
| `rerank` | none | `{ "model": "amazon.rerank-v1:0", "count": 3 }` — reorder retrieved chunks with a second model. Costs one extra model call per retrieval; the framework grants exactly that model and no other. |

**Why there are two filters, and why only one of them is rich.** The filter an *agent*
sends is a single scalar, its `corpus`. That is not a limitation, it is what makes the
permit enforceable: the generated Cedar rule is a value match
(`["reference"].contains(context.input.filter)`), evaluated at the Gateway *before* the
retrieve Lambda runs. A list-valued operator would make that argument a list, which the
rule cannot express — so it would degrade to "has a filter at all" while still looking
enforced. Hence `corpusOperator` offers only `equals`, `notEquals`, `startsWith` and
`stringContains`.

Multi-condition narrowing therefore lives in `filter`, on the target, where the caller
cannot influence it. It is **ANDed** with the agent's corpus filter, so it can only ever
narrow. A filter an agent supplies is scoping; a filter it cannot reach is a boundary.

```json
"filter": {
  "andAll": [
    { "equals": { "key": "tier", "value": "public" } },
    { "orAll": [
      { "equals": { "key": "region", "value": "emea" } },
      { "equals": { "key": "region", "value": "amer" } }
    ]}
  ]
}
```

**There is no `searchType`, and that is deliberate.** One was added, deployed and removed
the same day: S3 Vectors — the only vector store this framework provisions — refuses
hybrid search (`HYBRID search type is not supported for search operation on index …`),
which left `SEMANTIC`, the default, as the only legal value. A key whose one working value
is the default expresses nothing, and whose other value fails at *retrieval* after a green
deploy is worse than no key. If this framework ever provisions OpenSearch Serverless
instead, hybrid becomes real and the key comes back.

### `type: "websearch"`

One per deployment, and only in `us-east-1`, `eu-west-1`, `ap-northeast-1`.

**Domain filtering is one key**, applied on the Gateway target:

```json
"websearch": {
  "type": "websearch",
  "maxResults": 10,
  "domains": { "include": ["docs.aws.amazon.com"], "exclude": ["spam.example"] }
}
```

Because it lives on the target it is hidden from the agent and applied by the Gateway
to every request, so it is a **boundary** — a prompt-injected instruction cannot widen
it. A domain is dropped if it is on `exclude`, and returned only if it is on `include`
when `include` is set.

> **Renamed.** This was four keys: a request-level `includeDomains`/`excludeDomains`
> pair beside a target-level `targetIncludeDomains`/`targetExcludeDomains` pair. They
> expressed one intent, and the pair with the obvious name was the weaker one — a
> request-level filter is caller-supplied, so the Gateway treats it as scoping rather
> than as a boundary, and it additionally needs connector v1.2.0+. Both lists came from
> the same config file, so setting both only ever produced their intersection, which you
> could write directly. Nothing was lost. Any of the four old names is now **rejected**
> rather than ignored, and the message names the replacement.

`publishedFrom` / `publishedTo` (inclusive ISO-8601 UTC) stay request-level, because the
target has no equivalent — there is no stronger place to put them.

`maxResults` is 1–25, default 10. The query is capped at 200 characters and the app
clamps it. `connectorVersion` pins the connector, but **only on the Terraform path** —
CloudFormation's connector source accepts only `connectorId`, so CDK rejects the field
rather than accepting and ignoring it.

**Acceptable use:** you must retain and display the source citations returned with
each result. The framework enforces the display half — the connector's
`title`/`url`/`publishedDate` are preserved into the evidence, every citation URL is
verified against that evidence before it is kept, and the UI renders each source as
a link. An unverifiable URL is dropped and a `sourced-fact` resting on it is
downgraded to `agent-interpretation`.

### `type: "mcp"`

To use your own MCP server, change `endpoint` (and `call`/`arg` to match one of its
tools). That is the entire change — no code, no IaC, no policy edit.

`listingMode`: `DEFAULT` pre-indexes and caches the server's tool catalogue (needed
for semantic tool search); `DYNAMIC` forwards `tools/list` at invocation time, which
avoids a stale catalogue when the server's tool set changes. Either works.

**On tool names:** the Gateway composes every published name as
`<targetName>___<toolName>`, and AWS *also* prefixes tools on its managed servers
with `aws___`. So the shipped `docs` target publishes
`docs___aws___search_documentation`, and `call` is `aws___search_documentation` —
the published name minus the `<targetName>___` part. Doubly prefixed names work
fine; nothing splits on `___`.

**If you verify with a `tools/list` call, follow `nextCursor`.** The response is
paginated, and a target whose tools land on page two looks empty if you only read
page one.

**A server the build hosts: `hosted`.** Set `hosted` instead of `endpoint`, and the deploy
runs the server on its own AgentCore Runtime (protocol MCP, an ARM64 container serving
streamable HTTP on `0.0.0.0:8000/mcp`) and registers it on the build's Gateway. Its tools
are published as `<key>___<tool>`, like any MCP target. In the Builder: **Host it here** in
the tool's MCP server panel.

```json
"facts": { "type": "mcp", "description": "Look up reference facts.", "call": "lookup_fact", "arg": "query",
           "hosted": { "code": { "git": { "url": "https://git.example.com/team/facts-mcp.git", "ref": "v1.2.0" } },
                       "network": { "mode": "PUBLIC" } } }
```

| Key | Notes |
|---|---|
| `hosted.code` | Optional. Exactly one of `{"git": {...}}`, `{"s3": {...}}` (the [code source](#code-sources-git-s3-or-an-image) shapes) or `{"image": "<account>.dkr.ecr.<region>.amazonaws.com/<repo>[:tag \| @sha256:<digest>]"}` (a private ECR image; only here). Omitted: the files written or uploaded in the build, `toolCode.<key>`: `server.py`, `requirements.txt` (starts as `mcp==1.30.0`, `pydantic==2.13.5`) and an optional `Dockerfile`. |
| `hosted.network` | Optional. The server Runtime's network, the same shape as [`orchestrator.network`](#orchestrator). Omitted: `PUBLIC`. |

`hosted` and `endpoint` are mutually exclusive. `auth` is unset or `sigv4` (the Gateway signs
with its own role) and `service` unset. The key is at most 32 characters, because it is part
of the Runtime name `<agentName>_mcp_<key>`. What the server must serve, the IAM and the
images: [DEPLOYMENT](../../DEPLOYMENT.md#hosted-mcp-servers).

### `type: "openapi"`

For a service that already speaks HTTP and already has a contract: an internal
microservice, a third-party API, a vendor's catalogue. The Gateway loads the schema,
publishes **each `operationId` in it** as an MCP tool, and translates the call into an
HTTP request. There is no function to write and no server to run — the API is reached
as it already is.

```json
"lifecycle": {
  "type": "openapi",
  "description": "Support and end-of-life dates for versioned products.",
  "source": "lifecycle",
  "call": "getProductLifecycle",
  "arg": "product",
  "rowPath": "result.releases",
  "rowFields": { "version": "name", "supportedUntil": "eolFrom", "isEol": "isEol" }
}
```

**`call` is an `operationId`, and `arg` is a parameter name from the schema.** Both are
yours to choose, because you wrote the schema — which also means you can avoid the
`___` delimiter awkwardness that `mcp` targets inherit.

**Supply the schema one of two ways.** The Gateway can only load one from S3, so one of
these has to say which object:

- `schemaS3Uri` — an object **you** host, e.g. `s3://amzn-s3-demo-bucket/schemas/orders/v2.json`. The
  framework uploads nothing; it points the target at that key and grants the Gateway
  role `GetObject` on exactly it.
- `source` — a folder under `orchestrator/app/tools/` holding `openapi.json`, which the
  framework uploads to a bucket it owns and whose URI it then derives. This is what
  keeps a committed `workflow.json` account-neutral: **a bucket name is as
  account-specific as a Lambda ARN**, so hardcoding one makes the config undeployable
  anywhere else. Unlike `source` on a `lambda` tool, this takes **any** folder name —
  uploading a file needs no permissions, so there is nothing to restrict, and the check
  is simply whether the file is there.

**The schema is the allow-list, so keep it small.** Describe the one or two operations
the agent needs, not the service. Anything you leave out is unreachable — not merely
undocumented — and the same goes for response fields. The schema this sample ships is
~80 lines against an API that publishes far more.

**Write down where the rows are.** A REST API you did not design nests its records
wherever it likes. This one returns `{"result": {"releases": [...]}}`, which none of the
locations the framework probes for matches, so `rowPath` names it. Skip it and an agent
that computes gets zero rows from a full response and truthfully reports finding
nothing — a failure that looks exactly like a working agent against an empty API.

**A wrong identifier is a 404, not an empty result.** An `openapi` target is addressed
by path, so there is no fuzzy matching: an agent that builds a path segment from model
output should expect individual calls to fail and say which ones did, rather than
failing the run. See `app/subagents/lifecycle_research/agent.py`, which collects the
identifiers that did not resolve and names them in the asset's limitations.

### `type: "smithy"`

For an AWS service the agent should call directly, described by its Smithy model. The
Gateway publishes **each operation in the model** as a tool (`<key>___<Operation>`) and
makes the call itself, signed with SigV4 by this build's Gateway role
(`AgentCoreGateway-<agentName>`). There is no function to write.

```json
"places": {
  "type": "smithy",
  "description": "Find a store by name or address.",
  "schemaS3Uri": "s3://amzn-s3-demo-bucket/models/location.json",
  "allow": {
    "actions": ["geo:SearchPlaceIndexForText"],
    "resources": ["arn:aws:geo:us-east-1:111122223333:place-index/stores"]
  },
  "call": "SearchPlaceIndexForText",
  "arg": "Text"
}
```

| Key | Notes |
|---|---|
| `schema` / `schemaS3Uri` | Exactly one. `schema` is the Smithy 2.0 JSON AST itself (`{"smithy": "2.0", "shapes": {...}}`, one `service` shape, at least one `operation`), sent inline. `schemaS3Uri` is a JSON model you host; the Gateway role gets `s3:GetObject` on exactly that object. AWS publishes the models of its services at [aws/api-models-aws](https://github.com/aws/api-models-aws). |
| `allow` | **Required.** What the Gateway role may call for this tool: `{"actions": [...], "resources": [...]}`, 1 to 20 of each. Each action is one named action (`dynamodb:GetItem`, no `*`); each resource a full ARN with its region and account written out (or empty where the service has none, as in `arn:aws:s3:::bucket/key`), never a bare `*`. Actions of `iam`, `sts`, `organizations`, `account`, `sso`, `identitystore`, `kms`, `secretsmanager`, `bedrock-agentcore`, `lambda` and `cloudformation` are refused (`smithyDeniedServices` in `app/vocabulary.json`): the role is shared by every tool of the build, so those would let a model change who may do what, run code, or read secrets. The grant is a statement `SmithyCalls<key>` on the Gateway role, present only while the tool is. |
| `auth` | Not set: a Smithy target always uses the Gateway role (AWS documents no IAM signing options for it, and OAuth is for non-AWS APIs, which a Smithy target does not take). |

Limits from AWS: custom Smithy models are for **AWS services only**, the RestJson protocol only (a model with no `aws.protocols#restJson1` service is a
warning in the Build view), and up to 10 MB. Streaming operations are not supported. Each
`allow` adds to the Gateway role's inline policy, which IAM caps at 10,240 characters for
the role, so a build with many large grants can fail to deploy on that limit.
See [Smithy model targets](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-building-smithy-targets.html).

### `type: "lambda"`

The escape hatch: a Lambda reaches what the Gateway cannot — a warehouse (Redshift,
Snowflake), any RDBMS, an internal service, anything inside a VPC.

Supply the function **one** of two ways:

- `lambdaArn` — a function **you** own. The framework only registers it: it grants
  the Gateway `lambda:InvokeFunction` on exactly that ARN and, for a same-account
  function, adds the resource-policy statement. It never touches your code or your
  execution role. A cross-account function works, but its owning account must add
  that statement itself.
- `source` — a function the **framework** packages and deploys, which keeps this file
  account-neutral (a real ARN would pin it to one AWS account). The value names a
  **folder under `orchestrator/app/tools/`** holding a `handler.py`; both IaC paths zip
  that folder and deploy it. The accepted values are `pricing`, the demo at
  `orchestrator/app/tools/pricing/`, `codeInterpreter` (see
  [below](#source-codeinterpreter--run-code-in-a-sandbox)) and `browser` (see
  [below](#source-browser--open-a-web-page)). It is not a general "deploy
  any directory" feature: a framework-deployed function runs on a role the framework
  writes, one per value. The `pricing` role is fixed at logs plus
  `pricing:GetProducts` / `pricing:DescribeServices` — public list prices, no
  resource-level permissions available and none needed. Your own
  connector will usually need something else (a VPC, a secret, a table grant), which
  config cannot express, so deploy it yourself and use `lambdaArn`. Its source may live
  beside `app/tools/pricing/` if you like the symmetry — nothing reads it there unless
  `source` names it.

  **`source` means the same thing for `type: "openapi"`** — "the framework supplies this
  tool's artifact from `app/tools/<source>/`" — and only the artifact differs:
  `handler.py` here, `openapi.json` there. The accepted VALUES differ because only one
  of the two restrictions is real. A deployed *function* needs an execution role, so it
  is closed; an uploaded *schema* needs no permissions, so `openapi` takes any folder
  name. Your editor enforces exactly that split, because the enum is attached to the
  `lambda` branch of the schema only.

  `app/tools/` is excluded from the orchestrator container image, because nothing in it
  runs inside the container — a `lambda` tool's handler runs as its own Lambda and an
  `openapi` tool's schema is read by the Gateway, and the orchestrator reaches both
  through the Gateway rather than importing anything. It is the only *code* under `app/`
  that is excluded — the other two exclusions there, `app/keys.json` and
  `app/workflow.schema.json`, are editor-facing documentation the runtime never reads.

**What the shipped demo does, and why it is that.** `aws_prices(services, region)`
takes service names and returns their real on-demand unit rates from the AWS Price
List Query API. It answers the one question every design review asks about a
workload, for *any* use case, without the workflow needing to know the use case.

Three things in it are worth copying into your own Lambda:

- **It returns rates and never a total.** A total needs usage volumes, and volumes
  are a property of the workload, not of AWS. A tool that multiplies a real rate by
  a volume nobody supplied has invented the volume, and the invented half is the
  half that makes the total wrong. The tool instead names the volumes a total would
  need, and leaves supplying them to whoever knows them.
- **It resolves names explicitly, and reports what it cannot.** Pricing ServiceCodes
  are neither guessable nor consistent (`AmazonStates`, not `AWSStepFunctions`), so
  the mapping is a table, not a transformation. A name with no match or more than one
  comes back in `unresolvedServices` — pricing the wrong service is worse than
  admitting the gap, because the number looks exactly as authoritative as a correct
  one.
- **It curates which dimensions come back.** Unfiltered, "price AWS Lambda" returns
  EC2-shaped hourly instance rates. Instance and provisioned SKUs are excluded and a
  short per-service list of consumption dimensions is preferred, matched with a
  region-prefix tolerance so the same patterns work in every region.

`toolSchema` **declares** the tools, because unlike an MCP server there is no
`tools/list` for the Gateway to call. Each entry is `{ name, description,
properties }`, and each property is `{ type, required, description }` where `type`
is one of `string`, `number`, `integer`, `boolean`, `array`, `object`.

One function may publish several tools — the Gateway passes the tool name through so
your handler can dispatch on it. When it does, `call` is required and `arg` must
name a property of the called tool. Both are checked at plan/synth time, along with
the ARN format, an empty schema, a tool with no properties, and an unsupported
property type. All of those would otherwise be accepted by the API and then publish
a tool the agent cannot call — which shows up as an empty answer, not an error.

### Code sources: git, S3 or an image

Code written in the build (a `lambda` tool's `code`, an interceptor's `code`, a Run code
step's `task.code`, a hosted MCP server's files) can come from somewhere else, fetched by the
deploy (`deployer/code_fetch.py`). Checked by `bff/code_source.py` and `codeSource.ts`; in the
Builder, **Where the code comes from**.

| Where | Key |
|---|---|
| `lambda` tool | `tools.<key>.code.codeSource` |
| Interceptor | `orchestrator.interceptors.<request\|response>.code.codeSource` |
| Run code step | `agents.<id>.task.codeSource`, instead of `task.code` (one script, at most 50,000 bytes) |
| Hosted MCP server | `tools.<key>.hosted.code` |

Exactly one key:

```json
{ "git":   { "url": "https://git.example.com/team/tools.git", "ref": "v1.4.0", "path": "orders", "token": "gitDeployToken" } }
{ "s3":    { "uri": "s3://amzn-s3-demo-bucket/tools/orders/", "path": "src" } }
{ "image": "111122223333.dkr.ecr.us-east-1.amazonaws.com/facts-mcp:1.0" }
```

- `git.url`: `https://` only, no user name, password, query or fragment. `ref` (required): a
  branch, a tag or a commit. `path`: the folder inside the repository (for Run code, the
  script file). `token`: the NAME of a build secret of kind `gitTokens`, never the token.
- `s3.uri`: `s3://<bucket>/<prefix>/` (ending in `/`) or `s3://<bucket>/<key>.zip`. `path`: a
  folder inside it. In the console's own account only buckets in `codeSourceBuckets` /
  `code_source_buckets` are read, and never the console's own buckets.
- `image`: hosted MCP servers only, a private ECR image URI with an optional tag or `sha256`
  digest.
- Not set: the files are written or uploaded in the build, as before. A bundle carries git and
  S3 sources as references only, never the fetched code. Limits and IAM:
  [DEPLOYMENT](../../DEPLOYMENT.md#code-from-files-git-or-s3-code-sources).

### `source: "codeInterpreter"` — run code in a sandbox

An agent can write a program and run it, instead of only calling tools you prepared.
The framework deploys `orchestrator/app/tools/codeInterpreter/` as a Gateway Lambda
target, so Cedar policy and the interceptors apply to it like any other tool. Nothing
is created unless a tool names this `source`, and the shipped `workflow.json` does not.

```json
"sandbox": {
  "type": "lambda",
  "description": "Run a short program and return what it printed.",
  "source": "codeInterpreter",
  "call": "execute_code",
  "arg": "code",
  "toolSchema": [{
    "name": "execute_code",
    "description": "Run code in a sandbox. Returns stdout, stderr and the exit status.",
    "properties": {
      "language": { "type": "string", "required": true, "description": "python, javascript or typescript." },
      "code":     { "type": "string", "required": true, "description": "The program to run." }
    }
  }]
}
```

The `toolSchema` is the one above: the function reads `language` and `code`. It returns
`stdout`, `stderr`, `exitCode`, `language` and `seconds`. A program that fails (an
exception, a non-zero exit) comes back as an `error` result with the traceback in
`stderr`, so the model can read it and try again.

**What it executes, and where.** Code a model wrote, or that a person pasted into a
prompt. It runs in the AWS-managed AgentCore Code Interpreter `aws.codeinterpreter.v1`,
an isolated sandbox that AWS operates. It has no access to this deployment's tables,
buckets, secrets or credentials, and neither does the function that calls it: its role
may start, use and stop a session on that one interpreter, write to its own log group,
and do nothing else. **The sandbox's network access is the interpreter's default.** The
framework neither restricts nor widens it, and this repository did not test what that
default allows in your Region; check the AgentCore documentation before giving the
tool to an agent that reads untrusted text.

**Limits, held in the function's code** (`handler.py`): 50,000 bytes of code, 30 seconds
of run time, and 10,000 characters each of stdout and stderr. Longer output is cut and
the result says so (`truncated`, `note`). A call that runs out of time is stopped and
returns what it had printed. A call that is refused (no `code`, a language the
interpreter does not run, too much code) starts no session.

**Good to know.** Every call starts a fresh session, runs once and stops, so a second call
cannot read the first's variables or files. Use your Cedar policy to limit which agents call it.
Each call is billed as an AgentCore Code Interpreter session.

In the Builder, **Add a tool → Type → Code Interpreter** writes the entry above, with the
name you give as its description.
The form then shows only what you choose (the description, `approval`, policies); the
function, its schema and its call belong to the framework and are hidden.

### `source: "browser"` — open a web page

An agent can open a web page and read it: its title, its visible text and its links. The
framework deploys `orchestrator/app/tools/browser/` as a Gateway Lambda target, so Cedar
policy, the interceptors and `approval` apply to it like any other tool. Nothing is created
unless a tool names this `source`, and the shipped `workflow.json` does not. In the Builder:
**Add a tool → Type → Browser**.

```json
"web": {
  "type": "lambda",
  "description": "Open a web page and read it.",
  "source": "browser",
  "call": "open_page",
  "arg": "url",
  "allowDomains": ["docs.aws.amazon.com"],
  "toolSchema": [{
    "name": "open_page",
    "description": "Open a web page. Returns its title, visible text and links.",
    "properties": {
      "url": { "type": "string", "required": true, "description": "The page to open, https:// or http://." }
    }
  }]
}
```

The function publishes one tool, `open_page(url)`, and the checks refuse any other name in
`toolSchema`. It returns `url` (where the page ended up), `title`, `text`, `links` (up to 50,
each `text` and `href`) and `seconds`. A page that did not open, or was refused, comes back
as an `error` result.

| Key | Notes |
|---|---|
| `allowDomains` | Optional. The domains it may open, e.g. `["docs.aws.amazon.com"]`; each also allows the names under it. A name only: no `https://`, path, port or `*`; at most 50. Checked by the function for the page asked for **and** the page a redirect ends on, so a model cannot widen it. Left out: any public page. Only the Browser tool reads it. |

**How it drives the browser.** Each call starts a session on the AWS-managed AgentCore
Browser `aws.browser.v1`, connects to the session's automation stream (the Chrome DevTools
Protocol over a WebSocket signed with SigV4), opens the page in a new tab, waits for its load
event, reads it, and **always** stops the session. `InvokeBrowser` (mouse, keyboard and
screenshots at the operating-system level) cannot open or read a page, so it is not used.
The function's role may start and stop sessions on that one browser, connect to an
automation stream, and write its own log group; `ConnectBrowserAutomationStream` has no
resource type in IAM, so that one statement is on `*`.

**What it refuses, in its code.** Anything but `http://` and `https://`, a URL with
`user:password@`, a URL over 2,048 characters, and a host that is, or resolves to, a
loopback, link-local (the metadata address), private, CGNAT or other non-public address.
The same check runs again on the page the browser ended on, after any redirect; if that page
is refused nothing from it is returned. A refused URL starts no session.

**Limits, held in the function's code** (`handler.py`): 20 seconds for the page to load (a
page still loading is read as it is, and the result says so), 40 seconds for the whole call,
20,000 characters of text (longer is cut, with `truncated` and a `note`), 50 links.

**Good to know.** The page's text is untrusted: an agent that reads it can be told things by
the page, so give this tool to an agent whose other tools cannot do harm, or put `approval`
on those. The address and domain checks apply to the page opened, not to what the page loads.
The browser runs in AWS's network, so it reaches public pages, not your private services or
pages behind a sign-in. It reads pages: no clicking, typing, screenshots or downloads, and each
call is a fresh, billed AgentCore Browser session. It has run on a deployed CDK build and a
deployed Terraform build.

<!-- begin Track B docgen -->
### `source: "docgen"` — the Document generator: PDF, PPTX and DOCX files

An agent can write a PDF, PPTX or DOCX file from JSON data, into a template stored in S3
(PPTX, DOCX) or a built-in simple layout. The framework deploys
`orchestrator/app/tools/docgen/` as a Gateway Lambda target, so Cedar policy, the
interceptors and `approval` apply to it like any other tool. Nothing is created unless a
tool names this `source`, and the shipped `workflow.json` does not. In the Builder:
**Add a tool → Type → Document generator**.

```json
"documents": {
  "type": "lambda",
  "description": "Write the report files.",
  "source": "docgen",
  "call": "generate_document",
  "arg": "data",
  "formats": ["pdf", "pptx", "docx"],
  "templateS3Uri": "s3://amzn-s3-demo-bucket/templates/reports/",
  "outputPrefix": "s3://amzn-s3-demo-bucket/outputs/reports/",
  "toolSchema": [{
    "name": "generate_document",
    "description": "Write a PDF, PPTX or DOCX from data. Returns its s3:// URI and a download link.",
    "properties": {
      "format":   { "type": "string", "required": true, "description": "pdf, pptx or docx: one of this tool's formats." },
      "data":     { "type": "object", "required": true, "description": "The content as JSON." },
      "template": { "type": "string", "required": false, "description": "With a template folder: the template's file name in it." },
      "fileName": { "type": "string", "required": false, "description": "The file's name. Defaults to the data's title." },
      "runId":    { "type": "string", "required": false, "description": "Set by the workflow on every call; leave it out." },
      "agent":    { "type": "string", "required": false, "description": "Set by the workflow on every call; leave it out." }
    }
  }]
}
```

The function publishes one tool, `generate_document`, and the checks refuse any other name
in `toolSchema`. It returns `fileName`, `format`, `bytes`, `s3Uri`, `url` (a presigned link),
`expiresInSeconds` (3600), `runFile` (its key in the run's Files) and `template` (the one
used, or `built-in layout`). A call that is refused comes back as an `error` result.
`runId` and `agent` are set by the runtime on every call it makes through
`app/features/gateway/client.py` (the calls of every agent but a harness), from the run it
serves, whatever the model sent, so a model cannot write into another run's folder. The
Gateway passes the function only the arguments the schema declares, so both checks refuse a
`generate_document` that does not declare `runId` and `agent`. The runtime also sets
`attempt`, this run of the step (a gate's revise, a `rerun_from` or a re-run of part of a
stage is a new one); the deploy adds it to the published schema itself, so the config does
not declare it. A harness agent calls the
Gateway itself and no run is set on its calls, so both checks refuse a Document generator in
a harness agent's `tool`.

| Key | Notes |
|---|---|
| `formats` | Required. Any of `pdf`, `pptx`, `docx` (`docgenFormats` in `app/vocabulary.json`), each once. A call for another format is refused. |
| `templateS3Uri` | Optional. One template, `s3://<bucket>/<key>.pptx` or `.docx` (used for that format; its type must be in `formats`), or a folder of them, `s3://<bucket>/<prefix>/`, from which a call picks one by name (`template`, e.g. `report.docx`; no `..`, `.` or empty part). Left out, and for PDF always: the built-in layout. Also `${storage}/...` (the build's storage). |
| `outputPrefix` | Optional. `s3://<bucket>/<prefix>/`, ending in `/`. Each file is also written to `<prefix><run>/<name>`, and the link is to that copy. Left out: the run's Files only. Also `${storage}/.../`. |

**Where each file goes.** Always with its run: `runs/<run>/outputs/<name>` in the
deployment's assets bucket, on the run's **Files** tab, so a build with a Document generator
must set `orchestrator.files` (the validators, the CDK synth and the Terraform plan say so).
`<name>` is `fileName`, else the data's `title`, else the tool's key (else the agent's id),
reduced like any run file and given the format's extension. A file never replaces another
call's: each name is written with a conditional `PutObject` (`If-None-Match: *`), and when
the name is taken in that run (by another step, or by an earlier or parallel call of the same
attempt) the call takes `<stem>-2`, `<stem>-3` and so on. A step's new attempt replaces the
file its earlier attempt wrote under the name (read with `HeadObject`, written with
`If-Match`), so after a revise or a re-run the run keeps, and a deliver step sends, only the
new document. The name it got is the `fileName` it returns.

**Template rules.**

- A field is `{{name}}`, or `{{a.b}}` for a value inside an object; spaces inside the braces
  are allowed. Text is filled in as it is, a number or true/false as written, a list of plain
  values joined with `, `, anything else as JSON. A field `data` does not have renders as
  nothing.
- A list repeats what uses it. `{{rows.name}}`, where `data.rows` is a list, makes one table
  row (DOCX or PPTX) per item for a row that uses it, one paragraph per item for a DOCX
  paragraph outside a table, and one slide per item for a PPTX slide whose text outside its
  tables uses it. Each copy reads `{{rows.<field>}}` from its own item (a plain value is
  `{{rows.value}}`). An empty list removes the row, paragraph or slide. One list per row,
  paragraph or slide (the first it names), at most 1,000 items; a list inside an item is not
  repeated again.
- A paragraph is filled as a whole, so a field Word or PowerPoint split over several runs
  still works; the filled text takes the formatting of the paragraph's first run. DOCX headers
  and footers are filled too.
- PDF has no template format: it always uses the built-in layout.

**The built-in layout** (no template): `title` (and `subtitle`) as the heading, then each
other top-level field as a section, in order: a plain value as a line, an object as
`key: value` lines, a list of plain values as bullets, a list of objects as a table (the first
8 keys as columns; on PPTX, 12 rows a slide).

**What its role may do.** Read only the template object, or the objects under the template
folder; write only run outputs in the assets bucket and objects under `outputPrefix`; read
back only the copy it links to (a presigned link works only if its signer may read the
object); write its own log group. No `s3:ListBucket`, no other bucket, table or secret. A
template or output bucket encrypted with your own KMS key also needs that key's policy to
allow the function's role. The framework's own buckets are refused.

**Limits, held in the function's code** (`handler.py`): a 20 MB template, 1 MB of `data` as
JSON, a 25 MB file, 60 seconds and 512 MB. The link works for an hour at most: a link signed
with a role's temporary credentials stops working when they expire
([presigned URLs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html)).

**Its libraries.** `app/tools/docgen/requirements.txt` pins `python-pptx` (MIT),
`python-docx` (MIT), `reportlab` (BSD) and their dependencies `lxml` (BSD-3-Clause), `Pillow`
(MIT-CMU), `XlsxWriter` (BSD-2-Clause), `typing_extensions` (PSF-2.0) and
`charset-normalizer` (MIT). A console deploy installs them into the folder for Lambda's Python
3.12 on x86_64 before the zip is made (about 16 MB zipped, 43 MB unpacked, inside the
[Lambda .zip limits](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html),
so no layer or container image). Deploying from a checkout, install them first:
`python3 -m pip install -r app/tools/docgen/requirements.txt -t app/tools/docgen --platform manylinux2014_x86_64 --implementation cp --python-version 3.12 --only-binary=:all:`
(the CDK synth and the Terraform plan refuse to deploy without them).

**Test it.** The tool panel's **Test run with sample data** calls the deployed function once
with an editable sample event, for a run named `console-test`, and shows the file's link.
<!-- end Track B docgen -->

### `rowFields` — for an agent that computes rather than narrates

Optional, and only meaningful on a tool whose results are structured. It maps the
role an agent needs to the field name **your** payload uses:

```json
"rowFields": { "service": "service", "dimension": "dimension",
               "price": "pricePerUnit", "unit": "unit", "currency": "currency" }
```

An agent then calls `ctx.call_tool_rows(...)` and reads `row[fields["price"]]`
instead of parsing a rendered sentence. This is what keeps "a Lambda can return
anything" true: `cost_research` works unchanged against a payload of
`service/dimension/pricePerUnit` or one of `svc/dim/rate`, because the field names
live in config and not in the agent. Regex-parsing the tool's prose would have tied
the agent to one payload shape.

Omit it and nothing changes — agents that hand evidence to a model use the `text`
each row also carries.

## `skills`

A skill is know-how, not a data source: a SKILL.md (when to use it, then the steps) with
optional text reference files. An agent may use the ones it lists in `skills`.

```json
"skills": {
  "refundPolicy": {
    "description": "When a customer asks for money back.",
    "instructions": "1. Check the purchase date in the order.\n2. Within 30 days with a receipt: refund. Otherwise offer store credit.\n3. Quote policy.md when you decline.",
    "files": { "policy.md": "Refunds need a receipt and are paid to the original card." }
  }
},
"agents": { "support": { "name": "Support", "skills": ["refundPolicy"], "toolMode": "model", "tool": "orders" } }
```

| Key | Notes |
|---|---|
| `description` | Required. What the agent reads to decide whether to open the skill. |
| `instructions` | Required. The SKILL.md body, in Markdown. |
| `files` | Reference files by name (`.md`, `.txt`, `.json`, `.csv`, `.yaml`, `.yml`), opened only when needed. Up to 20 files and 200 KB of text per skill, instructions included. |
| `registry` | Where it came from in an AWS Agent Registry (`registryId`, `recordId`, `name`, `version`, `sync`). Set by Add from registry or the Assistant, never by hand. With `sync: true` the console refreshes it to the newest approved version when the build opens and before each deploy. The runtime ignores it. |

How an agent gets them (`app/common/skills.py`):

- **`toolMode: "model"` with tools**: the tool-calling turn sees each skill's name and
  description and opens what it needs with a `use_skill` tool (its reference files too). That is
  not a tool call: it uses none of `maxToolCalls` and is never evidence. Every model call after
  that carries the skills it opened in full and the rest by name only.
- **otherwise**: its skills are added to its instructions in full, reference files included, up
  to 60,000 characters.

Scripts are never run. In the Builder: the Skills tab (write one, or import a SKILL.md), each
agent's Skills setting, and the library (publish, share, add from library), like other parts.

## Named blocks: `guardrails`, `memories`, `evaluators`, `identities`, `policies`

Optional. Each is a map of name (letters and digits, starting with a letter, up to 32) to an
entry, defined once and used by name. The Builder shows each on its own tab and in the library.

| Block | Entry keys | Becomes | Used by |
|---|---|---|---|
| `guardrails.<name>` | the keys of [`guardrail`](#guardrail) | its own Bedrock Guardrail | `agentcore.guardrails.use` |
| `memories.<name>` | `description`, `strategies` (`semantic`, `summary`, `userPreference`, `episodic`), `expiryDays` (3–365), `scope` | its own AgentCore Memory | `agentcore.memory.use` |
| `evaluators.<name>` | `description`, `instructions` (may use `{context}` and `{assistant_turn}`), `model` (judge; default the workflow's), `scale` (2–20 `{value, label, definition}` points; default five from 0 to 1) | an AgentCore evaluator | `Custom.<name>` in `agentcore.evaluations.evaluators` |
| `identities.<name>` | `type` (`oauth2` client credentials or `apikey`), `description`, `clientId`, `scopes`, `discoveryUrl` or `tokenUrl`, `issuer` | an AgentCore Identity credential provider | a tool's `identity`; `agentcore.identity.outbound`; `harness.apiKey` |
| `policies.<name>` | `description`, `statement` (one `permit(...)` or `forbid(...)`) | one Cedar policy per tool it is attached to (with `{{tool}}`), or one as written | a tool's `policies` |

Secrets (an OAuth client secret, an API key) are never here: they go in the build's secrets or
`IDENTITY_SECRETS` / `TF_VAR_identity_secrets`. A Cedar statement's resource is always
`AgentCore::Gateway::"{{gateway}}"`, and conditions read `context.input.<argument>` only.

## `authorization`

Answers "may *this user* do this?", which the JWT authorizer does not. Twelve actions:
`start` (begin a run), `decision` (approve / revise / deny a gate, answer an agent's question or a
tool approval), `rerun`, `cancel`, `evaluate`, `insights`, `delete`; on a Builder console `deploy`
and `destroy`; `audit` (everyone's activity), `admin` (read every build and run, Analytics, Memory
records, Live config, Triggers page, registry publish) and `override` (pick a run's models). `audit`,
`admin` and `override` are closed unless granted, and `insights` whenever anything is restricted.

- An action **not listed** is unrestricted, so deleting the block restores the
  pre-RBAC behaviour where any authenticated user could do anything.
- An action listed **with groups** needs at least one of them.
- An action listed with an **empty list** is denied to everyone — that is how you
  switch a capability off.

`groupsClaim` is the JWT claim holding the caller's groups: `cognito:groups` for
Cognito, or a namespaced custom claim for Auth0 (e.g. `https://your-app/roles`,
added by a post-login Action) — Auth0 will not emit an unnamespaced one. Okta: `groups` (a
groups claim on the authorization server). Entra ID: `roles` (app roles; its `groups` claim holds
object IDs, not names).

`signIn` says who signs people in: `{"provider": "okta", "domain": "acme.okta.com", "clientId":
"…"}`, `{"provider": "entra", "tenantId": "<GUID>", "clientId": "…"}`, `{"provider": "auth0",
"domain": "…", "clientId": "…"}`, or nothing for built-in Cognito. A Builder console deploys a
build with it; a deployment of your own sets the same in its `idp` setting, and plan/synth stops
when the two disagree. `authorizationServer` (Okta) defaults to `default`.

Both IaC paths create a Cognito group for every group named here. *Membership* is
not managed in IaC: it is per-person and changes far more often than a deploy.

Enforced in `bff/authz.py` on the mutating routes **and** applied to the in-app
assistant's action tools, so "ask the chatbot to approve it" is not a way around it.
Read-only endpoints are not gated. A non-empty `actions` map with `idp = "none"` is
rejected at plan/synth time — with no authorizer there are no claims, so every rule
would deny everyone.

A build created in the Builder starts closed: run actions for `members`, `insights`, `audit`
and `admin` for `admins`. Its deploy puts the owner in every group, so the owner can do
everything and anyone else signs in with nothing until added.

## `guardrail`

The Bedrock Guardrail policy. Agents opt in per agent via
`agentcore.guardrails.{input,output}`; this block defines *what* is enforced.

| Key | Notes |
|---|---|
| `contentFilters` | Filter type → strength (`NONE`/`LOW`/`MEDIUM`/`HIGH`), applied to input and output. `PROMPT_ATTACK` is input-only, so its output strength is forced to `NONE`. |
| `deniedWords` | Exact custom terms. `BLOCKED_DEMO_TERM` is a deterministic string for testing that blocking works end to end — **replace it**. |
| `managedWordLists` | e.g. `PROFANITY`. |
| `deniedTopics` | Each needs a `name` (1–100 letters, digits, spaces, `- _ ! ? .`) and a `definition` (≤ 200 characters; Bedrock matches on it); `examples` optional, ≤ 5 of ≤ 100 characters. |
| `piiEntities` | Bedrock PII entity type → `BLOCK` or `ANONYMIZE`. `ANONYMIZE` masks the text and the run continues with it; `BLOCK` stops the step and the log names the rule. |
| `blockedInputMessage` / `blockedOutputMessage` | What the user sees when something is blocked. |

Omit any key to drop that policy; omit the whole block for an empty guardrail.

## `steps`

Each entry is one of:

- `{ "agent": "id" }` — a single agent
- `{ "parallel": ["a", "b"] }` — fan out, join at one gate
- `{ "sequence": ["a", "b"] }` — chain in order

Add `"hitl": true` for a human-review gate after the step (or an object: `mode` `always` / `threshold` / `auto`, `when` rules for a threshold, `approval` `console` / `event`, `timeout` `{"after": "24h", "action": "approve"|"deny"}`; see app/common/gates.py), and `gateId` / `gateName`
to name it (the id is how the BFF and UI address the gate). Add `branch` to let the
step's own output choose what runs next — see below.

Those five are the whole key set: `agent` / `parallel` / `sequence` (pick one),
`hitl`, `gateId`, `gateName`, `branch`.

Revise behaviour differs by shape, and the difference is the point: a **parallel**
gate re-runs only the agents the reviewer flags; a **sequence** gate re-runs the
whole chain from its first agent, because a later member's output depends on the
earlier ones.

Reorder, add or remove steps freely — the graph, the UI diagram and the IaC all
follow. The parallel stage renders two agents per row.

### `branch` — the output decides what runs next

`steps` is a fixed pipeline and `hitl` lets a *human* redirect it. `branch` is the
third case: the step's own output picks the next step. A triage agent sends a
high-risk case to a deep review and a routine one straight to settlement; an intake
agent ends the run when the request is not actionable. No agent code — the deciding
agent just returns its normal output.

```json
{ "agent": "triage",
  "branch": {
    "when": [
      { "field": "disposition", "equals": "escalate", "goto": "investigator" },
      { "field": "amount",      "gte": 10000,        "goto": "investigator" },
      { "field": "claimId",     "exists": false,     "goto": "END" }
    ],
    "default": "adjuster"
  } }
```

| Key | Meaning |
|---|---|
| `when` | rules, tried **in order**; the first match wins, so put the specific case first |
| `when[].field` | dot path into the agent's JSON output (`scope.tier`, `findings.0.claim` — a numeric segment indexes a list). **Omit it** and the comparison runs against the raw output text, so an agent that returns prose is still branchable |
| `when[].goto` | the step to run on a match, or `"END"` to finish the run |
| `default` | where to go when no rule matched. Omit it and the run simply continues to the next step |

A `default` **with no `when`** is an unconditional jump. That is what makes two
paths exclusive rather than merely optional: with steps
`[triage, investigator, adjuster, settlement]`, `triage` picks a specialist and
`investigator` carries `{ "default": "settlement" }` so the escalated path does not
fall into the adjuster's step on its way out.

**Operators.** One or more per rule; several are ANDed.

| | |
|---|---|
| `equals`, `notEquals` | single value |
| `in` | list of alternatives |
| `contains` | substring of a string, or membership of a list / dict keys |
| `exists` | `true` = present and not null/`""`/`[]`/`{}`; `false` = the negation |
| `gt`, `gte`, `lt`, `lte` | numeric. A **list or dict compares by its length**, so `{ "field": "openQuestions", "gt": 0 }` reads as "there is at least one" |

`equals`, `notEquals`, `in` and `contains` compare on stripped, case-folded text,
because the value was written by a model: one told to return `escalate` will
sometimes return `Escalate`. A branch that took the default because of a capital
letter would be an expensive thing to debug.

**Targets name a STEP, not an agent inside one** — a single-agent step's `agent` id,
or a group step's `gateId`. Jumping to a `parallel` stage therefore enters the whole
stage rather than stranding its gate on siblings that never ran. Targets must be
*later* steps; going backwards is what a review gate's `revise` is for.

**Where it may go.** On a single-agent step, or on a `sequence` step (its **last**
agent decides). Not on a `parallel` step — a group has no single agent whose output
decides — and not on the last step, which has nowhere to route.

**What you see.** The stage gets a `⑂ Branch` pill in the diagram (hover for the
rules), the timeline records the rule that matched and where it went, and the agents
the run bypassed are marked **skipped** rather than left looking queued. With a
review gate on the same step the human approves first, then the branch reads the
output they approved.

**The branch this sample ships**, on its intake step — two guards that fire only on a
degenerate brief, so the nine-agent demo is unchanged:

```json
{ "agent": "intake", "hitl": true,
  "branch": {
    "when": [
      { "field": "objective",    "exists": false, "goto": "END" },
      { "field": "keyQuestions", "lt": 1,         "goto": "analysis_reco" }
    ]
  } }
```

No objective in the brief and there is nothing downstream can work with, so end the
run rather than spend seven more agents. No research questions and there is nothing
for the research stage to gather, so jump to `analysis_reco` — the `gateId` of the
sequence step — and synthesize over the brief alone. On a normal request neither
matches and the run continues to the research stage, with one timeline line saying so.

Note what the second rule teaches: `keyQuestions` is a **list**, and the numeric
operators compare a collection by its **length**, so `lt: 1` means "empty". A field
that is *absent* is different again — it matches only `exists: false`, because every
other operator needs a value to compare.

Everything above is checked before anything deploys, because each of these mistakes
is otherwise silent — a misspelled operator, a rule with no comparison and a target
that names nothing all evaluate to "no match", so the run quietly takes the default
on every request and the branch looks like it is working.

<!-- begin v139 Track D (decide) -->
### Named options — a person's choice decides what runs next

A review gate can offer **named options** instead of approve / revise / deny, and an
agent can **request a decision** in the middle of its step. Either way the chosen option
becomes the run field `decision`, which the step's `branch.when` routes on.

```json
{ "agent": "check", "hitl": { "options": ["Override", "Abort"] },
  "branch": { "when": [
      { "field": "decision", "equals": "Override", "goto": "deliver" },
      { "field": "decision", "equals": "Abort",    "goto": "END" } ],
    "default": "deliver" } }
```

| Key | Read by | What it does |
|---|---|---|
| `steps[].hitl.options` | `app/common/gates.py`, `nodes.py` gate nodes; `bff/asks.py` | 2–6 labels (each at most 60 characters, unique ignoring case). The run page shows one button per option and an optional comment; the run then continues and the label is the run field `decision`. Single-agent and `sequence` steps only (a parallel stage cannot branch), and not with `approval: "event"` (an event cannot choose). |
| `agents.<id>.askPerson.options` | `app/common/asks.py` `request_decision` → `tool_loop.py` | 2–6 labels. With `toolMode: "model"`, the agent's model is also offered `request_decision(question)`: the run waits, the run page shows the question with one button per option, and the model reads the choice as the tool's result. Counts against `maxQuestions`; `timeout` lets it lapse. The choice is the run field `decision` for a branch on the agent's step. |

- `field: "decision"` reads the choice made at the step's gate first, then the one the
  step's deciding agent requested (a sequence decides with its last agent). With no
  choice it reads the agent's output as before, so a gate without `options` is unchanged.
- Compared like every branch string: case- and space-insensitive. A rule whose
  `equals` / `in` names no option is warned about, as is a gate with options and no rule
  that reads `decision`.
- No option is chosen when the gate approves itself (`mode` `auto`, a `threshold` with no
  match, an unattended run) or times out with `"approve"`: the branch takes its `default`.
  A timeout with `"deny"` stops the run. So "deliver if the check passes, otherwise ask to
  override or abort" is a threshold gate on the failed check, whose `default` is the deliver step:

  ```json
  { "agent": "check",
    "hitl": { "mode": "threshold", "when": [{ "field": "passed", "equals": false }],
              "options": ["Override", "Abort"] },
    "branch": { "when": [{ "field": "decision", "equals": "Abort", "goto": "END" }],
                "default": "deliver" } }
  ```
- A deliver target's `when` does not read `decision` (its `field` is `request` or
  `outputs.<step>.<field>`): route with the branch and put the deliver step after it.
- Answered through the decision route: `POST /api/sessions/{id}/decision
  {"option": "Override", "comment": "..."}`. The activity log records the option, who
  chose it and when (`run.decided`), and the timeline shows the choice with its comment.
- The Run Assistant does not choose an option: a person does, on the run page.
<!-- end v139 Track D (decide) -->

## What is checked before anything deploys

Both IaC paths validate this file at plan/synth time and name the exact problem.
See §7 of [`GETTING_STARTED.md`](../../GETTING_STARTED.md) for the full list. The
corpora and `lambda` schema checks matter most: those are the ones whose failure mode
is a clean deploy that retrieves nothing.
