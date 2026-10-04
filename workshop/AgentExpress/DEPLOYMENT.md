# Deployment Guide

How to deploy and operate the orchestrator. Overview: [README.md](README.md). Tutorial (tool
types, agents, the Builder): [GETTING_STARTED.md](GETTING_STARTED.md). Internals:
[ARCHITECTURE.md](ARCHITECTURE.md).

Two IaC paths deploy the same resources. Use one per account and region (names overlap); different
accounts or regions are fine.

| | Terraform (`orchestrator/terraform/`) | CDK (`orchestrator/cdk/`) |
|---|---|---|
| AgentCore capabilities | 9 of 9 | 9 of 9 |
| Tool plane (Gateway, KB, Cedar) | `enable_gateway = true` (default) | `-c enableGateway=true` (default `false`) |
| Container image | Per-stack ECR repository | `DockerImageAsset` (bootstrap ECR repo) |
| State | S3 with native locking | CloudFormation |
| Deploy permissions | `deploy-role-policy.json` | CDK bootstrap roles |

The Builder control plane is on by default: a `<agent>_builds` DynamoDB table, a versioned builds
bucket, and a CodeBuild project `<agent>-deploy` that deploys each build as its own stack. It can
create IAM roles, so starting it is gated by the `deploy` and `destroy` actions in
`authorization`. Leave it out with `enable_builder = false` / `-c builder=false`.

### What a deploy creates

Both paths build the same ARM64 image and read `orchestrator/app/workflow.json` as the single
source of truth. They create:

- The orchestrator runtime plus one runtime per `dedicated` agent; two AgentCore Memory resources
  (checkpointer, long-term semantic/summary store); status, events, telemetry and insights
  tables; CloudWatch Transaction Search (idempotent)
- The activity log (`<agent>_audit`, or `<agent>_builds` with the Builder on): sign-ins on a pool
  the stack creates, who started, decided, cancelled, re-ran or deleted a run, and how each run
  ended, written by the runtime (`AUDIT_TABLE`, IAM sid `RunOutcomesToActivityLog`)
- Bedrock Guardrails, only when they enforce something (see [Tuning](#tuning))
- With the tool plane: the Gateway, its Cedar policy engine, the Knowledge Base and an ingestion job
- Only when needed: the A2A stand-in Lambda (an agent with `source: "a2a_lambda"`); an assets
  bucket (an agent with `output: "image"`). Agents with `vision` get `s3:GetObject` on `runs/*`
  only when they read images
- The BFF Lambda behind an HTTP API, with one API-wide invoke permission instead of one per route
  (Lambda caps a resource policy at 20 KB)
- The in-app assistant, on unless `orchestrator.chatbot.enabled = false`

## Prerequisites

- A container engine that builds `linux/arm64`: Finch, Docker or Podman
- Node.js 20+ with npm. The UI is built during `terraform apply` / `cdk synth`
  (`npm ci && npm run build` in `orchestrator/web`), so a TypeScript error fails the deploy.
  CDK builds it in a `node:22-alpine` container when available, locally otherwise
- AWS CLI with credentials for the target account (`aws sts get-caller-identity`)
- Bedrock model access for the model (`model_id` / `modelId`, default Claude Haiku 4.5), for
  Claude Sonnet 5.5 on a Builder console (the Assistant), and,
  with the tool plane, the embedding model (default Titan Text Embeddings V2, overridable with
  `embeddingModel` on the `kb` tool). Deploys and KB ingestion fail without both
- Terraform 1.10+ (S3 native locking, `use_lockfile`), or the AWS CDK CLI (`npm i -g aws-cdk`)

## Identity provider

`idp` selects the provider for both auth boundaries: end-user login plus the JWT authorizer on
`/api/*`, and the machine-to-machine (M2M) token agents use to call the Gateway. Terraform derives
everything provider-specific in `terraform/identity.tf`. A missing required value fails plan or
synth with the exact name, so nothing half-configured reaches AWS.

| Option | Terraform (`terraform.tfvars`) | CDK |
|---|---|---|
| Cognito, created (recommended) | `idp = "cognito"`, `cognito = { create = true }` | `-c idp=cognito -c createCognito=true` |
| Cognito, your pool | `cognito = { create = false, user_pool_id, client_id, domain_prefix }` | `-c cognitoUserPoolId=… -c cognitoClientId=… -c cognitoDomainPrefix=…` |
| Auth0 | `idp = "auth0"`, `auth0 = { domain, client_id }` | `-c idp=auth0 -c auth0Domain=… -c auth0ClientId=…` |
| None (sandbox) | `idp = "none"`, `allow_unauthenticated = true`, `enable_gateway = false` | `-c idp=none -c allowUnauthenticated=true` |

- Cognito, created: the stack creates the User Pool, Hosted UI domain
  (`<agent-name>-<account-id>`), public SPA client and, with the tool plane, the Resource Server
  and confidential M2M client. Callback and sign-out URLs are wired to CloudFront
- Cognito, your pool: create the pool, a Hosted UI domain and a public App Client (no secret,
  authorization code grant, scopes `openid email profile`); with the tool plane, also a Resource
  Server with a custom scope (e.g. `gateway/invoke`) and a confidential App Client for it. Create
  the `authorization` groups yourself; stacks only create them in a pool they create
- Auth0: nothing is created in the tenant. Create a Single Page Application (domain without
  scheme or trailing slash) and, with the tool plane, an API (its Identifier is the audience) plus
  a Machine to Machine application. Set `authorization.groupsClaim` ([RBAC](#users-groups-and-rbac))
- Your pool or Auth0: after the first deploy, add the UI URL to the client's allowed callback and
  sign-out/logout URLs (Auth0: also Allowed Web Origins)
- None: the UI and `/api/*` are open to anyone with the URL. The tool plane is not allowed (the
  Gateway's `CUSTOM_JWT` authorizer needs an OIDC provider), so tool-bound agents, including the
  five research agents, fail with `ToolUnavailable`. Also remove `authorization.actions`: with no
  JWT claims every group check denies everyone. Both paths reject either combination

### M2M identity (agent to Gateway)

Needed with the tool plane unless Cognito is created for you. Terraform:
`gateway_identity = { client_id = "...", audience = "gateway/invoke" }`; CDK:
`-c gatewayClientId=... -c gatewayAudience=...`. The audience is the OAuth2 scope on Cognito and
the API Identifier on Auth0. The client secret goes in the environment only
(`TF_VAR_gateway_client_secret` / `GATEWAY_CLIENT_SECRET`).

| | Cognito M2M token | Auth0 M2M token |
|---|---|---|
| Claims present | `client_id` + `scope`, no `aud` | `aud` + `azp`, no `client_id` |
| Gateway pins on | `allowed_clients` | `allowed_audience` + an `azp` custom claim |
| Token request | HTTP Basic + `scope` | form body + `audience` |
| Token endpoint | `/oauth2/token` | `/oauth/token` |

## IAM to deploy

Terraform: attach `orchestrator/terraform/deploy-role-policy.json` to the deploying role or user.
Run history is kept unless the runtime has `RUN_TTL_DAYS` set (the status and events tables
expire rows by TTL). Runtime dependencies are pinned in `requirements.txt` and, transitively, in
`requirements.lock`; regenerate the lock after changing a pin (the steps are in its header).
It covers Cognito, ECR, AgentCore (runtimes, memories, gateway, policy engines, evaluators),
Bedrock KB, S3 Vectors, guardrails, DynamoDB, Lambda, API Gateway, S3, CloudFront, IAM execution
roles, logs, AgentCore Identity secrets, the Builder's CodeBuild project (`BuilderPlane`) and the
X-Ray calls for Transaction Search. `EvaluatorJudgeModel` allows `bedrock:InvokeModel` on
foundation models and inference profiles: creating a custom evaluator checks the caller can
invoke its judge model. CDK uses the bootstrap execution role instead; there is no policy file.

## Deploy with Terraform

All commands run from `orchestrator/terraform/`.

1. Remote state, once per account:

   ```bash
   ./bootstrap-state.sh
   # Bucket: agentcore-multiagent-orchestrator-tfstate-<account-id>
   # Key:    orchestrator/terraform.tfstate
   ```

   Creates a versioned, encrypted, private bucket, writes `backend.hcl` (git-ignored) and runs
   `terraform init`. Safe to re-run; local state is migrated after a timestamped backup.
   Overrides: `AWS_REGION`, `STATE_NAME`. Solo trial: comment out `backend "s3"` in
   `versions.tf` and run `terraform init -migrate-state`.

2. Configure: `cp terraform.tfvars.example terraform.tfvars`, then edit:

   ```hcl
   region           = "us-west-2"
   idp              = "cognito"
   cognito          = { create = true }
   enable_gateway   = true        # the default; the research agents need it
   container_engine = "finch"     # default "docker"; also "podman"
   ```

   Export any [secrets](#secrets), e.g. `TF_VAR_gateway_client_secret` for your own M2M client.

3. Apply:

   ```bash
   terraform apply -target=null_resource.ui_build   # first apply of a NEW stack only
   terraform apply
   ```

   The targeted apply builds the UI once; until then the file list `ui.tf` uploads is unknown and
   a fresh stack's plan is refused. Later applies are one command. Allow several minutes.

4. Outputs (`terraform output`): `ui_url` (the app on CloudFront), `api_endpoint` (HTTP API),
   `cognito_user_pool_id` (with `idp = "cognito"`), `knowledge_base_id` (with a `kb` tool).

## Deploy with CDK

More detail: [orchestrator/cdk/README.md](orchestrator/cdk/README.md).

```bash
cd orchestrator/cdk
npm install
export CDK_DOCKER=finch        # only without Docker; works for asset bundling too
cdk bootstrap                  # once per account/region
cdk deploy -c idp=cognito -c createCognito=true -c enableGateway=true
```

A bare `cdk deploy` fails at synth: `idp` defaults to `cognito`, which needs `createCognito=true` or
the three `cognito*` ids. Without `enableGateway=true` there is no Gateway, KB or policy engine and
tool-bound agents fail with `ToolUnavailable`; inference, guardrails, memory, evaluations and
telemetry still work.

| Context flag | Default | Purpose |
|---|---|---|
| `agentName` | `multiagent_orchestrator` | Runtime name; stack is `<agentName with dashes>-stack` |
| `region` | `us-east-1` | Target region; wins over `CDK_DEFAULT_REGION` |
| `modelId` | empty | Default model; empty uses the workflow's `orchestrator.defaultModel`, else Claude Haiku 4.5. Its `us.`/`eu.`/`apac.` prefix follows the region |
| `designerModel`, `designerSummaryModel` | empty | The Assistant's models, a list tried in order (`model[:effort],…`; empty is Claude Sonnet 5.5, then Claude Sonnet 5 if the account is refused 5.5), and its summary model (the default model) |
| `designerEffort` | `medium` | The Assistant's reasoning effort: `low`, `medium`, `high` or `model` |
| `idp`, `createCognito`, … | `cognito`, `false` | See [Identity provider](#identity-provider) |
| `selfSignUp`, `selfSignUpGroup` | `false`, `members` | Open registration |
| `enableGateway` | `false` | Tool plane |
| `memoryEventExpiryDays` | `30` | Short-term memory retention |
| `transactionSearchIndexingPercentage` | `100` | Span indexing (1% is free) |
| `builder` | `true` | Builder control plane |
| `consoleMode` | `app` | `builder` for a [Builder console](#deploy-a-builder-console) |
| `tags` | | JSON object of extra tags |

Outputs: `uiUrl`, `apiEndpoint`, `idp`, `authEnabled`, `consoleMode`; `cognitoUserPoolId` and
`cognitoClientId` with `createCognito=true`; `agentRuntimeArn`, `memoryId`, `dedicatedAgents`
unless `consoleMode=builder`; `gatewayUrl`, `knowledgeBaseId` with the tool plane.

## Users, groups and RBAC

Skip for `idp = none`; on Auth0, create users in your tenant. The Cognito pool is admin-create-only
by default (self sign-up on a public URL lets anyone spend your Bedrock budget). The stack creates
one group per group named in `authorization.actions`, but membership is not in IaC, and a user in
no group sees Approve/Revise/Deny greyed out. Create a user, add groups, then sign out and back in
(groups ride in the ID token):

```bash
POOL=$(terraform output -raw cognito_user_pool_id)   # CDK: the cognitoUserPoolId output
aws cognito-idp admin-create-user --user-pool-id "$POOL" \
  --username you@example.com \
  --user-attributes Name=email,Value=you@example.com Name=email_verified,Value=true \
  --message-action SUPPRESS
aws cognito-idp admin-set-user-password --user-pool-id "$POOL" \
  --username you@example.com --password '<a-strong-password>' --permanent
for G in approvers operators; do
  aws cognito-idp admin-add-user-to-group --user-pool-id "$POOL" \
    --username you@example.com --group-name "$G"
done
```

Self sign-up: `cognito = { self_signup = true }` / `-c selfSignUp=true`. New users verify their
email and join `members` (`self_signup_group` / `-c selfSignUpGroup=`). That group must be named in
`authorization.actions` or the deploy fails. Cognito's built-in email sender allows only a few
dozen messages a day; configure SES before real traffic.

Shipped mapping in `orchestrator/app/workflow.json`:

| Action | Meaning | Groups |
|---|---|---|
| `start` | start a run | unrestricted in the sample |
| `decision` | approve / revise / deny a review gate | `approvers`, `members` |
| `rerun` | re-run an agent and everything downstream | `approvers`, `members` |
| `cancel` | stop a running workflow | `approvers`, `operators`, `members` |
| `evaluate` | run an AgentCore evaluation on demand | `operators`, `members` |
| `insights` | cross-run Insights analysis (spans every user's runs) | `admins` |
| `delete` | permanently remove a run and its timeline | `operators`, `members` |
| `deploy` | deploy a build with the Builder's deploy project | `operators`, `members` |
| `destroy` | destroy a deployed build | `operators`, `members` |
| `audit` | see every user's activity | `admins` |
| `admin` | read every user's builds and runs; destroy any build | `admins` |

- Builds and runs are private to their creator; `members` acts only on its own
- An unlisted action is unrestricted; an action with an empty group list is denied to everyone
- `admin` and `audit` are closed unless granted, and so is `insights` whenever the block restricts
  anything. A deploy refuses a self sign-up group granted any of the three. Add `admins` from the
  backend only
- Full semantics: `authorization` in
  [orchestrator/docs/WORKFLOW_REFERENCE.md](orchestrator/docs/WORKFLOW_REFERENCE.md)

Check what the app thinks you can do (a denied action returns 403 naming the required group):

```bash
curl -s -H "Authorization: Bearer $ID_TOKEN" "$(terraform output -raw api_endpoint)/api/me"
# {"user":"you@example.com","groups":["approvers","operators"],
#  "permittedActions":["start","decision","rerun","cancel","evaluate","delete","deploy","destroy"], ...}
```

Auth0: set `authorization.groupsClaim` to a namespaced custom claim (e.g. `https://your-app/roles`)
emitted by a post-login Action. Auth0 will not issue an unnamespaced claim, so the default
`cognito:groups` finds nothing and every gated action is denied.

## Using it

Open the UI URL, sign in and start a run. The walkthrough of gates, observability, evaluations,
re-runs and the assistant is in [GETTING_STARTED.md](GETTING_STARTED.md).

## Secrets

Tool and data-source types are covered in [GETTING_STARTED.md](GETTING_STARTED.md) and
[orchestrator/docs/WORKFLOW_REFERENCE.md](orchestrator/docs/WORKFLOW_REFERENCE.md). Secrets never go
in `workflow.json` or any file; pass them as JSON objects in the environment:

| Secret | Keyed by | Terraform | CDK |
|---|---|---|---|
| Tool API keys | tool name | `TF_VAR_tool_api_keys` | `TOOL_API_KEYS` |
| Identity secrets (OAuth client secret or API key of an `identities` entry) | identity name | `TF_VAR_identity_secrets` | `IDENTITY_SECRETS` |
| Bearer tokens for `runtime: "a2a"` agents with `auth: "bearer"` | agent id | `TF_VAR_a2a_tokens` | `A2A_TOKENS` |
| M2M Gateway client secret | | `TF_VAR_gateway_client_secret` | `GATEWAY_CLIENT_SECRET` |

- Example: `export TF_VAR_tool_api_keys='{"internalTools":"your-key"}'`
- A tool API key is vaulted in an AgentCore credential provider and sent by the Gateway as an
  `X-API-Key` header; the agent never sees it
- An identity an agent uses (`agentcore.identity.outbound`) becomes a credential provider
  `bedrock-agentcore-<agent-name>-id-<identity>`, plus one workload identity
  `<agent_name>-agents` the runtimes exchange for credentials. Plan/synth fails when an identity
  in use has no secret
- `orchestrator.gatewayIdentity: "perAgent"` (one Gateway client per tool-using agent) needs the
  tool plane and a Cognito pool the stack creates
- The Gateway client secrets and A2A tokens reach a runtime through its own Secrets Manager secret
  (`agentexpress/<agent_name>/runtime-*` on Terraform), never its environment variables: the
  runtime gets `RUNTIME_SECRET_ARN` and reads the secret at start (`app/common/runtime_secret.py`).
  A dedicated agent's secret holds only its own Gateway client secret
- On CDK, a value supplied at deploy time (an A2A token, a bring-your-own M2M secret) is still in
  the synthesized template; the Cognito clients the stack creates are not

## Deploy a Builder console

A `builder`-mode console only designs, builds and deploys; each build's runs, observability and
assistant live in the build's own app. It deploys no workflow plane (image, runtimes, memory,
guardrail, Gateway, KB, run tables, Transaction Search), the BFF refuses run routes, and the UI has
no Runs or Observability. `consoleMode=builder` with `builder=false` fails at synth/plan.

```bash
cd orchestrator/cdk
export CDK_DOCKER=finch        # only if you use Finch instead of Docker
npx cdk deploy -c agentName=agentexpress -c idp=cognito -c createCognito=true -c enableGateway=true -c selfSignUp=true -c consoleMode=builder --require-approval never
```

Terraform: set `agent_name = "agentexpress"`, `idp = "cognito"`,
`cognito = { create = true, self_signup = true }` and `console_mode = "builder"` (needs
`enable_builder = true`, the default), then run the two applies of
[step 3](#deploy-with-terraform). `enableGateway` / `enable_gateway` has no effect in this mode.
Self sign-up users join `members`, which may deploy and destroy their own builds.

### How a build deploys

Deploying a build starts the console's CodeBuild project (`<agent>-deploy`, ARM, 2-hour timeout)
over a fresh copy of the framework source. `deployer/runner.py` fetches the build's frozen version,
runs `scaffold.py apply --exact`, and deploys it as its own stack named from the build's `ax_<id>`
agent name, with the tool the user picked:

| | CDK | Terraform |
|---|---|---|
| Settings | `-c agentName=ax_<id> -c idp=cognito -c createCognito=true -c enableGateway=<workflow has tools> -c builder=false` | `agent_name = "ax_<id>"`, `idp = "cognito"`, `cognito = { create = true }`, `enable_gateway = <workflow has tools>`, `enable_builder = false` |
| Deploys as | CDK bootstrap roles | `deploy-role-policy.json` plus `ReadOnlyAccess` |
| State | CloudFormation | the console's builds bucket |

- Build secrets live in Secrets Manager under `agentexpress/<console agent name>/builds/<build id>`
  and are passed as the [Secrets](#secrets) variables
- After a deploy, the owner is invited into the build's app with a temporary password
- A CDK build in the console's own account needs that account and region CDK-bootstrapped; a
  connected account with no bootstrap is bootstrapped on its first deploy
- A CDK build stack whose first create failed (`ROLLBACK_COMPLETE`, `ROLLBACK_FAILED`,
  `DELETE_FAILED`) is deleted through the CDK deploy role and created again. A stack that ever
  deployed is never deleted this way

### Connected accounts

Builds can deploy into another account. The console gives the user a CloudFormation template for
a role `AgentExpressDeploy-<id>`, trusted only by the console's deploy project and BFF, with an
External ID unique to that user and account. It carries `ReadOnlyAccess`, `deploy-role-policy.json`
minus the console-only `BuilderPlane` statement, and what CDK needs to bootstrap (`cdk-hnb659fds-*`).
The role has no AdministratorAccess, but a CDK deploy runs as the bootstrap's CloudFormation
execution role, which is AdministratorAccess unless the account was bootstrapped with
`--cloudformation-execution-policies`; bootstrap it yourself with a narrower policy first if that
matters. Terraform builds run with the role's own permissions. Verifying assumes the role once and shows the STS reason on failure (no such
role, trust or External ID mismatch, an SCP). Terraform state stays in the console's account.

### Moving a build out of a console

Export the bundle from the Build view, apply it in a framework checkout, and deploy with the build
settings above (`builder=false` / `enable_builder = false`) and its secrets in the environment:

```bash
cd orchestrator
python3 scaffold.py apply <bundle>.json --exact
```

`--exact` mirrors the bundle: it also deletes agent folders, `app/tools/` folders and `kb_docs/`
corpora the bundle does not name.

## Tuning

Edit `orchestrator/app/workflow.json`, then re-deploy.

| Want to | Change |
|---|---|
| Toggle a capability for one agent | its `agentcore` block (`memory.longTerm`, `guardrails.input/output`, `evaluations.enabled/auto`, `policy.enabled`) |
| Stop automatic evaluation | `evaluations.auto: false` (the UI button still works) |
| Test Cedar rules without blocking | `orchestrator.policy.mode: "LOG_ONLY"` |
| Turn policy off | `orchestrator.policy.enabled: false` (no engine is created) |
| Hide or restrict the assistant | `orchestrator.chatbot.enabled: false`, or `chatbot.tools.*` (`rerun`, `review`, `runEval`) set to `false` |
| Move an agent to its own runtime | `"runtime": "dedicated"` |
| Add a data source or agent | see [GETTING_STARTED.md](GETTING_STARTED.md) |
| Reduce span-indexing cost | `transaction_search_indexing_percentage` / `-c transactionSearchIndexingPercentage` (1% is free) |

- Guardrails and Cedar policy are generated from `workflow.json`. The policy permits exactly the
  declared `tools`; the `kb` entry's `restrictTo` pins retrieval to its corpora
- The build-wide `guardrail` block creates a guardrail only when it enforces something (Bedrock
  rejects an empty one), so with an empty block `guardrails.input/output` is a no-op unless the
  agent names a guardrail. Each `guardrails.<name>` entry is its own guardrail, applied with
  `agentcore.guardrails.use`. With none, `bedrock:ApplyGuardrail` targets a placeholder ARN

## Tear down

With the Builder, destroy every deployed build from the console first: each build is its own stack,
and the builds bucket (holding Terraform builds' state) is deleted with the console.

```bash
terraform destroy                        # leaves Transaction Search and the state bucket
./teardown.sh                            # destroy, then both of those and .terraform (prompts)
FORCE=1 ./teardown.sh                    # no prompt
KEEP_TRANSACTION_SEARCH=1 ./teardown.sh  # leave the account-wide setting alone
cdk destroy                              # CDK; leaves Transaction Search enabled
```

`teardown.sh` is best-effort and idempotent. A re-deploy gets a new UI URL; re-create your login
user. All data is removed, with the log groups of Lambdas this project creates (retention
`log_retention_days` / `LOG_RETENTION`, default 30 days). Left behind are log groups it does not
create (`/aws/bedrock-agentcore/runtimes/<runtime>-DEFAULT`, framework helper Lambdas):

```bash
aws logs describe-log-groups \
  --query "logGroups[?contains(logGroupName,'<agent_name>')].logGroupName" --output text \
  | xargs -n1 aws logs delete-log-group --log-group-name
```

## Troubleshooting

KB returns nothing for a document: S3 Vectors caps filterable metadata at 2048 bytes per vector
(both paths mark `AMAZON_BEDROCK_TEXT` and `AMAZON_BEDROCK_METADATA` non-filterable), so one large
document can fail while the deploy succeeds. Expect `COMPLETE` and `numberOfDocumentsFailed: 0`:

```bash
KB=$(terraform output -raw knowledge_base_id)   # CDK: the knowledgeBaseId output
DS=$(aws bedrock-agent list-data-sources --knowledge-base-id $KB \
       --query 'dataSourceSummaries[0].dataSourceId' --output text)
aws bedrock-agent list-ingestion-jobs --knowledge-base-id $KB --data-source-id $DS \
  --query 'sort_by(ingestionJobSummaries,&startedAt)[-1]'
```

The index and KB names carry a digest of their immutable properties (`kb-index-<sha8>`,
`<agent>-kb-<sha8>`), so changing the embedding dimension or non-filterable keys replaces them:
expect a new `knowledgeBaseId` and a fresh ingest.

An MCP target looks empty: `tools/list` paginates, so follow `nextCursor` (recipe in
[GETTING_STARTED.md](GETTING_STARTED.md)). If a target truly lists nothing, the agent raises
`ToolUnavailable` and the run fails with that reason.

| Symptom | Fix |
|---|---|
| Deploy or ingestion fails invoking a model | Enable Bedrock model access for the model and embedding model |
| Login redirect mismatch | Add the UI URL to the client's callback and sign-out URLs (Auth0: also web origins) |
| Gate buttons greyed out, API 403 | Add the user to a group, then sign out and back in |
| Every gated action denied | Auth0: set `authorization.groupsClaim`. `idp = none`: remove `authorization.actions` |
| Tool-bound agents fail with `ToolUnavailable` | Enable the tool plane, or check the target's `tools/list` |
| Plan/synth names a missing value or secret | Supply it (see [Identity provider](#identity-provider), [Secrets](#secrets)) |
