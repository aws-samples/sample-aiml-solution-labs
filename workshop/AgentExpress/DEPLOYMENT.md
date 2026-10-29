# Deployment Guide

How to deploy and operate the orchestrator. Overview: [README.md](README.md). Tutorial (tool
types, agents, the Builder): [GETTING_STARTED.md](GETTING_STARTED.md). Internals:
[ARCHITECTURE.md](ARCHITECTURE.md).

| Part | Sections |
|---|---|
| Set up | [Prerequisites](#prerequisites) · [Identity provider](#identity-provider) · [IAM to deploy](#iam-to-deploy) · [Organisation guardrails](#organisation-guardrails) · [Terraform](#deploy-with-terraform) · [CDK](#deploy-with-cdk) · [Users and RBAC](#users-groups-and-rbac) · [Secrets](#secrets) |
| Tools | [Acting as the person](#tools-that-act-as-the-person) · [Smithy](#tools-that-call-an-aws-api-smithy) · [KB chunking](#knowledge-base-chunking-and-parsing) · [KB vector stores](#knowledge-base-on-your-own-vector-store) · [KB sources](#knowledge-base-from-a-web-crawl-confluence-sharepoint-or-salesforce) · [Code Interpreter](#running-code-in-a-sandbox) · [Browser](#opening-web-pages-with-a-browser) · [Document generator](#generating-pdf-pptx-and-docx-files) · [Hosted MCP](#hosted-mcp-servers) |
| Agents | [Task steps](#steps-with-no-model-task-steps) · [Deliver step](#delivering-a-runs-files) · [Build storage](#storage-created-for-a-build) · [Harness](#agents-run-by-agentcore-harness) · [Runtime VPC](#running-agentcore-runtimes-in-your-vpc) · [LiteLLM](#using-non-bedrock-models-through-litellm) · [Model routing](#choosing-which-model-answers-per-run-override-routing-and-fallbacks) |
| Using it | [Smoke test](#smoke-test-a-deployment-before-a-release) · [Timeline chart](#the-timeline-chart-on-a-run) · [Flow tab](#the-flow-tab-on-a-run) · [Builder canvas](#the-builder-canvas) · [Analytics](#operational-analytics) · [Memory records](#memory-records-what-agents-remembered) · [Share links](#sharing-a-run-with-a-link) · [Files](#a-runs-files-and-the-files-page) · [Asks and approvals](#an-agent-asks-a-person-and-tool-calls-that-wait-for-approval) · [Teams](#teams-and-team-workspaces-builder-console) |
| Builder console | [Console mode](#deploy-a-builder-console) · [Assistant](#the-agentexpress-assistant-links-and-review-changes) · [Connectors](#connectors) · [How a build deploys](#how-a-build-deploys) · [Code sources](#code-from-files-git-or-s3-code-sources) · [Registry](#aws-agent-registry) · [Connected accounts](#connected-accounts) · [Moving a build](#moving-a-build-out-of-a-console) · [Test suites](#test-suites) |
| Operate | [Tuning](#tuning) · [Limits](#limits-on-spend-and-runs) · [Billing tag](#seeing-the-bill-in-aws) · [A2A endpoint](#serving-the-workflow-over-a2a) · [Live config](#changing-a-prompt-or-model-without-redeploying-live-config) · [Send runs to Langfuse, Datadog and others](#send-runs-to-langfuse-datadog-and-others) · [OTLP export](#sending-traces-to-another-otlp-backend) · [Tear down](#tear-down) · [Troubleshooting](#troubleshooting) |

Two IaC paths deploy the same resources. Use one per account and region (names overlap); different
accounts or regions are fine.

| | Terraform (`orchestrator/terraform/`) | CDK (`orchestrator/cdk/`) |
|---|---|---|
| AgentCore features | all | all |
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
  only when they read images. The Document generator's function (a tool with `source: "docgen"`),
  the runtime's delivery permissions (a `deliver` step) and the build's own bucket
  (`orchestrator.storage.create`) are created only when the workflow sets them
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
- On a Builder console, model access for the Assistant's reviewer (`designerReviewModel`,
  default `us.openai.gpt-6.1-sol`). Without it, Review changes reports "Review unavailable" and
  the change is saved unreviewed
- Terraform 1.10+ (S3 native locking, `use_lockfile`), or the AWS CDK CLI (`npm i -g aws-cdk@2.1144.0`)

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
| Okta | `idp = "okta"`, `okta = { domain, client_id, authorization_server = "default" }` | `-c idp=okta -c oktaDomain=… -c oktaClientId=… [-c oktaAuthServer=…]` |
| Microsoft Entra ID | `idp = "entra"`, `entra = { tenant_id, client_id }` | `-c idp=entra -c entraTenantId=… -c entraClientId=…` |
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
- Okta: nothing is created in the org. `domain` is its host (`acme.okta.com` or your custom
  domain), with no scheme or path. Sign-in goes through a custom authorization server (Security >
  API; `default` unless you name another), because the org server cannot issue the Gateway's
  client-credentials token. Create an OIDC Single-Page App integration with the Authorization Code
  and Refresh Token grants (rotation on), turn Federation Broker Mode off and assign its users
  (with it on, Okta answers "You are not allowed to access this app"). Allow it in the
  authorization server's access policy (a rule with Authorization Code for it), add the app URL as
  a Trusted Origin (CORS and Redirect), and give the app a sign-on policy your users can satisfy
  (a new org may require Okta Verify). For RBAC, add a `groups` claim to the ID token on that
  server and set `authorization.groupsClaim` to `groups`. The agents' Gateway client is made in a
  machine-only pool by default; to use an Okta API Services app instead, see
  [M2M identity](#m2m-identity-agent-to-gateway)
- Microsoft Entra ID: nothing is created in the tenant. `tenant_id` is the Directory (tenant) ID,
  a GUID: a domain name or `common` is refused, since the token's issuer carries the GUID and
  `common` would admit other tenants. Register a single-tenant app with the Single-page application
  platform. Add the optional `email` claim to its ID token (sharing and the audit log use it). For
  RBAC, define app roles, assign them, and set `authorization.groupsClaim` to `roles`. The
  `groups` claim carries object IDs, not names. With the tool plane, register an API app (Expose an
  API, an app role for callers, and `"requestedAccessTokenVersion": 2` in its manifest: a v1
  token's issuer is `sts.windows.net` and the Gateway refuses it), plus a client app with a secret
  granted that role, with admin consent
- Your pool, Auth0, Okta or Entra ID: after the first deploy, add the UI URL to the client's
  allowed callback (redirect) and sign-out URLs (Auth0: also Allowed Web Origins). The URL has no
  trailing slash. `terraform output login_issuer` names the issuer the API trusts
- None: the UI and `/api/*` are open to anyone with the URL. The tool plane is not allowed (the
  Gateway's `CUSTOM_JWT` authorizer needs an OIDC provider), so tool-bound agents, including the
  five research agents, fail with `ToolUnavailable`. Also remove `authorization.actions`: with no
  JWT claims every group check denies everyone. Both paths reject either combination

### M2M identity (agent to Gateway)

Optional. The agents sign in to the Gateway as a machine, not as a person, so by default the
deployment makes their client itself: in the Cognito pool it creates, or, when people sign in
with Okta, Auth0, Entra ID or a pool of your own, in a small pool for machines only (no users and
no sign-in page). Nothing has to be set up at your provider for tools to work. To use your
provider's own machine client instead, set it here. Terraform:
`gateway_identity = { client_id = "...", audience = "gateway/invoke", scope = "" }`; CDK:
`-c gatewayClientId=... -c gatewayAudience=... [-c gatewayScope=...]`. The audience is the OAuth2
scope on Cognito, the API Identifier on Auth0, the authorization server's Audience on Okta (e.g.
`api://default`), and the API app's client id (a GUID) on Entra ID. `scope` is required on Okta
(the custom scope); on Entra ID it defaults to `<audience>/.default`. The client secret goes in the
environment only (`TF_VAR_gateway_client_secret` / `GATEWAY_CLIENT_SECRET`).

| | Cognito M2M token | Auth0 M2M token | Okta M2M token | Entra ID M2M token (v2) |
|---|---|---|---|---|
| Claims present | `client_id` + `scope`, no `aud` | `aud` + `azp`, no `client_id` | `aud` + `cid` + `scp` | `aud` + `azp` + `roles` |
| Gateway pins on | `allowed_clients` | `allowed_audience` + an `azp` custom claim | `allowed_audience` + a `cid` custom claim | `allowed_audience` + an `azp` custom claim |
| Token request | HTTP Basic + `scope` | form body + `audience` | HTTP Basic + `scope` | form body + `scope` |
| Token endpoint | `/oauth2/token` | `/oauth/token` | `/oauth2/<server>/v1/token` | `/<tenant>/oauth2/v2.0/token` |

### Tools that act as the person

A tool with `auth: "user"` (each person's own account) or `"obo"` (the person's sign-in) gets a
second Gateway, `<agent-name>-gwu`, whose inbound JWT check trusts the app's sign-in (the
provider's discovery URL, audience = the SPA client id) instead of the agents' machine client.
Each call carries the ID token of the person who started the run. Nothing else to set: it
follows the sign-in provider (and so needs one).

- `user`: register the tool's callback URL at its provider as a redirect URL. It exists only after
  the first deploy: the Builder shows it on the tool, `cdk deploy` prints it as
  `toolCallbackUrls`, Terraform as `tool_callback_urls`. A person connects once from the run's
  **Connect** button; the app's `POST /api/connect` binds the grant to their sign-in.
- `obo`: the tool's provider must accept the app's sign-in token in a token exchange
  (`subject_token_type` ID token). Not available with Entra ID sign-in.

### Tools that call an AWS API (Smithy)

A `type: "smithy"` tool is an AWS service's API, from its Smithy JSON model; the Gateway calls
it signed by the build's Gateway role (`AgentCoreGateway-<agent-name>`). That role gets exactly
the tool's `allow` (named actions on full ARNs) as statement `SmithyCalls<key>`, and
`s3:GetObject` on the model's object when it is hosted in S3. Nothing is added for a build
without one. Actions of identity, code-running and secret-holding services are refused (see
`smithyDeniedServices` in `app/vocabulary.json`). If `iam_permissions_boundary_arn` is set, the
boundary still caps the role, so a grant outside it fails at call time with an access error.
See `docs/WORKFLOW_REFERENCE.md`, `type: "smithy"`.

### Knowledge Base chunking and parsing

A `type: "kb"` tool may set `chunking` (fixed size, two layers, by meaning, or none) and
`parsing` (`foundationModel`: a Bedrock model reads PDFs and images, tables included). The
Builder shows both under **Advanced → Document processing**. Without them nothing changes: the
data source is still named `docs` with Bedrock's defaults. With them, the data source is named
`docs-<digest>`; changing a setting replaces it and re-ingests every document on the next
deploy. Model parsing adds statement `ParsingModel` to role `AgentCoreKB-<agent-name>`
(`bedrock:InvokeModel` on that model, or on the inference profile and its model), and its
token cost per document page. See `docs/WORKFLOW_REFERENCE.md`, `type: "kb"`.

### Knowledge Base on your own vector store

By default a `type: "kb"` tool's Knowledge Base stores its vectors in an S3 Vectors bucket and
index that the deploy creates. With `vectorStore` it uses a store **you already have** instead:

| `vectorStore.type` | Store | The Knowledge Base role gets |
|---|---|---|
| `opensearchServerless` | Amazon OpenSearch Serverless collection + vector index | `aoss:APIAccessAll` on the collection |
| `opensearchManaged` | Amazon OpenSearch Service domain + vector index | `es:ESHttpGet/Post/Put/Delete` on that index, `es:DescribeDomain` on the domain |
| `aurora` | Aurora PostgreSQL with pgvector (Data API on) | `rds:DescribeDBClusters`, `rds-data:ExecuteStatement`/`BatchExecuteStatement` on the cluster, the secret |
| `neptuneAnalytics` | Neptune Analytics graph (GraphRAG) | `neptune-graph:GetGraph`, `Read/Write/DeleteDataViaQuery` on the graph, `bedrock:InvokeModel` on `graphModel` |
| `pinecone` | Pinecone index | `secretsmanager:GetSecretValue` on the secret |
| `mongodbAtlas` | MongoDB Atlas vector search index | the secret |

The deploy creates no store, no S3 Vectors bucket and no index; it creates the Knowledge Base on
yours, the data source (uploads or `s3Uri`) and the ingestion. Before deploying:
- Create the store and its vector index (or table, or graph) with the embedding model's
  dimension (1024 for the default Titan v2), and the fields you name.
- Let role `AgentCoreKB-<agent-name>` in where the store has its own access control: an
  OpenSearch Serverless data access policy, an OpenSearch domain access policy.
- Put credentials (Aurora, Pinecone, MongoDB Atlas) in a Secrets Manager secret and give
  its full ARN. A secret encrypted with your own KMS key also needs that key to allow the role.
- For corpus filtering, make `corpusKey` (default `doc_type`) a filterable field in your store
  (Aurora: a column, or the `customMetadataField` jsonb column).

Changing `vectorStore` replaces the Knowledge Base (its name is `<agent>-kb-<digest>` of the
store) and re-ingests. Destroying the deploy deletes the Knowledge Base, not your store; for
Neptune, AWS notes the graph keeps billing until you delete it. The Builder shows it under
**Advanced → Vector store**.

### Knowledge Base from a web crawl, Confluence, SharePoint or Salesforce

A `type: "kb"` tool's `dataSource` takes its documents from a connector instead of files in
S3. AWS supports these connectors **only on an OpenSearch Serverless vector store**, so the tool
also needs `vectorStore.type: "opensearchServerless"` (above). The Builder shows it under
**Advanced → Where documents come from**.

| `dataSource.type` | You give | The Knowledge Base role gets |
|---|---|---|
| `web` | `seedUrls`; optional `scope`, `inclusionFilters`, `exclusionFilters`, `rateLimit`, `maxPages`, `userAgent` | nothing extra |
| `confluence` | `hostUrl` (`https://<name>.atlassian.net`), `authType` (`basic` or `oauth2`), `credentialsSecretArn`; optional `filters` | `secretsmanager:GetSecretValue` on the secret; with `oauth2` also `PutSecretValue` (Bedrock stores the refreshed token there) |
| `sharepoint` | `domain`, `siteUrls`, `credentialsSecretArn`; optional `tenantId`, `filters` | `GetSecretValue` on the secret |
| `salesforce` | `hostUrl`, `credentialsSecretArn`; optional `filters` | `GetSecretValue` on the secret |

No documents bucket is created. The connector re-syncs on every deploy, as `s3Uri` does.
Before deploying, put the connector's credentials in a Secrets Manager secret with the keys AWS
documents for it (SharePoint: a Microsoft Entra ID app with `Sites.Read.All`; Salesforce: a
Connected App with client credentials; Confluence: an API token or an OAuth 2.0 app). Only crawl
sites you own or may crawl; the web crawler follows robots.txt. AWS stops accepting **new**
connectors of these four types on customer-managed knowledge bases from September 30, 2026
(existing ones keep syncing). For corpus filtering, set `corpusKey` to an attribute the
connector writes, or give agents no `corpus`.

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
invoke its judge model. `S3ManageBuildStorage` creates, configures, empties and deletes a build's
own bucket ([build storage](#storage-created-for-a-build)) on `arn:aws:s3:::ax-*-storage-*` only,
the names builds get, with or without the Region at the end ([bucket names](#bucket-names-and-regions));
it grants no object read or write. A stack with another `agent_name` that sets
`orchestrator.storage` needs the same actions on `<agent name>-storage-<account>` (and
`<agent name>-storage-<account>-<region>` with `bucket_naming = "regional"`). CDK uses the
bootstrap execution role instead; there is no policy file.

## Organisation guardrails

For accounts whose rules require them. All are off by default; unset, a deployment adds,
changes or replaces nothing.

| Terraform | CDK context | Default | What it does |
|---|---|---|---|
| `iam_permissions_boundary_arn` | `permissionsBoundaryArn` | empty | Permissions boundary on every IAM role the stack creates |
| `iam_role_path` | `iamRolePath` | `/` | IAM path of every role. **Changing it replaces every role** |
| `custom_domain`, `acm_certificate_arn` | `customDomain`, `acmCertificateArn` | empty | Your own host name on CloudFront, TLS 1.2 or later |
| `enable_waf`, `waf_rate_limit` | `enableWaf`, `wafRateLimit` | `false`, `2000` | AWS WAF on the CloudFront distribution (us-east-1 stacks only) |

**Permissions boundary.** The managed policy goes on every role: the runtime, the BFF, the Gateway,
the Knowledge Base, the tools, the deploy project, and, on the CDK path, CDK's own roles for its
custom resources, the UI upload and bucket auto-delete. A role has one boundary, so a code tool's
role (`tools.<name>.code`) takes yours in place of its own, and the Deny on the framework's
tables, secrets and buckets that its own boundary carried moves into the role's policy. The
Builder passes the boundary to every build it deploys. A build in a connected account gets the
policy of the same name in that account, so create it there too (an AWS managed policy is the
same everywhere). The deploy role's policy also refuses `iam:CreateRole`, `iam:PutRolePolicy`,
`iam:AttachRolePolicy` and `iam:PutRolePermissionsBoundary` unless the role has the boundary,
refuses `iam:DeleteRolePermissionsBoundary`, and refuses edits to the boundary policy. Set the
boundary yourself on the CDK bootstrap roles (bootstrap with `cdk bootstrap --custom-permissions-boundary <name>`), a
connected account's deploy role (the connection stack you create there), and roles AWS services
create for themselves (service-linked roles cannot have a boundary). The boundary must allow what
each role is granted; if it is narrower, the call that needs the missing permission fails at run
time, not at deploy.

**Role path.** Every role, and every role of a build, is created under the path; the deploy
policy's role patterns move under it too. IAM cannot move a role, so set the path on a NEW
deployment. On an existing one, Terraform deletes each role and creates it again under the path
(the policies on it are recreated, and the runtime, the BFF and every function are updated to the
new role; calls fail while a role is missing). CloudFormation cannot replace a role that has a
fixed name, and most of this stack's roles do, so a `cdk deploy` that changes the path fails and
rolls back: destroy the stack and deploy it again with the path. The CDK bootstrap and
connected-account roles stay where they are either way. With
`deploy-role-policy.json` attached by hand (Terraform), replace `:role/` with `:role<path>` in it.

**Custom domain.** `custom_domain = "agents.example.com"` with an ACM certificate for that name.
CloudFront only uses certificates in **us-east-1**, whatever region the stack is in; the setting
refuses one from anywhere else. The distribution serves the name with SNI and the
`TLSv1.2_2021` security policy (TLS 1.2 or later, strong ciphers only). CloudFront's own
`*.cloudfront.net` name keeps working and keeps CloudFront's default policy, which you cannot raise.
The Hosted UI callback and sign-out URLs, the builds and assets buckets' CORS, and the address a
person returns to after connecting an account for a tool all accept the custom domain as well.
The `ui_url` / `uiUrl` output becomes `https://<custom_domain>`. You create the DNS record:
a CNAME (or a Route 53 alias A/AAAA record) from the name to the distribution's
`*.cloudfront.net` domain. With Okta, Auth0 or Entra ID sign-in, add `https://<custom_domain>` to
the app's sign-in redirect and sign-out URLs there yourself. Builds the Builder deploys keep their
own CloudFront URLs.

**AWS WAF.** A web ACL (scope `CLOUDFRONT`) on the distribution: a per-IP rate limit
(`waf_rate_limit` requests in 5 minutes; people behind one NAT share an IP, so raise it for a
large office) and the AWS managed rule groups Amazon IP reputation, Core rule set and Known bad
inputs. The Core rule set's 8 KB body limit (`SizeRestrictions_BODY`) only counts, because
saving a build sends more. It covers the UI and `/api/*` on their way through CloudFront. The
HTTP API's own `execute-api` URL is not covered: AWS WAF associates with API Gateway REST APIs,
not HTTP APIs, so a caller who uses that URL directly meets the JWT authorizer and the stage
throttle, not WAF. A CloudFront web ACL can only be created in us-east-1, and this stack creates
everything in its own region, so `enable_waf` refuses any other region. WAF is billed per web ACL,
per rule and per million requests ([pricing](https://aws.amazon.com/waf/pricing/)). Builds keep
no web ACL.

**Server-side requests.** Every request the runtime makes to a URL from `workflow.json` or from
deployment settings (an A2A agent's card and endpoint, the IdP token URL) goes through one guard,
`app/common/netguard.py`: https only, no `user:password@`, and the connection refused if the host
resolves to loopback, link-local (including the instance metadata address 169.254.169.254),
private, CGNAT or an IPv6 form of those, checked on the address actually connected to and again
on every redirect. HTTPS proxies set in the environment are not used for those requests.
`A2A_ALLOW_PRIVATE_ENDPOINTS=true` on the runtime lifts the address check for a deliberately
private remote agent. MCP, OpenAPI and web-search tools are fetched by AgentCore Gateway, not by
this code.

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
   terraform apply -target=null_resource.ui_build   # a new stack, or any apply after the web code (web/) changed
   terraform apply
   ```

   The targeted apply builds the UI; until it runs, the file list `ui.tf` uploads is unknown and
   the plan is refused. That happens on a new stack and again whenever the web code (`web/`) has
   changed since the last apply, for example after you pull a new version. Otherwise one
   `terraform apply` is enough. Allow several minutes.

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
| `designerReviewModel` | empty | The model that reviews the Assistant's changes when **Review changes** is Auto or Always; empty is `us.openai.gpt-6.1-sol`. Sets `DESIGNER_REVIEW_MODEL` on the BFF; the UI never shows it (Terraform `designer_review_model`) |
| `codeSourceBuckets` | empty | Buckets an S3 [code source](#code-from-files-git-or-s3-code-sources) may read for builds deployed into this console's own account, comma-separated; empty means S3 code sources need a connected account (Terraform `code_source_buckets`) |
| `idp`, `createCognito`, … | `cognito`, `false` | See [Identity provider](#identity-provider) |
| `selfSignUp`, `selfSignUpGroup` | `false`, `members` | Open registration |
| `enableGateway` | `false` | Tool plane |
| `memoryEventExpiryDays` | `30` | Short-term memory retention |
| `transactionSearchIndexingPercentage` | `100` | Span indexing (1% is free) |
| `builder` | `true` | Builder control plane |
| `consoleMode` | `app` | `builder` for a [Builder console](#deploy-a-builder-console) |
| `demoVideoUrl` | empty | An `https://` link to a demo video, shown in a Builder console's About panel and first-run view; empty shows none (Terraform `demo_video_url`) |
| `docsUrl` | the public [user guide](docs/user-guide/README.md) on GitHub at the release tag | The documentation the top bar's **Documentation** link and each feature's info icon link to ([In-product documentation links](#in-product-documentation-links)): the GitHub guide, or an XWiki copy of it. A console passes it on to the builds it deploys. Empty shows no documentation link anywhere (Terraform `docs_url`) |
| `docsLinks` | `true` | `false` shows no info icon and no Documentation link anywhere, on the console and on every build it deploys (Terraform `docs_links`) |
| `tags` | | JSON object of extra tags on this stack's own resources only; the builds a console deploys do not get them (use `buildTags`) (Terraform `tags`) |
| `buildTags` | | JSON object of extra tags on every build this console deploys, in its own account or a connected one ([Tags on the builds a console deploys](#tags-on-the-builds-a-console-deploys)) (Terraform `build_tags`) |
| Others | off | [Organisation guardrails](#organisation-guardrails), [A2A](#serving-the-workflow-over-a2a), [Export to Langfuse, Datadog and others](#send-runs-to-langfuse-datadog-and-others), [OTLP](#sending-traces-to-another-otlp-backend), [LiteLLM](#using-non-bedrock-models-through-litellm), [teardown](#tear-down); every flag is in [cdk/README.md](orchestrator/cdk/README.md#configuration-context-keys) |

Outputs: `uiUrl`, `apiEndpoint`, `idp`, `authEnabled`, `consoleMode`; `cognitoUserPoolId` and
`cognitoClientId` with `createCognito=true`; `agentRuntimeArn`, `memoryId`, `dedicatedAgents`
unless `consoleMode=builder`; `gatewayUrl`, `knowledgeBaseId` with the tool plane.

## Users, groups and RBAC

Skip for `idp = none`; on Auth0, Okta or Entra ID, create (or assign) users in your tenant. The Cognito pool is admin-create-only
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

**Groups vs teams.** Groups (in Cognito or your sign-in provider) decide which **actions** a person
may take, through `authorization.actions`. [Teams](#teams-and-team-workspaces-builder-console) are a
Builder console feature that decides who **shares which builds** (owner, editor, viewer); a team member
can be a person or a whole group. **To make someone an admin**, add them to `admins` with
`admin-add-user-to-group` as above, and have them sign in again.

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
`cognito:groups` finds nothing and every gated action is denied. Okta: `groups`, from a groups
claim on the authorization server. Entra ID: `roles`, the app roles assigned to the user. Groups
named in `authorization.actions` are then those claim values. Only a pool the stack creates gets
them made for you.

## Using it

Open the UI URL, sign in and start a run. The walkthrough of gates, observability, evaluations,
re-runs and the assistant is in [GETTING_STARTED.md](GETTING_STARTED.md).

### Smoke test a deployment before a release

`orchestrator/scripts/e2e_smoke.py` signs in, runs the workflow through every review gate, then checks
the run, its telemetry and `/api/limits`, and exits `0` (passed), `1` (a check failed) or `2` (could not
run). It works against any deployed app and needs only the URL and a token:
see [scripts/README.md](orchestrator/scripts/README.md).

### In-product documentation links

The top bar has a **Documentation** link, and each feature (every Builder tab and Settings section, each
agent and tool panel section, the deploy dialog, the Assistant, and the execution plane's pages) has an
info icon. Its popover names the feature, says in a sentence what it is, and links to the feature's
section in a new tab. A Builder console passes its settings on to every build it deploys, so a build's own
app links to the same documentation.

**Where the links go: `docsUrl`.** By default, the public [user guide](docs/user-guide/README.md) on
GitHub, pinned to the release's tag:
`https://github.com/aws-samples/sample-aiml-solution-labs/blob/agentexpress-v1.3.11/workshop/AgentExpress/docs/user-guide/`.
A section's link is `<docsUrl><page>.md#<slug>`, GitHub's id for the heading (lower case, punctuation
but `-` and `_` removed, spaces as `-`, a repeated heading `-1`, `-2`). An organisation that keeps the
guide in an XWiki space, typically for internal or production deployments, points `docsUrl` at that
space instead, and the links follow the wiki's shape (`<docsUrl><page>#H<heading>`), chosen because the
path contains `/bin/view/`:

```bash
npx cdk deploy -c docsUrl=https://wiki.example.com/bin/view/AgentExpress/
# Terraform: docs_url = "https://wiki.example.com/bin/view/AgentExpress/"
```

An empty `docsUrl` renders no documentation link or icon anywhere. Anything but an `https://` URL fails
the synth or the plan.

**The default's release tag** is one constant, `ref` in
[`orchestrator/docs_default.json`](orchestrator/docs_default.json). CDK (`cdk/lib/docs-default.ts`) and
Terraform (`local.docs_default` in `terraform/builder.tf`) both read that file, so both paths deploy the
same default. Bump `ref` to the new tag when you cut a release; nothing else names it.

**Turn the links off: `docsLinks`.** `-c docsLinks=false` (Terraform `docs_links = false`) renders no
info icon and no Documentation link anywhere, on the console and on every build it deploys.

**Each person's choice: Show help icons.** Where a deployment has documentation links, the top bar's
account menu (or, without sign-in, its **Settings** menu) has a **Show help icons** checkbox. It is on by
default; off hides every info icon and the Documentation link for that person. The choice is kept in the
browser's local storage, like the Assistant's Review changes setting.

Every page and section is named in one file, `orchestrator/web/src/docs/docsLinks.ts`, beside each
feature's title and description: the page, and the heading's text, from which each target's anchor is
computed. `docsLinks.test.tsx` checks every entry against the headings of `docs/user-guide/*.md` (it fails
if the guide is missing; `AX_USER_GUIDE_DIR` checks another copy), and against an XWiki copy's page
sources when `AX_WIKI_DIR` names their directory. `orchestrator/scripts/check_docs_links.py` checks the
links the popovers render. It is not part of CI. Run it after the documentation changes:

```bash
cd orchestrator
# The GitHub guide: against the local Markdown, then also against github.com (public, no sign-in).
python3 scripts/check_docs_links.py --target github
python3 scripts/check_docs_links.py --target github --live
# An XWiki copy, in Chromium with a signed-in session.
pip install playwright==1.64.0 && playwright install chromium
python3 scripts/check_docs_links.py --target wiki --base-url https://wiki.example.com/bin/view/AgentExpress/ \
  --cookies ~/cookies.txt --signin-hint "sign in again to refresh ~/cookies.txt"
```

`--cookies` is a Netscape-format cookie file with a signed-in session, for a site that needs one. A page
that lands on another host is taken as its sign-in page: the check stops with exit code `2` and prints
`--signin-hint`. Exit `0` means every anchor was found, `1` that one or more is missing; every option is
in `--help` and [scripts/README.md](orchestrator/scripts/README.md#check_docs_linkspy-check-the-in-product-documentation-links).

### The Timeline chart on a run

A run's Timeline tab opens on **Chart**, a Gantt chart (time left to right); **Log** is the table
of timeline lines. It reads nothing new and adds no resource or setting.

- One row per agent in stage order: a bar from "started" to "complete", "failed" or "blocked by
  guardrail", and a second bar labelled v2 for a re-run.
- Inside each bar: its model, tool, guardrail, memory, Cedar policy and evaluation calls, from the
  Flow events (each starts at its row's time less its latency).
- A hatched band marks each HITL wait with its length and decision; a long wait is drawn short
  behind a break marker ("// 2 h //").
- The bars that set the run's length are outlined; **Slowest** jumps to the three slowest calls.

Good to know: times are to the second, so a short call can be drawn up to a second off. A shared
run's chart shows bars and waits but not the calls inside them.

### The Flow tab on a run

The Flow tab shows what each agent did with the Gateway, memory, Cedar policy and guardrails, one
sentence per event, and replays them in order (play, pause, step, scrub, 1x or 2x). It reads the
telemetry rows the runtime already writes (`GET /api/sessions/{id}/flow`); there is nothing to turn
on, and no AWS call until someone opens the tab.

- **Story** (the default) tells the run top to bottom: one numbered header per stage with status
  and time; under it each agent (2a, 2b) and one line per event with an emoji, a short sentence
  ("Asked Claude Haiku 4.5", "Cedar policy noSecrets: ALLOW"), time and cost. Up to four agents
  open by default; one with a failed, denied or blocked line opens itself. A re-run starts with
  "v2 after review: <note>"; each HITL gate, agent question and tool approval says how long it
  waited. The run's time, cost, calls and HITL waits are at the top.
- **Summary** is the per-agent list.
- Filter to problems only or to one kind (model, tools, Cedar, guardrails, memory, evaluations,
  HITL). Each line is a button: Enter or a click shows its full sentence, latency and cost; Arrow
  Up and Arrow Down move between lines. Replay drives both views.

Good to know:

- A run shows up to 2,000 events (the earliest), and the tab says when it stopped. Prompts, answers
  and tool results are in the Observability view, not here.
- It shows model, tool, memory, guardrail, Cedar policy, evaluation, image and compute events, plus
  the run's review gates. Branch decisions are on the Timeline.
- A running run is refreshed every few seconds. Replay steps through events one at a time.

**Step numbers.** Each step in the Graph, Table and Flow views carries a badge with its stage's
position (2a, 2b inside a parallel or sequence stage), coloured by status: green done, blue running,
red failed, amber waiting, grey not started.

**Deployed resource names.** Opening a tool, guardrail, memory, Cedar policy, evaluator or identity
row on the run's Graph shows what it is deployed as: Gateway target and Gateway, Bedrock Guardrail
(version `DRAFT`), AgentCore Memory id, policy engine and policy, evaluator, credential provider. The
BFF computes the names from `workflow.json` and its own function name, with no AWS call. A name says
what the deploy creates, not that it exists. A deploy-created Knowledge Base, a Builder build's
memory id, and rows on runs recorded before this change show "not available".

### The Builder canvas

- Each agent card lists its components as rows: tools, guardrails, memory, Cedar policies, skills,
  evaluations, identity and HITL. Rows come from the config, not a deployed account.
- **Show** turns kinds of row off and on (all on by default) and switches on the **AgentCore rail**:
  the deployment-wide pieces (Gateway, Memory, Policy engine, Guardrail, Identity, Observability,
  Triggers) with counts. The rail also appears while you drag a primitive; it opens expanded and can
  be collapsed. Every rail item is a button that opens it in the inspector.
- The palette's **Primitives** group lists the build's guardrails, memories, Cedar policies, skills,
  evaluators, interceptors and triggers. Dragging one marks where it may go; a drop writes the same
  keys as the inspector (one undo step). Guardrails, memory, skills and evaluators go on agents that
  run here (not A2A); a guardrail dropped on an agent with no checks turns both on. A Cedar policy goes
  on a tool row and governs that tool for every agent. A trigger goes on the first stage and is turned
  on. An interceptor has no target (it runs on every tool call). **Attach** and **Detach** in the
  inspector do the same by keyboard.
- "Cedar policy" on a card means the agent's `agentcore.policy.enabled`; the Gateway's engine
  applies to every tool call whether or not it is set.
- **+ New generic step** adds a [task step](#steps-with-no-model-task-steps); **Tools → Add a
  connector** opens the [Connectors](#connectors) gallery.

### Operational analytics

An app has an **Analytics** page, in the side navigation, for whoever holds the `admin` permission
(`authorization.actions.admin`, closed unless granted). It shows how the deployment is behaving over
a date range of up to 92 days: per tool, calls, error rate, p50/p95 latency and the Cedar denial rate;
per agent, calls, error rate, guardrail blocks, Cedar denials, p50/p95 model latency and a health
score with the reasons it is low; and warnings for agents no guardrail screens and tools no Cedar
policy decides. It reads the telemetry rows the runtime already writes, every user's, through
`GET /api/analytics?from=&to=` (JWT-authorized like every route, then refused with 403 without
`admin`). There is nothing to turn on, no new resource or setting, and no AWS call until someone
opens the page.

The health score is `100 − 60·E − 20·D − 10·G − 10·S`, clamped to 0–100 and rounded half up, where
E is the agent's error rate over its model, tool and image calls, D is Cedar denials ÷ (allowed +
denied), G is guardrail blocks ÷ guardrail checks, and S is 1 when the agent made at least 5 model
calls and its p95 model latency is more than 3 times its own median. Each term that costs points is
listed with its number. Example: 2 errors in 20 calls and 1 denial in 4 decisions is
100 − 6 − 5 = 89. The full definition is in `orchestrator/bff/analytics.py`. Good to know:

- A request reads up to 20,000 telemetry rows, newest first, and the page says when it stopped.
- In `LOG_ONLY` mode Cedar decisions are in CloudWatch, so those calls count as log-only.
- The warnings come from the deployed `workflow.json`: they say what is configured.
- For CloudWatch alarms, use AgentCore's own metrics
  ([runtime](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/observability-runtime-metrics.html),
  [policy](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/observability-policy-metrics.html)).
- A row of a kind or status the page does not know is counted as "other", never dropped.

### Memory records (what agents remembered)

An app's **Memory records** page shows the long-term memory records AgentCore extracted from what
agents stored (`agentcore.memory.longTerm`, or a named memory with `agentcore.memory.use`). Pick an
agent, then a namespace (one per actor: the agent plus a user digest, subject or run, see
`agentcore.memory.scope`), and read each record's text, when it was created and which strategy wrote it.
A search box ranks a namespace's records against a query (at most 1,000 characters), and each record
can be deleted after a confirmation that names it. There is nothing to turn on; the page makes no AWS
call until someone opens it.

- **Who can see it:** only callers with the `admin` permission, which is closed unless
  `authorization.actions.admin` names a group. A namespace is keyed by agent and actor, not by the
  person signed in, and some scopes are shared by everyone who runs the agent, so people cannot browse
  "their own" memory. Every look and every delete is in the Activity log (`memory.viewed`,
  `memory.deleted`) with the agent and namespace. The search text and the deleted record's text are
  not logged.
- **What delete does:** it removes that one record for good (`DeleteMemoryRecord`). It does not remove
  the stored turn it was extracted from, which stays until the memory's event expiry
  (`memory_event_expiry_days` / `memoryEventExpiryDays`, or a named memory's `expiryDays`). A later run that stores the same insight can be extracted into a new
  record, so fix what produced a wrong fact as well as deleting it.
- **IAM:** the BFF role gets `ListMemoryRecords`, `RetrieveMemoryRecords` and `GetMemoryRecord` on the
  long-term memories (the semantic memory and each named one), and `DeleteMemoryRecord` as a separate
  statement, `MemoryInspectorDelete` (`terraform/memories.tf`, the CDK stack). Remove that statement for
  a read-only page.
- **What it shows:** long-term records of the agents in `workflow.json`. An actor is shown as a user
  digest, not an email. A build deployed from a console has this page in its own app.

### Sharing a run with a link

A run is private to whoever started it. Its owner can press **Share** on the run page to make a
read-only link to that one run, expiring after 1 hour, 1 day, 7 days (the default) or 30 days, then
copy it, list the run's active links and revoke any of them. There is nothing to turn on: no new
setting, resource or IAM permission. Links live in the deployment's events table as their own rows,
and nothing is read or written until someone shares a run.

- **Who can open a link:** anyone who signs in to this same app. The link is never anonymous: its route
  is behind the same JWT authorizer as every other route, and the BFF also refuses a caller with no
  identity. On a deployment with `idp = "none"`, which has no sign-in, links cannot be made or opened.
  Anyone signed in who has the link can open it, so treat it like the run itself when you send it.
- **What they see:** the run's request, steps, outputs and timeline, with a banner naming who shared it
  and when the link expires. The page reads `GET /api/shared/{shareId}` only and has no action buttons.
- **What they do not see or do:** no telemetry (cost, tokens, captured prompts), no Flow tab, no
  attached files, no other run. They cannot approve, re-run, stop, evaluate or delete the run: those
  routes still answer only to its owner. Images an image agent drew are served by `/api/images`, which
  is owner-only, so they do not load on a shared page.
- **Expiry and revocation:** checked by the server on every read. A link that expires or is revoked
  stops working at the next read, including on a page already open. DynamoDB's TTL removes the rows
  a day after expiry, but the expiry check never depends on TTL. Deleting the run ends its links too.
- **Limits and the log:** a run has at most 10 active links. Making, opening (by whom) and revoking a
  link are events in the run owner's Activity log. The log names a link by its first 6 characters,
  never the whole link. Opening is logged once an hour per viewer and link, since the page polls a
  running run.
- **Who manages links:** only the run's owner. A [Builder console](#deploy-a-builder-console)
  (`consoleMode=builder`) has no runs, so no links.

### A run's files, and the Files page

Every run page has a **Files** tab with three groups: **Inputs** (what the run was started with),
**Outputs** (files an agent saved, and images an image agent drew) and **Code** (code the
`codeInterpreter` tool ran, with a `.log` of what it printed). Each row names the agent, the size and
the time, with **Download** and, for text, Markdown, JSON, CSV (the first 200 rows, as a table),
images and PDFs (in a new tab), **Preview**. The **Files** page in the navigation lists the files of
your runs, newest run first, ten runs at a time, with a filter; an admin can switch to everyone's.

- **Turning it on:** set `orchestrator.files` in workflow.json (`{}` is enough; `retentionDays`, 1 to
  3650, default 90). That creates the assets bucket if no agent needed it yet, lets agent code call
  `await ctx.save_file(name, data, content_type)` (25 MB at most per file), lets each dedicated runtime
  write `runs/*/outputs/*` and `runs/*/code/*` only, allows this app's origin to read a file through its
  link (CORS GET, for previews), and deletes everything under `runs/` after `retentionDays`.
  Without it, a deployment keeps what it kept before, with no expiry: inputs and images still show on
  the tab, and the orchestrator's own runtime still keeps sandbox code when the bucket exists.
- **Who sees a file:** whoever may read the run (its owner, an admin, a service trigger's approvers).
  `GET /api/sessions/{id}/files` lists it; `GET /api/sessions/{id}/files/url?key=` signs a 5-minute
  link for one key under that run's own folder only. Only images and PDFs open in the browser; every
  other file is a download, with a content type chosen from its extension, so an agent's `.html` is
  never served as a page. The page shows text as text. `GET /api/files?page=` lists the caller's runs;
  `scope=all` needs `admin` and, where the Activity log is on, is recorded in the admin's log.
- **Good to know:** a shared link shows no files. A build's run lists its files in the build's own
  app. Code is kept for calls the model makes (`toolMode: "model"`). Text previews show the first
  256 KB. The same file name saved twice in a run replaces the first. Retention applies to all of
  `runs/`.

### An agent asks a person, and tool calls that wait for approval

Two settings let a person into the middle of a step (app/common/asks.py; reference in
[WORKFLOW_REFERENCE.md](orchestrator/docs/WORKFLOW_REFERENCE.md)). `agents.<id>.askPerson` gives an
agent with `toolMode: "model"` an `ask_person` tool, and `tools.<name>.approval` makes every call of
that tool wait for a person. Both are off unless set, and add no resource, route or setting: the run
waits in the same `waiting_human` state as a review gate, and the run page shows the question with a
text box, or the tool with its arguments (as text) and Approve / Deny. The answer goes through the
same `POST /api/sessions/{id}/decision` route, with the same `decision` permission and the same
owner check, so whoever may decide a run's gates may answer its questions. The run assistant cannot.
Each ask and its outcome is a timeline line, a Flow tab event and an activity-log event
(`run.answered`, `run.tool_decided`).

When the run resumes, LangGraph runs the agent's step again from its start. The model turns, tool
calls, guardrail check and memory recall made before the question are recorded in the run's
checkpoint as they finish and replayed from it, so they are not billed or made again. Each recorded
call is one more checkpoint write to AgentCore Memory for an agent that can ask; an agent that cannot
ask records nothing.

Good to know:

- A conditional Cedar rule is evaluated by the Gateway when the call is made, which is after the
  person approved it, because AgentCore has no way to evaluate a policy without making the call. A
  tool the policy hides outright is never offered and never put to a person.
- `dedicated` and `a2a` agents cannot wait: `askPerson` is refused there, and so is binding a
  `dedicated` agent to a tool with `approval`.
- Questions are answered on the run page only; EventBridge (`approval: "event"`) works for tool
  approvals. A run started unattended (a trigger or a test suite with `gates: "auto"`) is told nobody
  is available, and refuses a tool call that needs approval.
- The images and files an agent reads (`vision`, `attachments`) are loaded again on resume, and the
  agent's "started" line appears again on the timeline.
- When two agents of one parallel stage both wait, the run page shows one at a time.

**Named options.** A gate with `hitl.options` (2 to 6 labels, such as `["Override", "Abort"]`)
offers those options instead of approve / revise / deny, and an agent with `askPerson.options` may
call `request_decision(question)` mid-step. The run page shows one button per option and an optional
comment; the choice is the run field `decision`, which the step's `branch.when` routes on
(`{"field": "decision", "equals": "Abort", "goto": "END"}`). It goes through the same
`POST /api/sessions/{id}/decision` route (`{"option": "Override", "comment": "..."}`), with the same
`decision` permission, and the activity log records who chose which option and when
(`run.decided`). No resource, IAM statement or setting is added. Not on a parallel stage, not with
`approval: "event"`, and the run assistant does not choose. Keys:
[WORKFLOW_REFERENCE.md](orchestrator/docs/WORKFLOW_REFERENCE.md#named-options--a-persons-choice-decides-what-runs-next).

### Teams and team workspaces (Builder console)
Teams decide who shares builds; what each person may do is still set by their groups
([Users, groups and RBAC](#users-groups-and-rbac)).

A team is a group of people who share a workspace of builds. There is nothing to turn on: no new
setting, resource or IAM permission. Teams live in the builds table under their own keys, and nothing
is read until someone opens **Teams** in the navigation or picks a workspace.

- **Who manages a team:** an admin (the `admin` action) creates and deletes teams, renames them, and
  adds, removes or changes owners. A team's owners add and remove its editors and viewers. A member is
  an email address or a group of your sign-in provider (the `groupsClaim` groups), and someone named
  more than once gets the highest of their roles.
- **What each role may do:** owners and editors open, change, deploy and destroy the team's builds.
  Deploying and destroying still need the `deploy` and `destroy` actions, as for anyone. Viewers open
  a team build and see its deployments, documents and secret names, but cannot save, deploy, destroy,
  share, move or delete it (403). Anyone not in the team gets 404 for its builds, as for any build they
  may not know exists. An admin reads every build, as before, but edits a team's builds only as a member.
- **Workspaces:** once someone is in a team, a switcher above the builds list offers **My builds** and
  each of their teams. A build made while a team is chosen is created in that team (editors and owners
  only). **Project → Move to team…** moves a build into a team where the caller is an editor or owner,
  or between two such teams. Only the person who created a build can take it back to their own builds.
- **The creator keeps access:** a build's creator never changes. A team build keeps its creator's
  Terraform state path and its deployed app's invitations, and still deploys into the creator's
  connected AWS accounts or this console's own. The creator keeps full access wherever the build is,
  as an owner does through any share. Sharing a team build with someone outside the team still works.
- **The log:** `team.created`, `team.updated` (who was added, removed or given a new role),
  `team.deleted` and `build.moved` are in the Activity log of whoever made the change.
- **Good to know:** teams apply to builds in the console; a deployed build's own app and its users
  are not affected. A team that still has builds cannot be deleted; move or delete them first.

## Secrets

Tool and data-source types are covered in [GETTING_STARTED.md](GETTING_STARTED.md) and
[orchestrator/docs/WORKFLOW_REFERENCE.md](orchestrator/docs/WORKFLOW_REFERENCE.md). Secrets never go
in `workflow.json` or any file; pass them as JSON objects in the environment:

| Secret | Keyed by | Terraform | CDK |
|---|---|---|---|
| Tool API keys | tool name | `TF_VAR_tool_api_keys` | `TOOL_API_KEYS` |
| Identity secrets (OAuth client secret or API key of an `identities` entry) | identity name | `TF_VAR_identity_secrets` | `IDENTITY_SECRETS` |
| Bearer tokens for `runtime: "a2a"` agents with `auth: "bearer"` | agent id | `TF_VAR_a2a_tokens` | `A2A_TOKENS` |
| Webhook signing secrets of `deliver` targets (`secret`) | secret name | `TF_VAR_deliver_secrets` | `DELIVER_SECRETS` |
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
- A deliver target's signing secret reaches the orchestrator runtime in its runtime secret
  (`DELIVER_SECRETS`), only the names a webhook target references. In the Builder it is a build
  secret of kind `deliverSecrets`, entered under **Signing secret** in the step's panel. The deploy
  fails when a target names a secret the build does not have
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

**Termination protection (CDK).** A CDK-deployed control plane (`consoleMode=builder`) has
CloudFormation termination protection on, and every deploy keeps it on. Workflow apps
(`consoleMode=app`) and the build stacks a console deploys (`ax-*-stack`) stay unprotected, so the
console's Destroy works. `-c terminationProtection=true|false` overrides the default. Terraform has no
equivalent. See [Tear down](#tear-down) to destroy a protected console.

### Tags on the builds a console deploys

`-c tags` (Terraform `tags`) tags the console only. Each build it deploys gets `agentexpress:app`,
`agentexpress:build`, `agentexpress:version`, `agentexpress:owner` and `agentexpress:console` from
the framework. To add your own tags to every build, set them on the console:

```bash
npx cdk deploy ... -c consoleMode=builder -c buildTags='{"cost-center":"1234","team":"data"}'
```

Terraform: `build_tags = { "cost-center" = "1234", team = "data" }`.

The console's deploy project carries them as `BUILD_EXTRA_TAGS`, and the deploy runner adds them to
every build, deployed with CDK or Terraform, in the console's own account or a connected one. The
framework's `agentexpress:*` tags always win.

The rules:

- keys and values use only ASCII letters, digits, spaces and `_ . : / = + - @`. AWS allows Unicode
  letters in general, but Amazon Bedrock's tag pattern is ASCII only, and a tag one resource refuses
  fails the whole build deploy;
- a key is 1 to 128 characters, a value up to 256;
- no key starts with `aws:` or `agentexpress:`, in any case;
- no two keys differ only in case (IAM treats `Team` and `team` as one tag);
- at most 40 tags.

An invalid value fails the synth or plan with a message naming the rule. If the deploy project's
`BUILD_EXTRA_TAGS` is edited by hand, the runner skips and prints any invalid entry (its key, never
its value) and deploys the build anyway.

New or changed tags reach a build at its next deploy: existing builds keep their old tags until
they are redeployed. On a Terraform build, the AgentCore runtimes and memories made with the awscc
provider get no provider default tags, as with every Terraform tag.

### The AgentExpress Assistant: links and Review changes

Both run in the BFF (`bff/webfetch.py`, `bff/review.py`) and need no new resource or IAM statement.

- **Links.** A message such as `build this based on <url>` makes the Assistant read the page with
  its `fetch_url` tool before it changes anything. Every fetch follows the address rules of
  `bff/netguard.py`, widened to `http://`: no `user:password@`, and refused when the
  host resolves to a loopback, link-local (including 169.254.169.254), private or CGNAT address,
  checked again on every redirect (at most 5). No proxy, cookie or `Authorization` header is sent,
  so a page that answers 401, 403 or 407 is reported as needing a sign-in.
- **Limits.** 10 seconds per fetch (wall clock, connecting included), 2 MB read, the first 40,000
  characters of text kept, 5 pages per message. HTML is turned into text without running scripts,
  so a page drawn by JavaScript comes back nearly empty. PDFs are not read yet.
- **Untrusted.** The page's text reaches the model wrapped as untrusted data, and the system prompt
  tells it never to follow instructions found there.
- **Review changes** (Off / Auto / Always in the Assistant panel, Auto by default, remembered per
  browser). A second model checks a proposed change before it is saved: one round, advisory only,
  and a failure or timeout never blocks the change. Auto reviews a new build, agents, stages, tools
  or named blocks added or removed, code changes, security-relevant settings, or more than 8 items
  changed (`LARGE_CHANGE`). The reviewer model is `designerReviewModel` / `designer_review_model`
  (`DESIGNER_REVIEW_MODEL` on the BFF, default `us.openai.gpt-6.1-sol`); the page never names it.
  The BFF's existing Bedrock grant (foundation models, and inference profiles in this region)
  already covers it. Its token counts are logged per call (`[designer] review ...`) and not metered
  in Observability.

### Connectors

**Add a connector** under **Tools** in the Build view (in the left pane and on the Tools tab; also **Use a connector** in **Add a tool**) opens
a gallery of ready-made tools for major services. Pick one, give its site or tenant and the OAuth
client ID, and it adds an ordinary `type: "openapi"` tool: an inline `schema` with a few of the
vendor's documented REST operations, `auth` `"user"` (each person connects their own account) or
`"oauth2"` (the app's own client), and `oauth`. Then set the client secret on the tool under
**How it connects**, and for `"user"` register the callback URL the tool shows after the first
deploy (see [Tools that act as the person](#tools-that-act-as-the-person)). The tool is editable
afterwards like any other. Each publishes several operations, so give it to an agent whose
`toolMode` is `"model"`; a `"direct"` agent needs `call` set to one of them.

| Connector | You supply | Signs in | What it can do |
|---|---|---|---|
| Salesforce | My Domain host (`acme.my.salesforce.com`), client ID | each person, or app (Client Credentials Flow) | SOQL query; read, create, update a record |
| Slack | client ID | app install (bot token) | list channels, read history, post a message |
| Jira | cloud ID, service account's client ID | app (service account) | JQL search, read and create issues, comment |
| Confluence | cloud ID, service account's client ID | app (service account) | CQL search, read a page, list spaces, create a page |
| ServiceNow | instance name, client ID | each person, or app (client credentials, Washington DC+) | Table API list, read, create, update |
| GitHub | client ID (OAuth app) | each person | search issues/PRs, read and open issues, comment |
| Microsoft 365 | tenant, client ID (Entra app) | each person | Microsoft Search, calendar view, send mail, post in Teams |
| Google Workspace | client ID | each person | search Drive files, file details, calendar events |
| Zendesk | subdomain, client ID | app (client credentials) | search, read, create, update tickets |
| HubSpot | client ID | each person | search and read contacts, companies, deals; create a contact |

Connectors are data in `orchestrator/web/src/builder/connectorCatalog.ts`, read by the page only:
they add no key, route, resource or runtime code, and a build that uses none is unchanged. The
values you type are checked against each vendor's own domain before they go into a URL.

Good to know:

- Each connector covers a few common operations of the vendor's REST API. Edit the tool to add more.
- Jira and Confluence sign in with a service account. With Google, people may need to connect again
  after a while.

### How a build deploys

Deploying a build starts the console's CodeBuild project (`<agent>-deploy`, ARM, 2-hour timeout)
over a fresh copy of the framework source. `deployer/runner.py` fetches the build's frozen version,
runs `scaffold.py apply --exact`, and deploys it as its own stack named from the build's `ax_<id>`
agent name, with the tool the user picked:

| | CDK | Terraform |
|---|---|---|
| Settings | `-c agentName=ax_<id> -c idp=cognito -c createCognito=true -c enableGateway=<workflow has tools> -c builder=false` | `agent_name = "ax_<id>"`, `idp = "cognito"`, `cognito = { create = true }`, `enable_gateway = <workflow has tools>`, `enable_builder = false` |
| Sign-in elsewhere | `-c idp=okta -c oktaDomain=… -c oktaClientId=…` (or the `auth0*` / `entra*` keys) instead of `createCognito` | `idp = "okta"`, `okta = { domain, client_id }` (or `auth0` / `entra`) instead of `cognito` |
| Deploys as | CDK bootstrap roles | `deploy-role-policy.json` plus `ReadOnlyAccess` |
| State | CloudFormation | the console's builds bucket |

- Build secrets live in Secrets Manager under `agentexpress/<console agent name>/builds/<build id>`
  and are passed as the [Secrets](#secrets) variables
- Who signs in to the build's app is chosen on its Identity tab, Sign-in (`authorization.signIn`
  in its workflow): built in (Cognito) by default, or Okta, Entra ID or Auth0. For the others the
  tab shows the two values to copy from the provider and, after a deploy, the address to register
  there. For built in there is nothing to set up, and the tab says what the deploy creates instead:
  the Amazon Cognito user pool for that app, its password rules, who is invited, which group the
  pool gets per role in `authorization.actions`, the pool id with a link to it in the Cognito
  console once deployed, and that a destroy removes the pool and every login in it
- After a deploy with built-in sign-in, the owner is invited into the build's app with a temporary
  password. With Okta, Entra ID or Auth0 there is no invite: people sign in with the provider
- A CDK build in the console's own account needs that account and region CDK-bootstrapped; a
  connected account with no bootstrap is bootstrapped on its first deploy
- A CDK build stack whose first create failed (`ROLLBACK_COMPLETE`, `ROLLBACK_FAILED`, or
  `DELETE_FAILED` after one) is cleared before the deploy and created again. The runner first proves
  the stack never deployed: its whole event history (read by StackId) starts with its create, shows a
  rollback, and has no `CREATE_COMPLETE`, `UPDATE_*` or `IMPORT_*`, and the build has no deploy on
  record in that account and Region. Then it empties only that stack's own framework buckets (an
  `AWS::S3::Bucket` of the stack whose name is one of the build's bucket names, or its UI logs
  bucket), with `ExpectedBucketOwner`, deletes the stack through the CDK deploy role and waits, and
  tries again if the delete ends `DELETE_FAILED`. Storage kept with `retain` is not emptied. Each
  step is a `runner:` line in the deploy log. A stack that ever deployed, or whose history cannot be
  read, is never emptied or deleted this way: the deploy fails and says to destroy the build or
  clear the stack by hand
- The UI, Knowledge Base documents and tool schema uploads each wait for their bucket's
  auto-delete resource, so a rollback can always empty and delete those buckets
  (`cdk/lib/auto-delete.ts`). Terraform does not roll back: a failed apply keeps its state, the
  next apply carries on from it, and every bucket there already has `force_destroy`

### Code from files, git or S3 (code sources)
A Lambda tool written in the build, a Gateway interceptor written in it, a Run code step and a
[hosted MCP server](#hosted-mcp-servers) can take their code from several places. Pick one with
**Where the code comes from** (**Where the script comes from** on a Run code step):

| Source | Kept in the build | When it is read |
|---|---|---|
| Written here | the files (`toolCode`), or `task.code` | exported with the build |
| Upload | files or a zip, unpacked in the page into the same files (`*.py`, `requirements.txt`, `events.json`; 20 files, 500 KB). A Run code step takes one script, or a zip with exactly one | exported with the build |
| Git repository | `code.codeSource` / `task.codeSource` / `hosted.code`: `{"git": {"url", "ref", "path", "token"}}` | fetched by the deploy |
| Amazon S3 | `{"s3": {"uri": "s3://bucket/prefix/" or "s3://bucket/key.zip", "path"}}` | read by the deploy |
| Container image | hosted MCP servers only: `hosted.code = {"image": "<account>.dkr.ecr.<region>.amazonaws.com/<repo>[:tag or @sha256:digest]"}`, a private ECR image | the Runtime's container; its role may pull from that repository only |

- Git: `url` is `https://` only. `ref` is a branch, a tag or a commit; pin a tag or a commit if a
  deploy must not change by itself. `path` is the folder with `handler.py` at its root (for Run
  code, the script file). The deploy runs `git fetch --depth 1 origin <ref>` and checks it out, so
  a commit works when the host lets a client fetch a commit by its id
- A private repository: set `token` to a name, then paste the token in the field that appears (a
  build secret of kind `gitTokens`, `user:token` or a token sent as `x-access-token`). It is never in
  `workflow.json`, never on a command line and never in the deploy log
- S3: the deploy reads with the credentials it deploys with. In a connected account it assumes
  that account's deploy role again with an
  [STS session policy](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html)
  that allows only `s3:GetObject` on that prefix (or that zip) and `s3:ListBucket` on the bucket
  with an `s3:prefix` condition. Both the console's deploy role and a connected account's deploy
  role already hold `ReadOnlyAccess`, so no IAM change is needed. A bucket policy or a KMS key on
  your bucket must allow that role
- S3 for a build in the console's own account (no connected account): only buckets listed in
  `-c codeSourceBuckets=a,b` / `code_source_buckets = ["a", "b"]` (empty by default, so S3 code
  sources need a connected account), and never the console's own buckets (builds, CDK staging, or
  any bucket tagged `agentexpress:*`). The deploy is refused before it starts otherwise
- Limits: 250 MB unzipped and 10,000 files for a function (the
  [Lambda deployment package limit](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html)),
  50,000 bytes for a Run code script. A zip member outside its folder, or a symbolic link, is refused
- A fetched function goes to `app/tools/_code/<key>/` before its `requirements.txt` is installed, so
  CDK and Terraform package it exactly like written code. A fetched script is deployed as the
  step's `task.code`. A destroy fetches nothing
- The Assistant knows the keys, and takes a file you attach to a message as a tool's file

### AWS Agent Registry
A Builder console works with the AWS Agent Registries in its own account and region
(`bff/registry.py`). Nothing to configure: with one registry it is used without asking; with
several the author picks.

- **Add from registry** (Tools tab, Skills tab, and the agent list): search what is APPROVED and
  take it into the build. An MCP server becomes an `mcp` tool with its endpoint and its tool list
  (choose How it connects afterwards), an A2A agent card a remote agent, a SKILL record a skill.
  A published AgentExpress workflow (an AGENT record with no A2A card) is listed with the reason
  it cannot be added: the workflow runs as its own app and serves no A2A endpoint. Its Gateway's
  MCP record carries no credentials, so a tool taken from it needs **How it connects** set to
  something that Gateway accepts.
  **Keep in sync** refreshes the item to each newer approved version when the build is opened and
  before each deploy; off, it stays as taken and the Builder offers the update.
- **Publish to registry** (admins only: the `admin` action): the deployed build (an AGENT record
  describing the workflow and its app, and an MCP record for its Gateway's tools) or its skills
  (SKILL records, as their SKILL.md, whose frontmatter `description` is written quoted so any
  prose is valid YAML). Records are submitted for approval; the console never
  approves its own. Publishing again submits the next version of the same record, and the
  registry keeps serving the approved one meanwhile. Destroying the build deprecates its records.
  A name and version another build already published is refused with a 409 that says so: rename
  the skill, or publish from the build that owns the record.
- IAM: the console's BFF gets `agent-registry:ListRegistries`, and on `registry/*` the
  discoverable reads plus `CreateRegistryRecord`, `UpdateRegistryRecord`, `GetRegistryRecord`,
  `SubmitRegistryRecordForApproval`, `UpdateRegistryRecordStatus` and `TagResource` (a published
  record is tagged `agentexpress:published`). It never creates or deletes
  a registry.

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

One connection works in every Region of the account: the deploy role is an IAM role, and IAM is
global. A deploy picks its Region per build; nothing is connected again for another Region. To
move a build to another Region, destroy it, then deploy it in the new Region. A build deployed
from now on has the Region in its bucket names, so the new deploy never meets its old names. A
build deployed before keeps its names; deployed in another Region straight after its destroy, its
deploy can fail with `A conflicting conditional operation is currently in progress (409)` while S3
releases the old names. Wait 30 to 60 minutes and deploy again.

#### Bucket names and Regions
A general purpose bucket name is unique across a partition, and a deleted name may not be free
again at once ([bucket naming rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html)).
So the names a stack gives its own buckets come in two schemes, chosen with `-c bucketNaming`
(CDK) or `bucket_naming` (Terraform):

| Bucket | `legacy` (default) | `regional` |
|---|---|---|
| Assets (images, run files) | `agentcore-<name>-assets-<account>` | `agentcore-<name>-assets-<account>-<region>` |
| Build storage | `<name>-storage-<account>` | `<name>-storage-<account>-<region>` |
| Knowledge Base documents | `agentcore-<name>-kbdocs-<account>` | `agentcore-<name>-kbdocs-<account>-<region>` |
| Tool schemas | `agentcore-<name>-schemas-<account>` | `agentcore-<name>-schemas-<account>-<region>` |
| UI (CDK) | `agentcore-<name>-ui-<account>-<region>` | the same |
| UI and UI logs (Terraform) | `agentcore-<name>-ui-<account>`, `...-ui-logs-<account>` | with `-<region>` at the end |

`<name>` is the agent name with `_` as `-`, lower-cased in the regional scheme. A regional name
over 63 characters keeps its prefix, `-<kind>-<account>-<region>` and the first 8 hex digits of
the agent name's SHA-256, and cuts the agent name to fit: `agentcore-<cut name>-<digest>-<kind>-<account>-<region>`.
A console build's name (`ax_` and 8 hex digits) always fits whole, even in the longest Region
codes. A name S3 reserves (`xn--`, `sthree-`) fails the synth or plan. The three implementations
(`cdk/lib/bucket-names.ts`, `terraform/bucket_names.tf`, `bff/bucket_names.py`) are held to the
same vectors (`tests/fixtures/bucket_names.json`).

Never change the scheme of a deployed stack: CloudFormation and Terraform replace a bucket whose
name changes, and what it holds goes with it. That is why `legacy` is the default, and why
upgrading the framework renames nothing. The console's deploy runner records the scheme on the
build (`bucketNaming` on its record) the first time it deploys it, and passes it on every later
deploy and destroy: `regional` when nothing of the build is deployed, `legacy` when the build
has a deploy on record from before schemes were recorded, its CDK stack still exists, or it has
Terraform state. A destroy forgets the scheme, unless storage was kept with `retain` (its bucket
keeps its name). Self-deployers opt in on a new stack with `-c bucketNaming=regional` or
`bucket_naming = "regional"`. The IAM patterns in `deploy-role-policy.json` and the connected
account's role match both schemes; the console BFF's read of a build's images names both.

### Moving a build out of a console

Export the bundle from the Build view, apply it in a framework checkout, and deploy with the build
settings above (`builder=false` / `enable_builder = false`) and its secrets in the environment:

```bash
cd orchestrator
python3 scaffold.py apply <bundle>.json --exact
```

`--exact` mirrors the bundle: it also deletes agent folders, `app/tools/` folders and `kb_docs/`
corpora the bundle does not name.

### Test suites

A build can keep a **test suite**: a list of requests (cases), each with checks on what the run should
produce. Run it after a change to a prompt or a model, and the answer to "did that make it better or
worse?" is a pass rate and the list of what broke, not a feeling. The Build view has a **Tests** tab
to write cases with forms and run them. The same is available as routes: `GET` and `PUT
/api/builds/{id}/suites`, `POST /api/builds/{id}/suite-runs` and `GET /api/builds/{id}/suite-runs/{run}`.

- **Checks are fixed comparisons**, never a model's opinion. A run ends `done` (or `failed`, `denied`,
  `cancelled`). An agent ends `done` or `skipped`. A field of an agent's JSON output is `equals`, `in`,
  `contains`, `exists`, `gt`, `gte`, `lt` or `lte` a value, with the same words and the same comparison
  as a `branch` rule. An agent's text does or does not contain a phrase. The run costs less than a number
  of dollars, and its calls take less than a number of milliseconds (the **sum** of every call's time,
  not the time on the clock). A case with no `run` check must also end `done`, so a run that failed at
  its second step does not pass a case that only reads the first agent's output. Scoring quality with an
  LLM judge is a different feature, the Evals tab.
- **Running** starts one run per case in the **deployed** build, as you, through the same start that Start
  run uses. It needs the `start` permission and is held to `orchestrator.limits`. The gates approve
  themselves, as for a trigger, so an unattended run can finish. A case the limits refuse is shown as
  *not started* with the reason, and is not counted against the build. Nothing is scheduled and no worker
  waits: the result is worked out when it is read, from the runs themselves, so a build that never runs a
  suite costs nothing. You can write suites before the build is deployed.
- **The trend**: the last 20 runs of each suite are kept with the build version they ran on, so a pass
  rate by version can be drawn. A suite run is final once every case has ended; a run still going three
  hours after it started counts as failed.
- **Where they live**: in the build's draft (`suites`, beside `workflow` and `prompts`), so they are
  saved, exported, imported and frozen into each version's bundle with the build. `scaffold.py apply`
  ignores them, because they are not part of what deploys. Results are items in the console's builds
  table, in the build's own partition, and go with the build when it is deleted. Only the person who
  started a suite run reads its cases. Anyone who can open the build sees the trend (counts only).

Good to know:

- A tool that acts as the person (auth `user`) needs that person in the run, so a suite cannot use it.
- All cases (at most 50) start at once; a case that fails to start says why.
- Suites run from a console in the default `app` mode. A `builder`-mode console can store suites but
  holds no runs. For quality scored by a model, use the Evals tab.

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
| Cache an agent's prompt | `agents.<id>.promptCache` ([reference](orchestrator/docs/WORKFLOW_REFERENCE.md#promptcache-prompt-caching)); no IAM change, cache calls use `bedrock:InvokeModel` like any model call |
| Add a data source or agent | see [GETTING_STARTED.md](GETTING_STARTED.md) |
| Reduce span-indexing cost | `transaction_search_indexing_percentage` / `-c transactionSearchIndexingPercentage` (1% is free) |

- Guardrails and Cedar policy are generated from `workflow.json`. The policy permits exactly the
  declared `tools`; the `kb` entry's `restrictTo` pins retrieval to its corpora
- The build-wide `guardrail` block creates a guardrail only when it enforces something (Bedrock
  rejects an empty one), so with an empty block `guardrails.input/output` is a no-op unless the
  agent names a guardrail. Each `guardrails.<name>` entry is its own guardrail, applied with
  `agentcore.guardrails.use`. With none, `bedrock:ApplyGuardrail` targets a placeholder ARN

## Running code in a sandbox
An agent can run code it wrote when a `type: "lambda"` tool in `workflow.json` has
`"source": "codeInterpreter"`. A deploy then adds one Lambda function and its role, and
registers it on the Gateway; a deploy without such a tool adds nothing and costs nothing.

- The code runs in the AWS-managed AgentCore Code Interpreter, not in your account's
  compute. The function's role can start, use and stop a session on that one interpreter and
  write its own logs. It cannot read your tables, buckets or secrets.
- The interpreter keeps its default network access.
- It is model-written code: grant the tool only to agents that need it, and narrow it with a
  Cedar `policy` as for any tool. Every call is a billed Code Interpreter session.
- The tool entry to copy and the limits (code size, 30 s, output size) are in
  [WORKFLOW_REFERENCE.md](orchestrator/docs/WORKFLOW_REFERENCE.md#source-codeinterpreter--run-code-in-a-sandbox).
- In the Builder: **Add a tool → Type → Code Interpreter** writes this tool for you.

## Opening web pages with a browser
An agent can open a web page and read its title, text and links when a `type: "lambda"` tool
has `"source": "browser"` (in the Builder: **Add a tool → Type → Browser**). A deploy then adds
one Lambda function and its role, and registers it on the Gateway; a deploy without such a tool
adds nothing and costs nothing.

- The page opens in the AWS-managed AgentCore Browser (`aws.browser.v1`), in AWS's network, not
  your VPC. The function's role can start and stop sessions on that browser, connect to a
  session's automation stream and write its own logs. It cannot read your tables, buckets or
  secrets.
- Only `http://` and `https://` pages on public addresses are opened, checked again after a
  redirect. `allowDomains` narrows it to the domains you list.
- The page's text is untrusted input to the model. Give the tool to agents whose other tools
  cannot do harm, or put `approval` on those. Every call is a billed Browser session.
- It reads pages; it does not click, fill forms or sign in. The tool entry and the limits (20 s
  page load, 40 s per call, 20,000 characters of text) are in
  [WORKFLOW_REFERENCE.md](orchestrator/docs/WORKFLOW_REFERENCE.md#source-browser--open-a-web-page).

## Generating PDF, PPTX and DOCX files
An agent can write a PDF, PPTX or DOCX file from JSON data when a `type: "lambda"` tool has
`"source": "docgen"` (in the Builder: **Add a tool → Type → Document generator**). A deploy then
adds one Lambda function (60 s, 512 MB) and its role, and registers it on the Gateway; a deploy
without such a tool adds nothing and costs nothing.

- `formats` (required: `pdf`, `pptx`, `docx`), `templateS3Uri` (optional: one `.pptx`/`.docx`
  object, or a folder ending in `/`) and `outputPrefix` (optional: `s3://<bucket>/<prefix>/`).
  Both S3 keys also take `${storage}/...` ([build storage](#storage-created-for-a-build)). PDF always
  uses the built-in layout, as does any format without a template.
- Each file is saved with the run (`runs/<run>/outputs/<name>`, the run's **Files** tab), so the
  workflow must set `orchestrator.files`; the validators, the CDK synth and the Terraform plan refuse
  it otherwise. With `outputPrefix`, the file is also written to `<prefix><run>/<name>`.
- The function's role reads only the template object or folder (`s3:GetObject`), writes only
  `runs/*/outputs/*` in the assets bucket and objects under `outputPrefix`, and writes its own log
  group. No `s3:ListBucket`, no other bucket, table or secret. A template or output bucket encrypted
  with your own KMS key also needs that key's policy to allow the function's role.
- It returns the file's `s3://` URI and a presigned link for up to an hour; a link signed with a
  role's temporary credentials stops working when they expire.
- The libraries (`app/tools/docgen/requirements.txt`: python-pptx, python-docx, reportlab and their
  dependencies, under MIT, MIT-CMU, BSD or PSF licences) are pinned. A console deploy installs them for Lambda's
  Python 3.12 on x86_64. Deploying from a checkout, install them first; the CDK synth and the
  Terraform plan refuse to deploy without them:
  `cd orchestrator && python3 -m pip install -r app/tools/docgen/requirements.txt -t app/tools/docgen --platform manylinux2014_x86_64 --implementation cp --python-version 3.12 --only-binary=:all:`
- A harness agent cannot use it (the runtime sets the run on every other agent's call). The tool
  panel's **Test run with sample data** calls the deployed function once.
- The tool entry, template rules and limits are in
  [WORKFLOW_REFERENCE.md](orchestrator/docs/WORKFLOW_REFERENCE.md#source-docgen--the-document-generator-pdf-pptx-and-docx-files).

## Steps with no model (task steps)
An agent entry with `"runtime": "task"` and a `task` block is a step that calls a tool, runs a
script, reads a document or writes a file, with no model call. In the Builder, add one from the
palette (**+ New generic step**) and pick what it does in its panel; until a kind is picked the
step is an error and the build cannot deploy. It runs in the orchestrator's runtime; a deploy grants that runtime's
role only what the workflow's steps use:
- a `code` step: start, use and stop sessions on the AWS-managed Code Interpreter
  (`aws.codeinterpreter.v1`). The script runs in that sandbox, not in your account's compute, and
  each step is a billed session;
- an `extract` step: Textract `AnalyzeDocument`, `StartDocumentAnalysis` and `GetDocumentAnalysis`
  (Textract has no resource type for these, so the grant is on `*`). It reads the run's files from
  the assets bucket, which `attachments: true` already lets the runtime read. Textract is billed
  per page and is not offered in every region;
- `tool` and `output` steps need nothing new (the Gateway, and `orchestrator.files` for the file);
- a `deliver` step: what its targets need, below.

A workflow with no task step gets no new permission or resource. The keys and limits are in
[WORKFLOW_REFERENCE.md](orchestrator/docs/WORKFLOW_REFERENCE.md#runtime-task--a-step-with-no-model).

## Delivering a run's files
A task step with `"task": {"kind": "deliver", "targets": [...]}` sends the files the run's steps
saved (`runs/<session>/outputs/`, so the workflow needs `orchestrator.files`) to 1 to 10 targets:
`s3`, `email` (Amazon SES) or `webhook`. In the Builder: **+ New generic step**, then **Deliver
files** in **What it does**. Each target can narrow the files (`files` name patterns, `producedBy`
step ids) and hold a condition (`when`). To deliver only after a check passed or a person chose to
go on, put the step after that gate or branch. Every delivery, skip and failure is a timeline line,
and each delivery or failure an activity-log event (`run.delivered`, `run.delivery_failed`). A failed
target fails the step after the others were tried. The keys are in
[WORKFLOW_REFERENCE.md](orchestrator/docs/WORKFLOW_REFERENCE.md#deliver-targets).

What a deploy grants the orchestrator runtime's role, only with a deliver step:

| Sid | Grant |
|---|---|
| `DeliverReadRunFiles` | `s3:GetObject` on `<assets bucket>/runs/*/outputs/*` |
| `DeliverListRunFiles` | `s3:ListBucket` on the assets bucket, for `s3:prefix` `runs/*/outputs/*` |
| `DeliverS3Put` | `s3:PutObject` on `arn:aws:s3:::<bucket>/<prefix>*`, per `s3` target |
| `DeliverSesSend` | `ses:SendEmail` on each configured identity ARN, with `ses:FromAddress` pinned to the configured From addresses |

No statement is on `*`. The framework's own buckets are refused as targets. A target bucket
encrypted with your own KMS key also needs that key's policy to allow the runtime role. On
Terraform the statements are the policy `AgentCoreRuntimeDeliver-<agent_name>`.

**Set up SES for an email target.**

1. In the SES console, in the Region you send from, create and verify an identity: a domain (send
   from any address at it) or one email address
   ([Creating and verifying identities](https://docs.aws.amazon.com/ses/latest/dg/creating-identities.html)).
   A Region needs its own identity.
2. Put its ARN in `identityArn` (`arn:aws:ses:<region>:<account>:identity/<domain or address>`) and an
   address at that identity in `fromAddress`.
3. Write the recipients in `to` (1 to 50). A step's output adds recipients only with the explicit
   `recipientsFrom` opt-in, one `{{outputs.<step>.<field>}}`, still at most 50 together.

**The SES sandbox.** A new SES account is in the sandbox, per Region. There it may send only to
verified addresses and domains (or the SES mailbox simulator), at most 200 messages in 24 hours and
1 a second. Verify each recipient as an identity while you test, then request production access
([Request production access](https://docs.aws.amazon.com/ses/latest/dg/request-production-access.html)).
SES's own error is on the run's timeline as SES wrote it, with a hint: a "not verified" rejection
names the sandbox, and `AccessDenied` says the runtime may send only from the configured identity and
From address. The SES docs note that sandbox restrictions can prevent some IAM policies from working
as written ([Controlling access to SES](https://docs.aws.amazon.com/ses/latest/dg/control-user-access.html));
whether a sandbox send is also authorized against a recipient's identity has not been tested live.

Other limits: one message per email target per step run; the files are attached while they are at
most 25 MB together (SES v2 takes 40 MB per message after base64), and sent as links above that or
with `attach: false`; links last `linkExpiresMinutes` (default 60, at most 10,080) or until the
runtime's credentials expire, whichever is first.

**Webhook targets** post through the SSRF guard (`app/common/netguard.py`): `https://` only, no
private, loopback or link-local address, each redirect checked again, 10 s timeout, 2xx required.
With `secret`, the body is signed as the BFF's webhook triggers are (`X-AX-Timestamp`,
`X-AX-Signature: sha256=<HMAC-SHA256 of "<timestamp>.<body>">`); the secret is a build secret of
kind `deliverSecrets` ([Secrets](#secrets)).

## Storage created for a build
`"orchestrator": {"storage": {"create": true}}` (in the Builder: **Settings → Storage → Create
storage for this build**) makes the deploy create a bucket for the build,
`<agent name with _ as ->-storage-<account>`, with `-<region>` at the end for a build deployed with
[regional bucket names](#bucket-names-and-regions). Without it nothing is created, and the deploy is the
same as before (`cdk/test/storage-identity.test.ts`, `tests/test_v139_identity.py`).

- The bucket: all four Block Public Access settings on, a policy refusing plain HTTP, versioned,
  SSE-KMS with the build's own key and an S3 Bucket Key, tagged `agentexpress:code-tools=true` with
  ABAC on, and a lifecycle rule that expires noncurrent versions after 30 days, clears expired delete
  markers and aborts unfinished uploads after 7 days.
- Tools name it with the token `${storage}`, at the start of the value: a code tool's or
  interceptor's `code.grants.s3Prefix`, a `kb` tool's `s3Uri`, a Document generator's
  `templateS3Uri` and `outputPrefix`, and a deliver step's `s3` target `uri`. Both validators and
  both IaC paths refuse it anywhere else, or when the build creates no storage. Each tool is granted
  only the folder it names; nothing else is granted on the bucket. A code tool's grant also gets
  `kms:Decrypt` (and `kms:GenerateDataKey` for `readwrite`) on the build's key, through S3 only,
  while the bucket uses that key. The
  orchestrator runtime gets `STORAGE_BUCKET` to resolve a deliver target at run time.
- **Destroy:** by default the bucket is emptied, every version included, and deleted with the build.
  With `"retain": true` (**Keep it when the build is destroyed**) it is kept: CDK sets
  `RemovalPolicy.RETAIN`, and on Terraform the runner and `teardown.sh` remove it from the state
  before `terraform destroy`. A kept bucket uses SSE-S3 instead of the build's key, which is deleted
  with the build; objects written before `retain` was turned on keep the key they were written with.
  A later deploy of the same build finds the name taken: delete or import the kept bucket first.
- The deploy role needs `S3ManageBuildStorage` ([IAM to deploy](#iam-to-deploy)).
- The keys are in [WORKFLOW_REFERENCE.md](orchestrator/docs/WORKFLOW_REFERENCE.md#orchestratorstorage--a-bucket-created-for-the-build).

## Agents run by AgentCore Harness

An agent entry with `"runtime": "harness"` and `instructions` has no code: the deploy creates an
AgentCore Harness for it (`aws_bedrockagentcore_harness` in `terraform/harness.tf`,
`AWS::BedrockAgentCore::Harness` in `cdk/lib/harness.ts`), and the orchestrator invokes it with
`InvokeHarness` for that step. In the Builder, pick **harness** as the agent's runtime and write
its **Instructions**. Per harness agent the deploy adds:
- the harness, named `<agent_name>_<agent id>` (at most 40 characters), with memory off;
- its execution role `AgentCoreHarness-<agent_name>-<id>`: Bedrock model calls on models and
  inference profiles, the ECR Public pull of the managed image, its logs, traces and metrics, and
  its workload token. No memory, browser, code interpreter, Gateway or API keys;
- on the orchestrator's role, `InvokeHarness` and `InvokeAgentRuntime` on these harnesses only,
  and a `HARNESS_ARNS` variable.

The console's deploy role (`terraform/deploy-role-policy.json`) may now create, read, update and
delete harnesses and pass `AgentCoreHarness-*` roles. A workflow with no harness agent gets no
new resource, permission or variable. There is no separate harness charge; you pay for the model
calls and the AgentCore Runtime session behind it. With `harness.shell` the agent can run commands
and reach the internet from its session. The keys are in
[WORKFLOW_REFERENCE.md](orchestrator/docs/WORKFLOW_REFERENCE.md#runtime-harness--an-agent-run-by-agentcore-harness).

### What a harness agent can have

Everything below is off until the agent's entry asks for it, and each adds only its own
resources and grants. In the Builder, the agent's settings panel has a section for each.
- **Gateway tools**: the agent's `tool`. The deploy adds an OAuth2 credential provider
  `bedrock-agentcore-<agent_name>-hgw-<id>` holding the agent's Gateway machine client (its own
  with `orchestrator.gatewayIdentity: "perAgent"`), so the Gateway's Cedar policy and
  interceptors see it like any agent. Needs `enable_gateway` / `-c enableGateway=true` and a
  Cognito client-credentials client (the pool the deployment makes, or your own Cognito one);
  plan/synth refuses it otherwise, and refuses tools that act as the person.
- **MCP servers, browser, code interpreter** (`harness.mcp`, `harness.browser`,
  `harness.codeInterpreter`): no new resource; the role gets the browser or code interpreter
  session actions on the managed one or on your custom one's ARN.
- **Other model providers** (`harness.provider` `openai`, `gemini`, `litellm`): put the key in
  the build's secrets as an `identities` entry of type `apikey`, and name it in `harness.apiKey`;
  the deploy makes an API key credential provider `bedrock-agentcore-<agent_name>-hk-<name>`
  from it. Or give the ARN of one you made. The role may read exactly those keys.
- **Skills** (`harness.skills`): the AWS catalog, a Git repository (a private one's token as an
  `apikey` identity), or an S3 prefix (the role may read that prefix).
- **Memory** (`harness.memory`): one of the build's `memories`; the role may write and read that
  memory only.
- **Environment**: a container image (`harness.container`; pull on that repository only),
  session storage, EFS or S3 Files mounts (`harness.mounts`; mount through those access points
  only) and a VPC (`harness.network`; you provide the ECR and S3 VPC endpoints).
- **An endpoint** (`harness.endpoint`): an `AWS::BedrockAgentCore::HarnessEndpoint` /
  `awscc_bedrockagentcore_harness_endpoint`. The orchestrator calls it, and the role may invoke
  it. On Terraform it is re-created when the harness changes, so a run calling it during that
  apply fails. The console's deploy role may manage harness endpoints
  (`BedrockAgentCoreHarnessEndpoints` in `deploy-role-policy.json`).
- **A harness of your own** (`harness.arn`, in this account and region): nothing is created for
  that agent; the orchestrator's role may invoke exactly that ARN (and its endpoint).

## Running AgentCore Runtimes in your VPC

Every AgentCore Runtime is `PUBLIC` unless `workflow.json` says otherwise. `orchestrator.network`
puts the shared orchestrator Runtime in your VPC, and with it every `main` and `task` agent. A
`dedicated` agent sets its own `agents.<id>.network`; it does not inherit the orchestrator's.
Both take `{"mode": "VPC", "subnetIds": ["subnet-..."], "securityGroupIds": ["sg-..."]}` (1-16 of
each), and both can be set in the Builder: **Settings → Network** for the orchestrator, and the
**Network** section of a dedicated agent's panel. Harness agents keep `harness.network`, and a
[hosted MCP server](#hosted-mcp-servers) takes `tools.<key>.hosted.network`. An `a2a`
agent runs on its owner's network, so it has no setting here. The Assistant can set each of these
from chat. The keys are in
[WORKFLOW_REFERENCE.md](orchestrator/docs/WORKFLOW_REFERENCE.md#orchestrator).

What the AgentCore docs say the VPC needs
([VPC configuration](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/agentcore-vpc.html)):

- Subnets in the Availability Zone IDs AgentCore supports in your Region. A subnet in another AZ
  fails the create. For example, us-east-1 supports `use1-az1`, `use1-az2` and `use1-az4`. Check a
  subnet with `aws ec2 describe-subnets --subnet-ids <id> --query 'Subnets[0].AvailabilityZoneId'`.
  Two private subnets in different AZs are recommended.
- No internet access by default, even in a public subnet. For the internet, use private subnets
  that route `0.0.0.0/0` to a NAT gateway.
- These VPC endpoints. They are required without internet access, and recommended with a NAT
  gateway: `com.amazonaws.<region>.ecr.dkr`, `com.amazonaws.<region>.ecr.api`,
  `com.amazonaws.<region>.logs`, and the S3 gateway endpoint `com.amazonaws.<region>.s3`, which
  holds the image layers.
- Outbound security group rules that allow whatever the Runtime calls. It needs no inbound rule.

The docs list only those four endpoints. The agents also call other AWS services: Bedrock model
calls, DynamoDB, Secrets Manager, and AgentCore Memory, Identity and Gateway
([AgentCore VPC endpoints](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/vpc-interface-endpoints.html)).
`a2a` agents, LiteLLM and run export reach hosts on the internet. Without a NAT gateway, each of
these needs its own endpoint. Which endpoints a build needs has not been checked live.

AgentCore creates the network interfaces with the service-linked role
`AWSServiceRoleForBedrockAgentCoreNetwork`. The first VPC Runtime creates it, which needs
`iam:CreateServiceLinkedRole` for `network.bedrock-agentcore.amazonaws.com`
([service-linked roles](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/service-linked-roles.html)).
The console's deploy role already allows that (`IamServiceLinkedRoles` in
`terraform/deploy-role-policy.json`). The Runtimes' execution roles need nothing new.

Two more effects. VPC mode can make session startup slower. After a Runtime is deleted, its
network interface can remain for up to 8 hours, which blocks deleting its subnets or security
groups until it is gone.

## Hosted MCP servers

An MCP tool can name a server you already run (`endpoint`), or one the build hosts:
`tools.<key> = {"type": "mcp", "hosted": {"code": ..., "network": ...}}`. The deploy runs the
server on its own AgentCore Runtime with protocol `MCP` and registers it as an MCP server target of
the build's Gateway. Agents call its tools like any Gateway tool, so Cedar policies, interceptors,
guardrails and Observability work the same. In the Builder, pick **Host it here** in the tool's
**MCP server** panel. The Assistant can also write one.

- Code. Leave `code` out to write or upload `server.py`, `requirements.txt` and an optional
  `Dockerfile` in the build. Set `{"git": {...}}` or `{"s3": {...}}` to fetch it at deploy
  ([code sources](#code-from-files-git-or-s3-code-sources)). Set
  `{"image": "<account>.dkr.ecr.<region>.amazonaws.com/<repo>:<tag>"}` to use an image you built
  (a private ECR URI with a tag or a `sha256` digest). `image` is accepted only here. With no
  Dockerfile, the deploy builds an ARM64 Python 3.13 image that installs `requirements.txt` and
  runs `python server.py`; without a `server.py` at the root the deploy stops.
- Dependencies. The starter `requirements.txt`, from the canvas or the Assistant, pins
  `mcp==1.30.0` and `pydantic==2.13.5`, the versions the framework itself is tested with (an
  older MCP SDK fails to import with a current pydantic). Only `name==version` lines are installed.
- Contract. The server must be an ARM64 container that serves MCP over streamable HTTP on
  `0.0.0.0:8000` at `/mcp`, stateless by default
  ([MCP protocol contract](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-mcp-protocol-contract.html)).
  It must offer tools. The container publishes no port; only AgentCore reaches it.
- Auth. The Runtime keeps the default IAM SigV4 inbound auth. The Gateway signs with its own role
  for the `bedrock-agentcore` service, and that role alone is granted
  `bedrock-agentcore:InvokeAgentRuntime` on the server
  ([target authorization](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-building-adding-targets-authorization.html)).
  Leave `auth` and `service` unset. The target passes `Mcp-Session-Id` both ways, as the
  [MCP server target](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-target-MCPservers.html)
  page advises for a server on AgentCore Runtime.
- Network. `hosted.network` takes the same shape as an agent's (see the section above). It
  defaults to `PUBLIC`.
- Names. The Runtime is `<agentName>_mcp_<key>`, so a hosted tool key can be at most 32
  characters.
- Images. CDK builds it as a container asset; export `CDK_DOCKER=finch` to build with Finch.
  Terraform builds and pushes it to `agentcore-<agentName>-mcp-<key>` with `container_engine`
  (for example `"finch"`). The console's deploy uses the CodeBuild container engine, as for the
  orchestrator image.
- Tool names. The Gateway publishes each of the server's tools as `<key>___<tool>`, as for any
  MCP target; `call` takes the tool name without the `<key>___` part.
- Tools that change. The Gateway lists the server's tools when the target is created or updated.
  After you change them, redeploy, or call `SynchronizeGatewayTargets`.
- IAM. The deploy role may create and pass the server's role, `AgentCoreMcp-<agentName>-<key>`
  (`terraform/deploy-role-policy.json`). A connected account set up before this change needs
  **Connect** again: that updates its role in place (same stack, same External ID) to the current
  permissions.

Not in v1: AWS permissions for the server's own calls, the build's credential providers for its
outbound calls, a resource policy on the Runtime, and a separate setup for outside MCP clients.
Outside clients can already reach the Gateway, which serves the hosted server's tools, with a
token its JWT authorizer accepts.

## Limits on spend and runs

Anyone who can start a run spends model money. Two optional limits, per user, in `workflow.json`;
both are off until you set them, and a deployment without them behaves exactly as before:

```json
"orchestrator": { "limits": { "userDailyUsd": 5, "userRunsPerHour": 20, "monthlyUsd": 200 } }
```

| Setting | Refuses | Resets |
|---|---|---|
| `monthlyUsd` | any new run, or re-run, once the app has spent this much this month (everyone together) | the 1st, Eastern time |
| `userDailyUsd` | a new run, or a re-run, once that user has spent this much today | midnight Eastern time |
| `userRunsPerHour` | the run past this many started in the current clock hour | the top of the hour |

A refusal is HTTP 429 with the reason in the message ("Today's spend limit of $5.00 is reached
($5.02 used)…"). It applies to a person on Start run, to a trigger (as whoever it runs as: `runAs: "service"` is the
trigger's own user, `runAs: "owner"` spends from the build owner's budget), and to each build
started from the console. A re-run counts against the daily spend, not as a new run.

What to know before you rely on it:

- **A run stops at its next step, not mid-step.** A run let in under a spend limit that crosses it
  stops before its next agent starts: that step fails, saying which limit and when it resets, having
  done no work, and can be run again after the reset. A step already going finishes, so the overshoot
  is about one step. `userRunsPerHour` bounds how many runs start before spend catches up, so set both
  when the cost matters.
- **It fails open.** If the counters cannot be read the run starts, and the log says
  `[limits] not enforced`. It is a cost guard, not a security control.
- **The hour is a fixed clock hour**, not a sliding window.
- **With `idp = "none"` everyone shares one budget**, because nobody is identified.
- **Only starting and re-running are covered.** The in-app assistant, on-demand evaluations and
  Insights are not.
- **The figure is the one the observability views show**, so it is only as exact as the model
  prices behind it (a row priced from the fallback guess is marked `~$` there).

How it works: each metered call adds its cost to one small reserved row per user per day in the
telemetry table, and the BFF reads that row when a run starts. The runtime and the BFF may touch only
rows whose key starts `_limit#` (an IAM condition on both IaC paths), so the runtime still cannot edit
a telemetry row. The runtime writes each total only when its limit is set.

**What people see.** The Start run box shows what the signed-in user has used of each limit that is
set ("Today: $1.20 of $5.00 · This hour: 3 of 20 runs"), warns from 80% of a limit, and when a limit is
reached says which one and when it resets, and disables Start. The server enforces the same limits
either way. The figures come from `GET /api/limits`, read each time the box opens.

### Seeing the bill in AWS

Every resource this project creates carries the tag `agentexpress:app = <agent_name>` (and each
build's name is its own), so once you activate it as a cost allocation tag in Billing it scopes
Lambda, DynamoDB, CloudFront, AgentCore and the rest to one deployment. Activating a tag is an
account-level billing action that this project does not take for you.
To give every build a console deploys your own cost allocation tags, see
[Tags on the builds a console deploys](#tags-on-the-builds-a-console-deploys).

**It does not scope model spend.** Amazon Bedrock tokens are not billed to a resource this project
tags. AWS attributes them through an application inference profile, a Bedrock Project, or IAM
principal attribution (each deployment's runtime runs under its own role). See
[Track usage and costs in Amazon Bedrock](https://docs.aws.amazon.com/bedrock/latest/userguide/cost-management.html).
Until you set one of those up, Cost Explorer shows Bedrock for the whole account, and the
figure in this app's observability views is the per-deployment one.

## Serving the workflow over A2A

Another team's agent can run this deployment's workflow over the
[A2A protocol, v0.3.0](https://a2a-protocol.org/v0.3.0/specification/): it reads an Agent Card,
sends a message (which starts a run), polls the task and reads the result. Off by default; off
means no routes, no app client and no environment change.

| | Terraform | CDK |
|---|---|---|
| Turn it on | `expose_a2a = true` | `-c exposeA2a=true` |
| Who decides A2A runs' review gates | `a2a_approvers = ["reviewers"]` | `-c a2aApprovers=reviewers` |
| Callers' identity, when this deployment does not create the Cognito pool | `a2a_identity = { audience, scope, client_ids, issuer, token_url }` | `-c a2aIdentity='{"audience":...,"scope":...,"clientIds":[...],"issuer":...,"tokenUrl":...}'` |

**Who can reach what.** The routes are `GET /api/a2a/.well-known/agent-card.json` and
`POST /api/a2a`, on the app's URL and on the API endpoint. They have their own JWT authorizer.

- With `idp = cognito` and a pool this deployment creates, it also creates a Cognito app client
  (`<agent_name>-a2a`) that uses OAuth2 client credentials with the scope `a2a/invoke`. The A2A
  routes accept only that client's tokens and require the scope. People's sign-in tokens are
  refused there, and the A2A client's tokens are refused on every other route.
- With Auth0, Okta, Entra or your own Cognito pool, register a machine-to-machine client and a
  scope with your provider, then set `a2a_identity`: the `audience` of those tokens (the API
  identifier; it must not be the sign-in client's), the `scope`, the `client_ids` allowed to call,
  and the `issuer` if it is not the sign-in issuer (Okta: the authorization server that issues
  machine tokens; Entra: the v2.0 issuer of the API app). Until they are set, the plan or synth
  fails, and no route is created.
- `idp = none`, or a Builder control-plane console (which runs no workflow), cannot expose A2A:
  the plan or synth fails.
- The Agent Card is behind the same authorizer as the RPC route. A caller needs a token to discover
  the agent as well as to call it.
- An A2A client sees only the runs it started. People in `a2a_approvers` groups see A2A runs
  beside their own on the Runs page, and can decide, re-run or cancel them, as a service trigger's
  approvers can. Nobody else sees them (an admin's `?scope=all` view lists every run).

**Getting a token and calling it** (Cognito, the created client). The client secret stays in
Cognito and is not a Terraform or stack output:

```bash
POOL=$(terraform output -raw cognito_user_pool_id)     # CDK: the cognitoUserPoolId output
CLIENT=$(terraform output -raw a2a_client_id)          # CDK: a2aClientId
SECRET=$(aws cognito-idp describe-user-pool-client --user-pool-id "$POOL" --client-id "$CLIENT" \
  --query UserPoolClient.ClientSecret --output text)
TOKEN=$(curl -s -u "$CLIENT:$SECRET" -d grant_type=client_credentials -d scope=a2a/invoke \
  "$(terraform output -raw a2a_token_url)" | jq -r .access_token)
A2A=$(terraform output -raw a2a_endpoint)              # CDK: a2aEndpoint

curl -s -H "Authorization: Bearer $TOKEN" "$A2A/.well-known/agent-card.json"
curl -s -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' "$A2A" -d '{
  "jsonrpc": "2.0", "id": 1, "method": "message/send",
  "params": {"message": {"role": "user", "messageId": "m1",
             "parts": [{"kind": "text", "text": "Design a serverless data pipeline on AWS"}]}}}'
# -> {"result": {"kind": "task", "id": "<task id>", "status": {"state": "submitted"}, ...}}
curl -s -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' "$A2A" \
  -d '{"jsonrpc": "2.0", "id": 2, "method": "tasks/get", "params": {"id": "<task id>"}}'
```

Hand the client id and secret to the calling team through your secret store. An AgentExpress
deployment calling another one declares it as a `runtime: "a2a"` agent with `auth: "oauth2"` and an
outbound identity holding those credentials.

**What a run started over A2A is.**

- `message/send` starts a run and returns the task at once (`submitted`); it does not wait.
  `tasks/get` maps the run: `running` -> `submitted` until an agent starts, then `working`;
  waiting at a gate -> `input-required`; `done` -> `completed`, with the run's result as the task's
  one artifact; `denied` -> `rejected`; `failed` -> `failed`; `cancelled` -> `canceled`; anything
  else -> `unknown`. `tasks/cancel` cancels it as the console's Cancel does.
- **It costs money, and the limits apply.** Each message starts a full run on this account's
  Bedrock budget. The run is owned by `a2a:<client id>` and `orchestrator.limits` counts it under
  that name, so `userRunsPerHour` and `userDailyUsd` cap each A2A client like one user. A refusal is
  JSON-RPC error `-32010` with `data.httpStatus` 429 and the same message the console shows. The
  API's own throttle is the stage-wide one; the A2A routes have no separate throttle.
- **Gates wait for a person.** The task shows `input-required` with a message saying a person
  decides in the console. A message on that task (`taskId` set) is refused with `-32004`; the caller
  keeps polling. With no `a2a_approvers`, nobody can decide it and it waits until the gate's own
  timeout, if the workflow gives it one.
- The start, the end and a cancel are in the activity log under `a2a:<client id>`.
- A request is text (and `data` parts, read as JSON) of at most 10,000 characters, in a body of at
  most 64 KB. File parts are refused (`-32005`). Only `message/send`, `tasks/get` and `tasks/cancel`
  are served; streaming, push notifications and the authenticated extended card are not.

## Changing a prompt or model without redeploying (live config)

Off by default. Turn it on in `workflow.json` and deploy once:

```json
"orchestrator": { "liveConfig": true },
"authorization": { "actions": { "admin": ["admins"] } }
```

Admins then get a **Live config** page. For each agent that runs in this deployment (not a remote
`a2a` one) you can set its system prompt and its model settings (`model`, `temperature`, `topP`,
`maxTokens`, `stopSequences`), see what changes against the active version, and save the result as
a numbered version. Saving does not change anything that runs. **Activate** points new runs at a
version; activating an older one, or version 0 ("as deployed"), is the rollback. Each save and each
activation is in the Activity log, with who did it and the version before and after.

What a run does with it:

- A run takes the version that is active when it starts, and every step of it uses that version:
  after a review gate, on a re-run, in another container, and in a dedicated agent's own runtime. A
  run page shows `Config: config vN` when it did not run as deployed.
- A change reaches new runs within a few seconds (each BFF container trusts the pointer it read for
  up to 5 seconds).
- If the versions cannot be read, the run starts as deployed and the BFF logs
  `[liveconfig] using deployed config: <why>`. A run never fails because of this.

Good to know:

- **Only those six settings change.** Tools, gates, guardrails, memory, Cedar policies, identity and
  the topology are provisioned by the deploy, so a change to them needs a redeploy, and the page and
  the API refuse them by name.
- **A model must be one the runtimes may already call**: a foundation model, or an inference profile
  in this region and account. Custom, imported and provisioned models are refused, because the
  runtimes' IAM grant does not cover them.
- **The deployed prompt is not shown.** It is in the image (`app/subagents/<id>/prompts.py`), which
  the console cannot read. A version's prompt replaces it in full; leaving the prompt empty keeps it.
  The prompt is replaced where the agent passes its own `system_prompt` to `ctx.llm`, as every shipped
  and scaffolded agent does; an agent that builds its system text another way keeps its own, and the
  run's timeline says so.
Who can reach what: the routes (`/api/liveconfig...`) are behind the sign-in like every other API
route, and the BFF answers only callers in a group granted `admin`; anyone else gets 403, and with the
setting off an admin gets 404. Versions are reserved rows (`_liveconfig#...`) in the telemetry
table. With the setting on, the BFF gets one extra IAM statement that reaches only those rows; the
runtimes get nothing new, because a run's version travels in its start request and its saved state.

## Send runs to Langfuse, Datadog and others

Each run can go to the observability tool your team already uses: Langfuse, Datadog, Grafana
Cloud, Honeycomb, or any backend that accepts OTLP over HTTP (an OpenTelemetry Collector, for
example). Off by default: without `orchestrator.export` nothing is sent, read or created.
**Not run live yet**: everything below is checked by unit tests only, pending the owner's live test.

Turn it on in `workflow.json`, save the tool's key as a build secret, and deploy:

```json
"orchestrator": { "export": { "destination": "langfuse", "region": "us", "secret": "langfuse" } }
```

`secret` names a build secret of kind `exportHeaders`: a JSON object of the HTTP headers that carry
the key, for example `{"Authorization": "Basic <base64 of pk-lf-...:sk-lf-...>"}`. The Builder
saves it write-only, like the other build secrets; the deploy stores it in Secrets Manager and only
the orchestrator runtime may read it. Deploying yourself, pass the same map in the environment:
`export TF_VAR_export_headers='{"langfuse":"{\"Authorization\":\"Basic ...\"}"}'` (Terraform) or
`export EXPORT_HEADERS='...'` (CDK). `secret` can also be the full ARN of a Secrets Manager secret
you already have.

| `destination` | `region` | `endpoint` | Headers in the secret |
|---|---|---|---|
| `langfuse` | `us`, `eu` (default) or `self-hosted` | self-hosted only: your Langfuse host | `Authorization: Basic base64(public key:secret key)` |
| `datadog` | your site: `datadoghq.com` (default), `us3.datadoghq.com`, `us5.datadoghq.com`, `ap1.datadoghq.com`, `ap2.datadoghq.com`, `uk1.datadoghq.com` or `datadoghq.eu` | not used | `dd-api-key` |
| `grafana` | not used | your stack's OTLP endpoint, e.g. `https://otlp-gateway-prod-us-east-0.grafana.net/otlp` | `Authorization: Basic base64(instance id:token)` |
| `honeycomb` | `us` (default) or `eu` | not used | `x-honeycomb-team` |
| `otlp` | not used | your collector's https base URL (`/v1/traces` is added) | whatever it checks |

The addresses come from each vendor's documentation: [Langfuse](https://langfuse.com/integrations/native/opentelemetry),
[Datadog traces](https://docs.datadoghq.com/opentelemetry/setup/otlp_ingest/traces/) and
[metrics](https://docs.datadoghq.com/opentelemetry/setup/otlp_ingest/metrics/), [Honeycomb](https://docs.honeycomb.io/send-data/metrics/application/),
[Grafana Cloud](https://grafana.com/docs/grafana-cloud/send-data/otlp/otlp-format-considerations/).
The runtime adds `x-langfuse-ingestion-version: 4` for Langfuse (without it new traces can take up
to ten minutes to appear) and `dd-otlp-source: llmobs` for Datadog (it routes the spans to LLM
Observability), unless your headers already set them. Langfuse's Japan and HIPAA regions: use
`self-hosted` with `https://jp.cloud.langfuse.com` or `https://hipaa.cloud.langfuse.com`.

**What is sent, once a run ends** (done, failed, denied or cancelled; a run waiting at a gate is
sent when it ends). One trace per run, rebuilt from the run's telemetry rows, so agents on their
own runtimes are in it too:

- the run: session id, user id, workflow name and version, total tokens and cost;
- each agent run (a re-run is a second span with its version);
- each model call: model, provider, input and output tokens, cost in USD from this app's meter, why
  that model was chosen (OpenTelemetry GenAI attributes; Langfuse's own generation, usage and cost
  attributes for Langfuse);
- each tool call: tool, target, latency, status;
- guardrail checks, Cedar decisions, memory reads and writes and questions to a person, with their
  outcome (span events; for Langfuse, small guardrail and event observations);
- evaluation scores (`send.evals`, default on): Langfuse's scores API on the trace, elsewhere a
  `gen_ai.evaluation.result` event on the agent span. An evaluation run later sends its scores then;
- metrics (`send.metrics`, default on) to Datadog, Honeycomb and `otlp`: runs, failures, duration,
  cost and tokens per agent and model, as per-run delta sums. Not to Langfuse (it computes its own)
  or Grafana Cloud (its metrics store expects cumulative counters).

**Privacy.** These go to a third party, under its retention and access rules. By default no
prompt, answer, document, tool input or output, run request or result, or judge explanation is
sent. The user id (the sign-in name) and everything in the list above are. With
`"send": {"content": true}` prompts, answers, tool input and output and the run's request and result
leave your account and may hold personal data. Under the AWS shared responsibility model you decide
whether that is allowed (GDPR, HIPAA and similar rules) and agree terms with the vendor; leave it off
when in doubt. The key is read from Secrets Manager and appears in no log, error, span or
environment variable.

**Failures never touch a run.** The export runs after the run's final status is written, each
request waits at most 5 seconds and the whole export at most 20. The run records how it went
(`export.status` `sent`, `failed` with a short reason, or `paused`), and it can be sent again from
the run: the same trace and span ids are used, so Langfuse updates the trace rather than adding one
(metrics are not sent twice). An admin can pause sending for the whole app; while paused nothing
leaves. Every request goes through the same SSRF guard as the app's other outbound calls: https
only, and never to a private address.

Not covered: no retry queue (send a run again instead); Datadog's US1-FED and US2-FED sites; the
live OpenTelemetry spans themselves (CloudWatch keeps receiving those, unchanged; for a copy of them
use the deploy settings below).

### Sending traces to another OTLP backend

For a stack you deploy yourself, three deploy settings copy every live span the runtimes emit to
one OTLP/HTTP traces URL, with headers from a Secrets Manager secret. When `orchestrator.export` is
set it wins: plan and synth say so and these are not used.

| Setting | Terraform | CDK |
|---|---|---|
| The OTLP/HTTP **traces** URL, `https://` only (the full path, usually ending `/v1/traces`) | `otlp_endpoint` | `-c otlpEndpoint=` |
| Full ARN of a Secrets Manager secret holding the request headers | `otlp_headers_secret_arn` | `-c otlpHeadersSecretArn=` |
| Also send prompt and answer content (default `false`) | `otlp_include_content` | `-c otlpIncludeContent=true` |

The endpoint and the secret go together; plan or synth fails with one and not the other, with an
`http://` URL, or with a secret ARN missing its six-character suffix. The secret is a JSON object of
header name to value, read once when a container starts. Each runtime role may read that one secret
and, for a customer managed key, decrypt it only through Secrets Manager. For Langfuse (EU):
`otlp_endpoint = "https://cloud.langfuse.com/api/public/otel/v1/traces"` and a secret of
`{"Authorization":"Basic <base64 of pk:sk>","x-langfuse-ingestion-version":"4"}`.

By default each copied span loses its content (task input and output, tool arguments and results,
prompts, completions and messages, span events except `exception` with only its type, and the
status description); `otlp_include_content = true` sends the spans as CloudWatch receives them. A
bad setting turns this off for that container with one `[otlp-export] off: ...` log line; a failing
endpoint drops spans with at most one warning a minute and holds a flush for at most 3 seconds.
Builds deployed from a Builder console use `orchestrator.export` instead.

### Set it up in the Builder

A build sets this on its **Settings** tab, in the **Export** section (`orchestrator.export` in
workflow.json):

1. Turn on **Send runs to another tool** and pick **Langfuse**, **Datadog**, **Grafana Cloud**,
   **Honeycomb** or **Any OTLP backend**.
2. Fill in only what that tool needs. Each field says where to find its value in the tool:
   - Langfuse: the region (EU, US, or the address of a self-hosted Langfuse or another Langfuse
     region), the public and secret keys, and an optional project ID for **View in Langfuse** links.
   - Datadog: the site (US1 is `datadoghq.com`, EU is `datadoghq.eu`, and so on) and an API key.
   - Grafana Cloud: the OTLP endpoint, instance ID and token from your stack's OpenTelemetry tile.
   - Honeycomb: an ingest API key, and US or EU.
   - Any OTLP backend: its OTLP endpoint, with or without `/v1/traces` at the end, and its headers.

   Or turn on **Use an existing secret (ARN)** and give a Secrets Manager secret that holds the
   headers as a JSON object.
3. **Test connection** sends one small test trace from the console now and says what the tool
   answered, for example "Langfuse accepted a test trace" or "Langfuse refused the key (HTTP 401)".
4. Choose what to send. Traces are always sent. Evaluation scores and metrics are on by default.
   **Prompts and answers** are off by default; turning them on shows the shared-responsibility note
   above.

**Save** writes the setup into the build. The keys become the request headers each tool documents:

| Tool | Headers |
|---|---|
| Langfuse | `Authorization: Basic base64(public key:secret key)` and `x-langfuse-ingestion-version: 4` |
| Datadog | `dd-api-key` and `compute_stats: true` |
| Grafana Cloud | `Authorization: Basic base64(instance ID:token)` |
| Honeycomb | `x-honeycomb-team` |

They are saved as one of the build's write-only secrets, named after the address they are for
(`export-` followed by 16 hex digits of the address's SHA-256). The page never shows them again. A
test with no keys typed uses the saved ones only for that same address. The **Deployment** panel
shows where runs go, for example "Export: Langfuse (EU)". **Save to library** keeps the setup for
other builds without its keys; a build that uses it asks for its own keys, or keeps an existing
secret's ARN.

In the build's own app:

- Each run's page shows whether the run reached the tool: Sent, Failed, Off, Paused, or Not sent.
  It also has a **View in Langfuse** or **View in Datadog** link to the run's trace. The Langfuse
  link needs the project ID. The Datadog link opens the Trace Explorer filtered on the run's
  `trace_id`, because Datadog documents no direct link to one trace.
- An admin, or the person who started the run, can use **Re-send to <tool>**.
- Admins have an **Export** card on the Observability tab. It shows the destination, whether
  sending is on or paused, and the runs sent and failed in the last 24 hours with the last error.
  It has **Send test trace** and **Pause** or **Resume** buttons. Pausing writes the status-table
  row `_export#paused`. Every one of these actions is recorded in the activity log.

Not covered:

- The console's test needs outbound HTTPS from the console's API Lambda. It refuses a private,
  loopback or link-local address, and it allows at most 10 tests a minute per person.
- An existing secret's ARN can only be tested from the deployed app with **Send test trace**.
- There are no trace links for Grafana Cloud, Honeycomb or a generic OTLP backend.

## Using non-Bedrock models through LiteLLM

If you run a [LiteLLM proxy](https://docs.litellm.ai/docs/simple_proxy), an agent can use any model
it serves (OpenAI, Gemini, Mistral and others): set the agent's `model` to `litellm/<name>`, where
`<name>` is the model name the proxy lists at `GET /v1/models`, for example `litellm/gpt-4o`. Every
other agent stays on Bedrock. Off by default; a deployment without these settings gets no new
environment variables, no new permissions and no new calls.

| Setting | Terraform | CDK |
|---|---|---|
| The proxy's base URL, `https://` only, on a public address | `litellm_base_url` | `-c litellmBaseUrl=` |
| Full ARN of a Secrets Manager secret holding the proxy's virtual key | `litellm_key_secret_arn` | `-c litellmKeySecretArn=` |

```bash
aws secretsmanager create-secret --name agentexpress/litellm-key --secret-string "sk-..."
# Terraform (terraform.tfvars)
litellm_base_url       = "https://litellm.example.com"
litellm_key_secret_arn = "arn:aws:secretsmanager:us-east-1:111122223333:secret:agentexpress/litellm-key-AbCdEf"
# CDK
npx cdk deploy -c litellmBaseUrl=https://litellm.example.com \
  -c litellmKeySecretArn=arn:aws:secretsmanager:us-east-1:111122223333:secret:agentexpress/litellm-key-AbCdEf ...
```

The two settings go together; plan or synth fails with one and not the other, with an `http://` URL,
or with a secret ARN missing its six-character suffix. The secret holds the virtual key itself, or
`{"apiKey": "sk-..."}`. Give the key only the models your agents use: the proxy enforces that, and the
Build view's model picker lists exactly the key's models ("via LiteLLM"). The runtimes and the BFF read
the key once per process; it never appears in an environment variable, a log line or an error. Each
runtime role and the BFF's role may read that one secret, and decrypt it only through Secrets Manager.
A Builder console passes its own two settings to every build it deploys.

Each call goes to `POST /v1/chat/completions` with the key as a bearer token, through the same SSRF
guard as every other outbound call here: https only, and refused if the host resolves to a private,
loopback or link-local address. A call that times out (the same 300 seconds as a Bedrock call), or
that the proxy answers with 408, 429 or a 5xx, is tried once more; anything else fails the agent with
the proxy's message.

What still works on a `litellm/` agent: guardrails (ApplyGuardrail runs on the request and the
agent's output whatever its model), long-term memory, skills, `toolMode: "model"` (tool specs and
results go as OpenAI `tools` and `tool_calls`), `vision` and run attachments (images as image parts,
CSV, TXT, Markdown and HTML inline as text, PDF and Office files as `file` parts, which only some
providers read), truncation warnings, Stop, and the telemetry row of every call. Tokens come from the
response's `usage`. Cost comes from the proxy's `x-litellm-response-cost` header when it tracks spend;
without it the row shows $0 with `rates_source = "none"`, never a guess. The agent's span still
carries its prompt, answer and token totals, so AgentCore Evaluations can score it; the judge itself
runs on Bedrock.

Good to know:

- `orchestrator.defaultModel` and evaluator judge models must stay Bedrock models (the validators
  refuse `litellm/` there); so does the AgentExpress Assistant.
- These calls show in the agent span and in the Cost and tokens view, not in AgentCore GenAI
  Observability's per-model-call view.
- Pick a proxy model that reads images or calls tools when the agent needs it; the picker cannot
  tell.
- The proxy must be on a public address: the runtimes run with public network access.
- A build deployed into another account reads the console's secret only if the secret's resource
  policy (and its KMS key policy, for a customer managed key) allows that account's roles.

## Choosing which model answers: per-run override, routing and fallbacks

Three workflow.json settings, all off unless set. None adds a resource, an IAM statement or a
call: the runtimes may already call any Bedrock foundation model and any inference profile in this
region and account, so every model named here must be one of those (or a `litellm/` model).

**Per-run override.** List the models people may pick in `orchestrator.models.overrides`, and grant
the `override` action to a group:

```json
"orchestrator": { "models": { "overrides": ["us.anthropic.claude-sonnet-4-5-20250929-v1:0"] } },
"authorization": { "actions": { "override": ["admins"] } }
```

Start run then shows, under Advanced, a model for every agent and a model per agent, chosen from that
list only. `override` is closed unless granted: without the grant nobody sees the choice, and the
server refuses a pick (403). It also refuses any model not on the list, a remote, harness or task
step, and a Builder build started from the console (400). The run stores the pick (the run page shows
"Model override"), keeps it through every review gate and re-run, and its agents use that model only,
with no routing and no fallback. Where the deployment keeps an activity log, its `run.started` event
carries the pick.

**Routing**, per agent, `agents.<id>.models.route`: rules checked on every model call, first match
wins, else the agent's model. `{"model": "...", "inputTokensOver": 20000}` matches when the call's
system and user text is over that many tokens, estimated at 4 characters per token.
`{"model": "...", "matches": "invoice|refund"}` matches when the run's request or subject matches
that regular expression, ignoring case.

**Fallbacks**, per agent, `agents.<id>.models.fallback`: up to 3 models tried in order when the
model is busy or not ready, which Bedrock reports as `ThrottlingException`,
`ServiceUnavailableException` or `ModelNotReadyException` (a LiteLLM proxy as HTTP 429 or 503).
Bedrock's own retry runs first. Any other error, such as a validation, access, timeout or guardrail
refusal, fails the step as before. The run's timeline says when a fallback is tried, and each call's
telemetry row records the model that was called and why (`chosen_by`: `route N`, `fallback N`,
`override`).

Good to know:

- `chosen_by` is on each call's telemetry row and on the run's timeline.
- Input size is estimated from the text; attached files and images are not counted.
- Write `matches` patterns without inline flags such as `(?i)`; matching already ignores case.
- Harness agents, remote (A2A) agents and task steps keep their own models.

## Tear down

With the Builder, destroy every deployed build from the console first: each build is its own stack,
and the builds bucket (holding Terraform builds' state) is deleted with the console. Export any
build you want to keep: destroying the console deletes the builds table.

```bash
terraform destroy                        # leaves Transaction Search and the state bucket
./teardown.sh                            # destroy, then both of those and .terraform (prompts)
FORCE=1 ./teardown.sh                    # no prompt
KEEP_TRANSACTION_SEARCH=1 ./teardown.sh  # leave the account-wide setting alone
cdk destroy                              # CDK; leaves Transaction Search enabled
```

**CDK control plane: turn termination protection off first.** A `consoleMode=builder` stack is
termination-protected, so `cdk destroy` fails until you clear it (use your stack's name,
`<agentName>-stack`):

```bash
aws cloudformation update-termination-protection --no-enable-termination-protection --stack-name agentexpress-stack
cdk destroy -c agentName=agentexpress -c consoleMode=builder
```

**Builds table protection.** The builds table has deletion protection on while the console runs. A
destroy you ask for clears it at the last moment, so `terraform destroy`, `./teardown.sh` and
`cdk destroy` each finish in one step. To make a destroy stop at the table instead, keep the
protection (`-var keep_builds_table_protected=true` / `-c keepBuildsTableProtected=true`), then
clear it on purpose with `aws dynamodb update-table --table-name <agentName>_builds
--no-deletion-protection-enabled`. Switching the setting is an in-place change. If clearing fails
(no `aws` CLI or no permission), the destroy stops at the table and prints the command to run.

**When a destroy fails**

| Symptom | Cause | Fix |
|---|---|---|
| `cdk destroy`: `DELETE_FAILED` on `Orchestrator` or another Runtime, `NotStabilized` ("Request timed out while deleting") | AgentCore was still draining the runtime | Run `cdk destroy` again. The console's destroy waits 60 s and destroys again itself, up to 3 more times. It reads every stack resource with `ListStackResources`, paged, so a failed Runtime is found on a stack with more than 100 resources too |
| `cdk destroy`: `DELETE_FAILED` on a Runtime, "Access denied for operation 'AWS::BedrockAgentCore::Runtime'", after a `NotStabilized` retry | AgentCore had already deleted the runtime (`aws bedrock-agentcore-control get-agent-runtime` returns `ResourceNotFoundException`) | Run `cdk destroy` again. The console's destroy does this itself, within its retry budget, once `GetAgentRuntime` confirms the runtime is gone |
| `cdk destroy`: `DELETE_FAILED` on `UiLogsBucket`, "bucket … not empty" | CloudFront delivered a late access log after the stack emptied the bucket | Empty that bucket and run `cdk destroy` again (see [A late CloudFront log](#a-late-cloudfront-log-stops-cdk-destroy) below). The console's destroy does this itself |
| `terraform destroy`: `NotStabilized` on `awscc_bedrockagentcore_runtime`, or Cloud Control's "Request timed out while deleting AWS::BedrockAgentCore::Runtime" with no resource address | AgentCore was still deleting the runtime when Cloud Control gave up | Run `terraform destroy` again. The console's destroy waits 60 s and destroys again itself, up to 3 more times, rebuilding the UI step first |
| `terraform destroy`: `NotStabilized` on `awscc_bedrockagentcore_runtime`, then `Invalid for_each argument ... data.external.ui_dist.result` on the retry | The first destroy removed the UI build step from state | `terraform apply -target=null_resource.ui_build`, then `terraform destroy` |

Terraform empties the UI logs bucket in the same step that deletes it (`force_destroy`), so the
late-log case is rare there; the same fix applies.

#### A late CloudFront log stops `cdk destroy`

CloudFront writes access logs a few minutes after a request. If one arrives after the stack has
emptied its UI logs bucket, a `cdk destroy` from your laptop ends `DELETE_FAILED` on `UiLogsBucket`
("The bucket you tried to delete is not empty"). Running the destroy again alone does not help,
because the stack empties the bucket only once. Look up the bucket's name in the stack's resources,
empty it, then destroy again (use your stack's name: `<agentName>-stack`, with `_` written `-`):

```bash
STACK=<agent-name>-stack                 # agentName with _ as -
BUCKET=$(aws cloudformation describe-stack-resources --stack-name "$STACK" \
  --query "StackResources[?ResourceType=='AWS::S3::Bucket' && starts_with(LogicalResourceId, 'UiLogsBucket')].PhysicalResourceId" \
  --output text)
aws s3 rm "s3://$BUCKET" --recursive
cdk destroy                              # with the same -c flags you deployed with
```

Only that one bucket is emptied: it holds CloudFront access logs and nothing else. The console's
destroy does the same thing itself and then retries.

**Check what a destroy left behind.** A failed or interrupted apply or destroy can leave resources
the state no longer knows about, and they block the next deploy under the same name:

```bash
./orchestrator/leftovers.sh <agent_name> [region]     # teardown.sh runs it; use it after cdk destroy too
```

- It lists what is still tagged `agentexpress:app = <agent_name>`, so it is safe in a shared
  account. It **only reports**; it deletes nothing.
- Exit `10`: something found; `1`: could not look (never read as "nothing left").
- It sees only taggable types, and a just-deleted resource can be listed for a few minutes. The
  stack's KMS key waits 7 days for deletion and is listed apart, not counted.
- A build's storage bucket kept with `orchestrator.storage.retain` stays on purpose
  ([build storage](#storage-created-for-a-build)).

`teardown.sh` is best-effort and idempotent. A re-deploy gets a new UI URL; re-create your login
user. Log groups of Lambdas this project creates are removed (retention `log_retention_days` /
`LOG_RETENTION`, default 365 days). Left behind are log groups it does not create
(`/aws/bedrock-agentcore/runtimes/<runtime>-DEFAULT`, framework helper Lambdas):

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
| Login redirect mismatch | Add the UI URL to the client's callback and sign-out URLs (Auth0: also web origins; Entra ID: as a Single-page application redirect URI) |
| "Could not sign you in" page | The provider refused the sign-in; its reason is shown (e.g. Entra `AADSTS50105`: the user is not assigned to the app) |
| Gate buttons greyed out, API 403 | Add the user to a group, then sign out and back in |
| Every gated action denied | Auth0, Okta, Entra ID: set `authorization.groupsClaim`. `idp = none`: remove `authorization.actions` |
| Tool-bound agents fail with `ToolUnavailable` | Enable the tool plane, or check the target's `tools/list` |
| Plan/synth names a missing value or secret | Supply it (see [Identity provider](#identity-provider), [Secrets](#secrets)) |
