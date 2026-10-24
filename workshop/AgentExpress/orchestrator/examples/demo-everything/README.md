# Demo: everything in one run

One build, one run: an Acme warehouse robot warranty claim, handled with every tool type and
AgentCore feature this framework deploys. It deploys from the Builder console with CDK or with
Terraform, in us-east-1, and needs no account outside the dev AWS account.

The short talk track is in [DEMO_SCRIPT.md](DEMO_SCRIPT.md).

## Files

| File | What it is |
|---|---|
| `project.json` | The body for `PUT /api/builds/{id}`: `{"project": {"name", "workflow", "prompts", "toolCode"}}`. Deployable as it is. |
| `bundle.json` | The same build as a Builder bundle: **Build view → Import a file → As a new build**. |
| `project.byo-harness.json`, `bundle.byo-harness.json` | The same, plus one agent that uses a harness you made elsewhere, by ARN. **Replace the placeholder first** (below). |
| `make_project.py` | Writes the four JSON files. Edit this, not the JSON, then run `python3 examples/demo-everything/make_project.py` from `orchestrator/`. The interceptors' code comes from the BFF's own templates (`bff/interceptor_code.py`), so it is what the Build view writes. |

## Load and deploy

1. Load it: Builder → **Import a file** → `bundle.json`, or `PUT /api/builds/<id>` with
   `project.json` as the body (`<id>` is 6 to 40 lowercase letters and digits).
2. Deploy: **Deploy → CDK** or **Deploy → Terraform**. Same file for both; nothing in it is
   path-specific (the web search sets no `domains.include`, which a CDK deploy refuses).
3. Open the deployed app. The owner is in every group, so the `admins` actions (live config,
   model override, audit) work for them.

### Inputs

The build reads three objects from one S3 bucket you own, in the deploy's account. The JSON files
here name the placeholder `amzn-s3-demo-bucket`: upload the objects to your bucket, then write the
files for it with `AX_DEMO_BUCKET=<your-bucket> python3 examples/demo-everything/make_project.py`
(from `orchestrator/`).

| Object | Used by |
|---|---|
| `inputs/invoice.pdf` | the request names it; **Read the invoice** reads it with Textract |
| `docs/kbdoc.txt` and `docs/kbdoc.txt.metadata.json` (`doc_type: policy`) | the `warranty` Knowledge Base |
| `models/bedrock.json` | the `bedrockApi` Smithy tool: the AWS-published Bedrock model, [`bedrock-2023-04-20.json`](https://github.com/aws/api-models-aws/blob/main/models/bedrock/service/2023-04-20/bedrock-2023-04-20.json) (959,770 bytes). If it is gone, download that file and upload it there. |

### Harness by ARN (`project.byo-harness.json`)

Agent `byoHarness` has `harness.arn` set to
`arn:aws:bedrock-agentcore:us-east-1:000000000000:harness/REPLACE_ME-XXXXXXXXXX`. That has the
shape of an ARN so the file validates, but account `000000000000` does not exist: replace it
with the ARN of a harness in the deploy's account and region before you deploy (for example
one an earlier build created, `harness/ax_<id>_<agent>-<10 chars>`), or that step fails.

## The request to type

The app's **Start run** box is pre-filled with it (`ui.defaultTopic`):

> Warranty claim CLM-1001: the customer's Acme R2 warehouse robot stopped with a battery fault on
> 2025-09-02. Their earlier claim was CLM-2002. The repair invoice is
> s3://<BUCKET>/inputs/invoice.pdf. Check the claim against the warranty policy and
> draft the reply to the customer.

Two people moments in the run: the Letter drafter asks **"Which language should the letter be
in?"** (answer `English`, or `Spanish` to show it follows), and the **Sign-off** gate waits for
**Approve**.

## What each step shows

| # | Step (id) | Runs as | Shows | What to look for |
|---|---|---|---|---|
| 1 | Claim intake (`intake`) | task, `code` | Code Interpreter task step; deployment guardrail on the input; a run file | `ageDays: 536`, `route: "standard"`; `claim-facts.json` and the script on **Files** |
| | branch on step 1 | | `branch` | `output.route equals 'standard'` → goes to **Evidence**; Fraud review is **skipped** |
| 2 | Fraud review (`fraudReview`) | main | the branch's other path | marked skipped. Put "suspected fraud" in the request to make it run |
| 3 | Evidence (`gather`, parallel): | | | |
| | Read the invoice (`readInvoice`) | task, `extract` | Textract; an S3 attachment named in the request | the invoice total, 42.50 |
| | Controller OS support (`osSupport`) | task, `tool` | `openapi` tool (inline schema) through the Gateway | Ubuntu releases; 22.04 support ends 2027-06-01 |
| | Policy check (`policyCheck`) | main | `kb` tool on your own bucket, fixed-size chunking, corpus `policy`; long-term memory `summary` | robot 3 years, battery 18 months: covered |
| | Research (`research`) | main, `toolMode: model`, Nova Pro | `websearch` and `mcp` (AWS Knowledge MCP Server); prompt caching (`promptCache` system and messages) | one web result and one AWS docs page, each with a link; cache reads on its later calls (Observability, **Cache saving**) |
| | Cover calculator (`coder`) | main, `toolMode: model` | `lambda` tool `source: codeInterpreter` | `daysOfCoverLeft: 12`; the code and its log on **Files** |
| | Service portal (`browse`) | main, `toolMode: model` | `lambda` tool `source: browser`, `allowDomains`, a Cedar permit narrowed to two URLs (`policy.restrictTo`) | title `Example Domain` |
| | Battery specialist (`specialist`) | **dedicated** runtime | an agent in its own AgentCore Runtime | three checks |
| | Sandbox harness (`ciHarness`) | **harness** | managed Code Interpreter and Browser, named endpoint `live` | `ageDays: 536`, title `Example Domain` |
| | Model card harness (`gwHarness`) | **harness** | Gateway tools from a harness, signed in through the OAuth2 credential provider (AgentCore Identity) the deploy makes; `smithy` tool (Bedrock `GetFoundationModel`, one exact ARN) | the model's name and provider (Claude 3 Haiku, Anthropic) |
| 4 | Letter drafter (`drafter`) | main, `toolMode: model` | `lambda` tool written in the build (`claimsDb`, `toolCode`); **Cedar forbid DENY**; response interceptor; `askPerson`; skill `claimLetter`; named memory `customerMemory` | `Refused by Cedar policy: claimsDb___get_claim(...)` for CLM-2002; the customer's email shows as `[EMAIL REDACTED]`; the question; `use_skill` |
| 5 | Sign-off (`signoff`, parallel, **HITL gate**): | | | |
| | Letter reviewer (`reviewer`) | main | model routing (`matches: "battery"` → Nova Pro, fallback Nova Lite); named guardrail `letterSafety` on the output; built-in evaluations, automatic | `chosen_by: route 1` in Observability; the phone number masked as `{PHONE}` in `finalLetter`; Helpfulness and Faithfulness scores after the run |
| | Outside compliance review (`compliance`) | **a2a** | the A2A protocol, SigV4, to the framework's stand-in agent (`source: a2a_lambda`, skill `compliance`) | an `a2a` chip; obligations and controls |
| 6 | Claim report (`report`) | task, `output` | an output task step; `orchestrator.files` | `claim-report-<session>.md` on **Files** |

Around the steps:

* **Interceptors**: the request interceptor logs each tool call with the run, agent and user that
  made it (`passRequestHeaders`), and refuses arguments matching
  `drop table` or `rm -rf`; the response interceptor masks email addresses in every tool answer.
* **Short-term memory** is the checkpointer: the run stops at the question and at the gate and
  resumes from it.
* **Limits** (`orchestrator.limits`): 25 USD a day and 10 runs an hour per user, 250 USD a month
  for the app.
* **Live config** is on; **model override** offers Haiku 4.5, Nova Pro and Nova Lite to admins.

## What to click in the demo

1. **Start run** with the pre-filled request. Optional: **Advanced → Model override →** Battery
   specialist → `us.amazon.nova-lite-v1:0` (the run page then shows "Model override").
2. **Graph**: the rows fill in; Fraud review turns *skipped*; the parallel Evidence row runs nine
   steps at once (ten in the harness-by-ARN variant), each with its tool chips and resource names.
3. **Timeline**: `Refused by Cedar policy: claimsDb___get_claim(...)` on the Letter drafter;
   the branch line on Claim intake.
4. Answer the drafter's question in the box on the run page.
5. **Flow**: who called what, in order (tools through the Gateway, the harnesses, the A2A call).
6. At **Sign-off**, read the reviewer's `finalLetter`: the phone number is `{PHONE}` (guardrail).
   **Approve**.
7. **Files**: `claim-facts.json`, the code the sandbox ran with its log, `claim-report-<session>.md`.
8. **Observability**: cost per step, the model each call used and why (`route 1`, `override`),
   the evaluation scores, guardrail interventions.
9. Optional, after the run: **Live config** → Policy check → edit the prompt → save and
   activate → the next run shows `config v1`. And a second run with `BLOCKED_DEMO_TERM` in the
   request is stopped at Claim intake by the deployment guardrail.

## Also available, not in this demo

| Feature | Why it is left out |
|---|---|
| `apigateway` tool | It registers a stage of a REST API that already exists in the account (`restApiId`, `stage`); the deploy does not create one. |
| `identities` (OAuth2 client credentials, API keys), `agentcore.identity.outbound`, tool `auth` `oauth2` / `user` / `obo` / `apikey` | Each needs a provider's client or key, a vendor account. AgentCore Identity still appears: the Model card harness signs in to the Gateway through the OAuth2 credential provider the deploy creates. |
| A third party's A2A agent | The demo calls the framework's own A2A stand-in, which speaks the same protocol, so it needs no outside agent. |
| Harness with OpenAI, Gemini or LiteLLM models, skills, memory, MCP servers, a custom container, S3 or EFS mounts, a VPC | Each needs a provider key or your own resources. |
| Tool `approval`, custom evaluators, triggers, image and vision agents, KB `vectorStore` / `dataSource`, `gatewayIdentity: perAgent`, web search `domains.include` | Each works on its own, but adds a wait, a vendor account or a Terraform-only setting to a single run. |

## Further reading

* [AWS Knowledge MCP Server](https://github.com/awslabs/mcp/blob/main/src/aws-knowledge-mcp-server/README.md)
* [AgentCore: action and resource scoped Cedar policies](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/example-policies-action-resource.html)
* [AgentCore harness](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/harness.html)
* [AWS service Smithy models](https://github.com/aws/api-models-aws)
