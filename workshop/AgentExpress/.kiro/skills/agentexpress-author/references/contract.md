# AgentExpress config contract

The exact shapes, taken from the shipped `orchestrator/app/workflow.json` and the agent
base class. `orchestrator/app/keys.json` is the authoritative source; this is the working
subset an author needs.

## Top-level keys

`$schema`, `$comment`, `orchestrator`, `ui`, `guardrail`, `authorization`, `tools`,
`agents`, `steps`, `skills`, and the named maps `guardrails`, `memories`, `evaluators`,
`identities` and `policies` (below).

## Tool types and their keys

Observed in the shipped config. `description` is required on every type; `type` selects
the rest.

| type | Keys used | Reaches |
|---|---|---|
| `kb` | `corpora`, `maxResults`, `policy`, or `s3Uri` / `knowledgeBaseId`; optional `chunking`, `parsing`, `rerank`, `vectorStore` (your own store), `dataSource` (web, Confluence, SharePoint, Salesforce; needs an OpenSearch Serverless store) | A Bedrock Knowledge Base. Each top-level folder in `kb_docs/` is a corpus |
| `websearch` | `maxResults`, `domains` | Web search (us-east-1, eu-west-1, ap-northeast-1 only) |
| `mcp` | `endpoint`, `call`, `arg`, `args`, `listingMode`, `auth` / `identity` | An existing MCP server |
| `openapi` | `schema` (inline), `schemaS3Uri` or `source`, `call`, `arg`, `auth` / `identity`, `rowFields`, `rowPath` | A REST API |
| `lambda` | `code` **or** `lambdaArn` **or** `source`, `call`, `arg`, `toolSchema`, `rowFields` | Anything else |
| `apigateway` | `restApiId`, `stage`, `toolFilters`, `toolOverrides`, `auth` (`sigv4`/`apikey`) / `identity` | An API Gateway REST API |
| `smithy` | `schemaS3Uri` or `schema` (the Smithy JSON model), `allow` (`actions` on `resources`) | An AWS service API (RestJson services only), signed by the Gateway role |

Any tool may also take `policies` (names in the build's `policies` map) and `identity` (a name in
`identities`, instead of `auth` + `oauth`). `auth` is `none`, `apikey` (sent as `X-API-Key`),
`sigv4` or `oauth2` (client credentials, `mcp` and `openapi` only). Two more act as the person
using the app (`mcp` and `openapi` only): `user` (each person connects their own account once;
with a `tokenUrl` also set `oauth.authorizationUrl`) and `obo` (the person's sign-in exchanged;
`oauth.audience` optional; not with Entra ID sign-in). An `mcp` tool in either must list its
tools in `toolSchema`.

`approval` (`"person"` or `"event"`, with optional `approvalTimeout`) on an `mcp`, `openapi`,
`lambda`, `apigateway` or `smithy` tool makes every call wait for a person. **Tools → Add a
connector** in the console writes a ready-made `openapi` tool (Salesforce, Slack, Jira, GitHub, …).

`skills` (optional, top level): `name -> {description, instructions, files?}`: know-how written
as a SKILL.md body, with text reference files. An agent lists the ones it may use in `skills`;
with `toolMode: "model"` it opens them itself (`use_skill`), otherwise they are added to its
instructions in full. Scripts are not run.

`registry` (on a tool, an agent or a skill): where the console took it from in an AWS Agent
Registry (`registryId`, `recordId`, `name`, `version`, `sync`). Written by the console, never by
hand; the runtime ignores it. Leave it as it is when editing the entry.

`rowFields` maps the source's own field names onto the ROLES an agent reads, so pointing a
tool at a different backend is a config edit rather than a code change.

### `lambda` with `code`: a function written in the build

The usual choice for a new function. The bundle carries it under `toolCode.<key>` and
`scaffold.py apply` writes it to `app/tools/_code/<key>/` (`handler.py` with
`lambda_handler(event, context)`, optional `requirements.txt`, test events). The framework
deploys it (Python 3.12) with a role of logs plus only the grants listed:

```json
"postChecker": {
  "type": "lambda", "description": "Word count, reading time and keyword counts for a draft.",
  "call": "check_post", "arg": "text",
  "toolSchema": [ { "name": "check_post", "description": "…", "properties": {
    "text": { "type": "string", "required": true, "description": "The draft." },
    "keywords": { "type": "array", "required": true, "description": "Keywords to count." } } } ],
  "code": { "grants": {}, "timeoutSeconds": 30, "memoryMB": 256 }
}
```

`grants` may name a `secret`, a `table` (`tableAccess` `read`|`readwrite`), an `s3Prefix`
(`s3Access`) and a `vpc`; the role is capped by a permissions boundary of the same, and a
grant only reaches a resource tagged `agentexpress:code-tools=true` (a bucket also needs S3 ABAC on). Nothing
else (no managed policies).

### `lambda`: `lambdaArn` for a function you already own, `source` only for shipped functions

**`source` is CLOSED for `type: "lambda"`.** It is enumerated by `builtinLambdaSource` in
`app/vocabulary.json`: `pricing` (the sample's price list tool), `codeInterpreter` (the agent
runs code it wrote in the AgentCore Code Interpreter) and `browser` (open a public page in the
AgentCore Browser; optional `allowDomains`). Each has a fixed execution role, which is right for
that function and wrong for a connector needing VPC config or a secret.

So for a customer's own Lambda tool:

```json
"partsPricing": {
  "type": "lambda",
  "description": "Unit prices for repair parts and labour rates.",
  "lambdaArn": "arn:aws:lambda:us-east-1:111122223333:function:claims-parts-pricing",
  "call": "price_parts",
  "arg": "items",
  "toolSchema": [ … ],
  "rowFields": { … }
}
```

The customer deploys and owns the function; the framework registers it as a Gateway target
and grants the Gateway permission to invoke it. It does not manage the code or the role.

**`source` for `type: "openapi"` is deliberately OPEN** — the framework only uploads a
file, which needs no permissions — so `app/tools/<source>/openapi.json` is the right home
for a customer's own spec.

### `toolSchema` is an ARRAY of tool descriptors

Not a JSON Schema object. One entry per tool the function publishes, and `required` sits on
each property rather than in a sibling list:

```json
"toolSchema": [
  {
    "name": "price_parts",
    "description": "Unit rates for the named parts or trades. Rates only, never a total.",
    "properties": {
      "items": {
        "type": "string",
        "required": true,
        "description": "Comma-separated part or trade names."
      }
    }
  }
]
```

### The Lambda handler contract

A function reached by `lambdaArn` is the customer's to write, but the Gateway calls it the
same way the shipped one is called:

```python
def price_parts(event: dict) -> dict:
    return {"results": [ … ], "unmatchedItems": [ … ], "note": "…"}

def _tool_name(context) -> str:
    """The Gateway passes the invoked tool's name on the client context; it arrives
    prefixed as <target>___<tool>, so split on the delimiter and take the last part."""

_TOOLS = {"price_parts": price_parts}

def lambda_handler(event, context):     # the entry point, not `handler`
    fn = _TOOLS.get(_tool_name(context) or next(iter(_TOOLS)))
    if fn is None:
        return {"error": "unknown tool", "availableTools": sorted(_TOOLS)}
    return fn(event or {})
```

Rows go under `results`, and `rowFields` maps their field names onto agent roles. Name every
shortfall — unmatched inputs, truncation — in its own field rather than dropping it, so the
calling agent can state what was *not* answered.

**Tool keys must match `^[A-Za-z][A-Za-z0-9]*$`** — the key builds both the Gateway target
name (no underscores allowed) and the Cedar policy name `permit_<key>` (no hyphens
allowed), so neither separator survives both. Agent ids are unconstrained this way and may
contain underscores.

## Agent entry

```json
"cost_research": {
  "name": "Cost Research",
  "runtime": "main",
  "produces": "research-finding",
  "maxTokens": 1500,
  "tool": "pricing",
  "agentcore": {
    "evaluations": {
      "enabled": true,
      "auto": false,
      "evaluators": ["Builtin.Faithfulness", "Builtin.ResponseRelevance"]
    },
    "policy": { "enabled": true }
  }
}
```

`runtime` is `main` (in-process), `dedicated` (its own AgentCore Runtime), `a2a` (an agent you
do not operate), `task` (a step with no model: `task: {"kind": "tool" | "code" | "extract" |
"output", ...}`) or `harness` (an AgentCore Harness: `instructions`, optional `harness` settings,
or `harness.arn` for an existing one). Only `main` and `dedicated` get a folder.

Other agent keys an author uses:

| Key | Meaning |
|---|---|
| `tool` | One tool key or a list |
| `toolMode` | `direct` (default: every tool queried once up front) or `model` (the model chooses calls, up to `maxToolCalls`). With `model`, the tool turn sees the agent's prompt and the approved upstream outputs, so "always call X" and "run the checker on the draft" work |
| `corpus` | The KB corpus this agent may read |
| `model`, `temperature`, `topP`, `stopSequences`, `maxTokens` | Model settings; `model` defaults to `orchestrator.defaultModel` |
| `framework` | `plain`, `strands` or `langgraph` — how `agent.py` is written |
| `output: "image"`, `image` | An image agent: its model writes a brief that Stability renders (us-west-2 today; outside the US needs `image.allowCrossRegion: true`) |
| `vision: {"from": ["<agent>"], "maxImages": n}` | Read the images an earlier agent produced; needs a model that takes images |
| `attachments: true` | Read the files a run is started with (uploads, or `s3://` paths inside `orchestrator.attachments.s3`); not on `a2a` agents |
| `maxToolCalls` | With `toolMode: "model"`, the most tool calls (default 6) |
| `skills` | Names in the top-level `skills` map |
| `askPerson` | `true` or `{"maxQuestions", "timeout"}`: the agent may ask a person mid-step (`main`, `toolMode: "model"`) |
| `models` | `{"route": [{"model", "inputTokensOver" or "matches"}], "fallback": [up to 3 models]}` (`main`, `dedicated`) |
| `model: "litellm/<name>"` | A non-Bedrock model through the deployment's LiteLLM proxy |

`agentcore` holds `guardrails` (`input`/`output`, `use`: a named guardrail), `memory`
(`longTerm` strategies, `scope`, `custom`, or `use`: a named memory), `evaluations` (`enabled`,
`auto`, `evaluators`: `Builtin.<name>` or `Custom.<name>`, `custom`) and `identity.outbound`
(identities the agent's code may fetch with `ctx.get_identity_token` / `ctx.get_api_key`).

## Named blocks

Defined once, used by name:

```json
"guardrails": { "brandSafety": { "contentFilters": { "HATE": "HIGH", "PROMPT_ATTACK": "HIGH" },
  "deniedTopics": [ { "name": "LegalAdvice", "definition": "…", "examples": ["…"] } ],
  "piiEntities": { "EMAIL": "ANONYMIZE", "PHONE": "ANONYMIZE" } } },
"memories": { "brandVoice": { "strategies": ["userPreference"], "scope": "user", "expiryDays": 30 } },
"evaluators": { "brandVoice": { "instructions": "Score 0-1 how well the post follows …" } },
"identities": { "cmsApi": { "type": "apikey" },
                "crm": { "type": "oauth2", "clientId": "…", "scopes": ["…"], "tokenUrl": "…" } },
"policies": { "noAcme": { "statement": "forbid(principal, action == AgentCore::Action::\"{{tool}}___WebSearch\", resource == AgentCore::Gateway::\"{{gateway}}\") when { context.input has query && context.input.query like \"*Acme*\" };" } }
```

- **Denied-topic limits** (Bedrock's, checked by the validator and the console): `name` matches `[0-9A-Za-z_ !?.-]{1,100}`, `definition` ≤ 200 characters,
  at most 5 `examples` of ≤ 100 characters each. A `piiEntities` action of `ANONYMIZE` masks
  the text and the run continues with it (logged as "anonymized"); `BLOCK` and every other
  finding stop the step, and the block log names the rule that matched.
- **Memory is always a named entry** used with `agentcore.memory.use`; that is what the
  console's Memory tab shows.
- **Identity secrets are never in the config.** They are entered in the console (Identity tab,
  Secrets) or passed as `IDENTITY_SECRETS` / `TF_VAR_identity_secrets`.
- **Policies** use `{{tool}}` (filled with the tool they are attached to) and `{{gateway}}`, and
  read only `context.input.<argument>`. `orchestrator.policy` sets `enabled` and `mode`
  (`ENFORCE` or `LOG_ONLY`).
- `orchestrator.gatewayIdentity: "perAgent"` gives each tool-using agent its own Gateway client.
- `orchestrator.interceptors.request` / `.response`: exactly one of `code` (files in the bundle's
  `toolCode["interceptor-request"]` / `["interceptor-response"]`, `handler.py` with
  `lambda_handler(event, context)` returning `{"interceptorOutputVersion": "1.0", "mcp": {...}}`)
  or `lambdaArn`; `passRequestHeaders`; `templates` records what the Builder generated it from.
- `orchestrator.triggers.<name>`: `type` `webhook` | `schedule` | `eventbridge` | `s3` | `sqs`, a
  `prompt` with `{{placeholders}}` from the delivery, and that type's keys (`signature`;
  `expression`, `timezone`; `pattern`, `bus`; `bucket`, `prefix`; `queueArn`). `runAs` `owner` or
  `service` (with `approvers`), `gates` `inherit` or `auto`, `maxRunsPerHour`, `attachPayload`.
- `orchestrator.limits` (`userDailyUsd`, `userRunsPerHour`, `monthlyUsd`), `liveConfig` (admins
  change prompts and model settings without a redeploy), `models.overrides` (models an `override`
  holder may pick per run), `files` (`retentionDays`; lets agents `ctx.save_file`) and
  `attachments.s3`: all off unless set.
- `orchestrator.export`: send each run to Langfuse, Datadog, Grafana Cloud, Honeycomb or any OTLP backend
  (`destination`, `region`, `endpoint`, `secret` = the name of an `exportHeaders` build secret or an ARN, never
  the key; `send.content` off by default). Set it on the Builder's Settings → Export.
- An entry that is `{"library": "<id>"}` is a shared item from the console's library: keep it
  as it is.

## Step forms

Three shapes, and a gate follows the shape it is attached to:

```json
{ "agent": "intake", "hitl": true, "branch": { "when": [ … ], "default": "…" } }

{ "parallel": ["a", "b", "c"], "hitl": true,
  "gateId": "research", "gateName": "Research" }

{ "sequence": ["analysis", "recommendation"], "hitl": true,
  "gateId": "analysis_reco", "gateName": "Analysis & Recommendation" }
```

- A `parallel` gate takes **one decision per agent**, so a reviewer can accept four
  findings and send one back. It is also the only place a subset re-run is offered.
- A `sequence` gate takes **one decision for the chain**.
- `parallel` and `sequence` stages **MUST** have `gateId` and `gateName`.
- `hitl` may be an object: `{"mode": "always" | "threshold" | "auto", "when": [rules without goto],
  "approval": "console" | "event", "timeout": {"after": "24h", "action": "approve" | "deny"}}`.
- `branch.when` rules are tried in order, first match wins; operators are `equals`,
  `notEquals`, `in`, `contains`, `exists`, `gt`, `gte`, `lt`, `lte`. `goto` takes an agent
  id, a `gateId`, or `END`.

## The agent module

`scaffold.py agent <id>` writes all three files. `__init__.py` must keep its
`from .agent import agent` line, which is how the loader finds the instance.

```python
class MyAgent(Agent):
    system_prompt = SYSTEM_PROMPT

    async def run(self, ctx: AgentContext) -> str:
        ...
        return json.dumps(asset.model_dump(by_alias=True, mode="json"), indent=2)

agent = MyAgent()
```

## The `ctx` API — the only route to anything external

```
ctx.input(agent_id)                                  an upstream agent's output
await ctx.llm(system, user, model, max_tokens, name)  a model call
await ctx.call_tool(tool_key, query)                  a declared tool
await ctx.call_tool_rows(tool_key, query)             tabular result + rowFields map
await ctx.retrieve(query, doc_type)                   Knowledge Base retrieval
await ctx.memory_recall(query)                        long-term memory, scoped by subject
await ctx.memory_store(content)
await ctx.guardrail(text, source)                     explicit guardrail application
await ctx.policy_check(action)                        always True: Cedar is enforced at the Gateway on tool calls
await ctx.get_identity_token(provider)                OAuth token from an identity
await ctx.get_api_key(identity)                       API key from an identity
await ctx.image(prompt, negative_prompt)              an image agent's render
await ctx.save_file(name, data, content_type)         a run file (needs orchestrator.files)
await ctx.log(msg)                                    a line on the run timeline
await ctx.heartbeat(pct)                              progress for the graph
```

Also on `ctx`: `ctx.topic`, `ctx.agent_id`, `ctx.tool`, `ctx.truncated_calls`.

Errors are deliberate and **MUST** propagate: `ModelUnavailable`, `ToolUnavailable`,
`ToolDenied`. A fabricated fallback value is indistinguishable from real evidence once it
reaches the report.

## The console bundle

**Export → Bundle (workflow, prompts and code)** in the console writes one JSON file:

```
{ "format": "agentexpress-bundle", "version": 1, "framework": { "version": "1.0.0" },
  "project": { "id": "…", "name": "…", "exportedAt": "…" },
  "workflow": { … },                                   the workflow.json
  "prompts":  { "<agent>": { "systemPrompt": "…", "schema": "…" } },
  "toolCode": { "<key>": { "handler.py": "…", "requirements.txt": "…", "events.json": "…" } } }
```

Two ways to work on it:
- **Edit the file and import it back:** change `workflow`, `prompts` or `toolCode`, then
  **Project → Import a file… → Into this build, replacing it** in the console (Undo takes it
  back). This is the round trip for a console build.
- **Write it into a checkout:** `python3 scaffold.py apply <bundle> --exact` (with `--dry-run`
  first) writes prompts into `prompts.py` and code tools into `app/tools/_code/<key>/`. Run the
  gates, then deploy that tree with CDK or Terraform yourself. A `prompts.py` is rewritten only
  while it still carries the Builder's marker, and an `agent.py` is never touched.

Keep `{"library": "<id>"}` entries as they are, put no secret in the file (secrets stay in the
console), and a code tool may hold only `*.py`, `requirements.txt` and `events.json`.

## `scaffold.py` CLI

```
python3 scaffold.py reset
    [--keep-kb]                      leave kb_docs/ alone
    [--dry-run]

Clears the shipped sample — agents, steps, tools, their folders and the KB corpora —
and leaves ONE working agent (`first_agent`) in one gated step, because an empty
`agents`/`steps` is not a valid workflow and fails the suite. Run it first;
`pytest` passes straight after, and that green suite is your safety net.
(A bundle drafted in the console and applied instead of reset fails some tests that
check the shipped sample's own conventions while it deploys fine; check it with
`cdk synth` or `terraform validate`.)

python3 scaffold.py agent <agent_id>
    [--produces PRODUCES]            deliverable name, default <id>-output
    [--runtime {main,dedicated}]
    [--max-tokens MAX_TOKENS]
    [--tool TOOL]                    a key in the tools block
    [--remote --skill SKILL]         runtime "a2a"; writes no folder
    [--dry-run]                      print, write nothing
```

## `ui` keys

`title`, `heading`, `defaultTopic`, `topicPlaceholder`, `subjectPlaceholder`,
`subjectHint`, `assistantTitle`, `assistantSubtitle`.

`title` and `heading` drive the browser tab, top navigation, side navigation, breadcrumb
root and info-panel heading. A `ui` key nothing renders fails
`tests/test_config_keys.py`, because a presentation key that does nothing is worse than an
absent one.

## `authorization`

Gates the mutating actions: `start`, `decision`, `rerun`, `cancel`, `evaluate`,
`insights`, `delete`, and on a console `deploy`, `destroy`, `audit`, `admin` and `override`
(the last three closed unless granted). `signIn` picks a build's sign-in provider (Cognito by
default, or Okta, Entra ID, Auth0). `groupsClaim` names the JWT claim carrying groups — Cognito issues
`cognito:groups`; Auth0 needs a namespaced custom claim added by a post-login Action; Okta
uses `groups` (a claim on its authorization server); Entra ID uses `roles` (app roles).

The UI hides what a caller cannot do; `bff/authz.py` refuses it again server-side. Hiding
a button is not the control.
