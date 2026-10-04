"""AgentExpress Assistant: a conversation that builds a workflow.

The user says what they want; the model (Claude Sonnet 5.5 by default) asks what it must,
drafts early, and edits the build's draft as it goes. Every edit lands in the SAME draft
the Build view edits by hand and an upload replaces, so the three ways of building are
one build.

How a turn runs:

* `start()` (POST /api/builds/{id}/design) stores the user's message and a pending
  reply, and hands the turn to a background invocation of this function (the model can
  take longer than API Gateway's 30 seconds). The page polls `conversation()`.
* `run_turn()` asks the model, which changes the build only through two tools:
  `apply_changes` (targeted edits: set or update one agent, tool, block, or the steps)
  and `undo_last_change`. An edit is applied to the LATEST draft, re-read at that
  moment, so a manual edit made while the model was thinking is kept. The result is
  checked by validate_build — the Build view's own rules — and errors go back to the
  model to fix, up to MAX_FIX_ROUNDS. An error on a value only the user can give (an
  endpoint, an ARN, a key) is reported as "needs your input" instead, and the draft is
  saved with it: the deploy route refuses the build until it is filled in.
* The reply streams (ConverseStream): the text written so far and a phase (thinking,
  writing, applying, checking) are saved into the pending turn every FLUSH_S, so the
  page's poll shows it growing. Lambda's Python runtime cannot stream a response, and a
  WebSocket API would be one more thing to run, so the conversation file is the channel.
* The model never deploys. It has no tool that could.

Stored next to the draft, private to the build's owner like the build itself:
    builds/<id>/design.json          the conversation, as the page shows it
    builds/<id>/design-rev/<n>.json  the draft as it was before change n (for undo)
"""

from __future__ import annotations

import copy
import json
import os
import re
import secrets
import time

import boto3
import buildstore
import validate_build
import workflow as wf_mod
from botocore.config import Config

REGION = os.environ.get("AWS_REGION", "us-east-1")
#: The Assistant's models, in order: DESIGNER_MODEL (CDK `-c designerModel`, Terraform
#: `designer_model`), a comma-separated list of `model[:effort]`, else Claude Sonnet 5.5
#: then Claude Sonnet 5. The first the account may call is used: a model the account is
#: refused (an AWS Marketplace private marketplace without it, a model never enabled) is
#: skipped for the next one, once per process. Seen in a workshop account whose
#: organisation's private marketplace had Sonnet 5 but not Sonnet 5.5 — every turn failed
#: with AccessDeniedException. Each through the inference profile of this console's own
#: geography, so the conversation stays there.
DEFAULT_MODELS = "us.anthropic.claude-sonnet-5-5,us.anthropic.claude-sonnet-5"
MAX_TOKENS = 16000
#: How hard the model reasons before it answers (Claude's `effort`): DESIGNER_EFFORT
#: (CDK `-c designerEffort`, Terraform `designer_effort`) = low | medium | high, or
#: "model" to send nothing and take the model's own default. Measured on Claude Sonnet 5
#: drafting a six-agent workflow in one call: the model's default (high) thought for
#: ~135 s, hidden, used all 16,000 output tokens and was cut off with nothing applied.
#: Claude Sonnet 5.5 (the default) wrote the whole workflow in ~31 s at medium and ~29 s at
#: low, with fewer validation errors than Sonnet 5 at either. The reasoning is not shown
#: (only its signature streams), and every edit is still checked by the Build view's rules
#: and sent back to fix.
EFFORT = (os.environ.get("DESIGNER_EFFORT") or "medium").strip().lower()


def _model_list(raw: str) -> list[tuple[str, str]]:
    """[(model id, effort)] from "a[:effort],b[:effort]"; an entry without one takes EFFORT."""
    out = []
    for entry in (raw or "").split(","):
        model, _, effort = entry.strip().partition(":")
        # A model id may itself contain ":" (`...-v1:0`); only a known effort is one.
        if effort and effort.strip().lower() not in ("low", "medium", "high", "model"):
            model, effort = entry.strip(), ""
        if model:
            out.append((wf_mod.regional_model_id(model, REGION), (effort or EFFORT).strip().lower()))
    return out


MODELS: list[tuple[str, str]] = (_model_list(os.environ.get("DESIGNER_MODEL") or "")
                                 or _model_list(DEFAULT_MODELS))
#: The entry in use: moved on when the account is refused the current one.
_model_at = [0]
#: Models that rejected `output_config` (no effort): each later call goes without it.
_no_effort: set[str] = set()


def current_model() -> tuple[str, str]:
    return MODELS[min(_model_at[0], len(MODELS) - 1)]


#: The first model, for anything that names the Assistant's model before a call is made.
MODEL = MODELS[0][0]
_ACCESS_DENIED = ("AccessDenied", "not authorized", "private marketplace",
                  "Model access is denied", "don't have access to the model")
#: Model calls in one turn: each tool round trip is one.
MAX_CALLS = 10
MAX_FIX_ROUNDS = 3
#: Earlier turns the model sees verbatim; the build itself always goes in whole. Turns
#: older than that are folded into a running summary (decisions, preferences, open
#: questions) that goes with every turn, so a long conversation keeps its early context.
HISTORY_TURNS = 24
#: Summarise only once this many turns beyond HISTORY_TURNS have built up, so it is one
#: short extra call every few messages, not one per message. Until then they stay verbatim.
SUMMARY_BATCH = 8
#: A small, fast model is enough to condense a conversation: DESIGNER_SUMMARY_MODEL
#: (CDK `-c designerSummaryModel`, Terraform `designer_summary_model`), else the
#: deployment's default model.
SUMMARY_MODEL = wf_mod.regional_model_id(
    os.environ.get("DESIGNER_SUMMARY_MODEL") or wf_mod.default_model(REGION), REGION)
SUMMARY_MAX_CHARS = 6000
#: Don't START a model call this long into one invocation: the function's timeout is
#: 300 s and one call can take ~100 s (a large change written as one tool call). Past it,
#: the turn carries on in a fresh invocation (see MAX_CONTINUATIONS) instead of stopping.
TURN_BUDGET_S = 180
#: How many fresh invocations one turn may carry on in. It used to stop at the budget with
#: "That took too long" and half a workflow built; now the next invocation picks up from
#: the saved draft, so a large request finishes. 3 more = ~12 minutes at most.
MAX_CONTINUATIONS = 3
MESSAGE_MAX = 8000

# boto3 waits 60 s for the next bytes by default. A streamed reply can pause longer than
# that while the model writes a large change (a whole workflow as one tool call), which
# failed the turn with ReadTimeoutError. Wait up to READ_TIMEOUT_S, well inside the
# turn's budget; the retries below are this module's, not botocore's.
READ_TIMEOUT_S = 180
_bedrock = boto3.client("bedrock-runtime", region_name=REGION,
                        config=Config(read_timeout=READ_TIMEOUT_S, connect_timeout=10,
                                      retries={"max_attempts": 1, "mode": "standard"}))

AGENT_ID_RE = validate_build.AGENT_ID_RE
TOOL_KEY_RE = validate_build.TOOL_KEY_RE
BLOCKS = ("orchestrator", "ui", "guardrail", "authorization",
          # The build's named maps: an agent names one of each by key (see _OPS_DOC).
          "guardrails", "memories", "evaluators", "identities", "policies")

STARTERS = [
    {"title": "Claims triage",
     "message": "I want to triage insurance claims: read the claim, check it against our "
                "policy documents, flag possible fraud, and have a person approve the decision."},
    {"title": "Research and report",
     "message": "Research a topic from our internal documents and the web, analyse the "
                "findings, and write a report a manager can review."},
    {"title": "Content with images",
     "message": "Draft a product launch blog post with a matching hero image, and let an "
                "editor review the text and the image before it is published."},
    {"title": "Explain this build",
     "message": "Explain this build: what each agent does, how they connect, and what "
                "still needs my input."},
]


def _clients():
    import builds
    return builds, builds._s3, builds.BUILDS_BUCKET


def conversation_key(build_id: str) -> str:
    return f"builds/{build_id}/design.json"


def revision_key(build_id: str, n: int) -> str:
    return f"builds/{build_id}/design-rev/{int(n)}.json"


# --- the system prompt: generated from the framework's own spec -----------------------

def _key_line(name: str, spec: dict, vocab: dict) -> str:
    bits = [spec.get("type") if isinstance(spec.get("type"), str)
            else "|".join(spec.get("type") or []) or "any"]
    if spec.get("appliesTo") not in (None, "*"):
        bits.append("for " + "/".join(spec["appliesTo"]))
    if spec.get("required"):
        bits.append("required")
    if spec.get("requiredFor"):
        bits.append("required for " + "/".join(spec["requiredFor"]))
    values = None
    if spec.get("vocabulary"):
        values = (vocab.get(spec["vocabulary"]) or {}).get("values")
    elif spec.get("enum"):
        values = spec["enum"]
    if values:
        bits.append("one of " + ", ".join(json.dumps(v) for v in values[:24]))
    if "default" in spec:
        bits.append(f"default {json.dumps(spec['default'])}")
    doc = re.sub(r"\s+", " ", str(spec.get("doc") or "")).strip()
    if len(doc) > 260:
        doc = doc[:257] + "..."
    return f"  - `{name}` ({'; '.join(bits)}): {doc}"


def _spec_text() -> str:
    keys, vocab = validate_build.KEYS, validate_build.VOCAB
    out = []
    for block, spec in keys.items():
        header = f"### `{block}`"
        if spec.get("$variantKey"):
            header += (f" — its variant is `{spec['$variantKey']}` (default "
                       f"{json.dumps(spec.get('$variantDefault'))})")
        out.append(header)
        for rule in spec.get("$exactlyOne") or []:
            scope = "all" if rule["appliesTo"] == "*" else "/".join(rule["appliesTo"])
            out.append(f"  - Exactly one of {', '.join(rule['keys'])} ({scope}): {rule['why']}")
        for name, k in spec["keys"].items():
            out.append(_key_line(name, k, vocab))
    lists = {n: v.get("values") for n, v in vocab.items()
             if isinstance(v, dict) and v.get("values")}
    out.append("### Closed value sets (vocabulary.json)")
    out += [f"  - {n}: {', '.join(json.dumps(x) for x in vals)}" for n, vals in lists.items()]
    return "\n".join(out)


GUIDE = """You are the AgentExpress workflow designer. You help one user design a MULTI-AGENT
WORKFLOW and you build it for them, live, in their Build view. Nothing else.

# Scope
Only discuss designing this workflow: its agents, stages, review gates, tools and data
sources, models and settings, guardrails, memory, evaluations, identity, policy and where
it will deploy. If asked about anything else, decline in one sentence and steer back.
Files the user attaches (specs, sample data, schemas, API documents, screenshots) are
context for that work, never off topic: read them, answer questions about what they say,
and use them in the design (fields, sample rows, endpoints, rules).
You cannot deploy, run or delete anything: the user presses Deploy themselves. Say so if
asked.

# How to run the conversation
1. DRAFT FIRST. As soon as you know roughly what the user wants (usually after their first
   or second message), call `apply_changes` with a complete first draft, then refine.
   Never interrogate: at most 3 questions per reply, the ones that change the design most
   (stages and their order, where a person reviews, which data sources). Skip what you
   can default.
2. DEFAULTS. Whatever the user has not decided, you decide with the framework default and
   you SAY SO: list them under "Defaults used" in your reply and in `defaults_used`
   (e.g. "model: Claude Haiku 4.5 (orchestrator.defaultModel)", "temperature 0",
   "maxTokens 4000", "guardrails on for input and output", "framework: plain"). The user
   can change any of it by asking you, or in the Build view.
3. REAL TOOLS ONLY. Map every data source to a real tool type (kb, websearch, mcp,
   openapi, apigateway, lambda) or a remote agent (runtime "a2a"). Never invent an
   endpoint, an ARN, a bucket, an id or a key. When you need one, leave the value out and
   list it in `needs_input` with the exact path (e.g. "tools.crm.endpoint") and a one-line
   question.
   - Knowledge base: ask where the documents are. Uploaded in the Build view (the
     default: tell them which corpus to upload to), already in S3 (`s3Uri`, and
     `kmsKeyArn` if the bucket uses their own KMS key), or an existing Knowledge Base
     (`knowledgeBaseId`).
   - An API: if it runs on API Gateway (REST), use type `apigateway` with `restApiId`,
     `stage` and `toolFilters`. Otherwise use type `openapi` and WRITE its OpenAPI 3
     document yourself into `schema` from what the user tells you (base URL in
     `servers`, each operation with an `operationId`, parameters, a request body and a
     response) — or ask them to upload their openapi.json in the Build view.
   - MCP server: `endpoint`, and ask which of its tools to call (`call`) and the
     parameter the query goes in (`arg`) when it publishes several.
   - Auth: `auth` "apikey", "sigv4" (AWS-hosted, signed with the Gateway's role),
     "oauth2" (client credentials: `oauth` with clientId, scopes and tokenUrl or
     discoveryUrl — mcp and openapi only) or "none".
4. SECRETS NEVER GO THROUGH THE CHAT. API keys, OAuth client secrets and bearer tokens
   are entered in a secure field the page shows, never typed to you. When a tool, a remote
   agent or a named identity (`identities`, by its name) needs one, list it in
   `secrets_needed` and tell the user to fill in the field that appears beside the chat.
   An identity is only useful once something uses it: a tool's `identity`, or an agent's
   agentcore.identity.outbound — say which, and if nothing calls the API yet, say so.
   If a user pastes a secret anyway, do not repeat it: tell them to use the secure field
   instead, and to rotate it.
5. PROMPTS. Every local agent (runtime main or dedicated) needs a `prompt`: a specific
   system prompt (role, inputs, rules, what to refuse, output) and a `schema` — a JSON
   example of the asset it returns, as a string. Write them properly; they decide the
   quality of the result.
5b. CODE TOOLS. When the user needs custom logic no existing tool gives (call their
   internal API, compute something, read their own table), write the function: a tool
   {"type": "lambda", "code": {"grants": {...}}, "toolSchema": [...]} plus its files with
   `set_tool_code`. handler.py defines `lambda_handler(event, context)`; `event` is the
   tool's arguments as a dict; with several tools in toolSchema, read which one was called
   from context.client_context.custom["bedrockAgentCoreToolName"] ("<key>___<tool>").
   Return JSON-serialisable data. Standard library first; requirements.txt only for what
   Lambda lacks, pinned with ==, never boto3. Grant only what it needs: `secret` (a
   Secrets Manager name, read as SECRET_NAME), `table` + `tableAccess` (read by default,
   as TABLE_NAME), `s3Prefix` + `s3Access` (as S3_PREFIX), `vpc` for private networks;
   {} means no AWS access. A granted secret, table or bucket must carry the tag
   agentexpress:code-tools=true (a bucket also needs S3 ABAC on); tell the user so.
   Never write a credential into code. Add events.json: 2-4 test
   events [{"name", "tool", "event"}], so the user can run it in the sandbox. Say that
   it is checked (syntax, lint, security) and run in a sandbox from the tool's panel.
6. EVERY CHANGE GOES THROUGH `apply_changes`. A large first draft (more than about five
   agents, or tools with code) goes in several calls: a few agents with their prompts per
   call, then the steps, then the rest, so no single reply runs out of room. Make the
   smallest edit that does the job:
   `update_agent` to change a few fields, `set_agent` only for a new agent or a rewrite.
   An agent or tool you do not touch keeps whatever the user set by hand. When the user
   asks to undo your last change, call `undo_last_change`.
7. After a change, reply briefly in Markdown: what you built or changed (a short list),
   "Defaults used", then any questions. When asked to explain the build, walk through the
   stages in order, name each agent's job and tools, and list what still needs input.

# Rules the workflow must follow (the server checks them and tells you what failed)
- Agent ids: letters, digits, underscores, starting with a letter (snake_case). Tool keys:
  letters and digits, starting with a letter (camelCase). No hyphens anywhere in ids.
- `steps` is an ordered list of stages. A stage is exactly one of `agent` (one agent),
  `parallel` (run together, then join) or `sequence` (one after another, one gate after
  all). Every agent appears in exactly one stage. `hitl: true` pauses for a person to
  approve, revise or deny after that stage. A group stage takes a `gateId` (snake_case)
  and a `gateName`. `branch` routes forward only: `when` rules on a field of that stage's
  asset, and/or a `default`, to a LATER stage's name or "END". No branch on a parallel
  stage or on the last stage.
- At least one agent must run here (runtime "main" or "dedicated"). Give at least one
  agent `agentcore.evaluations.enabled: true` (default evaluators Builtin.Faithfulness,
  Builtin.ResponseRelevance). Offer the built-ins that fit (Coherence, Conciseness,
  Correctness, Faithfulness, Harmfulness, Helpfulness, InstructionFollowing, Refusal,
  ResponseRelevance, Stereotyping — not GoalSuccessRate, ToolSelectionAccuracy or
  ToolParameterAccuracy, which this framework cannot score yet), and write a custom judge
  in `evaluations.custom` ({name, instructions}) when the user has their own criteria.
- Memory: `agentcore.memory.longTerm` from semantic, summary, userPreference, episodic;
  `scope` user (default: each user's own), subject, agent or run; `custom` ({base,
  instructions}) when the user wants their own extraction rules. Each strategy costs a
  model call per stored turn, so add only what the agent needs.
- Guardrails: the workflow shares one Bedrock guardrail (the `guardrail` block). Turn it
  on for an agent with `agentcore.guardrails.input` / `.output` — ON for both by default
  unless the user says otherwise. Use `agentcore.guardrails.guardrailId` only when the
  user brings their own guardrail.
- A tool is bound to an agent with the agent's `tool` (a key, or a list of keys). A
  knowledge base agent may also set `corpus`. At most one kb and one websearch tool.
- Give every agent that has tools `toolMode: "model"` (its model chooses which tool to
  call and with what arguments, reading each result first; `maxToolCalls`, default 6,
  caps it) unless the user wants every tool queried once up front ("direct").
- Model ids must be ones from "Models this account can use" below; omit `model` to use
  orchestrator.defaultModel. Some models (Claude Sonnet 5) refuse `temperature`; the
  runtime then drops it, so do not set temperature for them.
- An agent that must PRODUCE AN IMAGE gets `output: "image"` (and optionally an `image`
  block: model, aspectRatio, outputFormat, seed, negativePrompt). Its model writes an
  image brief and Stable Diffusion 3.5 Large renders it — in the deployment's region if
  Bedrock serves the model there, else one in the same geography (us-west-2 for a US
  deployment). Outside that geography the deploy is refused unless the agent sets
  `image.allowCrossRegion: true`; say so when you add one. Its `prompt.systemPrompt`
  should say what the image is for, its style and what
  it must never show; its `schema` is ignored (the brief shape is fixed). Put a review
  gate (`hitl: true`) on its stage unless the user says otherwise: guardrails do not
  check images.
- An agent that must LOOK AT images made earlier in the run (check, describe, compare or
  score them) gets `vision: {"from": ["<earlier agent id>"]}` (optionally `maxImages`):
  its model then receives those images with its text. Each agent named must run in an
  EARLIER step (not a parallel peer) and should be an image agent. Its `model` must read
  images (e.g. a Claude, Amazon Nova Pro/Lite or Llama 3.2 Vision model); say which you
  chose. Give it a schema for what it reports, e.g. a score and comments.
- `framework` (plain, strands, langgraph...) only changes how the agent's code is
  written; leave it at the default unless the user asks.
- Use only the keys below, only where they apply. A key that does not apply is rejected.

# The workflow.json key spec (generated from the framework; authoritative)
"""


def system_prompt() -> list[dict]:
    return [{"text": GUIDE + _spec_text()}, {"cachePoint": {"type": "default"}}]


# --- the tools ------------------------------------------------------------------------

_OPS_DOC = (
    "Edits, applied in order to the CURRENT build. Each is an object with `op`:\n"
    "- set_agent {id, agent, prompt?}: create or replace agent `id` (`agent` is the whole "
    "workflow.json entry; `prompt` is {systemPrompt, schema} for a local agent).\n"
    "- update_agent {id, set?, unset?, prompt?}: change some fields of an existing agent "
    "(`set` merges top-level keys; `unset` lists keys to remove).\n"
    "- remove_agent {id}: delete it, and take it out of its stage.\n"
    "- rename_agent {id, to}: change its id everywhere it is referenced.\n"
    "- set_tool {key, tool} / update_tool {key, set?, unset?} / remove_tool {key}.\n"
    "- set_tool_code {key, files}: the files of a tool whose `code` is written in the build "
    "(`files` maps handler.py, requirements.txt, other *.py and events.json to their text; a "
    "file you leave out is kept, a file set to null is removed).\n"
    "- set_steps {steps}: replace the ordered stage list.\n"
    "- set_block {name, value}: merge `value` into the orchestrator, ui, guardrail or "
    "authorization block, or into one of the build's named maps: guardrails, memories "
    "({name: {strategies, expiryDays, scope}}), evaluators ({name: {instructions}}), "
    "identities ({name: {type: oauth2|apikey, clientId, scopes, tokenUrl}}) and policies "
    "({name: {statement}}, written for AgentCore::Action::\"{{tool}}\"). Agents then use them "
    "by name: agentcore.guardrails.use, agentcore.memory.use, Custom.<name> in "
    "agentcore.evaluations.evaluators, agentcore.identity.outbound; a tool attaches policies "
    "with `policies` and signs in with `identity`. An entry that is {\"library\": \"<id>\"} is "
    "a shared item the user keeps in their library: use it by its key, never edit it. "
    "Long-term memory is ALWAYS a named entry in `memories` that the agent uses with "
    "agentcore.memory.use — never agentcore.memory.longTerm on the agent: that is the form "
    "the Memory tab and the agent's Memory setting show the user.\n"
    "- set_name {name}: rename the build (1-64 characters).")

TOOLS = {"tools": [
    {"toolSpec": {
        "name": "apply_changes",
        "description": "Change the user's build. The server applies the edits to the latest "
                       "draft, checks the result, and answers with what failed (fix it and "
                       "call again) or with the applied state.",
        "inputSchema": {"json": {
            "type": "object",
            "properties": {
                "summary": {"type": "string",
                            "description": "One line, for the user: what this change does."},
                "ops": {"type": "array", "items": {"type": "object"}, "description": _OPS_DOC},
                "defaults_used": {"type": "array", "items": {"type": "string"},
                                  "description": "Each default you chose for the user."},
                "secrets_needed": {
                    "type": "array",
                    "description": "Secrets the user must enter in the page's secure field "
                                   "(never in the chat): an API key or OAuth client secret "
                                   "for a tool, or a bearer token for a remote agent.",
                    "items": {"type": "object", "properties": {
                        "tool": {"type": "string", "description": "The tool key, for a tool's secret."},
                        "agent": {"type": "string", "description": "The agent id, for a remote agent's token."},
                        "identity": {"type": "string",
                                     "description": "The name in `identities`, for its API key or "
                                                    "OAuth client secret."},
                        "why": {"type": "string", "description": "One line: what it is for."}}}},
                "needs_input": {
                    "type": "array",
                    "description": "Values only the user can give.",
                    "items": {"type": "object", "properties": {
                        "path": {"type": "string"}, "question": {"type": "string"}},
                        "required": ["path", "question"]}},
            },
            "required": ["summary", "ops"]}}}},
    {"toolSpec": {
        "name": "undo_last_change",
        "description": "Put the build back the way it was before your last applied change.",
        "inputSchema": {"json": {"type": "object", "properties": {}}}}},
]}


# --- applying edits ---------------------------------------------------------------------

class OpError(ValueError):
    pass


def _obj(v, what: str) -> dict:
    if not isinstance(v, dict):
        raise OpError(f"{what} must be an object")
    return v


def _prompt(p) -> dict:
    p = _obj(p, "prompt")
    return {"systemPrompt": str(p.get("systemPrompt") or ""), "schema": str(p.get("schema") or "")}


def _without_agent(steps: list, aid: str) -> list:
    out = []
    for s in steps:
        s = dict(s)
        if s.get("agent") == aid:
            continue
        for lst in ("parallel", "sequence"):
            if isinstance(s.get(lst), list) and aid in s[lst]:
                s[lst] = [a for a in s[lst] if a != aid]
                if not s[lst]:
                    s = None
                    break
                if len(s[lst]) == 1:  # a group of one is a single-agent stage
                    only = s.pop(lst)[0]
                    s = {"agent": only, **{k: v for k, v in s.items()
                                           if k in ("hitl", "branch")}}
                break
        if s:
            out.append(s)
    return out


def _rename_in_steps(steps: list, old: str, new: str) -> list:
    def swap(v):
        return new if v == old else v
    out = []
    for s in steps:
        s = copy.deepcopy(s)
        if s.get("agent") == old:
            s["agent"] = new
        for lst in ("parallel", "sequence"):
            if isinstance(s.get(lst), list):
                s[lst] = [swap(a) for a in s[lst]]
        b = s.get("branch")
        if isinstance(b, dict):
            if b.get("default") == old:
                b["default"] = new
            for rule in b.get("when") or []:
                if isinstance(rule, dict) and rule.get("goto") == old:
                    rule["goto"] = new
        out.append(s)
    return out


def _tool_list(v) -> list:
    return validate_build.tools_of(v)


def apply_ops(project: dict, ops: list) -> tuple[dict, dict]:
    """(new project, what changed). Raises OpError on an edit that cannot apply."""
    if not isinstance(ops, list) or not ops:
        raise OpError("`ops` must be a non-empty list")
    p = copy.deepcopy(project)
    wf = p.setdefault("workflow", {})
    agents = wf.setdefault("agents", {})
    tools = wf.setdefault("tools", {})
    wf.setdefault("steps", [])
    prompts = p.setdefault("prompts", {})
    code = p.setdefault("toolCode", {})
    changed = {"agents": [], "tools": [], "steps": False, "blocks": [], "removed": []}

    def touch(kind, key):
        if key not in changed[kind]:
            changed[kind].append(key)

    for i, op in enumerate(ops):
        op = _obj(op, f"ops[{i}]")
        kind = op.get("op")
        try:
            if kind == "set_agent":
                aid = str(op.get("id") or "")
                if not AGENT_ID_RE.match(aid):
                    raise OpError(f"invalid agent id {aid!r}")
                agents[aid] = copy.deepcopy(_obj(op.get("agent"), "agent"))
                if op.get("prompt") is not None:
                    prompts[aid] = _prompt(op["prompt"])
                touch("agents", aid)
            elif kind == "update_agent":
                aid = str(op.get("id") or "")
                if aid not in agents:
                    raise OpError(f"no agent {aid!r} to update")
                entry = agents[aid]
                for k, v in _obj(op.get("set") or {}, "set").items():
                    entry[k] = copy.deepcopy(v)
                for k in op.get("unset") or []:
                    entry.pop(str(k), None)
                if op.get("prompt") is not None:
                    prompts[aid] = {**prompts.get(aid, {}), **_prompt(op["prompt"])}
                touch("agents", aid)
            elif kind == "remove_agent":
                aid = str(op.get("id") or "")
                if aid not in agents:
                    raise OpError(f"no agent {aid!r} to remove")
                del agents[aid]
                prompts.pop(aid, None)
                wf["steps"] = _without_agent(wf["steps"], aid)
                changed["removed"].append(aid)
                changed["steps"] = True
            elif kind == "rename_agent":
                old, new = str(op.get("id") or ""), str(op.get("to") or "")
                if old not in agents:
                    raise OpError(f"no agent {old!r} to rename")
                if not AGENT_ID_RE.match(new) or new in agents:
                    raise OpError(f"cannot rename to {new!r}")
                agents[new] = agents.pop(old)
                if old in prompts:
                    prompts[new] = prompts.pop(old)
                wf["steps"] = _rename_in_steps(wf["steps"], old, new)
                touch("agents", new)
                changed["removed"].append(old)
                changed["steps"] = True
            elif kind == "set_tool":
                key = str(op.get("key") or "")
                if not TOOL_KEY_RE.match(key):
                    raise OpError(f"invalid tool key {key!r}")
                tools[key] = copy.deepcopy(_obj(op.get("tool"), "tool"))
                touch("tools", key)
            elif kind == "update_tool":
                key = str(op.get("key") or "")
                if key not in tools:
                    raise OpError(f"no tool {key!r} to update")
                for k, v in _obj(op.get("set") or {}, "set").items():
                    tools[key][k] = copy.deepcopy(v)
                for k in op.get("unset") or []:
                    tools[key].pop(str(k), None)
                touch("tools", key)
            elif kind == "remove_tool":
                key = str(op.get("key") or "")
                if key not in tools:
                    raise OpError(f"no tool {key!r} to remove")
                del tools[key]
                code.pop(key, None)
                for aid, entry in agents.items():
                    if isinstance(entry, dict) and key in _tool_list(entry.get("tool")):
                        left = [t for t in _tool_list(entry["tool"]) if t != key]
                        if left:
                            entry["tool"] = left[0] if len(left) == 1 else left
                        else:
                            entry.pop("tool", None)
                            entry.pop("corpus", None)
                        touch("agents", aid)
                changed["removed"].append(key)
            elif kind == "set_tool_code":
                key = str(op.get("key") or "")
                if key not in tools or "code" not in (tools.get(key) or {}):
                    raise OpError(f"tool {key!r} is not written in the build: set its `code` first")
                files = {**code.get(key, {})}
                for name, body in _obj(op.get("files"), "files").items():
                    if body is None:
                        files.pop(str(name), None)
                    elif not isinstance(body, str):
                        raise OpError(f"files[{name!r}] must be text")
                    else:
                        files[str(name)] = body
                code[key] = files
                touch("tools", key)
            elif kind == "set_steps":
                steps = op.get("steps")
                if not isinstance(steps, list):
                    raise OpError("`steps` must be a list")
                wf["steps"] = copy.deepcopy(steps)
                changed["steps"] = True
            elif kind == "set_block":
                name = str(op.get("name") or "")
                if name not in BLOCKS:
                    raise OpError(f"`name` must be one of {', '.join(BLOCKS)}")
                block = wf.get(name) if isinstance(wf.get(name), dict) else {}
                wf[name] = {**block, **copy.deepcopy(_obj(op.get("value"), "value"))}
                touch("blocks", name)
            elif kind == "set_name":
                name = str(op.get("name") or "").strip()
                if not 1 <= len(name) <= buildstore.NAME_MAX:
                    raise OpError(f"a build name is 1 to {buildstore.NAME_MAX} characters")
                # The app's tab title and heading follow the build's name while they
                # still showed the old one (web/src/builder/model.ts renameProject).
                ui = dict(p["workflow"].get("ui") or {})
                for k in ("title", "heading"):
                    if ui.get(k) in (None, "", p.get("name")):
                        ui[k] = name
                p["workflow"]["ui"] = ui
                changed["blocks"] = sorted({*changed.get("blocks", []), "ui"})
                p["name"] = name
            else:
                raise OpError(f"unknown op {kind!r}")
        except OpError as e:
            raise OpError(f"ops[{i}] ({kind}): {e}") from None
    return p, changed


def _blocking(errors: list[dict], needs_input: list) -> list[dict]:
    """The errors the model must fix: all of them except those on a value it has said
    only the user can give."""
    paths = [str(n.get("path") or "") for n in needs_input if isinstance(n, dict)]

    def covered(path: str) -> bool:
        return any(p and (path == p or path.startswith((p + ".", p + "["))
                          or p.startswith(path + "."))
                   for p in paths)
    return [e for e in errors if not covered(e["path"])]


# --- storage ------------------------------------------------------------------------------

# --- attachments: files and S3 objects a message brings as context ------------------------

#: What Converse takes in one message: at most 5 documents of 4.5 MB, images of 3.75 MB.
ATTACH_MAX = 5
DOC_MAX_BYTES = 4_500_000
IMAGE_MAX_BYTES = 3_750_000
#: File extension -> (kind, Converse format). Text formats Converse has no name for
#: (JSON, YAML, code...) are sent as plain text.
ATTACH_FORMATS = {
    **{e: ("document", e) for e in ("pdf", "csv", "doc", "docx", "xls", "xlsx", "html", "txt", "md")},
    "htm": ("document", "html"), "markdown": ("document", "md"),
    **{e: ("document", "txt") for e in ("json", "yaml", "yml", "xml", "tf", "py", "ts", "js", "sql", "log")},
    "png": ("image", "png"), "jpg": ("image", "jpeg"), "jpeg": ("image", "jpeg"),
    "gif": ("image", "gif"), "webp": ("image", "webp"),
}
#: Buckets (optionally bucket/prefix) a message may name as s3://..., set by the IaC
#: (designerS3Buckets / designer_s3_buckets). Empty: no S3 paths at all.
S3_ALLOWED = [p.strip().strip("/") for p in os.environ.get("DESIGNER_S3_BUCKETS", "").split(",") if p.strip()]
S3_URI_RE = re.compile(r"s3://([a-z0-9][a-z0-9.\-]{1,61}[a-z0-9])(/[^\s\"'<>)]*)?")
_s3_any = boto3.client("s3", region_name=REGION)


def _attach_prefix(build_id: str) -> str:
    return f"builds/{build_id}/attachments/"


def _format_of(name: str):
    return ATTACH_FORMATS.get(name.rsplit(".", 1)[-1].lower()) if "." in name else None


def _check_one(name: str, size: int, where: str) -> dict:
    """{kind, format} for a file a message may attach, or a BuildError naming why not."""
    builds, *_ = _clients()
    got = _format_of(name)
    if not got:
        raise builds.BuildError(400, f"{where}: not a file type the assistant reads "
                                     f"({', '.join(sorted(ATTACH_FORMATS))})")
    limit = IMAGE_MAX_BYTES if got[0] == "image" else DOC_MAX_BYTES
    if size > limit:
        raise builds.BuildError(400, f"{where} is {size / 1e6:.1f} MB; the most is {limit / 1e6:.2f} MB")
    if size <= 0:
        raise builds.BuildError(400, f"{where} is empty")
    return {"kind": got[0], "format": got[1]}


def attachment_upload(build_id: str, owner, name: str) -> dict:
    """A presigned POST for one file to attach to the next message: it goes straight to
    the builds bucket, under the build (deleted with it)."""
    builds, s3, bucket = _clients()
    builds._meta(build_id, owner)
    name = os.path.basename(str(name or "")).strip()[:200]
    if not name:
        raise builds.BuildError(400, "name required")
    _check_one(name, 1, name)
    key = f"{_attach_prefix(build_id)}{secrets.token_hex(4)}-{name}"
    post = s3.generate_presigned_post(Bucket=bucket, Key=key, ExpiresIn=900,
                                      Conditions=[["content-length-range", 1, DOC_MAX_BYTES]])
    return {"url": post["url"], "fields": post["fields"], "key": key, "name": name,
            "maxBytes": DOC_MAX_BYTES}


def _s3_allowed(bucket: str, key: str) -> bool:
    """Whether s3://bucket/key is inside an allowed bucket or bucket/prefix. A prefix is a
    FOLDER: `bucket/reports` allows `reports/...` and `reports` itself, never
    `reports-private/...` — a plain startswith let a sibling folder through."""
    for p in S3_ALLOWED:
        if p == bucket:
            return True
        if p.startswith(bucket + "/"):
            folder = p[len(bucket) + 1:].rstrip("/")
            if key == folder or key.startswith(folder + "/"):
                return True
    return False


def resolve_attachments(build_id: str, uploads, message: str) -> list[dict]:
    """The files a message brings: its uploads, and every s3:// path in its text (an
    object, or a prefix ending in / for the files under it). Checked NOW, so a wrong one
    is refused with the reason before the turn starts."""
    builds, s3, bucket = _clients()
    out: list[dict] = []
    for u in uploads or []:
        key = str((u or {}).get("key") or "") if isinstance(u, dict) else ""
        if not key.startswith(_attach_prefix(build_id)) or "/" in key[len(_attach_prefix(build_id)):]:
            raise builds.BuildError(400, "an attachment is not one of this build's uploads")
        name = str(u.get("name") or key.rsplit("/", 1)[-1].split("-", 1)[-1])
        try:
            size = int(s3.head_object(Bucket=bucket, Key=key)["ContentLength"])
        except Exception:  # noqa: BLE001 - absent: the upload did not finish
            raise builds.BuildError(400, f"{name} has not finished uploading") from None
        out.append({"source": "upload", "key": key, "name": name, "size": size,
                    **_check_one(name, size, name)})
    for m in S3_URI_RE.finditer(message or ""):
        uri, b, key = m.group(0).rstrip(".,;:"), m.group(1), (m.group(2) or "/").lstrip("/").rstrip(".,;:")
        if not S3_ALLOWED:
            raise builds.BuildError(400, f"{uri}: this console reads no S3 paths; an admin can "
                                         "allow buckets with designerS3Buckets / designer_s3_buckets")
        if not _s3_allowed(b, key):
            raise builds.BuildError(400, f"{uri} is not in a bucket this console may read "
                                         f"(allowed: {', '.join(S3_ALLOWED)})")
        try:
            if not key or key.endswith("/"):
                found = [o for o in _s3_any.list_objects_v2(Bucket=b, Prefix=key, MaxKeys=50)
                         .get("Contents", []) if not o["Key"].endswith("/")]
                if not found:
                    raise builds.BuildError(400, f"{uri} holds no files")
                objs = [(o["Key"], int(o["Size"])) for o in found[:ATTACH_MAX]]
            else:
                objs = [(key, int(_s3_any.head_object(Bucket=b, Key=key)["ContentLength"]))]
        except builds.BuildError:
            raise
        except Exception as e:  # noqa: BLE001 - not found, or not readable by this console
            code = getattr(e, "response", {}).get("Error", {}).get("Code") or type(e).__name__
            raise builds.BuildError(400, f"{uri} could not be read ({code})") from None
        for k, size in objs:
            name = k.rsplit("/", 1)[-1]
            out.append({"source": "s3", "uri": f"s3://{b}/{k}", "bucket": b, "key": k, "name": name,
                        "size": size, **_check_one(name, size, f"s3://{b}/{k}")})
    if len(out) > ATTACH_MAX:
        raise builds.BuildError(400, f"at most {ATTACH_MAX} files per message (this one brings {len(out)})")
    return out


def _doc_name(name: str, taken: set) -> str:
    """A document name Converse accepts: letters, digits, spaces, hyphens, parentheses and
    brackets, no runs of spaces, unique in the message."""
    stem = name.rsplit(".", 1)[0]
    clean = " ".join(re.sub(r"[^A-Za-z0-9\s\-()\[\]]", " ", stem).split())[:180] or "file"
    out, n = clean, 2
    while out.lower() in taken:
        out, n = f"{clean} {n}", n + 1
    taken.add(out.lower())
    return out


def _attachment_blocks(items: list[dict]) -> list[dict]:
    """Converse content blocks for a message's attachments, read now."""
    _b, s3, bucket = _clients()
    taken: set = set()
    blocks = []
    for a in items or []:
        where = (bucket, a["key"]) if a.get("source") == "upload" else (a["bucket"], a["key"])
        data = (s3 if a.get("source") == "upload" else _s3_any).get_object(
            Bucket=where[0], Key=where[1])["Body"].read()
        if a["kind"] == "image":
            blocks.append({"image": {"format": a["format"], "source": {"bytes": data}}})
        else:
            blocks.append({"document": {"format": a["format"], "name": _doc_name(a["name"], taken),
                                        "source": {"bytes": data}}})
    return blocks


def _empty() -> dict:
    return {"turns": [], "status": "idle", "revision": 0, "pending": ""}


def _load(build_id: str) -> dict:
    _b, s3, bucket = _clients()
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=conversation_key(build_id))["Body"].read())
    except s3.exceptions.NoSuchKey:
        return _empty()


def _store(build_id: str, doc: dict) -> None:
    _b, s3, bucket = _clients()
    s3.put_object(Bucket=bucket, Key=conversation_key(build_id),
                  Body=json.dumps(doc, ensure_ascii=False).encode(),
                  ContentType="application/json")


def _public(doc: dict) -> dict:
    return {"turns": doc.get("turns", []), "status": doc.get("status", "idle"),
            "revision": doc.get("revision", 0), "model": current_model()[0], "starters": STARTERS,
            # How many of the earliest turns the designer now reads as a summary.
            "summarized": int((doc.get("summary") or {}).get("through") or 0),
            # What a message may bring (the chat's attach button and s3:// hint).
            "attach": {"maxFiles": ATTACH_MAX, "maxBytes": DOC_MAX_BYTES,
                       "types": sorted(ATTACH_FORMATS), "s3": S3_ALLOWED}}


def conversation(build_id: str, owner: str) -> dict:
    builds, *_ = _clients()
    builds._meta(build_id, owner)          # 404 unless it is the caller's build
    return _public(_load(build_id))


def reset(build_id: str, owner: str) -> dict:
    builds, *_ = _clients()
    builds._meta(build_id, owner)
    doc = _load(build_id)
    if doc.get("status") == "thinking":
        raise builds.BuildError(409, "wait for the current reply before starting over")
    fresh = {**_empty(), "revision": doc.get("revision", 0)}   # keep undo history
    _store(build_id, fresh)
    return _public(fresh)


def start(build_id: str, owner: str, email: str, message: str, self_invoke,
          attachments=None) -> dict:
    builds, *_ = _clients()
    builds._meta(build_id, owner)
    message = str(message or "").strip()
    if not message and attachments:
        message = "Here are some files for context."
    if not message:
        raise builds.BuildError(400, "message required")
    if len(message) > MESSAGE_MAX:
        raise builds.BuildError(400, f"a message is at most {MESSAGE_MAX} characters")
    doc = _load(build_id)
    if doc.get("status") == "thinking":
        raise builds.BuildError(409, "the designer is still replying to your last message")
    files = resolve_attachments(build_id, attachments, message)
    turn = secrets.token_hex(6)
    now = buildstore.now()
    doc["turns"].append({"id": f"u{turn}", "role": "user", "text": message, "at": now,
                         **({"attachments": files} if files else {})})
    doc["turns"].append({"id": turn, "role": "assistant", "text": "", "at": now,
                         "status": "thinking"})
    doc.update(status="thinking", pending=turn, attempts=0, cont=0)
    _store(build_id, doc)
    self_invoke({"action": "design", "build": build_id, "owner": owner, "email": email,
                 "turn": turn})
    return _public(doc)


# --- the turn -------------------------------------------------------------------------

_RETRYABLE = ("Throttling", "TooManyRequests", "ServiceUnavailable", "ModelTimeout",
              "InternalServer", "ModelStreamError", "ReadTimeout")


def _collect(stream, on_delta) -> dict:
    """Rebuild a Converse-shaped response from ConverseStream events, calling
    on_delta("text", piece) as the reply is written and on_delta("tool", name) when the
    model starts an edit. Reasoning blocks are kept (with their signature) so they can go
    back to the model with the tool results, as Converse requires."""
    blocks: dict[int, dict] = {}
    stop = "end_turn"
    for ev in stream:
        if "contentBlockStart" in ev:
            s = ev["contentBlockStart"]
            use = (s.get("start") or {}).get("toolUse")
            if use:
                blocks[s.get("contentBlockIndex", 0)] = {
                    "toolUse": {"toolUseId": use.get("toolUseId"), "name": use.get("name")},
                    "_json": ""}
                on_delta("tool", use.get("name") or "")
        elif "contentBlockDelta" in ev:
            d = ev["contentBlockDelta"]
            i, delta = d.get("contentBlockIndex", 0), d.get("delta") or {}
            if "text" in delta:
                b = blocks.setdefault(i, {"text": ""})
                b["text"] = b.get("text", "") + delta["text"]
                on_delta("text", delta["text"])
            elif "toolUse" in delta:
                b = blocks.setdefault(i, {"toolUse": {}, "_json": ""})
                piece = (delta["toolUse"] or {}).get("input") or ""
                b["_json"] += piece
                on_delta("tool_input", piece)
            elif "reasoningContent" in delta:
                r = delta["reasoningContent"]
                b = blocks.setdefault(i, {"reasoningContent": {"reasoningText": {"text": ""}}})
                rc = b["reasoningContent"]
                if "text" in r:
                    rc.setdefault("reasoningText", {"text": ""})
                    rc["reasoningText"]["text"] = rc["reasoningText"].get("text", "") + r["text"]
                    on_delta("reasoning", r["text"])
                if "signature" in r:
                    rc.setdefault("reasoningText", {"text": ""})["signature"] = r["signature"]
                if "redactedContent" in r:
                    rc.pop("reasoningText", None)
                    rc["redactedContent"] = r["redactedContent"]
        elif "messageStop" in ev:
            stop = ev["messageStop"].get("stopReason") or stop
        else:
            for k in ("internalServerException", "modelStreamErrorException",
                      "throttlingException", "serviceUnavailableException",
                      "validationException"):
                if k in ev:
                    raise RuntimeError(f"{k}: {ev[k].get('message', '')}")
    content = []
    for i in sorted(blocks):
        b = blocks[i]
        if "toolUse" in b:
            raw = b.pop("_json", "")
            try:
                b["toolUse"]["input"] = json.loads(raw) if raw.strip() else {}
            except json.JSONDecodeError:
                b["toolUse"]["input"] = {}
        content.append(b)
    return {"stopReason": stop, "output": {"message": {"role": "assistant", "content": content}}}


def _cached(messages: list) -> list:
    """`messages` with a cache point after the newest one. The system prompt is cached
    already; this caches the CONVERSATION too — the whole draft and every earlier call of
    this turn — so each fix round or follow-up call reads it from the cache instead of
    processing it again. Cleared from older messages, so at most one is sent."""
    out = []
    for i, m in enumerate(messages):
        content = [b for b in m.get("content") or [] if "cachePoint" not in b]
        if i == len(messages) - 1 and content:
            content = [*content, {"cachePoint": {"type": "default"}}]
        out.append({**m, "content": content})
    return out


def _converse(messages: list, on_delta=lambda kind, piece: None) -> dict:
    """One model call, streamed. A retryable failure before any text reached the page is
    retried; after that the turn reports the error rather than repeat itself."""
    began = time.monotonic()
    for attempt in range(3):
        wrote = [False]
        sizes = {"first": 0.0, "text": 0, "reasoning": 0, "tool_input": 0}

        def seen(kind, piece, wrote=wrote, sizes=sizes):
            wrote[0] = wrote[0] or kind == "text"
            sizes["first"] = sizes["first"] or time.monotonic() - began
            if kind in sizes:
                sizes[kind] += len(piece or "")
            on_delta(kind, piece)
        model, effort = current_model()
        sends_effort = effort in ("low", "medium", "high") and model not in _no_effort
        kw = dict(modelId=model, system=system_prompt(), messages=_cached(messages),
                  toolConfig=TOOLS, inferenceConfig={"maxTokens": MAX_TOKENS})
        if sends_effort:
            kw["additionalModelRequestFields"] = {"output_config": {"effort": effort}}
        try:
            try:
                resp = _bedrock.converse_stream(**kw)
            except Exception as e:
                text = type(e).__name__ + str(e)
                if (_model_at[0] < len(MODELS) - 1
                        and any(t.lower() in text.lower() for t in _ACCESS_DENIED)):
                    # This account may not call this model: use the next one, from now on.
                    _model_at[0] += 1
                    print(f"[designer] {model} refused ({str(e)[:160]}); using "
                          f"{current_model()[0]} instead")
                    return _converse(messages, on_delta)
                if not (sends_effort and "ValidationException" in text
                        and ("output_config" in str(e) or "effort" in str(e))):
                    raise
                # A model that has no `effort`: once, then never again.
                print(f"[designer] {model} does not take effort; calling it without")
                _no_effort.add(model)
                kw.pop("additionalModelRequestFields", None)
                sends_effort = False
                resp = _bedrock.converse_stream(**kw)
            out = _collect(resp["stream"], seen)
            # Where a slow turn's time went, in the function's log.
            print(f"[designer] call {model} (effort {effort if sends_effort else 'model'}) "
                  f"{time.monotonic() - began:.1f}s first-token "
                  f"{sizes['first']:.1f}s reasoning {sizes['reasoning']}c text {sizes['text']}c "
                  f"tool {sizes['tool_input']}c stop={out.get('stopReason')}")
            return out
        except Exception as e:  # retried below, or raised
            name = (type(e).__name__ + str(e)[:200]
                    + str(getattr(e, "response", {}).get("Error", {}).get("Code", "")))
            # A retry must still fit in the function's 300 s, so a call that already
            # waited out a long read timeout is not tried again.
            if attempt < 2 and not wrote[0] and time.monotonic() - began < 90 \
                    and any(t.lower() in name.lower() for t in _RETRYABLE):
                time.sleep(1.5 * (attempt + 1))
                continue
            raise
    raise RuntimeError("unreachable")


class _Live:
    """Writes the reply as it is being written, at most every FLUSH_S, so the page (which
    polls the conversation) shows it growing. phase: thinking | writing | applying |
    checking."""

    FLUSH_S = 0.4

    #: How much of the model's reasoning the page shows while it thinks: the latest part.
    THINKING_TAIL = 600

    def __init__(self, build_id: str, doc: dict, reply: dict, done: list[str] | None = None):
        self.build_id, self.doc, self.reply = build_id, doc, reply
        self.done: list[str] = list(done or [])   # text of earlier model calls in this turn
        self.current = ""
        self.last = 0.0
        self.reasoning = ""
        self.tool_json = ""

    def text(self) -> str:
        return "\n\n".join(p for p in [*self.done, self.current.strip()] if p)

    def flush(self, force: bool = False) -> None:
        if not force and time.monotonic() - self.last < self.FLUSH_S:
            return
        self.reply["text"] = self.text()
        self.reply["updated"] = buildstore.now()
        _store(self.build_id, self.doc)
        self.last = time.monotonic()

    def phase(self, phase: str) -> None:
        if self.reply.get("phase") != phase:
            self.reply["phase"] = phase
            self.flush(force=True)

    def delta(self, kind: str, piece: str) -> None:
        if kind == "text":
            self.current += piece
            if self.reply.get("phase") != "writing":
                self.phase("writing")
            else:
                self.flush()
        elif kind == "tool":
            self.tool_json = ""
            self.reply["progress"] = []
            self.phase("applying")
        elif kind == "tool_input":
            # A large change is written as one long tool call — a minute or more with
            # nothing to show. Name each edit as its JSON streams in ("Adding agent
            # trip_intake"), so the page says what is being built while it is written.
            self.tool_json += piece
            steps = progress_of(self.tool_json)
            if steps != self.reply.get("progress"):
                self.reply["progress"] = steps
                self.flush()
        elif kind == "reasoning":
            # The model's own reasoning, streamed: the latest part, so the wait before a
            # big change shows what it is working out rather than a bare spinner.
            self.reasoning = (self.reasoning + piece)[-self.THINKING_TAIL * 2:]
            self.reply["thinking"] = self.reasoning[-self.THINKING_TAIL:]
            if self.reply.get("phase") != "thinking":
                self.phase("thinking")
            else:
                self.flush()

    def end_call(self) -> None:
        if self.current.strip():
            self.done.append(self.current.strip())
        self.current = ""
        self.reasoning = ""
        self.reply.pop("thinking", None)


_OP_NAMES = {"set_agent": "Adding agent", "update_agent": "Updating agent",
             "remove_agent": "Removing agent", "rename_agent": "Renaming agent",
             "set_tool": "Adding tool", "update_tool": "Updating tool",
             "remove_tool": "Removing tool", "set_tool_code": "Writing the code of",
             "set_steps": "Wiring the stages", "set_block": "Setting", "set_name": "Naming the build"}
_OP_RE = re.compile(r'"op"\s*:\s*"(\w+)"')
_TARGET_RE = re.compile(r'"(?:id|key|name)"\s*:\s*"([^"]{1,80})"')


def progress_of(partial_json: str) -> list[str]:
    """The edits a still-streaming apply_changes call has named so far, in order, as the
    page shows them: ["Adding tool webSearch", "Adding agent trip_intake", ...]. Read from
    the raw JSON with patterns, because it is incomplete until the call ends."""
    out: list[str] = []
    ops = list(_OP_RE.finditer(partial_json))
    used = 0                       # past the previous op and the target it claimed
    for n, m in enumerate(ops):
        end = ops[n + 1].start() if n + 1 < len(ops) else len(partial_json)
        label = _OP_NAMES.get(m.group(1), m.group(1))
        # The op's own target: the first id/key/name just after "op", else just before it
        # — never one the previous op already claimed.
        target = (_TARGET_RE.search(partial_json, m.end(), min(end, m.end() + 200))
                  or _TARGET_RE.search(partial_json, max(used, m.start() - 160), m.start()))
        used = max(m.end(), target.end() if target else 0)
        if m.group(1) in ("set_steps", "set_name"):
            out.append(label)
        elif target:
            out.append(f"{label} {target.group(1)}")
    return out[-40:]


def _model_ids() -> list[str]:
    """Each model the account can use, tagged with what it cannot do, so the designer
    picks an image-reading model for a `vision` agent and a tool-calling one for
    toolMode "model"."""
    try:
        import handler
        out = []
        for m in handler._models().get("models", []):
            tags = ([] if m.get("vision", True) else ["no images"]) + \
                   ([] if m.get("tools", True) else ["no tool calls"]) + \
                   (["needs data-retention opt-in"] if m.get("note") else [])
            out.append(m["id"] + (f" ({', '.join(tags)})" if tags else ""))
        return out
    except Exception:  # noqa: BLE001 - the list is advice for the model, not a requirement
        return []


def _model_problems(workflow: dict) -> list[dict]:
    try:
        import handler
        return handler.model_problems(workflow)
    except Exception:  # noqa: BLE001 - advice, like the model list
        return []


def _context(project: dict) -> str:
    view = {"name": project.get("name"), "workflow": project.get("workflow") or {},
            "prompts": project.get("prompts") or {},
            **({"toolCode": project["toolCode"]} if project.get("toolCode") else {})}
    # Shared items the build uses, as they are now (the caller reached this build, so its
    # items too: bff/library.py refs_for gives a collaborator the owner's).
    import library
    refs = {i: library._public(it) for i in library.ids_in(project) if (it := library._get(i))}
    resolved = library.resolve(project, refs).get("workflow") or {}
    problems = validate_build.validate(resolved) + _model_problems(resolved)
    lines = [
        ("The build as it is right now (the user may have changed it by hand since your "
         "last reply — this is the truth):"),
        "```json", json.dumps(view, ensure_ascii=False, indent=1), "```",
        "Problems the Build view shows for it right now: "
        + (json.dumps([{"severity": i["severity"], "path": i["path"], "message": i["message"]}
                       for i in problems[:40]], ensure_ascii=False) if problems else "none"),
    ]
    ids = _model_ids()
    if ids:
        lines.append("Models this account can use: " + ", ".join(ids))
    return "\n".join(lines)


def _prior(doc: dict, turn: str) -> list[dict]:
    """Every turn before this one."""
    return [t for t in doc.get("turns", []) if t.get("id") not in (turn, f"u{turn}")]


def _turn_text(t: dict) -> str:
    text = t.get("text") or ""
    if t.get("attachments"):
        # Read in full on the turn they came with; later turns only name them.
        text += "\n\n[Attached then: " + ", ".join(a.get("uri") or a.get("name", "")
                                                   for a in t["attachments"]) + "]"
    if t.get("role") == "assistant":
        changes = [c.get("summary") for c in t.get("changes") or [] if c.get("summary")]
        if changes:
            text += "\n\n[Changes you applied: " + "; ".join(changes) + "]"
    return text


def _summarize(doc: dict, turn: str) -> None:
    """Fold the turns that no longer fit verbatim into doc["summary"]. Best-effort: a
    failure leaves them verbatim for the next turn to try again."""
    prior = _prior(doc, turn)
    summary = doc.get("summary") or {}
    done = int(summary.get("through") or 0)
    if len(prior) - done <= HISTORY_TURNS + SUMMARY_BATCH:
        return
    upto = len(prior) - HISTORY_TURNS
    fold = "\n\n".join(f"{'User' if t.get('role') == 'user' else 'Designer'}: "
                        f"{_turn_text(t)[:MESSAGE_MAX]}" for t in prior[done:upto])
    ask = ("You keep the running summary of a conversation in which a user designs an "
           "agent workflow with an AI designer. Merge the earlier summary with the newer "
           "turns into ONE summary of at most 350 words, as short bullet lists under: "
           "Goal; Decisions (what was agreed and built, and why); User preferences (models, "
           "naming, style, what they said no to); Still open (questions unanswered, values "
           "they must give). Keep names, ids and numbers exact. Leave out small talk. "
           "Write only the summary.\n\n"
           f"Earlier summary:\n{summary.get('text') or '(none)'}\n\nNewer turns:\n{fold}")
    try:
        resp = _bedrock.converse(modelId=SUMMARY_MODEL,
                                 messages=[{"role": "user", "content": [{"text": ask}]}],
                                 inferenceConfig={"maxTokens": 1200})
        text = "".join(b.get("text", "") for b in resp["output"]["message"]["content"]).strip()
    except Exception as e:  # noqa: BLE001 - the verbatim turns are still there
        print(f"[designer] summary: {type(e).__name__}: {e}")
        return
    if text:
        doc["summary"] = {"text": text[:SUMMARY_MAX_CHARS], "through": upto,
                          "at": buildstore.now()}


def _history(doc: dict, turn: str) -> list[dict]:
    """Earlier turns as plain text — what was said and what was changed — from where the
    running summary ends. The tool calls themselves are not replayed: the current build
    goes in whole with the new message."""
    msgs = []
    through = int((doc.get("summary") or {}).get("through") or 0)
    for t in _prior(doc, turn)[through:]:
        text = _turn_text(t)
        if t.get("role") == "assistant" and not text:
            continue
        msgs.append({"role": "user" if t.get("role") == "user" else "assistant",
                     "content": [{"text": text[:MESSAGE_MAX]}]})
    # A cap for safety: summarising normally keeps this to HISTORY_TURNS + SUMMARY_BATCH.
    msgs = msgs[-(HISTORY_TURNS + SUMMARY_BATCH):]
    while msgs and msgs[0]["role"] != "user":
        msgs.pop(0)
    # Converse wants strictly alternating roles; merge any neighbours that are not.
    merged: list[dict] = []
    for m in msgs:
        if merged and merged[-1]["role"] == m["role"]:
            merged[-1]["content"][0]["text"] += "\n\n" + m["content"][0]["text"]
        else:
            merged.append(m)
    if merged and merged[-1]["role"] == "user":
        merged.pop()                              # the new user message follows
    return merged


def _secrets_needed(items, workflow: dict) -> list[dict]:
    """The secrets the page must ask for in its secure field: each names a REAL tool (an
    API key, or an OAuth client secret for auth "oauth2") or a real a2a agent (a bearer
    token). Anything else is dropped, so the model cannot make the page ask for a
    secret nothing will use."""
    out, seen = [], set()
    tools, agents = workflow.get("tools") or {}, workflow.get("agents") or {}
    for s in items if isinstance(items, list) else []:
        if not isinstance(s, dict):
            continue
        why = str(s.get("why") or "")[:200]
        tool, agent = str(s.get("tool") or ""), str(s.get("agent") or "")
        identity = str(s.get("identity") or "")
        identities = workflow.get("identities") or {}
        if identity in identities:
            # A named identity's secret: its API key, or its OAuth client's secret.
            oauth = (identities[identity] or {}).get("type") == "oauth2"
            item = {"kind": "identitySecrets", "name": identity, "why": why,
                    "label": "OAuth client secret" if oauth else "API key"}
        elif tool in tools:
            oauth = (tools[tool] or {}).get("auth") == "oauth2"
            item = {"kind": "toolApiKeys", "name": tool, "why": why,
                    "label": "OAuth client secret" if oauth else "API key"}
        elif agent in agents and (agents[agent] or {}).get("runtime") == "a2a":
            item = {"kind": "a2aTokens", "name": agent, "why": why, "label": "Bearer token"}
        else:
            continue
        if (item["kind"], item["name"]) not in seen:
            seen.add((item["kind"], item["name"]))
            out.append(item)
    return out[:20]


def _save_draft(build_id: str, owner: str, email: str, project: dict) -> None:
    builds, *_ = _clients()
    builds.save(build_id, owner, project, email)


def _apply(build_id: str, owner: str, email: str, doc: dict, args: dict,
           fix_rounds: list) -> tuple[dict, dict | None]:
    """Run one apply_changes call. (tool result, change record or None)."""
    builds, s3, bucket = _clients()
    current = builds._draft(build_id)
    if not current.get("workflow"):
        return {"status": "error", "error": "the build has no draft to change"}, None
    needs = [n for n in args.get("needs_input") or [] if isinstance(n, dict)]
    try:
        new, changed = apply_ops(current, args.get("ops"))
    except OpError as e:
        return {"status": "error", "error": str(e), "applied": False,
                "hint": "Nothing from this call was applied, not even its other edits. Fix that "
                        "edit and call apply_changes again with ALL of the ops."}, None
    errors = validate_build.errors(new["workflow"]) + [
        {"severity": "error", "path": m.split(" ", 1)[0], "message": m.split(" ", 1)[1] if " " in m else m}
        for m in builds.code_errors(new)]
    blocking = _blocking(errors, needs)
    if blocking and fix_rounds[0] < MAX_FIX_ROUNDS:
        fix_rounds[0] += 1
        return {"status": "rejected", "applied": False,
                "errors": [{"path": e["path"], "message": e["message"]} for e in blocking[:20]],
                "hint": "Nothing was applied. Fix these and call apply_changes again with the "
                        "whole corrected set of ops. If a value can only come from the user, "
                        "list its path in needs_input instead."}, None
    revision = int(doc.get("revision") or 0) + 1
    s3.put_object(Bucket=bucket, Key=revision_key(build_id, revision),
                  Body=json.dumps(current, ensure_ascii=False).encode(),
                  ContentType="application/json")
    _save_draft(build_id, owner, email, new)
    doc["revision"] = revision
    record = {"summary": str(args.get("summary") or "")[:300], "revision": revision,
              "changed": changed,
              "defaultsUsed": [str(d)[:200] for d in args.get("defaults_used") or []][:30],
              "needsInput": [{"path": str(n.get("path"))[:120],
                              "question": str(n.get("question"))[:300]} for n in needs][:30],
              "secretsNeeded": _secrets_needed(args.get("secrets_needed"), new["workflow"]),
              "problems": [{"path": e["path"], "message": e["message"]} for e in errors[:20]]}
    return {"status": "applied", "revision": revision,
            # What THIS call changed, so the reply describes what was saved and not what
            # was meant. Observed live: a rejected call carried the writer's memory, the
            # resent one only the editor's vision, and the reply said both were done.
            "changed": changed,
            "check": "Only what `changed` lists was saved by this call. If the user asked for "
                     "something not listed here or in an earlier applied call this turn, it was "
                     "NOT applied: apply it now, before you reply. Never describe a change as "
                     "made unless an applied call saved it.",
            "agents": sorted(new["workflow"].get("agents", {})),
            "stages": len(new["workflow"].get("steps", [])),
            "remaining_problems": record["problems"]}, record


def _undo(build_id: str, owner: str, email: str, doc: dict) -> tuple[dict, dict | None]:
    _b, s3, bucket = _clients()
    revision = int(doc.get("revision") or 0)
    if revision < 1:
        return {"status": "error", "error": "there is no change of yours to undo"}, None
    try:
        before = json.loads(s3.get_object(Bucket=bucket,
                                          Key=revision_key(build_id, revision))["Body"].read())
    except s3.exceptions.NoSuchKey:
        return {"status": "error", "error": "that change can no longer be undone"}, None
    _save_draft(build_id, owner, email, before)
    doc["revision"] = revision - 1
    return {"status": "undone", "revision": revision - 1}, {
        "summary": "Undid the last change", "revision": revision - 1, "undo": True,
        "changed": {"agents": sorted((before.get("workflow") or {}).get("agents", {})),
                    "tools": [], "steps": True, "blocks": [], "removed": []}}


def _continue(event: dict) -> None:
    """Start the next part of a turn in a fresh invocation of this function."""
    boto3.client("lambda", region_name=REGION).invoke(
        FunctionName=os.environ["AWS_LAMBDA_FUNCTION_NAME"], InvocationType="Event",
        Payload=json.dumps(event).encode())


def run_turn(event: dict) -> dict:
    """The background half of a turn (handler dispatches {"action": "design"} here)."""
    builds, *_ = _clients()
    email, turn = str(event.get("email") or ""), str(event.get("turn") or "")
    # The caller, with their email: someone a build is shared with reaches it by that.
    build_id, owner = str(event.get("build") or ""), builds.Who(str(event.get("owner") or ""), email)
    try:
        builds._meta(build_id, owner)
    except builds.BuildError:
        return {"ok": False}
    doc = _load(build_id)
    cont = int(event.get("cont") or 0)
    if doc.get("pending") != turn or doc.get("status") != "thinking" \
            or cont != int(doc.get("cont") or 0):
        return {"ok": True, "skipped": True}     # a duplicate delivery of a finished turn
    reply = next(t for t in doc["turns"] if t.get("id") == turn)
    doc["attempts"] = int(doc.get("attempts") or 0) + 1
    if doc["attempts"] > 1:
        # Lambda redelivered a turn that crashed or timed out part-way: say so, don't loop.
        reply.update(status="error", text="That reply was interrupted. Please send your "
                                          "message again.")
        doc.update(status="idle", pending="")
        _store(build_id, doc)
        return {"ok": False}
    _store(build_id, doc)
    started = time.monotonic()
    user_turn = next((t for t in doc["turns"] if t.get("id") == f"u{turn}"), {})
    user_text = user_turn.get("text", "")
    if not cont:
        _summarize(doc, turn)
    messages = _history(doc, turn)
    earlier = (doc.get("summary") or {}).get("text")
    messages.append({"role": "user", "content": [
        *([{"text": "Summary of the earlier part of this conversation (older turns are not "
                    "repeated):\n" + earlier}] if earlier else []),
        {"text": _context(builds._draft(build_id))}, {"text": user_text},
        # A continuation: this same turn ran out of one invocation's time. The draft above
        # already has what it applied, so say what that was and ask for the rest.
        *([{"text": "You are continuing YOUR OWN reply to the message above: the previous "
                    "part ran out of time. Already applied in this turn (the build above "
                    "includes it): " + "; ".join(c.get("summary") or "a change"
                                                  for c in reply.get("changes") or [])
                    + ". Do not redo or describe those again; apply what is left of the "
                      "request, then reply."}] if cont else [])]})
    if user_turn.get("attachments"):
        try:
            messages[-1]["content"] += [
                {"text": "The user attached these files as context: "
                         + ", ".join(a.get("uri") or a["name"] for a in user_turn["attachments"])},
                *_attachment_blocks(user_turn["attachments"])]
        except Exception as e:  # noqa: BLE001 - say which, and end the turn
            print(f"[designer] attachments: {type(e).__name__}: {e}")
            reply.update(status="error", text=f"I couldn't read the attached files "
                                              f"({type(e).__name__}). Please attach them again.")
            reply.pop("phase", None)
            doc.update(status="idle", pending="")
            _store(build_id, doc)
            return {"ok": False}
    changes: list[dict] = list(reply.get("changes") or []) if cont else []
    fix_rounds = [0]
    cut_off = 0
    live = _Live(build_id, doc, reply, done=[reply["text"]] if cont and reply.get("text") else None)
    live.phase("thinking")
    note = ""
    try:
        for _ in range(MAX_CALLS):
            if time.monotonic() - started > TURN_BUDGET_S:
                if cont < MAX_CONTINUATIONS:
                    # Carry on in a fresh invocation rather than stop with half a build:
                    # it starts from the saved draft (which has every applied change) and
                    # is told what this part already did.
                    live.end_call()
                    reply.update(text=live.text(), changes=changes, phase="thinking")
                    doc.update(cont=cont + 1, attempts=0)
                    _store(build_id, doc)
                    _continue({**event, "cont": cont + 1})
                    print(f"[designer] turn {turn}: continuing in invocation {cont + 2}")
                    return {"ok": True, "continued": cont + 1}
                note = "That took too long, so I stopped. What I changed so far is saved."
                break
            resp = _converse(messages, live.delta)
            live.end_call()
            out = resp["output"]["message"]
            if resp.get("stopReason") == "max_tokens" and cut_off < 2:
                # The reply ran out of room mid-change (a whole large draft in one
                # apply_changes): the change is incomplete, so drop it and ask for it in
                # parts rather than end the turn with nothing applied and no word why.
                cut_off += 1
                kept = [b for b in out.get("content", []) if "toolUse" not in b] or [{"text": "(cut off)"}]
                messages.append({"role": "assistant", "content": kept})
                messages.append({"role": "user", "content": [{"text": (
                    "Your last reply was cut off at the output limit, so that change was NOT "
                    "applied. Apply it in smaller parts: a few agents (with their prompts) per "
                    "apply_changes call, then the steps, then the rest.")}]})
                live.phase("thinking")
                continue
            messages.append(out)
            if resp.get("stopReason") != "tool_use":
                break
            results = []
            for block in out.get("content", []):
                use = block.get("toolUse")
                if not use:
                    continue
                if use.get("name") == "apply_changes":
                    live.phase("checking")
                    result, record = _apply(build_id, owner, email, doc, use.get("input") or {},
                                            fix_rounds)
                elif use.get("name") == "undo_last_change":
                    result, record = _undo(build_id, owner, email, doc)
                else:
                    result, record = {"status": "error", "error": "unknown tool"}, None
                if record:
                    changes.append(record)
                    reply["changes"] = changes
                    live.flush(force=True)       # the page shows each change as it lands
                results.append({"toolResult": {"toolUseId": use["toolUseId"],
                                               "content": [{"json": result}]}})
            messages.append({"role": "user", "content": results})
            live.phase("thinking")
        else:
            note = "I made several changes but ran out of steps. Tell me what to do next."
        text = live.text() or note
        if note and live.text():
            text += "\n\n" + note
        reply.update(status="done", text=text.strip() or "Done.", changes=changes)
    except Exception as e:  # noqa: BLE001 - every failure must reach the page
        print(f"[designer] {type(e).__name__}: {e}")
        live.end_call()
        partial = live.text()
        reply.update(status="error", changes=changes,
                     text=(partial + "\n\n" if partial else "")
                     + f"I couldn't reach the model just now ({type(e).__name__}). "
                       "Anything I changed before that is saved; please try again.")
    for live_only in ("phase", "progress", "thinking"):
        reply.pop(live_only, None)
    reply["at"] = buildstore.now()
    doc.update(status="idle", pending="", cont=0)
    _store(build_id, doc)
    return {"ok": True}
