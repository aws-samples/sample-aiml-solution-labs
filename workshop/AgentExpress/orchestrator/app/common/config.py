"""Central configuration.

The workflow (agents + topology + HITL gates) is defined once in workflow.json
and loaded here. Everything else (graph, UI, BFF) derives from it, so adding an
agent is a config change plus a new agent module — no changes to the framework.

Load order for the workflow definition:
  1. WORKFLOW_JSON env var (inline JSON) — used by the Lambda BFF.
  2. app/workflow.json on disk — used by the runtime and local dev.
"""

import json
import os
from pathlib import Path

from app.common import defaults, runtime_secret, vocabulary

# Credentials from RUNTIME_SECRET_ARN into the environment, before anything below (or
# any module importing this one) reads them.
runtime_secret.load()

_CONFIG_PATH = Path(__file__).resolve().parent.parent / "workflow.json"


def load_workflow() -> dict:
    inline = os.getenv("WORKFLOW_JSON")
    if inline:
        return json.loads(inline)
    return json.loads(_CONFIG_PATH.read_text())


WORKFLOW = load_workflow()
AGENTS: dict = WORKFLOW["agents"]
STEPS: list = WORKFLOW["steps"]
NODE_IDS: list = list(AGENTS.keys())

# The final agent in the pipeline (used to surface the run's result). The last
# step may be a single agent, a parallel group, or a sequence group.
_LAST_STEP = STEPS[-1]
LAST_AGENT_ID = _LAST_STEP.get("agent") or (
    _LAST_STEP.get("parallel") or _LAST_STEP.get("sequence"))[-1]

# The FIRST agent in the pipeline. Its output is the run's authoritative brief:
# every downstream agent reads it for the objective/title that frames the work
# (see app/common/research.py and app/common/synthesis.py).
#
# Derived from the topology rather than hardcoded, so the first agent can be
# called anything. It used to be the literal "intake", which meant renaming that
# agent silently dropped the brief everywhere instead of failing.
_FIRST_STEP = STEPS[0]
FIRST_AGENT_ID = _FIRST_STEP.get("agent") or (
    _FIRST_STEP.get("parallel") or _FIRST_STEP.get("sequence"))[0]

# Engine-level settings that describe the orchestrator itself (not an agent).
# The app reads 'defaultModel' and 'ui'; the IaC reads the rest (policy, chatbot,
# guardrail). Environment variables still take precedence at deploy time.
ORCHESTRATOR: dict = WORKFLOW.get("orchestrator", {})

# How the orchestrator calls a DEDICATED agent runtime (app/common/agentcore_agent.py),
# and specifically WHETHER THE SDK MAY SILENTLY RUN AN AGENT TWICE.
#
# InvokeAgentRuntime is synchronous, slow (a research agent takes ~15-20s) and NOT
# idempotent: the remote container starts working the moment the request lands, and
# it has no idea whether the caller is still listening. boto3's default client
# enables retries — `retries={'mode': 'legacy'}`, up to 5 attempts — so one
# transient connection blip mid-call makes the SDK re-issue the request, the remote
# agent runs the WHOLE thing again, and the orchestrator returns whichever attempt
# answered last. The first attempt's model call is still billed, and nothing logs it:
# the node logs "Invoking dedicated AgentCore Runtime" once, before the retry exists.
#
# That is not hypothetical. On a live run the web_search runtime logged two
# invocations with different requestIds, 13s apart, for one node execution:
#   13:55:29  Invocation completed successfully (17.792s)  req 95297bd8...
#   13:55:41  Invocation completed successfully (16.669s)  req 26ad54ce...
# Two identical model calls, 6579 input tokens each, one result discarded, $0.0158
# of the run's $0.2448 spent on an answer nobody read.
#
# So retries are OFF by default here (maxAttempts 1 = one attempt, no retry) and the
# read timeout is generous instead. For a call this long the failure that matters is
# a LOST RESPONSE to work that already succeeded, and retrying that is strictly
# worse than failing: a raised error surfaces to the reviewer, whereas a duplicate
# silently double-bills. The framework already prefers a visible failure over a
# quiet degradation everywhere else (see app/common/errors.py).
#
#   maxAttempts         total attempts per invoke, not retries-after-the-first.
#                       1 disables retrying. Raise it only if you have made the
#                       call idempotent.
#   readTimeoutSeconds  how long to wait for the agent's response. Must exceed the
#                       slowest agent's wall-clock or you will time out mid-answer
#                       and, with maxAttempts 1, lose the run.
RUNTIME_INVOKE: dict = {**defaults.get("orchestrator", "runtimeInvoke"),
                        **(ORCHESTRATOR.get("runtimeInvoke") or {})}

# How the orchestrator calls a REMOTE agent over A2A (app/common/a2a_agent.py) — an
# agent this deployment does not operate, reached at its Agent Card URL.
#
# Separate from runtimeInvoke on purpose. A dedicated runtime is ours: we know how
# slow it is and we control its timeout. A remote agent is somebody else's service
# behind an interface that is explicitly allowed to be slow — A2A models a unit of
# work as a Task precisely so it can take minutes and be polled — so the budget that
# matters is not one request timeout but how long we are willing to wait overall.
#
#   timeoutSeconds      per HTTP request (the card fetch, each RPC call).
#   pollIntervalSeconds gap between `tasks/get` calls while their task is working.
#                       Their rate limit is unknown to us; a busy loop is rude and
#                       may be throttled, which looks like their agent failing.
#   maxPollSeconds      total wall-clock before we give up and fail the run with the
#                       last state we saw. Bounded rather than open-ended: a task
#                       that never leaves "working" would otherwise hold the whole
#                       workflow until the runtime's own 8-hour ceiling.
A2A_INVOKE: dict = {**defaults.get("orchestrator", "a2aInvoke"),
                    **(ORCHESTRATOR.get("a2aInvoke") or {})}
# Per-model token rates for the cost figures in the observability UI, keyed by a
# substring of the model id:
#
#   "modelRates": { "nova-pro": { "input": 0.80, "output": 3.20 } }
#
# OPTIONAL, and the reason it exists is that `defaultModel` and each agent's `model`
# are config: a customer is invited to choose a model, and
# app/features/observability/pricing.py only knows a handful of Claude ids. Anything
# else was priced at a fallback and reported as though measured. Now an unknown model
# is MARKED on the telemetry row (`rates_known=False`), and this key is how a customer
# supplies the real numbers — or corrects a price that changed — without editing
# framework code.
MODEL_RATES: dict = ORCHESTRATOR.get("modelRates") or {}
# Price models from the AWS Price List API for this region (observability/pricelist.py)
# when modelRates has no entry. On by default; false keeps every lookup offline, on the
# built-in snapshot in pricing.py.
PRICE_LIST: bool = bool(ORCHESTRATOR.get("priceList", defaults.get("orchestrator", "priceList")))
# Overrides for the non-model rates in observability/pricing.py (embedding, AgentCore
# Runtime compute and footprint, Gateway), keyed as pricing._SERVICE_DEFAULTS.
SERVICE_RATES: dict = ORCHESTRATOR.get("serviceRates") or {}
# Per-image prices for image models, keyed by a substring of the model id (USD per image).
# Win over the AWS Price List and the snapshot in vocabulary.json.
IMAGE_RATES: dict = ORCHESTRATOR.get("imageRates") or {}
# AgentCore Insights timings (lookbackHours, pollTimeoutSeconds, pollIntervalSeconds).
# Insights was the one feature with NO config block, so a deployment whose runs take
# longer to analyse than the built-in fifteen minutes had to edit framework code.
INSIGHTS: dict = ORCHESTRATOR.get("insights") or {}


# Presentation strings (title, the default topic, placeholders). Config rather
# than literals so re-branding for a different use case is a workflow.json edit.
UI: dict = WORKFLOW.get("ui", {})
DEFAULT_TOPIC: str = str(UI.get("defaultTopic") or "")

# --- runtime / infra settings (env-driven, then orchestrator block) -------

REGION = os.getenv("AWS_REGION", "us-east-1")
# BEDROCK_MODEL_ID is what both IaC paths resolved (an explicit -c modelId / model_id,
# else this workflow's defaultModel, else the framework default) — already moved to the
# deployment region's geography. The rewrite is repeated here for local runs and for a
# deployment that predates it, and is a no-op on an id that is already right.
MODEL_ID = vocabulary.regional_model_id(
    os.getenv("BEDROCK_MODEL_ID")
    or ORCHESTRATOR.get("defaultModel")
    or defaults.get("orchestrator", "defaultModel"), REGION)


def model_for(model_id: str | None) -> str:
    """The model an agent's call uses: its own `model`, else MODEL_ID, in this
    region's inference-profile geography (see vocabulary.regional_model_id)."""
    return vocabulary.regional_model_id(model_id or MODEL_ID, REGION)
MEMORY_ID = os.getenv("MEMORY_ID")
STATUS_TABLE = os.getenv("STATUS_TABLE")
EVENTS_TABLE = os.getenv("EVENTS_TABLE")

MCP_TIMEOUT = float(os.getenv("MCP_TIMEOUT", "25"))

# AgentCore Gateway (the tool plane). Agents reach every tool through one
# JWT-authed Gateway endpoint; the runtime fetches a client-credentials token.
# An agent names a tool with its `tool` field in workflow.json, matching a KEY in
# the `tools` block, and the Gateway routes to that target. Swapping a target
# (a different MCP server, a REST API, your own Lambda) is a workflow.json edit.
GATEWAY_URL = os.getenv("GATEWAY_URL", "")
GATEWAY_TOKEN_URL = os.getenv("GATEWAY_TOKEN_URL", "")
GATEWAY_CLIENT_ID = os.getenv("GATEWAY_CLIENT_ID", "")
GATEWAY_CLIENT_SECRET = os.getenv("GATEWAY_CLIENT_SECRET", "")
# What the token is for: the OAuth2 `scope` (Cognito), the API identifier sent as
# `audience` (Auth0), or the `aud` the token carries (Okta: the authorization
# server's audience; Entra: the API app's client id).
GATEWAY_AUDIENCE = os.getenv("GATEWAY_AUDIENCE", "")
# The scope to request, where it is not the audience: Okta (a custom scope on the
# authorization server, required) and Entra (default "<audience>/.default").
GATEWAY_SCOPE = os.getenv("GATEWAY_SCOPE", "")
# Which client-credentials request shape to build: "cognito" | "auth0" | "okta" |
# "entra". Set by the IaC from the `idp` variable (see terraform/identity.tf).
# Defaults to cognito so an older deployment keeps working.
GATEWAY_AUTH_FLOW = os.getenv("GATEWAY_AUTH_FLOW", "cognito").lower()

def step_agents(step: dict) -> list[str]:
    """The agent ids in one `steps` entry, whatever its shape."""
    if "agent" in step:
        return [step["agent"]]
    return list(step.get("parallel") or step.get("sequence") or [])


# Every agent id in the order the pipeline runs them. Derived from `steps` rather
# than from NODE_IDS (the `agents` map's key order), which is only conventionally
# the same — nothing enforces that a customer lists the agents in step order.
AGENT_ORDER: list[str] = [a for s in STEPS for a in step_agents(s)]


def upstream_of(agent_id: str) -> list[str]:
    """Agent ids that run BEFORE `agent_id`, in reverse topology order.

    Derived from the `steps` topology so a downstream agent automatically reads
    every new upstream agent: add a research agent to the parallel group and the
    analysis agent picks it up with no code change. Reverse order puts the most
    recent (most specific) assets first, which is how the synthesis agents want
    their context.

    Within the agent's OWN step, membership depends on the step kind:
      * "parallel" — the others are peers running concurrently, so they are NOT
        upstream (a group's members must not depend on each other).
      * "sequence" — members listed BEFORE this agent already ran, so they are.

    Raises ValueError if `agent_id` is in no step. That case is a mismatch between
    an agent module's own id and the workflow — the synthesis agents call this at
    import with their id (`upstream_of("report")`), so renaming the agent in
    workflow.json without renaming it in the module lands here. Falling off the end
    of the loop would return EVERY agent, including this one, and the agent would
    quietly synthesize from its own previous output. Failing at container start with
    the reason is the better trade.
    """
    seen: list[str] = []
    for step in STEPS:
        ids = step_agents(step)
        if agent_id in ids:
            if "sequence" in step:
                seen.extend(ids[: ids.index(agent_id)])
            return list(reversed(seen))
        seen.extend(ids)
    raise ValueError(
        f"upstream_of({agent_id!r}): no `steps` entry runs that agent. Agents in the "
        f"topology: {', '.join(NODE_IDS) or '(none)'}. If you renamed this agent in "
        f"workflow.json, rename its folder under app/subagents/ and the id it passes "
        f"to upstream_of() to match.")


#: The ONLY tool keys the running app ever sees. Three places have to agree on this
#: list — this one, `tools_env` in terraform/tools.tf, and `toolsEnv` in
#: cdk/lib/orchestrator-stack.ts — because each builds TOOLS_JSON independently. A key
#: missing from one of them is SILENT: the tool still works, just without whatever that
#: key configured, and only under that one deployment path. It has happened twice.
#: `publishedFrom`/`publishedTo` reached the app under Terraform and not under CDK; then
#: `rowPath` was added here and to neither IaC path, so a live agent read zero rows from
#: a response that was full of them and reported "nothing found". Named as a constant so
#: tests/test_config_keys.py can hold all three to it.
#:
#: What is deliberately NOT here is as load-bearing as what is. `endpoint`, `source`,
#: `lambdaArn`, `schemaS3Uri`, `toolSchema`, `auth` and `policy` are deploy-time detail
#: the IaC consumes; the app neither needs them nor should be able to put them in a
#: request. `domains` is the sharpest case: web search's domain filter is enforced ON THE
#: GATEWAY TARGET, so if the app could send one it would be offering a caller-supplied
#: scope in place of a boundary.
APP_TOOL_KEYS = ("type", "corpora", "maxResults", "publishedFrom", "publishedTo",
                 "call", "arg", "args", "rowPath", "rowFields")


def _load_tools() -> dict:
    """The `tools` block from app/workflow.json, injected by the IaC as TOOLS_JSON:

        {"kb":        {"type": "kb", "corpora": [...]},
         "websearch": {"type": "websearch", "maxResults": 10},
         "docs":      {"type": "mcp", "call": "...", "arg": "..."}}

    The app reads this to know HOW to call each tool (its argument shape), so
    adding a data source stays a config change. Only the call-shape fields are
    kept — the rest of a tool entry is deploy-time detail (endpoint, policy,
    schema) that the IaC consumes and the app must never send in a request.
    Falls back to the file when the env var is absent (local dev), then to {}.
    """
    raw = os.getenv("TOOLS_JSON", "").strip()
    if raw:
        try:
            return json.loads(raw)
        except ValueError:
            pass
    # Local dev / BFF: fall back to the `tools` block of the workflow already
    # loaded above, keeping only the call-shape fields the app needs.
    return {
        name: {k: v for k, v in (spec or {}).items() if k in APP_TOOL_KEYS}
        for name, spec in (WORKFLOW.get("tools") or {}).items()
    }


TOOLS: dict = _load_tools()

#: Tools that act as the PERSON using the app (auth "user": each person's own account;
#: "obo": the person's sign-in exchanged): reached on the person Gateway with the
#: caller's sign-in, never the agents' machine client (app/features/gateway/person.py).
PERSON_TOOLS: frozenset = frozenset(
    name for name, spec in (WORKFLOW.get("tools") or {}).items()
    if isinstance(spec, dict) and str(spec.get("auth") or "").lower() in ("user", "obo"))
#: The person Gateway's MCP endpoint ("" when no tool acts as the person).
GATEWAY_USER_URL = os.getenv("GATEWAY_USER_URL", "")


def tool_list(value) -> list[str]:
    """An agent's `tool` value as a list of tool keys, in declared order.

    `tool` is one key or a list of them, and every reader wants the list: this is the
    one place that turns either form into it, so no caller has to know both shapes.
    """
    if not value:
        return []
    if isinstance(value, str):
        return [value]
    return [str(v) for v in value if v]
