"""Agent registry: builds the map of agent_id -> Agent instance from workflow.json.

An agent's `runtime` decides its implementation:
  * runtime "main"      -> import app.subagents.<module> and run it in-process
                           (a LangGraph node inside the orchestrator runtime).
  * runtime "dedicated" -> AgentCoreRuntimeAgent: the orchestrator invokes that
                           agent's OWN AgentCore Runtime (its own container),
                           passing/returning the same data an in-process agent
                           would. The dedicated runtime itself runs the real
                           module via build_agent_module() below.
  * runtime "a2a"       -> A2AAgent: the step is run by an agent THIS WORKFLOW DOES
                           NOT OWN, reached over the Agent2Agent protocol at its
                           Agent Card URL. No module under app/subagents/, because
                           the code is somebody else's.
All three expose the same Agent interface, so the graph wiring is identical.

The first two are placements of OUR code; the third is a trust boundary. That is why
`a2a` is a `runtime` value and not a `tool`: a tool returns data for one of our
agents to reason over, whereas this replaces the agent.
"""

import importlib
import re

from app.common import defaults, vocabulary
from app.common.a2a_agent import A2AAgent
from app.common.agentcore_agent import AgentCoreRuntimeAgent
from app.common.base import Agent
from app.common.config import AGENTS, tool_list


def _configure(agent: Agent, agent_id: str, spec: dict) -> Agent:
    """Copy the workflow.json spec onto an Agent instance."""
    agent.id = agent_id
    agent.name = spec.get("name", agent_id)
    agent.kind = spec.get("kind", defaults.get("agent", "kind"))
    agent.runtime = spec.get("runtime", defaults.get("agent", "runtime"))
    # The tools this agent is bound to (keys in the workflow.json `tools` block —
    # `tool` is one key or a list), and for a Knowledge Base tool the corpus it is
    # scoped to. `agent.tool` stays the FIRST, for agent code written when an agent had
    # exactly one tool (`ctx.tool or "pricing"`); new code reads `tools`.
    agent.tools = tool_list(spec.get("tool"))
    agent.tool = agent.tools[0] if agent.tools else None
    agent.corpus = spec.get("corpus")
    # "direct" queries every bound tool before the model reasons; "model" lets the model
    # choose tools and arguments (app/common/tool_loop.py), up to maxToolCalls.
    agent.tool_mode = str(spec.get("toolMode") or defaults.get("agent", "toolMode"))
    agent.max_tool_calls = int(spec.get("maxToolCalls") or defaults.get("agent", "maxToolCalls"))
    # The build's skills this agent may use (app/common/skills.py): offered to the model
    # to open in toolMode "model", added in full otherwise. Names only; content stays in
    # the workflow's `skills`.
    agent.skills = [str(s) for s in spec.get("skills") or [] if isinstance(s, str)]
    agent.model = spec.get("model")  # None -> config.MODEL_ID default
    agent.temperature = spec.get("temperature", defaults.get("agent", "temperature"))
    # Unset means the model's own default; see app/common/llm.py for models that refuse
    # a sampling setting.
    agent.top_p = spec.get("topP")
    agent.stop_sequences = [str(s) for s in spec.get("stopSequences") or []]
    # An image agent (output "image") renders its brief with ctx.image; see images.py.
    agent.output = spec.get("output", defaults.get("agent", "output"))
    agent.image = dict(spec.get("image") or {})
    # Images from earlier agents this agent's model reads (ctx.llm; see images.load_for).
    agent.vision = dict(spec.get("vision") or {})
    # The files the run was started with (uploads, s3:// paths), as model content; see
    # app/common/attachments.py.
    agent.attachments = spec.get("attachments", defaults.get("agent", "attachments")) is True
    # Output budget for this agent's model calls, in tokens. camelCase to match the
    # rest of workflow.json (the old snake_case `max_tokens` key was never set by
    # any config, so every agent silently used the 300 default and then overrode it
    # with a literal at the call site — which put the budget in code, not config).
    #
    # It matters per agent: a research agent emits a full structured JSON payload
    # with several classified findings, and a budget that is too small truncates it
    # MID-JSON, which surfaces as "the model returned no parseable research JSON"
    # rather than as an obvious limit problem.
    agent.max_tokens = int(spec.get("maxTokens") or defaults.get("agent", "maxTokens"))
    # AgentCore feature flags (memory, guardrails, evaluations, policy, …).
    # AgentContext reads these to decide which features apply to this agent.
    agent.agentcore = dict(spec.get("agentcore") or {})
    # Where a remote (runtime "a2a") agent lives, and how we authenticate to it.
    # Set unconditionally so the attributes are never missing; they are read only by
    # A2AAgent, and validated at load for exactly the agents that use them.
    agent.agent_card = str(spec.get("agentCard") or "")
    agent.auth = str(spec.get("auth") or defaults.get("agent", "auth")).lower()
    return agent


RUNTIMES = vocabulary.RUNTIMES
AUTH_MODES = vocabulary.A2A_AUTH_MODES

#: The `type` a `tools` entry may declare. DELIBERATELY CLOSED, and it is the right
#: boundary rather than an oversight: each value is a different kind of Gateway target
#: with different infrastructure behind it — a Knowledge Base on S3 Vectors, a managed
#: connector, a Streamable HTTP MCP target, an OpenAPI target with a schema in S3, a
#: Lambda target with an inline schema. A sixth kind needs infrastructure that config
#: cannot describe, so it needs framework code by definition.
#:
#: What keeps that from being a limit on customers is `type: "lambda"` + `lambdaArn`:
#: a function you already own, registered as a tool. Anything reachable from a Lambda
#: is reachable — a warehouse, an RDBMS, something inside a VPC, a gRPC service, a
#: vendor SDK — without touching this list.
#:
#: Validated HERE as well as in both IaC paths because the app used to default an
#: unrecognised type to "mcp" (`spec.get("type", "mcp")` in the Gateway client). A
#: deployment cannot reach that — plan/synth rejects the type first — but local dev and
#: a WORKFLOW_JSON override can, and it made a typo'd type issue a plausible-looking
#: MCP call instead of an error.
TOOL_TYPES = vocabulary.TOOL_TYPES

#: Long-term memory strategies an agent may name in `agentcore.memory.longTerm`.
#: These are the ones the IaC actually PROVISIONS on the AgentCore memory — see
#: `semantic_memory_strategy` / `summary_memory_strategy` in terraform/main.tf and the
#: equivalent in cdk/lib/orchestrator-stack.ts — and the ones `context._NS_PREFIX`
#: knows how to build a namespace for.
#:
#: Validated because an unrecognised value was SILENT: `_namespace_for` falls back to
#: using the strategy name as its own prefix, so `longTerm: ["userPreference"]`
#: searched `userPreference/<actor>` — a namespace nothing ever writes — found
#: nothing, and reported a successful recall. The observability panel showed memory
#: working. That is the exact failure mode `tests/test_config_keys.py` exists to
#: prevent, and it slipped through because the KEY was allowed and only its VALUE was
#: wrong. `user_preference` was listed in the code as supported for a while with no
#: strategy behind it, which is how this was found.
MEMORY_STRATEGIES = vocabulary.MEMORY_STRATEGIES
#: The only `source` value: the stand-in A2A server this repo ships. Like a tools
#: entry's `source`, it asks the framework to DEPLOY the thing and inject its URL,
#: because a Function URL is generated by the deploy and cannot be committed.
A2A_SOURCES = vocabulary.A2A_SOURCES
#: Skills the shipped stand-in publishes (a2a_lambda/handler.py SKILLS). Only
#: meaningful with `source`: an EXTERNAL agent's skills are its own business, and
#: this framework has no way to know them.
A2A_LAMBDA_SKILLS = vocabulary.A2A_LAMBDA_SKILLS


#: What a `tools` KEY may be, and it is narrower than either AWS constraint on its own,
#: because the key is used to build TWO names with INCOMPATIBLE rules:
#:
#:   the Gateway target      `<key>`            ^([0-9a-zA-Z][-]?){1,100}$   no underscores
#:   the Cedar policy        `permit_<key>`     ^[A-Za-z][A-Za-z0-9_]*$      no hyphens
#:
#: So `policy_docs` is refused by the first and `policy-docs` by the second, and the
#: intersection is alphanumeric. Which is no real imposition — camelCase is already the
#: house style throughout workflow.json (`maxTokens`, `agentCard`, `gateId`), so
#: `policyDocs` is the form that matches the rest of the file anyway.
#:
#: BOTH of these were found by deploying a foreign workflow, one after the other, each
#: after a clean `cdk synth`: CloudFormation refused the change set, which is the last
#: possible place to learn it. Every tool in the shipped sample is a single alphanumeric
#: word, so nothing had ever exercised a separator.
#:
#: Mirrored in cdk/lib/orchestrator-stack.ts, terraform/tools.tf and the generated schema.
TARGET_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9]*$")


def validate_tool_types() -> None:
    """Reject a `tools` entry whose `type` this app does not know how to call.

    Both IaC paths reject it at plan/synth, so a deployed system never reaches here.
    This exists for local dev and for a WORKFLOW_JSON override, where the old
    `spec.get("type", "mcp")` default turned a typo into a plausible-looking MCP call
    against a target that was never provisioned.
    """
    from app.common.config import TOOLS

    for label, spec in (TOOLS or {}).items():
        # The tool key becomes the Gateway TARGET name, which AWS restricts to letters,
        # digits and hyphens. Underscores pass every local check — the JSON schema, both
        # IaC synths — and are then refused by CloudFormation when the change set is
        # created, which is the latest possible place to find out. Checked here too so a
        # local run fails the same way a deploy would.
        if not TARGET_NAME_RE.match(label):
            suggestion = re.sub(r"[_-](\w)", lambda m: m.group(1).upper(),
                                re.sub(r"[^A-Za-z0-9_-]", "", label)) or "myTool"
            raise ValueError(
                f"workflow.json tools.{label!r} — a tool's key must match "
                f"{TARGET_NAME_RE.pattern} (letters and digits, starting with a letter). "
                f"Try {suggestion!r}, and update the `tool` field of any agent bound to it.\n"
                f"WHY IT IS THIS NARROW: the key is used to build two AWS names whose rules "
                f"contradict each other — the Gateway target is `{label}` and forbids "
                f"underscores, while the Cedar policy is `permit_{label}` and forbids "
                f"hyphens. So neither separator survives both, and the intersection is "
                f"alphanumeric. camelCase matches the rest of workflow.json anyway "
                f"(`maxTokens`, `agentCard`). Note an AGENT id is different and may contain "
                f"underscores, because it becomes part of a runtime name instead.")
        declared = str((spec or {}).get("type") or "")
        if declared.lower() not in TOOL_TYPES:
            raise ValueError(
                f"tool {label!r} has type {declared or '(none)'!r}; valid values are "
                f"{', '.join(TOOL_TYPES)}. Each is a different kind of Gateway target, "
                f"so the set is closed by what the framework can provision — to reach "
                f"something else, put it behind a Lambda and use type \"lambda\" with "
                f"\"lambdaArn\".")


def validate_features() -> None:
    """Reject an `agentcore` VALUE that would be read as working and do nothing.

    `tests/test_config_keys.py` already closes the set of agentcore KEYS, for the
    stated reason that a switch which appears to control something and does not is
    worse than no switch at all. Both settings below were the same bug one level
    down: the key was recognised and the value was not, so nothing complained.
    """
    for agent_id, spec in AGENTS.items():
        features = spec.get("agentcore") or {}

        long_term = (features.get("memory") or {}).get("longTerm")
        # A bare truthy means "semantic" (see context._longterm_strategies), so only a
        # list needs checking.
        for strategy in (long_term if isinstance(long_term, list) else []):
            if strategy not in MEMORY_STRATEGIES:
                raise ValueError(
                    f"agent {agent_id!r} asks for long-term memory strategy "
                    f"{strategy!r}; this deployment provisions "
                    f"{', '.join(MEMORY_STRATEGIES)}. An unrecognised strategy is NOT "
                    f"ignored — it is searched as its own namespace, which nothing "
                    f"writes to, so recall returns nothing and reports success. To add "
                    f"one, provision the strategy in both IaC paths and give it a "
                    f"namespace prefix in app/common/context.py `_NS_PREFIX`.")

        evaluators = (features.get("evaluations") or {}).get("evaluators") or []
        for evaluator in evaluators:
            # Deliberately a SHAPE check, not a list of names: AWS adds built-in
            # evaluators and a closed list here would reject a valid new one and force
            # a framework edit. What this catches is the typo — "Builtin.Faithfullness",
            # or a bare "Faithfulness" — which otherwise surfaces as an opaque
            # ValidationException from the Evaluations API on the first run that
            # triggers it, long after the deploy said success.
            if not re.fullmatch(r"(Builtin|Custom)\.[A-Za-z][A-Za-z0-9]*", str(evaluator)):
                raise ValueError(
                    f"agent {agent_id!r} names evaluator {evaluator!r}, which is not of "
                    f"the form \"Builtin.<Name>\" or \"Custom.<Name>\" (e.g. "
                    f"\"Builtin.Faithfulness\"). AgentCore rejects an unknown evaluator "
                    f"at evaluation time, not at deploy time, so a typo here would sit "
                    f"unnoticed until someone ran an evaluation.")
        problem = features_problem(features)
        if problem:
            raise ValueError(f"agent {agent_id!r}: {problem}")


def features_problem(features: dict) -> str:
    """The first thing wrong with an agent's evaluators, custom evaluators or custom
    memory strategy, or "". Mirrored by bff/validate_build.py and validate.ts (which
    report every one, with its path)."""
    ev = features.get("evaluations") or {}
    custom = ev.get("custom") or []
    names = [c.get("name") for c in custom if isinstance(c, dict)]
    # And the build's own evaluators (workflow.json `evaluators`), which any agent may run.
    from app.common.config import WORKFLOW
    shared = list((WORKFLOW.get("evaluators") or {}) if isinstance(WORKFLOW.get("evaluators"), dict) else [])
    unsupported = vocabulary.field("builtinEvaluators", "unsupported") or []
    for evaluator in ev.get("evaluators") or []:
        if evaluator in unsupported:
            level = (vocabulary.field("builtinEvaluators", "levels") or {}).get(evaluator, "")
            return (f"evaluator {evaluator} scores a {level.lower().replace('_', ' ')}, and this "
                    f"framework scores one agent's run, so it is not offered yet")
        if str(evaluator).startswith("Custom.") and str(evaluator)[7:] not in names + shared:
            return f"evaluator {evaluator} is not defined in evaluations.custom or in the build's evaluators"
    for c in custom:
        if not isinstance(c, dict) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9]{0,31}", str(c.get("name") or "")):
            return "every evaluations.custom entry needs a `name`: a letter, then letters and digits (32 at most)"
        if not str(c.get("instructions") or "").strip():
            return f"custom evaluator {c['name']} needs `instructions`: what the judge scores, and how"
    if len(set(names)) != len(names):
        return "two evaluations.custom entries share a name"
    scope = (features.get("memory") or {}).get("scope")
    if scope is not None and scope not in vocabulary.values("memoryScopes"):
        return (f"memory.scope {scope!r} is not one of "
                f"{', '.join(vocabulary.values('memoryScopes'))}")
    mem = (features.get("memory") or {}).get("custom")
    if mem is not None:
        if not isinstance(mem, dict) or mem.get("base") not in vocabulary.values("customMemoryBases"):
            return (f"memory.custom needs `base`: one of "
                    f"{', '.join(vocabulary.values('customMemoryBases'))}")
        if not str(mem.get("instructions") or "").strip():
            return "memory.custom needs `instructions`: what to extract, and what to leave out"
    return ""


def validate_runtimes() -> None:
    """Reject a placement that cannot work, at container start, naming the agent.

    Mirrored by both IaC paths so a customer normally sees these at plan/synth time.
    Every case below is otherwise a LATE failure: a misspelled `runtime` silently
    falls through to "main" and then dies on a missing module; an `a2a` agent with no
    `agentCard` dies on the first request, after the run has already spent money on
    the agents before it.
    """
    for agent_id, spec in AGENTS.items():
        placement = str(spec.get("runtime") or defaults.get("agent", "runtime"))
        if placement not in RUNTIMES:
            raise ValueError(
                f"agent {agent_id!r} has runtime {placement!r}; valid values are "
                f"{', '.join(RUNTIMES)}. A misspelling would otherwise be treated as "
                f"'main' and fail on a missing module under app/subagents/.")
        card = str(spec.get("agentCard") or "")
        auth = str(spec.get("auth") or "")
        source = str(spec.get("source") or "")
        if placement != "a2a":
            # These read as settings, so on any other placement they would be read as
            # working and do nothing at all.
            for key, value in (("agentCard", card), ("auth", auth), ("source", source),
                               ("skill", str(spec.get("skill") or ""))):
                if value:
                    raise ValueError(
                        f"agent {agent_id!r} sets {key!r} but its runtime is {placement!r}. "
                        f"{key} is only read for runtime \"a2a\" — an agent this workflow does "
                        f"not operate.")
            continue
        if bool(card) == bool(source):
            # Exactly one, for the same reason a `tools` entry needs exactly one of
            # lambdaArn/source: an external agent's URL is a stable value you write
            # down, whereas a framework-deployed one is generated by the deploy. Both
            # set is ambiguous; neither leaves the agent with nowhere to call.
            raise ValueError(
                f"agent {agent_id!r} has runtime \"a2a\" and needs EXACTLY ONE of \"agentCard\" "
                f"(an agent that already exists — its base URL, or its card URL) or \"source\" "
                f"(one of {', '.join(A2A_SOURCES)}, the stand-in this repo deploys for you, whose "
                f"URL only exists after a deploy). Got "
                f"{'both' if card and source else 'neither'}.")
        skill = str(spec.get("skill") or "")
        if skill and not source:
            # An external agent's skills are advertised in ITS card; naming one here
            # would look like a selector and change nothing about the request.
            raise ValueError(
                f"agent {agent_id!r} sets \"skill\" but points at an external \"agentCard\". "
                f"`skill` selects among the skills of the stand-in this repo deploys "
                f"(source), and a third party's agent advertises its own in its card.")
        if source == "a2a_lambda" and skill and skill not in A2A_LAMBDA_SKILLS:
            raise ValueError(
                f"agent {agent_id!r} has skill {skill!r}; the stand-in A2A server publishes "
                f"{', '.join(A2A_LAMBDA_SKILLS)} (see a2a_lambda/handler.py SKILLS). An "
                f"unrecognised one would silently fall back to the default reviewer.")
        if source == "a2a_lambda" and auth.lower() != "sigv4":
            # The stand-in sits behind a Function URL with authType AWS_IAM, so an
            # unsigned request is a guaranteed 403 — and it arrives as "could not read
            # the Agent Card ... HTTP 403", which reads like the agent is missing rather
            # than like this config being wrong. Observed on a live run, because `auth`
            # defaults to "none" and this was left unset.
            raise ValueError(
                f"agent {agent_id!r} has source \"a2a_lambda\" and needs auth \"sigv4\" (got "
                f"{auth or 'none'!r}). That stand-in is deployed behind an AWS_IAM Function URL, "
                f"so the request must be SigV4-signed with the orchestrator's own role — there is "
                f"no token, by design.")
        if source and source not in A2A_SOURCES:
            raise ValueError(
                f"agent {agent_id!r} has source {source!r}; the only value is "
                f"{', '.join(A2A_SOURCES)}. A framework-deployed agent needs infrastructure "
                f"config cannot express, so this is not an arbitrary string.")
        if card and not card.lower().startswith("https://"):
            raise ValueError(
                f"agent {agent_id!r} agentCard must be an https:// URL — the request may carry a "
                f"bearer token. Got {card!r}.")
        if auth and auth.lower() not in AUTH_MODES:
            raise ValueError(
                f"agent {agent_id!r} has auth {auth!r}; valid values are {', '.join(AUTH_MODES)}.")
        # `access` is the UI's data-source label for an agent with no tool — but the BFF
        # projection matches `runtime == "a2a"` first and derives "A2A · <host>", so on a
        # remote agent it is read by nothing.
        decorative = [k for k in ("model", "temperature", "topP", "stopSequences", "maxTokens",
                                  "access", "framework", "output", "image", "vision", "attachments",
                                  "toolMode", "maxToolCalls", "skills") if k in spec]
        if decorative:
            # All three configure ctx.llm, and a remote agent never calls it — its
            # model, its temperature, its budget. Left settable, they would read as
            # governing the remote agent's cost and behaviour while doing nothing.
            raise ValueError(
                f"agent {agent_id!r} has runtime \"a2a\" and also {', '.join(decorative)}. Those "
                f"configure this deployment's model call, which a remote agent does not make — it "
                f"chooses its own model and owns its own token budget. Remove them.")
        if spec.get("tool") or spec.get("corpus"):
            # The remote agent owns its own tools. A `tool` here would look like this
            # deployment governs that access through its Gateway and Cedar policy,
            # and it does not.
            raise ValueError(
                f"agent {agent_id!r} has runtime \"a2a\" and also a tool/corpus binding. A remote "
                f"agent reaches its own data sources; binding one here would imply this "
                f"deployment's Gateway and Cedar policy govern those calls, which they cannot.")
        if str(spec.get("auth") or "none").lower() == "oauth2" and not (
                (spec.get("agentcore") or {}).get("identity", {}).get("outbound")):
            raise ValueError(
                f"agent {agent_id!r} has auth \"oauth2\" but no "
                f"agentcore.identity.outbound provider to get a token from. Add one, or use "
                f"auth \"bearer\" with a token injected by the IaC.")


#: What a folder under app/subagents/<id>/ must provide. THE WHOLE AUTHORING CONTRACT
#: for an agent whose code you ship, and it is deliberately three lines long: a package
#: that exports `agent`, an Agent subclass, and a `run()`. Everything else about the
#: folder — how many files, which helpers, whether it drives Strands or a nested graph —
#: is the author's business, because that is where a customer's actual work lives.
AGENT_MODULE_CONTRACT = (
    'app/subagents/<id>/__init__.py must contain `from .agent import agent`; '
    'app/subagents/<id>/agent.py must define a subclass of app.common.base.Agent that '
    'overrides `async def run(self, ctx) -> str` and assign `agent = YourAgent()`.'
)


def check_agent_module(agent_id: str, module) -> Agent:
    """The folder contract, checked on an imported agent package.

    Every failure here used to surface as something else. A missing `agent` export was
    `AttributeError: module 'app.subagents.x' has no attribute 'agent'` — true, and no
    help about what to add. Worse, a class that forgot to override `run` produced NO
    error at import: `Agent.run` raises NotImplementedError, so the agent loaded
    cleanly, configured cleanly, appeared on the diagram, and failed the instant the run
    reached it.

    Called from `build_agent_module`, so it runs wherever an agent's real code is loaded
    — in the orchestrator for a `main` agent, and inside its own container for a
    `dedicated` one. `tests/test_subagents.py` and both IaC planes make the same check
    STATICALLY, which is where a customer should normally meet it: at `pytest`,
    `terraform plan` or `cdk synth`, before anything is deployed.
    """
    where = f"app/subagents/{agent_id}/"
    instance = getattr(module, "agent", None)
    if instance is None:
        raise ValueError(
            f"agent {agent_id!r}: {where}__init__.py does not export `agent`. "
            f"{AGENT_MODULE_CONTRACT}")
    if not isinstance(instance, Agent):
        raise ValueError(
            f"agent {agent_id!r}: {where} exports `agent` as {type(instance).__name__}, "
            f"which is not an app.common.base.Agent. {AGENT_MODULE_CONTRACT}")
    if type(instance).run is Agent.run:
        raise ValueError(
            f"agent {agent_id!r}: {type(instance).__name__} in {where}agent.py does not "
            f"override `run`, so this step would raise NotImplementedError the moment the run "
            f"reached it — after every agent before it had finished and been billed. "
            f"{AGENT_MODULE_CONTRACT}")
    return instance


def build_agent_module(agent_id: str) -> Agent:
    """Import and configure an agent's REAL in-process implementation, ignoring
    its runtime placement. Used by the per-agent runtime (subagent_runtime) to
    run a dedicated agent's actual code inside its own container.

    The agent id IS the subagent package name — one convention, no override. An
    agent key in workflow.json maps to app/subagents/<that key>/, so adding an
    agent is a config entry plus a folder, and nothing has to name it twice."""
    spec = AGENTS[agent_id]
    try:
        module = importlib.import_module(f"app.subagents.{agent_id}")
    except ModuleNotFoundError as e:
        # The bare ModuleNotFoundError names the dotted path and nothing else, which
        # reads like a broken framework rather than a missing folder — and it is the
        # error a misspelled `runtime` produces too, so it needs to say which of the two
        # this is.
        raise ValueError(
            f"agent {agent_id!r} has runtime {spec.get('runtime') or 'main'!r}, so its code "
            f"ships in this repo — but app/subagents/{agent_id}/ could not be imported: {e}. "
            f"The agent id IS the package name. Create the folder with `python3 scaffold.py "
            f"agent {agent_id}`, or set runtime \"a2a\" if the agent is somebody else's "
            f"service.") from e
    return _configure(check_agent_module(agent_id, module), agent_id, spec)


def load_agents() -> dict[str, Agent]:
    """The orchestrator's view: dedicated agents are invoked cross-runtime, and a2a
    agents are delegated to over the network."""
    validate_runtimes()
    validate_tool_types()
    validate_features()
    registry: dict[str, Agent] = {}
    for agent_id, spec in AGENTS.items():
        placement = spec.get("runtime", defaults.get("agent", "runtime"))
        if placement == "dedicated":
            agent: Agent = AgentCoreRuntimeAgent()
            _configure(agent, agent_id, spec)
        elif placement == "a2a":
            agent = A2AAgent()
            _configure(agent, agent_id, spec)
        else:
            agent = build_agent_module(agent_id)
        registry[agent_id] = agent
    return registry
