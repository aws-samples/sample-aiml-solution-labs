"""HarnessAgent — a step run by a managed AgentCore Harness (workflow.json `runtime: "harness"`).

A harness is an agent with no code of yours: AgentCore runs the model loop, the session and
its sandbox from configuration
(https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/harness.html). The IaC builds
one per harness agent from its workflow.json entry (terraform/harness.tf, cdk/lib/harness.ts):
the system prompt is `instructions`, the model is `model` (else the deployment default) with
`temperature`, `topP` and `maxTokens` on the provider `harness.provider` names (Bedrock by
default; OpenAI, Gemini and LiteLLM with an API key credential provider), and the rest comes
from the `harness` block: tools, skills, memory, container, mounts, network and an endpoint.
Memory is switched OFF on the harness unless `harness.memory` names one of the build's
`memories`, because CreateHarness provisions a managed memory (billed) when the field is
omitted (harness-memory.html); this framework's own long-term memory (`agentcore.memory`), if
the agent declares it instead, is recalled and stored by the node wrapper as for any agent.
The two are not combined: both would store the same turns.

`harness.arn` brings a harness made elsewhere: the deploy creates nothing for the agent, and
this class invokes that harness (at `harness.endpoint` when set) with its own configuration.

This class invokes it with InvokeHarness for the step, the same inputs an A2A agent is given
(the request, the earlier outputs, reviewer feedback, recalled insights) as one user message,
and reads the event stream: the last assistant message's text is the step's output, given
the framework's asset envelope like an A2A reply (A2AAgent._as_asset).

Tools: the built-in `shell` and `file_operations` tools are on in every session unless
`allowedTools` restricts them (harness-tools.html), and the session can reach the internet,
so a harness agent gets exactly the tools its entry names and nothing else:
  * `tool`: the build's Gateway tools, through one agentcore_gateway tool named "gateway"
    whose outbound OAuth2 credential provider holds the agent's Gateway machine client (its
    own with orchestrator.gatewayIdentity "perAgent"), so Cedar policy and interceptors apply
    as for any agent; only the named tools ("@gateway/<tool>___*") are allowed;
  * `harness.mcp`: remote MCP servers by URL, `harness.browser` and `harness.codeInterpreter`:
    the managed AgentCore Browser and Code Interpreter (or a custom one by ARN);
  * `harness.shell`: the built-in shell and file tools.
`allowedTools` is set on the harness AND sent on every call, so a harness edited in the
console still gets only what the workflow says. A harness you bring (`harness.arn`) keeps
its own tools: no allowedTools is sent.

Guardrails, telemetry and evaluations are applied around run() by the node wrapper
(app/orchestrator/nodes.py), as for every placement. The model call is metered from the
stream's `metadata` events.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time

from app.common.a2a_agent import A2AAgent
from app.common.base import Agent

#: agent id -> harness ARN, injected by the IaC (HARNESS_ARNS), as AGENT_RUNTIME_ARNS is.
_ARNS: dict = json.loads(os.getenv("HARNESS_ARNS", "{}") or "{}")
_REGION = os.getenv("AWS_REGION", "us-east-1")

#: allowedTools for an agent with no tools: an MCP server name this harness does not have,
#: so the pattern matches nothing (patterns: harness-tools.html). Omitting allowedTools would
#: allow every tool, the built-in shell included. Same literal in harness.tf and harness.ts.
NO_TOOLS = "@no_tools"
#: allowedTools with `harness.shell`: the built-in shell and file_operations tools.
BUILTIN_TOOLS = "@builtin"
#: The harness tool names this framework gives the Gateway, the browser and the code
#: interpreter (HarnessTool.name). The Gateway is an MCP server, so its tools are allowed as
#: "@gateway/<tool>___*" (the Gateway names each "<target>___<tool>"). harness-tools.html does
#: not say how a browser or code interpreter tool is matched, by its name or as a server, so
#: both are allowed; a pattern that matches nothing allows nothing. Same in harness.tf/.ts.
GATEWAY_TOOL = "gateway"
BROWSER_TOOL = "browser"
CODE_TOOL = "code_interpreter"
#: The skills tool (Strands' AgentSkills, which powers the harness, registers "skills"):
#: allowed when the harness has skills, so they can be opened. Not named in the docs.
SKILLS_TOOLS = ("skills", "@builtin/skills")

#: The keys of the `harness` block, and their bounds. AgentCore publishes no bounds for
#: maxIterations or timeoutSeconds (botocore: plain integers); these are the framework's, so
#: that one step cannot loop or hold the run for long. 900 s is AgentCore Runtime's limit on
#: a synchronous request, which InvokeHarness is.
LIMITS = {"maxIterations": (1, 100), "timeoutSeconds": (1, 900)}
HARNESS_KEYS = ("maxIterations", "timeoutSeconds", "shell", "provider", "apiKey", "apiBase", "browser",
                "codeInterpreter", "mcp", "skills", "memory", "container", "mounts", "network", "endpoint", "arn")
#: What a harness you bring (`harness.arn`) may still set: where to call it, and per-call limits.
BYO_KEYS = ("arn", "endpoint", "maxIterations", "timeoutSeconds")
#: Agent keys a harness you bring does not take: they are set on that harness.
BYO_NOT = ("instructions", "model", "temperature", "topP", "maxTokens", "tool")
PROVIDERS = ("bedrock", "openai", "gemini", "litellm")
#: Keys of a local agent that a harness does not read: its code, what it reads, how it
#: reasons over its tools. Its Gateway tools are `tool` (the harness runs the tool loop).
NOT_FOR_HARNESS = ("framework", "output", "image", "vision", "attachments", "stopSequences",
                   "corpus", "access", "toolMode", "maxToolCalls", "skills", "askPerson")
#: CreateHarness `harnessName`: what the IaC names it, "<agentName>_<agent id>".
HARNESS_NAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,39}$")
#: InvokeHarness harnessArn and qualifier (botocore HarnessArn, HarnessEndpointName).
HARNESS_ARN_RE = re.compile(r"^arn:aws[a-z-]*:bedrock-agentcore:[a-z0-9-]+:[0-9]{12}:harness/[a-zA-Z][a-zA-Z0-9_]{0,39}-[a-zA-Z0-9]{10}$")
ENDPOINT_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,47}$")

#: How often the stream reader checks whether the user stopped the run (a DynamoDB read).
_CANCEL_CHECK_SECONDS = 5.0
#: Stop reasons of a final message that are a finished answer (InvokeHarness messageStop).
_DONE = ("end_turn",)
#: What a stop reason means, for the error a reviewer reads.
_STOPPED = {
    "max_iterations_exceeded": "it reached harness.maxIterations",
    "timeout_exceeded": "it reached harness.timeoutSeconds",
    "max_tokens": "the model reached maxTokens in one turn",
    "max_output_tokens_exceeded": "it used up its output budget",
    "tool_use": "it asked for a tool this deployment does not run",
    "content_filtered": "the model's content filter stopped it",
    "hook_stopped": "a lifecycle hook stopped it",
}
_STREAM_ERRORS = ("internalServerException", "validationException", "runtimeClientError")

_clients: dict = {}


class HarnessFailed(RuntimeError):
    """The harness did not produce an answer: no harness, an error event, or it stopped early."""


def gateway_tools(spec: dict) -> list[str]:
    """The build's tools a harness agent may call through the Gateway (its `tool`)."""
    raw = (spec or {}).get("tool")
    return [str(t) for t in (raw if isinstance(raw, list) else [raw]) if t]


def allowed_tools(spec: dict) -> list[str] | None:
    """The `allowedTools` for this agent: exactly the tools its entry names, in a fixed order
    (shell, Gateway tools, MCP servers, browser, code interpreter, skills), else none. None
    for a harness you bring, which keeps its own. Same list in harness.tf and harness.ts."""
    block = (spec or {}).get("harness") or {}
    if "arn" in block:
        return None
    out: list[str] = [BUILTIN_TOOLS] if block.get("shell") is True else []
    for key in gateway_tools(spec):
        # The app matches a target's tools case-blind (gateway/client.py), so both spellings.
        out += list(dict.fromkeys([f"@{GATEWAY_TOOL}/{key}___*", f"@{GATEWAY_TOOL}/{key.lower()}___*"]))
    out += [f"@{m.get('name')}" for m in block.get("mcp") or [] if isinstance(m, dict)]
    if block.get("browser"):
        out += [BROWSER_TOOL, f"@{BROWSER_TOOL}"]
    if block.get("codeInterpreter"):
        out += [CODE_TOOL, f"@{CODE_TOOL}"]
    if block.get("skills"):
        out += list(SKILLS_TOOLS)
    return out or [NO_TOOLS]


def problem(agent_id: str, spec: dict) -> str:
    """The first thing that stops this harness agent from working, or "". The container's own
    check of what it reads; bff/harness_check.py and harnessCheck.ts check everything else
    (sources, ARNs, URLs) before a deploy, and the IaC refuses the rest at plan/synth."""
    spec = spec or {}
    if str(spec.get("runtime") or "") != "harness":
        stray = [k for k in ("instructions", "harness") if k in spec]
        return (f"agent {agent_id!r} sets {stray[0]!r} but its runtime is "
                f"{spec.get('runtime') or 'main'!r}; it is read only for runtime \"harness\"") if stray else ""
    block = spec.get("harness")
    if block is not None and not isinstance(block, dict):
        return f"agent {agent_id!r}: `harness` must be an object"
    block = block or {}
    for key in block:
        if key not in HARNESS_KEYS:
            return f"agent {agent_id!r}: harness.{key} is not a setting; the block takes {', '.join(HARNESS_KEYS)}"
    if "arn" in block:
        if not HARNESS_ARN_RE.match(str(block["arn"])):
            return (f"agent {agent_id!r}: harness.arn must be a harness ARN, "
                    f"arn:aws:bedrock-agentcore:<region>:<account>:harness/<name>-<id>")
        set_here = [k for k in BYO_NOT if k in spec] + [f"harness.{k}" for k in block if k not in BYO_KEYS]
        if set_here:
            return (f"agent {agent_id!r} uses the harness at harness.arn, which has its own "
                    f"{', '.join(set_here)}. Remove {'them' if len(set_here) > 1 else 'it'}.")
    elif not isinstance(spec.get("instructions"), str) or not spec["instructions"].strip():
        return (f"agent {agent_id!r} has runtime \"harness\" and needs `instructions`: the system "
                f"prompt its AgentCore Harness is created with")
    extra = [k for k in NOT_FOR_HARNESS if k in spec]
    if extra:
        return (f"agent {agent_id!r} has runtime \"harness\" and also {', '.join(extra)}. A harness "
                f"agent has no code of this build, so nothing would read "
                f"{'them' if len(extra) > 1 else 'it'}. Remove {'them' if len(extra) > 1 else 'it'}.")
    if "shell" in block and not isinstance(block["shell"], bool):
        return f"agent {agent_id!r}: harness.shell must be true or false"
    for key, (low, high) in LIMITS.items():
        value = block.get(key)
        if value is not None and (isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high):
            return f"agent {agent_id!r}: harness.{key} must be a whole number from {low} to {high}"
    if str(block.get("provider") or "bedrock") not in PROVIDERS:
        return f"agent {agent_id!r}: harness.provider must be one of {', '.join(PROVIDERS)}"
    if block.get("endpoint") is not None and (not ENDPOINT_RE.match(str(block["endpoint"])) or block["endpoint"] == "DEFAULT"):
        return (f"agent {agent_id!r}: harness.endpoint must be 1-48 letters, digits and underscores, "
                f"starting with a letter, and not DEFAULT")
    for i, m in enumerate(block.get("mcp") or []):
        if not isinstance(m, dict) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", str(m.get("name") or "")):
            return f"agent {agent_id!r}: harness.mcp[{i}] needs a `name` of letters, digits, _ and -"
    if block.get("memory") and ((spec.get("agentcore") or {}).get("memory")):
        return (f"agent {agent_id!r}: harness.memory and agentcore.memory would both store this "
                f"agent's turns. Keep one.")
    return ""


def validate(agents: dict) -> None:
    """Refuse, at container start, a harness agent that cannot work."""
    for agent_id, spec in (agents or {}).items():
        why = problem(agent_id, spec or {})
        if why:
            raise ValueError(why)


def _client(read_timeout: float):
    """bedrock-agentcore with retries OFF, for the reason AgentCoreRuntimeAgent gives: a
    retried InvokeHarness runs the whole agent loop again and bills it twice."""
    key = int(read_timeout)
    if key not in _clients:
        import boto3
        from botocore.config import Config

        from app.common.config import RUNTIME_INVOKE
        _clients[key] = boto3.client("bedrock-agentcore", region_name=_REGION, config=Config(
            retries={"total_max_attempts": int(RUNTIME_INVOKE["maxAttempts"]), "mode": "standard"},
            read_timeout=read_timeout))
    return _clients[key]


def session_id(run_session: str, agent_id: str) -> str:
    """runtimeSessionId: 33-100 characters of [a-zA-Z0-9-_], starting with a letter or digit.
    One per (run, agent), so a re-run after a reviewer's revise continues the same session and
    sees its earlier answer."""
    raw = re.sub(r"[^a-zA-Z0-9_-]", "-", f"{run_session}-{agent_id}")
    raw = raw if raw[:1].isalnum() else f"s{raw}"
    return raw.ljust(33, "0")[:100]


def read_stream(stream, cancelled=None) -> dict:
    """Read an InvokeHarness event stream to the end.

    Returns {text, stop, usage: {inputTokens, outputTokens}, latencyMs, tools: [names]}: `text`
    is the last assistant message that said anything (earlier ones narrate tool calls), `stop`
    the final messageStop's stopReason. Raises HarnessFailed on an error event. `cancelled` is
    called every few seconds; when it returns True the stream is closed and "cancelled" is the
    stop reason.
    """
    messages: list[list[str]] = []
    role, stop = "", ""
    usage = {"inputTokens": 0, "outputTokens": 0, "cacheReadInputTokens": 0, "cacheWriteInputTokens": 0}
    latency, tools = 0, []
    checked = time.monotonic()
    try:
        for event in stream:
            for name in _STREAM_ERRORS:
                if name in event:
                    raise HarnessFailed(f"{name}: {(event[name] or {}).get('message', '')}")
            if "messageStart" in event:
                role = event["messageStart"].get("role", "")
                if role == "assistant":
                    messages.append([])
            elif "contentBlockStart" in event:
                use = (event["contentBlockStart"].get("start") or {}).get("toolUse")
                if use and use.get("name"):
                    tools.append(str(use["name"]))
            elif "contentBlockDelta" in event:
                text = (event["contentBlockDelta"].get("delta") or {}).get("text")
                if text and role == "assistant" and messages:
                    messages[-1].append(text)
            elif "messageStop" in event:
                stop = event["messageStop"].get("stopReason", "")
            elif "metadata" in event:
                got = event["metadata"].get("usage") or {}
                usage["inputTokens"] += int(got.get("inputTokens") or 0)
                usage["outputTokens"] += int(got.get("outputTokens") or 0)
                # HarnessTokenUsage also carries the prompt-cache counts (optional):
                # https://docs.aws.amazon.com/bedrock-agentcore/latest/APIReference/API_HarnessTokenUsage.html
                usage["cacheReadInputTokens"] += int(got.get("cacheReadInputTokens") or 0)
                usage["cacheWriteInputTokens"] += int(got.get("cacheWriteInputTokens") or 0)
                latency += int((event["metadata"].get("metrics") or {}).get("latencyMs") or 0)
            if cancelled and time.monotonic() - checked > _CANCEL_CHECK_SECONDS:
                checked = time.monotonic()
                if cancelled():
                    stop = "cancelled"
                    break
    finally:
        close = getattr(stream, "close", None)
        if callable(close):
            close()
    said = [m for m in ("".join(parts).strip() for parts in messages) if m]
    return {"text": said[-1] if said else "", "stop": stop, "usage": usage,
            "latencyMs": latency, "tools": tools}


class HarnessAgent(Agent):
    """Runs this step on its AgentCore Harness. Configured by the registry, plus use(spec)."""

    # Recalled insights travel in the message, with the caveat, as for an A2A agent; the
    # step's output is stored as an insight by the node wrapper as for any agent.
    recall_in_orchestrator = True
    store_in_orchestrator = True
    instructions: str = ""
    limits: dict = None
    tools_allowed: list = None

    # The same task, recollections and envelope an A2A agent is given and gives back.
    _recalled = A2AAgent._recalled
    _upstream_asset_ids = A2AAgent._upstream_asset_ids
    _as_asset = A2AAgent._as_asset

    def use(self, spec: dict) -> "HarnessAgent":
        """Take the harness-only keys (called by the registry after _configure)."""
        self.instructions = str((spec or {}).get("instructions") or "")
        self.limits = dict((spec or {}).get("harness") or {})
        self.tools_allowed = allowed_tools(spec)
        return self

    def _actor(self, ctx) -> str:
        """actorId for a harness with memory (or one you bring): the same partition the
        framework's own memory uses (AgentContext._memory_actor), with the scope of the
        `memories` entry harness.memory names (default per user), so one user's turns never
        reach another's run."""
        import hashlib

        from app.common.config import WORKFLOW
        from app.common.context import _slug
        from app.common import defaults
        scope = str(((WORKFLOW.get("memories") or {}).get(self.limits.get("memory")) or {}).get("scope")
                    or defaults.get("memory", "scope"))
        user = str(getattr(ctx, "user", "") or "").strip().lower()
        who = "-u" + hashlib.sha256(user.encode()).hexdigest()[:10] if user else ""
        subject = _slug(str(getattr(ctx, "subject_id", "") or ""))[:48]
        if scope == "run":
            return f"{self.id}{who}-r{_slug(str(ctx.session_id or ''))[:32]}"
        if scope == "agent" or (scope == "subject" and subject):
            return f"{self.id}-{subject}" if subject else self.id
        return f"{self.id}{who}-{subject}" if subject else f"{self.id}{who}"

    def _message(self, ctx) -> str:
        from app.common.config import AGENTS

        upstream = {k: v for k, v in ((ctx.state or {}).get("outputs") or {}).items() if k != self.id}
        produces = str((AGENTS.get(self.id) or {}).get("produces") or "").strip()
        task = {"request": ctx.topic, "producing": produces or None, "upstreamOutputs": upstream,
                "reviewerFeedback": (getattr(ctx, "feedback", "") or "") or None,
                "recalledContext": self._recalled(ctx)}
        return json.dumps({k: v for k, v in task.items() if v is not None}, ensure_ascii=False, default=str)

    async def run(self, ctx) -> str:
        brought = str(self.limits.get("arn") or "")
        arn = brought or _ARNS.get(self.id)
        if not arn:
            raise HarnessFailed(
                f"agent '{self.id}' has runtime \"harness\" but no harness was deployed for it "
                f"(HARNESS_ARNS has {sorted(_ARNS) or 'none'}). Deploy the workflow again.")
        from app.common.config import RUNTIME_INVOKE, model_for
        from app.common.sink import WorkflowCancelled, is_cancelled
        from app.features.observability import meter

        message = self._message(ctx)
        request = {"harnessArn": arn, "runtimeSessionId": session_id(str(ctx.session_id or ""), self.id),
                   "messages": [{"role": "user", "content": [{"text": message}]}]}
        # A harness you bring keeps its own tools; one built here gets exactly its entry's.
        if not brought:
            request["allowedTools"] = list(self.tools_allowed or [NO_TOOLS])
        else:
            request.update({k: self.limits[k] for k in ("maxIterations", "timeoutSeconds") if k in self.limits})
        if self.limits.get("endpoint"):
            request["qualifier"] = str(self.limits["endpoint"])
        if brought or self.limits.get("memory"):
            request["actorId"] = self._actor(ctx)
        # No traceParent here: ADOT's botocore instrumentation writes the traceparent header
        # itself, after the request is signed, so setting it too made the signed value differ
        # from the sent one and every call failed with "The request signature we calculated
        # does not match". The harness still joins the run's trace through ADOT's header.
        # Long enough for the harness's own limit to be the one that ends a slow step: it
        # says why ("timeout_exceeded"), a client that gives up first does not.
        read_timeout = max(float(RUNTIME_INVOKE["readTimeoutSeconds"]),
                           float(self.limits.get("timeoutSeconds") or 0) + 60)
        await ctx.log(f"Invoking AgentCore Harness for '{self.id}'"
                      + (f" at endpoint {request['qualifier']}" if "qualifier" in request else "")
                      + (" (shell and files allowed)" if BUILTIN_TOOLS in (self.tools_allowed or []) else ""))
        started = time.monotonic()
        sid = str(ctx.session_id or "")

        def call() -> dict:
            resp = _client(read_timeout).invoke_harness(**request)
            return read_stream(resp.get("stream") or [], lambda: is_cancelled(sid))

        try:
            got = await asyncio.to_thread(call)
        except HarnessFailed:
            raise
        except Exception as e:  # noqa: BLE001 - the step fails with the service's own words
            raise HarnessFailed(f"InvokeHarness for '{self.id}' failed: {type(e).__name__}: {e}") from e
        provider = str(self.limits.get("provider") or "bedrock")
        # Bedrock is priced as for any agent. Another provider's model, or a harness you
        # bring (whose model this deployment does not know), is recorded with its tokens and
        # rates_known false: no price table here covers it.
        priced = not brought and provider == "bedrock"
        model = (model_for(self.model) if priced
                 else f"harness:{arn.rsplit('/', 1)[-1]}" if brought else str(self.model or ""))
        meter.record_llm(
            model=model, input_tokens=got["usage"]["inputTokens"], output_tokens=got["usage"]["outputTokens"],
            system_text=self.instructions, latency_ms=got["latencyMs"] or int((time.monotonic() - started) * 1000),
            mode="harness", user_input=message, output_text=got["text"], temperature=self.temperature,
            max_tokens=self.max_tokens, finish_reason=got["stop"],
            # The harness's cache tokens are recorded, not priced: HarnessTokenUsage
            # (https://docs.aws.amazon.com/bedrock-agentcore/latest/APIReference/API_HarnessTokenUsage.html)
            # does not say whether inputTokens includes them, so pricing them separately could
            # count them twice. The row says cache cost unknown. The harness API has no
            # prompt caching setting (CreateHarness), so promptCache does not apply here.
            cache={"read": int(got["usage"].get("cacheReadInputTokens") or 0),
                   "write": int(got["usage"].get("cacheWriteInputTokens") or 0),
                   "write5m": 0, "write1h": 0}, cache_priced=False,
            **({} if priced else {"cost_usd": 0, "rates_source": "none"}))
        for tool in got["tools"]:
            await ctx.log(f"{self.name}: used {tool}")
        if got["stop"] == "cancelled":
            raise WorkflowCancelled(sid)
        if got["stop"] not in _DONE:
            why = _STOPPED.get(got["stop"], f"it stopped with {got['stop'] or 'no stop reason'}")
            raise HarnessFailed(f"the harness for '{self.id}' gave no finished answer: {why}")
        if not got["text"]:
            # An empty answer is not an answer (same reason as A2AAgent).
            raise HarnessFailed(f"the harness for '{self.id}' finished but returned no text")
        return self._as_asset(ctx, got["text"])


agent = HarnessAgent()
