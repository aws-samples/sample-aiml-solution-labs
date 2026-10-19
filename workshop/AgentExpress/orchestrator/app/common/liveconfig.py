"""Live config: an agent's prompt and model settings, changed on a running deployment.

DESIGN NOTE (written before the code; bff/liveconfig.py is the other half)

What can change live
  Per agent, and only these six things: the system prompt (`systemPrompt`) and the model
  settings `model`, `temperature`, `topP`, `maxTokens`, `stopSequences`. Each is read
  through the AgentContext when the agent runs, so a change needs no new image and no new
  infrastructure. Everything else is out on purpose: topology, tools, gates, guardrails,
  memory, Cedar, identity and anything else the IaC provisions from workflow.json. Those
  need IAM, Gateway or Policy changes to take effect, so a "live" change there would say
  it worked and do nothing. A model change is held to the models the deployment's IAM
  already lets the runtimes call (bff/liveconfig.py `model_allowed`).

  A `runtime: "a2a"` agent takes no change: its model and prompt are the remote agent's
  own (registry.validate_runtimes refuses the same settings in workflow.json).

Where versions are stored
  In the telemetry table, under reserved keys, written ONLY by the BFF:
      session_id = _liveconfig#versions   sk = v#<8-digit n>   one immutable version
      session_id = _liveconfig#pointer    sk = active          {"pointer": {"active": n}}
  Version 0 is "what was deployed" and has no row. Each version records who, when, a note
  and its full overlay ({agent id: {field: value}}). The pointer is a structure so that a
  later A/B or canary can carry {"active": n, "candidate": m, "share": 0.1}; nothing reads
  more than `active` today.

How a run picks one
  The BFF reads the pointer (cached a few seconds) and that version's row ONCE, when it
  writes the run's status row (`configVersion` on the run's snapshot). It sends the overlay
  to the runtime in the start payload, and the runtime puts it in the graph state
  (`liveconfig`, see app/common/state.py). The runtime itself reads nothing: it holds no new
  grant, and a run never sees a version it did not start with.

What happens on resume
  Nothing new is read. The overlay is part of the checkpointed graph state, so a gate
  resume or a re-run, in a later invocation and on another container, uses the version the
  run started with even after the pointer has moved. A resume payload is never consulted for
  it. A dedicated agent gets its own entry of the overlay in the InvokeAgentRuntime payload
  (app/common/agentcore_agent.py), so it runs the same version as the orchestrator.

Failure
  If the pointer or the version cannot be read the run starts on version 0 and the BFF
  logs `[liveconfig] using deployed config: <why>`. A run must not fail because the table
  is unreadable.

Off (the default)
  Without `orchestrator.liveConfig: true` the BFF reads nothing, the runtime ignores any
  `liveconfig` in a payload, the BFF's IAM statement does not exist and the UI shows nothing.

NOT covered
  * The deployed prompt (version 0) lives in the image (app/subagents/<id>/prompts.py), which
    the BFF cannot read, so the editor cannot show it; a version either replaces the prompt
    in full or leaves it alone.
  * A prompt is replaced where the agent passes its own `system_prompt` to ctx.llm (every
    shipped and scaffolded agent does). An agent that builds its system text some other way
    keeps its own, and the timeline says the live prompt was not applied.
  * A run of a Builder build started from the console (the build's own app has its own).
  * A/B and canary splits: the pointer leaves room for them, nothing implements them.
"""

from __future__ import annotations

from app.common import config

#: The only settings a live version may carry. Mirrored by FIELDS in bff/liveconfig.py.
FIELDS = ("systemPrompt", "model", "temperature", "topP", "maxTokens", "stopSequences")
#: The graph-state key (and payload key) a run's version travels under.
STATE_KEY = "liveconfig"


def enabled() -> bool:
    """`orchestrator.liveConfig: true` in this deployment's workflow.json."""
    return config.ORCHESTRATOR.get("liveConfig") is True


def _clean(overlay) -> dict:
    """One agent's overlay, keeping only FIELDS. Anything else is dropped here as well as
    refused by the BFF, so no payload can change a setting outside the six."""
    if not isinstance(overlay, dict):
        return {}
    return {k: overlay[k] for k in FIELDS if k in overlay}


def from_payload(payload: dict) -> dict | None:
    """The run's version from a START payload, for the graph state; None for version 0 or
    with the feature off (a payload carrying one is then ignored)."""
    if not enabled():
        return None
    raw = (payload or {}).get(STATE_KEY)
    if not isinstance(raw, dict):
        return None
    version = raw.get("version")
    if not isinstance(version, int) or isinstance(version, bool) or version <= 0:
        return None
    agents = {aid: _clean(o) for aid, o in (raw.get("agents") or {}).items()
              if aid in config.AGENTS and _clean(o)}
    return {"version": version, "agents": agents}


def for_agent(state: dict, agent_id: str) -> dict | None:
    """What a dedicated agent's runtime is sent: the run's version and that agent's own
    entry only. None when the run is on version 0."""
    live = (state or {}).get(STATE_KEY)
    if not isinstance(live, dict) or not live.get("version"):
        return None
    mine = (live.get("agents") or {}).get(agent_id)
    return {"version": live["version"], "agents": {agent_id: mine} if mine else {}}


def apply(ctx, agent, state: dict) -> None:
    """Put the run's version of this agent's settings on its context.

    On the context, never the Agent: one Agent instance serves every run in the container,
    and two runs on different versions can be in it at once."""
    ctx.config_version = 0
    ctx.live_prompt = None
    live = (state or {}).get(STATE_KEY)
    if not isinstance(live, dict) or not live.get("version"):
        return
    ctx.config_version = int(live["version"])
    over = _clean((live.get("agents") or {}).get(agent.id))
    if "model" in over:
        ctx.model = over["model"]
    if "temperature" in over:
        ctx.temperature = over["temperature"]
    if "topP" in over:
        ctx.top_p = over["topP"]
    if "maxTokens" in over:
        ctx.max_tokens = int(over["maxTokens"])
    if "stopSequences" in over:
        ctx.stop_sequences = [str(s) for s in over["stopSequences"] or []]
    if "systemPrompt" in over:
        ctx.live_prompt = str(over["systemPrompt"])
        ctx.deployed_prompt = str(getattr(agent, "system_prompt", "") or "")
        ctx.agent_prompt = ctx.live_prompt


def prompt(ctx, system: str) -> tuple[str, bool]:
    """(the system text to send, whether a live prompt was wanted and could not be placed).

    The live prompt replaces the agent's deployed one where the agent passed it in, which is
    how every shipped agent calls ctx.llm. Where it is not there, the agent's own text is
    kept: guessing where a prompt goes inside text the agent built would be worse."""
    live = getattr(ctx, "live_prompt", None)
    if live is None:
        return system, False
    deployed = getattr(ctx, "deployed_prompt", "")
    if deployed and deployed in system:
        return system.replace(deployed, live, 1), False
    return system, True
