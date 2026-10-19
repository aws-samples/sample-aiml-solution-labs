"""Which model answers a call: the run's override, an agent's route rules, its fallbacks.

DESIGN NOTE (bff/model_override.py is the BFF half)

Per-run override (`orchestrator.models.overrides`)
  A person granted the `override` action picks, on Start run, one model for every agent
  ({"all": m}), or models for chosen agents ({"agents": {id: m}}), or both (an agent's own
  pick wins over "all"). Only a model in `overrides` is accepted, by the BFF and again here.
  The BFF stores it on the run's status row (`modelOverride`) and sends it in the start
  payload; the runtime puts it in the graph state (`modelOverride`, app/common/state.py),
  which is checkpointed, so every gate resume and re-run of that run keeps it, on any
  container. A dedicated agent is sent its own model only (app/common/agentcore_agent.py).
  An overridden agent uses that model ONLY: no route, no fallback. The person asked for that
  model, and a run that quietly answered with another would say something it did not do.

Route (`agents.<id>.models.route`)
  Rules tried in order, first match wins, else the agent's model:
      {"model": m, "inputTokensOver": N}   the call's prompt, estimated at characters / 4
                                           (system + user text), is over N tokens
      {"model": m, "matches": "regex"}     the run's request or its subject matches,
                                           case-insensitive (re.search)
  Pure computation: no extra call is made to decide.

Fallback (`agents.<id>.models.fallback`)
  Tried in order when the model of the call fails with one of FALLBACK_CODES, the Bedrock
  Runtime errors that say "busy or not ready, try later or elsewhere" (Converse API
  reference, Errors): ThrottlingException (429), ServiceUnavailableException (503),
  ModelNotReadyException (429). From a LiteLLM proxy, HTTP 429 or 503. Never on anything
  else: a ValidationException, AccessDeniedException, ResourceNotFoundException,
  ModelErrorException, ModelTimeoutException or InternalServerException fails the step as
  before, and so does a guardrail block (GuardrailBlocked is raised outside the model call).
  botocore's own retry (llm._client_config) runs first, inside each try.

Telemetry
  Each call's row (meter.record_llm) carries the model that was called in `label` and
  `chosen_by`: "override", "route N", "fallback N" or "" (the agent's own model). A failed
  try is its own error row. The timeline says when a run uses a model other than the
  agent's and when a fallback is tried.

Off (the default)
  No override list, no `models` block: candidates() returns the agent's model alone and
  call() is one plain call.
"""

from __future__ import annotations

import contextvars
import re

from app.common import config, defaults
from app.common.errors import ModelUnavailable

#: The graph-state key (and payload key) a run's override travels under.
STATE_KEY = "modelOverride"
#: Bedrock Runtime error codes that move a call on to the next fallback model.
FALLBACK_CODES = ("ThrottlingException", "ServiceUnavailableException", "ModelNotReadyException")
#: The same, from a LiteLLM proxy (app/common/litellm_proxy.ProxyError.status).
FALLBACK_STATUS = (429, 503)
#: Characters per token, for the input-size estimate a route rule compares with.
CHARS_PER_TOKEN = 4
#: Placements whose model calls go through ctx.llm. A harness agent's model is set on its
#: AgentCore Harness; a remote agent and a task step have none of ours.
LOCAL = ("main", "dedicated")

#: Why the model of the call in flight was chosen, for its telemetry row.
_chosen_by: contextvars.ContextVar[str] = contextvars.ContextVar("model_chosen_by", default="")


def chosen_by() -> str:
    """For meter.record_llm: "override", "route N", "fallback N" or ""."""
    return _chosen_by.get()


def choices() -> list[str]:
    """`orchestrator.models.overrides` in this deployment's workflow.json."""
    block = config.ORCHESTRATOR.get("models")
    got = block.get("overrides") if isinstance(block, dict) else None
    return [m for m in got if isinstance(m, str) and m.strip()] if isinstance(got, list) else []


def _local(agent_id: str) -> bool:
    spec = config.AGENTS.get(agent_id)
    return isinstance(spec, dict) and str(spec.get("runtime") or defaults.get("agent", "runtime")) in LOCAL


def from_payload(payload: dict) -> dict | None:
    """The run's override from a START payload, for the graph state. Held to this
    deployment's list again (the BFF already was); None without a list or a valid pick."""
    allowed = choices()
    raw = (payload or {}).get(STATE_KEY)
    if not allowed or not isinstance(raw, dict):
        return None
    out: dict = {}
    if raw.get("all") in allowed:
        out["all"] = raw["all"]
    agents = raw.get("agents") if isinstance(raw.get("agents"), dict) else {}
    mine = {aid: m for aid, m in agents.items() if _local(aid) and m in allowed}
    if mine:
        out["agents"] = mine
    return out or None


def override_for(state: dict, agent_id: str) -> str | None:
    """The model the run's override gives this agent, if any."""
    over = (state or {}).get(STATE_KEY)
    if not isinstance(over, dict) or not _local(agent_id):
        return None
    return ((over.get("agents") or {}).get(agent_id) or over.get("all")) or None


def for_agent(state: dict, agent_id: str) -> dict | None:
    """What a dedicated agent's runtime is sent: its own model only, or None."""
    model = override_for(state, agent_id)
    return {"agents": {agent_id: model}} if model else None


def _models_block(agent_id: str) -> dict:
    spec = config.AGENTS.get(agent_id)
    block = spec.get("models") if isinstance(spec, dict) else None
    return block if isinstance(block, dict) else {}


def estimate_tokens(*texts: str) -> int:
    return sum(len(t or "") for t in texts) // CHARS_PER_TOKEN


def route(rules, *, tokens: int, request: str, subject: str) -> tuple[int, dict] | None:
    """(1-based rule number, rule) of the first rule that matches, else None."""
    for n, rule in enumerate(rules if isinstance(rules, list) else [], start=1):
        if not isinstance(rule, dict) or not isinstance(rule.get("model"), str) or not rule["model"]:
            continue
        over = rule.get("inputTokensOver")
        if isinstance(over, int) and not isinstance(over, bool) and tokens > over:
            return n, rule
        pattern = rule.get("matches")
        if isinstance(pattern, str) and pattern:
            try:
                rx = re.compile(pattern, re.IGNORECASE)
            except re.error:
                continue                    # the validators refuse it; never fail a run on it
            if rx.search(request or "") or rx.search(subject or ""):
                return n, rule
    return None


def _why(n: int, rule: dict, tokens: int) -> str:
    if "inputTokensOver" in rule:
        return f"route {n}: about {tokens:,} tokens in, over {rule['inputTokensOver']:,}"
    return f"route {n}: the request matches /{rule['matches']}/"


def candidates(ctx, system: str, user: str, explicit: str | None = None) -> list[tuple[str, str, str]]:
    """[(model, chosen_by, why)], in the order to try. The first is the call's model; the
    rest are its fallbacks. `model` None is the deployment default, as everywhere.
    `explicit` is a model the agent's code passed to ctx.llm itself: it stands in for the
    agent's model and its route; the override still wins."""
    over = override_for(getattr(ctx, "state", None) or {}, ctx.agent_id)
    if over:
        return [(over, "override", "the run's model override")]
    block = _models_block(ctx.agent_id)
    first: tuple[str | None, str, str] = (explicit or getattr(ctx, "model", None), "", "")
    if block.get("route") and not explicit:
        tokens = estimate_tokens(system, user)
        hit = route(block["route"], tokens=tokens, request=getattr(ctx, "topic", ""),
                    subject=getattr(ctx, "subject_id", ""))
        if hit:
            first = (hit[1]["model"], f"route {hit[0]}", _why(hit[0], hit[1], tokens))
    out = [first]
    seen = {config.model_for(first[0])}
    fallback = block.get("fallback") if isinstance(block.get("fallback"), list) else []
    for n, m in enumerate((m for m in fallback if isinstance(m, str) and m), start=1):
        if config.model_for(m) not in seen:
            seen.add(config.model_for(m))
            out.append((m, f"fallback {n}", f"fallback {n}"))
    return out


def failover_code(error: BaseException) -> str | None:
    """The error code that lets a call move on to a fallback model, else None. Walks the
    cause chain: ModelUnavailable wraps botocore's ClientError (langchain-aws re-raises it
    as it is) or the LiteLLM ProxyError."""
    seen = 0
    e: BaseException | None = error
    while e is not None and seen < 10:
        response = getattr(e, "response", None)
        if isinstance(response, dict):
            code = str((response.get("Error") or {}).get("Code") or "")
            return code if code in FALLBACK_CODES else None
        status = getattr(e, "status", None)
        if type(e).__name__ == "ProxyError" and isinstance(status, int):
            return f"HTTP {status}" if status in FALLBACK_STATUS else None
        e = e.__cause__ or e.__context__
        seen += 1
    return None


async def call(ctx, chain: list[tuple[str, str, str]], attempt):
    """`await attempt(model)` for the first model of `chain`, moving on to the next only when
    the failure is one of FALLBACK_CODES. The last failure is raised as it was."""
    for i, (model, tag, why) in enumerate(chain):
        if why and why != tag:
            await _note(ctx, model, why)
        token = _chosen_by.set(tag)
        try:
            return await attempt(model)
        except ModelUnavailable as e:
            code = failover_code(e)
            if code is None or i + 1 == len(chain):
                raise
            nxt = chain[i + 1][0]
            await ctx.log(f"{config.model_for(model)} is busy or not ready ({code}); trying "
                          f"{config.model_for(nxt)} (fallback {i + 1} of {len(chain) - 1})")
        finally:
            _chosen_by.reset(token)
    raise AssertionError("unreachable: the chain is never empty")


async def _note(ctx, model: str, why: str) -> None:
    """Say once per agent run which model a call uses when it is not the agent's own."""
    said = getattr(ctx, "_model_notes", None)
    if said is None:
        said = set()
        ctx._model_notes = said
    if (model, why) in said:
        return
    said.add((model, why))
    await ctx.log(f"Using {config.model_for(model)} ({why})")
