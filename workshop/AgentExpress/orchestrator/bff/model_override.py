"""Per-run model override, the BFF half (app/common/model_routing.py is the runtime's).

A person granted the `override` action (bff/authz.py: closed unless granted) may start a run
with `modelOverride`: {"all": model} for every agent, {"agents": {agentId: model}} for chosen
ones, or both (an agent's own pick wins). Every model must be in this workflow's
`orchestrator.models.overrides`; nothing else is accepted. The cleaned value is stored on the
run's status row and sent in the start payload, where the runtime checks it again and keeps it
in the run's state for every gate and re-run.

Only this deployment's own workflow: a Builder build started from the console has its own app
(and may run older framework code), so its runs take no override here.
"""

from __future__ import annotations

import re

import workflow

#: The request field, the status-row attribute and the start-payload key.
FIELD = "modelOverride"
#: Placements whose model calls go through ctx.llm (model_routing.LOCAL).
LOCAL = ("main", "dedicated")
AGENT_ID_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,63}$")
SHAPE = 'modelOverride must be {"all": model} and/or {"agents": {agentId: model}}'


class Refused(Exception):
    """A 400, with the reason the caller can act on."""


def choices(raw: dict) -> list[str]:
    """`orchestrator.models.overrides` of `raw` (the full workflow)."""
    block = ((raw or {}).get("orchestrator") or {}).get("models")
    got = block.get("overrides") if isinstance(block, dict) else None
    return [m for m in got if isinstance(m, str) and m.strip()] if isinstance(got, list) else []


def wanted(body: dict) -> bool:
    """Whether the request asks for an override at all (so the `override` check applies)."""
    return isinstance(body, dict) and bool(body.get(FIELD))


def clean(raw: dict, value) -> dict:
    """The override to store and send, or Refused. `value` is what the browser sent."""
    if not isinstance(value, dict) or set(value) - {"all", "agents"}:
        raise Refused(SHAPE)
    allowed = choices(raw)
    if not allowed:
        raise Refused("this workflow offers no model override (orchestrator.models.overrides)")

    def model(m, where: str) -> str:
        if not isinstance(m, str) or m not in allowed:
            shown = m if isinstance(m, str) and len(m) <= 200 else "that model"
            raise Refused(f"{where}: {shown} is not one of this workflow's override models")
        return m

    out: dict = {}
    if value.get("all") not in (None, ""):
        out["all"] = model(value["all"], "all")
    agents = value.get("agents") or {}
    if not isinstance(agents, dict):
        raise Refused(SHAPE)
    known = (raw or {}).get("agents") or {}
    picked: dict = {}
    for aid, m in agents.items():
        if not (isinstance(aid, str) and AGENT_ID_RE.match(aid)):
            raise Refused("agents: an agent id is letters, digits and underscores")
        spec = known.get(aid)
        if not isinstance(spec, dict):
            raise Refused(f"agents.{aid}: no such agent")
        if str(spec.get("runtime") or workflow._DEFAULTS["agent"]["runtime"]) not in LOCAL:
            raise Refused(f"agents.{aid}: a {spec.get('runtime')} agent keeps its own model")
        picked[aid] = model(m, f"agents.{aid}")
    if picked:
        out["agents"] = picked
    if not out:
        raise Refused(SHAPE)
    return out
