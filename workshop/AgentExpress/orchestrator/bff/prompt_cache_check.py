"""Checks for an agent's `promptCache` (Bedrock prompt caching, app/common/prompt_cache.py)
against what its model supports, per the AWS table copied into app/vocabulary.json
`promptCacheModels` (https://docs.aws.amazon.com/bedrock/latest/userguide/prompt-caching.html).
The generic key checks (keys.json) cover the shape; these are the rest. Mirrors
web/src/builder/promptCacheCheck.ts word for word (both run
tests/fixtures/validation_cases.json).

  * `ttl` other than "5m" / "1h" / "30m": an error.
  * a `ttl` the model's row does not list ("1h" on a 5m-only model, "30m" on a Converse
    model, "5m"/"1h" on an OpenAI GPT-5.6+ model, whose only TTL is 30m), `tools` or
    `messages` on a model whose row has no such field: errors (the model cannot do what
    the setting says).
  * a model with no explicit caching through Converse: a warning (calls are sent
    without cache points; the run is not affected).
"""

from __future__ import annotations

LOCAL = ("main", "dedicated")


def base_id(model: str, geos: list[str]) -> str:
    """A model id, inference-profile id or ARN, without its geography prefix."""
    mid = str(model or "").strip()
    if "/" in mid:
        mid = mid.rsplit("/", 1)[-1]
    head, _, rest = mid.partition(".")
    return rest if rest and head.lower() in geos else mid


def check(orch, agents: dict, add, vocab_block, default_model: str) -> None:
    """`vocab_block(name)` returns a vocabulary.json block; `default_model` is the
    framework default (keys.json orchestrator.defaultModel)."""
    table = vocab_block("promptCacheModels") or {}
    rows = table.get("byModel") or {}
    not_converse = (table.get("notOnConverse") or {}).get("byModel") or []
    geos = list((vocab_block("inferenceProfileGeos") or {}).get("values") or [])
    own_default = str(orch.get("defaultModel") or "") if isinstance(orch, dict) else ""
    for aid, a in agents.items():
        if not isinstance(a, dict) or (a.get("runtime") and str(a["runtime"]).lower() not in LOCAL):
            continue
        pc = a.get("promptCache")
        if not isinstance(pc, dict):
            continue
        where = {"kind": "agent", "id": aid}
        path = f"agents.{aid}.promptCache"
        ttl = pc.get("ttl")
        if "ttl" in pc and ttl not in ("5m", "1h", "30m"):
            add("error", where, f"{path}.ttl", 'must be "5m", "1h" or "30m"')
        if not any(pc.get(k) is True for k in ("system", "tools", "messages")):
            continue
        model = a.get("model") if isinstance(a.get("model"), str) and a["model"] else (
            own_default or default_model)
        base = base_id(model, geos)
        row = None if model.startswith("litellm/") else rows.get(base)
        if row is None:
            if base in not_converse:
                add("warning", where, path,
                    f"{model}: explicit caching for this model needs the Responses API or Chat "
                    f"Completions, and AgentExpress calls models through Converse; calls to it "
                    f"are sent without cache points")
            else:
                add("warning", where, path,
                    f"{model} has no documented explicit prompt caching through Converse (AWS "
                    f"table copied {table.get('copied', '')}); calls to it are sent without "
                    f"cache points")
            continue
        name, fields, ttls = row.get("name"), row.get("fields") or [], row.get("ttls") or []
        openai = row.get("api") == "InvokeModel"
        for flag in ("tools", "messages"):
            if pc.get(flag) is True and flag not in fields:
                add("error", where, f"{path}.{flag}",
                    f"{name} caches the system prompt only: tool-calling turns go through "
                    f"Converse, which takes no explicit cache point for this model" if openai
                    else f"{name} caches the system prompt and messages only, not tool definitions")
        if ttl in ("5m", "1h", "30m") and ttl not in ttls:
            if openai:
                msg = f'{name} caches for 30 minutes; use "30m" or leave ttl out'
            elif ttl == "1h":
                msg = f'{name} caches for 5 minutes only; use "5m"'
            else:
                msg = f'{name} has no 30-minute TTL; use ' + " or ".join(f'"{t}"' for t in ttls)
            add("error", where, f"{path}.ttl", msg)
