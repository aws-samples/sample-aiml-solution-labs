"""Checks for which model answers (app/common/model_routing.py), for validate_build.py:
`orchestrator.models.overrides` (the per-run override list) and an agent's
`models.fallback` / `models.route`. The generic key checks (app/keys.json) already say where
`models` applies and that each part is a list; these are the rest.

Mirrors web/src/builder/modelRoutingCheck.ts word for word (both run
tests/fixtures/validation_cases.json). A `matches` pattern is compiled here as Python reads
it and there as JavaScript does: the two agree on ordinary patterns, not on every
engine-specific construct (inline flags such as (?i) are Python only; the match is already
case-insensitive).
"""

from __future__ import annotations

import re

#: Bounds (app/common/model_routing.py reads whatever passes these).
MAX_OVERRIDES = 20
MAX_FALLBACK = 3
MAX_RULES = 10
MAX_TOKENS_OVER = 1_000_000
MAX_PATTERN = 200
RULE_KEYS = ("model", "inputTokensOver", "matches")
LOCAL = ("main", "dedicated")


def _model_problem(m) -> str:
    if not isinstance(m, str) or not m.strip():
        return "must be a model id"
    if m.startswith("litellm/") and not m[8:].strip():
        return "names no model: write litellm/ and the proxy's model name, e.g. litellm/gpt-4o"
    return ""


def _models(items: list, path: str, where: dict, add, own: str = "") -> None:
    seen: list = []
    for i, m in enumerate(items):
        p = f"{path}[{i}]"
        problem = _model_problem(m)
        if problem:
            add("error", where, p, problem)
        elif m in seen:
            add("error", where, p, "is listed twice")
        elif own and m == own:
            add("error", where, p, "is this agent's own model")
        seen.append(m)


def _int(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _rule(rule, p: str, where: dict, add) -> None:
    if not isinstance(rule, dict):
        add("error", where, p, 'must be {"model": ..., "inputTokensOver": N} or {"model": ..., "matches": "..."}')
        return
    for k in rule:
        if k not in RULE_KEYS:
            add("error", where, f"{p}.{k}", f"unknown key; a rule takes: {', '.join(RULE_KEYS)}")
    if "model" not in rule:
        add("error", where, f"{p}.model", "needs the model this rule picks")
    else:
        problem = _model_problem(rule["model"])
        if problem:
            add("error", where, f"{p}.model", problem)
    if ("inputTokensOver" in rule) == ("matches" in rule):
        add("error", where, p, "set exactly one of `inputTokensOver` / `matches`")
    if "inputTokensOver" in rule:
        v = rule["inputTokensOver"]
        if not (_int(v) and 1 <= v <= MAX_TOKENS_OVER):
            add("error", where, f"{p}.inputTokensOver", f"must be a whole number from 1 to {MAX_TOKENS_OVER}")
    if "matches" in rule:
        v = rule["matches"]
        if not (isinstance(v, str) and 1 <= len(v) <= MAX_PATTERN):
            add("error", where, f"{p}.matches", f"must be a regular expression of 1 to {MAX_PATTERN} characters")
        else:
            try:
                re.compile(v, re.IGNORECASE)
            except re.error:
                add("error", where, f"{p}.matches", "is not a valid regular expression")


def check(orch, agents: dict, auth, add) -> None:
    """Add the issues of the override list and of every agent's `models`."""
    block = orch.get("models") if isinstance(orch, dict) else None
    overrides = block.get("overrides") if isinstance(block, dict) else None
    if isinstance(overrides, list):
        where = {"kind": "block", "name": "orchestrator"}
        path = "orchestrator.models.overrides"
        if len(overrides) > MAX_OVERRIDES:
            add("error", where, path, f"at most {MAX_OVERRIDES} models")
        _models(overrides, path, where, add)
        actions = auth.get("actions") if isinstance(auth, dict) and isinstance(auth.get("actions"), dict) else {}
        granted = actions.get("override")
        if overrides and not (isinstance(granted, list) and granted):
            add("warning", where, path, "only a person granted `override` can pick a run's models, and no group "
                                        "is granted it in authorization.actions, so nobody can")
    default = str(orch.get("defaultModel") or "") if isinstance(orch, dict) else ""
    for aid, a in agents.items():
        # No runtime is a local placement (keys.json agent.runtime default).
        if not isinstance(a, dict) or (a.get("runtime") and str(a["runtime"]).lower() not in LOCAL):
            continue
        models = a.get("models")
        if not isinstance(models, dict):
            continue
        where = {"kind": "agent", "id": aid}
        path = f"agents.{aid}.models"
        own = a.get("model") if isinstance(a.get("model"), str) and a["model"] else default
        fallback = models.get("fallback")
        if isinstance(fallback, list):
            if len(fallback) > MAX_FALLBACK:
                add("error", where, f"{path}.fallback",
                    f"at most {MAX_FALLBACK} models: each is tried in turn while the step waits")
            _models(fallback, f"{path}.fallback", where, add, own)
        rules = models.get("route")
        if isinstance(rules, list):
            if len(rules) > MAX_RULES:
                add("error", where, f"{path}.route", f"at most {MAX_RULES} rules")
            for i, rule in enumerate(rules):
                _rule(rule, f"{path}.route[{i}]", where, add)
