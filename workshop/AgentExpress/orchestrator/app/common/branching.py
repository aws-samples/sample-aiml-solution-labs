"""Content-based branching: what an agent produced decides what runs next.

`steps` gives a fixed pipeline, and a HITL gate lets a HUMAN redirect it. This is
the third case: the workflow itself picks the next step from the deciding agent's
own output. A triage agent sends a high-risk case to a deep review and a routine
one straight to approval; an intake agent ends the run early when the request is
not actionable. Declared in workflow.json, so it costs no agent code:

    { "agent": "triage",
      "branch": {
        "when": [
          { "field": "disposition", "equals": "escalate", "goto": "deep_review" },
          { "field": "riskScore",   "gte": 80,            "goto": "deep_review" },
          { "field": "openItems",   "exists": false,      "goto": "END" }
        ],
        "default": "fast_track"
      } }

Rules are tried IN ORDER and the first match wins, so put the most specific first.
No match falls to `default`; with no `default` the run simply continues to the next
step, which means adding a `branch` can only ever redirect a run, never stall it.

A `default` on its own, with no `when`, is an UNCONDITIONAL jump to a later step.
That is what makes two paths genuinely exclusive rather than merely optional: with
steps [triage, investigator, adjuster, settlement], `triage` routes to one of the two
specialists and `investigator` carries `{"default": "settlement"}` so the escalated
path does not then fall into the adjuster's step on its way out.

NOTHING HERE KNOWS YOUR SCHEMA
`field` is a dot path into whatever JSON the agent returned (`scope.inScope`,
`findings.0.claim` — a numeric segment indexes a list). The framework ships no
opinion about which fields exist or what their values mean, because that opinion is
the customer's use case. Omit `field` entirely and the value is the agent's raw
output text, so `{"contains": "URGENT", "goto": "escalate"}` works on an agent that
returns prose.

STRING COMPARISON IS CASE- AND SPACE-INSENSITIVE
The values being compared were written by a model, and a model that was told to
return "escalate" will sometimes return "Escalate" or " escalate". A branch that
silently took the default because of a capital letter would be a very expensive
thing to debug, so `equals`, `notEquals`, `in` and `contains` compare on stripped,
case-folded text. Numeric and existence checks are exact.
"""

from __future__ import annotations

from typing import Any

from app.common import assets

# Comparison operators. Several in one rule means ALL of them must hold.
#   equals / notEquals  normalised equality (see the module docstring)
#   in                  the value is one of a list of alternatives
#   contains            substring of a string value, or membership of a list/dict
#   exists              true: the field is present and not null/""/[]/{}
#   gt / gte / lt / lte numeric comparison; a list or dict compares by its LENGTH,
#                       so {"field": "openQuestions", "gt": 0} reads as "there is
#                       at least one"
_OPS = frozenset({"equals", "notEquals", "in", "contains", "exists",
                  "gt", "gte", "lt", "lte"})
_TEXT_OPS = frozenset({"equals", "notEquals", "in", "contains"})
_NUM_OPS = frozenset({"gt", "gte", "lt", "lte"})
_RULE_KEYS = _OPS | {"field", "goto"}
_SPEC_KEYS = frozenset({"when", "default"})

# A distinct marker for "the path resolved to nothing", so a field that is genuinely
# present and null is not confused with a field that is absent.
_MISSING = object()

#: The target that ends the run instead of continuing to another step.
END_TARGET = "END"


# --- value resolution ------------------------------------------------------

def _dig(data: Any, path: str) -> Any:
    """Follow a dot path into parsed JSON. Returns `_MISSING` if it does not resolve.

    A numeric segment indexes a list (negative indexes work), so an agent whose
    output is shaped as a list of findings is still branchable.
    """
    cur = data
    for seg in (path or "").split("."):
        if isinstance(cur, dict):
            if seg not in cur:
                return _MISSING
            cur = cur[seg]
        elif isinstance(cur, list | tuple):
            try:
                idx = int(seg)
            except ValueError:
                return _MISSING
            if not -len(cur) <= idx < len(cur):
                return _MISSING
            cur = cur[idx]
        else:
            return _MISSING
    return cur


def _norm(v: Any) -> str:
    """Text form for comparison: stripped and case-folded. See the docstring."""
    if isinstance(v, bool):
        return "true" if v else "false"
    return str("" if v is None else v).strip().casefold()


def _magnitude(v: Any) -> float | None:
    """The number a value compares as, or None when it has none.

    A list or dict compares by length — the natural reading of "more than three
    findings" — and a numeric string by its value, because a model asked for a
    score returns "85" about as often as 85.
    """
    if isinstance(v, bool):
        return None  # a flag is not a quantity; use `equals` or `exists`
    if isinstance(v, int | float):
        return float(v)
    if isinstance(v, list | tuple | dict | set):
        return float(len(v))
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return None


def _is_present(v: Any) -> bool:
    """What `exists` means: there, and carrying something."""
    if v is _MISSING or v is None:
        return False
    if isinstance(v, str):
        return bool(v.strip())
    if isinstance(v, list | tuple | dict | set):
        return bool(v)
    return True


def _contains(value: Any, needle: Any) -> bool:
    n = _norm(needle)
    if isinstance(value, dict):
        return any(_norm(k) == n for k in value)
    if isinstance(value, list | tuple | set):
        return any(_norm(x) == n or (isinstance(x, str) and n in _norm(x)) for x in value)
    return n in _norm(value)


def _holds(op: str, value: Any, expected: Any) -> bool:
    """One operator against one resolved value."""
    if op == "exists":
        return _is_present(value) is bool(expected)
    if value is _MISSING:
        return False  # every other operator needs a value to compare
    if op == "equals":
        return _norm(value) == _norm(expected)
    if op == "notEquals":
        return _norm(value) != _norm(expected)
    if op == "in":
        return any(_norm(value) == _norm(x) for x in (expected or []))
    if op == "contains":
        return _contains(value, expected)
    got = _magnitude(value)
    want = _magnitude(expected)
    if got is None or want is None:
        return False
    return {"gt": got > want, "gte": got >= want,
            "lt": got < want, "lte": got <= want}[op]


# --- rule evaluation -------------------------------------------------------

def _describe(rule: dict) -> str:
    """The rule, in words, for the run's timeline. A branch that took an unexpected
    path is only debuggable if the reason is recorded next to it."""
    field = rule.get("field") or "output"
    parts = [f"{field} {op} {rule[op]!r}" for op in _OPS if op in rule]
    return " and ".join(parts) or field


def _matches(rule: dict, data: dict, raw: str) -> bool:
    """All of the rule's operators hold. `field` omitted compares the raw output."""
    value = _dig(data, rule["field"]) if rule.get("field") else raw
    return all(_holds(op, value, rule[op]) for op in _OPS if op in rule)


def choose(spec: dict, output: str) -> tuple[str | None, str]:
    """(target, reason) for one branch spec against the deciding agent's output.

    `target` is a step name, `"END"`, or None meaning "no rule matched and there is
    no default — continue to the next step". `reason` is for the timeline.

    The output is parsed with the same salvaging parser every agent's own asset goes
    through (`assets.extract_json`), so a payload the model truncated at its token
    budget can still be branched on.
    """
    data = assets.extract_json(output or "") or {}
    raw = output or ""
    for rule in spec.get("when") or []:
        if _matches(rule, data, raw):
            return str(rule["goto"]), _describe(rule)
    default = spec.get("default")
    if default:
        return str(default), "no rule matched, took `default`"
    return None, "no rule matched and no `default`; continuing to the next step"


# --- validation ------------------------------------------------------------
#
# Shape only. Whether a target names a real, LATER step is topology, so it is
# checked in app/orchestrator/graph_builder.py (and mirrored by both IaC paths at
# plan/synth time, which is where a customer sees it first).


def validate_spec(spec: Any, where: str) -> None:
    """Raise ValueError naming `where` if the branch spec is malformed.

    Every check here is a mistake that would otherwise be SILENT: a misspelled
    operator, or a rule with no operator at all, evaluates to "no match" and the run
    quietly takes the default forever.
    """
    if not isinstance(spec, dict):
        raise ValueError(f"{where}: `branch` must be an object with `when` (and optionally `default`).")
    unknown = sorted(set(spec) - _SPEC_KEYS)
    if unknown:
        raise ValueError(f"{where}: `branch` has unknown key(s) {unknown}. Valid keys: "
                         f"{sorted(_SPEC_KEYS)}.")
    when = spec.get("when")
    if when is None and not str(spec.get("default") or "").strip():
        raise ValueError(f"{where}: `branch` needs `when` (conditional rules), `default` (an "
                         f"unconditional jump to a later step), or both.")
    if when is not None and (not isinstance(when, list) or not when):
        raise ValueError(f"{where}: `branch.when` must be a non-empty list of rules. Omit it "
                         f"entirely for an unconditional jump via `default` alone.")
    for i, rule in enumerate(when or []):
        at = f"{where} branch.when[{i}]"
        if not isinstance(rule, dict):
            raise ValueError(f"{at}: each rule must be an object.")
        bad = sorted(set(rule) - _RULE_KEYS)
        if bad:
            raise ValueError(f"{at}: unknown key(s) {bad}. A rule is `goto`, an optional `field`, "
                             f"and one or more of {sorted(_OPS)}.")
        if not str(rule.get("goto") or "").strip():
            raise ValueError(f"{at}: needs a `goto` naming the step to run when it matches "
                             f'(or "{END_TARGET}").')
        ops = sorted(set(rule) & _OPS)
        if not ops:
            raise ValueError(f"{at}: has no comparison. Add one or more of {sorted(_OPS)} — a rule "
                             f"with none would never match, so the branch would always take "
                             f"`default`.")
        if "in" in rule and not isinstance(rule["in"], list | tuple):
            raise ValueError(f"{at}: `in` takes a list of alternatives.")
        if "exists" in rule and not isinstance(rule["exists"], bool):
            raise ValueError(f"{at}: `exists` takes true or false.")
        for op in _NUM_OPS & set(rule):
            if _magnitude(rule[op]) is None:
                raise ValueError(f"{at}: `{op}` takes a number, got {rule[op]!r}.")
        for op in _TEXT_OPS & set(rule) - {"in"}:
            if isinstance(rule[op], list | dict):
                raise ValueError(f"{at}: `{op}` takes a single value, not {type(rule[op]).__name__}. "
                                 f"Use `in` for a list of alternatives.")
    default = spec.get("default")
    if default is not None and not str(default).strip():
        raise ValueError(f"{where}: `branch.default` is empty. Omit it to continue to the next step.")


def targets(spec: dict) -> list[str]:
    """Every step this branch can route to, in declaration order, de-duplicated."""
    out = [str(r.get("goto")) for r in (spec.get("when") or []) if r.get("goto")]
    if spec.get("default"):
        out.append(str(spec["default"]))
    return list(dict.fromkeys(out))
