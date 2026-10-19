"""Test suites for a build: saved requests with expectations, run again after every change.

Quality used to be checked by hand: someone started a run and read it. A SUITE is a list of
requests (CASES), each with CHECKS on what the run should produce, kept with the build so it
is versioned, exported and imported with it. Run it after a change to a prompt or a model and
the answer to "better or worse?" is a pass rate and the list of what broke.

    {"name": "Triage",
     "cases": [{"name": "urgent claim goes to review",
                "request": "Claim 88213: water damage, claimant is hospitalised",
                "subject": "acme",                                     # optional
                "expect": [{"run": "done"},
                           {"agent": "triage", "field": "disposition", "equals": "escalate"},
                           {"agent": "triage", "field": "riskScore", "gte": 80},
                           {"agent": "summary", "notContains": "TODO"},
                           {"agent": "deep_review", "status": "done"},
                           {"cost": {"lt": 0.25}},
                           {"latencyMs": {"lt": 120000}}]}]}

EVERY CHECK IS DETERMINISTIC
A check compares a value in a finished run with a value written down in the suite. Nothing
here calls a model, so the same run always gets the same verdict. An LLM judge scoring
quality on a scale is a different feature (the Evals tab, AgentCore Evaluations) and is not
part of this one.

THE COMPARISONS ARE THE BRANCH RULES'
`field`, `equals`, `notEquals`, `in`, `contains`, `exists`, `gt`, `gte`, `lt`, `lte` mean
exactly what they mean in a `branch` (app/common/branching.py): a dot path into the agent's
JSON output, strings compared stripped and case-folded, a list or dict compared by its length.
The BFF ships as its own zip and cannot import `app.*`, so the functions are COPIED, and
tests/test_suites.py compares them with the originals as syntax trees: the two cannot drift
apart without a test failing. `notContains` is the only addition: the opposite of `contains`.

A RUN MUST FINISH `done`, UNLESS THE CASE SAYS OTHERWISE
A case with no `{"run": ...}` check gets `{"run": "done"}` added. Without that, a run that
failed at its second step would pass a case that only looks at the first agent's output, and
a suite that stays green while the build is broken is worse than none. A case that EXPECTS a
refusal says so: `{"run": "denied"}`.

WHAT EACH CHECK READS
  run                  the run's overall status: done, failed, denied or cancelled
  agent + status       one agent's status in the run: done, failed, skipped, denied, cancelled
  agent + field + op   a value in that agent's JSON output (a dot path)
  agent + op           the agent's output as text (`contains`, `notContains`, `equals`, ...)
  cost                 what the run's model and tool calls cost in USD (telemetry)
  latencyMs            the SUM of the time of every model and tool call in the run (telemetry),
                       not the time on the clock: parallel agents add up
The result of each check is {passed, expected, actual, message}; the message says what was
found, in plain words.
"""

from __future__ import annotations

import json
import re
from typing import Any

#: A suite is stored in the build's draft, whose size is capped (builds.MAX_PROJECT_BYTES), so
#: these keep one suite, and what a run of it reads and records, far below that.
MAX_SUITES = 20
MAX_CASES = 50
MAX_CHECKS = 20
MAX_NAME = 80
#: bff/handler.py TOPIC_MAX: a request is what the Start run box accepts (a test pins them).
MAX_REQUEST = 10000
MAX_SUBJECT = 200
#: An agent id, a field path, or the text a check looks for.
MAX_TEXT = 2000
MAX_SUITE_BYTES = 200_000

#: How a run can end, and how an agent in it can.
RUN_OUTCOMES = ("done", "failed", "denied", "cancelled")
AGENT_OUTCOMES = ("done", "failed", "skipped", "denied", "cancelled")
#: The overall statuses after which a run changes no more.
TERMINAL = frozenset(RUN_OUTCOMES)


# --- the branch rules' comparisons, copied --------------------------------------------------
#
# Everything from here to "suite validation" is a copy of app/common/branching.py (the
# operators) or app/common/assets.py (extract_json and its helpers). tests/test_suites.py
# compares each one with its original, so edit the original and copy the change here.

_OPS = frozenset({"equals", "notEquals", "in", "contains", "exists",
                  "gt", "gte", "lt", "lte"})
_TEXT_OPS = frozenset({"equals", "notEquals", "in", "contains"})
_NUM_OPS = frozenset({"gt", "gte", "lt", "lte"})

# A distinct marker for "the path resolved to nothing", so a field that is genuinely
# present and null is not confused with a field that is absent.
_MISSING = object()


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


def extract_json(text: str) -> dict | None:
    """Pull the JSON object out of a model response, repairing truncation.

    Handles a fenced ```json block, leading prose, and — the case that matters —
    output cut off mid-object because the agent's `maxTokens` ran out. Rebalancing
    salvages a usable partial asset instead of failing the whole agent.
    """
    t = (text or "").strip()
    if t.startswith("```"):
        t = t.split("\n", 1)[1] if "\n" in t else t
        if t.rstrip().endswith("```"):
            t = t.rstrip()[:-3]
    start = t.find("{")
    if start == -1:
        return None
    t = t[start:]
    # Fast path: the whole object parses.
    end = t.rfind("}")
    if end != -1:
        try:
            obj = json.loads(t[: end + 1])
            if isinstance(obj, dict):
                return obj
        except (ValueError, TypeError):
            pass
    # Several complete objects: the LAST is the answer. Observed live, an agent wrote out
    # a tool call ({"text", "keywords"}) before its real asset, and taking the first
    # object made the call the agent's output.
    objs = _complete_objects(t)
    if len(objs) > 1:
        return objs[-1]
    return _repair_json(t)


def _complete_objects(t: str) -> list[dict]:
    """Every top-level {...} in `t` that parses as an object, in order."""
    out: list[dict] = []
    dec = json.JSONDecoder()
    i = 0
    while True:
        i = t.find("{", i)
        if i == -1:
            return out
        try:
            obj, end = dec.raw_decode(t, i)
        except ValueError:
            i += 1
            continue
        if isinstance(obj, dict):
            out.append(obj)
        i = end


def _balance_close(s: str) -> dict | None:
    """Close any unclosed strings/brackets in a JSON prefix and parse it."""
    stack: list[str] = []
    in_str = False
    esc = False
    for ch in s:
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch in "{[":
            stack.append(ch)
        elif (ch == "}" and stack and stack[-1] == "{") or (ch == "]" and stack and stack[-1] == "["):
            stack.pop()
    fixed = s
    if in_str:
        fixed += '"'
    fixed = re.sub(r",\s*$", "", fixed.rstrip())
    fixed += "".join("}" if o == "{" else "]" for o in reversed(stack))
    try:
        obj = json.loads(fixed)
    except (ValueError, TypeError):
        return None
    return obj if isinstance(obj, dict) else None


def _repair_json(t: str) -> dict | None:
    lines = t.splitlines()
    # Try the full prefix, then progressively drop trailing (truncated) lines
    # until a rebalanced close parses.
    for cut in range(len(lines), 0, -1):
        obj = _balance_close("\n".join(lines[:cut]))
        if obj is not None:
            return obj
    return None


# --- suite validation -----------------------------------------------------------------------

#: What a check on an agent's output may use: the branch rules' operators, and the negation
#: of `contains`.
OPS = _OPS | {"notContains"}
_TEXT_LIKE = _TEXT_OPS | {"notContains"}
#: Cost and latency are compared as numbers only.
_LIMIT_OPS = _NUM_OPS
_CHECK_KINDS = ("run", "cost", "latencyMs", "agent")
_SUITE_KEYS = frozenset({"name", "cases"})
_CASE_KEYS = frozenset({"name", "request", "subject", "expect"})
_AGENT_KEYS = frozenset({"agent", "field", "status"}) | OPS


def _issue(severity: str, suite: str, path: str, message: str) -> dict:
    """The shape every validator here uses: {severity, where, path, message}."""
    return {"severity": severity, "where": {"kind": "suite", "id": suite}, "path": path,
            "message": message}


def _text(v: Any, limit: int) -> bool:
    return isinstance(v, str) and len(v.strip()) > 0 and len(v) <= limit


def _check_issues(check: Any, at: str, suite: str, agents: set[str] | None) -> list[dict]:
    def err(path: str, msg: str) -> dict:
        return _issue("error", suite, path, msg)

    if not isinstance(check, dict):
        return [err(at, "a check is an object, like {\"run\": \"done\"}")]
    kinds = [k for k in _CHECK_KINDS if k in check]
    if len(kinds) != 1:
        return [err(at, f"a check names exactly one of {', '.join(_CHECK_KINDS)}"
                        + (f", not {len(kinds)}" if kinds else ", and this names none of them"))]
    kind = kinds[0]
    out: list[dict] = []
    if kind == "run":
        extra = sorted(set(check) - {"run"})
        if extra:
            out.append(err(at, f"a run check has only `run`; remove {extra}"))
        if check["run"] not in RUN_OUTCOMES:
            out.append(err(f"{at}.run", f"must be one of: {', '.join(RUN_OUTCOMES)}"))
        return out
    if kind in ("cost", "latencyMs"):
        extra = sorted(set(check) - {kind})
        if extra:
            out.append(err(at, f"a {kind} check has only `{kind}`; remove {extra}"))
        spec = check[kind]
        if not isinstance(spec, dict) or not spec:
            out.append(err(f"{at}.{kind}", f"takes a comparison, like {{\"lt\": 1}}; one or more of "
                                           f"{sorted(_LIMIT_OPS)}"))
            return out
        for op, want in spec.items():
            if op not in _LIMIT_OPS:
                out.append(err(f"{at}.{kind}.{op}", f"unknown comparison; use one of {sorted(_LIMIT_OPS)}"))
            elif isinstance(want, bool) or not isinstance(want, int | float) or want < 0 or want != want:
                out.append(err(f"{at}.{kind}.{op}", f"takes a number, 0 or more, got {want!r}"))
        return out
    # An agent's status or output.
    if not _text(check["agent"], MAX_TEXT):
        out.append(err(f"{at}.agent", "must be the id of an agent"))
    elif agents is not None and check["agent"] not in agents:
        out.append(_issue("warning", suite, f"{at}.agent",
                          f"{check['agent']!r} is not an agent in this build's workflow, so this "
                          f"check fails until it is"))
    bad = sorted(set(check) - _AGENT_KEYS)
    if bad:
        out.append(err(at, f"unknown key(s) {bad}. An agent check is `agent`, then `status`, or an "
                           f"optional `field` and one or more of {sorted(OPS)}"))
    if "status" in check:
        if sorted(set(check) - {"agent", "status"}):
            out.append(err(at, "a status check has only `agent` and `status`; "
                               "put a comparison of the output in a check of its own"))
        if check["status"] not in AGENT_OUTCOMES:
            out.append(err(f"{at}.status", f"must be one of: {', '.join(AGENT_OUTCOMES)}"))
        return out
    ops = sorted(set(check) & OPS)
    if not ops:
        out.append(err(at, f"has no comparison. Add `status`, or one or more of {sorted(OPS)}"))
        return out
    if "field" in check and not _text(check["field"], MAX_TEXT):
        out.append(err(f"{at}.field", "must be a dot path into the agent's JSON output, like scope.inScope"))
    if "in" in check and not isinstance(check["in"], list | tuple):
        out.append(err(f"{at}.in", "takes a list of alternatives"))
    if "exists" in check and not isinstance(check["exists"], bool):
        out.append(err(f"{at}.exists", "takes true or false"))
    out.extend(err(f"{at}.{op}", f"takes a number, got {check[op]!r}")
               for op in sorted(_NUM_OPS & set(check)) if _magnitude(check[op]) is None)
    for op in sorted((_TEXT_LIKE & set(check)) - {"in"}):
        if isinstance(check[op], list | dict):
            out.append(err(f"{at}.{op}", f"takes a single value, not a {type(check[op]).__name__}. "
                                         f"Use `in` for a list of alternatives"))
        elif len(str(check[op])) > MAX_TEXT:
            out.append(err(f"{at}.{op}", f"is longer than {MAX_TEXT} characters"))
    return out


def validate_suite(suite: Any, prefix: str = "", agents: set[str] | None = None) -> list[dict]:
    """Every problem with one suite, as {severity, where, path, message}. `prefix` is put in
    front of each path (`suites.0.`), and `agents` (the workflow's agent ids), when given,
    makes a check on an agent that is not there a warning rather than a surprise at run time."""
    if not isinstance(suite, dict):
        return [_issue("error", "", prefix.rstrip(".") or "suite", "a suite is an object: {name, cases}")]
    sid = str(suite.get("name") or "").strip()

    def err(path: str, msg: str) -> dict:
        return _issue("error", sid, prefix + path, msg)

    out: list[dict] = []
    if not _text(suite.get("name"), MAX_NAME):
        out.append(err("name", f"a suite has a name of 1 to {MAX_NAME} characters"))
    bad = sorted(set(suite) - _SUITE_KEYS)
    if bad:
        out.append(err("", f"unknown key(s) {bad}. A suite is `name` and `cases`"))
    cases = suite.get("cases")
    if not isinstance(cases, list):
        out.append(err("cases", "must be a list of cases"))
        return out
    if not cases:
        out.append(_issue("warning", sid, prefix + "cases", "has no cases yet, so there is nothing to run"))
    if len(cases) > MAX_CASES:
        out.append(err("cases", f"a suite has at most {MAX_CASES} cases, this has {len(cases)}"))
    seen: set[str] = set()
    for i, case in enumerate(cases[:MAX_CASES]):
        at = f"cases.{i}"
        if not isinstance(case, dict):
            out.append(err(at, "a case is an object: {name, request, expect}"))
            continue
        bad = sorted(set(case) - _CASE_KEYS)
        if bad:
            out.append(err(at, f"unknown key(s) {bad}. A case is `name`, `request`, optional `subject`, and `expect`"))
        name = case.get("name")
        if not _text(name, MAX_NAME):
            out.append(err(f"{at}.name", f"a case has a name of 1 to {MAX_NAME} characters"))
        elif name.strip() in seen:
            out.append(err(f"{at}.name", f"another case is already called {name.strip()!r}"))
        else:
            seen.add(name.strip())
        request = case.get("request")
        if not isinstance(request, str) or not request.strip():
            out.append(err(f"{at}.request", "is empty: a case needs the request to run"))
        elif len(request) > MAX_REQUEST:
            out.append(err(f"{at}.request", f"a request is at most {MAX_REQUEST} characters"))
        subject = case.get("subject")
        if subject is not None and (not isinstance(subject, str) or len(subject) > MAX_SUBJECT):
            out.append(err(f"{at}.subject", f"must be text of at most {MAX_SUBJECT} characters"))
        expect = case.get("expect", [])
        if not isinstance(expect, list):
            out.append(err(f"{at}.expect", "must be a list of checks"))
            continue
        if len(expect) > MAX_CHECKS:
            out.append(err(f"{at}.expect", f"a case has at most {MAX_CHECKS} checks, this has {len(expect)}"))
        for j, check in enumerate(expect[:MAX_CHECKS]):
            out.extend(_check_issues(check, f"{prefix}{at}.expect.{j}", sid, agents))
    if not any(i["severity"] == "error" for i in out):
        try:
            size = len(json.dumps(suite, ensure_ascii=False).encode())
        except (TypeError, ValueError):
            size = MAX_SUITE_BYTES + 1
        if size > MAX_SUITE_BYTES:
            out.append(err("", f"a suite is at most {MAX_SUITE_BYTES // 1000} KB, this is {size // 1000} KB"))
    return out


def validate_suites(suites: Any, agents: set[str] | None = None) -> list[dict]:
    """Every problem with a build's list of suites. Names are unique: a suite is found by name."""
    if suites is None:
        return []
    if not isinstance(suites, list):
        return [_issue("error", "", "suites", "suites is a list of suites")]
    out: list[dict] = []
    if len(suites) > MAX_SUITES:
        out.append(_issue("error", "", "suites", f"a build has at most {MAX_SUITES} suites, this has {len(suites)}"))
    seen: set[str] = set()
    for i, suite in enumerate(suites[:MAX_SUITES]):
        out.extend(validate_suite(suite, f"suites.{i}.", agents))
        name = str(suite.get("name") or "").strip() if isinstance(suite, dict) else ""
        if name and name in seen:
            out.append(_issue("error", name, f"suites.{i}.name", f"another suite is already called {name!r}"))
        seen.add(name)
    return out


def errors(issues: list[dict]) -> list[dict]:
    return [i for i in issues if i["severity"] == "error"]


# --- evaluating one finished run ------------------------------------------------------------

def _clip(text: str, n: int = 120) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= n else text[: n - 1] + "…"


def _show(v: Any) -> str:
    """A value as the message prints it."""
    if v is _MISSING:
        return "(missing)"
    if isinstance(v, str):
        return _clip(json.dumps(v, ensure_ascii=False))
    try:
        return _clip(json.dumps(v, ensure_ascii=False, default=str))
    except (TypeError, ValueError):
        return _clip(repr(v))


def _say(op: str, expected: Any) -> str:
    """A comparison in words: `gte 80`, `contains "URGENT"`."""
    return f"{op} {_show(expected)}"


def _result(passed: bool, expected: str, actual: str, message: str) -> dict:
    return {"passed": bool(passed), "expected": expected, "actual": actual, "message": message}


def _usd(v: float) -> str:
    return f"${v:,.2f}" if v >= 1 or v == 0 else f"${v:,.4f}"


def _ms(v: float) -> str:
    return f"{v:,.0f} ms"


def _num(v: Any) -> float | None:
    if isinstance(v, bool) or v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _failed_agents(snapshot: dict) -> list[str]:
    return sorted(a for a, n in (snapshot.get("nodes") or {}).items()
                  if isinstance(n, dict) and n.get("status") == "failed")


def _check_run(check: dict, snapshot: dict) -> dict:
    want = check["run"]
    got = str(snapshot.get("overall") or "")
    if got == want:
        return _result(True, f"run {want}", got, f"The run ended {got}.")
    where = _failed_agents(snapshot)
    tail = f" (failed at: {', '.join(where)})" if got == "failed" and where else ""
    return _result(False, f"run {want}", got or "(unknown)",
                   f"The run ended {got or '(unknown)'}{tail}, not {want}.")


def _node(snapshot: dict, agent: str) -> dict | None:
    nodes = snapshot.get("nodes") or {}
    node = nodes.get(agent)
    return node if isinstance(node, dict) else None


def _check_agent(check: dict, snapshot: dict) -> dict:
    agent = str(check["agent"])
    node = _node(snapshot, agent)
    if node is None:
        known = sorted(snapshot.get("nodes") or {})
        return _result(False, _wants(check), "(no such agent)",
                       f"Agent {agent!r} is not in this run's workflow"
                       + (f" (it has: {', '.join(known)})." if known else "."))
    status = str(node.get("status") or "")
    if "status" in check:
        want = check["status"]
        ok = status == want
        return _result(ok, f"agent {agent} status {want}", status or "(none)",
                       f"Agent {agent} ended {status or '(no status)'}" + ("." if ok else f", not {want}."))
    raw = node.get("output")
    raw = "" if raw is None else str(raw)
    field = check.get("field")
    data = extract_json(raw)
    if field:
        if data is None:
            what = "has no output" if not raw.strip() else "did not return JSON, so its fields cannot be read"
            return _result(False, _wants(check), "(no JSON output)",
                           f"Agent {agent} {what} (status {status or 'none'}); wanted {field}.")
        value = _dig(data, field)
        subject = f"Agent {agent}, field {field}"
    else:
        value = raw
        subject = f"Agent {agent}'s output"
    shown = _show(value) if field else _clip(json.dumps(raw, ensure_ascii=False))
    failures: list[str] = []
    for op in sorted(set(check) & OPS):
        want = check[op]
        ok = (value is not _MISSING and not _contains(value, want)) if op == "notContains" \
            else _holds(op, value, want)
        if not ok:
            failures.append(_say(op, want))
    if not failures:
        return _result(True, _wants(check), shown, f"{subject} was {shown}, as expected ({_wants(check, False)}).")
    if value is _MISSING:
        return _result(False, _wants(check), "(missing)", f"{subject} is not in the output.")
    if not field and not raw.strip():
        return _result(False, _wants(check), "(no output)",
                       f"Agent {agent} has no output (status {status or 'none'}); expected {' and '.join(failures)}.")
    return _result(False, _wants(check), shown,
                   f"{subject} was {shown}, expected {' and '.join(failures)}.")


def _wants(check: dict, with_agent: bool = True) -> str:
    """What a check expects, in words."""
    if "run" in check:
        return f"run {check['run']}"
    parts = [_say(op, check[op]) for op in sorted(set(check) & OPS)]
    if "status" in check:
        parts = [f"status {check['status']}"]
    who = f"agent {check.get('agent')} " if with_agent else ""
    where = f"{check['field']} " if check.get("field") else ""
    return (who + where + " and ".join(parts)).strip()


def _check_limit(kind: str, check: dict, totals: dict | None) -> dict:
    spec = check[kind]
    key = "costUsd" if kind == "cost" else "latencyMs"
    value = _num((totals or {}).get(key))
    fmt = _usd if kind == "cost" else _ms
    label = "cost" if kind == "cost" else "latencyMs"
    want = " and ".join(f"{op} {fmt(float(spec[op]))}" for op in sorted(spec))
    if value is None:
        return _result(False, f"{label} {want}", "(not recorded)",
                       ("No cost was" if kind == "cost" else "No call times were")
                       + " recorded for this run, so the limit cannot be checked.")
    failed = [f"{op} {fmt(float(spec[op]))}" for op in sorted(spec) if not _holds(op, value, spec[op])]
    what = "The run cost" if kind == "cost" else "The model and tool calls took"
    if not failed:
        return _result(True, f"{label} {want}", fmt(value), f"{what} {fmt(value)} ({want}).")
    return _result(False, f"{label} {want}", fmt(value), f"{what} {fmt(value)}, expected {' and '.join(failed)}.")


def evaluate_check(check: Any, snapshot: dict, totals: dict | None = None) -> dict:
    """One check against one finished run: {passed, expected, actual, message}.

    NEVER raises and NEVER passes what it cannot read: a check of a kind this version does
    not know, or one that is malformed, fails and says so. A suite is JSON a person (or the
    assistant) wrote, and a typo that silently passed would be a test that tests nothing."""
    if not isinstance(check, dict):
        return _result(False, "a check", "(not an object)", "This check is not an object, so it was not run.")
    kinds = [k for k in _CHECK_KINDS if k in check]
    if len(kinds) != 1:
        return _result(False, "a check", "(unknown)",
                       "This check names none of " + ", ".join(_CHECK_KINDS) + " (or several), "
                       "so it was not run. It counts as failed rather than passed.")
    try:
        kind = kinds[0]
        if kind == "run":
            if check["run"] not in RUN_OUTCOMES:
                return _result(False, f"run {check['run']}", "(unknown)",
                               f"{check['run']!r} is not a way a run ends ({', '.join(RUN_OUTCOMES)}).")
            return _check_run(check, snapshot)
        if kind in ("cost", "latencyMs"):
            if not isinstance(check[kind], dict) or not check[kind] or set(check[kind]) - _LIMIT_OPS:
                return _result(False, kind, "(unknown)",
                               f"`{kind}` takes a comparison like {{\"lt\": 1}} (one of {sorted(_LIMIT_OPS)}).")
            return _check_limit(kind, check, totals)
        unknown = sorted(set(check) - _AGENT_KEYS)
        if unknown:
            return _result(False, _wants(check), "(unknown)",
                           f"This check has key(s) {unknown} that this version does not know, "
                           f"so it was not run. It counts as failed rather than passed.")
        if "status" not in check and not (set(check) & OPS):
            return _result(False, f"agent {check['agent']}", "(unknown)",
                           "This check has no comparison, so it was not run. It counts as failed rather than passed.")
        return _check_agent(check, snapshot)
    except Exception as e:  # noqa: BLE001 - one bad check must not hide the rest of the result
        return _result(False, "a check", "(error)", f"This check could not be evaluated ({type(e).__name__}).")


def checks_of(case: dict) -> list[dict]:
    """The checks a case runs: its own, and `{"run": "done"}` first unless it names the
    status the run should end with (see the module docstring)."""
    expect = list(case.get("expect") or [])
    return expect if _names_run(expect) else [{"run": "done"}, *expect]


def _names_run(expect: list) -> bool:
    return any(isinstance(c, dict) and "run" in c for c in expect)


def grade(case: dict, snapshot: dict | None, totals: dict | None = None, *, expired: bool = False) -> dict:
    """The result of one case from its run's snapshot:

        {"status": "passed" | "failed" | "running", "message": str, "checks": [...]}

    `running` while the run has not ended (nothing is judged yet). A run that has not ended
    when `expired` is set is `failed`: a suite must be able to finish. No snapshot means the
    run was deleted (or never written), which is a failure of the case to show a result."""
    if snapshot is None:
        return {"status": "failed", "checks": [],
                "message": "The run no longer exists (it was deleted), so it cannot be checked."}
    overall = str(snapshot.get("overall") or "")
    if overall not in TERMINAL:
        if expired:
            return {"status": "failed", "checks": [],
                    "message": f"The run was still {overall or 'running'} when this suite run gave up "
                               f"waiting for it, so it counts as failed."}
        return {"status": "running", "checks": [], "message": f"The run is {overall or 'running'}."}
    results = [evaluate_check(check, snapshot, totals) for check in checks_of(case)]
    if not _names_run(list(case.get("expect") or [])):
        results[0]["implicit"] = True
    failed = [r for r in results if not r["passed"]]
    message = ("All " + str(len(results)) + (" check passed." if len(results) == 1 else " checks passed.")
               if not failed else
               f"{len(failed)} of {len(results)} check{'s' if len(results) != 1 else ''} failed: "
               + _clip(failed[0]["message"], 200))
    return {"status": "passed" if not failed else "failed", "checks": results, "message": message}


def summarize(results: list[dict]) -> dict:
    """Counts over the cases of one suite run. `total` is the cases that actually started:
    one refused by `orchestrator.limits` is `notStarted`, not a failure of the build."""
    def count(status: str) -> int:
        return sum(1 for r in results if r.get("status") == status)

    passed, failed, running = count("passed"), count("failed"), count("running")
    return {"passed": passed, "failed": failed, "running": running,
            "notStarted": count("not_started"), "total": passed + failed + running}


def keep_newest(ordered_oldest_first: list, keep: int) -> tuple[list, list]:
    """(kept, dropped) of a list ordered oldest first: the newest `keep` stay."""
    n = max(0, int(keep))
    cut = len(ordered_oldest_first) - n
    return ordered_oldest_first[max(cut, 0):], ordered_oldest_first[:max(cut, 0)]
