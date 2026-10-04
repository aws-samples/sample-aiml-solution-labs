"""Checks one Cedar policy for an AgentCore Gateway before it is saved or deployed.

A build's own policies live in workflow.json `orchestrator.policy.custom`; the IaC
creates each as an AgentCore policy next to the permits it generates per tool. This is
a structural check, not a Cedar evaluator: it catches what AgentCore would refuse at
deploy — or worse, accept and never apply — while the user can still fix it:

  * one `permit(...)` or `forbid(...)` statement, closed with `;`, with the head
    `(principal…, action…, resource…)` and only `when { … }` / `unless { … }` after it,
  * an action that names a tool of THIS build: `action == AgentCore::Action::"<key>___<tool>"`
    for one tool, `action in AgentCore::Action::"<key>"` for all of a target's tools
    (AgentCore has no wildcard action),
  * the resource `AgentCore::Gateway::"{{gateway}}"` — the build's Gateway ARN only
    exists once it deploys, so the IaC fills it in,
  * a `when` on a permit for one tool: AgentCore rejects an unconditioned one as
    "Overly Permissive" and the deploy never stabilises,
  * one tool, not a target's group, when a condition reads `context.input`: only a
    tool's own action has an input, so AgentCore fails the deploy with "attribute
    `input` in context for AgentCore::Action::"<key>" not found" (seen live).

Mirrored by web/src/builder/cedar.ts, message for message
(tests/fixtures/validation_cases.json holds both to it).
"""

from __future__ import annotations

import re

GATEWAY = "{{gateway}}"
#: A policy in `policies` written for whichever tool it is attached to.
TOOL = "{{tool}}"
MAX_LEN = 10000
#: Names the AgentCore policy custom_<name>_<8 hex>: its 48-character limit leaves 32.
NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9]{0,31}$")
SEP = "___"

_TOKEN = re.compile(r"""
    (?P<ws>\s+) | (?P<comment>//[^\n]*) |
    (?P<str>"(?:[^"\\]|\\.)*") | (?P<ident>[A-Za-z_][A-Za-z0-9_]*) |
    (?P<num>\d+) | (?P<op>::|==|!=|<=|>=|&&|\|\||[()\[\]{},;<>!.+\-*@])
""", re.VERBOSE)
_CLOSE = {"(": ")", "[": "]", "{": "}"}


class _Bad(Exception):
    pass


def _tokens(text: str) -> list[tuple[str, str]]:
    out, i = [], 0
    while i < len(text):
        m = _TOKEN.match(text, i)
        if not m:
            if text[i] == '"':
                raise _Bad("a string is not closed: add the missing \"")
            raise _Bad(f"unexpected character `{text[i]}`")
        kind = m.lastgroup or ""
        if kind not in ("ws", "comment"):
            out.append((kind, m.group()))
        i = m.end()
    return out


def _matching(toks: list, i: int) -> int:
    """Index of the bracket closing toks[i]."""
    stack = []
    for j in range(i, len(toks)):
        v = toks[j][1] if toks[j][0] == "op" else ""
        if v in _CLOSE:
            stack.append(_CLOSE[v])
        elif v in _CLOSE.values():
            if not stack or stack.pop() != v:
                raise _Bad(f"a bracket does not match: unexpected {v}")
            if not stack:
                return j
    raise _Bad(f"a bracket is not closed: add the missing {stack[-1] if stack else ')'}")


def _parts(toks: list) -> list[list]:
    """Split on the commas at depth 0."""
    parts, cur, depth = [], [], 0
    for t in toks:
        v = t[1] if t[0] == "op" else ""
        if v in _CLOSE:
            depth += 1
        elif v in _CLOSE.values():
            depth -= 1
        if v == "," and depth == 0:
            parts.append(cur)
            cur = []
        else:
            cur.append(t)
    parts.append(cur)
    return parts


def _refs(toks: list, kind: str) -> list[str]:
    """The ids of every AgentCore::<kind>::"<id>" in toks."""
    return [toks[j + 4][1][1:-1] for j in range(len(toks) - 4)
            if [t[1] for t in toks[j:j + 4]] == ["AgentCore", "::", kind, "::"] and toks[j + 4][0] == "str"]


def parse(statement) -> dict:
    """{effect, actions, gateways, conditioned, oneTool, head} or raises _Bad."""
    if not isinstance(statement, str) or not statement.strip():
        raise _Bad("is empty: write one permit(...) or forbid(...) statement")
    if len(statement) > MAX_LEN:
        raise _Bad(f"is longer than {MAX_LEN} characters")
    toks = _tokens(statement)
    i = 0
    while i < len(toks) and toks[i][1] == "@":  # annotations: @id("...")
        if i + 2 >= len(toks) or toks[i + 1][0] != "ident" or toks[i + 2][1] != "(":
            raise _Bad('an annotation is @name("value")')
        i = _matching(toks, i + 2) + 1
    if i >= len(toks) or toks[i][1] not in ("permit", "forbid"):
        raise _Bad("must start with permit( or forbid(")
    effect = toks[i][1]
    if i + 1 >= len(toks) or toks[i + 1][1] != "(":
        raise _Bad(f"{effect} must be followed by (principal, action, resource)")
    end = _matching(toks, i + 1)
    head = _parts(toks[i + 2:end])
    if len(head) != 3 or [p[0][1] if p else "" for p in head] != ["principal", "action", "resource"]:
        raise _Bad(f"the head must be {effect}(principal…, action…, resource…), in that order")
    j, conditioned = end + 1, False
    while True:
        if j >= len(toks):
            raise _Bad("must end with ;")
        v = toks[j][1]
        if v == ";":
            if j != len(toks) - 1:
                raise _Bad("holds more than one statement: give each its own policy")
            break
        if v in ("when", "unless") and j + 1 < len(toks) and toks[j + 1][1] == "{":
            close = _matching(toks, j + 1)
            if close == j + 2:
                raise _Bad(f"`{v} {{ }}` is empty: give it a condition")
            conditioned, j = True, close + 1
            continue
        raise _Bad(f"after the head only `when {{ … }}` or `unless {{ … }}` may follow, not `{v}`")
    action = head[1]
    reads_input = any(toks[k][1] == "context" and toks[k + 1][1] == "." and toks[k + 2][1] == "input"
                      for k in range(end + 1, len(toks) - 2))
    # Anything read from `context` other than `input`: AgentCore's context for a tool call
    # holds the arguments under `input` and nothing else a policy can use.
    bad_context = next((toks[k + 2][1] for k in range(end + 1, len(toks) - 2)
                        if toks[k][1] == "context" and toks[k + 1][1] == "."
                        and toks[k + 2][1] != "input"), "")
    return {"effect": effect, "actions": _refs(action, "Action"), "gateways": _refs(head[2], "Gateway"),
            "conditioned": conditioned, "equals": any(t[1] == "==" for t in action),
            "readsInput": reads_input, "badContext": bad_context}


def problems(statement, tool_keys=None) -> list[str]:
    """What is wrong with one statement, [] when nothing. tool_keys: the build's tool
    keys, to check the actions against; None skips that (a library policy not yet
    attached to a build)."""
    try:
        p = parse(statement)
    except _Bad as e:
        return [str(e)]
    out = []
    if p.get("badContext"):
        # Observed live, three ways: context.arguments parsed and matched nothing, and
        # context.query failed the deploy ("attribute `query` in context ... not found").
        out.append(f"AgentCore passes a tool call's arguments as context.input, not "
                   f"context.{p['badContext']}: write context.input.<argument>, with action == "
                   f"the one tool")
    if not p["actions"]:
        out.append('must name the tool it governs: action == AgentCore::Action::"<key>___<tool>", '
                   'or action in AgentCore::Action::"<key>" for all of its tools')
    for a in p["actions"]:
        key = a.split(SEP)[0]
        if key == TOOL:
            # A template: whichever tool it is attached to (tools.<key>.policies). The
            # rendered statement is checked against that tool when it is attached.
            if p["equals"] and SEP not in a:
                out.append(f'action == needs one tool, "{TOOL}{SEP}<toolName>"; for all of its tools write '
                           f'action in AgentCore::Action::"{TOOL}"')
            elif p["readsInput"] and SEP not in a:
                out.append(f'a condition on context.input needs one tool, action == AgentCore::Action::'
                           f'"{TOOL}{SEP}<toolName>": all of a target\'s tools together have no input to read')
            continue
        if tool_keys is not None and key not in tool_keys:
            listed = ", ".join(sorted(tool_keys)) or "none yet"
            out.append(f'"{a}" is not a tool of this build (its tools: {listed})')
        elif p["equals"] and SEP not in a:
            out.append(f'action == needs one tool, "{a}{SEP}<toolName>"; for all of its tools write '
                       f'action in AgentCore::Action::"{a}"')
        elif p["readsInput"] and SEP not in a:
            out.append(f'a condition on context.input needs one tool, action == AgentCore::Action::'
                       f'"{a}{SEP}<toolName>": all of "{a}"\'s tools together have no input to read')
    if p["gateways"] != [GATEWAY]:
        out.append(f'the resource must be resource == AgentCore::Gateway::"{GATEWAY}": the '
                   "build's Gateway ARN is filled in when it deploys")
    if p["effect"] == "permit" and not p["conditioned"] and any(SEP in a for a in p["actions"]):
        out.append("a permit for one tool needs a `when` condition, e.g. when { context.input has "
                   "query } — AgentCore rejects an unconditioned one as overly permissive")
    return out


def summary(statement) -> dict:
    """{effect, actions} for a list view; {} when it does not parse."""
    try:
        p = parse(statement)
    except _Bad:
        return {}
    return {"effect": p["effect"], "actions": p["actions"]}


def is_template(statement) -> bool:
    """Written for whichever tool it is attached to: names AgentCore::Action::"{{tool}}"."""
    try:
        return any(a.split(SEP)[0] == TOOL for a in parse(statement)["actions"])
    except _Bad:
        return False


def for_tool(statement: str, key: str) -> str:
    """A policy template as attached to one tool."""
    return statement.replace(TOOL, key)


def governs(statement, key: str) -> bool:
    """Whether a (rendered) statement names this tool."""
    try:
        return any(a.split(SEP)[0] == key for a in parse(statement)["actions"])
    except _Bad:
        return False


def render(statement: str, gateway_arn: str) -> str:
    """The statement as deployed: the Gateway placeholder replaced by its ARN."""
    return statement.replace(GATEWAY, gateway_arn)
