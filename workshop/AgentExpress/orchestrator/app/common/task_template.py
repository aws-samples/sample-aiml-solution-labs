"""The placeholders a task step's settings may hold (agents.<id>.task, app/common/task_agent.py).

    {{request}}                   what the run was started with
    {{session}}                   the run's id
    {{outputs.<agent>}}           an earlier step's output
    {{outputs.<agent>.<field>}}   one field of it (and deeper: .<field>.<n> for a list item)

A string that is ONLY one placeholder takes the value as it is: a number stays a number and
a list stays a list, so a tool argument keeps its type. A placeholder inside other text is
replaced by its text (JSON for an object or a list).

Deliberately tiny: no expressions, no filters, no defaults. Anything that needs logic is a
code step. Mirrored for the Builder by web/src/builder/taskCheck.ts and bff/task_check.py,
which check the same references before a deploy.
"""

from __future__ import annotations

import json
import re

#: One placeholder. Inside the braces: a dotted reference, nothing else.
PLACEHOLDER = re.compile(r"\{\{\s*([^{}]*?)\s*\}\}")
#: A reference: request, session, or outputs.<agent>[.<field>...].
REF = re.compile(r"^(request|session|outputs\.[A-Za-z][A-Za-z0-9_]*(\.[A-Za-z0-9_-]+)*)$")


class TemplateError(ValueError):
    """A placeholder that names nothing the run has."""


def refs(value) -> list[str]:
    """Every placeholder reference in `value` (a string, or strings inside lists and objects)."""
    if isinstance(value, str):
        return [m.group(1) for m in PLACEHOLDER.finditer(value)]
    if isinstance(value, dict):
        return [r for v in value.values() for r in refs(v)]
    if isinstance(value, list):
        return [r for v in value for r in refs(v)]
    return []


def parsed(output):
    """An output as data: the JSON it holds when it is JSON, else the text itself."""
    if isinstance(output, str):
        s = output.strip()
        if s.startswith(("{", "[")):
            try:
                return json.loads(s)
            except ValueError:
                return output
    return output


def lookup(ref: str, data: dict):
    """The value `ref` names in `data` ({"request", "session", "outputs"}). Raises
    TemplateError when it names nothing."""
    if not REF.match(ref):
        raise TemplateError(f"{{{{{ref}}}}} is not a placeholder this step knows: use {{{{request}}}}, "
                            f"{{{{session}}}} or {{{{outputs.<agent>.<field>}}}}")
    if ref in ("request", "session"):
        return data.get(ref) or ""
    parts = ref.split(".")[1:]
    outputs = data.get("outputs") or {}
    if parts[0] not in outputs:
        raise TemplateError(f"{{{{{ref}}}}}: {parts[0]} has no output in this run yet")
    node = parsed(outputs[parts[0]])
    for i, part in enumerate(parts[1:], 1):
        if isinstance(node, dict) and part in node:
            node = node[part]
        elif isinstance(node, list) and part.isdigit() and int(part) < len(node):
            node = node[int(part)]
        else:
            where = ".".join(parts[:i])
            raise TemplateError(f"{{{{{ref}}}}}: {where} has no {part!r}")
    return node


def _text(value) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


def render(value, data: dict):
    """`value` with every placeholder replaced (strings inside lists and objects too)."""
    if isinstance(value, str):
        whole = PLACEHOLDER.fullmatch(value.strip())
        if whole:
            return lookup(whole.group(1), data)
        return PLACEHOLDER.sub(lambda m: _text(lookup(m.group(1), data)), value)
    if isinstance(value, dict):
        return {k: render(v, data) for k, v in value.items()}
    if isinstance(value, list):
        return [render(v, data) for v in value]
    return value
