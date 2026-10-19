"""A runtime "task" step's own checks, for validate_build.py.

A task is a step with no model: it calls a tool, runs a script in the Code Interpreter
sandbox, reads the run's documents with Textract, or writes a file (app/common/task_agent.py).
The generic key checks (app/keys.json) already say that `task` is required, that it is an
object, and that the model keys do not apply. These are the rest: what each kind needs, and
that every placeholder names something that exists by the time the step runs.

Mirrors web/src/builder/taskCheck.ts word for word (both run
tests/fixtures/validation_cases.json) and, for what the runtime itself refuses,
app/common/task_agent.problem.
"""
from __future__ import annotations

import re

#: The keys each kind reads (app/common/task_agent.KEYS_OF).
KEYS_OF = {"tool": ("kind", "tool", "arguments"), "code": ("kind", "language", "code"),
           "extract": ("kind", "file", "features"), "output": ("kind", "format", "name", "from")}
ALL_KEYS = {k for keys in KEYS_OF.values() for k in keys}
MAX_CODE_BYTES = 50_000
PLACEHOLDER = re.compile(r"\{\{\s*([^{}]*?)\s*\}\}")
REF = re.compile(r"^(request|session|outputs\.[A-Za-z][A-Za-z0-9_]*(\.[A-Za-z0-9_-]+)*)$")
FROM_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*(\.[A-Za-z0-9_-]+)*$")


def _not_one_of(v, allowed: list[str]) -> str:
    """The words for a value outside its set (a non-string is not echoed: Python and
    JavaScript would print it differently)."""
    return f'"{v}" is not one of: {", ".join(allowed)}' if isinstance(v, str) else f"must be one of: {', '.join(allowed)}"


def _refs(value) -> list[str]:
    if isinstance(value, str):
        return [m.group(1) for m in PLACEHOLDER.finditer(value)]
    if isinstance(value, dict):
        return [r for v in value.values() for r in _refs(v)]
    if isinstance(value, list):
        return [r for v in value for r in _refs(v)]
    return []


def _check_refs(value, before: list[str], where: dict, p: str, add) -> None:
    for ref in _refs(value):
        if not REF.match(ref):
            add("error", where, p, f"{{{{{ref}}}}} is not a placeholder this step knows: use "
                                   "{{request}}, {{session}} or {{outputs.<agent>.<field>}}")
        elif ref.startswith("outputs."):
            src = ref.split(".")[1]
            if src not in before:
                add("error", where, p, f'{{{{{ref}}}}}: "{src}" does not run before this step')


def check(agents: dict, tools: dict, steps: list, orch, add, vocab, upstream_ids) -> None:
    """Add the issues of every task step. `vocab` and `upstream_ids` are validate_build's."""
    kinds = vocab("taskKinds")
    for aid, a in agents.items():
        if not isinstance(a, dict) or str(a.get("runtime") or "").lower() != "task":
            continue
        task = a.get("task")
        if not isinstance(task, dict):
            continue          # required, and an object: the generic checks say so
        where = {"kind": "agent", "id": aid}
        path = f"agents.{aid}"
        tp = f"{path}.task"
        kind = task.get("kind")
        if "kind" not in task:
            add("error", where, f"{tp}.kind", f"say what this step does: one of {', '.join(kinds)}")
            continue
        if not isinstance(kind, str) or kind not in kinds:
            add("error", where, f"{tp}.kind", _not_one_of(kind, kinds))
            continue
        for k in task:
            # A key no kind reads is the generic checks' "unknown key"; this is the one that
            # belongs to another kind.
            if k in ALL_KEYS and k not in KEYS_OF[kind]:
                add("error", where, f"{tp}.{k}", f'does not apply to a "{kind}" step')
        before = upstream_ids(steps, aid)
        core = a.get("agentcore")
        for feature in (core if isinstance(core, dict) else {}):
            if feature != "guardrails":
                add("error", where, f"{path}.agentcore.{feature}",
                    f"a task step makes no model call, so {feature} has nothing to work on — remove it")
        if kind == "tool":
            key = task.get("tool")
            if not isinstance(key, str) or not key:
                add("error", where, f"{tp}.tool", "pick the tool this step calls")
            elif not isinstance(tools.get(key), dict):
                add("error", where, f"{tp}.tool",
                    f'"{key}" is not a tool. Declare it under Tools, or pick one of: '
                    f'{", ".join(tools) or "(none yet)"}')
            _check_refs(task.get("arguments"), before, where, f"{tp}.arguments", add)
        elif kind == "code":
            code = task.get("code")
            if not isinstance(code, str) or not code.strip():
                add("error", where, f"{tp}.code", "write the script this step runs")
            elif len(code.encode("utf-8")) > MAX_CODE_BYTES:
                add("error", where, f"{tp}.code",
                    f"is {len(code.encode('utf-8'))} bytes; a script may be at most {MAX_CODE_BYTES}")
            lang = task.get("language")
            if "language" in task and lang not in vocab("taskLanguages"):
                add("error", where, f"{tp}.language", _not_one_of(lang, vocab("taskLanguages")))
        elif kind == "extract":
            if a.get("attachments") is not True:
                add("error", where, f"{path}.attachments",
                    "an extract step reads the run's files: turn on attachments")
            feats = task.get("features")
            if isinstance(feats, list):
                if not feats:
                    add("error", where, f"{tp}.features",
                        f"name at least one of {', '.join(vocab('taskExtractFeatures'))}, or leave it out for both")
                for i, f in enumerate(feats):
                    if f not in vocab("taskExtractFeatures"):
                        add("error", where, f"{tp}.features[{i}]", _not_one_of(f, vocab("taskExtractFeatures")))
        elif kind == "output":
            fmt = task.get("format")
            if "format" not in task:
                add("error", where, f"{tp}.format",
                    f"pick the file format: {', '.join(vocab('taskOutputFormats'))}")
            elif fmt not in vocab("taskOutputFormats"):
                add("error", where, f"{tp}.format", _not_one_of(fmt, vocab("taskOutputFormats")))
            if not (isinstance(orch, dict) and isinstance(orch.get("files"), dict)):
                add("error", where, tp,
                    "an output step saves a file, which needs orchestrator.files so the run keeps it")
            src = task.get("from")
            for i, ref in enumerate(src if isinstance(src, list) else []):
                if not isinstance(ref, str) or not FROM_RE.match(ref):
                    add("error", where, f"{tp}.from[{i}]", "must be an agent id, or <agent>.<field>")
                elif ref.split(".")[0] not in before:
                    add("error", where, f"{tp}.from[{i}]", f'"{ref.split(".")[0]}" does not run before this step')
            _check_refs(task.get("name"), before, where, f"{tp}.name", add)
