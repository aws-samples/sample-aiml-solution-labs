"""Skills: know-how an agent can use (workflow.json `skills`, an agent's `skills`).

A skill is a SKILL.md (a description of when to use it, then the instructions) with
optional reference files, written in the Builder or taken from the library or an AWS
Agent Registry. It is not a tool and not a knowledge base: it tells the agent HOW to do
a task (which steps, which checks, which tools), and is read only when it applies.

How an agent gets its skills:
  * toolMode "model" with tools: the tool-calling turn sees each skill's name and
    description and opens the ones it needs with `use_skill` (app/common/tool_loop.py),
    a skill's reference files the same way. Every model call the agent makes after
    that (ctx.llm) carries the skills it opened in full, and the others by name only:
    progressive disclosure, so a skill costs tokens only when it is used.
  * otherwise (no model-driven tool turn): its skills are added to its instructions in
    full, reference files included, up to a budget.
Scripts in a skill are never run: there is nothing here that executes skill content.
"""
from __future__ import annotations

from app.common.config import WORKFLOW

#: name -> {"description", "instructions", "files"}. A library reference is resolved
#: before a deploy (bff/library.py), so one still here could not be loaded: skipped.
SKILLS: dict[str, dict] = {
    k: v for k, v in (WORKFLOW.get("skills") or {}).items()
    if isinstance(v, dict) and "library" not in v and isinstance(v.get("instructions"), str)}

#: The name of the tool the model opens a skill with.
TOOL_NAME = "use_skill"
#: How much skill text one agent's prompt may carry in full (instructions and files).
FULL_CHARS = 60_000
#: How much one opened file may put in front of the model.
FILE_CHARS = 30_000
HEADER = "=== YOUR SKILLS ==="


def known(names) -> list[str]:
    """The names this deployment has, in the agent's order."""
    return [n for n in (names or []) if n in SKILLS]


def _files(skill: dict) -> dict:
    files = skill.get("files")
    return {k: v for k, v in files.items() if isinstance(v, str)} if isinstance(files, dict) else {}


def catalog(names) -> str:
    """One line per skill: its name, when to use it, and its reference files."""
    lines = []
    for n in known(names):
        s = SKILLS[n]
        files = sorted(_files(s))
        extra = f" (reference files: {', '.join(files)})" if files else ""
        lines.append(f"- {n}: {str(s.get('description') or '').strip()}{extra}")
    return "\n".join(lines)


def open_skill(name: str, file: str | None = None, allowed=None) -> str:
    """A skill's instructions, or one of its reference files: what `use_skill` returns.
    `allowed`: the agent's own skills; another agent's skill is not opened."""
    have = known(allowed) if allowed is not None else list(SKILLS)
    if str(name or "") not in have:
        return f"There is no skill named {name!r} for you. Your skills: {', '.join(have) or '(none)'}."
    s = SKILLS[name]
    if file:
        files = _files(s)
        if file not in files:
            return (f"Skill {name!r} has no reference file {file!r}. "
                    f"Its files: {', '.join(sorted(files)) or '(none)'}.")
        return f"--- {name} / {file} ---\n{files[file][:FILE_CHARS]}"
    return f"--- SKILL: {name} ---\n{s['instructions']}"


def tool_spec(names) -> dict:
    """The `use_skill` tool, offered only with the agent's own skills."""
    return {"name": TOOL_NAME,
            "description": ("Open one of your skills: returns its instructions (follow them), or with `file` one "
                            "of its reference files. Open a skill before acting when its description fits "
                            "the request."),
            "input_schema": {"type": "object", "required": ["name"], "properties": {
                "name": {"type": "string", "enum": known(names), "description": "The skill to open."},
                "file": {"type": "string", "description": "One of the skill's reference files, to read it."}}}}


def prompt_block(names, opened, full: bool) -> str:
    """What ctx.llm adds to the system prompt: the skills in full (`full`, or each one
    the agent opened), and the rest by name and description. "" with no skills."""
    have = known(names)
    if not have:
        return ""
    parts, used = [HEADER], 0
    shown = have if full else [n for n in have if n in (opened or ())]
    for n in shown:
        s = SKILLS[n]
        block = [f"--- SKILL: {n} ---", s["instructions"].strip()]
        if full:
            block += [f"--- {n} / {f} ---\n{text.strip()}" for f, text in sorted(_files(s).items())]
        text = "\n".join(block)
        if used + len(text) > FULL_CHARS:
            parts.append(f"(Skill {n} not shown in full: the {FULL_CHARS}-character skills budget is spent. "
                         f"Follow its description: {str(s.get('description') or '').strip()})")
            continue
        parts.append(text)
        used += len(text)
    rest = [n for n in have if n not in shown]
    if rest:
        parts.append("Not opened for this request (by name and description only):\n" + catalog(rest))
    parts.append("Follow a skill's instructions when its description fits the request.")
    return "\n\n".join(parts)
