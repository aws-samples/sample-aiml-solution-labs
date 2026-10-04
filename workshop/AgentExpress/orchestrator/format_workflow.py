#!/usr/bin/env python3
"""Format app/workflow.json for humans to read.

workflow.json is THE file a customer edits, so it is worth formatting for reading
rather than for a machine. `json.dump(indent=2)` gets two things wrong for this
file:

  * it escapes every non-ASCII character, turning the em-dashes and the emoji in
    the explanatory notes into \\u2014 and \\ud83e\\udded — unreadable in the one
    place the prose actually matters;
  * it explodes small leaf objects over five lines each, so a schema property
    definition or a set of content-filter strengths takes a screenful.

This rewrites it so that:
  * text is real UTF-8;
  * a short object/array with no nested containers goes on ONE line;
  * anything longer, or with nesting, stays expanded at 2-space indent;
  * EVERY AGENT, TOOL AND STEP CARRIES ITS KEYS IN THE SAME ORDER.

THAT LAST ONE IS THE POINT, AND IT USED TO SAY THE OPPOSITE HERE
This file's docstring used to promise "key order is preserved exactly". That was
fine for a file one person maintained and wrong for a file every customer edits.
Hand-written entries drift: in the shipped sample one tool listed `description`
first and another listed it last, some agents put `produces` before `maxTokens`
and some after. Each entry was correct and the block as a whole was unreadable,
because nothing could be compared by position — a reader had to re-scan every
entry instead of learning one shape.

So the order is now CANONICAL and it comes from `app/keys.json`, which is also
what the allow-list tests and the JSON Schema read. Groups in the order that file
declares them (an agent: who it is -> how it reasons -> where it lives -> what it
reads -> what is switched on), keys in the order they appear inside each group.
Anything the spec does not know about is kept, in its original order, after the
keys the spec does know — a key nothing recognises is a mistake to surface, not
one to silently move or drop.

Only REPEATED entities are reordered: agents, tools, steps. `orchestrator`, `ui`,
`guardrail` and `authorization` are single blocks, so there is no second one to
read consistently against, and imposing an order on them would only churn diffs.

    python3 format_workflow.py            # format in place
    python3 format_workflow.py --check    # exit 1 if it would change (for CI)

Run it after any programmatic edit to the file.
"""

from __future__ import annotations

import json
import sys
from collections import OrderedDict
from pathlib import Path

APP = Path(__file__).resolve().parent / "app"
PATH = APP / "workflow.json"
KEYS_PATH = APP / "keys.json"
INDENT = "  "
# A leaf collection is inlined only if it fits this width including its indent.
# 100 keeps the file comfortable in a side-by-side diff.
MAX_INLINE = 100


def canonical_order(block: str) -> list[str]:
    """The key order `app/keys.json` declares for one kind of entry.

    Grouped, because that is what makes the shape learnable: the spec lists its groups
    in reading order and its keys within them, and this flattens the two into the single
    sequence the formatter writes. A key with no `group` sorts after the grouped ones.
    """
    spec = json.loads(KEYS_PATH.read_text())[block]
    if not spec.get("$ordered"):
        return []
    groups = list(spec.get("$groups") or {})
    keys = spec["keys"]
    return sorted(keys, key=lambda k: (
        groups.index(keys[k]["group"]) if keys[k].get("group") in groups else len(groups),
        list(keys).index(k),
    ))


def top_level_order(block: str) -> list[str]:
    """`canonical_order` with dotted keys collapsed to their parent, de-duplicated.

    A tool's spec names `policy.tool`, `policy.restrictTo` and `policy.permit`, but the
    entry itself has one `policy` key — so the order the formatter applies is over the
    parents. De-duplicated rather than left repeated because callers compare against
    this list, and three `policy` entries make an otherwise-correct entry look wrong.
    """
    seen: dict[str, None] = {}
    for key in canonical_order(block):
        seen.setdefault(key.partition(".")[0], None)
    return list(seen)


def reorder(entry: dict, order: list[str]) -> OrderedDict:
    """`entry` with the keys the spec knows first, in its order, then the rest as found.

    An unrecognised key is KEPT. The formatter's job is to make the file readable, not
    to enforce the schema — `tests/test_config_keys.py` is what rejects a key nothing
    reads, and it gives a message naming the key. Dropping it here would mean a
    customer's typo vanished on save, which is the worst of the three options.
    """
    if not isinstance(entry, dict) or not order:
        return entry
    known = [k for k in order if k in entry]
    rest = [k for k in entry if k not in known]
    return OrderedDict((k, entry[k]) for k in known + rest)


def canonicalise(doc: dict) -> dict:
    """Reorder every repeated entity in `doc`. Single blocks are left alone."""
    out = OrderedDict(doc)
    agent_order = canonical_order("agent")
    for aid, agent in (out.get("agents") or {}).items():
        out["agents"][aid] = reorder(agent, agent_order)
    tool_order = top_level_order("tool")
    for name, tool in (out.get("tools") or {}).items():
        out["tools"][name] = reorder(tool, tool_order)
    step_order = canonical_order("step")
    if isinstance(out.get("steps"), list):
        out["steps"] = [reorder(s, step_order) for s in out["steps"]]
    return out


def key_sequences(doc: dict) -> dict[str, list[str]]:
    """Every repeated entity's key order, for the round-trip guard below."""
    seqs = {f"agents.{k}": list(v) for k, v in (doc.get("agents") or {}).items()}
    seqs |= {f"tools.{k}": list(v) for k, v in (doc.get("tools") or {}).items()}
    seqs |= {f"steps[{i}]": list(s) for i, s in enumerate(doc.get("steps") or [])
             if isinstance(s, dict)}
    return seqs


def _is_leaf(v) -> bool:
    """True when `v` contains no dict or list, so inlining it loses no structure."""
    if isinstance(v, dict):
        return all(not isinstance(x, (dict, list)) for x in v.values())
    if isinstance(v, list):
        return all(not isinstance(x, (dict, list)) for x in v)
    return True


def _scalar(v) -> str:
    return json.dumps(v, ensure_ascii=False)


def _inline(v) -> str:
    """One-line form, with a space inside the braces so it reads as a unit."""
    if isinstance(v, dict):
        if not v:
            return "{}"
        return "{ " + ", ".join(f"{_scalar(k)}: {_scalar(x)}" for k, x in v.items()) + " }"
    if isinstance(v, list):
        if not v:
            return "[]"
        return "[" + ", ".join(_scalar(x) for x in v) + "]"
    return _scalar(v)


def render(v, depth: int = 0) -> str:
    pad = INDENT * depth
    inner = INDENT * (depth + 1)

    if isinstance(v, (dict, list)) and _is_leaf(v):
        one = _inline(v)
        # +2 for a trailing comma and a little slack.
        if len(pad) + len(one) + 2 <= MAX_INLINE:
            return one

    if isinstance(v, dict):
        if not v:
            return "{}"
        parts = [f"{inner}{_scalar(k)}: {render(x, depth + 1)}" for k, x in v.items()]
        return "{\n" + ",\n".join(parts) + f"\n{pad}}}"

    if isinstance(v, list):
        if not v:
            return "[]"
        parts = [f"{inner}{render(x, depth + 1)}" for x in v]
        return "[\n" + ",\n".join(parts) + f"\n{pad}]"

    return _scalar(v)


def main() -> int:
    doc = json.loads(PATH.read_text(), object_pairs_hook=OrderedDict)
    ordered = canonicalise(doc)
    out = render(ordered) + "\n"

    # Never let formatting change meaning.
    if json.loads(out) != doc:
        print("format_workflow: refusing to write - round-trip changed the document",
              file=sys.stderr)
        return 2

    # AND never let it change meaning in a way the check above cannot see. `==` on two
    # dicts ignores key order, so it passed a document whose keys had been shuffled or
    # DROPPED and re-added — which is precisely the class of bug reordering introduces.
    # Asserted on the written bytes, not on `ordered`, so a rendering bug counts too.
    written = json.loads(out, object_pairs_hook=OrderedDict)
    for path, keys in key_sequences(ordered).items():
        got = key_sequences(written)[path]
        if got != keys:
            print(f"format_workflow: refusing to write - reordering changed {path}: "
                  f"{keys} -> {got}", file=sys.stderr)
            return 2
    if sorted(key_sequences(written)) != sorted(key_sequences(doc)):
        print("format_workflow: refusing to write - reordering added or removed an entry",
              file=sys.stderr)
        return 2
    for path, before in key_sequences(doc).items():
        after = key_sequences(written)[path]
        if sorted(before) != sorted(after):
            print(f"format_workflow: refusing to write - {path} lost or gained key(s): "
                  f"{sorted(set(before) ^ set(after))}", file=sys.stderr)
            return 2

    if "--check" in sys.argv:
        if PATH.read_text() == out:
            print(f"format_workflow: {PATH.name} is formatted")
            return 0
        print(f"format_workflow: {PATH.name} needs formatting (run: python3 "
              f"{Path(__file__).name})", file=sys.stderr)
        return 1

    before = PATH.read_text()
    PATH.write_text(out)
    lines_before, lines_after = before.count("\n"), out.count("\n")
    print(f"format_workflow: {PATH.name} {lines_before} -> {lines_after} lines")
    return 0


if __name__ == "__main__":
    sys.exit(main())
