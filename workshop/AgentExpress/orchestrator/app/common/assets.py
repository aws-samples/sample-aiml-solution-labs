"""Asset plumbing shared by BOTH agent runners.

`research.py` (agents that gather evidence from a tool) and `synthesis.py` (agents
that reason over upstream assets) are two different flows, but they need the same
five mechanics: read the run's brief, count this agent's version, slug a title,
parse the model's JSON, and normalise provenance. Those lived in both files —
`slug` and `prior_version` were byte-identical copies, and `extract_json` had
DIVERGED: only synthesis could salvage a JSON object the model truncated at its
token budget, even though research emits the largest payload of the two and is
therefore the more likely to be cut off.

Nothing here is domain-specific. A customer writing their own agent runner can
import these directly.
"""

from __future__ import annotations

import json
import re

from app.common.config import FIRST_AGENT_ID
from app.common.contracts.base import Source

# The schema shows `"sourceType": "the kind of source"`, so a model that returns
# that instruction verbatim has supplied no provenance at all and we fall back to
# "other". Matched EXACTLY (case-insensitively) on purpose. The previous test was
# "contains a space", which rewrote every legitimate multi-word kind — observed
# live flattening "AWS documentation", "AWS web content", "AWS article",
# "enterprise guide" and "resource guide" to "other" in a single run — the exact
# coercion that keeping `Source.source_type` a plain str is meant to prevent.
PLACEHOLDER_SOURCE_TYPES = frozenset({
    "the kind of source", "kind of source", "the type of source", "type of source",
    "source type", "sourcetype", "string", "...",
})


def slug(text: str, limit: int = 48) -> str:
    """Lowercase, hyphenated, length-capped — safe inside an assetId."""
    s = re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")
    return s[:limit] or "request"


def parse(raw: str | None) -> dict:
    """Parse a stored asset (already valid JSON), or {} if absent/unparseable."""
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return obj if isinstance(obj, dict) else {}


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


# ---------------------------------------------------------------------------
# The run's brief — the FIRST agent's output
# ---------------------------------------------------------------------------
#
# Every downstream agent frames its work with the first agent's approved output.
# FIRST_AGENT_ID is derived from the topology (app/common/config.py), so that agent
# can be named anything.
#
# Two OPTIONAL keys are read from it by convention, and only for presentation and
# for building a retrieval query — never for logic:
#   title     — a short label, used in assetIds and asset headings
#   objective — what the run is trying to achieve, used as the retrieval query
# A first agent that emits neither still works: `brief_title` and `brief_query`
# fall back to the longest string in the brief, then to the run's topic. That
# keeps a customer whose brief is {"ticketId": ..., "severity": ...} out of the
# failure mode where every retrieval query silently became the raw user input.

_TITLE_KEYS = ("title", "name", "subject")
_QUERY_KEYS = ("objective", "goal", "question", "title")


def _longest_string(b: dict) -> str:
    vals = [v.strip() for v in b.values() if isinstance(v, str) and v.strip()]
    return max(vals, key=len) if vals else ""


def brief(ctx) -> dict:
    """The first agent's approved output, parsed. {} before it has run."""
    return parse(ctx.input(FIRST_AGENT_ID))


def brief_title(b: dict, ctx) -> str:
    """A short display label for the run. Never empty."""
    for k in _TITLE_KEYS:
        if str(b.get(k) or "").strip():
            return str(b[k]).strip()
    return _longest_string(b) or ctx.topic or ctx.agent_id


def brief_query(b: dict, fallback: str) -> str:
    """The retrieval query for a tool call, capped for the tool's own limits."""
    for k in _QUERY_KEYS:
        if str(b.get(k) or "").strip():
            return str(b[k]).strip()[:200]
    return (_longest_string(b) or fallback)[:200]


def prior_version(ctx) -> int:
    """The version this agent's asset should carry: 1 on the first run, then the
    previous asset's version + 1 for each revise / re-run.

    THE EMPTY CASE HAS TO BE HANDLED SEPARATELY. It used to be
    `int(prev.get("version", 1)) + 1` with no check, and on a first run `prev` is
    `{}`, so the default 1 was incremented to 2 — every asset in a fresh run was
    stamped `"version": 2` while the timeline said "complete (v1)". Two numbers for
    the same thing, disagreeing, in front of the reviewer: observed across all 24
    assets of three live runs with one history entry each and no revise.

    A previous asset whose own `version` is missing or unreadable still means THIS
    run is a re-run, so it counts from that asset rather than restarting at 1.
    """
    prev = parse(ctx.input(ctx.agent_id))
    if not prev:
        return 1
    try:
        return int(prev.get("version", 1)) + 1
    except (TypeError, ValueError):
        return 2


def build_sources(payload: dict, *, verify_urls_against: str = "") -> list[Source]:
    """Coerce the model's `sources` into validated Source objects.

    When `verify_urls_against` is given (the evidence the model was shown), a url
    is kept ONLY if it appears verbatim in it. The prompt tells the model never to
    invent one, but a prompt is not a control: a fabricated link is
    indistinguishable from a real citation to whoever reads the output. A synthesis
    agent passes nothing, because it only ever sees upstream assets whose urls were
    already verified at the point they were gathered.
    """
    out: list[Source] = []
    for i, s in enumerate(payload.get("sources", []) or []):
        if not isinstance(s, dict):
            continue
        # Casing is the customer's, not ours: lower-casing turned "AWS Bedrock KB"
        # into "aws bedrock kb" on the way to the reader.
        st = str(s.get("sourceType") or "other").strip()
        if not st or st.lower() in PLACEHOLDER_SOURCE_TYPES:
            st = "other"
        url = str(s.get("url") or "").strip() or None
        if url and verify_urls_against and url not in verify_urls_against:
            url = None
        out.append(Source(
            sourceId=s.get("sourceId") or f"source-{i}",
            sourceType=st,
            sourceName=str(s.get("sourceName") or s.get("sourceType") or "source"),
            url=url,
            sourceAssetId=s.get("sourceAssetId"),
        ))
    return out


def str_list(payload: dict, key: str) -> list[str]:
    """A list-of-strings field, with blanks dropped."""
    return [str(x) for x in (payload.get(key) or []) if str(x).strip()]


def for_prompt(parsed: dict) -> str:
    """An upstream asset rendered for a downstream prompt. Returns "" if there is
    nothing to render, so the caller can fall back to the raw string.
    """
    if not parsed:
        return ""
    # ensure_ascii=False so the model is shown the characters the upstream agent
    # actually wrote. The default escapes them, and that silently broke figure
    # grounding: an upstream asset saying "2\u20134 weeks" arrived as the literal
    # six characters \u2013, so no downstream agent could ever quote a range with
    # an en dash and have it recognised as supported. It also spent tokens on
    # escape sequences for every dash, quote and accent in the inputs.
    return json.dumps(parsed, indent=2, default=str, ensure_ascii=False)
