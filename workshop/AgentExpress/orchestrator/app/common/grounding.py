"""Do the figures in an agent's asset trace to the assets it was built from?

Every synthesis prompt in this framework carries the same rule — no figure that is
not in an upstream asset, not even as an illustration — and until now that rule was
only ever STATED. Three places state it (`_shared/synthesis.py`, `_shared/research.py`,
`a2a_lambda/handler.py`) and nothing checked it, so compliance was whatever the model
felt like that run. A measured run showed what that costs: the analysis asset said in
its own `limitations` that the evidence "does not specify cold-start latency impact …
sources acknowledge these as constraints but do not supply numerical thresholds", and
the recommendation built from that asset then advised measuring against "typically
100–1000 ms" and "typically 1–10 ms". Both numbers were invented. Both read exactly
like findings. The same workflow run against a second deployment produced no invented
figure at all, which is the tell: this is not a prompt to fix, it is a rule to enforce.

A comment in `cost_research/agent.py` had also described this as already handled —
"`unsupported-figure` catches it … the pipeline's existing guardrail does the rest" —
naming a classification that is not in `EvidenceClass` and a guardrail that did not
exist. This module is that guardrail, and the comment now points here.

WHAT IT DOES NOT DO: fail the run. A figure with no upstream match is a strong signal,
not a proof — the writer may have restated a number in a form the matcher cannot see.
Ending a five-minute run on that would trade a reviewable warning for a lost run. So
the finding is emitted onto the session timeline against the agent that produced it,
which puts it in front of the human at the very next HITL gate — the same reviewer the
evidence classifications on every finding already exist to serve.

WHY NO CONFIG KEY: the rule is already imposed on every synthesis agent
unconditionally, without asking the workflow's permission. Checking the rule needs no
more permission than stating it did, and a switch here would only ever be used to stop
hearing about a real problem.
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation

# --- what counts as a FIGURE -----------------------------------------------
# A number alone is not a figure. "three layers", "(1)", "(2)" and "v1" are prose and
# structure, and flagging them would bury the real finding in noise — which is how a
# check like this gets switched off. A figure is a number carrying a UNIT, which is
# also exactly what the rule enumerates: costs, thresholds, percentages, durations,
# cadences, timelines.
_UNITS = (
    r"ms|millisecond|milliseconds|sec|secs|second|seconds|min|mins|minute|minutes"
    r"|hr|hrs|hour|hours|day|days|week|weeks|month|months|quarter|quarters|year|years"
    r"|[kmgtp]b|byte|bytes|usd|eur|gbp|dollars?|cents?"
    r"|rps|qps|tps|iops|vcpus?|cpus?|cores?|dpu|dpus|nodes?|shards?|replicas?"
)
_NUM = r"\d+(?:,\d{3})*(?:\.\d+)?"
# A number and its unit: "15 minutes", "0.022 USD", "32%", "10GB", "$1,200".
_FIGURE = re.compile(
    rf"(?:[$€£]\s*{_NUM})"
    rf"|(?:{_NUM}\s*%)"
    rf"|(?:{_NUM}\s*(?:{_UNITS})\b)",
    re.IGNORECASE,
)
# A RANGE takes its unit from the far end — "100–1000 ms" and "2-4 weeks" are two
# figures each, and the first one carries no unit of its own. Missing that half is
# how "typically 100–1000 ms" would have passed with only the 1000 examined.
_RANGE = re.compile(
    rf"({_NUM})\s*(?:–|—|-|to)\s*({_NUM})\s*(?:%|(?:{_UNITS})\b)",
    re.IGNORECASE,
)
# Every number, for the UPSTREAM side. Grounding is deliberately more permissive than
# detection: a figure counts as grounded if its value appears upstream in any form at
# all, unit or not. Requiring the unit to match too would flag a faithfully carried
# number that the upstream asset happened to word differently.
_ANY_NUM = re.compile(_NUM)
# Structure, not content: asset ids and their versions, ISO timestamps, the envelope's
# own `version`. These are full of digits that mean nothing about the subject.
_STRUCTURE = re.compile(
    r"asset-[\w-]+"
    r"|\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?)?"
    r"|\"version\"\s*:\s*\d+"
    r"|\bv\d+\b",
    re.IGNORECASE,
)
# What an agent returns is a JSON document, and `json.dumps` escapes non-ASCII, so a
# range written "100–1000 ms" arrives as "100\u20131000 ms". Scanning that raw gives
# 2013 and 1000 as one 8-digit run — the first live asset this was pointed at reported
# "20131000 ms", a figure no one wrote. Decoding first also lets `_RANGE` see the en
# dash it is looking for. The same escaping already cost this framework a defect in
# `upstream_context` (see synthesis.py), which is why it is handled here rather than
# assumed away.
_ESCAPED = re.compile(r"\\u([0-9a-fA-F]{4})")


def _readable(text: str) -> str:
    """The text as its author wrote it: JSON unicode escapes resolved, structure dropped.

    A targeted substitution rather than a full JSON parse, because this has to work on
    a truncated or partly-repaired asset too — the cases where a figure is most likely
    to be wrong are the ones least likely to parse.
    """
    decoded = _ESCAPED.sub(lambda m: chr(int(m.group(1), 16)), text or "")
    return _STRUCTURE.sub(" ", decoded)


def _value(raw: str) -> str | None:
    """A numeric literal reduced to one canonical form, so 1,000 == 1000 == 1000.0.

    Without this, a figure carried faithfully from an upstream asset that wrote it
    with a thousands separator would look invented.
    """
    try:
        return format(Decimal(raw.replace(",", "")).normalize(), "f")
    except (InvalidOperation, ValueError):
        return None


def figures(text: str) -> dict[str, str]:
    """The figures in `text`, as {canonical value: the phrase it appeared in}.

    The phrase is kept because a bare number in a warning is not actionable — the
    reviewer needs to see "100–1000 ms" to know what is being questioned.
    """
    clean = _readable(text)
    found: dict[str, str] = {}
    # Ranges FIRST so a ranged bound reports the whole range. Both passes match the
    # unit-bearing end, and letting the narrower one win reported the same invented
    # latency twice — once as "1000 ms" and once as "100–1000 ms".
    for match in _RANGE.finditer(clean):
        for raw in match.groups():
            value = _value(raw)
            if value is not None:
                found.setdefault(value, match.group(0).strip())
    for match in _FIGURE.finditer(clean):
        phrase = match.group(0).strip()
        number = _ANY_NUM.search(phrase)
        value = _value(number.group(0)) if number else None
        if value is not None:
            found.setdefault(value, phrase)
    return found


def values(text: str) -> set[str]:
    """Every number in `text`, canonicalized — the grounding corpus side."""
    return {v for v in (_value(m.group(0)) for m in _ANY_NUM.finditer(_readable(text)))
            if v is not None}


def ungrounded(output: str, upstream: list[str]) -> list[str]:
    """Phrases in `output` carrying a figure that appears in no upstream text.

    `upstream` is every text this asset could honestly have taken a figure from: the
    other assets in this run and the request itself. Passing all of them rather than
    only the agent's declared inputs is intentional — the question being asked is
    "was this number invented", and a number already present anywhere in the run was
    not.
    """
    corpus: set[str] = set()
    for text in upstream:
        corpus |= values(text)
    # Deduplicated on the PHRASE: both ends of an ungrounded range resolve to the same
    # phrase, and naming it twice reads like two findings.
    flagged: list[str] = []
    for value, phrase in figures(output).items():
        if value not in corpus and phrase not in flagged:
            flagged.append(phrase)
    return flagged
