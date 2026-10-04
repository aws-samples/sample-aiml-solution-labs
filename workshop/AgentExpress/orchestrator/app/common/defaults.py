"""Every workflow.json key's DEFAULT, read from app/defaults.json.

WHY THIS IS A FILE AND NOT A PYTHON CONSTANT — the same argument as
app/common/vocabulary.py, and it had already gone wrong the same way. A default is a
decision about what an omitted key means, and it was being written out once per plane:
`runtime: "main"` appeared FIFTEEN times across Python, HCL and TypeScript,
`arg: "query"` three times, `corpusKey: "doc_type"` three times. Worse, `maxResults`
carried BOTH 5 and 10 inside terraform/tools.tf — the first applied to every tool
including the Knowledge Base one, the second re-read the kb spec — and only the second
reached the KB Lambda, so the file disagreed with itself about its own default.

A drifted default is quieter than a drifted allow-list. A missing VALUE is rejected at
plan or synth with a message; a default that differs by plane deploys cleanly on both
and behaves differently, and the difference is whatever the key controls. That is
indistinguishable from the feature working.

So the values are authored once in app/keys.json, beside the key's documentation and its
allowed values, and `build_schema.py` projects them into app/defaults.json, which every
plane reads: this module, cdk/lib/defaults.ts, and `local.key_defaults` in terraform/.
keys.json itself is 40 KB of prose and deliberately does not ship in the container image
(see .dockerignore), which is exactly why the generated projection exists.

This module is separate from `config.py` for the same reason vocabulary.py is: config.py
holds the CUSTOMER'S workflow, this holds the FRAMEWORK's answer for a key they left out.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

#: Beside app/workflow.json and app/vocabulary.json so `COPY app ./app` picks it up and
#: both IaC paths can stage it the same way they already stage the vocabulary.
_PATH = Path(__file__).resolve().parent.parent / "defaults.json"


def _load() -> dict[str, Any]:
    raw = json.loads(_PATH.read_text())
    return {k: v for k, v in raw.items() if not k.startswith("$")}


_DEFAULTS = _load()
#: Defaults that depend on a tool's `type`. Held apart from the rest so `get` cannot
#: silently return one variant's value for another — see `for_type`.
_PER_TYPE: dict[str, Any] = _DEFAULTS.pop("perType", {})


def get(block: str, key: str) -> Any:
    """The declared default for `<block>.<key>`.

    RAISES when the key declares none, rather than returning None. A caller asking for a
    default has already decided the key is optional; if keys.json disagrees, that is a
    mismatch between the code and the spec, and `None` would be applied as though it were
    the intended value. The same argument as `vocabulary.values`.
    """
    try:
        return _DEFAULTS[block][key]
    except KeyError:
        declared = ", ".join(sorted(f"{b}.{k}" for b, ks in _DEFAULTS.items() for k in ks))
        raise KeyError(
            f"{block}.{key} declares no `default` in app/keys.json. Add one there and "
            f"re-run `python3 build_schema.py`; do not hardcode it here. Declared: "
            f"{declared}") from None


def for_type(block: str, key: str, variant: str) -> Any:
    """The default for `<block>.<key>` under a specific variant (a tool's `type`).

    Separate from `get` because the caller MUST say which variant it means. `maxResults`
    has no single right answer — it is retrieval depth for a `kb` and page size for a
    `websearch`, 5 and 10 — so a lookup that picked one would hand a kb tool websearch's
    page size and retrieve ten chunks where the framework documents five. That is the
    defect this whole module exists to remove, so it is not reintroduced as a convenience.
    """
    try:
        return _PER_TYPE[block][key][variant]
    except KeyError:
        known = ", ".join(sorted(_PER_TYPE.get(block, {}).get(key) or {}))
        raise KeyError(
            f"{block}.{key} declares no default for {variant!r} in app/keys.json "
            f"(`defaultFor`). Declared variants: {known or '(none)'}") from None
