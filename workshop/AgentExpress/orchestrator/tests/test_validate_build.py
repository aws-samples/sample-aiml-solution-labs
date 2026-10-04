"""bff/validate_build.py is the page's validator on the server. Hold it to the page.

The Build view marks mistakes with web/src/builder/validate.ts as you type; the BFF
refuses a deploy with bff/validate_build.py, a port of it. A user who never opens the
page — or calls the API directly — must be refused for exactly what the page would
have flagged, in the same words. So both run tests/fixtures/validation_cases.json and
must produce each case's `expect` issue for issue (vitest: validate.parity.test.ts).
"""

from __future__ import annotations

import copy
import json
import sys

import pytest
from conftest import ORCH_ROOT

sys.path.insert(0, str(ORCH_ROOT / "bff"))
import validate_build

FIXTURE = json.loads((ORCH_ROOT / "tests" / "fixtures" / "validation_cases.json").read_text())


def _apply(base: dict, ops: list) -> dict:
    """A case's edits on a copy of the base. Mirrors `apply` in validate.parity.test.ts."""
    wf = copy.deepcopy(base)
    for op in ops:
        path = op[1]
        at = wf
        for k in path[:-1]:
            at = at[k]
        if op[0] == "set":
            at[path[-1]] = copy.deepcopy(op[2])
        else:
            del at[path[-1]]
    return wf


@pytest.mark.parametrize("case", FIXTURE["cases"], ids=[c["name"] for c in FIXTURE["cases"]])
def test_the_server_validator_agrees_with_the_page(case):
    assert validate_build.validate(_apply(FIXTURE["base"], case["ops"])) == case["expect"]


def test_the_shipped_workflow_has_no_errors():
    wf = json.loads((ORCH_ROOT / "app" / "workflow.json").read_text())
    assert validate_build.errors(wf) == []


def test_it_reads_the_same_spec_the_page_is_generated_from():
    meta = json.loads((ORCH_ROOT / "web" / "src" / "generated" / "builder-meta.json").read_text())
    keys = {k: v for k, v in meta["keys"].items() if not k.startswith("$")}
    assert keys == validate_build.KEYS
    assert meta["vocabulary"] == validate_build.VOCAB
