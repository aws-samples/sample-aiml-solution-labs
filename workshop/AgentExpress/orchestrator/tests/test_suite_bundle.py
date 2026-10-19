"""A build's test suites travel in its bundle without changing what a deploy does.

The bundle is what the Builder exports, what a deploy freezes as a version, and what
`scaffold.py apply` reads. Suites are in it so a build keeps its tests when it is exported,
imported or versioned; `apply` must neither refuse them nor write them into the tree, and a
bundle from before there were suites must still apply exactly as it did.
"""
from __future__ import annotations

import json
import sys

from conftest import ORCH_ROOT
from test_scaffold_apply import bundle, claims_workflow, run
from test_scaffold_apply import tree as tree

sys.path.insert(0, str(ORCH_ROOT / "bff"))
import buildstore

SUITES = [{"name": "Triage", "cases": [
    {"name": "urgent", "request": "Claim 1", "expect": [{"agent": "claims_intake", "field": "riskScore", "gte": 80}]}]}]
PROMPTS = {"claims_intake": {"systemPrompt": "You triage claims.", "schema": "{}"}}


def stored(**over):
    return {"id": "pclaims01", "name": "Claims", "workflow": claims_workflow(), "prompts": PROMPTS, **over}


def test_a_version_carries_the_suites_the_build_has():
    got = buildstore.bundle_of(stored(suites=SUITES), 3)
    assert got["suites"] == SUITES
    assert got["project"]["buildVersion"] == 3


def test_a_build_with_no_suites_has_the_bundle_it_always_had():
    for project in (stored(), stored(suites=[]), stored(suites="nonsense"), stored(suites=None)):
        assert "suites" not in buildstore.bundle_of(project, 1)
    keys = {"format", "version", "framework", "project", "workflow", "prompts"}
    assert set(buildstore.bundle_of(stored(), 1)) == keys


def test_apply_reads_a_bundle_with_suites_and_writes_nothing_of_them(tree, tmp_path):
    path = bundle(tmp_path, claims_workflow(), PROMPTS, suites=SUITES)
    result = run(tree, str(path))
    assert result.returncode == 0, result.stderr
    written = {p.relative_to(tree).as_posix(): p.read_text(errors="replace")
               for p in tree.rglob("*") if p.is_file() and "__pycache__" not in p.parts}
    assert [name for name, text in written.items() if "urgent" in text and "riskScore" in text] == []
    applied = json.loads((tree / "app" / "workflow.json").read_text())
    assert applied["agents"]["claims_intake"]["name"] == "Claims Intake"


def test_apply_does_the_same_with_and_without_suites(tree, tmp_path):
    without = run(tree, str(bundle(tmp_path, claims_workflow(), PROMPTS)), "--dry-run")
    given = bundle(tmp_path, claims_workflow(), PROMPTS, suites=SUITES)
    with_suites = run(tree, str(given), "--dry-run")
    assert without.returncode == with_suites.returncode == 0
    assert without.stdout == with_suites.stdout and without.stderr == with_suites.stderr


def test_exact_apply_accepts_suites_too(tree, tmp_path):
    path = bundle(tmp_path, claims_workflow(), PROMPTS, suites=SUITES)
    assert run(tree, str(path), "--exact").returncode == 0
    assert run(tree, str(path), "--exact", "--dry-run").returncode == 0
