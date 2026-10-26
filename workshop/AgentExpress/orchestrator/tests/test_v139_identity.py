"""v1.3.9's four new features are OFF unless configured: an existing workflow.json deploys
byte-identically to v1.3.8 (7ae5e19), on both deploy paths.

The four: build storage (orchestrator.storage), the Document generator (a lambda tool with
source "docgen"), the deliver step (task.kind "deliver") and named gate options
(steps[].hitl.options, agents.<id>.askPerson.options). A workflow that uses none of them
must plan and synthesize exactly what v1.3.8 did.

  * Terraform (here): the whole plan of each case below, planned offline with every provider
    mocked (tests/fixtures/tf_identity.tftest.hcl), must equal, line for line, the plan v1.3.8
    made of the same case (tests/fixtures/tf-identity/<case>.txt). Hashes are masked: a
    digest of the app's source files (an image tag, a UI build trigger) moves with any change
    to the app and is not part of what a workflow deploys. The console case also leaves out
    the Builder deploy role's managed policies (deploy-role-policy.json): letting the console
    create a build's storage is the one intended console change.
  * CDK: cdk/test/storage-identity.test.ts does the same with the synthesized templates
    (cdk/test/fixtures/storage-off/), for the same cases and more.

Regenerate the plans only from a checkout of a release, never to make this pass:
  AX_TF_IDENTITY_ROOT=<release checkout>/orchestrator AX_WRITE_TF_IDENTITY=1 \\
    python -m pytest tests/test_v139_identity.py


(the release checkout's terraform/ needs `terraform init -backend=false`, or a link to an
initialised .terraform/ with the same lock file).
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
from conftest import ORCH_ROOT

TERRAFORM = shutil.which("terraform")
FIXTURES = ORCH_ROOT / "tests" / "fixtures"
GOLDEN = FIXTURES / "tf-identity"
#: The tree to plan: this one, or a release checkout when writing the plans.
ROOT = Path(os.environ.get("AX_TF_IDENTITY_ROOT") or ORCH_ROOT)
WRITE = os.environ.get("AX_WRITE_TF_IDENTITY") == "1"
needs_tf = pytest.mark.skipif(not TERRAFORM or not (ROOT / "terraform" / ".terraform").exists(),
                              reason="terraform, initialised, is needed for the offline plan")


#: The resources the UI's files are (their for_each is known only after the UI build runs).
UNPLANNABLE = {"aws_s3_object.ui_asset"}
#: Resources the console case leaves out: the Builder deploy role's managed policies.
CONSOLE_ONLY_CHANGE = re.compile(r"^aws_iam_policy\.deploy\[")
CASES = {
    # The shipped workflow, as a build (what deployer/runner.py plans) and as the console.
    "build": {"enable_builder": False, "workflow": None},
    "console": {"enable_builder": True, "workflow": None},
    # A build that keeps its run files (what the Document generator and deliver step need).
    "build-files": {"enable_builder": False, "workflow": lambda wf: {
        **wf, "orchestrator": {**wf.get("orchestrator", {}), "files": {}}}},
}


def _targets(tf: Path) -> list[str]:
    found = []
    for f in sorted(tf.glob("*.tf")):
        found += [f"{t}.{n}" for t, n in re.findall(r'^resource\s+"([^"]+)"\s+"([^"]+)"', f.read_text(), re.MULTILINE)]
    return [a for a in found if a not in UNPLANNABLE]


def _copy(tmp: Path, case: dict) -> Path:
    """A copy of ROOT whose terraform/ holds only the identity test, and whose app/ has the
    case's workflow.json; everything else is linked (as tests/test_deliver_tf.py does)."""
    root = tmp / "orchestrator"
    root.mkdir()
    for entry in ROOT.iterdir():
        if entry.name not in ("app", "terraform"):
            (root / entry.name).symlink_to(entry)
    if case["workflow"]:
        shutil.copytree(ROOT / "app", root / "app", ignore=shutil.ignore_patterns("__pycache__"))
        wf_path = root / "app" / "workflow.json"
        wf_path.write_text(json.dumps(case["workflow"](json.loads(wf_path.read_text())), indent=2) + "\n")
    else:
        (root / "app").symlink_to(ROOT / "app")
    src, tf = ROOT / "terraform", root / "terraform"
    tf.mkdir()
    for entry in src.iterdir():
        if entry.name not in (".terraform.lock.hcl", "tests"):
            (tf / entry.name).symlink_to(entry)
    shutil.copy(src / ".terraform.lock.hcl", tf / ".terraform.lock.hcl")
    (tf / "tests").mkdir()
    test = (FIXTURES / "tf_identity.tftest.hcl").read_text()
    test = test.replace("  ENABLE_BUILDER", f"  enable_builder = {str(case['enable_builder']).lower()}")
    test = test.replace("      TARGETS", ",\n".join(f"      {a}" for a in _targets(src)))
    (tf / "tests" / "identity.tftest.hcl").write_text(test)
    return tf


_HASH = re.compile(r"\b[0-9a-f]{64}\b|\b[0-9a-f]{40}\b|\b[0-9a-f]{32}\b|[A-Za-z0-9+/]{43}=")
#: The release tag in the default docs URL (docs_default.json `ref`): every release moves it.
_DOCS_REF = re.compile(r"agentexpress-v\d+\.\d+\.\d+")


def _resources(plan: str) -> dict[str, str]:
    """The plan's resource blocks by address, hashes and the docs release tag masked."""
    out: dict[str, str] = {}
    assert "Terraform will perform the following actions:" in plan, plan[-2000:]
    plan = plan.split("Terraform will perform the following actions:", 1)[1].split("\nPlan: ", 1)[0]
    for block in re.split(r"\n(?=  # )", plan):
        m = re.match(r"  # (\S+)", block)
        if m and not m.group(1).startswith("("):
            out[m.group(1)] = _DOCS_REF.sub("<docs-ref>", _HASH.sub("<hash>", block.rstrip()))
    return out


def _plan(tmp: Path, name: str) -> str:
    tf = _copy(tmp, CASES[name])
    r = subprocess.run(  # noqa: S603 - fixed argv, no shell  # nosec B603  # nosemgrep
        [TERRAFORM, "test", "-no-color", "-verbose", "-filter=tests/identity.tftest.hcl"], cwd=tf,
        capture_output=True, text=True, timeout=900, check=False)
    assert r.returncode == 0 and "Success! 1 passed, 0 failed" in r.stdout, r.stdout[-4000:] + r.stderr[-2000:]
    blocks = _resources(r.stdout)
    if name == "console":
        blocks = {a: b for a, b in blocks.items() if not CONSOLE_ONLY_CHANGE.match(a)}
    return "\n".join(blocks[a] for a in sorted(blocks)) + "\n"


@needs_tf
@pytest.mark.parametrize("name", sorted(CASES))
def test_an_existing_workflow_plans_byte_identically_to_v138(tmp_path, name):
    got = _plan(tmp_path, name)
    file = GOLDEN / f"{name}.txt"
    if WRITE:
        GOLDEN.mkdir(exist_ok=True)
        file.write_text(got)
        return
    want = _DOCS_REF.sub("<docs-ref>", file.read_text())
    assert got.count("\n  # ") == want.count("\n  # "), "a resource was added or removed"
    assert got == want
