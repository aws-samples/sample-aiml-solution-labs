"""Deliver steps in Terraform (v139 Track C deliver; terraform/deliver.tf), planned offline
with every provider mocked: no credentials, no AWS call.

  * terraform/tests/deliver.tftest.hcl, on app/workflow.json (no deliver step): no deliver
    policy, no deliver check, and nothing merged into the runtime secret, so the build is
    what it was;
  * tests/fixtures/deliver_on.tftest.hcl, on a copy whose workflow.json has a deliver step
    (s3, email and a signed webhook target) and keeps run files: the four statements, each
    scoped to the run's files, the targets' prefixes and the SES identity with its From
    address; the named signing secret, and only it, for the runtime secret; and a plan that
    fails when the build lacks that secret.
Same as cdk/test/deliver.test.ts.
"""
from __future__ import annotations

import json
import re
import shutil
import subprocess

import pytest
from conftest import ORCH_ROOT

TF = ORCH_ROOT / "terraform"
TERRAFORM = shutil.which("terraform")
needs_tf = pytest.mark.skipif(not TERRAFORM or not (TF / ".terraform").exists(),
                              reason="terraform, initialised, is needed for the offline plan")


def _test(cwd, name: str) -> subprocess.CompletedProcess:
    return subprocess.run(  # noqa: S603 - fixed argv, no shell  # nosec B603  # nosemgrep
        [TERRAFORM, "test", "-no-color", f"-filter=tests/{name}"], cwd=cwd, capture_output=True,
        text=True, timeout=600, check=False)


@needs_tf
def test_a_build_without_a_deliver_step_plans_nothing_new():
    r = _test(TF, "deliver.tftest.hcl")
    assert r.returncode == 0, r.stdout[-4000:] + r.stderr[-2000:]
    assert re.search(r"Success! 1 passed, 0 failed", r.stdout), r.stdout[-2000:]


@needs_tf
def test_a_build_with_a_deliver_step_gets_only_what_its_targets_need(tmp_path):
    root = tmp_path / "orchestrator"
    root.mkdir()
    # Everything the .tf files read through ../, as links; app/ copied so its workflow.json
    # can gain the step; terraform/ its own directory (path.module is the copy) with only
    # this test in tests/.
    for entry in ORCH_ROOT.iterdir():
        if entry.name not in ("app", "terraform"):
            (root / entry.name).symlink_to(entry)
    shutil.copytree(ORCH_ROOT / "app", root / "app", ignore=shutil.ignore_patterns("__pycache__"))
    wf_path = root / "app" / "workflow.json"
    wf = json.loads(wf_path.read_text())
    wf.setdefault("orchestrator", {})["files"] = {}
    wf["agents"]["send"] = {"name": "Send", "runtime": "task", "task": {"kind": "deliver", "targets": [
        {"type": "s3", "uri": "s3://amzn-s3-demo-bucket/reports/", "files": ["*.pdf"]},
        {"type": "s3", "uri": "s3://amzn-s3-demo-bucket/"},
        {"type": "email", "identityArn": "arn:aws:ses:us-east-1:123456789012:identity/example.com",
         "fromAddress": "reports@example.com", "to": ["team@example.com"]},
        {"type": "webhook", "url": "https://hooks.example.com/in", "secret": "hook"},
    ]}}
    wf["steps"].append({"agent": "send"})
    wf_path.write_text(json.dumps(wf))
    tf = root / "terraform"
    tf.mkdir()
    for entry in TF.iterdir():
        if entry.name not in (".terraform.lock.hcl", "tests"):
            (tf / entry.name).symlink_to(entry)
    shutil.copy(TF / ".terraform.lock.hcl", tf / ".terraform.lock.hcl")
    (tf / "tests").mkdir()
    shutil.copy(ORCH_ROOT / "tests" / "fixtures" / "deliver_on.tftest.hcl", tf / "tests" / "deliver_on.tftest.hcl")
    r = _test(tf, "deliver_on.tftest.hcl")
    assert r.returncode == 0, r.stdout[-6000:] + r.stderr[-2000:]
    assert re.search(r"Success! 2 passed, 0 failed", r.stdout), r.stdout[-2000:]
