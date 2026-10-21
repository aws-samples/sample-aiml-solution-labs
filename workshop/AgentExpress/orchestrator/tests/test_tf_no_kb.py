"""A Terraform build with no Knowledge Base validates (terraform/kb.tf).

Terraform runs the provider's validators on a resource's configuration even when it has
count = 0, using every value it can already compute. With no tools in workflow.json the
Knowledge Base is off at validate time, so a value that is valid only with it on (type
S3_VECTORS with no s3_vectors_configuration, a bucket ARN of "arn:aws:s3:::") fails the
build's first `terraform apply` with "Invalid Attribute Combination".

terraform/tests/knowledge_base.tftest.hcl plans both cases with mocked providers, but it
reads app/workflow.json, which declares tools: there the Knowledge Base depends on
var.enable_gateway, which validation treats as unknown, so the validators skip it. This
test validates a copy whose workflow.json has no tools. Offline: `terraform validate`
makes no AWS call.
"""
from __future__ import annotations

import json
import shutil
import subprocess

import pytest
from conftest import ORCH_ROOT

TF = ORCH_ROOT / "terraform"
TERRAFORM = shutil.which("terraform")


@pytest.mark.skipif(not TERRAFORM or not (TF / ".terraform").exists(),
                    reason="terraform, initialised, is needed for the offline validate")
def test_a_build_with_no_tools_validates(tmp_path):
    root = tmp_path / "orchestrator"
    root.mkdir()
    # Everything the .tf files read through ../, as links; app/ copied so its workflow.json
    # can lose its tools; terraform/ its own directory so path.module is the copy.
    for entry in ORCH_ROOT.iterdir():
        if entry.name not in ("app", "terraform"):
            (root / entry.name).symlink_to(entry)
    shutil.copytree(ORCH_ROOT / "app", root / "app")
    wf_path = root / "app" / "workflow.json"
    wf = json.loads(wf_path.read_text())
    wf["tools"] = {}
    wf_path.write_text(json.dumps(wf))
    tf = root / "terraform"
    tf.mkdir()
    for entry in TF.iterdir():
        if entry.name != ".terraform.lock.hcl":
            (tf / entry.name).symlink_to(entry)
    shutil.copy(TF / ".terraform.lock.hcl", tf / ".terraform.lock.hcl")

    r = subprocess.run(  # noqa: S603 - fixed argv, no shell  # nosec B603  # nosemgrep
        [TERRAFORM, "validate", "-no-color"], cwd=tf, capture_output=True, text=True, timeout=600, check=False)
    assert r.returncode == 0, r.stdout[-4000:] + r.stderr[-4000:]
