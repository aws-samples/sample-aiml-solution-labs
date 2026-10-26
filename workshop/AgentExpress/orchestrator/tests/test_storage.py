"""Build storage (workflow.json `orchestrator.storage`, the `${storage}` token):

  * the server validator gives the page's issues for tests/fixtures/storage_cases.json
    (vitest: web/src/builder/storage.parity.test.ts), and the shipped workflow sets none;
  * a step resolves the token at run time from STORAGE_BUCKET (app/common/storage.py);
  * a Terraform destroy keeps a retained bucket by taking it out of the state first
    (deployer/runner.py, terraform/teardown.sh);
  * Terraform, offline with every provider mocked, plans nothing without it
    (terraform/tests/storage.tftest.hcl, run by tests/test_hardening.py) and, on a copy whose
    workflow sets it, the bucket with its settings and the resolved token
    (tests/fixtures/storage_on.tftest.hcl), with and without retain.
The CDK half is cdk/test/storage.test.ts and cdk/test/storage-identity.test.ts.
"""
from __future__ import annotations

import copy
import json
import re
import shutil
import subprocess
import sys

import pytest
from conftest import ORCH_ROOT

sys.path.insert(0, str(ORCH_ROOT / "bff"))
import validate_build

FIXTURES = ORCH_ROOT / "tests" / "fixtures"
BASE = json.loads((FIXTURES / "validation_cases.json").read_text())["base"]
CASES = json.loads((FIXTURES / "storage_cases.json").read_text())["cases"]
TOKEN = json.loads((ORCH_ROOT / "app" / "vocabulary.json").read_text())["storageToken"]["values"][0]
TF = ORCH_ROOT / "terraform"
TERRAFORM = shutil.which("terraform")


def _apply(ops: list) -> dict:
    wf = copy.deepcopy(BASE)
    for _, path, value in ops:
        at = wf
        for k in path[:-1]:
            at = at[k]
        at[path[-1]] = copy.deepcopy(value)
    return wf


@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
def test_the_server_validator_agrees_with_the_page_on_storage(case):
    got = [i for i in validate_build.validate(_apply(case["ops"]))
           if i["path"].startswith("tools.") or i["path"].startswith("orchestrator.")]
    assert got == case["expect"]


def test_the_shipped_workflow_creates_no_storage_and_uses_no_token():
    text = (ORCH_ROOT / "app" / "workflow.json").read_text()
    assert "storage" not in json.loads(text).get("orchestrator", {})
    assert TOKEN not in text


def test_the_token_is_the_one_every_plane_reads():
    assert TOKEN == "${storage}"  # nosec B105 - the build-storage placeholder, not a secret
    for f in ("cdk/lib/storage.ts", "web/src/builder/storageCheck.ts", "terraform/storage.tf", "app/common/storage.py"):
        assert "storageToken" in (ORCH_ROOT / f).read_text(), f


# --- at run time -------------------------------------------------------------------------

def test_a_step_resolves_the_token_from_the_runtime_environment(monkeypatch):
    from app.common import storage
    monkeypatch.setenv("STORAGE_BUCKET", "ax-1a2b3c4d-storage-123456789012")
    assert storage.resolve(f"{TOKEN}/out/") == "s3://ax-1a2b3c4d-storage-123456789012/out/"
    assert storage.resolve("s3://amzn-s3-demo-bucket/out/") == "s3://amzn-s3-demo-bucket/out/"
    monkeypatch.delenv("STORAGE_BUCKET")
    assert storage.resolve("s3://amzn-s3-demo-bucket/out/") == "s3://amzn-s3-demo-bucket/out/"
    with pytest.raises(storage.NoStorage, match=r"orchestrator\.storage\.create"):
        storage.resolve(f"{TOKEN}/out/")


# --- destroy -----------------------------------------------------------------------------

@pytest.fixture()
def runner():
    sys.path.insert(0, str(ORCH_ROOT / "deployer"))
    import runner as r
    return r


@pytest.mark.parametrize("storage,kept", [(None, False), ({"create": True}, False), ({"retain": True}, False),
                                          ({"create": True, "retain": False}, False), ({"create": True, "retain": True}, True)])
def test_only_a_created_and_retained_bucket_is_kept(runner, storage, kept):
    wf = {"orchestrator": {} if storage is None else {"storage": storage}}
    assert runner.storage_retained(wf) is kept


STATE = """aws_iam_role.runtime[0]
aws_s3_bucket.assets[0]
aws_s3_bucket.storage[0]
aws_s3_bucket_abac.storage[0]
aws_s3_bucket_lifecycle_configuration.storage[0]
aws_s3_bucket_policy.assets_tls[0]
aws_s3_bucket_policy.storage_tls[0]
aws_s3_bucket_public_access_block.storage[0]
aws_s3_bucket_server_side_encryption_configuration.storage[0]
aws_s3_bucket_versioning.storage[0]
terraform_data.storage_name[0]
"""


@pytest.mark.parametrize("retain", [False, True])
def test_a_terraform_destroy_takes_a_retained_bucket_out_of_the_state_first(runner, monkeypatch, tmp_path, retain):
    app = tmp_path / "app"
    app.mkdir()
    (app / "workflow.json").write_text(json.dumps({"orchestrator": {"storage": {"create": True, "retain": retain}}}))
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner.subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, 0, STATE, ""))
    ran: list = []
    monkeypatch.setattr(runner, "run", lambda cmd, cwd, env=None, keep=60: ran.append(cmd) or [])
    kept = runner.keep_retained_storage("terraform", tmp_path, {})
    if not retain:
        assert kept == [] and ran == []
        return
    assert kept == ["aws_s3_bucket.storage[0]", "aws_s3_bucket_abac.storage[0]",
                    "aws_s3_bucket_lifecycle_configuration.storage[0]", "aws_s3_bucket_policy.storage_tls[0]",
                    "aws_s3_bucket_public_access_block.storage[0]",
                    "aws_s3_bucket_server_side_encryption_configuration.storage[0]", "aws_s3_bucket_versioning.storage[0]"]
    assert ran == [["terraform", "state", "rm", *kept]]


def test_the_destroy_keeps_it_before_it_runs_on_both_terraform_paths():
    src = (ORCH_ROOT / "deployer" / "runner.py").read_text()
    body = src[src.index("def deploy_terraform("):]
    assert body.index("keep_retained_storage(tf, tf_dir, sub_env)") < body.index('destroy = [tf, "destroy"')
    teardown = (TF / "teardown.sh").read_text()
    assert teardown.index("terraform state rm") < teardown.index("terraform destroy -auto-approve")
    runner_re = re.search(r'STORAGE_ADDRESS = re\.compile\(r"(.*)"\)', src).group(1)
    assert runner_re in teardown


# --- Terraform, offline ------------------------------------------------------------------

def _tree_with(tmp_path, wf: dict, tftest: str):
    """A copy of the tree whose app/workflow.json is `wf`, with `tftest` its only test."""
    root = tmp_path / "orchestrator"
    root.mkdir()
    for entry in ORCH_ROOT.iterdir():
        if entry.name not in ("app", "terraform"):
            (root / entry.name).symlink_to(entry)
    shutil.copytree(ORCH_ROOT / "app", root / "app", ignore=shutil.ignore_patterns("__pycache__"))
    (root / "app" / "workflow.json").write_text(json.dumps(wf))
    tf = root / "terraform"
    tf.mkdir()
    for entry in TF.iterdir():
        if entry.name not in (".terraform.lock.hcl", "tests", ".build"):
            (tf / entry.name).symlink_to(entry)
    shutil.copy(TF / ".terraform.lock.hcl", tf / ".terraform.lock.hcl")
    (tf / "tests").mkdir()
    (tf / "tests" / "storage_on.tftest.hcl").write_text(tftest)
    return tf


def _with_storage(retain: bool) -> dict:
    wf = json.loads((ORCH_ROOT / "app" / "workflow.json").read_text())
    wf["orchestrator"]["storage"] = {"create": True, "retain": retain}
    kb = next(k for k, t in wf["tools"].items() if t.get("type") == "kb")
    wf["tools"][kb]["s3Uri"] = f"{TOKEN}/kb/"
    wf["tools"]["renderer"] = {
        "type": "lambda", "description": "Renders files, in code written here.",
        "toolSchema": [{"name": "render", "description": "Render a file.",
                        "properties": {"query": {"type": "string", "required": True}}}],
        "code": {"grants": {"s3Prefix": f"{TOKEN}/templates/", "s3Access": "readwrite"}}}
    return wf


@pytest.mark.skipif(not TERRAFORM or not (TF / ".terraform").exists(),
                    reason="terraform, initialised, is needed for the offline plan")
@pytest.mark.parametrize("retain", [False, True])
def test_terraform_plans_the_storage_and_resolves_the_token_offline(tmp_path, retain):
    assert validate_build.errors(_with_storage(retain)) == []
    text = (FIXTURES / "storage_on.tftest.hcl").read_text().replace("__RETAIN__", "true" if retain else "false")
    tf = _tree_with(tmp_path, _with_storage(retain), text)
    r = subprocess.run(  # noqa: S603 - fixed argv, no shell  # nosec B603  # nosemgrep
        [TERRAFORM, "test", "-no-color"], cwd=tf, capture_output=True, text=True, timeout=900, check=False)
    assert r.returncode == 0, r.stdout[-4000:] + r.stderr[-4000:]
    assert re.search(r"Success! 1 passed, 0 failed", r.stdout), r.stdout[-2000:]


@pytest.mark.skipif(not TERRAFORM or not (TF / ".terraform").exists(),
                    reason="terraform, initialised, is needed for the offline plan")
def test_terraform_plans_nothing_of_it_for_the_shipped_workflow_offline():
    r = subprocess.run(  # noqa: S603 - fixed argv, no shell  # nosec B603  # nosemgrep
        [TERRAFORM, "test", "-no-color", "-filter=tests/storage.tftest.hcl"], cwd=TF, capture_output=True,
        text=True, timeout=900, check=False)
    assert r.returncode == 0, r.stdout[-4000:] + r.stderr[-4000:]
    assert re.search(r"Success! 1 passed, 0 failed", r.stdout), r.stdout[-2000:]


@pytest.mark.skipif(not TERRAFORM or not (TF / ".terraform").exists(),
                    reason="terraform, initialised, is needed for the offline plan")
def test_terraform_refuses_the_token_without_storage_offline(tmp_path):
    wf = _with_storage(False)
    del wf["orchestrator"]["storage"]
    run = ('mock_provider "aws" {\n  override_during = plan\n  mock_data "aws_caller_identity" {\n'
           '    defaults = { account_id = "123456789012" }\n  }\n}\nmock_provider "awscc" {}\nmock_provider "null" {}\n'
           'mock_provider "external" {\n  mock_data "external" {\n    defaults = { result = {} }\n  }\n}\n'
           'mock_provider "archive" {}\nmock_provider "time" {}\nvariables {\n  region = "us-east-1"\n'
           '  enable_builder = false\n}\nrun "refused" {\n  command = plan\n  plan_options {\n'
           '    target = [terraform_data.storage_token]\n  }\n  expect_failures = [terraform_data.storage_token]\n}\n')
    tf = _tree_with(tmp_path, wf, run)
    r = subprocess.run(  # noqa: S603 - fixed argv, no shell  # nosec B603  # nosemgrep
        [TERRAFORM, "test", "-no-color"], cwd=tf, capture_output=True, text=True, timeout=900, check=False)
    assert r.returncode == 0, r.stdout[-4000:] + r.stderr[-4000:]
    assert re.search(r"Success! 1 passed, 0 failed", r.stdout), r.stdout[-2000:]
