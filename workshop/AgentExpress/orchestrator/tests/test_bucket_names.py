"""Bucket names in both schemes (bff/bucket_names.py), held to the vectors in
tests/fixtures/bucket_names.json, which cdk/test/bucket-names.test.ts holds lib/bucket-names.ts
to and terraform/tests/bucket_names.tftest.hcl holds terraform/bucket_names.tf to.

"legacy" is every deployment's name before (the default on both paths, so upgrading renames
no bucket: CloudFormation and Terraform REPLACE a renamed bucket). "regional" ends each name
with the Region, so a build destroyed in one Region can be deployed in another straight away
while S3 still holds the old names; a name over 63 characters keeps its prefix, kind, account
and Region, and cuts the agent name, with a digest of it.
"""
from __future__ import annotations

import json
import re
import shutil
import subprocess

import bucket_names as bn
import pytest
from conftest import ORCH_ROOT

VECTORS = json.loads((ORCH_ROOT / "tests" / "fixtures" / "bucket_names.json").read_text())
TF = ORCH_ROOT / "terraform"
TFTEST = (TF / "tests" / "bucket_names.tftest.hcl").read_text()
TERRAFORM = shutil.which("terraform")


def test_the_python_names_are_the_vectors():
    assert len(VECTORS) > 200
    for v in VECTORS:
        tool = "terraform" if v["tool"] == "terraform" else "cdk"
        assert bn.name(v["kind"], v["agent"], v["account"], v["region"], v["scheme"], tool) == v["name"], v


def test_every_regional_name_is_valid_and_ends_with_its_kind_account_and_region():
    for v in VECTORS:
        n = v["name"]
        assert re.fullmatch(r"[a-z0-9][a-z0-9-]*[a-z0-9]", n) or v["scheme"] == "legacy", n
        if v["scheme"] == "regional":
            assert len(n) <= 63 and n.endswith(f"-{v['kind']}-{v['account']}-{v['region']}"), n


def test_long_agent_names_that_begin_alike_get_different_names():
    a = bn.name("schemas", "a" * 40 + "_one", "123456789012", "ap-southeast-3", "regional")
    b = bn.name("schemas", "a" * 40 + "_two", "123456789012", "ap-southeast-3", "regional")
    assert a != b and len(a) == len(b) == 63


def test_a_name_s3_refuses_is_refused_and_all_names_covers_both_schemes():
    with pytest.raises(bn.BucketNameError, match="reserves"):
        bn.name("storage", "xn__a", "123456789012", "us-east-1", "regional")
    with pytest.raises(bn.BucketNameError, match="bucketNaming"):
        bn.name("assets", "a", "123456789012", "us-east-1", "global")
    names = bn.all_names("ax_1a2b3c4d", "123456789012", "us-east-1")
    assert {"agentcore-ax-1a2b3c4d-assets-123456789012", "agentcore-ax-1a2b3c4d-assets-123456789012-us-east-1",
            "agentcore-ax-1a2b3c4d-ui-123456789012", "agentcore-ax-1a2b3c4d-ui-123456789012-us-east-1",
            "ax-1a2b3c4d-storage-123456789012", "ax-1a2b3c4d-storage-123456789012-us-east-1"} <= names


def _tftest_cases() -> list[tuple[dict, dict]]:
    """Each run of the tftest that asserts a whole name map: its variables and the map."""
    out = []
    for block in re.split(r'\nrun "', TFTEST)[1:]:
        m = re.search(r"local\.bucket_names == \{(.*?)\n    \}", block, re.DOTALL)
        if not m:
            continue
        names = dict(re.findall(r'^\s*"?([a-z-]+)"?\s*=\s*"([^"]+)"', m.group(1), re.MULTILINE))
        var = dict(re.findall(r'^\s*(agent_name|region|bucket_naming)\s*=\s*"([^"]+)"',
                              block.split("assert", 1)[0], re.MULTILINE))
        out.append(({"region": "us-east-1", "bucket_naming": "legacy", **var}, names))
    return out


def test_the_terraform_tests_expect_the_vectors_names():
    cases = _tftest_cases()
    assert len(cases) == 4
    for var, names in cases:
        assert set(names) == {"assets", "kbdocs", "schemas", "storage", "ui", "ui-logs"}
        for kind, name in names.items():
            want = [v["name"] for v in VECTORS if v["kind"] == kind and v["agent"] == var["agent_name"]
                    and v["region"] == var["region"] and v["scheme"] == var["bucket_naming"]
                    and v["tool"] in ("any", "terraform")]
            assert want == [name], (var, kind)


@pytest.mark.skipif(not TERRAFORM or not (TF / ".terraform").exists(),
                    reason="terraform, initialised, is needed for the offline plan")
def test_terraform_names_its_buckets_as_the_vectors_do():
    r = subprocess.run(  # noqa: S603 - fixed argv, no shell  # nosec B603  # nosemgrep
        [TERRAFORM, "test", "-no-color", "-filter=tests/bucket_names.tftest.hcl"], cwd=TF,
        capture_output=True, text=True, timeout=600, check=False)
    assert r.returncode == 0 and "Success! 5 passed, 0 failed" in r.stdout, r.stdout[-4000:] + r.stderr[-2000:]
