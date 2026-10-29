"""Every presigned S3 URL and POST a browser opens is signed for the bucket's REGIONAL host.
Live: uploads failed with "Failed to fetch" outside us-east-1. A plain
boto3.client("s3", region_name=r) presigns for the global host <bucket>.s3.amazonaws.com; for a
new bucket outside us-east-1, S3 answers a POST there with a 307 to the regional host, which a
browser does not follow for a cross-origin form POST. bff/s3_presign.py (and its copy
app/common/s3_presign.py) signs with SigV4 for <bucket>.s3.<region>.amazonaws.com.
botocore presigns offline: fake credentials, no AWS call.
"""
from __future__ import annotations

import ast
import importlib.util
import sys
import urllib.parse
from pathlib import Path

import pytest
from conftest import ORCH_ROOT


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def presign(monkeypatch):
    for k, v in {"AWS_ACCESS_KEY_ID": "AKIAEXAMPLE", "AWS_SECRET_ACCESS_KEY": "secret",
                 "AWS_SESSION_TOKEN": "token", "AWS_CONFIG_FILE": "/dev/null",
                 "AWS_SHARED_CREDENTIALS_FILE": "/dev/null"}.items():
        monkeypatch.setenv(k, v)
    for k in ("AWS_PROFILE", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3"):
        monkeypatch.delenv(k, raising=False)
    # boto3's default session caches the credentials it first resolved; an earlier test can
    # leave one holding moto's (no session token). Start from none so the env above is used.
    import boto3
    monkeypatch.setattr(boto3, "DEFAULT_SESSION", None)
    mod = _load(ORCH_ROOT / "bff" / "s3_presign.py", "_s3_presign_under_test")
    yield mod


def _host(url: str) -> str:
    return urllib.parse.urlsplit(url).netloc


@pytest.mark.parametrize("region,suffix", [("us-west-2", "amazonaws.com"), ("us-east-1", "amazonaws.com"),
                                           ("ap-southeast-3", "amazonaws.com"),
                                           ("cn-north-1", "amazonaws.com.cn")])
def test_a_presigned_get_and_post_name_the_buckets_regional_host(presign, region, suffix):
    c = presign.client(region)
    get = c.generate_presigned_url("get_object", Params={"Bucket": "my-bucket", "Key": "runs/a/b.png"},
                                   ExpiresIn=60)
    assert _host(get) == f"my-bucket.s3.{region}.{suffix}"
    q = urllib.parse.parse_qs(urllib.parse.urlsplit(get).query)
    assert q["X-Amz-Algorithm"] == ["AWS4-HMAC-SHA256"]
    assert q["X-Amz-Credential"][0].split("/")[2] == region       # signed for that Region
    post = c.generate_presigned_post(Bucket="my-bucket", Key="uploads/x/a.pdf", ExpiresIn=60,
                                     Conditions=[["content-length-range", 1, 10]])
    assert _host(post["url"]) == f"my-bucket.s3.{region}.{suffix}"
    assert post["fields"]["x-amz-algorithm"] == "AWS4-HMAC-SHA256"
    assert post["fields"]["x-amz-credential"].split("/")[2] == region
    assert "x-amz-security-token" in post["fields"]


def test_the_region_defaults_to_aws_region_and_each_region_has_its_own_client(presign, monkeypatch):
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    url = presign.client().generate_presigned_url("get_object", Params={"Bucket": "b-1", "Key": "k"})
    assert _host(url) == "b-1.s3.eu-west-1.amazonaws.com"
    assert presign.client("us-west-2") is presign.client("us-west-2") is not presign.client("eu-west-1")


def _tree(path: Path) -> str:
    tree = ast.parse(path.read_text())
    if tree.body and isinstance(tree.body[0], ast.Expr) and isinstance(tree.body[0].value, ast.Constant):
        tree.body = tree.body[1:]
    return ast.dump(tree)


def test_the_bff_copy_is_the_original():
    """The BFF ships as its own zip and cannot import app.*, so bff/s3_presign.py is a copy,
    held to the original as a syntax tree (module docstring aside)."""
    assert _tree(ORCH_ROOT / "bff" / "s3_presign.py") == _tree(ORCH_ROOT / "app" / "common" / "s3_presign.py")


#: Presigning made with a client that is not s3_presign.client(...). The Document generator
#: ships as its own zip: its one client is configured the same way inline.
EXEMPT = {("app/tools/docgen/handler.py", "_s3")}


def _presign_calls():
    for root in ("bff", "app"):
        for path in sorted((ORCH_ROOT / root).rglob("*.py")):
            rel = str(path.relative_to(ORCH_ROOT))
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
                        and node.func.attr.startswith("generate_presigned"):
                    yield rel, node.func.value, node.lineno


def test_every_presigned_url_is_made_with_the_regional_client():
    found, wrong = 0, []
    for rel, on, line in _presign_calls():
        found += 1
        ok = (isinstance(on, ast.Call) and isinstance(on.func, ast.Attribute) and on.func.attr == "client"
              and isinstance(on.func.value, ast.Name) and on.func.value.id == "s3_presign") \
            or (isinstance(on, ast.Name) and (rel, on.id) in EXEMPT)
        if not ok:
            wrong.append(f"{rel}:{line}")
    assert found >= 7 and not wrong, wrong


def test_the_document_generators_own_client_is_configured_the_same_way():
    src = (ORCH_ROOT / "app" / "tools" / "docgen" / "handler.py").read_text()
    assert 'signature_version="s3v4"' in src
    assert 's3={"addressing_style": "virtual", "us_east_1_regional_endpoint": "regional"}' in src


# --- each call site, from the BFF modules themselves ------------------------------------
@pytest.fixture()
def bff(presign, monkeypatch):
    """bff/s3_presign under its real name, so the BFF modules use it, in us-west-2."""
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.setitem(sys.modules, "s3_presign", presign)
    return presign


def test_a_run_upload_is_posted_to_the_regional_host(bff, monkeypatch):
    sys.modules.pop("runfiles", None)
    import runfiles
    monkeypatch.setattr(runfiles, "REGION", "us-west-2")
    monkeypatch.setattr(runfiles, "_need_bucket", lambda: "agentcore-x-assets-123456789012-us-west-2")
    post = runfiles.upload("owner-1", "brief.pdf")
    assert _host(post["url"]) == "agentcore-x-assets-123456789012-us-west-2.s3.us-west-2.amazonaws.com"
    assert post["fields"]["key"] == post["key"]


def test_a_run_files_link_is_signed_for_the_regional_host(bff, monkeypatch):
    sys.modules.pop("runartifacts", None)
    import runartifacts
    monkeypatch.setattr(runartifacts, "REGION", "us-west-2")
    out = runartifacts.link("sess-123456", "runs/sess-123456/outputs/a.pdf", bucket="b-assets")
    assert _host(out["url"]) == "b-assets.s3.us-west-2.amazonaws.com"


def test_a_connect_templates_link_is_signed_for_the_consoles_region(bff, monkeypatch):
    """The template is in the console's builds bucket, so its link is signed for the console's
    Region; the quick-create link opens the connection's Region."""
    sys.modules.pop("accounts", None)
    import accounts
    monkeypatch.setattr(accounts, "REGION", "us-west-2")
    out = accounts._launch({"externalId": "e" * 32, "region": "eu-central-1", "stackName": "S",
                            "accountId": "123456789012"}, "agentcore-builds-1")
    assert _host(out["templateUrl"]) == "agentcore-builds-1.s3.us-west-2.amazonaws.com"
    assert out["launchUrl"].startswith("https://eu-central-1.console.aws.amazon.com/")
