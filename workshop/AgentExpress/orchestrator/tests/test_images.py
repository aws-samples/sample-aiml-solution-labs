"""Image agents: the brief is rendered by a Stability model in its own region, stored in
the deployment's assets bucket under the run it belongs to, and shown only to the run's
owner, through a short-lived link.
"""
# ruff: noqa: F811 - `env` is the builds fixture, imported so a test can take it
from __future__ import annotations

import base64
import io
import json
import sys
import urllib.parse

import boto3
import pytest
from test_builds import call, env, mark_deployed, save  # noqa: F401

moto = pytest.importorskip("moto")


class FakeBedrock:
    def __init__(self, out):
        self.out, self.calls = out, []

    def invoke_model(self, **kw):
        self.calls.append(kw)
        return {"body": io.BytesIO(json.dumps(self.out).encode())}


@pytest.fixture()
def images(monkeypatch):
    for k, v in {"AWS_ACCESS_KEY_ID": "t", "AWS_SECRET_ACCESS_KEY": "t",
                 "AWS_DEFAULT_REGION": "us-east-1", "AWS_REGION": "us-east-1"}.items():
        monkeypatch.setenv(k, v)
    with moto.mock_aws():
        boto3.client("s3").create_bucket(Bucket="agentcore-x-assets-1")
        sys.modules.pop("app.common.images", None)
        from app.common import images as mod
        mod._clients.clear()
        yield mod


def use(images, out):
    fake = FakeBedrock(out)
    images._clients[("bedrock-runtime", "us-west-2")] = fake
    return fake


PNG = base64.b64encode(b"\x89PNG fake").decode()


def test_a_brief_is_rendered_in_the_models_region_and_stored_under_its_run(images):
    fake = use(images, {"images": [PNG], "seeds": [42], "finish_reasons": [None]})
    got = images.render("a lighthouse at dawn", session_id="abc123def456", agent_id="hero",
                        image={"aspectRatio": "16:9", "negativePrompt": "text"},
                        negative_prompt="people", bucket="agentcore-x-assets-1")
    assert got["region"] == "us-west-2" and fake.calls[0]["modelId"] == "stability.sd3-5-large-v1:0"
    body = json.loads(fake.calls[0]["body"])
    assert body == {"prompt": "a lighthouse at dawn", "mode": "text-to-image",
                    "aspect_ratio": "16:9", "output_format": "png", "negative_prompt": "text, people"}
    assert got["imageKey"].startswith("runs/abc123def456/hero/") and got["seed"] == 42
    assert got["costUsd"] == 0.08 and got["ratesSource"] == "table"
    stored = boto3.client("s3").get_object(Bucket="agentcore-x-assets-1", Key=got["imageKey"])
    assert stored["Body"].read() == b"\x89PNG fake" and stored["ContentType"] == "image/png"


def test_a_filtered_image_is_an_error_not_a_blank(images):
    use(images, {"images": [""], "seeds": [1], "finish_reasons": ["Filter reason: prompt"]})
    with pytest.raises(images.ImageFiltered, match="withheld the image"):
        images.render("x", session_id="abc123def456", agent_id="hero", bucket="agentcore-x-assets-1")


def test_a_brief_does_not_leave_the_deployments_geography_unless_allowed(images, monkeypatch):
    """An EU deployment used to send every image brief to us-west-2 with nothing to say
    so. Now that is refused, and allowed only when the agent opts in."""
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    fake = use(images, {"images": [PNG], "seeds": [1], "finish_reasons": [None]})
    with pytest.raises(images.ImageRegionRefused, match="allowCrossRegion"):
        images.render("x", session_id="abc123def456", agent_id="hero", bucket="agentcore-x-assets-1")
    assert not fake.calls, "the brief was sent before the refusal"
    got = images.render("x", session_id="abc123def456", agent_id="hero",
                        image={"allowCrossRegion": True}, bucket="agentcore-x-assets-1")
    assert got["region"] == "us-west-2" and len(fake.calls) == 1


def test_the_render_region_prefers_the_deployments_own_then_its_geography(images):
    assert images.render_region("stability.sd3-5-large-v1:0", "us-west-2") == ("us-west-2", False)
    assert images.render_region("stability.sd3-5-large-v1:0", "us-east-1") == ("us-west-2", False)
    assert images.render_region("stability.sd3-5-large-v1:0", "eu-central-1") == ("us-west-2", True)
    assert images.residency_error("stability.sd3-5-large-v1:0", "us-east-2", False) == ""
    assert "allowCrossRegion" in images.residency_error("stability.sd3-5-large-v1:0", "ap-south-1", False)
    assert images.residency_error("stability.sd3-5-large-v1:0", "ap-south-1", True) == ""


def test_a_deployment_without_an_assets_bucket_says_why(images):
    with pytest.raises(RuntimeError, match="no assets bucket"):
        images.render("x", session_id="s", agent_id="a", bucket="")


# --- the link, from the BFF -------------------------------------------------------------

def _run_of(e, sid, owner="u1", table="console_status"):
    boto3.resource("dynamodb").Table(table).put_item(
        Item={"session_id": sid, "owner": owner, "overall": "done"})


def test_only_the_runs_owner_gets_a_link_to_its_image(env, monkeypatch):
    monkeypatch.setattr(env.handler, "ASSETS_BUCKET", "agentcore-console-assets-1")
    _run_of(env, "abc123def456")
    key = "runs/abc123def456/hero/1700000000000.png"
    status, body = call(env, "GET /api/images", path="/api/images", qs={"key": key})
    assert status == 200 and body["expiresIn"] == 900
    url = urllib.parse.urlparse(body["url"])
    assert "agentcore-console-assets-1" in url.netloc + url.path and url.path.endswith(".png")
    assert call(env, "GET /api/images", path="/api/images", qs={"key": key}, sub="u2")[0] == 404
    for bad in ("runs/../x.png", "other/abc123def456/hero/1.png", "runs/abc123def456/hero/1.svg"):
        assert call(env, "GET /api/images", path="/api/images", qs={"key": bad})[0] == 400


def test_a_builds_image_comes_from_that_builds_own_bucket(env):
    save(env)
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    agent = mark_deployed(env, "pclaims01")
    sid = call(env, "POST /api/sessions", body={"build": "pclaims01"})[1]["session_id"]
    status, body = call(env, "GET /api/images", path="/api/images",
                        qs={"key": f"runs/{sid}/hero/1.png"})
    assert status == 200
    assert f"agentcore-{agent.replace('_', '-')}-assets-1" in body["url"]
