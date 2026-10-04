"""Rendering for image agents (workflow.json `output: "image"`).

An image agent's model writes a brief — the prompt, a negative prompt and a caption —
and `render` turns it into an image with a Stability model on Bedrock, stores it in the
deployment's assets bucket, and returns where it is. The asset then carries the brief and
that location, so a reviewer at the gate sees the image and the words that made it.

Three things worth knowing:

* Each image model is served in a few regions only (`imageModels.regionsByModel` in
  vocabulary.json — us-west-2 alone, today). `render_region` picks the deployment's own
  region when the model is there, else one in the SAME geography (a US deployment renders
  in us-west-2). Sending the brief to another geography is refused unless the agent sets
  `image.allowCrossRegion: true`: it used to happen silently, so an EU deployment's image
  prompts left the EU with nothing to say so. The image comes back and is stored in the
  deployment's own bucket, in its own region.
* Bedrock's text guardrails and AgentCore Evaluations do not look at images. The model
  has its own safety filter: a filtered image raises ImageFiltered, which fails the step
  with the model's reason rather than returning a blank.
* Images are addressed `runs/<session>/<agent>/<n>.<ext>`, so the BFF can check that the
  caller owns the run before it hands out a link, and deleting the stack deletes them.
"""

from __future__ import annotations

import base64
import json
import os
import time
from pathlib import Path

_VOCAB = json.loads((Path(__file__).resolve().parent.parent / "vocabulary.json").read_text())
MODELS: tuple[str, ...] = tuple(_VOCAB["imageModels"]["values"])
REGIONS_BY_MODEL: dict[str, list[str]] = _VOCAB["imageModels"].get("regionsByModel") or {}
#: The offline price snapshot. observability/pricing.image_price prefers
#: orchestrator.imageRates, then the AWS Price List.
PRICES: dict = _VOCAB["imageModels"].get("pricePerImageUsd") or {}
ASPECT_RATIOS: tuple[str, ...] = tuple(_VOCAB["imageAspectRatios"]["values"])
FORMATS: tuple[str, ...] = tuple(_VOCAB["imageFormats"]["values"])

#: Set by both IaC paths when some agent has `output: "image"`.
ASSETS_BUCKET = os.getenv("ASSETS_BUCKET", "")
PROMPT_MAX = 10000


class ImageFiltered(RuntimeError):
    """The image model's safety filter withheld the image."""


class ImageRegionRefused(RuntimeError):
    """The model is not served in the deployment's geography, and the agent did not
    allow sending its brief to another one."""


def _deploy_region() -> str:
    return os.getenv("AWS_REGION", "us-east-1")


def render_region(model: str, deploy_region: str | None = None) -> tuple[str, bool]:
    """(region to render in, whether that leaves the deployment's geography)."""
    from app.common import vocabulary
    here = deploy_region or _deploy_region()
    regions = [str(r) for r in REGIONS_BY_MODEL.get(model) or []]
    if not regions:
        raise ValueError(f"{model} has no regionsByModel entry in app/vocabulary.json")
    if here in regions:
        return here, False
    geo = vocabulary.region_geo(here)
    same = [r for r in regions if vocabulary.region_geo(r) == geo]
    return (same[0], False) if same else (regions[0], True)


def residency_error(model: str, deploy_region: str, allow: bool) -> str:
    """Why a render may not run, or "" when it may. Shared by the runtime and both IaC
    checks so the refusal reads the same wherever it is raised."""
    _, crosses = render_region(model, deploy_region)
    if not crosses or allow:
        return ""
    return (f"{model} is served in {', '.join(REGIONS_BY_MODEL.get(model) or [])}, outside "
            f"this deployment's geography ({deploy_region}), so its image brief would leave "
            f"it. Set the agent's image.allowCrossRegion to true to allow that, or deploy in "
            f"a region of that geography.")


_clients: dict = {}


def _client(service: str, region: str):
    if (service, region) not in _clients:
        import boto3
        _clients[(service, region)] = boto3.client(service, region_name=region)
    return _clients[(service, region)]


def settings(image: dict | None) -> dict:
    """An agent's `image` block with every default filled in."""
    image = dict(image or {})
    return {
        "model": str(image.get("model") or MODELS[0]),
        "aspectRatio": str(image.get("aspectRatio") or "1:1"),
        "outputFormat": str(image.get("outputFormat") or "png"),
        "seed": image.get("seed"),
        "negativePrompt": str(image.get("negativePrompt") or ""),
        "allowCrossRegion": image.get("allowCrossRegion") is True,
    }


def render(prompt: str, *, session_id: str, agent_id: str, image: dict | None = None,
           negative_prompt: str = "", bucket: str | None = None) -> dict:
    """Render one image and store it. Returns {imageKey, model, seed, format,
    aspectRatio, latencyMs, costUsd}."""
    bucket = bucket if bucket is not None else ASSETS_BUCKET
    if not bucket:
        raise RuntimeError("this deployment has no assets bucket (ASSETS_BUCKET), so it cannot "
                           "store images; redeploy after setting an agent's output to \"image\"")
    prompt = str(prompt or "").strip()
    if not prompt:
        raise ValueError("the image brief has no prompt")
    s = settings(image)
    negative = ", ".join(n for n in (s["negativePrompt"], str(negative_prompt or "").strip()) if n)
    body = {"prompt": prompt[:PROMPT_MAX], "mode": "text-to-image",
            "aspect_ratio": s["aspectRatio"], "output_format": s["outputFormat"]}
    if s["seed"] is not None:
        body["seed"] = int(s["seed"])
    if negative:
        body["negative_prompt"] = negative[:PROMPT_MAX]
    refused = residency_error(s["model"], _deploy_region(), s["allowCrossRegion"])
    if refused:
        raise ImageRegionRefused(refused)
    region, _ = render_region(s["model"])
    started = time.perf_counter()
    resp = _client("bedrock-runtime", region).invoke_model(
        modelId=s["model"], body=json.dumps(body), accept="application/json",
        contentType="application/json")
    out = json.loads(resp["body"].read())
    latency_ms = int((time.perf_counter() - started) * 1000)
    reasons = out.get("finish_reasons") or [None]
    if reasons[0]:
        raise ImageFiltered(f"{s['model']} withheld the image: {reasons[0]}")
    data = base64.b64decode((out.get("images") or [""])[0])
    if not data:
        raise RuntimeError(f"{s['model']} returned no image")
    ext = "jpg" if s["outputFormat"] == "jpeg" else "png"
    key = f"runs/{session_id}/{agent_id}/{int(time.time() * 1000)}.{ext}"
    _client("s3", _deploy_region()).put_object(
        Bucket=bucket, Key=key, Body=data,
        ContentType="image/jpeg" if ext == "jpg" else "image/png")
    from app.features.observability import pricing
    cost, source = pricing.image_price(s["model"])
    return {"imageKey": key, "model": s["model"], "seed": (out.get("seeds") or [None])[0],
            "format": s["outputFormat"], "aspectRatio": s["aspectRatio"], "region": region,
            "latencyMs": latency_ms, "costUsd": float(cost), "ratesSource": source}


# --- reading images back (workflow.json `vision`) --------------------------------------

#: Bedrock Converse takes an image of at most 3.75 MB, and up to 20 in one message.
IMAGE_MAX_BYTES = 3_750_000
VISION_MAX = 20
#: The default `vision.maxImages`.
VISION_DEFAULT = 4
_FORMATS = {"png": "png", "jpg": "jpeg", "jpeg": "jpeg", "gif": "gif", "webp": "webp"}


def keys_in(output) -> list[str]:
    """The image keys an agent's output lists: `images: [{"imageKey": ...}, ...]`, the
    shape an image agent returns — and any other agent (or tool) may return."""
    data = output
    if isinstance(output, str):
        try:
            data = json.loads(output)
        except ValueError:
            from app.common.assets import extract_json
            data = extract_json(output)
    if not isinstance(data, dict):
        return []
    return [str(i["imageKey"]) for i in (data.get("images") or [])
            if isinstance(i, dict) and isinstance(i.get("imageKey"), str) and i["imageKey"]]


def load_for(outputs: dict, sources: list[str], *, session_id: str,
             max_images: int = VISION_DEFAULT, bucket: str | None = None) -> list[dict]:
    """The images the agents in `sources` produced in THIS run, for a model to read:
    [{key, format, bytes}], in the order `sources` names them, at most `max_images`.

    Only keys under this run's own folder (runs/<session>/) are read, so an output that
    names another run's image — or anything else in the bucket — reads nothing."""
    bucket = bucket if bucket is not None else ASSETS_BUCKET
    own = f"runs/{session_id}/"
    keys = [k for src in sources for k in keys_in((outputs or {}).get(src))
            if k.startswith(own) and ".." not in k]
    keys = keys[:max(1, min(int(max_images or VISION_DEFAULT), VISION_MAX))]
    if not keys:
        return []
    if not bucket:
        raise RuntimeError("this deployment has no assets bucket (ASSETS_BUCKET), so there "
                           "are no images to read; an agent with `output: \"image\"` creates it")
    s3 = _client("s3", _deploy_region())
    out = []
    for key in keys:
        fmt = _FORMATS.get(key.rsplit(".", 1)[-1].lower())
        if not fmt:
            raise ValueError(f"{key} is not an image format a model reads (png, jpeg, gif, webp)")
        data = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if len(data) > IMAGE_MAX_BYTES:
            raise ValueError(f"{key} is {len(data) / 1e6:.1f} MB; a model reads images of at "
                             f"most {IMAGE_MAX_BYTES / 1e6:.2f} MB")
        out.append({"key": key, "format": fmt, "bytes": data})
    return out
