"""Bedrock on-demand token prices from the AWS Price List API.

pricing.py used to know six Claude ids, compiled in, for us-east-1, as of one date.
Every other model was priced at a fallback, every other region at us-east-1's rates,
and every price change needed a framework edit. This module asks AWS instead: the
Price List API (`pricing:GetProducts`) for both Bedrock offers in this deployment's
region —

  * AmazonBedrock                    first-party and most third-party models (Nova,
                                     Llama, Mistral, DeepSeek, gpt-oss, ...);
  * AmazonBedrockFoundationModels    the AWS Marketplace editions (Anthropic Claude).

and keeps ONLY the standard on-demand text-token rates: input and output, per scope
(regional — an `us.`/`eu.` profile or a bare model id — and `global.`). Batch,
priority, flex, prompt caching, latency-optimised, long-context, image/audio/video
tokens and provisioned throughput are left out: those are not what a Converse call at
the default tier is billed at.

Joining a model id to a price row is the hard part. The Price List has no model-id
attribute (aws/aws-sdk-go-v2#3397): a row names its model by display name ("Claude
Haiku 4.5 (Amazon Bedrock Edition)", "Nova Pro") and sometimes embeds an id in the
usage type ("USE1-google.gemma-3-4b-it-mantle-input-tokens-standard"). Both are
normalised to one key — lower case, provider/geo/version/date dropped, punctuation
removed — and a model id is looked up by the same key, EXACT match only. A substring
match would price `claude-opus-4` at `claude-opus-4-1`'s rate; an unmatched model
falls through to pricing.py's next source instead, and the row says which one priced it.

Loaded once per process in a background thread (started when telemetry is on, so it is
usually ready before the first model call returns) and cached in /tmp for a day. A
lookup waits at most LOOKUP_WAIT_S for a load in flight; a failed load (no
`pricing:GetProducts`, throttling, no network) is remembered for an hour and pricing
falls back. Nothing here raises: cost telemetry must never break a run.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import tempfile
import threading
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path

SERVICE_CODES = ("AmazonBedrock", "AmazonBedrockFoundationModels")
#: The Price List API is served from a few regions only (us-east-1, eu-central-1,
#: ap-south-1); the region of the PRICES is a filter, not the endpoint.
API_REGION = os.getenv("PRICING_API_REGION", "us-east-1")
CACHE_TTL_S = 24 * 3600
RETRY_AFTER_S = 3600
LOOKUP_WAIT_S = float(os.getenv("PRICE_LIST_WAIT_SECONDS", "3"))

_MTOK = Decimal(1_000_000)
#: Rows that are not the standard on-demand text-token rate.
_EXCLUDE = re.compile(r"batch|priority|flex|cache|latency|long|image|video|audio|speech|"
                      r"reserved|provisioned|custom|training|storage|embed", re.IGNORECASE)
#: A per-image generation price (Stability on Bedrock: "Output image, SD3.5 Large").
_IMAGE_ROW = re.compile(r"created_image|output image", re.IGNORECASE)
_REGION_PREFIX = re.compile(r"^[A-Z0-9]+-")
_UT_ID_END = re.compile(r"-(?:mantle-)?(?:text-)?(?:input|output)", re.IGNORECASE)
_EDITION = re.compile(r"\(amazon bedrock edition\)", re.IGNORECASE)
#: `-v1:0`, `-v2`, `-1:0`, `:0` — never a bare `-4`, which is part of `claude-opus-4`.
_VERSION = re.compile(r"(?:-v\d+(?::\d+)?|-\d+:\d+|:\d+)$")
_DATE = re.compile(r"-\d{8}(?=$|-)")

_lock = threading.Lock()
_ready = threading.Event()
_book: dict | None = None        # {region: {key: {scope: {"input": str, "output": str}}}}
_loaded_region = ""
_failed_at = 0.0
_thread: threading.Thread | None = None


# --- keys -------------------------------------------------------------------

def _squash(text: str) -> str:
    # "+" is part of a name ("Command R+" is not "Command R"); everything else is noise.
    return re.sub(r"[^a-z0-9]", "", text.lower().replace("+", "plus"))


def _geo_prefixes() -> tuple[str, ...]:
    from app.common import vocabulary
    return vocabulary.INFERENCE_PROFILE_GEOS


def model_key(model_id: str) -> str:
    """The lookup key for a Bedrock model id or inference profile id.

    `us.anthropic.claude-haiku-4-5-20251001-v1:0` -> `claudehaiku45`,
    `amazon.nova-pro-v1:0` -> `novapro`, `meta.llama3-3-70b-instruct-v1:0` ->
    `llama3370binstruct`.
    """
    mid = str(model_id or "").strip().lower()
    if "/" in mid:          # an inference-profile or foundation-model ARN
        mid = mid.rsplit("/", 1)[-1]
    parts = mid.split(".")
    if len(parts) > 1 and parts[0] in _geo_prefixes():
        parts = parts[1:]
    # The provider segment (anthropic, amazon, meta, ...): no digits, and not the last.
    if len(parts) > 1 and not re.search(r"\d", parts[0]):
        parts = parts[1:]
    rest = ".".join(parts)
    rest = _VERSION.sub("", rest)
    rest = _DATE.sub("", rest)
    return _squash(rest)


def _provider(model_id: str) -> str:
    parts = str(model_id or "").strip().lower().rsplit("/", 1)[-1].split(".")
    if len(parts) > 1 and parts[0] in _geo_prefixes():
        parts = parts[1:]
    return _squash(parts[0]) if len(parts) > 1 and not re.search(r"\d", parts[0]) else ""


def candidate_keys(model_id: str) -> list[str]:
    """Keys to try for a model id, most specific first. Still exact matches each: the
    variants only drop what the Price List's display names leave out — the provider is
    sometimes IN the name ("DeepSeek v3.2"), a model id often ends `-instruct`/`-it`
    where the name does not ("Llama 3.3 70B"), and a dated Mistral id carries its
    release ("mistral-large-2402") where the row says "Mistral Large"."""
    key = model_key(model_id)
    out = [key]
    provider = _provider(model_id)
    if provider:
        out.append(provider + key)
    trimmed = re.sub(r"(instruct|it|chat)$", "", key)
    if trimmed and trimmed != key:
        out.append(trimmed)
    undated = re.sub(r"\d{4}$", "", trimmed or key)
    if undated and undated not in out and re.search(r"[a-z]", undated):
        out.append(undated)
    # Stability's ids abbreviate what the Price List spells out: `sd3-5-large` is
    # "Stable Diffusion 3.5 Large".
    if re.match(r"sd\d", key):
        out.append("stablediffusion" + key[2:])
    return [k for i, k in enumerate(out) if k and k not in out[:i]]


def scope_of(model_id: str) -> str:
    return "global" if str(model_id or "").lower().startswith("global.") else "regional"


def _row_keys(attrs: dict) -> set[str]:
    keys: set[str] = set()
    name = attrs.get("model") or ""
    service = attrs.get("servicename") or ""
    if not name and _EDITION.search(service):
        name = _EDITION.sub("", service)
    if name:
        # "Nova 2.0 Lite" is the model id `nova-2-lite`: a ".0" carries no information.
        name = re.sub(r"(\d)\.0\b", r"\1", name)
        keys.add(model_key(name) if "." in name and " " not in name else _squash(name))
        # "Stable Diffusion 3.5 Large v1.0" is the model id `sd3-5-large-v1:0`, whose key
        # drops its version: offer the name without its trailing "vN" as well.
        bare = re.sub(r"\s+v\d+\s*$", "", name.strip())
        if bare != name.strip():
            keys.add(_squash(bare))
    usage = attrs.get("usagetype") or ""
    if usage and ":" not in usage.split("-", 1)[0] and "MP:" not in usage:
        ident = _UT_ID_END.split(_REGION_PREFIX.sub("", usage), maxsplit=1)[0]
        if ident:
            keys.add(model_key(ident))
    return {k for k in keys if k}


# --- parsing ----------------------------------------------------------------

def _per_mtok(price: str, unit: str) -> Decimal | None:
    try:
        usd = Decimal(str(price))
    except (InvalidOperation, TypeError, ValueError):
        return None
    u = (unit or "").lower()
    if u.startswith("1k"):
        return usd * 1000
    if u.startswith("1m"):
        return usd
    return None


def parse_products(products: list) -> dict:
    """{key: {scope: {"input": str, "output": str}}} from GetProducts `PriceList` entries
    (JSON strings or dicts). Where several rows give one key/kind/scope different prices
    (a legacy and a current usage type for the same model), the HIGHER is kept: an
    estimate that errs high is the safer one to plan with."""
    out: dict[str, dict[str, dict[str, Decimal]]] = {}
    for raw in products:
        try:
            p = json.loads(raw) if isinstance(raw, str) else raw
            attrs = (p.get("product") or {}).get("attributes") or {}
            head = " ".join(str(attrs.get(k) or "") for k in
                            ("usagetype", "inferenceType", "feature", "service_tier"))
            image_row = bool(_IMAGE_ROW.search(head))
            if not image_row and (_EXCLUDE.search(head)
                                  or (attrs.get("feature") or "").lower().startswith("batch")):
                continue
            keys = _row_keys(attrs)
            if not keys:
                continue
            for term in ((p.get("terms") or {}).get("OnDemand") or {}).values():
                for dim in (term.get("priceDimensions") or {}).values():
                    text = f"{head} {dim.get('description') or ''}"
                    if image_row or (dim.get("unit") or "").lower() == "image":
                        # Priced per image, not per token: its own slot.
                        try:
                            usd = Decimal(str((dim.get("pricePerUnit") or {}).get("USD")))
                        except (InvalidOperation, TypeError, ValueError):
                            continue
                        if (dim.get("unit") or "").lower() != "image" or usd <= 0:
                            continue
                        for k in keys:
                            slot = out.setdefault(k, {}).setdefault("regional", {})
                            slot["image"] = max(slot.get("image", Decimal(0)), usd)
                        continue
                    if _EXCLUDE.search(text):
                        continue
                    low = text.lower()
                    kind = "output" if "output" in low else "input" if "input" in low else ""
                    rate = _per_mtok((dim.get("pricePerUnit") or {}).get("USD"), dim.get("unit"))
                    if not kind or rate is None or rate <= 0:
                        continue
                    scope = "global" if "global" in low else "regional"
                    for k in keys:
                        slot = out.setdefault(k, {}).setdefault(scope, {})
                        slot[kind] = max(slot.get(kind, Decimal(0)), rate)
        except Exception as e:  # noqa: BLE001 - one odd row must not lose the rest
            print(f"[observability] price list: skipped a row: {type(e).__name__}: {e}")
    book = {k: {s: {kind: str(v) for kind, v in rates.items()}
                for s, rates in scopes.items()
                if {"input", "output"} <= set(rates) or "image" in rates}
            for k, scopes in out.items()}
    return {k: v for k, v in book.items() if v}


# --- loading ----------------------------------------------------------------

def _cache_path(region: str) -> Path:
    return Path(tempfile.gettempdir()) / f"agentexpress-pricebook-{region}.json"


def _read_cache(region: str) -> dict | None:
    with contextlib.suppress(Exception):
        path = _cache_path(region)
        if time.time() - path.stat().st_mtime < CACHE_TTL_S:
            return json.loads(path.read_text())
    return None


def _fetch(region: str) -> dict:
    import boto3
    from botocore.config import Config
    client = boto3.client("pricing", region_name=API_REGION,
                          config=Config(connect_timeout=5, read_timeout=15,
                                        retries={"max_attempts": 3, "mode": "standard"}))
    products: list = []
    for code in SERVICE_CODES:
        pages = client.get_paginator("get_products").paginate(
            ServiceCode=code, FormatVersion="aws_v1",
            Filters=[{"Type": "TERM_MATCH", "Field": "regionCode", "Value": region}],
            PaginationConfig={"PageSize": 100})
        for page in pages:
            products.extend(page.get("PriceList") or [])
    return parse_products(products)


def _load(region: str) -> None:
    global _book, _loaded_region, _failed_at
    try:
        book = _read_cache(region)
        if book is None:
            book = _fetch(region)
            if not book:
                raise RuntimeError(f"the Price List returned no Bedrock token rates for {region}")
            with contextlib.suppress(Exception):
                _cache_path(region).write_text(json.dumps(book))
        with _lock:
            _book, _loaded_region = book, region
        print(f"[observability] price list: {len(book)} Bedrock models priced for {region}")
    except Exception as e:  # noqa: BLE001
        with _lock:
            _failed_at = time.time()
        print(f"[observability] price list unavailable, using configured/built-in rates: "
              f"{type(e).__name__}: {e}")
    finally:
        _ready.set()


def _region() -> str:
    from app.common.config import REGION
    return REGION


def prefetch() -> None:
    """Start loading this region's prices in the background (idempotent)."""
    global _thread
    with _lock:
        if _book is not None or (_thread and _thread.is_alive()):
            return
        if _failed_at and time.time() - _failed_at < RETRY_AFTER_S:
            return
        _ready.clear()
        _thread = threading.Thread(target=_load, args=(_region(),), name="pricelist", daemon=True)
        _thread.start()


def lookup(model_id: str) -> tuple[Decimal, Decimal] | None:
    """(input, output) USD per 1M tokens for `model_id` from the Price List, or None."""
    # Never STARTS a load: prefetch() does that, from meter.py, only where telemetry is
    # written. So a local run or a test makes no AWS call; it just waits briefly for a
    # load already in flight.
    if _book is None and _failed_at:
        prefetch()  # a retry, once RETRY_AFTER_S has passed since the failure
    if _book is None and _thread is not None and _thread.is_alive():
        _ready.wait(LOOKUP_WAIT_S)
    book = _book
    if not book:
        return None
    scopes = next((book[k] for k in candidate_keys(model_id)
                   if k in book and any("input" in r for r in book[k].values())), {})
    want = scope_of(model_id)
    rates = scopes.get(want) or scopes.get("regional" if want == "global" else "global")
    if not rates or "input" not in rates:
        return None
    return Decimal(rates["input"]), Decimal(rates["output"])


def lookup_image(model_id: str) -> Decimal | None:
    """USD per generated image for an image model from the Price List, or None."""
    if _book is None and _thread is not None and _thread.is_alive():
        _ready.wait(LOOKUP_WAIT_S)
    book = _book or {}
    for k in candidate_keys(model_id):
        price = ((book.get(k) or {}).get("regional") or {}).get("image")
        if price:
            return Decimal(price)
    return None


def _reset_for_tests() -> None:
    global _book, _loaded_region, _failed_at, _thread
    with _lock:
        _book, _loaded_region, _failed_at, _thread = None, "", 0.0, None
    _ready.clear()
