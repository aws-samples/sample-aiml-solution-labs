"""GET /api/models/litellm: the models this deployment's LiteLLM proxy serves, for the Build
view's model picker ("via LiteLLM"). An agent picks one as `litellm/<id>`
(app/common/litellm_proxy.py makes the calls).

Reads the proxy's OpenAI-compatible `GET /v1/models` with its virtual key
(https://docs.litellm.ai/docs/proxy/model_access): the key's own model list, so the picker
offers only what this deployment may call. Through bff/netguard.py, as every fetch here is.
Off (no LITELLM_BASE_URL), it answers {"configured": false} and calls nothing.
"""
from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.request

import netguard

BASE_URL_ENV = "LITELLM_BASE_URL"
KEY_SECRET_ENV = "LITELLM_KEY_SECRET_ARN"  # noqa: S105 - an env var name, not a secret  # nosec B105
PREFIX = "litellm/"
#: The same patterns as app/common/litellm_proxy.py, terraform/variables.tf and cdk/lib/litellm.ts.
BASE_URL_PATTERN = r"^https://[A-Za-z0-9.-]+(:[0-9]{1,5})?(/[^\s?#]*)?$"
SECRET_ARN_PATTERN = r"^arn:aws:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@!-]+-[A-Za-z0-9]{6}$"  # noqa: S105 - a pattern  # nosec B105
#: What a proxy model name may be, to be offered: a picker label and a workflow.json value.
MODEL_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,199}$")
#: The most models offered; the most of the response read.
MAX_MODELS = 500
MAX_BYTES = 2 * 1024 * 1024
TIMEOUT_S = 8
#: The list changes when the proxy's config does, not per request.
TTL_S = 300
_cache: dict = {"at": 0.0, "body": None}
_key: str | None = None


def configured() -> bool:
    return bool(os.getenv(BASE_URL_ENV, "").strip() and os.getenv(KEY_SECRET_ENV, "").strip())


def _read_key(arn: str, client=None) -> str:
    global _key
    if _key is None:
        if client is None:
            import boto3
            client = boto3.client("secretsmanager", region_name=arn.split(":")[3])
        raw = str(client.get_secret_value(SecretId=arn).get("SecretString") or "").strip()
        if raw.startswith("{"):
            try:
                raw = str((json.loads(raw) or {}).get("apiKey") or "").strip()
            except ValueError:
                raw = ""
        if not raw:
            raise ValueError("the LiteLLM key secret is empty")
        _key = raw
    return _key


def _fetch() -> list[dict]:
    base, arn = os.getenv(BASE_URL_ENV, "").strip(), os.getenv(KEY_SECRET_ENV, "").strip()
    if not re.match(BASE_URL_PATTERN, base) or not re.match(SECRET_ARN_PATTERN, arn):
        raise ValueError("the LiteLLM settings are not a valid https URL and secret ARN")
    req = urllib.request.Request(f"{base.rstrip('/')}/v1/models", method="GET", headers={  # noqa: S310 - https checked above
        "Authorization": f"Bearer {_read_key(arn)}", "Accept": "application/json"})
    with netguard.urlopen(req, timeout=TIMEOUT_S) as resp:
        raw = resp.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise ValueError("the model list is over 2 MB")
    data = json.loads(raw)
    rows = data.get("data") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        raise ValueError("the proxy's /v1/models has no `data` list")
    ids = sorted({str(r.get("id")) for r in rows if isinstance(r, dict) and MODEL_ID.match(str(r.get("id") or ""))})
    return [{"id": f"{PREFIX}{i}", "name": i, "provider": "LiteLLM", "via": "litellm"} for i in ids[:MAX_MODELS]]


def listing() -> dict:
    """{"configured", "models", "error"?}. Never raises: the picker still takes a typed id."""
    if not configured():
        return {"configured": False, "models": []}
    now = time.time()
    if _cache["body"] is not None and now - _cache["at"] < TTL_S:
        return _cache["body"]
    try:
        body = {"configured": True, "models": _fetch()}
    except urllib.error.HTTPError as e:
        print(f"[litellm] /v1/models answered HTTP {e.code}")
        return {"configured": True, "models": [],
                "error": f"The LiteLLM proxy refused the model list (HTTP {e.code}); type a model as litellm/<name>."}
    except Exception as e:  # noqa: BLE001 - the picker degrades to free text
        # The type only: an error's text could carry what the proxy echoed back.
        print(f"[litellm] /v1/models failed: {type(e).__name__}")
        return {"configured": True, "models": [],
                "error": f"Could not list the LiteLLM proxy's models ({type(e).__name__}); "
                         "type a model as litellm/<name>."}
    _cache.update(at=now, body=body)
    return body


def problems(workflow: dict) -> list[dict]:
    """Agents whose model is litellm/<name> that this deployment cannot call: LiteLLM is not
    set up, or the proxy does not list that name. Warnings, the shape of validate_build
    issues; the Build view computes the same (web/src/builder/models.ts modelIssues)."""
    default = str((workflow.get("orchestrator") or {}).get("defaultModel") or "")
    wanted = [(aid, str(a.get("model") or default)) for aid, a in (workflow.get("agents") or {}).items()
              if isinstance(a, dict) and a.get("runtime") not in ("a2a", "task")]
    wanted = [(aid, mid) for aid, mid in wanted if mid.startswith(PREFIX)]
    if not wanted:
        return []
    got = listing()
    listed = {m["id"] for m in got.get("models", [])}
    out = []
    for aid, mid in wanted:
        where, path = {"kind": "agent", "id": aid}, f"agents.{aid}.model"
        if not got.get("configured"):
            out.append({"severity": "warning", "where": where, "path": path,
                        "message": f"{mid} is a LiteLLM model, and LiteLLM is not set up on this deployment: "
                                   "deploy with a LiteLLM proxy (DEPLOYMENT.md), or pick a Bedrock model"})
        elif listed and mid not in listed:
            out.append({"severity": "warning", "where": where, "path": path,
                        "message": f"the LiteLLM proxy does not list {mid[len(PREFIX):]}: pick one it serves"})
    return out
