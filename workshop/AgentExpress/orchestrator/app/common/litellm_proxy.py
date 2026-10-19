"""Non-Bedrock models through the customer's LiteLLM proxy (off unless the deployment sets it).

An agent whose `model` is `litellm/<model>` (litellm/gpt-4o, litellm/gemini-1.5-pro) is
answered by the LiteLLM proxy the deployment names, over its OpenAI-compatible
`POST /v1/chat/completions`, with the proxy's virtual key as `Authorization: Bearer`
(https://docs.litellm.ai/docs/proxy/virtual_keys). `<model>` is the proxy's model name, as
`GET /v1/models` lists it. No litellm package: plain HTTPS through app/common/netguard.py,
so the proxy must be https and on a public address.

ctx.llm stays the only door. app/common/llm.py hands `litellm/` models here from run_llm
and run_llm_tools, so what wraps a model call is unchanged: guardrails (ApplyGuardrail in
the node wrapper, before and after the agent, whatever its model), long-term memory and
skills in the system prompt, truncation detection (OpenAI's finish_reason "length"),
journaling for agents that ask a person, and a telemetry row per call. Tokens come from the
response's `usage`; cost from the proxy's `x-litellm-response-cost` header
(https://docs.litellm.ai/docs/proxy/cost_tracking) when it sends one, else 0 with
rates_source "none" so nobody reads a 0 as measured.

Settings, both set by the IaC (terraform/litellm.tf, cdk/lib/litellm.ts) or neither:
  LITELLM_BASE_URL        the proxy, e.g. https://litellm.example.com
  LITELLM_KEY_SECRET_ARN  a Secrets Manager secret holding the virtual key: the key as the
                          whole SecretString, or {"apiKey": "sk-..."}. Read once per process,
                          in the secret's own region; never logged, never in an error.
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import time
import urllib.error
import urllib.request

from app.common import netguard
from app.common.errors import ModelUnavailable

PREFIX = "litellm/"
BASE_URL_ENV = "LITELLM_BASE_URL"
KEY_SECRET_ENV = "LITELLM_KEY_SECRET_ARN"  # noqa: S105 - an env var name, not a secret  # nosec B105
#: Mirrors var.litellm_base_url (terraform/variables.tf) and LITELLM_BASE_URL_PATTERN
#: (cdk/lib/litellm.ts); tests/test_litellm_proxy.py pins all three.
BASE_URL_PATTERN = r"^https://[A-Za-z0-9.-]+(:[0-9]{1,5})?(/[^\s?#]*)?$"
#: A full secret ARN, six-character suffix included: the grant names exactly this ARN.
SECRET_ARN_PATTERN = r"^arn:aws:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@!-]+-[A-Za-z0-9]{6}$"  # noqa: S105 - a pattern  # nosec B105
#: The proxy's cost of the call in USD, when it tracks spend.
COST_HEADER = "x-litellm-response-cost"
#: The same budget a Bedrock call gets (llm.READ_TIMEOUT_S): a long answer is written whole
#: before the proxy returns it.
READ_TIMEOUT_S = int(os.getenv("BEDROCK_READ_TIMEOUT_SECONDS", "300"))
#: One retry, as for Bedrock (total_max_attempts 2): each attempt may be a billed call.
ATTEMPTS = 2
#: Worth a second attempt: rate limited, or the proxy or its provider briefly unavailable.
RETRY_STATUS = frozenset({408, 429, 500, 502, 503, 504})
#: The most of a response read: a chat completion is text, never this big.
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
#: Documents sent as text: the proxy's providers differ on file parts, text they all read.
_TEXT_DOCS = {"csv", "txt", "md", "html"}
_MIME = {"pdf": "application/pdf", "doc": "application/msword",
         "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
         "xls": "application/vnd.ms-excel",
         "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
         "png": "image/png", "jpeg": "image/jpeg", "gif": "image/gif", "webp": "image/webp"}

_key: str | None = None


class ProxyError(Exception):
    """The proxy answered with an error, or could not be reached."""

    def __init__(self, message: str, status: int = 0, retry: bool = False):
        self.status = status
        #: Worth one more attempt: the request may well succeed the second time.
        self.retry = retry or status in RETRY_STATUS
        super().__init__(message)


def is_litellm(model: str | None) -> bool:
    return str(model or "").startswith(PREFIX)


def proxy_model(model: str) -> str:
    """The proxy's model name: what follows `litellm/`."""
    return str(model)[len(PREFIX):]


def configured() -> bool:
    return bool(os.getenv(BASE_URL_ENV, "").strip() and os.getenv(KEY_SECRET_ENV, "").strip())


def _settings() -> tuple[str, str]:
    base, arn = os.getenv(BASE_URL_ENV, "").strip(), os.getenv(KEY_SECRET_ENV, "").strip()
    if not (base and arn):
        raise ProxyError("LiteLLM is not set up on this deployment: deploy with litellm_base_url and "
                         "litellm_key_secret_arn (Terraform) or -c litellmBaseUrl and "
                         "-c litellmKeySecretArn (CDK)")
    if not re.match(BASE_URL_PATTERN, base):
        raise ProxyError(f"{BASE_URL_ENV} must be an https:// URL with no user info, query or fragment")
    if not re.match(SECRET_ARN_PATTERN, arn):
        raise ProxyError(f"{KEY_SECRET_ENV} must be the full ARN of a Secrets Manager secret")
    return base.rstrip("/"), arn


def _read_key(arn: str, client=None) -> str:
    """The virtual key: the whole SecretString, or its `apiKey` when it is a JSON object."""
    global _key
    if _key is None:
        if client is None:
            import boto3
            # The secret's own region, so it need not be in the runtime's.
            client = boto3.client("secretsmanager", region_name=arn.split(":")[3])
        raw = str(client.get_secret_value(SecretId=arn).get("SecretString") or "").strip()
        if raw.startswith("{"):
            try:
                raw = str((json.loads(raw) or {}).get("apiKey") or "").strip()
            except ValueError:
                raw = ""
        if not raw:
            raise ProxyError("the LiteLLM key secret is empty: store the virtual key, or {\"apiKey\": \"...\"}")
        _key = raw
    return _key


def _redact(text: str) -> str:
    """Error text from the proxy, without the key in it (some errors echo what was sent)."""
    out = str(text or "")
    if _key:
        out = out.replace(_key, "[key]")
    return re.sub(r"sk-[A-Za-z0-9_-]{6,}", "sk-[redacted]", out)[:600]


def _post(url: str, body: dict, key: str) -> tuple[dict, dict]:
    """One HTTPS POST through the SSRF guard. Returns (json body, lower-cased headers)."""
    # https enforced by _settings and again by netguard.urlopen.
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST", headers={  # noqa: S310
        "Authorization": f"Bearer {key}", "Content-Type": "application/json",
        "Accept": "application/json"})
    try:
        with netguard.urlopen(req, timeout=READ_TIMEOUT_S) as resp:
            raw = resp.read(MAX_RESPONSE_BYTES + 1)
            headers = {k.lower(): v for k, v in resp.headers.items()}
    except urllib.error.HTTPError as e:
        detail = ""
        try:
            got = json.loads(e.read(65536) or b"{}")
            err = got.get("error") if isinstance(got, dict) else None
            detail = str(err.get("message") if isinstance(err, dict) else err or got)
        except (ValueError, OSError, AttributeError):
            pass
        raise ProxyError(f"the proxy answered HTTP {e.code}{': ' + _redact(detail) if detail else ''}",
                         status=e.code) from None
    except netguard.BlockedURL as e:
        raise ProxyError(f"the proxy URL is refused: {e.reason}") from None
    except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
        reason = getattr(e, "reason", e)
        raise ProxyError(f"the proxy could not be reached: {type(reason).__name__}: {_redact(str(reason))}",
                         retry=True) from None
    if len(raw) > MAX_RESPONSE_BYTES:
        raise ProxyError("the proxy's response is over 16 MB")
    try:
        data = json.loads(raw)
    except ValueError:
        raise ProxyError("the proxy's response is not JSON") from None
    if not isinstance(data, dict) or not isinstance(data.get("choices"), list) or not data["choices"]:
        raise ProxyError("the proxy's response has no choices")
    return data, headers


def _cancelled() -> bool:
    try:
        from app.common.sink import is_cancelled
        from app.features.observability.scope import get_scope
        sid = get_scope().session_id
        return bool(sid) and is_cancelled(sid)
    except Exception:  # noqa: BLE001 - no run scope (a test, a script): nothing to cancel
        return False


async def _complete(body: dict) -> tuple[dict, dict]:
    """POST /v1/chat/completions, retried once on a refusal worth retrying."""
    base, arn = _settings()
    key = await asyncio.to_thread(_read_key, arn)
    last: ProxyError | None = None
    for attempt in range(ATTEMPTS):
        if attempt:
            if _cancelled():
                from app.common.sink import WorkflowCancelled
                from app.features.observability.scope import get_scope
                raise WorkflowCancelled(get_scope().session_id)
            await asyncio.sleep(min(2.0, 0.5 * (2 ** attempt)))
        try:
            return await asyncio.to_thread(_post, f"{base}/v1/chat/completions", body, key)
        except ProxyError as e:
            last = e
            if not e.retry:
                raise
    raise last  # type: ignore[misc]


# --- mapping: our calls -> OpenAI chat completions ------------------------------------------

def _data_url(fmt: str, data: bytes) -> str:
    return f"data:{_MIME.get(fmt, 'application/octet-stream')};base64,{base64.b64encode(data).decode()}"


def user_parts(text: str, images: list[dict] | None, files: list[dict] | None) -> str | list[dict]:
    """The user message's content: the text, then each image (`vision`) and attached file.
    Images as image_url data URLs; text documents inline; other documents as `file` parts
    (https://docs.litellm.ai/docs/completion/vision, .../completion/document_understanding)."""
    if not images and not files:
        return text
    parts: list[dict] = [{"type": "text", "text": text}]
    parts += [{"type": "image_url", "image_url": {"url": _data_url(i["format"], i["bytes"])}} for i in images or []]
    for f in files or []:
        if f.get("kind") == "image":
            parts.append({"type": "image_url", "image_url": {"url": _data_url(f["format"], f["bytes"])}})
        elif f.get("format") in _TEXT_DOCS:
            parts.append({"type": "text", "text": f"--- FILE: {f['name']} ---\n"
                                                  f"{bytes(f['bytes']).decode('utf-8', 'replace')}"})
        else:
            parts.append({"type": "file", "file": {"filename": f["name"],
                                                   "file_data": _data_url(f["format"], f["bytes"])}})
    return parts


def tool_specs(tools: list[dict]) -> list[dict]:
    """{"name", "description", "input_schema"} (tool_loop) as OpenAI `tools`."""
    return [{"type": "function", "function": {"name": t["name"], "description": t["description"],
                                              "parameters": t["input_schema"]}} for t in tools]


def _content_text(content) -> str:
    from app.common.llm import _text_of
    return _text_of(content) if content is not None else ""


def messages_of(system: str, messages: list) -> list[dict]:
    """langchain messages (HumanMessage, AIMessage with tool_calls, ToolMessage) as OpenAI's."""
    out: list[dict] = [{"role": "system", "content": system}]
    for m in messages:
        kind = getattr(m, "type", "")
        if kind == "human":
            out.append({"role": "user", "content": _content_text(m.content)})
        elif kind == "ai":
            msg: dict = {"role": "assistant", "content": _content_text(m.content) or None}
            calls = list(getattr(m, "tool_calls", None) or [])
            if calls:
                msg["tool_calls"] = [{"id": str(c.get("id") or c.get("name")), "type": "function",
                                      "function": {"name": c.get("name"),
                                                   "arguments": json.dumps(c.get("args") or {})}}
                                     for c in calls]
            out.append(msg)
        elif kind == "tool":
            out.append({"role": "tool", "tool_call_id": str(m.tool_call_id),
                        "content": _content_text(m.content)})
        elif kind == "system":
            out.append({"role": "system", "content": _content_text(m.content)})
    return out


def _sampling(temperature, top_p, stop) -> dict:
    out: dict = {"temperature": temperature}
    if top_p is not None:
        out["top_p"] = top_p
    if stop:
        out["stop"] = list(stop)
    return out


# --- mapping: OpenAI chat completion -> what our callers read --------------------------------

def ai_message(data: dict):
    """The first choice as a langchain AIMessage: text, tool_calls, usage, finish_reason. A
    call whose `arguments` is not a JSON object is kept out of tool_calls (as langchain does,
    in invalid_tool_calls), so no tool is ever called with arguments the model did not give."""
    from langchain_core.messages import AIMessage
    choice = data["choices"][0] if isinstance(data["choices"][0], dict) else {}
    message = choice.get("message") or {}
    calls, invalid = [], []
    for c in message.get("tool_calls") or []:
        fn = (c or {}).get("function") or {}
        raw = fn.get("arguments")
        try:
            args = json.loads(raw) if isinstance(raw, str) and raw.strip() else (raw or {})
        except ValueError:
            args = None
        if isinstance(args, dict) and fn.get("name"):
            calls.append({"name": str(fn["name"]), "args": args, "id": str(c.get("id") or fn["name"])})
        else:
            invalid.append({"name": fn.get("name"), "args": str(raw)[:2000], "id": c.get("id"),
                            "error": "arguments are not a JSON object"})
    usage = data.get("usage") or {}
    tokens_in, tokens_out = int(usage.get("prompt_tokens") or 0), int(usage.get("completion_tokens") or 0)
    return AIMessage(content=_content_text(message.get("content")), tool_calls=calls,
                     invalid_tool_calls=invalid,
                     usage_metadata={"input_tokens": tokens_in, "output_tokens": tokens_out,
                                     "total_tokens": tokens_in + tokens_out},
                     response_metadata={"finish_reason": str(choice.get("finish_reason") or ""),
                                        "model_name": str(data.get("model") or "")})


def cost_of(headers: dict):
    """(cost in USD, rates_source): the proxy's own figure, or 0 and "none" without one."""
    from decimal import Decimal, InvalidOperation
    raw = str((headers or {}).get(COST_HEADER) or "").strip()
    try:
        cost = Decimal(raw)
    except (InvalidOperation, ValueError):
        return Decimal(0), "none"
    if not cost.is_finite() or cost < 0:
        return Decimal(0), "none"
    return cost, "proxy"


def _meter(model, msg, headers, system, shown, out_text, latency_ms, *, mode, temperature,
           max_tokens, name) -> None:
    import contextlib
    with contextlib.suppress(Exception):  # metering must never break a call
        usage = (getattr(msg, "usage_metadata", None) or {}) if msg is not None else {}
        tokens_in, tokens_out = int(usage.get("input_tokens") or 0), int(usage.get("output_tokens") or 0)
        from app.features.observability import otel
        otel.add_tokens(tokens_in, tokens_out)
        from decimal import Decimal

        from app.features.observability import meter
        cost, source = cost_of(headers) if mode != "error" else (Decimal(0), "none")
        meter.record_llm(model=model, input_tokens=tokens_in, output_tokens=tokens_out,
                         system_text=system, latency_ms=latency_ms, mode=mode, user_input=shown,
                         output_text=out_text, temperature=temperature, max_tokens=max_tokens,
                         finish_reason=(getattr(msg, "response_metadata", None) or {}).get(
                             "finish_reason", "") if msg is not None else "",
                         prompt=name, cost_usd=cost, rates_source=source)


def _failed(name: str, model: str, e: Exception, what: str = "") -> ModelUnavailable:
    return ModelUnavailable(
        f"LiteLLM model call '{name}'{what} failed on {model}: {type(e).__name__}: {_redact(str(e))}. "
        f"Check that the proxy serves '{proxy_model(model)}' (GET /v1/models) and that its virtual key "
        f"may call it.")


async def _adapted(body: dict, sampling: dict, name: str, model: str) -> tuple[dict, dict]:
    """The call, retried without a sampling setting the model refuses by name (OpenAI's
    reasoning models refuse `temperature`), as run_llm does for Bedrock models."""
    from app.common.llm import _refused_setting
    for _attempt in range(3):
        try:
            return await _complete({**body, **sampling})
        except ProxyError as e:
            dropped = _refused_setting(e, sampling) if e.status == 400 else None
            if not dropped:
                raise
            print(f"[litellm] {model} refused `{dropped}`; calling '{name}' without it")
            sampling.pop(dropped)
    return await _complete({**body, **sampling})


async def run_llm(name: str, system: str, user: str, *, model: str, temperature: float = 0,
                  max_tokens: int = 300, top_p: float | None = None,
                  stop_sequences: list[str] | None = None, images: list[dict] | None = None,
                  files: list[dict] | None = None) -> tuple[str, bool]:
    """llm.run_llm for a `litellm/` model: (text, hit_token_ceiling)."""
    import contextlib

    from app.common.llm import _TRUNCATED_REASONS
    images, files = list(images or []), list(files or [])
    shown = user + (f"\n\n[{len(images)} image(s): {', '.join(i['key'] for i in images)}]"
                    if images else "") + (
        f"\n\n[{len(files)} attached file(s): {', '.join(f['name'] for f in files)}]" if files else "")
    with contextlib.suppress(Exception):
        from app.features.observability import otel as _otel
        _otel.capture_prompt(name, system, shown)
    body = {"model": proxy_model(model), "max_tokens": max_tokens,
            "messages": [{"role": "system", "content": system},
                         {"role": "user", "content": user_parts(user, images, files)}]}
    sampling = _sampling(temperature, top_p, stop_sequences)
    start = time.perf_counter()
    try:
        data, headers = await _adapted(body, sampling, name, model)
        msg = ai_message(data)
    except Exception as e:
        if type(e).__name__ == "WorkflowCancelled":
            raise
        _meter(model, None, {}, system, shown, "", 0, mode="error", temperature=temperature,
               max_tokens=max_tokens, name=name)
        raise _failed(name, model, e) from e
    latency_ms = int((time.perf_counter() - start) * 1000)
    out_text = _content_text(msg.content)
    _meter(model, msg, headers, system, shown, out_text, latency_ms, mode="litellm",
           temperature=temperature, max_tokens=max_tokens, name=name)
    with contextlib.suppress(Exception):
        from app.features.observability import otel as _otel
        _otel.capture_output(out_text)
    return out_text, msg.response_metadata.get("finish_reason", "").lower() in _TRUNCATED_REASONS


async def run_llm_tools(name: str, system: str, messages: list, tools: list[dict], *, model: str,
                        temperature: float = 0, max_tokens: int = 1500):
    """llm.run_llm_tools for a `litellm/` model: the AIMessage, its tool_calls the model's."""
    body = {"model": proxy_model(model), "max_tokens": max_tokens,
            "messages": messages_of(system, messages), "tools": tool_specs(tools)}
    sampling = _sampling(temperature, None, None)
    shown_in = "\n\n".join(str(getattr(m, "content", ""))[:2000] for m in messages)
    start = time.perf_counter()
    try:
        data, headers = await _adapted(body, sampling, name, model)
        msg = ai_message(data)
    except Exception as e:
        if type(e).__name__ == "WorkflowCancelled":
            raise
        _meter(model, None, {}, system, "", "", 0, mode="error", temperature=temperature,
               max_tokens=max_tokens, name=name)
        raise _failed(name, model, e, " (choosing tools)") from e
    latency_ms = int((time.perf_counter() - start) * 1000)
    said = _content_text(msg.content)
    calls = "; ".join(f"{c.get('name')}({json.dumps(c.get('args'), default=str)[:300]})"
                      for c in msg.tool_calls or [])
    _meter(model, msg, headers, system, shown_in, said + (f"\n[tool calls] {calls}" if calls else ""),
           latency_ms, mode="litellm", temperature=temperature, max_tokens=max_tokens, name=name)
    return msg
