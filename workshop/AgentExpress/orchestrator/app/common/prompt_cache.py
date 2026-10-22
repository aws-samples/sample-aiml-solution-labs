"""Bedrock prompt caching (Converse cache points) for one agent's model calls.

An agent opts in with `agents.<id>.promptCache` in workflow.json:
`{"system": bool, "tools": bool, "messages": bool, "ttl": "5m" | "1h"}`; absent = off.
Only what the CALLED model supports is sent: the per-model rules come from the
`promptCacheModels` table in app/vocabulary.json, copied from the AWS table "Supported
models, Regions, and explicit caching limits"
(https://docs.aws.amazon.com/bedrock/latest/userguide/prompt-caching.html). A model not in
that table gets no cache point: support is never guessed.

OpenAI GPT-5.6+ rows (`"api": "InvokeModel"`, vocabulary promptCacheModels.openaiExplicit)
are not Converse cache points: the system prompt of a single call gets a
`prompt_cache_breakpoint` in an InvokeModel Chat Completions body (openai_body), ttl "30m".

Pure functions only (no AWS call); app/common/llm.py applies them to the request and
reads the cache counts back for telemetry.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

_VOCAB = Path(__file__).resolve().parent.parent / "vocabulary.json"
_table: dict | None = None


def table() -> dict:
    """The `promptCacheModels` block of app/vocabulary.json (loaded once)."""
    global _table
    if _table is None:
        _table = json.loads(_VOCAB.read_text()).get("promptCacheModels") or {}
    return _table


def base_id(model: str) -> str:
    """The base model id of a model id, inference-profile id or ARN.

    `global.anthropic.claude-haiku-4-5-20251001-v1:0` and `us.anthropic...` both resolve to
    `anthropic.claude-haiku-4-5-20251001-v1:0`: a system-defined inference profile id is
    its geography prefix (vocabulary `inferenceProfileGeos`) plus the base id. An
    application inference profile ARN ends in an opaque id, so it stays unknown."""
    mid = str(model or "").strip()
    if "/" in mid:
        mid = mid.rsplit("/", 1)[-1]
    from app.common import vocabulary
    head, _, rest = mid.partition(".")
    if rest and head.lower() in vocabulary.INFERENCE_PROFILE_GEOS:
        return rest
    return mid


def support(model: str) -> dict | None:
    """The table row for the model (exact base-id match), or None: no explicit caching."""
    if str(model or "").startswith("litellm/"):
        return None
    return (table().get("byModel") or {}).get(base_id(model))


def settings(spec) -> dict | None:
    """An agent's `promptCache` as {"system", "tools", "messages", "ttl"}; None when off."""
    if not isinstance(spec, dict):
        return None
    out = {k: spec.get(k) is True for k in ("system", "tools", "messages")}
    if not any(out.values()):
        return None
    out["ttl"] = spec.get("ttl") if spec.get("ttl") in ("1h", "30m") else "5m"
    return out


@dataclass(frozen=True)
class Plan:
    """The cache points one request carries."""
    sections: tuple[str, ...] = ()     # in processing order: tools, system, messages
    point: dict = field(default_factory=dict)
    rule: str = ""                     # where the messages point goes: newest | first_user
    ttl: str = ""                      # the ttl sent ("" = field omitted: the 5m default)
    min_text: str = ""                 # the table's minimum tokens per checkpoint, verbatim
    api: str = ""                      # "" = Converse cache points; "InvokeModel" = OpenAI body

    def has(self, section: str) -> bool:
        return section in self.sections


OFF = Plan()

#: The OpenAI explicit cache breakpoint and its options, from
#: https://docs.aws.amazon.com/bedrock/latest/userguide/prompt-caching.html ("Prompt caching
#: for models from OpenAI"): `"prompt_cache_breakpoint": {"mode": "explicit"}` on a supported
#: content block (text content parts for Chat Completions and InvokeModel), and
#: `prompt_cache_options.mode` "explicit" (only explicit breakpoints are used: no automatic
#: one on the latest message, whose writes a single call never reads back) with
#: `prompt_cache_options.ttl` (default and only documented value "30m").
OPENAI_BREAKPOINT = {"prompt_cache_breakpoint": {"mode": "explicit"}}
OPENAI_TTL = "30m"


def plan(model: str, spec, *, call: str, has_tools: bool = False) -> Plan:
    """The cache points for one call of `model` (the model actually called, after
    routing), given the agent's `promptCache` and the call shape: "single" (run_llm, one
    user message) or "tools" (run_llm_tools, a tool-calling turn).

    Rules, from https://docs.aws.amazon.com/bedrock/latest/userguide/prompt-caching.html
    (the table and its notes):
      * fields: only those the model's row lists (Amazon Nova: system and messages; no
        tool definitions);
      * checkpoints are processed tools -> system -> messages and the minimum tokens per
        checkpoint is cumulative; below it the call still succeeds, uncached;
      * at most `maxCheckpoints` (4) per request: this plan uses at most 3;
      * `ttl`: "1h" only where the row lists it. For models with only the 5-minute TTL
        the field is OMITTED ("Including the field for these models can result in a
        ValidationException"), and no ttl means the default 5 minutes.
    """
    s = settings(spec)
    row = support(model) if s else None
    if not s or not row:
        return OFF
    if row.get("api") == "InvokeModel":
        # OpenAI GPT-5.6+ (vocabulary promptCacheModels.openaiExplicit): only a single call
        # goes through InvokeModel; a tool-calling turn stays on Converse, where "Converse
        # doesn't support explicit caching for this model" (same page, table).
        if call != "single" or not s["system"] or "system" not in (row.get("fields") or ()):
            return OFF
        return Plan(("system",), dict(OPENAI_BREAKPOINT), "", OPENAI_TTL,
                    str(row.get("minTokensText") or ""), "InvokeModel")
    fields = set(row.get("fields") or ())
    sections = []
    if s["tools"] and call == "tools" and has_tools and "tools" in fields:
        sections.append("tools")
    if s["system"] and "system" in fields:
        sections.append("system")
    if s["messages"] and call == "tools" and "messages" in fields:
        sections.append("messages")
    rule = "first_user" if base_id(model).startswith("amazon.nova") else "newest"
    if rule == "first_user" and "system" in sections and "messages" in sections:
        # Observed live 2026-10-21, dev account (.agents/tasks/prompt-cache-2026-10-21/
        # probe-tool-loop-results.json, key nova_tools_two_cps_small_system): Nova with a
        # system point and a message point wrote every call and never read. Not stated in
        # the AWS docs; one point (on the first user message, which follows the system
        # prompt and so covers it) is what read back.
        sections.remove("system")
    if not sections:
        return OFF
    sections = sections[: max(0, int(row.get("maxCheckpoints") or 0))]
    # CachePointBlock: `type` "default" (required), `ttl` "5m" | "1h" (optional):
    # https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_CachePointBlock.html
    ttl = "1h" if s["ttl"] == "1h" and "1h" in (row.get("ttls") or ()) else ""
    point = {"cachePoint": {"type": "default", **({"ttl": ttl} if ttl else {})}}
    return Plan(tuple(sections), point, rule, ttl, str(row.get("minTokensText") or ""))


class SystemPrompt(str):
    """An agent's system prompt with a cache boundary: `static` (instructions and skills)
    is cached, `dynamic` (recalled memory, today's date) follows the cache point. Its str
    value is exactly static + "\\n\\n" + dynamic, so every string caller is unchanged."""
    static: str
    dynamic: str

    def __new__(cls, static: str, dynamic: str = ""):
        s = super().__new__(cls, f"{static}\n\n{dynamic}" if dynamic else static)
        s.static, s.dynamic = static, dynamic
        return s


def system_content(system: str, p: Plan) -> list[dict] | None:
    """Converse `system` blocks with a cache point, or None when none is planned.

    A cache point is its own SystemContentBlock, never a key beside `text`:
    https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_SystemContentBlock.html
    """
    if not p.has("system") or not str(system or "").strip():
        return None
    # By attribute, not isinstance: a module reloaded in tests makes a second SystemPrompt class.
    if isinstance(getattr(system, "static", None), str):
        blocks = [{"type": "text", "text": system.static}, dict(p.point)]
        if system.dynamic:
            blocks.append({"type": "text", "text": system.dynamic})
        return blocks
    return [{"type": "text", "text": str(system)}, dict(p.point)]


def tools_with_point(specs: list, p: Plan) -> list:
    """The tool list with a cache point after the last tool (its own Tool union member:
    https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_Tool.html).
    langchain-aws 1.8.0 bind_tools passes a {"cachePoint": ...} entry through as it is."""
    if not p.has("tools") or not specs:
        return specs
    return [*specs, dict(p.point)]


def _with_point(msg, point: dict):
    content = msg.content
    blocks = ([{"type": "text", "text": content}] if isinstance(content, str)
              else list(content or []))
    return msg.model_copy(update={"content": [*blocks, dict(point)]})


def with_message_point(convo: list, p: Plan) -> list:
    """A NEW message list with one cache point for "Conversation so far"; the caller's
    list and messages are never mutated.

    `newest` (Claude): at the end of the last message (the user turn the model answers),
    the simplified cache management pattern in the Anthropic section of
    https://docs.aws.amazon.com/bedrock/latest/userguide/prompt-caching.html: each turn reads
    the previous turns' prefix and writes the new tail. langchain-aws places a cache point
    found in a ToolMessage after its toolResult, in the same user turn.

    `first_user` (Amazon Nova): at the end of the FIRST human message, never in a turn
    holding a toolResult. Observed live 2026-10-21, dev account
    (.agents/tasks/prompt-cache-2026-10-21/probe-tool-loop-results.json, key nova_lite_us):
    Nova rejects a cachePoint in a toolResult turn ("extraneous key [cachePoint] is not
    permitted"). Not stated in the AWS docs.
    """
    if not p.has("messages") or not convo:
        return convo
    from langchain_core.messages import HumanMessage, ToolMessage
    out = list(convo)
    if p.rule == "first_user":
        for i, m in enumerate(out):
            if isinstance(m, HumanMessage):
                out[i] = _with_point(m, p.point)
                break
        return out
    if isinstance(out[-1], (HumanMessage, ToolMessage)):
        out[-1] = _with_point(out[-1], p.point)
    return out


def usage_of(msg) -> dict:
    """The cache token counts of a ChatBedrockConverse reply: {"read", "write", "write5m",
    "write1h"}. Bedrock's TokenUsage carries cacheReadInputTokens, cacheWriteInputTokens
    and cacheDetails (writes by TTL)
    (https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_TokenUsage.html).
    langchain-aws 1.8.0 (`_extract_usage_metadata`,
    https://github.com/langchain-ai/langchain-aws/blob/langchain-aws%3D%3D1.8.0/libs/aws/langchain_aws/chat_models/bedrock_converse.py)
    maps them to usage_metadata.input_token_details: cache_read, cache_creation, and
    ephemeral_5m/1h_input_tokens, setting cache_creation to 0 when the TTL split is
    present. Its usage_metadata.input_tokens is already inputTokens + read + write (the
    total input), so these counts are a breakdown of it, never added to it."""
    usage = (getattr(msg, "usage_metadata", None) or {}) if msg is not None else {}
    d = usage.get("input_token_details") or {}
    read = int(d.get("cache_read") or 0)
    w5 = int(d.get("ephemeral_5m_input_tokens") or 0)
    w1 = int(d.get("ephemeral_1h_input_tokens") or 0)
    write = int(d.get("cache_creation") or 0) + w5 + w1
    return {"read": read, "write": write, "write5m": w5, "write1h": w1}


def split_ttl(usage: dict, model: str, p: Plan) -> tuple[int, int, int, int]:
    """(5m writes, 1h writes, 30m writes, writes of unknown TTL). Without a cacheDetails
    split, a write is 5m when this request sent cache points with no `ttl` (the documented
    default) or the model's row lists only 5m, and 30m when the row lists only 30m (OpenAI
    GPT-5.6+: "30-minute minimum TTL", the only TTL documented for them); otherwise its TTL
    is unknown."""
    w5, w1, write = usage["write5m"], usage["write1h"], usage["write"]
    rest = max(0, write - w5 - w1)
    if rest:
        row = support(model) or {}
        ttls = list(row.get("ttls") or ())
        if ttls == ["30m"]:
            return w5, w1, rest, 0
        if (p.sections and not p.ttl) or ttls == ["5m"]:
            return w5 + rest, w1, 0, 0
    return w5, w1, 0, rest


def refused(error: Exception) -> bool:
    """Bedrock refused the cache point itself (a model whose support changed after the
    table was copied): the call is retried once without it."""
    text = str(error).lower()
    return "cachepoint" in text or "prompt_cache" in text or ("ttl" in text and "cach" in text)


# --- OpenAI GPT-5.6+ through InvokeModel (Plan.api == "InvokeModel") -------------------

def openai_body(system, user: str, p: Plan, *, max_tokens: int, sampling: dict) -> dict:
    """The InvokeModel request body for an OpenAI model: an OpenAI Chat Completions
    request ("In an InvokeModel or OpenAI Chat Completions request, include the fields in
    the request body"; `max_completion_tokens`, `temperature`, `top_p`, and the system
    prompt as a `system` message:
    https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-openai.html).
    `stream` and `model` are omitted: InvokeModel fills them in from the operation and
    the modelId (same page). The breakpoint goes on the static system text part (its own
    content part, so recalled memory and the date after it stay uncached), with
    prompt_cache_options (https://docs.aws.amazon.com/bedrock/latest/userguide/prompt-caching.html)."""
    static = system.static if isinstance(getattr(system, "static", None), str) else str(system or "")
    dynamic = system.dynamic if isinstance(getattr(system, "dynamic", None), str) else ""
    parts = [{"type": "text", "text": static, **dict(p.point)}]
    if dynamic:
        parts.append({"type": "text", "text": dynamic})
    body = {"messages": [{"role": "system", "content": parts},
                         {"role": "user", "content": [{"type": "text", "text": user}]}],
            "max_completion_tokens": int(max_tokens),
            "prompt_cache_options": {"mode": "explicit", "ttl": p.ttl or OPENAI_TTL}}
    if "temperature" in sampling:
        body["temperature"] = sampling["temperature"]
    if "top_p" in sampling:
        body["top_p"] = sampling["top_p"]
    return body


def openai_message(reply: dict):
    """An InvokeModel reply (the OpenAI chat completion object, same page) as the
    langchain AIMessage the rest of llm.py reads: the text, `finish_reason`, and
    usage_metadata in langchain's shape. `usage.prompt_tokens` is the total input (it
    includes the cached and written tokens; checked live: 3,553 prompt tokens with 3,542
    cached), so the cache counts are a breakdown of it, as on Converse.
    `prompt_tokens_details.cached_tokens` is the cache read and `cache_write_tokens` the
    cache write (https://docs.aws.amazon.com/bedrock/latest/userguide/prompt-caching.html,
    "Understanding the response"). A `<reasoning>` block that precedes the text on
    InvokeModel (model-parameters-openai.html, "OpenAI response body") is dropped."""
    import re

    from langchain_core.messages import AIMessage
    choice = (reply.get("choices") or [{}])[0] or {}
    text = str((choice.get("message") or {}).get("content") or "")
    text = re.sub(r"^\s*<reasoning>.*?</reasoning>\s*", "", text, flags=re.DOTALL)
    usage = reply.get("usage") or {}
    details = usage.get("prompt_tokens_details") or {}
    total_in = int(usage.get("prompt_tokens") or 0)
    out = int(usage.get("completion_tokens") or 0)
    return AIMessage(
        content=text,
        response_metadata={"finish_reason": str(choice.get("finish_reason") or "")},
        usage_metadata={"input_tokens": total_in, "output_tokens": out,
                        "total_tokens": int(usage.get("total_tokens") or total_in + out),
                        "input_token_details": {
                            "cache_read": int(details.get("cached_tokens") or 0),
                            "cache_creation": int(details.get("cache_write_tokens") or 0)}})
