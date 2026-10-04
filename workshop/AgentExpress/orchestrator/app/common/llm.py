"""LLM helper. Calls Claude on Amazon Bedrock.

A failed model call RAISES ModelUnavailable — it is never substituted with
placeholder text. See app/common/errors.py for why: fabricated output is
indistinguishable from real evidence once it reaches the report.

The model, temperature, and max_tokens are per-call, so each agent can use a
different model (configured in workflow.json).

`name` names the model call. An agent may issue several distinct prompts; the
name is carried onto the telemetry row and the captured prompt so observability
and AgentCore Evaluations can scope to ONE prompt at a time.
"""

import contextlib
import json
import os

from app.common.config import REGION, model_for
from app.common.errors import ModelUnavailable

#: How long a model call may take to answer. boto3's default read timeout is 60 s, and a
#: Converse call returns nothing until the whole reply is written — so an agent with a
#: large maxTokens (an 8000-token report on Claude Haiku 4.5 takes ~60-80 s) failed with
#: ReadTimeoutError on a run where a shorter answer would have passed. One retry only:
#: each attempt is a billed model call.
READ_TIMEOUT_S = int(os.getenv("BEDROCK_READ_TIMEOUT_SECONDS", "300"))
_config = None


def _client_config():
    global _config
    if _config is None:
        from botocore.config import Config
        _config = Config(read_timeout=READ_TIMEOUT_S, connect_timeout=10,
                         retries={"total_max_attempts": 2, "mode": "standard"})
    return _config


def _text_of(content) -> str:
    """Normalise an AIMessage.content to plain text. ChatBedrockConverse (the
    Bedrock Converse API) returns a LIST of typed content blocks
    (e.g. [{"type": "text", "text": "..."}], plus optional reasoning/tool blocks),
    not a string. Concatenate the text blocks so downstream JSON parsing works;
    a naive str(list) would yield a Python repr that can't be parsed."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            # only real text blocks (skip reasoning/tool_use/etc.)
            elif (isinstance(block, dict) and isinstance(block.get("text"), str)
                  and block.get("type", "text") == "text"):
                parts.append(block["text"])
        if parts:
            return "".join(parts)
    return content if isinstance(content, str) else str(content)


def _refused_setting(error: Exception, sampling: dict) -> str | None:
    """The sampling setting a model refused, if that is what `error` says.

    Newer models refuse settings older ones take: Claude Sonnet 5 rejects any
    `temperature` ("temperature is deprecated for this model"), and several Claude 4.x
    models take a temperature or a top_p but not both ("temperature and top_p cannot
    both be specified"). The agent's config is right for the model it was written for,
    so the call is retried without the refused setting rather than failing the run.
    """
    text = str(error).lower()
    refused = ("deprecated", "not supported", "cannot both", "only one", "doesn't support",
               "does not support", "unsupported")
    if "temperature" in sampling and "temperature" in text and any(r in text for r in refused):
        return "temperature"
    if "top_p" in sampling and ("top_p" in text or "topp" in text) and any(
            r in text for r in refused):
        return "top_p"
    return None


def _token_limit(error: Exception) -> int | None:
    """The model's output-token ceiling, when that is why the call was refused: Llama 3
    takes at most 2048 ("exceeds the model limit of 2048"). The call is retried at the
    ceiling, so a budget written for a bigger model does not fail the run; a reply that
    then reaches it is reported as truncated like any other."""
    import re
    m = re.search(r"model limit of (\d+)", str(error))
    return int(m.group(1)) if m else None


def _no_system_prompt(error: Exception) -> bool:
    """Some models (Mistral 7B Instruct, Mixtral 8x7B) take no system message."""
    return "doesn't support system messages" in str(error).lower()


async def run_llm(name: str, system: str, user: str,
                  model: str | None = None, temperature: float = 0,
                  max_tokens: int = 300, top_p: float | None = None,
                  stop_sequences: list[str] | None = None,
                  images: list[dict] | None = None) -> tuple[str, bool]:
    """Call the model and return (text, hit_token_ceiling).

    The second value is the one that is easy to lose and expensive to lose. When a
    response stops because it reached `max_tokens` rather than because the model
    finished, the text is a PREFIX — for a structured agent that means JSON cut off
    mid-object. `assets.extract_json` deliberately repairs that into a usable partial
    asset rather than failing the run, which is the right trade, but it means the only
    remaining evidence that anything was lost is this flag. Observed live: an
    `analysis` call stopped at exactly its 6000-token budget, mid-way through writing
    a `sources` entry; the asset validated, the timeline said "Analysis complete", and
    a reviewer approved it at the gate with no way to know the tail was missing.
    """
    model = model_for(model)
    # `images` ({key, format, bytes}, from images.load_for) go to the model as Converse
    # image content after the text. The logs and telemetry name them, never carry them.
    images = list(images or [])
    shown = user + (f"\n\n[{len(images)} image(s): {', '.join(i['key'] for i in images)}]"
                    if images else "")
    # Capture this call's prompt (system + the real source inputs) so the agent's
    # AGENT span carries it as gen_ai.task.input for AgentCore Evaluations, and so
    # per-prompt evaluation can find it. No-op outside an agent run.
    with contextlib.suppress(Exception):
        from app.features.observability import otel as _otel
        _otel.capture_prompt(name, system, shown)
    sampling: dict = {"temperature": temperature}
    if top_p is not None:
        sampling["top_p"] = top_p
    try:
        import time as _t

        from langchain_aws import ChatBedrockConverse
        _start = _t.perf_counter()
        # Models differ in what a call may carry. Each refusal below is one the model
        # names precisely, so the call is adjusted and retried; any other error fails it.
        system_in_user = False
        for _attempt in range(5):
            llm = ChatBedrockConverse(model=model, region_name=REGION, max_tokens=max_tokens,
                                      config=_client_config(), **sampling,
                                      **({"stop_sequences": list(stop_sequences)}
                                         if stop_sequences else {}))
            if images:
                from langchain_core.messages import HumanMessage, SystemMessage
                # A block with no "type" is passed to Converse as it is.
                blocks = [{"image": {"format": i["format"], "source": {"bytes": i["bytes"]}}}
                          for i in images]
                text = f"{system}\n\n{user}" if system_in_user else user
                messages = ([] if system_in_user else [SystemMessage(content=system)]) + [
                    HumanMessage(content=[{"type": "text", "text": text}, *blocks])]
            else:
                messages = ([("human", f"{system}\n\n{user}")] if system_in_user
                            else [("system", system), ("human", user)])
            try:
                msg = await llm.ainvoke(messages)
                break
            except Exception as refusal:
                dropped = _refused_setting(refusal, sampling)
                limit = _token_limit(refusal)
                if dropped:
                    print(f"[llm] {model} refused `{dropped}`; calling '{name}' without it")
                    sampling.pop(dropped)
                elif limit and limit < max_tokens:
                    print(f"[llm] {model} takes at most {limit} output tokens; calling "
                          f"'{name}' with {limit} instead of {max_tokens}")
                    max_tokens = limit
                elif _no_system_prompt(refusal) and not system_in_user:
                    print(f"[llm] {model} takes no system message; sending '{name}' its "
                          f"instructions in the user message")
                    system_in_user = True
                else:
                    raise
        _latency_ms = int((_t.perf_counter() - _start) * 1000)
        out_text = _text_of(msg.content)
        _meter_llm(model, msg, system, shown, out_text, _latency_ms, mode="bedrock",
                   temperature=temperature, max_tokens=max_tokens, name=name)
        with contextlib.suppress(Exception):
            from app.features.observability import otel as _otel
            _otel.capture_output(out_text)  # pair this call's response with its prompt
        return out_text, _finish_reason_of(msg).lower() in _TRUNCATED_REASONS
    except Exception as e:
        # Record the failure, then fail the run — see the module docstring: a model
        # call that did not happen must never look like one that returned nothing.
        _meter_llm(model, None, system, shown, "", 0, mode="error",
                   temperature=temperature, max_tokens=max_tokens, name=name)
        raise ModelUnavailable(
            f"Bedrock model call '{name}' failed on {model}: {type(e).__name__}: {e}. "
            f"Check that the region has model access enabled for this model id and that "
            f"the runtime's credentials permit bedrock:InvokeModel."
            + (f" This call carried {len(images)} image(s) (the agent's `vision`), so the "
               f"model must accept image input." if images else "")
        ) from e


async def run_llm_tools(name: str, system: str, messages: list, tools: list[dict],
                        model: str | None = None, temperature: float = 0,
                        max_tokens: int = 1500):
    """One model turn that may call tools (toolMode "model", app/common/tool_loop.py).

    `messages` are langchain messages after the system prompt (HumanMessage, AIMessage,
    ToolMessage); `tools` are {"name", "description", "input_schema"}. Returns the
    AIMessage: its `tool_calls` are what the model chose, its text what it said. The
    same model refusals as run_llm are adapted to, and the turn is metered like any
    other model call."""
    model = model_for(model)
    sampling: dict = {"temperature": temperature}
    specs = [{"type": "function", "function": {"name": t["name"], "description": t["description"],
                                               "parameters": t["input_schema"]}} for t in tools]
    try:
        import time as _t

        from langchain_aws import ChatBedrockConverse
        from langchain_core.messages import HumanMessage, SystemMessage
        _start = _t.perf_counter()
        system_in_user = False
        for _attempt in range(5):
            llm = ChatBedrockConverse(model=model, region_name=REGION, max_tokens=max_tokens,
                                      config=_client_config(), **sampling).bind_tools(specs)
            if system_in_user:
                first, rest = messages[0], messages[1:]
                convo = [HumanMessage(content=f"{system}\n\n{first.content}"), *rest]
            else:
                convo = [SystemMessage(content=system), *messages]
            try:
                msg = await llm.ainvoke(convo)
                break
            except Exception as refusal:
                dropped = _refused_setting(refusal, sampling)
                limit = _token_limit(refusal)
                if dropped:
                    sampling.pop(dropped)
                elif limit and limit < max_tokens:
                    max_tokens = limit
                elif _no_system_prompt(refusal) and not system_in_user:
                    system_in_user = True
                else:
                    raise
        latency_ms = int((_t.perf_counter() - _start) * 1000)
        said = _text_of(msg.content)
        calls = "; ".join(f"{c.get('name')}({json.dumps(c.get('args'), default=str)[:300]})"
                          for c in getattr(msg, "tool_calls", None) or [])
        shown_in = "\n\n".join(str(getattr(m, "content", ""))[:2000] for m in messages)
        _meter_llm(model, msg, system, shown_in, said + (f"\n[tool calls] {calls}" if calls else ""),
                   latency_ms, mode="bedrock", temperature=temperature, max_tokens=max_tokens,
                   name=name)
        return msg
    except Exception as e:
        _meter_llm(model, None, system, "", "", 0, mode="error", temperature=temperature,
                   max_tokens=max_tokens, name=name)
        raise ModelUnavailable(
            f"Bedrock model call '{name}' (choosing tools) failed on {model}: "
            f"{type(e).__name__}: {e}. The model must support tool use through Converse."
        ) from e


# Stop reasons that mean "I ran out of room", not "I finished". Bedrock Converse says
# `max_tokens`; the two spellings cover the other providers langchain-aws fronts, so a
# model swap in workflow.json does not quietly turn this check off.
_TRUNCATED_REASONS = frozenset({"max_tokens", "max_token", "length"})


def _finish_reason_of(msg) -> str:
    """Best-effort stop reason from a ChatBedrockConverse response."""
    if msg is None:
        return ""
    meta = getattr(msg, "response_metadata", None) or {}
    return str(meta.get("stopReason") or meta.get("finish_reason") or "")


def _meter_llm(model, msg, system, user, output_text, latency_ms, mode,
               temperature: float = 0.0, max_tokens: int = 0, name: str = "") -> None:
    """Best-effort observability hook (isolated in app/features/observability)."""
    with contextlib.suppress(Exception):  # metering must never break a call
        usage = getattr(msg, "usage_metadata", None) or {} if msg is not None else {}
        input_tokens = int(usage.get("input_tokens", 0) or 0)
        output_tokens = int(usage.get("output_tokens", 0) or 0)
        # Stamp real token usage onto the OTEL agent span (ADOT's auto "chat"
        # span reports 0 for Converse as of 0.19.0). Accumulates across the
        # agent's calls; surfaces as gen_ai.usage.* in GenAI Observability.
        from app.features.observability import otel
        otel.add_tokens(input_tokens, output_tokens)
        from app.features.observability import meter
        meter.record_llm(
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            system_text=system, latency_ms=latency_ms, mode=mode,
            user_input=user, output_text=output_text,
            temperature=temperature, max_tokens=max_tokens,
            finish_reason=_finish_reason_of(msg), prompt=name,
        )
