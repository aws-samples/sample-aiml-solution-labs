"""Model-driven tool calls: an agent with `toolMode: "model"` lets its model decide which
of its tools to call, with what arguments, one result at a time.

`evidence.gather` hands over to `gather` here, and gets back the same thing it would
have built itself — labelled evidence blocks, and the limitations to report — so every
agent (plain, Strands, LangGraph, the research agents) keeps its prompt, its schema and
its source checks unchanged. Only WHO chooses the calls changes:

  direct  every bound tool is queried once, with the request, before the model reasons;
  model   the model is shown each bound tool (name, description, input schema) and calls
          what it needs — reading each result before deciding the next — until it says it
          has enough, or it reaches `maxToolCalls`.

What does not change, because every call still goes through the Gateway: the Cedar
permit (a denied call is refused by infrastructure and the model is told so), the
Knowledge Base corpus scope (forced here, whatever the model asks for), a tool's fixed
`args` (they win over the model's), the timeout, and the telemetry, span and policy
record of each call. Each call is also written to the run's timeline.
"""

from __future__ import annotations

import json
import re

from app.common.config import TOOLS
from app.common.errors import ToolDenied, ToolUnavailable
from app.common.sink import WorkflowCancelled, is_cancelled

#: Bedrock's tool-name rule.
_NAME_OK = re.compile(r"[^a-zA-Z0-9_-]")
#: What one result may put in front of the model (and into the evidence).
RESULT_CHARS = 4000
#: Why a bound Gateway tool can be missing from the listing. The second cause is easy to
#: miss: AgentCore Policy filters tools/list, so a forbid with no condition (the guided
#: form's "Always") removes the tool from what any agent can see, not just from what it
#: may call.
_HIDDEN_WHY = ("Either its target is not READY, or a policy forbids it outright: the Gateway "
               "hides a tool that a forbid with no condition always blocks.")

SYSTEM = (
    "You gather the evidence an agent needs, by calling its tools. You are working for the "
    "agent named below; the user's request and the agent's role follow.\n"
    "- Call the tools that can answer the request, with specific arguments. Read each "
    "result before deciding the next call: refine a query that found nothing, follow up on "
    "what a result names, stop repeating what already answered.\n"
    "- Call only the tools you are given, with arguments that match their schemas. When a "
    "tool works on something an earlier step produced (a draft, a brief, a list), pass it "
    "from the upstream outputs below, in full.\n"
    "- When you have what the agent needs — or the tools cannot provide more — stop calling "
    "tools and reply with one line saying what you found and what is missing.\n"
    "- Never invent a result. A tool that is refused or fails is reported to you; do not "
    "try to work around a refusal.")


def _safe(name: str, taken: set[str]) -> str:
    base = _NAME_OK.sub("_", name)[:64] or "tool"
    out, n = base, 2
    while out in taken:
        out = f"{base[:60]}_{n}"
        n += 1
    taken.add(out)
    return out


async def _specs(ctx, keys: list[str]) -> tuple[list[dict], dict[str, tuple]]:
    """The tool specs the model sees, and how each name routes to a call."""
    from app.features.gateway import client
    specs: list[dict] = []
    routes: dict[str, tuple] = {}
    taken: set[str] = set()
    gateway_keys = [k for k in keys
                    if str((TOOLS.get(k) or {}).get("type", "mcp")).lower() not in ("kb", "websearch")]
    published = await client.published_for(gateway_keys) if gateway_keys else {}
    for key in keys:
        entry = TOOLS.get(key) or {}
        kind = str(entry.get("type", "mcp")).lower()
        about = str(entry.get("description") or key)
        if kind == "kb":
            corpus = getattr(ctx, "corpus", None)
            name = _safe(f"{key}___retrieve", taken)
            specs.append({"name": name, "description": (
                f"Search the knowledge base '{key}'{f' (corpus {corpus})' if corpus else ''} for "
                f"passages about a query. {about}")[:1000],
                "input_schema": {"type": "object", "required": ["query"], "properties": {
                    "query": {"type": "string", "description": "What to search for."}}}})
            routes[name] = ("kb", key, None)
        elif kind == "websearch":
            name = _safe(f"{key}___search", taken)
            specs.append({"name": name, "description": f"Search the web. {about}"[:1000],
                          "input_schema": {"type": "object", "required": ["query"], "properties": {
                              "query": {"type": "string",
                                        "description": "The search query, under 200 characters."}}}})
            routes[name] = ("web", key, None)
        else:
            for tool in published.get(key, []):
                name = _safe(tool.name, taken)
                desc = str(getattr(tool, "description", "") or about)
                specs.append({"name": name, "description": desc[:1000],
                              "input_schema": client.input_schema_of(tool)})
                routes[name] = ("gateway", key, tool)
    return specs, routes


async def _call(ctx, route: tuple, args: dict) -> tuple[str, str]:
    from app.features.gateway import client
    kind, key, tool = route
    query = str((args or {}).get("query") or "").strip() or ctx._retrieval_query()
    if kind == "kb":
        # The agent's corpus, whatever the model asked for: the scope is config.
        return await ctx.retrieve(query, doc_type=getattr(ctx, "corpus", None))
    if kind == "web":
        return await ctx.call_tool(key, query)
    return await client.invoke_chosen(key, tool, args or {})


#: How much of the earlier steps' output the tool-calling turn is shown, in all.
UPSTREAM_CHARS = 24000


def _upstream(ctx) -> str:
    try:
        from app.common.config import upstream_of
        ids = upstream_of(ctx.agent_id)
    except Exception:  # noqa: BLE001 - an agent outside the topology has no upstream
        return ""
    blocks, used = [], 0
    for aid in ids:
        raw = ctx.input(aid) if hasattr(ctx, "input") else None
        if not raw:
            continue
        block = f"--- {aid.replace('_', ' ').upper()} ---\n{raw}"
        if used + len(block) > UPSTREAM_CHARS:
            blocks.append(f"[... {aid} and earlier steps omitted: {UPSTREAM_CHARS}-character budget]")
            break
        blocks.append(block)
        used += len(block)
    return "\n\n".join(blocks)


def _task(ctx, query: str) -> str:
    from app.common.context import today_line
    parts = [f"Agent: {getattr(ctx, 'agent_name', ctx.agent_id)} ({ctx.agent_id})", today_line(),
             f"=== REQUEST ===\n{ctx.topic}"]
    if query and query.strip() != str(ctx.topic).strip():
        parts.append(f"=== WHAT THIS AGENT NEEDS TO FIND ===\n{query}")
    # The agent's own role and instructions, so "always call X" or "check only the top
    # 8 keywords" reaches the turn that makes the calls. Without it, observed live: an
    # SEO agent told to always call an API never did, and an editor told to run a
    # checker wrote the call out as text in its answer instead.
    role = str(getattr(ctx, "agent_prompt", "") or "").strip()
    if role:
        parts.append(f"=== THE AGENT'S INSTRUCTIONS (follow what they say about tools) ===\n"
                     f"{role[:6000]}")
    # What earlier steps approved: a tool may act ON it (check a draft, price a brief),
    # and without it the model had nothing to pass — observed live, an editor told to
    # run a checker on the draft skipped it and wrote the call out as text instead.
    upstream = _upstream(ctx)
    if upstream:
        parts.append(f"=== APPROVED UPSTREAM OUTPUTS (pass from these when a tool needs them) ===\n{upstream}")
    feedback = getattr(ctx, "feedback", None)
    if feedback:
        parts.append(f"=== REVIEWER GUIDANCE ===\n{feedback}")
    return "\n\n".join(parts)


async def gather(ctx, query: str, *, tools: list[str],
                 labels: dict[str, str] | None = None) -> tuple[str, list[str]]:
    """Let the model call the agent's tools (`tools`, its bound keys). Returns
    (evidence, limitations), the shape evidence.gather returns; `labels` names each
    tool type in the evidence headings, as that caller does."""
    from langchain_core.messages import HumanMessage, ToolMessage

    from app.common.llm import run_llm_tools
    specs, routes = await _specs(ctx, list(tools))
    offered = {key for _kind, key, _tool in routes.values()}
    hidden = [k for k in tools if k not in offered]
    if not specs:
        # Nothing to call means nothing to reason from. Carrying on would hand the agent
        # an empty evidence block, and a model with a task and no evidence writes one —
        # observed live: it "called" the tool in prose and reported made-up numbers. So
        # this fails like direct mode does for an unpublished tool.
        raise ToolUnavailable(f"The Gateway lists none of this agent's tools ({', '.join(tools)}). "
                              + _HIDDEN_WHY)
    messages: list = [HumanMessage(content=_task(ctx, query))]
    parts: list[str] = []
    limitations: list[str] = [f"'{k}' was not offered: the Gateway lists none of its tools. {_HIDDEN_WHY}"
                              for k in hidden]
    calls = 0
    budget = max(1, int(getattr(ctx, "max_tool_calls", 6) or 6))
    for _turn in range(budget + 1):
        if is_cancelled(ctx.session_id):
            raise WorkflowCancelled(ctx.session_id)
        # Room for an argument that carries a whole draft, not just a search query.
        ai = await run_llm_tools(f"{ctx.agent_id}.tools", SYSTEM, messages, specs,
                                 model=getattr(ctx, "model", None),
                                 max_tokens=min(int(getattr(ctx, "max_tokens", 1500) or 1500), 8000))
        messages.append(ai)
        wanted = list(getattr(ai, "tool_calls", None) or [])
        if not wanted:
            break
        for call in wanted:
            name, args = str(call.get("name") or ""), call.get("args") or {}
            route = routes.get(name)
            if route is None:
                text = f"There is no tool named {name}. Use one of: {', '.join(routes)}."
            elif calls >= budget:
                text = (f"Not called: this agent may make at most {budget} tool calls, and has. "
                        f"Answer with what you have.")
            else:
                calls += 1
                _kind, key, _tool = route
                shown = json.dumps(args, ensure_ascii=False, default=str)[:300]
                await ctx.log(f"Tool call {calls}/{budget}: {name}({shown})")
                try:
                    found, mode = await _call(ctx, route, args)
                except ToolDenied as e:
                    found, mode = "", "denied"
                    text = f"REFUSED by the policy: {e}"
                    limitations.append(f"'{name}' was refused by the Gateway's policy for {shown}.")
                    # On the timeline too: the refusal is the policy working, and it
                    # was visible only to the model.
                    await ctx.log(f"Refused by Cedar policy: {name}({shown})")
                except ToolUnavailable as e:
                    found, mode = "", "error"
                    text = f"FAILED: {e}"
                    limitations.append(f"'{name}' failed: {str(e)[:200]}")
                else:
                    text = found[:RESULT_CHARS] if found else "No results."
                    if found:
                        tool_kind = str((TOOLS.get(key) or {}).get("type", "mcp")).lower()
                        label = (labels or {}).get(tool_kind, "TOOL")
                        parts.append(f"=== {label}: {key} -> {name}({shown}) "
                                     f"(mode={mode}, chosen by the model) ===\n{found}")
                    else:
                        limitations.append(f"'{name}' returned no results for {shown}.")
            messages.append(ToolMessage(content=text, tool_call_id=str(call.get("id") or name)))
    if not calls:
        limitations.append("The model called none of this agent's tools for this request.")
    elif calls >= budget:
        await ctx.log(f"Tool calls: reached this agent's limit of {budget} (maxToolCalls).")
    return "\n\n".join(parts), limitations
