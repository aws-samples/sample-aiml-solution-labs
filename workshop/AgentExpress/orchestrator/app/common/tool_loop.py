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

from app.common import artifacts
from app.common.config import TOOLS
from app.common.errors import ToolDenied, ToolNeedsConsent, ToolUnavailable
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
    "try to work around a refusal.\n"
    "- If the agent has skills, open (use_skill) each one whose description fits the request "
    "before calling other tools, and follow it: it says how this task is done.")

#: How many times one tool turn may open a skill or one of its files.
SKILL_OPENS = 10


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
    own = list(getattr(ctx, "skills", None) or [])
    if own:
        from app.common import skills
        listed = skills.catalog(own)
        if listed:
            parts.append(f"=== THE AGENT'S SKILLS (open with use_skill when one fits) ===\n{listed}")
    return "\n\n".join(parts)


async def gather(ctx, query: str, *, tools: list[str],
                 labels: dict[str, str] | None = None) -> tuple[str, list[str]]:
    """Let the model call the agent's tools (`tools`, its bound keys). Returns
    (evidence, limitations), the shape evidence.gather returns; `labels` names each
    tool type in the evidence headings, as that caller does."""
    from langchain_core.messages import HumanMessage, ToolMessage

    from app.common import asks
    from app.common.llm import run_llm_tools
    specs, routes = await _specs(ctx, list(tools))
    offered = {key for _kind, key, _tool in routes.values()}
    hidden = [k for k in tools if k not in offered]
    # askPerson: one more tool, ask_person (app/common/asks.py). It is a reason to run this
    # turn even with no tool bound, but never a substitute for tools that ARE bound and
    # that the Gateway does not list.
    ask = asks.ask_spec(getattr(ctx, "agent_id", ""))
    if not specs and (tools or not ask):
        # Nothing to call means nothing to reason from. Carrying on would hand the agent
        # an empty evidence block, and a model with a task and no evidence writes one —
        # observed live: it "called" the tool in prose and reported made-up numbers. So
        # this fails like direct mode does for an unpublished tool.
        raise ToolUnavailable(f"The Gateway lists none of this agent's tools ({', '.join(tools)}). "
                              + _HIDDEN_WHY)
    own_skills = list(getattr(ctx, "skills", None) or [])
    if own_skills:
        from app.common import skills
        if skills.known(own_skills):
            # First, so it reads as part of how the task is done; never a Gateway call.
            specs.insert(0, skills.tool_spec(own_skills))
            routes[skills.TOOL_NAME] = ("skill", None, None)
    if ask and asks.TOOL_NAME not in routes:
        specs.append(asks.tool_spec(ask))
        routes[asks.TOOL_NAME] = ("ask", None, None)
    if ask and ask.get("options") and asks.DECIDE_TOOL not in routes:  # v139 Track D
        specs.append(asks.decide_tool_spec(ask))
        routes[asks.DECIDE_TOOL] = ("decide", None, None)
    journal = asks.journaling(ctx)
    opens = 0
    messages: list = [HumanMessage(content=_task(ctx, query))]
    parts: list[str] = []
    limitations: list[str] = [f"'{k}' was not offered: the Gateway lists none of its tools. {_HIDDEN_WHY}"
                              for k in hidden]
    calls = 0
    budget = max(1, int(getattr(ctx, "max_tool_calls", 6) or 6))
    called: set[str] = set()        # the bound tools (by key) that were called at least once
    over: dict[str, str] = {}       # calls the model asked for past the limit: name -> arguments
    # Each question takes a model turn of its own, so it does not eat the tool budget.
    for _turn in range(budget + 1 + (ask["maxQuestions"] if ask else 0)):
        if is_cancelled(ctx.session_id):
            raise WorkflowCancelled(ctx.session_id)

        async def turn():
            # Room for an argument that carries a whole draft, not just a search query.
            # The same model choice as ctx.llm (app/common/model_routing.py).
            from app.common import model_routing
            chain = model_routing.candidates(
                ctx, SYSTEM, "\n\n".join(str(getattr(m, "content", "")) for m in messages))
            return await model_routing.call(ctx, chain, lambda m: run_llm_tools(
                f"{ctx.agent_id}.tools", SYSTEM, messages, specs, model=m,
                max_tokens=min(int(getattr(ctx, "max_tokens", 1500) or 1500), 8000),
                **({"prompt_cache": ctx.prompt_cache} if getattr(ctx, "prompt_cache", None) else {})))
        if journal:
            # Journaled (asks.py): a node re-run on resume replays this turn, not bills it again.
            async def recorded():
                ai = await turn()
                return {"content": ai.content, "tool_calls": list(getattr(ai, "tool_calls", None) or [])}
            from langchain_core.messages import AIMessage
            ai = AIMessage(**await asks.once(ctx, recorded))
        else:
            ai = await turn()
        messages.append(ai)
        wanted = list(getattr(ai, "tool_calls", None) or [])
        if not wanted:
            break
        for call in wanted:
            name, args = str(call.get("name") or ""), call.get("args") or {}
            route = routes.get(name)
            if route is not None and route[0] == "skill":
                # Know-how, not evidence: not a tool call, so not counted against
                # maxToolCalls, and kept out of the evidence. ctx.llm carries what
                # was opened into the agent's own model calls.
                from app.common import skills
                opens += 1
                skill, file = str(args.get("name") or ""), str(args.get("file") or "") or None
                if opens > SKILL_OPENS:
                    text = f"Not opened: at most {SKILL_OPENS} skill opens per turn. Work with what you have."
                else:
                    text = skills.open_skill(skill, file, allowed=own_skills)
                    if skill in skills.known(own_skills) and not text.startswith("Skill "):
                        ctx.opened_skills.add(skill)
                        await ctx.log(f"Opened skill: {skill}{f' / {file}' if file else ''}")
                messages.append(ToolMessage(content=text, tool_call_id=str(call.get("id") or name)))
                continue
            if route is None:
                text = f"There is no tool named {name}. Use one of: {', '.join(routes)}."
            elif route[0] == "ask":
                # A question for a person (asks.py): the run waits for the answer. Not a
                # tool call, so not counted against maxToolCalls; capped on its own.
                text = await asks.ask(ctx, args.get("question"))
            elif route[0] == "decide":
                # v139 Track D: a person chooses one of askPerson.options; capped with questions.
                text = await asks.request_decision(ctx, args.get("question"))
            elif calls >= budget:
                text = (f"Not called: this agent may make at most {budget} tool calls, and has. "
                        f"Answer with what you have.")
                over.setdefault(name, json.dumps(args, ensure_ascii=False, default=str)[:300])
            else:
                calls += 1
                _kind, key, _tool = route
                called.add(key)
                shown = json.dumps(args, ensure_ascii=False, default=str)[:300]
                # A tool with `approval` waits for a person, who sees the arguments the
                # Gateway will be sent (the tool's fixed `args` win, as in invoke_chosen).
                approved, told = (await asks.approve(
                    ctx, key, name, {**args, **dict((TOOLS.get(key) or {}).get("args") or {})})
                    if _kind == "gateway" else (True, ""))
                if not approved:
                    text = told
                    limitations.append(f"'{name}' was not called for {shown}: it needs a person's approval, "
                                       f"which it did not get.")
                    messages.append(ToolMessage(content=text, tool_call_id=str(call.get("id") or name)))
                    continue
                n = calls

                async def attempt(route=route, args=args, shown=shown, name=name, n=n):
                    # The call and its timeline lines are one journal entry (asks.py), and
                    # what the loop needs from it comes back as data: a node re-run on
                    # resume must not call the tool again, nor log it twice.
                    await ctx.log(f"Tool call {n}/{budget}: {name}({shown})")
                    try:
                        found, mode = await _call(ctx, route, args)
                    except ToolDenied as e:
                        by_interceptor = getattr(e, "by", "policy") == "interceptor"
                        # On the timeline too: the refusal is the policy working, and it
                        # was visible only to the model.
                        await ctx.log(f"Refused by interceptor: {name}({shown}) — {str(e).split(': ', 1)[-1][:200]}"
                                      if by_interceptor else f"Refused by Cedar policy: {name}({shown})")
                        return {"denied": str(e), "layer": "interceptor" if by_interceptor else "policy"}
                    except ToolNeedsConsent:
                        # Not something the model can work around: the person connects
                        # their account, then runs the step again. Fail it with that.
                        raise
                    except ToolUnavailable as e:
                        return {"failed": str(e)}
                    if route[0] == "gateway" and artifacts.is_code_tool(
                            TOOLS.get(route[1]), getattr(route[2], "name", "")):
                        # The code the sandbox ran, kept with the run (Files tab). Inside
                        # the journal entry, so a resumed node does not keep it twice.
                        import asyncio
                        await asyncio.to_thread(
                            artifacts.record_code_call, ctx.session_id, ctx.agent_id,
                            {**args, **dict((TOOLS.get(route[1]) or {}).get("args") or {})}, found)
                    return {"found": found, "mode": mode}
                got = await asks.once(ctx, attempt)
                if "denied" in got:
                    found, mode = "", "denied"
                    text = f"REFUSED by the {got['layer']}: {got['denied']}"
                    limitations.append(f"'{name}' was refused by the Gateway's {got['layer']} for {shown}.")
                elif "failed" in got:
                    found, mode = "", "error"
                    text = f"FAILED: {got['failed']}"
                    limitations.append(f"'{name}' failed: {got['failed'][:200]}")
                else:
                    found, mode = got["found"], got["mode"]
                    text = found[:RESULT_CHARS] if found else "No results."
                    if found:
                        tool_kind = str((TOOLS.get(key) or {}).get("type", "mcp")).lower()
                        label = (labels or {}).get(tool_kind, "TOOL")
                        parts.append(f"=== {label}: {key} -> {name}({shown}) "
                                     f"(mode={mode}, chosen by the model) ===\n{found}")
                    else:
                        limitations.append(f"'{name}' returned no results for {shown}.")
            messages.append(ToolMessage(content=text, tool_call_id=str(call.get("id") or name)))
    if not calls and tools:
        limitations.append("The model called none of this agent's tools for this request.")
    elif calls >= budget:
        await ctx.log(f"Tool calls: reached this agent's limit of {budget} (maxToolCalls).")
        # Said as limitations, so the answer cannot report a result of a call that never
        # ran: seen live, an agent out of calls before its publishing tool wrote "published
        # successfully" with a made-up link.
        limitations.extend(f"'{name}' was not called for {shown}: this agent reached its limit of {budget} "
                           "tool calls (maxToolCalls), so there is no result from it. Do not report one."
                           for name, shown in over.items())
        bound = dict.fromkeys(k for kind, k, _t in routes.values() if kind in ("kb", "web", "gateway"))
        limitations.extend(f"'{key}' was not called: this agent reached its limit of {budget} tool calls "
                           "(maxToolCalls) first, so there is no result from it. Do not report one."
                           for key in bound
                           if key not in called and not any(n.startswith(f"{key}___") or n == key for n in over))
    return "\n\n".join(parts), limitations
