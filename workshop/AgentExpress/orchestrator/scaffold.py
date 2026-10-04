#!/usr/bin/env python3
"""Start your own workflow, and create agents for it.

    python3 scaffold.py reset --dry-run          # what clearing the sample would remove
    python3 scaffold.py reset                    # clear it: the first thing you do

    python3 scaffold.py agent triage
    python3 scaffold.py agent triage --produces claim-triage --tool claimsDb
    python3 scaffold.py agent fraud_check --remote            # runtime "a2a", no folder
    python3 scaffold.py agent triage --dry-run                # show, write nothing
    python3 scaffold.py agent triage --framework strands      # or langgraph; default plain

    python3 scaffold.py apply my-workflow.agentexpress.json   # a bundle from the Build view

WHY `reset` EXISTS
A clone arrives carrying a nine-agent AWS-architecture sample, and replacing it is step
one for every customer. Done by hand it is easy to leave something behind, and the thing
that notices is the test suite — `app/subagents/` and `workflow.json` must agree in both
directions, and so must `app/tools/`. A leftover folder then fails a test whose message is
about the framework rather than about the leftover, which reads as "the framework is
broken" in a customer's first hour.

WHY THIS EXISTS
The framework's promise is that a customer touches two things: `workflow.json` and a
folder under `app/subagents/<id>/`. Both were learnable only by reading an existing
agent and copying it — which works, and means the first thing anyone does is inherit
whichever agent they happened to open. `cost_research` is 354 lines with a tool-args
model call and a rates table; `documentation_search` is 27 lines. Copying the wrong
one is a bad first hour.

So this writes the MINIMUM that is complete and runs: four required keys in
workflow.json and three files whose contract the registry actually enforces
(`check_agent_module`). Nothing is stubbed out with a TODO that would pass validation
and fail at run time — the generated `run()` really calls the model and really returns
its answer, so a customer can deploy immediately and then make it theirs.

WHAT IT DELIBERATELY DOES NOT DO
It does not put the agent in `steps`. Where a step belongs in the topology is the one
decision that is genuinely the customer's, it depends on what the agent consumes, and a
guess would either be wrong or teach that ordering does not matter. The command prints
the one line to add and where.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from collections import OrderedDict
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SUBAGENTS = ROOT / "app" / "subagents"
WORKFLOW = ROOT / "app" / "workflow.json"

#: Same rule the registry and the generated schema enforce: the id becomes part of an
#: AgentCore Runtime name, so no hyphens.
ID_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*$")

INIT_PY = '''from .agent import agent

__all__ = ["agent"]
'''

PROMPTS_PY = '''"""Prompts for the {title} agent.

EVERYTHING ABOUT WHAT THIS AGENT SAYS LIVES HERE, and nothing about how it runs. That
split is the only convention the framework asks of this folder, and it is worth keeping
for one practical reason: a prompt is the part you will edit fifty times, and you want
to open a file that is only prompt.

`SYSTEM_PROMPT` is the agent's instructions. `SCHEMA` is the shape you require back,
passed to the model as text — keep the two next to each other so a field you add to one
cannot be forgotten in the other.
"""

SYSTEM_PROMPT = (
    "You are the {title} agent. <Say what this agent is for, in one or two "
    "sentences.>\\n\\n"
    "RULES\\n"
    "1. Use ONLY the inputs you are given. Invent nothing.\\n"
    "2. No figure that is not in your inputs — no cost, threshold, percentage or "
    "duration. Not even as an illustration: a reader takes a number in a deliverable "
    "for one somebody agreed to.\\n"
    "3. Say plainly what you could not determine, rather than filling the gap.\\n"
    "4. Return ONLY valid JSON."
)

#: The shape the model must return. Sent as text, so describe each field in place —
#: that description is the most effective part of the whole prompt.
SCHEMA = (
    '{{"summary": "one or two sentences on what you found", '
    '"findings": ["each material point, as its own string"], '
    '"openQuestions": ["what you could not determine, or []"]}}'
)
'''

AGENT_PY = '''"""{title} — <one line on what this agent does and where it sits>.

THE CONTRACT THIS FILE HAS TO MEET is three lines long, and `registry.check_agent_module`
enforces every one of them: subclass `Agent`, override `async def run(self, ctx) -> str`,
and assign `agent = ...` at the bottom. Everything else here is yours to change.

WHAT `ctx` GIVES YOU (app/common/context.py), all of it config-driven — each helper is a
no-op when the corresponding block is absent from this agent's workflow.json entry, so
you can call them unconditionally:

    await ctx.llm(system, user)          the model call. ROUTE EVERY MODEL CALL THROUGH
                                        IT: guardrails, cost and token telemetry,
                                        long-term memory injection, truncation
                                        detection and cancellation all live there. A
                                        framework holding its own Bedrock client loses
                                        all of that silently while the run still
                                        reports success.
    ctx.tools                            the tool keys this agent is bound to (`tool` in
                                        workflow.json: one key or a list)
    await gather(ctx)                    query every one of them, as labelled evidence
    await ctx.call_tool(key, query)      one tool, by its key
    await ctx.call_tool_rows(key, query) the same, as data rows (see `rowFields`)
    await ctx.retrieve(query)            your Knowledge Base tool
    ctx.input("<agent_id>")              an upstream agent's approved output
    ctx.feedback                         a reviewer's comment when this is a revise
    await ctx.log(msg)                   a line on the run timeline
    await ctx.heartbeat(pct)             progress, for a long step

Your token budget is `maxTokens` in workflow.json and `ctx.llm` applies it — never pass
a number here, or the budget moves back into code where whoever edits the config cannot
see it (there is a test for that).
{framework_doc}"""

import json

from app.common.base import Agent
from app.common.context import AgentContext
from app.subagents._shared.evidence import gather
{framework_imports}
from .prompts import SCHEMA, SYSTEM_PROMPT
{framework_helpers}

class {cls}(Agent):
    # Used by the framework for evaluations and for the observability prompt inspector.
    system_prompt = SYSTEM_PROMPT

    async def run(self, ctx: AgentContext) -> str:
        user = (
            f"=== REQUEST ===\\n{{ctx.topic}}\\n"
            # Everything an earlier step approved. Derived from the topology, so adding
            # an agent before this one in `steps` feeds it here with no change to this
            # file.
            f"\\n=== APPROVED UPSTREAM OUTPUTS ===\\n{{self._upstream(ctx)}}\\n"
        )
        # Evidence from this agent's tools — none, one or several, as declared in
        # workflow.json. A tool that cannot be called raises; one that answers with
        # nothing is named, so the model does not fill the gap.
        evidence, gaps = await gather(ctx)
        if evidence:
            # The calls already happened; this step has no tools. Said outright, because
            # a prompt that says "call the checker" otherwise gets the call written out
            # as text — observed live, with the whole draft pasted into it.
            user += ("\\n=== EVIDENCE FROM YOUR TOOLS (already called — these are their "
                     "results; do not write tool calls, use the results) ===\\n"
                     f"{{evidence}}\\n")
        if gaps:
            user += "\\n=== YOUR TOOLS RETURNED NOTHING FOR ===\\n" + "\\n".join(gaps) + "\\n"
        if ctx.feedback:
            user += f"\\n=== REVIEWER GUIDANCE (this is a revision) ===\\n{{ctx.feedback}}\\n"
        user += f"\\nReturn ONLY JSON matching this schema:\\n{{SCHEMA}}"

        # Returned as text, which is what a node's output is. To emit a VALIDATED asset
        # instead — with an assetId a later agent can cite — give this agent a pydantic
        # contract and use app/subagents/_shared/synthesis.py; `report` is the worked
        # example.
{reason}

    @staticmethod
    def _upstream(ctx: AgentContext) -> str:
        from app.common.config import upstream_of

        blocks = []
        for agent_id in upstream_of("{agent_id}"):
            raw = ctx.input(agent_id)
            if raw:
                blocks.append(f"--- {{agent_id.replace('_', ' ').upper()}} ---\\n{{raw}}")
        return "\\n\\n".join(blocks) or json.dumps({{"note": "this is the first step"}})


agent = {cls}()
'''

#: How the reasoning step is written, per agent framework. The framework is an
#: AUTHORING choice, so it lives in the agent's code, never in workflow.json: nothing at
#: run time reads it. Each variant still reaches the model through `ctx.llm`, which is
#: what keeps guardrails, telemetry and memory working whatever framework is chosen.
#: Every piece is run through str.format with the agent's names, so literal braces in
#: the generated code are written doubled.
FRAMEWORKS = {
    "plain": {
        "doc": "",
        "imports": "",
        "helpers": "",
        "reason": "        return await ctx.llm(SYSTEM_PROMPT, user)",
    },
    "strands": {
        "doc": '''
FRAMEWORK: Strands Agents. The reasoning runs inside a `strands.Agent` whose model is
`ctx.llm` (app/subagents/_shared/strands_bridge.py), so every model call keeps the
framework's guardrails and telemetry. Strands' own @tool calling is not available through
that bridge: declare data sources in workflow.json `tools` and they arrive as evidence.
''',
        "imports": "from app.subagents._shared.strands_bridge import strands_agent\n",
        "helpers": "",
        "reason": '''        result = await strands_agent(ctx, system_prompt=SYSTEM_PROMPT).invoke_async(user)
        return str(result)''',
    },
    "langgraph": {
        "doc": '''
FRAMEWORK: LangGraph. The reasoning is a small graph of its own (draft, then one repair
pass if the draft is not valid JSON), nested inside the orchestrator's graph. Each node
calls `ctx.llm`. Grow it: add nodes, add edges, route on state.
''',
        "imports": "",
        "helpers": '''

async def _reason(ctx: AgentContext, user: str) -> str:
    """Draft, check, repair once: a LangGraph graph you can extend."""
    from typing import TypedDict

    from langgraph.graph import END, START, StateGraph

    class State(TypedDict):
        draft: str
        attempts: int

    async def draft(state: State) -> dict:
        return {{"draft": await ctx.llm(SYSTEM_PROMPT, user), "attempts": 1}}

    async def repair(state: State) -> dict:
        fix = (user + "\\n\\nYour previous answer was not valid JSON. Return ONLY the JSON "
               "object, nothing else.\\n\\nPrevious answer:\\n" + state["draft"])
        answer = await ctx.llm(SYSTEM_PROMPT, fix, name=f"{{ctx.agent_id}}.repair")
        return {{"draft": answer, "attempts": state["attempts"] + 1}}

    def review(state: State) -> str:
        try:
            json.loads(state["draft"])
            return END
        except ValueError:
            return END if state["attempts"] >= 2 else "repair"

    graph = StateGraph(State)
    graph.add_node("draft", draft)
    graph.add_node("repair", repair)
    graph.add_edge(START, "draft")
    graph.add_conditional_edges("draft", review, {{"repair": "repair", END: END}})
    graph.add_conditional_edges("repair", review, {{"repair": "repair", END: END}})
    # No checkpointer: the orchestrator's own graph already checkpoints this step.
    result = await graph.compile().ainvoke({{"draft": "", "attempts": 0}})
    return result["draft"]
''',
        "reason": "        return await _reason(ctx, user)",
    },
}


def title_of(agent_id: str) -> str:
    return agent_id.replace("_", " ").title()


def class_of(agent_id: str) -> str:
    return "".join(p.capitalize() for p in agent_id.split("_")) + "Agent"


def entry(args) -> OrderedDict:
    """The workflow.json entry, in the canonical key order.

    Built here rather than copied from a template so it cannot drift from
    `app/keys.json` — the order comes from the same place `format_workflow.py` reads.
    """
    sys.path.insert(0, str(ROOT))
    try:
        from format_workflow import top_level_order
        order = top_level_order("agent")
    finally:
        sys.path.pop(0)

    spec: dict = {"name": title_of(args.agent_id), "produces": args.produces}
    if args.remote:
        spec |= {"runtime": "a2a", "source": "a2a_lambda", "skill": args.skill or "compliance",
                 "auth": "sigv4"}
    else:
        spec |= {"runtime": args.runtime, "maxTokens": args.max_tokens}
        if args.tool:
            spec["tool"] = args.tool
            tool = (args.workflow.get("tools") or {}).get(args.tool) or {}
            # A Knowledge Base tool is the one type where a second key is effectively
            # required: without `corpus` the agent retrieves across every corpus, which is
            # rarely what someone binding to a KB means and is invisible when it is wrong.
            # Defaulted to the tool's first declared corpus, which is at least a real one.
            if str(tool.get("type") or "").lower() == "kb" and tool.get("corpora"):
                spec["corpus"] = tool["corpora"][0]
        else:
            spec["access"] = ["Upstream assets (orchestrator graph state)"]
    return OrderedDict((k, spec[k]) for k in order if k in spec)


#: The reasoning step of an IMAGE agent (workflow.json `output: "image"`), whatever its
#: framework: the model writes a brief, ctx.image renders it (app/common/images.py), and
#: the asset carries both — so the reviewer at the gate sees the image and its words.
IMAGE_REASON = """        raw = await ctx.llm(SYSTEM_PROMPT, user)
        brief = extract_json(raw) or {{}}
        prompt = str(brief.get("prompt") or "").strip()
        if not prompt:
            raise ValueError("the model wrote no image prompt; got: " + raw[:300])
        rendered = await ctx.image(prompt, negative_prompt=str(brief.get("negativePrompt") or ""))
        return json.dumps({{"summary": brief.get("caption") or prompt[:200], "brief": brief,
                           "images": [rendered]}})"""

#: What an image agent's model must return: the brief, not the image.
IMAGE_SCHEMA = ('{"prompt": "what the image shows, concretely: subject, setting, style, '
                'lighting, composition", "negativePrompt": "what it must not show", '
                '"caption": "one sentence for the reader"}')


def files(agent_id: str, framework: str = "plain", output: str = "text") -> dict[str, str]:
    fw = FRAMEWORKS[framework if output != "image" else "plain"]
    fmt = {"agent_id": agent_id, "title": title_of(agent_id), "cls": class_of(agent_id)}
    # The framework pieces are templates too (they contain `{{`/`}}` for literal braces
    # and may name the agent), so they are formatted first and then spliced in.
    parts = {
        "framework_doc": fw["doc"].format(**fmt),
        "framework_imports": fw["imports"].format(**fmt),
        "framework_helpers": fw["helpers"].format(**fmt),
        "reason": (IMAGE_REASON if output == "image" else fw["reason"]).format(**fmt),
    }
    agent_py = AGENT_PY.format(**fmt, **parts)
    prompts_py = PROMPTS_PY.format(**fmt)
    if output == "image":
        # The brief shape is fixed (ctx.image needs a prompt), so it is not the SCHEMA a
        # prompt may redefine: the agent asks for IMAGE_SCHEMA.
        agent_py = agent_py.replace(
            "from .prompts import SCHEMA, SYSTEM_PROMPT",
            "from app.common.assets import extract_json\n\nfrom .prompts import SYSTEM_PROMPT"
        ).replace("{SCHEMA}", "{IMAGE_SCHEMA}").replace(
            "class " + fmt["cls"] + "(Agent):",
            "IMAGE_SCHEMA = " + json.dumps(IMAGE_SCHEMA) + "\n\n\nclass " + fmt["cls"] + "(Agent):")
    return {
        "__init__.py": INIT_PY,
        "prompts.py": prompts_py,
        "agent.py": agent_py,
    }


#: The line that marks a prompts.py as the Builder's rather than the customer's. While it
#: is present, the next `apply` may rewrite the file; delete it and the file is yours.
BUILDER_MARKER = "GENERATED BY THE AGENTEXPRESS BUILDER"

#: What `apply` accepts. Versioned so a bundle from a newer Builder is refused with a
#: message instead of half-applied.
BUNDLE_FORMAT = "agentexpress-bundle"
BUNDLE_VERSION = 1
#: This framework's version (orchestrator/VERSION). A bundle records the one it was
#: built on; `apply` says so when they differ, since a key or a default may have moved.
FRAMEWORK_VERSION = (ROOT / "VERSION").read_text().strip() if (ROOT / "VERSION").exists() else ""


def _py_str(text: str, indent: str = "    ") -> str:
    """`text` as a parenthesised Python string expression, one literal per line.

    Each literal is written with json.dumps, whose escapes (\\n, \\", \\\\, \\uXXXX) are
    all valid Python string escapes — so whatever a user typed into the Builder, quotes
    and backslashes included, comes back out of the module byte for byte. Split at line
    breaks only so the file reads like the hand-written prompts beside it.
    """
    lines = text.split("\n")
    parts = [json.dumps(line + ("\n" if i < len(lines) - 1 else ""), ensure_ascii=False)
             for i, line in enumerate(lines)]
    return "(\n" + "\n".join(f"{indent}{p}" for p in parts) + "\n)"


def builder_prompts(agent_id: str, system_prompt: str, schema: str) -> str:
    """prompts.py for an agent whose prompt was written in the Builder.

    Same two names the scaffold's own prompts.py defines (and agent.py imports), so the
    generated agent.py works with either file unchanged.
    """
    return (
        f'"""Prompts for the {title_of(agent_id)} agent.\n'
        "\n"
        f"{BUILDER_MARKER}. `scaffold.py apply` rewrites this file from the next\n"
        "bundle you export while this paragraph is here. Delete the paragraph to take the\n"
        "file over by hand; after that `apply` leaves it alone and tells you so.\n"
        '"""\n'
        "\n"
        f"SYSTEM_PROMPT = {_py_str(system_prompt)}\n"
        "\n"
        "#: The shape the model must return. Sent as text.\n"
        f"SCHEMA = {_py_str(schema)}\n"
    )


def _write_marker() -> None:
    """Mark this tree as a workflow implementation, which turns the edit boundary on."""
    # From here on this tree is a workflow implementation rather than the framework's own
    # development, so the edit boundary applies. tests/test_edit_boundary.py enforces it
    # only when this marker exists — without it, framework files changing is normal and
    # the check would fail on every commit the framework's own authors make.
    (ROOT / ".agentexpress-customer").write_text(
        "This tree is a workflow implementation, not the framework's own development.\n"
        "\n"
        "Written by `scaffold.py reset` or `scaffold.py apply`. While it exists,\n"
        "tests/test_edit_boundary.py fails if anything changes outside the four surfaces\n"
        "you own: app/workflow.json, app/subagents/, app/tools/ and kb_docs/.\n"
        "\n"
        "Delete it to make a deliberate framework change.\n")


#: Where a code tool's files go (tools.<key>.code): app/tools/_code/<key>/. Written from
#: the bundle's `toolCode`, so a build carries its functions like it carries its prompts.
CODE_DIR = "_code"
#: A folder the Builder wrote, which the next apply may rewrite. Delete it to take the
#: folder over by hand; after that `apply` leaves it alone, like a prompts.py.
CODE_MARKER = ".from-builder"
CODE_FILE_RE = re.compile(r"^([a-z_][a-z0-9_]*\.py|requirements\.txt|events\.json)$")
CODE_MAX_FILES = 20
CODE_MAX_BYTES = 500_000


def code_tools(workflow: dict) -> list[str]:
    """The tool keys whose function is written in the build."""
    return [k for k, t in (workflow.get("tools") or {}).items()
            if isinstance(t, dict) and str(t.get("type") or "").lower() == "lambda" and "code" in t]


def code_problems(key: str, files) -> list[str]:
    """What stops a code tool's files from being written."""
    if not isinstance(files, dict) or not isinstance(files.get("handler.py"), str) \
            or not files["handler.py"].strip():
        return [f"tools.{key} is written in the build but its handler.py is missing"]
    out = [f"tools.{key}: {name!r} is not a file a code tool may hold (*.py, requirements.txt, events.json)"
           for name in files if not CODE_FILE_RE.match(str(name))]
    out += [f"tools.{key}: {name} is not text" for name, body in files.items()
            if CODE_FILE_RE.match(str(name)) and not isinstance(body, str)]
    if len(files) > CODE_MAX_FILES:
        out.append(f"tools.{key}: at most {CODE_MAX_FILES} files")
    if sum(len(str(b).encode()) for b in files.values()) > CODE_MAX_BYTES:
        out.append(f"tools.{key}: its files are over {CODE_MAX_BYTES // 1000} KB")
    return out


def _referenced(workflow: dict) -> tuple[set[str], set[str]]:
    """The app/tools/ folders and kb_docs/ corpora the workflow actually uses."""
    tools = (workflow.get("tools") or {}).values()
    sources = {str(t.get("source")) for t in tools if isinstance(t, dict) and t.get("source")}
    if code_tools(workflow):
        sources.add(CODE_DIR)
    corpora = {str(c) for t in tools if isinstance(t, dict)
               and str(t.get("type") or "").lower() == "kb" for c in (t.get("corpora") or [])}
    return sources, corpora


def apply(bundle_path: Path, dry_run: bool, exact: bool = False) -> int:
    """Write a bundle exported from the Builder into this tree.

    WITH `exact`, THE TREE IS MADE TO MIRROR THE BUNDLE, AND NOTHING ELSE SURVIVES.
    This is what a deploy from the console runs, on a fresh copy of the framework, so
    the same stored build version always produces the same tree:

      * every agent the Builder wrote a prompt for is regenerated from scratch (its
        folder is deleted and written again from the template and that prompt);
      * an agent with no Builder prompt keeps the folder the framework ships, or gets
        the starter when there is none;
      * agent folders, app/tools/ folders and kb_docs/ corpora the workflow does not
        name are DELETED.

    WITHOUT IT, THE RULE IS: THE BUILDER OWNS DATA, YOU OWN CODE, AND NEITHER
    OVERWRITES THE OTHER.

      * workflow.json is data, so it is replaced — then formatted, so the file you open
        is in the canonical order whatever the browser wrote.
      * An agent with no folder gets one: the scaffold's agent.py and __init__.py, plus a
        prompts.py carrying the prompt written in the Builder.
      * An agent that already has a folder keeps its agent.py and __init__.py, always.
        Its prompts.py is rewritten only while it still carries BUILDER_MARKER, i.e.
        only if the Builder wrote it and you have not taken it over.
      * A folder the workflow no longer names is reported, never deleted. It is code, and
        removing it is your decision; the test suite will point at it until you do.

    That is what makes the Builder a two-way door rather than a code generator you can
    only use once: export, hand-edit an agent, change the topology in the Builder,
    export again — and the hand edit survives.
    """
    try:
        bundle = json.loads(bundle_path.read_text(), object_pairs_hook=OrderedDict)
    except (OSError, ValueError) as e:
        print(f"scaffold: cannot read {bundle_path}: {e}", file=sys.stderr)
        return 2
    if not isinstance(bundle, dict) or bundle.get("format") != BUNDLE_FORMAT:
        print(f"scaffold: {bundle_path} is not an AgentExpress bundle (expected "
              f"\"format\": \"{BUNDLE_FORMAT}\"). To use a plain workflow.json, copy it to "
              f"app/workflow.json and run `scaffold.py agent <id>` for each new agent.",
              file=sys.stderr)
        return 2
    if bundle.get("version") != BUNDLE_VERSION:
        print(f"scaffold: bundle version {bundle.get('version')!r} is not supported by this "
              f"framework (it reads version {BUNDLE_VERSION}). Update the framework, or "
              f"export again from a matching Builder.", file=sys.stderr)
        return 2

    framework = bundle.get("framework") if isinstance(bundle.get("framework"), dict) else {}
    built_on = str(framework.get("version") or "")
    if built_on and FRAMEWORK_VERSION and built_on != FRAMEWORK_VERSION:
        print(f"scaffold: warning: this bundle was built on framework {built_on}; this is "
              f"framework {FRAMEWORK_VERSION}. Applying it anyway. A key or a default may "
              f"have changed between the two, so review the workflow before you deploy.",
              file=sys.stderr)
    workflow = bundle.get("workflow")
    if not isinstance(workflow, dict) or not isinstance(workflow.get("agents"), dict) \
            or not isinstance(workflow.get("steps"), list) or not workflow["steps"]:
        print("scaffold: the bundle's workflow has no `agents` or no `steps`, so it is not "
              "a workflow that can run. Fix it in the Builder and export again.",
              file=sys.stderr)
        return 2
    bad = [a for a in workflow["agents"] if not ID_RE.match(str(a))]
    if bad:
        print(f"scaffold: invalid agent id(s) {bad}: each must match {ID_RE.pattern} — no "
              f"hyphens, because the id becomes part of an AgentCore Runtime name.",
              file=sys.stderr)
        return 2
    prompts = bundle.get("prompts") or {}
    tool_code = bundle.get("toolCode") if isinstance(bundle.get("toolCode"), dict) else {}
    coded = code_tools(workflow)
    wrong = [p for k in coded for p in code_problems(k, tool_code.get(k))]
    if wrong:
        print("scaffold: " + "; ".join(wrong) + ". Write it in the Builder and export again.",
              file=sys.stderr)
        return 2

    def framework_of(aid: str) -> str:
        """workflow.json `framework`, else the older bundles' prompts field, else plain."""
        spec = workflow["agents"].get(aid) or {}
        return str(spec.get("framework") or (prompts.get(aid) or {}).get("framework") or "plain")

    unknown = sorted({framework_of(a) for a in workflow["agents"]
                      if framework_of(a) not in FRAMEWORKS
                      and str((workflow["agents"][a] or {}).get("runtime") or "main") != "a2a"})
    if unknown:
        print(f"scaffold: unknown agent framework(s) {unknown} in the bundle; this framework "
              f"version writes {sorted(FRAMEWORKS)}.", file=sys.stderr)
        return 2

    local = [aid for aid, spec in workflow["agents"].items()
             if str((spec or {}).get("runtime") or "main") != "a2a"]
    created, rewritten, kept, regenerated = [], [], [], []
    for aid in local:
        folder = SUBAGENTS / aid
        given = prompts.get(aid) or {}
        custom = (builder_prompts(aid, given["systemPrompt"], given.get("schema") or "")
                  if str(given.get("systemPrompt") or "").strip() else None)
        if not folder.exists():
            created.append((aid, custom, framework_of(aid)))
            continue
        if exact and custom:
            regenerated.append((aid, custom, framework_of(aid)))
            continue
        current = folder / "prompts.py"
        if custom and current.exists() and BUILDER_MARKER in current.read_text():
            rewritten.append((aid, custom))
        else:
            kept.append(aid)
    existing = sorted(d.name for d in SUBAGENTS.iterdir()
                      if d.is_dir() and d.name not in KEEP_IN_SUBAGENTS
                      and not d.name.startswith("__")) if SUBAGENTS.is_dir() else []
    orphans = [d for d in existing if d not in local]
    sources, used_corpora = _referenced(workflow)
    tools_root, kb_root = ROOT / "app" / "tools", ROOT / "kb_docs"
    stale_tools = sorted(d.name for d in tools_root.iterdir()
                         if d.is_dir() and d.name not in sources) if tools_root.is_dir() else []
    stale_corpora = sorted(d.name for d in kb_root.iterdir()
                           if d.is_dir() and d.name not in used_corpora) if kb_root.is_dir() else []
    code_root = tools_root / CODE_DIR
    stale_code = sorted(d.name for d in code_root.iterdir()
                        if d.is_dir() and d.name not in coded) if code_root.is_dir() else []
    # Without --exact a folder you took over (its marker deleted) is yours, like a prompts.py.
    write_code = [k for k in coded if exact or not (code_root / k).exists()
                  or (code_root / k / CODE_MARKER).exists()]

    print(f"app/workflow.json — replaced ({len(workflow['agents'])} agents, "
          f"{len(workflow['steps'])} steps, {len(workflow.get('tools') or {})} tools)")
    for aid, custom, fw in created:
        print(f"app/subagents/{aid}/ — new ({'your Builder prompt' if custom else 'starter prompt'}"
              f"{'' if fw == 'plain' else f', {fw}'})")
    for aid, _, fw in regenerated:
        print(f"app/subagents/{aid}/ — regenerated from the Builder"
              f"{'' if fw == 'plain' else f' ({fw})'}")
    for aid, _ in rewritten:
        print(f"app/subagents/{aid}/prompts.py — rewritten from the Builder")
    for aid in kept:
        print(f"app/subagents/{aid}/ — kept as is (your code)")
    if exact:
        for aid in orphans:
            print(f"app/subagents/{aid}/ — DELETED (not in the workflow)")
        for name in stale_tools:
            print(f"app/tools/{name}/ — DELETED (no tool uses it)")
        for name in stale_corpora:
            print(f"kb_docs/{name}/ — DELETED (no knowledge-base tool names it)")
        for name in stale_code:
            print(f"app/tools/{CODE_DIR}/{name}/ — DELETED (no code tool is named {name})")
    for key in coded:
        print(f"app/tools/{CODE_DIR}/{key}/ — "
              + ("written from the Builder" if key in write_code else "kept as is (your code)"))
    if not exact:
        for aid in orphans:
            print(f"app/subagents/{aid}/ — NOT in the workflow any more; left in place. Delete "
                  f"it when you are sure, or apply with --exact to remove everything the "
                  f"bundle does not name.")
    # A Knowledge Base corpus is a folder of the customer's documents, which a browser
    # cannot supply. Terraform and CDK both refuse a corpus with no folder, so say it
    # HERE, naming the folder, rather than at the end of a deploy.
    corpora = sorted({str(c) for t in (workflow.get("tools") or {}).values()
                      if str((t or {}).get("type") or "").lower() == "kb"
                      # Your own bucket or Knowledge Base holds these, not kb_docs/.
                      and not t.get("s3Uri") and not t.get("knowledgeBaseId")
                      for c in (t.get("corpora") or [])})
    missing = [c for c in corpora if not (ROOT / "kb_docs" / c).is_dir()]
    for c in missing:
        print(f"kb_docs/{c}/ — MISSING. Put the documents for corpus {c!r} there before you "
              f"deploy; the deploy refuses a corpus with no folder.")

    if dry_run:
        print("\n--dry-run: nothing written.")
        return 0

    WORKFLOW.write_text(json.dumps(workflow, indent=2, ensure_ascii=False) + "\n")
    if exact:
        for aid in orphans:
            shutil.rmtree(SUBAGENTS / aid)
        for name in stale_tools:
            shutil.rmtree(tools_root / name)
        for name in stale_corpora:
            shutil.rmtree(kb_root / name)
        for aid, _, _ in regenerated:
            shutil.rmtree(SUBAGENTS / aid)
        for name in stale_code:
            shutil.rmtree(code_root / name)
        created = created + regenerated
    for key in write_code:
        folder = code_root / key
        if folder.exists():
            shutil.rmtree(folder)
        folder.mkdir(parents=True)
        for name, body in tool_code[key].items():
            (folder / name).write_text(body)
        (folder / CODE_MARKER).write_text(
            "Written by `scaffold.py apply` from the Builder. Delete this file to keep your\n"
            "own edits to this folder: apply then leaves it alone.\n")
    for aid, custom, fw in created:
        folder = SUBAGENTS / aid
        folder.mkdir(parents=True)
        # The framework picks how agent.py is written — it is code, so it is chosen
        # ONCE, when the folder is created, and never rewritten after.
        output = str((workflow["agents"].get(aid) or {}).get("output") or "text")
        for name, body in files(aid, fw, output).items():
            (folder / name).write_text(custom if name == "prompts.py" and custom else body)
    for aid, custom in rewritten:
        (SUBAGENTS / aid / "prompts.py").write_text(custom)

    subprocess.run([sys.executable, str(ROOT / "format_workflow.py")],  # noqa: S603
                   cwd=ROOT, check=True)
    _write_marker()

    print("\nDone. Run `pytest` — it checks the workflow and every agent folder the same "
          "way the deploy will.")
    return 0


#: What `reset` keeps under app/subagents/. `_shared/` holds code the agents import and
#: the worked example of a well-formed asset contract; __init__.py makes it a package.
KEEP_IN_SUBAGENTS = {"_shared", "__init__.py"}

#: The id of the one agent `reset` leaves behind. Deliberately not domain-shaped: it
#: should read as scaffolding to rename, not as a suggestion about your topology.
STARTER_ID = "first_agent"


def _sample_inventory():
    """Everything `reset` would remove: the config entries, the agent folders, the tool
    folders and the Knowledge Base corpora."""
    workflow = json.loads(WORKFLOW.read_text(), object_pairs_hook=OrderedDict)
    agent_dirs = sorted(
        d for d in SUBAGENTS.iterdir()
        if d.is_dir() and d.name not in KEEP_IN_SUBAGENTS and not d.name.startswith("__"))
    tools_root = ROOT / "app" / "tools"
    tool_dirs = sorted(d for d in tools_root.iterdir() if d.is_dir()) if tools_root.is_dir() else []
    kb_root = ROOT / "kb_docs"
    corpora = sorted(d for d in kb_root.iterdir() if d.is_dir()) if kb_root.is_dir() else []
    return workflow, agent_dirs, tool_dirs, corpora


def reset(dry_run: bool, keep_kb: bool) -> int:
    """Strip the shipped sample so a customer's own workflow can be built on top.

    IT LEAVES ONE WORKING AGENT, NOT AN EMPTY FILE. An empty `agents`/`steps` is not a
    valid workflow, and the first version of this command produced one: 221 tests across
    18 files failed immediately, because most of them quite reasonably assume a workflow
    has at least one agent and one step. That switches the customer's safety net off at
    the exact moment they start editing, and makes "my config is wrong" indistinguishable
    from "I have not finished yet". So this lands on the smallest workflow that is
    genuinely valid — one agent, one gated step, no tools — and they build outward.
    """
    workflow, agent_dirs, tool_dirs, corpora = _sample_inventory()
    agents = list(workflow["agents"])
    tools = list(workflow.get("tools") or {})

    print("workflow.json")
    print(f"  agents  — removing {len(agents)}: {', '.join(agents) or 'none'}")
    print(f"  steps   — removing {len(workflow.get('steps') or [])}")
    print(f"  tools   — removing {len(tools)}: {', '.join(tools) or 'none'}")
    for d in agent_dirs:
        print(f"app/subagents/{d.name}/")
    for d in tool_dirs:
        print(f"app/tools/{d.name}/")
    if keep_kb:
        print(f"kb_docs/ — keeping {len(corpora)} corpus folder(s) (--keep-kb)")
    else:
        for d in corpora:
            print(f"kb_docs/{d.name}/")
    print(f"\nthen writing one starter agent, {STARTER_ID!r}, in one gated step.")
    print("keeping app/subagents/_shared/ — the agents' shared code and the worked "
          "example of an asset contract. Delete it once your own agents stop importing it.")

    if dry_run:
        print("\n--dry-run: nothing written.")
        return 0

    for d in agent_dirs:
        shutil.rmtree(d)
    for d in tool_dirs:
        shutil.rmtree(d)
    if not keep_kb:
        for d in corpora:
            shutil.rmtree(d)

    starter = argparse.Namespace(
        agent_id=STARTER_ID, produces="your-deliverable", runtime="main",
        max_tokens=4000, tool="", remote=False, skill="", workflow=workflow)
    spec = entry(starter)
    # Evaluations on, because tests/test_evaluations_gate.py requires at least one
    # evaluable agent — and because a customer who never sees the block will not learn
    # the capability exists.
    spec["agentcore"] = OrderedDict([
        ("evaluations", OrderedDict([
            ("enabled", True), ("auto", False),
            ("evaluators", ["Builtin.Faithfulness", "Builtin.ResponseRelevance"]),
        ])),
    ])
    workflow["agents"] = OrderedDict([(STARTER_ID, spec)])
    workflow["steps"] = [OrderedDict([("agent", STARTER_ID), ("hitl", True)])]
    workflow["tools"] = OrderedDict()
    WORKFLOW.write_text(json.dumps(workflow, indent=2, ensure_ascii=False) + "\n")

    folder = SUBAGENTS / STARTER_ID
    folder.mkdir(parents=True, exist_ok=True)
    for name, body in files(STARTER_ID).items():
        (folder / name).write_text(body)

    subprocess.run([sys.executable, str(ROOT / "format_workflow.py")],  # noqa: S603
                   cwd=ROOT, check=True)
    # The schema enumerates some values from what the repo contains, so regenerate it
    # rather than leaving an editor validating against a sample that is now gone.
    subprocess.run([sys.executable, str(ROOT / "build_schema.py")],  # noqa: S603
                   cwd=ROOT, check=True)

    _write_marker()

    print(f"\nDone. The sample is gone and you have the smallest workflow that still "
          f"works: one agent, {STARTER_ID!r}, in one gated step, and no tools.")
    print("Run `pytest` now — it should pass. That is your safety net while you build.")
    print("\nThe edit boundary is now enforced: `pytest` fails if anything changes outside")
    print("app/workflow.json, app/subagents/, app/tools/ and kb_docs/. Delete")
    print("orchestrator/.agentexpress-customer to make a deliberate framework change.")
    print("\nNext:")
    print("  1. declare your data sources in `tools`")
    print(f"  2. rename {STARTER_ID!r}, and add the rest with `scaffold.py agent <id>`")
    print("  3. write `steps` for your topology, with a gate where a human signs off")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create a new agent's workflow.json entry and its folder.")
    sub = parser.add_subparsers(dest="what", required=True)

    r = sub.add_parser("reset", help="remove the shipped sample workflow, agents and tools")
    r.add_argument("--keep-kb", action="store_true",
                   help="leave kb_docs/ alone (you are reusing the sample corpora)")
    r.add_argument("--dry-run", action="store_true", help="print, write nothing")

    a = sub.add_parser("apply", help="write a bundle exported from the Builder into this tree")
    a.add_argument("bundle", help="the .json bundle the Build view exported")
    a.add_argument("--dry-run", action="store_true", help="print, write nothing")
    a.add_argument("--exact", action="store_true",
                   help="mirror the bundle exactly: regenerate every agent it has a prompt "
                        "for, and DELETE agent folders, app/tools/ folders and kb_docs/ "
                        "corpora it does not name (what a console deploy runs)")

    p = sub.add_parser("agent", help="a new agent")
    p.add_argument("agent_id", help="the agent id; also the folder name under app/subagents/")
    p.add_argument("--produces", default="", help="the deliverable name (default: <id>-output)")
    p.add_argument("--runtime", default="main", choices=("main", "dedicated"),
                   help="main = in-process, dedicated = its own AgentCore Runtime")
    p.add_argument("--max-tokens", type=int, default=4000, help="output token budget")
    p.add_argument("--tool", default="", help="a key in the workflow.json `tools` block")
    p.add_argument("--framework", default="plain", choices=sorted(FRAMEWORKS),
                   help="how agent.py reasons: plain ctx.llm, a Strands agent, or a LangGraph graph")
    p.add_argument("--remote", action="store_true",
                   help='runtime "a2a": an agent you do NOT operate. Writes no folder.')
    p.add_argument("--skill", default="", help="with --remote: which stand-in skill")
    p.add_argument("--dry-run", action="store_true", help="print, write nothing")
    args = parser.parse_args()

    if args.what == "reset":
        return reset(args.dry_run, args.keep_kb)
    if args.what == "apply":
        return apply(Path(args.bundle), args.dry_run, args.exact)

    agent_id = args.agent_id
    if not ID_RE.match(agent_id):
        print(f"scaffold: {agent_id!r} is not a valid agent id. It becomes part of an "
              f"AgentCore Runtime name, so it must match {ID_RE.pattern} — no hyphens.",
              file=sys.stderr)
        return 2
    args.produces = args.produces or f"{agent_id.replace('_', '-')}-output"

    workflow = json.loads(WORKFLOW.read_text(), object_pairs_hook=OrderedDict)
    # ALREADY IN CONFIG -> write the folder only, and leave workflow.json alone.
    #
    # This used to be a hard error, which made the tool useless for the order the docs
    # actually recommend: "define the workflow.json configuration, then define the agent
    # logic in the subagents folder". Designing the pipeline first and implementing the
    # agents second is the natural direction, and it was the one direction the scaffold
    # refused — found while building a foreign workflow exactly that way.
    existing = agent_id in workflow["agents"]
    if existing:
        spec_in_config = workflow["agents"][agent_id]
        placement = str(spec_in_config.get("runtime") or "main")
        if placement == "a2a":
            print(f"scaffold: agent {agent_id!r} is runtime \"a2a\" in workflow.json, so its "
                  f"code is somebody else's — there is no folder to create.", file=sys.stderr)
            return 2
        if (SUBAGENTS / agent_id).exists():
            print(f"scaffold: app/subagents/{agent_id}/ already exists, and {agent_id!r} is "
                  f"already in workflow.json. Nothing to do.", file=sys.stderr)
            return 2
    if args.tool and args.tool not in (workflow.get("tools") or {}):
        print(f"scaffold: --tool {args.tool!r} is not a key in the `tools` block "
              f"({', '.join(workflow.get('tools') or {}) or 'none declared'}). Declare the "
              f"tool first, or leave it off.", file=sys.stderr)
        return 2

    args.workflow = workflow
    # When the entry already exists it is the CUSTOMER'S; generating one and overwriting
    # theirs would throw away the decisions they came here having already made.
    spec = spec_in_config if existing else entry(args)
    folder = SUBAGENTS / agent_id
    written = {} if args.remote else files(agent_id, args.framework)

    if existing:
        print(f"agents.{agent_id} is already configured; writing the folder only:")
        print("  " + json.dumps(spec, indent=2, ensure_ascii=False).replace("\n", "\n  "))
    else:
        print(f"workflow.json  agents.{agent_id}:")
        print("  " + json.dumps(spec, indent=2, ensure_ascii=False).replace("\n", "\n  "))
    for name in written:
        print(f"app/subagents/{agent_id}/{name}")
    if args.remote:
        print("(no folder: runtime \"a2a\" means the code is somebody else's)")

    if args.dry_run:
        print("\n--dry-run: nothing written.")
        return 0

    if written:
        if folder.exists():
            print(f"scaffold: {folder} already exists.", file=sys.stderr)
            return 2
        folder.mkdir(parents=True)
        for name, body in written.items():
            (folder / name).write_text(body)

    if existing:
        # Their config, untouched. The whole point of this path.
        print(f"\nDone. app/subagents/{agent_id}/ now implements the entry you already "
              f"wrote; workflow.json was not modified.")
        return 0

    workflow["agents"][agent_id] = spec
    WORKFLOW.write_text(json.dumps(workflow, indent=2, ensure_ascii=False) + "\n")
    # Straight back into canonical order and readable form, so the file a customer opens
    # next never shows them a shape the framework would not have written.
    subprocess.run([sys.executable, str(ROOT / "format_workflow.py")],  # noqa: S603
                   cwd=ROOT, check=True)

    print(f"\nDone. One thing left, and it is the decision only you can make: put "
          f"{agent_id!r} in `steps`.")
    print(f'  {{ "agent": "{agent_id}", "hitl": true }}')
    print("Position matters — an agent reads whatever ran in an EARLIER step, so put it "
          "after the agents whose output it needs.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
