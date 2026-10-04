"""The OTHER half of the surface a customer edits: `app/subagents/<id>/`.

`workflow.json` says which agents exist; this folder is what they do. The config half
is now described once in `app/keys.json`, ordered by the formatter and explained to the
editor by a generated schema. This file does the same job for the code half, and the
job is smaller, because most of what is in an agent folder SHOULD vary — that is where
a customer's actual work lives.

SO THE CONTRACT IS DELIBERATELY THREE LINES LONG
A package that exports `agent`, an `Agent` subclass, a `run()`. That is all the
framework needs and all it asks. `registry.check_agent_module` enforces it wherever an
agent's real code is loaded, and the tests here make the same check STATICALLY — which
is where a customer should normally meet it: at `pytest`, before a deploy.

WHAT IS CONVENTION AND NOT CONTRACT, and the distinction matters
Splitting prompts into `prompts.py`, naming the output shape `SCHEMA`, one class per
folder: all good practice, none of it enforced against a customer. An agent that is
pure code and makes no model call has no use for a prompt file, and a framework that
rejected it would be asserting an opinion it cannot justify. Those conventions ARE
asserted over the SHIPPED agents, so the sample stays worth copying — which is a
different promise from dictating.

THE FAILURE THIS CLOSES
`Agent.run` raises NotImplementedError. So an agent folder that was scaffolded and never
finished used to import cleanly, configure cleanly, appear on the diagram, and fail the
instant the run reached it — after every agent before it had finished and been billed.
Nothing caught it earlier because nothing looked.
"""

from __future__ import annotations

import importlib
import json
import re
import shutil
import subprocess
import sys

import pytest
from conftest import ORCH_ROOT, some_agent, some_tool

SUBAGENTS = ORCH_ROOT / "app" / "subagents"
SHIPPED = json.loads((ORCH_ROOT / "app" / "workflow.json").read_text())

#: Agents whose code ships in this repo. Derived from the workflow, so renaming or
#: removing one is not a test to fix — and `runtime: "a2a"` is excluded because there is
#: no folder to check: the code is somebody else's.
LOCAL_AGENTS = sorted(aid for aid, spec in SHIPPED["agents"].items()
                      if str(spec.get("runtime") or "main") != "a2a")

#: Folders that are not agents. `_shared` is SAMPLE code the agents happen to reuse
#: (the two runners and the contracts), deliberately under subagents/ rather than in the
#: framework, because a customer replaces it.
NOT_AN_AGENT = {"_shared", "__pycache__"}


def folders() -> list[str]:
    return sorted(p.name for p in SUBAGENTS.iterdir()
                  if p.is_dir() and p.name not in NOT_AN_AGENT)


# ---------------------------------------------------------------------------
# The contract the framework actually enforces
# ---------------------------------------------------------------------------

def test_there_is_a_folder_for_every_local_agent_and_no_folder_without_one():
    """Both directions. A missing folder is a run that dies on an import; a LEFTOVER
    folder is worse in a quieter way — it is code a reader assumes is live, and it is
    the residue of exactly the change that removed an agent from the workflow.
    """
    assert LOCAL_AGENTS, "no local agents; this whole module would pass vacuously"
    assert folders() == LOCAL_AGENTS, (
        f"app/subagents/ and workflow.json disagree. Folders with no agent: "
        f"{sorted(set(folders()) - set(LOCAL_AGENTS))}. Agents with no folder: "
        f"{sorted(set(LOCAL_AGENTS) - set(folders()))}")


@pytest.mark.parametrize("agent_id", LOCAL_AGENTS)
def test_every_agent_folder_meets_the_contract(agent_id):
    """Run through the framework's own checker, not a re-implementation of it, so the
    static check and the runtime check cannot disagree."""
    from app.common.base import Agent
    from app.orchestrator.registry import check_agent_module

    module = importlib.import_module(f"app.subagents.{agent_id}")
    instance = check_agent_module(agent_id, module)
    assert isinstance(instance, Agent)
    assert type(instance).run is not Agent.run


@pytest.mark.parametrize("broken,expect", [
    ("no agent export", "does not export `agent`"),
    ("agent is not an Agent", "is not an app.common.base.Agent"),
    ("run not overridden", "does not override `run`"),
])
def test_the_checker_rejects_each_way_a_folder_can_be_wrong(broken, expect):
    """Each message has to name the file and what to add. A bare `AttributeError: module
    has no attribute 'agent'` is true and tells a customer nothing about the fix."""
    from types import SimpleNamespace

    from app.common.base import Agent
    from app.orchestrator.registry import check_agent_module

    class Unfinished(Agent):
        pass

    module = {
        "no agent export": SimpleNamespace(),
        "agent is not an Agent": SimpleNamespace(agent="just a string"),
        "run not overridden": SimpleNamespace(agent=Unfinished()),
    }[broken]

    with pytest.raises(ValueError, match=re.escape(expect)) as caught:
        check_agent_module("mine", module)
    # The message must point at the folder, and say what a correct one contains.
    assert "app/subagents/mine/" in str(caught.value)
    assert "from .agent import agent" in str(caught.value)


def test_a_missing_folder_is_reported_as_a_missing_folder():
    """Not as `ModuleNotFoundError: No module named 'app.subagents.x'`, which reads like a
    broken framework — and is the SAME error a misspelled `runtime` produces, so the
    message has to distinguish the two."""
    from conftest import workflow

    defn = {
        "orchestrator": {},
        "agents": {"ghost": {"name": "Ghost", "runtime": "main", "maxTokens": 100}},
        "steps": [{"agent": "ghost"}],
    }
    with workflow(defn) as imp:
        registry = imp("app.orchestrator.registry")
        with pytest.raises(ValueError) as caught:
            registry.build_agent_module("ghost")
    message = str(caught.value)
    assert "app/subagents/ghost/" in message
    assert "scaffold.py agent ghost" in message      # the way out
    assert 'runtime "a2a"' in message                # the other way out


# ---------------------------------------------------------------------------
# Conventions, asserted over the SAMPLE so it stays worth copying
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("agent_id", LOCAL_AGENTS)
def test_every_shipped_agent_folder_has_the_same_three_files(agent_id):
    """Uniform on purpose: whichever agent a customer opens first, they see the same
    shape. Not enforced against a customer — see this module's docstring."""
    present = {p.name for p in (SUBAGENTS / agent_id).iterdir() if p.suffix == ".py"}
    assert {"__init__.py", "agent.py", "prompts.py"} <= present, (
        f"app/subagents/{agent_id}/ has {sorted(present)}")


@pytest.mark.parametrize("agent_id", LOCAL_AGENTS)
def test_every_shipped_init_does_only_one_thing(agent_id):
    """Re-export `agent`, and nothing else. An `__init__.py` with logic in it is a place
    a reader has to look and normally should not have to."""
    body = (SUBAGENTS / agent_id / "__init__.py").read_text()
    assert body.strip() == 'from .agent import agent\n\n__all__ = ["agent"]', body


@pytest.mark.parametrize("agent_id", LOCAL_AGENTS)
def test_every_shipped_agent_keeps_its_prompt_out_of_its_code(agent_id):
    """The one split worth being strict about in the sample: a prompt is the part you
    edit fifty times, and you want to open a file that is only prompt. Asserted as "the
    system prompt is imported, not written here" rather than by line count."""
    import ast

    source = (SUBAGENTS / agent_id / "agent.py").read_text()
    assert re.search(r"from \.prompts import .*SYSTEM_PROMPT", source), (
        f"{agent_id}/agent.py does not import SYSTEM_PROMPT from its prompts.py")
    assert "SYSTEM_PROMPT" in (SUBAGENTS / agent_id / "prompts.py").read_text()

    # The prompt names must be DEFINED in prompts.py and only imported here. Stated as an
    # assignment check rather than as "no long string literal in agent.py", which is what
    # this was first: that flagged every agent's own module docstring, and then — with
    # docstrings exempted — flagged `cost_research` for a `limitations` sentence it BUILDS
    # AT RUN TIME from which service names came back unpriced. That text is an asset field
    # computed from data, it cannot live in a prompts file, and the test was simply wrong
    # about it. Length was never the property worth measuring.
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        for target in targets:
            if isinstance(target, ast.Name) and target.id in ("SYSTEM_PROMPT", "SCHEMA"):
                pytest.fail(
                    f"{agent_id}/agent.py line {node.lineno} defines {target.id}. The prompt "
                    f"and the output shape belong in prompts.py — that file is the one you "
                    f"will edit fifty times, and it should hold nothing else.")


@pytest.mark.parametrize("agent_id", LOCAL_AGENTS)
def test_every_shipped_agent_declares_its_system_prompt_on_the_class(agent_id):
    """`system_prompt` is what the evaluations feature and the observability prompt
    inspector read. Omitted, both degrade quietly to whatever the framework can infer."""
    module = importlib.import_module(f"app.subagents.{agent_id}")
    assert getattr(module.agent, "system_prompt", ""), (
        f"{agent_id} sets no system_prompt on its Agent subclass")


# ---------------------------------------------------------------------------
# The scaffold: what a customer's FIRST agent folder looks like
# ---------------------------------------------------------------------------
#
# The point of these is that the generated agent is COMPLETE, not a stub with a TODO
# that passes validation and fails at run time. Everything the scaffold writes is put
# through the same checks the shipped agents are.


def scaffold(*args: str):
    return subprocess.run(  # noqa: S603 - fixed argv, no shell
        [sys.executable, str(ORCH_ROOT / "scaffold.py"), "agent", *args],
        capture_output=True, text=True, cwd=ORCH_ROOT, check=False)


def test_the_scaffold_writes_an_agent_that_actually_loads_and_runs():
    """End to end, against the real repo, then cleaned up. Generating a folder that does
    not satisfy `check_agent_module` would hand every new customer a broken first agent,
    and nothing else in the suite would notice."""

    agent_id = "scaffold_probe"
    workflow_path = ORCH_ROOT / "app" / "workflow.json"
    before = workflow_path.read_text()
    folder = SUBAGENTS / agent_id
    try:
        kb = some_tool("kb")
        result = scaffold(agent_id, "--tool", kb)
        assert result.returncode == 0, result.stderr

        # The three files, and the contract.
        assert {p.name for p in folder.iterdir() if p.suffix == ".py"} == {
            "__init__.py", "agent.py", "prompts.py"}

        doc = json.loads(workflow_path.read_text())
        # Re-imported with the NEW workflow: `AGENTS` is resolved once at import, so the
        # already-loaded registry knows nothing about an agent written a moment ago. This
        # is also the honest test — it is the path a container takes on its next start.
        from conftest import workflow as with_workflow
        with with_workflow(doc) as imp:
            built = imp("app.orchestrator.registry").build_agent_module(agent_id)
            assert isinstance(built, imp("app.common.base").Agent)
            assert type(built).run is not imp("app.common.base").Agent.run
            assert built.id == agent_id
            assert built.max_tokens == 4000      # from the entry it wrote, not a default
            assert built.tool == kb
            assert built.system_prompt

        # The entry it wrote is valid config, in canonical order, with the corpus filled
        # in — a kb binding with no corpus retrieves across every corpus, which is almost
        # never what someone means and is invisible when it is wrong.
        spec = doc["agents"][agent_id]
        assert list(spec) == ["name", "runtime", "produces", "maxTokens", "tool", "corpus"]
        assert spec["corpus"] in doc["tools"][kb]["corpora"]

        import jsonschema
        schema = json.loads((ORCH_ROOT / "app" / "workflow.schema.json").read_text())
        assert list(jsonschema.Draft202012Validator(schema).iter_errors(doc)) == []

        # It does NOT put the agent in `steps`: where a step belongs depends on what it
        # consumes, and a guess would teach that ordering does not matter.
        assert agent_id not in json.dumps(doc["steps"])
        assert "steps" in result.stdout
    finally:
        shutil.rmtree(folder, ignore_errors=True)
        workflow_path.write_text(before)
        sys.modules.pop(f"app.subagents.{agent_id}", None)
        sys.modules.pop(f"app.subagents.{agent_id}.agent", None)


def test_the_scaffold_refuses_the_mistakes_worth_refusing():
    for args, expect in (
        (("cost-research", "--dry-run"), "no hyphens"),
        ((some_agent(runtime="main"), "--dry-run"), "already exists"),
        (("fine", "--tool", "nope", "--dry-run"), "not a key in the `tools` block"),
    ):
        result = scaffold(*args)
        assert result.returncode == 2, f"{args} was accepted"
        assert expect in result.stderr, result.stderr


def test_a_dry_run_writes_nothing():
    """It is the command a customer runs first, to see what they are about to get."""
    workflow_path = ORCH_ROOT / "app" / "workflow.json"
    before = workflow_path.read_text()
    result = scaffold("probe_dry", "--dry-run")
    assert result.returncode == 0, result.stderr
    assert workflow_path.read_text() == before
    assert not (SUBAGENTS / "probe_dry").exists()
    assert "nothing written" in result.stdout


def test_the_remote_variant_writes_config_and_no_folder():
    """`runtime: "a2a"` has no folder by definition, and scaffolding an empty one would
    contradict the placement it just configured."""
    result = scaffold("partner_probe", "--remote", "--dry-run")
    assert result.returncode == 0, result.stderr
    assert '"runtime": "a2a"' in result.stdout
    assert '"auth": "sigv4"' in result.stdout          # required with `source`
    assert "no folder" in result.stdout
    assert "app/subagents/partner_probe" not in result.stdout


def test_the_previewed_entry_is_already_in_canonical_order():
    """Asserted on the `--dry-run` OUTPUT, which is the one place the scaffold's own
    ordering is observable.

    This started as a grep for `top_level_order` in scaffold.py, and a mutation showed
    that was worth nothing: the scaffold runs `format_workflow.py` after writing, so the
    file ends up canonical however badly the entry was built, and the written-file
    assertion could not tell the difference. The PREVIEW is not formatted — it is what a
    customer reads before deciding — so a preview in a different order from the file they
    will get is the actual defect, and this is where it shows.
    """
    sys.path.insert(0, str(ORCH_ROOT))
    try:
        from format_workflow import top_level_order
        canonical = top_level_order("agent")
    finally:
        sys.path.pop(0)

    result = scaffold("order_probe", "--tool", some_tool("kb"), "--dry-run")
    assert result.returncode == 0, result.stderr
    previewed = re.findall(r'^\s+"(\w+)":', result.stdout, re.MULTILINE)
    assert previewed, result.stdout
    assert previewed == [k for k in canonical if k in previewed], (
        f"the preview reads {previewed}, canonical order is "
        f"{[k for k in canonical if k in previewed]}")


# ---------------------------------------------------------------------------
# app/tools/ — the other half of the customer's code
# ---------------------------------------------------------------------------
# A `type: "lambda"` tool's function is CUSTOMER logic, not framework: the pricing demo
# knows about AWS service codes and price-list dimensions, which is domain knowledge in
# exactly the way an agent's prompt is. It used to sit at `orchestrator/tool_lambda/`,
# among the framework directories, so custom logic lived in two unrelated places and
# "you edit workflow.json and app/subagents/" was not quite the whole truth.
#
# It is `app/tools/<name>/` now, beside `app/subagents/<id>/`. The two directories under
# app/ mirror the two blocks of workflow.json a customer fills in — `tools` and `agents`
# — which is the property worth having: where the config has a block, the code has a
# folder with the same name.

TOOLS_DIR = ORCH_ROOT / "app" / "tools"


# WHICH FILE a `source` folder must hold depends on the tool's type, because `source`
# means "the framework supplies this tool's artifact" and the artifact differs: a
# `lambda` target needs code to deploy, an `openapi` target needs a schema to upload.
# Both IaC paths check exactly this, and they check it because getting it wrong is
# silent — the upload or the zip succeeds with nothing in it and the Gateway target
# fails later, at invoke, naming neither the folder nor the file.
SOURCE_ARTIFACT = {"lambda": "handler.py", "openapi": "openapi.json"}


def _source_tools():
    """(tool name, source folder, required filename) for every tool using `source`."""
    for name, spec in (SHIPPED.get("tools") or {}).items():
        source = spec.get("source")
        if source:
            yield name, source, SOURCE_ARTIFACT[spec["type"]]


def test_a_tool_the_framework_supplies_has_its_source_under_app_tools():
    """`source` is a folder name under app/tools/, so a declared one must exist on disk —
    otherwise the IaC packages nothing and the Gateway target points at an empty
    function or an absent schema."""
    for name, source, artifact in _source_tools():
        assert (TOOLS_DIR / source / artifact).exists(), (
            f"tools.{name} declares source {source!r}, but "
            f"app/tools/{source}/{artifact} does not exist")


def test_every_tool_type_that_can_use_source_is_covered_by_the_artifact_map():
    """The map above is the test's own knowledge of what `source` means per type. If a
    sixth tool type ever accepts `source`, this fails rather than the map silently
    KeyError-ing on the first workflow that uses it."""
    keys = json.loads((ORCH_ROOT / "app" / "keys.json").read_text())
    applies = keys["tool"]["keys"]["source"]["appliesTo"]
    assert set(applies) == set(SOURCE_ARTIFACT), (
        f"app/keys.json says `source` applies to {sorted(applies)}, but this test "
        f"knows the artifact for {sorted(SOURCE_ARTIFACT)}")


def test_no_tool_source_folder_is_orphaned():
    """The other direction. A folder here is deployed only if a tool names it, so one
    nothing names is dead code that reads as live — the same rule app/subagents/ has."""
    declared = {spec["source"] for spec in (SHIPPED.get("tools") or {}).values()
                if spec.get("source")}
    present = {p.name for p in TOOLS_DIR.iterdir()
               if p.is_dir() and p.name != "__pycache__"}
    assert present == declared, (
        f"app/tools/ and workflow.json disagree. Folders no tool declares: "
        f"{sorted(present - declared)}. Declared with no folder: {sorted(declared - present)}")


def test_the_tool_source_is_not_shipped_inside_the_orchestrator_container():
    """It sits under app/, and everything else under app/ IS copied into the image — so
    this needs saying explicitly. The function is deployed as its own Lambda zip and
    called through the Gateway; the orchestrator never imports it, and shipping it would
    mean a change to a tool's code rebuilt and redeployed the orchestrator too."""
    ignored = (ORCH_ROOT / ".dockerignore").read_text().splitlines()
    assert "app/tools/" in [line.strip() for line in ignored], (
        ".dockerignore does not exclude app/tools/, so the tool function's source would "
        "be baked into the orchestrator image")


def test_a_tool_function_is_not_importable_as_an_agent():
    """app/tools/ must not accidentally satisfy the agent folder contract, or a tool
    would show up as an agent the framework tried to run in-process."""
    for folder in TOOLS_DIR.iterdir():
        if not folder.is_dir() or folder.name == "__pycache__":
            continue
        assert not (folder / "__init__.py").exists(), (
            f"app/tools/{folder.name}/ has an __init__.py, which makes it an importable "
            f"package — a tool's handler or schema is not an agent and is not "
            f"imported here")
        assert folder.name not in (SHIPPED.get("agents") or {}), (
            f"app/tools/{folder.name}/ collides with an agent id")
