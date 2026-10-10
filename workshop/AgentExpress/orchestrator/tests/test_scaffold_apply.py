"""`scaffold.py apply`: what a bundle exported from the Build view does to a tree.

The Builder is only useful if it is a TWO-WAY door. A customer designs in the browser,
exports, hand-edits an agent, goes back to the browser to change the topology, and
exports again. If the second export flattened the hand edit, the Builder would be a
generator you can use exactly once, and nobody would trust it with a second pass.

So `apply` follows one rule, and these tests hold it to it: THE BUILDER OWNS DATA, THE
DEVELOPER OWNS CODE, AND NEITHER OVERWRITES THE OTHER.

Every test runs against a COPY of the framework in a temp directory, because `apply`
replaces app/workflow.json and writes agent folders, and doing that to the real tree
would change the sample every other test reads.
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ORCH = Path(__file__).resolve().parent.parent

#: A prompt that exercises every escape a Python literal can get wrong.
TRICKY = 'You triage "claims".\nRULES\n1. A path like C:\\claims\\ stays as is.\n2. Café — ünïcode.'


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    """The parts of orchestrator/ that `apply` and an agent import touch."""
    root = tmp_path / "orchestrator"
    root.mkdir()
    for name in ("scaffold.py", "format_workflow.py", "build_schema.py", "VERSION"):
        shutil.copy2(ORCH / name, root / name)
    shutil.copytree(ORCH / "app", root / "app",
                    ignore=shutil.ignore_patterns("__pycache__"))
    return root


def bundle(tmp: Path, workflow: dict, prompts: dict | None = None, **over) -> Path:
    doc = {"format": "agentexpress-bundle", "version": 1,
           "project": {"name": "Claims"}, "workflow": workflow,
           "prompts": prompts or {}, **over}
    path = tmp / "claims.agentexpress.json"
    path.write_text(json.dumps(doc, ensure_ascii=False))
    return path


def claims_workflow() -> dict:
    """The sample's single blocks, with a two-agent workflow of the customer's own."""
    sample = json.loads((ORCH / "app" / "workflow.json").read_text())
    return {
        **{k: sample[k] for k in ("$schema", "$comment", "orchestrator", "ui",
                                   "guardrail", "authorization")},
        "tools": {},
        "agents": {
            "claims_intake": {"name": "Claims Intake", "runtime": "main",
                              "maxTokens": 2000,
                              "agentcore": {"evaluations": {"enabled": True}}},
            "partner_check": {"name": "Partner Check", "runtime": "a2a",
                              "source": "a2a_lambda", "skill": "compliance",
                              "auth": "sigv4", "produces": "check"},
        },
        "steps": [{"agent": "claims_intake", "hitl": True}, {"agent": "partner_check"}],
    }


def run(root: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(  # noqa: S603 - fixed argv, no shell
        [sys.executable, str(root / "scaffold.py"), "apply", *args],
        capture_output=True, text=True, cwd=root, check=False)


def load_prompts(path: Path):
    spec = importlib.util.spec_from_file_location("builder_prompts_probe", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_a_new_agent_gets_a_folder_carrying_the_prompt_written_in_the_builder(tree, tmp_path):
    path = bundle(tmp_path, claims_workflow(),
                  {"claims_intake": {"systemPrompt": TRICKY, "schema": '{"claimId": "..."}'}})
    result = run(tree, str(path))
    assert result.returncode == 0, result.stderr

    folder = tree / "app" / "subagents" / "claims_intake"
    assert {p.name for p in folder.iterdir() if p.suffix == ".py"} == {
        "__init__.py", "agent.py", "prompts.py"}
    # Byte for byte: quotes, backslashes, line breaks and non-ASCII all survive the trip
    # through a Python literal.
    prompts = load_prompts(folder / "prompts.py")
    assert prompts.SYSTEM_PROMPT == TRICKY
    assert prompts.SCHEMA == '{"claimId": "..."}'
    # The scaffold's own agent.py, which reads the two names prompts.py defines.
    assert "from .prompts import SCHEMA, SYSTEM_PROMPT" in (folder / "agent.py").read_text()

    # A remote agent's code is somebody else's: no folder, by definition.
    assert not (tree / "app" / "subagents" / "partner_check").exists()


def test_the_generated_agent_passes_the_contract_the_registry_enforces(tree, tmp_path):
    """Loaded the way a container loads it, against the written workflow.json."""
    path = bundle(tmp_path, claims_workflow(),
                  {"claims_intake": {"systemPrompt": TRICKY, "schema": "{}"}})
    assert run(tree, str(path)).returncode == 0
    probe = ("from app.orchestrator.registry import build_agent_module\n"
             "a = build_agent_module('claims_intake')\n"
             "import json; print(json.dumps([a.id, a.max_tokens, a.system_prompt]))\n")
    env = {**os.environ, "PYTHONPATH": str(tree), "AWS_REGION": "us-east-1",
           "AWS_DEFAULT_REGION": "us-east-1"}
    env.pop("WORKFLOW_JSON", None)
    result = subprocess.run([sys.executable, "-c", probe], cwd=tree, env=env,  # noqa: S603
                            capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.strip().splitlines()[-1]) == ["claims_intake", 2000, TRICKY]


def test_the_written_workflow_is_already_in_canonical_form(tree, tmp_path):
    """Whatever key order the browser used, the file the customer opens next is the one
    `format_workflow.py --check` accepts."""
    wf = claims_workflow()
    # Deliberately scrambled: maxTokens before name, hitl before agent.
    wf["agents"]["claims_intake"] = {"maxTokens": 2000, "runtime": "main",
                                     "name": "Claims Intake",
                                     "agentcore": {"evaluations": {"enabled": True}}}
    wf["steps"][0] = {"hitl": True, "agent": "claims_intake"}
    assert run(tree, str(bundle(tmp_path, wf))).returncode == 0
    check = subprocess.run([sys.executable, str(tree / "format_workflow.py"), "--check"],  # noqa: S603
                           cwd=tree, capture_output=True, text=True, check=False)
    assert check.returncode == 0, check.stderr
    assert (tree / ".agentexpress-customer").exists()


def test_the_builder_rewrites_its_own_prompts_and_never_a_hand_edit(tree, tmp_path):
    folder = tree / "app" / "subagents" / "claims_intake"
    first = bundle(tmp_path, claims_workflow(),
                   {"claims_intake": {"systemPrompt": "v1", "schema": "{}"}})
    assert run(tree, str(first)).returncode == 0

    # 1. Still the Builder's file: a second export rewrites it.
    second = bundle(tmp_path, claims_workflow(),
                    {"claims_intake": {"systemPrompt": "v2", "schema": "{}"}})
    assert run(tree, str(second)).returncode == 0
    assert load_prompts(folder / "prompts.py").SYSTEM_PROMPT == "v2"

    # 2. The developer takes prompts.py over by deleting the marker paragraph, and
    #    rewrites agent.py. Neither is touched by the next export.
    owned = 'SYSTEM_PROMPT = "mine"\nSCHEMA = "{}"\n'
    (folder / "prompts.py").write_text(owned)
    (folder / "agent.py").write_text((folder / "agent.py").read_text() + "\n# hand edit\n")
    agent_before = (folder / "agent.py").read_text()
    third = bundle(tmp_path, claims_workflow(),
                   {"claims_intake": {"systemPrompt": "v3", "schema": "{}"}})
    result = run(tree, str(third))
    assert result.returncode == 0, result.stderr
    assert (folder / "prompts.py").read_text() == owned
    assert (folder / "agent.py").read_text() == agent_before
    assert "kept as is (your code)" in result.stdout


def test_an_agent_dropped_from_the_workflow_is_reported_not_deleted(tree, tmp_path):
    """A folder is code. Removing it is the developer's call; the tests will point at it."""
    shipped = [d.name for d in (tree / "app" / "subagents").iterdir()
               if d.is_dir() and not d.name.startswith("_")]
    result = run(tree, str(bundle(tmp_path, claims_workflow())))
    assert result.returncode == 0, result.stderr
    for name in shipped:
        assert (tree / "app" / "subagents" / name).is_dir()
        assert f"app/subagents/{name}/ — NOT in the workflow any more" in result.stdout


@pytest.mark.parametrize("mutate, expect", [
    (lambda b: b.update(format="something-else"), "is not an AgentExpress bundle"),
    (lambda b: b.update(version=99), "bundle version 99 is not supported"),
    (lambda b: b["workflow"].update(steps=[]), "no `agents` or no `steps`"),
    (lambda b: b["workflow"]["agents"].update({"claims-intake": {"name": "x"}}), "no hyphens"),
])
def test_a_bundle_that_cannot_be_applied_is_refused_before_anything_is_written(
        tree, tmp_path, mutate, expect):
    before = (tree / "app" / "workflow.json").read_text()
    doc = {"format": "agentexpress-bundle", "version": 1, "workflow": claims_workflow()}
    mutate(doc)
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(doc))
    result = run(tree, str(path))
    assert result.returncode == 2
    assert expect in result.stderr, result.stderr
    assert (tree / "app" / "workflow.json").read_text() == before


#: Runs a generated agent end to end in the temp tree, against a fake ctx: two tools
#: whose evidence must reach the model, and a model that answers with JSON. Prints
#: what the model was shown and what the agent returned.
RUN_PROBE = r'''
import asyncio, json
from app.orchestrator.registry import build_agent_module

agent = build_agent_module("claims_intake")
seen = []

class Ctx:
    agent_id = "claims_intake"; model = None; topic = "Triage claim 88213"
    feedback = ""; corpus = None; tools = ["policyDocs", "claimsDb"]; tool = "policyDocs"
    truncated_calls = []
    def input(self, _id): return None
    def _retrieval_query(self): return "claim 88213"
    async def retrieve(self, query, doc_type=None): return "Policy covers storm damage.", "gateway"
    async def call_tool(self, key, query): return "Claim 88213: roof, storm, 4 June.", "gateway"
    async def llm(self, system, user, name=None, **kw):
        seen.append(user)
        return json.dumps({"summary": "covered", "findings": ["storm damage"], "openQuestions": []})

out = asyncio.run(agent.run(Ctx()))
print(json.dumps({"out": out, "calls": len(seen), "user": seen[0]}))
'''


@pytest.mark.parametrize("framework", ["plain", "strands", "langgraph"])
def test_every_agent_framework_the_builder_offers_writes_an_agent_that_runs(tree, tmp_path, framework):
    """The Builder offers three ways to write an agent. Each must load under the
    registry's contract AND run: gather the evidence from every tool it is bound to,
    hand it to the model through ctx.llm, and return the model's answer."""
    wf = claims_workflow()
    wf["tools"] = {"policyDocs": {"type": "kb", "corpora": ["claims"], "maxResults": 5},
                   "claimsDb": {"type": "mcp", "endpoint": "https://example.com/mcp"}}
    wf["agents"]["claims_intake"]["tool"] = ["policyDocs", "claimsDb"]
    wf["agents"]["claims_intake"].pop("access", None)
    # The framework is a workflow.json key now, so an uploaded file carries it.
    wf["agents"]["claims_intake"]["framework"] = framework
    path = bundle(tmp_path, wf, {"claims_intake": {"systemPrompt": "You triage claims.",
                                                   "schema": "{}"}})
    result = run(tree, str(path))
    assert result.returncode == 0, result.stderr
    agent_py = (tree / "app" / "subagents" / "claims_intake" / "agent.py").read_text()
    if framework == "strands":
        assert "strands_agent" in agent_py
    if framework == "langgraph":
        assert "StateGraph" in agent_py

    env = {**os.environ, "PYTHONPATH": str(tree), "AWS_REGION": "us-east-1",
           "AWS_DEFAULT_REGION": "us-east-1",
           "TOOLS_JSON": json.dumps({"policyDocs": {"type": "kb"}, "claimsDb": {"type": "mcp"}})}
    env.pop("WORKFLOW_JSON", None)
    probe = subprocess.run([sys.executable, "-c", RUN_PROBE], cwd=tree, env=env,  # noqa: S603
                           capture_output=True, text=True, check=False)
    assert probe.returncode == 0, probe.stderr[-2000:]
    got = json.loads(probe.stdout.strip().splitlines()[-1])
    assert json.loads(got["out"])["summary"] == "covered"
    assert got["calls"] == 1                     # valid JSON first time: no repair pass
    assert "Policy covers storm damage." in got["user"]
    assert "Claim 88213: roof, storm, 4 June." in got["user"]


def test_an_unknown_framework_is_refused(tree, tmp_path):
    path = bundle(tmp_path, claims_workflow(),
                  {"claims_intake": {"systemPrompt": "x", "schema": "{}", "framework": "fortran"}})
    result = run(tree, str(path))
    assert result.returncode == 2
    assert "unknown agent framework" in result.stderr


def test_an_older_bundle_that_names_the_framework_in_its_prompts_still_applies(tree, tmp_path):
    path = bundle(tmp_path, claims_workflow(),
                  {"claims_intake": {"systemPrompt": "x", "schema": "{}", "framework": "strands"}})
    assert run(tree, str(path)).returncode == 0
    assert "strands_agent" in (tree / "app" / "subagents" / "claims_intake" / "agent.py").read_text()


def test_the_frameworks_workflow_json_accepts_are_the_ones_scaffold_writes():
    sys.path.insert(0, str(ORCH))
    try:
        import scaffold
    finally:
        sys.path.pop(0)
    vocab = json.loads((ORCH / "app" / "vocabulary.json").read_text())
    assert sorted(vocab["agentFrameworks"]["values"]) == sorted(scaffold.FRAMEWORKS)


def test_a_kb_corpus_with_no_documents_folder_is_named_before_the_deploy_finds_it(tree, tmp_path):
    """The one input a browser cannot supply is the documents. Both IaC paths refuse a
    corpus with no kb_docs/<corpus>/ folder, so `apply` names the folder up front."""
    wf = claims_workflow()
    wf["tools"] = {"policyDocs": {"type": "kb", "corpora": ["policies"], "maxResults": 5}}
    result = run(tree, str(bundle(tmp_path, wf)), "--dry-run")
    assert result.returncode == 0, result.stderr
    assert "kb_docs/policies/ — MISSING" in result.stdout


def test_dry_run_writes_nothing(tree, tmp_path):
    before = (tree / "app" / "workflow.json").read_text()
    result = run(tree, str(bundle(tmp_path, claims_workflow())), "--dry-run")
    assert result.returncode == 0, result.stderr
    assert "app/subagents/claims_intake/ — new" in result.stdout
    assert (tree / "app" / "workflow.json").read_text() == before
    assert not (tree / "app" / "subagents" / "claims_intake").exists()
    assert not (tree / ".agentexpress-customer").exists()


# --- --exact: what a console deploy runs ---------------------------------------------
# A deploy from the console applies ONE stored build version to a FRESH copy of the
# framework, and the result must be the same every time. So `--exact` makes the tree
# mirror the bundle: nothing from the shipped sample, and nothing from an earlier version
# of the build, survives unless the bundle names it.

def test_exact_deletes_every_agent_tool_folder_and_corpus_the_bundle_does_not_name(tree, tmp_path):
    (tree / "kb_docs" / "reference").mkdir(parents=True)
    (tree / "kb_docs" / "claims").mkdir(parents=True)
    (tree / "kb_docs" / "claims" / "policy.md").write_text("storm cover")
    wf = claims_workflow()
    wf["tools"] = {"claims_kb": {"type": "kb", "corpora": ["claims"]}}
    shipped = [d.name for d in (tree / "app" / "subagents").iterdir()
               if d.is_dir() and not d.name.startswith("_")]
    assert shipped, "the sample should ship agents for this test to mean anything"

    result = run(tree, str(bundle(tmp_path, wf)), "--exact")
    assert result.returncode == 0, result.stderr

    agents = {d.name for d in (tree / "app" / "subagents").iterdir() if d.is_dir()
              and not d.name.startswith("__")}
    assert agents == {"_shared", "claims_intake"}
    for name in shipped:
        assert f"app/subagents/{name}/ — DELETED" in result.stdout
    # No tool names a `source`, so every shipped tool folder goes.
    assert not [d for d in (tree / "app" / "tools").iterdir() if d.is_dir()]
    # The corpus the KB tool names stays; the one it does not is deleted.
    assert sorted(d.name for d in (tree / "kb_docs").iterdir()) == ["claims"]
    assert "kb_docs/reference/ — DELETED" in result.stdout


def test_exact_keeps_a_tool_folder_a_tool_still_uses(tree, tmp_path):
    wf = claims_workflow()
    wf["tools"] = {"pricing": {"type": "lambda", "source": "pricing"}}
    # Not left to the sample: after `scaffold.py reset` the tree has no tool folders.
    folder = tree / "app" / "tools" / "pricing"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "openapi.json").write_text("{}\n")
    assert run(tree, str(bundle(tmp_path, wf)), "--exact").returncode == 0
    assert sorted(d.name for d in (tree / "app" / "tools").iterdir() if d.is_dir()) == ["pricing"]


def test_exact_regenerates_an_agent_the_builder_wrote_a_prompt_for(tree, tmp_path):
    """In exact mode the Builder owns every agent it has a prompt for, so a hand edit
    left in the tree by an earlier apply does NOT survive — the stored version is the
    only source."""
    folder = tree / "app" / "subagents" / "claims_intake"
    first = bundle(tmp_path, claims_workflow(),
                   {"claims_intake": {"systemPrompt": "v1", "schema": "{}"}})
    assert run(tree, str(first)).returncode == 0
    (folder / "prompts.py").write_text('SYSTEM_PROMPT = "mine"\nSCHEMA = "{}"\n')
    (folder / "stray.py").write_text("x = 1\n")

    second = bundle(tmp_path, claims_workflow(),
                    {"claims_intake": {"systemPrompt": "v2", "schema": "{}",
                                       "framework": "strands"}})
    result = run(tree, str(second), "--exact")
    assert result.returncode == 0, result.stderr
    assert "regenerated from the Builder (strands)" in result.stdout
    assert load_prompts(folder / "prompts.py").SYSTEM_PROMPT == "v2"
    assert not (folder / "stray.py").exists()
    assert "Strands" in (folder / "agent.py").read_text()


def test_exact_keeps_a_shipped_agent_the_builder_has_no_prompt_for(tree, tmp_path):
    """A workflow opened from the sample carries no prompts for the sample's agents; their
    shipped code is the only implementation there is, so it stays."""
    sample = json.loads((tree / "app" / "workflow.json").read_text())
    keep = next(a for a in sample["agents"] if (tree / "app" / "subagents" / a).is_dir())
    before = (tree / "app" / "subagents" / keep / "agent.py").read_text()
    wf = claims_workflow()
    wf["agents"][keep] = sample["agents"][keep]
    wf["steps"].append({"agent": keep})
    result = run(tree, str(bundle(tmp_path, wf)), "--exact")
    assert result.returncode == 0, result.stderr
    assert (tree / "app" / "subagents" / keep / "agent.py").read_text() == before


def test_exact_is_deterministic(tree, tmp_path):
    """Applying the same version twice yields a byte-identical tree."""
    path = bundle(tmp_path, claims_workflow(),
                  {"claims_intake": {"systemPrompt": TRICKY, "schema": "{}"}})

    def snapshot() -> dict:
        return {str(p.relative_to(tree)): p.read_bytes() for p in sorted(tree.rglob("*"))
                if p.is_file() and "__pycache__" not in p.parts}

    assert run(tree, str(path), "--exact").returncode == 0
    once = snapshot()
    assert run(tree, str(path), "--exact").returncode == 0
    assert snapshot() == once


def test_exact_dry_run_names_the_deletions_and_writes_nothing(tree, tmp_path):
    before = {p for p in (tree / "app" / "subagents").iterdir()}
    result = run(tree, str(bundle(tmp_path, claims_workflow())), "--exact", "--dry-run")
    assert result.returncode == 0, result.stderr
    assert "DELETED" in result.stdout
    assert {p for p in (tree / "app" / "subagents").iterdir()} == before


def test_a_bundle_from_another_framework_version_is_applied_with_a_warning(tree, tmp_path):
    here = (tree / "VERSION").read_text().strip()
    same = run(tree, str(bundle(tmp_path, claims_workflow(), framework={"version": here})))
    assert same.returncode == 0, same.stderr
    assert "built on framework" not in same.stderr
    other = run(tree, str(bundle(tmp_path, claims_workflow(), framework={"version": "0.1.0"})))
    assert other.returncode == 0, other.stderr
    assert f"built on framework 0.1.0; this is framework {here}" in other.stderr


IMAGE_PROBE = r'''
import asyncio, json
from app.orchestrator.registry import build_agent_module

agent = build_agent_module("claims_intake")
rendered = []

class Ctx:
    agent_id = "claims_intake"; model = None; topic = "A hero image for the launch post"
    feedback = ""; corpus = None; tools = []; tool = None; truncated_calls = []
    def input(self, _id): return None
    def _retrieval_query(self): return self.topic
    async def llm(self, system, user, name=None, **kw):
        assert "negativePrompt" in user          # it asks for the brief, not the SCHEMA
        return json.dumps({"prompt": "a lighthouse at dawn", "negativePrompt": "text",
                           "caption": "Dawn over the coast"})
    async def image(self, prompt, negative_prompt=""):
        rendered.append((prompt, negative_prompt))
        return {"imageKey": "runs/s1/claims_intake/1.png", "model": "m"}

out = json.loads(asyncio.run(agent.run(Ctx())))
print(json.dumps({"out": out, "rendered": rendered}))
'''


def test_an_image_agent_writes_a_brief_and_renders_it(tree, tmp_path):
    wf = claims_workflow()
    wf["agents"]["claims_intake"].update(output="image", framework="strands",
                                         image={"aspectRatio": "16:9"})
    path = bundle(tmp_path, wf, {"claims_intake": {"systemPrompt": "You draw.", "schema": "{}"}})
    result = run(tree, str(path))
    assert result.returncode == 0, result.stderr
    agent_py = (tree / "app" / "subagents" / "claims_intake" / "agent.py").read_text()
    assert "ctx.image(" in agent_py and "strands" not in agent_py.split('"""', 2)[2]
    env = {**os.environ, "PYTHONPATH": str(tree), "AWS_REGION": "us-east-1",
           "AWS_DEFAULT_REGION": "us-east-1", "TOOLS_JSON": "{}"}
    env.pop("WORKFLOW_JSON", None)
    probe = subprocess.run([sys.executable, "-c", IMAGE_PROBE], cwd=tree, env=env,  # noqa: S603
                           capture_output=True, text=True, check=False)
    assert probe.returncode == 0, probe.stderr[-2000:]
    got = json.loads(probe.stdout.strip().splitlines()[-1])
    assert got["rendered"] == [["a lighthouse at dawn", "text"]]
    assert got["out"]["summary"] == "Dawn over the coast"
    assert got["out"]["images"] == [{"imageKey": "runs/s1/claims_intake/1.png", "model": "m"}]
