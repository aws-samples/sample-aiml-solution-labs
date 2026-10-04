"""Shared harness for the config-plane tests.

These tests exercise the code that TRANSLATES `workflow.json` into a running
system — the topology derivation, the graph wiring, the tool call shapes, the
contract coercion and the RBAC rules. They call no AWS APIs and no model, so they
run in about a second and are safe to run in CI.

Why a harness is needed
-----------------------
`app/common/config.py` reads the workflow ONCE, at import, and binds `AGENTS`,
`STEPS`, `FIRST_AGENT_ID` and `LAST_AGENT_ID` as module-level values. Other
modules then bind those same objects (`graph_builder` imports `STEPS`,
`registry` imports `AGENTS`). That is the right design for a container that serves
one workflow for its whole life, but it means a test cannot simply mutate config
and re-call a function: the bindings are already made.

`workflow()` below therefore sets `WORKFLOW_JSON` and re-imports the `app`
package tree from scratch, so each test gets a genuinely fresh view. That is also
exactly what a customer does when they swap the file, which is the behaviour under
test.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest

# The tests import `app.*` (the runtime) and `authz`/`chatbot` (the BFF, which
# ships as its own zip and so has no package prefix). Both live under
# orchestrator/, one level up from here.
ORCH_ROOT = Path(__file__).resolve().parent.parent
for p in (str(ORCH_ROOT), str(ORCH_ROOT / "bff")):
    if p not in sys.path:
        sys.path.insert(0, p)

# The BFF creates its DynamoDB resource handles at import time. Constructing a
# boto3 resource makes no network call, but it does need a table name and a
# region, so provide throwaway ones for the whole session.
os.environ.setdefault("STATUS_TABLE", "test-status")
os.environ.setdefault("EVENTS_TABLE", "test-events")
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")


def _purge_app_modules() -> None:
    """Drop every already-imported `app.*` module so the next import re-reads config."""
    for name in [m for m in sys.modules if m == "app" or m.startswith("app.")]:
        del sys.modules[name]


@contextmanager
def workflow(defn: dict):
    """Import the runtime with `defn` as the workflow, then restore.

    Yields a function that imports a module by name from the fresh tree:

        with workflow(WF) as imp:
            cfg = imp("app.common.config")
            assert cfg.FIRST_AGENT_ID == "start"
    """
    previous = os.environ.get("WORKFLOW_JSON")
    os.environ["WORKFLOW_JSON"] = json.dumps(defn)
    _purge_app_modules()
    try:
        yield importlib.import_module
    finally:
        if previous is None:
            os.environ.pop("WORKFLOW_JSON", None)
        else:
            os.environ["WORKFLOW_JSON"] = previous
        # Purge on the way out too: the modules imported above hold bindings to
        # `defn`, and leaving them in sys.modules would leak into the next test.
        _purge_app_modules()


def agents(*ids: str, **overrides) -> dict:
    """Minimal `agents` entries for the given ids.

    `runtime: "dedicated"` on purpose. It is a real, supported placement, and it
    means the registry builds an `AgentCoreRuntimeAgent` instead of importing
    `app.subagents.<id>` — so a topology test can use any agent ids it likes
    without also inventing agent packages on disk.
    """
    return {i: {"name": i.replace("_", " ").title(), "runtime": "dedicated", **overrides}
            for i in ids}


def wf(steps: list[dict], *, tools: dict | None = None, **extra) -> dict:
    """A complete workflow definition whose `agents` are derived from `steps`."""
    ids: list[str] = []
    for s in steps:
        ids += s.get("parallel") or s.get("sequence") or [s["agent"]]
    return {"orchestrator": {}, "agents": agents(*dict.fromkeys(ids)),
            "steps": steps, "tools": tools or {}, **extra}


@pytest.fixture()
def shipped() -> dict:
    """The workflow.json actually shipped in this repo.

    Used for the tests that assert the sample's own topology, so that editing the
    sample without updating these expectations is caught.
    """
    return json.loads((ORCH_ROOT / "app" / "workflow.json").read_text())


# ---------------------------------------------------------------------------
# Expectations DERIVED from a workflow, not restated as literals
# ---------------------------------------------------------------------------
#
# The "shipped workflow" tests used to hardcode this sample's agent ids —
# `FIRST_AGENT_ID == "intake"`, `upstream_of("analysis") == [...]`. That quietly
# made the test suite part of the customer-editable surface: renaming or removing
# an agent in workflow.json turned tests red even though the framework was fine,
# and the promise is that workflow.json + app/subagents/ is all you touch.
#
# The helpers below compute the same expectations from whatever workflow is under
# test. They assert the INVARIANT rather than the sample, which is both portable
# and a stronger check: `expected_upstream` covers every agent instead of the
# three that were spot-checked.


def ids_of(step: dict) -> list[str]:
    """The agent ids in one `steps` entry, whatever its shape."""
    return list(step.get("parallel") or step.get("sequence") or [step["agent"]])


def all_ids(defn: dict) -> list[str]:
    """Every agent id in topology order."""
    return [i for s in defn["steps"] for i in ids_of(s)]


def expected_first(defn: dict) -> str:
    return ids_of(defn["steps"][0])[0]


def expected_last(defn: dict) -> str:
    return ids_of(defn["steps"][-1])[-1]


def expected_upstream(defn: dict, agent_id: str) -> list[str]:
    """What `config.upstream_of` must return: every agent from an earlier step, plus
    earlier members of this agent's own SEQUENCE step, newest first. Parallel peers
    are excluded — a group's members must not depend on each other."""
    seen: list[str] = []
    for step in defn["steps"]:
        members = ids_of(step)
        if agent_id in members:
            if "sequence" in step:
                seen.extend(members[: members.index(agent_id)])
            return list(reversed(seen))
        seen.extend(members)
    raise AssertionError(f"{agent_id} is in no step")


def expected_gate_nodes(defn: dict) -> set[str]:
    """One gate node per gated step, named the way graph_builder names it: a
    single-agent step gates on the agent id, a group on `gateId` or a positional
    fallback (see graph_builder._gate_id)."""
    out: set[str] = set()
    for i, step in enumerate(defn["steps"]):
        if not step.get("hitl"):
            continue
        base = step["agent"] if "agent" in step else (step.get("gateId") or f"group{i}")
        out.add(f"{base}_gate")
    return out


@pytest.fixture(scope="session")
def shipped_ids() -> dict:
    """The shipped workflow's first and last agent ids.

    A test that needs to reach into a specific agent's PACKAGE (its prompts, its
    source) resolves the folder through this rather than naming it, so renaming an
    agent stays a workflow.json + app/subagents/ change.
    """
    defn = json.loads((ORCH_ROOT / "app" / "workflow.json").read_text())
    return {"first": expected_first(defn), "last": expected_last(defn)}


# ---------------------------------------------------------------------------
# Tests about the SAMPLE's own agents, vs tests about the FRAMEWORK
# ---------------------------------------------------------------------------
#
# The promise is that a customer edits `workflow.json` + `app/subagents/<id>/` and has
# "no test to fix". That was measured and it was FALSE: replacing the shipped eight-agent
# workflow with a five-agent insurance-claims pipeline produced 93 failures out of 734,
# and every single one of them named a deleted sample agent or tool. Not one was a
# framework defect — the framework was fine, and the suite was telling the customer their
# config was broken.
#
# Two different kinds of test were tangled together:
#
#   FRAMEWORK tests must pass against ANY workflow. Where one needs a concrete agent to
#   mutate or a tool to bind to, it takes it FROM the workflow under test (see
#   `some_agent` / `some_tool` below) instead of naming one.
#
#   SAMPLE tests are about this sample's editorial choices — that `cost_research` reads
#   tool ROWS rather than paraphrasing them, that `web_search` reasons inside Strands,
#   that the report agent orders its sections. Those are worth keeping, because they are
#   what makes the sample worth copying, and they are meaningless when the agent is gone.
#   They SKIP, with a reason that says so.
#
# Skipping rather than deleting, and skipping rather than failing: a customer who keeps
# `cost_research` still gets its tests, and one who replaces it is not handed a red suite
# to interpret.


def _shipped() -> dict:
    return json.loads((ORCH_ROOT / "app" / "workflow.json").read_text())


def needs_agent(*agent_ids: str):
    """Skip unless every named agent is in the workflow. For a test about THAT agent."""
    present = set(_shipped().get("agents") or {})
    missing = [a for a in agent_ids if a not in present]
    return pytest.mark.skipif(
        bool(missing),
        reason=(f"about the sample agent(s) {', '.join(missing)}, which this workflow does "
                f"not have. The framework's own behaviour is covered elsewhere."))


def needs_tool(*tool_names: str):
    """Skip unless every named tool is declared. For a test about THAT tool's shape."""
    present = set(_shipped().get("tools") or {})
    missing = [t for t in tool_names if t not in present]
    return pytest.mark.skipif(
        bool(missing),
        reason=(f"about the sample tool(s) {', '.join(missing)}, which this workflow does "
                f"not declare."))


def some_agent(*, runtime: str = "", with_tool: bool | None = None) -> str:
    """ANY agent id from the shipped workflow that matches, for a FRAMEWORK test.

    A framework test that needs something to mutate — "rejects maxTokens on a remote
    agent" — needs *a* remote agent, not a particular one. Naming `analysis` made those
    tests part of the customer-editable surface: 23 of the 93 failures above were exactly
    this, in a file whose own docstring says it is about the framework.
    """
    for agent_id, spec in (_shipped().get("agents") or {}).items():
        if runtime and str(spec.get("runtime") or "main") != runtime:
            continue
        if with_tool is True and not spec.get("tool"):
            continue
        if with_tool is False and spec.get("tool"):
            continue
        return agent_id
    pytest.skip(f"this workflow has no agent with runtime={runtime or 'any'!r}, "
                f"with_tool={with_tool}")


def some_tool(tool_type: str = "") -> str:
    """ANY tool name of the given type, for a FRAMEWORK test."""
    for name, spec in (_shipped().get("tools") or {}).items():
        if not tool_type or str(spec.get("type") or "").lower() == tool_type:
            return name
    pytest.skip(f"this workflow declares no tool of type {tool_type!r}")
