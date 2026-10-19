"""examples/demo-everything: the one build whose single run shows every tool type and
AgentCore feature. The owner deploys it from the Builder console, so it must pass the
check the BFF runs before a deploy (bff/validate_build.py, and the code tools' static
check), keep showing what its README says it shows, and stay what make_project.py
writes. The page's validator runs the same files in web/src/builder/demoEverything.test.ts.
"""
from __future__ import annotations

import json
import subprocess
import sys

import pytest
from conftest import ORCH_ROOT

sys.path.insert(0, str(ORCH_ROOT / "bff"))
import codecheck
import validate_build

HERE = ORCH_ROOT / "examples" / "demo-everything"
FILES = ["project.json", "project.byo-harness.json"]
PLACEHOLDER = "REPLACE_ME"


def _project(name: str) -> dict:
    return json.loads((HERE / name).read_text())["project"]


def test_files_are_what_the_generator_writes():
    run = subprocess.run([sys.executable, str(HERE / "make_project.py"), "--check"],  # noqa: S603  # nosec B603
                         capture_output=True, text=True, cwd=ORCH_ROOT)
    assert run.returncode == 0, run.stderr


@pytest.mark.parametrize("name", FILES)
def test_the_deploy_check_finds_no_error(name):
    p = _project(name)
    assert validate_build.errors(p["workflow"]) == []


@pytest.mark.parametrize("name", FILES)
def test_code_written_in_the_build_passes_the_static_check(name):
    """What builds.code_errors refuses a deploy for: each function's files."""
    p = _project(name)
    wf = p["workflow"]
    coded = [k for k, t in wf["tools"].items() if t.get("type") == "lambda" and "code" in t]
    coded += [f"interceptor-{pt}" for pt, ic in wf["orchestrator"]["interceptors"].items() if "code" in ic]
    assert sorted(p["toolCode"]) == sorted(coded)
    for key in coded:
        names = [s["name"] for s in (wf["tools"].get(key) or {}).get("toolSchema") or []]
        errors = [i for i in codecheck.static(p["toolCode"][key], names) if i["severity"] == "error"]
        assert errors == [], key


def test_one_run_uses_every_kind():
    wf = _project("project.json")["workflow"]
    agents, tools = wf["agents"], wf["tools"]
    assert {t["type"] for t in tools.values()} == {"kb", "websearch", "mcp", "openapi", "lambda", "smithy"}
    assert {t.get("source") for t in tools.values() if t["type"] == "lambda"} == {None, "codeInterpreter", "browser"}
    assert {a.get("runtime", "main") for a in agents.values()} == {"main", "dedicated", "task", "harness", "a2a"}
    assert {a["task"]["kind"] for a in agents.values() if a.get("runtime") == "task"} == {
        "code", "extract", "tool", "output"}
    harness = [a["harness"] for a in agents.values() if a.get("runtime") == "harness"]
    assert any(h.get("codeInterpreter") and h.get("browser") for h in harness)
    assert any(a.get("tool") for a in agents.values() if a.get("runtime") == "harness")
    every_tool = {k for a in agents.values() for k in validate_build.tools_of(a.get("tool"))}
    every_tool |= {a["task"]["tool"] for a in agents.values() if (a.get("task") or {}).get("kind") == "tool"}
    assert every_tool == set(tools), "a tool no step uses is not shown by the run"
    core = [a.get("agentcore") or {} for a in agents.values()]
    assert any((c.get("guardrails") or {}).get("use") for c in core)
    assert any((c.get("guardrails") or {}).get("input") and not (c.get("guardrails") or {}).get("use") for c in core)
    assert any((c.get("memory") or {}).get("use") for c in core)
    assert any((c.get("memory") or {}).get("longTerm") for c in core)
    assert any((c.get("evaluations") or {}).get("auto") for c in core)
    assert any(a.get("askPerson") for a in agents.values())
    assert any(a.get("skills") for a in agents.values())
    assert any(a.get("models", {}).get("route") and a["models"].get("fallback") for a in agents.values())
    assert any(s.get("hitl") for s in wf["steps"]) and any(s.get("branch") for s in wf["steps"])
    orch = wf["orchestrator"]
    for key in ("limits", "liveConfig", "models", "files", "attachments", "interceptors", "policy"):
        assert orch.get(key), key


def test_the_policy_deny_is_reachable_in_the_run():
    """The forbid names the second claim id the request carries, so the Drafter's lookup
    of it is refused; the browser's permit is narrowed to the page its agent opens."""
    wf = _project("project.json")["workflow"]
    statement = wf["policies"]["noOtherCustomers"]["statement"]
    assert statement.startswith("forbid(") and '"CLM-2002"' in statement
    assert "CLM-2002" in wf["ui"]["defaultTopic"]
    assert "noOtherCustomers" in wf["tools"]["claimsDb"]["policies"]
    assert "claimsDb" in wf["agents"]["drafter"]["tool"]
    assert "https://example.com" in wf["tools"]["web"]["policy"]["restrictTo"]["url"]


def test_the_branch_skips_the_fraud_review_on_this_request():
    """intake's code prints route "standard" unless the request says fraud."""
    wf = _project("project.json")["workflow"]
    assert "fraud" not in wf["ui"]["defaultTopic"].lower()
    rule = wf["steps"][0]["branch"]["when"][0]
    assert (rule["field"], rule["equals"], rule["goto"]) == ("output.route", "standard", "gather")
    assert wf["steps"][1] == {"agent": "fraudReview"}


def test_only_the_variant_carries_the_harness_placeholder():
    main, byo = (HERE / FILES[0]).read_text(), (HERE / FILES[1]).read_text()
    assert PLACEHOLDER not in main
    byo_wf = _project(FILES[1])["workflow"]
    assert PLACEHOLDER in byo_wf["agents"]["byoHarness"]["harness"]["arn"]
    assert "byoHarness" in byo_wf["steps"][2]["parallel"]
    assert byo.count(PLACEHOLDER) == 1
