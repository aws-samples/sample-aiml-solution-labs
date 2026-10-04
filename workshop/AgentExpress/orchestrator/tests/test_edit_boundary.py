"""A workflow implementation touches only the four surfaces a customer owns.

WHY THIS EXISTS ALONGSIDE THE HOOK
`.kiro/hooks/guard-edit-boundary.sh` blocks a write to a framework file before it happens,
which is the better place to stop it. But it only sees the write TOOLS: an agent can reach
the filesystem through `execute_bash` instead, and closing that path means policing every
shell command, which breaks the build, the formatters and the generators.

So the hook prevents and this detects. It inspects the working TREE rather than the call,
so it does not care how a change arrived — and if the hook is disabled, bypassed, or
running somewhere jq is not installed, the gates still catch it. The skill's own final step
runs this suite, so it cannot report success over a violated boundary.

WHY IT IS OFF DURING FRAMEWORK DEVELOPMENT
In this repository, framework files are *supposed* to change — that is what developing the
framework is. The boundary only applies once the tree has become a customer's deployment,
which is exactly what `scaffold.py reset` marks. No marker, no enforcement, and the skip
message says which mode it is in rather than staying silent.
"""

import subprocess
from pathlib import Path

import pytest

ORCH = Path(__file__).resolve().parent.parent
REPO = ORCH.parent

#: Written by `scaffold.py reset`. Its presence means "this tree is a workflow
#: implementation, not the framework's own development".
MARKER = ORCH / ".agentexpress-customer"

#: The four surfaces, repo-relative. A path is allowed when it starts with one of these.
#:
#: workflow.schema.json and defaults.json are GENERATED from app/keys.json by
#: build_schema.py, so they move on their own and are not a customer edit.
#:
#: cdk/cdk.json is DEPLOY CONFIG, not framework code: it carries the agent name, the model
#: id and the gateway switch as CDK context, which is a customer's choice. Terraform's
#: equivalent, terraform.tfvars, needs no entry — it is gitignored, so it never appears in
#: `git status` at all. The hook allows both, because the hook judges the write rather than
#: the working tree.
ALLOWED = (
    "orchestrator/app/workflow.json",
    "orchestrator/app/workflow.schema.json",
    "orchestrator/app/defaults.json",
    "orchestrator/app/subagents/",
    "orchestrator/app/tools/",
    "orchestrator/kb_docs/",
    "orchestrator/cdk/cdk.json",
    "orchestrator/.agentexpress-customer",
    ".kiro/",
)


def _changed_paths() -> list[str]:
    """Every path git reports as modified, added, deleted or untracked.

    `--porcelain` so the format is stable, `-uall` so a new file inside a new directory is
    listed individually rather than as its parent — otherwise a stray file hides behind the
    directory that contains it.
    """
    out = subprocess.run(
        ["git", "status", "--porcelain", "-uall"],  # noqa: S607
        cwd=REPO, capture_output=True, text=True, check=False)
    if out.returncode != 0:
        pytest.skip("not a git repository, so there is nothing to compare against")
    paths = []
    for line in out.stdout.splitlines():
        if not line.strip():
            continue
        entry = line[3:]
        # A rename is reported as "old -> new"; the destination is what was written.
        if " -> " in entry:
            entry = entry.split(" -> ", 1)[1]
        paths.append(entry.strip().strip('"'))
    return paths


@pytest.mark.skipif(
    not MARKER.exists(),
    reason="framework development mode: orchestrator/.agentexpress-customer is absent, so "
           "framework files are expected to change. `scaffold.py reset` writes that marker "
           "and turns this check on.")
def test_only_the_four_customer_surfaces_have_changed():
    outside = [p for p in _changed_paths() if not p.startswith(ALLOWED)]
    assert not outside, (
        "these changes are outside the four surfaces a workflow implementation owns:\n"
        + "\n".join(f"    {p}" for p in outside)
        + "\n\n  orchestrator/app/workflow.json     agents, topology, tools, gates, RBAC, UI"
          "\n  orchestrator/app/subagents/<id>/   one folder per agent"
          "\n  orchestrator/app/tools/<name>/     a tool's handler.py or openapi.json"
          "\n  orchestrator/kb_docs/<corpus>/     your documents"
          "\n\nEverything else is generated from those four, so editing it directly is "
          "either overwritten on the next generate or left disagreeing with the config "
          "that produced its neighbours. If a framework file genuinely has to change, do "
          "it deliberately and on its own — and delete "
          "orchestrator/.agentexpress-customer while you do."
    )


def test_the_allowlist_matches_the_surfaces_the_docs_promise():
    """The boundary and the documented promise are the same four places.

    Guards against the allowlist quietly widening: a path added here to make a failure go
    away is the framework's promise narrowing, and nothing else would notice.
    """
    generated_or_config = (".schema.json", "defaults.json", ".agentexpress-customer",
                           "cdk/cdk.json")
    surfaces = {p for p in ALLOWED
                if not p.endswith(generated_or_config) and p != ".kiro/"}
    assert surfaces == {
        "orchestrator/app/workflow.json",
        "orchestrator/app/subagents/",
        "orchestrator/app/tools/",
        "orchestrator/kb_docs/",
    }, f"the allowlist no longer matches the four documented surfaces: {sorted(surfaces)}"


def test_the_hook_that_prevents_this_is_still_installed():
    """The test and the hook are a pair; losing the hook silently leaves only detection."""
    script = REPO / ".kiro" / "hooks" / "guard-edit-boundary.sh"
    config = REPO / ".kiro" / "hooks" / "guard-edit-boundary.json"
    assert script.is_file(), f"{script} is missing: the boundary is no longer PREVENTED"
    assert config.is_file(), f"{config} is missing: the guard script is never invoked"
    body = script.read_text()
    for surface in ("app/subagents/", "app/tools/", "kb_docs/", "app/workflow.json"):
        assert surface in body, f"the hook no longer allows {surface}"
    # Deploy config, which the first version of the hook blocked — it would have stopped a
    # customer setting their own region.
    for deploy in ("terraform.tfvars", "cdk/cdk.json"):
        assert deploy in body, (
            f"the hook no longer allows {deploy}, so a customer cannot configure their own "
            f"deployment")
    assert "exit 2" in body, "the hook no longer blocks: exit 2 is what stops a tool call"
