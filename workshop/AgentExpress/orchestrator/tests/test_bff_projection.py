"""What the browser is allowed to see, and how big a workflow may get.

`bff/workflow.py` projects workflow.json down to the subset the page reads. These
tests are the Python home of assertions that used to live in TypeScript
(`buildBffWorkflow` in cdk/test/config-plane.test.ts) and be compared against a
second HCL implementation by a parity test. There is one implementation now, so the
parity test is gone and these came here with it.

Two things are being pinned.

THE TRUST BOUNDARY. `GET /api/workflow` returns the projection verbatim, so
anything the projection carries is public to every authenticated UI user. Tool
endpoints, tool schemas, Cedar policy blocks, credentials and the guardrail's
denied-word list are deploy-time detail the page has no use for. The projection is
an ALLOW-LIST, so the default for a new workflow.json key is "not shipped" — the
safe direction.

THE CEILING, WHICH USED TO EXIST. The projection was built by the IaC and shipped in
a `WORKFLOW_JSON` Lambda environment variable. Lambda caps the whole environment at
4 KB and that quota cannot be raised, so both IaC paths carried a 3400-byte
precondition. The shipped ten-agent workflow measured 3153 bytes — about 147 bytes
per agent — so ELEVEN agents fitted and twelve did not, and a customer's twelfth
agent failed their deploy with advice to shorten its name. The workflow now travels
in the deployment package, and `test_a_large_workflow_is_not_a_deploy_failure`
exists to keep it that way.
"""
import json

import pytest


def _wf(**over) -> dict:
    """A minimal raw workflow. `over` replaces top-level blocks."""
    base = {
        "agents": {"a": {"name": "A"}},
        "steps": [{"agent": "a"}],
    }
    base.update(over)
    return base


def _agents(*ids) -> dict:
    return {i: {"name": i.upper()} for i in ids}


@pytest.fixture()
def project():
    """`bff.workflow.project`, imported without needing a bundled file."""
    import importlib
    import sys

    sys.modules.pop("workflow", None)
    module = importlib.import_module("workflow")
    yield module.project
    sys.modules.pop("workflow", None)


# ---------------------------------------------------------------------------
# The trust boundary
# ---------------------------------------------------------------------------

def test_deploy_time_detail_never_reaches_the_browser(project):
    """The reason the projection exists at all.

    Each of these is something a customer legitimately puts in workflow.json and
    which the page has no business holding: where a tool lives, what it accepts, who
    Cedar lets call it, and which words the guardrail blocks (a denied-word list is a
    map of what someone is trying to stop).
    """
    workflow = _wf(
        tools={"docs": {"type": "mcp", "endpoint": "https://internal.example/mcp",
                        "toolSchema": [{"name": "search", "properties": {"q": {}}}],
                        "policy": {"tool": "retrieve", "restrictTo": {"filter": ["x"]}}}},
        guardrail={"deniedWords": ["PROJECT_BLUEBIRD"],
                   "piiEntities": {"EMAIL": "ANONYMIZE"}},
    )
    shipped = json.dumps(project(workflow))
    for secret in ("internal.example", "toolSchema", "restrictTo", "PROJECT_BLUEBIRD",
                   "piiEntities", "guardrail"):
        assert secret not in shipped, f"the projection leaked {secret!r} to the browser"


def test_the_projection_is_an_allow_list_not_a_blocklist(project):
    """A key nobody thought about must default to NOT shipped.

    If this were a blocklist, every future workflow.json key would be public until
    someone remembered to exclude it — and the one that matters is always the one
    nobody thought about.
    """
    workflow = _wf(somethingNobodyHasThoughtOfYet={"apiSecret": "hunter2"})
    workflow["agents"]["a"]["privateNote"] = "internal only"
    out = project(workflow)
    assert "somethingNobodyHasThoughtOfYet" not in out
    assert "privateNote" not in out["agents"]["a"]
    assert set(out) == {"agents", "steps", "evalAgents", "chatbot", "ui", "authorization"}


def test_an_agents_projected_keys_are_exactly_what_the_ui_reads(project):
    out = project(_wf())
    assert set(out["agents"]["a"]) == {
        "name", "kind", "runtime", "tool", "corpus", "model", "source"}


# ---------------------------------------------------------------------------
# No ceiling
# ---------------------------------------------------------------------------

def test_a_large_workflow_is_not_a_deploy_failure(project):
    """Forty agents with long names — comfortably past the old 11-agent ceiling.

    The number 40 is not the new limit; there isn't one worth asserting. It is simply
    far enough past 11 to prove the constraint is gone rather than raised, which is
    what SSM Parameter Store would have done (4 KB standard, 8 KB advanced).
    """
    agents = {
        f"specialist_review_agent_{n:02d}": {
            "name": f"Specialist Review and Escalation Agent number {n:02d}",
            "runtime": "dedicated",
            "agentcore": {"evaluations": {"enabled": True}},
        }
        for n in range(40)
    }
    out = project({"agents": agents, "steps": [{"parallel": list(agents)}]})
    assert len(out["agents"]) == 40
    assert len(out["evalAgents"]) == 40
    # Recorded so the old ceiling is legible: this is what used to fail the deploy.
    assert len(json.dumps(out)) > 3400


def test_the_shipped_workflow_projects(project):
    """The real file, not a fixture."""
    from conftest import ORCH_ROOT

    shipped = json.loads((ORCH_ROOT / "app" / "workflow.json").read_text())
    out = project(shipped)
    assert set(out["agents"]) == set(shipped["agents"])
    assert out["steps"] == shipped["steps"]
    # Every agent gets a chip; an em dash here would mean an agent whose provenance
    # the reviewer cannot see.
    assert all(a["source"] for a in out["agents"].values())


# ---------------------------------------------------------------------------
# The data-source chip
# ---------------------------------------------------------------------------

def test_the_chip_comes_from_the_tools_declared_type(project):
    """Derived from the TYPE, not the name, so a customer's own tool gets a sensible
    chip with no UI change."""
    tools = {
        "kb": {"type": "kb", "corpora": ["reference"]},
        "ws": {"type": "websearch"},
        "docs": {"type": "mcp", "endpoint": "https://x.test"},
        "api": {"type": "openapi", "schemaS3Uri": "s3://b/s.json"},
        "fn": {"type": "lambda", "lambdaArn": "arn:aws:lambda:us-east-1:1:function:f"},
    }
    agents = _agents("a", "b", "c", "d", "e")
    agents["a"].update(tool="kb", corpus="reference")
    agents["b"].update(tool="ws")
    agents["c"].update(tool="docs")
    agents["d"].update(tool="api")
    agents["e"].update(tool="fn")
    out = project({"agents": agents, "steps": [{"parallel": list(agents)}], "tools": tools})
    assert out["agents"]["a"]["source"] == "Knowledge Base · reference"
    assert out["agents"]["b"]["source"] == "Web Search"
    assert out["agents"]["c"]["source"] == "MCP · docs"
    assert out["agents"]["d"]["source"] == "REST API · api"
    assert out["agents"]["e"]["source"] == "Function · fn"


def test_a_kb_tool_with_no_corpus_is_labelled_all(project):
    agents = {"a": {"name": "A", "tool": "kb"}}
    out = project({"agents": agents, "steps": [{"agent": "a"}],
                   "tools": {"kb": {"type": "kb", "corpora": ["reference"]}}})
    assert out["agents"]["a"]["source"] == "Knowledge Base · all"


def test_an_unknown_tool_type_falls_back_to_the_mcp_chip(project):
    """Matches app/features/gateway/client.py, which also treats an unrecognised type
    as MCP. A wrong chip is better than a blank one, and both IaC paths reject the
    type at plan/synth anyway."""
    out = project({"agents": {"a": {"name": "A", "tool": "x"}}, "steps": [{"agent": "a"}],
                   "tools": {"x": {"type": "something_new"}}})
    assert out["agents"]["a"]["source"] == "MCP · x"


def test_a_toolless_agents_chip_comes_from_its_access_wording(project):
    agents = _agents("a", "b", "c")
    agents["a"]["access"] = ["Session input (topic)"]
    agents["b"]["access"] = ["Upstream assets (orchestrator graph state)"]
    agents["c"]["access"] = ["Something else entirely"]
    out = project({"agents": agents, "steps": [{"parallel": list(agents)}]})
    assert out["agents"]["a"]["source"] == "Session input"
    assert out["agents"]["b"]["source"] == "Upstream agent outputs"
    assert out["agents"]["c"]["source"] == "—"


def test_a_remote_agent_is_matched_before_its_missing_tool(project):
    """An a2a agent has no `tool` by construction, so without being matched first it
    rendered as an em dash — and it is the one agent on the diagram whose provenance
    a reviewer most needs to see."""
    agents = {
        "external": {"name": "E", "runtime": "a2a",
                     "agentCard": "https://agents.partner.example/credit/v2"},
        "shipped": {"name": "S", "runtime": "a2a", "source": "a2a_lambda",
                    "skill": "compliance"},
    }
    out = project({"agents": agents, "steps": [{"parallel": list(agents)}]})
    # Host only — the chip is narrow and a full URL is mostly noise.
    assert out["agents"]["external"]["source"] == "A2A · agents.partner.example"
    # A framework-deployed stand-in has no host at synth time, so it is labelled by
    # its source. "remote" said nothing.
    assert out["agents"]["shipped"]["source"] == "A2A · a2a_lambda"


def test_the_agent_card_host_match_is_case_insensitive_but_does_not_lower_the_host(project):
    """Both halves were live bugs in the HCL version: a case-SENSITIVE scheme replace
    rendered "HTTPS://B.example" as the host "HTTPS:", and an empty card produced an
    empty chip."""
    from workflow import a2a_host

    assert a2a_host("HTTPS://B.example/z") == "B.example"
    assert a2a_host("https://a.example") == "a.example"
    assert a2a_host("") == "remote"
    assert a2a_host(None) == "remote"


# ---------------------------------------------------------------------------
# The rest of the projection
# ---------------------------------------------------------------------------

def test_tool_and_corpus_are_named_as_workflow_json_names_them(project):
    """The live drift this pins: workflow.json renamed these from mcp/rag, one
    projection followed and the other did not, so CDK deployments silently lost the
    data-source chips."""
    out = project({"agents": {"r": {"name": "R", "tool": "kb", "corpus": "reference"}},
                   "steps": [{"agent": "r"}],
                   "tools": {"kb": {"type": "kb", "corpora": ["reference"]}}})
    assert out["agents"]["r"]["tool"] == "kb"
    assert out["agents"]["r"]["corpus"] == "reference"
    assert "mcp" not in out["agents"]["r"]
    assert "rag" not in out["agents"]["r"]


def test_only_agents_with_evaluations_enabled_get_an_evaluate_button(project):
    agents = _agents("a", "b", "c")
    agents["a"]["agentcore"] = {"evaluations": {"enabled": True}}
    agents["b"]["agentcore"] = {"evaluations": {"enabled": False}}
    out = project({"agents": agents, "steps": [{"parallel": list(agents)}]})
    assert out["evalAgents"] == ["a"]


def test_only_the_disabled_chatbot_tool_flags_are_shipped(project):
    """`chatbot._enabled_tools` defaults anything it is not told about to on, so the
    two are equivalent and this keeps the payload proportional to what was switched
    off."""
    out = project(_wf(orchestrator={"chatbot": {
        "enabled": True, "model": "m", "tools": {"costs": False, "evals": True}}}))
    assert out["chatbot"] == {"enabled": True, "model": "m", "greeting": None,
                              "placeholder": None, "tools": {"costs": False}}


def test_the_assistants_greeting_and_placeholder_reach_the_page(project):
    """These were NOT shipped once, which made two workflow.json keys decorative: a
    customer reworded the assistant and the UI kept showing its own hardcoded copy."""
    out = project(_wf(orchestrator={"chatbot": {
        "enabled": True, "greeting": "Hello there", "placeholder": "Ask me…"}}))
    assert out["chatbot"]["greeting"] == "Hello there"
    assert out["chatbot"]["placeholder"] == "Ask me…"


def test_the_assistant_is_on_unless_switched_off(project):
    """Live: a build made in the Builder never set orchestrator.chatbot, so its app had
    no assistant. It is on by default now; only `"enabled": false` turns it off."""
    on = project(_wf())["chatbot"]
    assert on["enabled"] is True and on["tools"] == {}
    assert project(_wf(orchestrator={"chatbot": {"greeting": "Hi"}}))["chatbot"]["greeting"] == "Hi"
    assert project(_wf(orchestrator={"chatbot": {"enabled": False}}))["chatbot"] is None


def test_authorization_is_shipped_only_when_something_is_restricted(project):
    """With no `actions` map authz.py is a no-op, so there is nothing for the page to
    act on."""
    assert project(_wf())["authorization"] is None
    out = project(_wf(authorization={"groupsClaim": "cognito:groups",
                                     "actions": {"decision": ["approvers"]}}))
    assert out["authorization"] == {"groupsClaim": "cognito:groups",
                                    "actions": {"decision": ["approvers"]}}


def test_steps_pass_through_untouched_so_the_ui_renders_the_real_topology(project):
    steps = [{"agent": "a", "hitl": True},
             {"parallel": ["b", "c"], "hitl": True, "gateId": "g"},
             {"agent": "d", "branch": {"when": [{"field": "x", "exists": False,
                                                 "goto": "END"}]}}]
    assert project({"agents": _agents("a", "b", "c", "d"), "steps": steps})["steps"] == steps


def test_the_ui_block_drops_notes_and_empty_values(project):
    """A `*Note` is for whoever edits the file. An empty string would override the
    page's own fallback with nothing, which reads as a rendering bug."""
    out = project(_wf(ui={"title": "T", "heading": "H", "empty": "",
                          "uiNote": "for humans editing the file", "missing": None}))
    assert out["ui"] == {"title": "T", "heading": "H"}


def test_ui_is_null_rather_than_empty_when_absent(project):
    """So the page uses its own fallbacks instead of an empty object that looks
    configured."""
    assert project(_wf())["ui"] is None


# ---------------------------------------------------------------------------
# Where the workflow comes from
# ---------------------------------------------------------------------------

def test_the_env_var_overrides_the_bundled_file(monkeypatch):
    """The override is for tests and local dev, and it is the same precedence
    app/common/config.py uses for the runtime — one convention, not two."""
    import importlib
    import sys

    monkeypatch.setenv("WORKFLOW_JSON", json.dumps(_wf(ui={"defaultTopic": "from env"})))
    sys.modules.pop("workflow", None)
    module = importlib.import_module("workflow")
    try:
        assert module.DEFAULT_TOPIC == "from env"
        assert module.NODE_IDS == ["a"]
    finally:
        sys.modules.pop("workflow", None)


def test_a_missing_workflow_raises_instead_of_serving_an_empty_one(monkeypatch, tmp_path):
    """The old default was `{"agents":{},"steps":[]}`, which rendered a UI with no
    nodes and no error — indistinguishable from a broken deployment."""
    import importlib
    import sys

    monkeypatch.delenv("WORKFLOW_JSON", raising=False)
    sys.modules.pop("workflow", None)
    module = importlib.import_module("workflow")
    try:
        monkeypatch.setattr(module, "_BUNDLED", tmp_path / "absent.json")
        monkeypatch.setattr(module, "_IN_TREE", tmp_path / "also-absent.json")
        with pytest.raises(RuntimeError, match="found no workflow"):
            module.load_raw()
    finally:
        sys.modules.pop("workflow", None)


def test_a_source_checkout_falls_back_to_the_file_in_the_tree(monkeypatch):
    """So this module imports from a clone, where nothing has staged a copy next to
    it yet. In the Lambda the bundled copy is always present and wins."""
    import importlib
    import sys

    from conftest import ORCH_ROOT

    monkeypatch.delenv("WORKFLOW_JSON", raising=False)
    sys.modules.pop("workflow", None)
    module = importlib.import_module("workflow")
    try:
        assert not module._BUNDLED.exists(), (
            "a stale bff/workflow.json is in the source tree; only the BUILD should "
            "create it")
        assert module._IN_TREE == ORCH_ROOT / "app" / "workflow.json"
        assert module.NODE_IDS, "the in-tree fallback produced no agents"
    finally:
        sys.modules.pop("workflow", None)
