"""An agent can read SEVERAL tools: `tool` is one key or a list of them.

Every plane that reads `tool` has to agree on that, and the failure if one does not is
quiet: an agent bound to ["policyDocs", "claimsDb"] whose runtime only looked at the
first would answer from half its evidence and report success. So each plane is held to
the list here — the registry and context (what an agent sees), the evidence gatherer
(what reaches the model), the schema (what an editor accepts), and the BFF projection
(what the page shows). The two IaC validators are covered in cdk/test and by the HCL
expression in terraform/tools.tf.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import sys
import types

import pytest
from conftest import ORCH_ROOT, workflow

TOOLS = {"policyDocs": {"type": "kb", "corpora": ["policies", "claims"]},
         "claimsDb": {"type": "lambda", "lambdaArn": "arn:aws:lambda:us-east-1:123456789012:function:f",
                      "toolSchema": [{"name": "q", "properties": {"query": {"type": "string"}}}]},
         "docs": {"type": "mcp", "endpoint": "https://example.com/mcp"}}


def wf(tool) -> dict:
    return {"orchestrator": {}, "tools": TOOLS,
            "agents": {"triage": {"name": "Triage", "runtime": "dedicated", "maxTokens": 100,
                                  "tool": tool, "corpus": "claims"}},
            "steps": [{"agent": "triage"}]}


@pytest.mark.parametrize("tool, tools, first", [
    ("docs", ["docs"], "docs"),
    (["policyDocs", "claimsDb", "docs"], ["policyDocs", "claimsDb", "docs"], "policyDocs"),
    ([], [], None),
])
def test_the_registry_reads_one_tool_or_a_list(tool, tools, first):
    """`agent.tools` is always the list; `agent.tool` stays the first, for agent code
    written when there was only ever one (`ctx.tool or "pricing"`)."""
    with workflow(wf(tool)) as imp:
        registry = imp("app.orchestrator.registry")
        agent = registry._configure(imp("app.common.base").Agent(), "triage", wf(tool)["agents"]["triage"])
        assert agent.tools == tools
        assert agent.tool == first


class FakeCtx:
    """The slice of AgentContext the evidence gatherer uses."""

    def __init__(self, tools, corpus=None, answers=None):
        self.tools = tools
        self.tool = tools[0] if tools else None
        self.corpus = corpus
        self.calls: list[tuple] = []
        self.answers = answers or {}

    def _retrieval_query(self):
        return "the objective"

    async def retrieve(self, query, doc_type=None):
        self.calls.append(("retrieve", query, doc_type))
        return self.answers.get("kb", "kb text"), "gateway"

    async def call_tool(self, key, query):
        self.calls.append(("call_tool", key, query))
        return self.answers.get(key, f"{key} text"), "gateway"


def test_every_tool_is_queried_in_order_and_labelled_by_its_type():
    with workflow(wf(["policyDocs", "claimsDb", "docs"])) as imp:
        evidence = imp("app.subagents._shared.evidence")
        ctx = FakeCtx(["policyDocs", "claimsDb", "docs"], corpus="claims")
        text, gaps = asyncio.run(evidence.gather(ctx))
    assert ctx.calls == [("retrieve", "the objective", "claims"),
                         ("call_tool", "claimsDb", "the objective"),
                         ("call_tool", "docs", "the objective")]
    blocks = text.split("\n\n")
    assert blocks[0].startswith("=== KNOWLEDGE BASE: policyDocs (mode=gateway, corpus=claims) ===")
    assert blocks[1].startswith("=== TOOL FUNCTION: claimsDb (mode=gateway) ===")
    assert blocks[2].startswith("=== MCP SERVER: docs (mode=gateway) ===")
    assert gaps == []


def test_a_tool_that_answers_with_nothing_is_named_not_hidden():
    with workflow(wf(["policyDocs", "docs"])) as imp:
        evidence = imp("app.subagents._shared.evidence")
        ctx = FakeCtx(["policyDocs", "docs"], answers={"docs": ""})
        text, gaps = asyncio.run(evidence.gather(ctx, "a query"))
    assert "MCP SERVER" not in text
    assert gaps == ["'docs' returned no matching results for this query."]


def test_an_agent_with_no_tool_gathers_nothing():
    with workflow(wf([])) as imp:
        evidence = imp("app.subagents._shared.evidence")
        ctx = FakeCtx([])
        assert asyncio.run(evidence.gather(ctx)) == ("", [])
        assert ctx.calls == []


def test_the_policy_deny_in_knowledge_research_clears_every_tool():
    """It used to set `ctx.tool = None`. The gatherer reads `ctx.tools`, so clearing only
    the first would have left retrieval running after a deny."""
    path = ORCH_ROOT / "app" / "subagents" / "knowledge_research" / "agent.py"
    if not path.is_file():
        pytest.skip("the sample's knowledge_research agent was removed (scaffold.py reset)")
    source = path.read_text()
    assert "ctx.tools = []" in source


def test_the_schema_takes_a_key_or_a_list_of_keys():
    import jsonschema
    schema = json.loads((ORCH_ROOT / "app" / "workflow.schema.json").read_text())
    validator = jsonschema.Draft202012Validator(schema)
    for tool in ("docs", ["policyDocs", "docs"]):
        assert list(validator.iter_errors(wf(tool))) == [], tool
    assert list(validator.iter_errors(wf(["docs", 3]))), "a non-string entry was accepted"
    assert list(validator.iter_errors(wf({"a": 1}))), "an object was accepted"


@pytest.fixture()
def project():
    sys.modules.pop("workflow", None)
    yield importlib.import_module("workflow").project
    sys.modules.pop("workflow", None)


def test_the_page_shows_every_tool_an_agent_reads(project):
    out = project({**wf(["policyDocs", "claimsDb", "docs"]), "tools": TOOLS})
    assert out["agents"]["triage"]["source"] == (
        "Knowledge Base \u00b7 claims + Function \u00b7 claimsDb + MCP \u00b7 docs")
    assert out["agents"]["triage"]["tool"] == ["policyDocs", "claimsDb", "docs"]
    assert project(wf("docs"))["agents"]["triage"]["source"] == "MCP \u00b7 docs"


# ---------------------------------------------------------------------------
# GET /api/models — the model picker's list, read from the account
# ---------------------------------------------------------------------------

class FakeBedrock:
    def __init__(self, fail=False):
        self.fail = fail

    def list_foundation_models(self, byOutputModality):
        assert byOutputModality == "TEXT"
        if self.fail:
            raise RuntimeError("AccessDenied")
        return {"modelSummaries": [
            {"modelId": "anthropic.claude-x", "modelName": "Claude X", "providerName": "Anthropic",
             "inferenceTypesSupported": ["INFERENCE_PROFILE"], "inputModalities": ["TEXT", "IMAGE"]},
            {"modelId": "mistral.small", "modelName": "Small", "providerName": "Mistral AI",
             "inferenceTypesSupported": ["ON_DEMAND"]},
            {"modelId": "cohere.rerank-v3-5:0", "modelName": "Rerank", "providerName": "Cohere",
             "inferenceTypesSupported": ["ON_DEMAND"]},
            {"modelId": "old.model", "modelName": "Old", "providerName": "Old",
             "inferenceTypesSupported": ["ON_DEMAND"], "modelLifecycle": {"status": "LEGACY"}},
        ]}

    def list_inference_profiles(self, typeEquals, **kw):
        page = [
            {"inferenceProfileId": "us.anthropic.claude-x", "inferenceProfileName": "US Claude X",
             "models": [{"modelArn": "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-x"}]},
            # An embedding model's profile: its model is not in the TEXT list.
            {"inferenceProfileId": "us.cohere.embed-v4:0", "inferenceProfileName": "Embed",
             "models": [{"modelArn": "arn:aws:bedrock:us-east-1::foundation-model/cohere.embed-v4:0"}]},
        ]
        if "nextToken" not in kw:
            return {"inferenceProfileSummaries": page[:1], "nextToken": "p2"}
        return {"inferenceProfileSummaries": page[1:]}


@pytest.fixture()
def handler(monkeypatch):
    monkeypatch.setenv("WORKFLOW_JSON", json.dumps(wf("docs")))
    monkeypatch.setenv("STATUS_TABLE", "t")
    monkeypatch.setenv("EVENTS_TABLE", "t")
    monkeypatch.setenv("RUNTIME_ARN", "arn:aws:bedrock-agentcore:us-east-1:1:runtime/x")
    for mod in ("workflow", "authz", "chatbot", "handler"):
        sys.modules.pop(mod, None)
    module = importlib.import_module("handler")
    yield module
    for mod in ("workflow", "authz", "chatbot", "handler"):
        sys.modules.pop(mod, None)


def _get_models(handler):
    event = {"requestContext": {"http": {"method": "GET"}, "authorizer": {"jwt": {"claims": {}}}},
             "rawPath": "/api/models", "routeKey": "GET /api/models", "pathParameters": {}}
    resp = handler._api(event, types.SimpleNamespace(function_name="f"))
    return resp["statusCode"], json.loads(resp["body"])


def test_the_model_list_is_text_models_the_account_can_invoke(handler, monkeypatch):
    monkeypatch.setattr(handler.boto3, "client", lambda name, **kw: FakeBedrock())
    handler._models_cache.update(at=0.0, body=None)
    status, body = _get_models(handler)
    assert status == 200
    # The profile (the id workflow.json wants), then an on-demand model no profile
    # covers. Not: the embedding profile, the reranker, the legacy model.
    assert [m["id"] for m in body["models"]] == ["us.anthropic.claude-x", "mistral.small"]
    # Tagged with what an agent may ask of it: Bedrock lists Claude X as reading images.
    assert body["models"][0] == {"id": "us.anthropic.claude-x", "name": "US Claude X",
                                 "provider": "Anthropic", "vision": True, "tools": True}
    assert body["models"][1]["vision"] is False


def test_an_agent_whose_model_cannot_do_what_it_asks_is_a_problem(handler, monkeypatch):
    monkeypatch.setattr(handler, "_models", lambda: {"models": [
        {"id": "us.claude", "vision": True, "tools": True},
        {"id": "gpt-oss", "vision": False, "tools": True},
        {"id": "google.gemma-3-27b-it", "vision": True, "tools": False},
        {"id": "us.anthropic.claude-fable-5", "vision": True, "tools": True, "note": handler.OPT_IN_NOTE}]})
    flow = {"orchestrator": {"defaultModel": "gpt-oss"}, "agents": {
        "checker": {"vision": {"from": ["artist"]}},                          # default model: no images
        "pricer": {"model": "google.gemma-3-27b-it", "toolMode": "model"},   # no tool calls
        "fine": {"model": "us.claude", "vision": {"from": ["artist"]}, "toolMode": "model"},
        "typed": {"model": "my.custom-import", "toolMode": "model"},          # not listed: trusted
        "fable": {"model": "us.anthropic.claude-fable-5"}}}
    got = {(p["where"]["id"], p["severity"]) for p in handler.model_problems(flow)}
    assert got == {("checker", "error"), ("pricer", "error"), ("fable", "warning")}


def test_a_model_list_that_cannot_be_read_degrades_to_typing_an_id(handler, monkeypatch):
    monkeypatch.setattr(handler.boto3, "client", lambda name, **kw: FakeBedrock(fail=True))
    handler._models_cache.update(at=0.0, body=None)
    status, body = _get_models(handler)
    assert status == 200
    assert body["models"] == []
    assert "type a model id" in body["error"]


def test_the_models_route_is_declared_on_both_iac_paths():
    assert '"GET /api/models"' in (ORCH_ROOT / "terraform" / "bff.tf").read_text()
    assert 'path: "/api/models"' in (ORCH_ROOT / "cdk" / "lib" / "orchestrator-stack.ts").read_text()
