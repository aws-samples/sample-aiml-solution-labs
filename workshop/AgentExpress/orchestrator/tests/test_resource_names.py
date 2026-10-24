"""The deployed resource behind each run-graph component (bff/resource_names.py).

The names are computed, not read, so they are only right while they match what the two
IaC paths call each resource. The last test pins that: each formula's Terraform and CDK
source line must still be there, word for word.
"""
import importlib
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture()
def rn(monkeypatch):
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "AgentCoreBFF-demo")
    monkeypatch.setenv("SEMANTIC_MEMORY_ID", "demo_semantic-AbC123")
    monkeypatch.setenv("MEMORIES", json.dumps({"notes": "demo_m_notes-XyZ789"}))
    sys.modules.pop("resource_names", None)
    sys.modules.pop("workflow", None)
    yield importlib.import_module("resource_names")
    sys.modules.pop("resource_names", None)
    sys.modules.pop("workflow", None)


def _workflow() -> dict:
    return {
        "agents": {"a": {
            "name": "A", "tool": ["docs", "mine", "calc"], "skills": ["triage"], "askPerson": True,
            "agentcore": {"guardrails": {"input": True, "use": "strict"}, "memory": {"use": "notes"},
                          "evaluations": {"enabled": True, "evaluators": ["Builtin.Helpfulness", "Custom.tone", "Custom.shared"],
                                          "custom": [{"name": "tone", "instructions": "SECRET_INSTRUCTIONS"}]},
                          "identity": {"outbound": ["crm", "elsewhere"]}}}},
        "steps": [{"agent": "a"}],
        "tools": {"docs": {"type": "mcp", "endpoint": "https://internal.example/mcp", "policies": ["tmpl", "fixed"]},
                  "mine": {"type": "openapi", "auth": "user"},
                  "calc": {"type": "code"}},
        "policies": {"tmpl": {"statement": "forbid(...) when { {{tool}} SECRET_RULE };"},
                     "fixed": {"statement": "permit(...);"}},
        "guardrails": {"strict": {"deniedWords": ["PROJECT_BLUEBIRD"]}},
        "memories": {"notes": {"strategies": ["semantic"]}},
        "evaluators": {"shared": {"instructions": "x"}},
        "identities": {"crm": {"type": "apikey", "apiKey": "hunter2"}},
    }


def _by_name(view: dict) -> dict:
    return {(c["kind"], c["name"]): {r["label"]: r["value"] for r in c["resources"]}
            for c in view["agents"]["a"]["components"]}


def test_each_component_names_what_it_is_deployed_as(rn):
    wf = importlib.import_module("workflow")
    got = _by_name(wf.project(_workflow()))
    assert got[("tool", "docs")] == {"Gateway target": "docs", "Gateway": "demo-gw"}
    # A tool that acts as the person is on the person Gateway.
    assert got[("tool", "mine")] == {"Gateway target": "mine", "Gateway": "demo-gwu"}
    # Not a Gateway target: nothing to name.
    assert got[("tool", "calc")] == {}
    assert got[("guardrail", "strict")] == {"Bedrock Guardrail": "demo-gr-strict", "Version": "DRAFT"}
    assert got[("memory", "notes")] == {"AgentCore Memory": "demo_m_notes", "Memory ID": "demo_m_notes-XyZ789"}
    # A {{tool}} template is deployed once per tool; any other once, as named.
    assert got[("policy", "tmpl")] == {"Policy engine": "demo_policy", "Cedar policy": "custom_tmplb9460840_89e495e7"}
    assert got[("policy", "fixed")] == {"Policy engine": "demo_policy", "Cedar policy": "custom_fixed_89e495e7"}
    assert got[("evaluation", "Helpfulness")] == {"Evaluator ID": "Builtin.Helpfulness"}
    assert got[("evaluation", "tone")] == {"Evaluator": "demo_ade228_tone"}
    assert got[("evaluation", "shared")] == {"Evaluator": "demo_15ca19_shared"}
    assert got[("identity", "crm")] == {"Credential provider": "bedrock-agentcore-demo-id-crm"}
    # Not one this deploy creates: said, not guessed.
    assert got[("identity", "elsewhere")] == {"Credential provider": "not available"}
    assert got[("skill", "triage")] == {} and got[("human", "Asks questions")] == {}


def test_names_and_ids_only_never_a_secret(rn):
    wf = importlib.import_module("workflow")
    shipped = json.dumps(wf.project(_workflow())["agents"]["a"]["components"])
    for secret in ("internal.example", "SECRET_RULE", "SECRET_INSTRUCTIONS", "PROJECT_BLUEBIRD", "hunter2", "apiKey"):
        assert secret not in shipped


def test_the_default_guardrail_semantic_memory_and_an_existing_guardrail(rn):
    wf = {"agents": {"a": {"agentcore": {"guardrails": {"output": True}, "memory": {"longTerm": True}}},
                     "b": {"agentcore": {"guardrails": {"input": True, "guardrailId": "gr-abc123"}}}},
          "steps": [{"agent": "a"}, {"agent": "b"}], "guardrail": {"contentFilters": {"HATE": "HIGH"}}}
    def res(agent, kind, workflow=wf):
        return rn.resources(workflow, agent, {"kind": kind}, "demo", rn.own_memory_ids(), {})
    assert res("a", "guardrail") == [{"label": "Bedrock Guardrail", "value": "demo-guardrail"}, {"label": "Version", "value": "DRAFT"}]
    assert res("b", "guardrail") == [{"label": "Bedrock Guardrail ID", "value": "gr-abc123"}, {"label": "Version", "value": "DRAFT"}]
    assert res("a", "memory") == [{"label": "AgentCore Memory", "value": "demo_semantic"},
                                  {"label": "Memory ID", "value": "demo_semantic-AbC123"}]
    # A guardrail block with no policy deploys no guardrail (Bedrock refuses one): the check is a no-op.
    bare = {**wf, "guardrail": {"blockedInputMessage": "no"}}
    assert "none deployed" in res("a", "guardrail", bare)[0]["value"]


def test_policy_off_and_no_agent_name_say_so(rn):
    wf = _workflow()
    comp = {"kind": "policy", "name": "fixed", "tool": "docs"}
    off = {**wf, "orchestrator": {"policy": {"enabled": False}}}
    assert rn.resources(off, "a", comp, "demo", {}, {})[0]["value"] == "not deployed: orchestrator.policy is off"
    # The default (app/defaults.json) is on.
    assert rn.resources(wf, "a", comp, "demo", {}, {"orchestrator": {"policy": {"enabled": True}}})[1]["value"] \
        == "custom_fixed_89e495e7"
    # Outside Lambda there is no agent name: every name that needs it is "not available".
    assert rn.resources(wf, "a", {"kind": "guardrail"}, "", {}, {})[0] == {"label": "Bedrock Guardrail", "value": "not available"}
    assert rn.resources(wf, "a", {"kind": "tool", "tool": "docs", "name": "docs"}, "", {}, {}) == [
        {"label": "Gateway target", "value": "docs"}, {"label": "Gateway", "value": "not available"}]


def test_the_agent_name_is_the_bff_function_name_less_its_prefix(rn, monkeypatch):
    assert rn.own_agent_name() == "demo"
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "SomethingElse")
    assert rn.own_agent_name() == ""
    monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME")
    assert rn.own_agent_name() == ""


def test_a_build_deployed_as_its_own_stack_uses_its_agent_name_and_no_memory_ids(rn):
    """builds.target projects a build's workflow with the build's agentName; this BFF's own
    memory ids belong to this deployment, not the build's."""
    wf = importlib.import_module("workflow")
    got = _by_name(wf.project(_workflow(), "bld_x"))
    assert got[("guardrail", "strict")]["Bedrock Guardrail"] == "bld_x-gr-strict"
    assert got[("tool", "docs")]["Gateway"] == "bld-x-gw"
    assert got[("memory", "notes")] == {"AgentCore Memory": "bld_x_m_notes", "Memory ID": "not available"}


@pytest.mark.parametrize("path,needle", [
    ("terraform/bff.tf", 'function_name    = "AgentCoreBFF-${var.agent_name}"'),
    ("cdk/lib/orchestrator-stack.ts", "const bffFunctionName = `AgentCoreBFF-${agentName}`;"),
    ("terraform/gateway.tf", 'name            = "${replace(var.agent_name, "_", "-")}-gw"'),
    ("terraform/gateway.tf", 'name            = "${replace(var.agent_name, "_", "-")}-gwu"'),
    ("cdk/lib/tool-plane.ts", "name: `${dashName}-gw`,"),
    ("cdk/lib/tool-plane.ts", "name: `${dashName}-gwu`,"),
    ("cdk/lib/tool-plane.ts", "name, // MUST equal the agent's `tool` label in workflow.json"),
    ("terraform/tools.tf", "name        = each.key"),
    ("terraform/guardrail.tf", 'name                      = "${var.agent_name}-guardrail"'),
    ("terraform/guardrail.tf", 'name                      = "${var.agent_name}-gr-${each.key}"'),
    ("cdk/lib/orchestrator-stack.ts", "Name: `${agentName}-guardrail`,"),
    ("cdk/lib/orchestrator-stack.ts", "Name: `${agentName}-gr-${n}`,"),
    ("terraform/main.tf", 'GUARDRAIL_VERSION = "DRAFT"'),
    ("cdk/lib/orchestrator-stack.ts", 'GUARDRAIL_VERSION: "DRAFT",'),
    ("terraform/evaluations.tf", 'name                  = "${var.agent_name}_m_${each.key}"'),
    ("terraform/main.tf", 'name                  = "${var.agent_name}_semantic"'),
    ("cdk/lib/orchestrator-stack.ts", "Name: `${agentName}_m_${n}`,"),
    ("cdk/lib/orchestrator-stack.ts", "Name: `${agentName}_semantic`,"),
    ("terraform/policy.tf", 'name        = "${var.agent_name}_policy"'),
    ("cdk/lib/tool-plane.ts", "name: `${agentName}_policy`,"),
    ("terraform/policy.tf", 'cedar_policy_suffix = substr(sha1(var.agent_name), 0, 8)'),
    ("terraform/policy.tf", 'name             = "custom_${each.key}_${local.cedar_policy_suffix}"'),
    ("terraform/policy.tf", 'name        = "${substr(n, 0, 20)}${substr(sha1("${n}.${k}"), 0, 8)}"'),
    ("cdk/lib/tool-plane.ts", 'return `custom_${name}_${crypto.createHash("sha1").update(agentName).digest("hex").slice(0, 8)}`;'),
    ("cdk/lib/orchestrator-stack.ts", 'return `${policy.slice(0, 20)}${crypto.createHash("sha1").update(`${policy}.${tool}`).digest("hex").slice(0, 8)}`;'),
    ("terraform/evaluations.tf", 'name         = substr("${var.agent_name}_${substr(sha256("${id}.${c.name}"), 0, 6)}_${c.name}", 0, 48)'),
    ("terraform/evaluations.tf", 'evaluator_name = substr("${var.agent_name}_${substr(sha256("*.${each.key}"), 0, 6)}_${each.key}", 0, 48)'),
    ("cdk/lib/orchestrator-stack.ts", "evaluatorName: `${agentName}_${digest}_${c.name}`.slice(0, 48),"),
    ("cdk/lib/orchestrator-stack.ts", "evaluatorName: `${agentName}_${digest}_${n}`.slice(0, 48),"),
    ("terraform/identities.tf", 'n => "bedrock-agentcore-${replace(var.agent_name, "_", "-")}-id-${n}" }'),
    ("cdk/lib/orchestrator-stack.ts", "const provider = `bedrock-agentcore-${dash}-id-${n}`;"),
])
def test_each_formula_still_matches_both_iac_paths(path, needle):
    assert needle in (ROOT / path).read_text(), f"{path} no longer names the resource as bff/resource_names.py does"
