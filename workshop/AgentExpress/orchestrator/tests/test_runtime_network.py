"""An AgentCore Runtime's network on the Terraform path (terraform/runtime_network.tf).

`orchestrator.network` places the shared orchestrator Runtime and `agents.<id>.network` a
dedicated agent's own Runtime. Absent, every Runtime stays network_mode "PUBLIC" with no
network_mode_config, so the plan is what it was before the setting. Mode "VPC" sends
network_mode_config {subnets, security_groups}; what the API refuses
(https://docs.aws.amazon.com/bedrock-agentcore-control/latest/APIReference/API_VpcConfig.html)
fails the plan with the place named.

Evaluated offline with `terraform console` on a copy whose workflow.json is edited (a local
backend, the providers already downloaded): no AWS call. cdk/test/runtime-network.test.ts
holds the CDK path to the same cases.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from conftest import ORCH_ROOT

TF = ORCH_ROOT / "terraform"
TERRAFORM = shutil.which("terraform")
PROVIDERS = next((p for p in (TF / ".terraform" / "providers",
                              Path(os.environ.get("TF_DATA_DIR", "/nonexistent")) / "providers") if p.is_dir()), None)
SUBNETS = ["subnet-0123456789abcdef0", "subnet-0fedcba9876543210"]
GROUPS = ["sg-0123456789abcdef0"]

pytestmark = pytest.mark.skipif(not TERRAFORM or PROVIDERS is None,
                                reason="terraform and its downloaded providers are needed offline")


def _locals(tmp_path: Path, edit, expr: str | None = None) -> dict:
    root = tmp_path / "orchestrator"
    root.mkdir()
    for entry in ORCH_ROOT.iterdir():
        if entry.name not in ("app", "terraform"):
            (root / entry.name).symlink_to(entry)
    shutil.copytree(ORCH_ROOT / "app", root / "app")
    wf_path = root / "app" / "workflow.json"
    wf = json.loads(wf_path.read_text())
    edit(wf)
    wf_path.write_text(json.dumps(wf))
    tf = root / "terraform"
    tf.mkdir()
    for entry in TF.iterdir():
        if entry.name not in (".terraform", ".terraform.lock.hcl"):
            (tf / entry.name).symlink_to(entry)
    shutil.copy(TF / ".terraform.lock.hcl", tf / ".terraform.lock.hcl")
    (tf / "backend_override.tf").write_text('terraform {\n  backend "local" {}\n}\n')
    env = {k: v for k, v in os.environ.items() if k != "TF_DATA_DIR"}
    run = lambda *args, **kw: subprocess.run(  # noqa: E731,S603 - fixed argv, no shell  # nosec B603  # nosemgrep
        [TERRAFORM, *args], cwd=tf, env=env, capture_output=True, text=True, timeout=600, check=False, **kw)
    init = run("init", "-input=false", "-lockfile=readonly", f"-plugin-dir={PROVIDERS}", "-no-color")
    assert init.returncode == 0, init.stdout[-3000:] + init.stderr[-3000:]
    expr = expr or ("jsonencode({cfg = local.runtime_network_config, problems = local.runtime_network_problems, "
                    "misplaced = local.misplaced_networks})")
    out = run("console", "-var", "region=us-east-1", "-no-color", input=expr + "\n")
    assert out.returncode == 0, out.stdout[-3000:] + out.stderr[-3000:]
    return json.loads(json.loads(out.stdout.strip().splitlines()[-1]))


def test_no_network_setting_keeps_every_runtime_public(tmp_path):
    got = _locals(tmp_path, lambda wf: None)
    assert set(got["cfg"]) == {"orchestrator.network", "agents.knowledge_research.network",
                               "agents.web_search.network", "agents.documentation_search.network"}
    assert all(c == {"network_mode": "PUBLIC", "network_mode_config": None} for c in got["cfg"].values())
    assert all(p == [] for p in got["problems"].values()) and got["misplaced"] == []


def test_vpc_settings_reach_each_runtime(tmp_path):
    def edit(wf):
        wf["orchestrator"]["network"] = {"mode": "VPC", "subnetIds": SUBNETS, "securityGroupIds": GROUPS}
        wf["agents"]["knowledge_research"]["network"] = {"mode": "VPC", "subnetIds": SUBNETS[:1], "securityGroupIds": GROUPS}
        wf["agents"]["web_search"]["network"] = {"mode": "PUBLIC"}
    got = _locals(tmp_path, edit)
    assert got["cfg"]["orchestrator.network"] == {
        "network_mode": "VPC", "network_mode_config": {"subnets": SUBNETS, "security_groups": GROUPS}}
    assert got["cfg"]["agents.knowledge_research.network"] == {
        "network_mode": "VPC", "network_mode_config": {"subnets": SUBNETS[:1], "security_groups": GROUPS}}
    # Not inherited from orchestrator.network: each dedicated Runtime is its own setting.
    for aid in ("web_search", "documentation_search"):
        assert got["cfg"][f"agents.{aid}.network"] == {"network_mode": "PUBLIC", "network_mode_config": None}
    assert all(p == [] for p in got["problems"].values()) and got["misplaced"] == []


def test_what_the_api_refuses_is_named_before_apply(tmp_path):
    def edit(wf):
        wf["orchestrator"]["network"] = {"mode": "VPC"}
        wf["agents"]["knowledge_research"]["network"] = {"mode": "public"}
        wf["agents"]["web_search"]["network"] = {"mode": "PUBLIC", "subnetIds": SUBNETS}
        wf["agents"]["documentation_search"]["network"] = {"mode": "VPC", "subnetIds": ["subnet-xyz"], "securityGroupIds": GROUPS}
        wf["agents"]["intake"]["network"] = {"mode": "VPC", "subnetIds": SUBNETS, "securityGroupIds": GROUPS}
    got = _locals(tmp_path, edit)
    p = got["problems"]
    assert p["orchestrator.network"] == [
        'orchestrator.network.securityGroupIds: mode "VPC" needs 1-16 ids matching ^sg-[0-9a-zA-Z]{8,17}$',
        'orchestrator.network.subnetIds: mode "VPC" needs 1-16 ids matching ^subnet-[0-9a-zA-Z]{8,17}$']
    assert p["agents.knowledge_research.network"] == [
        'agents.knowledge_research.network.mode must be one of PUBLIC, VPC (got "public")']
    assert p["agents.web_search.network"] == [
        "agents.web_search.network: subnetIds and securityGroupIds apply to mode \"VPC\" only"]
    assert p["agents.documentation_search.network"] == [
        'agents.documentation_search.network.subnetIds: mode "VPC" needs 1-16 ids matching ^subnet-[0-9a-zA-Z]{8,17}$']
    assert got["misplaced"] == ["intake"]


def test_dedicated_agents_with_different_keys_still_plan(tmp_path):
    """A VPC agent with a tool beside a PUBLIC one gives local.dedicated_agents an object type, so
    `cond ? local.dedicated_agents : {}` fails the plan ("Inconsistent conditional result types",
    seen live on a Terraform deploy). The per-agent LiteLLM and OTLP-export policies use a filtered
    for instead; evaluated here with both features off and on."""
    def edit(wf):
        wf["agents"]["knowledge_research"]["network"] = {"mode": "VPC", "subnetIds": SUBNETS, "securityGroupIds": GROUPS}
        wf["agents"]["web_search"]["network"] = {"mode": "PUBLIC"}
    forms = {f.name: f.read_text() for f in (TF / "litellm.tf", TF / "otlp_export.tf")}
    assert "? local.dedicated_agents : {}" not in "".join(forms.values())
    assert "for_each = { for id, a in local.dedicated_agents : id => id if local.litellm_enabled }" in forms["litellm.tf"]
    assert "for_each = { for id, a in local.dedicated_agents : id => id if local.otlp_export_enabled }" in forms["otlp_export.tf"]
    got = _locals(tmp_path, edit, "jsonencode({off = { for id, a in local.dedicated_agents : id => id if false }, "
                                  "on = { for id, a in local.dedicated_agents : id => id if true }})")
    assert got == {"off": {}, "on": {a: a for a in ("documentation_search", "knowledge_research", "web_search")}}


def test_both_runtimes_read_it_and_fail_the_plan_on_a_problem():
    main = (TF / "main.tf").read_text()
    sub = (TF / "subagent_runtimes.tf").read_text()
    assert 'network_configuration = local.runtime_network_config["orchestrator.network"]' in main
    assert 'length(local.runtime_network_problems["orchestrator.network"]) == 0' in main
    assert "length(local.misplaced_networks) == 0" in main
    assert 'network_configuration = local.runtime_network_config["agents.${each.key}.network"]' in sub
    assert 'length(local.runtime_network_problems["agents.${each.key}.network"]) == 0' in sub
    assert 'network_mode = "PUBLIC"' not in main + sub


def test_the_deployer_may_create_the_network_service_linked_role():
    """AgentCore makes the VPC's network interfaces with AWSServiceRoleForBedrockAgentCoreNetwork
    (network.bedrock-agentcore.amazonaws.com), created on the first VPC Runtime: the deployer
    needs iam:CreateServiceLinkedRole for it, and nothing else is documented.
    https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/agentcore-vpc.html"""
    import fnmatch
    doc = json.loads((TF / "deploy-role-policy.json").read_text())
    st = next(s for s in doc["Statement"] if s.get("Sid") == "IamServiceLinkedRoles")
    assert "iam:CreateServiceLinkedRole" in st["Action"]
    names = st["Condition"]["StringLike"]["iam:AWSServiceName"]
    assert any(fnmatch.fnmatch("network.bedrock-agentcore.amazonaws.com", n) for n in names)
