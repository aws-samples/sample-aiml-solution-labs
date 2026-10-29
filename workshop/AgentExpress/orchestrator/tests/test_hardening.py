"""Organisation guardrails (terraform/hardening.tf, cdk/lib/hardening.ts): a permissions
boundary and a path on every IAM role, a custom domain with TLS 1.2, and AWS WAF. All off by
default, and unset they must change nothing.

The Terraform half is planned for real, offline, by `terraform test` with every provider
mocked (terraform/tests/hardening.tftest.hcl); this file runs it, enumerates every role the
.tf files declare, holds the two IaC paths to the same values, and covers what the deploy
runner passes on to each build. The CDK half is cdk/test/hardening.test.ts.
"""
from __future__ import annotations

import re
import shutil
import subprocess
import sys

import pytest
from conftest import ORCH_ROOT

TF = ORCH_ROOT / "terraform"
TS = (ORCH_ROOT / "cdk" / "lib" / "hardening.ts").read_text()
HCL = (TF / "hardening.tf").read_text()
VARS = (TF / "variables.tf").read_text()


def _blocks(kind: str):
    """(file, name, body) of every `resource "<kind>" "<name>" { ... }` in terraform/*.tf."""
    out = []
    for f in sorted(TF.glob("*.tf")):
        src = f.read_text()
        for m in re.finditer(rf'^resource "{kind}" "(\w+)" \{{\n', src, re.MULTILINE):
            depth, i = 1, m.end()
            while depth:
                depth += {"{": 1, "}": -1}.get(src[i], 0)
                i += 1
            out.append((f.name, m.group(1), src[m.end():i - 1]))
    return out


def test_every_terraform_role_carries_the_boundary_and_the_path():
    """Every aws_iam_role, enumerated from the files: a role added later without the two
    lines fails here by name."""
    roles = _blocks("aws_iam_role")
    assert len(roles) >= 13, roles
    for f, name, body in roles:
        assert re.search(r"^  path\s+= var\.iam_role_path$", body, re.MULTILINE), f"{f} aws_iam_role.{name} has no path"
        if name == "tool_code":
            # Its own boundary unless the organisation's is set (one boundary per role).
            own = "coalesce(local.iam_permissions_boundary, aws_iam_policy.tool_code_boundary"
            assert f"permissions_boundary = {own}" in body
        else:
            assert re.search(r"^  permissions_boundary = local\.iam_permissions_boundary$", body, re.MULTILINE), \
                f"{f} aws_iam_role.{name} has no permissions boundary"


def test_no_role_arn_is_built_as_if_every_role_were_at_slash():
    """With a path, a role's ARN is role/<path><name>: one written out as role/<name> for a
    role this stack creates would name nothing. Only roles made elsewhere may be written out
    (the CDK bootstrap roles, a connected account's role)."""
    for f in sorted(TF.glob("*.tf")):
        for line in f.read_text().splitlines():
            for arn in re.findall(r"arn:aws:iam::[^\"]*:role/[^\"]*", line):
                assert "cdk-hnb659fds-" in arn or "connect_role_prefix" in arn, f"{f.name}: {arn}"


TERRAFORM = shutil.which("terraform")


@pytest.mark.skipif(not TERRAFORM or not (TF / ".terraform").exists(),
                    reason="terraform, initialised, is needed for the offline plan")
def test_terraform_plans_the_guardrails_offline():
    """terraform/tests/hardening.tftest.hcl: unset changes nothing, set reaches every role,
    and each variable's validation refuses a bad value. Mocked providers: no AWS call."""
    r = subprocess.run(  # noqa: S603 - fixed argv, no shell  # nosec B603  # nosemgrep
        [TERRAFORM, "test", "-no-color"], cwd=TF, capture_output=True, text=True, timeout=600, check=False)
    assert r.returncode == 0, r.stdout[-4000:] + r.stderr[-2000:]
    assert re.search(r"Success! (\d+) passed, 0 failed", r.stdout), r.stdout[-2000:]


# --- the two paths agree ---------------------------------------------------------------------

def _ts_list(name: str) -> list[str]:
    body = re.search(rf"export const {name} = \[(.*?)\];", TS, re.DOTALL).group(1)
    return re.findall(r'"([^"]+)"', body)


def test_both_paths_use_the_same_managed_rule_groups_tls_and_rate_limit():
    tf_groups = re.findall(r'"([^"]+)"', re.search(r"waf_managed_rule_groups = \[(.*?)\]", HCL, re.DOTALL).group(1))
    assert tf_groups == _ts_list("WAF_MANAGED_RULE_GROUPS") == [
        "AWSManagedRulesAmazonIpReputationList", "AWSManagedRulesCommonRuleSet", "AWSManagedRulesKnownBadInputsRuleSet"]
    assert '["SizeRestrictions_BODY"]' in HCL and 'AWSManagedRulesCommonRuleSet: ["SizeRestrictions_BODY"]' in TS
    assert 'min_viewer_tls = "TLSv1.2_2021"' in HCL and "TLS_V1_2_2021" in TS
    tf_default = re.search(r'variable "waf_rate_limit" \{.*?default\s+= (\d+)', VARS, re.DOTALL).group(1)
    assert f"DEFAULT_WAF_RATE_LIMIT = {tf_default};" in TS
    assert 'scope       = "CLOUDFRONT"' in HCL and 'scope: "CLOUDFRONT"' in TS
    assert 'aggregate_key_type = "IP"' in HCL and 'aggregateKeyType: "IP"' in TS


def test_both_paths_require_the_boundary_of_the_deploy_role_the_same_way():
    tf = {m.group(1): set(re.findall(r'"(iam:\w+)"', m.group(2)))
          for m in re.finditer(r'Sid\s+= "(\w+)"\s+Effect\s+= "Deny"\s+Action\s+= \[([^\]]*)\]', HCL)}
    ts = {m.group(1): set(re.findall(r'"(iam:\w+)"', m.group(2)))
          for m in re.finditer(r'sid: "(\w+)", effect: iam\.Effect\.DENY,\s+actions: \[([^\]]*)\]', TS)}
    assert tf == ts and set(tf) == {"RolesOnlyWithTheBoundary", "KeepTheBoundary", "LeaveTheBoundaryPolicyAlone"}
    assert {"iam:CreateRole", "iam:PutRolePolicy", "iam:AttachRolePolicy", "iam:PutRolePermissionsBoundary"} \
        == tf["RolesOnlyWithTheBoundary"]
    assert '"iam:PermissionsBoundary" = local.iam_permissions_boundary' in HCL
    assert '"iam:PermissionsBoundary": boundaryArn' in TS
    for env in ("PERMISSIONS_BOUNDARY_ARN", "IAM_ROLE_PATH"):
        assert env in HCL and env in TS


def _hcl_regex(var: str) -> str:
    block = re.search(rf'variable "{var}" \{{.*?\n\}}', VARS, re.DOTALL).group(0)
    return re.search(r'regex\("((?:[^"\\]|\\.)*)"', block).group(1).replace("\\\\", "\\")


def _ts_regex(const: str) -> str:
    return re.search(rf"const {const} = /(.*)/;", TS).group(1).replace("\\/", "/")


@pytest.mark.parametrize("var,const", [("iam_permissions_boundary_arn", "BOUNDARY_ARN"),
                                       ("iam_role_path", "ROLE_PATH"),
                                       ("custom_domain", "HOST_NAME"),
                                       ("acm_certificate_arn", "CERT_ARN")])
def test_both_paths_accept_the_same_values(var, const):
    assert _hcl_regex(var) == _ts_regex(const)


def test_the_runner_checks_the_same_shapes():
    sys.path.insert(0, str(ORCH_ROOT / "deployer"))
    import runner
    assert runner.BOUNDARY_ARN.pattern == _hcl_regex("iam_permissions_boundary_arn")
    assert runner.ROLE_PATH.pattern == _hcl_regex("iam_role_path")


def test_the_new_settings_stay_out_of_the_example_tfvars():
    example = (TF / "terraform.tfvars.example").read_text()
    for var in ("iam_permissions_boundary_arn", "iam_role_path", "custom_domain", "acm_certificate_arn",
                "enable_waf", "waf_rate_limit"):
        assert not re.search(rf"^\s*{var}\s*=", example, re.MULTILINE), var


# --- what the deploy runner passes on to a build --------------------------------------------

@pytest.fixture
def runner():
    sys.path.insert(0, str(ORCH_ROOT / "deployer"))
    import runner as r
    return r


BOUNDARY = "arn:aws:iam::111122223333:policy/OrgBoundary"


def test_unset_the_runner_passes_nothing_new(runner):
    assert runner.guardrails({}) == {}
    assert runner.guardrails({"IAM_ROLE_PATH": "/"}) == {}
    assert runner.cdk_context("ax_12345678", True, None, None, {}) == runner.cdk_context("ax_12345678", True)
    assert runner.tf_vars("ax_12345678", True, "us-east-1", None, None, {}) \
        == runner.tf_vars("ax_12345678", True, "us-east-1")


def test_the_console_boundary_and_path_reach_a_build_on_both_paths(runner):
    guard = runner.guardrails({"PERMISSIONS_BOUNDARY_ARN": BOUNDARY, "IAM_ROLE_PATH": "/agentexpress/"})
    assert guard == {"permissionsBoundaryArn": BOUNDARY, "iamRolePath": "/agentexpress/"}
    flags = runner.cdk_context("ax_12345678", True, None, None, guard)
    assert f"permissionsBoundaryArn={BOUNDARY}" in flags and "iamRolePath=/agentexpress/" in flags
    assert flags[flags.index(f"permissionsBoundaryArn={BOUNDARY}") - 1] == "-c"
    tf = runner.tf_vars("ax_12345678", True, "us-east-1", None, None, guard)
    assert tf["iam_permissions_boundary_arn"] == BOUNDARY and tf["iam_role_path"] == "/agentexpress/"
    # The names the build's own stack reads.
    assert 'variable "iam_permissions_boundary_arn"' in VARS and 'variable "iam_role_path"' in VARS
    bin_ts = (ORCH_ROOT / "cdk" / "bin" / "orchestrator.ts").read_text()
    assert 'ctx("permissionsBoundaryArn"' in bin_ts and 'ctx("iamRolePath"' in bin_ts


def test_in_a_connected_account_the_boundary_is_that_accounts_policy(runner):
    """A boundary must be a policy in the role's own account."""
    target = runner.Target("444455556666", "eu-west-1", "arn:aws:iam::444455556666:role/x", "ext", "b1")
    assert runner.guardrails({"PERMISSIONS_BOUNDARY_ARN": BOUNDARY}, target) == {
        "permissionsBoundaryArn": "arn:aws:iam::444455556666:policy/OrgBoundary"}
    managed = "arn:aws:iam::aws:policy/PowerUserAccess"
    assert runner.guardrails({"PERMISSIONS_BOUNDARY_ARN": managed}, target) == {"permissionsBoundaryArn": managed}


@pytest.mark.parametrize("env", [{"PERMISSIONS_BOUNDARY_ARN": "arn:aws:iam::1:role/x"},
                                 {"PERMISSIONS_BOUNDARY_ARN": 'arn:aws:iam::111122223333:policy/a" -c x=y'},
                                 {"IAM_ROLE_PATH": "/a*/"}, {"IAM_ROLE_PATH": "nope"}])
def test_the_runner_refuses_a_malformed_guardrail(runner, env):
    with pytest.raises(runner.StepFailed):
        runner.guardrails(env)


def test_the_runner_hands_the_guardrails_to_the_deploy():
    """main() computes them for the build's target and passes them to the deploy step. Read
    from the source: main() needs a builds table and bucket, which tests/test_builds.py fakes
    for the deploy flow itself."""
    src = (ORCH_ROOT / "deployer" / "runner.py").read_text()
    assert "guard = guardrails(env, target)" in src
    assert "deploy_cdk(action, agent, gateway, sub_env, tags, target, signin, guard,\n" in src
    assert "deploy_terraform(action, agent, gateway, env, sub_env, tags, target, signin, guard,\n" in src
    assert "cdk_context(agent, gateway, tags, signin, guard)" in src
    assert "tf_vars(agent, gateway, region, tags, signin, guard, bucket_naming)" in src
