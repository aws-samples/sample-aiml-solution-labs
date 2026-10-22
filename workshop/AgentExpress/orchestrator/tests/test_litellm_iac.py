"""The LiteLLM settings on both IaC paths (terraform/litellm.tf, cdk/lib/litellm.ts): off by
default and adding nothing; on, the same two variables on every runtime and the BFF, and a
grant on that one secret for each of their roles. Also the route, and what a build deployed
from the Builder is given (deployer/runner.py). Text checks; cdk/test/litellm.test.ts
synthesizes the CDK stack."""
from __future__ import annotations

import re

from conftest import ORCH_ROOT as ORCH

ARN = "arn:aws:secretsmanager:eu-west-1:210987654321:secret:litellm-key-AbC123"


def _tf_regex(tf: str, var: str) -> str:
    block = tf.split(f'variable "{var}"')[1].split("\nvariable ")[0]
    m = re.search(r'can\(regex\("(.*)", var\.' + var + r"\)\)", block)
    assert m, var
    return m.group(1).replace("\\\\", "\\")


def _ts_regex(text: str, name: str) -> str:
    m = re.search(rf"export const {name} = /(.*)/;", text)
    assert m, name
    return m.group(1).replace("\\/", "/")


def test_the_four_validators_use_the_same_patterns():
    import litellm_models

    from app.common import litellm_proxy
    tf = (ORCH / "terraform" / "variables.tf").read_text()
    ts = (ORCH / "cdk" / "lib" / "litellm.ts").read_text()
    assert (_tf_regex(tf, "litellm_base_url") == litellm_proxy.BASE_URL_PATTERN
            == litellm_models.BASE_URL_PATTERN == _ts_regex(ts, "LITELLM_BASE_URL_PATTERN"))
    assert (_tf_regex(tf, "litellm_key_secret_arn") == litellm_proxy.SECRET_ARN_PATTERN
            == litellm_models.SECRET_ARN_PATTERN == _ts_regex(ts, "LITELLM_SECRET_ARN_PATTERN"))


def test_both_paths_set_the_same_environment_and_only_when_on():
    from app.common import litellm_proxy
    tf = (ORCH / "terraform" / "litellm.tf").read_text()
    ts = (ORCH / "cdk" / "lib" / "litellm.ts").read_text()
    for name in (litellm_proxy.BASE_URL_ENV, litellm_proxy.KEY_SECRET_ENV):
        assert re.search(rf"\b{name}\b", tf) and re.search(rf"\b{name}\b", ts), name
    assert re.search(r"litellm_env\s*=\s*local\.litellm_enabled \? \{", tf)
    main = (ORCH / "terraform" / "main.tf").read_text()
    sub = (ORCH / "terraform" / "subagent_runtimes.tf").read_text()
    bff = (ORCH / "terraform" / "bff.tf").read_text()
    for text in (main, sub):
        assert len(re.findall(r"\}, local\.otlp_export_env, local\.litellm_env[,)]", text)) == 1
    assert bff.count(", local.litellm_env)") == 1
    stack = (ORCH / "cdk" / "lib" / "orchestrator-stack.ts").read_text()
    assert "...litellm.env," in stack and "bff.addEnvironment(k, v)" in stack


def test_terraform_grants_the_secret_only_when_on_and_only_on_that_arn():
    tf = (ORCH / "terraform" / "litellm.tf").read_text()
    blocks = {name: tf.split(f'resource "aws_iam_role_policy" "{name}"')[1].split("\n}\n")[0]
              for name in ("runtime_litellm", "subagent_litellm", "bff_litellm")}
    assert "count  = local.workflow_plane && local.litellm_enabled ? 1 : 0" in blocks["runtime_litellm"]
    assert "for_each = { for id, a in local.dedicated_agents : id => id if local.litellm_enabled }" in blocks["subagent_litellm"]
    assert "count  = local.litellm_enabled ? 1 : 0" in blocks["bff_litellm"]
    assert "role   = aws_iam_role.bff.id" in blocks["bff_litellm"]
    policy = tf.split("litellm_policy = ")[1].split("\n  })\n")[0]
    assert 'Action   = ["secretsmanager:GetSecretValue"]' in policy
    assert "Resource = var.litellm_key_secret_arn" in policy
    assert '"kms:EncryptionContext:SecretARN" = var.litellm_key_secret_arn' in policy
    assert '"*"' not in policy
    main = (ORCH / "terraform" / "main.tf").read_text()
    subagents = (ORCH / "terraform" / "subagent_runtimes.tf").read_text()
    wait = main.split('resource "time_sleep" "iam_propagation"')[1].split("\n}\n")[0]
    sub_wait = subagents.split('resource "time_sleep" "subagent_iam_propagation"')[1].split("\n}\n")[0]
    assert re.search(r"depends_on\s*=\s*\[[^\]]*\baws_iam_role_policy\.runtime_litellm\b", wait)
    assert re.search(r"depends_on\s*=\s*\[[^\]]*\baws_iam_role_policy\.subagent_litellm\b", sub_wait)


def test_the_variables_are_off_by_default_and_not_in_the_example():
    tf = (ORCH / "terraform" / "variables.tf").read_text()
    for var in ("litellm_base_url", "litellm_key_secret_arn"):
        block = tf.split(f'variable "{var}"')[1].split("\nvariable ")[0]
        assert re.search(r'default\s*=\s*""\n', block), var
    assert "together, or neither" in tf.split('variable "litellm_key_secret_arn"')[1]
    example = (ORCH / "terraform" / "terraform.tfvars.example").read_text()
    assert not re.search(r"^litellm_", example, re.MULTILINE)
    bin_ = (ORCH / "cdk" / "bin" / "orchestrator.ts").read_text()
    assert 'litellmBaseUrl: ctx("litellmBaseUrl", ""),' in bin_
    assert 'litellmKeySecretArn: ctx("litellmKeySecretArn", ""),' in bin_


def test_the_route_is_on_both_paths_behind_the_authorizer():
    bff = (ORCH / "terraform" / "bff.tf").read_text()
    stack = (ORCH / "cdk" / "lib" / "orchestrator-stack.ts").read_text()
    assert '"GET /api/models/litellm",' in bff
    assert '{ path: "/api/models/litellm", methods: [HttpMethod.GET] },' in stack
    assert re.search(r'route_key == "GET /api/models/litellm"', (ORCH / "bff" / "handler.py").read_text())


def test_a_build_gets_the_consoles_proxy_on_both_paths():
    import importlib
    import sys
    sys.path.insert(0, str(ORCH / "deployer"))
    runner = importlib.import_module("runner")
    env = {"LITELLM_BASE_URL": "https://litellm.example.com", "LITELLM_KEY_SECRET_ARN": ARN}
    guard = runner.litellm_settings(env)
    flags = runner.cdk_context("ax_1", True, None, None, guard)
    assert "litellmBaseUrl=https://litellm.example.com" in flags and f"litellmKeySecretArn={ARN}" in flags
    tf = runner.tf_vars("ax_1", True, "us-east-1", None, None, guard)
    assert (tf["litellm_base_url"], tf["litellm_key_secret_arn"]) == ("https://litellm.example.com", ARN)
    # A console without the setting passes nothing on: the build's plan does not change.
    assert runner.litellm_settings({}) == {} and runner.litellm_settings({"LITELLM_BASE_URL": "x"}) == {}
    assert not any("litellm" in f for f in runner.cdk_context("ax_1", True))
    assert not any(k.startswith("litellm") for k in runner.tf_vars("ax_1", True, "us-east-1"))
    builder = (ORCH / "terraform" / "builder.tf").read_text()
    assert "for_each = local.litellm_build_env" in builder
    plane = (ORCH / "cdk" / "lib" / "builder-plane.ts").read_text()
    assert "props.litellmEnv ?? {}" in plane
