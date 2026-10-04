"""Self sign-up: off unless asked for, and a new user lands in a group that reaches only
their own builds and runs.

The console is on a public URL and every user can spend the account's Bedrock budget,
so registration stays closed by default on both IaC paths. Opened, each new user joins
one group through a post-confirmation trigger (signup_lambda/handler.py) — and that
group must be one workflow.json names, or confirmation would fail for everyone.
"""

import importlib.util
import json
import re
import sys
import types
from pathlib import Path

ORCH = Path(__file__).resolve().parent.parent
TS = (ORCH / "cdk" / "lib" / "orchestrator-stack.ts").read_text()
BIN = (ORCH / "cdk" / "bin" / "orchestrator.ts").read_text()
TF_VARS = (ORCH / "terraform" / "variables.tf").read_text()
TF_SIGNUP = (ORCH / "terraform" / "signup.tf").read_text()
TF_COGNITO = (ORCH / "terraform" / "cognito.tf").read_text()


def load_trigger(monkeypatch, calls):
    fake = types.SimpleNamespace(admin_add_user_to_group=lambda **kw: calls.append(kw))
    monkeypatch.setenv("DEFAULT_GROUP", "members")
    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(client=lambda *_a, **_k: fake))
    spec = importlib.util.spec_from_file_location("signup_handler", ORCH / "signup_lambda" / "handler.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_a_confirmed_sign_up_joins_the_default_group(monkeypatch):
    calls = []
    mod = load_trigger(monkeypatch, calls)
    event = {"triggerSource": "PostConfirmation_ConfirmSignUp", "userPoolId": "us-east-1_X",
             "userName": "abc-123"}
    assert mod.handler(event, None) is event            # a trigger hands the event back
    assert calls == [{"UserPoolId": "us-east-1_X", "Username": "abc-123", "GroupName": "members"}]


def test_a_password_reset_changes_nobodys_groups(monkeypatch):
    calls = []
    mod = load_trigger(monkeypatch, calls)
    mod.handler({"triggerSource": "PostConfirmation_ConfirmForgotPassword",
                 "userPoolId": "p", "userName": "u"}, None)
    assert calls == []


def test_registration_is_closed_unless_asked_for_on_both_paths():
    assert "selfSignUpEnabled: Boolean(props.selfSignUp)" in TS
    assert 'String(app.node.tryGetContext("selfSignUp")) === "true"' in BIN
    assert "self_signup       = optional(bool, false)" in TF_VARS
    assert "allow_admin_create_user_only = !local.self_signup" in TF_COGNITO


def test_the_default_group_is_the_same_on_both_paths_and_workflow_json_names_it():
    assert 'ctx("selfSignUpGroup", "members")' in BIN
    assert 'self_signup_group = optional(string, "members")' in TF_VARS
    actions = json.loads((ORCH / "app" / "workflow.json").read_text())["authorization"]["actions"]
    granted = sorted(a for a, groups in actions.items() if "members" in groups)
    # A self-registered user may act on their OWN builds and runs...
    assert {"deploy", "destroy", "delete", "decision"} <= set(granted)
    # ...and on nothing that spans other users' runs.
    assert "members" not in actions.get("insights", [])


def test_a_missing_group_is_refused_at_synth_and_plan_not_at_first_sign_up():
    assert "no action in " in TS and "authorization.actions names it" in TS
    assert re.search(r"precondition \{\s*condition\s*=\s*contains\(local\.authz_groups", TF_SIGNUP)


def test_build_stacks_never_open_registration():
    """Stacks the deploy runner creates are workflows, not consoles for strangers."""
    sys.path.insert(0, str(ORCH / "deployer"))
    try:
        import runner
    finally:
        sys.path.pop(0)
    assert not any("selfSignUp" in f for f in runner.cdk_context("ax_12345678", True))
    assert "self_signup" not in json.dumps(runner.tf_vars("ax_12345678", True, "us-east-1"))


def test_both_paths_log_sign_ins_whenever_the_deployment_has_its_own_pool():
    # The same function, a second trigger: post-authentication writes the activity log —
    # a console's builds table, or a build app's own audit table.
    assert "audit_sign_ins    = local.create_cognito\n" in TF_SIGNUP
    assert "post_authentication = local.audit_sign_ins ? aws_lambda_function.signup[0].arn : null" in TF_COGNITO
    assert "AUDIT_TABLE = local.audit_table_name" in TF_SIGNUP
    tf_audit = (ORCH / "terraform" / "audit.tf").read_text()
    assert ('audit_table_name = local.builder_enabled ? "${var.agent_name}_builds" : '
            '"${var.agent_name}_audit"') in tf_audit
    assert "const auditSignIns = true;" in TS
    assert ("const auditTableName = props.builder === false ? `${agentName}_audit` : "
            "`${agentName}_builds`;") in TS
    assert "cognito.UserPoolOperation.POST_AUTHENTICATION, signup" in TS
