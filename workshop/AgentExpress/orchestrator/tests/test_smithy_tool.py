"""type="smithy": an AWS service's Smithy model as a Gateway target.

The Terraform path (tools_smithy.tf, the gateway.tf statements, policy.tf ordering), the
server validator's grant rules beyond the shared fixture cases, and the runtime's view
of the type. The CDK path is cdk/test/smithy-tool.test.ts.
"""
from __future__ import annotations

import re
import sys

from conftest import ORCH_ROOT

sys.path.insert(0, str(ORCH_ROOT / "bff"))
import smithy_tool  # noqa: E402
import validate_build  # noqa: E402

TF = ORCH_ROOT / "terraform"
SMITHY_TF = (TF / "tools_smithy.tf").read_text()
ALLOW = {"actions": ["geo:SearchPlaceIndexForText"],
         "resources": ["arn:aws:geo:us-east-1:111122223333:place-index/stores"]}


def _issues(tool: dict) -> list[tuple[str, str]]:
    out: list = []
    smithy_tool.check(tool, "tools.p", {"kind": "tool", "id": "p"}, lambda *a: out.append(a),
                      validate_build.vocab("smithyDeniedServices"))
    return [(sev, path) for sev, _w, path, _m in out]


def _block(src: str, header: str) -> str:
    return src.split(header, 1)[1].split("\n}\n", 1)[0]


def test_the_target_is_a_smithy_model_on_the_machine_gateway_signed_by_its_role_alone():
    block = _block(SMITHY_TF, 'resource "aws_bedrockagentcore_gateway_target" "smithy"')
    assert "smithy_model {" in block
    assert "aws_bedrockagentcore_gateway.mcp[0].gateway_id" in block
    # "For Lambda, API Gateway, and Smithy targets, do not include the iamCredentialProvider field."
    assert re.search(r"gateway_iam_role \{\}", block)
    assert "for_each           = local.smithy_tools" in block


def test_the_grants_join_the_gateway_policy_so_the_iam_wait_covers_them():
    gateway = (TF / "gateway.tf").read_text()
    assert "local.interceptor_statements, local.smithy_statements)" in gateway
    assert 'Sid      = "SmithyCalls${n}"' in SMITHY_TF
    assert 'Sid      = "ReadSmithyModels"' in SMITHY_TF


def test_every_cedar_permit_waits_for_the_smithy_targets():
    policy = (TF / "policy.tf").read_text()
    assert policy.count("aws_bedrockagentcore_gateway_target.smithy,") == 2


def test_terraform_reads_the_denied_services_from_the_vocabulary():
    assert "local.vocab.smithyDeniedServices.values" in SMITHY_TF
    # The same patterns as the server validator, so plan refuses what the Build view does.
    assert smithy_tool.ACTION_RE.pattern in SMITHY_TF
    assert smithy_tool.ARN_RE.pattern.replace("\\", "\\\\") in SMITHY_TF


def test_a_named_action_on_a_full_arn_passes():
    assert _issues({"type": "smithy", "schemaS3Uri": "s3://b-1/m.json", "allow": ALLOW}) == []
    assert _issues({"allow": {"actions": ["s3:GetObject"], "resources": ["arn:aws:s3:::amzn-s3-demo-bucket/reports/*"]}}) == []


def test_the_grant_is_capped_and_never_reaches_identity_code_or_secrets():
    many = {"actions": [f"geo:Op{i}" for i in range(21)], "resources": ALLOW["resources"]}
    assert ("error", "tools.p.allow.actions") in _issues({"allow": many})
    for svc in ("iam", "sts", "kms", "secretsmanager", "bedrock-agentcore", "lambda"):
        assert ("error", "tools.p.allow.actions[0]") in _issues({"allow": {**ALLOW, "actions": [f"{svc}:Anything"]}}), svc
    for arn in ("*", "arn:aws:s3:::*", "arn:aws:geo:*:111122223333:place-index/x", "arn:aws:geo:us-east-1:*:x"):
        assert ("error", "tools.p.allow.resources[0]") in _issues({"allow": {**ALLOW, "resources": [arn]}}), arn


def test_the_runtime_knows_the_type():
    from app.orchestrator import registry
    assert "smithy" in registry.TOOL_TYPES
