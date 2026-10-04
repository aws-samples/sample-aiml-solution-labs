"""The policy library (bff/policies.py) and the Cedar checks (bff/cedar.py).

What has to hold:
  * a library is private: another user lists none of it, and changes none of it (404);
  * a policy is saved only once it checks out, and names are unique per user;
  * plain English is written by AgentCore Policy against the caller's DEPLOYED build (its
    engine and Gateway), is checked, and is never saved by the generator itself;
  * every change is in the caller's activity log;
  * a custom policy deploys the same way on both IaC paths.
"""
from __future__ import annotations

import cedar  # bff/ is on sys.path (conftest)
import pytest
from conftest import ORCH_ROOT

moto = pytest.importorskip("moto")
from test_builds import call  # noqa: E402  - the moto-backed BFF, and its fixture:
from test_builds import env as env  # noqa: E402

GW = 'resource == AgentCore::Gateway::"{{gateway}}"'
FORBID = (f'forbid(principal, action == AgentCore::Action::"refunds___issueRefund", {GW}) '
          'when { context.input.amount > 500 };')
TOOLS = {"refunds": {"type": "lambda", "description": "Issue refunds.",
                     "lambdaArn": "arn:aws:lambda:us-east-1:123456789012:function:refunds",
                     "toolSchema": [{"name": "issueRefund", "properties": {"amount": {"type": "integer"},
                                                                           "orderId": {"type": "string"}}}]},
         "kb": {"type": "kb", "corpora": ["policies"]}}


# --- cedar.py ---------------------------------------------------------------------------

def test_a_good_statement_has_no_problems_and_a_summary():
    assert cedar.problems(FORBID, list(TOOLS)) == []
    assert cedar.summary(FORBID) == {"effect": "forbid", "actions": ["refunds___issueRefund"]}
    assert cedar.render(FORBID, "arn:gw").count('AgentCore::Gateway::"arn:gw"') == 1


@pytest.mark.parametrize("statement, fragment", [
    ("", "is empty"),
    ("allow(principal, action, resource);", "must start with permit( or forbid("),
    (f'forbid(principal, action in AgentCore::Action::"refunds", {GW})', "must end with ;"),
    (f'forbid(principal, action in AgentCore::Action::"refunds", {GW}) when {{ x ;', "not closed"),
    (f'permit(principal, action == AgentCore::Action::"refunds___issueRefund", {GW});',
     "needs a `when`"),
    (f'forbid(principal, action == AgentCore::Action::"refunds", {GW});', "action == needs one tool"),
    ('forbid(principal, action in AgentCore::Action::"refunds", resource);', "{{gateway}}"),
    (f'forbid(principal, action in AgentCore::Action::"nope", {GW});', "not a tool of this build"),
    (f'forbid(principal, action, {GW});', "must name the tool"),
    ((f'forbid(principal, action in AgentCore::Action::"kb", {GW}); '
      f'forbid(principal, action in AgentCore::Action::"kb", {GW});'), "more than one statement"),
    ("forbid(principal, action, resource) because;", "after the head"),
    ("forbid(action, principal, resource);", "in that order"),
    ("forbid(principal, action, resource) when { # };", "unexpected character `#`"),
    # Live, from AgentCore: attribute `input` in context for AgentCore::Action::"fxConvert" not found.
    (f'forbid(principal, action in AgentCore::Action::"refunds", {GW}) when {{ context.input.amount > 5 }};',
     'a condition on context.input needs one tool, action == AgentCore::Action::"refunds___<toolName>"'),
    # Observed twice live from the Assistant: it parses and deploys, and matches nothing.
    ((f'forbid(principal, action == AgentCore::Action::"kb___retrieve", {GW}) '
      'when { context.arguments.query like "*Acme*" };'), "not context.arguments"),
    # Live, from AgentCore: attribute `query` in context ... not found. Did you mean `input`?
    ((f'forbid(principal, action in AgentCore::Action::"kb", {GW}) '
      'when { context.query like "*Acme*" };'), "not context.query"),
])
def test_what_agentcore_would_refuse_is_caught_first(statement, fragment):
    assert any(fragment in p for p in cedar.problems(statement, list(TOOLS))), cedar.problems(statement, list(TOOLS))


def test_a_library_policy_is_not_checked_against_any_one_build():
    s = f'forbid(principal, action in AgentCore::Action::"anyTool", {GW});'
    assert cedar.problems(s, None) == []
    assert cedar.problems(s, ["other"]) != []


def test_comments_strings_and_annotations_do_not_confuse_it():
    s = ('// a comment with forbid( and ;\n@id("x;y")\nforbid(principal, action == AgentCore::Action::"kb___retrieve", '
         f'{GW}) unless {{ context.input.q like "*;*" }};')
    assert cedar.problems(s, list(TOOLS)) == []


# --- the library ------------------------------------------------------------------------

def test_a_library_is_private_and_names_are_unique(env):
    status, p = call(env, "POST /api/policies", body={"name": "noBigRefunds", "description": "Over 500.",
                                                      "statement": FORBID, "source": "english"})
    assert status == 200 and p["effect"] == "forbid" and p["actions"] == ["refunds___issueRefund"]
    assert call(env, "POST /api/policies", body={"name": "noBigRefunds", "statement": FORBID})[0] == 409
    assert [x["name"] for x in call(env, "GET /api/policies")[1]] == ["noBigRefunds"]
    # Someone else sees none of it and changes none of it.
    assert call(env, "GET /api/policies", sub="u2")[1] == []
    assert call(env, "PUT /api/policies/{id}", params={"id": p["id"]}, body={"name": "x"}, sub="u2")[0] == 404
    assert call(env, "DELETE /api/policies/{id}", params={"id": p["id"]}, sub="u2")[0] == 404
    # The owner edits and deletes it.
    status, q = call(env, "PUT /api/policies/{id}", params={"id": p["id"]}, body={"description": "Refunds over 500."})
    assert status == 200 and q["description"] == "Refunds over 500." and q["statement"] == FORBID
    assert call(env, "DELETE /api/policies/{id}", params={"id": p["id"]})[0] == 200
    assert call(env, "GET /api/policies")[1] == []
    acts = [e["action"] for e in call(env, "GET /api/audit")[1]]
    assert {"policy.created", "policy.updated", "policy.deleted"} <= set(acts)
    # Out of the builds list: no `updated`, so not in the owner index.
    assert call(env, "GET /api/builds")[1] == []


@pytest.mark.parametrize("body, status", [
    ({"name": "bad name", "statement": FORBID}, 400),
    ({"name": "ok", "statement": "permit(principal, action, resource);"}, 400),
    ({"name": "ok", "statement": FORBID, "source": "magic"}, 400),
    ({"name": "ok", "statement": FORBID, "description": "x" * 1001}, 400),
])
def test_a_policy_is_saved_only_once_it_checks_out(env, body, status):
    assert call(env, "POST /api/policies", body=body)[0] == status
    assert call(env, "GET /api/policies")[1] == []


def test_an_unknown_policy_is_404(env):
    assert call(env, "PUT /api/policies/{id}", params={"id": "zz"}, body={})[0] == 404
    assert call(env, "DELETE /api/policies/{id}", params={"id": "0badc0de"})[0] == 404


# --- plain English, by AgentCore Policy -------------------------------------------------

GW_ARN = "arn:aws:bedrock-agentcore:us-east-1:123456789012:gateway/ax-deadbeef-gw-abc"


class FakeControl:
    """bedrock-agentcore-control, as far as policy generation goes."""

    class exceptions:
        class ResourceNotFoundException(Exception):
            pass

    def __init__(self, agent, engine=True, assets=None, status="GENERATED"):
        self.agent, self.engine, self.status, self.started = agent, engine, status, []
        self.assets = assets if assets is not None else [
            {"definition": {"policy": {"statement": FORBID.replace("{{gateway}}", GW_ARN)}},
             "rawTextFragment": "Never refund more than 500.", "findings": []},
            {"rawTextFragment": "Only managers.", "findings": [{"type": "INVALID", "description": "Non-translatable"}]}]

    def list_policy_engines(self, **kw):
        return {"policyEngines": [{"name": "other_policy", "policyEngineId": "x"}]
                + ([{"name": f"{self.agent}_policy", "policyEngineId": "eng1"}] if self.engine else [])}

    def list_gateways(self, **kw):
        return {"items": [{"name": f"{self.agent.replace('_', '-')}-gw", "gatewayId": "gw1"}]}

    def get_gateway(self, gatewayIdentifier):
        return {"gatewayArn": GW_ARN}

    def start_policy_generation(self, **kw):
        self.started.append(kw)
        return {"policyGenerationId": "ax_1-gen", "status": "GENERATING"}

    def get_policy_generation(self, policyEngineId, policyGenerationId):
        if policyGenerationId != "ax_1-gen":
            raise self.exceptions.ResourceNotFoundException()
        return {"status": self.status, "statusReasons": ["boom"] if self.status == "GENERATE_FAILED" else []}

    def list_policy_generation_assets(self, **kw):
        return {"policyGenerationAssets": self.assets}


def deployed_build(env, monkeypatch, **fake):
    from test_builds import project
    p = project(tools={"refunds": {"type": "lambda", "description": "x", "toolSchema": [
        {"name": "issueRefund", "properties": {"amount": {"type": "integer", "required": True}}}],
        "arg": "amount", "lambdaArn": "arn:aws:lambda:us-east-1:123456789012:function:r"}})
    p["workflow"]["agents"]["claims_intake"]["tool"] = "refunds"
    call(env, "PUT /api/builds/{id}", params={"id": "pclaims01"}, body={"project": p})
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    from test_builds import mark_deployed
    agent = mark_deployed(env, "pclaims01")
    ctl = FakeControl(agent, **fake)

    class Session:
        def __init__(self, **kw):
            pass

        def client(self, *a, **kw):
            return ctl
    monkeypatch.setattr(env.handler.policies.boto3, "Session", Session)
    return ctl


def test_agentcore_policy_writes_it_for_the_deployed_build(env, monkeypatch):
    ctl = deployed_build(env, monkeypatch)
    status, started = call(env, "POST /api/policies/generate",
                           body={"build": "pclaims01", "text": "never refund more than 500 dollars"})
    assert status == 202 and started["generationId"] == "ax_1-gen"
    assert ctl.started[0]["policyEngineId"] == "eng1" and ctl.started[0]["resource"] == {"arn": GW_ARN}
    assert ctl.started[0]["content"] == {"rawText": "never refund more than 500 dollars"}
    status, got = call(env, "GET /api/policies/generate/{id}", params={"id": "ax_1-gen"},
                       qs={"build": "pclaims01"})
    assert status == 200 and got["status"] == "GENERATED"
    first, second = got["assets"]
    # The Gateway's ARN goes back to the placeholder, so it deploys anywhere.
    assert first["statement"] == FORBID and first["problems"] == []
    assert second["statement"] == "" and second["findings"][0]["type"] == "INVALID" and second["problems"]
    # Drafted, not saved; and in the log.
    assert call(env, "GET /api/policies")[1] == []
    assert "policy.generated" in [e["action"] for e in call(env, "GET /api/audit")[1]]


def test_it_reports_generating_and_failed(env, monkeypatch):
    ctl = deployed_build(env, monkeypatch, status="GENERATING")
    got = call(env, "GET /api/policies/generate/{id}", params={"id": "ax_1-gen"}, qs={"build": "pclaims01"})[1]
    assert got == {"status": "GENERATING", "reasons": []}
    ctl.status = "GENERATE_FAILED"
    got = call(env, "GET /api/policies/generate/{id}", params={"id": "ax_1-gen"}, qs={"build": "pclaims01"})[1]
    assert got["status"] == "GENERATE_FAILED" and got["reasons"] == ["boom"]
    assert call(env, "GET /api/policies/generate/{id}", params={"id": "nope"}, qs={"build": "pclaims01"})[0] == 404
    assert call(env, "GET /api/policies/generate/{id}", params={"id": "bad id!"}, qs={"build": "pclaims01"})[0] == 400


def test_plain_english_needs_a_deployed_build_with_its_policy_engine(env, monkeypatch):
    from test_builds import save
    save(env)
    status, body = call(env, "POST /api/policies/generate", body={"build": "pclaims01", "text": "x"})
    assert status == 409 and "deploy the build first" in body["error"]
    deployed_build(env, monkeypatch, engine=False)
    status, body = call(env, "POST /api/policies/generate", body={"build": "pclaims01", "text": "x"})
    assert status == 409 and "policy engine off" in body["error"]
    assert call(env, "POST /api/policies/generate", body={"build": "pclaims01", "text": ""})[0] == 400
    # Only your own build.
    assert call(env, "POST /api/policies/generate", body={"build": "pclaims01", "text": "x"}, sub="u2")[0] == 404


def test_an_agentcore_failure_is_a_502_with_a_way_forward(env, monkeypatch):
    ctl = deployed_build(env, monkeypatch)

    def boom(**kw):
        raise RuntimeError("throttled")
    ctl.start_policy_generation = boom
    status, body = call(env, "POST /api/policies/generate", body={"build": "pclaims01", "text": "x"})
    assert status == 502 and "form" in body["error"]


# --- the routes and the deploy ----------------------------------------------------------

def test_the_routes_are_declared_on_both_iac_paths():
    ts = (ORCH_ROOT / "cdk" / "lib" / "orchestrator-stack.ts").read_text()
    tf = (ORCH_ROOT / "terraform" / "bff.tf").read_text()
    for route in ("GET /api/policies", "POST /api/policies", "POST /api/policies/generate",
                  "GET /api/policies/generate/{id}", "PUT /api/policies/{id}", "DELETE /api/policies/{id}"):
        assert f'"{route}"' in tf
        method, path = route.split(" ")
        assert f'path: "{path}"' in ts and f"HttpMethod.{method}" in ts


def test_a_statement_older_boto3_cannot_parse_is_read_from_the_raw_response(env, monkeypatch):
    """Lambda's boto3 predates the `policy` member of a definition: it hands back
    SDK_UNKNOWN_MEMBER and no statement. The raw body still has it."""
    import json as _json
    import types
    ctl = deployed_build(env, monkeypatch)
    handlers = []
    ctl.meta = types.SimpleNamespace(events=types.SimpleNamespace(
        register=lambda name, fn: handlers.append(fn), unregister=lambda name, fn: handlers.remove(fn)))
    body = {"policyGenerationAssets": [{"definition": {"policy": {"statement": FORBID.replace("{{gateway}}", GW_ARN)}},
                                        "rawTextFragment": "No big refunds.", "findings": []}]}

    def old_boto3(**kw):
        for fn in handlers:
            fn(http_response=types.SimpleNamespace(content=_json.dumps(body).encode()))
        return {"policyGenerationAssets": [{"definition": {"SDK_UNKNOWN_MEMBER": {"name": "policy"}},
                                            "rawTextFragment": "No big refunds.", "findings": []}]}
    ctl.list_policy_generation_assets = old_boto3
    got = call(env, "GET /api/policies/generate/{id}", params={"id": "ax_1-gen"}, qs={"build": "pclaims01"})[1]
    assert got["assets"][0]["statement"] == FORBID and got["assets"][0]["problems"] == []
    assert handlers == []          # unregistered afterwards
