"""GET /api/models/litellm (bff/litellm_models.py): the LiteLLM proxy's models for the Build
view's picker, who may list them, and the model problems the Assistant sees. A fake proxy
stands in for bff/netguard.urlopen; no AWS, no network."""
from __future__ import annotations

import io
import json
import sys
import urllib.error

import pytest

BASE = "https://litellm.example.com"
ARN = "arn:aws:secretsmanager:eu-west-1:210987654321:secret:litellm-key-AbC123"
KEY = "sk-virtual-0123456789abcdef"


class _Resp:
    def __init__(self, body):
        self._raw = json.dumps(body).encode()

    def read(self, n=-1):
        return self._raw

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


@pytest.fixture
def lm(monkeypatch):
    import litellm_models as mod
    monkeypatch.setenv(mod.BASE_URL_ENV, BASE)
    monkeypatch.setenv(mod.KEY_SECRET_ENV, ARN)
    monkeypatch.setattr(mod, "_key", None)
    monkeypatch.setattr(mod, "_cache", {"at": 0.0, "body": None})
    secrets = []

    class Secrets:
        def get_secret_value(self, **kw):
            from botocore.session import get_session
            from botocore.validate import validate_parameters
            validate_parameters(kw, get_session().get_service_model("secretsmanager")
                                .operation_model("GetSecretValue").input_shape)
            secrets.append(kw)
            return {"SecretString": KEY}
    import boto3
    real = boto3.client
    monkeypatch.setattr(boto3, "client", lambda svc, *a, **k: Secrets() if svc == "secretsmanager"
                        else real(svc, *a, **k))
    seen = []

    def answer(body):
        def urlopen(req, timeout=10, allow_private=False):
            seen.append({"url": req.full_url, "auth": req.get_header("Authorization"), "allow_private": allow_private})
            if isinstance(body, BaseException):
                raise body
            return _Resp(body)
        monkeypatch.setattr(mod.netguard, "urlopen", urlopen)
    mod.answer, mod.seen, mod.secrets = answer, seen, secrets
    return mod


def test_lists_the_proxys_models_as_litellm_ids(lm):
    lm.answer({"object": "list", "data": [{"id": "gpt-4o", "object": "model"}, {"id": "gemini-1.5-pro"},
                                          {"id": "bad id with spaces"}, {"id": ""}, "junk"]})
    got = lm.listing()
    assert got == {"configured": True, "models": [
        {"id": "litellm/gemini-1.5-pro", "name": "gemini-1.5-pro", "provider": "LiteLLM", "via": "litellm"},
        {"id": "litellm/gpt-4o", "name": "gpt-4o", "provider": "LiteLLM", "via": "litellm"}]}
    assert lm.seen == [{"url": f"{BASE}/v1/models", "auth": f"Bearer {KEY}", "allow_private": False}]
    lm.listing()                                   # cached: no second call
    assert len(lm.seen) == 1 and len(lm.secrets) == 1


def test_off_it_says_so_and_calls_nothing(lm, monkeypatch):
    monkeypatch.delenv(lm.BASE_URL_ENV)
    lm.answer({"data": []})
    assert lm.listing() == {"configured": False, "models": []}
    assert lm.seen == [] and lm.secrets == []


@pytest.mark.parametrize("failure", [urllib.error.HTTPError(BASE, 401, "no", {}, io.BytesIO(KEY.encode())),
                                     urllib.error.URLError("refused"), ValueError(f"echo {KEY}")])
def test_a_failure_degrades_to_typing_and_never_shows_the_key(lm, failure, capsys):
    lm.answer(failure)
    got = lm.listing()
    assert got["configured"] is True and got["models"] == [] and "type a model" in got["error"]
    assert KEY not in json.dumps(got) and KEY not in capsys.readouterr().out


def _event(groups=None, sub="user-1"):
    claims = {"sub": sub, "email": "a@example.com"} if sub else {}
    if groups is not None:
        claims["cognito:groups"] = groups
    return {"requestContext": {"http": {"method": "GET"}, "authorizer": {"jwt": {"claims": claims}}},
            "rawPath": "/api/models/litellm", "routeKey": "GET /api/models/litellm", "pathParameters": {}}


@pytest.fixture
def _bff(monkeypatch):
    """bff/handler.py, imported fresh as tests/test_multi_tool.py does."""
    import importlib
    monkeypatch.setenv("STATUS_TABLE", "t")
    monkeypatch.setenv("EVENTS_TABLE", "t")
    for mod in ("workflow", "authz", "chatbot", "handler"):
        sys.modules.pop(mod, None)
    yield importlib.import_module("handler")
    for mod in ("workflow", "authz", "chatbot", "handler"):
        sys.modules.pop(mod, None)


def test_the_route_lists_for_a_builder_user(_bff, lm, monkeypatch):
    handler = _bff
    monkeypatch.setattr(handler.builds, "ENABLED", True)
    lm.answer({"data": [{"id": "gpt-4o"}]})
    out = handler._route(_event(), None)
    assert out["statusCode"] == 200
    assert json.loads(out["body"])["models"][0]["id"] == "litellm/gpt-4o"


def test_off_the_builder_only_an_admin_may_list(_bff, lm, monkeypatch):
    handler = _bff
    monkeypatch.setattr(handler.builds, "ENABLED", False)
    monkeypatch.setattr(handler.authz, "permitted", lambda action, event: False)
    lm.answer({"data": [{"id": "gpt-4o"}]})
    out = handler._route(_event(), None)
    assert out["statusCode"] == 403 and lm.seen == [] and lm.secrets == []
    monkeypatch.setattr(handler.authz, "permitted", lambda action, event: action == "admin")
    assert handler._route(_event(), None)["statusCode"] == 200


def test_no_identity_is_not_a_builder_user(_bff, lm, monkeypatch):
    handler = _bff
    monkeypatch.setattr(handler.builds, "ENABLED", True)
    monkeypatch.setattr(handler.authz, "permitted", lambda action, event: False)
    assert handler._route(_event(sub=""), None)["statusCode"] == 403


def test_the_route_is_404_while_litellm_is_off(_bff, lm, monkeypatch):
    handler = _bff
    monkeypatch.setattr(handler.builds, "ENABLED", True)
    monkeypatch.delenv(lm.BASE_URL_ENV)
    assert handler._route(_event(), None)["statusCode"] == 404


def _wf(model):
    return {"orchestrator": {"defaultModel": "us.anthropic.claude-haiku-4-5-20251001-v1:0"},
            "agents": {"a": {"model": model}, "b": {}, "t": {"runtime": "task", "model": model}}}


def test_problems_warn_when_litellm_is_off(lm, monkeypatch):
    monkeypatch.delenv(lm.BASE_URL_ENV)
    assert lm.problems(_wf("litellm/gpt-4o")) == [{
        "severity": "warning", "where": {"kind": "agent", "id": "a"}, "path": "agents.a.model",
        "message": "litellm/gpt-4o is a LiteLLM model, and LiteLLM is not set up on this deployment: "
                   "deploy with a LiteLLM proxy (DEPLOYMENT.md), or pick a Bedrock model"}]


def test_problems_warn_on_a_name_the_proxy_does_not_serve(lm):
    lm.answer({"data": [{"id": "gpt-4o"}]})
    assert lm.problems(_wf("litellm/gpt-4o")) == []
    assert [p["message"] for p in lm.problems(_wf("litellm/gpt-9"))] == [
        "the LiteLLM proxy does not list gpt-9: pick one it serves"]


def test_problems_never_call_the_proxy_for_a_bedrock_workflow(lm):
    lm.answer({"data": []})
    assert lm.problems(_wf("us.amazon.nova-pro-v1:0")) == [] and lm.seen == []
