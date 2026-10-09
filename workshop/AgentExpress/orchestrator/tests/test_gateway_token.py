"""The client-credentials request the runtime sends for a Gateway token, per IdP.

Each provider wants the client's credentials in a different place and asks for the
token differently; the Gateway's authorizer (terraform/gateway.tf, cdk/lib/tool-plane.ts)
is configured for the token each request produces. A wrong shape is a token the IdP
refuses, or one the Gateway refuses, so every agent tool call fails.
"""
import base64
import urllib.parse

import pytest

from app.common.errors import ToolUnavailable
from app.features.gateway import client


def _sent(monkeypatch, flow, *, audience="", scope="", url="https://idp.example.com/token"):
    monkeypatch.setattr(client, "GATEWAY_AUTH_FLOW", flow)
    monkeypatch.setattr(client, "GATEWAY_AUDIENCE", audience)
    monkeypatch.setattr(client, "GATEWAY_SCOPE", scope)
    monkeypatch.setattr(client, "GATEWAY_TOKEN_URL", url)
    req = client._token_request("cid-1", "s3cret")
    body = dict(urllib.parse.parse_qsl(req.data.decode()))
    basic = req.get_header("Authorization") or ""
    return req, body, basic


def _basic(user, secret):
    return "Basic " + base64.b64encode(f"{user}:{secret}".encode()).decode()


def test_cognito_basic_auth_and_the_audience_as_scope(monkeypatch):
    _, body, basic = _sent(monkeypatch, "cognito", audience="gateway/invoke")
    assert body == {"grant_type": "client_credentials", "scope": "gateway/invoke"}
    assert basic == _basic("cid-1", "s3cret")


def test_auth0_credentials_in_the_body_and_an_audience(monkeypatch):
    _, body, basic = _sent(monkeypatch, "auth0", audience="https://api.example.com")
    assert body == {"grant_type": "client_credentials", "client_id": "cid-1",
                    "client_secret": "s3cret", "audience": "https://api.example.com"}
    assert basic == ""


def test_okta_basic_auth_and_its_own_scope_not_the_audience(monkeypatch):
    # The audience (api://default) is what the token carries; the scope is what is asked for.
    _, body, basic = _sent(monkeypatch, "okta", audience="api://default", scope="gateway.invoke")
    assert body == {"grant_type": "client_credentials", "scope": "gateway.invoke"}
    assert basic == _basic("cid-1", "s3cret")


def test_entra_credentials_in_the_body_and_the_default_scope_of_the_api(monkeypatch):
    aud = "11111111-2222-3333-4444-555555555555"
    _, body, basic = _sent(monkeypatch, "entra", audience=aud)
    assert body == {"grant_type": "client_credentials", "client_id": "cid-1",
                    "client_secret": "s3cret", "scope": f"{aud}/.default"}
    assert basic == ""
    _, body, _ = _sent(monkeypatch, "entra", audience=aud, scope="api://agentexpress/.default")
    assert body["scope"] == "api://agentexpress/.default"


@pytest.mark.parametrize("flow", ["cognito", "auth0", "okta", "entra"])
def test_never_sends_the_secret_over_plain_http(monkeypatch, flow):
    with pytest.raises(ToolUnavailable):
        _sent(monkeypatch, flow, audience="a", scope="s", url="http://idp.example.com/token")
