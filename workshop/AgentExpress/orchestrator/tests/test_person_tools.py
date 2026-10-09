"""Tools that act as the PERSON using the app (auth "user" and "obo").
What has to hold:
  * the caller's sign-in reaches the runtime only for the run's OWNER, signed in to
    the build's own app, and only when some tool acts as the person (bff/handler.py);
  * the runtime keeps it per invocation (a context variable), and a person tool is
    reached on the person Gateway with it, never with the machine client, nor the
    reverse (app/features/gateway/person.py, client._tools_for);
  * a Gateway that asks the person to connect their account (-32042, URL elicitation)
    fails the step with where to connect, which the app turns into a button;
  * POST /api/connect binds a returning person's session to THEIR sign-in, and
    refuses anything that is not such a session.
"""
from __future__ import annotations

import asyncio
import contextvars
import json

import pytest
from test_bff_routes import CTX
from test_bff_routes import bff as bff  # the fixture

from app.common.errors import ToolNeedsConsent, ToolUnavailable
from app.features.gateway import client, person

JWT = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ1MSJ9.c2ln"
CONSENT = ("https://bedrock-agentcore.us-east-1.amazonaws.com/identities/oauth2/authorize"
           "?request_uri=urn%3Aietf%3Aparams%3Aoauth%3Arequest_uri%3Aabc&client_id=x")


# --- the runtime ------------------------------------------------------------------

def test_only_a_jwt_is_kept_and_only_for_this_context():
    def inner():
        person.set_token(JWT)
        return person.token()
    assert contextvars.copy_context().run(inner) == JWT
    assert person.token() == ""  # the copy's value never leaks back
    person.set_token("not a token")
    assert person.token() == ""
    person.set_token(None)  # type: ignore[arg-type]
    assert person.token() == ""


def test_the_consent_url_is_found_in_a_json_escaped_error():
    # As the MCP client hands it over (data, dumped by client._error_text)...
    assert person.consent_url(json.dumps({"elicitations": [{"mode": "url", "url": CONSENT}]})) == CONSENT
    # ...and as raw response text, where "&" may arrive JSON-escaped.
    raw = '{"error":{"code":-32042,"data":{"elicitations":[{"url":"' + CONSENT.replace("&", "\\u0026") + '"}]}}}'
    assert person.consent_url(raw) == CONSENT
    assert person.consent_url("Unauthorized") == ""
    # Only AgentCore Identity's own authorize address, never any link in an error.
    assert person.consent_url("go to https://evil.example.com/identities/oauth2/authorize?x=1") == ""


class _McpError(Exception):
    """The shape the MCP client raises: an `error` with code and data."""
    def __init__(self, code, message, data=None):
        super().__init__(message)
        self.error = type("E", (), {"code": code, "message": message, "data": data})()


def test_a_request_to_connect_fails_the_step_with_where_to_connect():
    class Tool:
        name = "crm___listOrders"
        async def ainvoke(self, args):
            raise _McpError(-32042, "URL elicitation required",
                            {"elicitations": [{"mode": "url", "url": CONSENT}]})
    with pytest.raises(ToolNeedsConsent) as got:
        asyncio.run(client._invoke(Tool(), {}))
    assert got.value.url == CONSENT and got.value.tool == "crm"
    # The exact line web/src/views/ConnectAccount.tsx reads.
    assert str(got.value) == f"Connect your account for 'crm' first, then run this step again: {CONSENT}"
    assert isinstance(got.value, ToolUnavailable)


def test_person_tools_go_to_the_person_gateway_only(monkeypatch):
    calls = []

    async def fake(as_person=False):
        calls.append(as_person)
        return [f"{'person' if as_person else 'machine'}-tool"]
    monkeypatch.setattr(client, "_gateway_tools", fake)
    monkeypatch.setattr(client, "PERSON_TOOLS", frozenset({"crm"}))
    assert asyncio.run(client._tools_for(["crm"])) == ["person-tool"]
    assert asyncio.run(client._tools_for(["docs"])) == ["machine-tool"]
    assert asyncio.run(client._tools_for(["docs", "crm"])) == ["machine-tool", "person-tool"]
    assert calls == [True, False, False, True]


def test_a_person_tool_with_no_person_says_so(monkeypatch):
    monkeypatch.setattr(client, "GATEWAY_USER_URL", "")
    with pytest.raises(ToolUnavailable, match="no person Gateway"):
        asyncio.run(client._gateway_tools(as_person=True))
    monkeypatch.setattr(client, "GATEWAY_USER_URL", "https://gwu.example.com/mcp")
    person.set_token("")
    with pytest.raises(ToolUnavailable, match="not started by them"):
        asyncio.run(client._gateway_tools(as_person=True))


# --- the BFF -----------------------------------------------------------------------


def _event(path="/api/connect", body=None, token=JWT, email="user@example.com"):
    ev = {"requestContext": {"http": {"method": "POST"},
                             "authorizer": {"jwt": {"claims": {"email": email}}}},
          "rawPath": path, "routeKey": f"POST {path}", "pathParameters": {},
          "headers": {"Authorization": f"Bearer {token}"} if token else {}}
    if body is not None:
        ev["body"] = json.dumps(body)
    return ev


def test_the_sign_in_goes_only_to_the_owner_of_a_run_with_person_tools(bff):
    person_target = {"build": "", "raw": {"tools": {"crm": {"auth": "user"}}}}
    plain_target = {"build": "", "raw": {"tools": {"crm": {"auth": "apikey"}}}}
    ev = _event()
    assert bff._person_token(ev, person_target, "user@example.com") == JWT
    assert bff._person_token(ev, person_target, "someone-else@example.com") == ""
    assert bff._person_token(ev, person_target, "") == ""
    assert bff._person_token(ev, plain_target, "user@example.com") == ""
    # Run from the Builder console: the console's sign-in is not the build's.
    assert bff._person_token(ev, {**person_target, "build": "b-1"}, "user@example.com") == ""
    assert bff._person_token(_event(token=""), person_target, "user@example.com") == ""


def test_connect_binds_the_session_to_the_callers_own_sign_in(bff, monkeypatch):
    seen = {}

    class Identity:
        def complete_resource_token_auth(self, **kw):
            seen.update(kw)
    monkeypatch.setattr(bff, "_identity", lambda: Identity())
    urn = "urn:ietf:params:oauth:request_uri:abc-123"
    resp = bff._api(_event(body={"sessionUri": urn}), CTX)
    assert resp["statusCode"] == 200
    assert seen == {"sessionUri": urn, "userIdentifier": {"userToken": JWT}}


@pytest.mark.parametrize("body,token,status", [
    ({"sessionUri": "https://evil.example.com"}, JWT, 400),
    ({"sessionUri": "urn:ietf:params:oauth:request_uri:a b"}, JWT, 400),
    ({}, JWT, 400),
    ({"sessionUri": "urn:ietf:params:oauth:request_uri:abc"}, "", 401),
])
def test_connect_refuses_anything_else(bff, monkeypatch, body, token, status):
    monkeypatch.setattr(bff, "_identity", lambda: pytest.fail("must not be called"))
    assert bff._api(_event(body=body, token=token), CTX)["statusCode"] == status


def test_a_connection_that_cannot_be_bound_says_why(bff, monkeypatch):
    class Identity:
        def complete_resource_token_auth(self, **kw):
            raise RuntimeError("ValidationException: session expired")
    monkeypatch.setattr(bff, "_identity", lambda: Identity())
    resp = bff._api(_event(body={"sessionUri": "urn:ietf:params:oauth:request_uri:abc"}), CTX)
    assert resp["statusCode"] == 409
    assert "expired" in json.loads(resp["body"])["error"]



def test_an_entra_user_is_named_by_their_sign_in_name_not_an_id(bff):
    """Entra ID sends no `email` unless it is configured; its sign-in name is
    `preferred_username`. Seen live: runs showed an opaque id as who started them."""
    def who(claims):
        return bff._user({"requestContext": {"authorizer": {"jwt": {"claims": claims}}}})
    upn = "ax@contoso.onmicrosoft.com"
    assert who({"preferred_username": upn, "sub": "hyCFHonTA1NY"}) == upn
    assert who({"email": "ana@example.com", "preferred_username": "x", "sub": "s"}) == "ana@example.com"
    assert who({"sub": "s"}) == "s"
