"""The runtime side of the build's named blocks and per-agent Gateway identity.

What has to hold:
  * an agent that uses a named guardrail is checked by THAT guardrail (GUARDRAILS), and
    one that is not deployed stops the agent rather than letting it run unchecked;
  * an agent that uses a named memory reads and writes THAT memory (MEMORIES), with the
    memory's strategies and scope;
  * an identity an agent lists is exchanged through the workload identity for the right
    provider, OAuth or API key; a failure degrades to "no credential", never a crash;
  * with GATEWAY_AGENT_CLIENTS, each agent signs in to the Gateway as itself, and one
    agent's token is never handed to another.
"""
from __future__ import annotations

import asyncio
import json

import pytest


def _ctx(core: dict, agent_id: str = "analysis"):
    from app.common.context import AgentContext
    c = AgentContext.__new__(AgentContext)
    c.agent_id, c.subject_id, c.topic, c.user, c.session_id = agent_id, "", "t", "alice@example.com", "s1"
    c._agentcore = core
    return c


# --- guardrails -------------------------------------------------------------------------

def test_a_named_guardrail_is_the_one_applied(monkeypatch):
    from app.features.guardrails import client
    seen: list = []

    async def fake_check(text, source="INPUT", guardrail_id="", version=""):
        seen.append((guardrail_id, version))
        return text
    monkeypatch.setattr(client, "check", fake_check)
    monkeypatch.setenv("GUARDRAILS", json.dumps({"strict": {"id": "gr-strict", "version": "DRAFT"}}))
    c = _ctx({"guardrails": {"input": True, "use": "strict"}})
    c._record_guardrail = lambda *a, **k: None
    assert asyncio.run(c.guardrail("hi", "INPUT")) == "hi"
    assert seen == [("gr-strict", "DRAFT")]
    # Output was not turned on: nothing is checked.
    assert asyncio.run(c.guardrail("hi", "OUTPUT")) == "hi" and len(seen) == 1


def test_a_named_guardrail_that_is_not_deployed_stops_the_agent(monkeypatch):
    monkeypatch.setenv("GUARDRAILS", "{}")
    c = _ctx({"guardrails": {"input": True, "use": "strict"}})
    with pytest.raises(RuntimeError, match=r"guardrail 'strict'.*not deployed"):
        asyncio.run(c.guardrail("hi", "INPUT"))


# --- memory -----------------------------------------------------------------------------

def test_a_named_memory_is_read_and_written_with_its_own_strategies_and_scope(monkeypatch):
    from app.common import config
    from app.features import memory as memory_pkg
    monkeypatch.setitem(config.WORKFLOW, "memories", {"shared": {"strategies": ["semantic", "userPreference"],
                                                                  "scope": "agent", "expiryDays": 90}})
    monkeypatch.setenv("MEMORIES", json.dumps({"shared": "mem-shared-123"}))
    calls: list = []

    async def fake_recall(query, namespace="", memory_id=""):
        calls.append(("recall", namespace, memory_id))
        return []

    stored_requests: list = []
    async def fake_store(session_id, actor_id, content, memory_id="", request=""):
        calls.append(("store", actor_id, memory_id))
        stored_requests.append(request)
    monkeypatch.setattr(memory_pkg, "recall", fake_recall)
    monkeypatch.setattr(memory_pkg, "store", fake_store)
    c = _ctx({"memory": {"use": "shared"}})
    c._record_memory = lambda *a, **k: None
    assert c._longterm_strategies() == ["semantic", "userPreference"]
    # Scope "agent": shared by everyone, so no user digest in the actor.
    assert c._memory_actor() == "analysis"
    asyncio.run(c.memory_recall("t"))
    asyncio.run(c.memory_store("a plain insight about t"))
    assert calls == [("recall", "insights/analysis", "mem-shared-123"),
                     ("recall", "preferences/analysis", "mem-shared-123"),
                     ("store", "analysis", "mem-shared-123")]
    # The run's request is stored as the USER turn, so a userPreference strategy
    # can extract what the user asked for ("Tone: casual") as a preference.
    assert stored_requests == [str(c.topic or "")]


def test_a_named_memory_that_is_not_deployed_is_refused(monkeypatch):
    from app.features.memory.client import memory_for
    monkeypatch.setenv("MEMORIES", "{}")
    with pytest.raises(RuntimeError, match=r"memory 'shared'.*not deployed"):
        memory_for("shared")


def test_an_agent_with_its_own_strategies_keeps_the_deployment_memory(monkeypatch):
    c = _ctx({"memory": {"longTerm": ["semantic"]}})
    assert c._memory_def() is None and c._memory_id() == "" and c._longterm_strategies() == ["semantic"]


# --- identity ---------------------------------------------------------------------------

class _Plane:
    def __init__(self, fail=False):
        self.calls, self.fail = [], fail

    def get_workload_access_token(self, workloadName):
        self.calls.append(("wat", workloadName))
        return {"workloadAccessToken": "wat-1"}

    def get_resource_oauth2_token(self, **kw):
        self.calls.append(("oauth", kw))
        if self.fail:
            raise RuntimeError("AccessDenied")
        return {"accessToken": "tok-1"}

    def get_resource_api_key(self, **kw):
        self.calls.append(("key", kw))
        return {"apiKey": "key-1"}


def test_an_identity_is_exchanged_through_the_workload_identity(monkeypatch):
    from app.features.identity import client
    plane = _Plane()
    monkeypatch.setattr(client, "_data_plane", lambda: plane)
    monkeypatch.setenv("WORKLOAD_IDENTITY", "ax_1-agents")
    monkeypatch.setenv("IDENTITY_PROVIDERS", json.dumps({
        "partner": {"provider": "bedrock-agentcore-ax-1-id-partner", "type": "oauth2", "scopes": ["r"]},
        "keyed": {"provider": "bedrock-agentcore-ax-1-id-keyed", "type": "apikey", "scopes": []}}))
    c = _ctx({"identity": {"outbound": ["partner", "keyed"]}})
    assert asyncio.run(c.get_identity_token()) == "tok-1"
    assert asyncio.run(c.get_api_key("keyed")) == "key-1"
    oauth = next(k for op, k in plane.calls if op == "oauth")
    assert oauth == {"workloadIdentityToken": "wat-1",
                     "resourceCredentialProviderName": "bedrock-agentcore-ax-1-id-partner",
                     "scopes": ["r"], "oauth2Flow": "M2M"}
    assert ("wat", "ax_1-agents") in plane.calls
    # An existing provider named directly still works.
    assert asyncio.run(c.get_identity_token("my-existing-provider")) == "tok-1"


def test_a_failed_exchange_is_no_credential_not_a_crash(monkeypatch):
    from app.features.identity import client
    monkeypatch.setattr(client, "_data_plane", lambda: _Plane(fail=True))
    monkeypatch.setenv("WORKLOAD_IDENTITY", "ax_1-agents")
    assert asyncio.run(client.get_token("partner")) == ""
    assert asyncio.run(_ctx({}).get_api_key()) == ""


# --- the Gateway, per agent -------------------------------------------------------------

def test_each_agent_signs_in_to_the_gateway_as_itself(monkeypatch):
    from app.features.gateway import client
    from app.features.observability.scope import set_scope
    monkeypatch.setattr(client, "GATEWAY_TOKEN_URL", "https://auth.example.com/oauth2/token")
    monkeypatch.setattr(client, "GATEWAY_CLIENT_ID", "shared-id")
    monkeypatch.setattr(client, "GATEWAY_CLIENT_SECRET", "shared-secret")
    monkeypatch.setenv("GATEWAY_AGENT_CLIENTS", json.dumps({"analysis": {"id": "a-id", "secret": "a-secret"}}))
    client._token_cache.clear()
    issued: list = []

    class _Resp:
        def __init__(self, who):
            self.who = who

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return json.dumps({"access_token": f"tok-{self.who}", "expires_in": 3600}).encode()

    def fake_urlopen(req, timeout=10):
        import base64
        who = base64.b64decode(req.headers["Authorization"].split()[1]).decode().split(":")[0]
        issued.append(who)
        return _Resp(who)
    monkeypatch.setattr(client.urllib.request, "urlopen", fake_urlopen)
    set_scope("s1", "analysis")
    assert client._gateway_token() == "tok-a-id"
    set_scope("s1", "report")                 # no client of its own: the shared one
    assert client._gateway_token() == "tok-shared-id"
    set_scope("s1", "analysis")
    assert client._gateway_token() == "tok-a-id"   # cached, per client
    assert issued == ["a-id", "shared-id"]
    client._token_cache.clear()
