"""A2A — delegating a step to an agent this workflow does not operate.

`main` and `dedicated` run code from this image. `runtime: "a2a"` is the third
placement and the only one across a trust boundary: the agent is somebody else's
service, reached over the Agent2Agent protocol at its Agent Card URL.

That boundary is what these tests are about. Every other agent fails in ways we can
see; this one fails in ways only the remote side knows, so the contract worth pinning
is: the request we send is the shape the protocol specifies, the answer we read is
found wherever the protocol allows it to be, and anything else surfaces as
`RemoteAgentUnavailable` with their reason attached rather than as an empty asset.

The wire is stubbed at `urllib.request.urlopen` — the one seam the client uses — so
these run with no network, no credentials and no partner agent. The request bodies are
asserted against the published shapes (JSON-RPC 2.0 `message/send`, Agent Card at
`/.well-known/agent-card.json`), because an integration that silently sends the wrong
field looks identical to one that works until a real partner rejects it.

TWO SHAPES OF EVERYTHING. The spec renamed task states and part discriminators, and
deployed agents lag, so both forms are in the wild: `completed` / `TASK_STATE_COMPLETED`
and `{"kind":"text"}` / `{"type":"text"}` / a bare `{"text":…}`. Reading only the
current form is a run that hangs on a task that already finished, which is why every
one of those is a case below.
"""

import asyncio
import io
import json
import sys
import urllib.error

import pytest
from conftest import ORCH_ROOT, workflow

CARD_HOST = "https://agents.partner.example"
RPC_URL = f"{CARD_HOST}/rpc"

# A workflow whose one step is somebody else's agent. `wf()` from conftest cannot be
# used here: it gives every agent `runtime: "dedicated"`, and the point is the third
# placement.
REMOTE_WF = {
    "orchestrator": {"a2aInvoke": {"timeoutSeconds": 5, "pollIntervalSeconds": 0,
                                   "maxPollSeconds": 1}},
    "tools": {},
    "agents": {
        "intake": {"name": "Intake", "runtime": "dedicated", "maxTokens": 1000},
        "partner": {"name": "Partner Agent", "runtime": "a2a",
                    "agentCard": CARD_HOST, "produces": "risk-assessment"},
    },
    "steps": [{"agent": "intake"}, {"agent": "partner"}],
}


#: A Function URL: the kind of AWS endpoint a SigV4-signed request may go to.
LAMBDA_URL = "https://fn.lambda-url.us-east-1.on.aws/"


def remote_wf(**agent_overrides) -> dict:
    """REMOTE_WF with the partner agent's entry adjusted."""
    defn = json.loads(json.dumps(REMOTE_WF))
    defn["agents"]["partner"].update(agent_overrides)
    return defn


def no_contract_wf(**agent_overrides) -> dict:
    """REMOTE_WF with `produces` removed, so a reply comes back exactly as sent.

    Used by the tests about READING the wire. `produces` is what turns a structured
    reply into a stamped asset, and leaving it on would mean those tests assert the
    envelope too — so a change to the envelope would break nine tests about JSON-RPC
    part shapes, which are not about the envelope at all.
    """
    defn = remote_wf(**agent_overrides)
    del defn["agents"]["partner"]["produces"]
    return defn


class Ctx:
    """Only what A2AAgent touches on the context.

    `agent_id` and `input()` are here because the framework now stamps an asset
    envelope on a structured reply, and the two things it cannot guess — this agent's
    previous version and the upstream assetIds — are read off the context exactly the
    way a local agent reads them. They mirror app/common/context.py: `input(id)` is
    `state["outputs"][id]`, nothing more.
    """

    def __init__(self, outputs=None, feedback="", token="", agent_id="partner"):
        self.topic = "assess this applicant"
        self.session_id = "sess-1"
        self.agent_id = agent_id
        self.state = {"outputs": outputs or {}, "subject_id": ""}
        self.feedback = feedback
        self.logs: list[str] = []
        self._token = token

    def input(self, agent_id: str):
        return (self.state.get("outputs") or {}).get(agent_id)

    async def log(self, msg):
        self.logs.append(msg)

    async def get_identity_token(self, provider=""):
        return self._token


class Wire:
    """Stub for urllib.request.urlopen. Records every request; replies from a script.

    Keyed by (method-or-"card") so a test declares only the exchanges it cares about:
    the card fetch, the `message/send`, and each `tasks/get` in turn.
    """

    def __init__(self, card=None, send=None, polls=None, raise_on=None, stuck=False):
        self.card = card if card is not None else {"url": RPC_URL}
        self.send = send
        self.polls = list(polls or [])
        self.raise_on = raise_on or {}
        # `stuck`: every tasks/get answers "working", forever. A finite script cannot
        # test the poll budget — it runs out first, the stub falls back to its default
        # reply, and the run ends for the wrong reason (which is exactly what the first
        # version of the timeout test below did).
        self.stuck = stuck
        self.requests: list[dict] = []

    def __call__(self, request, timeout=None):
        url = request.full_url
        body = json.loads(request.data) if request.data else None
        method = (body or {}).get("method", "card")
        self.requests.append({
            "url": url, "method": method, "body": body,
            # header_items(), not .headers: urllib keeps "unredirected" headers in a
            # SECOND dict and sends both. Auth headers belong there (so a signature is
            # not replayed to a redirect target), so reading only .headers would show
            # no Authorization at all and a signing test would pass while unsigned.
            "headers": {k.lower(): v for k, v in request.header_items()},
            "timeout": timeout,
        })
        if method in self.raise_on:
            raise self.raise_on[method]
        if method == "card":
            payload = self.card
        elif method == "message/send":
            payload = {"jsonrpc": "2.0", "id": body["id"], **self.send}
        elif self.stuck:
            payload = {"jsonrpc": "2.0", "id": body["id"],
                       "result": {"id": "t1", "status": {"state": "working"}}}
        else:
            nxt = self.polls.pop(0) if self.polls else {"result": {"status": {"state": "completed"}}}
            payload = {"jsonrpc": "2.0", "id": body["id"], **nxt}
        return _Resp(payload)

    def of(self, method):
        return [r for r in self.requests if r["method"] == method]


class _Resp(io.BytesIO):
    """Context-manager response, like urlopen's."""

    def __init__(self, payload):
        super().__init__(json.dumps(payload).encode())

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def run(defn, wire, ctx=None, agent_id="partner"):
    """Build the agent from `defn` with the wire stubbed, and run it."""
    with workflow(defn) as imp:
        mod = imp("app.common.a2a_agent")
        mod.urllib.request.urlopen = wire
        agent = imp("app.orchestrator.registry").load_agents()[agent_id]
        return asyncio.run(agent.run(ctx or Ctx())), wire


ARTIFACT_OK = {"result": {"id": "t1", "status": {"state": "completed"},
                          "artifacts": [{"artifactId": "a1",
                                         "parts": [{"kind": "text", "text": "risk: low"}]}]}}


# ---------------------------------------------------------------------------
# The registry builds the right kind of agent
# ---------------------------------------------------------------------------

def test_a2a_placement_builds_a_remote_agent_and_needs_no_module_on_disk():
    """The point of the placement: no package under app/subagents/<id>/, because the
    code is somebody else's. A `main` agent with no module fails at import."""
    with workflow(REMOTE_WF) as imp:
        registry = imp("app.orchestrator.registry").load_agents()
        partner = registry["partner"]
        assert type(partner).__name__ == "A2AAgent"
        assert partner.agent_card == CARD_HOST
        assert partner.auth == "none"
        # The other placements are untouched by this.
        assert type(registry["intake"]).__name__ == "AgentCoreRuntimeAgent"


# ---------------------------------------------------------------------------
# Discovery: the Agent Card
# ---------------------------------------------------------------------------

def test_the_card_is_fetched_from_the_well_known_path():
    _, wire = run(REMOTE_WF, Wire(send=ARTIFACT_OK))
    assert wire.of("card")[0]["url"] == f"{CARD_HOST}/.well-known/agent-card.json"


def test_a_card_url_given_in_full_is_used_as_given():
    """Either form is reasonable to put in config, so both are accepted rather than
    one silently becoming `…/agent-card.json/.well-known/agent-card.json`."""
    card = f"{CARD_HOST}/custom/card.json"
    _, wire = run(remote_wf(agentCard=card), Wire(send=ARTIFACT_OK))
    assert wire.of("card")[0]["url"] == card


def test_the_rpc_endpoint_comes_from_the_card_not_from_the_configured_url():
    """The card is the discovery document. An agent may serve its RPC endpoint on a
    different host from its card, and this is how it says so — assuming the card's own
    origin would send every request to the wrong place."""
    elsewhere = "https://rpc.partner.example/v1"
    _, wire = run(REMOTE_WF, Wire(card={"url": elsewhere}, send=ARTIFACT_OK))
    assert wire.of("message/send")[0]["url"] == elsewhere


def test_supported_interfaces_preference_order_is_honoured():
    """`supportedInterfaces` is ordered by the agent's preference, and the spec says
    take the first one we can speak — not the first one listed."""
    card = {"url": "https://ignored.example/rpc",
            "supportedInterfaces": [
                {"transport": "GRPC", "url": "https://grpc.partner.example"},
                {"transport": "JSONRPC", "url": RPC_URL},
            ]}
    _, wire = run(REMOTE_WF, Wire(card=card, send=ARTIFACT_OK))
    assert wire.of("message/send")[0]["url"] == RPC_URL


def test_a_minimal_card_falls_back_to_the_configured_base_url():
    """A card with no url at all still works, so a bare-bones agent is integrable."""
    _, wire = run(REMOTE_WF, Wire(card={"name": "Partner"}, send=ARTIFACT_OK))
    assert wire.of("message/send")[0]["url"] == CARD_HOST


def test_an_unreachable_card_fails_with_where_it_looked():
    err = urllib.error.URLError("nodename nor servname provided")
    with pytest.raises(Exception, match="could not read the Agent Card for 'partner'"):
        run(REMOTE_WF, Wire(raise_on={"card": err}))


def test_a_card_that_is_not_an_object_is_rejected():
    with pytest.raises(Exception, match="is not a JSON object"):
        run(REMOTE_WF, Wire(card=["not", "a", "card"]))


# ---------------------------------------------------------------------------
# Placement validation, at load — before anything is spent
# ---------------------------------------------------------------------------

def test_a_remote_agent_needs_exactly_one_of_agent_card_or_source():
    """A URL is not always knowable at commit time: an external partner's card is a
    stable value you write down, while a framework-deployed agent's Function URL is
    generated by the deploy. Both set is ambiguous; neither leaves the agent with
    nowhere to call. Same rule as a `tools` entry's lambdaArn/source."""
    neither = remote_wf()
    del neither["agents"]["partner"]["agentCard"]
    with pytest.raises(ValueError, match=r"EXACTLY ONE.*Got neither"), workflow(neither) as imp:
        imp("app.orchestrator.registry").load_agents()

    both = remote_wf(source="a2a_lambda")   # agentCard is already set
    with pytest.raises(ValueError, match=r"EXACTLY ONE.*Got both"), workflow(both) as imp:
        imp("app.orchestrator.registry").load_agents()


def test_source_is_a_closed_list_not_an_arbitrary_string():
    """A framework-deployed agent needs infrastructure that config cannot express, so
    an unrecognised value would name something nothing deploys."""
    defn = remote_wf(source="my_own_lambda")
    del defn["agents"]["partner"]["agentCard"]
    with pytest.raises(ValueError, match="the only value is a2a_lambda"), workflow(defn) as imp:
        imp("app.orchestrator.registry").load_agents()


def test_a_source_agent_takes_its_endpoint_from_the_injected_map(fake_aws_credentials):
    """The URL the IaC discovered at deploy time, delivered the same way the dedicated
    runtime ARNs are.

    Takes `fake_aws_credentials` because `source: a2a_lambda` REQUIRES auth "sigv4"
    (registry.validate_runtimes), so reaching the endpoint means signing. Without the
    fixture this passed only while the developer happened to hold live credentials and
    failed the moment they expired — which is exactly what the fixture's own docstring
    warns about, and it was missing here."""
    defn = remote_wf(source="a2a_lambda", auth="sigv4")
    del defn["agents"]["partner"]["agentCard"]
    with workflow(defn) as imp:
        mod = imp("app.common.a2a_agent")
        mod._ENDPOINTS = {"partner": "https://fn.lambda-url.us-east-1.on.aws"}
        mod.urllib.request.urlopen = wire = Wire(card={"name": "P"}, send=ARTIFACT_OK)
        agent = imp("app.orchestrator.registry").load_agents()["partner"]
        asyncio.run(agent.run(Ctx()))
    assert wire.of("card")[0]["url"] == (
        "https://fn.lambda-url.us-east-1.on.aws/.well-known/agent-card.json")


def test_a_source_agent_with_no_injected_endpoint_says_so(fake_aws_credentials):
    """Which is what a customer sees if they add the agent but skip the deploy that
    creates it — a clearer failure than a request to the empty string."""
    defn = remote_wf(source="a2a_lambda", auth="sigv4")
    del defn["agents"]["partner"]["agentCard"]
    with workflow(defn) as imp:
        mod = imp("app.common.a2a_agent")
        mod._ENDPOINTS = {}
        mod.urllib.request.urlopen = Wire(send=ARTIFACT_OK)
        agent = imp("app.orchestrator.registry").load_agents()["partner"]
        with pytest.raises(Exception, match="has no endpoint"):
            asyncio.run(agent.run(Ctx()))


@pytest.mark.parametrize("bad", ["a2A", "remote", "dedicated_runtime", "A2A"])
def test_an_unknown_runtime_is_rejected_rather_than_silently_becoming_main(bad):
    """The whole placement switch is one string, so a typo is cheap to make. Untreated,
    it falls through to "main" and dies on `ModuleNotFoundError: app.subagents.partner`
    — an error about a missing module, for a config problem about a URL.

    Case-sensitive on purpose: "A2A" is rejected rather than accepted, so there is one
    spelling of each placement in every config and in every doc. An ABSENT or empty
    `runtime` does mean "main", which is the normal default and not a typo."""
    with pytest.raises(ValueError, match="valid values are main, dedicated, a2a"), \
            workflow(remote_wf(runtime=bad)) as imp:
        imp("app.orchestrator.registry").load_agents()


# ---------------------------------------------------------------------------
# The request we put on the wire
# ---------------------------------------------------------------------------

def test_the_send_is_jsonrpc_2_0_message_send_with_a_text_part():
    """Asserted field by field against the published shape. A request that is subtly
    wrong looks exactly like one that works until a real partner rejects it."""
    _, wire = run(REMOTE_WF, Wire(send=ARTIFACT_OK))
    body = wire.of("message/send")[0]["body"]
    assert body["jsonrpc"] == "2.0"
    assert body["method"] == "message/send"
    assert body["id"]
    message = body["params"]["message"]
    assert message["role"] == "user"
    assert message["messageId"]
    assert [p["kind"] for p in message["parts"]] == ["text"]


def test_the_remote_agent_receives_the_request_upstream_outputs_and_feedback():
    """The same inputs an in-process agent reads. Without the upstream outputs a
    remote agent placed after other agents is reasoning from the topic alone."""
    ctx = Ctx(outputs={"intake": '{"objective": "assess"}', "partner": "stale"},
              feedback="focus on affordability")
    _, wire = run(REMOTE_WF, Wire(send=ARTIFACT_OK), ctx)
    task = json.loads(wire.of("message/send")[0]["body"]["params"]["message"]["parts"][0]["text"])
    assert task["request"] == "assess this applicant"
    assert task["producing"] == "risk-assessment"      # from `produces` in config
    assert task["reviewerFeedback"] == "focus on affordability"
    assert "intake" in task["upstreamOutputs"]
    # Its OWN previous output is not fed back to it as an input.
    assert "partner" not in task["upstreamOutputs"]


def test_a_context_id_ties_this_run_s_calls_together():
    """So a stateful remote agent can correlate a revise cycle instead of seeing
    unrelated requests."""
    _, wire = run(REMOTE_WF, Wire(send=ARTIFACT_OK))
    assert wire.of("message/send")[0]["body"]["params"]["contextId"] == "sess-1-partner"


def test_the_protocol_version_is_declared():
    """So a version-aware agent can reject us explicitly rather than misread us."""
    _, wire = run(REMOTE_WF, Wire(send=ARTIFACT_OK))
    assert wire.of("message/send")[0]["headers"]["a2a-version"]


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

def test_no_auth_sends_no_authorization_header():
    _, wire = run(REMOTE_WF, Wire(send=ARTIFACT_OK))
    assert "authorization" not in wire.of("message/send")[0]["headers"]


def test_bearer_auth_uses_the_injected_token_and_never_config():
    """The token is a credential for someone else's service, so it arrives as an env
    var from the IaC. workflow.json is committed."""
    with workflow(remote_wf(auth="bearer")) as imp:
        mod = imp("app.common.a2a_agent")
        mod._TOKENS = {"partner": "t0k3n"}
        mod.urllib.request.urlopen = wire = Wire(send=ARTIFACT_OK)
        agent = imp("app.orchestrator.registry").load_agents()["partner"]
        asyncio.run(agent.run(Ctx()))
    assert wire.of("message/send")[0]["headers"]["authorization"] == "Bearer t0k3n"


def test_bearer_auth_with_no_token_fails_before_any_request():
    """Rather than calling their agent unauthenticated and reporting their 401 — which
    tells the operator nothing about the cause being on our side."""
    with workflow(remote_wf(auth="bearer")) as imp:
        mod = imp("app.common.a2a_agent")
        mod._TOKENS = {}
        mod.urllib.request.urlopen = wire = Wire(send=ARTIFACT_OK)
        agent = imp("app.orchestrator.registry").load_agents()["partner"]
        with pytest.raises(Exception, match="no token was injected"):
            asyncio.run(agent.run(Ctx()))
    assert wire.requests == []


@pytest.fixture()
def fake_aws_credentials(monkeypatch):
    """Dummy credentials for the signing tests.

    Set here rather than taken from the environment, because the promise of this suite
    is that it needs no AWS credentials — and a signing test that silently depends on
    ambient ones passes on a developer's laptop and fails in CI. Signing is pure
    arithmetic over the request, so fake keys exercise it exactly as real ones would.
    """
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)


def test_sigv4_auth_signs_the_request_with_the_runtimes_own_role(fake_aws_credentials):
    """The best-secured remote agent has no token to steal and no public endpoint: an
    AWS-hosted one behind IAM. Nothing in config, nothing to rotate. Asserted on the
    signature headers, because "it didn't crash" would pass unsigned."""
    with workflow(remote_wf(auth="sigv4", agentCard=LAMBDA_URL)) as imp:
        mod = imp("app.common.a2a_agent")
        mod.urllib.request.urlopen = wire = Wire(card={"url": LAMBDA_URL}, send=ARTIFACT_OK)
        agent = imp("app.orchestrator.registry").load_agents()["partner"]
        asyncio.run(agent.run(Ctx()))
    for req in (wire.of("card")[0], wire.of("message/send")[0]):
        auth = req["headers"]["authorization"]
        assert auth.startswith("AWS4-HMAC-SHA256 ")
        assert "SignedHeaders=" in auth and "Signature=" in auth
        # Signed for the right service, and over the body — a Function URL rejects a
        # signature that covered only the headers.
        assert "/lambda/aws4_request" in auth
        assert req["headers"].get("x-amz-date")


def test_sigv4_uses_no_bearer_token_at_all(fake_aws_credentials):
    with workflow(remote_wf(auth="sigv4", agentCard=LAMBDA_URL)) as imp:
        mod = imp("app.common.a2a_agent")
        mod._TOKENS = {"partner": "should-be-ignored"}
        mod.urllib.request.urlopen = wire = Wire(card={"url": LAMBDA_URL}, send=ARTIFACT_OK)
        asyncio.run(imp("app.orchestrator.registry").load_agents()["partner"].run(Ctx()))
    assert "should-be-ignored" not in wire.of("message/send")[0]["headers"]["authorization"]


def test_oauth2_auth_uses_the_existing_per_agent_identity_feature():
    """Reuses `agentcore.identity.outbound`, so an OAuth-protected remote agent needs
    no new config concept and no long-lived secret anywhere."""
    defn = remote_wf(auth="oauth2",
                     agentcore={"identity": {"outbound": ["partner-oauth"]}})
    _, wire = run(defn, Wire(send=ARTIFACT_OK), Ctx(token="minted"))
    assert wire.of("message/send")[0]["headers"]["authorization"] == "Bearer minted"


# ---------------------------------------------------------------------------
# Reading the answer
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("send,expected", [
    # A Task with artifacts — the normal case.
    (ARTIFACT_OK, "risk: low"),
    # Several artifacts, several parts: all of it, in order.
    ({"result": {"id": "t1", "status": {"state": "completed"}, "artifacts": [
        {"parts": [{"kind": "text", "text": "one"}, {"kind": "text", "text": "two"}]},
        {"parts": [{"kind": "text", "text": "three"}]}]}}, "one\ntwo\nthree"),
    # A bare Message reply, with no task at all.
    ({"result": {"messageId": "m1", "role": "agent",
                 "parts": [{"kind": "text", "text": "answered inline"}]}}, "answered inline"),
    # A Task whose answer is in its final status message rather than an artifact.
    ({"result": {"id": "t1", "status": {"state": "completed", "message": {
        "parts": [{"kind": "text", "text": "from the status"}]}}}}, "from the status"),
    # The legacy part discriminator.
    ({"result": {"id": "t1", "status": {"state": "completed"},
                 "artifacts": [{"parts": [{"type": "text", "text": "legacy kind"}]}]}},
     "legacy kind"),
    # No discriminator at all, just text.
    ({"result": {"id": "t1", "status": {"state": "completed"},
                 "artifacts": [{"parts": [{"text": "bare part"}]}]}}, "bare part"),
    # A structured part is content too, and is kept rather than dropped.
    ({"result": {"id": "t1", "status": {"state": "completed"}, "artifacts": [
        {"parts": [{"kind": "data", "data": {"score": 7}}]}]}}, '{"score": 7}'),
    # A structured part using the LEGACY discriminator. This is the combination that
    # `kind` alone gets wrong: with no `kind`, a data part defaults to being read as
    # text, finds no `text` field, and is silently dropped — so an agent that answers
    # only in structured data looks like it answered nothing.
    ({"result": {"id": "t1", "status": {"state": "completed"}, "artifacts": [
        {"parts": [{"type": "data", "data": {"score": 7}}]}]}}, '{"score": 7}'),
    # The enum-style terminal state.
    ({"result": {"id": "t1", "status": {"state": "TASK_STATE_COMPLETED"},
                 "artifacts": [{"parts": [{"kind": "text", "text": "enum state"}]}]}},
     "enum state"),
])
def test_the_answer_is_found_wherever_the_protocol_allows_it(send, expected):
    """On a workflow with no `produces`, so what comes back is what was sent. The
    envelope stamping that a `produces` agent gets is asserted separately below."""
    out, _ = run(no_contract_wf(), Wire(send=send))
    assert out == expected


def test_artifacts_win_over_the_status_message():
    """The artifact is the deliverable; the status message is usually progress
    narration, so preferring the narration would return the wrong thing."""
    send = {"result": {"id": "t1", "artifacts": [{"parts": [{"kind": "text", "text": "the asset"}]}],
                       "status": {"state": "completed",
                                  "message": {"parts": [{"kind": "text", "text": "done!"}]}}}}
    out, _ = run(REMOTE_WF, Wire(send=send))
    assert out == "the asset"


def test_a_completed_task_with_no_content_fails_rather_than_returning_empty():
    """An empty string would flow into every downstream agent's inputs looking exactly
    like a real finding of nothing — the reason ModelOutputUnusable exists too."""
    send = {"result": {"id": "t1", "status": {"state": "completed"}, "artifacts": []}}
    with pytest.raises(Exception, match="returned no text content"):
        run(REMOTE_WF, Wire(send=send))


# ---------------------------------------------------------------------------
# A remote agent is a first-class asset producer
# ---------------------------------------------------------------------------
#
# The remote agent supplies the CONTENT; the framework supplies the ENVELOPE. That
# split is not a convenience — the envelope is bookkeeping about THIS run (which
# version, which step id, which upstream assets) and the remote cannot know any of it.
# Asked to invent `version`, it gets it wrong on every re-run, silently, because a
# wrong integer still validates.
#
# What this buys: `synthesis.upstream_context` collects `assetId` off every upstream
# output and passes the ids to the model for claim tracing. A remote reply with no
# assetId went in as an unattributable block — a report could mention it in prose but
# never cite it. The last test in this section is that payoff, asserted end to end.


def json_reply(payload: dict) -> dict:
    """A completed task whose artifact carries `payload` as JSON text."""
    return {"result": {"id": "t1", "status": {"state": "completed"}, "artifacts": [
        {"parts": [{"kind": "text", "text": json.dumps(payload)}]}]}}


def test_a_structured_reply_is_stamped_with_the_envelope_the_framework_owns():
    ctx = Ctx(outputs={"intake": '{"title": "Applicant 42"}'})
    out, _ = run(REMOTE_WF, Wire(send=json_reply({"verdict": "low risk"})), ctx)
    asset = json.loads(out)
    # The remote's content, untouched.
    assert asset["verdict"] == "low risk"
    # The envelope, all of it from config and run state — never from the remote.
    assert asset["assetType"] == "risk-assessment"        # `produces`
    assert asset["createdByAgent"] == "partner"           # the id THIS workflow gave it
    assert asset["version"] == 1
    assert asset["status"] == "in-review"
    assert asset["assetId"] == "asset-risk-assessment-applicant-42-v1"
    assert asset["createdAt"]


def test_a_prose_reply_is_left_exactly_as_the_agent_wrote_it():
    """A remote reviewer that answers in sentences is a legitimate remote agent. Its
    text is the deliverable, and wrapping it in a JSON envelope would put a wall of
    escaped prose into the timeline and into every downstream prompt."""
    send = {"result": {"id": "t1", "status": {"state": "completed"}, "artifacts": [
        {"parts": [{"kind": "text", "text": "Data residency is unaddressed."}]}]}}
    out, _ = run(REMOTE_WF, Wire(send=send))
    assert out == "Data residency is unaddressed."


def test_an_agent_that_declares_no_produces_is_never_wrapped():
    """`produces` is what names the contract, so with no `produces` there is no asset
    to build — and a remote agent used as a plain step is a normal thing to configure."""
    out, _ = run(no_contract_wf(), Wire(send=json_reply({"verdict": "low risk"})))
    assert json.loads(out) == {"verdict": "low risk"}


def test_the_remotes_own_fields_are_not_overwritten():
    """A contract-aware partner may send its own envelope fields. Filling gaps is the
    framework's job; overruling a remote agent about its own output is not."""
    send = json_reply({"assetId": "partner-own-id", "executiveSummary": "theirs",
                       "verdict": "low risk"})
    out, _ = run(REMOTE_WF, Wire(send=send), Ctx(outputs={"intake": '{"title": "X"}'}))
    asset = json.loads(out)
    assert asset["assetId"] == "partner-own-id"
    assert asset["executiveSummary"] == "theirs"
    # And the fields it did NOT send are still stamped.
    assert asset["createdByAgent"] == "partner"


def test_the_version_counts_this_agents_runs_and_the_remote_is_not_asked():
    """The field the remote provably cannot know. On a revise the previous asset is in
    state, so this run is v2 — and the remote sent no version at all either time."""
    prior = json.dumps({"assetId": "asset-risk-assessment-x-v1", "version": 1,
                        "verdict": "stale"})
    ctx = Ctx(outputs={"intake": '{"title": "X"}', "partner": prior},
              feedback="look again at affordability")
    out, wire = run(REMOTE_WF, Wire(send=json_reply({"verdict": "fresh"})), ctx)
    asset = json.loads(out)
    assert asset["version"] == 2
    assert asset["assetId"].endswith("-v2")
    # Nothing about the version was asked of them, so nothing about it can be wrong.
    sent = wire.of("message/send")[0]["body"]["params"]["message"]["parts"][0]["text"]
    assert "version" not in json.loads(sent)


def test_the_asset_records_which_upstream_assets_it_was_built_from():
    """`sourceAssetIds` is asset-level provenance, and only this side knows the ids:
    the remote was handed the upstream CONTENT, not this graph's bookkeeping."""
    ctx = Ctx(outputs={"intake": json.dumps({"title": "X", "assetId": "asset-brief-x-v1"})})
    out, _ = run(REMOTE_WF, Wire(send=json_reply({"verdict": "low risk"})), ctx)
    assert json.loads(out)["sourceAssetIds"] == ["asset-brief-x-v1"]


def test_an_upstream_with_no_asset_id_contributes_nothing_rather_than_a_blank():
    """An upstream agent may legitimately answer in prose. A "" in sourceAssetIds
    would be a citation to nothing."""
    ctx = Ctx(outputs={"intake": "just some prose"})
    out, _ = run(REMOTE_WF, Wire(send=json_reply({"verdict": "low risk"})), ctx)
    assert json.loads(out)["sourceAssetIds"] == []


def test_a_downstream_agent_can_now_trace_a_claim_to_the_remote_agents_asset():
    """The payoff, through the code a synthesis agent actually runs.

    `upstream_context` appends an assetId only when the upstream output has one, and
    puts it in the prompt header so the model can cite it. Before the envelope, a
    remote agent's output reached this function with no assetId: it still went into the
    prompt, but as a block with nothing to cite, so rule 2 ("trace every material claim
    to the assetId(s) above") had nothing to point at."""
    ctx = Ctx(outputs={"intake": '{"title": "Applicant 42"}'})
    out, _ = run(REMOTE_WF, Wire(send=json_reply({"verdict": "low risk"})), ctx)

    ctx.state["outputs"]["partner"] = out
    with workflow(REMOTE_WF) as imp:
        context, ids = imp("app.subagents._shared.synthesis").upstream_context(
            ctx, ["intake", "partner"])
    assert "asset-risk-assessment-applicant-42-v1" in ids
    assert "assetId: asset-risk-assessment-applicant-42-v1" in context
    assert "low risk" in context


# ---------------------------------------------------------------------------
# The task lifecycle
# ---------------------------------------------------------------------------

def test_a_working_task_is_polled_until_it_completes():
    """A2A models work as a Task precisely so it can take a while. Reading only the
    first response would return nothing for any agent slower than one round trip."""
    send = {"result": {"id": "t1", "status": {"state": "submitted"}}}
    polls = [
        {"result": {"id": "t1", "status": {"state": "working"}}},
        {"result": {"id": "t1", "status": {"state": "completed"},
                    "artifacts": [{"parts": [{"kind": "text", "text": "eventually"}]}]}},
    ]
    out, wire = run(REMOTE_WF, Wire(send=send, polls=polls))
    assert out == "eventually"
    gets = wire.of("tasks/get")
    assert len(gets) == 2
    assert gets[0]["body"]["params"]["id"] == "t1"


@pytest.mark.parametrize("state", ["failed", "canceled", "rejected", "TASK_STATE_FAILED"])
def test_a_terminal_failure_state_fails_the_run(state):
    send = {"result": {"id": "t1", "status": {"state": state, "message": {
        "parts": [{"kind": "text", "text": "applicant not found"}]}}}}
    with pytest.raises(Exception, match="applicant not found"):
        run(REMOTE_WF, Wire(send=send))


@pytest.mark.parametrize("state", ["input-required", "auth-required"])
def test_a_task_waiting_on_a_human_fails_immediately_instead_of_polling(state):
    """These states never advance on their own, so polling them burns the entire
    budget to reach the same answer. The message points at the mechanism this
    framework actually has for human involvement."""
    send = {"result": {"id": "t1", "status": {"state": state}}}
    with pytest.raises(Exception, match="cannot supply it mid-task"):
        run(REMOTE_WF, Wire(send=send))


def test_a_task_that_never_finishes_gives_up_with_the_last_state_seen():
    """Unbounded, a task stuck in `working` would hold the whole workflow until the
    runtime's own 8-hour ceiling. The budget is config, so this run sets it to a tenth
    of a second and the agent answers "working" indefinitely."""
    defn = json.loads(json.dumps(REMOTE_WF))
    defn["orchestrator"]["a2aInvoke"] = {"timeoutSeconds": 5, "pollIntervalSeconds": 0.02,
                                         "maxPollSeconds": 0.1}
    send = {"result": {"id": "t1", "status": {"state": "working"}}}
    with pytest.raises(Exception, match=r"did not finish task t1.*last state: working"):
        run(defn, Wire(send=send, stuck=True))


# ---------------------------------------------------------------------------
# Failure surfacing
# ---------------------------------------------------------------------------

def test_a_jsonrpc_error_is_surfaced_with_its_code_and_message():
    send = {"error": {"code": -32052, "message": "Validation error - Invalid request data"}}
    with pytest.raises(Exception, match="Validation error"):
        run(REMOTE_WF, Wire(send=send))


def test_an_http_error_body_is_read_for_the_real_reason():
    """The spec says deliver RPC errors over HTTP 200, but real servers — AgentCore
    Runtime among them — return the true status code with the error in the body.
    Giving up on the status line alone replaces the reason with a bare number."""
    body = json.dumps({"error": {"code": -32054, "message": "Session operation in progress"}})
    err = urllib.error.HTTPError(RPC_URL, 409, "Conflict", {}, io.BytesIO(body.encode()))
    with pytest.raises(Exception, match=r"HTTP 409.*Session operation in progress"):
        run(REMOTE_WF, Wire(send=ARTIFACT_OK, raise_on={"message/send": err}))


def test_an_http_error_with_an_unreadable_body_still_reports_the_status():
    err = urllib.error.HTTPError(RPC_URL, 503, "Unavailable", {}, io.BytesIO(b"<html>nope"))
    with pytest.raises(Exception, match="HTTP 503"):
        run(REMOTE_WF, Wire(send=ARTIFACT_OK, raise_on={"message/send": err}))


def test_a_reply_with_no_result_object_fails():
    with pytest.raises(Exception, match="returned no result object"):
        run(REMOTE_WF, Wire(send={"result": "just a string"}))


def test_the_failure_type_is_its_own_class():
    """So a remote agent's failure is distinguishable from a model or tool outage: the
    cause is outside this deployment entirely."""
    with workflow(REMOTE_WF) as imp:
        errors = imp("app.common.errors")
        mod = imp("app.common.a2a_agent")
        mod.urllib.request.urlopen = Wire(card={"name": "x"},
                                          send={"result": {"status": {"state": "failed"}}})
        agent = imp("app.orchestrator.registry").load_agents()["partner"]
        with pytest.raises(errors.RemoteAgentUnavailable):
            asyncio.run(agent.run(Ctx()))
        assert issubclass(errors.RemoteAgentUnavailable, errors.DependencyUnavailable)


# ---------------------------------------------------------------------------
# Transport safety
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("card", ["http://agents.partner.example",
                                  "file:///etc/passwd",
                                  "ftp://agents.partner.example"])
def test_a_non_https_agent_card_is_rejected_before_anything_is_sent(card):
    """The request may carry a bearer token, so plaintext is not an option — and
    `file://` would make urllib read a local path instead of making a request. Caught
    at load, so it is a container that will not start rather than a token on the wire."""
    with pytest.raises(ValueError, match="must be an https:// URL"), workflow(remote_wf(agentCard=card)) as imp:
        imp("app.orchestrator.registry").load_agents()


def test_an_https_card_pointing_at_a_plaintext_rpc_endpoint_is_also_rejected():
    """The card is data from another party, so its contents are checked too — the
    config-time check on `agentCard` says nothing about where the card then points."""
    with pytest.raises(Exception, match="must be an https:// URL"):
        run(REMOTE_WF, Wire(card={"url": "http://rpc.partner.example"}, send=ARTIFACT_OK))


def test_the_configured_timeout_reaches_the_request():
    _, wire = run(REMOTE_WF, Wire(send=ARTIFACT_OK))
    assert wire.of("message/send")[0]["timeout"] == 5.0   # a2aInvoke.timeoutSeconds


# ---------------------------------------------------------------------------
# It is an ordinary node in the topology
# ---------------------------------------------------------------------------

def test_a_remote_agent_is_wired_like_any_other_node():
    """The payoff of keeping the Agent interface uniform: gates, branch, re-run and
    version history all work on a remote agent with no special case."""
    defn = json.loads(json.dumps(REMOTE_WF))
    defn["agents"]["report"] = {"name": "Report", "runtime": "dedicated", "maxTokens": 1000}
    defn["steps"] = [
        {"agent": "intake", "hitl": True,
         "branch": {"when": [{"field": "skip", "equals": True, "goto": "report"}]}},
        {"agent": "partner", "hitl": True},
        {"agent": "report"},
    ]
    with workflow(defn) as imp:
        graph = imp("app.orchestrator.graph_builder").build_graph()
        nodes = set(graph.get_graph().nodes)
        assert {"partner", "partner_gate", "intake_branch"} <= nodes
        # And it can be rewound to, and branched past, like anything else.
        gb = imp("app.orchestrator.graph_builder")
        assert gb.rerun_plan("partner")["step_agents"] == ["partner"]
        assert gb._bypassed_agents(0, "report") == ["partner"]


def test_a_remote_agent_is_upstream_of_the_agents_after_it():
    """So a later synthesis agent reads the partner's output automatically, which is
    the whole reason to put it in the topology rather than call it from inside an
    agent."""
    with workflow(REMOTE_WF) as imp:
        assert imp("app.common.config").upstream_of("partner") == ["intake"]


# ---------------------------------------------------------------------------
# The two halves, against each other
# ---------------------------------------------------------------------------
#
# Everything above stubs the wire and asserts the client against the protocol as
# WRITTEN DOWN. That catches a client that drifts from the spec, but not the more
# likely mistake: two halves of this repo that each look right and do not interoperate.
# The shipped A2A server (`a2a_lambda/handler.py`) is the other half, so this drives
# the real client against the real handler in-process — no AWS, no network, nothing
# stubbed between them except Bedrock itself.
#
# It already earned its place. The handler first selected its skill from a QUERY
# STRING, which made its own Agent Card advertise `https://host/?skill=x`; a client
# appending `/.well-known/agent-card.json` to that produces a URL where the path
# follows the query, which resolves to nothing. Both halves passed their own tests.


def a2a_server():
    """The shipped A2A server module, loaded under its OWN name.

    NOT via `sys.path` + `import handler`: `bff/handler.py` is also importable as
    `handler`, and conftest already puts `bff/` on the path — so inserting
    `a2a_lambda/` ahead of it made every BFF test import this module instead and 29 of
    them fail with AttributeError. A file-location import keeps the two apart.
    """
    import importlib.util

    if "a2a_lambda_handler" in sys.modules:
        return sys.modules["a2a_lambda_handler"]
    spec = importlib.util.spec_from_file_location(
        "a2a_lambda_handler", ORCH_ROOT / "a2a_lambda" / "handler.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules["a2a_lambda_handler"] = module
    spec.loader.exec_module(module)
    return module


def _bedrock_returning(text):
    return type("B", (), {"converse": lambda self, **_k: {
        "output": {"message": {"content": [{"text": text}]}}}})()


def round_trip(answer, *, skill="compliance", outputs=None):
    """Run the client against the handler, both real, and return (output, requests)."""
    from urllib.parse import urlsplit

    server = a2a_server()
    server._bedrock = _bedrock_returning(answer)
    seen: list[tuple[str, str]] = []

    def urlopen(request, timeout=None):
        url = urlsplit(request.full_url)
        seen.append((request.get_method(), url.path))
        out = server.lambda_handler({
            "requestContext": {"http": {"method": request.get_method()}},
            "rawPath": url.path,
            "headers": {"Host": url.netloc},
            "body": request.data.decode() if request.data else "",
        })
        assert out["statusCode"] == 200, out
        return _Resp(json.loads(out["body"]))

    defn = remote_wf(agentCard=f"{CARD_HOST}/{skill}")
    with workflow(defn) as imp:
        mod = imp("app.common.a2a_agent")
        mod.urllib.request.urlopen = urlopen
        agent = imp("app.orchestrator.registry").load_agents()["partner"]
        return asyncio.run(agent.run(Ctx(outputs=outputs))), seen


def test_the_shipped_server_and_the_client_interoperate():
    answer = '{"reviewer": "compliance", "verdict": "Data residency is unaddressed."}'
    out, seen = round_trip(answer, outputs={"intake": '{"objective": "build a pipeline"}'})
    # The remote's content arrives intact, inside the envelope the framework stamps.
    asset = json.loads(out)
    assert asset["reviewer"] == "compliance"
    assert asset["verdict"] == "Data residency is unaddressed."
    assert asset["assetType"] == "risk-assessment"
    # The card was fetched from the well-known path under the skill, then the RPC went
    # to the url the CARD returned — not to the configured base.
    assert seen[0] == ("GET", "/compliance/.well-known/agent-card.json")
    assert seen[1] == ("POST", "/compliance")


def test_each_shipped_skill_is_reachable_and_distinct():
    """Several remote agents backed by one function, which is the point of the
    path-based skill: different agents, not the same one called four times.

    Driven off `SKILLS` rather than a list written here, so adding a skill to the
    handler is covered the moment it exists instead of when somebody remembers to
    extend this test."""
    server = a2a_server()
    assert len(server.SKILLS) > 1
    for skill in server.SKILLS:
        out, seen = round_trip(f'{{"reviewer": "{skill}"}}', skill=skill)
        assert json.loads(out)["reviewer"] == skill
        assert seen[1] == ("POST", f"/{skill}")
    # And they really are different prompts, not one with a label swapped.
    prompts = {k: v["prompt"] for k, v in server.SKILLS.items()}
    assert len(set(prompts.values())) == len(prompts)


@pytest.mark.parametrize("skill,contract,content", [
    ("analysis", "Analysis", {
        "executiveSummary": "A layered design is the load-bearing choice.",
        "summary": "The three findings converge on a layered architecture.",
        "rationale": "Documentation and web search agreed; the cost finding was set aside.",
        "claims": [{"statement": "A layered split is the consensus design.",
                    "tracedToAssetIds": ["asset-brief-x-v1"], "confidence": "high"}],
        "assumptions": ["The workload is request-driven."],
        "limitations": ["No volume was supplied."],
        "sources": [{"sourceId": "s1", "sourceType": "research-finding",
                     "sourceName": "web search", "sourceAssetId": "asset-brief-x-v1"}],
    }),
    ("recommendation", "Recommendation", {
        "executiveSummary": "Settle the workload profile first.",
        "summary": "Four choices wait on one missing input.",
        "items": [{"title": "Describe the workload", "detail": "Volume and latency.",
                   "priority": "high", "rationale": "Four decisions turn on it.",
                   "tracedToAssetIds": ["asset-brief-x-v1"]}],
        "risks": ["Choosing before the profile is known."],
        "assumptions": [],
        "sources": [{"sourceId": "s1", "sourceType": "analysis", "sourceName": "analysis"}],
    }),
])
def test_a_remote_agents_asset_validates_against_the_contract_it_produces(
        skill, contract, content):
    """The two halves of the contract split, checked against each other.

    The stand-in is asked for CONTENT only — its `shape` has no assetId, no version, no
    status — and the framework stamps the envelope. Neither half is a valid asset alone,
    so the thing worth asserting is that together they satisfy the pydantic contract
    this sample's report agent reads.

    A framework CANNOT do this check at run time for a remote agent: there is no local
    contract class for code somebody else owns, which is the real cost of the trust
    boundary (see A2AAgent._as_asset). It can be done HERE, against the stand-in we do
    ship, and that is what makes the shape in handler.py a claim rather than a hope.
    """
    import importlib

    contracts = importlib.import_module("app.subagents._shared.contracts")
    defn = remote_wf(agentCard=f"{CARD_HOST}/{skill}", produces=skill)
    server = a2a_server()
    server._bedrock = _bedrock_returning(json.dumps(content))

    from urllib.parse import urlsplit

    def urlopen(request, timeout=None):
        url = urlsplit(request.full_url)
        out = server.lambda_handler({
            "requestContext": {"http": {"method": request.get_method()}},
            "rawPath": url.path, "headers": {"Host": url.netloc},
            "body": request.data.decode() if request.data else "",
        })
        return _Resp(json.loads(out["body"]))

    ctx = Ctx(outputs={"intake": json.dumps({"title": "X", "assetId": "asset-brief-x-v1"})})
    with workflow(defn) as imp:
        mod = imp("app.common.a2a_agent")
        mod.urllib.request.urlopen = urlopen
        out = asyncio.run(imp("app.orchestrator.registry").load_agents()["partner"].run(ctx))

    # `extra="forbid"` on the envelope, so this fails if the shape asks for a field the
    # contract does not have OR the stamped envelope misses one it requires.
    asset = getattr(contracts, contract)(**json.loads(out))
    assert asset.asset_type == skill
    assert asset.version == 1
    assert asset.status.value == "in-review"
    assert asset.created_by_agent == "partner"
    assert asset.source_asset_ids == ["asset-brief-x-v1"]


@pytest.mark.parametrize("skill,contract", [("analysis", "Analysis"),
                                            ("recommendation", "Recommendation")])
def test_the_shape_a_contract_skill_asks_for_is_one_the_contract_accepts(skill, contract):
    """Checks the REQUEST, where the test above checks a reply.

    An agent answers the shape it was given, so a shape naming a field the contract
    forbids produces an invalid asset every single time — and nothing on the client side
    notices, because the framework has no contract class for a remote agent and does not
    validate. That is the documented cost of the boundary, and it makes this the only
    place the mismatch can be caught: at the point where we WRITE the shape down.

    It found a live one from the other direction too. `extra="forbid"` on these contracts
    means a remote agent inventing one field invalidates the whole asset: observed on a
    real run, one claim out of twenty-one carried its own `rationale`. A shape cannot
    stop that on its own, so rule 10 in the prompt says so explicitly — but it can at
    least guarantee that an agent which follows the shape EXACTLY produces something
    valid, and that is what this asserts.
    """
    import importlib

    contracts = importlib.import_module("app.subagents._shared.contracts")
    model = getattr(contracts, contract)

    def aliases(m):
        return {f.alias or name for name, f in m.model_fields.items()}

    def check(node, m, path):
        unknown = set(node) - aliases(m)
        assert not unknown, f"{path}: {sorted(unknown)} not in {m.__name__}"
        for key, value in node.items():
            field = next((f for n, f in m.model_fields.items() if (f.alias or n) == key), None)
            # Descend into a nested contract: `claims: list[TracedClaim]`,
            # `items: list[RecommendationItem]`, `sources: list[Source]`.
            inner = getattr(field.annotation, "__args__", ())
            nested = next((a for a in inner if hasattr(a, "model_fields")), None)
            if nested and isinstance(value, list) and value and isinstance(value[0], dict):
                check(value[0], nested, f"{path}.{key}[]")

    check(json.loads(a2a_server().SKILLS[skill]["shape"]), model, skill)


def test_a_card_the_server_generates_is_one_the_client_can_follow():
    """The regression this file exists for. Asserted on the URL the client would
    build from the card, because that is where a query string breaks and a path
    does not."""
    server = a2a_server()
    card = server.agent_card("https://host.example/resilience", "resilience")
    base = card["url"].rstrip("/")
    assert f"{base}/.well-known/agent-card.json" == (
        "https://host.example/resilience/.well-known/agent-card.json")
    assert card["supportedInterfaces"][0]["transport"] == "JSONRPC"
    assert card["protocolVersion"]


def test_the_server_reports_a_model_failure_as_a_failed_task():
    """Not as a JSON-RPC error: the request was valid, the work did not succeed. Only
    the failed-task form carries a reason the reviewer can read, and the client turns
    it into RemoteAgentUnavailable with that reason attached."""
    server = a2a_server()
    server._bedrock = type("B", (), {
        "converse": lambda self, **_k: (_ for _ in ()).throw(RuntimeError("AccessDenied"))})()
    out = server.lambda_handler({
        "requestContext": {"http": {"method": "POST"}}, "rawPath": "/compliance",
        "body": json.dumps({"jsonrpc": "2.0", "id": 1, "method": "message/send",
                            "params": {"message": {"messageId": "m1", "parts": [
                                {"kind": "text", "text": "review"}]}}}),
    })
    result = json.loads(out["body"])["result"]
    assert result["status"]["state"] == "failed"
    assert "AccessDenied" in result["status"]["message"]["parts"][0]["text"]


def test_each_skill_gets_its_own_output_budget_and_it_reaches_the_model():
    """A reviewer's verdict and a full analysis are not the same size of job. One budget
    for all of them means either the reviewers are given room they cannot use or the
    synthesis skills are cut off — and a cut-off asset across this boundary is the
    failure the client cannot see. Asserted on the inferenceConfig actually sent."""
    server = a2a_server()
    seen: dict = {}

    def converse(self, **kwargs):
        seen.update(kwargs["inferenceConfig"])
        return {"output": {"message": {"content": [{"text": "{}"}]}}}

    server._bedrock = type("B", (), {"converse": converse})()

    server.review("x", "compliance")
    assert seen["maxTokens"] == server.MAX_TOKENS       # the env default
    server.review("x", "analysis")
    assert seen["maxTokens"] == server.SKILLS["analysis"]["maxTokens"]
    assert seen["maxTokens"] > server.MAX_TOKENS


def test_every_skill_the_vocabulary_allows_is_one_this_server_implements():
    """The silent failure this prevents: `_skill()` falls back to DEFAULT_SKILL for a
    name it does not know. So a skill listed in app/vocabulary.json but missing from
    SKILLS passes every config check, deploys, and answers every request with a
    COMPLIANCE REVIEW under the name of whatever agent asked — a wrong asset that looks
    like a right one."""
    from app.common.vocabulary import A2A_LAMBDA_SKILLS

    assert set(A2A_LAMBDA_SKILLS) == set(a2a_server().SKILLS), (
        "app/vocabulary.json a2aLambdaSkills and a2a_lambda/handler.py SKILLS disagree")


def test_the_card_names_the_skill_and_credits_the_operator_separately():
    """One function serves several paths, and each path is a distinct agent as far as
    the protocol is concerned — so a single `name` for all of them would advertise the
    analysis agent as a reviewer. Who runs them is `provider`, which is where A2A puts
    it."""
    server = a2a_server()
    names = {s: server.agent_card(f"https://h.example/{s}", s)["name"] for s in server.SKILLS}
    assert len(set(names.values())) == len(names), f"cards do not distinguish skills: {names}"
    card = server.agent_card("https://h.example/analysis", "analysis")
    assert card["provider"]["organization"] == server.AGENT_NAME
    assert card["skills"][0]["id"] == "analysis"


def test_the_server_never_fabricates_an_answer():
    """The standard the rest of this sample holds. A stand-in that invented a review
    when the model was unavailable would teach exactly the wrong lesson about what a
    remote agent's output is worth."""
    server = a2a_server()
    server._bedrock = _bedrock_returning("")
    with pytest.raises(RuntimeError, match="no content"):
        server.review("anything", "compliance")


def test_sigv4_is_never_sent_to_a_non_aws_host(fake_aws_credentials):
    """The signature is made with the runtime's own role: a partner's server must not
    receive it."""
    with workflow(remote_wf(auth="sigv4")) as imp:
        mod = imp("app.common.a2a_agent")
        mod.urllib.request.urlopen = wire = Wire(send=ARTIFACT_OK)
        agent = imp("app.orchestrator.registry").load_agents()["partner"]
        with pytest.raises(mod.RemoteAgentUnavailable, match="only sent to an AWS endpoint"):
            asyncio.run(agent.run(Ctx()))
    assert not wire.of("card"), "the request was sent before the refusal"


@pytest.mark.parametrize("host", ["127.0.0.1", "169.254.169.254", "10.0.0.5", "[::1]"])
def test_an_endpoint_on_a_private_address_is_refused(host):
    from app.common import a2a_agent as mod
    with pytest.raises(mod.RemoteAgentUnavailable, match="not a public address"):
        mod._require_https(f"https://{host}/a2a", "agent 'partner' endpoint")


def test_a_private_endpoint_can_be_allowed_deliberately(monkeypatch):
    from app.common import a2a_agent as mod
    monkeypatch.setattr(mod, "_ALLOW_PRIVATE", True)
    assert mod._require_https("https://10.0.0.5/a2a", "x") == "https://10.0.0.5/a2a"
