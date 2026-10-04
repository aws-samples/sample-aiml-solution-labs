"""An agent may be authored with any agentic framework — and it still cannot dodge
governance.

Two of the four shipped research agents do their reasoning inside a framework:
`web_search` in a Strands agent, `knowledge_research` in a nested LangGraph. The
other two (`documentation_search`, `cost_research`) use no framework at all, which
is what makes the set a comparison rather than a fashion show.

The claim being tested is narrow and worth stating plainly, because it is the whole
reason the bridges exist:

    THE FRAMEWORK CHOOSES HOW AN AGENT REASONS. IT DOES NOT GET ITS OWN MODEL
    CLIENT.

Guardrails, cost and token telemetry, long-term memory injection, truncation
detection and cancellation all live in `ctx.llm` (app/common/context.py). A
framework holding its own boto3 client would lose every one of them AND STILL
REPORT SUCCESS — no error, just an agent that has quietly left the cost panel and
the guardrail behind. So these tests pin:

  * every framework's model call arrives at `ctx.llm`, with the agent's system
    prompt and the assembled evidence prompt intact,
  * the call is NAMED, because observability and AgentCore Evaluations scope by
    prompt name,
  * bookkeeping `ctx.llm` does on the way back (the truncation flag) still reaches
    the agent's asset through the framework,
  * a model failure still propagates — no framework's retry or fallback turns it
    into placeholder text,
  * offering the framework's own tools RAISES rather than being dropped, because a
    dropped tool produces a plausible answer and no way to notice,
  * and all four agents emit the same ResearchOutput contract, which is what makes
    the framework an authoring choice instead of a fork.
"""
import asyncio
import json
import threading

import pytest
from conftest import needs_agent

# ABOUT THE TWO SAMPLE AGENTS THAT REASON INSIDE STRANDS / A NESTED LANGGRAPH.
# Skipped, not deleted, when this workflow does not have those agents: the tests
# below are what makes the sample worth copying, and they are meaningless without
# their subject. A customer who keeps the agent keeps its tests.
pytestmark = needs_agent("web_search", "knowledge_research")

BRIEF = json.dumps({
    "title": "Design a Serverless Data Pipeline",
    "objective": "Choose an ingestion and storage design for a serverless pipeline.",
    "keyQuestions": ["Ordering guarantees?", "Replay?"],
})

# A well-formed research answer, as a model would return it.
GOOD = json.dumps({
    "summary": "Ingestion choice turns on ordering guarantees.",
    "findings": [{"statement": "Kinesis preserves order per shard.",
                  "classification": "sourced-fact",
                  "sourceRef": "Kinesis developer guide"}],
    "dataLimitations": [],
    "sources": [{"sourceType": "documentation", "sourceName": "Kinesis guide"}],
})

FRAMEWORK_AGENTS = ["web_search", "knowledge_research"]
#: Every research agent, framework or not. The point of running the same assertions
#: over both sets is that the contract does not know which is which.
ALL_RESEARCH_AGENTS = [*FRAMEWORK_AGENTS, "documentation_search", "cost_research"]


class Recorder:
    """A stand-in for ctx.llm that records what each framework actually sent."""

    def __init__(self, *replies, truncate_on=()):
        self.replies = list(replies) or [GOOD]
        self.calls: list[dict] = []
        self.threads: set[int] = set()
        self._truncate_on = set(truncate_on)
        self.ctx = None

    async def __call__(self, system, user, model=None, max_tokens=None, name=None):
        self.calls.append({"system": system or "", "user": user or "",
                           "name": name, "max_tokens": max_tokens})
        self.threads.add(threading.get_ident())
        index = min(len(self.calls) - 1, len(self.replies) - 1)
        if name in self._truncate_on and self.ctx is not None:
            # Exactly what the real ctx.llm does when a response hits its output
            # ceiling (app/common/context.py). Recorded here so the test can prove
            # the flag survives the trip through a third-party framework.
            self.ctx.truncated_calls.append(name)
        return self.replies[index]


def _ctx(agent_id: str, recorder: Recorder):
    """A real AgentContext for a shipped agent, with the model and tools stubbed."""
    from app.common.context import AgentContext
    from app.orchestrator.registry import build_agent_module

    agent = build_agent_module(agent_id)
    ctx = AgentContext(agent, {"topic": "serverless pipeline",
                               "outputs": {"intake": BRIEF}, "history": {}},
                       {"configurable": {"thread_id": "s-frameworks"}})
    recorder.ctx = ctx
    ctx.llm = recorder

    async def call_tool(tool_key, query):
        return "1. title: Kinesis guide\n   url: https://example.test/kinesis", "gateway"

    async def retrieve(query, doc_type=None):
        return "1. title: Internal reference\n   url: https://example.test/ref", "kb"

    async def call_tool_rows(tool_key, query=None):
        return ([{"service": "AWS Lambda", "dimension": "Lambda-GB-Second",
                  "unit": "Lambda-GB-Second", "pricePerUnit": "0.0000150000",
                  "currency": "USD", "region": "us-east-1"}],
                {"service": "service", "dimension": "dimension", "unit": "unit",
                 "price": "pricePerUnit", "currency": "currency",
                 "region": "region", "requested": "requestedAs"},
                "gateway")

    async def noop(*a, **k):
        return None

    ctx.call_tool = call_tool
    ctx.call_tool_rows = call_tool_rows
    ctx.retrieve = retrieve
    ctx.log = noop
    ctx.heartbeat = noop
    # Cedar is a separate concern with its own tests; keep this deterministic.
    ctx.policy_check = lambda action: asyncio.sleep(0, result=True)
    return agent, ctx


def _run(agent_id: str, recorder: Recorder):
    agent, ctx = _ctx(agent_id, recorder)
    return json.loads(asyncio.run(agent.run(ctx))), ctx


# ---------------------------------------------------------------------------
# The contract is the same whatever framework produced it
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("agent_id", ALL_RESEARCH_AGENTS)
def test_every_research_agent_emits_the_same_contract(agent_id):
    """Strands, a nested LangGraph and no framework at all land on ResearchOutput.

    This is the claim that makes the framework an authoring choice: whatever runs
    inside, the asset a reviewer and every downstream agent sees is identical in
    shape. If a framework could change the output contract it would be a fork of
    the workflow, not a choice of library.
    """
    asset, _ = _run(agent_id, Recorder(GOOD))
    for field in ("assetId", "version", "status", "createdAt", "createdByAgent",
                  "summary", "findings", "dataLimitations", "sources"):
        assert field in asset, f"{agent_id} asset is missing {field}"
    assert asset["createdByAgent"] == agent_id


@pytest.mark.parametrize("agent_id", FRAMEWORK_AGENTS)
def test_framework_agents_reach_the_model_only_through_ctx_llm(agent_id):
    """The whole point. Every framework's reasoning arrives at ctx.llm."""
    recorder = Recorder(GOOD)
    asset, _ = _run(agent_id, recorder)
    assert recorder.calls, f"{agent_id} produced an asset without calling ctx.llm"
    assert asset["summary"] == "Ingestion choice turns on ordering guarantees.", (
        "the asset was not built from what ctx.llm returned")


@pytest.mark.parametrize("agent_id", FRAMEWORK_AGENTS)
def test_framework_agents_cost_one_model_call_on_the_happy_path(agent_id):
    """A framework is not allowed to quietly multiply the bill.

    Each of these replaced a single ctx.llm call. A crew with more agents, or a
    graph with more nodes, costs more per run — that is a fine choice to make
    deliberately and a bad one to make by accident, so it is pinned here.
    """
    recorder = Recorder(GOOD)
    _run(agent_id, recorder)
    assert len(recorder.calls) == 1, [c["name"] for c in recorder.calls]


@pytest.mark.parametrize("agent_id", FRAMEWORK_AGENTS)
def test_framework_agents_name_their_model_call_after_themselves(agent_id):
    """Observability and AgentCore Evaluations scope by prompt name, so an
    unnamed or misnamed call disappears from both."""
    recorder = Recorder(GOOD)
    _run(agent_id, recorder)
    assert recorder.calls[0]["name"] == agent_id


@pytest.mark.parametrize("agent_id", FRAMEWORK_AGENTS)
def test_the_agents_own_system_prompt_still_reaches_the_model(agent_id):
    """A framework composes its own prompts — Strands from its system_prompt,
    CrewAI (not shipped, but the pattern is documented) from role/goal/backstory. The
    identity in app/subagents/<id>/prompts.py must survive that, or the agent
    silently becomes a generic one."""
    from app.orchestrator.registry import build_agent_module

    recorder = Recorder(GOOD)
    _run(agent_id, recorder)
    expected = build_agent_module(agent_id).system_prompt
    sent = recorder.calls[0]["system"]
    # Compared on a distinctive sentence rather than by equality: a framework is
    # entitled to wrap the prompt in its own framing, and some do.
    marker = expected.split("\n")[0]
    assert marker and marker in sent, (
        f"{agent_id}: its own system prompt did not reach the model.\n"
        f"expected to contain: {marker!r}\ngot: {sent[:300]!r}")


@pytest.mark.parametrize("agent_id", FRAMEWORK_AGENTS)
def test_the_evidence_still_reaches_the_model(agent_id):
    """Evidence is gathered by the framework-independent runner and handed to the
    model. If a framework dropped the user prompt, the agent would answer from the
    system prompt alone and look fine doing it."""
    recorder = Recorder(GOOD)
    _run(agent_id, recorder)
    assert "APPROVED REQUEST" in recorder.calls[0]["user"]


@pytest.mark.parametrize("agent_id", FRAMEWORK_AGENTS)
def test_truncation_bookkeeping_survives_the_framework(agent_id):
    """ctx.llm's truncation flag has to come back OUT through the framework.

    This is the bookkeeping most easily lost: `extract_json` repairs a cut-off
    response into a valid asset, so the only remaining evidence that anything was
    lost is the flag ctx.llm set. If a framework swallowed it, a reviewer would
    approve a complete-looking asset with its tail missing.
    """
    recorder = Recorder(GOOD, truncate_on=(agent_id,))
    asset, ctx = _run(agent_id, recorder)
    assert ctx.truncated_calls == [agent_id]
    assert any("output token limit" in limit for limit in asset["dataLimitations"]), (
        asset["dataLimitations"])


@pytest.mark.parametrize("agent_id", FRAMEWORK_AGENTS)
def test_a_model_failure_is_not_turned_into_placeholder_text(agent_id):
    """Frameworks like to retry and to fall back. Neither may invent output here:
    a fabricated finding is indistinguishable from a real one downstream."""
    from app.common.errors import ModelUnavailable

    class Failing(Recorder):
        async def __call__(self, *a, **k):
            await super().__call__(*a, **k)
            raise ModelUnavailable("bedrock is down")

    recorder = Failing(GOOD)
    agent, ctx = _ctx(agent_id, recorder)
    with pytest.raises(ModelUnavailable):
        asyncio.run(agent.run(ctx))


# ---------------------------------------------------------------------------
# Strands bridge
# ---------------------------------------------------------------------------

def test_strands_passes_system_and_user_through_unchanged():
    from app.subagents._shared.strands_bridge import strands_thinker

    recorder = Recorder("ANSWER")
    _, ctx = _ctx("web_search", recorder)
    out = asyncio.run(strands_thinker(ctx)("SYSTEM HERE", "USER HERE"))
    assert out.strip() == "ANSWER"
    assert recorder.calls[0]["system"] == "SYSTEM HERE"
    assert recorder.calls[0]["user"] == "USER HERE"


def test_strands_call_name_can_be_overridden():
    """An agent issuing several distinct prompts needs to name each one."""
    from app.subagents._shared.strands_bridge import strands_thinker

    recorder = Recorder("ANSWER")
    _, ctx = _ctx("web_search", recorder)
    asyncio.run(strands_thinker(ctx, name="web_search.second_pass")("s", "u"))
    assert recorder.calls[0]["name"] == "web_search.second_pass"


def test_strands_refuses_tools_rather_than_dropping_them():
    """ctx.llm cannot carry a tool call, so a @tool would never be invoked — and
    the answer would look perfectly fine, which is why this must raise."""
    from app.subagents._shared.strands_bridge import strands_agent

    _, ctx = _ctx("web_search", Recorder(GOOD))
    with pytest.raises(NotImplementedError, match=r"ctx\.call_tool"):
        strands_agent(ctx, system_prompt="s", tools=[lambda: None])


def test_strands_model_refuses_tool_specs_at_the_model_boundary():
    """Belt to the braces above: even a tool reaching the provider is refused."""
    from app.subagents._shared import strands_bridge

    _, ctx = _ctx("web_search", Recorder(GOOD))
    model = strands_bridge._model_class()(ctx, "web_search")

    async def drain():
        async for _ in model.stream([{"role": "user", "content": [{"text": "u"}]}],
                                    tool_specs=[{"name": "search_web"}]):
            pass

    with pytest.raises(NotImplementedError, match="search_web"):
        asyncio.run(drain())


def test_strands_structured_output_says_what_to_use_instead():
    from app.subagents._shared import strands_bridge

    _, ctx = _ctx("web_search", Recorder(GOOD))
    model = strands_bridge._model_class()(ctx, "web_search")
    with pytest.raises(NotImplementedError, match="extract_json"):
        model.structured_output(dict, [])


def test_strands_reports_end_turn_not_max_tokens():
    """ctx.llm has already detected, logged and recorded a truncated response.
    Reporting max_tokens to Strands as well would make it spend another model call
    continuing a turn this repo has already accounted for."""
    from app.subagents._shared import strands_bridge

    recorder = Recorder("partial")
    _, ctx = _ctx("web_search", recorder)
    model = strands_bridge._model_class()(ctx, "web_search")

    async def collect():
        return [e async for e in
                model.stream([{"role": "user", "content": [{"text": "u"}]}])]

    events = asyncio.run(collect())
    stops = [e["messageStop"]["stopReason"] for e in events if "messageStop" in e]
    assert stops == ["end_turn"]


def test_strands_skips_non_text_blocks_instead_of_stringifying_them():
    """A Python repr of an image block is not something a model can read.

    Covers BOTH shapes, because they fail differently: a block with no `text` key at
    all (an image), and a block whose `text` is present but is not a string — which
    is the one a `key in block` check would wave through and drop a dict repr into
    the prompt.
    """
    from app.subagents._shared.strands_bridge import _user_text

    text = _user_text([{"role": "user", "content": [
        {"text": "keep me"},
        {"image": {"format": "png", "source": {"bytes": b"\x89PNG"}}},
        {"text": {"nested": "not a string"}},
        {"toolResult": {"content": [{"text": "a tool said this"}]}},
        {"text": "and me"},
    ]}])
    assert text == "keep me\n\nand me"


# ---------------------------------------------------------------------------
# The nested LangGraph
# ---------------------------------------------------------------------------

def test_langgraph_happy_path_takes_the_end_edge():
    recorder = Recorder(GOOD)
    _run("knowledge_research", recorder)
    assert [c["name"] for c in recorder.calls] == ["knowledge_research"]


def test_langgraph_repairs_an_unusable_first_draft():
    """The conditional edge earns its keep: an unparseable draft would otherwise
    raise ModelOutputUnusable and fail the run."""
    recorder = Recorder("I'm afraid I can't help with that.", GOOD)
    asset, _ = _run("knowledge_research", recorder)
    assert [c["name"] for c in recorder.calls] == [
        "knowledge_research", "knowledge_research.repair"]
    assert asset["summary"] == "Ingestion choice turns on ordering guarantees.", (
        "the repaired draft is not what ended up in the asset")


def test_langgraph_shows_the_model_its_own_rejected_draft():
    """A bare 'try again' re-runs the prompt that already failed once."""
    recorder = Recorder("NOT JSON AT ALL", GOOD)
    _run("knowledge_research", recorder)
    repair_prompt = recorder.calls[1]["user"]
    assert "NOT JSON AT ALL" in repair_prompt
    assert "REJECTED" in repair_prompt


def test_langgraph_repair_is_bounded():
    """Unbounded retries on a bad prompt are a cost incident. Two attempts, then
    the run fails with the error that explains itself."""
    from app.common.errors import ModelOutputUnusable

    recorder = Recorder("nope", "nope", "nope", "nope")
    agent, ctx = _ctx("knowledge_research", recorder)
    with pytest.raises(ModelOutputUnusable):
        asyncio.run(agent.run(ctx))
    assert len(recorder.calls) == 2, [c["name"] for c in recorder.calls]


def test_langgraph_usability_test_matches_what_synthesize_requires():
    """If these two disagreed, the repair edge would never fire on the case it
    exists for (or would fire on drafts that were fine)."""
    from app.subagents.knowledge_research.agent import _usable

    assert _usable(GOOD) is True
    assert _usable('{"summary": "", "findings": []}') is False
    assert _usable("not json") is False
    assert _usable('{"findings": [{"statement": "x"}]}') is True


# ---------------------------------------------------------------------------
# The seam itself
# ---------------------------------------------------------------------------

def test_synthesize_defaults_to_ctx_llm():
    """No `think` argument means the un-framework-ed path, unchanged."""
    from app.subagents._shared import research

    recorder = Recorder(GOOD)
    _, ctx = _ctx("cost_research", recorder)
    asyncio.run(research.synthesize(ctx, system_prompt="SYS"))
    assert recorder.calls[0]["system"] == "SYS"
    assert recorder.calls[0]["name"] is None  # ctx.llm defaults it to the agent id


def test_synthesize_uses_the_think_hook_when_given_one():
    from app.subagents._shared import research

    recorder = Recorder(GOOD)
    _, ctx = _ctx("cost_research", recorder)
    seen = []

    async def think(system, user):
        seen.append((system, user))
        return GOOD

    asset = json.loads(asyncio.run(
        research.synthesize(ctx, system_prompt="SYS", think=think)))
    assert len(seen) == 1
    assert seen[0][0] == "SYS"
    assert not recorder.calls, "ctx.llm was called directly, bypassing `think`"
    assert asset["summary"]
