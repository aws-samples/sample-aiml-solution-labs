"""What reaches long-term memory — the one write whose mistakes outlive the run.

A bad prompt produces a bad report and the next run gets another chance. A bad
long-term memory record is recalled by EVERY later run until someone deletes the
namespace, and the two failures observed live were exactly that:

  * the whole asset JSON was stored, so recall came back as bookkeeping
    ("asset ID: asset-analysis-…-v2, version 2, status: in-review"), and
  * the extraction model inferred things about the requester that nobody said
    ("the user's background (hands-on with AWS serverless, Lambda, DynamoDB)
    suggests …" for a request whose entire text was five words), which a later
    run then cited as the rationale for a recommendation.

So `_insight_from` condenses the asset to prose and strips sentences that are
about the requester or about this system's own run history.
"""

import pytest


@pytest.fixture()
def ctx():
    """An AgentContext with no __init__ — these two helpers only read `topic`."""
    from app.common.context import AgentContext
    c = AgentContext.__new__(AgentContext)
    c.topic = "Build an agentic ai application"
    return c


ASSET = {
    "assetId": "asset-analysis-build-an-agentic-ai-application-v2",
    "version": 2,
    "status": "in-review",
    "createdByAgent": "analysis",
    "title": "Agentic AI Application Build",
    "executiveSummary": "Framework choice is the load-bearing decision.",
    "openQuestions": ["What is the deployment target?", "Who owns the data?"],
}


def test_bookkeeping_fields_never_reach_memory(ctx):
    """The regression that made recall worthless: ids, versions and statuses were
    the most repeated strings in the stored blob, so that is what got extracted."""
    import json

    insight = ctx._insight_from(json.dumps(ASSET))
    for bookkeeping in ("assetId", "asset-analysis", "in-review", "version"):
        assert bookkeeping not in insight
    assert "Framework choice is the load-bearing decision." in insight


def test_the_topic_and_the_unknowns_are_what_gets_kept(ctx):
    """Recall is queried by the run's topic, so the topic has to be in the record
    for it to match; the unresolved questions are the reusable part."""
    import json

    insight = ctx._insight_from(json.dumps(ASSET))
    assert "Agentic AI Application Build" in insight
    assert "What is the deployment target?" in insight


def test_the_unknowns_are_found_in_a_customers_own_vocabulary(ctx):
    """The framework must not need to know your schema to remember the useful part.

    This used to look for `openQuestions`, `limitations` and `dataLimitations` — this
    sample's own field names — so a workflow that called the same thing something
    else stored the gist and silently dropped the unresolved half. Found by shape
    (a list of strings) and a generic uncertainty word instead.
    """
    import json

    for key in ("caveats", "openGaps", "unknowns", "outstandingItems",
                "blockers", "riskRegister"):
        insight = ctx._insight_from(json.dumps({
            "assetId": "asset-claim-decision-v1",
            "assetType": "claim-decision",
            "executiveSummary": "The claim is payable in part.",
            key: ["Policy wording for clause 4(b) is ambiguous."],
        }))
        assert "The claim is payable in part." in insight
        assert "Unresolved: Policy wording for clause 4(b) is ambiguous." in insight, key


def test_a_list_that_is_not_about_uncertainty_is_not_stored_as_unresolved(ctx):
    """The heuristic has to be narrow enough to stay meaningful: a list of parties
    or line items is not a list of open questions."""
    import json

    insight = ctx._insight_from(json.dumps({
        "assetId": "asset-claim-decision-v1",
        "executiveSummary": "The claim is payable in part.",
        "claimants": ["A. Patel", "R. Gomez"],
        "lineItems": ["Windscreen", "Courtesy car"],
    }))
    assert "Unresolved" not in insight
    assert "A. Patel" not in insight


def test_claims_about_the_requester_are_dropped(ctx):
    """Observed live, and the reason a later run recommended Lambda: memory
    asserted a background the user never mentioned."""
    import json

    insight = ctx._insight_from(json.dumps({
        "title": "T",
        "executiveSummary": (
            "Framework choice is the load-bearing decision. The user has "
            "hands-on experience with AWS serverless. Cost was not supplied."
        ),
    }))
    assert "load-bearing" in insight
    assert "Cost was not supplied." in insight
    assert "hands-on" not in insight


def test_run_counts_are_dropped_because_they_are_stale_tomorrow(ctx):
    """"…has completed four prior runs" was true when written and wrong on the
    next run. A record that ages badly should not be written at all."""
    import json

    insight = ctx._insight_from(json.dumps({
        "title": "T",
        "executiveSummary": "The user has completed four prior runs on this "
                            "subject. Framework choice dominates.",
    }))
    assert "four prior runs" not in insight
    assert "Framework choice dominates." in insight


def test_a_plain_text_agent_output_is_still_filtered_and_capped(ctx):
    """The non-JSON path stored content verbatim, so it bypassed the filter."""
    insight = ctx._insight_from(
        "The user prefers Terraform. " + "Grounded finding. " * 200)
    assert "prefers Terraform" not in insight
    assert len(insight) <= ctx._INSIGHT_MAX_CHARS


def test_an_ordinary_sentence_mentioning_a_user_survives(ctx):
    """The filter targets assertions ABOUT the requester, not the word 'user' —
    over-filtering would silently drop legitimate subject matter."""
    kept = ctx._drop_meta_sentences(
        "Each user session needs an isolated memory namespace.")
    assert kept == "Each user session needs an isolated memory namespace."


# ---------------------------------------------------------------------------
# Recall must not reach across unrelated subjects
# ---------------------------------------------------------------------------

TOPIC = "Design a serverless data pipeline on AWS"
# Verbatim from a live run's recall panel, on the pipeline request above. Both were
# stored by an EARLIER run about an agentic AI application.
BLED_THROUGH = [
    "The user is interested in building an agentic AI application.",
    ("The user is working on building an agentic AI application. An analysis "
     "identified nine core architectural components and five orchestration "
     "patterns required for the system."),
]


def test_an_insight_from_an_unrelated_subject_is_dropped():
    """Every request in the deployment shared one namespace per agent, so semantic
    search returned its top_k whether or not anything was close. The agents
    correctly refused to use these as rationale — the prompt forbids it — but the
    framework should not have offered them, and every synthesis agent paid tokens
    to read them."""
    from app.common.context import _on_topic
    assert _on_topic(BLED_THROUGH, TOPIC) == []


def test_an_insight_about_this_subject_survives():
    """The bar is one shared content word, so a near-miss on wording still recalls.
    A filter that dropped these would be worse than the leak it replaced."""
    from app.common.context import _on_topic
    on_topic = [
        ("Serverless data pipelines on AWS organise work into ingestion, storage, "
         "processing and consumption layers."),
        "A prior pipeline run found Glue ETL DPU-hours dominate the bill.",
    ]
    assert _on_topic(on_topic, TOPIC) == on_topic


def test_a_query_with_nothing_to_compare_keeps_everything():
    """No significant words means no evidence either way, and silently discarding
    a recall is the failure this rule is supposed to prevent."""
    from app.common.context import _on_topic
    assert _on_topic(BLED_THROUGH, "") == BLED_THROUGH
    assert _on_topic(BLED_THROUGH, "the it a") == BLED_THROUGH


def test_sharing_only_a_filler_word_is_not_sharing_a_subject():
    """"building", "request" and "user" appear in almost every insight this system
    stores, so matching on them would readmit everything the rule just excluded."""
    from app.common.context import _on_topic
    assert _on_topic(["The user is building something for a request."],
                     "Design a serverless data pipeline") == []


def _actor(agent_id: str, *, subject: str = "", topic: str = "") -> str:
    """`_memory_actor` on a bare context — the real function, not a reimplementation.

    The two tests this replaced only exercised `_slug`, so they passed whatever
    `_memory_actor` actually did, and went on passing after its behaviour changed.
    """
    from app.common.context import AgentContext
    c = AgentContext.__new__(AgentContext)
    c.agent_id, c.subject_id, c.topic = agent_id, subject, topic
    return c._memory_actor()


def test_runs_on_different_topics_share_one_namespace_per_agent():
    """Long-term memory needs a corpus, and topic scoping left it write-only.

    Measured on the deployment before this changed: 66 writes had produced 88
    distinct namespaces — about one per run, each with a single record — and of 101
    recalls the only 23 that ever hit predate the scoping. Relevance is handled by
    the semantic query and `_on_topic`, not by partitioning.
    """
    a = _actor("analysis", topic="Build a data lake application")
    b = _actor("analysis", topic="Build a data lake in AWS")
    assert a == b == "analysis", "two topics must reach the same per-agent namespace"


def test_two_agents_never_share_a_namespace():
    """An agent recalls its OWN past insights; the report's are not the intake's."""
    assert _actor("analysis", topic="X") != _actor("report", topic="X")


def test_an_explicit_subject_partitions_and_still_wins():
    """`subject_id` is how an operator groups runs deliberately — a customer, an
    account, a product line — and it must override the per-agent default."""
    assert _actor("analysis", subject="ACME Corp") == "analysis-acme-corp"
    # The subject, not the topic, is what separates them.
    assert (_actor("analysis", subject="ACME Corp", topic="anything")
            != _actor("analysis", subject="Globex", topic="anything"))


def test_each_user_has_their_own_memory_and_the_namespace_does_not_show_who():
    """One deployment, many users: what one user's runs taught an agent must not be
    recalled into another user's run."""
    from app.common.context import AgentContext

    def actor(user, subject=""):
        c = AgentContext.__new__(AgentContext)
        c.agent_id, c.subject_id, c.topic, c.user = "analysis", subject, "t", user
        return c._memory_actor()
    alice, bob = actor("alice@example.com"), actor("bob@example.com")
    assert alice != bob and alice.startswith("analysis-u") and "alice" not in alice
    assert actor("Alice@Example.com ") == alice                 # the same person, however typed
    assert actor("alice@example.com", "ACME Corp") == alice + "-acme-corp"
    assert actor("alice@example.com", "ACME Corp") != actor("bob@example.com", "ACME Corp")
    assert actor("") == "analysis"                              # single-tenant: per agent


def test_the_memory_scope_decides_whose_memories_an_agent_reads():
    from app.common.context import AgentContext

    def ctx(scope, user="alice@example.com", subject="", **mem):
        c = AgentContext.__new__(AgentContext)
        c.agent_id, c.subject_id, c.topic, c.user, c.session_id = "analysis", subject, "t", user, "sess1"
        c._agentcore = {"memory": {"longTerm": ["semantic", "episodic"], **({"scope": scope} if scope else {}), **mem}}
        return c
    alice, bob = ctx(None)._memory_actor(), ctx(None, "bob@example.com")._memory_actor()
    assert alice != bob                                                   # user: the default
    assert ctx("subject", subject="ACME")._memory_actor() == ctx("subject", "bob@example.com", "ACME")._memory_actor()
    assert ctx("subject")._memory_actor() == alice                        # no subject: per user
    assert ctx("agent")._memory_actor() == ctx("agent", "bob@example.com")._memory_actor() == "analysis"
    assert ctx("run")._memory_actor().endswith("-rsess1") and ctx("run")._memory_actor() != alice
    # Each strategy reads its own namespace; a custom strategy is this agent's own.
    c = ctx(None, custom={"base": "semantic", "instructions": "x"})
    assert c._longterm_strategies() == ["semantic", "episodic", "custom"]
    assert c._namespace_for("episodic") == f"episodes/{alice}"
    assert c._namespace_for("userPreference") == f"preferences/{alice}"
    assert c._namespace_for("custom") == f"custom-analysis/{alice}"


def test_a_long_subject_id_stays_a_legible_namespace_segment():
    from app.common.context import AgentContext
    actor = _actor("analysis", subject="x" * 400)
    assert len(actor) <= len("analysis-") + AgentContext._SUBJECT_SLUG_CHARS


# ---------------------------------------------------------------------------
# _on_topic — the filter the widened namespace now depends on
# ---------------------------------------------------------------------------

def test_a_plural_still_matches_its_singular():
    """`_on_topic` compares word sets, so an exact match made a near-miss a miss.

    Measured live: a run on "Build a data lake application" could not match a
    recalled insight about "data lakes", because `lake` and `lakes` were different
    words. Widening the namespace made this filter load-bearing, so its weakest
    point had to go.
    """
    from app.common.context import _on_topic, _significant

    assert "lake" in _significant("data lakes on AWS")
    assert _significant("data lake") & _significant("data lakes")
    item = "Data lakes separate storage from compute across four zones."
    assert _on_topic([item], "Build a data lake application") == [item]


def test_an_irregular_plural_folds_too():
    from app.common.context import _significant

    assert _significant("policies") == _significant("policy")


def test_a_double_s_word_is_not_mangled():
    """`-ss` is excluded, or `access` becomes `acces` and `process` `proces`."""
    from app.common.context import _significant

    assert _significant("access control") >= {"access"}
    assert _significant("processing") >= {"processing"}
    assert "acces" not in _significant("access")


def test_an_unrelated_subject_is_still_dropped():
    """Stemming must not turn the filter off — this is the case it exists for.

    Observed live: a data-lake run recalled "agentic AI applications on AWS using
    Amazon Bedrock AgentCore …" and correctly kept none of it.
    """
    from app.common.context import _on_topic

    item = ("The user is interested in building agentic AI applications on AWS using "
            "Amazon Bedrock AgentCore, LangGraph, CrewAI and Strands.")
    assert _on_topic([item], "Build a data lake application") == []


def test_a_query_with_no_content_words_keeps_everything():
    """Nothing to compare against is not evidence of irrelevance."""
    from app.common.context import _on_topic

    assert _on_topic(["anything at all"], "the and of") == ["anything at all"]


def test_a_generic_container_noun_is_not_a_shared_subject():
    """The tension plural folding created, pinned so it cannot silently reopen.

    Stemming is needed so "data lakes" reaches "data lake". But it also folded
    "applications" onto "application", and THAT made a data-lake run match an
    agentic-AI insight on the one word both topics happen to contain. Both topics
    are "build an application"; what distinguishes them is `lake` and `agentic`.

    So the generic nouns for "the thing being built" are stopwords, and the two
    behaviours have to hold at once: a real near-miss matches, a shared container
    noun does not.
    """
    from app.common.context import _on_topic, _significant

    # The container noun carries no subject.
    assert not (_significant("data lake application")
                & _significant("agentic AI application"))
    # But the real subject word still does.
    assert _significant("data lake application") & _significant("data lakes on AWS")
    # Both directions, end to end.
    assert _on_topic(["Agentic AI applications need a tool loop."],
                     "Build a data lake application") == []
    assert _on_topic(["Data lakes use zone separation."],
                     "Build a data lake application")


# ---------------------------------------------------------------------------
# Observability: what the agent GOT, not what the store returned
# ---------------------------------------------------------------------------

def _recall_ctx(returns, *, topic, recorded):
    """A context wired for memory_recall with the store and the meter stubbed."""
    from app.common.context import AgentContext

    c = AgentContext.__new__(AgentContext)
    c.agent_id, c.subject_id, c.topic = "analysis", "", topic
    c.session_id = "s"
    c._agentcore = {"memory": {"longTerm": ["semantic"]}}
    c._record_memory = lambda op, ns, q, res, ms: recorded.append((op, ns, res))
    return c


def test_a_recall_whose_every_record_is_dropped_is_not_reported_as_a_hit(monkeypatch):
    """The panel said memory was working while the agent received nothing.

    `_record_memory` logs the RAW result per namespace and `_on_topic` runs after,
    so a recall that found five off-topic records and kept none looked like a hit.
    Observed while verifying a live run: a data-lake run recalled agentic-AI
    insights, correctly discarded all of them, and the row said HIT.
    """
    import asyncio

    from app.features import memory as memory_pkg

    off_topic = "Agentic AI applications need a tool loop and a state machine."

    async def fake_recall(query, namespace="", memory_id=""):
        return [off_topic]

    monkeypatch.setattr(memory_pkg, "recall", fake_recall)

    recorded: list = []
    ctx = _recall_ctx([off_topic], topic="Build a data lake", recorded=recorded)
    got = asyncio.run(ctx.memory_recall("Build a data lake"))

    assert got == [], "an off-topic recall must not reach the agent"
    ops = [r[0] for r in recorded]
    assert "recall" in ops, "the raw per-namespace result is still recorded"
    assert "recall-filtered" in ops, "and so is what survived the filter"
    note = next(r[2] for r in recorded if r[0] == "recall-filtered")
    assert "0 of 1" in note, note


def test_a_recall_that_survives_the_filter_records_no_extra_row(monkeypatch):
    """The happy path stays one row per namespace — no noise when nothing is lost."""
    import asyncio

    from app.features import memory as memory_pkg

    on_topic = "Data lakes separate storage from compute across zones."

    async def fake_recall(query, namespace="", memory_id=""):
        return [on_topic]

    monkeypatch.setattr(memory_pkg, "recall", fake_recall)

    recorded: list = []
    ctx = _recall_ctx([on_topic], topic="Build a data lake", recorded=recorded)
    got = asyncio.run(ctx.memory_recall("Build a data lake"))

    assert got == [on_topic]
    assert [r[0] for r in recorded] == ["recall"]


# ---------------------------------------------------------------------------
# WHERE the memory lifecycle runs
# ---------------------------------------------------------------------------
# The node wrapper recalls before run() and stores after it, which is what makes
# memory config-driven for an ordinary agent. For an agent whose work happens
# elsewhere, both were wrong — and silently so. These pin the split.

def test_a_dedicated_agent_does_not_recall_or_store_in_the_orchestrator():
    """The dedicated CONTAINER owns both halves (app/subagent_runtime.py), because
    that is where ctx.llm injects the recalled insights into the prompt.

    Doing it in the orchestrator as well was two defects per run. The recall was
    billed, written to the telemetry table as a recall row, and then DISCARDED —
    `recalled_memory` is read only by ctx.llm and the InvokeAgentRuntime payload never
    carried it. The store wrote the same insight to the same actor namespace twice,
    and duplicates come back as two copies on every later recall.
    """
    from app.common.agentcore_agent import AgentCoreRuntimeAgent

    assert AgentCoreRuntimeAgent.recall_in_orchestrator is False
    assert AgentCoreRuntimeAgent.store_in_orchestrator is False


def test_a_remote_a2a_agent_gets_both_halves_of_memory():
    """Unlike a dedicated agent, and the difference is the payload.

    InvokeAgentRuntime has a fixed shape that cannot carry recalled insights, so
    recalling for one would be billed and discarded. A2A carries OPAQUE TEXT, so the
    task message can hand them over — and does. Leaving recall off here would have made
    `agentcore.memory.longTerm` on a remote agent mean store-only: insights
    accumulating run after run that nothing ever reads back, while the config reads as
    though the feature is on.
    """
    from app.common.a2a_agent import A2AAgent

    assert A2AAgent.recall_in_orchestrator is True
    assert A2AAgent.store_in_orchestrator is True


def test_recalled_insights_reach_the_remote_agent_with_their_caveat():
    """The flag above is only true if the message actually carries them.

    And the caveat has to travel with them. In-process, `ctx.llm` appends it to the
    system prompt; a remote agent has none of ours, so bare `items` would hand a
    third-party agent a list of unverified assertions from earlier runs with nothing
    saying they are not evidence.
    """
    import json
    from types import SimpleNamespace

    from app.common.a2a_agent import A2AAgent
    from app.common.context import RECALL_CAVEAT

    ctx = SimpleNamespace(
        topic="assess this applicant", session_id="s1", agent_id="partner",
        state={"outputs": {}}, feedback="",
        # The blank is not incidental: recall returns whatever the store held, and an
        # empty entry in the list would reach the remote agent as a blank insight.
        recalled_memory=["Applicant 42 was declined in March.", "  "])

    agent = A2AAgent()
    agent.id = "partner"
    task = json.loads(A2AAgent._message(agent, ctx)["parts"][0]["text"])
    assert task["recalledContext"]["items"] == ["Applicant 42 was declined in March."]
    assert task["recalledContext"]["caveat"] == RECALL_CAVEAT


def test_the_timeline_says_how_many_recollections_left_the_deployment():
    """Visible, not inferred. This is the one step where a reviewer cannot read the
    prompt to see what the agent was told — and recollections crossing a trust boundary
    is something an operator should be able to watch happen rather than deduce from the
    config asking for it."""
    import asyncio
    import json
    from types import SimpleNamespace

    from conftest import workflow

    defn = {
        "orchestrator": {"a2aInvoke": {"timeoutSeconds": 5, "pollIntervalSeconds": 0,
                                       "maxPollSeconds": 1}},
        "tools": {},
        "agents": {
            "intake": {"name": "Intake", "runtime": "dedicated", "maxTokens": 100},
            "partner": {"name": "Partner", "runtime": "a2a",
                        "agentCard": "https://agents.partner.example"},
        },
        "steps": [{"agent": "intake"}, {"agent": "partner"}],
    }
    logs: list[str] = []
    ctx = SimpleNamespace(
        topic="t", session_id="s1", agent_id="partner", state={"outputs": {}},
        feedback="", recalled_memory=["one", "two"],
        log=lambda m: (logs.append(m), asyncio.sleep(0))[1])

    class Resp:
        def __init__(self, payload):
            self._b = json.dumps(payload).encode()

        def read(self):
            return self._b

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def urlopen(request, timeout=None):
        if request.data:
            body = json.loads(request.data)
            return Resp({"jsonrpc": "2.0", "id": body["id"], "result": {
                "id": "t1", "status": {"state": "completed"},
                "artifacts": [{"parts": [{"kind": "text", "text": "done"}]}]}})
        return Resp({"url": "https://agents.partner.example/rpc"})

    with workflow(defn) as imp:
        mod = imp("app.common.a2a_agent")
        mod.urllib.request.urlopen = urlopen
        asyncio.run(imp("app.orchestrator.registry").load_agents()["partner"].run(ctx))

    assert any("carrying 2 recalled insight(s)" in m for m in logs), logs

    # And an agent with no memory configured says nothing about it, rather than
    # "carrying 0", which would read as memory having run and found nothing.
    logs.clear()
    ctx.recalled_memory = []
    with workflow(defn) as imp:
        mod = imp("app.common.a2a_agent")
        mod.urllib.request.urlopen = urlopen
        asyncio.run(imp("app.orchestrator.registry").load_agents()["partner"].run(ctx))
    assert any("Delegating 'partner'" in m for m in logs)
    assert not any("recalled insight" in m for m in logs), logs


def test_no_recalled_insights_means_no_key_at_all():
    """Rather than an empty list, which reads as "memory ran and found nothing" when
    what happened is that memory is not configured for this agent."""
    import json
    from types import SimpleNamespace

    from app.common.a2a_agent import A2AAgent

    ctx = SimpleNamespace(topic="t", session_id="s1", agent_id="partner",
                          state={"outputs": {}}, feedback="", recalled_memory=[])

    agent = A2AAgent()
    agent.id = "partner"
    task = json.loads(A2AAgent._message(agent, ctx)["parts"][0]["text"])
    assert "recalledContext" not in task


def test_the_recall_caveat_is_one_paragraph_used_by_both_paths():
    """Two copies of a safety caveat is one copy to get wrong. `ctx.llm` and the A2A
    task message both read this constant rather than inlining the text."""
    from pathlib import Path

    from app.common import context

    assert "not evidence" in context.RECALL_CAVEAT
    a2a_src = Path(context.__file__).with_name("a2a_agent.py").read_text()
    assert "RECALL_CAVEAT" in a2a_src
    # The prose appears ONCE in the tree — at the constant. `ctx.llm` used to inline it,
    # and a second copy is the copy that goes stale.
    tree = Path(context.__file__).parent.parent
    copies = sum(src.read_text().count("UNVERIFIED recollections")
                 for src in tree.rglob("*.py"))
    assert copies == 1, f"the recall caveat is written out {copies} times, not once"


def test_an_ordinary_in_process_agent_still_gets_both_for_free():
    """The config-driven promise: `memory.longTerm` in workflow.json and no agent
    code."""
    from app.common.base import Agent

    assert Agent.recall_in_orchestrator is True
    assert Agent.store_in_orchestrator is True


def test_the_node_wrapper_honours_both_flags():
    """A flag nothing reads is worse than no flag. Asserted against the source of the
    wrapper, because driving a full graph node here would need the whole AWS surface
    stubbed for what is a two-line guard."""
    import inspect

    from app.orchestrator import nodes

    src = inspect.getsource(nodes)
    assert "if agent.recall_in_orchestrator:" in src
    assert "if agent.store_in_orchestrator:" in src
    # The OUTPUT guardrail must NOT be behind either flag: it is about what crosses
    # into the rest of the run, so it applies to every placement.
    guard = src.index('await ctx.guardrail(str(out), "OUTPUT")')
    assert "if agent." not in src[guard - 200:guard].split("\n")[-1]
