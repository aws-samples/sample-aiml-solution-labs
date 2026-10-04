"""The promise, tested: a DIFFERENT use case needs no framework edit.

The claim this framework makes is narrow and worth pinning down: to build something
else, a customer writes `app/workflow.json` and puts their agent logic under
`app/subagents/<id>/`. Nothing under `app/common/`, `app/orchestrator/`,
`app/features/`, `bff/` or the IaC should need touching.

So this file builds a workflow with nothing in common with the shipped sample —
insurance claims triage, different agent ids, a different topology, a different
gate layout, its own asset type, its own contract, and NO use of the sample's shared
runners in `app/subagents/_shared/` — and asserts the framework carries it.

It exists because the framework used to fail this test in ways that were invisible
from inside the sample:

  * `rules.py` checked every payload against one editorial standard built for AWS
    research reports. `_ARTIFACT_NOUNS` contained "document", "record", "evidence"
    and "transcript", so a claims or legal workflow got its most useful sentences
    reported as defects. Deleted.
  * `AssetEnvelope` carried `ruleViolations`, a field only that engine wrote.
  * `AssetType` was an enum of the sample's five names, living in the framework.
  * The long-term-memory prompt injected into EVERY agent illustrated its point
    with "the user's background (hands-on with AWS serverless, Lambda, DynamoDB)".
  * The memory insight extractor looked for `openQuestions` / `limitations` /
    `dataLimitations` by name, so a workflow using its own words silently stored
    half of what it should.

None of that was reachable from the sample's own tests, because the sample agreed
with all of it.
"""

from __future__ import annotations

import json
import re
import shutil

import pytest
from conftest import ORCH_ROOT, workflow

# A claims-triage workflow. No agent id, asset type, tool or gate name is shared
# with the shipped sample, and no step shape is copied from it either: a sequence
# feeding a parallel pair, one gate, at the end rather than the start.
CLAIMS_WF = {
    "orchestrator": {"defaultModel": "us.anthropic.claude-haiku-4-5-20251001-v1:0"},
    "ui": {"title": "Claims Triage", "defaultTopic": "Assess claim 88213"},
    "tools": {},
    "agents": {
        "fnol": {"name": "First Notice of Loss", "runtime": "main", "maxTokens": 2000,
                 "produces": "loss report"},
        "policy_check": {"name": "Policy Check", "runtime": "main", "maxTokens": 2000,
                         "produces": "coverage position"},
        "fraud_screen": {"name": "Fraud Screen", "runtime": "main", "maxTokens": 2000,
                         "produces": "fraud indicators"},
        "adjust": {"name": "Adjustment", "runtime": "main", "maxTokens": 3000,
                   "produces": "settlement decision"},
    },
    "steps": [
        {"sequence": ["fnol", "policy_check"]},
        {"parallel": ["fraud_screen", "adjust"], "gateId": "review",
         "gateName": "Claims Review", "hitl": True},
    ],
}

# The customer's own agent. It uses ctx.llm and the ENVELOPE, and imports nothing
# from app/subagents/_shared/ — that directory is the sample's, not the framework's.
AGENT_SRC = '''
"""A customer agent: its own contract, its own asset type, no shared runner."""
from __future__ import annotations

import json
from typing import Literal

from pydantic import Field

from app.common.base import Agent
from app.common.contracts import AssetEnvelope, AssetStatus, Base


class Deduction(Base):
    reason: str
    amount_gbp: float = Field(alias="amountGbp")


class SettlementDecision(AssetEnvelope):
    """A shape the framework has never heard of."""
    asset_type: Literal["settlement-decision"] = Field(
        default="settlement-decision", alias="assetType")
    disposition: str
    settlement_gbp: float = Field(alias="settlementGbp")
    deductions: list[Deduction] = Field(default_factory=list)
    unresolved_queries: list[str] = Field(default_factory=list,
                                          alias="unresolvedQueries")


class AdjustAgent(Agent):
    system_prompt = "You are a claims adjuster."

    async def run(self, ctx) -> str:
        raw = await ctx.llm(self.system_prompt, f"Assess: {ctx.topic}")
        payload = json.loads(raw)
        asset = SettlementDecision(
            assetId="asset-settlement-88213-v1",
            version=1,
            status=AssetStatus.IN_REVIEW,
            createdAt="2026-09-18 12:00:00",
            createdByAgent=ctx.agent_id,
            executiveSummary=payload["executiveSummary"],
            disposition=payload["disposition"],
            settlementGbp=payload["settlementGbp"],
            deductions=payload.get("deductions") or [],
            unresolvedQueries=payload.get("unresolvedQueries") or [],
        )
        return json.dumps(asset.model_dump(by_alias=True, mode="json"))


agent = AdjustAgent()
'''

MODEL_REPLY = json.dumps({
    "executiveSummary": "Claim 88213 is payable in part; storm damage is covered.",
    "disposition": "partially-approved",
    "settlementGbp": 4820.5,
    "deductions": [{"reason": "Policy excess", "amountGbp": 250.0}],
    "unresolvedQueries": ["The loss adjuster's site photographs were not supplied."],
})


@pytest.fixture()
def claims_packages():
    """Create the four agent packages a customer would write, then remove them."""
    made = []
    try:
        for aid in CLAIMS_WF["agents"]:
            d = ORCH_ROOT / "app" / "subagents" / aid
            d.mkdir(parents=True, exist_ok=False)
            made.append(d)
            (d / "__init__.py").write_text("from .agent import agent\n")
            (d / "agent.py").write_text(AGENT_SRC)
        yield
    finally:
        for d in made:
            shutil.rmtree(d, ignore_errors=True)


def test_a_foreign_workflow_builds_its_graph_and_loads_its_agents(claims_packages):
    """Topology, gates and the registry are derived, so unknown ids are fine."""
    with workflow(CLAIMS_WF) as imp:
        cfg = imp("app.common.config")
        assert cfg.FIRST_AGENT_ID == "fnol"
        assert cfg.LAST_AGENT_ID == "adjust"
        # A sequence member sees the one before it; parallel peers do not see each other.
        assert cfg.upstream_of("policy_check") == ["fnol"]
        assert set(cfg.upstream_of("adjust")) == {"fnol", "policy_check"}
        assert "fraud_screen" not in cfg.upstream_of("adjust")

        registry = imp("app.orchestrator.registry").load_agents()
        assert sorted(registry) == ["adjust", "fnol", "fraud_screen", "policy_check"]
        assert registry["adjust"].max_tokens == 3000  # config reached the object

        graph = imp("app.orchestrator.graph_builder").build_graph()
        nodes = set(graph.get_graph().nodes)
        assert {"fnol", "policy_check", "fraud_screen", "adjust"} <= nodes
        # The gate is named from `gateId`, not from any agent id.
        assert "review_gate" in nodes


def test_a_customer_asset_the_framework_has_never_seen_round_trips(claims_packages):
    """The envelope is the only shape the framework fixes; the rest is the customer's."""
    import asyncio

    with workflow(CLAIMS_WF) as imp:
        agent = imp("app.orchestrator.registry").build_agent_module("adjust")

        class Ctx:
            agent_id = "adjust"
            topic = "Assess claim 88213"

            async def llm(self, *_a, **_k):
                return MODEL_REPLY

        out = json.loads(asyncio.run(agent.run(Ctx())))

    assert out["assetType"] == "settlement-decision"
    assert out["settlementGbp"] == 4820.5
    assert out["deductions"] == [{"reason": "Policy excess", "amountGbp": 250.0}]
    # The framework neither added nor demanded anything of its own beyond the envelope.
    assert "ruleViolations" not in out
    for envelope_field in ("assetId", "version", "status", "createdAt", "createdByAgent"):
        assert envelope_field in out


def test_the_customers_own_unresolved_field_reaches_long_term_memory(claims_packages):
    """`unresolvedQueries` is not a name the framework knows.

    The insight extractor used to look for `openQuestions`/`limitations`/
    `dataLimitations`, so this workflow would have remembered the gist and dropped
    the open queries — the most reusable half.
    """
    with workflow(CLAIMS_WF) as imp:
        AgentContext = imp("app.common.context").AgentContext
        ctx = AgentContext.__new__(AgentContext)
        ctx.topic = "Assess claim 88213"
        insight = ctx._insight_from(json.dumps({
            "assetId": "asset-settlement-88213-v1",
            "assetType": "settlement-decision",
            "executiveSummary": "Claim 88213 is payable in part.",
            "unresolvedQueries": ["The site photographs were not supplied."],
        }))

    assert "Claim 88213 is payable in part." in insight
    assert "Unresolved: The site photographs were not supplied." in insight
    assert "asset-settlement" not in insight  # bookkeeping still excluded


def test_the_framework_does_not_import_the_sample():
    """The seam, asserted rather than assumed.

    `app/subagents/_shared/` holds the SAMPLE's runners and contracts. If a
    framework module imports them, deleting the sample breaks the framework and the
    promise is false.
    """
    offenders = []
    for area in ("common", "orchestrator", "features"):
        for path in (ORCH_ROOT / "app" / area).rglob("*.py"):
            for i, line in enumerate(path.read_text().splitlines(), 1):
                s = line.strip()
                if s.startswith(("import ", "from ")) and "subagents" in s:
                    offenders.append(f"{path.relative_to(ORCH_ROOT)}:{i}: {s}")
    assert not offenders, "framework imports the sample:\n" + "\n".join(offenders)


def _shipped_vocabulary() -> dict[str, set[str]]:
    """Every name THIS SAMPLE chose, read from the shipped files rather than listed.

    Derived, not hardcoded, because a list goes stale: the previous version of this
    test named eight agent ids and silently stopped covering the two a2a review agents
    the moment they shipped.
    """
    workflow = json.loads((ORCH_ROOT / "app" / "workflow.json").read_text())
    vocab = {
        "agent id": set(workflow["agents"]),
        "corpus": {a["corpus"] for a in workflow["agents"].values() if a.get("corpus")},
    }
    # This sample's asset types. Read from the AssetType enum only — NOT from the
    # whole file — because ArtifactKind beside it holds ordinary words like "link"
    # and "table" that appear legitimately in framework code as JSON field names.
    types_src = (ORCH_ROOT / "app" / "subagents" / "_shared" / "contracts"
                 / "types.py").read_text()
    asset_block = types_src[types_src.index("class AssetType"):]
    asset_block = asset_block[:asset_block.index("\n\nclass ")] if "\n\nclass " in asset_block \
        else asset_block
    vocab["asset type"] = set(re.findall(r'^\s+[A-Z_]+ = "([a-z-]+)"', asset_block,
                                         re.MULTILINE))
    # TOOL LABELS ARE DELIBERATELY NOT CHECKED HERE, and the reason is worth writing
    # down because it looks like a gap. Framework code legitimately compares a tool's
    # declared TYPE against a literal — `kind == "websearch"` in the Gateway client is
    # correct, because the five types are framework vocabulary, identical in every
    # deployment. A tool's LABEL is the customer's. In source both read as
    # `x == "kb"`, and an AST constant scan cannot tell which side it is looking at.
    #
    # Running this check anyway produced four false positives out of five hits, so it
    # would have been turned off. The one true positive it found is now pinned
    # directly instead, by the two tests below — which is the better trade: a test that
    # asserts the specific mistake beats one that flags the shape and gets ignored.
    return {k: v for k, v in vocab.items() if v}


#: Framework areas that must not know this sample. `features` is included because it
#: was NOT — it held the one real offender this test previously missed, a fallback to
#: the literal tool label "kb" in app/features/gateway/client.py.
FRAMEWORK_AREAS = ("common", "orchestrator", "features")


def test_no_framework_module_names_this_samples_vocabulary_in_executable_code():
    """A hardcoded sample name breaks the moment a customer renames or removes it.

    Comments and docstrings are allowed to use the sample's names as examples — they
    are how the reasoning gets explained. A string literal in EXECUTABLE code is not,
    because it is read at runtime.

    Widened three ways after an audit found the earlier version was narrower than its
    name suggested. It covered agent ids only, in two of the three framework areas,
    against a hand-written list of eight that had already gone stale. It therefore
    missed `app/features/gateway/client.py` returning the literal `"kb"` — this
    sample's tool key — as a fallback, so a customer who called theirs `policies` got
    a Gateway call for a tool that does not exist and an empty retrieval reported as a
    successful one.
    """
    import ast

    vocab = _shipped_vocabulary()
    # Ordinary English, or somebody ELSE's field name, that happens to collide with a
    # sample id. Each exemption is a decision, listed so it reads as one rather than as
    # a gap:
    #
    #   analysis, report, summary, sources, kind, status
    #       plain words this codebase uses about its own data structures.
    #   recommendation
    #       a field in the AgentCore Insights API RESPONSE. `app/features/optimization/
    #       insights.py` reads `rc.get("recommendation")` from AWS's payload; it has
    #       nothing to do with this sample's `recommendation` agent, and renaming that
    #       agent would not affect it.
    ignore = {"analysis", "report", "summary", "sources", "kind", "status",
              "recommendation"}
    offenders = []
    for area in FRAMEWORK_AREAS:
        for path in (ORCH_ROOT / "app" / area).rglob("*.py"):
            tree = ast.parse(path.read_text())
            # Docstrings are Constant nodes too, so collect and exclude them.
            docstrings = {
                id(node.body[0].value)
                for node in ast.walk(tree)
                if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                                     ast.AsyncFunctionDef))
                and node.body and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
            }
            for node in ast.walk(tree):
                if (not isinstance(node, ast.Constant)
                        or not isinstance(node.value, str)
                        or id(node) in docstrings
                        or node.value in ignore):
                    continue
                for kind, names in vocab.items():
                    if node.value in names:
                        offenders.append(
                            f"{path.relative_to(ORCH_ROOT)}:{node.lineno}: "
                            f"{kind} {node.value!r}")
                        break  # one report per literal, not one per vocabulary it is in
    assert not offenders, (
        "framework code hardcodes a name that belongs to THIS SAMPLE, so a customer "
        "who renames it gets a silent failure:\n" + "\n".join(offenders))


#: Directories under app/ that are the CUSTOMER'S, not the framework's, and so are not
#: scanned for hardcoded sample names — they are where the sample names are supposed to
#: live. `subagents` holds one package per agent; `tools` holds the source of the
#: functions behind `type: "lambda"` tools. Both mirror a block in workflow.json, and
#: neither is imported by the orchestrator (app/tools/ is deployed as its own Lambda zip
#: and is excluded from the container image).
CUSTOMER_AREAS = {"subagents", "tools"}


def test_the_framework_areas_under_test_are_all_of_them():
    """The previous version scanned two of three areas, and the third held the only
    real offender. This fails if a new top-level FRAMEWORK package appears — and it
    caught `app/tools/` on the day it was created, which is the behaviour wanted: a new
    directory under app/ has to be classified deliberately as framework or customer."""
    present = {p.name for p in (ORCH_ROOT / "app").iterdir()
               if p.is_dir() and not p.name.startswith(("_", "."))
               and p.name not in CUSTOMER_AREAS}
    assert present == set(FRAMEWORK_AREAS), (
        f"app/ has framework package(s) this test does not scan: "
        f"{sorted(present - set(FRAMEWORK_AREAS))}")


def test_the_vocabulary_is_derived_from_the_shipped_files_not_listed_here():
    """If this ever returns an empty set the test above passes vacuously."""
    vocab = _shipped_vocabulary()
    # Compared against workflow.json itself rather than against ids typed here. Two of
    # the ids this used to name were removed from the sample, and the test then failed
    # for the wrong reason — it was asserting that a particular workflow exists, when
    # the thing it means to assert is that the ids come from the workflow AT ALL.
    shipped_ids = set(json.loads((ORCH_ROOT / "app" / "workflow.json").read_text())["agents"])
    assert shipped_ids and vocab["agent id"] >= shipped_ids, (
        "the agent ids are not being read from workflow.json")
    assert vocab["asset type"] >= {"research-finding", "request-brief"}, (
        "the asset types are not being read from the AssetType enum")
    # ArtifactKind sits in the same file and holds ordinary words that appear
    # legitimately in framework code as JSON field names.
    assert "link" not in vocab["asset type"]
    assert "table" not in vocab["asset type"]


# --- a tool's LABEL is the customer's; its TYPE is the framework's ----------
# The distinction an AST scan cannot make, pinned where it actually matters. Both of
# these were live defects found by widening the scan above, and both had the same
# shape: framework code comparing a literal against something that is a tool's NAME.

def test_a_retrieval_charges_embedding_cost_by_tool_type_not_by_tool_name():
    """The KB path embeds the query with Titan, and that cost was charged only when
    the tool happened to be KEYED "kb" — this sample's key.

    A customer who calls theirs `policies`, which they are told they may, got no
    embedding cost on any retrieval and a permanently zero `embed_tokens_est`, with no
    error anywhere.
    """
    from app.features.observability import meter

    recorded = []

    class _Store:
        @staticmethod
        def put(record):
            recorded.append(record)

    original = meter.store
    meter.store = _Store()  # type: ignore[assignment]
    try:
        meter.record_tool(provider="policies", tool_type="kb", query="a query" * 20)
        meter.record_tool(provider="kb", tool_type="websearch", query="a query" * 20)
    finally:
        meter.store = original

    by_type, by_name = recorded
    assert by_type.embed_tokens_est > 0, (
        "a kb-TYPE tool named something other than 'kb' was not charged for embedding")
    assert by_name.embed_tokens_est == 0, (
        "a non-kb tool that happens to be NAMED 'kb' was charged for embedding")


def test_the_gateway_client_passes_the_declared_type_to_the_meter():
    """The fix above only holds if the caller actually supplies the type."""
    import inspect

    from app.features.gateway import client

    src = inspect.getsource(client._record_tool)
    assert "tool_type=" in src, (
        "_record_tool no longer passes the tool's declared type, so meter.record_tool "
        "falls back to charging no embedding cost for every retrieval")
