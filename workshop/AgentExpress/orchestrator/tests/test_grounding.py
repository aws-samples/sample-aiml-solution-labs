"""Figures in an asset must trace to the assets it was built from.

Three synthesis prompts state this rule ("no figure that is not in an upstream asset,
not even as an illustration") and nothing enforced it, so compliance was per-run luck.
A measured run is the reason this exists: the analysis asset wrote in its own
`limitations` that the evidence "does not supply numerical thresholds" for Lambda cold
starts, and the recommendation built from that asset advised measuring against
"typically 100-1000 ms" and "typically 1-10 ms". The SAME workflow against a second
deployment invented nothing — which is the tell that a prompt edit would not have
fixed it.

The same shape as test_citations.py, for the same reason: a fabricated figure is
indistinguishable from a sourced one to whoever reads the deliverable.

Two layers:
  1. the MATCHER (app/common/grounding.py) — a pure function, so every near-miss that
     would make this noisy enough to switch off is asserted directly;
  2. the PLACEMENT (nodes._check_grounding) — which agents it runs for, and what the
     reviewer actually sees.
"""

import asyncio

import pytest
from conftest import wf, workflow

# The upstream text from the run that motivated this, quoted rather than paraphrased:
# the point is that the analysis NAMED the gap the recommendation then filled with
# invented numbers.
ANALYSIS = (
    '{"assetId": "asset-analysis-serverless-data-pipeline-v1", "version": 1, '
    '"createdAt": "2026-09-20T14:33:46.078202", '
    '"limitations": ["Evidence does not specify cold-start latency impact, Lambda '
    'concurrency limit effects, or throughput/latency characteristics under load; '
    'sources acknowledge these as constraints but do not supply numerical thresholds '
    'or mitigation strategies."], '
    '"claims": [{"statement": "AWS Lambda is the default for short, event-driven '
    'transforms under 15 minutes.", "confidence": "high"}]}'
)


@pytest.fixture()
def g():
    from app.common import grounding
    return grounding


# --- layer 1: the matcher --------------------------------------------------

def test_the_live_regression_invented_cold_start_latencies_are_flagged(g):
    """The exact defect. Neither bound appears anywhere upstream."""
    item = ('Cold-start latency: time to first execution when a new container is '
            'initialized (typically 100-1000 ms). Warm-start latency (typically 1-10 ms).')
    flagged = g.ungrounded(item, [ANALYSIS])
    assert flagged, "the figures the analysis said did not exist were not flagged"
    assert {v for v in ("100", "1000", "10") if any(v in p for p in flagged)}


def test_both_ends_of_a_range_are_examined(g):
    """"100-1000 ms" carries its unit only on the far end. Reading the unit-bearing
    number alone would have passed the half of the live defect that was largest."""
    assert set(g.figures("between 100-1000 ms")) == {"100", "1000"}


def test_a_figure_the_upstream_asset_supplies_is_not_flagged(g):
    """The other live run's only number. It was sourced, and must stay silent."""
    upstream = ('{"findings": [{"statement": "AWS Glue 5.0 runs Apache Spark 3.5.2, '
                'delivering up to 32% faster job execution.", '
                '"classification": "sourced-fact"}]}')
    assert g.ungrounded("Glue 5.0 gives up to 32% faster execution.", [upstream]) == []


def test_a_figure_carried_from_an_upstream_asset_is_not_flagged(g):
    """15 minutes is in ANALYSIS. Restating an input is what synthesis is."""
    assert g.ungrounded("Use Lambda for transforms under 15 minutes.", [ANALYSIS]) == []


def test_a_figure_from_the_request_itself_is_not_flagged(g):
    """The requester's own numbers are grounded by definition — they are the ask."""
    assert g.ungrounded("Budget 5000 USD per month.",
                        ["Design a pipeline for under 5,000 USD a month"]) == []


def test_thousands_separators_and_trailing_zeros_do_not_look_invented(g):
    """A number written one way upstream and another downstream is the same number.
    Without canonicalisation this check would flag faithful carrying, which is the
    fastest way to make it worthless."""
    assert g.ungrounded("1,200 USD", ["the total was 1200 USD"]) == []
    assert g.ungrounded("5.0 GB", ["5 GB of state"]) == []
    assert g.ungrounded("0.022 USD per GB-Mo", ["TimedStorage at 0.0220 USD"]) == []


def test_counts_in_prose_are_not_figures(g):
    """"three layers", "(1)", "(2)" and "two services" are structure and language.
    Flagging them would bury the one line that matters."""
    prose = ("The architecture has 4 layers. Apply this decision tree: (1) if ordered, "
             "use Kinesis; (2) if not, use SQS. There are 2 consumption options.")
    assert g.ungrounded(prose, [""]) == []


def test_the_envelope_is_not_content(g):
    """Asset ids, versions and timestamps are full of digits that say nothing about
    the subject, and every asset carries them."""
    envelope = ('{"assetId": "asset-recommendation-pipeline-v1", "version": 3, '
                '"createdAt": "2026-09-20T14:33:46.078202", "sourceAssetIds": '
                '["asset-analysis-pipeline-v2"]}')
    assert g.ungrounded(envelope, [""]) == []


def test_a_figure_needs_a_unit_to_be_one(g):
    """The rule names costs, thresholds, percentages, durations and cadences — all
    quantities. A bare number is not enough evidence of a claim to warn about."""
    assert g.figures("scaled to 400") == {}
    assert set(g.figures("scaled to 400 ms")) == {"400"}
    assert set(g.figures("costs $400")) == {"400"}
    assert set(g.figures("400% growth")) == {"400"}


def test_the_phrase_is_reported_not_just_the_number(g):
    """A warning saying "1000" is not actionable; one saying "100-1000 ms" is."""
    flagged = g.ungrounded("expect 100-1000 ms of latency", [""])
    assert any("ms" in p for p in flagged), flagged


def test_grounding_looks_across_every_upstream_text(g):
    """The question is "was this invented", so a number present in ANY asset of the
    run was not — even one the agent does not formally declare as an input."""
    assert g.ungrounded("32% faster", ["nothing here", "elsewhere: 32% faster"]) == []


# --- layer 2: the placement ------------------------------------------------

class Ctx:
    def __init__(self, topic=""):
        self.session_id = "s1"
        self.topic = topic


class StubAgent:
    def __init__(self, tool=None):
        self.id = "recommendation"
        self.name = "Recommendation"
        self.tool = tool


def logs_from(*, tool, out, outputs, topic=""):
    """Run the node's grounding step and return the timeline lines it emitted."""
    with workflow(wf([{"agent": "recommendation"}])) as imp:
        nodes = imp("app.orchestrator.nodes")
        captured: list[dict] = []

        async def fake_emit(session_id, event):
            captured.append(event)

        nodes.emit = fake_emit
        asyncio.run(nodes._check_grounding(
            Ctx(topic), StubAgent(tool), {"outputs": outputs}, out))
        return [e["log"] for e in captured if e.get("type") == "log"]


def test_an_agent_with_no_tool_is_checked_and_the_reviewer_is_told():
    """`recommendation` is `runtime: "a2a"` and cannot declare a tool, so it has no
    source of new figures at all. This is the placement the live defect needed."""
    lines = logs_from(tool=None, out="expect 100-1000 ms", outputs={"analysis": ANALYSIS})
    assert len(lines) == 1
    assert "Ungrounded figures in Recommendation" in lines[0]
    assert "100-1000 ms" in lines[0]


def test_an_agent_with_a_tool_is_exempt():
    """A tool IS a live evidence source, so a figure no upstream asset knows is the
    whole point of having one. `cost_research` returning a real AWS rate and
    `lifecycle_research` returning a real EOL date must both stay silent."""
    assert logs_from(tool="pricing", out="Lambda is 0.000015 USD per GB-second",
                     outputs={"analysis": ANALYSIS}) == []


def test_a_grounded_asset_produces_no_timeline_noise():
    """The common case. A check that logs on every run teaches reviewers to skip it."""
    assert logs_from(tool=None, out="Use Lambda under 15 minutes",
                     outputs={"analysis": ANALYSIS}) == []


def test_the_run_is_not_failed_by_an_ungrounded_figure():
    """Deliberate. The matcher is a strong signal, not a proof, and ending a
    five-minute run on it would trade a reviewable warning for a lost run. The
    assertion is that _check_grounding returns normally and emits nothing but a log."""
    lines = logs_from(tool=None, out="expect 100-1000 ms", outputs={"analysis": ANALYSIS})
    assert lines and all("Ungrounded figures" in line for line in lines)
