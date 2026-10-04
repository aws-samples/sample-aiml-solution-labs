"""Citation verification — a URL is kept only if the model was actually shown it.

The prompt tells the model never to invent a URL. A prompt is not a control: a
fabricated link is indistinguishable from a real citation to whoever reads the
output. So `assets.build_sources` and `research._findings` check every URL against
the evidence block the model was given, drop what they cannot verify, and downgrade
a "sourced-fact" whose only provenance was an invented link.

A live run caught the model doing exactly this, which is why the check exists
rather than being theoretical.
"""

import pytest


@pytest.fixture()
def research():
    from app.subagents._shared import research as r
    return r


@pytest.fixture()
def build_sources():
    """The shared source normaliser. Both runners use it: a research agent passes
    the evidence to verify against, a synthesis agent passes nothing."""
    from app.common.assets import build_sources as fn
    return fn


EVIDENCE = (
    "=== WEB SEARCH: websearch (mode=gateway) ===\n"
    "[1] | title: AWS Lambda FAQs | url: https://aws.amazon.com/lambda/faqs/ | "
    "published: 2026-02-01\n"
    "Lambda scales automatically.\n"
    "---\n"
    "[2] | title: S3 Vectors | url: https://docs.aws.amazon.com/s3vectors/\n"
    "S3 Vectors stores embeddings."
)


# --- assets.build_sources: URL verification --------------------------------

def test_a_url_present_in_the_evidence_is_kept(build_sources):
    out = build_sources({"sources": [
        {"sourceType": "mcp-tool", "sourceName": "AWS Lambda FAQs",
         "url": "https://aws.amazon.com/lambda/faqs/"}]}, verify_urls_against=EVIDENCE)
    assert out[0].url == "https://aws.amazon.com/lambda/faqs/"


def test_a_fabricated_url_is_dropped_but_the_source_is_kept(build_sources):
    """Dropping the whole source would lose the fact that something informed the
    finding; dropping only the link removes the false citation."""
    out = build_sources({"sources": [
        {"sourceType": "mcp-tool", "sourceName": "AWS Lambda Pricing",
         "url": "https://aws.amazon.com/lambda/pricing/"}]}, verify_urls_against=EVIDENCE)
    assert len(out) == 1
    assert out[0].url is None
    assert out[0].source_name == "AWS Lambda Pricing"


def test_a_near_miss_url_is_still_a_fabrication(build_sources):
    """A plausible variant of a real link is the dangerous case: it looks right."""
    out = build_sources({"sources": [
        {"sourceType": "mcp-tool", "sourceName": "x",
         "url": "https://aws.amazon.com/lambda/faq/"}]},   # faq, not faqs
        verify_urls_against=EVIDENCE)
    assert out[0].url is None


def test_urls_are_not_checked_when_there_was_no_evidence(build_sources):
    """An agent with no tool has no evidence block. Verifying against "" would drop
    every URL, including ones legitimately carried from an upstream asset."""
    out = build_sources({"sources": [
        {"sourceType": "other", "sourceName": "x", "url": "https://anything.test/"}]},
        verify_urls_against="")
    assert out[0].url == "https://anything.test/"


def test_a_customers_own_source_type_survives(build_sources):
    """sourceType is an OPEN string. It used to be a closed Literal, and anything
    outside this sample's vocabulary was silently rewritten to "other" — so a
    customer whose provenance is a warehouse or a claim file lost that word from
    every citation, and the only way to keep it was to edit the shared contract."""
    out = build_sources({"sources": [
        {"sourceType": "snowflake", "sourceName": "warehouse.public.claims"}]})
    assert out[0].source_type == "snowflake"
    assert out[0].source_name == "warehouse.public.claims"


def test_a_source_type_that_is_just_the_schema_placeholder_becomes_other(build_sources):
    """The schema shows "the kind of source" as a placeholder. A model that echoes
    it back has told us nothing, so that is the one case still coerced."""
    out = build_sources({"sources": [
        {"sourceType": "the kind of source", "sourceName": "a"}]})
    assert out[0].source_type == "other"


def test_a_multi_word_source_type_survives(build_sources):
    """The regression this exists for. The placeholder guard used to be
    `" " in value`, so EVERY legitimate multi-word kind was rewritten to "other".
    Observed live in one run: documentation_search returned "AWS documentation",
    "AWS web content" and "AWS article", web_search returned "enterprise guide"
    and "resource guide", and all five reached the downstream agent as "other" —
    the exact coercion that opening sourceType to a plain str was meant to stop."""
    for kind in ("AWS documentation", "AWS web content", "AWS article",
                 "enterprise guide", "resource guide", "internal claim file"):
        out = build_sources({"sources": [{"sourceType": kind, "sourceName": "n"}]})
        assert out[0].source_type == kind, kind


def test_the_placeholder_is_matched_exactly_not_by_shape(build_sources):
    """Only the schema's own placeholder wording coerces — not anything that
    merely looks like prose."""
    for placeholder in ("the kind of source", "The Kind Of Source", "source type",
                        "type of source", "...", "", "   "):
        out = build_sources({"sources": [{"sourceType": placeholder, "sourceName": "n"}]})
        assert out[0].source_type == "other", repr(placeholder)


def test_source_type_casing_is_preserved(build_sources):
    """Lower-casing lost the customer's own capitalisation ("AWS" -> "aws") on the
    way to the reader."""
    out = build_sources({"sources": [
        {"sourceType": "AWS Bedrock KB", "sourceName": "n"}]})
    assert out[0].source_type == "AWS Bedrock KB"


def test_sources_get_stable_ids_and_skip_junk_entries(build_sources):
    out = build_sources({"sources": [
        {"sourceType": "knowledge-base", "sourceName": "a"},
        "not-a-dict",
        {"sourceType": "knowledge-base", "sourceName": "b", "sourceId": "explicit"}]})
    assert [s.source_id for s in out] == ["source-0", "explicit"]


def test_missing_sources_key_is_not_an_error(build_sources):
    assert build_sources({}) == []
    assert build_sources({"sources": None}) == []


# --- _findings -------------------------------------------------------------

def test_a_sourced_fact_resting_on_a_fabricated_link_is_downgraded(research):
    """The important half. Removing the link but leaving the claim labelled
    "sourced-fact" would assert provenance that cannot be established."""
    out = research._findings({"findings": [
        {"statement": "Lambda costs $0.20 per million requests.",
         "classification": "sourced-fact",
         "sourceRef": "https://aws.amazon.com/lambda/pricing/"}]}, EVIDENCE)
    assert out[0].classification == "agent-interpretation"
    assert "[unverifiable link removed]" in out[0].source_ref


def test_a_sourced_fact_with_a_verified_link_is_left_alone(research):
    out = research._findings({"findings": [
        {"statement": "Lambda scales automatically.",
         "classification": "sourced-fact",
         "sourceRef": "https://aws.amazon.com/lambda/faqs/"}]}, EVIDENCE)
    assert out[0].classification == "sourced-fact"
    assert out[0].source_ref == "https://aws.amazon.com/lambda/faqs/"


def test_a_non_url_sourceref_is_untouched(research):
    """Citing by title is legitimate for evidence that carried no link (KB chunks)."""
    out = research._findings({"findings": [
        {"statement": "s", "classification": "sourced-fact",
         "sourceRef": "AWS Lambda FAQs"}]}, EVIDENCE)
    assert out[0].classification == "sourced-fact"
    assert out[0].source_ref == "AWS Lambda FAQs"


def test_only_the_unverifiable_url_is_replaced_in_a_mixed_sourceref(research):
    out = research._findings({"findings": [
        {"statement": "s", "classification": "calculation",
         "sourceRef": "see https://aws.amazon.com/lambda/faqs/ and https://made.up/x"}]},
        EVIDENCE)
    ref = out[0].source_ref
    assert "https://aws.amazon.com/lambda/faqs/" in ref
    assert "https://made.up/x" not in ref
    assert "[unverifiable link removed]" in ref
    # A calculation was never claiming source provenance, so it is not downgraded.
    assert out[0].classification == "calculation"


def test_an_unrecognised_classification_becomes_the_most_cautious_label(research):
    for bad in ("fact", "", "SOURCED FACT", "guess"):
        out = research._findings({"findings": [
            {"statement": "s", "classification": bad}]})
        assert out[0].classification == "agent-interpretation"


def test_all_four_evidence_classes_are_accepted(research):
    for cls in ("sourced-fact", "calculation", "assumption", "agent-interpretation"):
        out = research._findings({"findings": [{"statement": "s", "classification": cls}]})
        assert out[0].classification == cls


def test_findings_without_a_statement_are_dropped(research):
    out = research._findings({"findings": [
        {"classification": "sourced-fact"}, {"statement": "", "classification": "x"},
        "not-a-dict", {"statement": "kept", "classification": "assumption"}]})
    assert [f.statement for f in out] == ["kept"]


# --- assets.build_sources: no evidence to verify against -------------------

def test_synthesis_carries_urls_through_without_re_verifying(research):
    """A synthesis agent only ever reads upstream ASSETS, whose URLs were already
    verified against the evidence at research time. Re-checking against a
    non-existent evidence block would strip every legitimate citation."""
    from app.subagents._shared import synthesis

    out = synthesis.build_sources({"sources": [
        {"sourceType": "research-finding", "sourceName": "AWS Lambda FAQs",
         "url": "https://aws.amazon.com/lambda/faqs/"}]})
    assert out[0].url == "https://aws.amazon.com/lambda/faqs/"


def test_synthesis_keeps_a_customers_own_source_type_too(research):
    """Same helper, so the two runners cannot disagree about provenance."""
    from app.subagents._shared import synthesis

    out = synthesis.build_sources({"sources": [
        {"sourceType": "redshift", "sourceName": "analytics.claims"}]})
    assert out[0].source_type == "redshift"
    assert out[0].source_name == "analytics.claims"


# ---------------------------------------------------------------------------
# Asset-level provenance: which upstream assets was this one built from?
# ---------------------------------------------------------------------------

def test_the_envelope_records_the_upstream_assets_it_was_built_from():
    """`sourceAssetIds` shipped empty on every synthesized asset for three releases.

    `synthesize` collects the ids while assembling the prompt and hands them back in
    `meta["upstream_asset_ids"]`; no agent ever passed them to the contract, so the
    envelope field that exists for exactly this was always `[]` — with the answer one
    call away. Setting it in the shared `envelope()` helper means all three synthesis
    agents get it, and so does a customer agent that uses the helper.
    """
    from app.subagents._shared import synthesis

    class Ctx:
        agent_id = "analysis"

    meta = {"title": "A Title", "version": 2,
            "upstream_asset_ids": ["asset-request-brief-a-title-v1",
                                   "asset-research-web_search-a-title-v1"]}
    env = synthesis.envelope(Ctx(), meta, "analysis")
    assert env["sourceAssetIds"] == ["asset-request-brief-a-title-v1",
                                     "asset-research-web_search-a-title-v1"]
    # It must be a copy, not the caller's list, so a later mutation of meta cannot
    # rewrite an asset that has already been built.
    meta["upstream_asset_ids"].append("asset-late-v9")
    assert "asset-late-v9" not in env["sourceAssetIds"]


def test_the_envelope_survives_a_run_with_no_approved_upstream():
    """The first agent, or a degraded run, has nothing upstream. Empty is correct
    there — what was wrong was empty when there WAS something."""
    from app.subagents._shared import synthesis

    class Ctx:
        agent_id = "intake"

    env = synthesis.envelope(Ctx(), {"title": "T", "version": 1}, "request-brief")
    assert env["sourceAssetIds"] == []


def test_the_shared_envelope_reaches_a_real_synthesis_contract():
    """End to end: the helper's output has to be accepted by the contract, by alias."""
    from app.subagents._shared import synthesis
    from app.subagents._shared.contracts import Analysis

    class Ctx:
        agent_id = "analysis"

    ids = ["asset-research-web_search-t-v1"]
    asset = Analysis(**synthesis.envelope(
        Ctx(), {"title": "T", "version": 1, "upstream_asset_ids": ids}, "analysis"))
    assert asset.source_asset_ids == ids
    assert asset.model_dump(by_alias=True)["sourceAssetIds"] == ids
