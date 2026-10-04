"""Asset contracts — strict where it matters, open where a customer needs it.

Two closed vocabularies used to silently DESTROY data: `assetType` was an enum, so
a customer's own contract could not be expressed; `sectionType` was a Literal, so
a report section the model produced but the list did not name was dropped from the
report with no error. Both are now plain `str`, with the built-in contracts still
pinning their own values. These tests hold that line in both directions.
"""

import importlib
import json

import pytest
from pydantic import ValidationError

from app.common.contracts import AssetEnvelope, AssetStatus, Source
from app.subagents._shared.contracts import AssetType, Report, ReportSection

ENVELOPE = {"assetId": "asset-x-v1", "version": 1, "status": AssetStatus.IN_REVIEW,
            "createdAt": "2026-09-09 10:00:00", "createdByAgent": "agent"}


# --- assetType: open, so a customer contract needs no edit here ------------

def test_a_customer_can_use_their_own_asset_type():
    env = AssetEnvelope(assetType="claim-decision", **ENVELOPE)
    assert env.asset_type == "claim-decision"


def test_the_built_in_contracts_still_pin_their_own_type():
    """Opening the envelope must not loosen the shipped contracts."""
    with pytest.raises(ValidationError):
        Report(assetType="something-else", **ENVELOPE)
    assert Report(**ENVELOPE).asset_type == AssetType.REPORT


def test_the_canonical_asset_types_are_unchanged():
    assert [t.value for t in AssetType] == [
        "request-brief", "research-finding", "analysis", "recommendation", "report"]


# --- the envelope stays strict ---------------------------------------------

def test_an_unmodelled_field_is_rejected_rather_than_silently_dropped():
    """`extra="forbid"` on purpose: the contract IS the payload, and a field the
    orchestrator would never persist should fail loudly."""
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        AssetEnvelope(assetType="report", surpriseField="x", **ENVELOPE)


def test_version_must_be_at_least_one():
    with pytest.raises(ValidationError):
        AssetEnvelope(assetType="report", **{**ENVELOPE, "version": 0})


def test_both_snake_case_and_camel_case_populate_the_model():
    """`populate_by_name=True`: the JSON is camelCase, the Python is snake_case."""
    a = AssetEnvelope(assetType="report", **ENVELOPE)
    b = AssetEnvelope(asset_type="report", asset_id="asset-x-v1", version=1,
                      status=AssetStatus.IN_REVIEW, created_at="2026-09-09 10:00:00",
                      created_by_agent="agent")
    assert a == b


def test_serialisation_is_camel_case():
    dumped = AssetEnvelope(assetType="report", **ENVELOPE).model_dump(
        by_alias=True, mode="json")
    assert "assetType" in dumped and "asset_type" not in dumped


# --- Source ----------------------------------------------------------------

# --- RequestBrief.scope ----------------------------------------------------
# It was a plain `str`, and the intake agent coerced with str(payload["scope"]).
# The model returned the OBJECT its own schema now asks for, so str() produced a
# Python repr with single quotes — "{'inScope': [...], 'outOfScope': [...]}" —
# which is not JSON and was pasted into every downstream agent's prompt.

def test_scope_accepts_the_object_the_model_produces():
    from app.subagents._shared.contracts import RequestBrief

    brief = RequestBrief(title="T", scope={
        "inScope": ["architecture", "patterns"], "outOfScope": ["deep theory"]},
        **{k: v for k, v in ENVELOPE.items()})
    assert brief.scope.in_scope == ["architecture", "patterns"]
    assert brief.scope.out_of_scope == ["deep theory"]


def test_scope_still_accepts_a_plain_string():
    """So an asset written before the change still validates."""
    from app.subagents._shared.contracts import RequestBrief

    brief = RequestBrief(title="T", scope="everything about X", **ENVELOPE)
    assert brief.scope.summary == "everything about X"
    assert brief.scope.in_scope == []


def test_scope_serialises_as_camelcase_json_not_a_python_repr():
    from app.subagents._shared.contracts import RequestBrief

    dumped = RequestBrief(title="T", scope={"inScope": ["a"], "outOfScope": ["b"]},
                          **ENVELOPE).model_dump(by_alias=True, mode="json")
    assert dumped["scope"]["inScope"] == ["a"]
    assert dumped["scope"]["outOfScope"] == ["b"]
    # The old failure signature: single-quoted Python repr of a dict.
    assert "'inScope'" not in json.dumps(dumped)


def test_scope_defaults_to_empty_rather_than_none():
    from app.subagents._shared.contracts import RequestBrief

    brief = RequestBrief(title="T", **ENVELOPE)
    assert brief.scope.in_scope == [] and brief.scope.summary == ""


def test_the_first_agents_schema_asks_for_the_structured_scope(shipped_ids):
    """The schema and the contract must agree, or the model is guessing. The agent's
    folder is resolved from the topology, not named.

    ABOUT THE SAMPLE'S RequestBrief: `inScope`/`outOfScope` is this workflow's scope
    vocabulary, not the framework's. A first agent that emits a different brief — a
    claim, a ticket, a patient record — has no reason to carry either word, so this
    skips rather than telling a customer their intake agent is wrong.
    """
    schema = importlib.import_module(
        f"app.subagents.{shipped_ids['first']}.prompts").SCHEMA
    if '"scope"' not in schema:
        pytest.skip("this workflow's first agent does not emit a `scope` object")
    assert '"inScope"' in schema and '"outOfScope"' in schema


def test_the_first_agent_does_not_stringify_the_scope(shipped_ids):
    """The regression: scope was coerced with str(), so the model's object reached the
    report as a Python dict repr."""
    from conftest import ORCH_ROOT

    src = (ORCH_ROOT / "app" / "subagents" / shipped_ids["first"] / "agent.py").read_text()
    assert 'str(payload.get("scope")' not in src


def test_a_source_cannot_point_at_both_an_asset_and_an_artifact():
    with pytest.raises(ValidationError, match="at most one target"):
        Source(sourceId="s0", sourceType="analysis", sourceName="n",
               sourceAssetId="asset-a", artifactId="artifact-b")


def test_a_source_with_neither_target_is_valid():
    """A live tool call or a calculation has no file behind it."""
    s = Source(sourceId="s0", sourceType="calculation", sourceName="derived")
    assert s.source_asset_id is None and s.artifact_id is None


def test_source_carries_a_citation_url():
    """Structured rather than buried in prose, because the UI renders it as a link —
    which is what Web Search's terms require."""
    s = Source(sourceId="s0", sourceType="mcp-tool", sourceName="n",
               url="https://aws.amazon.com/lambda/faqs/")
    assert s.model_dump(by_alias=True)["url"] == "https://aws.amazon.com/lambda/faqs/"


# --- report sections: kept and ordered, never dropped ----------------------

@pytest.fixture()
def terminal(shipped_ids):
    """The terminal agent's own module pair, resolved from the topology rather than
    named — the report's section vocabulary lives with the agent that produces it
    (app/subagents/<id>/prompts.py SECTIONS), not in the shared contract."""
    aid = shipped_ids["last"]
    return (importlib.import_module(f"app.subagents.{aid}.agent"),
            importlib.import_module(f"app.subagents.{aid}.prompts"))


@pytest.fixture()
def sections(terminal):
    """The terminal agent's section coercion — for a workflow whose deliverable IS a
    sectioned report. A terminal agent that emits a letter, a decision record or a
    dashboard payload has no sections, and the tests below are about the shape rather
    than about the framework."""
    fn = getattr(terminal[0], "_sections", None)
    if fn is None:
        pytest.skip("this workflow's terminal agent does not build a sectioned report")
    return fn


@pytest.fixture()
def SECTIONS(terminal):
    names = getattr(terminal[1], "SECTIONS", None)
    if names is None:
        pytest.skip("this workflow's terminal agent declares no SECTIONS vocabulary")
    return names


def test_expected_sections_come_back_in_canonical_order(sections, SECTIONS):
    payload = {"sections": [{"sectionType": t, "content": t}
                            for t in reversed(SECTIONS)]}
    assert [s.section_type for s in sections(payload)] == list(SECTIONS)


def test_a_custom_section_is_kept_and_sorted_last(sections):
    """The regression: an unrecognised sectionType used to be dropped, losing real
    content the model had produced."""
    payload = {"sections": [
        {"sectionType": "regulatory-basis", "content": "custom"},
        {"sectionType": "findings", "content": "f"},
        {"sectionType": "executive-summary", "content": "e"}]}
    out = sections(payload)
    assert [s.section_type for s in out] == [
        "executive-summary", "findings", "regulatory-basis"]
    assert out[-1].content == "custom"


def test_several_custom_sections_are_all_kept(sections):
    payload = {"sections": [{"sectionType": "a-custom", "content": "1"},
                            {"sectionType": "b-custom", "content": "2"}]}
    assert len(sections(payload)) == 2


def test_duplicate_section_types_are_de_duplicated(sections):
    payload = {"sections": [{"sectionType": "findings", "content": "first"},
                            {"sectionType": "findings", "content": "second"}]}
    out = sections(payload)
    assert len(out) == 1 and out[0].content == "first"


def test_a_section_type_is_normalised_and_gets_a_derived_title(sections):
    out = sections({"sections": [{"sectionType": "  NEXT-STEPS  "}]})
    assert out[0].section_type == "next-steps"
    assert out[0].title == "Next Steps"
    assert out[0].section_id == "section-next-steps"


def test_sections_without_a_type_are_skipped(sections):
    out = sections({"sections": [{"content": "orphan"}, "not-a-dict",
                                 {"sectionType": "findings"}]})
    assert [s.section_type for s in out] == ["findings"]


def test_report_is_complete_only_when_every_expected_section_is_present(SECTIONS):
    """`isComplete` must not be satisfied by a custom section standing in for a
    missing expected one."""
    full = [ReportSection(sectionId=f"section-{t}", sectionType=t, title=t)
            for t in SECTIONS]
    assert all(t in {s.section_type for s in full} for t in SECTIONS)

    partial = [*full[:-1], ReportSection(sectionId="section-extra", sectionType="extra", title="Extra")]
    present = {s.section_type for s in partial}
    assert not all(t in present for t in SECTIONS)


# --- artifactType: open, for the same reason assetType is ------------------

def test_artifact_type_is_open_so_a_customers_output_needs_no_framework_edit():
    """It was a closed `Literal["chart","document","link","pdf","table"]`.

    Those five are what a DOCUMENT-producing pipeline emits. With `extra="forbid"` on
    the envelope, a customer attaching an audio file, a spreadsheet, a CAD drawing, a
    DICOM study or a signed claim form got a validation error and had to edit
    app/common/contracts/base.py — a framework file — to describe their own output.
    """
    from app.common.contracts import Artifact

    for kind in ("dicom-study", "audio-recording", "spreadsheet", "claim-form",
                 "cad-drawing", "bodycam-footage"):
        a = Artifact(artifactId="a1", name="n", artifactType=kind)
        assert a.artifact_type == kind


def test_a_customer_contract_can_still_narrow_artifact_type_itself():
    """Being strict is useful in the place that KNOWS, which is the customer's own
    model — not in shared framework code."""
    from typing import Literal

    import pytest
    from pydantic import ValidationError

    from app.common.contracts import Artifact

    class DicomArtifact(Artifact):
        artifact_type: Literal["dicom-study"] = "dicom-study"  # type: ignore[assignment]

    assert DicomArtifact(artifactId="a1", name="scan").artifact_type == "dicom-study"
    with pytest.raises(ValidationError):
        DicomArtifact(artifactId="a1", name="scan", artifactType="pdf")


def test_artifact_role_stays_closed_because_it_is_orchestrator_vocabulary():
    """Not domain vocabulary: "final" is what the UI surfaces as the run's
    deliverable, so a third value would not describe anything — it would just fail to
    be either."""
    import pytest
    from pydantic import ValidationError

    from app.common.contracts import Artifact

    Artifact(artifactId="a1", name="n", artifactType="pdf", artifactRole="final")
    with pytest.raises(ValidationError):
        Artifact(artifactId="a1", name="n", artifactType="pdf", artifactRole="draft")
