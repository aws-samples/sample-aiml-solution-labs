"""Shared asset envelope and controlled vocabulary.

Every asset an agent produces is structured JSON validated by a Pydantic model.
They share one envelope (`AssetEnvelope`) and a small controlled vocabulary, so
the whole pipeline emits consistent, machine-checkable output.

Two conventions worth knowing before editing:

1. The JSON is camelCase; a database would be snake_case. Models are declared in
   snake_case with camelCase aliases, and `populate_by_name=True` lets both work.
   Serialise with `by_alias=True` when producing the customer-shaped JSON.

2. `status` in the JSON is lowercase-hyphenated ('in-review', 'final') and is the
   asset's own payload status. It is distinct from the orchestrator's workflow
   readiness label, which never goes in the payload.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Base(BaseModel):
    """Shared config. Extra fields are forbidden on purpose.

    The contract IS the payload. If an asset arrives with a field we have not
    modelled, failing loudly is better than silently dropping data the
    orchestrator would then never persist.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        use_enum_values=False,
        str_strip_whitespace=True,
    )


# ---------------------------------------------------------------------------
# Controlled vocabulary
# ---------------------------------------------------------------------------


class AssetStatus(StrEnum):
    """The asset's own `status` field.

    Distinct from any workflow state the orchestrator tracks separately.
    """

    PENDING = "pending"
    IN_REVIEW = "in-review"
    APPROVED = "approved"
    FINAL = "final"


# Deliberately `str`, for exactly the reason given for SourceType below. As a closed
# Literal this listed five kinds a DOCUMENT-PRODUCING pipeline happens to emit —
# chart, document, link, pdf, table — and anything else failed validation outright,
# because `extra="forbid"` above makes the envelope strict. A customer whose workflow
# attaches an audio file, a spreadsheet, a CAD drawing, a DICOM study or a signed
# claim form had to edit THIS framework file to describe their own output, which is
# precisely the coupling the rest of this module was cleaned up to remove.
#
# The five names above are this sample's vocabulary and now live with the sample, in
# app/subagents/_shared/contracts/types.py, where a customer replaces them. Your own
# contract can still narrow it — declare `artifact_type: Literal["dicom-study"]` on
# your model and pydantic enforces YOUR list, which is the useful place to be strict.
ArtifactType = str

# Still closed, and for a different reason: these two values are not domain
# vocabulary, they are how the ORCHESTRATOR reads an artifact. "final" is what the UI
# surfaces as the run's deliverable and what a terminal asset is checked against, so
# a third value here would not describe anything — it would just fail to be either.
ArtifactRole = Literal["final", "supporting-artifact"]

# Deliberately `str`, not a closed Literal. As a Literal this listed THIS sample's
# provenance kinds and anything outside the list was silently coerced to "other": a
# customer whose provenance is a "claim-file" or a "policy-doc" lost that word from
# every citation, and the only way to keep it was to edit this shared framework file.
# Whatever your agent writes here is carried through as given.
SourceType = str


# ---------------------------------------------------------------------------
# Shared components
# ---------------------------------------------------------------------------


class Artifact(Base):
    """A concrete file or record. Distinct from an asset, which is structured JSON."""

    artifact_id: str = Field(alias="artifactId")
    name: str
    artifact_type: ArtifactType = Field(alias="artifactType")
    artifact_role: ArtifactRole | None = Field(default=None, alias="artifactRole")
    uri: str | None = None
    description: str | None = None


class Source(Base):
    """One provenance entry.

    Points at another asset (`source_asset_id`), an artifact (`artifact_id`), or
    neither - a live tool call or a calculation has no file behind it.
    """

    source_id: str = Field(alias="sourceId")
    source_type: SourceType = Field(alias="sourceType")
    source_name: str = Field(alias="sourceName")
    source_asset_id: str | None = Field(default=None, alias="sourceAssetId")
    artifact_id: str | None = Field(default=None, alias="artifactId")
    # The citation link, when the evidence came with one (web results always do).
    # Kept structured rather than buried in prose because AgentCore Web Search's
    # acceptable-use terms require the returned citations and links to be retained
    # and DISPLAYED wherever the result is surfaced to an end user — the UI renders
    # this as an anchor.
    url: str | None = None
    description: str | None = None

    @model_validator(mode="after")
    def _not_both_targets(self) -> Source:
        if self.source_asset_id and self.artifact_id:
            raise ValueError(
                f"source {self.source_id!r} points at both an asset and an artifact; "
                "a source resolves to at most one target"
            )
        return self


# ---------------------------------------------------------------------------
# Envelope
# ---------------------------------------------------------------------------


class AssetEnvelope(Base):
    """Fields common to every asset."""

    asset_id: str = Field(alias="assetId")
    # Deliberately a plain `str`, and there is no enum of permitted values anywhere
    # in the framework: your contract declares its own type
    # (`Literal["claim-decision"]`) without editing this file. The sample's own five
    # names live with the sample, in app/subagents/_shared/contracts/types.py.
    asset_type: str = Field(alias="assetType")
    version: int = Field(ge=1)
    status: AssetStatus
    created_at: datetime = Field(alias="createdAt")
    created_by_agent: str = Field(alias="createdByAgent")

    executive_summary: str | None = Field(default=None, alias="executiveSummary")
    source_asset_ids: list[str] = Field(default_factory=list, alias="sourceAssetIds")
    artifacts: list[Artifact] = Field(default_factory=list)
    sources: list[Source] = Field(default_factory=list)
