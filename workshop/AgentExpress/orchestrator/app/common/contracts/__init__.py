"""The asset ENVELOPE — the only asset shape the framework itself defines.

Every agent emits a validated asset, so downstream agents and the UI can rely on
a shape instead of parsing prose. What the framework fixes is the *provenance*
half: an id, a type, a version, a status, who made it and when, and where its
content came from (`sources`, `artifacts`, `sourceAssetIds`). That much is
identical for every use case, and the UI, the sink and the review gates all read
it.

What an asset CONTAINS is yours. Subclass `AssetEnvelope` in your own agent's
folder, add your fields, and pin `assetType` to whatever you call it:

    # app/subagents/claim_triage/contract.py
    from app.common.contracts import AssetEnvelope, Base

    class ClaimDecision(AssetEnvelope):
        asset_type: Literal["claim-decision"] = Field(alias="assetType")
        disposition: str
        policy_refs: list[str] = Field(default_factory=list, alias="policyRefs")

Nothing in this package needs editing to add one. `assetType`, `sourceType` and
`sectionType` are open strings for exactly that reason — as closed enums they
silently coerced a customer's own vocabulary to "other".

This sample's own five contracts (request-brief, research-finding, analysis,
recommendation, report) live with the agents that emit them, in
`app/subagents/_shared/contracts/`. They are examples to copy, not a fixed set,
and deleting them costs the framework nothing.
"""

from .base import (
    Artifact,
    ArtifactRole,
    ArtifactType,
    AssetEnvelope,
    AssetStatus,
    Base,
    Source,
    SourceType,
)

__all__ = [
    "Artifact",
    "ArtifactRole",
    "ArtifactType",
    "AssetEnvelope",
    "AssetStatus",
    "Base",
    "Source",
    "SourceType",
]
