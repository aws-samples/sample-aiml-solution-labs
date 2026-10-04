"""Analysis contract — the step-3 (Analysis) asset.

Synthesizes the brief and the two research findings into one analysis. The core
of the contract is `claims`: every material claim is traced structurally to the
upstream assets that support it, so a reviewer can follow each assertion back to
its evidence rather than trusting prose. Assumptions and limitations are named
explicitly.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from app.common.contracts.base import AssetEnvelope, Base, Source

from .types import AssetType


class TracedClaim(Base):
    """A material claim with explicit provenance — traced to the upstream
    asset(s) that support it, enforced structurally rather than as prose."""

    statement: str
    traced_to_asset_ids: list[str] = Field(default_factory=list, alias="tracedToAssetIds")
    confidence: Literal["high", "medium", "low"] | None = None


class Analysis(AssetEnvelope):
    """The Analysis output: findings + rationale + traced claims."""

    asset_type: Literal[AssetType.ANALYSIS] = Field(
        default=AssetType.ANALYSIS, alias="assetType"
    )

    summary: str = ""
    rationale: str = ""
    claims: list[TracedClaim] = Field(default_factory=list)
    assumptions: list[str] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)
    sources: list[Source] = Field(default_factory=list)
