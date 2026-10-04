"""Recommendation contract — the step-4 (Recommendation) asset.

Turns the approved analysis into a prioritized set of recommendations, each with
its own rationale and any associated risk. Consumes the analysis (and the brief)
from graph state.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from app.common.contracts.base import AssetEnvelope, Base, Source

from .types import AssetType


class RecommendationItem(Base):
    """One recommended action, with its supporting rationale and risk level."""

    title: str
    detail: str = ""
    priority: Literal["high", "medium", "low"] | None = None
    rationale: str = ""
    traced_to_asset_ids: list[str] = Field(default_factory=list, alias="tracedToAssetIds")


class Recommendation(AssetEnvelope):
    asset_type: Literal[AssetType.RECOMMENDATION] = Field(
        default=AssetType.RECOMMENDATION, alias="assetType"
    )

    summary: str = ""
    items: list[RecommendationItem] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)
    assumptions: list[str] = Field(default_factory=list)
    sources: list[Source] = Field(default_factory=list)
