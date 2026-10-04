"""Report contract — the terminal (Report) asset.

Assembles the approved upstream assets into a final, sectioned report. Each
section is pinned to the upstream assetIds it draws on, and a completeness flag
records whether every expected section is present.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from app.common.contracts.base import AssetEnvelope, Base

from .types import AssetType

# Open on purpose: a section type is any slug. Closing this Literal previously
# meant an unrecognised sectionType was SILENTLY DROPPED from the report.
#
# Which sections a report HAS, and in what order, is deliberately NOT here — it is
# domain vocabulary, so it lives with the agent that produces it
# (app/subagents/report/prompts.py SECTIONS). This contract only says a report is
# a list of typed sections.
SectionType = str


class ReportSection(Base):
    """One section of the assembled report."""

    section_id: str = Field(alias="sectionId")
    section_type: SectionType = Field(alias="sectionType")
    title: str
    content: str = ""
    source_asset_ids: list[str] = Field(default_factory=list, alias="sourceAssetIds")


class Report(AssetEnvelope):
    asset_type: Literal[AssetType.REPORT] = Field(
        default=AssetType.REPORT, alias="assetType"
    )

    title: str = ""
    sections: list[ReportSection] = Field(default_factory=list)
    is_complete: bool = Field(default=False, alias="isComplete")
