"""Research finding contract — the output of the step-2 research agents.

Every research sub-agent emits this same structured shape, differing only
in their evidence source and content.

The core of the contract is the EVIDENCE CLASSIFICATION on every finding —
separating sourced facts from calculations, assumptions, and the agent's own
interpretation — plus explicit data limitations (unavailable / incomplete /
unsupported evidence). That classification and lineage is what lets a reviewer
trust, exclude, or challenge each input at the HITL gate.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from app.common.contracts.base import AssetEnvelope, Base

from .types import AssetType

# The evidence taxonomy every finding is tagged with, so a reviewer can tell a
# sourced fact from an agent's own interpretation.
EvidenceClass = Literal[
    "sourced-fact",
    "calculation",
    "assumption",
    "agent-interpretation",
]


class Finding(Base):
    """One research statement, tagged with how it is evidenced and where it came
    from. The classification is mandatory — an unclassified claim is exactly what
    this contract exists to prevent."""

    statement: str
    classification: EvidenceClass
    source_ref: str | None = Field(default=None, alias="sourceRef")


class ResearchOutput(AssetEnvelope):
    """A single research sub-agent's output: the summary, the classified
    findings, and the named data limitations."""

    asset_type: Literal[AssetType.RESEARCH_FINDING] = Field(
        default=AssetType.RESEARCH_FINDING, alias="assetType"
    )

    summary: str = ""
    findings: list[Finding] = Field(default_factory=list)
    data_limitations: list[str] = Field(default_factory=list, alias="dataLimitations")
