"""The five asset shapes THIS SAMPLE's agents emit. Examples, not a fixed set.

  * RequestBrief    — framing: what the run is trying to achieve
  * ResearchOutput  — evidence gathered from a tool, with provenance per finding
  * Analysis        — claims traced back to the assets that support them
  * Recommendation  — options with rationale and risks
  * Report          — the assembled, sectioned final asset

Each one subclasses `AssetEnvelope` from `app.common.contracts`, which is the only
asset shape the FRAMEWORK defines. That is the seam: the envelope is provenance
and is the same for everyone; the fields below describe a research-and-report
pipeline and would be wrong for a claims workflow or a triage workflow.

So these are here, beside the agents that emit them, rather than in app/common.
Copy the pattern for your own asset and delete whichever of these you do not use —
the framework does not import any of them.
"""

from .analysis import Analysis, TracedClaim
from .brief import RequestBrief
from .recommendation import Recommendation, RecommendationItem
from .report import Report, ReportSection, SectionType
from .research import EvidenceClass, Finding, ResearchOutput
from .types import AssetType

# Alphabetical to keep the linter quiet; the pipeline order is in the docstring above.
__all__ = [
    "Analysis",
    "AssetType",
    "EvidenceClass",
    "Finding",
    "Recommendation",
    "RecommendationItem",
    "Report",
    "ReportSection",
    "RequestBrief",
    "ResearchOutput",
    "SectionType",
    "TracedClaim",
]
