"""The asset-type vocabulary THIS SAMPLE's five contracts use.

It lives here, not in `app.common.contracts`, because the names are a property of
this workflow: a claims pipeline has no "research-finding" and a triage pipeline has
no "report". The framework only requires that `AssetEnvelope.asset_type` be a
string; it never compares it against a list.

Kept as an Enum so the five contracts below cannot drift apart in spelling, and so
each can pin itself strictly with `Literal[AssetType.X]`. Your own contract does not
need an entry here — a plain `Literal["claim-decision"]` is enough.
"""

from enum import StrEnum


class AssetType(StrEnum):
    REQUEST_BRIEF = "request-brief"
    RESEARCH_FINDING = "research-finding"
    ANALYSIS = "analysis"
    RECOMMENDATION = "recommendation"
    REPORT = "report"


class ArtifactKind(StrEnum):
    """The artifact kinds THIS SAMPLE's report agent attaches.

    Here for the same reason as AssetType above: these five are what a
    document-producing pipeline emits, and they used to be a closed `Literal` in
    `app/common/contracts/base.py` — so a customer whose workflow attaches an audio
    file, a spreadsheet, a CAD drawing or a signed claim form had to edit a FRAMEWORK
    file to describe their own output, and `extra="forbid"` on the envelope meant
    anything unlisted failed validation outright.

    `AssetEnvelope.artifact_type` is a plain `str` now. Narrow it on your own
    contract if you want the check — `artifact_type: Literal["dicom-study"]` — which
    is the useful place to be strict, because it is the place that knows.
    """

    CHART = "chart"
    DOCUMENT = "document"
    LINK = "link"
    PDF = "pdf"
    TABLE = "table"
