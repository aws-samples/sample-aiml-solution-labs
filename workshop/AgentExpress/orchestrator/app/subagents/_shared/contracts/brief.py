"""Request brief contract — the step-1 (Intake) output.

Turns the user's free-text request into a structured brief the rest of the
pipeline can consume: a clear objective, the scope, the specific questions to
answer, any constraints, and the open questions a human should confirm.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from app.common.contracts.base import AssetEnvelope, Base

from .types import AssetType


class Scope(Base):
    """What is in and out of scope, as two lists.

    Structured rather than a single string because that is what the model
    naturally produces for "define the scope (what is in and out)", and because a
    downstream agent can then read the two halves separately.

    It was a plain `str`, and the intake agent coerced with `str(payload["scope"])`.
    When the model returned an object — which it did — that produced a PYTHON REPR
    with single quotes:

        "scope": "{'inScope': [...], 'outOfScope': [...]}"

    which is not JSON, and was then pasted into every downstream agent's prompt.
    `summary` keeps a plain-string scope working, so an asset written before this
    change still validates.
    """

    in_scope: list[str] = Field(default_factory=list, alias="inScope")
    out_of_scope: list[str] = Field(default_factory=list, alias="outOfScope")
    summary: str = ""

    @model_validator(mode="before")
    @classmethod
    def _accept_a_plain_string(cls, v):
        if isinstance(v, str):
            return {"summary": v}
        return v


class RequestBrief(AssetEnvelope):
    asset_type: Literal[AssetType.REQUEST_BRIEF] = Field(
        default=AssetType.REQUEST_BRIEF, alias="assetType"
    )

    title: str
    objective: str = ""
    scope: Scope = Field(default_factory=Scope)
    key_questions: list[str] = Field(default_factory=list, alias="keyQuestions")
    constraints: list[str] = Field(default_factory=list)
    assumptions: list[str] = Field(default_factory=list)
    open_questions: list[str] = Field(default_factory=list, alias="openQuestions")
