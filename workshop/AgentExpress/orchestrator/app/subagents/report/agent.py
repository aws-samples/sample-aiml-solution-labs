"""Report — assembles the final sectioned report (terminal step).

The terminal step (no HITL gate after it). It maps the approved upstream assets
into the sections named in prompts.SECTIONS, each pinned to the
upstream assetIds it draws on, and marks the report complete when every expected
section is present. The whole report is one versioned Report asset.
"""

from __future__ import annotations

import json

from app.common.base import Agent
from app.common.config import upstream_of
from app.common.context import AgentContext
from app.subagents._shared import synthesis
from app.subagents._shared.contracts import Report, ReportSection

from .prompts import SCHEMA, SECTIONS, SYSTEM_PROMPT

# Derived from the workflow.json topology (see app/common/config.upstream_of).
UPSTREAM = upstream_of("report")

# Presentation order, from this agent's own SECTIONS (prompts.py) — the report's
# vocabulary is this agent's business, not the shared contract's. Anything else
# sorts after them (see _sections) rather than being discarded.
_ORDER = {t: i for i, t in enumerate(SECTIONS)}


def _sections(payload: dict) -> list[ReportSection]:
    """Coerce the model's `sections` into ordered ReportSections.

    A section type outside SECTIONS is KEPT (sorted after the expected ones)
    rather than dropped — silently discarding a section the model produced loses
    real content, and a customer's report may legitimately have its own sections.
    """
    out: list[ReportSection] = []
    seen: set[str] = set()
    for s in payload.get("sections", []) or []:
        if not isinstance(s, dict):
            continue
        st = str(s.get("sectionType", "")).strip().lower()
        if not st or st in seen:
            continue
        seen.add(st)
        out.append(ReportSection(
            sectionId=f"section-{st}",
            sectionType=st,
            title=str(s.get("title") or st.replace("-", " ").title()),
            content=str(s.get("content") or "").strip(),
            sourceAssetIds=[str(a) for a in (s.get("sourceAssetIds") or [])],
        ))
    out.sort(key=lambda s: _ORDER.get(s.section_type, 99))
    return out


class ReportAgent(Agent):
    system_prompt = SYSTEM_PROMPT

    async def run(self, ctx: AgentContext) -> str:
        payload, meta = await synthesis.synthesize(
            ctx, upstream_ids=UPSTREAM, system_prompt=SYSTEM_PROMPT, schema=SCHEMA
        )
        sections = _sections(payload)
        present = {s.section_type for s in sections}
        # A report assembled from a cut-off response is NOT complete, whatever sections
        # survived — `isComplete` is what the UI and the reviewer read as "this is
        # ready", and the repaired JSON validates, so this flag is the only thing
        # standing between a truncated report and an approval.
        is_complete = (bool(sections) and all(t in present for t in SECTIONS)
                       and not meta["truncated"])

        asset = Report(
            **synthesis.envelope(ctx, meta, "report"),
            executiveSummary=str(payload.get("executiveSummary") or "").strip() or None,
            title=str(payload.get("title") or meta["title"]).strip(),
            sections=sections,
            isComplete=is_complete,
        )
        return json.dumps(asset.model_dump(by_alias=True, mode="json"), indent=2)


agent = ReportAgent()
