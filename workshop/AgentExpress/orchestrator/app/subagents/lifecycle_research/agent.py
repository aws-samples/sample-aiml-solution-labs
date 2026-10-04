"""Lifecycle Research — when the design's dependencies stop being supported
(step 2, parallel).

THE `type: "openapi"` DEMONSTRATION. Bound by workflow.json to a tool whose target
is a REST API described by an OpenAPI schema, which is how an agent reaches a
service that already speaks HTTP and already has a contract: an internal microservice,
a partner API, a vendor's catalogue. The Gateway loads the schema, publishes each
`operationId` in it as an MCP tool, and translates the call into an HTTP request. No
function to write and no MCP server to run — the API is reached as it already is.

Note what is NOT in this file: no URL, no HTTP verb, no path, no parameter name, no
schema. All of it is the tool's entry in workflow.json plus the schema at
app/tools/lifecycle/openapi.json. Pointing this agent at your own lifecycle service
is a config edit and a schema file; this file does not change.

WHY LIFECYCLE
Every design review eventually asks "how long is this good for", and before this the
answer was silence — correctly, because nothing supplied a support date. That leaves
an agent two options: say nothing useful, or state a date from memory. The second is
the dangerous one. "Python 3.9 is supported until October 2025" reads exactly like a
looked-up fact, and support dates move in both directions, so a stale one is wrong in
a way no reader can spot.

THE SPLIT: JUDGEMENT TO THE MODEL, DATES TO THE API
This agent makes exactly ONE model call, and it is not the one you would expect.

  MODEL   "which versioned products would this design stand on?" — a genuinely
          semantic question. It reads the brief and answers with identifiers.
  CODE    "when do those stop being supported?" — a factual question, answered by
          the REST API and reported verbatim.

The model never sees a date. It cannot shift one, mis-transcribe one, or compare two,
because by the time dates exist its work is finished.

AND THE COMPARISONS ARE THE API'S, NOT OURS
`isEol` and `isEoas` are booleans the API computes against its own
generation timestamp, and this agent reports them rather than comparing `eolFrom` to
a clock of its own. A date is a fact; "is it in the past" is a fact about a date AND
a moment, and the two clocks are not the same one. Deriving it here would also mean
an asset approved at a HITL gate could disagree with the same asset re-read a day
later, for reasons nothing in the run recorded.

WHY A 404 IS NOT A FAILED RUN
An identifier the model proposes may simply not exist in the catalogue — an `openapi`
target is reached BY PATH, so a near-miss is a 404 rather than a fuzzy match. Those
are collected and named in the asset's limitations, one call at a time, because
"three of five products were looked up and these two were not" is actionable and "the
run failed" is not. A tool that cannot be reached AT ALL still raises: see the
re-raise in `_releases` below.
"""
from __future__ import annotations

import json

from app.common import assets, clock
from app.common.base import Agent
from app.common.context import AgentContext
from app.common.contracts.base import AssetStatus, Source
from app.common.errors import ToolDenied, ToolUnavailable
from app.subagents._shared.contracts import Finding, ResearchOutput

from .prompts import PRODUCTS_SCHEMA, SYSTEM_PROMPT

# Roles this agent needs from a release record. Not field names — the tool's
# `rowFields` block in workflow.json maps ITS field names onto these, so pointing the
# tool at a different lifecycle API (an internal one listing your own platform's
# supported versions, say) is a config edit rather than a code change.
_ROLES = ("version", "released", "activeUntil", "supportedUntil", "isEol", "isLts")

# `isEol` IS THE AUTHORITY ON WHETHER SOMETHING IS STILL SUPPORTED, and the field that
# looks like it should be is not. The catalogue also publishes `isMaintained`, which is
# true for Node 12 — whose support ended in 2022 — because it marks membership of the
# maintenance TRACK rather than current support. Reading it as "still supported" produced
# a live asset claiming eight supported Node releases and naming 2022-04-30 as a future
# planning deadline. So this agent maps `isEol` and nothing else onto that question, and
# `isMaintained` is deliberately absent from `rowFields` in workflow.json: a field whose
# name answers the question you are asking and whose value does not is worth leaving out
# rather than leaving available.

# Ceiling on how many products the model may ask about, so a scattergun answer cannot
# turn into a 30-call fan-out. Each product is one HTTP request through the Gateway.
MAX_PRODUCTS = 6

#: Said when the design has no versioned dependency at all — which is a real answer for a
#: system built entirely from managed services, not a failed lookup. The point of the
#: wording is that "no end-of-life dates" must not be read as "no support": the services
#: carry their own commitments, this source does not track them, and the moment someone
#: picks a runtime version there WILL be dates worth having.
NO_VERSIONED_DEPENDENCIES = (
    "This is not a statement that the design is unsupported. Managed services carry "
    "their own support commitments, which this source does not track; a runtime, engine "
    "or OS version chosen later WILL have published dates, so re-run this agent once "
    "one is named.")

# How many releases to name per product. A long-lived product has twenty, and listing
# all of them buries the two a reader acts on. The ones kept are chosen by the API's
# own `isEol`, newest first by release date, so this trims noise rather than evidence.
MAX_RELEASES_NAMED = 4


def _products(payload: dict) -> list[str]:
    """The identifiers the model proposed, cleaned and capped.

    Normalised the way the catalogue spells them (lowercase, hyphenated) rather than
    trusted as typed: the prompt asks for that form, and a model that answers
    "Amazon Linux" instead of "amazon-linux" has given the right answer in the wrong
    alphabet. Fixing it here costs nothing and turns a 404 into a hit.
    """
    out: list[str] = []
    for item in payload.get("products") or []:
        slug = " ".join(str(item).split()).strip().lower().replace(" ", "-")
        # Path segment, so anything that could escape it is not an identifier.
        if slug and slug.replace("-", "").replace(".", "").isalnum() and slug not in out:
            out.append(slug)
    return out[:MAX_PRODUCTS]


def _rows(raw: list[dict], field_map: dict[str, str]) -> list[dict[str, str]]:
    """Release records reduced to this agent's roles, via the configured mapping.

    A record is kept only when it has a version AND at least one date or status to
    report. A release with a name and nothing else says only "this exists", which the
    reader already assumed.
    """
    out: list[dict[str, str]] = []
    for row in raw:
        mapped = {
            role: ("" if row.get(field_map.get(role, role)) is None
                   else str(row.get(field_map.get(role, role))).strip())
            for role in _ROLES
        }
        if mapped["version"] and any(mapped[r] for r in
                                     ("activeUntil", "supportedUntil", "isEol")):
            out.append(mapped)
    return out


def _newest_first(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Releases ordered by their published release date, newest first.

    SORTED, not reversed. This used to reverse the list on the assumption that the
    catalogue returns oldest first; it returns NEWEST first, so reversing inverted every
    ordered claim the asset made — the "newest supported releases" were the oldest four,
    and the "most recent" release to reach end of life was the oldest one on record.
    Ordering that comes from a response's shape is an assumption about someone else's
    API; ordering that comes from a field in it is a fact. Rows with no date sort last
    rather than crashing or silently leading.
    """
    return sorted(rows, key=lambda r: r["released"] or "", reverse=True)


def _truthy(value: str) -> bool:
    """The API's booleans arrive as JSON true/false and survive the row mapping as
    strings. Compared explicitly rather than with bool(), which is True for "false"."""
    return value.strip().lower() in {"true", "1", "yes"}


def _date(value: str) -> str:
    """A date as published, or a phrase saying there is none. Never a guess.

    An unannounced end-of-life is the common case for a current release and it is
    genuinely different from a known far-off one, so it gets said rather than blanked.
    """
    return value if value and value.lower() not in {"none", "null"} else "no announced date"


def _findings(product: str, rows: list[dict[str, str]]) -> list[Finding]:
    """What the API said about one product. Every date here is copied from a record;
    none is derived, and none is compared against a clock in this process."""
    supported = [r for r in rows if not _truthy(r["isEol"])]
    ended = [r for r in rows if _truthy(r["isEol"])]
    out: list[Finding] = []

    if supported:
        named = _newest_first(supported)[:MAX_RELEASES_NAMED]
        detail = "; ".join(
            f"{r['version']}"
            + (" (LTS)" if _truthy(r["isLts"]) else "")
            + f" supported until {_date(r['supportedUntil'])}"
            + (f", active support until {_date(r['activeUntil'])}"
               if r["activeUntil"] else "")
            for r in named)
        more = (f" {len(supported) - len(named)} further supported release(s) are not "
                f"listed here." if len(supported) > len(named) else "")
        out.append(Finding(
            statement=(f"{product}: {len(supported)} release(s) still supported. "
                       f"{detail}.{more} Dates as published by the vendor, not "
                       f"estimates."),
            classification="sourced-fact",
            sourceRef=f"Product lifecycle API, {product}",
        ))

    if ended:
        # A SELECTION, not a calculation: the most recent release that has already
        # ended is the one that tells a reader whether they are behind.
        latest_ended = _newest_first(ended)[0]
        out.append(Finding(
            statement=(f"{product}: {len(ended)} release(s) have already reached "
                       f"end of life, the most recent being {latest_ended['version']} "
                       f"(ended {_date(latest_ended['supportedUntil'])}). Building on "
                       f"one of these means no security updates."),
            classification="sourced-fact",
            sourceRef=f"Product lifecycle API, {product}",
        ))

    if not supported and not ended:
        out.append(Finding(
            statement=(f"{product}: the catalogue returned release records but none "
                       f"carried a support status, so whether it is still supported "
                       f"cannot be stated from this source."),
            classification="sourced-fact",
            sourceRef=f"Product lifecycle API, {product}",
        ))
    return out


def _soonest(by_product: dict[str, list[dict[str, str]]]) -> Finding | None:
    """The supported release whose support ends first, across every product.

    A SELECTION over published dates, not arithmetic on them: the date is quoted from
    the record it came from. This is the one cross-product observation a reader always
    wants, because the earliest date is what sets the next planning deadline. It says
    nothing about how far away that is — that would need a clock, and the API's own
    booleans are the only "is it past" this agent trusts.
    """
    dated = [
        (r["supportedUntil"], product, r["version"])
        for product, rows in by_product.items()
        for r in rows
        if not _truthy(r["isEol"]) and r["supportedUntil"]
        and r["supportedUntil"][:4].isdigit()
    ]
    if len(dated) < 2:
        return None
    # ISO-8601 sorts correctly as text, which is why no date is parsed here.
    when, product, version = min(dated)
    return Finding(
        statement=(f"Of the supported releases found, {product} {version} is the "
                   f"first to lose support, on {when}. That date is the earliest "
                   f"planning deadline this dependency set implies."),
        classification="sourced-fact",
        sourceRef=f"Product lifecycle API, {product}",
    )


class LifecycleResearchAgent(Agent):
    system_prompt = SYSTEM_PROMPT

    async def run(self, ctx: AgentContext) -> str:
        brief = assets.brief(ctx)
        brief_text = assets.for_prompt(brief) if brief else ctx.topic

        # --- the ONE model call: which products does this design stand on? ------
        payload = assets.extract_json(await ctx.llm(
            SYSTEM_PROMPT,
            f"=== APPROVED REQUEST ===\n{brief_text}\n\n"
            f"List the versioned products this design would depend on, most "
            f"load-bearing first, at most {MAX_PRODUCTS}. Give the catalogue "
            f"identifier only — you are NOT asked for dates or versions, and you "
            f"will not be shown any.\n\nReturn ONLY JSON matching this schema:\n"
            f"{PRODUCTS_SCHEMA}")) or {}
        products = _products(payload)
        truncated_note = ([
            ("The product list was cut off by this agent's output token limit, so the "
             "dates below cover fewer dependencies than the design has. Raise "
             "`maxTokens` for this agent in workflow.json.")
        ] if ctx.truncated_calls else [])
        version = assets.prior_version(ctx)
        title = assets.brief_title(brief, ctx)

        if not products:
            # AN EMPTY LIST IS AN ANSWER, NOT A FAILURE, and the `basis` is the whole of
            # it — so it is carried through rather than replaced by a fixed sentence.
            # A design built entirely from managed services has no support dates to
            # report, and saying WHICH services led to that conclusion is what makes the
            # finding checkable. The earlier fixed wording ("the dependency set could
            # not be established") described a lookup that went wrong, which for this
            # case is simply untrue and invites a reviewer to re-run it.
            basis = " ".join(str(payload.get("basis") or "").split())
            return _asset(
                ctx, title, version,
                summary=("No dependency in this design publishes a support lifecycle, "
                         "so there are no end-of-life dates to report."
                         + (f" {basis}" if basis else "")),
                findings=[],
                limits=[NO_VERSIONED_DEPENDENCIES, *truncated_note],
                sources=[],
            )

        # --- the tool: real dates for exactly those products --------------------
        by_product: dict[str, list[dict[str, str]]] = {}
        unresolved: list[str] = []
        field_map: dict[str, str] = {}
        for product in products:
            rows, field_map = await self._releases(ctx, product)
            if rows:
                by_product[product] = rows
            else:
                unresolved.append(product)

        findings: list[Finding] = []
        for product, rows in by_product.items():
            findings += _findings(product, rows)
        soonest = _soonest(by_product)
        if soonest:
            findings.append(soonest)

        moving = ("Support dates are vendor commitments and do change — they are "
                  "extended and occasionally brought forward. These are the dates "
                  "published at the time of this run.")
        limits = [moving, *truncated_note]
        if unresolved:
            limits.insert(0, (
                "No lifecycle record was found for "
                + ", ".join(unresolved)
                + ", so those are absent from the findings above and from anything "
                  "derived from them. Either the product is not in this catalogue, "
                  "or the identifier does not match the one it is filed under. This "
                  "is NOT a statement that the product is unsupported."))
        if not by_product:
            limits.insert(0, (
                "No lifecycle record was found for any of: " + ", ".join(products)
                + ". Looked for ["
                + ", ".join(field_map.get(r, r) for r in _ROLES) + "]."))

        looked_up = list(by_product)
        summary = (
            f"Checked support and end-of-life dates for {len(looked_up)} of "
            f"{len(products)} dependency(ies) this design would stand on: "
            f"{', '.join(looked_up)}. "
            + (f"No lifecycle record was found for {', '.join(unresolved)}. "
               if unresolved else "")
            + "Dates are as published by each vendor; this agent states no date "
              "that did not come from the catalogue."
        ) if by_product else (
            f"Identified {len(products)} dependency(ies) this design would stand on "
            f"({', '.join(products)}), but no lifecycle record was returned for them."
        )

        return _asset(
            ctx, title, version,
            summary=summary,
            findings=findings,
            limits=limits,
            sources=[Source(
                sourceId="product-lifecycle-api",
                sourceType="rest-api",
                sourceName="Product lifecycle REST API (via OpenAPI Gateway target)",
            )] if by_product else [],
        )

    async def _releases(self, ctx: AgentContext, product: str):
        """One product's release records, or ([], field_map) if it is not in the
        catalogue.

        THE DISTINCTION THIS DRAWS. `ToolUnavailable` and `ToolDenied` mean the tool
        itself is unreachable or the Cedar policy refused the call — conditions that
        apply to every product equally and that the run must fail on, loudly, rather
        than report as "nothing found" for six products in a row. Anything else on a
        single call is treated as that identifier not resolving: an `openapi` target
        is addressed BY PATH, so a wrong identifier is an HTTP error and not an empty
        result set, and one bad guess out of six should cost one finding rather than
        the whole agent.
        """
        try:
            rows, field_map, _mode = await ctx.call_tool_rows(ctx.tool or "lifecycle",
                                                             product)
        except (ToolUnavailable, ToolDenied):
            raise
        except Exception:  # noqa: BLE001 - see the docstring: anything else here is
            # "this identifier did not resolve", and the Gateway's HTTP-error type for
            # an openapi target is not part of its contract. Narrowing this to the
            # exception classes observed today would turn the next unlisted one into a
            # failed run for what is a wrong guess about a product name.
            return [], {}
        return _rows(rows, field_map), field_map


def _asset(ctx, title, version, *, summary, findings, limits, sources) -> str:
    asset = ResearchOutput(
        assetId=f"asset-research-{ctx.agent_id}-{assets.slug(str(title))}-v{version}",
        version=version,
        status=AssetStatus.IN_REVIEW,
        createdAt=clock.now_et(),  # Eastern wall-clock
        createdByAgent=ctx.agent_id,
        executiveSummary=summary,
        summary=summary,
        findings=findings,
        dataLimitations=limits,
        sources=sources,
    )
    return json.dumps(asset.model_dump(by_alias=True, mode="json"), indent=2)


agent = LifecycleResearchAgent()
