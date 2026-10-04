"""Cost Research — what the requested use case costs to run (step 2, parallel).

The `type: "lambda"` demonstration. Bound by workflow.json to a tool whose target
is an AWS Lambda function, which is how an agent reaches anything the Gateway
cannot reach directly: a warehouse (Redshift, Snowflake), any RDBMS, an internal
service, a resource inside a VPC, or — as here — a control-plane API with no MCP
server in front of it. Here the function queries the AWS Price List Query API.

Note what is NOT in this file: no Lambda ARN, no argument names, no tool name, no
IAM. All of it is the tool's entry in workflow.json — `source` (or `lambdaArn`),
`toolSchema`, `call` and `arg`. Swapping the function for one that prices your own
private rate card is a config edit, and this file does not change.

WHY COST
Whatever the use case, someone will ask what it costs, and before this the answer
was always "no cost figures were supplied" — correctly, because nothing supplied
any. That leaves an agent two options: say nothing useful, or invent a number. The
second is the dangerous one. A fabricated "$50,000 budget" is indistinguishable
from a real estimate once it is in a report, which is why this agent asks the model
for a service LIST and never for a number.

THE SPLIT: JUDGEMENT TO THE MODEL, ARITHMETIC TO CODE
This agent makes exactly ONE model call, and it is not the one you would expect.

  MODEL   "which AWS services would this use case need?" — a genuinely semantic
          question. It reads the brief and answers with service names.
  CODE    "what do those services cost?" — a factual question, answered by the
          tool against the real Price List API, and reported verbatim.

The model never sees a price. It cannot round one, mis-transcribe one, or multiply
two together, because by the time prices exist its work is finished. That is the
same division a customer deployment of this framework draws for money: budget
splits and creator mixes are computed in code and the model is told plainly that
"the arithmetic is computed deterministically by the system; your job is the
PLANNING NARRATIVE".

WHY THERE IS NO TOTAL, AND WHY THAT IS THE HONEST ANSWER
A total needs volumes — how many tokens per request, how many requests per month,
how much stored. Those are properties of the customer's workload, not of AWS, and
a five-word request contains none of them. An agent that multiplies a real rate by
a volume nobody gave it has invented the volume, and the invented half is the half
that makes the total wrong.

So this agent publishes the RATES and names the volumes a total would need. If a
later agent then states a total, it must have got the volumes from somewhere real —
and if it invented them, `app/common/grounding.py` flags it on the timeline, because
the figure appears in no upstream asset.

This paragraph used to claim `unsupported-figure` caught that. It never could:
`EvidenceClass` is sourced-fact | calculation | assumption | agent-interpretation, so
the value could not occur and nothing was checking figures at all. A comment
describing a guardrail that does not exist is worse than no comment, because it stops
anyone looking for the real one.
"""
from __future__ import annotations

import json

from app.common import assets, clock
from app.common.base import Agent
from app.common.context import AgentContext
from app.common.contracts.base import AssetStatus, Source
from app.subagents._shared.contracts import Finding, ResearchOutput

from .prompts import SERVICES_SCHEMA, SYSTEM_PROMPT

# Roles this agent needs from a priced row. Not field names — the tool's
# `rowFields` block in workflow.json maps ITS field names onto these, so pointing
# the tool at a private rate card is a config edit rather than a code change.
_ROLES = ("service", "dimension", "unit", "price", "currency", "region",
          # The caller's own wording, so a service that came back unpriced can be
          # named. AWS's `servicename` differs from what anyone asks for.
          "requested")

# Ceiling on how many services the model may ask to price, so a scattergun answer
# cannot turn into a 40-service payload. The tool caps again on its own side.
MAX_SERVICES = 8




def _services(payload: dict) -> list[str]:
    """The service names the model proposed, cleaned and capped."""
    raw = payload.get("services") or []
    out: list[str] = []
    for item in raw:
        name = str(item).strip()
        if name and name.lower() not in {n.lower() for n in out}:
            out.append(name)
    return out[:MAX_SERVICES]


def _rows(raw: list[dict], field_map: dict[str, str]) -> list[dict[str, str]]:
    """Priced rows reduced to this agent's roles, via the configured mapping.

    A row is kept only when it has a service, a price and a unit: a rate without
    its unit is not a rate a reader can use, and reporting one would be worse than
    omitting it.
    """
    out: list[dict[str, str]] = []
    for row in raw:
        mapped = {
            role: str(row.get(field_map.get(role, role)) or "").strip()
            for role in _ROLES
        }
        if mapped["service"] and mapped["price"] and mapped["unit"]:
            out.append(mapped)
    return out


# A unit tells you which volume a total would need. Matched on the unit the tool
# returned rather than assumed from the use case, because the hardcoded list this
# replaced asked a SERVERLESS DATA PIPELINE for "average input and output tokens
# per model call" — a question about an LLM, carried into the asset and then into
# the analysis, on a run that priced Glue and Kinesis. The volumes a total needs
# are a property of the RATES you actually have.
# ORDER IS SIGNIFICANT AND MOST-SPECIFIC WINS: each unit contributes exactly one
# question, taken from the first rule it matches. Scanning every rule against every
# unit instead made `Lambda-GB-Second` ask for data transfer as well as compute
# time, because it contains "gb".
_VOLUME_BY_UNIT: tuple[tuple[tuple[str, ...], str], ...] = (
    (("token",),
     "tokens per call, and calls per month"),
    (("gb-second",),
     "memory size multiplied by execution time, per invocation"),
    (("shardhour", "streamhr"),
     "how many shards or streams are provisioned, and for how long"),
    (("gb-mo", "gb-month", "obj-month", "bytehrs"),
     "data stored (GB) and how long it is retained"),
    (("terabyte",),
     "data scanned per query, and queries per month"),
    (("dpu-hour", "node-hour", "rpu-hr", "vcpu-hour", "hrs", "hour"),
     "hours of compute per month, and how many units run in parallel"),
    (("gb",),
     "data moved between services (GB) per month"),
    (("request", "object", "statetransition", "notification", "session", "job",
      "key", "unit"),
     "requests per month"),
)
# Said when there are no rows to reason from, so the sentence is never empty.
_VOLUMES_FALLBACK = ("the usage volumes for each priced dimension, over a stated "
                     "period")


def _volumes_needed(rows: list[dict[str, str]]) -> list[str]:
    """The volumes a total would need, derived from the units actually priced."""
    units = {r["unit"].strip().lower() for r in rows if r["unit"]}
    matched: set[str] = set()
    for unit in units:
        for keys, question in _VOLUME_BY_UNIT:
            if any(k in unit for k in keys):
                matched.add(question)
                break
    # Table order, not set order, so the sentence reads the same way every run.
    out = [q for _keys, q in _VOLUME_BY_UNIT if q in matched]
    return out or [_VOLUMES_FALLBACK]


def _rate(row: dict[str, str]) -> str:
    """One rate, exactly as published. Trailing zeros trimmed for readability —
    the VALUE is never altered, only how many zeros of it are printed."""
    price = row["price"]
    if "." in price:
        price = price.rstrip("0").rstrip(".") or "0"
    cur = f" {row['currency']}" if row["currency"] else ""
    return f"{price}{cur} per {row['unit']}"


def _findings(rows: list[dict[str, str]], region: str) -> list[Finding]:
    """The published rates, grouped by service. Every figure here is copied from a
    row; none is derived, and none is multiplied by anything."""
    by_service: dict[str, list[dict[str, str]]] = {}
    for r in rows:
        by_service.setdefault(r["service"], []).append(r)

    out: list[Finding] = []
    for service, items in by_service.items():
        rates = "; ".join(f"{i['dimension']} at {_rate(i)}" for i in items)
        out.append(Finding(
            statement=(f"{service} on-demand rates in {region}: {rates}. "
                       f"Published unit prices, not a forecast."),
            classification="sourced-fact",
            sourceRef=f"AWS Price List Query API, {items[0].get('region') or region}",
        ))

    # The cheapest and dearest model token rate, when models were priced. This is a
    # comparison a reader always wants and it is a SELECTION, not a calculation —
    # both numbers are quoted from rows.
    token_rows = [r for r in rows if "token" in r["unit"].lower()]
    if len(token_rows) > 1:
        cheapest = min(token_rows, key=lambda r: float(r["price"]))
        dearest = max(token_rows, key=lambda r: float(r["price"]))
        out.append(Finding(
            statement=(f"Token rates span {_rate(cheapest)} ({cheapest['service']}, "
                       f"{cheapest['dimension']}) to {_rate(dearest)} "
                       f"({dearest['service']}, {dearest['dimension']}). Model "
                       f"choice therefore moves the per-call cost by a large "
                       f"multiple, before any volume is assumed."),
            classification="sourced-fact",
            sourceRef="AWS Price List Query API",
        ))
    return out


class CostResearchAgent(Agent):
    system_prompt = SYSTEM_PROMPT

    async def run(self, ctx: AgentContext) -> str:
        brief = assets.brief(ctx)
        brief_text = assets.for_prompt(brief) if brief else ctx.topic

        # --- the ONE model call: which services does this use case need? --------
        payload = assets.extract_json(await ctx.llm(
            SYSTEM_PROMPT,
            f"=== APPROVED REQUEST ===\n{brief_text}\n\n"
            f"List the AWS services this use case would run on, most significant "
            f"cost first, at most {MAX_SERVICES}. Name the service only — you are "
            f"NOT asked for prices, quantities, or a total, and you will not be "
            f"shown any.\n\nReturn ONLY JSON matching this schema:\n"
            f"{SERVICES_SCHEMA}")) or {}
        services = _services(payload)
        # A cut-off service list means we priced fewer services than the use case
        # needs, and the summary's "N of M" would be counting a truncated M.
        truncated_note = ([
            ("The service list was cut off by this agent's output token limit, so the "
             "prices below cover fewer services than the use case needs. Raise "
             "`maxTokens` for this agent in workflow.json.")
        ] if ctx.truncated_calls else [])
        version = assets.prior_version(ctx)
        title = assets.brief_title(brief, ctx)

        if not services:
            # The model gave nothing usable. Degrade to a valid asset that says so,
            # rather than pricing a guessed service list.
            return _asset(
                ctx, title, version,
                summary=("No AWS services could be derived for this request, so no "
                         "prices were looked up."),
                findings=[],
                limits=[("The service list for this use case could not be "
                         "established, so nothing was priced."), *truncated_note],
                sources=[],
            )

        # --- the tool: real prices for exactly those services -------------------
        # Still RAISES on a tool that cannot be reached (ToolUnavailable /
        # ToolDenied) so the run fails with the reason on this agent rather than
        # continuing without the evidence.
        raw, field_map, _mode = await ctx.call_tool_rows(
            ctx.tool or "pricing", ", ".join(services))
        rows = _rows(raw, field_map)
        region = (rows[0]["region"] if rows else "") or "the deployment region"

        priced = {r["service"] for r in rows}
        # Which of the names WE asked for produced no rate. Compared on the
        # caller's own wording, carried back on each row, because AWS's
        # `servicename` never matches it: "Amazon SQS" comes back as "Amazon
        # Simple Queue Service". Without this the asset could only say how MANY
        # went unpriced, which is the one thing a reader cannot act on.
        #
        # Guarded on the field being PRESENT. `requested` is optional in
        # `rowFields`, and a tool that does not supply it leaves every value empty
        # — which would make this list every service as unpriced, turning a config
        # omission into a false claim about AWS. When no row carries it we cannot
        # tell, so we say nothing.
        answered = {r["requested"].strip().lower() for r in rows if r["requested"]}
        unpriced = ([s for s in services if s.strip().lower() not in answered]
                    if answered else [])
        no_total = ("These are UNIT PRICES, not a total. A total needs volumes this "
                    "request does not state: "
                    + "; ".join(_volumes_needed(rows)) + ".")
        excluded = ("Free-tier allowances, committed-use discounts, private pricing "
                    "and data transfer between services are not included.")
        limits = [no_total, excluded, *truncated_note]
        if unpriced and rows:
            limits.insert(0, (
                "No published unit price was returned for "
                + ", ".join(unpriced)
                + ", so those are absent from the rates above and from anything "
                  "derived from them. Either the name could not be resolved to an "
                  "AWS pricing service code, or the service publishes no "
                  "on-demand consumption rate in this region."))
        if not rows:
            limits.insert(0, (
                "No published price was returned for any of: "
                + ", ".join(services)
                + f". Looked for [{', '.join(field_map.get(r, r) for r in _ROLES)}]"
                + (f"; the rows carry [{', '.join(sorted(raw[0]))}]." if raw
                   else " and the tool returned no rows.")))

        # HOW MANY OF THE NAMES WE ASKED FOR CAME BACK PRICED — not how many AWS
        # servicenames the rows carry. Those are different populations, and the
        # summary used to divide one by the other: asking for 5 services where one
        # was "Amazon Bedrock" returned 10 distinct servicenames, because Bedrock
        # prices per model ("Claude Opus 4.7 (Amazon Bedrock Edition)" and five
        # siblings), and the asset reported "Priced 10 of 5 service(s)". Observed
        # live. A name that fans out is normal and the rates are all real; only the
        # arithmetic was wrong.
        #
        # `unpriced` is only knowable when the rows carry the caller's own wording
        # (the optional `requested` field in `rowFields`). Without it we cannot say
        # which of our names resolved, so we count what we can defend: the
        # servicenames actually priced, described as such.
        if answered:
            resolved = len(services) - len(unpriced)
            counted = (f"Priced {resolved} of {len(services)} service(s) this use "
                       f"case would run on")
            # A fan-out is worth one clause, so a reader is not left wondering why
            # five services list ten names.
            fanned = (f" ({len(priced)} priced dimensions, as some services price "
                      f"per model or per tier)" if len(priced) > resolved else "")
        else:
            counted = (f"Priced {len(priced)} service(s) for the {len(services)} this "
                       f"use case would run on")
            fanned = ""

        summary = (
            counted + fanned
            + ", from the AWS Price List Query API"
            + (f" in {region}" if rows else "")
            + f": {', '.join(sorted(priced)) if priced else 'none'}. "
            + (f"No rate was returned for {', '.join(unpriced)}. " if unpriced else "")
            + "Published unit rates only — a monthly total needs usage volumes "
              "this request does not state."
        ) if rows else (
            f"Identified {len(services)} service(s) this use case would run on "
            f"({', '.join(services)}), but no published unit price was returned "
            f"for them."
        )

        return _asset(
            ctx, title, version,
            summary=summary,
            findings=_findings(rows, region),
            limits=limits,
            sources=[Source(
                sourceId="aws-price-list",
                sourceType="aws-price-list",
                sourceName=f"AWS Price List Query API ({region})",
            )] if rows else [],
        )


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


agent = CostResearchAgent()
