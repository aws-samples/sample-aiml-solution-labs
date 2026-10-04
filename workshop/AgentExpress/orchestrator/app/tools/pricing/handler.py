"""
Gateway Lambda target: real AWS unit prices, as a tool.

This is the BUILT-IN demo function for `type: "lambda"` in workflow.json — the
tool type that lets an agent reach anything the Gateway cannot reach directly (a
warehouse, an RDBMS, an internal service, something inside a VPC, or as here a
control-plane API with no MCP server in front of it). Every other tool type ships
with a live agent proving it works; this exists so that one does too.

It publishes ONE tool:

  aws_prices — given a list of AWS services, the REAL on-demand unit prices for
               them in a region, from the AWS Price List Query API.

WHY PRICING
Whatever a customer's use case is, it costs money to run, and the cost is the one
question a design review always asks. Before this, every run of this workflow
reported "no cost figures were supplied" — correctly, because nothing supplied
any. An agent then either says nothing useful about cost or invents a number, and
inventing it is the worse failure: a fabricated "$5,000/month" is indistinguishable
from a real estimate once it is in a report.

WHAT IT DOES NOT DO — AND THIS IS THE POINT
It returns UNIT PRICES, never a total. A total needs volumes (how many tokens, how
many requests, how much storage) and those are a property of the customer's
workload, not of AWS. An agent that multiplies a real rate by a volume nobody gave
it has invented the volume, and the invented part is the part that makes the total
wrong. So this tool supplies the rates; the agent states them, names the volume
assumptions it would need, and leaves the arithmetic to whoever knows the volumes.

Nothing here is generated, sampled or stubbed. Prices come from the Price List
Query API for the region asked about. A service that cannot be resolved is
REPORTED as unresolved rather than guessed at, and a region with no published
price for a dimension yields no row rather than a plausible one.

Replacing this with your own function is the point. Change the `tools` entry to
`"lambdaArn": "<your function>"` and the framework stops deploying anything — it
registers yours and grants the Gateway permission to invoke it.

Invocation contract (identical to kb_lambda/handler.py): the Gateway passes the
tool arguments as `event`, and the tool name as
`context.client_context.custom["bedrockAgentCoreToolName"]`, formatted
"<targetName>___<toolName>".
"""

import json
import os
import re

import boto3
from botocore.config import Config

# The Price List Query API is published in only two regions. It describes prices
# for EVERY region, so this is about where the endpoint lives, not what it can
# answer about. Pinned rather than taken from AWS_REGION, which would break the
# tool on a deployment anywhere else.
PRICING_ENDPOINT_REGION = os.environ.get("PRICING_API_REGION", "us-east-1")

# The region prices are requested FOR, when the caller does not say.
DEFAULT_REGION = os.environ.get("AWS_REGION", "us-east-1")

# Ceilings, so a broad request cannot return a payload that blows the agent's
# context window or the Gateway's response limit.
MAX_SERVICES = 8        # services per call
MAX_ROWS_PER_SERVICE = 12
MAX_PAGES_PER_SERVICE = 40   # ~0.05s per page; AgentCore needs ~30
MAX_OUTPUT_CHARS = 300

_pricing = boto3.client(
    "pricing",
    region_name=PRICING_ENDPOINT_REGION,
    # Retries because a whole agent's cost analysis should not fail on one
    # throttle, and a short timeout because the Gateway caps how long it waits.
    config=Config(retries={"max_attempts": 3, "mode": "standard"},
                  connect_timeout=5, read_timeout=20),
)

# ---------------------------------------------------------------------------
# Service resolution: a name a model wrote -> an AWS ServiceCode
# ---------------------------------------------------------------------------
#
# An explicit table, in one place, exactly like the mapping tables a production
# deployment keeps for its own source columns. A model asked "which AWS services
# does this use case need?" answers in prose ("Bedrock", "Lambda", "a vector
# store"), and ServiceCodes are neither guessable nor stable enough to infer:
# Step Functions is `AmazonStates`, SQS is `AWSQueueService`, and Bedrock is split
# across `AmazonBedrock`, `AmazonBedrockFoundationModels` and
# `AmazonBedrockAgentCore`.
#
# Unknown names are NOT guessed. They fall through to a conservative match against
# the live service list, and if that finds nothing they are returned in
# `unresolved` so the agent can say which services it could not price — a named
# gap instead of a silent omission.
ALIASES = {
    # Models and agents
    "bedrock": "AmazonBedrockFoundationModels",
    "amazon bedrock": "AmazonBedrockFoundationModels",
    "bedrock foundation models": "AmazonBedrockFoundationModels",
    "foundation models": "AmazonBedrockFoundationModels",
    "llm": "AmazonBedrockFoundationModels",
    "claude": "AmazonBedrockFoundationModels",
    "bedrock agentcore": "AmazonBedrockAgentCore",
    "agentcore": "AmazonBedrockAgentCore",
    "bedrock knowledge base": "AmazonBedrockAgentCore",
    "knowledge base": "AmazonBedrockAgentCore",
    "bedrock guardrails": "AmazonBedrock",
    "guardrails": "AmazonBedrock",
    # Compute
    "lambda": "AWSLambda",
    "aws lambda": "AWSLambda",
    "fargate": "AmazonECS",
    "ecs": "AmazonECS",
    "eks": "AmazonEKS",
    "ec2": "AmazonEC2",
    # Data
    "dynamodb": "AmazonDynamoDB",
    "s3": "AmazonS3",
    "s3 vectors": "AmazonS3",
    "vector store": "AmazonS3",
    "rds": "AmazonRDS",
    "aurora": "AmazonRDS",
    "opensearch": "AmazonES",
    "elasticache": "AmazonElastiCache",
    # Integration
    "api gateway": "AmazonApiGateway",
    "apigateway": "AmazonApiGateway",
    "step functions": "AmazonStates",
    "stepfunctions": "AmazonStates",
    "sqs": "AWSQueueService",
    "sns": "AmazonSNS",
    "eventbridge": "AmazonEventBridge",
    "kinesis": "AmazonKinesis",
    "kinesis data streams": "AmazonKinesis",
    "firehose": "AmazonKinesisFirehose",
    "kinesis data firehose": "AmazonKinesisFirehose",
    "data firehose": "AmazonKinesisFirehose",
    # Analytics — a data pipeline names these as often as it names Lambda, and
    # `kinesis` alone is ambiguous across three catalog codes.
    "glue": "AWSGlue",
    "athena": "AmazonAthena",
    "redshift": "AmazonRedshift",
    "redshift serverless": "AmazonRedshift",
    # Operations
    "cloudwatch": "AmazonCloudWatch",
    "cloudfront": "AmazonCloudFront",
    "secrets manager": "AWSSecretsManager",
    "kms": "awskms",
    "cognito": "AmazonCognito",
    # AI services
    "textract": "AmazonTextract",
    "comprehend": "comprehend",
    "transcribe": "AmazonTranscribe",
    "polly": "AmazonPolly",
    "rekognition": "AmazonRekognition",
    "sagemaker": "AmazonSageMaker",
}

# Usage types that are NOT what a consumption estimate needs. This exclusion is the
# single most important line in the file: without it, "price AWS Lambda" returns
# managed-instance hourly rates for c7i.8xlarge, and "price AgentCore" returns
# several hundred EC2-shaped instance SKUs — real prices, for a question nobody
# asked, drowning the handful of rates that matter.
_EXCLUDE = re.compile(
    r"Instance-based|Managed-Instances|Provisioned|Reserved|Dedicated|SpotUsage"
    r"|DedicatedUsage|HostBoxUsage|CapacityUnit-Hrs|ExtendedSupport"
    # `-Mirror` and `:Mirror` both occur: RDS writes `RDS:Mirror-GP2-Storage`.
    r"|[-:]Mirror"
    # `InstanceUsage:db.m8i.large` is an instance rate that none of the patterns
    # above spell. RDS has no curated entry, so it falls back to cheapest-first
    # across everything not excluded here — which returned a list of database
    # instance sizes where a reader wanted a cost dimension. This is the safety
    # net for every service the table does not name.
    r"|InstanceUsage"
    # Committed-capacity rates, spelled `-CR-1YR-AU` on Redshift Serverless. The
    # asset says in as many words that committed-use pricing is NOT included, and
    # one of these came back as "2430.00 USD per RPU-Hr" — an annual upfront
    # amount wearing an hourly unit, beside a real on-demand rate of 0.375.
    r"|-CR-\d+YR"
    # `IA-` is DynamoDB/S3 Infrequent Access — a different storage class with its
    # own rates. Excluded explicitly because it is also a two-letter uppercase
    # prefix, so the region-prefix tolerance below would otherwise treat
    # `IA-ReadRequestUnits` as the standard read rate.
    r"|(?:^|-)IA-"
    # Redshift writes its provisioned node and concurrency-scaling SKUs as
    # `Node:ra3.4xlarge` and `CS:dc2.large` — instance rates by another spelling,
    # and 36 of its 40 published rates. Without these two, asking for Redshift
    # returns a price list of node sizes instead of a cost dimension.
    r"|^Node:|^CS:",
    re.IGNORECASE,
)

# Usage types carry a REGION PREFIX outside us-east-1: `Lambda-GB-Second` in
# N. Virginia is `EU-Lambda-GB-Second` in Ireland and `APS1-Lambda-GB-Second` in
# Mumbai. Without tolerating it, every curated pattern below misses on every region
# except one, and the tool silently falls back to an unfiltered sample — which it
# labels honestly, but which is worse for a customer who deploys in Frankfurt.
_REGION_PREFIX = r"(?:[A-Z]{2,5}\d?-)?"


def _dim(*names: str) -> re.Pattern:
    """A pattern matching exactly these usage types, in any region."""
    return re.compile(
        r"^" + _REGION_PREFIX + r"(?:" + "|".join(names) + r")$", re.IGNORECASE)

# The PRIMARY consumption dimensions per service — the handful an estimate is
# actually built from.
#
# Needed because excluding the instance SKUs is not enough on its own. A large
# service publishes hundreds of legitimate rates, and picking by price alone
# surfaces the obscure ones: "price AWS Lambda" led with SnapStart-cached
# GB-seconds and Lambda@Edge, and "price Bedrock" led with image generation and
# video embedding models — all real, none of them what a reader estimating an
# LLM application needs.
#
# Anchored (^...$) where a name would otherwise match its own variants, since
# `Request` is a prefix of `Requests-Tier1`, `Lambda-Managed-Instances-Request`
# and several more.
#
# A service absent from this table still works: it falls back to cheapest-first
# across everything not excluded, and the response says the selection was
# unfiltered so the agent does not present it as curated.
_PREFERRED = {
    # Text in/out on the standard (non-batch) tier, which is how an LLM
    # application is charged. One row per model, so a reader can compare.
    "AmazonBedrockFoundationModels": re.compile(
        r"_input_tokens_standard-|_output_tokens_standard-", re.IGNORECASE),
    # AgentCore publishes ~27 consumption dimensions, several of them variants of
    # each other (`-v2`, per-tier, built-in vs custom memory). Naming the primary
    # ones keeps Runtime vCPU/Memory in the answer: they are the dimensions an
    # agentic workload is mostly billed on, and they are also the most expensive
    # PER UNIT, so a cheapest-first cut across all 27 dropped exactly them.
    "AmazonBedrockAgentCore": re.compile(
        r"Runtime:Consumption-based:(vCPU|Memory)$"
        r"|Gateway:Consumption-based:API-Invocations"
        r"|Memory:Consumption-based:(Short-Term-Memory|Long-Term-Memory-Retrieval)"
        r"|Knowledge-Base:Consumption-based:(Retrieval|Storage)"
        r"|WebSearchTool:Consumption-based:Queries"
        r"|Evaluations:Consumption-based:BuiltIn-(Input|Output):Tier1",
        re.IGNORECASE),
    "AWSLambda": _dim("Lambda-GB-Second", "Request"),
    "AmazonDynamoDB": _dim("ReadRequestUnits", "WriteRequestUnits",
                           "TimedStorage-ByteHrs"),
    # S3 publishes 128 priced usage types in one region — retrieval tiers, early
    # deletion, checksums, inventory, replication — so this is the clearest case
    # for curation in the table. S3 VECTORS IS DELIBERATELY ABSENT: it is a
    # separate product, and carrying its two rows meant every pipeline, website
    # and backup estimate was handed vector-store rates it had no use for. An
    # agentic workload gets its vector cost from the AgentCore Knowledge-Base
    # dimensions instead, which are priced for what that workload actually calls.
    "AmazonS3": _dim("TimedStorage-ByteHrs", "Requests-Tier1", "Requests-Tier2"),
    "AmazonApiGateway": _dim("ApiGatewayRequest", "ApiGatewayHttpRequest"),
    "AmazonCloudWatch": _dim("CW:Requests", "DataProcessing-Bytes",
                             "TimedStorage-ByteHrs"),
    # Verified against the live API. The previous patterns were guesses and both
    # matched nothing: SQS bills `Requests-RBP` (not `Requests-Tier1`), and Step
    # Functions bills `StepFunctions-Request`, not a bare `Request`.
    "AWSQueueService": _dim("Requests-RBP", "Requests-FIFO-RBP"),
    "AmazonStates": _dim("StateTransition", "StepFunctions-Request",
                         "StepFunctions-GB-Second"),
    "AmazonSNS": _dim("Requests-Tier1", "DeliveryAttempts-HTTP"),
    # Glue publishes 21 rates, most of them adjacent products: DataBrew
    # (`DBrew-*`), zero-ETL, interactive sessions, dev endpoints, materialized
    # views. Cheapest-first surfaced DataBrew sessions and zero-ETL ingestion —
    # real prices for services the caller did not ask about. ETL DPU-hours, both
    # generations and the Flex tier, plus the catalog and crawler, are what an ETL
    # estimate is built from.
    "AWSGlue": _dim("ETL-DPU-Hour", "ETL-DPU-Hour-Gen2", "ETL-Flex-DPU-Hour",
                    "ETL-Flex-DPU-Hour-Gen2", "Crawler-DPU-Hour",
                    "Catalog-Request", "Catalog-Storage"),
    # `AmazonKinesis` covers Data Streams AND the Firehose delivery channels, so
    # an uncurated cut returned twelve rows spanning both products plus
    # long-term-retention and enhanced-fan-out variants. These are the on-demand
    # trio a reader sizes a stream with, plus the two provisioned-shard rates.
    "AmazonKinesis": _dim("OnDemand-BilledIncomingBytes",
                          "OnDemand-BilledOutgoingBytes", "OnDemand-StreamHour",
                          "Storage-ShardHour", "PutRequestPayloadUnits"),
    "AmazonKinesisFirehose": _dim("BilledBytes", "S3DeliveryObjectCount"),
    # Athena bills on data scanned, and that single rate is the one that decides
    # whether a partitioning strategy pays for itself.
    "AmazonAthena": _dim("DataScannedInTB", "CodeExecutionInDPUHours"),
    # Serverless RPU-hours and Spectrum data scanned. The provisioned node SKUs are
    # 36 of Redshift's 40 published rates and are excluded above.
    "AmazonRedshift": _dim("Redshift:ServerlessUsage", "DataScanned"),
    "AmazonTextract": re.compile(r"AnalyzeDocument|DetectDocumentText",
                                 re.IGNORECASE),
}


def _tool_name(context) -> str:
    """The tool the Gateway is asking for, minus the "<target>___" prefix.

    Note the name may be doubly prefixed when the underlying server prefixes its
    own tools; splitting on the LAST "___" is what makes that safe.
    """
    try:
        full = context.client_context.custom.get("bedrockAgentCoreToolName", "")
    except Exception:  # noqa: BLE001 - a direct test invoke has no client_context
        return ""
    return full.rsplit("___", 1)[-1] if full else ""


def _split_services(raw) -> list[str]:
    """The caller's service list, however it arrived.

    The Gateway passes one string for a single-parameter tool, and a model writes
    that string in whatever shape it likes: "Bedrock, Lambda, DynamoDB", a JSON
    array, or newline-separated. All three are accepted rather than rejected —
    the alternative is an empty result for a request that was perfectly clear.
    """
    if isinstance(raw, list):
        items = [str(x) for x in raw]
    else:
        s = str(raw or "").strip()
        if s.startswith("["):
            try:
                parsed = json.loads(s)
                s = ",".join(str(x) for x in parsed) if isinstance(parsed, list) else s
            except ValueError:
                pass
        items = re.split(r"[,\n;]+", s)
    out: list[str] = []
    for item in items:
        # Strip list bullets, quotes and the parenthetical asides a model adds
        # ("Amazon Bedrock (for the LLM)").
        name = re.sub(r"\(.*?\)", " ", item)
        name = re.sub(r"^[\s\-*\d.]+", "", name).strip().strip('"\'').strip()
        if name and name.lower() not in {n.lower() for n in out}:
            out.append(name)
    return out[:MAX_SERVICES]


_SERVICE_CODES: list[str] = []


def _service_codes() -> list[str]:
    """Every ServiceCode the Pricing API publishes, fetched once per container."""
    global _SERVICE_CODES
    if _SERVICE_CODES:
        return _SERVICE_CODES
    codes: list[str] = []
    token = None
    while True:
        kwargs = {"NextToken": token} if token else {}
        resp = _pricing.describe_services(**kwargs)
        codes.extend(s["ServiceCode"] for s in resp.get("Services", []))
        token = resp.get("NextToken")
        if not token:
            break
    _SERVICE_CODES = codes
    return codes


def _resolve(name: str) -> str | None:
    """A service name -> a ServiceCode, or None if it cannot be established.

    Alias table first, then a conservative match against the live catalog. None is
    a legitimate answer and is reported to the caller; guessing would price the
    wrong service, which is worse than admitting the gap.
    """
    key = re.sub(r"\s+", " ", name.strip().lower())
    bare = re.sub(r"^(amazon|aws)\s+", "", key)
    # THE ALIAS TABLE IS TRIED WITH AND WITHOUT THE VENDOR PREFIX, and this is not
    # cosmetic. A model writes the full product name — "AWS Step Functions", not
    # "step functions" — so trying only the unstripped key meant the table was
    # consulted for a name it never contains, and the two services whose
    # ServiceCode does NOT resemble their product name were the two that failed:
    # Step Functions is `AmazonStates` and SQS is `AWSQueueService`. Every other
    # service in a live eight-service list resolved by the exact-code match below
    # and hid the defect.
    for candidate in (key, bare):
        if candidate in ALIASES:
            return ALIASES[candidate]
    key_nospace = key.replace(" ", "")
    for code in _service_codes():
        if code.lower() == key_nospace:
            return code
    # "amazon xyz" / "aws xyz" -> a code containing xyz. Only accepted when
    # exactly one code matches, so an ambiguous name is reported, not picked.
    #
    # The length floor applies HERE ONLY. A three-letter substring is too weak to
    # match a catalog on, but there is nothing ambiguous about a three-letter
    # ALIAS: applying the floor before the table above silently lost sqs, sns,
    # kms, rds, ecs, eks and ec2 — seven services a customer names constantly.
    bare_nospace = bare.replace(" ", "")
    if len(bare_nospace) >= 4:
        hits = [c for c in _service_codes() if bare_nospace in c.lower()]
        if len(hits) == 1:
            return hits[0]
    return None


def _rows_for(code: str, region: str) -> tuple[list[dict], bool, bool]:
    """Consumption unit prices for one ServiceCode in one region.

    Returns (rows, truncated, curated).
      truncated  the page cap was hit, so the list is partial — said out loud
                 rather than implying it is complete.
      curated    the service has a `_PREFERRED` entry AND it matched, so these are
                 the primary consumption dimensions rather than a cheapest-first
                 sample of everything.
    """
    rows: list[dict] = []
    seen: set[str] = set()
    token = None
    pages = 0
    truncated = False
    preferred = _PREFERRED.get(code)
    while pages < MAX_PAGES_PER_SERVICE:
        kwargs = {
            "ServiceCode": code,
            "Filters": [{"Type": "TERM_MATCH", "Field": "regionCode", "Value": region}],
            "MaxResults": 100,
        }
        if token:
            kwargs["NextToken"] = token
        resp = _pricing.get_products(**kwargs)
        pages += 1
        for raw in resp.get("PriceList", []):
            product = json.loads(raw)
            attrs = product.get("product", {}).get("attributes", {})
            usage = str(attrs.get("usagetype", ""))
            # Dedupe on (product, dimension), NOT on the dimension alone. Every
            # Bedrock foundation model shares one usage type — the MODEL is in
            # `servicename` — so deduping by usage type collapsed nine models'
            # token rates into one row and silently hid the price of the other
            # eight.
            key = f"{attrs.get('servicename', '')}|{usage}"
            if not usage or _EXCLUDE.search(usage) or key in seen:
                continue
            # Filter to the primary dimensions HERE rather than after collecting.
            # Collecting a capped sample of everything and filtering afterwards
            # dropped preferred rows that happened to appear on a later page —
            # Bedrock's own per-model token rates were being cut that way.
            if preferred and not preferred.search(usage):
                continue
            for term in (product.get("terms", {}).get("OnDemand") or {}).values():
                for dim in (term.get("priceDimensions") or {}).values():
                    prices = dim.get("pricePerUnit") or {}
                    currency = "USD" if "USD" in prices else next(iter(prices), "")
                    amount = str(prices.get(currency, "")).strip()
                    # A zero rate is a free tier or a placeholder, not a price a
                    # reader can act on; omit it rather than reporting "$0".
                    if not amount or float(amount or 0) == 0:
                        continue
                    seen.add(key)
                    rows.append({
                        # `servicename` is the MODEL name for Bedrock foundation
                        # models ("Claude Sonnet 5 (Amazon Bedrock Edition)"),
                        # which is exactly what a reader needs to see there.
                        "service": str(attrs.get("servicename") or code),
                        "serviceCode": code,
                        "dimension": usage,
                        "unit": str(dim.get("unit") or ""),
                        "pricePerUnit": amount,
                        "currency": currency,
                        "region": region,
                        "sku": str(product.get("product", {}).get("sku") or ""),
                        "description": str(dim.get("description") or "")[:180],
                    })
                    break
                break
        token = resp.get("NextToken")
        if not token:
            break
        # Enough to fill the per-service cap with room to sort. Only reachable in
        # the unfiltered case; a preferred pattern matches few enough rows to page
        # the service out.
        if len(rows) >= MAX_ROWS_PER_SERVICE * 4:
            truncated = True
            break
    else:
        truncated = bool(token)

    curated = bool(preferred and rows)
    if preferred and not rows:
        # The pattern matched nothing — most likely AWS renamed a usage type, or
        # this region does not publish these dimensions. Retry unfiltered rather
        # than returning nothing: an empty answer reads as "this service has no
        # prices", which is a false claim, where a sample plus `curated: false` is
        # a true one.
        return _rows_unfiltered(code, region)

    # Collapse regional-prefix twins: CloudWatch publishes both
    # `TimedStorage-ByteHrs` and `USE1-TimedStorage-ByteHrs` at the same rate, and
    # two rows saying the same thing spend the reader's attention twice. Keeps the
    # shorter name.
    best: dict[tuple, dict] = {}
    for r in sorted(rows, key=lambda r: len(r["dimension"])):
        best.setdefault((r["service"], r["unit"], r["pricePerUnit"]), r)
    rows = list(best.values())

    if curated:
        # Group by product, then by dimension, so a model's input and output rates
        # sit together and the per-service cap keeps WHOLE products. Sorting by
        # price instead split models across the cut, leaving a reader with input
        # rates for nine models and output rates for three.
        rows.sort(key=lambda r: (r["service"], r["dimension"]))
    else:
        # No curated list, so favour the small consumption rates an estimate is
        # built from over the large ones a broad service buries them under.
        rows.sort(key=lambda r: float(r["pricePerUnit"]))
    return rows[:MAX_ROWS_PER_SERVICE], truncated, curated


def _rows_unfiltered(code: str, region: str) -> tuple[list[dict], bool, bool]:
    """The fallback path: no curated dimension list, so sample what exists."""
    saved = _PREFERRED.pop(code, None)
    try:
        rows, truncated, _ = _rows_for(code, region)
    finally:
        if saved is not None:
            _PREFERRED[code] = saved
    return rows, truncated, False


def aws_prices(args: dict) -> dict:
    """Real on-demand unit prices for the named AWS services, in one region."""
    names = _split_services(args.get("services"))
    region = str(args.get("region") or DEFAULT_REGION).strip() or DEFAULT_REGION
    if not names:
        return {
            "error": "no services named. Pass `services` as a comma-separated list, "
                     "e.g. \"Bedrock, AWS Lambda, DynamoDB\".",
            "knownAliases": sorted(ALIASES),
        }

    results: list[dict] = []
    unresolved: list[str] = []
    no_prices: list[str] = []
    truncated: list[str] = []
    unfiltered: list[str] = []
    for name in names:
        code = _resolve(name)
        if not code:
            unresolved.append(name)
            continue
        try:
            rows, was_truncated, curated = _rows_for(code, region)
        except Exception as e:  # noqa: BLE001 - one bad service must not fail them all
            print(f"[pricing] aws_prices {name} ({code}) failed: {type(e).__name__}: {e}")
            no_prices.append(f"{name} ({type(e).__name__})")
            continue
        if not rows:
            no_prices.append(name)
            continue
        if was_truncated:
            truncated.append(name)
        if not curated:
            unfiltered.append(name)
        for r in rows:
            # A comprehension here would have to shed the comments below, which carry
            # why each field exists. The list is at most a few dozen rows.
            results.append({  # noqa: PERF401 - kept a loop so the fields stay annotated
                # Both shapes on every row, and both load-bearing: `text` for an
                # agent that hands evidence to a model, the fields for one that
                # computes. See ctx.call_tool_rows and the tool's `rowFields`.
                "text": (f"{r['service']} — {r['dimension']}: "
                         f"{r['pricePerUnit']} {r['currency']} per {r['unit']} "
                         f"({r['region']})")[:MAX_OUTPUT_CHARS],
                "title": f"{r['service']} {r['dimension']}",
                # The caller's OWN wording for this service, carried back on every
                # row. `servicename` is what AWS calls it, and the two rarely
                # match: ask for "Amazon SQS" and the rows say "Amazon Simple Queue
                # Service". An agent comparing what it asked for against what came
                # back therefore could not tell which names went unpriced, and a
                # live asset reported "priced 6 of 8" without ever saying which two
                # were missing. The envelope's `unresolvedServices` says so, but
                # `extract_rows` hands an agent the ROWS only — so the answer has
                # to travel on a row to reach it. Mapped via `rowFields`.
                "requestedAs": name,
                **r,
            })

    return {
        "query": ", ".join(names),
        "region": region,
        "count": len(results),
        "results": results,
        # Every shortfall named, never silently dropped. The calling agent turns
        # these into stated limitations so a reader knows what was NOT priced.
        "unresolvedServices": unresolved,
        "servicesWithNoPublishedPrice": no_prices,
        "partialServices": truncated,
        # Services returned as a cheapest-first sample of all their dimensions
        # rather than their primary consumption ones, because this tool has no
        # curated dimension list for them. Named so the agent can say the rates are
        # illustrative for those services rather than the ones that matter most.
        "unfilteredServices": unfiltered,
        # Said on every response, because it is the difference between a grounded
        # estimate and an invented one. These are RATES; a total needs volumes,
        # and volumes are a property of the workload, not of AWS.
        "note": ("On-demand unit prices from the AWS Price List Query API. These are "
                 "RATES, not a total: a total requires usage volumes (tokens, "
                 "requests, GB-months) which this tool does not know and must not "
                 "assume. Free-tier allowances are not applied. Zero-rated and "
                 "instance-based/provisioned SKUs are excluded."),
    }


# Tool name -> implementation. The Gateway publishes each; `call` in workflow.json
# says which one the bound agent invokes.
_TOOLS = {"aws_prices": aws_prices}


def lambda_handler(event, context):
    tool = _tool_name(context) or ("aws_prices" if len(_TOOLS) == 1 else "")
    fn = _TOOLS.get(tool)
    if fn is None:
        # Named explicitly rather than defaulting to one of them: silently running
        # the wrong tool would return real-looking data for a question nobody asked.
        return {"error": f"unknown tool {tool!r}", "availableTools": sorted(_TOOLS)}
    try:
        return fn(event or {})
    except Exception as e:  # noqa: BLE001 - surface the reason, never a fake answer
        print(f"[pricing] {tool} failed: {type(e).__name__}: {e}")
        return {"error": f"{tool} failed: {type(e).__name__}: {e}"}
