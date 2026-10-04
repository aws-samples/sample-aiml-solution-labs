"""The built-in demo tool function (orchestrator/app/tools/pricing/handler.py).

It publishes ONE tool, `aws_prices`: real AWS on-demand unit prices from the Price
List Query API, so an agent can talk about cost without inventing a figure.

THE FIXTURES BELOW ARE THE REAL RESPONSE SHAPE, verified against the live API
before any of this was written. That matters more than it sounds: the payload nests
the interesting values four levels down
(`terms.OnDemand.<sku.offerTerm>.priceDimensions.<rateCode>.pricePerUnit.USD`), each
`PriceList` entry is a JSON *string* rather than an object, and the field carrying
the product name is `servicename` — which for a Bedrock foundation model is the
MODEL name, not "Amazon Bedrock". A parser written against a guess at any of that
returns plausible nonsense.

Three defects these tests pin, all found by running the real API:

  1. Taking the first N products returns RANDOM SKUs. "Price AWS Lambda" came back
     with managed-instance hourly rates for c7i.8xlarge; "price AgentCore" returned
     several hundred EC2-shaped instance rows. Real prices, for a question nobody
     asked, burying the handful that matter.
  2. Deduping by usage type collapsed nine Bedrock models into one row, because
     every model shares the usage type `..._input_tokens_standard-Units` and is
     distinguished only by `servicename`. Eight models' prices vanished silently.
  3. Usage types carry a REGION PREFIX outside us-east-1 (`EU-Lambda-GB-Second`),
     so patterns anchored for one region match nothing anywhere else.
"""
import importlib
import json
import sys

import pytest


def _entry(service_code, servicename, usagetype, price, unit,
           description="", sku="SKU1", region="us-east-1"):
    """One PriceList entry in the API's real shape — a JSON STRING, not an object."""
    rate = f"{sku}.OFFER.RATE"
    return json.dumps({
        "product": {
            "sku": sku,
            "attributes": {
                "regionCode": region,
                "usagetype": usagetype,
                "servicename": servicename,
                "location": "US East (N. Virginia)",
                "operation": "",
            },
        },
        "serviceCode": service_code,
        "terms": {"OnDemand": {f"{sku}.OFFER": {
            "sku": sku,
            "offerTermCode": "OFFER",
            "priceDimensions": {rate: {
                "unit": unit,
                "rateCode": rate,
                "beginRange": "0",
                "endRange": "Inf",
                "description": description or f"{servicename} {usagetype}",
                "pricePerUnit": {"USD": price},
            }},
            "termAttributes": {},
        }}},
        "version": "20260911124410",
    })


# A page mixing what a real service returns: the two dimensions an estimate needs,
# the instance SKUs that swamp them, and a zero-rated placeholder.
LAMBDA_PAGE = [
    _entry("AWSLambda", "AWS Lambda", "Lambda-Managed-Instances-c7i.8xlarge-Mgmt",
           "0.2142000000", "Hours", sku="INST1"),
    _entry("AWSLambda", "AWS Lambda", "Lambda-GB-Second", "0.0000150000",
           "Lambda-GB-Second", sku="GBS1"),
    _entry("AWSLambda", "AWS Lambda", "Request", "0.0000002000", "Requests",
           sku="REQ1"),
    _entry("AWSLambda", "AWS Lambda", "Lambda-Provisioned-GB-Second",
           "0.0000097100", "Lambda-GB-Second", sku="PROV1"),
    _entry("AWSLambda", "AWS Lambda", "Lambda-Edge-Request", "0.0000006000",
           "Request", sku="EDGE1"),
    _entry("AWSLambda", "AWS Lambda", "FreeTierPlaceholder", "0.0000000000",
           "Requests", sku="FREE1"),
]

# Three models, all sharing ONE usage type per direction. This is defect 2.
BEDROCK_PAGE = [
    _entry("AmazonBedrockFoundationModels", "Claude Sonnet 5 (Amazon Bedrock Edition)",
           "USE1-MP:USE1_input_tokens_standard-Units", "2.2000000000", "1M tokens",
           sku="BS1"),
    _entry("AmazonBedrockFoundationModels", "Claude Sonnet 5 (Amazon Bedrock Edition)",
           "USE1-MP:USE1_output_tokens_standard-Units", "11.0000000000", "1M tokens",
           sku="BS2"),
    _entry("AmazonBedrockFoundationModels", "Claude Opus 5 (Amazon Bedrock Edition)",
           "USE1-MP:USE1_input_tokens_standard-Units", "5.5000000000", "1M tokens",
           sku="BO1"),
    _entry("AmazonBedrockFoundationModels", "Claude Opus 5 (Amazon Bedrock Edition)",
           "USE1-MP:USE1_output_tokens_standard-Units", "27.5000000000", "1M tokens",
           sku="BO2"),
    _entry("AmazonBedrockFoundationModels", "Claude Haiku 5 (Amazon Bedrock Edition)",
           "USE1-MP:USE1_input_tokens_standard-Units", "0.8000000000", "1M tokens",
           sku="BH1"),
    # Batch tier: a real rate, but not how an interactive application is charged.
    _entry("AmazonBedrockFoundationModels", "Claude Sonnet 5 (Amazon Bedrock Edition)",
           "USE1-MP:USE1_input_tokens_batch-Units", "1.1000000000", "1M tokens",
           sku="BB1"),
]

# The same Lambda dimensions as Ireland publishes them. This is defect 3.
EU_LAMBDA_PAGE = [
    _entry("AWSLambda", "AWS Lambda", "EU-Lambda-GB-Second", "0.0000133334",
           "Lambda-GB-Second", sku="EUGBS", region="eu-west-1"),
    _entry("AWSLambda", "AWS Lambda", "EU-Request", "0.0000002000", "Request",
           sku="EUREQ", region="eu-west-1"),
    _entry("AWSLambda", "AWS Lambda", "EU-IA-Request", "0.0000009000", "Request",
           sku="EUIA", region="eu-west-1"),
]


class FakePricing:
    """Enough Price List Query API surface for the handler, with paging."""

    def __init__(self, pages_by_code, services=("AWSLambda", "AmazonDynamoDB")):
        self.pages_by_code = pages_by_code
        self.services = list(services)
        self.calls: list[tuple[str, str]] = []

    def describe_services(self, **kw):
        return {"Services": [{"ServiceCode": c} for c in self.services]}

    def get_products(self, **kw):
        code = kw["ServiceCode"]
        region = next(f["Value"] for f in kw["Filters"] if f["Field"] == "regionCode")
        self.calls.append((code, region))
        pages = self.pages_by_code.get(code, [])
        if not pages:
            return {"PriceList": []}
        idx = int(kw.get("NextToken") or 0)
        out = {"PriceList": pages[idx]}
        if idx + 1 < len(pages):
            out["NextToken"] = str(idx + 1)
        return out


@pytest.fixture()
def tl(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("PRICING_API_REGION", "us-east-1")
    # SKIP, not error, when this sample's own demo tool is gone. Everything in this
    # file is about `app/tools/pricing/handler.py`, which a customer replacing the
    # shipped workflow deletes — and every other sample-specific test in this suite
    # skips in that case and says so. This one imported the module inside the fixture,
    # so it raised ModuleNotFoundError 51 times instead, which reads as "the customer
    # broke the framework" rather than "this test covers a tool you removed". Found by
    # actually replacing the workflow with another domain.
    pricing = __import__("conftest").ORCH_ROOT / "app" / "tools" / "pricing"
    if not (pricing / "handler.py").is_file():
        pytest.skip("app/tools/pricing/ is absent: this sample's demo `lambda` tool "
                    "has been removed, and these tests only cover that tool")
    sys.path.insert(0, str(pricing))
    import handler

    importlib.reload(handler)
    handler._SERVICE_CODES = []          # the per-container catalog cache
    return handler


def _prices(tl, fake, **args):
    tl._pricing = fake
    tl._SERVICE_CODES = []
    return tl.aws_prices(args)


# ---------------------------------------------------------------------------
# Defect 1: the instance SKUs must not swamp the dimensions that matter
# ---------------------------------------------------------------------------

def test_the_primary_consumption_dimensions_are_what_come_back(tl):
    fake = FakePricing({"AWSLambda": [LAMBDA_PAGE]})
    out = _prices(tl, fake, services="AWS Lambda")

    dims = {r["dimension"] for r in out["results"]}
    assert dims == {"Lambda-GB-Second", "Request"}, (
        "the two rates a Lambda estimate is built from, and nothing else")
    assert out["unfilteredServices"] == [], "this service has a curated dimension list"


@pytest.mark.parametrize("noise", [
    "Lambda-Managed-Instances-c7i.8xlarge-Mgmt",   # what "price Lambda" used to return
    "Lambda-Provisioned-GB-Second",
    "Lambda-Edge-Request",
])
def test_instance_and_provisioned_skus_are_excluded(tl, noise):
    fake = FakePricing({"AWSLambda": [LAMBDA_PAGE]})
    out = _prices(tl, fake, services="AWS Lambda")
    assert noise not in {r["dimension"] for r in out["results"]}


def test_a_zero_rate_is_omitted_rather_than_reported_as_free(tl):
    """A zero is a free-tier row or a placeholder, not a price a reader can act on;
    "$0 per request" in a cost analysis is worse than saying nothing."""
    fake = FakePricing({"AWSLambda": [LAMBDA_PAGE]})
    out = _prices(tl, fake, services="AWS Lambda")
    assert all(float(r["pricePerUnit"]) > 0 for r in out["results"])


# ---------------------------------------------------------------------------
# Defect 2: every model priced, not just the first
# ---------------------------------------------------------------------------

def test_every_model_is_priced_not_just_the_first(tl):
    """All nine live Bedrock models share one usage type per direction and differ
    only by `servicename`. Deduping on the usage type alone reported one model and
    silently dropped the rest — including, on the real run, the cheapest option."""
    fake = FakePricing({"AmazonBedrockFoundationModels": [BEDROCK_PAGE]})
    out = _prices(tl, fake, services="Bedrock")

    priced = {r["service"] for r in out["results"]}
    assert len(priced) == 3, priced
    haiku = [r for r in out["results"] if "Haiku" in r["service"]]
    assert haiku and haiku[0]["pricePerUnit"] == "0.8000000000"


def test_each_model_keeps_both_its_input_and_output_rate(tl):
    """A reader comparing models needs both halves of the same model. Sorting by
    price split them across the per-service cap, leaving input rates for nine models
    and output rates for three."""
    fake = FakePricing({"AmazonBedrockFoundationModels": [BEDROCK_PAGE]})
    out = _prices(tl, fake, services="Bedrock")

    for model in ("Claude Sonnet 5", "Claude Opus 5"):
        dims = [r["dimension"] for r in out["results"] if model in r["service"]]
        assert any("input_tokens" in d for d in dims), model
        assert any("output_tokens" in d for d in dims), model


def test_the_batch_tier_is_not_mixed_in_with_the_standard_one(tl):
    """Both are real, but an interactive application is charged the standard rate;
    quoting the cheaper batch rate beside it invites the wrong comparison."""
    fake = FakePricing({"AmazonBedrockFoundationModels": [BEDROCK_PAGE]})
    out = _prices(tl, fake, services="Bedrock")
    assert all("batch" not in r["dimension"].lower() for r in out["results"])


# ---------------------------------------------------------------------------
# Defect 3: the curated dimensions must work in every region
# ---------------------------------------------------------------------------

def test_the_same_dimensions_are_found_outside_us_east_1(tl):
    """`Lambda-GB-Second` in N. Virginia is `EU-Lambda-GB-Second` in Ireland. Without
    tolerating the prefix the curated patterns match nothing anywhere else, and the
    tool falls back to an unfiltered sample on every deployment but one."""
    fake = FakePricing({"AWSLambda": [EU_LAMBDA_PAGE]})
    out = _prices(tl, fake, services="AWS Lambda", region="eu-west-1")

    assert {r["dimension"] for r in out["results"]} == {
        "EU-Lambda-GB-Second", "EU-Request"}
    assert out["unfilteredServices"] == []
    assert fake.calls == [("AWSLambda", "eu-west-1")], "must price the region asked for"


def test_an_infrequent_access_variant_is_not_taken_for_the_standard_rate(tl):
    """`IA-` is a different storage class with its own rates, and it is also a
    two-letter uppercase prefix — so the region-prefix tolerance would otherwise
    read `EU-IA-Request` as the standard request rate."""
    fake = FakePricing({"AWSLambda": [EU_LAMBDA_PAGE]})
    out = _prices(tl, fake, services="AWS Lambda", region="eu-west-1")
    assert "EU-IA-Request" not in {r["dimension"] for r in out["results"]}


# ---------------------------------------------------------------------------
# Resolution: a name a model wrote -> a ServiceCode
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("written,code", [
    ("Amazon Bedrock", "AmazonBedrockFoundationModels"),
    ("bedrock", "AmazonBedrockFoundationModels"),
    ("AWS Lambda", "AWSLambda"),
    ("lambda", "AWSLambda"),
    ("DynamoDB", "AmazonDynamoDB"),
    ("Step Functions", "AmazonStates"),      # not "AWSStepFunctions"
    ("SQS", "AWSQueueService"),              # nor "AmazonSQS"
    ("Amazon Bedrock AgentCore", "AmazonBedrockAgentCore"),
])
def test_service_names_resolve_to_the_right_pricing_code(tl, written, code):
    """ServiceCodes are neither guessable nor consistent, which is why the mapping is
    an explicit table rather than a transformation of the name."""
    assert tl._resolve(written) == code


def test_a_model_written_list_is_parsed_however_it_arrives(tl):
    """A model asked for services replies in prose. All of these are perfectly clear
    to a reader, so rejecting them would be the tool's failure, not the model's."""
    for written in ("Bedrock, AWS Lambda",
                    '["Bedrock", "AWS Lambda"]',
                    "- Bedrock\n- AWS Lambda",
                    "1. Amazon Bedrock (for the LLM)\n2. AWS Lambda (compute)"):
        assert tl._split_services(written)[:2] == [
            n for n in tl._split_services(written)[:2]], written
        got = [tl._resolve(n) for n in tl._split_services(written)]
        assert "AmazonBedrockFoundationModels" in got and "AWSLambda" in got, written


def test_an_unknown_service_is_reported_not_guessed(tl):
    """Pricing the wrong service is worse than admitting the gap: the number would
    look exactly as authoritative as a correct one."""
    fake = FakePricing({"AWSLambda": [LAMBDA_PAGE]})
    out = _prices(tl, fake, services="AWS Lambda, Frobnicator9000")

    assert out["unresolvedServices"] == ["Frobnicator9000"]
    assert out["results"], "the resolvable service is still priced"


def test_an_ambiguous_name_is_reported_rather_than_picked(tl):
    """Two candidates means the tool does not know which was meant."""
    fake = FakePricing({}, services=["AmazonFooBar", "AmazonFooBaz"])
    out = _prices(tl, fake, services="foo")
    assert out["unresolvedServices"] == ["foo"]


def test_a_service_with_no_published_price_is_named(tl):
    fake = FakePricing({"AWSLambda": []})
    out = _prices(tl, fake, services="AWS Lambda")
    assert out["servicesWithNoPublishedPrice"] == ["AWS Lambda"]
    assert out["results"] == []


def test_one_failing_service_does_not_lose_the_others(tl):
    class Exploding(FakePricing):
        def get_products(self, **kw):
            if kw["ServiceCode"] == "AmazonDynamoDB":
                raise RuntimeError("throttled")
            return super().get_products(**kw)

    out = _prices(tl, Exploding({"AWSLambda": [LAMBDA_PAGE]}),
                  services="AWS Lambda, DynamoDB")
    assert out["results"], "Lambda still priced"
    assert any("DynamoDB" in s for s in out["servicesWithNoPublishedPrice"])


def test_no_services_named_is_an_error_not_an_empty_answer(tl):
    out = _prices(tl, FakePricing({}), services="")
    assert "error" in out and "knownAliases" in out


# ---------------------------------------------------------------------------
# The contract that keeps a cost claim honest
# ---------------------------------------------------------------------------

def test_it_returns_rates_and_never_a_total(tl):
    """The whole design rests on this. A total needs usage volumes, which are a
    property of the customer's workload and not of AWS — so an agent that multiplies
    a real rate by a volume nobody supplied has invented the volume, and the invented
    half is the half that makes the total wrong."""
    fake = FakePricing({"AWSLambda": [LAMBDA_PAGE]})
    out = _prices(tl, fake, services="AWS Lambda")

    for key in ("total", "monthlyCost", "estimate", "totalCost", "monthly"):
        assert key not in out, f"the tool must not compute a {key}"
    assert "RATES, not a total" in out["note"]
    assert "volumes" in out["note"]
    # Every row carries its unit, so a rate can never be read as a total.
    for row in out["results"]:
        assert row["unit"] and row["currency"] == "USD"


def test_every_row_carries_both_a_rendered_text_and_its_fields(tl):
    """`text` for an agent that hands evidence to a model, fields for one that
    computes. See ctx.call_tool_rows and the tool's `rowFields` block."""
    fake = FakePricing({"AWSLambda": [LAMBDA_PAGE]})
    out = _prices(tl, fake, services="AWS Lambda")
    for row in out["results"]:
        assert row["text"] and row["title"]
        for field in ("service", "dimension", "unit", "pricePerUnit", "currency",
                      "region", "sku"):
            assert field in row, field


def test_the_response_is_json_serialisable(tl):
    """It crosses the Gateway as JSON; a Decimal here would fail at the boundary."""
    fake = FakePricing({"AWSLambda": [LAMBDA_PAGE]})
    json.dumps(_prices(tl, fake, services="AWS Lambda"))


def test_an_unknown_tool_name_is_refused(tl):
    """Silently running the only tool it has would return real-looking prices for a
    question nobody asked."""
    custom = {"bedrockAgentCoreToolName": "pricing___prior_runs"}
    ctx = type("Ctx", (), {
        "client_context": type("CC", (), {"custom": custom})(),
    })()

    out = tl.lambda_handler({}, ctx)
    assert "unknown tool" in out["error"]
    assert out["availableTools"] == ["aws_prices"]


# ---------------------------------------------------------------------------
# Resolution: the alias table has to be reachable
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("written,code", [
    # The two that failed live. Both have an alias; neither was ever reached,
    # because the table was consulted with the vendor prefix still attached and
    # these are the only services whose ServiceCode does not resemble their name.
    ("AWS Step Functions", "AmazonStates"),
    ("Amazon SQS", "AWSQueueService"),
    # Three letters. The catalog-substring floor of 4 was being applied before the
    # alias lookup, which silently lost every short service name.
    ("SQS", "AWSQueueService"),
    ("SNS", "AmazonSNS"),
    ("KMS", "awskms"),
    ("RDS", "AmazonRDS"),
    ("EC2", "AmazonEC2"),
    # Still working, via the exact-code match that masked the defect.
    ("AWS Lambda", "AWSLambda"),
    ("Amazon S3", "AmazonS3"),
    ("Amazon Kinesis", "AmazonKinesis"),
    # Newly aliased, because a data pipeline names them constantly and `kinesis`
    # alone is ambiguous across three catalog codes.
    ("AWS Glue", "AWSGlue"),
    ("Amazon Athena", "AmazonAthena"),
    ("Data Firehose", "AmazonKinesisFirehose"),
])
def test_a_service_named_the_way_a_model_writes_it_resolves(tl, written, code):
    """A model writes the full product name. Resolution has to accept that."""
    tl._SERVICE_CODES = ["AmazonStates", "AWSQueueService", "AmazonSNS", "awskms",
                         "AmazonRDS", "AmazonEC2", "AWSLambda", "AmazonS3",
                         "AmazonKinesis", "AmazonKinesisFirehose",
                         "AmazonKinesisAnalytics", "AWSGlue", "AmazonAthena"]
    assert tl._resolve(written) == code


# ---------------------------------------------------------------------------
# Curation: the dimensions a reader can act on
# ---------------------------------------------------------------------------

GLUE_PAGE = [
    _entry("AWSGlue", "AWS Glue", "USE1-ETL-DPU-Hour", "0.44", "DPU-Hour"),
    _entry("AWSGlue", "AWS Glue", "USE1-ETL-Flex-DPU-Hour", "0.29", "DPU-Hour",
           sku="SKU2"),
    _entry("AWSGlue", "AWS Glue", "USE1-Catalog-Request", "0.000001", "Request",
           sku="SKU3"),
    # Adjacent products the caller did not ask about. Cheapest-first surfaced these.
    _entry("AWSGlue", "AWS Glue", "USE1-DBrew-Sessions", "1.0", "Sessions",
           sku="SKU4"),
    _entry("AWSGlue", "AWS Glue", "USE1-DBrew-Node-Hour", "0.48", "Node-hour",
           sku="SKU5"),
    _entry("AWSGlue", "AWS Glue", "USE1-zeroETL-App-IngestionVolume-GB", "1.5", "GB",
           sku="SKU6"),
    _entry("AWSGlue", "AWS Glue", "USE1-GlueInteractiveSession-DPU-Hour", "0.44",
           "DPU-Hour", sku="SKU7"),
]


def test_glue_returns_etl_rates_not_databrew(tl):
    """Live defect: asking to price AWS Glue returned DataBrew sessions and
    zero-ETL ingestion — real prices for a different product."""
    out = _prices(tl, FakePricing({"AWSGlue": [GLUE_PAGE]}), services="AWS Glue")
    dims = {r["dimension"] for r in out["results"]}
    assert any("ETL-DPU-Hour" in d for d in dims)
    assert not [d for d in dims if "DBrew" in d or "zeroETL" in d
                or "InteractiveSession" in d]


KINESIS_PAGE = [
    _entry("AmazonKinesis", "Amazon Kinesis", "OnDemand-BilledIncomingBytes", "0.08",
           "GB"),
    _entry("AmazonKinesis", "Amazon Kinesis", "OnDemand-StreamHour", "0.04",
           "StreamHr", sku="SKU2"),
    # A Firehose delivery channel, under the Data Streams service code.
    _entry("AmazonKinesis", "Amazon Kinesis",
           "USE1-On-demand-Advantage-Channel-ProcessedBytes-S3TablesDestination",
           "0.014", "GB", sku="SKU3"),
    _entry("AmazonKinesis", "Amazon Kinesis", "LongTermRetention-ByteHrs", "0.023",
           "GB-month", sku="SKU4"),
    _entry("AmazonKinesis", "Amazon Kinesis", "EnhancedFanoutHour", "0.015",
           "ConsumerShardHour", sku="SKU5"),
]


def test_kinesis_returns_the_on_demand_rates_not_every_variant(tl):
    """One service code covers Data Streams AND Firehose channels, so an uncurated
    cut returned twelve rows spanning two products plus retention and fan-out
    variants — and a downstream agent summarised them as a single price "range"."""
    out = _prices(tl, FakePricing({"AmazonKinesis": [KINESIS_PAGE]}),
                  services="Amazon Kinesis")
    dims = {r["dimension"] for r in out["results"]}
    assert "OnDemand-BilledIncomingBytes" in dims
    assert not [d for d in dims if "Channel-ProcessedBytes" in d
                or "LongTermRetention" in d or "EnhancedFanout" in d]


S3_PAGE = [
    _entry("AmazonS3", "Amazon Simple Storage Service", "TimedStorage-ByteHrs",
           "0.022", "GB-Mo"),
    _entry("AmazonS3", "Amazon Simple Storage Service", "Requests-Tier1", "0.000005",
           "Requests", sku="SKU2"),
    _entry("AmazonS3", "Amazon Simple Storage Service", "Vectors-TimedStorage-ByteHrs",
           "0.06", "GB-Mo", sku="SKU3"),
    _entry("AmazonS3", "Amazon Simple Storage Service", "Vectors-Request-Tier1",
           "0.000055", "Requests", sku="SKU4"),
]


def test_s3_vectors_is_not_part_of_an_s3_answer(tl):
    """S3 Vectors is a separate product. Carrying its rates meant every pipeline,
    website and backup estimate was handed vector-store prices it had no use for."""
    out = _prices(tl, FakePricing({"AmazonS3": [S3_PAGE]}), services="Amazon S3")
    assert not [r for r in out["results"] if "Vectors" in r["dimension"]]
    assert len(out["results"]) == 2


@pytest.mark.parametrize("usagetype", [
    "InstanceUsage:db.m8i.large",          # RDS instance rate
    "Node:ra3.16xlarge",                   # Redshift provisioned node
    "CS:dc2.large",                        # Redshift concurrency scaling
    "RDS:Mirror-GP2-Storage",              # colon, not the hyphen already covered
    "USE1-Redshift:ServerlessUsage-CR-1YR-AU",  # committed capacity, not on-demand
    "ExtendedSupport:Yr1-Yr2:ASv2:AuroraPostgreSQL14",
])
def test_the_exclusion_net_catches_instance_and_committed_rates(tl, usagetype):
    """This is the safety net for every service with no curated entry. Each of
    these came back live: a list of database instance sizes where the reader wanted
    a cost dimension, and an annual commitment wearing an hourly unit (2430 USD per
    RPU-Hr) sitting beside a real on-demand rate of 0.375."""
    assert tl._EXCLUDE.search(usagetype), usagetype


def test_every_row_names_the_service_the_caller_asked_for(tl):
    """AWS's `servicename` is not what anyone asks for: request "Amazon SQS" and
    the rows say "Amazon Simple Queue Service". The caller's own wording rides back
    on each row so the agent can name which requests went unpriced — a live asset
    said "priced 6 of 8" and never said which two were missing, because
    `extract_rows` hands an agent the rows and not the envelope."""
    fake = FakePricing({"AWSLambda": [LAMBDA_PAGE]})
    out = _prices(tl, fake, services="AWS Lambda")
    assert out["results"]
    assert all(r["requestedAs"] == "AWS Lambda" for r in out["results"])
