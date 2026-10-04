"""
Gateway Lambda target: retrieves relevant chunks from a Bedrock Knowledge Base.

The AgentCore Gateway invokes this Lambda with the tool arguments as the `event`
and the tool name in `context.client_context.custom['bedrockAgentCoreToolName']`
(format: "<target>___<tool>"). We return a JSON object the agent can use.

EVERY RETRIEVAL PARAMETER IS CONFIG, NOT CODE. This file used to hardcode the
request shape: `numberOfResults` from one env var, and a filter that was always
`{"equals": {"key": "doc_type", ...}}`. That made two things impossible without
editing the framework — a corpus tagged with anything other than `doc_type`, and any
narrowing beyond a single equality. Both are ordinary requirements, so both are now
keys on the `kb` tool in workflow.json and arrive here as env.

THE ONE THING THAT IS DELIBERATELY NOT OPEN IS WHAT THE AGENT CAN SEND.
There are two filters here and the difference is the whole security model:

  the AGENT's filter   one scalar, its `corpus`, arriving as the `filter` argument.
                       The generated Cedar permit is a scalar value match
                       (`["reference"].contains(context.input.filter)`), so the
                       Gateway refuses a corpus the agent was not granted BEFORE
                       this function runs. That only works while the value stays
                       scalar, which is why `corpusOperator` is restricted to
                       single-value operators.
  the TARGET's filter  `KB_STATIC_FILTER`, arbitrary Bedrock filter syntax, set in
                       workflow.json and invisible to the caller. It is ANDed with
                       the agent's, so it can only ever narrow.

A filter an agent supplies is scoping; a filter it cannot reach is a boundary. Making
the rich one target-level is what lets the rich one exist at all.
"""

import contextlib
import json
import os

import boto3

KB_ID = os.environ["KB_ID"]
REGION = os.environ.get("AWS_REGION", "us-east-1")
NUM_RESULTS = int(os.environ.get("KB_NUM_RESULTS", "5"))
#: Which metadata attribute an agent's `corpus` is matched against. "doc_type" is what
#: this framework's ingestion sidecars write, so it is the default rather than a
#: constant — a customer whose documents are already tagged by customer id or product
#: line sets `corpusKey` and changes nothing else.
CORPUS_KEY = os.environ.get("KB_CORPUS_KEY", "doc_type")
CORPUS_OPERATOR = os.environ.get("KB_CORPUS_OPERATOR", "equals")
# THERE IS DELIBERATELY NO `searchType` KNOB. It was added, deployed, and removed the
# same day, because S3 Vectors — the only vector store this framework provisions — does
# not support hybrid search:
#     ValidationException: HYBRID search type is not supported for search operation
#     on index <id>. Retry your request with a different search type.
# That leaves SEMANTIC as the only legal value, so the key could express nothing. A config
# key whose one working value is the default is not a feature, and one whose other value
# always fails at RETRIEVAL — long after a green deploy — is worse than no key at all.
# If this framework ever provisions OpenSearch Serverless instead, hybrid becomes real and
# `overrideSearchType` is the field to set here.
#: Bedrock-shaped filter applied to every call. See the module docstring for why this
#: one may be arbitrary while the agent's may not.
STATIC_FILTER = os.environ.get("KB_STATIC_FILTER", "")
RERANK = os.environ.get("KB_RERANK", "")

_rt = boto3.client("bedrock-agent-runtime", region_name=REGION)


def _json_env(raw: str, name: str):
    """Parse a JSON-valued env var, or RAISE naming it.

    Not a silent `{}` fallback. These are filters and reranker settings: retrieving
    successfully with the filter dropped returns real chunks from outside the scope the
    config asked for, and nothing downstream can tell. A 500 naming the variable is the
    only safe failure.
    """
    if not raw.strip():
        return None
    try:
        return json.loads(raw)
    except ValueError as exc:
        raise RuntimeError(
            f"{name} is not valid JSON, so the retrieval scope it defines cannot be "
            f"applied. Refusing to retrieve without it. Value: {raw[:200]!r}") from exc


def _corpus_filter(corpus: str) -> dict:
    """The agent's own scoping: one operator, one key, one scalar value."""
    return {CORPUS_OPERATOR: {"key": CORPUS_KEY, "value": corpus}}


def _combine(agent_filter: dict | None, static_filter: dict | None) -> dict | None:
    """AND the two filters, in the shape Bedrock wants.

    `andAll` needs at least two members, so a single filter is passed through alone
    rather than wrapped — a one-member andAll is rejected by the API, which would turn
    the common case (a static filter and no corpus, or vice versa) into a failure.
    """
    present = [f for f in (agent_filter, static_filter) if f]
    if not present:
        return None
    if len(present) == 1:
        return present[0]
    return {"andAll": present}


def lambda_handler(event, context):
    tool = ""
    with contextlib.suppress(Exception):
        tool = context.client_context.custom.get("bedrockAgentCoreToolName", "")

    query = (event or {}).get("query", "").strip()
    if not query:
        return {"error": "missing required 'query' argument", "tool": tool}

    vector_cfg: dict = {"numberOfResults": NUM_RESULTS}

    corpus = (event or {}).get("filter", "").strip()
    combined = _combine(
        _corpus_filter(corpus) if corpus else None,
        _json_env(STATIC_FILTER, "KB_STATIC_FILTER"),
    )
    if combined:
        vector_cfg["filter"] = combined

    rerank = _json_env(RERANK, "KB_RERANK")
    if rerank and rerank.get("model"):
        # Reranking reorders what retrieval already found, so asking for more survivors
        # than were retrieved is a config mistake with a silent outcome — you pay for the
        # rerank call and get the same list back. Clamped rather than passed through.
        keep = min(int(rerank.get("count") or NUM_RESULTS), NUM_RESULTS)
        vector_cfg["rerankingConfiguration"] = {
            "type": "BEDROCK_RERANKING_MODEL",
            "bedrockRerankingConfiguration": {
                "numberOfRerankedResults": keep,
                "modelConfiguration": {
                    "modelArn": (
                        rerank["model"]
                        if rerank["model"].startswith("arn:")
                        else f"arn:aws:bedrock:{REGION}::foundation-model/{rerank['model']}"
                    )
                },
            },
        }

    resp = _rt.retrieve(
        knowledgeBaseId=KB_ID,
        retrievalQuery={"text": query},
        retrievalConfiguration={"vectorSearchConfiguration": vector_cfg},
    )
    results = [
        {
            "text": r.get("content", {}).get("text", ""),
            "score": r.get("score"),
            "source": r.get("location", {}).get("s3Location", {}).get("uri"),
        }
        for r in resp.get("retrievalResults", [])
    ]
    return {"query": query, "count": len(results), "results": results}
