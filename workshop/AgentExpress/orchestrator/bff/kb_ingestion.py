"""A type="kb" tool's `chunking` and `parsing` checks, for validate_build.py.
Both become the Bedrock data source's vectorIngestionConfiguration, which the API checks
only when the data source is created: a bad value fails the deploy halfway, after the
vector store and the Knowledge Base exist. So the ranges Bedrock enforces are checked
here first, from one home (vocabulary.json kbChunkingStrategies / kbParsingStrategies).
Mirrors web/src/builder/kbIngestion.ts (same words: tests/fixtures/validation_cases.json),
cdk/lib/kb-ingestion.ts validateKbIngestion and the preconditions in terraform/kb.tf.
https://docs.aws.amazon.com/bedrock/latest/APIReference/API_agent_VectorIngestionConfiguration.html
"""
from __future__ import annotations

import re

#: A Bedrock model id (anthropic.claude-3-haiku-20240307-v1:0) or a system inference
#: profile id (us.amazon.nova-pro-v1:0). No ARN, no wildcard: the id is put into the
#: Knowledge Base role's grant, so it must name exactly one model.
MODEL_ID_RE = re.compile(r"^[a-z0-9-]+(\.[a-z0-9-]+)+(:[0-9]+)?$")
#: ParsingPromptText: 1 to 10000 characters (botocore bedrock-agent).
PROMPT_MAX = 10000


def _is_obj(v) -> bool:
    return isinstance(v, dict)


def _whole(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and float(v).is_integer()


def check(tool: dict, path: str, where: dict, add, vocab_extra, vocab) -> None:
    chunking = tool.get("chunking")
    if _is_obj(chunking):
        _check_chunking(chunking, f"{path}.chunking", where, add, vocab_extra, vocab,
                        own_store=isinstance(tool.get("vectorStore"), dict))
    parsing = tool.get("parsing")
    if _is_obj(parsing):
        _check_parsing(parsing, f"{path}.parsing", where, add, vocab)


def _check_chunking(c: dict, p: str, where: dict, add, vocab_extra, vocab, own_store: bool = False) -> None:
    strategies = vocab("kbChunkingStrategies")
    strategy = c.get("strategy")
    if strategy not in strategies:
        add("error", where, f"{p}.strategy", f"must be one of: {', '.join(strategies)}")
        return
    params = (vocab_extra("kbChunkingStrategies", "params") or {}).get(strategy) or []
    ranges = vocab_extra("kbChunkingStrategies", "ranges") or {}
    for name in params:
        if c.get(name) is None:
            add("error", where, f"{p}.{name}", f"is required for {strategy} chunking")
            continue
        lo, hi = ranges.get(name, [None, None])
        v = c[name]
        if _whole(v) and (v < lo or (hi is not None and v > hi)):
            add("error", where, f"{p}.{name}",
                f"must be a whole number from {lo} to {hi}" if hi is not None
                else f"must be a whole number, {lo} or more")
    for name in c:
        if name != "strategy" and name in ranges and name not in params:
            add("error", where, f"{p}.{name}", f"does not apply to {strategy} chunking")
    if strategy == "hierarchical":
        parent, child = c.get("parentMaxTokens"), c.get("childMaxTokens")
        if _whole(parent) and _whole(child) and child >= parent:
            add("error", where, f"{p}.childMaxTokens",
                "must be smaller than parentMaxTokens: the second layer holds the smaller chunks")
        if own_store:
            return      # the warning is about S3 Vectors' metadata limit
        add("warning", where, f"{p}.strategy",
            "AWS does not recommend hierarchical chunking with S3 Vectors, the store this framework creates: "
            "large chunks can exceed its per-vector metadata limit and fail ingestion")


def _check_parsing(c: dict, p: str, where: dict, add, vocab) -> None:
    strategies = vocab("kbParsingStrategies")
    if c.get("strategy") not in strategies:
        add("error", where, f"{p}.strategy", f"must be one of: {', '.join(strategies)}")
        return
    model = c.get("model")
    if model is None or model == "":
        add("error", where, f"{p}.model", "is required: the Bedrock model that reads the documents")
    elif isinstance(model, str) and not MODEL_ID_RE.match(model):
        add("error", where, f"{p}.model",
            "must be a Bedrock model or inference profile id, like anthropic.claude-3-haiku-20240307-v1:0")
    prompt = c.get("prompt")
    if isinstance(prompt, str) and not 1 <= len(prompt) <= PROMPT_MAX:
        add("error", where, f"{p}.prompt", f"must be 1 to {PROMPT_MAX} characters")
