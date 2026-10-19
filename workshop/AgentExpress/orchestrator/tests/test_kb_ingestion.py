"""A type="kb" tool's `chunking` and `parsing`: the Bedrock data source's
vectorIngestionConfiguration, on both IaC paths.

The Build view and the BFF check the shape (tests/fixtures/validation_cases.json holds the
cases both validators must agree on). This file pins what the fixture cannot: that the
ranges come from Bedrock's own model, that the Terraform path wires every setting the
CDK path does, and that a tool declaring neither key deploys exactly as before.
"""
from __future__ import annotations

import json
import re
import sys

import pytest
from conftest import ORCH_ROOT

sys.path.insert(0, str(ORCH_ROOT / "bff"))
import kb_ingestion
import validate_build

VOCAB = json.loads((ORCH_ROOT / "app" / "vocabulary.json").read_text())
KB_TF = (ORCH_ROOT / "terraform" / "kb.tf").read_text()
INGEST_TF = (ORCH_ROOT / "terraform" / "kb_ingestion.tf").read_text()
TOOLS_TF = (ORCH_ROOT / "terraform" / "tools.tf").read_text()
INGEST_TS = (ORCH_ROOT / "cdk" / "lib" / "kb-ingestion.ts").read_text()
PLANE_TS = (ORCH_ROOT / "cdk" / "lib" / "tool-plane.ts").read_text()
WEB_TS = (ORCH_ROOT / "web" / "src" / "builder" / "kbIngestion.ts").read_text()


def _issues(tool: dict) -> list[tuple[str, str, str]]:
    out: list = []
    kb_ingestion.check(tool, "tools.kb", {"kind": "tool", "id": "kb"},
                       lambda sev, where, path, msg: out.append((sev, path, msg)),
                       validate_build.vocab_extra, validate_build.vocab)
    return out


# --- the ranges are Bedrock's, not ours -------------------------------------------------

#: Our setting -> the botocore shape member that carries it.
_BOTOCORE = {
    "fixedSize": ("FixedSizeChunkingConfiguration", {"maxTokens": "maxTokens", "overlapPercentage": "overlapPercentage"}),
    "semantic": ("SemanticChunkingConfiguration", {"maxTokens": "maxTokens", "bufferSize": "bufferSize",
                                                   "breakpointPercentileThreshold": "breakpointPercentileThreshold"}),
}


def test_the_ranges_match_the_bedrock_agent_model():
    botocore = pytest.importorskip("botocore.session")
    model = botocore.get_session().get_service_model("bedrock-agent")
    ranges = VOCAB["kbChunkingStrategies"]["ranges"]
    for strategy, (shape_name, members) in _BOTOCORE.items():
        shape = model.shape_for(shape_name)
        for ours, theirs in members.items():
            md = shape.members[theirs].metadata
            assert ranges[ours] == [md.get("min"), md.get("max")], (strategy, ours, md)
            assert theirs in shape.required_members
    hier = model.shape_for("HierarchicalChunkingConfiguration")
    level = model.shape_for("HierarchicalChunkingLevelConfiguration").members["maxTokens"].metadata
    assert ranges["parentMaxTokens"] == ranges["childMaxTokens"] == [level["min"], level["max"]]
    assert ranges["overlapTokens"] == [hier.members["overlapTokens"].metadata["min"], None]
    assert hier.members["levelConfigurations"].metadata == {"min": 2, "max": 2}
    # The values sent to Bedrock are ones its enums accept.
    assert set(VOCAB["kbChunkingStrategies"]["api"].values()) <= set(model.shape_for("ChunkingStrategy").enum)
    assert set(VOCAB["kbParsingStrategies"]["api"].values()) <= set(model.shape_for("ParsingStrategy").enum)
    assert model.shape_for("ParsingPromptText").metadata["max"] == kb_ingestion.PROMPT_MAX


def test_every_strategy_lists_settings_that_have_a_range():
    v = VOCAB["kbChunkingStrategies"]
    assert set(v["params"]) == set(v["values"]) == set(v["api"])
    for params in v["params"].values():
        assert set(params) <= set(v["ranges"])
    assert set(VOCAB["kbParsingStrategies"]["api"]) == set(VOCAB["kbParsingStrategies"]["values"])


# --- the server validator ------------------------------------------------------------------

def test_a_tool_without_chunking_or_parsing_has_nothing_to_say():
    assert _issues({"type": "kb"}) == []


def test_a_bool_is_not_a_whole_number():
    # JSON true is not 1: the range check must not wave it through.
    assert _issues({"chunking": {"strategy": "semantic", "maxTokens": 300, "bufferSize": True,
                                 "breakpointPercentileThreshold": 95}}) == []  # the type check reports it
    assert not kb_ingestion._whole(True)


@pytest.mark.parametrize("model", [
    "anthropic.claude-3-haiku-20240307-v1:0", "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
    "amazon.nova-pro-v1:0", "global.anthropic.claude-haiku-4-5-20251001-v1:0", "meta.llama4-scout-17b-instruct-v1:0"])
def test_real_model_and_profile_ids_are_accepted(model):
    assert _issues({"parsing": {"strategy": "foundationModel", "model": model}}) == []


@pytest.mark.parametrize("model", ["*", "anthropic.*", "arn:aws:bedrock:us-east-1::foundation-model/x",
                                   "claude", "us.anthropic.claude/../x", "Anthropic.Claude"])
def test_a_model_that_is_not_one_id_is_refused(model):
    # The id goes into the Knowledge Base role's grant: it must name exactly one model.
    assert [p for _, p, _ in _issues({"parsing": {"strategy": "foundationModel", "model": model}})] == [
        "tools.kb.parsing.model"]


def test_the_four_planes_share_one_model_pattern():
    pattern = kb_ingestion.MODEL_ID_RE.pattern
    assert f"/{pattern}/" in INGEST_TS
    assert f"/{pattern}/" in WEB_TS
    assert f"can(regex({json.dumps(pattern)}," in INGEST_TF   # an HCL string escapes \ as JSON does


# --- Terraform wires what CDK wires -----------------------------------------------------------

def _data_source_block() -> str:
    return KB_TF.split('resource "aws_bedrockagent_data_source" "kb"', 1)[1].split("\n}\n", 1)[0]


def test_terraform_names_the_data_source_from_the_digest_and_keeps_docs_by_default():
    block = _data_source_block()
    assert re.search(r"^\s*name\s*=\s*local\.kb_data_source_name\s*$", block, re.M)
    assert re.search(r'kb_data_source_name\s*=\s*local\.kb_ingestion_set \? "docs-\$\{substr\(sha256\(local\.kb_ingestion_key\), 0, 8\)\}" : "docs"',
                     INGEST_TF)
    # The canonical string is the one cdk/lib/kb-ingestion.ts builds (pinned by digest in
    # cdk/test/kb-ingestion.test.ts, whose literals were computed with terraform console).
    assert re.search(r'kb_ingestion_key\s*=\s*"c=\$\{local\.kb_chunk_strategy\}:\$\{join\(",",', INGEST_TF)
    assert "|p=${local.kb_parse_strategy}:${local.kb_parse_model_id}:${local.kb_parse_prompt}" in INGEST_TF
    assert "`c=${cs}:${values.join(\",\")}|p=${p?.strategy ?? \"\"}:${p?.model ?? \"\"}:${p?.prompt ?? \"\"}${e}`" in INGEST_TS
    # Neptune's graph model is appended only when there is one, so other digests are unchanged.
    assert '${local.kb_graph_model == "" ? "" : "|e=${local.kb_graph_model}"}' in INGEST_TF


def test_terraform_sends_every_setting_only_when_declared():
    block = _data_source_block()
    assert 'dynamic "vector_ingestion_configuration"' in block
    assert "for_each = local.kb_ingestion_set ? [1] : []" in block
    for arg in ("max_tokens         = local.kb_chunk.maxTokens", "overlap_percentage = local.kb_chunk.overlapPercentage",
                "max_tokens = local.kb_chunk.parentMaxTokens", "max_tokens = local.kb_chunk.childMaxTokens",
                "overlap_tokens = local.kb_chunk.overlapTokens", "max_token                       = local.kb_chunk.maxTokens",
                "buffer_size                     = local.kb_chunk.bufferSize",
                "breakpoint_percentile_threshold = local.kb_chunk.breakpointPercentileThreshold",
                "model_arn = local.kb_parse_model_arn", "parsing_prompt_string = local.kb_parse_prompt"):
        assert arg in block, arg
    # The parent layer is the first level, as Bedrock orders them.
    assert block.index("local.kb_chunk.parentMaxTokens") < block.index("local.kb_chunk.childMaxTokens")
    assert "length(local.kb_ingestion_problems) == 0" in block


def test_the_parsing_grant_is_on_the_kb_role_and_only_when_parsing_is_on():
    policy = KB_TF.split('resource "aws_iam_role_policy" "kb"', 1)[1].split("\n}\n", 1)[0]
    assert "local.kb_parsing_statements," in policy
    assert "kb_parsing_statements = !contains(local.vocab.kbParsingStrategies.values, local.kb_parse_strategy) ? [] : [" in INGEST_TF
    assert 'Sid    = "ParsingModel"' in INGEST_TF and 'sid: "ParsingModel"' in INGEST_TS
    assert "foundation-model/*" not in INGEST_TF and "foundation-model/*" not in INGEST_TS


def test_an_existing_knowledge_base_refuses_both_keys_on_every_plane():
    assert '["s3Uri", "kmsKeyArn", "embeddingModel", "dimensions", "dataSource", "vectorStore", "chunking", "parsing"]' in TOOLS_TF
    stack = (ORCH_ROOT / "cdk" / "lib" / "orchestrator-stack.ts").read_text()
    assert '["s3Uri", "kmsKeyArn", "embeddingModel", "dimensions", "dataSource", "vectorStore", "chunking", "parsing"]' in stack
    assert "validateKbIngestion(name, t);" in stack


def test_cdk_wires_the_ingestion_into_the_data_source_and_role():
    assert "name: ingestion.dataSourceName," in PLANE_TS
    assert "vectorIngestionConfiguration: ingestion.vectorIngestionConfiguration" in PLANE_TS
    assert "for (const s of ingestion.statements) kbRole.addToPolicy(s);" in PLANE_TS
    # Before the Knowledge Base: Bedrock checks the role when it is created.
    assert PLANE_TS.index("kbIngestion(kbName, kbSpec, region, account)") < PLANE_TS.index('new bedrock.CfnKnowledgeBase(this, "KnowledgeBase"')
