"""A type="kb" tool's `vectorStore`: the Knowledge Base on a store the customer already has.

The Build view and the BFF check the shape (tests/fixtures/validation_cases.json holds the
cases both validators must agree on), and cdk/test/kb-vector-store.test.ts pins the CDK
synth. This file pins what neither can: that every store's settings and patterns come from
Bedrock's own API model, that Terraform wires every store CDK wires (plus Redis), that the
grants are the documented ones, and that a tool without `vectorStore` deploys as before.
"""
from __future__ import annotations

import json
import re
import sys

import pytest
from conftest import ORCH_ROOT

sys.path.insert(0, str(ORCH_ROOT / "bff"))
import kb_vector_store
import validate_build

VOCAB = json.loads((ORCH_ROOT / "app" / "vocabulary.json").read_text())["kbVectorStores"]
KB_TF = (ORCH_ROOT / "terraform" / "kb.tf").read_text()
VS_TF = (ORCH_ROOT / "terraform" / "kb_vector_store.tf").read_text()
VS_TS = (ORCH_ROOT / "cdk" / "lib" / "kb-vector-store.ts").read_text()
PLANE_TS = (ORCH_ROOT / "cdk" / "lib" / "tool-plane.ts").read_text()

#: Our store -> (the botocore StorageConfiguration member, our setting -> their member).
_BOTOCORE = {
    "opensearchServerless": ("opensearchServerlessConfiguration", {"collectionArn": "collectionArn", "indexName": "vectorIndexName"}),
    "opensearchManaged": ("opensearchManagedClusterConfiguration",
                          {"domainArn": "domainArn", "domainEndpoint": "domainEndpoint", "indexName": "vectorIndexName"}),
    "aurora": ("rdsConfiguration", {"resourceArn": "resourceArn", "credentialsSecretArn": "credentialsSecretArn",
                                    "databaseName": "databaseName", "tableName": "tableName"}),
    "neptuneAnalytics": ("neptuneAnalyticsConfiguration", {"graphArn": "graphArn"}),
    "pinecone": ("pineconeConfiguration", {"connectionString": "connectionString", "credentialsSecretArn": "credentialsSecretArn",
                                           "namespace": "namespace"}),
    "mongodbAtlas": ("mongoDbAtlasConfiguration", {"endpoint": "endpoint", "credentialsSecretArn": "credentialsSecretArn",
                                                   "databaseName": "databaseName", "collectionName": "collectionName",
                                                   "indexName": "vectorIndexName", "endpointServiceName": "endpointServiceName",
                                                   "textIndexName": "textIndexName"}),
}
_FIELDS = {"vectorField", "textField", "metadataField", "primaryKeyField", "customMetadataField"}


def _issues(vs: dict) -> list[tuple[str, str]]:
    out: list = []
    kb_vector_store.check({"vectorStore": vs}, "tools.kb", {"kind": "tool", "id": "kb"},
                          lambda sev, where, path, msg: out.append((sev, path)),
                          validate_build.vocab_extra, validate_build.vocab)
    return out


def test_every_store_matches_the_bedrock_agent_model():
    """Required means required in Bedrock's StorageConfiguration, and nothing is missing."""
    botocore = pytest.importorskip("botocore.session")
    model = botocore.get_session().get_service_model("bedrock-agent")
    sc = model.shape_for("StorageConfiguration")
    assert set(VOCAB["api"].values()) <= set(sc.members["type"].enum)
    assert set(VOCAB["api"]) == set(VOCAB["values"]) == set(VOCAB["required"]) == set(VOCAB["optional"])
    for store, (member, names) in _BOTOCORE.items():
        shape = sc.members[member]
        fm = shape.members["fieldMapping"]
        ours = set(VOCAB["required"][store]) | set(VOCAB["optional"][store])
        theirs = {k for k, v in names.items()} | {f for f in fm.members}
        # graphModel is not storage: it is the data source's context enrichment.
        assert ours - {"graphModel"} == theirs, store
        for o, t in names.items():
            assert (o in VOCAB["required"][store]) == (t in shape.required_members), (store, o)
        for f in fm.members:
            assert (f in VOCAB["required"][store]) == (f in fm.required_members), (store, f)


def test_the_patterns_are_bedrocks_and_anchored():
    botocore = pytest.importorskip("botocore.session")
    model = botocore.get_session().get_service_model("bedrock-agent")
    for name, shape in (("collectionArn", "OpenSearchServerlessCollectionArn"), ("graphArn", "GraphArn"),
                        ("credentialsSecretArn", "SecretArn"), ("resourceArn", "RdsArn")):
        assert model.shape_for(shape).metadata["pattern"]           # Bedrock constrains it too
    for name, pattern in VOCAB["patterns"].items():
        assert pattern.startswith("^") and pattern.endswith("$"), name
        # RE2 (Terraform regex()) refuses a repeat count over 1000.
        assert all(int(n) <= 1000 for n in re.findall(r"\{(?:\d+,)?(\d+)\}", pattern)), name
    # An ARN in an IAM grant must name its account: no empty or wildcard account.
    for name in ("collectionArn", "domainArn", "resourceArn", "credentialsSecretArn", "graphArn"):
        assert ":[0-9]{12}:" in VOCAB["patterns"][name], name


@pytest.mark.parametrize("arn", ["arn:aws:aoss:us-east-1:*:collection/abc", "arn:aws:aoss:us-east-1::collection/abc",
                                 "arn:aws:aoss:us-east-1:123456789012:collection/*"])
def test_a_wildcard_or_accountless_arn_is_refused(arn):
    vs = {"type": "opensearchServerless", "collectionArn": arn, "indexName": "i", "vectorField": "v",
          "textField": "t", "metadataField": "m"}
    assert ("error", "tools.kb.vectorStore.collectionArn") in _issues(vs)


def test_redis_is_not_offered():
    # CloudFormation cannot express it, so it would be Terraform only; left out for now.
    assert "redisEnterprise" not in VOCAB["values"] and "redis" not in KB_TF.lower()


def test_terraform_has_a_storage_block_for_every_store():
    block = KB_TF.split('resource "aws_bedrockagent_knowledge_base" "kb"', 1)[1].split("\n}\n", 1)[0]
    for store in VOCAB["values"]:
        assert f'local.kb_vs_type == "{store}" ? [local.kb_vs_v] : []' in block, store
    assert 'type = local.kb_vs_set ? local.kb_vs_vocab.api[local.kb_vs_type] : "S3_VECTORS"' in block
    assert "length(local.kb_vs_problems) == 0" in block


def test_without_a_store_nothing_changes():
    # The S3 Vectors resources keep their addresses; their count is the new local, which is
    # exactly the old condition when no `vectorStore` is set.
    assert 'kb_s3vectors = local.kb_create && local.kb_vs == null' in VS_TF
    for r in ('resource "aws_s3vectors_vector_bucket" "kb"', 'resource "aws_s3vectors_index" "kb"'):
        assert KB_TF.split(r, 1)[1].split("\n", 2)[1].strip() == "count              = local.kb_s3vectors ? 1 : 0"
    assert 'kb_name     = local.kb_vs_set ? local.kb_vs_name : "${replace(var.agent_name, "_", "-")}-kb-${local.kb_storage_digest}"' in KB_TF
    # The data source keeps its address unless the store is Neptune (context enrichment).
    assert 'count             = local.kb_create && local.kb_graph_model == "" ? 1 : 0' in KB_TF
    assert "kbSpec.vectorStore ? kbVectorStore(kbSpec.vectorStore) : undefined" in PLANE_TS


def test_the_grants_are_the_documented_ones_on_both_planes():
    for action in ("aoss:APIAccessAll", "es:ESHttpGet", "es:ESHttpPost", "es:ESHttpPut", "es:ESHttpDelete",
                   "es:DescribeDomain", "rds:DescribeDBClusters", "rds-data:BatchExecuteStatement",
                   "rds-data:ExecuteStatement", "neptune-graph:GetGraph", "neptune-graph:ReadDataViaQuery",
                   "neptune-graph:WriteDataViaQuery", "neptune-graph:DeleteDataViaQuery", "secretsmanager:GetSecretValue"):
        assert f'"{action}"' in VS_TF and f'"{action}"' in VS_TS, action
    assert '"*"' not in VS_TF.split("kb_vs_statements", 1)[1].split("kb_vs_pattern", 1)[0]
    # The store grants reach the Knowledge Base role.
    assert "local.kb_vs_statements," in KB_TF and "local.kb_graph_statements," in KB_TF
    assert "for (const s of ownStore?.statements ?? []) kbRole.addToPolicy(s);" in PLANE_TS


def test_the_key_digest_is_built_the_same_way_on_both_planes():
    assert 'kb_vs_key  = "vs=${local.kb_vs_type}:${join(",", [for k in local.kb_vs_settings : local.kb_vs_v[k]])}|d=${local.kb_dims}|m=${local.kb_embed_model_id}"' in VS_TF
    assert "`vs=${vs.type}:${storeSettings(vs.type).map((k) => vs[k] ?? \"\").join(\",\")}|d=${dims}|m=${embedModelId}`" in VS_TS


def test_neptune_builds_its_graph_with_the_named_model_on_terraform():
    graph = KB_TF.split('resource "awscc_bedrock_data_source" "kb_graph"', 1)[1].split("\n}\n", 1)[0]
    assert 'count             = local.kb_create && local.kb_graph_model != "" ? 1 : 0' in graph
    assert "vector_ingestion_configuration = local.kb_ingestion_awscc" in graph
    ingest = (ORCH_ROOT / "terraform" / "kb_ingestion.tf").read_text()
    assert 'enrichment_strategy_configuration = { method = "CHUNK_ENTITY_EXTRACTION" }' in ingest
    assert "kb_data_source_id = try(aws_bedrockagent_data_source.kb[0].data_source_id, awscc_bedrock_data_source.kb_graph[0].data_source_id" in KB_TF
