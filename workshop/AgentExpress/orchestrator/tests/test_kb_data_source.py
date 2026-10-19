"""A type="kb" tool's `dataSource`: documents from a web crawl or a Confluence, SharePoint or
Salesforce site.

The Build view and the BFF check the shape (tests/fixtures/validation_cases.json), and
cdk/test/kb-data-source.test.ts pins the CDK synth. This file pins the rest: that the
settings and their limits are Bedrock's, that Terraform wires every connector CDK wires,
that the only grant is the connector's secret, and that a tool without `dataSource`
deploys as before.
"""
from __future__ import annotations

import json
import sys

import pytest
from conftest import ORCH_ROOT

sys.path.insert(0, str(ORCH_ROOT / "bff"))
import builds
import kb_data_source
import validate_build

VOCAB = json.loads((ORCH_ROOT / "app" / "vocabulary.json").read_text())["kbDataSources"]
KB_TF = (ORCH_ROOT / "terraform" / "kb.tf").read_text()
DS_TF = (ORCH_ROOT / "terraform" / "kb_data_source.tf").read_text()
DS_TS = (ORCH_ROOT / "cdk" / "lib" / "kb-data-source.ts").read_text()
PLANE_TS = (ORCH_ROOT / "cdk" / "lib" / "tool-plane.ts").read_text()
OSS = {"type": "opensearchServerless"}

#: Our connector -> (botocore DataSourceConfiguration member, our setting -> where Bedrock has it).
_SOURCE = {"web": "WebSourceConfiguration", "confluence": "ConfluenceSourceConfiguration",
           "sharepoint": "SharePointSourceConfiguration", "salesforce": "SalesforceSourceConfiguration"}


def _issues(ds: dict, **tool) -> list[tuple[str, str]]:
    out: list = []
    kb_data_source.check({"dataSource": ds, "vectorStore": OSS, **tool}, "tools.kb", {"kind": "tool", "id": "kb"},
                         lambda sev, where, path, msg: out.append((sev, path)),
                         validate_build.vocab_extra, validate_build.vocab)
    return out


def test_the_connectors_and_limits_are_bedrocks():
    botocore = pytest.importorskip("botocore.session")
    m = botocore.get_session().get_service_model("bedrock-agent")
    assert set(VOCAB["api"].values()) <= set(m.shape_for("DataSourceType").enum)
    assert set(VOCAB["api"]) == set(VOCAB["values"]) == set(VOCAB["required"]) == set(VOCAB["optional"])
    s = VOCAB["settings"]
    limits = m.shape_for("WebCrawlerLimits")
    assert s["rateLimit"]["min"] == limits.members["rateLimit"].metadata["min"]
    assert s["rateLimit"]["max"] == limits.members["rateLimit"].metadata["max"]
    assert s["seedUrls"]["max"] == m.shape_for("SeedUrls").metadata["max"]
    assert s["siteUrls"]["max"] == m.shape_for("SharePointSiteUrls").metadata["max"]
    assert s["inclusionFilters"]["max"] == s["exclusionFilters"]["max"] == m.shape_for("FilterList").metadata["max"]
    assert s["filters"]["max"] == m.shape_for("PatternObjectFilterList").metadata["max"]
    ua = m.shape_for("UserAgent").metadata
    assert s["userAgent"]["pattern"] == f"^.{{{ua['min']},{ua['max']}}}$"
    assert set(s["scope"]["values"].values()) == set(m.shape_for("WebScopeType").enum)
    assert set(s["authType"]["values"].values()) == set(m.shape_for("ConfluenceAuthType").enum)
    # The values hard-coded in the IaC are the ones Bedrock accepts.
    assert m.shape_for("ConfluenceHostType").enum == ["SAAS"] and "ONLINE" in m.shape_for("SharePointHostType").enum
    assert "OAUTH2_CLIENT_CREDENTIALS" in m.shape_for("SharePointAuthType").enum
    assert m.shape_for("SalesforceAuthType").enum == ["OAUTH2_CLIENT_CREDENTIALS"]
    # Every required source setting of Bedrock's is one we ask for.
    for kind, shape in _SOURCE.items():
        req = set(m.shape_for(shape).required_members) - {"hostType", "urlConfiguration", "authType"}
        assert req <= set(VOCAB["required"][kind]), (kind, req)


def test_a_connector_needs_opensearch_serverless():
    web = {"type": "web", "seedUrls": ["https://docs.example.com/"]}
    assert _issues(web) == []
    for store in (None, {"type": "aurora"}, {"type": "pinecone"}):
        out: list = []
        kb_data_source.check({"dataSource": web, "vectorStore": store}, "tools.kb", {}, lambda *a: out.append(a[2]),
                             validate_build.vocab_extra, validate_build.vocab)
        assert out == ["tools.kb.dataSource.type"], store


def test_a_bool_is_not_a_page_count():
    assert ("error", "tools.kb.dataSource.maxPages") not in _issues(
        {"type": "web", "seedUrls": ["https://a.b/"], "maxPages": 10})
    assert not kb_data_source._whole(True)


def test_terraform_wires_every_connector_and_keeps_s3_by_default():
    block = KB_TF.split('resource "aws_bedrockagent_data_source" "kb"', 1)[1].split("\n}\n", 1)[0]
    for blk, kind in (("web_configuration", "web"), ("confluence_configuration", "confluence"),
                      ("share_point_configuration", "sharepoint"), ("salesforce_configuration", "salesforce")):
        assert f'dynamic "{blk}"' in block and f'local.kb_ds_type == "{kind}"' in block, kind
    assert 'type = local.kb_ds_set ? local.kb_ds_vocab.api[local.kb_ds_type] : "S3"' in block
    assert 'for_each = local.kb_ds_set ? [] : [1]' in block            # s3_configuration, as before
    assert '(local.kb_ds != null ? "connector" : "upload")' in KB_TF
    assert "length(local.kb_ds_problems) == 0" in KB_TF
    for v in ('host_type              = "SAAS"', 'host_type              = "ONLINE"', 'auth_type              = "OAUTH2_CLIENT_CREDENTIALS"'):
        assert v in block, v


def test_the_only_grant_is_the_connectors_secret_on_both_planes():
    assert 'Sid      = "ConnectorSecret"' in DS_TF and 'sid: "ConnectorSecret"' in DS_TS
    for plane in (DS_TF, DS_TS):
        assert '"secretsmanager:GetSecretValue"' in plane and '"secretsmanager:PutSecretValue"' in plane
        assert '"*"' not in plane
    # PutSecretValue only for Confluence OAuth, which rotates its refresh token.
    assert 'local.kb_ds_type == "confluence" && try(local.kb_ds.authType, "") == "oauth2"' in DS_TF
    assert 'ds.type === "confluence" && ds.authType === "oauth2"' in DS_TS
    assert "local.kb_ds_statements," in KB_TF
    assert "for (const s of connector?.statements ?? []) kbRole.addToPolicy(s);" in PLANE_TS


def test_no_documents_bucket_or_upload_check_for_a_connector():
    assert '"connector"' in PLANE_TS and 'kbSpec.dataSource ? "connector" : "upload"' in PLANE_TS
    assert '} else if (kbSource === "s3") {' in PLANE_TS
    # No upload is expected for a connector, so the deploy does not ask for one (and returns
    # before it would list the build's documents).
    wf = {"tools": {"kb": {"type": "kb", "corpora": ["x"], "dataSource": {"type": "web"}}}}
    assert builds.empty_corpora("b1", "owner", wf) == []
    stack = (ORCH_ROOT / "cdk" / "lib" / "orchestrator-stack.ts").read_text()
    assert "!kbEntry[1].knowledgeBaseId && !kbEntry[1].dataSource" in stack
    assert "validateKbConnector(name, t);" in stack
