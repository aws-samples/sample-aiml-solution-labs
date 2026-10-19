/**
 * A type="kb" tool's `vectorStore` (lib/kb-vector-store.ts), synthesized through ToolPlane:
 * the Knowledge Base points at YOUR store, no S3 Vectors bucket or index is made, and the
 * Knowledge Base role is granted only what AWS documents for that store, on that resource.
 */
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import * as path from "path";

import { ToolPlane, ToolSpec } from "../lib/tool-plane";
import { validateTools } from "../lib/orchestrator-stack";
import { kbVectorStoreName } from "../lib/kb-vector-store";

const ORCH_ROOT = path.join(__dirname, "..", "..");
const ACCOUNT = "123456789012";
const SECRET = `arn:aws:secretsmanager:us-east-1:${ACCOUNT}:secret:kb-AbCdEf`;

function plane(vectorStore?: Record<string, string>) {
  const app = new cdk.App();
  const stack = new cdk.Stack(app, "T", { env: { account: ACCOUNT, region: "us-east-1" } });
  new ToolPlane(stack, "ToolPlane", {
    agentName: "test_orch",
    tools: { kb: { type: "kb", description: "Corpus", corpora: ["reference"], ...(vectorStore ? { vectorStore } : {}) } as ToolSpec },
    toolApiKeys: {},
    gatewayDiscoveryUrl: "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc/.well-known/openid-configuration",
    gatewayClientId: "client-abc",
    gatewayAudience: "gateway/invoke",
    isCognito: true,
    policyEnabled: true,
    policyMode: "ENFORCE",
    customPolicies: [],
    orchRoot: ORCH_ROOT,
  });
  return Template.fromStack(stack);
}

const kbProps = (t: Template) => Object.values<any>(t.findResources("AWS::Bedrock::KnowledgeBase"))[0].Properties;
const roleStatements = (t: Template): any[] => Object.entries<any>(t.findResources("AWS::IAM::Policy"))
  .filter(([id]) => id.includes("KbRole")).flatMap(([, p]) => p.Properties.PolicyDocument.Statement);
const asList = (v: any) => (Array.isArray(v) ? v : [v]);

const OSS = { type: "opensearchServerless", collectionArn: `arn:aws:aoss:us-east-1:${ACCOUNT}:collection/abc123`,
  indexName: "kb-index", vectorField: "vec", textField: "text", metadataField: "meta" };
const MANAGED = { type: "opensearchManaged", domainArn: `arn:aws:es:us-east-1:${ACCOUNT}:domain/kbdomain`,
  domainEndpoint: "https://search-kb.us-east-1.es.amazonaws.com", indexName: "kb", vectorField: "v", textField: "t", metadataField: "m" };
const AURORA = { type: "aurora", resourceArn: `arn:aws:rds:us-east-1:${ACCOUNT}:cluster:kb`, credentialsSecretArn: SECRET,
  databaseName: "postgres", tableName: "bedrock_integration.bedrock_kb", primaryKeyField: "id", vectorField: "embedding",
  textField: "chunks", metadataField: "metadata", customMetadataField: "custom_metadata" };
const NEPTUNE = { type: "neptuneAnalytics", graphArn: `arn:aws:neptune-graph:us-east-1:${ACCOUNT}:graph/g-abcdefghij`,
  graphModel: "us.anthropic.claude-haiku-4-5-20251001-v1:0", textField: "text", metadataField: "meta" };
const PINECONE = { type: "pinecone", connectionString: "https://kb-abc.svc.pinecone.io", credentialsSecretArn: SECRET,
  namespace: "docs", textField: "text", metadataField: "meta" };
const MONGO = { type: "mongodbAtlas", endpoint: "cluster0.abc.mongodb.net", credentialsSecretArn: SECRET, databaseName: "kb",
  collectionName: "chunks", indexName: "vector_index", vectorField: "embedding", textField: "text", metadataField: "metadata" };

describe("kb vector store", () => {
  it("without one: S3 Vectors as before, and no store grant", () => {
    const t = plane();
    t.resourceCountIs("AWS::S3Vectors::VectorBucket", 1);
    expect(kbProps(t).StorageConfiguration.Type).toBe("S3_VECTORS");
    expect(roleStatements(t).some((s) => String(s.Sid).startsWith("VectorStore"))).toBe(false);
  });

  it("OpenSearch Serverless: no S3 Vectors, the collection named, aoss:APIAccessAll on it only", () => {
    const t = plane(OSS);
    t.resourceCountIs("AWS::S3Vectors::VectorBucket", 0);
    t.resourceCountIs("AWS::S3Vectors::Index", 0);
    expect(kbProps(t).StorageConfiguration).toEqual({
      Type: "OPENSEARCH_SERVERLESS",
      OpensearchServerlessConfiguration: { CollectionArn: OSS.collectionArn, VectorIndexName: "kb-index",
        FieldMapping: { VectorField: "vec", TextField: "text", MetadataField: "meta" } },
    });
    const s = roleStatements(t);
    expect(s.find((x) => x.Sid === "VectorStore")).toMatchObject({ Action: "aoss:APIAccessAll", Resource: OSS.collectionArn });
    expect(s.some((x) => x.Sid === "S3VectorsData")).toBe(false);
  });

  it("an OpenSearch domain: HTTP verbs on that index only, DescribeDomain on the domain", () => {
    const s = roleStatements(plane(MANAGED));
    expect(asList(s.find((x) => x.Sid === "VectorStore").Resource)).toEqual([`${MANAGED.domainArn}/kb`, `${MANAGED.domainArn}/kb/*`]);
    expect(s.find((x) => x.Sid === "VectorStoreDomain")).toMatchObject({ Action: "es:DescribeDomain", Resource: MANAGED.domainArn });
  });

  it("Aurora: the Data API on that cluster and that secret, custom metadata column passed", () => {
    const t = plane(AURORA);
    expect(kbProps(t).StorageConfiguration.RdsConfiguration).toEqual({
      ResourceArn: AURORA.resourceArn, CredentialsSecretArn: SECRET, DatabaseName: "postgres", TableName: "bedrock_integration.bedrock_kb",
      FieldMapping: { PrimaryKeyField: "id", VectorField: "embedding", TextField: "chunks", MetadataField: "metadata", CustomMetadataField: "custom_metadata" },
    });
    const s = roleStatements(t);
    expect(s.find((x) => x.Sid === "VectorStore")).toMatchObject({
      Action: ["rds:DescribeDBClusters", "rds-data:BatchExecuteStatement", "rds-data:ExecuteStatement"], Resource: AURORA.resourceArn });
    expect(s.find((x) => x.Sid === "VectorStoreSecret")).toMatchObject({ Action: "secretsmanager:GetSecretValue", Resource: SECRET });
  });

  it("Neptune Analytics: the graph, and the data source builds it with the graph model (GraphRAG)", () => {
    const t = plane(NEPTUNE);
    expect(kbProps(t).StorageConfiguration.NeptuneAnalyticsConfiguration).toEqual({
      GraphArn: NEPTUNE.graphArn, FieldMapping: { TextField: "text", MetadataField: "meta" } });
    const ds = Object.values<any>(t.findResources("AWS::Bedrock::DataSource"))[0].Properties;
    const profile = `arn:aws:bedrock:us-east-1:${ACCOUNT}:inference-profile/${NEPTUNE.graphModel}`;
    expect(ds.VectorIngestionConfiguration.ContextEnrichmentConfiguration).toEqual({
      Type: "BEDROCK_FOUNDATION_MODEL",
      BedrockFoundationModelConfiguration: { ModelArn: profile, EnrichmentStrategyConfiguration: { Method: "CHUNK_ENTITY_EXTRACTION" } },
    });
    // Same digest as terraform/kb_ingestion.tf for these settings (terraform console).
    expect(ds.Name).toBe("docs-7ad58cdd");
    const s = roleStatements(t);
    expect(asList(s.find((x) => x.Sid === "GraphModel").Resource)).toEqual(
      [profile, "arn:aws:bedrock:*::foundation-model/anthropic.claude-haiku-4-5-20251001-v1:0"]);
    expect(asList(s.find((x) => x.Sid === "VectorStore").Action)).toContain("neptune-graph:WriteDataViaQuery");
  });

  it("Pinecone and MongoDB Atlas: only the secret", () => {
    for (const vs of [PINECONE, MONGO]) {
      const s = roleStatements(plane(vs)).filter((x) => String(x.Sid).startsWith("VectorStore"));
      expect(s).toEqual([expect.objectContaining({ Sid: "VectorStoreSecret", Resource: SECRET })]);
    }
    expect(kbProps(plane(PINECONE)).StorageConfiguration.PineconeConfiguration.Namespace).toBe("docs");
    expect(kbProps(plane(MONGO)).StorageConfiguration.MongoDbAtlasConfiguration.VectorIndexName).toBe("vector_index");
  });

  it("names the Knowledge Base from a digest of the store (same digests as Terraform)", () => {
    // Literals computed by terraform/kb_vector_store.tf for the same stores (terraform console).
    const m = "amazon.titan-embed-text-v2:0";
    expect(kbVectorStoreName("test-orch", OSS, 1024, m)).toBe("test-orch-kb-a42a8b84");
    expect(kbVectorStoreName("test-orch", NEPTUNE, 1024, m)).toBe("test-orch-kb-8b7a894d");
    expect(kbVectorStoreName("test-orch", PINECONE, 1024, m)).toBe("test-orch-kb-f882ce98");
    expect(kbVectorStoreName("test-orch", AURORA, 1024, m)).toBe("test-orch-kb-84fc6587");
    expect(kbVectorStoreName("test-orch", MANAGED, 1024, m)).toBe("test-orch-kb-a5e806f6");
    expect(kbProps(plane(OSS)).Name).toBe("test-orch-kb-a42a8b84");
  });

  it("refuses what Bedrock would refuse", () => {
    const v = (vectorStore: Record<string, unknown>) => () =>
      validateTools({ kb: { type: "kb", corpora: ["r"], vectorStore } }, {}, "us-east-1");
    expect(v({ type: "chroma" })).toThrow(/type must be one of: opensearchServerless/);
    expect(v({ ...OSS, collectionArn: undefined as any })).toThrow(/collectionArn is required/);
    expect(v({ ...OSS, collectionArn: "arn:aws:aoss:us-east-1:*:collection/x" })).toThrow(/does not match/);
    expect(v({ ...OSS, graphArn: NEPTUNE.graphArn })).toThrow(/graphArn does not apply/);
    expect(v({ ...AURORA, vectorField: "bad field" })).toThrow(/vectorField "bad field" does not match/);
    expect(v({ type: "redisEnterprise" })).toThrow(/type must be one of/);
    expect(() => validateTools({ kb: { type: "kb", corpora: ["r"], knowledgeBaseId: "ABCDEFGH12", vectorStore: OSS } }, {}, "us-east-1"))
      .toThrow(/vectorStore.*do not apply/);
    expect(v(OSS)).not.toThrow();
  });
});
