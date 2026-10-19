/**
 * A type="kb" tool's `vectorStore`: a vector store the customer already has. The
 * Knowledge Base this stack creates is pointed at it (storageConfiguration) instead of the
 * S3 Vectors bucket and index it would otherwise create, and the Knowledge Base role is
 * granted what AWS documents for that store, on exactly the resource named.
 *
 * Mirrors terraform/kb_vector_store.tf (locals, the KB role statements, the name digest),
 * bff/kb_vector_store.py and web/src/builder/kbVectorStore.ts (the Build view's checks).
 *
 * What the docs and CfnKnowledgeBase allow, and what this uses:
 *   - StorageConfiguration types OPENSEARCH_SERVERLESS, OPENSEARCH_MANAGED_CLUSTER, RDS,
 *     NEPTUNE_ANALYTICS, PINECONE, MONGO_DB_ATLAS. (Not REDIS_ENTERPRISE_CLOUD, which the
 *     API and Terraform have and CloudFormation does not.)
 *   - StorageConfiguration is "Update requires: Replacement": the Knowledge Base name
 *     carries a digest of it (kbVectorStoreName), so a change is a clean replacement.
 *   - The role grants are the ones in "Create a service role for Amazon Bedrock Knowledge
 *     Bases": aoss:APIAccessAll on the collection; es:ESHttp* on the index and
 *     es:DescribeDomain on the domain; rds:DescribeDBClusters + rds-data:ExecuteStatement /
 *     BatchExecuteStatement on the cluster; neptune-graph:GetGraph + Read/Write/
 *     DeleteDataViaQuery on the graph; secretsmanager:GetSecretValue on the secret.
 * https://docs.aws.amazon.com/bedrock/latest/userguide/kb-permissions.html
 * https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-properties-bedrock-knowledgebase-storageconfiguration.html
 */
import * as crypto from "crypto";
import * as iam from "aws-cdk-lib/aws-iam";
import type * as bedrock from "aws-cdk-lib/aws-bedrock";
import * as vocab from "./vocabulary";

export interface KbVectorStore { type: string; [setting: string]: string }

const extra = <T>(field: string): T => vocab.extra<T>("kbVectorStores", field) as T;
const AURORA_FIELDS = ["vectorField", "textField", "metadataField"];

export function storeSettings(store: string): string[] {
  return [...(extra<Record<string, string[]>>("required")[store] ?? []), ...(extra<Record<string, string[]>>("optional")[store] ?? [])];
}

/** The checks a synth makes, as errors: the same rules terraform/kb.tf's Knowledge Base
 *  precondition applies at plan. */
export function validateKbVectorStore(name: string, t: { vectorStore?: KbVectorStore }): void {
  const vs = t.vectorStore;
  if (vs === undefined) return;
  const where = `workflow.json tools.${name}.vectorStore`;
  const stores = vocab.values("kbVectorStores");
  if (!stores.includes(vs?.type)) throw new Error(`${where}.type must be one of: ${stores.join(", ")}.`);
  const takes = storeSettings(vs.type);
  const patterns = extra<Record<string, string>>("patterns");
  for (const k of extra<Record<string, string[]>>("required")[vs.type]) {
    if (!vs[k]) throw new Error(`${where}.${k} is required for the ${vs.type} store.`);
  }
  for (const [k, v] of Object.entries(vs)) {
    if (k === "type") continue;
    if (!takes.includes(k)) throw new Error(`${where}.${k} does not apply to the ${vs.type} store.`);
    const pattern = vs.type === "aurora" && AURORA_FIELDS.includes(k) ? extra<string>("columnPattern") : patterns[k];
    if (typeof v !== "string" || !new RegExp(pattern).test(v)) throw new Error(`${where}.${k} "${v}" does not match ${pattern}.`);
  }
}

/** Everything that forces a new Knowledge Base, as one string. Must match
 *  local.kb_vs_key in terraform/kb_vector_store.tf character for character. */
export function kbVectorStoreKey(vs: KbVectorStore, dims: number, embedModelId: string): string {
  return `vs=${vs.type}:${storeSettings(vs.type).map((k) => vs[k] ?? "").join(",")}|d=${dims}|m=${embedModelId}`;
}

export function kbVectorStoreName(dashName: string, vs: KbVectorStore, dims: number, embedModelId: string): string {
  return `${dashName}-kb-${crypto.createHash("sha256").update(kbVectorStoreKey(vs, dims, embedModelId)).digest("hex").slice(0, 8)}`;
}

/** The Knowledge Base's storageConfiguration for the store, and the role's grants on it. */
export function kbVectorStore(vs: KbVectorStore): {
  storageConfiguration: bedrock.CfnKnowledgeBase.StorageConfigurationProperty; statements: iam.PolicyStatement[];
} {
  const type = extra<Record<string, string>>("api")[vs.type];
  const secret = vs.credentialsSecretArn
    ? [new iam.PolicyStatement({ sid: "VectorStoreSecret", actions: ["secretsmanager:GetSecretValue"], resources: [vs.credentialsSecretArn] })]
    : [];
  const fields3 = { vectorField: vs.vectorField, textField: vs.textField, metadataField: vs.metadataField };
  switch (vs.type) {
    case "opensearchServerless":
      return {
        storageConfiguration: { type, opensearchServerlessConfiguration: { collectionArn: vs.collectionArn, vectorIndexName: vs.indexName, fieldMapping: fields3 } },
        statements: [new iam.PolicyStatement({ sid: "VectorStore", actions: ["aoss:APIAccessAll"], resources: [vs.collectionArn] })],
      };
    case "opensearchManaged":
      return {
        storageConfiguration: {
          type,
          opensearchManagedClusterConfiguration: { domainArn: vs.domainArn, domainEndpoint: vs.domainEndpoint, vectorIndexName: vs.indexName, fieldMapping: fields3 },
        },
        statements: [
          // The index and the requests under it (_search, _doc, ...): that index only.
          new iam.PolicyStatement({ sid: "VectorStore", actions: ["es:ESHttpGet", "es:ESHttpPost", "es:ESHttpPut", "es:ESHttpDelete"],
            resources: [`${vs.domainArn}/${vs.indexName}`, `${vs.domainArn}/${vs.indexName}/*`] }),
          new iam.PolicyStatement({ sid: "VectorStoreDomain", actions: ["es:DescribeDomain"], resources: [vs.domainArn] }),
        ],
      };
    case "aurora":
      return {
        storageConfiguration: {
          type,
          rdsConfiguration: {
            resourceArn: vs.resourceArn, credentialsSecretArn: vs.credentialsSecretArn, databaseName: vs.databaseName, tableName: vs.tableName,
            fieldMapping: { ...fields3, primaryKeyField: vs.primaryKeyField, ...(vs.customMetadataField ? { customMetadataField: vs.customMetadataField } : {}) },
          },
        },
        statements: [
          new iam.PolicyStatement({ sid: "VectorStore", actions: ["rds:DescribeDBClusters", "rds-data:BatchExecuteStatement", "rds-data:ExecuteStatement"],
            resources: [vs.resourceArn] }),
          ...secret,
        ],
      };
    case "neptuneAnalytics":
      return {
        storageConfiguration: { type, neptuneAnalyticsConfiguration: { graphArn: vs.graphArn, fieldMapping: { textField: vs.textField, metadataField: vs.metadataField } } },
        statements: [new iam.PolicyStatement({ sid: "VectorStore",
          actions: ["neptune-graph:GetGraph", "neptune-graph:ReadDataViaQuery", "neptune-graph:WriteDataViaQuery", "neptune-graph:DeleteDataViaQuery"],
          resources: [vs.graphArn] })],
      };
    case "pinecone":
      return {
        storageConfiguration: {
          type,
          pineconeConfiguration: {
            connectionString: vs.connectionString, credentialsSecretArn: vs.credentialsSecretArn,
            ...(vs.namespace ? { namespace: vs.namespace } : {}),
            fieldMapping: { textField: vs.textField, metadataField: vs.metadataField },
          },
        },
        statements: secret,
      };
    case "mongodbAtlas":
      return {
        storageConfiguration: {
          type,
          mongoDbAtlasConfiguration: {
            endpoint: vs.endpoint, credentialsSecretArn: vs.credentialsSecretArn, databaseName: vs.databaseName,
            collectionName: vs.collectionName, vectorIndexName: vs.indexName, fieldMapping: fields3,
            ...(vs.endpointServiceName ? { endpointServiceName: vs.endpointServiceName } : {}),
            ...(vs.textIndexName ? { textIndexName: vs.textIndexName } : {}),
          },
        },
        statements: secret,
      };
    default:
      throw new Error(`vectorStore type ${vs.type} is not a known store.`);
  }
}
