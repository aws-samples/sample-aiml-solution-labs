/** A Knowledge Base tool's `vectorStore`: where its vectors live. "Created for you" removes
 *  the key (the S3 Vectors store the framework makes); any other choice shows that store's
 *  settings, each described from app/keys.json. The checks are in kbVectorStore.ts. */
import FormField from "@cloudscape-design/components/form-field";
import Input from "@cloudscape-design/components/input";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import type { Json } from "./model";
import { block } from "./meta";
import { requiredSettings, storeSettings } from "./kbVectorStore";

type Obj = { [k: string]: Json };
const asObj = (v: Json | undefined): Obj => (v && typeof v === "object" && !Array.isArray(v) ? v : {});

export const STORES: { value: string; label: string }[] = [
  { value: "", label: "Created for you: Amazon S3 Vectors (pay per use)" },
  { value: "opensearchServerless", label: "Amazon OpenSearch Serverless" },
  { value: "opensearchManaged", label: "Amazon OpenSearch Service (managed cluster)" },
  { value: "aurora", label: "Amazon Aurora PostgreSQL (pgvector)" },
  { value: "neptuneAnalytics", label: "Amazon Neptune Analytics (GraphRAG)" },
  { value: "pinecone", label: "Pinecone" },
  { value: "mongodbAtlas", label: "MongoDB Atlas" },
];

const LABELS: Record<string, string> = {
  collectionArn: "Collection ARN", domainArn: "Domain ARN", domainEndpoint: "Domain endpoint", indexName: "Vector index name",
  resourceArn: "Cluster ARN", databaseName: "Database name", tableName: "Table name", collectionName: "Collection name",
  graphArn: "Graph ARN", graphModel: "Graph-building model", connectionString: "Index host (connection string)",
  namespace: "Namespace", endpoint: "Endpoint", endpointServiceName: "PrivateLink service name",
  textIndexName: "Text search index", credentialsSecretArn: "Credentials secret ARN", vectorField: "Vector field",
  textField: "Text field", metadataField: "Metadata field", primaryKeyField: "Primary key column",
  customMetadataField: "Custom metadata column",
};

export function VectorStoreField({ value, onChange }: { value: Json | undefined; onChange: (v: Json | undefined) => void }) {
  const cur = asObj(value);
  const store = typeof cur.type === "string" ? cur.type : "";
  const selected = STORES.find((o) => o.value === store) ?? { value: store, label: store };
  const docs = (block("tool").keys.vectorStore?.properties ?? {}) as Record<string, { doc?: string }>;
  const required = requiredSettings(store);
  return (
    <SpaceBetween size="s">
      <Select ariaLabel="Where vectors are stored" selectedOption={selected} options={STORES}
        onChange={({ detail }) => {
          const next = detail.selectedOption.value ?? "";
          // A new store starts empty: settings of one store mean nothing to another.
          onChange(next ? { type: next } : undefined);
        }} />
      {storeSettings(store).map((k) => (
        <FormField key={k} label={LABELS[k] ?? k} description={docs[k]?.doc}
          constraintText={required.includes(k) ? "Required" : "Optional"}>
          <Input ariaLabel={LABELS[k] ?? k} value={typeof cur[k] === "string" ? (cur[k] as string) : ""}
            onChange={({ detail }) => {
              const next: Obj = { ...cur };
              const v = detail.value.trim();
              if (v === "") delete next[k]; else next[k] = v;
              onChange(next);
            }} />
        </FormField>
      ))}
    </SpaceBetween>
  );
}
