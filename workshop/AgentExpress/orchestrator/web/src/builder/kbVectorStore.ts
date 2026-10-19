/** A type="kb" tool's `vectorStore`: a vector store the customer already has, which the
 *  Knowledge Base writes to and searches instead of the S3 Vectors store the framework
 *  would create. Bedrock checks it only when the Knowledge Base is created, and its ARNs
 *  go into the Knowledge Base role's grants, so each store's settings are checked here
 *  first, from one home (vocabulary.json kbVectorStores). Mirrors bff/kb_vector_store.py
 *  word for word (both run tests/fixtures/validation_cases.json), cdk/lib/kb-vector-store.ts
 *  and the precondition in terraform/kb.tf.
 *  https://docs.aws.amazon.com/bedrock/latest/userguide/knowledge-base-setup.html */
import type { Issue, Where } from "./validate";
import { vocab, vocabExtra } from "./meta";

type Add = (severity: Issue["severity"], where: Where, path: string, message: string) => void;
const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);
/** Aurora names columns, not document fields: Bedrock's ColumnName shape. */
const AURORA_FIELDS = ["vectorField", "textField", "metadataField"];

export function requiredSettings(store: string): string[] {
  return vocabExtra<Record<string, string[]>>("kbVectorStores", "required")?.[store] ?? [];
}

/** Every setting a store takes, required first, in the digest's order. */
export function storeSettings(store: string): string[] {
  return [...requiredSettings(store), ...(vocabExtra<Record<string, string[]>>("kbVectorStores", "optional")?.[store] ?? [])];
}

export function checkKbVectorStore(tool: Record<string, unknown>, path: string, where: Where, add: Add): void {
  const vs = tool.vectorStore;
  if (!isObj(vs)) return;
  const p = `${path}.vectorStore`;
  const stores = vocab("kbVectorStores");
  const store = vs.type;
  if (typeof store !== "string" || !stores.includes(store)) {
    add("error", where, `${p}.type`, `must be one of: ${stores.join(", ")}`);
    return;
  }
  const takes = storeSettings(store);
  const patterns = vocabExtra<Record<string, string>>("kbVectorStores", "patterns") ?? {};
  const column = vocabExtra<string>("kbVectorStores", "columnPattern") ?? "";
  for (const name of requiredSettings(store)) {
    if (vs[name] === undefined || vs[name] === null || vs[name] === "") {
      add("error", where, `${p}.${name}`, `is required for the ${store} store`);
    }
  }
  for (const [name, value] of Object.entries(vs)) {
    if (name === "type") continue;
    if (!takes.includes(name)) {
      add("error", where, `${p}.${name}`, `does not apply to the ${store} store`);
      continue;
    }
    if (typeof value !== "string" || value === "") continue;
    const pattern = store === "aurora" && AURORA_FIELDS.includes(name) ? column : patterns[name] ?? "";
    if (pattern && !new RegExp(pattern).test(value)) {
      add("error", where, `${p}.${name}`, `"${value}" does not match ${pattern}`);
    }
  }
}
