/** A type="kb" tool's `dataSource`: documents from a web crawl or a Confluence, SharePoint
 *  or Salesforce site, instead of files in S3. AWS supports these connectors only on an
 *  OpenSearch Serverless vector store, and Bedrock checks a connector only when the data
 *  source is created, so the settings are checked here first, from one home
 *  (vocabulary.json kbDataSources). Mirrors bff/kb_data_source.py word for word (both run
 *  tests/fixtures/validation_cases.json), cdk/lib/kb-data-source.ts and terraform/kb.tf.
 *  https://docs.aws.amazon.com/bedrock/latest/userguide/data-source-connectors.html */
import type { Issue, Where } from "./validate";
import { vocab, vocabExtra } from "./meta";

type Add = (severity: Issue["severity"], where: Where, path: string, message: string) => void;
const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);
const whole = (v: unknown): v is number => typeof v === "number" && Number.isInteger(v);
export const CONNECTOR_STORE = "opensearchServerless";
const FILTER_KEYS = ["objectType", "include", "exclude"];

export interface SettingSpec { type: "string" | "int" | "list" | "enum" | "filters"; pattern?: string; min?: number; max?: number; values?: Record<string, string> }

export function connectorSettings(kind: string): { required: string[]; optional: string[] } {
  return {
    required: vocabExtra<Record<string, string[]>>("kbDataSources", "required")?.[kind] ?? [],
    optional: vocabExtra<Record<string, string[]>>("kbDataSources", "optional")?.[kind] ?? [],
  };
}

export function settingSpec(name: string): SettingSpec | undefined {
  return vocabExtra<Record<string, SettingSpec>>("kbDataSources", "settings")?.[name];
}

function patterns(items: unknown[], pattern: string, p: string, where: Where, add: Add): void {
  items.forEach((item, i) => {
    if (typeof item === "string" && pattern && !new RegExp(pattern).test(item)) {
      add("error", where, `${p}[${i}]`, `"${item}" does not match ${pattern}`);
    }
  });
}

function filters(value: unknown, spec: SettingSpec, p: string, where: Where, add: Add): void {
  if (!Array.isArray(value)) return;            // the type check has said so
  if (value.length < spec.min! || value.length > spec.max!) add("error", where, p, `must list ${spec.min} to ${spec.max} filters`);
  value.forEach((f, i) => {
    const fp = `${p}[${i}]`;
    if (!isObj(f)) {
      add("error", where, fp, 'must be {"objectType": "...", "include": [...], "exclude": [...]}');
      return;
    }
    for (const k of Object.keys(f)) {
      if (!FILTER_KEYS.includes(k)) add("error", where, `${fp}.${k}`, `unknown key; allowed: ${FILTER_KEYS.join(", ")}`);
    }
    const ot = f.objectType;
    if (typeof ot !== "string" || ot.length < 1 || ot.length > 50) {
      add("error", where, `${fp}.objectType`, "is required: a content type such as Page, 1 to 50 characters");
    }
    for (const k of ["include", "exclude"]) {
      const v = f[k];
      if (k in f && (!Array.isArray(v) || v.length < 1 || v.length > 25 || !v.every((x) => typeof x === "string" && x))) {
        add("error", where, `${fp}.${k}`, "must list 1 to 25 patterns");
      }
    }
  });
}

export function checkKbDataSource(tool: Record<string, unknown>, path: string, where: Where, add: Add): void {
  const ds = tool.dataSource;
  if (!isObj(ds)) return;
  const p = `${path}.dataSource`;
  const kinds = vocab("kbDataSources");
  const kind = ds.type;
  if (typeof kind !== "string" || !kinds.includes(kind)) {
    add("error", where, `${p}.type`, `must be one of: ${kinds.join(", ")}`);
    return;
  }
  const { required, optional } = connectorSettings(kind);
  const takes = [...required, ...optional];
  const vs = tool.vectorStore;
  if (!isObj(vs) || vs.type !== CONNECTOR_STORE) {
    add("error", where, `${p}.type`,
      `needs vectorStore.type "${CONNECTOR_STORE}": AWS supports the ${kind} connector only on OpenSearch Serverless`);
  }
  if (tool.s3Uri !== undefined && tool.s3Uri !== null) {
    add("error", where, p, "takes the place of s3Uri: the documents come from one or the other");
  }
  for (const name of required) {
    const v = ds[name];
    if (v === undefined || v === null || v === "" || (Array.isArray(v) && v.length === 0)) {
      add("error", where, `${p}.${name}`, `is required for the ${kind} connector`);
    }
  }
  for (const [name, value] of Object.entries(ds)) {
    if (name === "type") continue;
    if (!takes.includes(name)) {
      add("error", where, `${p}.${name}`, `does not apply to the ${kind} connector`);
      continue;
    }
    const spec = settingSpec(name);
    const sp = `${p}.${name}`;
    if (!spec) continue;
    if (spec.type === "string" && typeof value === "string" && value && !new RegExp(spec.pattern!).test(value)) {
      add("error", where, sp, `"${value}" does not match ${spec.pattern}`);
    } else if (spec.type === "enum" && typeof value === "string" && !(value in spec.values!)) {
      add("error", where, sp, `must be one of: ${Object.keys(spec.values!).join(", ")}`);
    } else if (spec.type === "int" && whole(value) && (value < spec.min! || value > spec.max!)) {
      add("error", where, sp, `must be a whole number from ${spec.min} to ${spec.max}`);
    } else if (spec.type === "list" && Array.isArray(value)) {
      if (value.length < spec.min! || value.length > spec.max!) add("error", where, sp, `must list ${spec.min} to ${spec.max}`);
      patterns(value, spec.pattern ?? "", sp, where, add);
    } else if (spec.type === "filters") {
      filters(value, spec, sp, where, add);
    }
  }
}
