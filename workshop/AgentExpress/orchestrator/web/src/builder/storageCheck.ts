/** Build storage checks (`orchestrator.storage` and the `${storage}` token).
 *
 *  The generic key checks (keys.json) already say `storage` takes `create` and `retain`, both
 *  true or false. These are the rest. The token (vocabulary.json storageToken) stands for
 *  s3://<the build's bucket> and is resolved by the IaC only in the fields it lists
 *  (cdk/lib/storage.ts resolveStorage, terraform/storage.tf), so a value carrying it is
 *  refused anywhere else in the tools, the orchestrator block or a task step; when the build
 *  creates no storage; and unless it starts the value, once, followed by / or nothing.
 *  `retain` without `create` keeps nothing, which is said rather than ignored.
 *
 *  Mirrors bff/storage_check.py word for word (both run tests/fixtures/storage_cases.json). */
import { vocab, vocabExtra } from "./meta";
import type { Issue, Where } from "./validate";
type Add = (severity: Issue["severity"], where: Where, path: string, message: string) => void;
const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);

export const STORAGE_TOKEN = vocab("storageToken")[0] ?? "";
export const STORAGE_FIELDS: string[] = vocabExtra<string[]>("storageToken", "fields") ?? [];
/** A valid bucket name the token's place is checked as, by the S3 patterns that do not know
 *  the token (validate.ts checkCode, the kb tool's s3Uri). */
const STAND_IN = "s3://build-storage";

/** Does the workflow create the build's storage? */
export function createsStorage(wf: unknown): boolean {
  const orch = isObj(wf) && isObj(wf.orchestrator) ? wf.orchestrator : {};
  const st = isObj(orch.storage) ? orch.storage : {};
  return st.create === true;
}

/** The value with a leading token in the stand-in's place, for a check of its shape. */
export function asChecked(value: unknown): unknown {
  return typeof value === "string" && value.startsWith(STORAGE_TOKEN) ? STAND_IN + value.slice(STORAGE_TOKEN.length) : value;
}

type Seg = string | number;
function* strings(node: unknown, at: Seg[]): Generator<[Seg[], string]> {
  if (typeof node === "string") yield [at, node];
  else if (Array.isArray(node)) for (let i = 0; i < node.length; i++) yield* strings(node[i], [...at, i]);
  else if (isObj(node)) for (const [k, v] of Object.entries(node)) yield* strings(v, [...at, k]);
}
const dotted = (at: Seg[]) => at.map((k, i) => (typeof k === "number" ? `[${k}]` : i ? `.${k}` : String(k))).join("");
const matches = (field: string, at: Seg[]) => {
  const parts = field.split(".");
  return parts.length === at.length && parts.every((p, i) => p === "*" || p === String(at[i]));
};

export function checkStorage(wf: Record<string, unknown>, add: Add): void {
  const token = STORAGE_TOKEN;
  const orch = isObj(wf.orchestrator) ? wf.orchestrator : {};
  const st = isObj(orch.storage) ? orch.storage : null;
  const block: Where = { kind: "block", name: "orchestrator" };
  if (st !== null && st.retain === true && st.create !== true) {
    add("warning", block, "orchestrator.storage.retain",
      "keeps nothing: the build creates no storage unless orchestrator.storage.create is true");
  }
  const on = createsStorage(wf);
  const roots: [Seg[], unknown][] = [[["tools"], isObj(wf.tools) ? wf.tools : {}],
    [["orchestrator"], Object.fromEntries(Object.entries(orch).filter(([k]) => k !== "storage"))]];
  const agents = isObj(wf.agents) ? wf.agents : {};
  for (const [aid, a] of Object.entries(agents)) {
    if (isObj(a) && "task" in a) roots.push([["agents", aid, "task"], a.task]);
  }
  for (const [base, node] of roots) {
    for (const [at, value] of strings(node, base)) {
      if (!value.includes(token)) continue;
      const where: Where = at[0] === "tools" && at.length > 1 ? { kind: "tool", id: String(at[1]) }
        : at[0] === "agents" ? { kind: "agent", id: String(at[1]) } : block;
      const p = dotted(at);
      if (!STORAGE_FIELDS.some((f) => matches(f, at))) {
        add("error", where, p, `${token} is read only in: ${STORAGE_FIELDS.join(", ")}`);
      } else if (!on) {
        add("error", where, p, `uses ${token}, but this build creates no storage: set orchestrator.storage.create to true`);
      } else if (!(value.startsWith(token) && value.split(token).length === 2
        && (value.length === token.length || value[token.length] === "/"))) {
        add("error", where, p, `must start with ${token}/ (it stands for s3://<the build's bucket>), once`);
      }
    }
  }
}
