/** workflow.json as `format_workflow.py` writes it.
 *
 *  A port, not an approximation: the Builder's download must be byte-identical to what
 *  the formatter produces, or the first `format_workflow.py --check` a customer runs
 *  after exporting fails on a file they did not touch. `format.test.ts` asserts it on
 *  the shipped workflow.json. */

import { canonicalOrder, topLevelOrder } from "./meta";

const INDENT = "  ";
const MAX_INLINE = 100;

type J = unknown;

const isObj = (v: J): v is Record<string, J> =>
  typeof v === "object" && v !== null && !Array.isArray(v);

function reorder(entry: J, order: string[]): J {
  if (!isObj(entry) || order.length === 0) return entry;
  const known = order.filter((k) => k in entry);
  const rest = Object.keys(entry).filter((k) => !known.includes(k));
  const out: Record<string, J> = {};
  for (const k of [...known, ...rest]) out[k] = entry[k];
  return out;
}

/** Reorder every repeated entity (agents, tools, steps); single blocks stay as found. */
export function canonicalise(doc: Record<string, J>): Record<string, J> {
  const out: Record<string, J> = { ...doc };
  if (isObj(out.agents)) {
    const order = canonicalOrder("agent");
    out.agents = Object.fromEntries(
      Object.entries(out.agents).map(([k, v]) => [k, reorder(v, order)]));
  }
  if (isObj(out.tools)) {
    const order = topLevelOrder("tool");
    out.tools = Object.fromEntries(
      Object.entries(out.tools).map(([k, v]) => [k, reorder(v, order)]));
  }
  if (Array.isArray(out.steps)) {
    const order = canonicalOrder("step");
    out.steps = out.steps.map((s) => reorder(s, order));
  }
  return out;
}

const scalar = (v: J) => JSON.stringify(v);

const isLeaf = (v: J) =>
  Array.isArray(v) ? v.every((x) => typeof x !== "object" || x === null)
    : isObj(v) ? Object.values(v).every((x) => typeof x !== "object" || x === null)
      : true;

function inline(v: J): string {
  if (Array.isArray(v)) return v.length ? `[${v.map(scalar).join(", ")}]` : "[]";
  if (isObj(v)) {
    const e = Object.entries(v);
    return e.length ? `{ ${e.map(([k, x]) => `${scalar(k)}: ${scalar(x)}`).join(", ")} }` : "{}";
  }
  return scalar(v);
}

export function render(v: J, depth = 0): string {
  const pad = INDENT.repeat(depth);
  const inner = INDENT.repeat(depth + 1);
  if ((Array.isArray(v) || isObj(v)) && isLeaf(v)) {
    const one = inline(v);
    if (pad.length + one.length + 2 <= MAX_INLINE) return one;
  }
  if (isObj(v)) {
    const e = Object.entries(v);
    if (!e.length) return "{}";
    return "{\n" + e.map(([k, x]) => `${inner}${scalar(k)}: ${render(x, depth + 1)}`).join(",\n")
      + `\n${pad}}`;
  }
  if (Array.isArray(v)) {
    if (!v.length) return "[]";
    return "[\n" + v.map((x) => `${inner}${render(x, depth + 1)}`).join(",\n") + `\n${pad}]`;
  }
  return scalar(v);
}

/** The exact text of workflow.json for this document. */
export function formatWorkflow(doc: Record<string, J>): string {
  return render(canonicalise(doc)) + "\n";
}
