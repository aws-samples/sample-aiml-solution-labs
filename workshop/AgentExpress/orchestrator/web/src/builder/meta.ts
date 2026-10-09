/** The framework's key spec, as the Build view reads it.
 *
 *  Everything a form shows and everything the validator checks comes from
 *  `generated/builder-meta.json`, which `build_schema.py` copies out of app/keys.json,
 *  app/vocabulary.json and the defaults — the same three files the CLI, Terraform and
 *  CDK read. So the Builder cannot offer a key the deploy would reject, and a key the
 *  framework adds shows up here with no UI change. */

import raw from "../generated/builder-meta.json";

export type KeyType = "string" | "number" | "integer" | "boolean" | "array" | "object";

export interface KeySpec {
  group?: string;
  /** One type, or several when a key takes either form (`tool`: a key or a list). */
  type?: KeyType | KeyType[];
  items?: string;
  appliesTo?: "*" | string[];
  required?: boolean;
  requiredFor?: string[];
  vocabulary?: string;
  vocabularyFor?: Record<string, string>;
  keyVocabulary?: string;
  valueVocabulary?: string;
  enum?: unknown[];
  pattern?: string;
  itemRequired?: string[];
  properties?: Record<string, { type?: string; items?: string; doc: string }>;
  default?: unknown;
  defaultFor?: Record<string, unknown>;
  doc: string;
  reads?: string;
}

export interface ExactlyOne {
  keys: string[];
  appliesTo: "*" | string[];
  why: string;
}

export interface BlockSpec {
  $comment?: string;
  $variantKey?: string;
  $variantDefault?: string;
  $variantAliases?: Record<string, string[]>;
  $ordered?: boolean;
  $groups?: Record<string, string>;
  $exactlyOne?: ExactlyOne[];
  $removed?: Record<string, string>;
  keys: Record<string, KeySpec>;
}

export type BlockName =
  | "agent" | "agentcore" | "tool" | "step"
  | "orchestrator" | "ui" | "guardrail" | "authorization"
  | "memory" | "evaluator" | "identity" | "policy" | "skill";

interface Meta {
  keys: Record<string, unknown>;
  vocabulary: Record<string, { values?: string[]; [k: string]: unknown }>;
  defaults: Record<string, unknown>;
  frameworkVersion: string;
}

const META = raw as unknown as Meta;

/** orchestrator/VERSION, as build_schema.py stamped it into the generated meta. */
export const FRAMEWORK_VERSION: string = META.frameworkVersion;
export function block(name: BlockName): BlockSpec {
  return META.keys[name] as BlockSpec;
}

export function vocab(name: string): string[] {
  return [...(META.vocabulary[name]?.values ?? [])];
}

/** A vocabulary entry's extra fields, e.g. `embeddingModels.dimensionsByModel`. */
export function vocabExtra<T>(name: string, field: string): T | undefined {
  return META.vocabulary[name]?.[field] as T | undefined;
}

export function defaults(): Record<string, Record<string, unknown>> {
  return META.defaults as Record<string, Record<string, unknown>>;
}

/** The variant an entry is (an agent's runtime, a tool's type), with the block's
 *  default when it names none. */
export function variantOf(name: BlockName, entry: Record<string, unknown>): string {
  const spec = block(name);
  if (!spec.$variantKey) return "";
  const v = entry[spec.$variantKey];
  return typeof v === "string" && v ? v.toLowerCase() : (spec.$variantDefault ?? "");
}

function expand(scope: "*" | string[] | undefined, aliases: Record<string, string[]>): string[] {
  if (!scope || scope === "*") return [];
  return scope.flatMap((s) => aliases[s] ?? [s]);
}

/** Does `key` apply to `variant`? A block without variants applies every key. */
export function applies(name: BlockName, key: string, variant: string): boolean {
  const spec = block(name);
  const k = spec.keys[key];
  if (!k) return false;
  if (!spec.$variantKey || k.appliesTo === undefined || k.appliesTo === "*") return true;
  return expand(k.appliesTo, spec.$variantAliases ?? {}).includes(variant);
}

export function requiredFor(name: BlockName, key: string, variant: string): boolean {
  const spec = block(name);
  const k = spec.keys[key];
  if (!k) return false;
  if (k.required) return true;
  return expand(k.requiredFor, spec.$variantAliases ?? {}).includes(variant);
}

/** The closed value set for a key, for this variant, or null when it is open. */
export function allowedValues(name: BlockName, key: string, variant: string): string[] | null {
  const k = block(name).keys[key];
  if (!k) return null;
  const perVariant = k.vocabularyFor?.[variant];
  if (perVariant) return vocab(perVariant);
  if (k.vocabulary) return vocab(k.vocabulary);
  if (k.enum) return k.enum.map(String);
  return null;
}

export function defaultFor(name: BlockName, key: string, variant: string): unknown {
  const k = block(name).keys[key];
  if (!k) return undefined;
  if (k.defaultFor && variant in k.defaultFor) return k.defaultFor[variant];
  return k.default;
}

/** Keys in the order keys.json declares them: groups in `$groups` order, keys in
 *  declaration order within each group. Mirrors format_workflow.canonical_order. */
export function canonicalOrder(name: BlockName): string[] {
  const spec = block(name);
  if (!spec.$ordered) return [];
  const groups = Object.keys(spec.$groups ?? {});
  const keys = Object.keys(spec.keys);
  const rank = (k: string) => {
    const g = spec.keys[k].group;
    const gi = g !== undefined && groups.includes(g) ? groups.indexOf(g) : groups.length;
    return gi * 10000 + keys.indexOf(k);
  };
  return [...keys].sort((a, b) => rank(a) - rank(b));
}

/** `canonicalOrder` with dotted keys collapsed to their parent. */
export function topLevelOrder(name: BlockName): string[] {
  const seen: string[] = [];
  for (const k of canonicalOrder(name)) {
    const head = k.split(".")[0];
    if (!seen.includes(head)) seen.push(head);
  }
  return seen;
}

/** Keys that apply to `variant`, grouped as keys.json groups them, for a form. */
export function formGroups(name: BlockName, variant: string): { group: string; doc: string; keys: string[] }[] {
  const spec = block(name);
  const order = spec.$ordered ? canonicalOrder(name) : Object.keys(spec.keys);
  const groups = spec.$groups ?? {};
  const out: { group: string; doc: string; keys: string[] }[] = [];
  for (const key of order) {
    if (!applies(name, key, variant)) continue;
    const g = spec.keys[key].group ?? "";
    let bucket = out.find((b) => b.group === g);
    if (!bucket) {
      bucket = { group: g, doc: groups[g] ?? "", keys: [] };
      out.push(bucket);
    }
    bucket.keys.push(key);
  }
  return out;
}

/** The two id rules, which the schema, the registry and both IaC paths share. */
export const AGENT_ID_RE = /^[a-zA-Z][a-zA-Z0-9_]*$/;
export const TOOL_KEY_RE = /^[A-Za-z][A-Za-z0-9]*$/;
export const BRANCH_OPS = ["equals", "notEquals", "in", "contains", "exists", "gt", "gte", "lt", "lte"];
