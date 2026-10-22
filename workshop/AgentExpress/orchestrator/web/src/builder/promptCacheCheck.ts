/** Checks for an agent's `promptCache` (Bedrock prompt caching, app/common/prompt_cache.py)
 *  against what its model supports, per the AWS table copied into app/vocabulary.json
 *  `promptCacheModels` (https://docs.aws.amazon.com/bedrock/latest/userguide/prompt-caching.html).
 *  The generic key checks (keys.json) cover the shape; these are the rest. Mirrors
 *  bff/prompt_cache_check.py word for word (both run tests/fixtures/validation_cases.json). */
import { defaults, vocab, vocabExtra } from "./meta";
import type { Issue, Where } from "./validate";
type Add = (severity: Issue["severity"], where: Where, path: string, message: string) => void;
const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);
const LOCAL = ["main", "dedicated"];

/** One row of the AWS table, as copied. */
export interface CacheModel {
  name: string;
  status: string;
  minTokens: number;
  minTokensText: string;
  maxCheckpoints: number;
  ttls: string[];
  fields: string[];
  /** "InvokeModel": an OpenAI GPT-5.6+ row, cached through InvokeModel (vocabulary openaiExplicit). */
  api?: string;
}

/** A model id, inference-profile id or ARN, without its geography prefix. */
export function baseId(model: string): string {
  let mid = String(model ?? "").trim();
  if (mid.includes("/")) mid = mid.slice(mid.lastIndexOf("/") + 1);
  const dot = mid.indexOf(".");
  if (dot > 0 && dot < mid.length - 1 && vocab("inferenceProfileGeos").includes(mid.slice(0, dot).toLowerCase())) {
    return mid.slice(dot + 1);
  }
  return mid;
}

/** The table row for a model, or null when it has no explicit caching through Converse. */
export function cacheModel(model: string): CacheModel | null {
  if (String(model ?? "").startsWith("litellm/")) return null;
  const rows = vocabExtra<Record<string, CacheModel>>("promptCacheModels", "byModel") ?? {};
  return rows[baseId(model)] ?? null;
}

/** True for the models the table says Converse cannot cache explicitly (OpenAI GPT-5.6+). */
export function notOnConverse(model: string): boolean {
  const block = vocabExtra<{ byModel?: string[] }>("promptCacheModels", "notOnConverse");
  return (block?.byModel ?? []).includes(baseId(model));
}

export function copiedOn(): string {
  return String(vocabExtra<string>("promptCacheModels", "copied") ?? "");
}

/** The model an agent's calls use: its own, else the build default, else the framework's. */
export function agentModel(agent: Record<string, unknown>, orch: unknown): string {
  if (typeof agent.model === "string" && agent.model) return agent.model;
  const own = isObj(orch) && orch.defaultModel ? String(orch.defaultModel) : "";
  return own || String(defaults().orchestrator?.defaultModel ?? "");
}

export function checkPromptCache(orch: unknown, agents: Record<string, unknown>, add: Add): void {
  for (const [aid, a] of Object.entries(agents)) {
    if (!isObj(a) || (a.runtime && !LOCAL.includes(String(a.runtime).toLowerCase()))) continue;
    const pc = a.promptCache;
    if (!isObj(pc)) continue;
    const where: Where = { kind: "agent", id: aid };
    const path = `agents.${aid}.promptCache`;
    const ttl = pc.ttl;
    if ("ttl" in pc && ttl !== "5m" && ttl !== "1h" && ttl !== "30m") add("error", where, `${path}.ttl`, 'must be "5m", "1h" or "30m"');
    if (!["system", "tools", "messages"].some((k) => pc[k] === true)) continue;
    const model = agentModel(a, orch);
    const row = cacheModel(model);
    if (!row) {
      if (notOnConverse(model)) {
        add("warning", where, path,
          `${model}: explicit caching for this model needs the Responses API or Chat Completions, and AgentExpress calls models through Converse; calls to it are sent without cache points`);
      } else {
        add("warning", where, path,
          `${model} has no documented explicit prompt caching through Converse (AWS table copied ${copiedOn()}); calls to it are sent without cache points`);
      }
      continue;
    }
    const fields = row.fields ?? [];
    const ttls = row.ttls ?? [];
    const openai = row.api === "InvokeModel";
    for (const flag of ["tools", "messages"]) {
      if (pc[flag] === true && !fields.includes(flag)) {
        add("error", where, `${path}.${flag}`, openai
          ? `${row.name} caches the system prompt only: tool-calling turns go through Converse, which takes no explicit cache point for this model`
          : `${row.name} caches the system prompt and messages only, not tool definitions`);
      }
    }
    if ((ttl === "5m" || ttl === "1h" || ttl === "30m") && !ttls.includes(ttl)) {
      const msg = openai ? `${row.name} caches for 30 minutes; use "30m" or leave ttl out`
        : ttl === "1h" ? `${row.name} caches for 5 minutes only; use "5m"`
        : `${row.name} has no 30-minute TTL; use ${ttls.map((t) => `"${t}"`).join(" or ")}`;
      add("error", where, `${path}.ttl`, msg);
    }
  }
}
