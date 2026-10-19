/** Checks for which model answers (app/common/model_routing.py): `orchestrator.models.overrides`
 *  (the per-run override list) and an agent's `models.fallback` / `models.route`. The generic
 *  key checks (keys.json) say where `models` applies and that each part is a list; these are
 *  the rest. Mirrors bff/model_routing_check.py word for word (both run
 *  tests/fixtures/validation_cases.json). A `matches` pattern is compiled as JavaScript reads
 *  it, there as Python does: they agree on ordinary patterns, not on every engine-specific
 *  construct. */
import type { Issue, Where } from "./validate";

type Add = (severity: Issue["severity"], where: Where, path: string, message: string) => void;
const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);

export const MAX_OVERRIDES = 20;
export const MAX_FALLBACK = 3;
export const MAX_RULES = 10;
export const MAX_TOKENS_OVER = 1_000_000;
export const MAX_PATTERN = 200;
const RULE_KEYS = ["model", "inputTokensOver", "matches"];
const LOCAL = ["main", "dedicated"];

export function modelProblem(m: unknown): string {
  if (typeof m !== "string" || !m.trim()) return "must be a model id";
  if (m.startsWith("litellm/") && !m.slice(8).trim()) {
    return "names no model: write litellm/ and the proxy's model name, e.g. litellm/gpt-4o";
  }
  return "";
}

function checkModels(items: unknown[], path: string, where: Where, add: Add, own = ""): void {
  const seen: unknown[] = [];
  items.forEach((m, i) => {
    const p = `${path}[${i}]`;
    const problem = modelProblem(m);
    if (problem) add("error", where, p, problem);
    else if (seen.includes(m)) add("error", where, p, "is listed twice");
    else if (own && m === own) add("error", where, p, "is this agent's own model");
    seen.push(m);
  });
}

/** "" when `pattern` compiles, else why not (the words the Python twin uses). */
export function patternProblem(v: unknown): string {
  if (!(typeof v === "string" && v.length >= 1 && v.length <= MAX_PATTERN)) {
    return `must be a regular expression of 1 to ${MAX_PATTERN} characters`;
  }
  try {
    new RegExp(v, "i");
  } catch {
    return "is not a valid regular expression";
  }
  return "";
}

function checkRule(rule: unknown, p: string, where: Where, add: Add): void {
  if (!isObj(rule)) {
    add("error", where, p, 'must be {"model": ..., "inputTokensOver": N} or {"model": ..., "matches": "..."}');
    return;
  }
  for (const k of Object.keys(rule)) {
    if (!RULE_KEYS.includes(k)) add("error", where, `${p}.${k}`, `unknown key; a rule takes: ${RULE_KEYS.join(", ")}`);
  }
  if (!("model" in rule)) add("error", where, `${p}.model`, "needs the model this rule picks");
  else {
    const problem = modelProblem(rule.model);
    if (problem) add("error", where, `${p}.model`, problem);
  }
  if (("inputTokensOver" in rule) === ("matches" in rule)) {
    add("error", where, p, "set exactly one of `inputTokensOver` / `matches`");
  }
  if ("inputTokensOver" in rule) {
    const v = rule.inputTokensOver;
    if (!(typeof v === "number" && Number.isInteger(v) && v >= 1 && v <= MAX_TOKENS_OVER)) {
      add("error", where, `${p}.inputTokensOver`, `must be a whole number from 1 to ${MAX_TOKENS_OVER}`);
    }
  }
  if ("matches" in rule) {
    const problem = patternProblem(rule.matches);
    if (problem) add("error", where, `${p}.matches`, problem);
  }
}

export function checkModelRouting(orch: unknown, agents: Record<string, unknown>, auth: unknown, add: Add): void {
  const block = isObj(orch) ? orch.models : undefined;
  const overrides = isObj(block) ? block.overrides : undefined;
  if (Array.isArray(overrides)) {
    const where: Where = { kind: "block", name: "orchestrator" };
    const path = "orchestrator.models.overrides";
    if (overrides.length > MAX_OVERRIDES) add("error", where, path, `at most ${MAX_OVERRIDES} models`);
    checkModels(overrides, path, where, add);
    const actions = isObj(auth) && isObj(auth.actions) ? auth.actions : {};
    const granted = actions.override;
    if (overrides.length && !(Array.isArray(granted) && granted.length)) {
      add("warning", where, path, "only a person granted `override` can pick a run's models, and no group "
        + "is granted it in authorization.actions, so nobody can");
    }
  }
  const dflt = isObj(orch) ? String(orch.defaultModel ?? "") : "";
  for (const [aid, a] of Object.entries(agents)) {
    // No runtime is a local placement (keys.json agent.runtime default).
    if (!isObj(a) || (a.runtime && !LOCAL.includes(String(a.runtime).toLowerCase()))) continue;
    const models = a.models;
    if (!isObj(models)) continue;
    const where: Where = { kind: "agent", id: aid };
    const path = `agents.${aid}.models`;
    const own = typeof a.model === "string" && a.model ? a.model : dflt;
    if (Array.isArray(models.fallback)) {
      if (models.fallback.length > MAX_FALLBACK) {
        add("error", where, `${path}.fallback`, `at most ${MAX_FALLBACK} models: each is tried in turn while the step waits`);
      }
      checkModels(models.fallback, `${path}.fallback`, where, add, own);
    }
    if (Array.isArray(models.route)) {
      if (models.route.length > MAX_RULES) add("error", where, `${path}.route`, `at most ${MAX_RULES} rules`);
      models.route.forEach((rule, i) => checkRule(rule, `${path}.route[${i}]`, where, add));
    }
  }
}
