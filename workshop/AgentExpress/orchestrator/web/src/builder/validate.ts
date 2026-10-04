/** Live validation for the Builder, so a mistake is named while you are making it.
 *
 *  Two halves, deliberately:
 *
 *   1. SHAPE, driven entirely by keys.json: which keys an agent may have for its runtime,
 *      which a tool may have for its type, which are required, what values they take,
 *      and the exactly-one rules. Nothing here names a key; a key the framework adds is
 *      validated with no change to this file.
 *   2. MEANING, which no schema can express: references between agents, tools and
 *      steps, the branch grammar, and the tool rules the IaC enforces at deploy. These
 *      mirror, one for one, the validators in app/orchestrator (registry, graph_builder,
 *      branching), cdk/lib/orchestrator-stack.ts and terraform/tools.tf, and each carries
 *      the plane's own reason in its message.
 *
 *  It is ADVISORY. Terraform, CDK and the container still refuse a bad workflow at
 *  deploy; this only moves that refusal to the moment of the edit, which is the whole
 *  difference between a tool people adopt and one they have to be taught. An `error`
 *  is something a deploy or the customer's test suite rejects; a `warning` is a trap
 *  that deploys fine and misbehaves. */

import {
  AGENT_ID_RE, BRANCH_OPS, TOOL_KEY_RE, allowedValues, applies, block, requiredFor,
  variantOf, vocab, vocabExtra, type BlockName,
} from "./meta";
import { toolsOf } from "../lib/tools";
import * as cedar from "./cedar";
import { stepAgents, stepName, type Entry, type StepSpec, type Workflow } from "./model";

export type Where =
  | { kind: "workflow" }
  | { kind: "block"; name: string }
  | { kind: "agent"; id: string }
  | { kind: "tool"; id: string }
  | { kind: "step"; index: number };

export interface Issue {
  severity: "error" | "warning";
  where: Where;
  /** Dotted location in workflow.json, e.g. `agents.intake.maxTokens`. */
  path: string;
  message: string;
}

/** The one tool name the framework gives a target of these types (cdk/lib/tool-plane.ts).
 *  Mirrors validate_build.py _FIXED_TOOL_NAMES. */
const FIXED_TOOL_NAMES: Record<string, string> = { websearch: "WebSearch", kb: "retrieve" };

const TOP_LEVEL = ["$schema", "$comment", "orchestrator", "ui", "guardrail", "authorization",
  "tools", "agents", "steps"];
const LAMBDA_ARN_RE = /^arn:aws[a-z-]*:lambda:[a-z0-9-]+:[0-9]{12}:function:[a-zA-Z0-9-_]+(:[a-zA-Z0-9-_$]+)?$/;
const S3_URI_RE = /^s3:\/\/[a-z0-9.-]{3,63}\/.+/;
/** A kb tool's own documents: s3://bucket or s3://bucket/prefix (the prefix optional). */
const S3_LOCATION_RE = /^s3:\/\/[a-z0-9][a-z0-9.-]{1,61}[a-z0-9](\/.*)?$/;
const KB_ID_RE = /^[0-9A-Z]{10}$/;
const KMS_KEY_ARN_RE = /^arn:aws[a-z-]*:kms:[a-z0-9-]+:[0-9]{12}:key\/[A-Za-z0-9-]+$/;
const REST_API_ID_RE = /^[a-z0-9]{10}$/;
const STAGE_RE = /^[A-Za-z0-9_-]{1,128}$/;
const TOOL_NAME_RE = /^[A-Za-z][A-Za-z0-9_-]{0,63}$/;
const HTTPS_RE = /^https:\/\/\S+\.\S+/;
const OPENAPI_OPS = ["get", "put", "post", "delete", "options", "head", "patch", "trace"];
const CUSTOM_NAME_RE = /^[A-Za-z][A-Za-z0-9]{0,31}$/;

type Add = (severity: Issue["severity"], where: Issue["where"], path: string, message: string) => void;

const POLICY_KEYS = ["name", "description", "statement", "libraryId"];
const CODE_GRANTS = ["secret", "table", "tableAccess", "s3Prefix", "s3Access", "vpc"];
const SECRET_NAME_RE = /^[A-Za-z0-9/_+=.@-]{1,512}$/;
const TABLE_NAME_RE = /^[A-Za-z0-9_.-]{3,255}$/;
const S3_PREFIX_RE = /^s3:\/\/[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]\/([^*?]+\/)?$/;
const SUBNET_RE = /^subnet-[0-9a-f]{8,17}$/;
const SG_RE = /^sg-[0-9a-f]{8,17}$/;
const ENV_NAME_RE = /^[A-Za-z][A-Za-z0-9_]{0,63}$/;
/** ToolLambda-<agentName>-<key> within Lambda's 64: ax_xxxxxxxx leaves 41 for the key. */
export const CODE_KEY_MAX = 41;
/** What the framework itself keeps, which no code tool may be granted (the IaC's
 *  permissions boundary denies it too). Mirrors bff/validate_build.py. */
const FRAMEWORK_TABLE_RE = /^ax_|_(builds|audit|status|events|telemetry|insights)$/;
const FRAMEWORK_SECRET_RE = /^(agentexpress\/|bedrock-agentcore)/;
const FRAMEWORK_BUCKET_RE = /^s3:\/\/(agentcore-|[^/]*builderbuildsbucket)/;
const intIn = (v: unknown, lo: number, hi: number) => typeof v === "number" && Number.isInteger(v) && v >= lo && v <= hi;

/** tools.<key>.code: a function written in the build. Mirrored by
 *  bff/validate_build.py _check_code. */
function checkCode(key: string, code: unknown, path: string, where: Where, add: Add): void {
  const p = `${path}.code`;
  if (key.length > CODE_KEY_MAX) {
    add("error", where, path, `a code tool's key is at most ${CODE_KEY_MAX} characters: it names the function ToolLambda-<agentName>-<key>`);
  }
  if (!isObj(code)) return;
  if (code.grants !== undefined && code.grants !== null && !isObj(code.grants)) {
    add("error", where, `${p}.grants`, `must be an object of grants: ${CODE_GRANTS.join(", ")}`);
  }
  const grants = isObj(code.grants) ? code.grants : {};
  for (const k of Object.keys(grants)) {
    if (!CODE_GRANTS.includes(k)) add("error", where, `${p}.grants.${k}`, `unknown grant; a code tool may have: ${CODE_GRANTS.join(", ")}`);
  }
  const str = (v: unknown, re: RegExp) => typeof v === "string" && re.test(v);
  if ("secret" in grants && !str(grants.secret, SECRET_NAME_RE)) {
    add("error", where, `${p}.grants.secret`, "must be the name of a Secrets Manager secret in the build's account");
  } else if ("secret" in grants && FRAMEWORK_SECRET_RE.test(String(grants.secret))) {
    add("error", where, `${p}.grants.secret`, "is one of the framework's own secrets, which no tool may read");
  }
  if ("table" in grants && !str(grants.table, TABLE_NAME_RE)) {
    add("error", where, `${p}.grants.table`, "must be the name of a DynamoDB table in the build's account");
  } else if ("table" in grants && FRAMEWORK_TABLE_RE.test(String(grants.table))) {
    add("error", where, `${p}.grants.table`, "is named like one of the framework's own tables, which no tool may reach");
  }
  if ("s3Prefix" in grants && !str(grants.s3Prefix, S3_PREFIX_RE)) {
    add("error", where, `${p}.grants.s3Prefix`, "must be s3://<bucket>/ or s3://<bucket>/<prefix>/, ending in /");
  } else if ("s3Prefix" in grants && FRAMEWORK_BUCKET_RE.test(String(grants.s3Prefix))) {
    add("error", where, `${p}.grants.s3Prefix`, "is one of the framework's own buckets, which no tool may reach");
  }
  const access = vocab("codeGrantAccess");
  for (const [k, needs] of [["tableAccess", "table"], ["s3Access", "s3Prefix"]] as const) {
    if (!(k in grants)) continue;
    if (!access.includes(grants[k] as string)) add("error", where, `${p}.grants.${k}`, `must be one of: ${access.join(", ")}`);
    else if (!(needs in grants)) add("error", where, `${p}.grants.${k}`, `only applies with ${needs}`);
  }
  if ("vpc" in grants) {
    const vpc = isObj(grants.vpc) ? grants.vpc : {};
    const ok = (v: unknown, re: RegExp) => Array.isArray(v) && v.length > 0 && v.every((s) => str(s, re));
    if (!ok(vpc.subnetIds, SUBNET_RE)) add("error", where, `${p}.grants.vpc.subnetIds`, "must list one or more subnet ids, subnet-...");
    if (!ok(vpc.securityGroupIds, SG_RE)) add("error", where, `${p}.grants.vpc.securityGroupIds`, "must list one or more security group ids, sg-...");
  }
  if ("timeoutSeconds" in code && !intIn(code.timeoutSeconds, 1, 300)) {
    add("error", where, `${p}.timeoutSeconds`, "must be a whole number of seconds, 1 to 300");
  }
  if ("memoryMB" in code && !intIn(code.memoryMB, 128, 10240)) {
    add("error", where, `${p}.memoryMB`, "must be a whole number of MB, 128 to 10240");
  }
  const env = isObj(code.environment) ? code.environment : {};
  for (const [name, value] of Object.entries(env)) {
    if (!ENV_NAME_RE.test(name) || name.toUpperCase().startsWith("AWS_") || ["SECRET_NAME", "TABLE_NAME", "S3_PREFIX"].includes(name)) {
      add("error", where, `${p}.environment.${name}`, "is not a name you can set: letters, digits and _, not AWS_..., SECRET_NAME, TABLE_NAME or S3_PREFIX");
    } else if (typeof value !== "string" || value.length > 1000) {
      add("error", where, `${p}.environment.${name}`, "must be a string of at most 1000 characters");
    }
  }
}

/** orchestrator.policy: the mode, and each custom Cedar policy (cedar.ts). Mirrored by
 *  bff/validate_build.py _check_policy. */
function checkPolicy(orch: unknown, tools: Record<string, unknown>, add: Add): void {
  const o = isObj(orch) && isObj(orch.policy) ? orch.policy as Record<string, unknown> : {};
  const where: Where = { kind: "block", name: "orchestrator" };
  const modes = vocab("policyModes");
  if (typeof o.mode === "string" && !modes.includes(o.mode.toUpperCase())) {
    add("error", where, "orchestrator.policy.mode", `must be one of: ${modes.join(", ")}`);
  }
  const custom = Array.isArray(o.custom) ? o.custom : [];
  const seen: string[] = [];
  custom.forEach((c: unknown, i: number) => {
    const p = `orchestrator.policy.custom[${i}]`;
    if (!isObj(c)) {
      add("error", where, p, "must be {name, description, statement}");
      return;
    }
    for (const k of Object.keys(c)) {
      if (!POLICY_KEYS.includes(k)) add("error", where, `${p}.${k}`, `unknown key; a policy takes: ${POLICY_KEYS.join(", ")}`);
    }
    const name = c.name;
    if (typeof name !== "string" || !cedar.NAME_RE.test(name)) {
      add("error", where, `${p}.name`, "needs a name: a letter, then letters and digits (32 at most)");
    } else if (seen.includes(name)) {
      add("error", where, `${p}.name`, `"${name}" is used twice`);
    } else seen.push(name);
    if ("description" in c && typeof c.description !== "string") add("error", where, `${p}.description`, "must be a string");
    for (const message of cedar.problems(c.statement, Object.keys(tools))) add("error", where, `${p}.statement`, message);
  });
  if (custom.length && o.enabled === false) {
    add("warning", where, "orchestrator.policy.custom", "is not deployed: the policy engine is off (orchestrator.policy.enabled)");
  }
}

/** Evaluators, custom evaluators and a custom memory strategy. Mirrored by
 *  bff/validate_build.py _check_features and registry.features_problem. */
function checkFeatures(core: Record<string, unknown>, path: string, where: Issue["where"], add: Add, shared: string[] = []): void {
  const ev = isObj(core.evaluations) ? core.evaluations as Record<string, unknown> : {};
  const custom = Array.isArray(ev.custom) ? ev.custom : [];
  const names = [...custom.filter(isObj).map((c) => (c as Record<string, unknown>).name), ...shared];
  const unsupported = vocabExtra<string[]>("builtinEvaluators", "unsupported") ?? [];
  const levels = vocabExtra<Record<string, string>>("builtinEvaluators", "levels") ?? {};
  (Array.isArray(ev.evaluators) ? ev.evaluators : []).forEach((e: unknown, i: number) => {
    if (typeof e === "string" && unsupported.includes(e)) {
      add("error", where, `${path}.evaluations.evaluators[${i}]`,
        `${e} scores a ${String(levels[e] ?? "").toLowerCase().replace("_", " ")}, and this framework scores one agent's run — not offered yet`);
    } else if (typeof e === "string" && e.startsWith("Custom.") && !names.includes(e.slice(7))) {
      add("error", where, `${path}.evaluations.evaluators[${i}]`, `${e} is not defined in evaluations.custom or in the build's evaluators`);
    }
  });
  const seen: unknown[] = [];
  custom.forEach((c: unknown, i: number) => {
    const p = `${path}.evaluations.custom[${i}]`;
    const o = c as Record<string, unknown>;
    if (!isObj(c) || typeof o.name !== "string" || !CUSTOM_NAME_RE.test(o.name)) {
      add("error", where, `${p}.name`, "needs a name: a letter, then letters and digits (32 at most)");
      return;
    }
    if (seen.includes(o.name)) add("error", where, `${p}.name`, `"${o.name}" is used twice`);
    seen.push(o.name);
    if (typeof o.instructions !== "string" || !o.instructions.trim()) {
      add("error", where, `${p}.instructions`, "is required: what the judge scores, and how");
    }
    const scale = o.scale;
    if (scale !== undefined && scale !== null && (!Array.isArray(scale) || scale.length < 2 || scale.length > 20 || scale.some((s) => {
      const x = s as Record<string, unknown>;
      return !isObj(s) || typeof x.value !== "number" || typeof x.label !== "string" || typeof x.definition !== "string";
    }))) {
      add("error", where, `${p}.scale`, "must be 2 to 20 {value, label, definition} points");
    }
  });
  const mem = isObj(core.memory) ? core.memory as Record<string, unknown> : {};
  if ("custom" in mem) {
    const m = mem.custom as Record<string, unknown>;
    const bases = vocab("customMemoryBases");
    if (!isObj(m) || !bases.includes(m.base as string)) {
      add("error", where, `${path}.memory.custom.base`, `must be one of: ${bases.join(", ")}`);
    } else if (typeof m.instructions !== "string" || !m.instructions.trim()) {
      add("error", where, `${path}.memory.custom.instructions`, "is required: what to extract, and what to leave out");
    }
  }
}

/** The optional named maps, and the block each entry is checked against. */
export const NAMED = { guardrails: "guardrail", memories: "memory", evaluators: "evaluator",
  identities: "identity", policies: "policy" } as const;
export type NamedMap = keyof typeof NAMED;
/** What a guardrail enforces: one of these, or it is refused by Bedrock. */
const GUARDRAIL_POLICIES = ["contentFilters", "deniedWords", "managedWordLists", "deniedTopics", "piiEntities"];
/** A build keeps a shared (library) item as {"library": "<id>"} until it is resolved. */
const SHARED_WHAT: Record<string, string> = { tools: "tool", guardrails: "guardrail", memories: "memory",
  evaluators: "evaluator", identities: "identity", policies: "policy" };
const named = (wf: Workflow, name: string): Record<string, unknown> =>
  (isObj(wf[name]) ? wf[name] : {}) as Record<string, unknown>;

/** The named maps (guardrails, memories, evaluators, identities, policies), what agents
 *  and tools name from them, and shared items that could not be loaded. Mirrored by
 *  bff/validate_build.py _check_named. */
/** The agents that run before `agentId`: every earlier step's, and the ones before it in
 *  its own `sequence` (a `parallel` group's peers run at the same time). Mirrors
 *  bff/validate_build.py upstream_ids and app/common/config.upstream_of. */
export function upstreamIds(steps: StepSpec[], agentId: string): string[] {
  const seen: string[] = [];
  for (const step of steps) {
    const ids = isObj(step) ? stepAgents(step) : [];
    if (ids.includes(agentId)) {
      if (Array.isArray(step.sequence) && step.sequence.length) seen.push(...ids.slice(0, ids.indexOf(agentId)));
      return seen;
    }
    seen.push(...ids);
  }
  return seen;
}

/** `vision`: the agents named must run earlier, and draw images. Mirrors _check_vision. */
function checkVision(a: Record<string, unknown>, aid: string, agents: Record<string, unknown>, steps: StepSpec[],
  where: Where, path: string, add: Add): void {
  if (!isObj(a.vision)) return;
  const v = a.vision as Record<string, unknown>;
  let sources: unknown[] = Array.isArray(v.from) ? v.from : [];
  if (!Array.isArray(v.from) || !v.from.length) {
    add("error", where, `${path}.vision.from`, "name at least one earlier agent whose images this agent reads");
    sources = [];
  }
  const before = upstreamIds(steps, aid);
  for (const src of sources) {
    if (typeof src !== "string" || !(src in agents)) {
      add("error", where, `${path}.vision.from`, `"${typeof src === "string" ? src : JSON.stringify(src)}" is not an agent in this workflow`);
    } else if (src === aid) {
      add("error", where, `${path}.vision.from`, "an agent cannot read its own images");
    } else if (!before.includes(src)) {
      add("error", where, `${path}.vision.from`, `"${src}" does not run before ${aid}, so it has no images yet when ${aid} runs`);
    } else if (!isObj(agents[src]) || (agents[src] as Record<string, unknown>).output !== "image") {
      add("warning", where, `${path}.vision.from`, `"${src}" is not an image agent (output "image"): only images its output lists are read`);
    }
  }
  const n = v.maxImages;
  if ("maxImages" in v && !(typeof n === "number" && Number.isInteger(n) && n >= 1 && n <= 20)) {
    add("error", where, `${path}.vision.maxImages`, "must be a whole number from 1 to 20");
  }
}

function checkNamed(wf: Workflow, add: Add, out: Issue[]): void {
  const tools = (isObj(wf.tools) ? wf.tools : {}) as Record<string, unknown>;
  const agents = (isObj(wf.agents) ? wf.agents : {}) as Record<string, unknown>;
  for (const m of Object.keys(SHARED_WHAT)) {
    for (const [key, entry] of Object.entries(named(wf, m))) {
      if (isObj(entry) && "library" in entry) {
        const where: Where = m === "tools" ? { kind: "tool", id: key } : { kind: "block", name: m };
        add("error", where, `${m}.${key}`, `a shared ${SHARED_WHAT[m]} that could not be loaded: it was deleted, or is no `
          + "longer shared with you. Remove it, or pick another");
      }
    }
  }
  for (const [m, blk] of Object.entries(NAMED)) {
    if (m in wf && !isObj(wf[m])) {
      add("error", { kind: "block", name: m }, m, "must be an object of named entries");
      continue;
    }
    for (const [key, entry] of Object.entries(named(wf, m))) {
      const where: Where = { kind: "block", name: m };
      const path = `${m}.${key}`;
      if (isObj(entry) && "library" in entry) continue;
      if (!cedar.NAME_RE.test(key)) add("error", where, path, "a name is a letter, then letters and digits (32 at most)");
      checkEntry(blk as BlockName, entry, path, where, out);
      if (!isObj(entry)) continue;
      if (m === "guardrails" && !GUARDRAIL_POLICIES.some((k) => truthy(entry[k]))) {
        add("error", where, path, "a guardrail needs something to enforce: content filters, denied "
          + "words or topics, managed word lists or PII entities");
      }
      if (m === "memories") {
        if (Array.isArray(entry.strategies) && !entry.strategies.length) add("error", where, `${path}.strategies`, "name at least one strategy");
        const days = entry.expiryDays;
        if (typeof days === "number" && (days < 3 || days > 365)) add("error", where, `${path}.expiryDays`, "must be from 3 to 365");
      }
      if (m === "evaluators") {
        if (typeof entry.instructions === "string" && !entry.instructions.trim()) {
          add("error", where, `${path}.instructions`, "is required: what the judge scores, and how");
        }
        const scale = entry.scale;
        if (scale !== undefined && scale !== null && (!Array.isArray(scale) || scale.length < 2 || scale.length > 20 || scale.some((s) => {
          const x = s as Record<string, unknown>;
          return !isObj(s) || typeof x.value !== "number" || typeof x.label !== "string" || typeof x.definition !== "string";
        }))) {
          add("error", where, `${path}.scale`, "must be 2 to 20 {value, label, definition} points");
        }
      }
      if (m === "identities" && entry.type === "oauth2") {
        const urls = [entry.discoveryUrl, entry.tokenUrl].filter((u) => u !== undefined && u !== null && u !== "");
        if (urls.length !== 1) add("error", where, path, "an oauth2 identity needs exactly one of discoveryUrl or tokenUrl");
        for (const u of urls) {
          if (typeof u !== "string" || !HTTPS_RE.test(u)) add("error", where, path, "discoveryUrl / tokenUrl must be an https:// URL");
        }
      }
      if (m === "policies") {
        for (const message of cedar.problems(entry.statement, Object.keys(tools))) add("error", where, `${path}.statement`, message);
        const attached = Object.entries(tools).filter(([, t]) => isObj(t) && Array.isArray(t.policies) && t.policies.includes(key));
        if (cedar.isTemplate(entry.statement) && !attached.length) {
          add("warning", where, path, "is written for {{tool}} and attached to no tool, so it deploys nowhere — attach it to a tool");
        }
      }
    }
  }
  // --- what tools name
  const identities = named(wf, "identities");
  const policies = named(wf, "policies");
  const pol = isObj(wf.orchestrator) ? (wf.orchestrator as Record<string, unknown>).policy : undefined;
  const engineOff = isObj(pol) && pol.enabled === false;
  for (const [key, tool] of Object.entries(tools)) {
    if (!isObj(tool) || "library" in tool) continue;
    const where: Where = { kind: "tool", id: key };
    const path = `tools.${key}`;
    const ident = tool.identity;
    if (typeof ident === "string" && ident) {
      const got = identities[ident];
      const t = String(tool.type ?? "").toLowerCase();
      if (!isObj(got)) {
        add("error", where, `${path}.identity`, `"${ident}" is not an identity. Define it under Identity, or pick one of: `
          + `${Object.keys(identities).join(", ") || "(none yet)"}`);
      } else if (got.type === "oauth2" && !vocab("oauthToolTypes").includes(t)) {
        add("error", where, `${path}.identity`, `an oauth2 identity works with ${vocab("oauthToolTypes").join(" or ")} tools`);
      } else if (got.type === "apikey" && !vocab("apiKeyToolTypes").includes(t)) {
        add("error", where, `${path}.identity`, `an apikey identity works with ${vocab("apiKeyToolTypes").join(", ")} tools`);
      }
      for (const k of ["auth", "oauth"]) {
        if (tool[k] !== undefined && tool[k] !== null && tool[k] !== "") add("error", where, `${path}.${k}`, "the identity says how to authenticate — remove this");
      }
    }
    const names = Array.isArray(tool.policies) ? tool.policies : [];
    names.forEach((n: unknown, i: number) => {
      const p = `${path}.policies[${i}]`;
      if (typeof n !== "string") return;
      const got = policies[n];
      if (!isObj(got)) {
        add("error", where, p, `"${n}" is not a policy. Define it under Policies, or pick one of: ${Object.keys(policies).join(", ") || "(none yet)"}`);
        return;
      }
      if (names.indexOf(n) !== i) {
        add("error", where, p, `"${n}" is listed twice`);
        return;
      }
      if ("library" in got || typeof got.statement !== "string") return;
      // Mirrors validate_build.py: a tool whose one name the framework fixes.
      const tt = String(tool.type ?? "").toLowerCase();
      const fixed = FIXED_TOOL_NAMES[tt];
      if (fixed) {
        let acts: string[] = [];
        try { acts = cedar.parse(cedar.forTool(got.statement, key)).actions; } catch { /* named below */ }
        for (const a of acts) {
          if (a.startsWith(`${key}___`) && a !== `${key}___${fixed}`) {
            add("error", where, p, `"${n}" names ${a}, but a ${tt} tool's one tool is "${fixed}": write AgentCore::Action::"${key}___${fixed}"`);
          }
        }
      }
      if (!cedar.isTemplate(got.statement)) {
        if (!cedar.governs(got.statement, key)) {
          add("error", where, p, `"${n}" names other tools, not ${key}: write it for AgentCore::Action::"{{tool}}" to attach it to any tool`);
        }
        return;
      }
      for (const message of cedar.problems(cedar.forTool(got.statement, key), Object.keys(tools))) add("error", where, p, `"${n}" for ${key}: ${message}`);
    });
    if (names.length && engineOff) add("warning", where, `${path}.policies`, "is not deployed: the policy engine is off (orchestrator.policy.enabled)");
  }
  // --- what agents name
  const guardrails = named(wf, "guardrails");
  const memories = named(wf, "memories");
  for (const [aid, a] of Object.entries(agents)) {
    if (!isObj(a) || !isObj(a.agentcore)) continue;
    const core = a.agentcore as Record<string, unknown>;
    const where: Where = { kind: "agent", id: aid };
    const path = `agents.${aid}.agentcore`;
    const g = (isObj(core.guardrails) ? core.guardrails : {}) as Record<string, unknown>;
    if (typeof g.use === "string" && g.use) {
      if (!Object.keys(guardrails).includes(g.use)) {
        add("error", where, `${path}.guardrails.use`, `"${g.use}" is not a guardrail. Define it under Guardrails, or pick one of: `
          + `${Object.keys(guardrails).join(", ") || "(none yet)"}`);
      }
      if (Boolean(g.guardrailId)) add("error", where, `${path}.guardrails.guardrailId`, "use a guardrail by name or by id, not both");
      if (!g.input && !g.output) add("warning", where, `${path}.guardrails`, "names a guardrail but checks neither input nor output — turn one on");
    }
    const mem = (isObj(core.memory) ? core.memory : {}) as Record<string, unknown>;
    if (typeof mem.use === "string" && mem.use) {
      if (!Object.keys(memories).includes(mem.use)) {
        add("error", where, `${path}.memory.use`, `"${mem.use}" is not a memory. Define it under Memory, or pick one of: `
          + `${Object.keys(memories).join(", ") || "(none yet)"}`);
      }
      for (const k of ["longTerm", "scope", "custom"]) {
        if (k in mem) add("error", where, `${path}.memory.${k}`, "the memory it uses sets this — remove it");
      }
    }
  }
}

const isObj = (v: unknown): v is Record<string, unknown> =>
  typeof v === "object" && v !== null && !Array.isArray(v);
const truthy = (v: unknown) => v !== undefined && v !== null && v !== "" && v !== false
  && !(Array.isArray(v) && v.length === 0);

function typeOk(v: unknown, t: string | string[] | undefined): boolean {
  if (Array.isArray(t)) return t.some((x) => typeOk(v, x));
  switch (t) {
    case undefined: return true;
    case "string": return typeof v === "string";
    case "number": return typeof v === "number" && Number.isFinite(v);
    case "integer": return typeof v === "number" && Number.isInteger(v);
    case "boolean": return typeof v === "boolean";
    case "array": return Array.isArray(v);
    case "object": return isObj(v);
    default: return true;
  }
}

/** One entry's SHAPE against its block in keys.json. */
function checkEntry(name: BlockName, entry: unknown, path: string, where: Where, out: Issue[]): void {
  const err = (p: string, message: string, severity: Issue["severity"] = "error") =>
    out.push({ severity, where, path: p, message });
  if (!isObj(entry)) {
    err(path, "must be an object");
    return;
  }
  const spec = block(name);
  const variant = variantOf(name, entry);
  const variantLabel = spec.$variantKey ? `${spec.$variantKey} "${variant}"` : "";

  if (spec.$variantKey && typeof entry[spec.$variantKey] === "string") {
    const allowed = allowedValues(name, spec.$variantKey, variant);
    if (allowed && !allowed.includes(String(entry[spec.$variantKey]).toLowerCase())) {
      err(`${path}.${spec.$variantKey}`,
        `"${String(entry[spec.$variantKey])}" is not one of: ${allowed.join(", ")}`);
      return;
    }
  }

  const dotted = Object.keys(spec.keys).filter((k) => k.includes("."));
  const parents = new Set(dotted.map((k) => k.split(".")[0]));

  const checkValue = (key: string, value: unknown, p: string) => {
    const k = spec.keys[key];
    if (!applies(name, key, variant)) {
      err(p, `\`${key.split(".").pop()}\` does not apply to ${variantLabel || "this entry"} — `
        + "a key that looks like a setting and controls nothing is rejected, not ignored");
      return;
    }
    if (!typeOk(value, k.type)) {
      const t = Array.isArray(k.type) ? k.type.join(" or ") : k.type ?? "";
      err(p, `must be ${t === "integer" ? "a whole number" : `a${/^[aeiou]/.test(t) ? "n" : ""} ${t}`}`);
      return;
    }
    const allowed = allowedValues(name, key, variant);
    if (Array.isArray(value)) {
      value.forEach((item, i) => {
        if (k.items && k.items !== "object" && !typeOk(item, k.items)) err(`${p}[${i}]`, `must be a ${k.items}`);
        if (allowed && typeof item === "string" && !allowed.includes(item)) {
          err(`${p}[${i}]`, `"${item}" is not one of: ${allowed.join(", ")}`);
        }
        if (k.pattern && typeof item === "string" && !new RegExp(k.pattern).test(item)) {
          err(`${p}[${i}]`, `"${item}" does not match ${k.pattern}`);
        }
        if (k.itemRequired && isObj(item)) {
          for (const f of k.itemRequired) if (!truthy(item[f])) err(`${p}[${i}]`, `each entry needs \`${f}\``);
        }
      });
    } else if (allowed && typeof value === "string" && !allowed.includes(value)) {
      err(p, `"${value}" is not one of: ${allowed.join(", ")}`);
    }
    if (k.pattern && typeof value === "string" && !new RegExp(k.pattern).test(value)) {
      err(p, `"${value}" does not match ${k.pattern}`);
    }
    if (isObj(value)) {
      if (k.valueVocabulary) {
        const ok = vocab(k.valueVocabulary);
        for (const [mk, mv] of Object.entries(value)) {
          if (!ok.includes(String(mv))) err(`${p}.${mk}`, `"${String(mv)}" is not one of: ${ok.join(", ")}`);
        }
      }
      if (k.keyVocabulary) {
        const ok = vocab(k.keyVocabulary);
        for (const mk of Object.keys(value)) if (!ok.includes(mk)) err(`${p}.${mk}`, `"${mk}" is not one of: ${ok.join(", ")}`);
      }
      if (k.properties) {
        for (const [sk, sv] of Object.entries(value)) {
          const sub = k.properties[sk];
          if (!sub) err(`${p}.${sk}`, `unknown key; allowed: ${Object.keys(k.properties).join(", ")}`);
          else if (!typeOk(sv, sub.type)) err(`${p}.${sk}`, `must be a${sub.type === "array" || sub.type === "object" || sub.type === "integer" ? "n" : ""} ${sub.type ?? "string"}`);
        }
      }
    }
  };

  for (const [key, value] of Object.entries(entry)) {
    const p = `${path}.${key}`;
    if (spec.keys[key]) {
      checkValue(key, value, p);
    } else if (parents.has(key)) {
      if (!isObj(value)) {
        err(p, "must be an object");
        continue;
      }
      for (const [sub, sv] of Object.entries(value)) {
        const full = `${key}.${sub}`;
        if (!spec.keys[full]) {
          const legal = dotted.filter((d) => d.startsWith(`${key}.`)).map((d) => d.split(".")[1]);
          err(`${p}.${sub}`, `unknown key; ${key} takes: ${legal.join(", ")}`);
        } else {
          checkValue(full, sv, `${p}.${sub}`);
        }
      }
    } else if (spec.$removed?.[key]) {
      err(p, spec.$removed[key]);
    } else {
      err(p, `unknown key — nothing in the framework reads \`${key}\``);
    }
  }

  for (const key of Object.keys(spec.keys)) {
    if (key.includes(".")) continue;
    if (requiredFor(name, key, variant) && !(key in entry)) {
      err(`${path}.${key}`, `\`${key}\` is required${variantLabel ? ` for ${variantLabel}` : ""}`);
    }
  }

  for (const rule of spec.$exactlyOne ?? []) {
    if (rule.appliesTo !== "*" && !rule.appliesTo.includes(variant)) continue;
    const set = rule.keys.filter((k) => truthy(entry[k]));
    if (set.length !== 1) {
      err(path, `set exactly one of ${rule.keys.map((k) => `\`${k}\``).join(" / ")}`
        + `${set.length ? ` (found ${set.join(" and ")})` : ""}. ${rule.why}`);
    }
  }

  if (name === "agent" && "agentcore" in entry) {
    checkEntry("agentcore", entry.agentcore, `${path}.agentcore`, where, out);
  }
}

/** The branch grammar. Mirrors app/common/branching.validate_spec and the placement
 *  rules in graph_builder.validate_branches. */
function checkBranch(steps: StepSpec[], i: number, out: Issue[]): void {
  const step = steps[i];
  const where: Where = { kind: "step", index: i };
  const path = `steps[${i}].branch`;
  const err = (message: string, p = path) => out.push({ severity: "error", where, path: p, message });
  const b = step.branch as unknown;
  if (b === undefined) return;
  if (step.parallel) err("a parallel stage cannot branch — no single agent's output decides. Branch on a stage after it.");
  if (i === steps.length - 1) err("the last stage cannot branch — there is nowhere left to route.");
  if (!isObj(b)) {
    err("must be an object with `when` and/or `default`");
    return;
  }
  for (const k of Object.keys(b)) if (!["when", "default"].includes(k)) err(`unknown key \`${k}\`; a branch takes \`when\` and \`default\``);
  const hasDefault = typeof b.default === "string" && b.default.trim() !== "";
  if (b.when === undefined && !hasDefault) err("needs `when` rules, a `default`, or both");
  if (b.default !== undefined && !hasDefault) err("`default` must name a later stage or END", `${path}.default`);

  const names = steps.map(stepName);
  const target = (t: unknown, p: string) => {
    if (typeof t !== "string" || !t.trim()) return;
    if (t === "END") return;
    const at = names.indexOf(t);
    if (at < 0) err(`"${t}" is not a stage. Targets are END or a later stage's name: ${names.slice(i + 1).join(", ") || "(none)"}`, p);
    else if (at <= i) err(`"${t}" is not AFTER this stage — a branch may only skip forward, never loop back`, p);
  };
  if (hasDefault) target(b.default, `${path}.default`);

  if (b.when !== undefined) {
    if (!Array.isArray(b.when) || b.when.length === 0) {
      err("`when` must be a non-empty list of rules", `${path}.when`);
      return;
    }
    b.when.forEach((rule, r) => {
      const p = `${path}.when[${r}]`;
      if (!isObj(rule)) {
        err("each rule must be an object", p);
        return;
      }
      for (const k of Object.keys(rule)) {
        if (!["field", "goto", ...BRANCH_OPS].includes(k)) err(`unknown key \`${k}\`; operators are ${BRANCH_OPS.join(", ")}`, p);
      }
      if (typeof rule.goto !== "string" || !rule.goto.trim()) err("each rule needs a `goto`", p);
      const ops = BRANCH_OPS.filter((o) => o in rule);
      if (!ops.length) err(`each rule needs an operator: ${BRANCH_OPS.join(", ")}`, p);
      for (const o of ops) {
        const v = rule[o];
        if (o === "in" && !Array.isArray(v)) err("`in` takes a list", `${p}.in`);
        if (o === "exists" && typeof v !== "boolean") err("`exists` takes true or false", `${p}.exists`);
        if (["gt", "gte", "lt", "lte"].includes(o)
          && !(typeof v === "number" || (typeof v === "string" && v.trim() !== "" && Number.isFinite(Number(v))))) {
          err(`\`${o}\` takes a number`, `${p}.${o}`);
        }
        if (["equals", "notEquals", "contains"].includes(o) && (Array.isArray(v) || isObj(v))) {
          err(`\`${o}\` takes a single value, not a list or object`, `${p}.${o}`);
        }
      }
      target(rule.goto, `${p}.goto`);
    });
  }
}

export function validate(wf: Workflow): Issue[] {
  const out: Issue[] = [];
  const W: Where = { kind: "workflow" };
  const add = (severity: Issue["severity"], where: Where, path: string, message: string) =>
    out.push({ severity, where, path, message });

  for (const k of Object.keys(wf)) {
    if (!TOP_LEVEL.includes(k) && !(k in NAMED)) add("error", W, k, `unknown top-level block \`${k}\``);
  }
  for (const k of TOP_LEVEL) {
    if (!(k in wf)) add("warning", W, k, `\`${k}\` is missing — the test suite expects every top-level block to be present`);
  }
  for (const name of ["orchestrator", "ui", "guardrail", "authorization"] as const) {
    if (name in wf) checkEntry(name, wf[name], name, { kind: "block", name }, out);
  }

  const agents = isObj(wf.agents) ? wf.agents : {};
  const tools = isObj(wf.tools) ? wf.tools : {};
  const steps = Array.isArray(wf.steps) ? wf.steps : [];
  checkPolicy(wf.orchestrator, tools, add);
  checkNamed(wf, add, out);

  // --- tools -------------------------------------------------------------------
  const kinds: Record<string, string[]> = {};
  for (const [key, tool] of Object.entries(tools)) {
    const where: Where = { kind: "tool", id: key };
    const path = `tools.${key}`;
    if (isObj(tool) && "library" in tool) continue;     // a shared tool that was not loaded (checkNamed says so)
    if (!TOOL_KEY_RE.test(key)) {
      add("error", where, path, "a tool key must be letters and digits, starting with a letter — it names both the "
        + "Gateway target (no underscores) and the Cedar policy permit_<key> (no hyphens). Use camelCase, e.g. \"claimsDb\".");
    }
    checkEntry("tool", tool, path, where, out);
    if (!isObj(tool)) continue;
    const type = String(tool.type ?? "").toLowerCase();
    (kinds[type] ??= []).push(key);
    if (type === "kb") {
      if (!Array.isArray(tool.corpora) || tool.corpora.length === 0) {
        add("error", where, `${path}.corpora`, "a Knowledge Base needs at least one corpus — a folder under kb_docs/");
      }
      const dims = vocabExtra<Record<string, number[]>>("embeddingModels", "dimensionsByModel");
      const model = String(tool.embeddingModel ?? vocab("embeddingModels")[0] ?? "");
      if (tool.dimensions !== undefined && dims?.[model] && !dims[model].includes(Number(tool.dimensions))) {
        add("error", where, `${path}.dimensions`, `${model} supports ${dims[model].join(", ")}`);
      }
    }
    if (type === "mcp" && typeof tool.endpoint === "string" && !/^https:\/\/\S+\.\S+/.test(tool.endpoint)) {
      add("error", where, `${path}.endpoint`, "must be the server's https:// URL");
    }
    if (type === "kb") {
      const kbId = tool.knowledgeBaseId, s3Uri = tool.s3Uri, kms = tool.kmsKeyArn;
      if (typeof kbId === "string" && !KB_ID_RE.test(kbId)) {
        add("error", where, `${path}.knowledgeBaseId`, "must be a Knowledge Base id: 10 capital letters and digits");
      }
      if (typeof s3Uri === "string" && !S3_LOCATION_RE.test(s3Uri)) {
        add("error", where, `${path}.s3Uri`, "must be s3://<bucket> or s3://<bucket>/<prefix>/");
      }
      if (typeof kms === "string" && !KMS_KEY_ARN_RE.test(kms)) {
        add("error", where, `${path}.kmsKeyArn`, "must be a KMS key ARN, arn:aws:kms:<region>:<account>:key/<id>");
      }
      if (kbId) {
        for (const k of ["s3Uri", "kmsKeyArn", "embeddingModel", "dimensions"]) {
          if (tool[k] !== undefined && tool[k] !== null) {
            add("error", where, `${path}.${k}`,
              "does not apply with knowledgeBaseId: that Knowledge Base already has its documents and embeddings");
          }
        }
      } else if (kms && !s3Uri) {
        add("error", where, `${path}.kmsKeyArn`, "only applies with s3Uri: it decrypts that bucket");
      }
    }
    if (type === "apigateway") {
      if (typeof tool.restApiId === "string" && !REST_API_ID_RE.test(tool.restApiId)) {
        add("error", where, `${path}.restApiId`, "must be a REST API id: 10 lowercase letters and digits");
      }
      if (typeof tool.stage === "string" && !STAGE_RE.test(tool.stage)) {
        add("error", where, `${path}.stage`, "must be a stage name: letters, digits, - and _");
      }
      const methods = vocab("httpMethods");
      const filters = tool.toolFilters;
      if (Array.isArray(filters)) {
        if (!filters.length) add("error", where, `${path}.toolFilters`, "needs at least one filter, or the target exposes no tool");
        filters.forEach((f: unknown, i: number) => {
          const fp = `${path}.toolFilters[${i}]`;
          const o = f as Record<string, unknown>;
          if (!isObj(f) || typeof o.path !== "string" || !o.path.startsWith("/")) {
            add("error", where, fp, 'must be {"path": "/...", "methods": [...]}, the path starting with /');
            return;
          }
          const ms = o.methods;
          if (!Array.isArray(ms) || !ms.length || ms.some((m) => !methods.includes(m as string))) {
            add("error", where, `${fp}.methods`, `must list one or more of: ${methods.join(", ")}`);
          }
        });
      }
      const overrides = tool.toolOverrides;
      if (Array.isArray(overrides)) {
        overrides.forEach((ov: unknown, i: number) => {
          const op = `${path}.toolOverrides[${i}]`;
          const o = ov as Record<string, unknown>;
          if (!isObj(ov) || typeof o.path !== "string" || !o.path.startsWith("/") || o.path.includes("*")) {
            add("error", where, op, "needs an explicit path (no *), starting with /");
            return;
          }
          if (!methods.includes(o.method as string)) add("error", where, `${op}.method`, `must be one of: ${methods.join(", ")}`);
          if (typeof o.name !== "string" || !TOOL_NAME_RE.test(o.name)) {
            add("error", where, `${op}.name`, "must be a tool name: a letter, then letters, digits, - or _ (64 at most)");
          }
        });
      }
    }
    if (type === "openapi" && "schema" in tool) {
      const doc = tool.schema as Record<string, unknown>;
      if (!isObj(doc) || !String(doc.openapi ?? "").startsWith("3") || !isObj(doc.paths)) {
        add("error", where, `${path}.schema`, 'must be an OpenAPI 3 document: {"openapi": "3.0.x", "info": {...}, "paths": {...}}');
      } else {
        const paths = doc.paths as Record<string, unknown>;
        if (!Object.keys(paths).length) add("error", where, `${path}.schema.paths`, "has no operations, so the target would expose no tool");
        for (const [p, item] of Object.entries(paths)) {
          for (const [m, oper] of Object.entries(isObj(item) ? (item as Record<string, unknown>) : {})) {
            if (OPENAPI_OPS.includes(m) && (!isObj(oper) || !(oper as Record<string, unknown>).operationId)) {
              add("error", where, `${path}.schema.paths.${p}.${m}`, "needs an operationId: it becomes the tool's name");
            }
          }
        }
        const comps = doc.components as Record<string, unknown> | undefined;
        if (isObj(comps) && comps.securitySchemes) {
          add("error", where, `${path}.schema.components.securitySchemes`, "is not supported by the Gateway: set `auth` on the tool instead");
        }
      }
    }
    if (tool.auth === "oauth2") {
      if (!vocab("oauthToolTypes").includes(type)) {
        add("error", where, `${path}.auth`, `oauth2 is only for ${vocab("oauthToolTypes").join(", ")} tools`);
      }
      const oa = tool.oauth as Record<string, unknown>;
      if (!isObj(oa)) {
        add("error", where, `${path}.oauth`, 'auth "oauth2" needs {"clientId", "scopes", and "discoveryUrl" or "tokenUrl"}');
      } else {
        if (typeof oa.clientId !== "string" || !oa.clientId.trim()) add("error", where, `${path}.oauth.clientId`, "is required: the OAuth client's id");
        if (!Array.isArray(oa.scopes) || oa.scopes.some((s) => typeof s !== "string")) {
          add("error", where, `${path}.oauth.scopes`, "must be a list of scope names (it may be empty)");
        }
        const urls = ["discoveryUrl", "tokenUrl"].filter((k) => oa[k]);
        if (urls.length !== 1) {
          add("error", where, `${path}.oauth`, "needs exactly one of discoveryUrl (the issuer's .well-known/openid-configuration) or tokenUrl");
        }
        for (const k of ["discoveryUrl", "tokenUrl", "issuer"]) {
          if (oa[k] !== undefined && oa[k] !== null && (typeof oa[k] !== "string" || !HTTPS_RE.test(oa[k] as string))) {
            add("error", where, `${path}.oauth.${k}`, "must be an https:// URL");
          }
        }
      }
    } else if ("oauth" in tool) {
      add("warning", where, `${path}.oauth`, 'is only used with auth "oauth2"');
    }
    if (type === "openapi" && typeof tool.schemaS3Uri === "string" && !S3_URI_RE.test(tool.schemaS3Uri)) {
      add("error", where, `${path}.schemaS3Uri`, "must be s3://<bucket>/<key> — the Gateway loads an OpenAPI schema only from S3");
    }
    if (type === "lambda") {
      if ("code" in tool) checkCode(key, tool.code, path, where, add);
      // Empty is the exactly-one rule's to report ("set lambdaArn or source"); saying
      // it twice for one missing value is noise.
      if (typeof tool.lambdaArn === "string" && tool.lambdaArn && !LAMBDA_ARN_RE.test(tool.lambdaArn)) {
        add("error", where, `${path}.lambdaArn`, "must be a Lambda function ARN, arn:aws:lambda:<region>:<account>:function:<name>");
      }
      const schema = Array.isArray(tool.toolSchema) ? tool.toolSchema : [];
      if (schema.length === 0) add("error", where, `${path}.toolSchema`, "a Lambda tool needs at least one tool in `toolSchema`, so the Gateway can publish it");
      const names: string[] = [];
      const propsOf: Record<string, string[]> = {};
      schema.forEach((t, i) => {
        const p = `${path}.toolSchema[${i}]`;
        if (!isObj(t)) {
          add("error", where, p, "each entry must be an object");
          return;
        }
        if (typeof t.name !== "string" || !t.name) add("error", where, p, "each entry needs a `name`");
        else names.push(t.name);
        const props = isObj(t.properties) ? t.properties : {};
        if (!Object.keys(props).length) add("error", where, `${p}.properties`, "each entry needs at least one property");
        propsOf[String(t.name)] = Object.keys(props);
        for (const [pn, pv] of Object.entries(props)) {
          const pt = String((isObj(pv) ? pv.type : undefined) ?? "string").toLowerCase();
          if (!vocab("toolSchemaPropertyTypes").includes(pt)) {
            add("error", where, `${p}.properties.${pn}.type`, `"${pt}" is not one of: ${vocab("toolSchemaPropertyTypes").join(", ")}`);
          }
        }
      });
      if (names.length > 1 && !tool.call) add("error", where, `${path}.call`, "with more than one tool in `toolSchema`, say which one agents call");
      if (tool.call && names.length && !names.includes(String(tool.call))) {
        add("error", where, `${path}.call`, `"${String(tool.call)}" is not a name in toolSchema (${names.join(", ")})`);
      }
      const called = String(tool.call ?? names[0] ?? "");
      const arg = String(tool.arg ?? "query");
      if (propsOf[called] && !propsOf[called].includes(arg)) {
        add("error", where, `${path}.arg`, `"${arg}" is not a property of ${called} (${propsOf[called].join(", ")}). `
          + "Set `arg` to the property the agent's query goes into.");
      }
    }
  }
  for (const t of ["kb", "websearch"]) {
    if ((kinds[t]?.length ?? 0) > 1) {
      for (const key of kinds[t]) add("error", { kind: "tool", id: key }, `tools.${key}`, `at most one ${t} tool per workflow (found ${kinds[t].join(", ")})`);
    }
  }

  // --- agents ------------------------------------------------------------------
  let local = 0;
  let evaluated = 0;
  for (const [id, agent] of Object.entries(agents)) {
    const where: Where = { kind: "agent", id };
    const path = `agents.${id}`;
    if (!AGENT_ID_RE.test(id)) {
      add("error", where, path, "an agent id must be letters, digits and underscores, starting with a letter — "
        + "it becomes part of an AgentCore Runtime name, so no hyphens");
    }
    checkEntry("agent", agent, path, where, out);
    if (!isObj(agent)) continue;
    const a = agent as Entry;
    const runtime = variantOf("agent", a);
    if (runtime !== "a2a") {
      local += 1;
      if (typeof a.maxTokens === "number" && a.maxTokens <= 0) add("error", where, `${path}.maxTokens`, "must be greater than 0");
      const bound = toolsOf(a.tool);
      if (a.tool !== undefined && !bound.length) add("error", where, `${path}.tool`, "`tool` must name at least one tool — remove it to have none");
      if (bound.length && a.access) add("error", where, `${path}.access`, "`access` describes what an agent reads when it has NO tool — remove it now that `tool` is set");
      bound.forEach((key, i) => {
        if (!tools[key]) add("error", where, `${path}.tool`, `"${key}" is not a tool. Declare it under Tools, or pick one of: ${Object.keys(tools).join(", ") || "(none yet)"}`);
        if (bound.indexOf(key) !== i) add("error", where, `${path}.tool`, `"${key}" is listed twice`);
      });
      if (typeof a.maxToolCalls === "number" && (a.maxToolCalls < 1 || a.maxToolCalls > 20)) {
        add("error", where, `${path}.maxToolCalls`, "must be from 1 to 20");
      }
      if (a.toolMode === "model" && !bound.length) {
        add("warning", where, `${path}.toolMode`, "\"model\" lets the model choose among the agent's tools, and it has none — bind one with `tool`");
      }
      checkVision(a, id, agents as Record<string, unknown>, steps as StepSpec[], where, path, add);
      if (a.image !== undefined && a.output !== "image") {
        add("error", where, `${path}.image`, "`image` configures how an image agent renders — set `output` to \"image\", or remove it");
      }
      if (a.output === "image") {
        const img = isObj(a.image) ? a.image : {};
        const model = String(img.model ?? vocab("imageModels")[0] ?? "");
        const regions = (vocabExtra<Record<string, string[]>>("imageModels", "regionsByModel")?.[model] ?? []).join(", ") || "its own region";
        for (const [k, set] of [["model", "imageModels"], ["aspectRatio", "imageAspectRatios"], ["outputFormat", "imageFormats"]] as const) {
          if (img[k] !== undefined && !vocab(set).includes(String(img[k]))) {
            add("error", where, `${path}.image.${k}`, `"${String(img[k])}" is not one of: ${vocab(set).join(", ")}`);
          }
        }
        if (typeof img.seed === "number" && (img.seed < 0 || img.seed > 4294967294)) {
          add("error", where, `${path}.image.seed`, "must be between 0 and 4294967294");
        }
        add("warning", where, `${path}.output`, `images are rendered by ${model}, which Bedrock serves in ${regions}: a `
          + "deployment elsewhere sends this agent's image brief there, and one outside that geography is refused unless "
          + "image.allowCrossRegion is true. Bedrock guardrails and evaluations do not check images");
      }
      if (a.corpus !== undefined) {
        const kb = bound.map((k) => tools[k]).find((t) => t && String(t.type) === "kb");
        if (!kb) add("error", where, `${path}.corpus`, "`corpus` needs a Knowledge Base among this agent's tools");
        else if (!Array.isArray(kb.corpora) || !kb.corpora.includes(a.corpus as string)) {
          add("error", where, `${path}.corpus`, `"${String(a.corpus)}" is not one of this KB's corpora (${Array.isArray(kb.corpora) ? kb.corpora.join(", ") : "none"})`);
        }
      }
    } else {
      if (a.source === "a2a_lambda" && (a.auth ?? "none") !== "sigv4") {
        add("error", where, `${path}.auth`, "the framework-deployed stand-in (source \"a2a_lambda\") is called with SigV4 — set auth to \"sigv4\"");
      }
      if (a.skill && !a.source) add("error", where, `${path}.skill`, "`skill` picks a stand-in skill, so it needs `source`");
      if (typeof a.agentCard === "string" && a.agentCard && !a.agentCard.startsWith("https://")) {
        add("error", where, `${path}.agentCard`, "must be an https:// URL");
      }
      const outbound = isObj(a.agentcore) && isObj(a.agentcore.identity) ? a.agentcore.identity.outbound : undefined;
      if (a.auth === "oauth2" && !truthy(outbound)) {
        add("error", where, `${path}.auth`, "oauth2 needs a credential provider in agentcore.identity.outbound");
      }
    }
    const core = a.agentcore;
    if (isObj(core)) {
      if (Object.keys(core).length === 0) add("warning", where, `${path}.agentcore`, "an empty agentcore block reads like a setting and does nothing — remove it");
      for (const [feature, cfg] of Object.entries(core)) {
        if (isObj(cfg) && Object.keys(cfg).length === 1 && cfg.enabled === false) {
          add("warning", where, `${path}.agentcore.${feature}`, `\`{"enabled": false}\` is the same as leaving ${feature} out — remove it`);
        }
      }
      if (isObj(core.evaluations) && core.evaluations.enabled === true) evaluated += 1;
      checkFeatures(core, `${path}.agentcore`, where, add, Object.keys(named(wf, "evaluators")));
    }
  }
  if (Object.keys(agents).length && local === 0) {
    add("error", W, "agents", "at least one agent must run here (runtime main or dedicated) — a workflow of only remote agents has nothing to deploy");
  }
  if (Object.keys(agents).length && evaluated === 0) {
    add("warning", W, "agents", "no agent has evaluations enabled; the test suite requires at least one (agentcore.evaluations.enabled)");
  }

  // --- steps -------------------------------------------------------------------
  if (steps.length === 0) add("error", W, "steps", "a workflow needs at least one stage — drag an agent onto the canvas");
  const seen = new Map<string, number>();
  steps.forEach((step, i) => {
    const where: Where = { kind: "step", index: i };
    const path = `steps[${i}]`;
    checkEntry("step", step, path, where, out);
    if (!isObj(step)) return;
    for (const list of ["parallel", "sequence"] as const) {
      if (Array.isArray(step[list]) && step[list]!.length === 0) add("error", where, `${path}.${list}`, "must name at least one agent");
    }
    for (const id of stepAgents(step)) {
      if (!(id in agents)) add("error", where, path, `"${id}" is not a defined agent`);
      if (seen.has(id)) add("error", where, path, `"${id}" already runs in stage ${seen.get(id)! + 1} — an agent can appear in one stage only`);
      else seen.set(id, i);
    }
    if (step.gateId && step.gateId in agents) add("error", where, `${path}.gateId`, `"${step.gateId}" is also an agent id — pick a different gate id`);
    checkBranch(steps, i, out);
    // graph_builder.rerun_plan cannot rewind across an ungated parallel stage.
    const prev = steps[i - 1];
    if (prev?.parallel && !prev.hitl && step.hitl) {
      add("warning", where, path, "the stage before this is an ungated parallel group, so a reviewer here cannot send "
        + "work back past it (re-run cannot rewind across it). Gate that group, or accept the limit.");
    }
  });
  const names = steps.map(stepName);
  if (steps.some((s) => s.branch)) {
    names.forEach((n, i) => {
      if (names.indexOf(n) !== i) add("error", { kind: "step", index: i }, `steps[${i}]`, `two stages are both named "${n}", so a branch target is ambiguous — give one a gateId`);
    });
  }
  for (const id of Object.keys(agents)) {
    if (!seen.has(id)) add("error", { kind: "agent", id }, `agents.${id}`, "not in any stage — drag it onto the canvas, or delete it. The deploy rejects an agent that never runs.");
  }
  return out;
}

export function issuesFor(issues: Issue[], where: Where): Issue[] {
  return issues.filter((x) => JSON.stringify(x.where) === JSON.stringify(where));
}
