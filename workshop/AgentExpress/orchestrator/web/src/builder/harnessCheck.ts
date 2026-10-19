/** A runtime "harness" agent's own checks: an agent with no code, run by a managed AgentCore
 *  Harness built from its `instructions`, model, `tool` and `harness` block
 *  (app/common/harness_agent.py), or one made elsewhere (`harness.arn`). The generic key
 *  checks (keys.json) say which keys apply and their types; these are the rest, in the
 *  shapes CreateHarness accepts. Mirrors bff/harness_check.py word for word (both run
 *  tests/fixtures/validation_cases.json). */
import type { Issue, Where } from "./validate";

type Add = (severity: Issue["severity"], where: Where, path: string, message: string) => void;
type Obj = Record<string, unknown>;
const isObj = (v: unknown): v is Obj => typeof v === "object" && v !== null && !Array.isArray(v);

/** Bounds of the `harness` block (app/common/harness_agent.LIMITS). */
export const HARNESS_LIMITS: Record<string, [number, number]> = { maxIterations: [1, 100], timeoutSeconds: [1, 900] };
export const HARNESS_PROVIDERS = ["bedrock", "openai", "gemini", "litellm"];
const BYO_KEYS = ["arn", "endpoint", "maxIterations", "timeoutSeconds"];
const BYO_NOT = ["instructions", "model", "temperature", "topP", "maxTokens", "tool"];
const PERSON_AUTH = ["user", "obo"];
const RESERVED_TOOLS = ["gateway", "browser", "code_interpreter"];
const MAX_MCP = 10, MAX_SKILLS = 20, MAX_MOUNTS = 5;

// botocore's patterns (bedrock-agentcore-control CreateHarness), anchored.
export const HARNESS_ARN_RE = /^arn:aws[a-z-]*:bedrock-agentcore:[a-z0-9-]+:[0-9]{12}:harness\/[a-zA-Z][a-zA-Z0-9_]{0,39}-[a-zA-Z0-9]{10}$/;
const API_KEY_ARN_RE = /^arn:aws:bedrock-agentcore:[a-z0-9-]+:[0-9]{12}:token-vault\/[a-zA-Z0-9-.]+\/apikeycredentialprovider\/[a-zA-Z0-9-.]+$/;
const BROWSER_ARN_RE = /^arn:aws(-[^:]+)?:bedrock-agentcore:[a-z0-9-]+:(aws|[0-9]{12}):browser(-custom)?\/(aws\.browser\.v1|[a-zA-Z][a-zA-Z0-9_]{0,47}-[a-zA-Z0-9]{10})$/;
const CODE_ARN_RE = /^arn:aws(-[^:]+)?:bedrock-agentcore:[a-z0-9-]+:(aws|[0-9]{12}):code-interpreter(-custom)?\/(aws\.codeinterpreter\.v1|[a-zA-Z][a-zA-Z0-9_]{0,47}-[a-zA-Z0-9]{10})$/;
const CONTAINER_RE = /^(([0-9]{12})\.dkr\.ecr\.([a-z0-9-]+)\.amazonaws\.com|public\.ecr\.aws)\/((?:[a-z0-9]+(?:[._-][a-z0-9]+)*\/)*[a-z0-9]+(?:[._-][a-z0-9]+)*)(?::([^:@]{1,300}))?(?:@(.+))?$/;
const EFS_ARN_RE = /^arn:aws[-a-z]*:elasticfilesystem:[0-9a-z-]+:[0-9]{12}:access-point\/fsap-[0-9a-f]{8,40}$/;
const S3FILES_ARN_RE = /^arn:aws[-a-z]*:s3files:[0-9a-z-]+:[0-9]{12}:file-system\/fs-[0-9a-f]{17,40}\/access-point\/fsap-[0-9a-f]{17,40}$/;
const MOUNT_RE = /^\/mnt\/[a-zA-Z0-9._-]+\/?$/;
const ENDPOINT_RE = /^[a-zA-Z][a-zA-Z0-9_]{0,47}$/;
const TOOL_NAME_RE = /^[a-zA-Z0-9_-]{1,64}$/;
const SUBNET_RE = /^subnet-[0-9a-zA-Z]{8,17}$/;
const SG_RE = /^sg-[0-9a-zA-Z]{8,17}$/;
const S3_URI_RE = /^s3:\/\/[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]\/\S*$/;

/** IPv4 ranges bff/netguard.blocked_address refuses: loopback, link-local, private,
 *  unspecified, multicast, reserved, CGNAT and the documentation / benchmark nets. */
const BLOCKED_V4: [string, number][] = [["0.0.0.0", 8], ["10.0.0.0", 8], ["100.64.0.0", 10], ["127.0.0.0", 8],
  ["169.254.0.0", 16], ["172.16.0.0", 12], ["192.0.0.0", 24], ["192.0.2.0", 24], ["192.88.99.0", 24], ["192.168.0.0", 16],
  ["198.18.0.0", 15], ["198.51.100.0", 24], ["203.0.113.0", 24], ["224.0.0.0", 4], ["240.0.0.0", 4]];
const v4 = (s: string): number | null => {
  const p = s.split(".");
  if (p.length !== 4 || p.some((x) => !/^\d{1,3}$/.test(x) || Number(x) > 255)) return null;
  return p.reduce((n, x) => n * 256 + Number(x), 0);
};
function blockedV4(host: string): boolean {
  const ip = v4(host);
  if (ip === null) return true;
  return BLOCKED_V4.some(([base, bits]) => Math.floor(ip / 2 ** (32 - bits)) === Math.floor(v4(base)! / 2 ** (32 - bits)));
}

/** An https:// URL, no user:password@, on a host that is not this machine or a private
 *  network (bff/harness_check.public_https). Names are not resolved here. */
export function publicHttps(url: unknown): boolean {
  const m = /^https:\/\/([^/?#]+)/.exec(String(url ?? ""));
  if (!m || m[1].includes("@")) return false;
  let host = m[1];
  host = host.startsWith("[") && host.includes("]") ? host.slice(1, host.indexOf("]")) : host.includes(":") ? host.slice(0, host.lastIndexOf(":")) : host;
  host = host.toLowerCase().replace(/\.+$/, "");
  if (!host || host === "localhost" || [".localhost", ".local", ".internal"].some((s) => host.endsWith(s)) || host.includes(":")) return false;
  if (/^[0-9.]+$/.test(host)) return !blockedV4(host);
  return host.includes(".");
}

function apiKey(value: string, identities: Obj, add: Add, where: Where, p: string): void {
  if (value.startsWith("arn:")) {
    if (!API_KEY_ARN_RE.test(value)) {
      add("error", where, p, "must be an API key credential provider ARN "
        + "(arn:aws:bedrock-agentcore:<region>:<account>:token-vault/default/apikeycredentialprovider/<name>)");
    }
  } else {
    const ident = identities[value];
    if (!(isObj(ident) && String(ident.type ?? "").toLowerCase() === "apikey")) {
      add("error", where, p, `"${value}" is not an apikey identity. Declare it under \`identities\` `
        + "(type apikey, its key in the build's secrets), or give a credential provider ARN");
    }
  }
}

export function checkHarness(agents: Record<string, unknown>, add: Add, wf: Obj = {}): void {
  const tools = isObj(wf.tools) ? wf.tools : {};
  const identities = isObj(wf.identities) ? wf.identities : {};
  const memories = isObj(wf.memories) ? wf.memories : {};
  for (const [aid, a] of Object.entries(agents)) {
    if (!isObj(a) || String(a.runtime ?? "").toLowerCase() !== "harness") continue;
    const where: Where = { kind: "agent", id: aid };
    const path = `agents.${aid}`;
    const block: Obj = isObj(a.harness) ? a.harness : {};
    const hp = `${path}.harness`;
    const brought = "arn" in block;
    if (typeof a.instructions === "string" && !a.instructions.trim()) {
      add("error", where, `${path}.instructions`, "write the system prompt the harness is created with");
    } else if (!("instructions" in a) && !brought) {
      add("error", where, `${path}.instructions`, '`instructions` is required for runtime "harness"');
    }
    for (const [key, [low, high]] of Object.entries(HARNESS_LIMITS)) {
      const v = block[key];
      if (typeof v === "number" && Number.isInteger(v) && !(v >= low && v <= high)) {
        add("error", where, `${hp}.${key}`, `must be a whole number from ${low} to ${high}`);
      }
    }
    if (block.shell === true) {
      add("warning", where, `${hp}.shell`, "the agent can run shell commands and reach the internet from its own session, "
        + "with the request and every earlier step's output in hand");
    }
    const core = a.agentcore;
    if (isObj(core) && "identity" in core) {
      add("error", where, `${path}.agentcore.identity`, "a harness agent calls no API with this build's identities — remove it");
    }
    if ("hooks" in block) {
      add("error", where, `${hp}.hooks`, "lifecycle hooks cannot be deployed yet: neither CloudFormation "
        + "(AWS::BedrockAgentCore::Harness) nor the Terraform provider can set them on a harness");
    }

    if (brought) {
      if (!HARNESS_ARN_RE.test(String(block.arn))) {
        add("error", where, `${hp}.arn`, "must be a harness ARN: arn:aws:bedrock-agentcore:<region>:<account>:harness/<name>-<id>");
      }
      for (const k of BYO_NOT) {
        if (k in a) add("error", where, `${path}.${k}`, "is set on the harness you brought (harness.arn) — remove it");
      }
      for (const k of Object.keys(block)) {
        if (!BYO_KEYS.includes(k) && k !== "hooks") add("error", where, `${hp}.${k}`, "is set on the harness you brought (harness.arn) — remove it");
      }
    }
    const ep = block.endpoint;
    if (typeof ep === "string" && (!ENDPOINT_RE.test(ep) || ep === "DEFAULT")) {
      add("error", where, `${hp}.endpoint`, "1-48 letters, digits and underscores, starting with a letter, not DEFAULT");
    }
    if (brought) continue;

    // --- the model
    const provider = "provider" in block ? block.provider : "bedrock";
    if ("provider" in block && !HARNESS_PROVIDERS.includes(String(provider))) {
      add("error", where, `${hp}.provider`, `must be one of: ${HARNESS_PROVIDERS.join(", ")}`);
    }
    const model = a.model;
    if (typeof model === "string" && model.startsWith("litellm/")) {
      add("error", where, `${path}.model`, "a harness calls its model itself, not through this deployment's "
        + 'LiteLLM proxy: set harness.provider "litellm" and the provider\'s model id (e.g. gemini/gemini-2.5-pro)');
    }
    if (["openai", "gemini", "litellm"].includes(String(provider)) && !(typeof model === "string" && model.trim())) {
      add("error", where, `${path}.model`, `harness.provider "${String(provider)}" needs \`model\`: the provider's model id`);
    }
    if ("apiKey" in block) {
      if (provider === "bedrock") add("error", where, `${hp}.apiKey`, "a Bedrock model needs no API key — remove it");
      else if (typeof block.apiKey === "string") apiKey(block.apiKey, identities, add, where, `${hp}.apiKey`);
    } else if (provider === "openai" || provider === "gemini") {
      add("error", where, `${hp}.apiKey`, `harness.provider "${provider}" needs \`apiKey\`: an apikey `
        + "identity or an API key credential provider ARN");
    }
    if ("apiBase" in block) {
      if (provider !== "litellm") add("error", where, `${hp}.apiBase`, 'only for harness.provider "litellm"');
      else if (!publicHttps(block.apiBase)) add("error", where, `${hp}.apiBase`, "must be an https:// URL on a public host");
    }

    // --- tools
    const bound = Array.isArray(a.tool) ? a.tool : a.tool ? [a.tool] : [];
    for (const key of bound) {
      const t = typeof key === "string" ? tools[key] : undefined;
      if (isObj(t) && PERSON_AUTH.includes(String(t.auth ?? "").toLowerCase())) {
        add("error", where, `${path}.tool`, `"${String(key)}" acts as the person who runs it, and a harness `
          + "calls the Gateway as this build's machine client — pick a tool that does not");
      }
      if (typeof key === "string" && `@gateway/${key}___*`.length > 64) {
        add("error", where, `${path}.tool`, `"${key}" is too long for a harness: at most 51 characters`);
      }
    }
    for (const [name, pattern] of [["browser", BROWSER_ARN_RE], ["codeInterpreter", CODE_ARN_RE]] as const) {
      const v = block[name];
      if (typeof v === "string" && !pattern.test(v)) {
        add("error", where, `${hp}.${name}`, `must be true, false or a custom ${name === "browser" ? "browser" : "code interpreter"} ARN`);
      }
    }
    const mcp = Array.isArray(block.mcp) ? block.mcp : [];
    if (mcp.length > MAX_MCP) add("error", where, `${hp}.mcp`, `at most ${MAX_MCP} servers`);
    const seen = new Set<unknown>();
    mcp.forEach((m, i) => {
      const mp = `${hp}.mcp[${i}]`;
      if (!isObj(m)) { add("error", where, mp, 'must be {"name": ..., "url": "https://..."}'); return; }
      for (const k of Object.keys(m)) {
        if (!["name", "url"].includes(k)) add("error", where, `${mp}.${k}`, "unknown key; allowed: name, url");
      }
      const name = m.name;
      if (typeof name !== "string" || !TOOL_NAME_RE.test(name)) add("error", where, `${mp}.name`, "1-64 letters, digits, _ and -");
      else if (RESERVED_TOOLS.includes(name) || seen.has(name)) add("error", where, `${mp}.name`, `"${name}" is taken — pick another name`);
      seen.add(name);
      if (!publicHttps(m.url)) add("error", where, `${mp}.url`, "must be an https:// URL on a public host");
    });
    if (block.shell === true && (bound.length || "apiKey" in block)) {
      add("warning", where, `${hp}.shell`, "shell commands run in the session that holds this agent's "
        + "Gateway token and model API key — turn shell off unless the agent needs it");
    }

    // --- skills
    const skills = Array.isArray(block.skills) ? block.skills : [];
    if (skills.length > MAX_SKILLS) add("error", where, `${hp}.skills`, `at most ${MAX_SKILLS} skills`);
    skills.forEach((s, i) => checkSkill(s, identities, add, where, `${hp}.skills[${i}]`));

    // --- memory
    const mem = block.memory;
    if (typeof mem === "string") {
      if (!isObj(memories[mem])) {
        add("error", where, `${hp}.memory`, `"${mem}" is not one of the build's memories: ${Object.keys(memories).join(", ") || "(none yet)"}`);
      }
      if (isObj(core) && core.memory) {
        add("error", where, `${hp}.memory`, "harness.memory and agentcore.memory would both store this agent's turns — keep one");
      }
    }

    // --- environment
    if (typeof block.container === "string" && !CONTAINER_RE.test(block.container)) {
      add("error", where, `${hp}.container`, "must be an image URI: <account>.dkr.ecr.<region>.amazonaws.com/"
        + "<repo>:<tag> or public.ecr.aws/<repo>:<tag>");
    }
    const net = block.network;
    if (isObj(net)) {
      for (const [k, pattern, what] of [["subnetIds", SUBNET_RE, "subnet-"], ["securityGroupIds", SG_RE, "sg-"]] as const) {
        const v = net[k];
        if (!Array.isArray(v) || !(v.length >= 1 && v.length <= 16) || !v.every((x) => typeof x === "string" && pattern.test(x))) {
          add("error", where, `${hp}.network.${k}`, `1-16 ids, each ${what}...`);
        }
      }
      for (const k of Object.keys(net)) {
        if (!["subnetIds", "securityGroupIds"].includes(k)) add("error", where, `${hp}.network.${k}`, "unknown key; allowed: subnetIds, securityGroupIds");
      }
    }
    const mounts = Array.isArray(block.mounts) ? block.mounts : [];
    if (mounts.length > MAX_MOUNTS) add("error", where, `${hp}.mounts`, `at most ${MAX_MOUNTS} mounts`);
    const paths = new Set<string>();
    mounts.forEach((m, i) => {
      const mp = `${hp}.mounts[${i}]`;
      const kinds = isObj(m) ? ["session", "efs", "s3Files"].filter((k) => k in m) : [];
      if (kinds.length !== 1) { add("error", where, mp, 'set exactly one of "session", "efs", "s3Files"'); return; }
      const mo = m as Obj;
      const kind = kinds[0];
      const mount = kind === "session" ? mo.session : mo.path;
      if (typeof mount !== "string" || !MOUNT_RE.test(mount)) {
        add("error", where, `${mp}.${kind === "session" ? "session" : "path"}`, "must be a path under /mnt, like /mnt/data");
      } else if (paths.has(mount.replace(/\/+$/, ""))) {
        add("error", where, mp, `${mount} is mounted twice`);
      }
      if (typeof mount === "string") paths.add(mount.replace(/\/+$/, ""));
      if (kind !== "session") {
        const pattern = kind === "efs" ? EFS_ARN_RE : S3FILES_ARN_RE;
        if (typeof mo[kind] !== "string" || !pattern.test(String(mo[kind]))) {
          add("error", where, `${mp}.${kind}`, `must be an ${kind === "efs" ? "EFS" : "S3 Files"} access point ARN`);
        }
        if (!isObj(net)) add("error", where, `${mp}.${kind}`, "EFS and S3 Files mounts need harness.network (a VPC)");
      }
    });
  }
}

function checkSkill(s: unknown, identities: Obj, add: Add, where: Where, sp: string): void {
  const kinds = isObj(s) ? ["aws", "git", "s3"].filter((k) => k in s) : [];
  if (kinds.length !== 1) { add("error", where, sp, 'set exactly one of "aws", "git", "s3"'); return; }
  const o = s as Obj;
  const kind = kinds[0];
  const allowed = ({ aws: ["aws"], git: ["git", "path", "credential"], s3: ["s3"] } as Record<string, string[]>)[kind];
  for (const k of Object.keys(o)) {
    if (!allowed.includes(k)) add("error", where, `${sp}.${k}`, `unknown key; allowed: ${allowed.join(", ")}`);
  }
  const v = o[kind];
  if (kind === "aws") {
    if (!Array.isArray(v) || !v.every((p) => typeof p === "string" && p && !p.startsWith("/") && !p.includes("..") && !/[?[\]]/.test(p))) {
      add("error", where, `${sp}.aws`, 'a list of catalog paths like "core-skills/*" ([] for all), relative, no ".."');
    }
  } else if (kind === "git") {
    if (typeof v !== "string" || !publicHttps(v) || v.includes("#") || v.includes("@")) {
      add("error", where, `${sp}.git`, "must be the repository's https:// URL on a public host");
    }
    if (typeof o.credential === "string") apiKey(o.credential, identities, add, where, `${sp}.credential`);
  } else if (typeof v !== "string" || !S3_URI_RE.test(v)) {
    add("error", where, `${sp}.s3`, "must be s3://<bucket>/<prefix>/");
  }
}
