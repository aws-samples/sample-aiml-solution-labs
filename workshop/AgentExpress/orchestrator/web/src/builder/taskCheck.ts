/** A runtime "task" step's own checks: a step with no model that calls a tool, runs a
 *  script in the Code Interpreter sandbox, reads the run's documents with Textract, or
 *  writes a file (app/common/task_agent.py). The generic key checks (keys.json) say that
 *  `task` is required and an object; these say what each kind needs, and that every
 *  placeholder names something that has run by then. Mirrors bff/task_check.py word for
 *  word (both run tests/fixtures/validation_cases.json). */
import type { Issue, Where } from "./validate";
import type { StepSpec } from "./model";
import { codeSourceProblems } from "./codeSource";

type Add = (severity: Issue["severity"], where: Where, path: string, message: string) => void;
const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);

/** The keys each kind reads (app/common/task_agent.KEYS_OF). */
export const KEYS_OF: Record<string, string[]> = {
  tool: ["kind", "tool", "arguments"], code: ["kind", "language", "code", "codeSource"],
  extract: ["kind", "file", "features"], output: ["kind", "format", "name", "from"],
  // v139 Track C deliver
  deliver: ["kind", "targets"],
};
const ALL_KEYS = new Set(Object.values(KEYS_OF).flat());
export const MAX_CODE_BYTES = 50_000;
const PLACEHOLDER = /\{\{\s*([^{}]*?)\s*\}\}/g;
const REF = /^(request|session|outputs\.[A-Za-z][A-Za-z0-9_]*(\.[A-Za-z0-9_-]+)*)$/;
const FROM_RE = /^[A-Za-z][A-Za-z0-9_]*(\.[A-Za-z0-9_-]+)*$/;

const notOneOf = (v: unknown, allowed: string[]) =>
  typeof v === "string" ? `"${v}" is not one of: ${allowed.join(", ")}` : `must be one of: ${allowed.join(", ")}`;

/** Every placeholder reference in a value (strings inside lists and objects too). */
export function taskRefs(value: unknown): string[] {
  if (typeof value === "string") return [...value.matchAll(PLACEHOLDER)].map((m) => m[1]);
  if (Array.isArray(value)) return value.flatMap(taskRefs);
  if (isObj(value)) return Object.values(value).flatMap(taskRefs);
  return [];
}

function checkRefs(value: unknown, before: string[], where: Where, p: string, add: Add): void {
  for (const ref of taskRefs(value)) {
    if (!REF.test(ref)) {
      add("error", where, p, `{{${ref}}} is not a placeholder this step knows: use {{request}}, {{session}} or {{outputs.<agent>.<field>}}`);
    } else if (ref.startsWith("outputs.")) {
      const src = ref.split(".")[1];
      if (!before.includes(src)) add("error", where, p, `{{${ref}}}: "${src}" does not run before this step`);
    }
  }
}

export function checkTasks(agents: Record<string, unknown>, tools: Record<string, unknown>, steps: StepSpec[],
  orch: unknown, add: Add, vocab: (name: string) => string[], upstreamIds: (steps: StepSpec[], id: string) => string[]): void {
  const kinds = vocab("taskKinds");
  for (const [aid, a] of Object.entries(agents)) {
    if (!isObj(a) || String(a.runtime ?? "").toLowerCase() !== "task") continue;
    const task = a.task;
    if (!isObj(task)) continue;   // required, and an object: the generic checks say so
    const where: Where = { kind: "agent", id: aid };
    const path = `agents.${aid}`;
    const tp = `${path}.task`;
    const kind = task.kind;
    if (kind === undefined) {
      add("error", where, `${tp}.kind`, `say what this step does: one of ${kinds.join(", ")}`);
      continue;
    }
    if (typeof kind !== "string" || !kinds.includes(kind)) {
      add("error", where, `${tp}.kind`, notOneOf(kind, kinds));
      continue;
    }
    for (const k of Object.keys(task)) {
      // A key no kind reads is the generic checks' "unknown key"; this is the one that
      // belongs to another kind.
      if (ALL_KEYS.has(k) && !KEYS_OF[kind].includes(k)) add("error", where, `${tp}.${k}`, `does not apply to a "${kind}" step`);
    }
    const before = upstreamIds(steps, aid);
    const core = isObj(a.agentcore) ? a.agentcore : {};
    for (const feature of Object.keys(core)) {
      if (feature !== "guardrails") {
        add("error", where, `${path}.agentcore.${feature}`, `a task step makes no model call, so ${feature} has nothing to work on — remove it`);
      }
    }
    if (kind === "tool") {
      const key = task.tool;
      if (typeof key !== "string" || !key) add("error", where, `${tp}.tool`, "pick the tool this step calls");
      else if (!isObj(tools[key])) {
        add("error", where, `${tp}.tool`, `"${key}" is not a tool. Declare it under Tools, or pick one of: ${Object.keys(tools).join(", ") || "(none yet)"}`);
      }
      checkRefs(task.arguments, before, where, `${tp}.arguments`, add);
    } else if (kind === "code") {
      const code = task.code;
      if ("codeSource" in task) {
        // The script is fetched at deploy time (deployer/code_fetch.py read_script).
        if ("code" in task) add("error", where, `${tp}.codeSource`, "use one of code (written or uploaded here) or codeSource, not both");
        for (const c of codeSourceProblems(task.codeSource, `${tp}.codeSource`)) add("error", where, c.path, c.message);
      } else if (typeof code !== "string" || !code.trim()) add("error", where, `${tp}.code`, "write the script this step runs");
      else {
        const size = new TextEncoder().encode(code).length;
        if (size > MAX_CODE_BYTES) add("error", where, `${tp}.code`, `is ${size} bytes; a script may be at most ${MAX_CODE_BYTES}`);
      }
      const lang = task.language;
      if (lang !== undefined && !(typeof lang === "string" && vocab("taskLanguages").includes(lang))) {
        add("error", where, `${tp}.language`, notOneOf(lang, vocab("taskLanguages")));
      }
    } else if (kind === "extract") {
      if (a.attachments !== true) add("error", where, `${path}.attachments`, "an extract step reads the run's files: turn on attachments");
      const feats = task.features;
      if (Array.isArray(feats)) {
        if (!feats.length) {
          add("error", where, `${tp}.features`, `name at least one of ${vocab("taskExtractFeatures").join(", ")}, or leave it out for both`);
        }
        feats.forEach((f, i) => {
          if (!(typeof f === "string" && vocab("taskExtractFeatures").includes(f))) {
            add("error", where, `${tp}.features[${i}]`, notOneOf(f, vocab("taskExtractFeatures")));
          }
        });
      }
    } else if (kind === "output") {
      const fmt = task.format;
      if (fmt === undefined) add("error", where, `${tp}.format`, `pick the file format: ${vocab("taskOutputFormats").join(", ")}`);
      else if (!(typeof fmt === "string" && vocab("taskOutputFormats").includes(fmt))) {
        add("error", where, `${tp}.format`, notOneOf(fmt, vocab("taskOutputFormats")));
      }
      if (!(isObj(orch) && isObj(orch.files))) {
        add("error", where, tp, "an output step saves a file, which needs orchestrator.files so the run keeps it");
      }
      const src = task.from;
      (Array.isArray(src) ? src : []).forEach((ref, i) => {
        if (typeof ref !== "string" || !FROM_RE.test(ref)) add("error", where, `${tp}.from[${i}]`, "must be an agent id, or <agent>.<field>");
        else if (!before.includes(ref.split(".")[0])) add("error", where, `${tp}.from[${i}]`, `"${ref.split(".")[0]}" does not run before this step`);
      });
      checkRefs(task.name, before, where, `${tp}.name`, add);
    } else if (kind === "deliver") {
      checkDeliver(task, before, orch, where, tp, add, vocab);
    }
  }
}

// --- begin v139 Track C deliver ------------------------------------------------------------
// A deliver step (app/common/deliver.py): where the run's files go. Mirrors _check_deliver in
// bff/task_check.py word for word.
export const MAX_TARGETS = 10;
export const MAX_RECIPIENTS = 50;
export const LINK_MAX_MINUTES = 7 * 24 * 60;
const COMMON_TARGET_KEYS = ["type", "files", "producedBy", "when"];
/** The keys each target type reads (app/common/deliver.TARGET_KEYS). */
export const TARGET_KEYS: Record<string, string[]> = {
  s3: [...COMMON_TARGET_KEYS, "uri"],
  email: [...COMMON_TARGET_KEYS, "identityArn", "fromAddress", "to", "subject", "body", "attach",
    "recipientsFrom", "linkExpiresMinutes"],
  webhook: [...COMMON_TARGET_KEYS, "url", "secret", "linkExpiresMinutes"],
};
const ALL_TARGET_KEYS = new Set(Object.values(TARGET_KEYS).flat());
/** branch.when's operators (app/common/branching.py _OPS). */
export const WHEN_OPS = ["equals", "notEquals", "in", "contains", "exists", "gt", "gte", "lt", "lte"];
const S3_URI_RE = /^(s3:\/\/[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]|\$\{storage\})\/([^*?]+\/)?$/;
const FRAMEWORK_BUCKET_RE = /^s3:\/\/(agentcore-|[^/]*builderbuildsbucket)/;
export const IDENTITY_ARN_RE = /^arn:aws[a-z-]*:ses:[a-z0-9-]+:[0-9]{12}:identity\/([^/\s]+)$/;
export const EMAIL_RE = /^[^@\s,;<>"]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+$/;
const ONE_OUTPUT_RE = /^\{\{\s*(outputs\.[A-Za-z][A-Za-z0-9_]*(\.[A-Za-z0-9_-]+)*)\s*\}\}$/;
const SECRET_NAME_RE = /^[A-Za-z0-9_.-]{1,128}$/;
const HTTPS_RE = /^https:\/\/[^/@\s]+(\/\S*)?$/;
const isNum = (v: unknown) => typeof v === "number";
const isWhole = (v: unknown) => typeof v === "number" && Number.isInteger(v);

function checkWhen(when: unknown, before: string[], where: Where, p: string, add: Add): void {
  if (!isObj(when)) {
    add("error", where, p, 'must be an object: {"field": "outputs.<step>.<field>", "equals": ...}');
    return;
  }
  for (const k of Object.keys(when)) {
    if (k !== "field" && !WHEN_OPS.includes(k)) add("error", where, `${p}.${k}`, `unknown key; a condition takes field and one of: ${WHEN_OPS.join(", ")}`);
  }
  const field = when.field;
  if (typeof field !== "string" || !REF.test(field)) add("error", where, `${p}.field`, "name what to test: request, or outputs.<step>.<field>");
  else if (field.startsWith("outputs.") && !before.includes(field.split(".")[1])) {
    add("error", where, `${p}.field`, `"${field.split(".")[1]}" does not run before this step`);
  }
  if (!WHEN_OPS.some((op) => op in when)) add("error", where, p, `needs one comparison: ${WHEN_OPS.join(", ")}`);
  if ("in" in when && !Array.isArray(when.in)) add("error", where, `${p}.in`, "takes a list of alternatives");
  if ("exists" in when && typeof when.exists !== "boolean") add("error", where, `${p}.exists`, "takes true or false");
  for (const op of ["gt", "gte", "lt", "lte"]) {
    if (op in when && !isNum(when[op])) add("error", where, `${p}.${op}`, "takes a number");
  }
}

function checkLinks(t: Record<string, unknown>, where: Where, p: string, add: Add): void {
  const m = t.linkExpiresMinutes;
  if ("linkExpiresMinutes" in t && !(isWhole(m) && (m as number) >= 1 && (m as number) <= LINK_MAX_MINUTES)) {
    add("error", where, `${p}.linkExpiresMinutes`, `must be a whole number of minutes, 1 to ${LINK_MAX_MINUTES}`);
  }
}

function checkEmail(t: Record<string, unknown>, before: string[], where: Where, p: string, add: Add): void {
  const arn = t.identityArn;
  const m = typeof arn === "string" ? IDENTITY_ARN_RE.exec(arn) : null;
  if (!("identityArn" in t)) {
    add("error", where, `${p}.identityArn`, "pick the SES identity it is sent from: arn:aws:ses:<region>:<account>:identity/<domain or address>");
  } else if (!m) {
    add("error", where, `${p}.identityArn`, "must be an SES identity ARN: arn:aws:ses:<region>:<account>:identity/<domain or address>");
  }
  const sender = t.fromAddress;
  if (!("fromAddress" in t)) add("error", where, `${p}.fromAddress`, "write the From address, at the identity's domain");
  else if (!(typeof sender === "string" && EMAIL_RE.test(sender))) add("error", where, `${p}.fromAddress`, "must be an email address");
  else if (m) {
    const ident = m[1].toLowerCase();
    if (ident.includes("@") && sender.toLowerCase() !== ident) {
      add("error", where, `${p}.fromAddress`, `must be the identity's own address, ${m[1]}`);
    } else if (!ident.includes("@") && sender.toLowerCase().split("@").pop() !== ident) {
      add("error", where, `${p}.fromAddress`, `must be an address at ${m[1]}, the identity's domain`);
    }
  }
  const to = t.to;
  if (!Array.isArray(to) || !to.length) add("error", where, `${p}.to`, "write at least one recipient");
  else {
    if (to.length > MAX_RECIPIENTS) add("error", where, `${p}.to`, `at most ${MAX_RECIPIENTS} recipients (SES's limit per message)`);
    to.forEach((a, j) => {
      if (typeof a === "string" && /\{\{\s*([^{}]*?)\s*\}\}/.test(a)) {
        add("error", where, `${p}.to[${j}]`, "recipients are written here, not taken from a step's output: use recipientsFrom for that");
      } else if (!(typeof a === "string" && EMAIL_RE.test(a))) add("error", where, `${p}.to[${j}]`, "must be an email address");
    });
  }
  for (const k of ["subject", "body"]) {
    if (k in t && typeof t[k] !== "string") add("error", where, `${p}.${k}`, "must be a string");
    else checkRefs(t[k], before, where, `${p}.${k}`, add);
  }
  if ("attach" in t && typeof t.attach !== "boolean") add("error", where, `${p}.attach`, "must be true or false");
  if ("recipientsFrom" in t) {
    const rf = t.recipientsFrom;
    const one = typeof rf === "string" ? ONE_OUTPUT_RE.exec(rf) : null;
    if (!one) add("error", where, `${p}.recipientsFrom`, "must be one placeholder, {{outputs.<step>.<field>}}");
    else if (!before.includes(one[1].split(".")[1])) {
      add("error", where, `${p}.recipientsFrom`, `"${one[1].split(".")[1]}" does not run before this step`);
    }
  }
  checkLinks(t, where, p, add);
}

export function checkDeliver(task: Record<string, unknown>, before: string[], orch: unknown, where: Where, tp: string,
  add: Add, vocab: (name: string) => string[]): void {
  if (!(isObj(orch) && isObj(orch.files))) {
    add("error", where, tp, "a deliver step sends the run's files, which needs orchestrator.files so the run keeps them");
  }
  const targets = task.targets;
  if (!("targets" in task) || (Array.isArray(targets) && !targets.length)) {
    add("error", where, `${tp}.targets`, "add at least one target: where to send the files");
    return;
  }
  if (!Array.isArray(targets)) return;   // must be an array: the generic checks say so
  if (targets.length > MAX_TARGETS) add("error", where, `${tp}.targets`, `at most ${MAX_TARGETS} targets`);
  const types = vocab("deliverTargetTypes");
  targets.forEach((t, i) => {
    const p = `${tp}.targets[${i}]`;
    if (!isObj(t)) { add("error", where, p, "must be an object"); return; }
    const kind = t.type;
    if (!("type" in t)) { add("error", where, `${p}.type`, `pick where it goes: one of ${types.join(", ")}`); return; }
    if (typeof kind !== "string" || !types.includes(kind)) { add("error", where, `${p}.type`, notOneOf(kind, types)); return; }
    for (const k of Object.keys(t)) {
      if (ALL_TARGET_KEYS.has(k) && !TARGET_KEYS[kind].includes(k)) add("error", where, `${p}.${k}`, `does not apply to type "${kind}"`);
      else if (!ALL_TARGET_KEYS.has(k)) add("error", where, `${p}.${k}`, `unknown key; type "${kind}" takes: ${TARGET_KEYS[kind].join(", ")}`);
    }
    const files = t.files;
    if ("files" in t && !Array.isArray(files)) add("error", where, `${p}.files`, "must be a list of file-name patterns, e.g. *.pdf");
    (Array.isArray(files) ? files : []).forEach((f, j) => {
      if (typeof f !== "string" || !f.trim() || f.includes("/") || f.includes("\\")) {
        add("error", where, `${p}.files[${j}]`, "must be a file-name pattern, e.g. *.pdf (no folder)");
      }
    });
    const by = t.producedBy;
    if ("producedBy" in t && !Array.isArray(by)) add("error", where, `${p}.producedBy`, "must be a list of step ids");
    (Array.isArray(by) ? by : []).forEach((s, j) => {
      if (typeof s !== "string" || !before.includes(s)) {
        add("error", where, `${p}.producedBy[${j}]`, typeof s === "string" ? `"${s}" does not run before this step` : "must be a step id");
      }
    });
    if ("when" in t) checkWhen(t.when, before, where, `${p}.when`, add);
    if (kind === "s3") {
      const uri = t.uri;
      if (!("uri" in t)) add("error", where, `${p}.uri`, "write the bucket and prefix: s3://<bucket>/<prefix>/ or ${storage}/<prefix>/");
      else if (!(typeof uri === "string" && S3_URI_RE.test(uri))) {
        add("error", where, `${p}.uri`, "must be s3://<bucket>/ or s3://<bucket>/<prefix>/, ending in /, or ${storage}/<prefix>/");
      } else if (FRAMEWORK_BUCKET_RE.test(uri)) add("error", where, `${p}.uri`, "is one of the framework's own buckets, which no delivery may write to");
    } else if (kind === "email") {
      checkEmail(t, before, where, p, add);
    } else if (kind === "webhook") {
      const url = t.url;
      if (!("url" in t)) add("error", where, `${p}.url`, "write the https URL to post to");
      else if (!(typeof url === "string" && HTTPS_RE.test(url))) add("error", where, `${p}.url`, "must be an https:// URL, with no user:password@");
      const sec = t.secret;
      if ("secret" in t && !(typeof sec === "string" && SECRET_NAME_RE.test(sec))) {
        add("error", where, `${p}.secret`, "must be the name of a deliverSecrets build secret (letters, digits, . _ -), never the secret itself");
      }
      checkLinks(t, where, p, add);
    }
  });
}
// --- end v139 Track C deliver --------------------------------------------------------------
