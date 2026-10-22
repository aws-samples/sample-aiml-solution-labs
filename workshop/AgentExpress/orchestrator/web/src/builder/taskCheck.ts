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
    }
  }
}
