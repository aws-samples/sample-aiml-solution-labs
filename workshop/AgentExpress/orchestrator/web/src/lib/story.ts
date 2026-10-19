/** The Flow tab's Story view, laid out: a run told top to bottom, one numbered stage after
 *  another, each with the agents in it and one short line per thing that happened. Pure
 *  functions only, so the layout is tested without a browser.
 *
 *  In: the run's events (lib/flow.ts buildEvents) and what the run page already holds about
 *  the run (the workflow it ran with, node statuses, log lines, the gate it waits at, each
 *  agent's version history). Nothing new is read. Out: stages -> agents -> lines, and the
 *  run's totals.
 *
 *  Order. Lines keep the order buildEvents gives (ts, then seq), so a line's number is the
 *  one the replay counter says. A telemetry row's `ts` is stamped when the call FINISHED
 *  and has one-second resolution (app/features/observability/records.py).
 *
 *  Starts, ends and HITL review gates come from the run's log lines (app/orchestrator/
 *  nodes.py: "<name> started", "<name> complete (vN)", "<name> failed: ...", "<name> blocked
 *  by guardrail", "Awaiting human review after ...", "Awaiting per-agent review after ...",
 *  "<Who> decision (<id>): <decision>", "<label> review: <decision>", "<label>: approved
 *  automatically, ..."). Log lines and events share one clock (clock.now_str). The log
 *  lines do not say WHO decided at a gate, so a gate line says the decision only. */
import type { HistoryEntry, LogLine, NodeState, Workflow } from "../types";
import { toEpochMs } from "./clock";
import { FEATURE_EMOJI, toolEmoji } from "./emoji";
import type { FlowEvent } from "./flow";
import { fmtCost, fmtSpan } from "./gantt";
import { stepNumbers } from "./stepNumbers";

/** How a line went. Each tone is also said in the line's words, never by colour alone. */
export type StoryTone = "ok" | "failed" | "denied" | "blocked" | "skipped" | "note" | "waiting";

export interface StoryLine {
  key: string;
  type: "event" | "gate" | "version";
  /** The replay position from which the line is shown: once `at` events have happened. */
  at: number;
  /** Where it falls among all lines and log items, for merging a stage's lines in order. */
  pos: number;
  emoji: string;
  /** The short sentence the line reads. */
  text: string;
  /** For a gate still undecided at the replay position: what it said then, and from when
   *  the decided text applies. */
  pendingText?: string;
  doneAt?: number;
  /** The full words, for the tooltip: the long sentence, a model's full id. */
  title: string;
  /** "1.2 s", "$0.0020", "waited 2 m 10 s". */
  meta: string[];
  tone: StoryTone;
  /** The filter kind: a FlowEvent kind, "person" for HITL. "" for a version header. */
  kind: string;
  /** A failure, a denial or a block: Problems only keeps it, and its agent opens. */
  problem: boolean;
  event?: FlowEvent;
  agent: string;
  version?: number;
}

export interface StoryAgent {
  id: string;
  name: string;
  emoji: string;
  /** "code step", "harness": what kind of step it is, when it is not a plain agent. */
  role: string;
  /** Its number as the Graph view's badge shows it ("2", "2a"); "" for an unlisted agent. */
  num: string;
  status: string;
  /** Time it ran (start to end of each run of it, added up); null when not known. */
  durationMs: number | null;
  costUsd: number;
  calls: number;
  lines: StoryLine[];
  problem: boolean;
}

export interface StoryStage {
  key: string;
  /** The stage's number ("1", "2"); "" for the extra sections (other agents, the run). */
  num: string;
  kind: "single" | "parallel" | "sequence" | "extra";
  title: string;
  agents: StoryAgent[];
  /** Lines that belong to the stage, not to one agent: its review gates. */
  lines: StoryLine[];
  status: string;
  durationMs: number | null;
}

export interface Story {
  stages: StoryStage[];
  totals: { durationMs: number | null; costUsd: number; calls: number; gates: number; waitMs: number };
  /** Kinds present, for the filter chips, in a fixed order. */
  kinds: string[];
}

export interface StoryInput {
  events: FlowEvent[];
  workflow: Workflow;
  nodes?: Record<string, NodeState>;
  logs?: LogLine[];
  /** The gate the run waits at now (the snapshot's `hitl`). */
  hitl?: { node: string; kind?: string } | null;
  history?: Record<string, HistoryEntry[]>;
}

/** Plain names for the filter chips, in the order they are offered. */
export const KIND_LABEL: Record<string, string> = {
  llm: "Model calls", image: "Images", tool: "Tools", policy: "Cedar policy",
  guardrail: "Guardrails", memory: "Memory", eval: "Evaluations", person: "HITL",
  agentcore: "Runtime", other: "Other",
};
const KIND_ORDER = Object.keys(KIND_LABEL);

// --- words ----------------------------------------------------------------------------

export const span = (ms: number) => fmtSpan(ms);
export const money = (n: number) => fmtCost(n);

const CAP = (w: string) => (w ? w.charAt(0).toUpperCase() + w.slice(1) : w);

/** A model id as a person names it: "us.anthropic.claude-haiku-4-5-20251001-v1:0" is
 *  "Claude Haiku 4.5", "amazon.nova-lite-v1:0" is "Nova Lite". Anything it cannot read is
 *  shown without its region, vendor and version tails. The full id stays in the tooltip. */
export function modelName(id: string): string {
  let s = String(id ?? "").trim();
  if (!s) return "the model";
  if (s.startsWith("arn:")) s = s.split("/").pop() ?? s;
  if (s.includes("/")) s = s.split("/").pop() ?? s;          // LiteLLM "openai/gpt-4o"
  s = s.replace(/^(?:us|eu|apac|ap|global|us-gov|ca|jp|au)\./, "");
  const dot = s.indexOf(".");
  if (dot > 0 && /^[a-z0-9-]+$/.test(s.slice(0, dot)) && !/^\d/.test(s.slice(dot + 1))) s = s.slice(dot + 1);
  s = s.replace(/:\d+$/, "").replace(/-v\d+$/, "").replace(/-\d{8}$/, "");
  const parts = s.split("-").filter(Boolean);
  if (parts[0] === "claude") {
    const fam = parts.findIndex((p) => /^(?:haiku|sonnet|opus|instant)$/.test(p));
    const nums = parts.filter((p) => /^\d+$/.test(p)).join(".");
    if (fam < 0) return `Claude${nums ? ` ${nums}` : ""}`;
    const firstNum = parts.findIndex((p) => /^\d+$/.test(p));
    const family = CAP(parts[fam]);
    if (!nums) return `Claude ${family}`;
    return firstNum >= 0 && firstNum < fam ? `Claude ${nums} ${family}` : `Claude ${family} ${nums}`;
  }
  const llama = /^llama(\d+)(?:-(\d+))?-(\d+b)/.exec(s);
  if (llama) return `Llama ${llama[1]}${llama[2] ? `.${llama[2]}` : ""} ${llama[3].toUpperCase()}`;
  if (/^gpt-/.test(s)) return `GPT-${parts.slice(1).join(" ")}`;
  return parts.map((p) => (/^\d+b$/.test(p) ? p.toUpperCase() : CAP(p))).join(" ");
}

/** The run snapshot carries a tool's type only as the words bff/workflow.py writes in a
 *  tool component's `detail` (_TOOL_TYPE_LABEL); read it back. */
const TYPE_OF_DETAIL: [string, string][] = [
  ["Knowledge Base", "kb"], ["MCP server", "mcp"], ["REST API (OpenAPI)", "openapi"],
  ["Lambda function", "lambda"], ["Web search", "websearch"], ["REST API (API Gateway)", "apigateway"],
  ["AWS API (Smithy)", "smithy"],
];
const TYPE_WORDS: Record<string, string> = {
  kb: "Knowledge Base", mcp: "MCP server", openapi: "REST API", lambda: "Lambda function",
  websearch: "web search", apigateway: "REST API", smithy: "AWS API",
};
function toolType(workflow: Workflow, key: string): string | undefined {
  for (const a of Object.values(workflow.agents ?? {})) {
    const c = (a.components ?? []).find((x) => x.kind === "tool" && (x.tool ?? x.name) === key);
    if (c?.detail) return TYPE_OF_DETAIL.find(([words]) => c.detail!.startsWith(words))?.[1];
  }
  return undefined;
}
/** A Cedar policy's name, when the agent's components name one on that tool. */
function policyName(workflow: Workflow, agent: string, tool: string): string {
  const c = (workflow.agents?.[agent]?.components ?? []).find((x) => x.kind === "policy" && x.tool === tool);
  return c?.name ?? "";
}

const toolKey = (label: string) => label.split("___")[0] || "a tool";
const toolOp = (label: string) => (label.includes("___") ? label.split("___").slice(1).join("___") : "");

function toneOf(e: FlowEvent): StoryTone {
  if (e.tone === "bad") return "failed";
  if (e.tone === "stop") return e.status === "denied" ? "denied" : "blocked";
  return e.tone === "note" ? "note" : "ok";
}

/** The line for one event: emoji and a short sentence. The long sentence (lib/flow.ts)
 *  stays the tooltip and the detail. */
export function eventWords(e: FlowEvent, workflow: Workflow): { emoji: string; text: string; title: string } {
  const failed = e.status === "error";
  const plain = (emoji: string, text: string, title = e.sentence) => ({ emoji, text, title });
  switch (e.kind) {
    case "llm": {
      const m = modelName(e.label);
      return plain("💬", failed ? `Call to ${m} failed` : `Asked ${m}`, `${e.sentence} (${e.label || "model"})`);
    }
    case "image": {
      const m = modelName(e.label);
      return plain("🖼️", failed ? `Image from ${m} failed` : `Drew an image with ${m}`, `${e.sentence} (${e.label || "model"})`);
    }
    case "tool": {
      const key = toolKey(e.label);
      const op = toolOp(e.label);
      // A task step's own calls (app/common/task_agent.py _record): labelled by service.
      if (key === "codeInterpreter") {
        return plain(toolEmoji({ source: "codeInterpreter" }), failed ? "Code in Code Interpreter failed" : "Ran code in Code Interpreter");
      }
      if (key === "textract") return plain("📄", failed ? "Textract could not read the document" : "Read a document with Textract");
      const type = toolType(workflow, key);
      const emoji = toolEmoji({ type });
      if (failed) return plain(emoji, `Call to ${key}${op ? ` (${op})` : ""} failed`);
      if (type === "kb") return plain(emoji, `Searched ${key} (Knowledge Base)`);
      if (type === "websearch") return plain(emoji, `Searched the web with ${key}`);
      const what = type ? ` (${TYPE_WORDS[type]})` : "";
      return plain(emoji, `Called ${key}${op ? ` · ${op}` : ""}${what}`);
    }
    case "policy": {
      const tool = toolKey(e.label);
      const name = policyName(workflow, e.agent, tool);
      const who = name ? `Cedar policy ${name}` : `Cedar policy on ${tool}`;
      const word = e.status === "allowed" ? "ALLOW" : e.status === "denied" ? "DENY, call refused"
        : e.status === "log-only" ? "logged, not enforced" : (e.status || "decided");
      return plain(FEATURE_EMOJI.policy, `${who}: ${word}`);
    }
    case "guardrail": {
      const side = e.label.toUpperCase() === "OUTPUT" ? "output" : e.label.toUpperCase() === "INPUT" ? "input" : "text";
      const word = e.status === "blocked" ? "blocked" : e.status === "anonymized" ? "masked sensitive details"
        : e.status === "passed" ? "passed" : (e.status || "checked");
      return plain(FEATURE_EMOJI.guardrail, `Guardrail checked ${side}: ${word}`);
    }
    case "memory":
      if (e.label === "recall") return plain(FEATURE_EMOJI.memory, "Recalled from memory");
      if (e.label === "store") return plain(FEATURE_EMOJI.memory, "Saved to memory");
      if (e.label === "recall-filtered") return plain(FEATURE_EMOJI.memory, "Dropped off-topic memories");
      return plain(FEATURE_EMOJI.memory, `Used memory${e.label ? ` (${e.label})` : ""}`);
    case "eval": {
      const name = e.label.replace(/^(?:Builtin|Custom)\./, "") || "an evaluator";
      return plain(FEATURE_EMOJI.evaluation, failed ? `Could not score with ${name}` : `Scored by ${name}`);
    }
    case "agentcore":
      return plain("🚀", "AgentCore Runtime was active");
    case "person": {
      const tool = e.label && e.label !== "question" ? e.label : "a call";
      if (e.label === "question") {
        if (e.status === "asked") return plain("⏸", "Asked HITL a question");
        if (e.status === "answered") return plain("⏸", "HITL answered");
        return plain("⏸", "Nobody answered, carried on");
      }
      if (e.status === "waiting") return plain("⏸", `Asked HITL to approve ${tool}`);
      if (e.status === "approved") return plain("⏸", `HITL approved ${tool}`);
      return plain("⏸", `HITL did not approve ${tool}`);
    }
    default:
      return plain("❔", `Recorded an event${e.label ? ` (${e.label})` : ""}`);
  }
}

// --- log lines (moved from the old Sequence view, unchanged in what they read) ----------

export type LogItem =
  | { t: "start"; ts: string; agent: string }
  | { t: "end"; ts: string; agent: string; version: number | null; tone: StoryTone }
  | { t: "gateReq"; ts: string; node: string; auto: boolean }
  | { t: "gateRes"; ts: string; node: string; decision: string };

function decisionWord(raw: string): string {
  const d = raw.trim().toLowerCase();
  if (d === "approve" || d === "approved") return "approved";
  if (d === "deny" || d === "denied") return "denied";
  return d;
}

/** The log lines that say an agent started or ended, or a gate waited or was decided. */
export function parseLogs(logs: LogLine[] | undefined, workflow: Workflow): LogItem[] {
  const out: LogItem[] = [];
  for (const l of logs ?? []) {
    const node = String(l.node ?? "");
    const msg = String(l.msg ?? "");
    const ts = String(l.ts ?? "");
    if (!node) continue;
    const name = String(workflow.agents?.[node]?.name ?? node);
    if (msg === `${name} started`) { out.push({ t: "start", ts, agent: node }); continue; }
    const done = msg.startsWith(`${name} `) ? / complete \(v(\d+)\)$/.exec(msg) : null;
    if (done) { out.push({ t: "end", ts, agent: node, version: Number(done[1]), tone: "ok" }); continue; }
    if (msg.startsWith(`${name} failed: `)) { out.push({ t: "end", ts, agent: node, version: null, tone: "failed" }); continue; }
    if (msg.startsWith(`${name} blocked by guardrail`)) { out.push({ t: "end", ts, agent: node, version: null, tone: "blocked" }); continue; }
    if (msg.startsWith("Awaiting human review after ") || msg.startsWith("Awaiting per-agent review after ")) {
      out.push({ t: "gateReq", ts, node, auto: false }); continue;
    }
    if (msg.includes(": approved automatically, ")) { out.push({ t: "gateReq", ts, node, auto: true }); continue; }
    // A reviewer's note follows " — note: "; the version history carries it, not this line.
    const bare = msg.split(" — note: ")[0];
    const human = /^(?:Human|Automatic) decision \(.+?\): (.+)$/.exec(bare);
    const review = human ? null : /^.+ review: (.+)$/.exec(bare);
    const m = human ?? review;
    if (m) out.push({ t: "gateRes", ts, node, decision: decisionWord(m[1]) });
  }
  return out;
}

type Item = { t: "event"; e: FlowEvent } | LogItem;

/** Events and log lines on one clock. On a tie a start comes first, everything else after
 *  the events of that second (a row is stamped when its call ends). */
function merge(events: FlowEvent[], logs: LogItem[]): Item[] {
  const out: Item[] = [];
  let i = 0;
  let j = 0;
  while (i < events.length || j < logs.length) {
    const e = events[i];
    const l = logs[j];
    if (!l || (e && (e.ts < l.ts || (e.ts === l.ts && l.t !== "start")))) {
      out.push({ t: "event", e });
      i += 1;
    } else {
      out.push(l);
      j += 1;
    }
  }
  return out;
}

// --- layout ---------------------------------------------------------------------------

const ms = (ts: string) => toEpochMs(ts);

/** Total length of a set of intervals, overlaps counted once. */
function unionMs(list: [number, number][]): number {
  const sorted = list.filter(([a, b]) => Number.isFinite(a) && Number.isFinite(b) && b >= a).sort((x, y) => x[0] - y[0]);
  let total = 0;
  let cur: [number, number] | null = null;
  for (const [a, b] of sorted) {
    if (!cur || a > cur[1]) { if (cur) total += cur[1] - cur[0]; cur = [a, b]; } else cur[1] = Math.max(cur[1], b);
  }
  return cur ? total + cur[1] - cur[0] : total;
}

function gateName(workflow: Workflow, node: string): string {
  const step = (workflow.steps ?? []).find((s) => s.gateId === node);
  if (step?.gateName) return step.gateName;
  const agent = workflow.agents?.[node]?.name;
  return agent ? `after ${agent}` : node;
}

function agentLook(workflow: Workflow, id: string): { emoji: string; role: string } {
  const a = workflow.agents?.[id];
  if (a?.runtime === "task") return { emoji: FEATURE_EMOJI.task, role: `${a.task || "task"} step` };
  if (a?.runtime === "harness") return { emoji: "🧰", role: "harness" };
  return { emoji: FEATURE_EMOJI.agent, role: "" };
}

/** A stage's status from its agents': the worst, else the one still moving. */
export function stageStatus(statuses: string[]): string {
  for (const s of ["failed", "denied", "waiting_human", "running", "revise", "cancelling"]) if (statuses.includes(s)) return s;
  if (statuses.length && statuses.every((s) => s === "skipped")) return "skipped";
  if (statuses.length && statuses.every((s) => s === "cancelled")) return "cancelled";
  if (statuses.length && statuses.every((s) => s === "done" || s === "skipped")) return "done";
  if (statuses.some((s) => s === "done")) return "running";
  return statuses[0] ?? "pending";
}

const TERMINAL = new Set(["done", "failed", "denied", "skipped", "cancelled"]);

/** The whole story of a run. */
export function buildStory(input: StoryInput): Story {
  const { events, workflow, nodes = {}, hitl, history = {} } = input;
  const logs = parseLogs(input.logs, workflow);
  const total = events.length;
  const nums = stepNumbers(workflow.steps);

  // Each item's place in time and the replay position from which it shows. A log item
  // shows with the event before it (and a stage's gate never before its first event).
  const items = merge(events, logs);
  const posOfEvent = new Map<number, number>();
  const logPos = new Map<LogItem, { pos: number; at: number }>();
  let seen = 0;
  items.forEach((it, pos) => {
    if (it.t === "event") { seen += 1; posOfEvent.set(it.e.index, pos); } else {
      logPos.set(it, { pos, at: total === 0 ? 0 : Math.max(1, seen) });
    }
  });

  // Activations per agent, from its start and end lines.
  const runs = new Map<string, [number, number][]>();
  const openAt = new Map<string, number>();
  for (const l of logs) {
    if (l.t === "start") openAt.set(l.agent, ms(l.ts));
    if (l.t === "end") {
      const a = openAt.get(l.agent);
      if (a !== undefined) runs.set(l.agent, [...(runs.get(l.agent) ?? []), [a, ms(l.ts)]]);
      openAt.delete(l.agent);
    }
  }

  // Waits on an agent's own asks: from the ask to its answer.
  const askWait = new Map<number, number>();
  const asking = new Map<string, FlowEvent>();
  for (const e of events) {
    if (e.kind !== "person") continue;
    const k = `${e.agent}|${e.label}`;
    if (e.status === "asked" || e.status === "waiting") asking.set(k, e);
    else if (asking.has(k)) {
      const w = ms(e.ts) - ms(asking.get(k)!.ts);
      if (Number.isFinite(w)) askWait.set(e.index, Math.max(0, w));
      asking.delete(k);
    }
  }

  const lineOf = (e: FlowEvent): StoryLine => {
    const w = eventWords(e, workflow);
    const tone = toneOf(e);
    const meta: string[] = [];
    if (e.latencyMs > 0) meta.push(span(e.latencyMs));
    if (e.costUsd > 0) meta.push(money(e.costUsd));
    if (askWait.has(e.index)) meta.push(`waited ${span(askWait.get(e.index)!)}`);
    return {
      key: `e${e.index}`, type: "event", at: e.index + 1, pos: posOfEvent.get(e.index) ?? e.index,
      emoji: w.emoji, text: w.text, title: w.title, meta, tone, kind: e.kind,
      problem: tone === "failed" || tone === "denied" || tone === "blocked", event: e, agent: e.agent,
      version: e.version || 1,
    };
  };

  const agentOf = (id: string): StoryAgent => {
    const mine = events.filter((e) => e.agent === id);
    const lines: StoryLine[] = [];
    let ver = 1;
    for (const e of mine) {
      const v = e.version || 1;
      if (v !== ver && v > 1) {
        const note = (history[id] ?? []).find((h) => h.version === v)?.comment?.trim() ?? "";
        const text = `v${v} after review${note ? `: ${note}` : ""}`;
        lines.push({
          key: `v-${id || "run"}-${v}`, type: "version", at: e.index + 1, pos: (posOfEvent.get(e.index) ?? e.index) - 0.5,
          emoji: "🔁", text, title: text, meta: [], tone: "note", kind: "", problem: false, agent: id, version: v,
        });
      }
      ver = v;
      lines.push(lineOf(e));
    }
    const intervals = runs.get(id) ?? [];
    const look = id === "" ? { emoji: "🚀", role: "" } : agentLook(workflow, id);
    return {
      id, name: id === "" ? "The run" : String(workflow.agents?.[id]?.name ?? id),
      emoji: look.emoji, role: look.role, num: nums[id] ?? "",
      status: id === "" ? "" : nodes[id]?.status ?? (mine.length ? "done" : "pending"),
      durationMs: intervals.length ? unionMs(intervals)
        : mine.length && (id === "" || TERMINAL.has(String(nodes[id]?.status))) ? mine.reduce((t, e) => t + e.latencyMs, 0) : null,
      costUsd: mine.reduce((t, e) => t + e.costUsd, 0),
      calls: mine.filter((e) => e.kind !== "agentcore").length,
      lines,
      problem: lines.some((l) => l.problem),
    };
  };

  // Gates, and which stage each belongs to.
  const stageOfNode = (node: string): number => (workflow.steps ?? []).findIndex((s) =>
    s.gateId === node || (s.parallel ?? s.sequence ?? (s.agent ? [s.agent] : [])).includes(node));
  const gateLines = new Map<number, StoryLine[]>();
  const pending = new Map<string, { line: StoryLine; ts: string; name: string }>();
  let gates = 0;
  let waitMs = 0;
  const addGate = (stage: number, line: StoryLine) => gateLines.set(stage, [...(gateLines.get(stage) ?? []), line]);
  for (const l of logs) {
    if (l.t === "gateReq") {
      const p = logPos.get(l)!;
      const name = gateName(workflow, l.node);
      gates += 1;
      const text = l.auto ? `HITL review ${name}: approved automatically` : `HITL review ${name}: waiting`;
      const line: StoryLine = {
        key: `g${p.pos}`, type: "gate", at: p.at, pos: p.pos, emoji: "⏸", text, title: text, meta: [],
        tone: l.auto ? "ok" : "waiting", kind: "person", problem: false, agent: l.node,
      };
      addGate(stageOfNode(l.node), line);
      if (!l.auto) pending.set(l.node, { line, ts: l.ts, name });
    } else if (l.t === "gateRes") {
      const g = pending.get(l.node);
      if (!g) continue;   // a line that only looks like a decision: no gate was waiting
      pending.delete(l.node);
      const wait = ms(l.ts) - ms(g.ts);
      const waited = Number.isFinite(wait) ? Math.max(0, wait) : null;
      if (waited !== null) waitMs += waited;
      g.line.pendingText = g.line.text;
      g.line.doneAt = logPos.get(l)!.at;
      g.line.text = `HITL review ${g.name}: ${waited !== null ? `waited ${span(waited)} · ` : ""}${l.decision}`;
      g.line.title = g.line.text;
      g.line.tone = l.decision === "approved" ? "ok" : l.decision === "denied" ? "denied" : "note";
      g.line.problem = l.decision === "denied";
    }
  }
  // A review gate the run waits at now with no log line for it: shown, without a wait.
  if (hitl?.node && !hitl.kind && ![...pending.keys()].includes(hitl.node)) {
    const text = `HITL review ${gateName(workflow, hitl.node)}: waiting now`;
    gates += 1;
    addGate(stageOfNode(hitl.node), {
      key: "g-now", type: "gate", at: total, pos: items.length, emoji: "⏸", text, title: text, meta: [],
      tone: "waiting", kind: "person", problem: false, agent: hitl.node,
    });
  }
  // A gate with no decision line: waiting now if the run says so, else the run moved on
  // (cancelled, or the decision was not logged) and the line says only that.
  for (const [node, { line, name }] of pending) {
    line.text = hitl?.node === node && !hitl.kind ? `${line.text} now` : `HITL review ${name}: no decision recorded`;
    if (line.text.endsWith("no decision recorded")) line.tone = "skipped";
    line.title = line.text;
  }

  const stages: StoryStage[] = [];
  const listed = new Set<string>();
  (workflow.steps ?? []).forEach((s, i) => {
    const ids = s.parallel ?? s.sequence ?? (s.agent ? [s.agent] : []);
    ids.forEach((id) => listed.add(id));
    const agents = ids.map(agentOf);
    const kind = s.parallel ? "parallel" : s.sequence ? "sequence" : "single";
    const intervals = ids.flatMap((id) => runs.get(id) ?? []);
    const known = agents.map((a) => a.durationMs).filter((d): d is number => d !== null);
    stages.push({
      key: `s${i}`, num: String(i + 1), kind,
      title: kind === "single" ? agents[0]?.name ?? "" : `${agents.length} agents ${kind === "parallel" ? "in parallel" : "in sequence"}`,
      agents, lines: gateLines.get(i) ?? [], status: stageStatus(agents.map((a) => a.status)),
      durationMs: intervals.length ? unionMs(intervals)
        : known.length ? (kind === "parallel" ? Math.max(...known) : known.reduce((t, d) => t + d, 0)) : null,
    });
  });
  const extra = [...new Set(events.map((e) => e.agent))].filter((a) => a !== "" && !listed.has(a));
  if (extra.length) {
    const agents = extra.map(agentOf);
    stages.push({ key: "extra", num: "", kind: "extra", title: "Other agents", agents, lines: [],
      status: stageStatus(agents.map((a) => a.status)), durationMs: null });
  }
  const orphan = gateLines.get(-1) ?? [];
  if (events.some((e) => e.agent === "") || orphan.length) {
    const run = agentOf("");
    stages.push({ key: "run", num: "", kind: "extra", title: "The run", agents: run.lines.length ? [run] : [],
      lines: orphan, status: "", durationMs: run.durationMs });
  }

  // The run's time: first log line to last, else the stages added up.
  const stamps = (input.logs ?? []).map((l) => ms(String(l.ts ?? ""))).filter(Number.isFinite);
  const stageSum = stages.filter((s) => s.kind !== "extra" && s.durationMs !== null).map((s) => s.durationMs!);
  const kinds = new Set(events.map((e) => e.kind));
  if (gates) kinds.add("person");
  return {
    stages,
    totals: {
      durationMs: stamps.length >= 2 ? Math.max(...stamps) - Math.min(...stamps) : stageSum.length ? stageSum.reduce((t, d) => t + d, 0) : null,
      costUsd: events.reduce((t, e) => t + e.costUsd, 0),
      calls: events.filter((e) => e.kind !== "agentcore").length,
      gates, waitMs,
    },
    kinds: KIND_ORDER.filter((k) => kinds.has(k)),
  };
}

/** The lines a stage shows under its header: a single-agent stage's agent lines and gates
 *  in time order; any other stage's gates (its agents list their own). */
export function stageLines(stage: StoryStage): StoryLine[] {
  if (stage.kind !== "single") return stage.lines;
  return [...(stage.agents[0]?.lines ?? []), ...stage.lines].sort((a, b) => a.pos - b.pos);
}

export type StoryFilter = "all" | "problems" | string;

const keep = (l: StoryLine, f: StoryFilter) =>
  f === "all" ? true : f === "problems" ? l.problem : l.kind === f;

/** The story with only the lines the filter keeps. A version header stays when a line of
 *  its version does; an agent or stage with nothing left goes. "all" changes nothing. */
export function filterStory(story: Story, f: StoryFilter): Story {
  if (f === "all") return story;
  const lines = (list: StoryLine[]) => {
    const kept = list.filter((l) => l.type !== "version" && keep(l, f));
    return list.filter((l) => (l.type === "version"
      ? kept.some((k) => k.agent === l.agent && k.version === l.version) : kept.includes(l)));
  };
  // A single-agent stage keeps its agent: the header names it.
  const stages = story.stages.map((s) => ({
    ...s,
    agents: s.agents.map((a) => ({ ...a, lines: lines(a.lines) }))
      .filter((a) => s.kind === "single" || a.lines.length > 0),
    lines: lines(s.lines),
  })).filter((s) => s.lines.length > 0 || s.agents.some((a) => a.lines.length > 0));
  return { ...story, stages };
}

/** How many of a filtered story's lines are event and gate lines. */
export const lineCount = (story: Story) => story.stages.reduce((t, s) =>
  t + s.lines.length + s.agents.reduce((u, a) => u + a.lines.filter((l) => l.type !== "version").length, 0), 0);
