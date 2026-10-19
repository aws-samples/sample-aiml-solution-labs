/** The layout behind the Timeline tab's chart: a Gantt of one run, time left to right.
 *  Pure functions only, so rows, bars, sub-bars, the critical path and the compressed
 *  waits are all testable without a browser.
 *
 *  Where each mark comes from (nothing new is stored or read):
 *  * An agent's BAR spans its "<name> started" log line to its "<name> complete (vN)",
 *    "<name> failed: ..." or "<name> blocked by guardrail" line (app/orchestrator/nodes.py
 *    make_agent_node). The node history is NOT used: it records only when a version
 *    finished (`at`), not when it started. A re-run is a second "started" line, so a second
 *    bar segment, labelled with the version its "complete (vN)" line names.
 *  * A bar's SUB-BARS are the run's Flow events (lib/flow.ts). A telemetry row's `ts` is
 *    stamped when the row is written, after the call returns
 *    (app/features/observability/records.py, `ts = clock.now_str()`), so a call starts at
 *    ts - latencyMs. Both stamps are whole seconds, so a mark can be up to a second off.
 *  * A HITL BAND spans a gate's "Awaiting ... review after X" line to its decision line
 *    ("Human decision (X): approve", "<label> review: approved", ...).
 */

import type { FlowEvent, Tone } from "./flow";
import { toEpochMs } from "./clock";
import { FEATURE_EMOJI } from "./emoji";
import type { LogLine, Step, Workflow } from "../types";

export type BarStatus = "done" | "running" | "failed" | "waiting" | "stopped";
export type CallKind = "model" | "tool" | "guardrail" | "memory" | "policy" | "eval" | "hitl" | "other";

/** The legend, in the order it is shown. */
export const CALL_KINDS: { kind: CallKind; label: string; emoji: string }[] = [
  { kind: "model", label: "Model", emoji: "💬" },
  { kind: "tool", label: "Tool", emoji: FEATURE_EMOJI.tool },
  { kind: "guardrail", label: "Guardrail", emoji: FEATURE_EMOJI.guardrail },
  { kind: "memory", label: "Memory", emoji: FEATURE_EMOJI.memory },
  { kind: "policy", label: "Policy", emoji: FEATURE_EMOJI.policy },
  { kind: "eval", label: "Evaluation", emoji: FEATURE_EMOJI.evaluation },
  { kind: "hitl", label: "HITL", emoji: FEATURE_EMOJI.human },
  { kind: "other", label: "Other", emoji: "•" },
];

const KIND_OF: Record<string, CallKind> = {
  llm: "model", image: "model", tool: "tool", guardrail: "guardrail", memory: "memory",
  policy: "policy", eval: "eval", person: "hitl",
};

/** One call inside an agent's bar. Times are ms from the run's start. */
export interface GanttCall {
  key: string;
  /** The event's position in the run's Flow events (FlowEvent.index). */
  index: number;
  agent: string;
  kind: CallKind;
  label: string;
  status: string;
  tone: Tone;
  sentence: string;
  latencyMs: number;
  costUsd: number;
  /** Drawn span, kept inside its bar (the stamps are whole seconds). */
  start: number;
  end: number;
}

export interface GanttBar {
  key: string;
  agent: string;
  version: number;
  status: BarStatus;
  start: number;
  end: number;
  /** No end line yet: still running, or the run stopped before the agent finished. */
  open: boolean;
  /** One of the bars that set the run's total duration. */
  critical: boolean;
  calls: GanttCall[];
}

export interface GanttRow {
  agent: string;
  name: string;
  emoji: string;
  /** 1-based, the workflow's stage order. */
  stage: number;
  stageKind: "single" | "parallel" | "sequence";
  /** "Stage 2 · 3 in parallel": shown once, above the stage's first row. */
  stageLabel: string;
  firstInStage: boolean;
  /** The run's own status for the agent (snapshot nodes). */
  status: string;
  bars: GanttBar[];
}

export interface GanttGate {
  key: string;
  node: string;
  name: string;
  start: number;
  end: number;
  open: boolean;
  /** "approved", "denied", "revise requested", "re-run requested", "waiting", "not answered". */
  outcome: string;
  /** "⏸ HITL: 3 m 20 s, approved". */
  label: string;
}

/** A stretch of real time drawn at its own scale; a compressed one is a long idle wait. */
export interface GanttSegment {
  from: number;
  to: number;
  d0: number;
  d1: number;
  compressed: boolean;
}

export interface GanttBreak {
  start: number;
  end: number;
  /** "// 2 h //". */
  label: string;
}

export interface Gantt {
  /** Epoch ms of the run's start (0 on the chart); NaN when there is nothing to draw. */
  t0: number;
  /** ms from the start to the end of the chart: the last mark, or now while live. */
  end: number;
  live: boolean;
  /** ms from the start to now while live; null when settled. */
  now: number | null;
  rows: GanttRow[];
  gates: GanttGate[];
  breaks: GanttBreak[];
  segments: GanttSegment[];
  /** Display length of the whole chart (the sum of the segments' display lengths). */
  display: number;
  calls: GanttCall[];
  slowest: GanttCall[];
  totals: { durationMs: number; costUsd: number; calls: number };
}

export interface GanttInput {
  workflow: Pick<Workflow, "agents" | "steps">;
  logs?: LogLine[] | null;
  nodes?: Record<string, { status?: string } | undefined> | null;
  /** The run's Flow events; null when not read (a shared run reads none). */
  events?: FlowEvent[] | null;
  created?: string;
  overall: string;
  /** Epoch ms of "now", for a live run. */
  now: number;
}

/** A wait is compressed when it is longer than this AND than half the time anything ran. */
export const BREAK_MIN_MS = 60_000;
/** A compressed wait is drawn this share of the time anything ran. */
export const BREAK_SHARE = 0.12;

const LIVE = new Set(["running", "waiting_human", "cancelling"]);

const ids = (s: Step): string[] => s.parallel ?? s.sequence ?? (s.agent ? [s.agent] : []);

/** "450 ms", "4.5 s", "45 s", "3 m 20 s", "2 h 5 m". */
export function fmtSpan(ms: number): string {
  if (!(ms > 0)) return "0 s";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  const s = ms / 1000;
  if (s < 10 && Math.abs(s - Math.round(s)) >= 0.05) return `${s.toFixed(1)} s`;
  const total = Math.round(s);
  if (total < 60) return `${total} s`;
  if (total < 3600) {
    const r = total % 60;
    return `${Math.floor(total / 60)} m${r ? ` ${r} s` : ""}`;
  }
  const m = Math.floor(total / 60) % 60;
  return `${Math.floor(total / 3600)} h${m ? ` ${m} m` : ""}`;
}

export const fmtCost = (n: number): string =>
  (n <= 0 ? "$0" : n < 0.0001 ? "<$0.0001" : `$${n.toFixed(4)}`);

function outcomeOf(word: string): string {
  const w = word.trim().toLowerCase();
  if (w.startsWith("approve")) return "approved";
  if (w.startsWith("deny")) return "denied";
  if (w.startsWith("revise")) return "revise requested";
  if (w.startsWith("re-run")) return "re-run requested";
  return w || "decided";
}

/** A decision line for a gate: nodes.py make_gate_node / make_sequence_gate_node /
 *  make_group_gate_node, with _with_note's ' — note: "..."' tail removed. */
const DECISION = [
  /^(?:Human|Automatic) decision \(.+?\): (.+?)(?: \u2014 note: .*)?$/,
  /^.+? review: (.+?)(?: \u2014 note: .*)?$/,
];

function barStatusOf(node: string | undefined, live: boolean): BarStatus {
  switch (node) {
    case "done": return "done";
    case "failed": case "denied": return "failed";
    case "waiting_human": return "waiting";
    case "running": case "revise": return live ? "running" : "stopped";
    default: return "stopped";
  }
}

const empty = (live: boolean): Gantt => ({
  t0: Number.NaN, end: 0, live, now: null, rows: [], gates: [], breaks: [], segments: [],
  display: 1, calls: [], slowest: [], totals: { durationMs: 0, costUsd: 0, calls: 0 },
});

export function buildGantt(input: GanttInput): Gantt {
  const { workflow, created, overall } = input;
  const live = LIVE.has(overall);
  const logs = input.logs ?? [];
  const nodes = input.nodes ?? {};
  const events = input.events ?? [];
  const agents = workflow.agents ?? {};

  // --- rows, in stage order --------------------------------------------------------
  const rows: GanttRow[] = [];
  const rowOf: Record<string, GanttRow> = {};
  (workflow.steps ?? []).forEach((s, i) => {
    const members = ids(s).filter((id) => !(id in rowOf));
    const kind: GanttRow["stageKind"] = s.parallel ? "parallel" : s.sequence ? "sequence" : "single";
    const many = ids(s).length;
    const stageLabel = `Stage ${i + 1}` + (kind === "parallel" ? ` · ${many} in parallel`
      : kind === "sequence" ? ` · ${many} in sequence` : "");
    members.forEach((id, j) => {
      const meta = agents[id] ?? {};
      const row: GanttRow = {
        agent: id, name: String(meta.name ?? id),
        emoji: meta.runtime === "task" ? FEATURE_EMOJI.task : FEATURE_EMOJI.agent,
        stage: i + 1, stageKind: kind, stageLabel, firstInStage: j === 0,
        status: String(nodes[id]?.status ?? "pending"), bars: [],
      };
      rows.push(row);
      rowOf[id] = row;
    });
  });

  // --- the clock: the run's start is 0 -----------------------------------------------
  const stamps = [toEpochMs(created), ...logs.map((l) => toEpochMs(l.ts)),
    ...events.map((e) => toEpochMs(e.ts) - e.latencyMs)].filter(Number.isFinite);
  if (!stamps.length) return empty(live);
  const t0 = Math.min(...stamps);
  const rel = (ts: string) => {
    const t = toEpochMs(ts);
    return Number.isFinite(t) ? t - t0 : Number.NaN;
  };

  // --- bars and gates, from the log ------------------------------------------------
  const open: Record<string, GanttBar> = {};
  const openGate: Record<string, GanttGate> = {};
  const gates: GanttGate[] = [];
  let last = 0;
  const gateName = (node: string) =>
    (workflow.steps ?? []).find((s) => s.gateId === node)?.gateName ?? agents[node]?.name ?? node;
  for (const l of logs) {
    const t = rel(l.ts);
    if (!Number.isFinite(t)) continue;
    last = Math.max(last, t);
    const node = l.node ?? "";
    const row = rowOf[node];
    if (row) {
      const close = (status: BarStatus, version?: number) => {
        const b = open[node];
        if (!b) return false;
        b.end = Math.max(b.start, t);
        b.status = status;
        b.open = false;
        if (version) b.version = version;
        delete open[node];
        return true;
      };
      if (l.msg === `${row.name} started` || l.msg === `${node} started`) {
        // A start with the previous run still open: that run stopped without saying so.
        close("stopped");
        const b: GanttBar = { key: `${node}#${row.bars.length + 1}`, agent: node, version: row.bars.length + 1,
          status: "running", start: t, end: t, open: true, critical: false, calls: [] };
        row.bars.push(b);
        open[node] = b;
        continue;
      }
      const done = /^(.+) complete \(v(\d+)\)$/.exec(l.msg);
      if (done && (done[1] === row.name || done[1] === node)) {
        if (!close("done", Number(done[2]))) {
          // An end with no start line (a truncated log): a bar of no length, still shown.
          row.bars.push({ key: `${node}#${row.bars.length + 1}`, agent: node, version: Number(done[2]),
            status: "done", start: t, end: t, open: false, critical: false, calls: [] });
        }
        continue;
      }
      if (l.msg.startsWith(`${row.name} failed: `) || l.msg.startsWith(`${row.name} blocked by guardrail`)) {
        close("failed");
        continue;
      }
    }
    if (/^Awaiting (?:human|per-agent) review after /.test(l.msg) && node) {
      const g: GanttGate = { key: `gate-${node}-${gates.length}`, node, name: String(gateName(node)),
        start: t, end: t, open: true, outcome: "waiting", label: "" };
      gates.push(g);
      openGate[node] = g;
      continue;
    }
    const g = openGate[node];
    if (g) {
      const m = DECISION.map((r) => r.exec(l.msg)).find(Boolean);
      if (m) {
        g.end = Math.max(g.start, t);
        g.open = false;
        g.outcome = outcomeOf(m[1]);
        delete openGate[node];
      }
    }
  }

  // --- calls, onto their agent's bars ------------------------------------------------
  const calls: GanttCall[] = [];
  for (const e of events) {
    const t = rel(e.ts);
    if (!Number.isFinite(t)) continue;
    last = Math.max(last, t);
    const row = rowOf[e.agent];
    if (!row) continue;
    calls.push({
      key: `call-${e.index}`, index: e.index, agent: e.agent, kind: KIND_OF[e.kind] ?? "other",
      label: e.label, status: e.status, tone: e.tone, sentence: e.sentence,
      latencyMs: e.latencyMs, costUsd: e.costUsd, start: Math.max(0, t - e.latencyMs), end: t,
    });
  }
  const end = live ? Math.max(last, input.now - t0) : last;
  for (const b of Object.values(open)) {
    b.end = Math.max(b.start, end);
    b.status = barStatusOf(rowOf[b.agent]?.status, live);
  }
  for (const g of Object.values(openGate)) {
    g.end = Math.max(g.start, end);
    g.outcome = live ? "waiting" : "not answered";
  }
  for (const g of gates) g.label = `⏸ HITL: ${fmtSpan(g.end - g.start)}, ${g.outcome}`;

  for (const c of calls) {
    const row = rowOf[c.agent];
    if (!row.bars.length) {
      // Events but no start line (an old run, or a log that was cut): a bar from its calls.
      const mine = calls.filter((x) => x.agent === c.agent);
      row.bars.push({ key: `${c.agent}#1`, agent: c.agent, version: 1,
        status: barStatusOf(row.status, live), start: Math.min(...mine.map((x) => x.start)),
        end: Math.max(...mine.map((x) => x.end)), open: false, critical: false, calls: [] });
    }
    // The bar it falls in (a second of slack for whole-second stamps), else the nearest.
    const slack = 1000;
    const inside = row.bars.filter((b) => c.end >= b.start - slack && c.end <= b.end + slack);
    const bar = inside.length ? inside[inside.length - 1]
      : row.bars.reduce((best, b) => (dist(c.end, b) < dist(c.end, best) ? b : best));
    c.start = clamp(c.start, bar.start, bar.end);
    c.end = clamp(c.end, c.start, bar.end);
    bar.calls.push(c);
  }

  markCritical(rows);

  // --- time scale: long idle waits drawn short ----------------------------------------
  const busy = merge(rows.flatMap((r) => r.bars.map((b) => [b.start, b.end] as [number, number])));
  const idle: [number, number][] = [];
  let cursor = 0;
  for (const [a, b] of busy) {
    if (a > cursor) idle.push([cursor, a]);
    cursor = Math.max(cursor, b);
  }
  if (end > cursor) idle.push([cursor, end]);
  const active = end - idle.reduce((n, [a, b]) => n + (b - a), 0);
  const long = active > 0
    ? idle.filter(([a, b]) => b - a > Math.max(BREAK_MIN_MS, active * 0.5)) : [];
  const segments: GanttSegment[] = [];
  let d = 0;
  cursor = 0;
  const push = (from: number, to: number, compressed: boolean) => {
    if (to <= from) return;
    const len = compressed ? active * BREAK_SHARE : to - from;
    segments.push({ from, to, d0: d, d1: d + len, compressed });
    d += len;
  };
  for (const [a, b] of long) {
    push(cursor, a, false);
    push(a, b, true);
    cursor = b;
  }
  push(cursor, end, false);

  const slowest = calls.filter((c) => c.latencyMs > 0)
    .sort((a, b) => b.latencyMs - a.latencyMs || a.index - b.index).slice(0, 3);
  return {
    t0, end, live, now: live ? end : null, rows, gates,
    breaks: long.map(([a, b]) => ({ start: a, end: b, label: `// ${fmtSpan(b - a)} //` })),
    segments, display: d > 0 ? d : 1, calls, slowest,
    totals: { durationMs: end, costUsd: events.reduce((n, e) => n + e.costUsd, 0), calls: calls.length },
  };
}

const clamp = (n: number, lo: number, hi: number) => Math.max(lo, Math.min(hi, n));
const dist = (t: number, b: GanttBar) => (t < b.start ? b.start - t : t > b.end ? t - b.end : 0);

function merge(spans: [number, number][]): [number, number][] {
  const out: [number, number][] = [];
  for (const [a, b] of spans.slice().sort((x, y) => x[0] - y[0])) {
    const top = out[out.length - 1];
    if (top && a <= top[1]) top[1] = Math.max(top[1], b);
    else out.push([a, b]);
  }
  return out;
}

/** The bars that set the run's total duration. In a single-agent or sequence stage every
 *  bar does: the next one waits for it. In a parallel stage the agents run side by side
 *  and the stage waits for the slowest, so of each group of overlapping bars (one round;
 *  a re-run of some agents is another round) only the one that ends last counts. */
export function markCritical(rows: GanttRow[]): void {
  const stages = new Map<number, GanttRow[]>();
  for (const r of rows) stages.set(r.stage, [...(stages.get(r.stage) ?? []), r]);
  for (const members of stages.values()) {
    const bars = members.flatMap((r) => r.bars);
    if (members[0].stageKind !== "parallel") {
      for (const b of bars) b.critical = true;
      continue;
    }
    let round: GanttBar[] = [];
    let roundEnd = -Infinity;
    const finish = () => {
      if (!round.length) return;
      round.reduce((best, b) => (b.end > best.end || (b.end === best.end && b.end - b.start > best.end - best.start)
        ? b : best)).critical = true;
    };
    for (const b of bars.slice().sort((x, y) => x.start - y.start)) {
      if (b.start > roundEnd) {
        finish();
        round = [];
      }
      round.push(b);
      roundEnd = Math.max(roundEnd, b.end);
    }
    finish();
  }
}

/** Where a time (ms from the start) falls across the chart, from 0 to 1. */
export function toX(g: Gantt, t: number): number {
  if (!g.segments.length) return 0;
  const at = clamp(t, 0, g.end);
  const s = g.segments.find((x) => at <= x.to) ?? g.segments[g.segments.length - 1];
  const f = s.to > s.from ? (at - s.from) / (s.to - s.from) : 0;
  return (s.d0 + f * (s.d1 - s.d0)) / g.display;
}

const STEPS = [100, 200, 500, 1000, 2000, 5000, 10_000, 15_000, 30_000, 60_000, 120_000, 300_000,
  600_000, 900_000, 1_800_000, 3_600_000, 7_200_000, 14_400_000];

/** Axis ticks for a zoom level: about six across the visible width, at round times
 *  ("30 s", "1 m"), and none inside a compressed wait (its break marker says how long). */
export function ticks(g: Gantt, zoom = 1): { t: number; x: number; label: string }[] {
  const want = g.display / Math.max(1, zoom) / 6;
  const step = STEPS.find((s) => s >= want) ?? STEPS[STEPS.length - 1];
  const out: { t: number; x: number; label: string }[] = [];
  for (const s of g.segments) {
    if (s.compressed) continue;
    for (let t = Math.ceil(s.from / step) * step; t <= s.to && out.length < 500; t += step) {
      if (out.length && out[out.length - 1].t === t) continue;
      out.push({ t, x: toX(g, t), label: fmtSpan(t) });
    }
  }
  return out;
}
