import { describe, expect, it } from "vitest";

import { buildEvents, type FlowRow } from "./flow";
import { BREAK_SHARE, buildGantt, fmtSpan, type GanttInput, ticks, toX } from "./gantt";
import { toEpochMs } from "./clock";
import type { LogLine, Workflow } from "../types";

/** "2025-01-06 10:00:00" plus `s` seconds, as the backend stamps it (US Eastern). */
function at(s: number): string {
  const d = new Date(Date.UTC(2025, 0, 6, 10, 0, 0) + s * 1000);
  const p = (n: number) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())} ${p(d.getUTCHours())}:${p(d.getUTCMinutes())}:${p(d.getUTCSeconds())}`;
}
const log = (s: number, node: string | undefined, msg: string): LogLine => ({ ts: at(s), node, msg });

const wf = {
  agents: {
    intake: { name: "Intake" }, a: { name: "Alpha" }, b: { name: "Beta" },
    w1: { name: "Writer" }, w2: { name: "Editor", runtime: "task" },
  },
  steps: [
    { agent: "intake", hitl: true },
    { parallel: ["a", "b"], gateId: "research", gateName: "Research review", hitl: true },
    { sequence: ["w1", "w2"] },
  ],
} as unknown as Workflow;

const LOGS: LogLine[] = [
  log(0, "intake", "Intake started"),
  log(10, "intake", "Intake complete (v1)"),
  log(10, "intake", "Awaiting human review after intake"),
  log(210, "intake", 'Human decision (intake): approve \u2014 note: "fine"'),
  log(210, "intake", "intake approved; continuing"),
  log(211, "a", "Alpha started"),
  log(211, "b", "Beta started"),
  log(220, "b", "Beta complete (v1)"),
  log(240, "a", "Alpha complete (v1)"),
  log(240, "research", "Awaiting per-agent review after Research review"),
  log(250, "research", "Research review review: re-run b"),
  log(250, "b", "Research review: re-running b"),
  log(251, "b", "Beta started"),
  log(256, "b", "Beta complete (v2)"),
  log(256, "w1", "Writer started"),
  log(270, "w1", "Writer complete (v1)"),
  log(270, "w2", "Editor started"),
  log(280, "w2", "Editor failed: ValueError: no"),
];

const ROWS: FlowRow[] = [
  { agent_id: "intake", kind: "llm", label: "claude", status: "ok", latency_ms: 4000, ts: at(6), seq: 1, cost_usd: 0.01 },
  { agent_id: "intake", kind: "tool", label: "search", status: "ok", latency_ms: 2000, ts: at(9), seq: 2, cost_usd: 0.002 },
  { agent_id: "b", kind: "guardrail", label: "OUTPUT", status: "passed", latency_ms: 300, ts: at(219), seq: 3 },
  { agent_id: "b", kind: "llm", label: "nova", status: "ok", latency_ms: 3000, ts: at(255), seq: 4, cost_usd: 0.003 },
  { agent_id: "a", kind: "memory", label: "recall", status: "agentcore", latency_ms: 900, ts: at(213), seq: 5 },
  { agent_id: "w1", kind: "eval", label: "Builtin.Helpfulness", status: "ok", latency_ms: 20000, ts: at(269), seq: 6 },
  { agent_id: "w1", kind: "policy", label: "search", status: "denied", latency_ms: 50, ts: at(260), seq: 7 },
  { agent_id: "__session__", kind: "agentcore", label: "runtime", status: "ok", latency_ms: 280000, ts: at(280), seq: 8, cost_usd: 0.05 },
];

const input = (over: Partial<GanttInput> = {}): GanttInput => ({
  workflow: wf, logs: LOGS, events: buildEvents(ROWS), created: at(0), overall: "failed",
  nodes: { intake: { status: "done" }, a: { status: "done" }, b: { status: "done" }, w1: { status: "done" }, w2: { status: "failed" } },
  now: toEpochMs(at(400)), ...over,
});

describe("the run's Gantt layout", () => {
  it("has one row per agent in stage order, each stage labelled once", () => {
    const g = buildGantt(input());
    expect(g.rows.map((r) => [r.agent, r.stage, r.firstInStage])).toEqual([
      ["intake", 1, true], ["a", 2, true], ["b", 2, false], ["w1", 3, true], ["w2", 3, false]]);
    expect(g.rows[1].stageLabel).toBe("Stage 2 · 2 in parallel");
    expect(g.rows[3].stageLabel).toBe("Stage 3 · 2 in sequence");
    expect(g.rows[0].stageLabel).toBe("Stage 1");
    expect(g.rows[0].emoji).toBe("🤖");
    expect(g.rows[4].emoji).toBe("⚙️");
  });

  it("spans each bar from its started line to its complete or failed line, a re-run as v2", () => {
    const g = buildGantt(input());
    const bars = Object.fromEntries(g.rows.map((r) => [r.agent, r.bars.map((b) => [b.version, b.status, b.start, b.end])]));
    expect(bars.intake).toEqual([[1, "done", 0, 10_000]]);
    expect(bars.b).toEqual([[1, "done", 211_000, 220_000], [2, "done", 251_000, 256_000]]);
    expect(bars.w2).toEqual([[1, "failed", 270_000, 280_000]]);
    expect(g.end).toBe(280_000);
  });

  it("starts each call at ts - latency, inside the run of the agent it belongs to", () => {
    const g = buildGantt(input());
    const intake = g.rows[0].bars[0].calls;
    expect(intake.map((c) => [c.kind, c.label, c.start, c.end])).toEqual([
      ["model", "claude", 2000, 6000], ["tool", "search", 7000, 9000]]);
    const [v1, v2] = g.rows[2].bars;
    expect(v1.calls.map((c) => c.kind)).toEqual(["guardrail"]);
    expect(v2.calls.map((c) => [c.kind, c.start, c.end])).toEqual([["model", 252_000, 255_000]]);
    // A 20 s call stamped 13 s into an agent's 14 s run is drawn from the run's start, not before it.
    const ev = g.rows[3].bars[0].calls.find((c) => c.kind === "eval")!;
    expect([ev.start, ev.end, ev.latencyMs]).toEqual([256_000, 269_000, 20_000]);
    // Run-level rows (runtime compute) are not a step's call, but their cost counts.
    expect(g.calls.some((c) => c.agent === "__session__")).toBe(false);
    expect(g.totals.calls).toBe(7);
    expect(g.totals.costUsd).toBeCloseTo(0.065, 6);
  });

  it("draws each HITL wait from its request to its decision, with the outcome", () => {
    const g = buildGantt(input());
    expect(g.gates.map((w) => [w.node, w.name, w.start, w.end, w.outcome, w.label])).toEqual([
      ["intake", "Intake", 10_000, 210_000, "approved", "⏸ HITL: 3 m 20 s, approved"],
      ["research", "Research review", 240_000, 250_000, "re-run requested", "⏸ HITL: 10 s, re-run requested"],
    ]);
  });

  it("says a wait that is still open is waiting, and one the run stopped at was not answered", () => {
    const logs = [log(0, "intake", "Intake started"), log(5, "intake", "Intake complete (v1)"),
      log(5, "intake", "Awaiting human review after intake")];
    const live = buildGantt(input({ logs, events: [], overall: "waiting_human", now: toEpochMs(at(65)) }));
    expect(live.gates[0]).toMatchObject({ open: true, end: 65_000, outcome: "waiting", label: "⏸ HITL: 1 m, waiting" });
    expect(live.now).toBe(65_000);
    const stopped = buildGantt(input({ logs, events: [], overall: "cancelled" }));
    expect(stopped.gates[0]).toMatchObject({ end: 5000, outcome: "not answered" });
    expect(stopped.now).toBeNull();
  });

  it("grows a running bar to now while the run is live, and stops it when settled", () => {
    const logs = [log(0, "intake", "Intake started")];
    const live = buildGantt(input({ logs, events: [], overall: "running", nodes: { intake: { status: "running" } },
      now: toEpochMs(at(30)) }));
    expect(live.rows[0].bars[0]).toMatchObject({ status: "running", open: true, start: 0, end: 30_000 });
    const later = buildGantt(input({ logs, events: [], overall: "running", nodes: { intake: { status: "running" } },
      now: toEpochMs(at(45)) }));
    expect(later.rows[0].bars[0].end).toBe(45_000);
    const asking = buildGantt(input({ logs, events: [], overall: "waiting_human",
      nodes: { intake: { status: "waiting_human" } }, now: toEpochMs(at(30)) }));
    expect(asking.rows[0].bars[0].status).toBe("waiting");
    const cancelled = buildGantt(input({ logs: [...logs, log(9, undefined, "Workflow cancelled")], events: [],
      overall: "cancelled", nodes: { intake: { status: "running" } } }));
    expect(cancelled.rows[0].bars[0]).toMatchObject({ status: "stopped", end: 9000 });
  });

  it("marks the critical path: the slowest of each parallel round, every bar of a sequence", () => {
    const g = buildGantt(input());
    const crit = g.rows.flatMap((r) => r.bars.filter((b) => b.critical).map((b) => b.key));
    // Alpha (211-240 s) outlasts Beta v1 (211-220 s); Beta v2 is a round on its own.
    expect(crit).toEqual(["intake#1", "a#1", "b#2", "w1#1", "w2#1"]);
  });

  it("compresses a long HITL wait to a break and keeps the scale monotonic", () => {
    const g = buildGantt(input());
    // Nothing runs from Intake's end (10 s) to Alpha's start (211 s): 3 m 21 s, compressed.
    // The 11 s gap at Research review is short and keeps its real length.
    expect(g.breaks).toEqual([{ start: 10_000, end: 211_000, label: "// 3 m 21 s //" }]);
    const active = 280_000 - 201_000 - 11_000;
    expect(toX(g, 211_000) - toX(g, 10_000)).toBeCloseTo((active * BREAK_SHARE) / g.display, 9);
    expect(g.display).toBeCloseTo(active * (1 + BREAK_SHARE) + 11_000, 6);
    let prev = -1;
    for (let t = 0; t <= g.end; t += 5000) {
      const x = toX(g, t);
      expect(x).toBeGreaterThanOrEqual(prev);
      prev = x;
    }
    expect(toX(g, 0)).toBe(0);
    expect(toX(g, g.end)).toBeCloseTo(1, 9);
    // No tick inside the compressed wait: its break marker says how long it was.
    expect(ticks(g).every((t) => t.t <= 10_000 || t.t >= 211_000)).toBe(true);
  });

  it("leaves a short wait uncompressed", () => {
    const logs = [log(0, "intake", "Intake started"), log(30, "intake", "Intake complete (v1)"),
      log(30, "intake", "Awaiting human review after intake"), log(50, "intake", "Human decision (intake): deny"),
      log(50, "a", "Alpha started"), log(80, "a", "Alpha complete (v1)")];
    const g = buildGantt(input({ logs, events: [] }));
    expect(g.breaks).toEqual([]);
    expect(g.gates[0].outcome).toBe("denied");
    expect(toX(g, 40_000)).toBeCloseTo(0.5, 9);
  });

  it("lists the three slowest calls", () => {
    const g = buildGantt(input());
    expect(g.slowest.map((c) => [c.label, c.latencyMs])).toEqual([
      ["Builtin.Helpfulness", 20_000], ["claude", 4000], ["nova", 3000]]);
  });

  it("gives an agent with calls but no started line a bar from its calls", () => {
    const g = buildGantt(input({ logs: [] }));
    expect(g.rows[0].bars[0]).toMatchObject({ start: 2000, end: 9000, version: 1, status: "done" });
    expect(g.rows[4].bars).toEqual([]);
  });

  it("draws nothing for a run with no stamps at all", () => {
    const g = buildGantt(input({ logs: [], events: [], created: undefined }));
    expect(g.rows).toEqual([]);
    expect(Number.isNaN(g.t0)).toBe(true);
  });

  it("ignores an agent's own log line that only looks like an end", () => {
    const logs = [log(0, "intake", "Intake started"), log(3, "intake", "Search failed: timeout, retrying"),
      log(6, "intake", "Intake complete (v1)")];
    const g = buildGantt(input({ logs, events: [] }));
    expect(g.rows[0].bars.map((b) => [b.status, b.end])).toEqual([["done", 6000]]);
  });

  it("stays usable at 12 agents, 300 calls and a 2 h run with a long wait", () => {
    const agents = Object.fromEntries(Array.from({ length: 12 }, (_, i) => [`g${i}`, { name: `Agent ${i}` }]));
    const steps = Array.from({ length: 6 }, (_, i) => ({ parallel: [`g${2 * i}`, `g${2 * i + 1}`] }));
    const logs: LogLine[] = [];
    const rows: FlowRow[] = [];
    let t = 0;
    for (let s = 0; s < 6; s += 1) {
      for (const id of [`g${2 * s}`, `g${2 * s + 1}`]) logs.push(log(t, id, `${agents[id].name} started`));
      for (let k = 0; k < 50; k += 1) {
        rows.push({ agent_id: `g${2 * s + (k % 2)}`, kind: "llm", label: `m${k}`, latency_ms: 1500, ts: at(t + 2 + k), seq: k });
      }
      for (const id of [`g${2 * s}`, `g${2 * s + 1}`]) logs.push(log(t + 60, id, `${agents[id].name} complete (v1)`));
      t += s === 2 ? 60 + 2 * 3600 : 61; // a 2 h HITL wait after stage 3
    }
    const g = buildGantt({ workflow: { agents, steps } as unknown as Workflow, logs, events: buildEvents(rows),
      created: at(0), overall: "done", now: 0 });
    expect(g.rows).toHaveLength(12);
    expect(g.calls).toHaveLength(300);
    expect(g.breaks.map((b) => b.label)).toEqual(["// 2 h //"]);
    // The 6 minutes of work fill most of the width; the 2 h wait is a sliver.
    const wait = toX(g, g.breaks[0].end) - toX(g, g.breaks[0].start);
    expect(wait).toBeLessThan(0.15);
    for (const z of [1, 2, 4, 8, 16, 32]) expect(ticks(g, z).length).toBeLessThanOrEqual(500);
    expect(ticks(g, 1).length).toBeGreaterThanOrEqual(4);
  });
});

describe("fmtSpan", () => {
  it("reads as s, m and h", () => {
    expect([0, 450, 4500, 45_000, 60_000, 200_000, 7_200_000, 7_500_000].map(fmtSpan)).toEqual(
      ["0 s", "450 ms", "4.5 s", "45 s", "1 m", "3 m 20 s", "2 h", "2 h 5 m"]);
  });
});
