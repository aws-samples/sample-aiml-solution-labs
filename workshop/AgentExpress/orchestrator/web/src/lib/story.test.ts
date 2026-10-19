/** The Story view's layout: events, log lines and history in; stages, agents and lines out.
 *  No component. */
import { describe, expect, it } from "vitest";
import type { LogLine, Workflow } from "../types";
import { buildEvents, type FlowRow } from "./flow";
import {
  buildStory, eventWords, filterStory, lineCount, modelName, parseLogs, stageLines, stageStatus,
} from "./story";

const WF = {
  agents: {
    prep: { name: "Prep", runtime: "task", task: "code" },
    researcher: {
      name: "Researcher",
      components: [
        { kind: "tool", name: "warranty", tool: "warranty", detail: "Knowledge Base" },
        { kind: "tool", name: "orders", tool: "orders", detail: "Lambda function" },
        { kind: "policy", name: "noSecrets", tool: "orders", detail: "on orders" },
      ],
    },
    checker: { name: "Checker" },
    writer: { name: "Writer", runtime: "harness" },
  },
  steps: [{ agent: "prep" }, { parallel: ["researcher", "checker"] }, { agent: "writer", hitl: true }],
} as unknown as Workflow;

const row = (o: Partial<FlowRow>): FlowRow => ({
  agent_id: "researcher", kind: "tool", label: "warranty", status: "ok", latency_ms: 1200,
  cost_usd: 0.0004, version: 1, ts: "2025-01-01 10:00:01", seq: 1, ...o,
});
const ev = (rows: FlowRow[]) => buildEvents(rows, (id) => WF.agents[id]?.name ?? id);
const log = (ts: string, node: string, msg: string): LogLine => ({ ts: `2025-01-01 ${ts}`, node, msg });
const ROWS = [
  row({ agent_id: "prep", label: "codeInterpreter", mode: "agentcore", ts: "2025-01-01 10:00:01", seq: 1, latency_ms: 2800, cost_usd: 0 }),
  row({ ts: "2025-01-01 10:00:03", seq: 2 }),
  row({ kind: "llm", label: "us.anthropic.claude-haiku-4-5-20251001-v1:0", ts: "2025-01-01 10:00:04", seq: 3, latency_ms: 1800, cost_usd: 0.002 }),
  row({ kind: "policy", label: "orders", status: "denied", ts: "2025-01-01 10:00:04", seq: 4, latency_ms: 0, cost_usd: 0 }),
  row({ agent_id: "checker", kind: "guardrail", label: "INPUT", status: "passed", ts: "2025-01-01 10:00:05", seq: 5, cost_usd: 0 }),
  row({ agent_id: "writer", kind: "memory", label: "store", ts: "2025-01-01 10:00:08", seq: 6, cost_usd: 0 }),
  row({ agent_id: "__session__", kind: "agentcore", label: "runtime-compute", ts: "2025-01-01 10:00:09", seq: 7, cost_usd: 0.001 }),
];
const LOGS = [
  log("10:00:00", "prep", "Prep started"),
  log("10:00:02", "prep", "Prep complete (v1)"),
  log("10:00:02", "researcher", "Researcher started"),
  log("10:00:02", "checker", "Checker started"),
  log("10:00:05", "checker", "Checker complete (v1)"),
  log("10:00:06", "researcher", "Researcher complete (v1)"),
  log("10:00:07", "writer", "Writer started"),
  log("10:00:09", "writer", "Writer complete (v1)"),
  log("10:00:10", "writer", "Awaiting human review after writer"),
  log("10:03:30", "writer", "Human decision (writer): approve"),
];
const NODES = { prep: { status: "done" }, researcher: { status: "done" }, checker: { status: "done" }, writer: { status: "done" } } as const;
const texts = (lines: { text: string }[]) => lines.map((l) => l.text);

describe("stages", () => {
  it("are numbered as the Graph view numbers them, with the agents of a parallel stage under it", () => {
    const s = buildStory({ events: ev(ROWS), workflow: WF, nodes: { ...NODES }, logs: LOGS });
    expect(s.stages.map((x) => [x.num, x.kind, x.title])).toEqual([
      ["1", "single", "Prep"], ["2", "parallel", "2 agents in parallel"], ["3", "single", "Writer"], ["", "extra", "The run"],
    ]);
    expect(s.stages[1].agents.map((a) => [a.num, a.name, a.emoji])).toEqual([["2a", "Researcher", "🤖"], ["2b", "Checker", "🤖"]]);
    expect([s.stages[0].agents[0].emoji, s.stages[0].agents[0].role]).toEqual(["⚙️", "code step"]);
    expect(s.stages[2].agents[0].role).toBe("harness");
  });

  it("take their durations from the start and end lines, parallel agents counted once", () => {
    const s = buildStory({ events: ev(ROWS), workflow: WF, nodes: { ...NODES }, logs: LOGS });
    expect(s.stages[0].durationMs).toBe(2000);
    expect(s.stages[1].agents.map((a) => a.durationMs)).toEqual([4000, 3000]);
    expect(s.stages[1].durationMs).toBe(4000);
    expect(s.totals).toMatchObject({ durationMs: 210_000, calls: 6, gates: 1, waitMs: 200_000 });
    expect(s.totals.costUsd).toBeCloseTo(0.0034, 10);
  });

  it("fall back to the events' own times without log lines, and say nothing for a step still running", () => {
    const s = buildStory({ events: ev(ROWS), workflow: WF, nodes: { ...NODES, checker: { status: "running" } } });
    expect(s.stages[1].agents.map((a) => a.durationMs)).toEqual([3000, null]);
    expect(s.totals.durationMs).toBe(2800 + 3000 + 1200);
  });

  it("put an agent the workflow does not list in its own section", () => {
    const s = buildStory({ events: ev([row({ agent_id: "ghost" })]), workflow: WF });
    expect(s.stages.at(-1)).toMatchObject({ key: "extra", title: "Other agents" });
    expect(s.stages.at(-1)!.agents[0].name).toBe("ghost");
  });

  it("take the worst agent status as the stage's", () => {
    expect(stageStatus(["done", "failed"])).toBe("failed");
    expect(stageStatus(["done", "waiting_human"])).toBe("waiting_human");
    expect(stageStatus(["done", "skipped"])).toBe("done");
    expect(stageStatus(["done", "pending"])).toBe("running");
    expect(stageStatus(["pending", "pending"])).toBe("pending");
  });
});

describe("lines", () => {
  it("are one short sentence per event with an emoji, time and cost", () => {
    const s = buildStory({ events: ev(ROWS), workflow: WF, nodes: { ...NODES }, logs: LOGS });
    const r = s.stages[1].agents[0].lines;
    expect(r.map((l) => [l.emoji, l.text, l.meta.join(" · ")])).toEqual([
      ["📚", "Searched warranty (Knowledge Base)", "1.2 s · $0.0004"],
      ["💬", "Asked Claude Haiku 4.5", "1.8 s · $0.0020"],
      ["📜", "Cedar policy noSecrets: DENY, call refused", ""],
    ]);
    expect(r[1].title).toContain("us.anthropic.claude-haiku-4-5-20251001-v1:0");
    expect(r[2]).toMatchObject({ tone: "denied", problem: true });
    expect(s.stages[1].agents[0].problem).toBe(true);
    expect(texts(stageLines(s.stages[0]))).toEqual(["Ran code in Code Interpreter"]);
    expect(stageLines(s.stages[0])[0].emoji).toBe("🧮");
  });

  it("give each line the replay position of its event", () => {
    const s = buildStory({ events: ev(ROWS), workflow: WF, logs: LOGS });
    expect(s.stages[1].agents[0].lines.map((l) => l.at)).toEqual([2, 3, 4]);
  });

  it("word every kind a reader meets", () => {
    const w = (o: Partial<FlowRow>) => eventWords(ev([row(o)])[0], WF).text;
    expect(w({ kind: "guardrail", label: "OUTPUT", status: "blocked" })).toBe("Guardrail checked output: blocked");
    expect(w({ kind: "guardrail", label: "INPUT", status: "anonymized" })).toBe("Guardrail checked input: masked sensitive details");
    expect(w({ kind: "memory", label: "recall" })).toBe("Recalled from memory");
    expect(w({ kind: "eval", label: "Builtin.Helpfulness" })).toBe("Scored by Helpfulness");
    expect(w({ label: "orders___get_order" })).toBe("Called orders · get_order (Lambda function)");
    expect(w({ label: "orders", status: "error" })).toBe("Call to orders failed");
    expect(w({ label: "mystery" })).toBe("Called mystery");
    expect(w({ kind: "policy", label: "warranty", status: "allowed" })).toBe("Cedar policy on warranty: ALLOW");
    expect(w({ agent_id: "prep", label: "textract", mode: "textract" })).toBe("Read a document with Textract");
    expect(w({ kind: "person", label: "refund", status: "waiting" })).toBe("Asked HITL to approve refund");
    expect(w({ kind: "hologram" })).toBe("Recorded an event (warranty)");
  });

  it("say how long an agent's own question waited for HITL", () => {
    const s = buildStory({ events: ev([
      row({ kind: "person", label: "question", status: "asked", ts: "2025-01-01 10:00:00", latency_ms: 0, cost_usd: 0 }),
      row({ kind: "person", label: "question", status: "answered", ts: "2025-01-01 10:03:20", seq: 2, latency_ms: 0, cost_usd: 0 }),
    ]), workflow: WF });
    const lines = s.stages[1].agents[0].lines;
    expect(lines.map((l) => [l.emoji, l.text, l.meta.join(" · ")])).toEqual([
      ["⏸", "Asked HITL a question", ""], ["⏸", "HITL answered", "waited 3 m 20 s"],
    ]);
  });
});

describe("HITL review gates", () => {
  it("are their own line with the wait and the decision, after the stage's events", () => {
    const s = buildStory({ events: ev(ROWS), workflow: WF, logs: LOGS });
    const lines = stageLines(s.stages[2]);
    expect(texts(lines)).toEqual(["Saved to memory", "HITL review after Writer: waited 3 m 20 s · approved"]);
    expect(lines[1]).toMatchObject({ type: "gate", emoji: "⏸", kind: "person", tone: "ok", pendingText: "HITL review after Writer: waiting" });
    // shown with the event before it, decided from the event before the decision
    expect(lines[1].at).toBe(7);
    expect(lines[1].doneAt).toBe(7);
  });

  it("say waiting now for the gate the run waits at, and nothing made up for one it left", () => {
    const req = [log("10:00:10", "writer", "Awaiting human review after writer")];
    const now = buildStory({ events: ev(ROWS), workflow: WF, logs: req, hitl: { node: "writer" } });
    expect(texts(now.stages[2].lines)).toEqual(["HITL review after Writer: waiting now"]);
    const left = buildStory({ events: ev(ROWS), workflow: WF, logs: req });
    expect(left.stages[2].lines[0]).toMatchObject({ text: "HITL review after Writer: no decision recorded", tone: "skipped" });
    const noLog = buildStory({ events: ev(ROWS), workflow: WF, hitl: { node: "writer" } });
    expect(texts(noLog.stages[2].lines)).toEqual(["HITL review after Writer: waiting now"]);
    // an agent asking mid-step is an event, not a gate
    expect(buildStory({ events: ev(ROWS), workflow: WF, hitl: { node: "writer", kind: "question" } }).stages[2].lines).toHaveLength(0);
  });

  it("name a group gate by its gate name, and say when it approved itself", () => {
    const wf = { ...WF, steps: [{ sequence: ["researcher", "checker"], gateId: "g1", gateName: "Draft" }] } as unknown as Workflow;
    const s = buildStory({ events: ev([row({})]), workflow: wf, logs: [
      log("10:00:02", "g1", "Draft: approved automatically, the score was high"),
      log("10:00:02", "g1", "Draft review: approve"),
    ] });
    expect(s.stages[0].title).toBe("2 agents in sequence");
    expect(texts(s.stages[0].lines)).toEqual(["HITL review Draft: approved automatically"]);
  });

  it("read the gate lines and ignore the others", () => {
    const items = parseLogs([...LOGS, log("10:00:02", "researcher", "Tool call 1/8: lookup()")], WF);
    expect(items.map((i) => i.t)).toEqual(["start", "end", "start", "start", "end", "end", "start", "end", "gateReq", "gateRes"]);
  });
});

describe("re-runs", () => {
  it("start with a v2 header carrying the reviewer's note from the history", () => {
    const rows = [row({ agent_id: "writer", kind: "llm", label: "amazon.nova-lite-v1:0", version: 1 }),
      row({ agent_id: "writer", kind: "llm", label: "amazon.nova-lite-v1:0", version: 2, seq: 2, ts: "2025-01-01 10:05:00" })];
    const s = buildStory({
      events: ev(rows), workflow: WF,
      history: { writer: [{ version: 1, at: "x" }, { version: 2, at: "y", comment: "shorter please" }] },
      logs: [log("10:02:00", "writer", "Awaiting human review after writer"), log("10:04:00", "writer", "Human decision (writer): revise — note: \"shorter please\"")],
    });
    const lines = stageLines(s.stages[2]);
    expect(lines.map((l) => [l.type, l.text])).toEqual([
      ["event", "Asked Nova Lite"],
      ["gate", "HITL review after Writer: waited 2 m · revise"],
      ["version", "v2 after review: shorter please"],
      ["event", "Asked Nova Lite"],
    ]);
  });
});

describe("filters", () => {
  const s = buildStory({ events: ev(ROWS), workflow: WF, nodes: { ...NODES }, logs: LOGS });
  it("keep only problems, dropping the agents and stages with none", () => {
    const f = filterStory(s, "problems");
    expect(f.stages.map((x) => x.key)).toEqual(["s1"]);
    expect(f.stages[0].agents.map((a) => a.id)).toEqual(["researcher"]);
    expect(lineCount(f)).toBe(1);
  });
  it("keep one kind; a single-agent stage keeps its agent for the header", () => {
    const f = filterStory(s, "person");
    expect(f.stages.map((x) => x.key)).toEqual(["s2"]);
    expect(f.stages[0].agents[0].lines).toHaveLength(0);
    expect(texts(stageLines(f.stages[0]))).toEqual(["HITL review after Writer: waited 3 m 20 s · approved"]);
    expect(filterStory(s, "all")).toBe(s);
  });
  it("offer chips for the kinds this run has, in a fixed order", () => {
    expect(s.kinds).toEqual(["llm", "tool", "policy", "guardrail", "memory", "person", "agentcore"]);
  });
});

describe("model names", () => {
  it("are short and friendly", () => {
    expect(modelName("us.anthropic.claude-haiku-4-5-20251001-v1:0")).toBe("Claude Haiku 4.5");
    expect(modelName("anthropic.claude-3-5-haiku-20241022-v1:0")).toBe("Claude 3.5 Haiku");
    expect(modelName("global.anthropic.claude-sonnet-4-20250514-v1:0")).toBe("Claude Sonnet 4");
    expect(modelName("amazon.nova-lite-v1:0")).toBe("Nova Lite");
    expect(modelName("us.amazon.nova-2-lite-v1:0")).toBe("Nova 2 Lite");
    expect(modelName("meta.llama3-1-70b-instruct-v1:0")).toBe("Llama 3.1 70B");
    expect(modelName("openai/gpt-4o-mini")).toBe("GPT-4o mini");
    expect(modelName("gemini-2.5-flash")).toBe("Gemini 2.5 Flash");
    expect(modelName("arn:aws:bedrock:us-east-1:123456789012:inference-profile/us.amazon.nova-pro-v1:0")).toBe("Nova Pro");
    expect(modelName("")).toBe("the model");
  });
});

describe("scale", () => {
  it("lays out 12 parallel agents and 300 events; more than four agents start collapsed", () => {
    const agents = Object.fromEntries(Array.from({ length: 12 }, (_, i) => [`a${i}`, { name: `Agent ${i}` }]));
    const wf = { agents, steps: [{ parallel: Object.keys(agents) }] } as unknown as Workflow;
    const rows = Array.from({ length: 300 }, (_, i) => row({
      agent_id: `a${i % 12}`, seq: i, ts: `2025-01-01 10:${String(Math.floor(i / 60)).padStart(2, "0")}:${String(i % 60).padStart(2, "0")}`,
    }));
    const s = buildStory({ events: ev(rows), workflow: wf });
    expect(s.stages[0].agents).toHaveLength(12);
    expect(lineCount(s)).toBe(300);
    expect(s.stages[0].agents.map((a) => a.num).slice(0, 3)).toEqual(["1a", "1b", "1c"]);
  });
});
