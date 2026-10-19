/** The Flow tab's Story view, through RunFlow (it owns the data and the replay): what a
 *  reader sees, filters, opens, replays and drives by keyboard. The layout itself is tested
 *  in lib/story.test.ts. */
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { FlowResponse, FlowRow } from "../lib/flow";
import type { HistoryEntry, LogLine, NodeState, Workflow } from "../types";
import { RunFlow } from "./RunFlow";

const PARALLEL = ["researcher", "checker"];
const wf = (parallel = PARALLEL) => ({
  agents: {
    researcher: { name: "Researcher", components: [{ kind: "tool", name: "warranty", tool: "warranty", detail: "Knowledge Base" }] },
    checker: { name: "Checker" }, writer: { name: "Writer" },
    ...Object.fromEntries(parallel.filter((id) => !PARALLEL.includes(id)).map((id) => [id, { name: id.toUpperCase() }])),
  },
  steps: [{ parallel }, { agent: "writer", hitl: true }],
}) as unknown as Workflow;
const NODES: Record<string, NodeState> = { researcher: { status: "done" }, checker: { status: "done" }, writer: { status: "done" } };

const row = (o: Partial<FlowRow>): FlowRow => ({
  agent_id: "researcher", kind: "tool", label: "warranty", status: "ok", mode: "gateway",
  latency_ms: 1200, cost_usd: 0.0004, version: 1, ts: "2025-01-01 10:00:00", seq: 1, ...o,
});
const ROWS: FlowRow[] = [
  row({ seq: 1, ts: "2025-01-01 10:00:01" }),
  row({ kind: "llm", label: "us.anthropic.claude-haiku-4-5-20251001-v1:0", seq: 2, ts: "2025-01-01 10:00:02", latency_ms: 1800, cost_usd: 0.002 }),
  row({ agent_id: "checker", kind: "guardrail", label: "INPUT", status: "blocked", seq: 3, ts: "2025-01-01 10:00:03", cost_usd: 0 }),
  row({ agent_id: "writer", kind: "memory", label: "store", seq: 4, ts: "2025-01-01 10:00:10", cost_usd: 0 }),
];
const LOGS: LogLine[] = [
  { ts: "2025-01-01 10:00:00", node: "researcher", msg: "Researcher started" },
  { ts: "2025-01-01 10:00:04", node: "researcher", msg: "Researcher complete (v1)" },
  { ts: "2025-01-01 10:00:11", node: "writer", msg: "Awaiting human review after writer" },
  { ts: "2025-01-01 10:02:21", node: "writer", msg: "Human decision (writer): approve" },
];
const flow = (rows: FlowRow[]): FlowResponse => ({ session_id: "s1", events: rows, capped: false, max: 2000 });

async function mount({ rows = ROWS, logs = LOGS, workflow = wf(), history }: {
  rows?: FlowRow[]; logs?: LogLine[]; workflow?: Workflow; history?: Record<string, HistoryEntry[]>;
} = {}) {
  const load = vi.fn().mockResolvedValue(flow(rows));
  const view = render(<RunFlow sessionId="s1" workflow={workflow} nodes={NODES} refreshKey="a" load={load} logs={logs} history={history} />);
  await waitFor(() => expect(screen.queryByText(/Reading this run/)).toBeNull());
  return { load, ...view };
}
const line = (key: string) => screen.getByTestId(`story-line-${key}`);
const lines = () => screen.queryAllByTestId(/^story-line-/);
const slider = () => screen.getByRole("slider", { name: /Replay position/ });

function reducedMotion(on: boolean) {
  Object.defineProperty(window, "matchMedia", {
    configurable: true, writable: true,
    value: (q: string) => ({
      matches: on && q.includes("prefers-reduced-motion"), media: q,
      addEventListener: () => {}, removeEventListener: () => {},
    }),
  });
}
beforeEach(() => reducedMotion(false));
afterEach(() => { cleanup(); vi.useRealTimers(); });

describe("the Story view", () => {
  it("is the Flow tab's default, with Summary one click away", async () => {
    await mount();
    expect(screen.getByTestId("run-story")).toBeTruthy();
    expect(screen.queryByTestId("flow-agent-researcher")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Summary" }));
    expect(screen.queryByTestId("run-story")).toBeNull();
    expect(screen.getByTestId("flow-agent-researcher")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Story" }));
    expect(screen.getByTestId("run-story")).toBeTruthy();
  });

  it("numbers each stage like the Graph view and lists a parallel stage's agents", async () => {
    await mount();
    const s0 = screen.getByTestId("story-stage-s0");
    expect(s0.textContent).toContain("2 agents in parallel");
    expect(within(s0).getByRole("img", { name: "Step 1, done" })).toBeTruthy();
    expect(within(s0).getByRole("img", { name: "Step 1a, done" })).toBeTruthy();
    expect(screen.getByTestId("story-agent-researcher").textContent).toMatch(/Researcher.*Succeeded.*4 s/);
    const s1 = screen.getByTestId("story-stage-s1");
    expect(within(s1).getByRole("img", { name: "Step 2, done" })).toBeTruthy();
    expect(s1.textContent).toContain("Writer");
  });

  it("tells each event in one short line with its time and cost, and the totals on top", async () => {
    await mount();
    expect(line("e0").textContent).toBe("📚Searched warranty (Knowledge Base)1.2 s · $0.0004");
    expect(line("e1").textContent).toContain("Asked Claude Haiku 4.5");
    expect(line("e1").getAttribute("title")).toContain("us.anthropic.claude-haiku-4-5-20251001-v1:0");
    expect(line("e2").closest("li")!.className).toContain("st-problem");
    expect(line("e2").closest("li")!.className).toContain("st-tone-blocked");
    const totals = screen.getByRole("list", { name: "Run totals" }).textContent;
    expect(totals).toContain("Took 2 m 21 s");
    expect(totals).toContain("Cost $0.0024");
    expect(totals).toContain("4 calls");
    expect(totals).toContain("1 HITL review, waited 2 m 10 s");
  });

  it("shows a HITL gate as its own line with the wait and the decision", async () => {
    await mount();
    expect(within(screen.getByTestId("story-stage-s1")).getByText("HITL review after Writer: waited 2 m 10 s · approved")).toBeTruthy();
  });

  it("opens a line's detail under it by click or Enter, and closes it", async () => {
    await mount();
    fireEvent.click(line("e2"));
    const detail = screen.getByTestId("flow-detail");
    expect(detail.textContent).toContain("The guardrail blocked the input to Checker.");
    expect(detail.textContent).toContain("Latency 1.2 s · Cost $0");
    expect(line("e2").getAttribute("aria-expanded")).toBe("true");
    expect(line("e2").closest("li")!.contains(detail)).toBe(true);
    fireEvent.click(line("e1"));
    expect(screen.getByTestId("flow-detail").textContent).toContain("Model us.anthropic.claude-haiku-4-5-20251001-v1:0");
    fireEvent.click(line("e1"));
    expect(screen.queryByTestId("flow-detail")).toBeNull();
  });

  it("collapses agents beyond four, but opens one with a problem", async () => {
    const many = ["researcher", "checker", "a3", "a4", "a5"];
    await mount({ workflow: wf(many), rows: [...ROWS, row({ agent_id: "a3", seq: 9, ts: "2025-01-01 10:00:05" })] });
    const head = (id: string) => within(screen.getByTestId(`story-agent-${id}`)).getAllByRole("button")[0];
    expect(head("researcher").getAttribute("aria-expanded")).toBe("false");
    expect(head("checker").getAttribute("aria-expanded")).toBe("true");      // its guardrail blocked
    expect(head("a3").getAttribute("aria-expanded")).toBe("false");
    fireEvent.click(head("a3"));
    expect(head("a3").getAttribute("aria-expanded")).toBe("true");
    expect(screen.getByTestId("story-agent-a3").textContent).toContain("Searched warranty");
  });

  it("names a re-run with the reviewer's note", async () => {
    const rows = [row({ agent_id: "writer", kind: "llm", label: "amazon.nova-lite-v1:0", version: 1 }),
      row({ agent_id: "writer", kind: "llm", label: "amazon.nova-lite-v1:0", version: 2, seq: 2, ts: "2025-01-01 10:05:00" })];
    await mount({ rows, logs: [], history: { writer: [{ version: 1, at: "a" }, { version: 2, at: "b", comment: "shorter please" }] } });
    expect(screen.getByTestId("story-v-writer-2").textContent).toContain("v2 after review: shorter please");
  });
});

describe("filters", () => {
  it("show problems only, one kind, or everything, and say how many lines are shown", async () => {
    await mount();
    expect(lines()).toHaveLength(5);
    fireEvent.click(screen.getByRole("button", { name: "Problems only" }));
    expect(lines().map((l) => l.getAttribute("data-testid"))).toEqual(["story-line-e2"]);
    await waitFor(() => expect(document.body.textContent).toContain("1 line shown"));
    fireEvent.click(screen.getByRole("button", { name: "HITL" }));
    expect(lines()).toHaveLength(1);
    expect(lines()[0].textContent).toContain("HITL review");
    fireEvent.click(screen.getByRole("button", { name: "All" }));
    expect(lines()).toHaveLength(5);
  });
});

describe("replaying the story", () => {
  it("shows only what has happened and highlights the current line", async () => {
    await mount();
    fireEvent.change(slider(), { target: { value: "0" } });
    expect(lines()).toHaveLength(0);
    fireEvent.change(slider(), { target: { value: "2" } });
    expect(lines().map((l) => l.getAttribute("data-testid"))).toEqual(["story-line-e0", "story-line-e1"]);
    expect(line("e1").closest("li")!.className).toContain("st-current");
    // the replay's event is the selected one, with its detail open
    expect(screen.getByTestId("flow-detail").textContent).toContain("Researcher asked the model");
    // mid-replay a step shows what the events so far support
    expect(within(screen.getByTestId("story-stage-s1")).getByRole("img", { name: "Step 2, not started" })).toBeTruthy();
  });

  it("scrolls the current line into view as the replay plays", async () => {
    const seen: string[] = [];
    const spy = vi.fn(function (this: HTMLElement) { seen.push(this.getAttribute("data-testid") ?? ""); });
    Object.defineProperty(HTMLElement.prototype, "scrollIntoView", { configurable: true, writable: true, value: spy });
    await mount();
    fireEvent.change(slider(), { target: { value: "0" } });
    vi.useFakeTimers();
    fireEvent.click(screen.getByRole("button", { name: "Play" }));
    act(() => { vi.advanceTimersByTime(900); });
    expect(lines()).toHaveLength(1);
    act(() => { vi.advanceTimersByTime(900); });
    expect(lines()).toHaveLength(2);
    expect(seen.filter((k) => k.startsWith("story-line-"))).toEqual(["story-line-e0", "story-line-e1"]);
    delete (HTMLElement.prototype as { scrollIntoView?: unknown }).scrollIntoView;
  });

  it("moves between lines with the arrow keys", async () => {
    await mount();
    line("e0").focus();
    fireEvent.keyDown(line("e0"), { key: "ArrowDown" });
    expect(document.activeElement).toBe(line("e1"));
    fireEvent.keyDown(line("e1"), { key: "ArrowDown" });
    // the next stop is the next agent's header
    expect(document.activeElement?.closest("[data-testid]")?.getAttribute("data-testid")).toBe("story-agent-checker");
    fireEvent.keyDown(document.activeElement!, { key: "ArrowUp" });
    expect(document.activeElement).toBe(line("e1"));
  });

  it("keeps stepping with reduced motion", async () => {
    reducedMotion(true);
    await mount();
    expect((screen.getByRole("button", { name: "Play" }) as HTMLButtonElement).disabled).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "Step back" }));
    expect(lines()).toHaveLength(3);      // the writer's line and the gate after it are not yet
    await waitFor(() => expect(document.body.textContent).toContain("Event 3 of 4: The guardrail blocked the input to Checker."));
  });

  it("adds lines as a live run records more", async () => {
    const load = vi.fn().mockResolvedValueOnce(flow(ROWS.slice(0, 2))).mockResolvedValueOnce(flow(ROWS));
    const { rerender } = render(<RunFlow sessionId="s1" workflow={wf()} nodes={NODES} refreshKey="a" load={load} logs={LOGS} />);
    await waitFor(() => expect(lines()).toHaveLength(3));    // two events and the gate after them
    rerender(<RunFlow sessionId="s1" workflow={wf()} nodes={NODES} refreshKey="b" load={load} logs={LOGS} />);
    await waitFor(() => expect(lines()).toHaveLength(5));
  });
});
