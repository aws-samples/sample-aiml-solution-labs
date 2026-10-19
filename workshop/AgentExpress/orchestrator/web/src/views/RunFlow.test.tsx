/** The Flow tab: quiet until asked, one sentence per event, and a replay that can be driven
 *  by keyboard alone. The event model and the reducer are tested in lib/flow.test.ts; this file
 *  tests what a reader sees and can do. */
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { FlowResponse, FlowRow } from "../lib/flow";
import type { NodeState, Workflow } from "../types";
import { RunFlow } from "./RunFlow";

const WORKFLOW = {
  agents: { researcher: { name: "Researcher" }, writer: { name: "Writer" } },
  steps: [{ agent: "researcher" }, { agent: "writer" }],
} as unknown as Workflow;
const NODES: Record<string, NodeState> = {
  researcher: { status: "done" }, writer: { status: "failed" },
};

const row = (o: Partial<FlowRow>): FlowRow => ({
  agent_id: "researcher", kind: "tool", label: "docs", status: "ok", mode: "gateway",
  latency_ms: 120, cost_usd: 0.0012, version: 1, ts: "2025-01-01 10:00:00", seq: 1, ...o,
});
const ROWS: FlowRow[] = [
  row({ kind: "policy", label: "search_documentation", status: "allowed", seq: 1, ts: "2025-01-01 10:00:01" }),
  row({ kind: "tool", label: "docs", seq: 2, ts: "2025-01-01 10:00:02" }),
  row({ kind: "policy", label: "delete_everything", status: "denied", seq: 3, ts: "2025-01-01 10:00:03" }),
  row({ agent_id: "writer", kind: "guardrail", label: "INPUT", status: "blocked", seq: 4, ts: "2025-01-01 10:00:04", cost_usd: 0 }),
  row({ agent_id: "__session__", kind: "agentcore", label: "runtime-compute", latency_ms: 2400, seq: 5, ts: "2025-01-01 10:00:05" }),
];
const flow = (rows: FlowRow[], extra: Partial<FlowResponse> = {}): FlowResponse =>
  ({ session_id: "s1", events: rows, capped: false, max: 2000, ...extra });

async function mount(rows: FlowRow[] = ROWS, extra: Partial<FlowResponse> = {}) {
  const load = vi.fn().mockResolvedValue(flow(rows, extra));
  // These tests are about the Summary view; the Story view has its own (RunStory.test.tsx).
  const view = render(<RunFlow sessionId="s1" workflow={WORKFLOW} nodes={NODES} refreshKey="a" load={load} defaultView="summary" />);
  await waitFor(() => expect(screen.queryByText(/Reading this run/)).toBeNull());
  return { load, ...view };
}

const ID: Record<string, string> = { Researcher: "researcher", Writer: "writer", "The run": "run" };
/** An agent's box: the first button in its block (the marks are the buttons after it). */
const box = (name: string) => within(screen.getByTestId(`flow-agent-${ID[name]}`)).getAllByRole("button")[0];
const mark = (sentence: RegExp) => screen.getByRole("button", { name: sentence });
const position = () => screen.getByText(/^Event \d+ of \d+$/).textContent;

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

describe("collapsed by default", () => {
  it("shows the agents in the run's order with their status and counts, and no event marks", async () => {
    await mount();
    const agents = screen.getAllByTestId(/^flow-agent-/);
    expect(agents.map((a) => a.getAttribute("data-testid")))
      .toEqual(["flow-agent-researcher", "flow-agent-writer", "flow-agent-run"]);
    expect(agents[0].textContent).toMatch(/Researcher.*Succeeded.*3 events · 1 stopped/);
    expect(agents[1].textContent).toMatch(/Writer.*Failed.*1 event · 1 stopped/);
    expect(box("Researcher").getAttribute("aria-expanded")).toBe("false");
    expect(screen.queryAllByTestId(/^mark-/)).toHaveLength(0);
    expect(screen.queryByTestId("flow-detail")).toBeNull();
  });

  it("lists the AgentCore pieces once, on one rail, with how many were stopped", async () => {
    await mount();
    const rail = screen.getByRole("complementary", { name: /AgentCore components/ });
    expect(within(rail).getByText("Policy").parentElement?.textContent).toMatch(/2 · 1 stopped/);
    expect(within(rail).getByText("Gateway")).toBeTruthy();
    expect(within(rail).getByText("Guardrails")).toBeTruthy();
    expect(within(rail).getByText("Runtime")).toBeTruthy();
    expect(within(rail).queryByText("Memory")).toBeNull();     // not used by this run
  });
});

describe("expanding an agent", () => {
  it("shows its events as marks on an edge to each piece it used, and only that agent's", async () => {
    await mount();
    fireEvent.click(box("Researcher"));
    expect(screen.getByRole("group", { name: "Researcher to Gateway, 1 event" })).toBeTruthy();
    expect(screen.getByRole("group", { name: "Researcher to Policy, 2 events" })).toBeTruthy();
    expect(screen.getAllByTestId(/^mark-/)).toHaveLength(3);
    expect(box("Researcher").getAttribute("aria-expanded")).toBe("true");
    expect(box("Writer").getAttribute("aria-expanded")).toBe("false");
    fireEvent.click(box("Researcher"));
    expect(screen.queryAllByTestId(/^mark-/)).toHaveLength(0);
  });

  it("puts run-level events (runtime compute) in their own box", async () => {
    await mount();
    fireEvent.click(box("The run"));
    expect(mark(/AgentCore Runtime was active for 2.4 s/)).toBeTruthy();
  });

  it("shows an event whose kind it does not know, as a generic event", async () => {
    await mount([row({ kind: "hologram", label: "x", status: "ok" })]);
    fireEvent.click(box("Researcher"));
    expect(mark(/recorded a `hologram` event/)).toBeTruthy();
  });
});

describe("selecting an event", () => {
  it("shows its sentence, latency and cost, and nothing else", async () => {
    await mount();
    fireEvent.click(box("Researcher"));
    fireEvent.click(mark(/Cedar denied `delete_everything`/));
    const detail = screen.getByTestId("flow-detail");
    expect(detail.textContent).toContain("Cedar denied `delete_everything` for Researcher, so the Gateway refused the call.");
    expect(detail.textContent).toContain("Latency 120 ms · Cost $0.0012");
    expect(detail.textContent).not.toMatch(/allowed/);
    expect(mark(/Cedar denied/).getAttribute("aria-pressed")).toBe("true");
    fireEvent.click(mark(/Cedar allowed `search_documentation`/));
    expect(screen.getByTestId("flow-detail").textContent).toContain("Cedar allowed `search_documentation` for Researcher.");
  });

  it("says a blocked guardrail was blocked", async () => {
    await mount();
    fireEvent.click(box("Writer"));
    fireEvent.click(mark(/guardrail blocked/));
    expect(screen.getByTestId("flow-detail").textContent).toContain("The guardrail blocked the input to Writer.");
    expect(screen.getByTestId("flow-detail").textContent).toContain("Cost $0");
  });
});

describe("replay", () => {
  it("starts at the end and steps back and forward one event at a time", async () => {
    await mount();
    expect(position()).toBe("Event 5 of 5");
    expect((screen.getByRole("button", { name: "Step forward" }) as HTMLButtonElement).disabled).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "Step back" }));
    expect(position()).toBe("Event 4 of 5");
    // the event the replay is at is selected, and its agent opens
    expect(screen.getByTestId("flow-detail").textContent).toContain("The guardrail blocked the input to Writer.");
    expect(box("Writer").getAttribute("aria-expanded")).toBe("true");
    fireEvent.click(screen.getByRole("button", { name: "Step forward" }));
    expect(position()).toBe("Event 5 of 5");
  });

  it("scrubs to a position, and at the start nothing has happened yet", async () => {
    await mount();
    const slider = screen.getByRole("slider", { name: /Replay position/ });
    fireEvent.change(slider, { target: { value: "0" } });
    expect(position()).toBe("Event 0 of 5");
    expect(screen.queryByTestId("flow-detail")).toBeNull();
    expect((screen.getByRole("button", { name: "Step back" }) as HTMLButtonElement).disabled).toBe(true);
    // before an agent's first event it has not started, whatever the run later did
    expect(box("Writer").textContent).toMatch(/Not started/);
    fireEvent.change(slider, { target: { value: "3" } });
    expect(position()).toBe("Event 3 of 5");
    expect(screen.getByTestId("flow-detail").textContent).toContain("Cedar denied");
    // mid-run the writer has no events yet, so it is still not started; the researcher is done with its events
    expect(box("Writer").textContent).toMatch(/Not started/);
    expect(box("Researcher").textContent).toMatch(/Succeeded/);
  });

  it("hides events that have not happened yet", async () => {
    await mount();
    fireEvent.click(box("Researcher"));
    expect(screen.getAllByTestId(/^mark-/)).toHaveLength(3);
    fireEvent.change(screen.getByRole("slider", { name: /Replay position/ }), { target: { value: "1" } });
    expect(screen.getAllByTestId(/^mark-/)).toHaveLength(1);
    expect(screen.getByText(/1 event$/, { selector: ".sfn-state-sub" })).toBeTruthy();
  });

  it("plays one event per tick, twice as fast at 2x, and stops at the end", async () => {
    await mount();
    fireEvent.change(screen.getByRole("slider", { name: /Replay position/ }), { target: { value: "0" } });
    vi.useFakeTimers();
    fireEvent.click(screen.getByRole("button", { name: "Play" }));
    expect(screen.getByRole("button", { name: "Pause" })).toBeTruthy();
    act(() => { vi.advanceTimersByTime(899); });
    expect(position()).toBe("Event 0 of 5");
    act(() => { vi.advanceTimersByTime(1); });
    expect(position()).toBe("Event 1 of 5");
    fireEvent.click(screen.getByRole("button", { name: "2x" }));
    act(() => { vi.advanceTimersByTime(450); });
    expect(position()).toBe("Event 2 of 5");
    // each tick is scheduled by the render the previous tick caused, so advance one at a time
    for (let i = 0; i < 3; i++) act(() => { vi.advanceTimersByTime(450); });
    expect(position()).toBe("Event 5 of 5");
    expect(screen.getByRole("button", { name: "Play" })).toBeTruthy();       // it stopped by itself
    act(() => { vi.advanceTimersByTime(5000); });
    expect(position()).toBe("Event 5 of 5");
  });

  it("pauses when asked", async () => {
    await mount();
    fireEvent.change(screen.getByRole("slider", { name: /Replay position/ }), { target: { value: "0" } });
    vi.useFakeTimers();
    fireEvent.click(screen.getByRole("button", { name: "Play" }));
    act(() => { vi.advanceTimersByTime(900); });
    fireEvent.click(screen.getByRole("button", { name: "Pause" }));
    act(() => { vi.advanceTimersByTime(5000); });
    expect(position()).toBe("Event 1 of 5");
  });

  it("announces the event it is at to a screen reader", async () => {
    await mount();
    fireEvent.click(screen.getByRole("button", { name: "Step back" }));
    await waitFor(() => expect(document.body.textContent).toContain("Event 4 of 5: The guardrail blocked the input to Writer."));
  });
});

describe("reduced motion", () => {
  it("turns automatic playback off and leaves stepping and scrubbing", async () => {
    reducedMotion(true);
    await mount();
    expect((screen.getByRole("button", { name: "Play" }) as HTMLButtonElement).disabled).toBe(true);
    expect(screen.getByText(/reduced motion/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Step back" }));
    expect(position()).toBe("Event 4 of 5");
    fireEvent.change(screen.getByRole("slider", { name: /Replay position/ }), { target: { value: "2" } });
    expect(position()).toBe("Event 2 of 5");
  });

  it("does not show the note when motion is allowed", async () => {
    await mount();
    expect(screen.queryByText(/reduced motion/)).toBeNull();
  });
});

describe("the data", () => {
  it("says so when the run was cut short", async () => {
    await mount(ROWS, { capped: true, max: 2000 });
    expect(screen.getByText(/The earliest 2,000 are shown/)).toBeTruthy();
  });

  it("says what an empty run is, and offers no replay", async () => {
    await mount([]);
    expect(screen.getByText(/No events recorded for this run yet/)).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Play" })).toBeNull();
  });

  it("shows a failed read as an error", async () => {
    const load = vi.fn().mockRejectedValue(new Error("unknown session"));
    render(<RunFlow sessionId="s1" workflow={WORKFLOW} nodes={NODES} load={load} />);
    expect(await screen.findByText("unknown session")).toBeTruthy();
  });

  it("reads the events again when the run changes, and stays at the end", async () => {
    const load = vi.fn()
      .mockResolvedValueOnce(flow(ROWS.slice(0, 2)))
      .mockResolvedValueOnce(flow(ROWS));
    const { rerender } = render(<RunFlow sessionId="s1" workflow={WORKFLOW} nodes={NODES} refreshKey="a" load={load} />);
    await waitFor(() => expect(position()).toBe("Event 2 of 2"));
    rerender(<RunFlow sessionId="s1" workflow={WORKFLOW} nodes={NODES} refreshKey="b" load={load} />);
    await waitFor(() => expect(position()).toBe("Event 5 of 5"));
    expect(load).toHaveBeenCalledTimes(2);
  });
});
