/** The Timeline tab's chart: what a reader sees and can do. The layout (rows, bars, calls,
 *  critical path, compressed waits) is tested in lib/gantt.test.ts. */
import { act, cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { toEpochMs } from "../lib/clock";
import { buildEvents, type FlowRow } from "../lib/flow";
import type { SessionSnapshot, Workflow } from "../types";
import { RunGantt } from "./RunGantt";

const WF = {
  agents: { researcher: { name: "Researcher" }, writer: { name: "Writer" } },
  steps: [{ agent: "researcher", hitl: true }, { agent: "writer" }],
} as unknown as Workflow;
const ts = (s: number) => `2025-01-06 10:${String(Math.floor(s / 60)).padStart(2, "0")}:${String(s % 60).padStart(2, "0")}`;
const SNAP: SessionSnapshot = {
  session_id: "s1", topic: "t", overall: "done", created: ts(0),
  nodes: { researcher: { status: "done" }, writer: { status: "done" } },
  logs: [
    { ts: ts(0), node: "researcher", msg: "Researcher started" },
    { ts: ts(20), node: "researcher", msg: "Researcher complete (v1)" },
    { ts: ts(20), node: "researcher", msg: "Awaiting human review after researcher" },
    { ts: ts(220), node: "researcher", msg: "Human decision (researcher): approve" },
    { ts: ts(221), node: "writer", msg: "Writer started" },
    { ts: ts(240), node: "writer", msg: "Writer complete (v1)" },
  ],
};
const ROWS: FlowRow[] = [
  { agent_id: "researcher", kind: "tool", label: "docs", status: "ok", latency_ms: 8000, ts: ts(10), seq: 1, cost_usd: 0.002 },
  { agent_id: "researcher", kind: "llm", label: "claude", status: "ok", latency_ms: 3000, ts: ts(19), seq: 2, cost_usd: 0.0123 },
  { agent_id: "writer", kind: "guardrail", label: "OUTPUT", status: "blocked", latency_ms: 400, ts: ts(239), seq: 3 },
];
const EVENTS = buildEvents(ROWS);

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

const mount = (snap: SessionSnapshot = SNAP, events: typeof EVENTS | null = EVENTS, now?: number) =>
  render(<RunGantt snap={snap} workflow={WF} events={events} now={now} />);

describe("the run's Gantt chart", () => {
  it("draws one row per agent with a focusable, labelled bar and its calls", () => {
    mount();
    expect(screen.getAllByTestId(/^gantt-row-/).map((r) => r.getAttribute("data-testid")))
      .toEqual(["gantt-row-researcher", "gantt-row-writer"]);
    const bar = screen.getByTestId("gantt-bar-researcher#1");
    expect(bar.getAttribute("tabindex")).toBe("0");
    expect(bar.getAttribute("aria-label")).toBe(
      "Researcher v1, done, on the critical path: starts at 0 s, takes 20 s, 2 calls");
    const call = screen.getByTestId("gantt-call-0");
    expect(call.getAttribute("tabindex")).toBe("0");
    expect(call.getAttribute("aria-label")).toBe("Researcher: Tool docs, ok: 8 s, $0.0020, ends at 10 s");
    expect(call.className).toContain("gantt-k-tool");
  });

  it("shows a call's label, latency and cost on hover and on focus", () => {
    mount();
    const call = screen.getByTestId("gantt-call-1");
    fireEvent.mouseEnter(call);
    expect(within(call).getByRole("tooltip").textContent).toBe("Model claude, ok: 3 s, $0.0123, ends at 19 s");
    fireEvent.mouseLeave(call);
    expect(screen.queryByRole("tooltip")).toBeNull();
    fireEvent.focus(call);
    expect(within(call).getByRole("tooltip")).toBeTruthy();
    fireEvent.blur(call);
    expect(screen.queryByRole("tooltip")).toBeNull();
  });

  it("labels the HITL wait across the chart and compresses it", () => {
    mount();
    expect(screen.getByTestId("gantt-gate-researcher").textContent).toBe("⏸ HITL: 3 m 20 s, approved");
    expect(screen.getByText("// 3 m 21 s //")).toBeTruthy();
  });

  it("puts the run's duration, cost and calls in the totals", () => {
    mount();
    expect(screen.getByTestId("gantt-totals").textContent).toBe("Run 4 m · Cost $0.0143 · Calls 3");
  });

  it("lists the slowest calls, and a click moves focus to that call", () => {
    mount();
    const chips = within(screen.getByRole("group", { name: "Slowest calls" })).getAllByRole("button");
    expect(chips.map((c) => c.textContent)).toEqual(
      ["Researcher · docs · 8 s", "Researcher · claude · 3 s", "Writer · OUTPUT · 400 ms"]);
    fireEvent.click(chips[1]);
    expect(document.activeElement).toBe(screen.getByTestId("gantt-call-1"));
  });

  it("zooms in and out, and fits back to the width", () => {
    const { container } = mount();
    const inner = container.querySelector(".gantt-inner") as HTMLElement;
    const zoom = () => inner.style.getPropertyValue("--gantt-zoom");
    expect(zoom()).toBe("1");
    expect(screen.getByRole("button", { name: "Zoom out" }).hasAttribute("disabled")).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "Zoom in" }));
    fireEvent.click(screen.getByRole("button", { name: "Zoom in" }));
    expect(zoom()).toBe("4");
    fireEvent.click(screen.getByRole("button", { name: "Zoom out" }));
    expect(zoom()).toBe("2");
    fireEvent.click(screen.getByRole("button", { name: "Fit to width" }));
    expect(zoom()).toBe("1");
  });

  it("drag-scrolls the chart sideways", () => {
    // jsdom has no PointerEvent; a MouseEvent carries the same button and clientX.
    if (!("PointerEvent" in window)) Object.assign(window, { PointerEvent: MouseEvent });
    const { container } = mount();
    const scroller = container.querySelector(".gantt-scroll") as HTMLElement;
    scroller.scrollLeft = 200;
    fireEvent.pointerDown(scroller, { button: 0, clientX: 300 });
    fireEvent.pointerMove(scroller, { clientX: 250 });
    expect(scroller.scrollLeft).toBe(250);
    fireEvent.pointerUp(scroller);
    fireEvent.pointerMove(scroller, { clientX: 0 });
    expect(scroller.scrollLeft).toBe(250);
  });

  it("carries the same data in a table for a screen reader", () => {
    mount();
    const rows = within(screen.getByTestId("gantt-table")).getAllByRole("row");
    // header + 2 runs + 3 calls + 1 wait
    expect(rows).toHaveLength(7);
    expect(rows[1].textContent).toBe("ResearcherRun v1 (critical path)done0 s20 s—");
    expect(rows[rows.length - 1].textContent).toBe("ResearcherHITL waitapproved20 s3 m 20 s—");
  });

  it("on a shared run, draws the bars without calls and says why", () => {
    mount(SNAP, null);
    expect(screen.queryAllByTestId(/^gantt-call-/)).toHaveLength(0);
    expect(screen.getByText("The calls inside each step are not shown on a shared link.")).toBeTruthy();
    expect(screen.getByTestId("gantt-totals").textContent).toBe("Run 4 m");
  });

  it("while live, draws a now line that advances with the clock", () => {
    vi.useFakeTimers();
    const start = toEpochMs(ts(0));
    vi.setSystemTime(start + 30_000);
    const live: SessionSnapshot = { ...SNAP, overall: "running", nodes: { researcher: { status: "running" } },
      logs: [SNAP.logs![0]] };
    mount(live, []);
    const bar = screen.getByTestId("gantt-bar-researcher#1");
    expect(bar.className).toContain("gantt-b-running");
    expect(bar.getAttribute("aria-label")).toContain("takes 30 s so far");
    act(() => { vi.advanceTimersByTime(5000); });
    expect(screen.getByTestId("gantt-bar-researcher#1").getAttribute("aria-label")).toContain("takes 35 s so far");
    expect(screen.getByText("now")).toBeTruthy();
  });

  it("turns the running edge's animation off with prefers-reduced-motion", () => {
    reducedMotion(true);
    const { container } = mount();
    expect(container.querySelector(".gantt")?.className).toContain("gantt-still");
  });

  it("says so when nothing has run yet", () => {
    mount({ session_id: "s1", topic: "t", overall: "running" }, [], 0);
    expect(screen.getByTestId("run-gantt").textContent).toBe("Nothing has run yet.");
  });
});
