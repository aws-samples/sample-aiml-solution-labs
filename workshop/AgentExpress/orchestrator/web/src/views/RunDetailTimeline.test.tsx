/** The run page's Timeline tab: a Chart | Log toggle, the chart first, and the log table
 *  it always had one click away. */
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { SessionSnapshot, Workflow } from "../types";

const get = vi.fn();
vi.mock("../api", () => ({ api: { get: (p: string) => get(p) } }));

import { RunDetail } from "./RunDetail";

const wf = { agents: { a: { name: "Alpha" } }, steps: [{ agent: "a" }] } as unknown as Workflow;
const snap: SessionSnapshot = {
  session_id: "run 1", topic: "t", overall: "done", created: "2025-01-01 10:00:00", updated_at: "2025-01-01 10:00:09",
  nodes: { a: { status: "done", output: "x" } },
  logs: [
    { ts: "2025-01-01 10:00:00", node: "a", msg: "Alpha started" },
    { ts: "2025-01-01 10:00:09", node: "a", msg: "Alpha complete (v1)" },
  ],
};
const mount = (readOnly = false) => render(
  <RunDetail snap={snap} workflow={wf} selected={null} onSelect={() => {}} can={() => true} readOnly={readOnly}
             onCancel={() => {}} activeTab="timeline" onTabChange={() => {}} onRerun={async () => {}} />);

afterEach(() => { cleanup(); get.mockReset(); });
beforeEach(() => {
  get.mockResolvedValue({ session_id: "run 1", capped: false, max: 2000, events: [
    { agent_id: "a", kind: "llm", label: "m", status: "ok", latency_ms: 2000, ts: "2025-01-01 10:00:05", seq: 1 }] });
});

describe("the Timeline tab", () => {
  it("opens on the chart, with the calls from the run's flow events", async () => {
    mount();
    expect(screen.getByTestId("run-gantt")).toBeTruthy();
    expect(screen.getByTestId("gantt-bar-a#1")).toBeTruthy();
    await waitFor(() => expect(screen.getByTestId("gantt-call-0")).toBeTruthy());
    // One read of the flow route, the one the page already made for the graph.
    expect(get).toHaveBeenCalledTimes(1);
    expect(screen.queryByText("Events")).toBeNull();
  });

  it("switches to the log table and back", async () => {
    mount();
    await screen.findByTestId("gantt-call-0");
    fireEvent.click(screen.getByRole("button", { name: "Log" }));
    expect(screen.queryByTestId("run-gantt")).toBeNull();
    expect(screen.getByText("Events")).toBeTruthy();
    expect(screen.getByText("Alpha complete (v1)")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Chart" }));
    expect(screen.getByTestId("run-gantt")).toBeTruthy();
  });

  it("opens the log from the 'why the run stopped' alert, where the reason line is", async () => {
    const failed: SessionSnapshot = { ...snap, overall: "failed", nodes: { a: { status: "failed" } },
      logs: [snap.logs![0], { ts: "2025-01-01 10:00:09", node: "a", msg: "Alpha failed: ValueError: no" }] };
    const onTabChange = vi.fn();
    render(<RunDetail snap={failed} workflow={wf} selected={null} onSelect={() => {}} can={() => true}
                      onCancel={() => {}} activeTab="timeline" onTabChange={onTabChange} onRerun={async () => {}} />);
    await screen.findByTestId("gantt-call-0");
    fireEvent.click(screen.getByRole("button", { name: "Open the Timeline" }));
    expect(onTabChange).toHaveBeenCalledWith("timeline");
    expect(screen.queryByTestId("run-gantt")).toBeNull();
    expect(screen.getAllByText("Alpha failed: ValueError: no").length).toBeGreaterThan(0);
  });

  it("on a shared run, draws the chart from the log alone and reads nothing", () => {
    mount(true);
    expect(within(screen.getByTestId("run-gantt")).getByTestId("gantt-bar-a#1")).toBeTruthy();
    expect(get).not.toHaveBeenCalled();
  });
});
