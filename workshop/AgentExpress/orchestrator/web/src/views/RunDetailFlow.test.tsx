/** The run page has a Flow tab beside Graph and Timeline, and opening it reads the run's own
 *  flow route; the other tabs are still there. */
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { SessionSnapshot, Workflow } from "../types";

const get = vi.fn();
vi.mock("../api", () => ({ api: { get: (p: string) => get(p) } }));
import { RunDetail } from "./RunDetail";

const wf = { agents: { a: { name: "Alpha" } }, steps: [{ agent: "a" }] } as unknown as Workflow;
const snap: SessionSnapshot = {
  session_id: "run 1", topic: "t", overall: "done", updated_at: "2025-01-01 10:00:00",
  nodes: { a: { status: "done", output: "x" } },
};
const mount = (activeTab: string) => render(
  <RunDetail snap={snap} workflow={wf} selected={null} onSelect={() => {}} can={() => true}
             onCancel={() => {}} activeTab={activeTab} onTabChange={() => {}} onRerun={async () => {}} />);

afterEach(() => { cleanup(); get.mockReset(); });
beforeEach(() => { get.mockResolvedValue({ session_id: "run 1", capped: false, max: 2000, events: [] }); });

describe("the Flow tab on a run", () => {
  it("is offered next to the existing tabs", () => {
    mount("graph");
    const tabs = screen.getAllByRole("tab").map((t) => t.textContent);
    expect(tabs).toEqual(["Graph view", "Table view", "Timeline", "Flow", "Outputs", "Files"]);
  });

  it("reads the run's flow route, with the id escaped, when opened", async () => {
    get.mockResolvedValue({ session_id: "run 1", capped: false, max: 2000, events: [
      { agent_id: "a", kind: "llm", label: "m", status: "ok", ts: "2025-01-01 10:00:01", seq: 1 }] });
    mount("flow");
    await waitFor(() => expect(screen.getByTestId("run-flow")).toBeTruthy());
    expect(get).toHaveBeenCalledWith("/api/sessions/run%201/flow");
    // The Story view opens first: Alpha's stage and its one model call; Summary is one click away.
    expect(screen.getByTestId("story-stage-s0").textContent).toContain("Alpha");
    expect(screen.getByTestId("story-line-e0").textContent).toContain("Asked M");
    fireEvent.click(screen.getByRole("button", { name: "Summary" }));
    expect(screen.getByTestId("flow-agent-a").textContent).toMatch(/Alpha.*1 event/);
  });

  it("is read once on the graph too, for each step's component rows", async () => {
    mount("graph");
    await waitFor(() => expect(get).toHaveBeenCalledWith("/api/sessions/run%201/flow"));
    expect(screen.queryByTestId("run-flow")).toBeNull();
  });
});
