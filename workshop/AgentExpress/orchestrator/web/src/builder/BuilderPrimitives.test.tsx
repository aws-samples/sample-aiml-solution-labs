/** The Build view with primitives: the palette lists the build's, dragging one marks where it
 *  may go, a drop attaches it as one undo step, a refused drop says why and records nothing,
 *  and Undo and Redo take the attach back and forth. jsdom: DOM drag events, no real drag. */
import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { Builder } from "./Builder";
import { DND_MIME, type DragPayload } from "./Canvas";
import { newProject, type Workflow } from "./model";
import type { BuildStore, BuildSummary } from "./storage";

beforeAll(() => {
  vi.stubGlobal("ResizeObserver", class { observe() {} unobserve() {} disconnect() {} });
  vi.stubGlobal("DOMMatrixReadOnly", class { m22 = 1; constructor() {} });
});
// A timed-out test must not leave fake timers on for the next one.
afterEach(() => { vi.useRealTimers(); });
/** Its own Storage, as Builder.test.tsx explains: Node's global one may lack the methods. */
beforeEach(() => {
  const m = new Map<string, string>();
  vi.stubGlobal("localStorage", {
    get length() { return m.size; }, clear: () => m.clear(), getItem: (k: string) => m.get(k) ?? null,
    key: (i: number) => [...m.keys()][i] ?? null, removeItem: (k: string) => { m.delete(k); },
    setItem: (k: string, v: string) => { m.set(k, String(v)); },
  });
});

const dt = (payload: DragPayload) => ({ getData: (k: string) => (k === DND_MIME ? JSON.stringify(payload) : ""), setData: vi.fn(), types: [DND_MIME], dropEffect: "", effectAllowed: "" });

function setup() {
  const stored = { ...newProject("Prims"), id: "pprims01" };
  (stored.workflow as Workflow).memories = { notes: { strategies: ["semantic"] } };
  (stored.workflow as Workflow).skills = { triage: { description: "Sort requests", instructions: "Read it, then sort it." } };
  stored.workflow.agents.remote = { name: "Remote", runtime: "a2a", agentCard: "https://agent.example.com/.well-known/agent.json" };
  stored.workflow.steps.push({ agent: "remote" });
  const summary: BuildSummary = { id: stored.id, name: stored.name, updatedAt: stored.updatedAt, agentName: "ax_00000000", versions: 0 };
  const saved: Workflow[] = [];
  const store: BuildStore = {
    server: false, list: async () => [summary],
    load: async () => ({ project: structuredClone(stored), build: summary }),
    save: async (p) => { saved.push(structuredClone(p.workflow)); return summary; },
    remove: async () => ({}),
  };
  return { store, saved, before: structuredClone(stored.workflow.agents.first_agent.agentcore) as Record<string, unknown> };
}

describe("primitives in the Build view", () => {
  it("drags a memory onto an agent, as one undo step, and undoes and redoes it", async () => {
    const { store, saved, before } = setup();
    const notify = vi.fn();
    vi.useFakeTimers();
    await act(async () => { render(<Builder notify={notify} store={store} />); });
    const item = screen.getByTestId("palette-memory-notes");
    const payload: DragPayload = { kind: "primitive", primitive: { kind: "memory", key: "notes" } };
    await act(async () => { fireEvent.dragStart(item, { dataTransfer: dt(payload) }); });
    // While dragging, the agent that runs here is a target; the remote one is not.
    expect(screen.getByTestId("builder-agent-first_agent").className).toContain("axb-drop-ok");
    expect(screen.getByTestId("builder-agent-remote").className).not.toContain("axb-drop-ok");
    expect(screen.getByTestId("agentcore-rail")).toBeTruthy();

    // A refused drop says why and changes nothing.
    await act(async () => { fireEvent.drop(screen.getByTestId("builder-agent-remote"), { dataTransfer: dt(payload) }); });
    expect(notify).toHaveBeenCalledWith("info", expect.stringMatching(/remote \(A2A\) agent/));
    expect(screen.getByRole("button", { name: "Undo" }).hasAttribute("disabled")).toBe(true);
    // The drag ended with the drop: nothing is marked any more.
    expect(document.querySelector(".axb-drop-ok")).toBeNull();

    // A skill, then at once the memory (twice): each attach is its own step, even straight after
    // another edit, and the repeated drop records nothing.
    const skill: DragPayload = { kind: "primitive", primitive: { kind: "skill", key: "triage" } };
    await act(async () => { fireEvent.drop(screen.getByTestId("builder-agent-first_agent"), { dataTransfer: dt(skill) }); });
    await act(async () => { fireEvent.drop(screen.getByTestId("builder-agent-first_agent"), { dataTransfer: dt(payload) }); });
    await act(async () => { fireEvent.drop(screen.getByTestId("builder-agent-first_agent"), { dataTransfer: dt(payload) }); });
    await act(async () => { vi.advanceTimersByTime(900); });
    expect(saved.at(-1)!.agents.first_agent.agentcore).toEqual({ ...before, memory: { use: "notes" } });
    expect(saved.at(-1)!.agents.first_agent.skills).toEqual(["triage"]);

    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Undo" })); });
    await act(async () => { vi.advanceTimersByTime(900); });
    expect(saved.at(-1)!.agents.first_agent.agentcore).toEqual(before);
    expect(saved.at(-1)!.agents.first_agent.skills).toEqual(["triage"]);
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Undo" })); });
    await act(async () => { vi.advanceTimersByTime(900); });
    expect(saved.at(-1)!.agents.first_agent.skills).toBeUndefined();
    expect(screen.getByRole("button", { name: "Undo" }).hasAttribute("disabled")).toBe(true);   // two drops, two steps

    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Redo" })); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Redo" })); });
    await act(async () => { vi.advanceTimersByTime(900); });
    vi.useRealTimers();
    expect(saved.at(-1)!.agents.first_agent.agentcore).toEqual({ ...before, memory: { use: "notes" } });
    expect(saved.at(-1)!.agents.first_agent.skills).toEqual(["triage"]);
  }, 30_000);   // about 2 s alone; many renders of the whole Build view, so slow under a full run
  it("opens a primitive's tab from the palette with Enter", async () => {
    const { store } = setup();
    await act(async () => { render(<Builder notify={vi.fn()} store={store} />); });
    await act(async () => { fireEvent.keyDown(screen.getByTestId("palette-memory-notes"), { key: "Enter" }); });
    expect(screen.getByTestId("tab-level-memory")).toBeTruthy();
  });
});
