/** The Build view, rendered: it opens on a working draft, the canvas mounts, and the
 *  drafts it makes survive a reload.
 *
 *  jsdom has no layout engine, so React Flow cannot measure anything here — these tests
 *  prove the page renders and wires up without throwing, which is the failure a pure
 *  unit test of model.ts cannot catch (a bad import, a hook order bug, a Cloudscape prop
 *  that does not exist). The geometry is tested in layout.test.ts, the edits in
 *  model.test.ts. */

import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { act, fireEvent, render, screen } from "@testing-library/react";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

import { Builder } from "./Builder";
import { newProject } from "./model";
import { listProjects, type BuildStore, type BuildSummary } from "./storage";

/** An in-memory Storage. Recent Node versions ship their own global `localStorage`
 *  (a stub without `clear` unless started with a storage file), and it shadows jsdom's,
 *  so the test brings its own rather than depending on which one wins. */
function memoryStorage(): Storage {
  const m = new Map<string, string>();
  return {
    get length() { return m.size; },
    clear: () => m.clear(),
    getItem: (k) => m.get(k) ?? null,
    key: (i) => [...m.keys()][i] ?? null,
    removeItem: (k) => { m.delete(k); },
    setItem: (k, v) => { m.set(k, String(v)); },
  };
}

beforeAll(() => {
  // React Flow observes its container; jsdom implements neither of these.
  class RO { observe() {} unobserve() {} disconnect() {} }
  vi.stubGlobal("ResizeObserver", RO);
  vi.stubGlobal("DOMMatrixReadOnly", class { m22 = 1; constructor() {} });
});

beforeEach(() => { vi.stubGlobal("localStorage", memoryStorage()); });

describe("Builder", () => {
  it("opens on a valid starter workflow with its agent on the canvas", async () => {
    await act(async () => { render(<Builder notify={vi.fn()} />); });
    expect(screen.getByText("My workflow")).toBeTruthy();
    expect(screen.getByText("Valid")).toBeTruthy();
    // The palette offers a new agent; the starter agent is already placed, so it is
    // not listed as "not on the canvas".
    expect(screen.getByText("+ New agent")).toBeTruthy();
    expect(screen.queryByText("not on the canvas")).toBeNull();
  });

  it("keeps one Import and one Export, shows problems from the header, and has the new tabs", async () => {
    await act(async () => { render(<Builder notify={vi.fn()} />); });
    const w = createWrapper(document.body);
    expect(screen.queryByRole("button", { name: "Upload" })).toBeNull();
    const menu = w.findButtonDropdown()!;
    await act(async () => { menu.openDropdown(); });
    const texts = menu.findItems().map((i) => i.getElement().textContent ?? "");
    expect(texts.some((t) => t.startsWith("Import a file"))).toBe(true);
    expect(texts.some((t) => /Upload|Open build/.test(t))).toBe(false);
    await act(async () => { menu.findItemById("import")!.click(); });
    expect(screen.getByText("Into this build, replacing it")).toBeTruthy();
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Show the problems" })); });
    expect(screen.getByText(/ready to deploy/)).toBeTruthy();
    for (const tab of ["Guardrails", "Memory", "Evals", "Identity", "Settings"]) {
      expect(screen.getAllByRole("tab").some((t) => (t.textContent ?? "").startsWith(tab))).toBe(true);
    }
    expect(screen.getAllByRole("tab").some((t) => (t.textContent ?? "").startsWith("Problems"))).toBe(false);
  });
  it("orders the tabs by where each takes effect, and says the level at the top of each", async () => {
    await act(async () => { render(<Builder notify={vi.fn()} />); });
    const names = screen.getAllByRole("tab").map((t) => (t.textContent ?? "").replace(/\s*\(\d+\)$/, ""))
      .filter((n) => !["AgentExpress Assistant", "Build manually", "Sign-in", "Tool access"].includes(n));
    expect(names).toEqual(["Design", "Tools", "Skills", "Memory", "Guardrails", "Identity", "Policies", "Interceptors",
      "Triggers", "Evals", "Settings", "workflow.json"]);
    await act(async () => { fireEvent.click(screen.getByText("Policies (0)", { selector: "[role=tab] *" })); });
    expect(screen.getByTestId("tab-level-policies").textContent).toMatch(/^Gateway level\./);
    await act(async () => { fireEvent.click(screen.getByText("Skills (0)", { selector: "[role=tab] *" })); });
    expect(screen.getByTestId("tab-level-skills").textContent).toMatch(/^Agent level\./);
  });
  it("adds an agent from the palette with the keyboard, and autosaves the draft", async () => {
    vi.useFakeTimers();
    await act(async () => { render(<Builder notify={vi.fn()} />); });
    const item = screen.getByText("+ New agent").closest("[role=button]")!;
    await act(async () => { fireEvent.keyDown(item, { key: "Enter" }); });
    // The inspector opens on the new agent.
    expect(screen.getAllByText("New Agent").length).toBeGreaterThan(0);
    await act(async () => { vi.advanceTimersByTime(900); });
    vi.useRealTimers();
    const [saved] = listProjects();
    expect(Object.keys(saved.workflow.agents)).toEqual(["first_agent", "new_agent"]);
    expect(saved.workflow.steps.map((s) => s.agent)).toEqual(["first_agent", "new_agent"]);
  });

  it("opens a new build from the side navigation under a name no other build has", async () => {
    // The navigation lists builds by name, so two called "My workflow" could not be told apart.
    vi.useFakeTimers();
    const view = await act(async () => render(<Builder notify={vi.fn()} request={null} />));
    await act(async () => { vi.advanceTimersByTime(900); });
    await act(async () => { view.rerender(<Builder notify={vi.fn()} request={{ id: "new", nonce: 1 }} />); });
    await act(async () => { vi.advanceTimersByTime(900); });
    vi.useRealTimers();
    expect(listProjects().map((p) => p.name).sort()).toEqual(["My workflow", "My workflow 2"]);
  });

  it("opens the build a link names, even when the first open (the last-opened build) answers later", async () => {
    const mk = (id: string, name: string) => ({ ...newProject(name), id });
    const a = mk("plinka01", "Linked A"), b = mk("plinkb01", "Recent B");
    const sum = (p: typeof a): BuildSummary => ({ id: p.id, name: p.name, updatedAt: p.updatedAt, agentName: "ax_00000000", versions: 0 });
    let releaseB: () => void = () => {};
    const store: BuildStore = {
      server: false, list: async () => [sum(b), sum(a)],
      // The last-opened build loads slowly; the linked one at once.
      load: (id) => (id === b.id
        ? new Promise((res) => { releaseB = () => res({ project: b, build: sum(b) }); })
        : Promise.resolve({ project: a, build: sum(a) })),
      save: async (p) => sum(p as typeof a), remove: async () => ({}),
    };
    const view = await act(async () => render(<Builder notify={vi.fn()} store={store} request={null} />));
    await act(async () => { view.rerender(<Builder notify={vi.fn()} store={store} request={{ id: a.id, nonce: 1 }} />); });
    await act(async () => { releaseB(); });
    expect(screen.getByRole("heading", { name: /Linked A/ })).toBeTruthy();
    expect(screen.queryByRole("heading", { name: /Recent B/ })).toBeNull();
  });

  it("renames the build in place, to any non-blank name", async () => {
    vi.useFakeTimers();
    await act(async () => { render(<Builder notify={vi.fn()} />); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Rename this build" })); });
    const field = screen.getByRole("textbox", { name: "Build name" });
    await act(async () => { fireEvent.change(field, { target: { value: "  Claims settlement — v2 (EU)  " } }); });
    await act(async () => { fireEvent.keyDown(field, { key: "Enter" }); });
    await act(async () => { vi.advanceTimersByTime(900); });
    vi.useRealTimers();
    expect(listProjects()[0].name).toBe("Claims settlement — v2 (EU)");
    expect(screen.getByText("Claims settlement — v2 (EU)")).toBeTruthy();
  });

  it("shows the exact workflow.json the export writes", async () => {
    await act(async () => { render(<Builder notify={vi.fn()} />); });
    await act(async () => { fireEvent.click(screen.getByText("workflow.json", { selector: "[role=tab] *" })); });
    const pre = document.querySelector(".axb-json");
    expect(pre?.textContent).toContain('"first_agent": {');
    expect(pre?.textContent?.startsWith("{\n  \"$schema\": \"./workflow.schema.json\"")).toBe(true);
  });

  it("opens the workflow.json tab from Export, from another tab, without scrolling to it", async () => {
    await act(async () => { render(<Builder notify={vi.fn()} />); });
    await act(async () => { fireEvent.click(screen.getByText("Settings", { selector: "[role=tab] *" })); });
    expect(document.querySelector(".axb-json")).toBeNull();
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Export" })); });
    await act(async () => { fireEvent.click(screen.getByText("View workflow.json")); });
    expect(document.querySelector(".axb-json")?.textContent).toContain('"first_agent": {');
  });

  it("edits and deletes a tool from its row on the Tools tab", async () => {
    const stored = { ...newProject("Tools"), id: "ptools01" };
    stored.workflow.tools = { lookup: { type: "mcp", endpoint: "https://mcp.example.com/mcp", description: "Look things up" } };
    stored.workflow.agents.first_agent.tool = "lookup";
    const summary: BuildSummary = { id: stored.id, name: stored.name, updatedAt: stored.updatedAt, agentName: "ax_00000000", versions: 0 };
    const saved: Array<Record<string, unknown>> = [];
    const store: BuildStore = {
      server: false, list: async () => [summary],
      load: async () => ({ project: stored, build: summary }),
      save: async (p) => { saved.push(p.workflow.tools as Record<string, unknown>); return summary; },
      remove: async () => ({}),
    };
    vi.stubGlobal("confirm", () => true);
    vi.useFakeTimers();
    await act(async () => { render(<Builder notify={vi.fn()} store={store} />); });
    await act(async () => { fireEvent.click(screen.getByText(/^Tools \(1\)/, { selector: "[role=tab] *" })); });
    // Edit opens the tool's settings in the inspector.
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Edit lookup" })); });
    expect(screen.getByText("Look things up")).toBeTruthy();
    await act(async () => { fireEvent.click(screen.getByText(/^Tools \(1\)/, { selector: "[role=tab] *" })); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Delete lookup" })); });
    await act(async () => { vi.advanceTimersByTime(900); });
    vi.useRealTimers();
    expect(screen.getByText(/^Tools \(0\)/, { selector: "[role=tab] *" })).toBeTruthy();
    expect(saved.at(-1)).toEqual({});
  });

  it("with the console's builds store, opens a stored build and offers to deploy it", async () => {
    const stored = { ...newProject("Claims triage"), id: "pclaims01" };
    const summary: BuildSummary = { id: stored.id, name: stored.name, updatedAt: stored.updatedAt,
      agentName: "ax_1a2b3c4d", versions: 0 };
    const saved: string[] = [];
    const store: BuildStore = {
      server: true,
      list: async () => [summary],
      load: async (id) => (id === stored.id ? { project: stored, build: summary } : null),
      save: async (p) => { saved.push(p.name); return { ...summary, name: p.name }; },
      remove: async () => ({}),
    };
    await act(async () => { render(<Builder notify={vi.fn()} store={store} />); });
    expect(screen.getByText("Claims triage")).toBeTruthy();
    expect(screen.getByText("Deployment")).toBeTruthy();
    expect(screen.getByText("Not deployed")).toBeTruthy();
    // Opening a build does not save it again unchanged.
    expect(saved).toEqual([]);
  });
});
