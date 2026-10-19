/** The Builder canvas's agent cards: everything attached to an agent is listed on it, labelled
 *  by kind and named, without selecting it. The Show menu hides kinds the reader does not want. */
import { act, cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { Canvas } from "./Canvas";
import type { Workflow } from "./model";

beforeAll(() => {
  // React Flow measures nodes with ResizeObserver, which jsdom does not have.
  vi.stubGlobal("ResizeObserver", class { observe() {} unobserve() {} disconnect() {} });
  vi.stubGlobal("DOMMatrixReadOnly", class { m22 = 1; constructor() {} });
  // jsdom has no layout, so React Flow warns that it has no size or stylesheet; neither matters here.
  const warn = console.warn.bind(console);
  vi.spyOn(console, "warn").mockImplementation((...a: unknown[]) => {
    if (!String(a[0]).startsWith("[React Flow]")) warn(...a);
  });
});
afterEach(cleanup);

const wf = {
  agents: {
    guarded: {
      name: "Guarded", tool: "lookup", skills: ["triage"],
      agentcore: { guardrails: { input: true, output: true }, memory: { longTerm: true } },
    },
    plain: { name: "Plain" },
  },
  steps: [{ parallel: ["guarded", "plain"] }],
  tools: { lookup: { type: "mcp", policies: ["readOnly"] } },
} as unknown as Workflow;

const mount = (props: Partial<Parameters<typeof Canvas>[0]> = {}) => {
  const onSelect = vi.fn();
  render(
    <div style={{ width: 800, height: 600 }}>
      <Canvas workflow={wf} issues={[]} selection={null} onSelect={onSelect}
              onDropPayload={() => {}} onMoveAgent={() => {}} {...props} />
    </div>);
  return { onSelect };
};
const card = (id: string) => screen.getByTestId(`builder-agent-${id}`);
const kinds = (id: string) => [...card(id).querySelectorAll(".axb-comp[data-kind]")].map((e) => e.getAttribute("data-kind"));
const heightOf = (id: string) => Number.parseFloat(card(id).style.height);

describe("agent cards", () => {
  it("list every attached component by default, each with its kind in words and its name", () => {
    mount();
    const list = within(screen.getByTestId("components-guarded"));
    for (const tag of ["Tool", "Guardrail", "Memory", "Policy", "Skill"]) expect(list.getByText(tag)).toBeTruthy();
    expect(list.getByText("lookup")).toBeTruthy();
    expect(list.getByText("readOnly")).toBeTruthy();
    expect(list.getByText("triage")).toBeTruthy();
    expect(list.getByText("checks input and output")).toBeTruthy();
    expect(kinds("guarded")).toEqual(["tool", "guardrail", "memory", "policy", "skill"]);
  });

  it("draw a plain agent with no list, and as short as before", () => {
    mount();
    expect(screen.queryByTestId("components-plain")).toBeNull();
    expect(heightOf("plain")).toBe(60);
    expect(heightOf("guarded")).toBeGreaterThan(60 + 4 * 20);
  });

  it("open a row's settings by clicking it, as that agent's row", () => {
    const { onSelect } = mount();
    fireEvent.click(screen.getByTestId("chip-guarded-lookup"));
    expect(onSelect).toHaveBeenCalledWith({ kind: "component", agent: "guarded", key: "tool:lookup:lookup" });
    expect(onSelect).not.toHaveBeenCalledWith({ kind: "agent", id: "guarded" });
  });

  it("hide a kind from the Show menu, and the card shrinks to what is left", () => {
    mount();
    const before = heightOf("guarded");
    fireEvent.click(screen.getByRole("checkbox", { name: /Guardrails/, hidden: true }));
    expect(kinds("guarded")).toEqual(["tool", "memory", "policy", "skill"]);
    expect(heightOf("guarded")).toBeLessThan(before);
    fireEvent.click(screen.getByRole("checkbox", { name: /Guardrails/, hidden: true }));
    expect(kinds("guarded")).toContain("guardrail");
  });

  it("offer remove on every row, and + Add on every agent, when the canvas can be edited", () => {
    mount();
    expect(screen.queryByLabelText("Remove Tool: lookup from Guarded")).toBeNull();
    cleanup();
    mount({ onRemoveComponent: () => {}, onAddChoice: () => {} });
    expect(screen.getByLabelText("Remove Tool: lookup from Guarded")).toBeTruthy();
    expect(screen.getByLabelText("Remove Guardrail: default guardrail from Guarded")).toBeTruthy();
    expect(screen.getByLabelText("Add to Guarded")).toBeTruthy();
  });

  it("drag a row off its agent to remove it, onto another agent to move it, and back onto its own to leave it", async () => {
    const onRemoveComponent = vi.fn();
    const onMoveComponent = vi.fn();
    mount({ onRemoveComponent, onMoveComponent, onAddChoice: () => {} });
    const store: Record<string, string> = {};
    const data = { setData: (k: string, v: string) => { store[k] = v; }, getData: (k: string) => store[k] ?? "",
      types: [] as string[], effectAllowed: "", dropEffect: "" };
    const rowEl = screen.getByTestId("chip-guarded-skill::triage").closest("li")!;
    await act(async () => { fireEvent.dragStart(rowEl, { dataTransfer: data }); await new Promise((r) => setTimeout(r, 5)); });
    expect(screen.getByTestId("row-drag-hint").textContent).toMatch(/Skill "triage" anywhere off Guarded/);
    fireEvent.drop(screen.getByTestId("builder-agent-plain"), { dataTransfer: data });
    expect(onMoveComponent).toHaveBeenCalledWith("guarded", "skill::triage", { kind: "agent", id: "plain" });
    fireEvent.dragStart(rowEl, { dataTransfer: data });
    fireEvent.drop(screen.getByTestId("builder-agent-guarded"), { dataTransfer: data });
    expect(onRemoveComponent).not.toHaveBeenCalled();
    fireEvent.dragStart(rowEl, { dataTransfer: data });
    fireEvent.drop(document.querySelector(".axb-canvas")!, { dataTransfer: data, clientX: 5000, clientY: 5000 });
    expect(onRemoveComponent).toHaveBeenCalledWith("guarded", "skill::triage");
  });
  it("show the components on a read-only preview too", () => {
    mount({ readOnly: true });
    expect(kinds("guarded")).toEqual(["tool", "guardrail", "memory", "policy", "skill"]);
  });
});
