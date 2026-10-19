/** The canvas while a primitive is dragged (only valid targets marked, the drop read from the
 *  element under the pointer), the palette's Primitives group, and the AgentCore rail: absent
 *  from the default canvas, collapsed when it appears, counted from the config, and opening
 *  its items in the inspector. jsdom has no layout and no real drag: these are DOM events. */
import { act, cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
vi.mock("../api", () => ({ api: { get: vi.fn(async () => ({ models: [] })), post: vi.fn(), put: vi.fn(), del: vi.fn() } }));
import type { Primitive } from "./attach";
import { Canvas, DND_MIME, type DragPayload, type Selection } from "./Canvas";
import { Inspector } from "./Inspector";
import type { Project, Workflow } from "./model";
import { PrimitivePalette } from "./Primitives";
import { railItems } from "./Rail";

beforeAll(() => {
  vi.stubGlobal("ResizeObserver", class { observe() {} unobserve() {} disconnect() {} });
  vi.stubGlobal("DOMMatrixReadOnly", class { m22 = 1; constructor() {} });
  const warn = console.warn.bind(console);
  vi.spyOn(console, "warn").mockImplementation((...a: unknown[]) => {
    if (!String(a[0]).startsWith("[React Flow]")) warn(...a);
  });
});
afterEach(cleanup);

const GW = 'resource == AgentCore::Gateway::"{{gateway}}"';
const wf = (): Workflow => ({
  agents: {
    writer: { name: "Writer", tool: "lookup", agentcore: { guardrails: { input: true, use: "strict" }, memory: { use: "notes" } } },
    helper: { name: "Helper" },
    remote: { name: "Remote", runtime: "a2a", agentCard: "https://agent.example.com/.well-known/agent.json" },
  },
  tools: { lookup: { type: "mcp", endpoint: "https://mcp.example.com/mcp" }, search: { type: "websearch" } },
  steps: [{ agent: "writer" }, { parallel: ["helper", "remote"] }],
  guardrail: { deniedWords: ["BLOCKED"] },
  guardrails: { strict: { deniedWords: ["SECRET"] } },
  memories: { notes: { strategies: ["semantic"] }, facts: { strategies: ["summary"] } },
  identities: { crm: { type: "apikey" } },
  policies: { noCalls: { statement: `forbid(principal, action in AgentCore::Action::"{{tool}}", ${GW});` } },
  skills: { triage: { description: "Sort", instructions: "Sort it." } },
  orchestrator: {
    policy: { enabled: false, custom: [{ name: "extra", statement: `permit(principal, action, ${GW});` }] },
    triggers: { nightly: { type: "schedule", expression: "rate(1 day)", prompt: "go", enabled: false }, hook: { type: "webhook", prompt: "x" } },
    interceptors: { request: { lambdaArn: "arn:aws:lambda:us-east-1:123456789012:function:audit" } },
  },
} as unknown as Workflow);

function mount(props: Partial<Parameters<typeof Canvas>[0]> = {}) {
  const onDropPayload = vi.fn();
  const onSelect = vi.fn();
  const view = render(
    <div style={{ width: 800, height: 600 }}>
      <Canvas workflow={wf()} issues={[]} selection={null} onSelect={onSelect}
              onDropPayload={onDropPayload} onMoveAgent={() => {}} {...props} />
    </div>);
  return { onDropPayload, onSelect, view };
}
const agentEl = (id: string) => screen.getByTestId(`builder-agent-${id}`);
const marked = () => [...document.querySelectorAll(".axb-drop-ok")].map((e) =>
  e.getAttribute("data-axb-agent") ?? e.getAttribute("data-axb-tool") ?? (e.classList.contains("axb-stage") ? "stage" : "?"));
const dt = (payload: DragPayload) => ({ getData: (k: string) => (k === DND_MIME ? JSON.stringify(payload) : ""), setData: vi.fn(), types: [DND_MIME], dropEffect: "", effectAllowed: "" });

describe("the AgentCore rail's counts", () => {
  it("counts each piece from the config", () => {
    const got = Object.fromEntries(railItems(wf()).map((i) => [i.id, i.count]));
    expect(got).toEqual({ gateway: 2, memory: 2, policy: 2, guardrail: 2, identity: 1, observability: 2, triggers: 2 });
    const notes = Object.fromEntries(railItems(wf()).map((i) => [i.id, i.note]));
    expect(notes.policy).toBe("2 policies; the engine is off");
    expect(notes.triggers).toBe("2 triggers, 1 on");
    expect(notes.gateway).toBe("2 tools behind it, 1 interceptor");
    expect(railItems(wf()).find((i) => i.id === "memory")!.users).toEqual(["writer"]);
  });
  it("counts nothing in an empty build", () => {
    const empty = railItems({ agents: {}, tools: {}, steps: [] } as Workflow);
    expect(empty.map((i) => i.count)).toEqual([0, 0, 0, 0, 0, 0, 0]);
    expect(empty[0].note).toMatch(/no Gateway/);
  });
});

describe("the canvas", () => {
  it("by default draws no rail and marks nothing", () => {
    mount();
    expect(screen.queryByTestId("agentcore-rail")).toBeNull();
    expect(marked()).toEqual([]);
  });
  it("draws the rail expanded when it is turned on in the Show menu, and can collapse it", async () => {
    const { onSelect } = mount();
    await act(async () => { fireEvent.click(screen.getByRole("checkbox", { name: /AgentCore rail/, hidden: true })); });
    const rail = screen.getByTestId("agentcore-rail");
    const toggle = screen.getByRole("button", { name: "Collapse the AgentCore rail", hidden: true });
    expect(toggle.getAttribute("aria-expanded")).toBe("true");
    expect(rail.querySelectorAll(".axb-rail-label")).toHaveLength(7);
    expect(screen.getByTestId("rail-memory").getAttribute("aria-label")).toMatch(/^Memory: 2\./);
    await act(async () => { fireEvent.click(toggle); });
    expect(rail.querySelector(".axb-rail-label")).toBeNull();
    await act(async () => { fireEvent.click(screen.getByTestId("rail-triggers")); });
    expect(onSelect).toHaveBeenCalledWith({ kind: "core", id: "triggers" });
  });
  it("draws the rail when turned on, marking the pieces whose kinds the cards show", async () => {
    mount();
    expect(screen.queryByTestId("rail-guardrail")).toBeNull();          // off until asked for
    await act(async () => { fireEvent.click(screen.getByRole("checkbox", { name: /AgentCore rail/, hidden: true })); });
    expect(screen.getByTestId("rail-guardrail").className).toContain("axb-rail-marked");
    await act(async () => { fireEvent.click(screen.getByRole("checkbox", { name: /Guardrails/, hidden: true })); });
    expect(screen.getByTestId("rail-guardrail").className).not.toContain("axb-rail-marked");
    expect(screen.getByTestId("rail-memory").className).toContain("axb-rail-marked");
  });
  it("marks the selected piece", () => {
    mount({ selection: { kind: "core", id: "gateway" } as Selection, dragging: { kind: "memory", key: "notes" } });
    expect(screen.getByTestId("rail-gateway").getAttribute("aria-pressed")).toBe("true");
  });
});

describe("dragging a primitive", () => {
  const drag = (p: Primitive, extra: Partial<Parameters<typeof Canvas>[0]> = {}) => mount({ dragging: p, ...extra });
  it("marks only the agents a memory may go on, and its rail piece", () => {
    drag({ kind: "memory", key: "notes" });
    expect(marked().sort()).toEqual(["helper", "writer"]);
    expect(screen.getByTestId("rail-memory").className).toContain("axb-rail-marked");
  });
  it("marks only the first stage for a trigger", () => {
    drag({ kind: "trigger", key: "nightly" });
    expect(marked().sort()).toEqual(["stage", "writer"]);   // the first stage, and the agent in it
    expect(document.querySelector(".axb-stage.axb-drop-ok .axb-stage-num")?.textContent).toBe("1");
  });
  it("marks the tool chips of the selected agent for a policy, and no agent", () => {
    drag({ kind: "policy", key: "noCalls" }, { selection: { kind: "agent", id: "writer" } });
    expect(marked()).toEqual(["lookup"]);
  });
  it("marks nothing for an interceptor", () => {
    drag({ kind: "interceptor", key: "request" });
    expect(marked()).toEqual([]);
  });
  it("drops on the agent under the pointer", async () => {
    const { onDropPayload } = drag({ kind: "memory", key: "notes" });
    const payload: DragPayload = { kind: "primitive", primitive: { kind: "memory", key: "notes" } };
    await act(async () => { fireEvent.drop(agentEl("helper").querySelector(".axb-name")!, { dataTransfer: dt(payload) }); });
    expect(onDropPayload).toHaveBeenCalledWith(payload, { kind: "agent", id: "helper" });
  });
  it("drops a policy on the tool chip under the pointer", async () => {
    const { onDropPayload } = drag({ kind: "policy", key: "noCalls" }, { selection: { kind: "agent", id: "writer" } });
    const payload: DragPayload = { kind: "primitive", primitive: { kind: "policy", key: "noCalls" } };
    await act(async () => { fireEvent.drop(screen.getByTestId("chip-writer-lookup"), { dataTransfer: dt(payload) }); });
    expect(onDropPayload).toHaveBeenCalledWith(payload, { kind: "tool", id: "lookup" });
  });
  it("an agent or a tool from the palette is still placed by the layout, as before", async () => {
    const { onDropPayload } = mount();
    const payload: DragPayload = { kind: "tool", key: "search" };
    await act(async () => { fireEvent.drop(agentEl("helper"), { dataTransfer: dt(payload) }); });
    expect(onDropPayload.mock.calls[0][1].kind).not.toBe("tool");
  });
});

describe("the palette's Primitives group", () => {
  it("lists only what this build has, and nothing for a build with none", () => {
    const view = render(<PrimitivePalette workflow={wf()} onDrag={() => {}} onOpen={() => {}} />);
    const items = [...document.querySelectorAll("[data-testid^='palette-']")].map((e) => e.getAttribute("data-testid"))
      .filter((id) => id !== "palette-primitives" && !id?.startsWith("palette-group-"));
    expect(items).toEqual(["palette-skill-triage", "palette-memory-notes", "palette-memory-facts", "palette-guardrail-deployment",
      "palette-guardrail-strict", "palette-identity-crm", "palette-policy-noCalls", "palette-interceptor-request", "palette-trigger-nightly",
      "palette-trigger-hook"]);
    view.unmount();
    // A build with none still lists every group, so it is clear what can be added, but nothing to drag.
    const onOpen = vi.fn();
    render(<PrimitivePalette workflow={{ agents: {}, tools: {}, steps: [] } as Workflow} onDrag={() => {}} onOpen={onOpen} />);
    const groups = [...document.querySelectorAll("[data-testid^='palette-group-']")].map((e) => e.getAttribute("data-testid"));
    // The same order as the Builder's tabs and the library: skills, memory, guardrails, identity,
    // policies, interceptors, triggers, evals.
    expect(groups).toEqual(["palette-group-skill", "palette-group-memory", "palette-group-guardrail", "palette-group-identity",
      "palette-group-policy", "palette-group-interceptor", "palette-group-trigger", "palette-group-evaluator"]);
    expect(document.querySelectorAll("[draggable='true']")).toHaveLength(0);
    fireEvent.click(screen.getByText("Add on the Memory tab"));
    expect(onOpen).toHaveBeenCalledWith("memory");
  });
  it("groups each kind under a collapsible header with its count", () => {
    render(<PrimitivePalette workflow={wf()} onDrag={() => {}} onOpen={() => {}} />);
    const memory = screen.getByTestId("palette-group-memory");
    const header = within(memory).getByRole("button", { expanded: true });
    expect(header.textContent).toContain("Memory");
    expect(memory.textContent).toContain("(2)");
    // An empty kind starts closed; opening it is a click (or Enter) on its header.
    const evals = within(screen.getByTestId("palette-group-evaluator")).getByRole("button", { expanded: false });
    fireEvent.click(evals);
    expect(within(screen.getByTestId("palette-group-evaluator")).getByRole("button", { expanded: true })).toBeTruthy();
  });
  it("starts a drag with the primitive, ends it, and opens its tab with Enter", () => {
    const onDrag = vi.fn();
    const onOpen = vi.fn();
    render(<PrimitivePalette workflow={wf()} onDrag={onDrag} onOpen={onOpen} />);
    const item = screen.getByTestId("palette-skill-triage");
    const transfer = dt({ kind: "new-agent" });
    fireEvent.dragStart(item, { dataTransfer: transfer });
    expect(transfer.setData).toHaveBeenCalledWith(DND_MIME, JSON.stringify({ kind: "primitive", primitive: { kind: "skill", key: "triage" } }));
    expect(onDrag).toHaveBeenLastCalledWith({ kind: "skill", key: "triage" });
    fireEvent.dragEnd(item);
    expect(onDrag).toHaveBeenLastCalledWith(null);
    fireEvent.keyDown(item, { key: "Enter" });
    expect(onOpen).toHaveBeenCalledWith("skill");
  });
});

describe("a rail piece in the inspector", () => {
  it("says what it is, lists this build's entries, and links the agents using it", async () => {
    const onSelect = vi.fn();
    const p: Project = { id: "p1", name: "P", createdAt: "", updatedAt: "", prompts: {}, workflow: wf() };
    await act(async () => {
      render(<Inspector project={p} selection={{ kind: "core", id: "memory" }} issues={[]} onChange={() => {}} onSelect={onSelect} />);
    });
    expect(screen.getByRole("heading", { name: "Memory" })).toBeTruthy();
    expect(screen.getByText("notes")).toBeTruthy();
    expect(screen.getByText("facts")).toBeTruthy();
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Writer" })); });
    expect(onSelect).toHaveBeenCalledWith({ kind: "agent", id: "writer" });
  });
});
