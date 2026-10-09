/** Add from registry: the only registry is used without asking and several are offered;
 *  a record that cannot be used cannot be picked; "keep in sync" travels with what is
 *  added; and an update replaces only what came from the registry. */
import { act, render, screen } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

const calls: string[] = [];
let registries: unknown[] = [];
vi.mock("../api", () => ({ api: {
  get: async (url: string) => {
    calls.push(url);
    if (url === "/api/registry") return registries;
    return [
      { recordId: "r1", name: "orders-mcp", displayName: "", description: "Orders", type: "MCP", version: "1.0.0",
        updatedAt: "", kind: "tool", key: "ordersMcp", tools: ["listOrders"],
        entry: { type: "mcp", description: "Orders", endpoint: "https://mcp.example.com/mcp",
          registry: { registryId: "R1", recordId: "r1", name: "orders-mcp", version: "1.0.0", sync: false } } },
      { recordId: "r2", name: "no-url", displayName: "", description: "", type: "MCP", version: "1", updatedAt: "",
        kind: "tool", why: "the record names no https streamable-HTTP endpoint for this server" },
    ];
  },
  put: async () => ({}), post: async () => ({}), del: async () => ({}),
} }));
import type { Project } from "./model";
import { addHits, applyUpdate, RegistryPicker } from "./RegistryPicker";
import type { RegistryHit } from "./storage";

const w = () => createWrapper(document.body);
const base = (): Project => ({ id: "p", name: "P", createdAt: "", updatedAt: "", prompts: {},
  workflow: { agents: { a: { name: "A" } }, steps: [{ agent: "a" }],
    tools: { ordersMcp: { type: "mcp", endpoint: "https://old" } } } as never });

describe("adding and updating", () => {
  const hit = { kind: "tool", key: "ordersMcp", entry: { type: "mcp", endpoint: "https://mcp.example.com/mcp",
    registry: { registryId: "R1", recordId: "r1", version: "1.0.0", sync: false } } } as unknown as RegistryHit;
  it("adds under a free name, with keep-in-sync as chosen, and skips what cannot be used", () => {
    const { project, added } = addHits(base(), [hit, { kind: "tool", why: "no" } as RegistryHit], true);
    expect(added).toEqual(["ordersMcp2"]);
    const t = project.workflow.tools.ordersMcp2 as Record<string, any>;
    expect(t.endpoint).toBe("https://mcp.example.com/mcp");
    expect(t.registry.sync).toBe(true);
    expect((project.workflow.tools.ordersMcp as Record<string, unknown>).endpoint).toBe("https://old");
  });
  it("an agent produces under its own name", () => {
    const a = { kind: "agent", key: "ordersAgent", entry: { name: "Orders", runtime: "a2a", agentCard: "https://a",
      produces: "x", registry: {} } } as unknown as RegistryHit;
    const { project } = addHits(base(), [a], false);
    expect((project.workflow.agents.ordersAgent as Record<string, unknown>).produces).toBe("ordersAgent");
  });
  it("an update replaces what came from the registry and keeps the build's own settings", () => {
    const p = base();
    (p.workflow.tools as Record<string, unknown>).ordersMcp = { type: "mcp", endpoint: "https://old", auth: "apikey",
      toolSchema: [{ name: "old" }], registry: { version: "1.0.0" } };
    const next = applyUpdate(p, { map: "tools", key: "ordersMcp", sync: false, from: "1.0.0", to: "1.1.0",
      entry: { type: "mcp", endpoint: "https://new", description: "New", registry: { version: "1.1.0" } } });
    expect(next.workflow.tools.ordersMcp).toEqual({ type: "mcp", endpoint: "https://new", auth: "apikey",
      description: "New", registry: { version: "1.1.0" } });
  });
});

describe("the picker", () => {
  beforeEach(() => { calls.length = 0; });

  it("uses the only registry without asking, lists what it can add, and passes keep-in-sync", async () => {
    registries = [{ id: "R1", name: "Org tools", description: "", status: "READY" }];
    const onAdd = vi.fn();
    await act(async () => { render(<RegistryPicker kind="tool" visible onDismiss={() => {}} onAdd={onAdd} />); });
    expect(w().findSelect()).toBeNull();
    expect(screen.getByText("Org tools")).toBeTruthy();
    expect(calls).toContain("/api/registry/search?registry=R1&q=&kind=tool");
    const table = w().findTable()!;
    expect(table.findRows()).toHaveLength(2);
    expect(document.body.textContent).toContain('An MCP tool "ordersMcp" · tools: listOrders');
    expect(document.body.textContent).toContain("Cannot add: the record names no https");
    // The unusable one cannot be selected.
    expect((table.findRowSelectionArea(2)!.findCheckbox()!.findNativeInput().getElement() as HTMLInputElement).disabled).toBe(true);
    await act(async () => { table.findRowSelectionArea(1)!.click(); });
    const sync = w().findAllCheckboxes().find((c) => c.getElement().textContent?.includes("Keep in sync"))!;
    await act(async () => { sync.findNativeInput().click(); });
    await act(async () => { w().findAllButtons().find((b) => b.getElement().textContent === "Add (1)")!.click(); });
    expect(onAdd).toHaveBeenCalledTimes(1);
    expect(onAdd.mock.calls[0][0].map((h: RegistryHit) => h.recordId)).toEqual(["r1"]);
    expect(onAdd.mock.calls[0][1]).toBe(true);
  });

  it("offers a choice when there are several, and says when there are none", async () => {
    registries = [{ id: "R1", name: "Team A", description: "", status: "READY" },
      { id: "R2", name: "Team B", description: "", status: "READY" }];
    const { unmount } = await act(async () => render(<RegistryPicker kind="skill" visible onDismiss={() => {}} onAdd={() => {}} />));
    expect(w().findSelect()).not.toBeNull();
    unmount();
    registries = [];
    await act(async () => { render(<RegistryPicker kind="skill" visible onDismiss={() => {}} onAdd={() => {}} />); });
    expect(screen.getByText("No Agent Registry here")).toBeTruthy();
  });
});
