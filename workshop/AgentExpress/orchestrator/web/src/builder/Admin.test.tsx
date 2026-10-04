/** The Admin page: every user's builds and runs, read-only, and Destroy to clean up. */
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

const calls: { method: string; path: string }[] = [];
vi.mock("../api", () => {
  const answer = (method: string, path: string) => {
    calls.push({ method, path });
    if (path === "/api/builds?scope=all") {
      return Promise.resolve([
        { id: "pb1", name: "Claims", owner: "u1", ownerEmail: "alice@example.com", updatedAt: "2026-09-09T10:00:00Z",
          versions: 1, tool: "cdk", region: "us-east-1",
          deployed: { version: 1, tool: "cdk", agentName: "ax_1", runtimeArn: "arn:x" } },
        { id: "pb2", name: "Draft", owner: "u2", ownerEmail: "bob@example.com", updatedAt: "2026-09-08T10:00:00Z" },
      ]);
    }
    if (path === "/api/sessions?scope=all") {
      return Promise.resolve([{ session_id: "s1", topic: "Triage claim 88213\nwith details", overall: "done",
        owner: "u1", user: "alice@example.com" }]);
    }
    if (path === "/api/builds/pb1") {
      return Promise.resolve({ project: { id: "pb1", name: "Claims", updatedAt: "", prompts: {},
        workflow: { agents: { a: { name: "A", runtime: "main", maxTokens: 10 } }, steps: [{ agent: "a" }] } } });
    }
    if (path === "/api/builds/pb1/design") {
      return Promise.resolve({ turns: [{ id: "u1", role: "user", text: "Build a claims flow", at: "" }],
        status: "idle", revision: 0, model: "m", starters: [] });
    }
    if (path.startsWith("/api/sessions?build=pb1")) return Promise.resolve([]);
    return Promise.resolve({ ok: true });
  };
  return {
    api: {
      get: (p: string) => answer("GET", p),
      post: (p: string) => answer("POST", p),
      put: (p: string) => answer("PUT", p),
      del: (p: string) => answer("DELETE", p),
    },
  };
});
import { Admin, matches } from "./Admin";

beforeAll(() => {
  class RO { observe() {} unobserve() {} disconnect() {} }
  vi.stubGlobal("ResizeObserver", RO);
  vi.stubGlobal("DOMMatrixReadOnly", class { m22 = 1; constructor() {} });
});
beforeEach(() => { calls.length = 0; });

describe("Admin", () => {
  it("lists every user's builds and runs with their owners, and filters them", async () => {
    await act(async () => { render(<Admin canDestroy notify={vi.fn()} onOpenRun={vi.fn()} />); });
    expect(screen.getByText("alice@example.com")).toBeTruthy();
    expect(screen.getByText("bob@example.com")).toBeTruthy();
    expect(screen.getByText("Builds (2)")).toBeTruthy();
    expect(screen.getByText("Runs (1)")).toBeTruthy();
    expect(matches("bob", "Draft", "bob@example.com")).toBe(true);
    expect(matches("carol", "Draft", "bob@example.com")).toBe(false);
    await act(async () => { createWrapper(document.body).findTextFilter()!.findInput().setInputValue("bob"); });
    expect(screen.queryByText("alice@example.com")).toBeNull();
  });
  it("opens a build read-only: its workflow, its chat, its runs", async () => {
    await act(async () => { render(<Admin canDestroy notify={vi.fn()} onOpenRun={vi.fn()} />); });
    await act(async () => { fireEvent.click(screen.getByText("Claims")); });
    await waitFor(() => expect(screen.getByText(/Read-only/)).toBeTruthy());
    expect(calls.map((c) => c.path)).toEqual(expect.arrayContaining(
      ["/api/builds/pb1", "/api/builds/pb1/design", "/api/sessions?build=pb1&scope=all"]));
    expect(calls.every((c) => c.method === "GET")).toBe(true);
  });
  it("on a control-plane console lists builds only, and asks for no runs", async () => {
    await act(async () => { render(<Admin canDestroy notify={vi.fn()} onOpenRun={vi.fn()} runs={false} />); });
    expect(screen.getByText("Builds (2)")).toBeTruthy();
    expect(screen.queryByText(/^Runs/)).toBeNull();
    await act(async () => { fireEvent.click(screen.getByText("Claims")); });
    await waitFor(() => expect(screen.getByText(/Read-only/)).toBeTruthy());
    expect(calls.some((c) => c.path.startsWith("/api/sessions"))).toBe(false);
  });
  it("destroys only what is deployed, and only once its name is typed", async () => {
    const notify = vi.fn();
    await act(async () => { render(<Admin canDestroy notify={notify} onOpenRun={vi.fn()} />); });
    const table = createWrapper(document.body).findTable()!.getElement();
    const buttons = [...table.querySelectorAll("button")].filter((b) => b.textContent === "Destroy") as HTMLButtonElement[];
    expect(buttons.map((b) => b.disabled)).toEqual([false, true]);   // the draft has nothing in AWS
    await act(async () => { fireEvent.click(buttons[0]); });
    const modal = createWrapper(document.body).findAllModals().find((m) => m.findHeader().getElement().textContent?.includes("Destroy another"))!;
    const confirm = modal.findFooter()!.findAllButtons().find((b) => b.getElement().textContent === "Destroy")!;
    expect(confirm.getElement().hasAttribute("disabled")).toBe(true);
    await act(async () => { modal.findContent().findInput()!.setInputValue("Claims"); });
    await act(async () => { confirm.click(); });
    expect(calls.some((c) => c.method === "POST" && c.path === "/api/builds/pb1/destroy")).toBe(true);
    await waitFor(() => expect(notify).toHaveBeenCalledWith("info", expect.stringContaining("alice@example.com")));
  });
});
