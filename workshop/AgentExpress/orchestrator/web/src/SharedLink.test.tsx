/** A share link (#shared:<id>) opens the shared run in the shell, and Share is on the run
 *  page for the run's owner only. */
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const SHARE = "S".repeat(32);
const runs = vi.hoisted(() => ({ owner: "u1" }));
const reads = vi.hoisted(() => [] as string[]);
vi.mock("./auth", () => ({
  initAuth: async () => ({ user: "u@example.com" }),
  authEnabled: () => true,
  authConfig: () => ({}),
  logout: () => {},
}));
vi.mock("./api", () => ({
  ApiError: class extends Error {},
  uploadRunFile: vi.fn(),
  api: {
    get: async (path: string) => {
      reads.push(path);
      if (path === "/api/me") return { owner: "u1", groups: [], permittedActions: null, authzEnabled: false, consoleMode: "app" };
      if (path.startsWith("/api/workflow")) return { agents: { a: { name: "Alpha" } }, steps: [{ agent: "a" }], ui: { title: "My app" } };
      if (path === `/api/shared/${SHARE}`) {
        return { session_id: "abc123", topic: "Shared topic", overall: "done", nodes: {}, logs: [],
          sharedBy: "alice@example.com", expires: "2030-01-08T10:00:00Z", readOnly: true };
      }
      if (path === "/api/sessions/abc123") {
        return { session_id: "abc123", topic: "My topic", overall: "done", nodes: {}, logs: [], owner: runs.owner };
      }
      return [];
    },
    post: async () => ({}), put: async () => ({}), del: async () => ({}),
  },
}));
import App from "./App";

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

beforeEach(() => { vi.stubGlobal("localStorage", memoryStorage()); reads.length = 0; });
afterEach(() => { cleanup(); window.location.hash = ""; runs.owner = "u1"; });

describe("a share link", () => {
  it("opens the shared run read-only", async () => {
    window.location.hash = `#shared:${SHARE}`;
    render(<App />);
    await waitFor(() => expect(screen.getByText(/^Shared by alice@example.com, read-only, expires /)).toBeTruthy());
    expect(screen.getAllByText("Shared topic").length).toBeGreaterThan(0);
    expect(screen.queryByRole("button", { name: "Share" })).toBeNull();
    expect(reads).not.toContain("/api/sessions/abc123");     // the owner's run route is never read
  });
});

describe("Share on the run page", () => {
  it("is offered to the run's owner", async () => {
    window.location.hash = "#run:abc123";
    render(<App />);
    await waitFor(() => expect(screen.getAllByText("My topic").length).toBeGreaterThan(0));
    expect(screen.getByRole("button", { name: "Share" })).toBeTruthy();
  });

  it("is not offered on someone else's run (an admin reading it)", async () => {
    runs.owner = "u2";
    window.location.hash = "#run:abc123";
    render(<App />);
    await waitFor(() => expect(screen.getAllByText("My topic").length).toBeGreaterThan(0));
    expect(screen.queryByRole("button", { name: "Share" })).toBeNull();
  });
});
