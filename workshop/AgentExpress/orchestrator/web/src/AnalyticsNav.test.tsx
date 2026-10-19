/** The shell offers Analytics to an admin of a workflow's app, and to nobody else: it reads
 *  every user's telemetry, and GET /api/analytics refuses anyone without `admin`. */
import { render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const me = vi.hoisted(() => ({ value: {} as Record<string, unknown> }));
const asked = vi.hoisted(() => ({ paths: [] as string[] }));
vi.mock("./auth", () => ({
  initAuth: async () => ({ user: { email: "u@example.com" } }),
  authEnabled: () => false,
  authConfig: () => ({}),
  logout: () => {},
}));
vi.mock("./api", () => ({
  ApiError: class extends Error {},
  uploadRunFile: vi.fn(),
  getToken: () => null,
  api: {
    get: async (path: string) => {
      asked.paths.push(path);
      if (path === "/api/me") return me.value;
      if (path.startsWith("/api/workflow")) return { agents: {}, steps: [], ui: { title: "My app" } };
      if (path.startsWith("/api/analytics")) {
        return { from: "2025-03-01", to: "2025-03-07", rowsRead: 7, capped: false, maxRows: 20000,
          percentiles: "exact", kinds: {}, tools: [], agents: [],
          coverage: { agents: [], tools: [], policyEngine: { enabled: true, mode: "ENFORCE" } } };
      }
      return [];
    },
    post: async () => ({}), put: async () => ({}), del: async () => ({}),
  },
}));
import App from "./App";

const BASE = { owner: "u1", groups: [], permittedActions: null, authzEnabled: true, email: "u@example.com",
  builder: false, consoleMode: "app" };

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

const navLinks = () => [...document.querySelectorAll(".axn-links li")].map((li) => li.textContent ?? "");

describe("the Analytics link", () => {
  beforeEach(() => { vi.stubGlobal("localStorage", memoryStorage()); asked.paths = []; });

  it("is offered to an admin, and opens the view", async () => {
    me.value = { ...BASE, admin: true };
    window.location.hash = "#analytics";
    try {
      render(<App />);
      await waitFor(() => expect(screen.getByText(/7 telemetry rows read/)).toBeTruthy());
      expect(navLinks()).toContain("Analytics");
      expect(asked.paths.some((p) => p.startsWith("/api/analytics?from="))).toBe(true);
    } finally { window.location.hash = ""; }
  });

  it("is not offered, and nothing is asked, without admin", async () => {
    me.value = { ...BASE, admin: false };
    window.location.hash = "#analytics";
    try {
      render(<App />);
      await waitFor(() => expect(navLinks()).toContain("About this application"));
      expect(navLinks()).not.toContain("Analytics");
      expect(asked.paths.some((p) => p.startsWith("/api/analytics"))).toBe(false);
    } finally { window.location.hash = ""; }
  });

  it("is not on a control-plane console, which runs nothing", async () => {
    me.value = { ...BASE, admin: true, builder: true, consoleMode: "builder" };
    render(<App />);
    await waitFor(() => expect(navLinks()).toContain("About this application"));
    expect(navLinks()).not.toContain("Analytics");
  });
});
