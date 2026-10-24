/** The Memory records page is offered to an app's admins only: not to other users, and not
 *  on a control-plane console, which runs nothing and so remembers nothing. */
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
  api: {
    get: async (path: string) => {
      asked.paths.push(path);
      if (path === "/api/me") return me.value;
      if (path.startsWith("/api/workflow")) return { agents: {}, steps: [], ui: { title: "My app" } };
      if (path === "/api/memory/agents") return { deployed: true, agents: [] };
      return [];
    },
    post: async () => ({}), put: async () => ({}), del: async () => ({}),
  },
}));

import App from "./App";

const BASE = { owner: "u1", groups: [], permittedActions: null, authzEnabled: true, email: "u@example.com", builder: false };
/** Node's own global localStorage shadows jsdom's (see App.test.tsx). */
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
// The words only: each link's emoji (lib/emoji.ts, aria-hidden) is decoration.
const links = () => [...document.querySelectorAll(".axn-links li")].map((li) => {
  const c = li.cloneNode(true) as Element;
  c.querySelectorAll(".axn-emoji").forEach((x) => x.remove());
  return c.textContent ?? "";
});

describe("the Memory records link", () => {
  beforeEach(() => { vi.stubGlobal("localStorage", memoryStorage()); asked.paths = []; window.location.hash = ""; });

  it("is there for an app's admin, and opens the page", async () => {
    me.value = { ...BASE, admin: true, consoleMode: "app" };
    window.location.hash = "#memory";
    try {
      render(<App />);
      await waitFor(() => expect(links()).toContain("Memory records"));
      await waitFor(() => expect(screen.getByText("No agent here keeps long-term memory.")).toBeTruthy());
      expect(asked.paths).toContain("/api/memory/agents");
    } finally { window.location.hash = ""; }
  });

  it("is not there for anyone else, and the page does not open from its address", async () => {
    me.value = { ...BASE, admin: false, consoleMode: "app" };
    window.location.hash = "#memory";
    try {
      render(<App />);
      await waitFor(() => expect(links()).toContain("About this application"));
      expect(links()).not.toContain("Memory records");
      expect(asked.paths).not.toContain("/api/memory/agents");
    } finally { window.location.hash = ""; }
  });

  it("is not there on a control-plane console", async () => {
    me.value = { ...BASE, admin: true, builder: true, consoleMode: "builder" };
    render(<App />);
    await waitFor(() => expect(links()).toContain("About this application"));
    expect(links()).not.toContain("Memory records");
  });
});
