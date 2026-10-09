/** The shell offers Build only where the deployment has the Builder (a console). A
 *  build's own app — deployed with builder=false — is for running and observing it. */
import { render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const me = vi.hoisted(() => ({ value: {} as Record<string, unknown> }));
const workflowUi = vi.hoisted(() => ({ value: { title: "My app" } as Record<string, string> }));

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
      if (path === "/api/me") return me.value;
      if (path.startsWith("/api/workflow")) return { agents: {}, steps: [], ui: workflowUi.value };
      if (path.startsWith("/api/builds")) return [];
      return [];
    },
    post: async () => ({}), put: async () => ({}), del: async () => ({}),
  },
}));

import App from "./App";

const BASE = { owner: "u1", groups: [], permittedActions: null, authzEnabled: false, email: "u@example.com" };

/** Node's own global localStorage shadows jsdom's (see Builder.test.tsx). */
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

describe("the Build view", () => {
  beforeEach(() => { vi.stubGlobal("localStorage", memoryStorage()); });

  it("is absent from a build's own app", async () => {
    me.value = { ...BASE, builder: false, consoleMode: "app" };
    render(<App />);
    await waitFor(() => expect(screen.getAllByText("Runs").length).toBeGreaterThan(0));
    expect(screen.queryByRole("link", { name: /^Build/ })).toBeNull();
    expect(screen.queryByText("AWS accounts")).toBeNull();
  });

  it("is branded AgentExpress - <its title> in a build's own app, and the console is not", async () => {
    me.value = { ...BASE, builder: false, consoleMode: "app" };
    const { unmount } = render(<App />);
    await waitFor(() => expect(document.title).toBe("AgentExpress - My app"));
    expect(screen.getAllByText("🧭 AgentExpress - My app").length).toBeGreaterThan(0);
    unmount();
    workflowUi.value = { title: "AgentExpress", heading: "🧭 AgentExpress" };
    me.value = { ...BASE, builder: true, consoleMode: "builder" };
    render(<App />);
    await waitFor(() => expect(screen.getAllByText("🧭 AgentExpress").length).toBeGreaterThan(0));
    expect(document.title).toBe("AgentExpress");
    workflowUi.value = { title: "My app" };
  });

  it("opens what a link names, on load and when the address changes", async () => {
    me.value = { ...BASE, builder: true, consoleMode: "builder" };
    window.location.hash = "#library:interceptor";
    try {
      render(<App />);
      await waitFor(() => expect(screen.getAllByText(/Publish one from a build's Interceptors tab/).length).toBeGreaterThan(0));
      window.location.hash = "#accounts";
      window.dispatchEvent(new HashChangeEvent("hashchange"));
      await waitFor(() => expect(screen.queryAllByText(/Publish one from a build's Interceptors tab/)).toHaveLength(0));
    } finally { window.location.hash = ""; }
  });
  it("is there on a console with the Builder", async () => {
    me.value = { ...BASE, builder: true, consoleMode: "builder" };
    render(<App />);
    await waitFor(() => expect(screen.getAllByRole("link", { name: /^Build/ }).length).toBeGreaterThan(0));
  });
  it("lists the library grouped by where each kind takes effect, in the Builder's tab order", async () => {
    me.value = { ...BASE, builder: true, consoleMode: "builder" };
    render(<App />);
    await waitFor(() => expect(screen.getAllByText("AWS accounts").length).toBeGreaterThan(0));
    const links = [...document.querySelectorAll(".axn-links li")].map((li) => li.textContent ?? "");
    const from = links.indexOf("Agent library");
    expect(links.slice(from, from + 11)).toEqual(["Agent library", "Tools", "Skills", "Memory", "Guardrails",
      "Gateway and access library", "Identity", "Policies", "Interceptors", "Quality library", "Evals"]);
    const indented = [...document.querySelectorAll(".axn-links li.axn-indent")].map((li) => li.textContent);
    expect(indented).toEqual(["Tools", "Skills", "Memory", "Guardrails", "Identity", "Policies", "Interceptors", "Evals"]);
  });
});
