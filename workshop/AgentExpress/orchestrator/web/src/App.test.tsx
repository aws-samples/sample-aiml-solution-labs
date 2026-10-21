/** The shell offers Build only where the deployment has the Builder (a console). A
 *  build's own app — deployed with builder=false — is for running and observing it. */
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const me = vi.hoisted(() => ({ value: {} as Record<string, unknown> }));
const workflowUi = vi.hoisted(() => ({ value: { title: "My app" } as Record<string, string> }));
/** What GET /api/builds lists (the caller's builds and those shared with them). */
const buildList = vi.hoisted(() => ({ value: [] as Record<string, unknown>[] }));

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
      // The AgentExpress Assistant's conversation, before the first message.
      if (/^\/api\/builds\/[^/]+\/design$/.test(path)) return { turns: [], status: "idle", revision: 0, starters: [] };
      if (path === "/api/builds") return buildList.value;
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
    // The words only: each link's emoji (lib/emoji.ts) is decoration.
    const words = (li: Element) => { const c = li.cloneNode(true) as Element; c.querySelectorAll(".axn-emoji").forEach((x) => x.remove()); return c.textContent ?? ""; };
    const links = [...document.querySelectorAll(".axn-links li")].map(words);
    const from = links.indexOf("Agent library");
    expect(links.slice(from, from + 11)).toEqual(["Agent library", "Tools", "Skills", "Memory", "Guardrails",
      "Gateway and access library", "Identity", "Policies", "Interceptors", "Quality library", "Evals"]);
    const indented = [...document.querySelectorAll(".axn-links li.axn-indent")].map(words);
    expect(indented).toEqual(["Tools", "Skills", "Memory", "Guardrails", "Identity", "Policies", "Interceptors", "Evals"]);
  });
});

describe("the demo video link", () => {
  beforeEach(() => {
    vi.stubGlobal("localStorage", memoryStorage());
    // The Assistant's workflow preview is a React Flow canvas (see DesignChat.test.tsx).
    vi.stubGlobal("ResizeObserver", class { observe() {} unobserve() {} disconnect() {} });
    vi.stubGlobal("DOMMatrixReadOnly", class { m22 = 1; });
  });
  const URL_ = "https://videos.example.com/demo";
  const about = () => screen.queryByRole("link", { name: /Watch the 10-minute demo/ });
  const openAbout = async () => {
    // The top bar's Info button.
    await waitFor(() => expect(screen.getAllByRole("button", { name: "About this application" }).length).toBeGreaterThan(0));
    fireEvent.click(screen.getAllByRole("button", { name: "About this application" })[0]);
  };

  it("is on a console's About panel and first-run view, opening in a new tab", async () => {
    me.value = { ...BASE, builder: true, consoleMode: "builder", demoVideoUrl: URL_ };
    render(<App />);
    // Before any build: the Assistant's start view offers it.
    await waitFor(() => expect(screen.getByTestId("design-demo-video").textContent).toMatch(/^New here\?/));
    await openAbout();
    await waitFor(() => expect(screen.getAllByRole("link", { name: /Watch the 10-minute demo/ }).length).toBe(2));
    for (const a of screen.getAllByRole("link", { name: /Watch the 10-minute demo/ })) {
      expect(a.getAttribute("href")).toBe(URL_);
      expect(a.getAttribute("target")).toBe("_blank");
      expect(a.getAttribute("rel")).toMatch(/noopener/);
      expect(a.getAttribute("rel")).toMatch(/noreferrer/);
    }
  });
  it("links to the whole URL, its #fragment included", async () => {
    const hashed = "https://amazon.awsapps.com/workdocs-amazon/index.html#/document/599976615066782999eeb979bf63d7fa2b7eed78bf6e20390b414d6d4cd0b9db";
    me.value = { ...BASE, builder: true, consoleMode: "builder", demoVideoUrl: hashed };
    render(<App />);
    await waitFor(() => expect(screen.getByTestId("design-demo-video")).toBeTruthy());
    await openAbout();
    await waitFor(() => expect(screen.getAllByRole("link", { name: /Watch the 10-minute demo/ }).length).toBe(2));
    for (const a of screen.getAllByRole("link", { name: /Watch the 10-minute demo/ })) {
      expect(a.getAttribute("href")).toBe(hashed);
    }
  });
  it("is on the first-run view while every build listed is someone else's, and not once one is theirs", async () => {
    me.value = { ...BASE, builder: true, consoleMode: "builder", demoVideoUrl: URL_ };
    // Shared with everyone by another user: not the caller's own.
    const shared = { id: "pshared01", name: "Shared", updated: "2026-10-20T10:00:00Z", shared: true,
      ownerEmail: "someone@example.com", shares: { emails: [], groups: [], everyone: true } };
    buildList.value = [shared];
    try {
      const { unmount } = render(<App />);
      await waitFor(() => expect(screen.getByTestId("design-demo-video").textContent).toMatch(/^New here\?/));
      unmount();
      buildList.value = [shared, { id: "pmine0001", name: "Mine", updated: "2026-10-20T09:00:00Z" }];
      render(<App />);
      await waitFor(() => expect(screen.getByText("What would you like to build?")).toBeTruthy());
      expect(screen.queryByTestId("design-demo-video")).toBeNull();
    } finally { buildList.value = []; }
  });
  it("is absent from a console with none configured", async () => {
    me.value = { ...BASE, builder: true, consoleMode: "builder" };
    render(<App />);
    await waitFor(() => expect(screen.getByText("What would you like to build?")).toBeTruthy());
    await openAbout();
    await waitFor(() => expect(screen.getAllByText("What you can do").length).toBeGreaterThan(0));
    expect(about()).toBeNull();
    expect(screen.queryByTestId("design-demo-video")).toBeNull();
  });
  it("is absent from a build's own app, even with a value", async () => {
    me.value = { ...BASE, builder: false, consoleMode: "app", demoVideoUrl: URL_ };
    render(<App />);
    await openAbout();
    await waitFor(() => expect(screen.getAllByText("Key features").length).toBeGreaterThan(0));
    expect(about()).toBeNull();
  });
  it("is absent from an app with the Builder beside it", async () => {
    me.value = { ...BASE, builder: true, consoleMode: "app", demoVideoUrl: URL_ };
    window.location.hash = "#build";
    try {
      render(<App />);
      await waitFor(() => expect(screen.getByText("What would you like to build?")).toBeTruthy());
      expect(screen.queryByTestId("design-demo-video")).toBeNull();
      await openAbout();
      await waitFor(() => expect(screen.getAllByText("Key features").length).toBeGreaterThan(0));
      expect(about()).toBeNull();
    } finally { window.location.hash = ""; }
  });
});
