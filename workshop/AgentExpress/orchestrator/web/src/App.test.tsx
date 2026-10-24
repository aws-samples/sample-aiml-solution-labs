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
// The views App loads lazily (React.lazy in App.tsx), imported here so their module trees
// (React Flow, Cloudscape) are compiled when this file is collected. Otherwise the FIRST
// test to open one compiles it inside its 1 s wait for what the view shows, which under the
// whole suite's parallel load takes longer than that. The lazy import() then resolves from
// the module cache, and each wait times only the rendering it is about.
import "./builder/Accounts";
import "./builder/Builder";
import "./builder/Library";
import "./builder/Teams";

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
    // Before any build: the Assistant's start view offers it. The whole App's first render
    // in this group, so it gets longer than the default 1 s to appear under a full run.
    await waitFor(() => expect(screen.getByTestId("design-demo-video").textContent).toMatch(/^New here\?/),
      { timeout: 4000 });
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
describe("the documentation links", () => {
  beforeEach(() => {
    vi.stubGlobal("localStorage", memoryStorage());
    vi.stubGlobal("ResizeObserver", class { observe() {} unobserve() {} disconnect() {} });
    vi.stubGlobal("DOMMatrixReadOnly", class { m22 = 1; });
  });
  const DOCS_URL = "https://docs.example.com/product/";
  const docLinks = () => screen.queryAllByRole("link", { name: /Documentation/ });
  it("are a top-bar Documentation link to the home page, in a new tab, and info icons, when one is set", async () => {
    me.value = { ...BASE, builder: true, consoleMode: "builder", docsUrl: DOCS_URL };
    render(<App />);
    await waitFor(() => expect(docLinks().length).toBeGreaterThan(0));
    const top = docLinks().find((a) => a.getAttribute("href") === DOCS_URL)!;
    expect(top).toBeTruthy();
    expect(top.getAttribute("target")).toBe("_blank");
    expect(top.getAttribute("rel")).toMatch(/noopener/);
    // The Build page's own info icon.
    await waitFor(() => expect(screen.getAllByRole("button", { name: "About Build page" }).length).toBeGreaterThan(0));
  });
  it("are on a build's own app too", async () => {
    me.value = { ...BASE, builder: false, consoleMode: "app", docsUrl: DOCS_URL };
    render(<App />);
    await waitFor(() => expect(docLinks().some((a) => a.getAttribute("href") === DOCS_URL)).toBe(true));
  });
  /** The Assistant's own help (DesignChat.tsx): text that is only in a popover, so its
   *  icon stays with documentation off too (InfoLink `always`), with no Documentation link. */
  const ALWAYS = ["About AgentExpress Assistant", "About Review changes", "About Sending messages"];
  /** Nothing documentation-related on the page: no link, no info icon, no help toggle. */
  const expectNoDocs = () => {
    expect(docLinks()).toHaveLength(0);
    const icons = screen.queryAllByRole("button", { name: /^About (?!this application)/ });
    expect(icons.map((b) => b.getAttribute("aria-label")).filter((n) => !ALWAYS.includes(n ?? ""))).toEqual([]);
    expect(document.querySelectorAll("[data-docs-feature]")).toHaveLength(icons.length);
  };
  // One test per case: each renders the whole App, so three in one test is three times the work
  // inside one test's time limit.
  it.each([
    ["none is set", {}],
    ["it is not https", { docsUrl: "http://docs.example.com/product/" }],
    ["the deployment turned them off (docsLinks=false)", { docsUrl: DOCS_URL, docsLinks: false }],
  ])("are absent when %s", async (_why, docs) => {
    me.value = { ...BASE, builder: true, consoleMode: "builder", ...docs };
    const { unmount } = render(<App />);
    await waitFor(() => expect(screen.getByText("What would you like to build?")).toBeTruthy());
    expectNoDocs();
    // Nothing to turn off, so no Show help icons either.
    expect(screen.queryByRole("button", { name: "Settings" })).toBeNull();
    unmount();
  });
  const GITHUB = "https://github.com/aws-samples/sample-aiml-solution-labs/blob/agentexpress-v1.3.6/workshop/AgentExpress/docs/user-guide/";
  const WIKI = "https://wiki.example.com/bin/view/AgentExpress/";
  it.each([
    ["the user guide's Markdown on GitHub by default", GITHUB, `${GITHUB}Builder_Console.md#read-the-build-page`],
    ["the wiki's sections on a wiki", WIKI, `${WIKI}Builder_Console#HReadthebuildpage`],
  ])("go to %s", async (_where, docsUrl, href) => {
    me.value = { ...BASE, builder: true, consoleMode: "builder", docsUrl };
    const { unmount } = render(<App />);
    const [icon] = await screen.findAllByRole("button", { name: "About Build page" });
    fireEvent.click(icon);
    const link = await screen.findByRole("link", { name: "Documentation: Build page (opens in a new tab)" });
    expect(link.getAttribute("href")).toBe(href);
    expect(docLinks().some((a) => a.getAttribute("href") === docsUrl)).toBe(true);
    unmount();
  });
  it("go when a person turns Show help icons off, and stay off after a reload", async () => {
    me.value = { ...BASE, builder: true, consoleMode: "builder", docsUrl: DOCS_URL };
    const first = render(<App />);
    await waitFor(() => expect(screen.getAllByRole("button", { name: "About Build page" }).length).toBeGreaterThan(0));
    fireEvent.click(screen.getByRole("button", { name: "Settings" }));
    const toggle = await screen.findByRole("menuitemcheckbox", { name: "Show help icons" });
    expect(toggle.getAttribute("aria-checked")).toBe("true");
    fireEvent.click(toggle);
    await waitFor(() => expectNoDocs());
    expect(localStorage.getItem("agentexpress.docs.helpIcons")).toBe("off");
    first.unmount();
    // A reload: still off, and the toggle says so.
    render(<App />);
    await waitFor(() => expect(screen.getByText("What would you like to build?")).toBeTruthy());
    expectNoDocs();
    fireEvent.click(screen.getByRole("button", { name: "Settings" }));
    const again = await screen.findByRole("menuitemcheckbox", { name: "Show help icons" });
    expect(again.getAttribute("aria-checked")).toBe("false");
    // On again.
    fireEvent.click(again);
    await waitFor(() => expect(screen.getAllByRole("button", { name: "About Build page" }).length).toBeGreaterThan(0));
    expect(docLinks().some((a) => a.getAttribute("href") === DOCS_URL)).toBe(true);
    expect(localStorage.getItem("agentexpress.docs.helpIcons")).toBe("on");
  }, 30_000);   // about 4 s alone; two whole App renders, so slow under a full run
});
