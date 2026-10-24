/** In-product documentation, the gaps the owner found: no info icon ever stands alone
 *  (each sits beside visible heading text), the agent form's Advanced sections and every
 *  section of the tool panel have one, and none of it renders without a documentation URL. */
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("../api", () => ({ api: {
  get: vi.fn(async (p: string) => (p.endsWith("/docs") ? [] : { models: [] })),
  post: vi.fn(async () => ({})), put: vi.fn(async () => ({})), del: vi.fn(async () => ({})),
} }));
import { Builder, DESIGN_CAPTION, DESIGN_CAPTION_SHORT } from "../builder/Builder";
import type { Selection } from "../builder/Canvas";
import { Inspector } from "../builder/Inspector";
import { addTool, newProject, type Entry, type Project } from "../builder/model";
import { DOCS, DocsContext, docsHref, type DocsFeature } from "./docsLinks";
import { InfoLink } from "./InfoLink";

const BASE = "https://docs.example.com/product/";
/** Many tabs and panels under the whole suite's parallel load. */
const SLOW = 60_000;

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
beforeAll(() => {
  // React Flow observes its container; jsdom implements neither (see Builder.test.tsx).
  vi.stubGlobal("ResizeObserver", class { observe() {} unobserve() {} disconnect() {} });
  vi.stubGlobal("DOMMatrixReadOnly", class { m22 = 1; });
});
beforeEach(() => { vi.stubGlobal("localStorage", memoryStorage()); });
afterEach(cleanup);

const withDocs = (base: string, node: ReactNode) => <DocsContext.Provider value={base}>{node}</DocsContext.Provider>;
const icons = () => [...document.querySelectorAll("[data-docs-feature]")];

/** The visible text an icon sits beside: the text of its nearest ancestor that has any,
 *  at most three levels up (the label, heading or note it belongs to). An aria-label is
 *  not text, so an icon on its own finds nothing. */
function labelOf(icon: Element): string {
  let el: Element | null = icon;
  for (let i = 0; i < 3 && el; i++) {
    el = el.parentElement;
    const t = (el?.textContent ?? "").replace(/\s+/g, " ").trim();
    if (t) return t;
  }
  return "";
}
/** Every icon on the page has an accessible label and visible heading text beside it. */
function expectNoneAlone(where: string) {
  const alone = icons().filter((i) => !labelOf(i)).map((i) => `${where}: ${i.getAttribute("data-docs-feature")}`);
  expect(alone).toEqual([]);
  for (const i of icons()) {
    const button = within(i as HTMLElement).getByRole("button");
    expect(button.getAttribute("aria-label")).toBe(`About ${DOCS[i.getAttribute("data-docs-feature") as DocsFeature].title}`);
  }
}
/** The icon of `feature` is on the page, beside `text`. */
function expectIcon(feature: DocsFeature, text: string) {
  const mine = icons().filter((i) => i.getAttribute("data-docs-feature") === feature);
  expect(mine.length, `${feature} has an icon`).toBeGreaterThan(0);
  expect(labelOf(mine[0]), feature).toContain(text);
}
/** Open `feature`'s popover and check its link goes to its section. */
async function expectLink(feature: DocsFeature) {
  const { title, description } = DOCS[feature];
  const icon = icons().find((i) => i.getAttribute("data-docs-feature") === feature)!;
  await act(async () => { fireEvent.click(within(icon as HTMLElement).getByRole("button", { name: `About ${title}` })); });
  const open = () => screen.getAllByRole("dialog").filter((d) => within(d).queryByText(description));
  await waitFor(() => expect(open()).toHaveLength(1));
  const link = within(open()[0]).getByRole("link", { name: `Documentation: ${title} (opens in a new tab)` });
  expect(link.getAttribute("href")).toBe(docsHref(BASE, feature));
  await act(async () => { fireEvent.click(within(open()[0]).getByRole("button", { name: "Close" })); });
}

const clickTab = async (name: string) => {
  await act(async () => { fireEvent.click(screen.getByText(name, { selector: "[role=tab] *" })); });
};
const TABS = ["Tools (0)", "Skills (0)", "Memory (0)", "Guardrails (0)", "Identity (0)", "Policies (0)", "Interceptors (0)",
  "Triggers (0)", "Evals (0)", "Tests (0)", "Settings", "workflow.json"];

describe("labelOf", () => {
  it("finds nothing for an icon on its own, and the text beside one that is not", () => {
    render(withDocs(BASE, <div><div><div><InfoLink feature="tab.design" /></div></div></div>));
    expect(labelOf(icons()[0])).toBe("");
    cleanup();
    render(withDocs(BASE, <div><span>Design<InfoLink feature="tab.design" /></span></div>));
    expect(labelOf(icons()[0])).toBe("Design");
  });
});

describe("no info icon stands alone", () => {
  it("on any Builder tab: Design and Settings say what they hold, workflow.json names itself", async () => {
    await act(async () => { render(withDocs(BASE, <Builder notify={vi.fn()} />)); });
    expectNoneAlone("Design");
    // Design's caption is in the canvas, beside Show: no row between the tab bar and the canvas.
    expectIcon("tab.design", DESIGN_CAPTION);
    // What shows is short enough never to be cut off; the whole sentence is its tooltip,
    // and the only copy a screen reader reads.
    expect(DESIGN_CAPTION_SHORT.split(/\s+/).length).toBeLessThanOrEqual(4);
    const caption = document.querySelector("[data-testid=canvas-caption] .axb-canvas-caption-text")!;
    expect(caption.getAttribute("title")).toBe(DESIGN_CAPTION);
    expect(within(caption as HTMLElement).getByText(DESIGN_CAPTION_SHORT).closest("[aria-hidden=true]")).not.toBeNull();
    expect(within(caption as HTMLElement).getByText(DESIGN_CAPTION).closest("[aria-hidden=true]")).toBeNull();
    expect(document.querySelector("[data-testid=canvas-caption]")?.closest(".axb-canvas")).not.toBeNull();
    expect(document.querySelector("[data-testid=tab-level-design]")).toBeNull();
    await expectLink("tab.design");
    for (const tab of TABS) {
      await clickTab(tab);
      expectNoneAlone(tab);
    }
    await clickTab("Settings");
    expectIcon("tab.settings", "Build-wide settings that apply to every agent");
    await clickTab("workflow.json");
    expectIcon("tab.json", "workflow.json");
    await expectLink("tab.json");
    await clickTab("Identity (0)");
    await clickTab("Tool access");
    expectNoneAlone("Identity, Tool access");
  }, SLOW);

  it("adds no Design caption or Settings row without a documentation URL", async () => {
    await act(async () => { render(withDocs("", <Builder notify={vi.fn()} />)); });
    expect(document.querySelector("[data-testid=canvas-caption]")).toBeNull();
    expect(document.querySelector("[data-testid=tab-level-design]")).toBeNull();
    expect(screen.queryByText(DESIGN_CAPTION)).toBeNull();
    await clickTab("Settings");
    expect(document.querySelector("[data-testid=tab-level-settings]")).toBeNull();
  }, SLOW);

  it("in the agent, stage, tool and AgentCore panels", async () => {
    const r = addTool(newProject("Docs test"), "Claims", "mcp");
    const project = r.project;
    const agent = Object.keys(project.workflow.agents)[0];
    const at = (selection: Selection) => withDocs(BASE,
      <Inspector project={project} selection={selection} issues={[]} onChange={vi.fn()} onSelect={vi.fn()} server />);
    const { rerender } = render(at({ kind: "agent", id: agent }));
    expectNoneAlone("agent");
    expectIcon("agent.attached", "Attached to this agent");
    for (const selection of [{ kind: "stage", index: 0 }, { kind: "tool", id: r.key }, { kind: "core", id: "gateway" }, null] as Selection[]) {
      rerender(at(selection));
      expectNoneAlone(JSON.stringify(selection));
    }
    rerender(at({ kind: "core", id: "gateway" }));
    expectIcon("gateway", "Gateway");
  }, SLOW);
});

describe("the agent form's Advanced sections", () => {
  const ADVANCED: [DocsFeature, string][] = [["agent.tuning", "Model tuning"], ["agent.toolUse", "Tool use"],
    ["agent.images", "Images"], ["agent.code", "Code"], ["agent.labels", "Labels"], ["agent.other", "Other"]];
  const panel = (base: string) => {
    const project = newProject("Docs test");
    const agent = Object.keys(project.workflow.agents)[0];
    return withDocs(base, <Inspector project={project} selection={{ kind: "agent", id: agent }} issues={[]} onChange={vi.fn()} onSelect={vi.fn()} />);
  };
  it("each have an info icon beside their name, linking to their section", async () => {
    render(panel(BASE));
    for (const [feature, title] of ADVANCED) {
      expectIcon(feature, title);
      await expectLink(feature);
    }
    expectNoneAlone("agent");
  }, SLOW);
  it("render, with no icon, without a documentation URL", () => {
    render(panel(""));
    for (const [, title] of ADVANCED) expect(screen.getAllByText(title).length).toBeGreaterThan(0);
    expect(icons()).toEqual([]);
    expect(screen.queryAllByRole("button", { name: /^About / })).toEqual([]);
  });
});

describe("the tool panel's sections", () => {
  /** A project with one tool of each kind the panel draws differently. */
  function tools(): { project: Project; keys: Record<string, string> } {
    let p = newProject("Docs test");
    const keys: Record<string, string> = {};
    for (const type of ["mcp", "kb", "websearch", "openapi", "lambda", "apigateway", "smithy"]) {
      const r = addTool(p, `T ${type}`, type);
      p = r.project;
      keys[type] = r.key;
    }
    // A Lambda tool whose code is written in the build.
    const code = addTool(p, "T code", "lambda");
    p = code.project;
    const { lambdaArn: _a, ...rest } = p.workflow.tools[code.key] as Entry;
    p = { ...p, workflow: { ...p.workflow, tools: { ...p.workflow.tools, [code.key]: { ...rest, code: {} } as Entry } } };
    keys.code = code.key;
    return { project: p, keys };
  }
  const EXPECT: Record<string, [DocsFeature, string][]> = {
    mcp: [["tool.mcp", ""], ["tool.connects", "How it connects"], ["tool.calling", "Calling"], ["tool.policy", "Policy"],
      ["tool.other", "Other"], ["tool.hostedMcp", "MCP server"]],
    kb: [["tool.kb", ""], ["tool.search", "Search and filters"], ["tool.kbProcessing", "Document processing"],
      ["tool.kbDataSource", "Where documents come from"], ["tool.kbVectorStore", "Vector store"], ["tool.kbDocuments", "Documents"]],
    websearch: [["tool.websearch", ""], ["tool.search", "Search and filters"]],
    openapi: [["tool.openapi", ""], ["tool.openapiUpload", "Upload an OpenAPI document"], ["tool.calling", "Calling"]],
    lambda: [["tool.lambda", ""], ["tool.lambdaFunction", "The function"], ["tool.calling", "Calling"]],
    apigateway: [["tool.apigateway", ""], ["tool.calling", "Calling"]],
    smithy: [["tool.smithy", ""]],
    code: [["tool.lambdaFunction", "The function"], ["tool.codeSource", "Where the code comes from"], ["tool.code", "What it may reach"],
      ["tool.codeCheck", "Check and run"], ["tool.codeTest", "Test the deployed tool"]],
  };
  it("each have an info icon beside their name, for every tool type", async () => {
    const { project, keys } = tools();
    const at = (id: string) => withDocs(BASE,
      <Inspector project={project} selection={{ kind: "tool", id }} issues={[]} onChange={vi.fn()} onSelect={vi.fn()} server />);
    const { rerender } = render(at(keys.mcp));
    for (const [type, want] of Object.entries(EXPECT)) {
      await act(async () => { rerender(at(keys[type])); });
      expectNoneAlone(type);
      for (const [feature, text] of want) expectIcon(feature, text);
    }
    await act(async () => { rerender(at(keys.kb)); });
    await expectLink("tool.kbDataSource");
    await act(async () => { rerender(at(keys.code)); });
    await expectLink("tool.codeCheck");
  }, SLOW);
  it("have no icon without a documentation URL", async () => {
    const { project, keys } = tools();
    const at = (id: string) => withDocs("",
      <Inspector project={project} selection={{ kind: "tool", id }} issues={[]} onChange={vi.fn()} onSelect={vi.fn()} server />);
    const { rerender } = render(at(keys.mcp));
    for (const type of Object.keys(EXPECT)) {
      await act(async () => { rerender(at(keys[type])); });
      expect(icons(), type).toEqual([]);
    }
    expect(screen.getAllByText("Check and run").length).toBeGreaterThan(0);
  }, SLOW);
});
