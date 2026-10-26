/** In-product documentation (docsLinks.ts, InfoLink.tsx): nothing without a documentation
 *  URL; with one, an info icon whose popover names the feature, says what it is, and links
 *  to its section in a new tab; and the map and the code agree, entry for entry. */
import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { join, resolve } from "node:path";
import type { ReactNode } from "react";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

import { Builder } from "../builder/Builder";
import { Inspector } from "../builder/Inspector";
import { addTool, newProject, type Project } from "../builder/model";
import {
  DOCS, DocsContext, docsBase, docsHref, docsTarget, githubSlug, xwikiAnchor, type DocsEntry, type DocsFeature,
} from "./docsLinks";
import { InfoLink } from "./InfoLink";

const BASE = "https://docs.example.com/product/";
/** The default every deployment gets (orchestrator/docs_default.json, read by CDK and TF). */
const GITHUB_DEFAULT: string = (() => {
  const d = JSON.parse(readFileSync(join(__dirname, "..", "..", "..", "docs_default.json"), "utf8"));
  return String(d.urlTemplate).replace("{ref}", d.ref);
})();
/** An XWiki copy of the guide: its pages are under /bin/view/. */
const WIKI = "https://wiki.example.com/bin/view/AgentExpress/";

/** A Markdown page's headings as GitHub renders their text: ATX headings outside fenced
 *  code, with links, code spans, emphasis and HTML reduced to their text. */
function markdownHeadings(md: string): string[] {
  const out: string[] = [];
  let fence = "";
  for (const line of md.split("\n")) {
    const f = /^\s{0,3}(`{3,}|~{3,})/.exec(line);
    if (f) {
      if (!fence) fence = f[1][0];
      else if (f[1][0] === fence) fence = "";
      continue;
    }
    if (fence) continue;
    const m = /^\s{0,3}#{1,6}\s+(.*?)(?:\s+#+)?\s*$/.exec(line);
    if (!m) continue;
    out.push(m[1]
      .replace(/!\[([^\]]*)\]\([^)]*\)/g, "$1").replace(/\[([^\]]*)\]\([^)]*\)/g, "$1")
      .replace(/<[^>]+>/g, "").replace(/`/g, "").replace(/\*\*|__|\*/g, "").replace(/\\([\\`*_{}[\]()#+\-.!])/g, "$1")
      .trim());
  }
  return out;
}
/** An XWiki page's headings: `= text =` lines outside {{code}}, markup reduced to text. */
function wikiHeadings(src: string): string[] {
  const out: string[] = [];
  let code = false;
  for (const line of src.split("\n")) {
    if (/^\{\{code/.test(line)) code = true;
    if (/\{\{\/code\}\}/.test(line)) { code = false; continue; }
    if (code) continue;
    const m = /^(=+)\s*(.*?)\s*=+\s*$/.exec(line);
    if (m) out.push(m[2].replace(/\[\[([^\]>]*)>>[^\]]*\]\]/g, "$1").replace(/\*\*|\/\/|##|__/g, "").trim());
  }
  return out;
}
/** The ids a page's headings get: each target numbers a repeat of the same id -1, -2. */
function numbered(headings: string[], id: (h: string, n: number) => string): string[] {
  const seen = new Map<string, number>();
  return headings.map((h) => {
    const base = id(h, 0);
    const n = seen.get(base) ?? 0;
    seen.set(base, n + 1);
    return id(h, n);
  });
}
const githubIds = (headings: string[]) => numbered(headings, githubSlug);
const wikiIds = (headings: string[]) => numbered(headings, xwikiAnchor);
/** Every map entry whose page or section is not in `ids(page)` (undefined: no such page). */
function unresolved(ids: (page: string) => string[] | undefined, target: string): string[] {
  const bad: string[] = [];
  for (const [feature, e] of Object.entries(DOCS) as [DocsFeature, DocsEntry][]) {
    const have = ids(e.page);
    const want = new URL(docsHref(target, feature)).hash.slice(1);
    if (!have) bad.push(`${feature}: no page ${e.page || "(home)"}`);
    else if (!have.includes(want)) bad.push(`${feature}: no #${want} (${JSON.stringify(e.heading)}) on ${e.page || "(home)"}`);
  }
  return bad;
}
/** The Builder and Inspector tests click through many tabs and popovers: under the whole
 *  suite's parallel load they need more than vitest's default 5 seconds. */
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

const withDocs = (base: string, node: ReactNode) => <DocsContext.Provider value={base}>{node}</DocsContext.Provider>;
/** Every info icon on the page, and every documentation link. */
const docsMarks = () => document.querySelectorAll("[data-docs-feature]").length
  + screen.queryAllByRole("button", { name: /^About / }).length
  + screen.queryAllByRole("link", { name: /Documentation/ }).length;

function sample(): { project: Project; agent: string; tool: string } {
  const r = addTool(newProject("Docs test"), "Claims", "mcp");
  return { project: r.project, agent: Object.keys(r.project.workflow.agents)[0], tool: r.key };
}
const clickTab = async (name: string) => {
  await act(async () => { fireEvent.click(screen.getByText(name, { selector: "[role=tab] *" })); });
};

describe("docsBase", () => {
  it("takes an https URL only, and ends it with a slash", () => {
    expect(docsBase({ docsUrl: BASE })).toBe(BASE);
    expect(docsBase({ docsUrl: "https://docs.example.com/product" })).toBe(BASE);
    for (const bad of ["", undefined, "http://docs.example.com/", "javascript:alert(1)", "not a url"]) {
      expect(docsBase({ docsUrl: bad })).toBeUndefined();
    }
  });
  it("is undefined when the deployment turned documentation links off", () => {
    expect(docsBase({ docsUrl: BASE, docsLinks: false })).toBeUndefined();
    expect(docsBase({ docsUrl: BASE, docsLinks: true })).toBe(BASE);
  });
  it("links to the GitHub Markdown by default: base + page.md + # + slug, the home page as Home.md", () => {
    expect(docsTarget(BASE)).toBe("github");
    expect(docsTarget(GITHUB_DEFAULT)).toBe("github");
    expect(docsHref(BASE, "agent.promptCache")).toBe(`${BASE}Prompt_Caching.md#turn-it-on-for-an-agent`);
    expect(docsHref(GITHUB_DEFAULT, "run.gate")).toBe(`${GITHUB_DEFAULT}Human_in_the_Loop.md#approve-revise-or-deny`);
    expect(docsHref(GITHUB_DEFAULT, "about.console")).toBe(`${GITHUB_DEFAULT}Home.md#what-is-agentexpress`);
  });
  it("links to an XWiki site (a /bin/view/ path) as base + page + # + H-id, the home page as the base", () => {
    expect(docsTarget(WIKI)).toBe("wiki");
    expect(docsHref(WIKI, "agent.promptCache")).toBe(`${WIKI}Prompt_Caching#HTurnitonforanagent`);
    expect(docsHref(WIKI, "run.gate")).toBe(`${WIKI}Human_in_the_Loop#HApprove2Creviseordeny`);
    expect(docsHref(WIKI, "about.console")).toBe(`${WIKI}#HWhatisAgentExpress`);
  });
});

describe("the anchors, per target, from real headings", () => {
  it("XWiki: H + the text without spaces; - . _ : kept; anything else as hex; a repeat -1, -2", () => {
    expect(xwikiAnchor("Approve, revise or deny")).toBe("HApprove2Creviseordeny");
    expect(xwikiAnchor("Set the execution plane's text")).toBe("HSettheexecutionplane27stext");
    expect(xwikiAnchor("Use the named-item tables")).toBe("HUsethenamed-itemtables");
    expect(xwikiAnchor("Can the AgentExpress Assistant deploy my build?")).toBe("HCantheAgentExpressAssistantdeploymybuild3F");
    expect(xwikiAnchor("Run Assistant, live config and run files")).toBe("HRunAssistant2Cliveconfigandrunfiles");
    expect(xwikiAnchor("Workflow.json: snake_case keys")).toBe("HWorkflow.json:snake_casekeys");
    expect(xwikiAnchor("Model", 1)).toBe("HModel-1");
    expect(xwikiAnchor("Model", 2)).toBe("HModel-2");
  });
  it("GitHub: lower case, punctuation but - and _ removed, spaces as -; a repeat -1, -2", () => {
    expect(githubSlug("Approve, revise or deny")).toBe("approve-revise-or-deny");
    expect(githubSlug("Set the execution plane's text")).toBe("set-the-execution-planes-text");
    expect(githubSlug("Use the named-item tables")).toBe("use-the-named-item-tables");
    expect(githubSlug("Can the AgentExpress Assistant deploy my build?")).toBe("can-the-agentexpress-assistant-deploy-my-build");
    expect(githubSlug("Stages, steps, branches and review gates")).toBe("stages-steps-branches-and-review-gates");
    expect(githubSlug("Workflow.json: snake_case keys")).toBe("workflowjson-snake_case-keys");
    expect(githubSlug("Model", 1)).toBe("model-1");
    expect(githubSlug("Model", 2)).toBe("model-2");
  });
  it("counts a repeated heading the way each target does", () => {
    // Runtimes' AgentCore Harness sub-sections, were a page to repeat them.
    const page = ["AgentCore Harness", "Model", "Tools", "Model", "Model"];
    expect(githubIds(page)).toEqual(["agentcore-harness", "model", "tools", "model-1", "model-2"]);
    expect(wikiIds(page)).toEqual(["HAgentCoreHarness", "HModel", "HTools", "HModel-1", "HModel-2"]);
  });
  it("reads a Markdown heading as GitHub renders it", () => {
    expect(markdownHeadings([
      "# AgentExpress", "", "```bash", "# not a heading", "```", "## Edit `workflow.json` by **hand**",
      "### [Teams](Teams.md) and team workspaces ###", "#nospace is text",
    ].join("\n"))).toEqual(["AgentExpress", "Edit workflow.json by hand", "Teams and team workspaces"]);
  });
});

describe("with no documentation URL", () => {
  it("renders no info icon or documentation link on the Builder's tabs, Settings included", async () => {
    await act(async () => { render(<Builder notify={vi.fn()} />); });
    expect(docsMarks()).toBe(0);
    for (const tab of ["Tools (0)", "Skills (0)", "Memory (0)", "Guardrails (0)", "Policies (0)", "Interceptors (0)",
      "Triggers (0)", "Evals (0)", "Tests (0)", "Settings", "workflow.json"]) {
      await clickTab(tab);
      expect(docsMarks()).toBe(0);
    }
  }, SLOW);
  it("renders none in the Inspector: an agent, a stage, a tool", async () => {
    const { project, agent, tool } = sample();
    const { rerender } = render(<Inspector project={project} selection={{ kind: "agent", id: agent }} issues={[]} onChange={vi.fn()} onSelect={vi.fn()} />);
    expect(screen.getByText("Prompt caching")).toBeTruthy();
    expect(docsMarks()).toBe(0);
    rerender(<Inspector project={project} selection={{ kind: "stage", index: 0 }} issues={[]} onChange={vi.fn()} onSelect={vi.fn()} />);
    expect(docsMarks()).toBe(0);
    rerender(<Inspector project={project} selection={{ kind: "tool", id: tool }} issues={[]} onChange={vi.fn()} onSelect={vi.fn()} />);
    expect(screen.getByText("How it connects")).toBeTruthy();
    expect(docsMarks()).toBe(0);
  }, SLOW);
  it("InfoLink itself renders nothing", () => {
    const { container } = render(<InfoLink feature="agent" />);
    expect(container.innerHTML).toBe("");
  });
});

describe("with a documentation URL", () => {
  /** Open `About <title>` and check its popover: header, description, and the link. */
  async function openAndCheck(feature: DocsFeature, nth = 0) {
    const { title, description } = DOCS[feature];
    // No two features on one screen share a label: "About X" always explains the same thing.
    const byLabel = new Map<string, Set<string>>();
    for (const b of screen.getAllByRole("button", { name: /^About / })) {
      const l = b.getAttribute("aria-label") ?? "";
      byLabel.set(l, (byLabel.get(l) ?? new Set()).add(b.closest("[data-docs-feature]")?.getAttribute("data-docs-feature") ?? ""));
    }
    expect([...byLabel].filter(([, fs]) => fs.size > 1).map(([l, fs]) => `${l}: ${[...fs].join(", ")}`)).toEqual([]);
    const button = screen.getAllByRole("button", { name: `About ${title}` })[nth];
    await act(async () => { fireEvent.click(button); });
    // The popover (the Builder keeps its closed modals in the page as dialogs too).
    const open = () => screen.getAllByRole("dialog").filter((d) => within(d).queryByText(description));
    await waitFor(() => expect(open()).toHaveLength(1));
    const dialog = open()[0];
    expect(within(dialog).getByText(title)).toBeTruthy();
    expect(within(dialog).getByText(description)).toBeTruthy();
    const link = within(dialog).getByRole("link", { name: `Documentation: ${title} (opens in a new tab)` });
    expect(link.getAttribute("href")).toBe(docsHref(BASE, feature));
    expect(link.getAttribute("target")).toBe("_blank");
    expect(link.getAttribute("rel") ?? "").toMatch(/noopener/);
    // Dismiss, so the next popover is the only dialog.
    await act(async () => { fireEvent.click(within(dialog).getByRole("button", { name: "Close" })); });
  }

  it("gives each Builder tab an info icon whose popover links to its section", async () => {
    await act(async () => { render(withDocs(BASE, <Builder notify={vi.fn()} />)); });
    await openAndCheck("builder");
    await openAndCheck("tab.design");
    await clickTab("Policies (0)");
    await openAndCheck("tab.policies");
    await openAndCheck("policies.engine");
    await clickTab("Settings");
    await openAndCheck("tab.settings");
    for (const f of ["settings.ui", "settings.orchestrator", "settings.authorization", "settings.export", "settings.network",
      "settings.storage"] as const) {
      await openAndCheck(f);
    }
    await clickTab("workflow.json");
    await openAndCheck("tab.json");
  }, SLOW);
  it("gives the agent panel's sections, a stage and a tool theirs", async () => {
    const { project, agent, tool } = sample();
    const { rerender } = render(withDocs(BASE,
      <Inspector project={project} selection={{ kind: "agent", id: agent }} issues={[]} onChange={vi.fn()} onSelect={vi.fn()} />));
    for (const f of ["agent", "agent.prompt", "agent.promptCache", "agent.features", "agent.guardrails",
      "agent.memory", "agent.evaluations", "agent.identity", "agent.model", "agent.tools"] as const) {
      await openAndCheck(f);
    }
    // Network: the agent's own, second to none here (no Settings tab on this page).
    await openAndCheck("agent.network");
    rerender(withDocs(BASE, <Inspector project={project} selection={{ kind: "stage", index: 0 }} issues={[]} onChange={vi.fn()} onSelect={vi.fn()} />));
    await openAndCheck("stage");
    await openAndCheck("stage.gate");
    rerender(withDocs(BASE, <Inspector project={project} selection={{ kind: "tool", id: tool }} issues={[]} onChange={vi.fn()} onSelect={vi.fn()} />));
    await openAndCheck("tool.mcp");
    await openAndCheck("tool.connects");
    await openAndCheck("tool.hostedMcp");
  }, SLOW);
  it("labels the icon button for assistive technology and opens from the keyboard", async () => {
    render(withDocs(BASE, <InfoLink feature="agent.promptCache" />));
    const button = screen.getByRole("button", { name: "About Prompt caching" });
    expect(button.tagName).toBe("BUTTON");
    button.focus();
    expect(document.activeElement).toBe(button);
    // A native <button>: Enter and Space activate it, which fires click.
    await act(async () => { fireEvent.click(button); });
    expect(screen.getByRole("dialog")).toBeTruthy();
  });
});

describe("the map", () => {
  it("gives every entry a page, a heading, a title and a short description", () => {
    for (const [id, e] of Object.entries(DOCS) as [string, DocsEntry][]) {
      expect(e.heading.trim(), id).toBe(e.heading);
      expect(e.heading, id).not.toBe("");
      expect(e.n ?? 1, id).toBeGreaterThan(0);
      expect(e.page, id).toMatch(/^[A-Za-z0-9_]*$/);
      expect(e.title.trim(), id).not.toBe("");
      expect(e.description.trim(), id).not.toBe("");
      expect(e.description.length, `${id}: ${e.description.length} characters`).toBeLessThanOrEqual(160);
    }
  });

  /** Every featureId the code names, read from the source: InfoLink's feature, infoSlot's
   *  second argument, FeatureToggle's docs, InfoStrip's features, and the values of maps
   *  and functions typed DocsFeature. */
  function usedInCode(): Set<string> {
    const root = join(__dirname, "..");
    const files: string[] = [];
    const walk = (d: string) => {
      for (const n of readdirSync(d)) {
        const p = join(d, n);
        if (statSync(p).isDirectory()) walk(p);
        else if (/\.tsx?$/.test(n) && !/\.test\.tsx?$/.test(n) && !p.endsWith(join("docs", "docsLinks.ts"))) files.push(p);
      }
    };
    walk(root);
    const used = new Set<string>();
    const add = (re: RegExp, s: string) => { for (const m of s.matchAll(re)) used.add(m[1]); };
    for (const f of files) {
      const s = readFileSync(f, "utf8");
      add(/feature="([\w.]+)"/g, s);
      // feature={cond ? "a" : "b"}
      for (const m of s.matchAll(/feature=\{([^{}]*)\}/g)) add(/"([\w.]+)"/g, m[1]);
      add(/infoSlot\(docs, "([\w.]+)"\)/g, s);
      add(/\bdocs="([\w.]+)"/g, s);
      // formLayout.ts: an Advanced section's docs, and a layout's otherDocs.
      add(/\b(?:docs|otherDocs): "([\w.]+)"/g, s);
      for (const m of s.matchAll(/features=\{\[([\s\S]*?)\]\}/g)) add(/"([\w.]+)"/g, m[1]);
      for (const m of s.matchAll(/DocsFeature> = \{([\s\S]*?)\};/g)) add(/:\s*"([\w.]+)"/g, m[1]);
      for (const m of s.matchAll(/\): DocsFeature \{([\s\S]*?)\n\}/g)) add(/(?:return|\?\?) "([\w.]+)"/g, m[1]);
    }
    return used;
  }

  it("names only features the map has, and the code uses every entry", () => {
    const used = usedInCode();
    const keys = new Set(Object.keys(DOCS));
    expect([...used].filter((u) => !keys.has(u))).toEqual([]);
    expect([...keys].filter((k) => !used.has(k))).toEqual([]);
  });
  it("is the only place a documentation URL is written", () => {
    // No page path of the map, and no anchor, appears anywhere else in the source.
    const root = join(__dirname, "..");
    const hits: string[] = [];
    const walk = (d: string) => {
      for (const n of readdirSync(d)) {
        const p = join(d, n);
        if (statSync(p).isDirectory()) { walk(p); continue; }
        if (!/\.tsx?$/.test(n) || /\.test\.tsx?$/.test(n) || p.endsWith(join("docs", "docsLinks.ts"))) continue;
        const s = readFileSync(p, "utf8");
        for (const e of Object.values(DOCS) as DocsEntry[]) {
          // An XWiki id, a Markdown file's section, or a page path.
          if (s.includes(`#${xwikiAnchor(e.heading, e.n)}`) || s.includes(`.md#${githubSlug(e.heading, e.n)}`)
            || (e.page && (s.includes(`/${e.page}#`) || s.includes(`/${e.page}.md`)))) hits.push(`${p}: ${e.page}#${e.heading}`);
        }
      }
    };
    walk(root);
    expect(hits).toEqual([]);
  });
});

describe("every map entry resolves to a heading", () => {
  /** The public user guide, docs/user-guide/<Slug>.md at the repository root (Home.md is
   *  the home page). AX_USER_GUIDE_DIR checks another copy. A missing directory fails: the
   *  GitHub links are the default, so the guide must ship with the code. */
  const guide = resolve(process.env.AX_USER_GUIDE_DIR || join(__dirname, "..", "..", "..", "..", "docs", "user-guide"));
  it("in the generated Markdown (docs/user-guide/*.md), by GitHub slug", () => {
    expect(existsSync(guide), `${guide} is missing: the user guide must be at docs/user-guide/`).toBe(true);
    const ids = (page: string) => {
      const f = join(guide, `${page || "Home"}.md`);
      return existsSync(f) ? githubIds(markdownHeadings(readFileSync(f, "utf8"))) : undefined;
    };
    expect(unresolved(ids, GITHUB_DEFAULT)).toEqual([]);
  });
  /** The XWiki copy's page sources (<Slug>.xwiki, Home.xwiki the home page), when
   *  AX_WIKI_DIR names their directory. */
  const wiki = process.env.AX_WIKI_DIR;
  it.skipIf(!wiki)("in the wiki pages (AX_WIKI_DIR/*.xwiki), by XWiki id", () => {
    const ids = (page: string) => {
      const f = join(wiki!, `${page || "Home"}.xwiki`);
      return existsSync(f) ? wikiIds(wikiHeadings(readFileSync(f, "utf8"))) : undefined;
    };
    expect(unresolved(ids, WIKI)).toEqual([]);
  });
});
