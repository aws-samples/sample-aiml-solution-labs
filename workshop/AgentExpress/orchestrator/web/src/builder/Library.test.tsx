/** The library in the Build view: items used live (resolve / relink), a build's own list
 *  of one kind, sharing, and the tool chips on the canvas. */
import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { useState } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const calls: { method: string; path: string; body?: unknown }[] = [];
let items: unknown[] = [];
let groups: unknown[] = [];
vi.mock("../api", () => {
  const answer = (method: string, path: string, body?: unknown) => {
    calls.push({ method, path, body });
    if (method === "GET" && path.startsWith("/api/library")) return Promise.resolve(items);
    if (method === "GET" && path === "/api/groups") return Promise.resolve(groups);
    if (method === "POST" && path === "/api/library") return Promise.resolve({ id: "abcd0001", mine: true, ...(body as object) });
    if (method === "PUT" && path.endsWith("/shares")) return Promise.resolve(body);
    if (method === "PUT" && path.startsWith("/api/library/")) return Promise.resolve({ id: path.split("/")[3], mine: true, kind: "memory", name: "shared", ...(body as object) });
    return Promise.resolve({ ok: true });
  };
  return {
    api: {
      get: (p: string) => answer("GET", p),
      post: (p: string, b?: unknown) => answer("POST", p, b),
      put: (p: string, b?: unknown) => answer("PUT", p, b),
      del: (p: string) => answer("DELETE", p),
    },
  };
});

import { LibraryPage, NamedTab, ShareDialog, keptNote, libraryName, publishEntries, sharedLabel } from "./Library";
import { attachItem, relink, resolveProject, type Project, type Ref } from "./model";
import { layout, AGENT_H } from "./layout";
import type { LibraryItem } from "./storage";
import { validate } from "./validate";

const MEM: Ref = { id: "11111111", kind: "memory", name: "shared", definition: { strategies: ["semantic"], expiryDays: 60 } };
const TOOL: Ref = { id: "22222222", kind: "tool", name: "docs", definition: { type: "mcp", description: "Docs", endpoint: "https://d.example.com/mcp" },
  files: undefined };
const base = (): Project => ({ id: "p1", name: "P", createdAt: "", updatedAt: "", prompts: {},
  workflow: { agents: { a: { name: "A", maxTokens: 1000, tool: "docs", agentcore: { memory: { use: "shared" }, evaluations: { enabled: true } } } }, steps: [{ agent: "a" }],
    tools: { docs: { library: TOOL.id } }, memories: { shared: { library: MEM.id } } } as never });

beforeEach(() => { calls.length = 0; items = []; groups = []; });

describe("items used live", () => {
  it("shows a live item as it is now, and keeps it live through an edit elsewhere", () => {
    const raw = base();
    const refs = { [MEM.id]: MEM, [TOOL.id]: TOOL };
    const view = resolveProject(raw, refs);
    expect(view.workflow.tools.docs).toEqual(TOOL.definition);
    expect((view.workflow.memories as Record<string, unknown>).shared).toEqual(MEM.definition);
    expect(validate(view.workflow).filter((i) => i.severity === "error")).toEqual([]);
    // Renaming the agent changes nothing about the live items.
    const edited = structuredClone(view);
    edited.workflow.agents.a.name = "Renamed";
    const back = relink(raw, edited, refs);
    expect(back.workflow.tools.docs).toEqual({ library: TOOL.id });
    expect(back.workflow.agents.a.name).toBe("Renamed");
    // Editing the live tool itself makes it this build's own copy.
    const changed = structuredClone(view);
    changed.workflow.tools.docs = { ...changed.workflow.tools.docs, description: "Mine now" };
    expect(relink(raw, changed, refs).workflow.tools.docs).toMatchObject({ description: "Mine now", type: "mcp" });
  });
  it("names an item it could not load, and keeps the link", () => {
    const raw = base();
    const view = resolveProject(raw, {});
    expect(validate(view.workflow).some((i) => /could not be loaded/.test(i.message))).toBe(true);
    expect(relink(raw, view, {}).workflow.tools.docs).toEqual({ library: TOOL.id });
  });
  it("uses an item under a free key, once", () => {
    const p = { ...base(), workflow: { ...base().workflow, tools: { docs: { type: "kb", corpora: ["x"] } } } } as Project;
    const r = attachItem(p, "tools", TOOL);
    expect(r.key).toBe("docs2");
    expect(attachItem(r.project, "tools", TOOL).key).toBe("docs2");
  });
});

describe("a build's own list", () => {
  function Harness({ refs = {} as Record<string, LibraryItem> }) {
    const [p, setP] = useState<Project>(() => ({ ...base(), workflow: { ...base().workflow, memories: {} } }) as Project);
    (window as unknown as { latest: Project }).latest = p;
    return <NamedTab kind="memory" project={p} view={resolveProject(p, refs as never)} setProject={setP} refs={refs}
      addRefs={() => {}} issues={[]} server notify={vi.fn()} />;
  }
  const latest = () => (window as unknown as { latest: Project }).latest;
  it("adds one to the build, then publishes it to the library and uses it from there", async () => {
    await act(async () => { render(<Harness />); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Add memory" })); });
    const m = createWrapper(document.body).findAllModals().find((x) => x.isVisible())!;
    await act(async () => { m.findContent().findInput()!.setInputValue("shared"); });
    await act(async () => { fireEvent.click(within(m.getElement()).getByRole("button", { name: "Save" })); });
    expect((latest().workflow.memories as Record<string, unknown>).shared).toMatchObject({ strategies: ["semantic"] });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Publish shared to the library" })); });
    expect(calls.find((c) => c.method === "POST" && c.path === "/api/library")!.body).toMatchObject({ kind: "memory", name: "shared" });
    await waitFor(() => expect((latest().workflow.memories as Record<string, unknown>).shared).toEqual({ library: "abcd0001" }));
  });
  it("adds one from the library, live", async () => {
    items = [{ id: MEM.id, kind: "memory", name: "shared", definition: MEM.definition, mine: false, ownerEmail: "b@example.com" }];
    await act(async () => { render(<Harness />); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Add from library" })); });
    await waitFor(() => expect(screen.getByText("b@example.com")).toBeTruthy());
    const t = createWrapper(document.body).findAllModals().find((x) => x.isVisible())!.findContent().findTable()!;
    await act(async () => { t.findRowSelectionArea(1)!.click(); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Add 1" })); });
    expect((latest().workflow.memories as Record<string, unknown>).shared).toEqual({ library: MEM.id });
  });
});

describe("publishing to the library", () => {
  const selectAndPublish = async () => {
    const t = createWrapper(document.body).findTable()!;
    await act(async () => { t.findRowSelectionArea(1)!.click(); t.findRowSelectionArea(2)!.click(); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Publish to library (2)" })); });
  };
  it("publishes the selected entries at once, each under a free library name", async () => {
    items = [{ id: "x1", kind: "memory", name: "shared", definition: {}, mine: true }];
    function Two() {
      const [p, setP] = useState<Project>(() => ({ ...base(), workflow: { ...base().workflow,
        memories: { shared: { strategies: ["semantic"] }, notes_v2: { strategies: ["summary"] } } } }) as Project);
      (window as unknown as { latest: Project }).latest = p;
      return <NamedTab kind="memory" project={p} view={p} setProject={setP} refs={{}} addRefs={() => {}} issues={[]} server notify={vi.fn()} />;
    }
    await act(async () => { render(<Two />); });
    await selectAndPublish();
    const posts = calls.filter((c) => c.method === "POST" && c.path === "/api/library").map((c) => c.body as Record<string, unknown>);
    expect(posts.map((b) => b.name)).toEqual(["shared2", "notesv2"]);
    expect(posts[1].definition).toEqual({ strategies: ["summary"] });
    const mem = (window as unknown as { latest: Project }).latest.workflow.memories as Record<string, unknown>;
    await waitFor(() => expect(mem).toEqual({ shared: { library: "abcd0001" }, notes_v2: { library: "abcd0001" } }));
  });
  it("takes a code tool's files with it, and skips entries already from the library", async () => {
    const raw = { ...base(), toolCode: { calc: { "handler.py": "def lambda_handler(e, c): return 1" } },
      workflow: { ...base().workflow, tools: { calc: { type: "lambda", description: "Adds.", code: {} }, docs: { library: TOOL.id } } } } as Project;
    const notify = vi.fn();
    const next = (await publishEntries("tool", ["calc", "docs"], raw, resolveProject(raw, { [TOOL.id]: TOOL }), notify, () => {}))?.project;
    const posts = calls.filter((c) => c.method === "POST").map((c) => c.body as Record<string, unknown>);
    expect(posts).toHaveLength(1);
    expect(posts[0]).toMatchObject({ kind: "tool", name: "calc", files: { "handler.py": expect.stringContaining("lambda_handler") } });
    expect(next!.workflow.tools).toEqual({ calc: { library: "abcd0001" }, docs: { library: TOOL.id } });
    expect(next!.toolCode?.calc).toBeUndefined();
    expect(notify).toHaveBeenCalledWith("success", expect.stringContaining("calc is in your library now"));
  });
  it("offers Unpublish on a live entry, and says which builds kept a copy", async () => {
    const item = { id: MEM.id, kind: "memory", name: "shared", definition: MEM.definition, mine: true } as LibraryItem;
    const onUnpublish = vi.fn(async () => {});
    await act(async () => {
      render(<NamedTab kind="memory" project={base()} view={resolveProject(base(), { [MEM.id]: MEM })} setProject={() => {}}
        refs={{ [MEM.id]: item }} addRefs={() => {}} issues={[]} server notify={vi.fn()} onUnpublish={onUnpublish} />);
    });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Unpublish shared from the library" })); });
    expect(onUnpublish).toHaveBeenCalledWith(MEM.id, "shared");
    expect(keptNote([{ name: "Claims" }, { name: "Intake" }])).toBe(" The builds that used it (Claims, Intake) each keep their own copy.");
    expect(keptNote([])).toBe("");
  });
  it("shares from a build's tab: publishes its own entry first, then shares it", async () => {
    const confirm = vi.spyOn(window, "confirm").mockReturnValue(true);
    function One() {
      const [p, setP] = useState<Project>(() => ({ ...base(), workflow: { ...base().workflow, memories: { notes: { strategies: ["summary"] } } } }) as Project);
      (window as unknown as { latest: Project }).latest = p;
      return <NamedTab kind="memory" project={p} view={p} setProject={setP} refs={{}} addRefs={() => {}} issues={[]} server notify={vi.fn()} />;
    }
    await act(async () => { render(<One />); });
    await act(async () => { createWrapper(document.body).findTable()!.findRowSelectionArea(1)!.click(); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Share (1)" })); });
    expect(confirm).toHaveBeenCalled();
    expect(calls.some((c) => c.method === "POST" && c.path === "/api/library")).toBe(true);
    const m = await waitFor(() => createWrapper(document.body).findAllModals().find((x) => x.isVisible() && /Share notes/.test(x.getElement().textContent ?? ""))!);
    await act(async () => { m.findContent().findToggle()!.findNativeInput().click(); });
    await act(async () => { fireEvent.click(within(m.getElement()).getByRole("button", { name: "Save" })); });
    expect(calls.find((c) => c.method === "PUT" && c.path === "/api/library/abcd0001/shares")!.body).toMatchObject({ everyone: true });
    confirm.mockRestore();
  });
  it("makes a library name from any key", () => {
    expect(libraryName("my_tool-1", [])).toBe("mytool1");
    expect(libraryName("9lives", [])).toBe("x9lives");
    expect(libraryName("a".repeat(40), []).length).toBeLessThanOrEqual(32);
    expect(libraryName("docs", ["docs", "docs2"])).toBe("docs3");
  });
});

describe("sharing", () => {
  it("shares the items ticked on a library page with one Share button", async () => {
    items = [{ id: "t1", kind: "tool", name: "addNumbers", definition: {}, mine: true, shares: { emails: [], groups: [], everyone: false } },
      { id: "t2", kind: "tool", name: "subtractNumbers", definition: {}, mine: true, shares: { emails: ["x@example.com"], groups: [], everyone: false } }];
    await act(async () => { render(<LibraryPage kind="tool" notify={vi.fn()} />); });
    await waitFor(() => expect(screen.getByText("addNumbers")).toBeTruthy());
    expect(screen.getAllByRole("button", { name: /^Share/ })).toHaveLength(1);       // on top, none per row
    const t = createWrapper(document.body).findTable()!;
    await act(async () => { t.findRowSelectionArea(1)!.click(); t.findRowSelectionArea(2)!.click(); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Share (2)" })); });
    const m = createWrapper(document.body).findAllModals().find((x) => x.isVisible())!;
    expect(m.getElement().textContent).toContain("shared differently now");
    await act(async () => { m.findContent().findToggle()!.findNativeInput().click(); });
    await act(async () => { fireEvent.click(within(m.getElement()).getByRole("button", { name: "Save" })); });
    expect(calls.filter((c) => c.method === "PUT" && c.path.endsWith("/shares")).map((c) => c.path))
      .toEqual(["/api/library/t1/shares", "/api/library/t2/shares"]);
  });
  it("shares with people, groups and everyone", async () => {
    groups = [{ name: "claims team" }];
    const onSave = vi.fn(async () => {});
    await act(async () => { render(<ShareDialog visible title="Claims" onDismiss={() => {}} onSave={onSave} />); });
    const m = createWrapper(document.body).findModal()!;
    await act(async () => { m.findContent().findTextarea()!.setTextareaValue("A@Example.com\nb@example.com"); });
    await act(async () => { m.findContent().findToggle()!.findNativeInput().click(); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Save" })); });
    expect(onSave).toHaveBeenCalledWith({ emails: ["a@example.com", "b@example.com"], groups: [], everyone: true });
    expect(sharedLabel({ emails: ["a"], groups: ["g"], everyone: false })).toBe("2 people and groups");
    expect(sharedLabel(undefined)).toBe("Only you");
  });
  it("refuses what is not an email address", async () => {
    await act(async () => { render(<ShareDialog visible title="Claims" onDismiss={() => {}} onSave={vi.fn()} />); });
    const m = createWrapper(document.body).findModal()!;
    await act(async () => { m.findContent().findTextarea()!.setTextareaValue("not-an-address"); });
    expect(screen.getByText(/Not an email address: not-an-address/)).toBeTruthy();
  });
});

describe("the canvas shows an expanded agent's tools", () => {
  it("makes the expanded agent, and its stage, taller for its chips", () => {
    const wf = { agents: { a: { tool: ["x", "y", "z"] }, b: {} }, steps: [{ parallel: ["a", "b"] }], tools: {} } as never;
    const flat = layout(wf);
    const open = layout(wf, "a");
    expect(flat.agents[0].h).toBe(AGENT_H);
    expect(open.agents.find((x) => x.id === "a")!.h).toBeGreaterThan(AGENT_H);
    expect(open.agents.find((x) => x.id === "b")!.h).toBe(AGENT_H);
    expect(open.stages[0].h).toBeGreaterThan(flat.stages[0].h);
  });
});
