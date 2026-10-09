/** The Interceptors tab: checked templates become the interceptor's code, the code
 *  travels with the build, and an edit by hand is never overwritten silently. */
import { act, fireEvent, render, screen } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { useState } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const calls: { method: string; path: string; body?: unknown }[] = [];
// The interceptors in the library: one of the caller's, one shared with them.
const SHARED = { id: "lib-1", kind: "interceptor", name: "piiMask", description: "Masks emails.", mine: false,
  ownerEmail: "ana@example.com", definition: { point: "response", code: {}, templates: { redactPii: {} } },
  files: { "handler.py": "def lambda_handler(event, context):\n    return 'masked'\n" } };
const listed: unknown[] = [];
const SHARED_ALT: { id: string; kind: string; name: string; mine: boolean; definition: unknown; files: unknown } = {
  id: "lib-3", kind: "interceptor", name: "generatedOne", mine: true, definition: {}, files: {} };
const MINE = { id: "lib-2", kind: "interceptor", name: "noRefunds", description: "", mine: true,
  definition: { point: "request", lambdaArn: "arn:aws:lambda:us-east-1:123456789012:function:guard" } };
vi.mock("../api", () => {
  const answer = (method: string, path: string, body?: unknown) => {
    calls.push({ method, path, body });
    if (path === "/api/code/check") return Promise.resolve({ ok: true, problems: [], sandbox: { ran: true, ms: 10, note: "n", results: [] } });
    if (method === "GET" && path === "/api/library?kind=interceptor") return Promise.resolve([SHARED, MINE, ...listed]);
    if (method === "POST" && path === "/api/library") return Promise.resolve({ id: "lib-new", mine: true, ...(body as object) });
    if (method === "GET") return Promise.resolve([]);
    return Promise.resolve({ ok: true });
  };
  return { api: {
    get: (p: string) => answer("GET", p), post: (p: string, b?: unknown) => answer("POST", p, b),
    put: (p: string, b?: unknown) => answer("PUT", p, b), del: (p: string) => answer("DELETE", p),
  } };
});
import { BuildInterceptors, interceptorsOf } from "./Interceptors";
import { interceptorFiles } from "./interceptorCode";
import { toBundle, type Project } from "./model";
import { validate } from "./validate";

const base = (): Project => ({ id: "p1", name: "P", createdAt: "", updatedAt: "", prompts: {},
  workflow: { agents: { intake: { name: "Intake", tool: "refunds" } }, steps: [{ agent: "intake" }],
    tools: { refunds: { type: "lambda", description: "Refunds.", lambdaArn: "arn:aws:lambda:us-east-1:123456789012:function:r",
      toolSchema: [{ name: "issueRefund" }] } } } as never });

let latest: Project;
function Harness({ start = base() }: { start?: Project }) {
  const [p, setP] = useState(start);
  latest = p;
  // A new notify on every render, as an inline one would be: the picker must not reload for it.
  return <BuildInterceptors project={p} setProject={setP} issues={validate(p.workflow)} server notify={(t, m) => notes.push([t, m])} />;
}
const notes: [string, string][] = [];
const w = () => createWrapper(document.body);
const toggles = () => w().findAllToggles();
const box = (label: string) => w().findAllCheckboxes().find((c) => c.getElement().textContent?.includes(label))!;
beforeEach(() => { calls.length = 0; notes.length = 0; vi.restoreAllMocks(); });
const click = async (name: string | RegExp) => { await act(async () => { fireEvent.click(screen.getByRole("button", { name })); }); };
const picker = () => w().findAllModals().find((m) => m.getElement().textContent?.includes("Add an interceptor from the library"))!;
async function openPicker() {
  await click("Add from library");
  await act(async () => { await Promise.resolve(); });
}
async function choose(name: string) {
  const t = createWrapper(picker().getElement()).findTable()!;
  const row = t.findRows().findIndex((r) => r.getElement().textContent?.includes(name));
  await act(async () => { t.findRowSelectionArea(row + 1)!.click(); });
}
const addButton = () => picker().findFooter()!.findAllButtons().find((b) => b.getElement().textContent === "Add")!;

describe("the Interceptors tab", () => {
  it("turns one on, written here, with an audit log to start", async () => {
    await act(async () => { render(<Harness />); });
    await act(async () => { toggles()[0].findNativeInput().click(); });
    expect(interceptorsOf(latest.workflow).request).toEqual({ code: {}, templates: { audit: {} } });
    expect(latest.toolCode!["interceptor-request"]["handler.py"]).toContain("def audit(");
    expect(toBundle(latest).toolCode).toHaveProperty("interceptor-request");
  });
  it("writes each checked template into the code, and validates clean", async () => {
    await act(async () => { render(<Harness />); });
    await act(async () => { toggles()[0].findNativeInput().click(); });
    await act(async () => { box("Block tools").findNativeInput().click(); });
    const input = w().findAllInputs().find((i) => i.findNativeInput().getElement().getAttribute("aria-label") === "blockTools tools")!;
    await act(async () => { input.setInputValue("refunds___issueRefund"); });
    const ic = interceptorsOf(latest.workflow).request!;
    expect(ic.templates).toEqual({ audit: {}, blockTools: { tools: ["refunds___issueRefund"] } });
    const code = latest.toolCode!["interceptor-request"]["handler.py"];
    expect(code).toContain("def block_tools(");
    expect(code).toContain('"refunds___issueRefund"');
    expect(validate(latest.workflow).filter((i) => i.path.startsWith("orchestrator.interceptors"))).toEqual([]);
    await act(async () => { box("Block tools").findNativeInput().click(); });
    expect(latest.toolCode!["interceptor-request"]["handler.py"]).not.toContain("def block_tools(");
  });
  it("keeps code edited by hand until asked to generate it again", async () => {
    await act(async () => { render(<Harness />); });
    await act(async () => { toggles()[1].findNativeInput().click(); });
    const handler = () => w().findAllTextareas().find((a) => a.findNativeTextarea().getElement().getAttribute("aria-label") === "handler.py")!;
    await act(async () => { w().findAllExpandableSections()[0].findExpandButton().click(); });
    await act(async () => { handler().setTextareaValue("def lambda_handler(event, context):\n    return event\n"); });
    await act(async () => { box("Redact personal data").findNativeInput().click(); });
    expect(interceptorsOf(latest.workflow).response!.templates).toHaveProperty("redactPii");
    expect(latest.toolCode!["interceptor-response"]["handler.py"]).toContain("return event");
    expect(screen.getByText(/was edited by hand/)).toBeTruthy();
    vi.spyOn(window, "confirm").mockReturnValue(true);
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Generate code" })); });
    expect(latest.toolCode!["interceptor-response"]["handler.py"]).toContain("def redact_pii(");
  });
  it("takes a function of yours by ARN instead, and drops the generated files", async () => {
    await act(async () => { render(<Harness />); });
    await act(async () => { toggles()[0].findNativeInput().click(); });
    await act(async () => { w().findSegmentedControl()!.findSegments()[1].click(); });
    expect(interceptorsOf(latest.workflow).request).toEqual({ lambdaArn: "" });
    expect(latest.toolCode?.["interceptor-request"]).toBeUndefined();
    expect(validate(latest.workflow).some((i) => i.path === "orchestrator.interceptors.request.lambdaArn")).toBe(true);
  });
  it("checks the code with the same route as a code tool", async () => {
    await act(async () => { render(<Harness />); });
    await act(async () => { toggles()[0].findNativeInput().click(); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Check and run in the sandbox" })); });
    const sent = calls.find((c) => c.path === "/api/code/check")!.body as Record<string, unknown>;
    expect(sent.key).toBe("interceptor-request");
    expect(Object.keys(sent.files as object)).toContain("handler.py");
  });
  it("turning it off removes it and its files", async () => {
    await act(async () => { render(<Harness />); });
    await act(async () => { toggles()[0].findNativeInput().click(); });
    await act(async () => { toggles()[0].findNativeInput().click(); });
    expect((latest.workflow.orchestrator as Record<string, unknown>).interceptors).toBeUndefined();
    expect(latest.toolCode?.["interceptor-request"]).toBeUndefined();
  });
});

describe("Add from library", () => {
  it("opens the library and lists every interceptor, yours and shared, with where it runs", async () => {
    await act(async () => { render(<Harness />); });
    await openPicker();
    expect(calls.some((c) => c.method === "GET" && c.path === "/api/library?kind=interceptor")).toBe(true);
    const rows = createWrapper(picker().getElement()).findTable()!.findRows().map((r) => r.getElement().textContent ?? "");
    expect(rows).toHaveLength(2);
    expect(rows[0]).toContain("piiMask");
    expect(rows[0]).toContain("After each answer");
    expect(rows[0]).toContain("redactPii");
    expect(rows[0]).toContain("ana@example.com");
    expect(rows[1]).toContain("noRefunds");
    expect(rows[1]).toContain("Before each request");
    expect(rows[1]).toContain("Your Lambda");
    expect(rows[1]).toContain("You");
    expect(addButton().getElement().hasAttribute("disabled")).toBe(true);
  });
  it("adds the selected one to its own point, as a copy with its code", async () => {
    await act(async () => { render(<Harness />); });
    await openPicker();
    await choose("piiMask");
    expect(picker().getElement().textContent).toContain("return 'masked'");
    await act(async () => { addButton().click(); });
    expect(interceptorsOf(latest.workflow).response).toEqual({ code: {}, templates: { redactPii: {} } });
    expect(interceptorsOf(latest.workflow).request).toBeUndefined();
    expect(latest.toolCode!["interceptor-response"]["handler.py"]).toContain("return 'masked'");
    expect(latest.toolCode!["interceptor-response"]).not.toBe(SHARED.files);
    expect(validate(latest.workflow).filter((i) => i.path.startsWith("orchestrator.interceptors"))).toEqual([]);
    expect(notes.some(([t, m]) => t === "success" && m.includes("piiMask added"))).toBe(true);
    // Only the one GET: a new notify on each render did not reload the list.
    expect(calls.filter((c) => c.path === "/api/library?kind=interceptor")).toHaveLength(1);
  });
  it("knows code it generated when the library returns its settings in another order", async () => {
    // Published with blockTools then audit; the store hands them back audit first.
    const tpl = { blockTools: { tools: ["refunds"] }, audit: {} };
    const files = interceptorFiles("request", tpl, base().workflow);
    SHARED_ALT.definition = { point: "request", code: {}, templates: { audit: {}, blockTools: { tools: ["refunds"] } } };
    SHARED_ALT.files = files;
    listed.push(SHARED_ALT);
    try {
      await act(async () => { render(<Harness />); });
      await openPicker();
      await choose("generatedOne");
      await act(async () => { addButton().click(); });
      expect(latest.toolCode!["interceptor-request"]["handler.py"]).toBe(files["handler.py"]);
      expect(screen.queryByText(/was edited by hand/)).toBeNull();
    } finally { listed.pop(); }
  });
  it("adds one by ARN with no files", async () => {
    await act(async () => { render(<Harness />); });
    await openPicker();
    await choose("noRefunds");
    await act(async () => { addButton().click(); });
    expect(interceptorsOf(latest.workflow).request).toEqual({ lambdaArn: MINE.definition.lambdaArn });
    expect(latest.toolCode?.["interceptor-request"]).toBeUndefined();
  });
  it("asks before replacing the interceptor already at that point, and keeps it when told no", async () => {
    await act(async () => { render(<Harness />); });
    await act(async () => { toggles()[1].findNativeInput().click(); });
    const before = latest.toolCode!["interceptor-response"]["handler.py"];
    const ask = vi.spyOn(window, "confirm").mockReturnValue(false);
    await openPicker();
    await choose("piiMask");
    await act(async () => { addButton().click(); });
    expect(ask).toHaveBeenCalledWith(expect.stringContaining("already has a response interceptor"));
    expect(latest.toolCode!["interceptor-response"]["handler.py"]).toBe(before);
    ask.mockReturnValue(true);
    await act(async () => { addButton().click(); });
    expect(latest.toolCode!["interceptor-response"]["handler.py"]).toContain("return 'masked'");
  });
});

describe("Publish to library", () => {
  it("is there only for a point that has an interceptor", async () => {
    await act(async () => { render(<Harness />); });
    expect(screen.queryAllByRole("button", { name: "Publish to library" })).toHaveLength(0);
    await act(async () => { toggles()[0].findNativeInput().click(); });
    expect(screen.queryAllByRole("button", { name: "Publish to library" })).toHaveLength(1);
  });
  it("publishes the point, its settings and its code, then offers to share it", async () => {
    await act(async () => { render(<Harness />); });
    await act(async () => { toggles()[0].findNativeInput().click(); });
    await click("Publish to library");
    const modal = w().findAllModals().find((m) => m.getElement().textContent?.includes("Publish the request interceptor"))!;
    const publish = () => modal.findFooter()!.findAllButtons().find((b) => b.getElement().textContent === "Publish")!;
    const field = (label: string) => createWrapper(modal.getElement()).findAllInputs().find((i) => i.findNativeInput().getElement().getAttribute("aria-label") === label)!;
    await act(async () => { field("Library name").setInputValue("1bad"); });
    expect(publish().getElement().hasAttribute("disabled")).toBe(true);
    await act(async () => { field("Library name").setInputValue("auditAll"); });
    await act(async () => { field("Library description").setInputValue("Logs every call."); });
    await act(async () => { publish().click(); });
    const sent = calls.find((c) => c.method === "POST" && c.path === "/api/library")!.body as Record<string, unknown>;
    expect(sent).toMatchObject({ kind: "interceptor", name: "auditAll", description: "Logs every call.",
      definition: { point: "request", code: {}, templates: { audit: {} } } });
    expect((sent.files as Record<string, string>)["handler.py"]).toContain("def audit(");
    expect(notes.some(([t, m]) => t === "success" && m.includes("auditAll is in your library"))).toBe(true);
    expect(screen.getByRole("button", { name: "Share auditAll" })).toBeTruthy();
  });
  it("publishes one by ARN without files", async () => {
    const arn = "arn:aws:lambda:us-east-1:123456789012:function:mine";
    const start = { ...base(), workflow: { ...base().workflow, orchestrator: { interceptors: { response: { lambdaArn: arn } } } } } as Project;
    await act(async () => { render(<Harness start={start} />); });
    await click("Publish to library");
    const modal = w().findAllModals().find((m) => m.getElement().textContent?.includes("Publish the response interceptor"))!;
    await act(async () => { createWrapper(modal.getElement()).findAllInputs()[0].setInputValue("myGuard"); });
    await act(async () => { modal.findFooter()!.findAllButtons().find((b) => b.getElement().textContent === "Publish")!.click(); });
    const sent = calls.find((c) => c.method === "POST" && c.path === "/api/library")!.body as Record<string, unknown>;
    expect(sent.definition).toEqual({ point: "response", lambdaArn: arn });
    expect(sent).not.toHaveProperty("files");
  });
});
