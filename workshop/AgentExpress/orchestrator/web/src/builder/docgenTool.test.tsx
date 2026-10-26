/** Track B docgen. The Document generator in "Add a tool": it writes the framework's own
 *  type="lambda" tool (`source: "docgen"`), the form shows its formats, template and output
 *  prefix and nothing of the function, its checks match the server's, and its test run calls
 *  the deployed function with sample data and shows the file's link. */
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { Builder } from "./Builder";
import { addBuiltinTool, builtinLabel, builtinOverrides } from "./browserTool";
import { DocgenTest } from "./DocgenTest";
import { checkDocgenTool, DOCGEN_TOOL, OUTPUT_RE, sampleEvent, TEMPLATE_RE } from "./docgenTool";
import { newProject, type Entry } from "./model";
import { codeTool } from "./storage";
import { validate, type Issue } from "./validate";

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
  class RO { observe() {} unobserve() {} disconnect() {} }
  vi.stubGlobal("ResizeObserver", RO);
  vi.stubGlobal("DOMMatrixReadOnly", class { m22 = 1; constructor() {} });
});
beforeEach(() => { vi.stubGlobal("localStorage", memoryStorage()); });
afterEach(() => { vi.restoreAllMocks(); });

const toolIssues = (wf: ReturnType<typeof newProject>["workflow"], key: string) =>
  validate(wf).filter((i) => i.severity === "error" && i.path.startsWith(`tools.${key}`)).map((i) => `${i.path}: ${i.message}`);

describe("the Document generator choice", () => {
  it("writes a docgen tool with all three formats, which checks clean once the build keeps run files", () => {
    const { project, key } = addBuiltinTool(newProject("x"), "Reports", "docgen");
    const t = project.workflow.tools[key];
    expect(t).toMatchObject({ type: "lambda", source: "docgen", call: DOCGEN_TOOL, arg: "data", formats: ["pdf", "pptx", "docx"],
      description: "Reports" });
    expect((t.toolSchema as { name: string }[])[0].name).toBe(DOCGEN_TOOL);
    expect(Object.keys((t.toolSchema as { properties: Record<string, unknown> }[])[0].properties))
      .toEqual(["format", "data", "template", "fileName", "runId", "agent"]);
    expect(t.lambdaArn).toBeUndefined();
    expect(builtinLabel(t)).toBe("Document generator");
    expect(toolIssues(project.workflow, key)).toEqual([`tools.${key}.source: the Document generator saves each file with its run: `
      + "set orchestrator.files (even {}) so this deployment keeps run files"]);
    const wf = { ...project.workflow, orchestrator: { ...((project.workflow.orchestrator ?? {}) as Record<string, unknown>), files: {} } };
    expect(toolIssues(wf, key)).toEqual([]);
  });

  it("shows its own settings on the form and hides the function's", () => {
    const hidden = Object.keys(builtinOverrides({ type: "lambda", source: "docgen" } as Entry));
    expect(hidden).toEqual(expect.arrayContaining(["lambdaArn", "source", "code", "toolSchema", "call", "arg", "allowDomains"]));
    for (const k of ["formats", "templateS3Uri", "outputPrefix"]) expect(hidden).not.toContain(k);
  });
});

describe("the Document generator's checks", () => {
  const run = (tool: Record<string, unknown>, filesOn = true) => {
    const out: Issue[] = [];
    checkDocgenTool(tool, "tools.docs", { kind: "tool", id: "docs" }, (severity, where, path, message) =>
      out.push({ severity, where, path, message } as Issue), filesOn);
    return out.map((i) => `${i.path}: ${i.message}`);
  };
  const base = { type: "lambda", source: "docgen", formats: ["pdf", "docx"] };
  it("takes one template or a folder, and an output folder, in S3 or the build's storage", () => {
    for (const ok of ["s3://my-templates/t.docx", "s3://my-templates/a/b/", "s3://my-templates/", "${storage}/templates/report.pptx"]) {
      expect(TEMPLATE_RE.test(ok)).toBe(true);
    }
    for (const bad of ["s3://my-templates/t.pdf", "s3://my-templates/t*/", "https://x/t.docx", "s3://B/t.docx", "s3://my-templates"]) {
      expect(TEMPLATE_RE.test(bad)).toBe(false);
    }
    for (const ok of ["s3://my-out/", "s3://my-out/a/b/", "${storage}/out/"]) expect(OUTPUT_RE.test(ok)).toBe(true);
    for (const bad of ["s3://my-out/a", "s3://my-out", "s3://my-out/a?/"]) expect(OUTPUT_RE.test(bad)).toBe(false);
  });
  it("refuses its keys on another tool, and needs run files and formats on it", () => {
    expect(run({ type: "lambda", source: "browser", formats: ["pdf"], outputPrefix: "s3://b-1/" })).toEqual([
      'tools.docs.formats: only the Document generator (source "docgen") reads formats',
      'tools.docs.outputPrefix: only the Document generator (source "docgen") reads outputPrefix']);
    expect(run({ type: "lambda", source: "docgen" }, false)).toEqual([
      "tools.docs.formats: the Document generator needs `formats`: any of pdf, pptx, docx",
      "tools.docs.source: the Document generator saves each file with its run: set orchestrator.files (even {}) so this deployment keeps run files"]);
  });
  it("matches a single template to the formats and refuses the framework's buckets", () => {
    expect(run({ ...base, templateS3Uri: "s3://b-1/deck.pptx" })).toEqual(['tools.docs.templateS3Uri: is a .pptx template, so `formats` must include "pptx"']);
    expect(run({ ...base, outputPrefix: "s3://agentcore-x-assets-1/" })).toEqual(["tools.docs.outputPrefix: is one of the framework's own buckets, which no tool may reach"]);
    expect(run({ ...base, formats: ["pdf", "pdf"] })).toEqual(['tools.docs.formats[1]: "pdf" is listed twice']);
  });
});

describe("the test run with sample data", () => {
  it("is an event for one of the tool's formats, a run of its own and sample data", () => {
    const e = sampleEvent({ type: "lambda", source: "docgen", formats: ["docx", "pdf"] } as Entry);
    expect(e).toMatchObject({ format: "docx", runId: "console-test", fileName: "sample" });
    expect(Object.keys(e.data as object)).toEqual(["title", "subtitle", "summary", "rows"]);
  });
  it("calls the deployed function with it and links to the file", async () => {
    const answer = { fileName: "sample.pdf", s3Uri: "s3://b/runs/console-test/outputs/sample.pdf", url: "https://b.s3.amazonaws.com/x?sig" };
    const test = vi.spyOn(codeTool, "test").mockResolvedValue({ ok: true, status: 200, error: "", output: JSON.stringify(answer), log: "", ms: 9 });
    const tool = { type: "lambda", source: "docgen", formats: ["pdf"] } as Entry;
    await act(async () => { render(<DocgenTest buildId="b1" id="docs" tool={tool} />); });
    const w = createWrapper(document.body);
    await act(async () => { w.findExpandableSection()!.findExpandButton().click(); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Run with sample data" })); });
    expect(test).toHaveBeenCalledWith("b1", "docs", DOCGEN_TOOL, sampleEvent(tool));
    const link = screen.getByRole("link", { name: /Download sample.pdf/ });
    expect(link.getAttribute("href")).toBe(answer.url);
  });
});

describe("Add a tool, in the Build view", () => {
  it("adds a Document generator and opens it with its formats, template and output prefix", async () => {
    await act(async () => { render(<Builder notify={vi.fn()} />); });
    const w = createWrapper(document.body);
    await act(async () => { screen.getAllByRole("button", { name: "Add a tool" })[0].click(); });
    const modal = w.findModal()!;
    await act(async () => { modal.findContent().findInput()!.setInputValue("Reports"); });
    const select = modal.findContent().findSelect()!;
    await act(async () => { select.openDropdown(); });
    await act(async () => { select.selectOptionByValue("docgen"); });
    await act(async () => { modal.findFooter()!.findAllButtons().find((b) => b.getElement().textContent === "Add")!.click(); });
    expect(screen.getByText(/^Document generator tool · used by/)).toBeTruthy();
    const fieldLabels = w.findAllFormFields().map((f) => f.findLabel()?.getElement().textContent ?? "");
    expect(fieldLabels).toEqual(expect.arrayContaining(["Formats", "Template s3 uri", "Output prefix"]));
    expect(fieldLabels).not.toContain("Tool schema");
    expect(fieldLabels).not.toContain("Lambda arn");
    expect(fieldLabels).not.toContain("Allow domains");
  });
});
