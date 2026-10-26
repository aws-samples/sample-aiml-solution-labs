/** Code Interpreter and Browser in "Add a tool": each writes the framework's own type="lambda"
 *  tool (`source: "codeInterpreter"` / `"browser"`), the form shows only what a person
 *  chooses, and the Browser's allowDomains is checked like the server checks it. */
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { act, render, screen } from "@testing-library/react";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { Builder } from "./Builder";
import { addBuiltinTool, BUILTIN_CHOICES, builtinLabel, builtinOverrides, checkBrowserTool, DOMAIN_RE } from "./browserTool";
import { newProject, type Entry } from "./model";
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

const toolErrors = (wf: ReturnType<typeof newProject>["workflow"], key: string) =>
  validate(wf).filter((i) => i.severity === "error" && i.path.startsWith(`tools.${key}`));

describe("the Code Interpreter and Browser choices", () => {
  it("are offered by name after the tool types", () => {
    expect(BUILTIN_CHOICES.map((c) => [c.value, c.label])).toEqual([["codeInterpreter", "Code Interpreter"], ["browser", "Browser"],
      ["docgen", "Document generator"]]);
  });

  it("write exactly the documented Code Interpreter tool, which checks clean", () => {
    const { project, key } = addBuiltinTool(newProject("x"), "Sandbox", "codeInterpreter");
    const t = project.workflow.tools[key];
    expect(t).toEqual({
      type: "lambda", source: "codeInterpreter", call: "execute_code", arg: "code", description: "Sandbox",
      toolSchema: [{ name: "execute_code", description: "Run code in a sandbox. Returns stdout, stderr and the exit status.",
        properties: {
          language: { type: "string", required: true, description: "python, javascript or typescript." },
          code: { type: "string", required: true, description: "The program to run." },
        } }],
    });
    expect(toolErrors(project.workflow, key)).toEqual([]);
    expect(builtinLabel(t)).toBe("Code Interpreter");
  });

  it("write a Browser tool with no ARN, code or allow-list, which checks clean", () => {
    const { project, key } = addBuiltinTool(newProject("x"), "Web pages", "browser");
    const t = project.workflow.tools[key];
    expect(t).toMatchObject({ type: "lambda", source: "browser", call: "open_page", arg: "url", description: "Web pages" });
    expect(t.lambdaArn).toBeUndefined();
    expect(t.code).toBeUndefined();
    expect(t.allowDomains).toBeUndefined();
    expect(toolErrors(project.workflow, key)).toEqual([]);
    expect(builtinLabel(t)).toBe("Browser");
  });

  it("hide the function's fields on the form, and allowDomains everywhere but the Browser", () => {
    const hidden = (t: Entry) => Object.keys(builtinOverrides(t)).sort();
    const lambdaOnly = ["arg", "args", "call", "code", "lambdaArn", "rowFields", "rowPath", "source", "toolSchema"];
    // Track B docgen: the Document generator's settings are hidden on every other tool too.
    const docgen = ["formats", "outputPrefix", "templateS3Uri"];
    expect(hidden({ type: "lambda", source: "browser" })).toEqual([...docgen, ...lambdaOnly].sort());
    expect(hidden({ type: "lambda", source: "codeInterpreter" })).toEqual(["allowDomains", ...docgen, ...lambdaOnly].sort());
    expect(hidden({ type: "lambda", lambdaArn: "arn:aws:lambda:us-east-1:1:function:f" })).toEqual(["allowDomains", ...docgen].sort());
    expect(hidden({ type: "mcp" })).toEqual(["allowDomains", ...docgen].sort());
    expect(builtinLabel({ type: "lambda", source: "pricing" })).toBeUndefined();
  });
});

describe("the Browser's allowDomains", () => {
  const run = (tool: Record<string, unknown>) => {
    const out: Issue[] = [];
    checkBrowserTool(tool, "tools.web", { kind: "tool", id: "web" }, (severity, where, path, message) =>
      out.push({ severity, where, path, message } as Issue));
    return out.map((i) => `${i.path}: ${i.message}`);
  };
  it("takes domain names and nothing that only looks like one", () => {
    for (const ok of ["docs.aws.amazon.com", "example.org", "a-b.co.uk", "xn--bcher-kva.example"]) expect(DOMAIN_RE.test(ok)).toBe(true);
    for (const bad of ["https://example.com", "*.example.com", "example.com/x", "example.com:443", "Example.com", "localhost",
      "1.2.3.4", "-a.com", "a..com", ""]) expect(DOMAIN_RE.test(bad)).toBe(false);
  });
  it("is refused on any tool but the Browser, and capped at 50", () => {
    expect(run({ type: "lambda", source: "codeInterpreter", allowDomains: ["example.com"] }))
      .toEqual(['tools.web.allowDomains: only the Browser tool (source "browser") reads allowDomains']);
    const many = Array.from({ length: 51 }, (_, i) => `d${i}.example.com`);
    expect(run({ type: "lambda", source: "browser", allowDomains: many })).toEqual(["tools.web.allowDomains: at most 50 domains"]);
    expect(run({ type: "lambda", source: "browser", allowDomains: many.slice(0, 50) })).toEqual([]);
  });
  it("names the Browser's one tool when the schema names another", () => {
    expect(run({ type: "lambda", source: "browser", toolSchema: [{ name: "open_page" }, { name: "click" }] }))
      .toEqual(['tools.web.toolSchema[1].name: the Browser function publishes one tool, "open_page"']);
  });
});

describe("Add a tool, in the Build view", () => {
  it("adds a Browser tool and opens it with only the fields a person chooses", async () => {
    await act(async () => { render(<Builder notify={vi.fn()} />); });
    const w = createWrapper(document.body);
    await act(async () => { screen.getAllByRole("button", { name: "Add a tool" })[0].click(); });
    const modal = w.findModal()!;
    await act(async () => { modal.findContent().findInput()!.setInputValue("Web pages"); });
    const select = modal.findContent().findSelect()!;
    await act(async () => { select.openDropdown(); });
    const labels = select.findDropdown().findOptions().map((o) => o.getElement().textContent ?? "");
    expect(labels.some((l) => l.startsWith("Code Interpreter"))).toBe(true);
    await act(async () => { select.selectOptionByValue("browser"); });
    await act(async () => { modal.findFooter()!.findAllButtons().find((b) => b.getElement().textContent === "Add")!.click(); });
    expect(screen.getByText(/^Browser tool · used by/)).toBeTruthy();
    const fieldLabels = w.findAllFormFields().map((f) => f.findLabel()?.getElement().textContent ?? "");
    expect(fieldLabels).toContain("Allow domains");
    expect(fieldLabels).not.toContain("Tool schema");
    expect(fieldLabels).not.toContain("The function");
    expect(fieldLabels).not.toContain("Lambda arn");
  });
});
