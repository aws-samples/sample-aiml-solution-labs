/** The interceptor code the Builder generates.
 *
 *  tests/fixtures/interceptor_cases.json holds the handler.py generated for a set of
 *  template choices; tests/test_interceptors.py RUNS each one on Gateway payloads, so
 *  what a user deploys is tested as Python, not only as text. This test fails when the
 *  generator's output drifts from the fixture. After a deliberate change:
 *    AX_WRITE_INTERCEPTOR_CASES=1 npx vitest run interceptorCode */
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { TEMPLATES, interceptorFiles, render, sampleEvents, type Point } from "./interceptorCode";
import type { Json, Workflow } from "./model";
import { INTERCEPTOR_SETTINGS } from "./validate";
import { vocab } from "./meta";

const FILE = resolve(process.cwd(), "../tests/fixtures/interceptor_cases.json");
const WF = {
  agents: { intake: { name: "Intake" } }, steps: [{ agent: "intake" }],
  tools: { refunds: { type: "lambda", toolSchema: [{ name: "issueRefund" }], code: {} } },
} as unknown as Workflow;

type T = Record<string, Record<string, Json>>;
const CASES: { name: string; point: Point; templates: T }[] = [
  { name: "request: nothing checked", point: "request", templates: {} },
  { name: "request: audit", point: "request", templates: { audit: {} } },
  { name: "request: block a tool", point: "request", templates: { blockTools: { tools: ["refunds___issueRefund"] } } },
  { name: "request: block for one agent", point: "request",
    templates: { blockTools: { tools: ["refunds"], agents: ["intake"] } } },
  { name: "request: argument guard", point: "request",
    templates: { argumentGuard: { denyPatterns: ["(?i)drop\\s+table"], maxArgumentChars: 50 } } },
  { name: "request: inject context", point: "request", templates: { injectContext: { arguments: { tenant: "user", runId: "session" } } } },
  { name: "request: everything", point: "request", templates: {
    audit: {}, blockTools: { tools: ["refunds___issueRefund"] }, argumentGuard: { denyPatterns: ["secret"] },
    injectContext: { arguments: { runId: "session" } }, custom: {} } },
  { name: "response: nothing checked", point: "response", templates: {} },
  { name: "response: redact", point: "response", templates: { redactPii: { types: ["email", "card"], mask: "***" } } },
  { name: "response: hide a tool", point: "response", templates: { hideTools: { tools: ["refunds"] } } },
  { name: "response: cap", point: "response", templates: { capResult: { maxChars: 100 } } },
  { name: "response: everything", point: "response", templates: {
    audit: {}, redactPii: {}, hideTools: { tools: ["refunds___issueRefund"] }, capResult: { maxChars: 100 }, custom: {} } },
];

describe("the generated interceptor code", () => {
  if (process.env.AX_WRITE_INTERCEPTOR_CASES) {
    it("writes the cases", () => {
      const cases = CASES.map((c) => ({ ...c, handler: interceptorFiles(c.point, c.templates, WF)["handler.py"] }));
      writeFileSync(FILE, `${JSON.stringify({ $comment: "Written by web/src/builder/interceptorCode.test.ts; run by tests/test_interceptors.py.", cases }, null, 1)}\n`);
    });
    return;
  }
  const fixture = JSON.parse(readFileSync(FILE, "utf8")) as { cases: { name: string; handler: string }[] };
  for (const c of CASES) {
    it(`matches the fixture: ${c.name}`, () => {
      expect(interceptorFiles(c.point, c.templates, WF)["handler.py"]).toBe(fixture.cases.find((f) => f.name === c.name)?.handler);
    });
  }
  it("generates the same code whatever order the templates and settings come in", () => {
    // A library item read back from the store does not keep key order.
    const a = interceptorFiles("request", { audit: {}, blockTools: { tools: ["x"], agents: ["intake"] } }, WF)["handler.py"];
    const b = interceptorFiles("request", { blockTools: { agents: ["intake"], tools: ["x"] }, audit: {} }, WF)["handler.py"];
    expect(b).toBe(a);
  });
  it("keeps only the checked sections, and no markers", () => {
    const out = interceptorFiles("request", { audit: {} }, WF)["handler.py"];
    expect(out).toContain("def audit(");
    expect(out).not.toContain("def block_tools(");
    expect(out).not.toContain("import re");
    expect(out).not.toMatch(/# (>>>|<<<) /);
    expect(out).not.toMatch(/\n\n\n\n/);
  });
  it("writes the settings of what is checked", () => {
    const out = render("# >>> settings\nX = 1\n# <<< settings\n", [], { blockTools: { tools: ["a\"\"\"b"] } });
    expect(out).toContain('SETTINGS = json.loads(r"""');
    expect(out).not.toContain("X = 1");
    // A JSON string escapes its quotes, so a value cannot close the raw string early.
    expect(out.split('"""').length).toBe(3);
  });
  it("has sample events for the sandbox at each point", () => {
    const req = sampleEvents("request", WF, { blockTools: { tools: ["refunds___issueRefund"] } }) as { event: { mcp: Record<string, unknown> } }[];
    expect(req).toHaveLength(2);
    expect(JSON.stringify(req[1])).toContain("refunds___issueRefund");
    const res = sampleEvents("response", WF, {}) as { event: { mcp: Record<string, unknown> } }[];
    expect(res.every((e) => "gatewayResponse" in e.event.mcp)).toBe(true);
  });
  it("offers exactly the templates the validators know, with settings they accept", () => {
    expect(TEMPLATES.request.map((t) => t.id)).toEqual(vocab("interceptorRequestTemplates"));
    expect(TEMPLATES.response.map((t) => t.id)).toEqual(vocab("interceptorResponseTemplates"));
    for (const t of [...TEMPLATES.request, ...TEMPLATES.response]) {
      for (const k of Object.keys(t.defaults)) expect(INTERCEPTOR_SETTINGS[t.id]).toContain(k);
    }
  });
});
