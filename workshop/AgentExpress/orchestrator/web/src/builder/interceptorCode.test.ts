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
import { TEMPLATES, hasProtocolGuard, interceptorFiles, interceptorIssues, isGenerated, render, sampleEvents, type Point } from "./interceptorCode";
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
  it("always keeps the response protocol guard, whatever is checked", () => {
    // tests/test_interceptor_protocol.py runs it: only a tool's result may change, never
    // initialize's protocolVersion, capabilities or serverInfo, jsonrpc or id.
    for (const t of [{}, { redactPii: {} }, { custom: {} }, { hideTools: { tools: ["x"] }, capResult: {} }] as T[]) {
      const out = interceptorFiles("response", t, WF)["handler.py"];
      expect(out).toContain("def _keep_protocol(");
      expect(out).toContain("body = _keep_protocol(sent, body, asked)");
      expect(out).toContain("import copy\n");
    }
    expect(interceptorFiles("request", { audit: {} }, WF)["handler.py"]).not.toContain("_keep_protocol");
  });
  it("takes code generated before the guard for generated code, not code edited by hand", () => {
    const legacy = JSON.parse(readFileSync(resolve(process.cwd(), "../tests/fixtures/interceptor_legacy_cases.json"), "utf8")) as
      { cases: { name: string; point: Point; templates: T; handler: string }[] };
    expect(legacy.cases.length).toBeGreaterThan(0);
    for (const c of legacy.cases) {
      expect(c.handler).not.toBe(interceptorFiles(c.point, c.templates, WF)["handler.py"]);
      expect(isGenerated(c.point, c.templates, c.handler)).toBe(true);
      expect(isGenerated(c.point, c.templates, `${c.handler}# mine\n`)).toBe(false);
    }
    const now = interceptorFiles("response", { capResult: {} }, WF)["handler.py"];
    expect(isGenerated("response", { capResult: {} }, now)).toBe(true);
  });
  it("warns about a stored response handler with no protocol guard, and only that one", () => {
    const withResponse = (templates: T) => ({ ...WF, orchestrator: { interceptors: { response: { code: {}, templates } } } }) as unknown as Workflow;
    const code = (handler: string) => ({ "interceptor-response": { "handler.py": handler, "requirements.txt": "" } });
    // Written whole, before the guard, as an Assistant once did: library catalog (mask ISBNs
    // everywhere) and a tide gauge log (mask every number in a record).
    const catalog = "import re\n\ndef lambda_handler(event, context):\n"
      + "    body = event['mcp']['gatewayResponse']['body']\n"
      + "    text = re.sub(r'\\d[\\d -]{6,}\\d', '[ISBN]', __import__('json').dumps(body))\n"
      + "    return {'interceptorOutputVersion': '1.0', 'mcp': {'transformedGatewayResponse': {'body': __import__('json').loads(text)}}}\n";
    const legacy = JSON.parse(readFileSync(resolve(process.cwd(), "../tests/fixtures/interceptor_legacy_cases.json"), "utf8")) as
      { cases: { point: Point; templates: T; handler: string }[] };
    const oldCustom = legacy.cases.find((c) => c.point === "response" && "custom" in c.templates);
    expect(oldCustom).toBeTruthy();
    const tideLog = oldCustom!.handler.replace(/def custom\(body: dict, request: dict\) -> dict:\n(?: {4}.*\n)+/,
      "def custom(body: dict, request: dict) -> dict:\n    return json.loads(re.sub(r'\\d+', '#', json.dumps(body)))\n");
    expect(tideLog).not.toBe(oldCustom!.handler);
    for (const [templates, handler] of [[{}, catalog], [oldCustom!.templates, tideLog]] as [T, string][]) {
      const issues = interceptorIssues(withResponse(templates), code(handler));
      expect(issues).toHaveLength(1);
      expect(issues[0]).toMatchObject({ severity: "warning", path: "orchestrator.interceptors.response.code" });
      expect(issues[0].message).toContain("Generate code");
    }
    // Nothing for: generated code now; generated code from before the guard; the guarded
    // handler with custom() filled in; a request interceptor; one deployed by ARN.
    const now = interceptorFiles("response", { custom: {} }, WF)["handler.py"];
    const filled = now.replace("    return body\n", "    return json.loads(re.sub(r'\\d+', '#', json.dumps(body)))\n");
    expect(hasProtocolGuard(now) && hasProtocolGuard(filled) && filled !== now).toBe(true);
    for (const [templates, handler] of [[{ custom: {} }, now], [oldCustom!.templates, oldCustom!.handler], [{ custom: {} }, filled]] as [T, string][]) {
      expect(interceptorIssues(withResponse(templates), code(handler))).toEqual([]);
    }
    const byArn = { ...WF, orchestrator: { interceptors: { response: { lambdaArn: "arn:aws:lambda:us-east-1:123456789012:function:f" } } } } as unknown as Workflow;
    expect(interceptorIssues(byArn, code(catalog))).toEqual([]);
    expect(interceptorIssues(WF, { "interceptor-request": { "handler.py": catalog } })).toEqual([]);
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
