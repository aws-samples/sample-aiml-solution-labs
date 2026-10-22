/** Hosted MCP server rules (tools.<key>.hosted): the page's validator and bff/validate_build.py
 *  must give the same issues for tests/fixtures/hosted_mcp_cases.json, on the base of
 *  validation_cases.json (Python side: tests/test_hosted_mcp.py).
 *
 *  To regenerate `expect` after a deliberate rule change:
 *    AX_WRITE_HOSTED_MCP_CASES=1 npx vitest run hostedMcp.parity */
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import type { Workflow } from "./model";
import { validate, type Issue } from "./validate";
type Op = ["set", (string | number)[], unknown];
interface Case { name: string; ops: Op[]; expect: Issue[] }
const BASE: Workflow = JSON.parse(readFileSync(resolve(process.cwd(), "../tests/fixtures/validation_cases.json"), "utf8")).base;
const FILE = resolve(process.cwd(), "../tests/fixtures/hosted_mcp_cases.json");
const FIXTURE: { $comment: string; cases: Case[] } = JSON.parse(readFileSync(FILE, "utf8"));
function apply(ops: Op[]): Workflow {
  const wf = structuredClone(BASE) as unknown as Record<string, any>;
  for (const [, path, value] of ops) {
    let at = wf;
    for (const k of path.slice(0, -1)) at = at[k];
    at[path[path.length - 1]] = structuredClone(value);
  }
  return wf as unknown as Workflow;
}
/** Only the issues about tools: the base's own issues are validation_cases.json's business. */
const toolIssues = (wf: Workflow) => validate(wf).filter((i) => i.path.startsWith("tools."));

describe("hosted MCP: validate.ts and bff/validate_build.py agree", () => {
  if (process.env.AX_WRITE_HOSTED_MCP_CASES) {
    it("writes the expected issues", () => {
      for (const c of FIXTURE.cases) c.expect = toolIssues(apply(c.ops));
      writeFileSync(FILE, `${JSON.stringify(FIXTURE, null, 1)}\n`);
    });
    return;
  }
  for (const c of FIXTURE.cases) {
    it(c.name, () => expect(toolIssues(apply(c.ops))).toEqual(c.expect));
  }
  it("accepts the valid shapes and refuses the others", () => {
    const errors = (n: string) => FIXTURE.cases.find((c) => c.name.startsWith(n))!.expect.filter((i) => i.severity === "error");
    expect(errors("a hosted server with an image")).toEqual([]);
    expect(errors("a hosted server written in the build")).toEqual([]);
    for (const n of ["hosted and endpoint", "a hosted server takes", "a hosted server's image", "a hosted server's key", "hosted applies"]) {
      expect(errors(n).length).toBeGreaterThan(0);
    }
  });
});
