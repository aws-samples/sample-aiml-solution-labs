/** Build storage rules (orchestrator.storage and the ${storage} token): the page's validator
 *  and bff/validate_build.py must give the same issues for tests/fixtures/storage_cases.json,
 *  on the base of validation_cases.json (Python side: tests/test_storage.py).
 *
 *  To regenerate `expect` after a deliberate rule change:
 *    AX_WRITE_STORAGE_CASES=1 npx vitest run storage.parity */
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import type { Workflow } from "./model";
import { validate, type Issue } from "./validate";
type Op = ["set", (string | number)[], unknown];
interface Case { name: string; ops: Op[]; expect: Issue[] }
const BASE: Workflow = JSON.parse(readFileSync(resolve(process.cwd(), "../tests/fixtures/validation_cases.json"), "utf8")).base;
const FILE = resolve(process.cwd(), "../tests/fixtures/storage_cases.json");
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
/** Only the issues about tools and the orchestrator block: the base's own are
 *  validation_cases.json's business. */
const storageIssues = (wf: Workflow) => validate(wf).filter((i) => i.path.startsWith("tools.") || i.path.startsWith("orchestrator."));
describe("build storage: validate.ts and bff/validate_build.py agree", () => {
  if (process.env.AX_WRITE_STORAGE_CASES) {
    it("writes the expected issues", () => {
      for (const c of FIXTURE.cases) c.expect = storageIssues(apply(c.ops));
      writeFileSync(FILE, `${JSON.stringify(FIXTURE, null, 1)}\n`);
    });
    return;
  }
  for (const c of FIXTURE.cases) {
    it(c.name, () => expect(storageIssues(apply(c.ops))).toEqual(c.expect));
  }
  it("accepts the token where it is read and refuses it everywhere else", () => {
    const errors = (n: string) => FIXTURE.cases.find((c) => c.name.startsWith(n))!.expect.filter((i) => i.severity === "error");
    expect(errors("storage created: a code tool")).toEqual([]);
    expect(errors("storage created and kept")).toEqual([]);
    for (const n of ["the token with no storage", "the token with storage create false", "the token is read only",
      "the token must start", "the token in place of the bucket", "storage takes create"]) {
      expect(errors(n).length).toBeGreaterThan(0);
    }
    const retain = FIXTURE.cases.find((c) => c.name.startsWith("retain without create"))!.expect;
    expect(retain.map((i) => [i.severity, i.path])).toEqual([["warning", "orchestrator.storage.retain"]]);
  });
});
