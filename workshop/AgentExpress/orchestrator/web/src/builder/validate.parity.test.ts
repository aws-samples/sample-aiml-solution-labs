/** The page's validator and the server's must agree, issue for issue.
 *
 *  The BFF refuses to deploy a build with an error, using bff/validate_build.py — a
 *  port of validate.ts. Both run the cases in tests/fixtures/validation_cases.json and
 *  must produce exactly `expect`: the same severity, place, path and message. A rule
 *  changed on one side fails the other side's test until the port catches up.
 *
 *  To regenerate `expect` after a deliberate rule change:
 *    AX_WRITE_VALIDATION_CASES=1 npx vitest run validate.parity */

import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

import type { Workflow } from "./model";
import { validate, type Issue } from "./validate";

type Path = (string | number)[];
type Op = ["set", Path, unknown] | ["del", Path];
interface Case { name: string; ops: Op[]; expect: Issue[] }
interface Fixture { $comment: string; base: Workflow; cases: Case[] }

const FILE = resolve(process.cwd(), "../tests/fixtures/validation_cases.json");
const FIXTURE: Fixture = JSON.parse(readFileSync(FILE, "utf8"));

/** Apply a case's edits to a copy of the base. Mirrored in tests/test_validate_build.py. */
function apply(base: Workflow, ops: Op[]): Workflow {
  const wf = structuredClone(base) as unknown as Record<string | number, unknown>;
  for (const op of ops) {
    const path = op[1];
    let at = wf as Record<string | number, unknown>;
    for (const k of path.slice(0, -1)) at = at[k] as Record<string | number, unknown>;
    const last = path[path.length - 1];
    if (op[0] === "set") at[last] = structuredClone(op[2]);
    else if (Array.isArray(at)) at.splice(last as number, 1);
    else delete at[last];
  }
  return wf as unknown as Workflow;
}

describe("validate.ts and bff/validate_build.py agree", () => {
  if (process.env.AX_WRITE_VALIDATION_CASES) {
    it("writes the expected issues", () => {
      for (const c of FIXTURE.cases) c.expect = validate(apply(FIXTURE.base, c.ops));
      writeFileSync(FILE, `${JSON.stringify(FIXTURE, null, 1)}\n`);
    });
    return;
  }
  for (const c of FIXTURE.cases) {
    it(c.name, () => {
      expect(validate(apply(FIXTURE.base, c.ops))).toEqual(c.expect);
    });
  }
  it("covers errors and warnings", () => {
    const all = FIXTURE.cases.flatMap((c) => c.expect);
    expect(all.some((i) => i.severity === "error")).toBe(true);
    expect(all.some((i) => i.severity === "warning")).toBe(true);
  });
});
