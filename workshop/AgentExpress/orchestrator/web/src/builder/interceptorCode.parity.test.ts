/** The page and the server generate an interceptor's files alike, byte for byte.
 *
 *  The Assistant sets an interceptor's templates on the server (bff/interceptor_code.py,
 *  a port of interceptorFiles). The Interceptors tab takes handler.py for edited by hand
 *  when it is not what the templates generate, so the two must agree exactly. Both run
 *  the cases in tests/fixtures/interceptor_files_cases.json and must produce `expect`
 *  (handler.py alone, for many more template sets, is in interceptor_cases.json).
 *
 *  To regenerate `expect` after a deliberate template change:
 *    AX_WRITE_INTERCEPTOR_CASES=1 npx vitest run interceptorCode.parity */
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { interceptorFiles, type Point } from "./interceptorCode";
import type { Json, ToolFiles, Workflow } from "./model";

interface Case { name: string; point: Point; templates: Record<string, Record<string, Json>>; workflow: Workflow; expect?: ToolFiles }
interface Fixture { $comment: string; cases: Case[] }
const FILE = resolve(process.cwd(), "../tests/fixtures/interceptor_files_cases.json");
const FIXTURE: Fixture = JSON.parse(readFileSync(FILE, "utf8"));

describe("interceptor files: page and server agree", () => {
  if (process.env.AX_WRITE_INTERCEPTOR_CASES) {
    it("writes the expected files", () => {
      for (const c of FIXTURE.cases) c.expect = interceptorFiles(c.point, c.templates, c.workflow);
      writeFileSync(FILE, `${JSON.stringify(FIXTURE, null, 2)}\n`);
    });
    return;
  }
  for (const c of FIXTURE.cases) {
    it(c.name, () => {
      expect(interceptorFiles(c.point, c.templates, c.workflow)).toEqual(c.expect);
    });
  }
});
