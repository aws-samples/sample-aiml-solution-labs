/** examples/demo-everything: the page's validator finds no error in the demo build, and
 *  its bundle imports through Import a file. The server's check runs the same files in
 *  tests/test_demo_everything.py. */
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { fromFile, type Workflow } from "./model";
import { validate } from "./validate";

const DIR = resolve(process.cwd(), "../examples/demo-everything");
const read = (name: string) => readFileSync(resolve(DIR, name), "utf8");

describe("demo-everything", () => {
  for (const name of ["project.json", "project.byo-harness.json"]) {
    it(`${name} has no error`, () => {
      const wf = JSON.parse(read(name)).project.workflow as Workflow;
      expect(validate(wf).filter((i) => i.severity === "error")).toEqual([]);
    });
  }
  for (const name of ["bundle.json", "bundle.byo-harness.json"]) {
    it(`${name} imports with its prompts and code`, () => {
      const project = fromFile(read(name), "demo");
      expect(Object.keys(project.prompts)).toContain("drafter");
      expect(Object.keys(project.toolCode ?? {}).sort()).toEqual(
        ["claimsDb", "interceptor-request", "interceptor-response"]);
      expect(validate(project.workflow).filter((i) => i.severity === "error")).toEqual([]);
    });
  }
});
