/** A build's test suites in the project model: exported, imported and kept with the build.
 *
 *  The suites are the user's tests; losing them on an export, or refusing a bundle because it
 *  has them, would make the feature something a build cannot take with it. */
import { describe, expect, it } from "vitest";
import { fromFile, newProject, replaceWorkflow, toBundle, type Project } from "./model";
import {
  blankCheck, describeCheck, kindOf, newCase, newSuite, operatorOf, passRate, runBlockReason, type Check, type Suite,
} from "./suiteSpec";

const SUITES: Suite[] = [{
  name: "Triage",
  cases: [{
    name: "urgent", request: "Claim 1", subject: "acme",
    expect: [{ run: "done" }, { agent: "triage", field: "riskScore", gte: 80 }, { cost: { lt: 0.5 } }],
  }],
}];

function withSuites(): Project {
  return { ...newProject("Claims"), suites: SUITES };
}

describe("suites in the bundle", () => {
  it("are exported with the build and read back intact", () => {
    const bundle = toBundle(withSuites());
    expect(bundle.suites).toEqual(SUITES);
    const again = fromFile(JSON.stringify(bundle), "x");
    expect(again.suites).toEqual(SUITES);
    expect(again.name).toBe("Claims");
  });

  it("are left out of a build that has none, so its bundle is what it was", () => {
    expect("suites" in toBundle(newProject("Claims"))).toBe(false);
    expect("suites" in toBundle({ ...newProject("Claims"), suites: [] })).toBe(false);
  });

  it("are absent from a bundle made before there were suites, which still imports", () => {
    const old = JSON.stringify({ ...toBundle(newProject("Claims")), suites: undefined });
    expect(JSON.parse(old).suites).toBeUndefined();
    const project = fromFile(old, "x");
    expect(project.suites).toBeUndefined();
    expect(project.workflow.agents).toBeDefined();
  });

  it("are not invented for a plain workflow.json", () => {
    const plain = JSON.stringify(newProject("Claims").workflow);
    expect(fromFile(plain, "x").suites).toBeUndefined();
  });

  it("read the shape the server freezes into a version (bff/buildstore.py bundle_of)", () => {
    const frozen = {
      format: "agentexpress-bundle", version: 1, framework: { version: "1.3.0" },
      project: { id: "pclaims01", name: "Claims", buildVersion: 2 },
      workflow: newProject("Claims").workflow, prompts: {}, suites: SUITES,
    };
    expect(fromFile(JSON.stringify(frozen), "x").suites).toEqual(SUITES);
  });

  it("stay with the build when a workflow file is put into it, unless the file brings its own", () => {
    const base = withSuites();
    const plain = JSON.stringify(newProject("Other").workflow);
    expect(replaceWorkflow(base, plain).suites).toEqual(SUITES);
    const other: Suite[] = [{ name: "Smoke", cases: [] }];
    const bundle = JSON.stringify(toBundle({ ...newProject("Other"), suites: other }));
    expect(replaceWorkflow(base, bundle).suites).toEqual(other);
  });
});

describe("checks", () => {
  it("make each kind valid as it is made", () => {
    const kinds = ["run", "agentStatus", "agentField", "agentText", "cost", "latencyMs"] as const;
    for (const kind of kinds) expect(kindOf(blankCheck(kind, "triage"))).toBe(kind);
    expect(blankCheck("run")).toEqual({ run: "done" });
    // Numbers, not text: the server refuses a limit that is a string.
    expect(blankCheck("cost")).toEqual({ cost: { lt: 1 } });
    expect(blankCheck("latencyMs")).toEqual({ latencyMs: { lt: 60000 } });
    expect(blankCheck("agentStatus", "triage")).toEqual({ agent: "triage", status: "done" });
    expect(blankCheck("agentField", "triage")).toMatchObject({ agent: "triage", field: expect.any(String) });
  });

  it("are described in words", () => {
    const say = (c: Check) => describeCheck(c);
    expect(say({ run: "failed" })).toBe("the run ends failed");
    expect(say({ agent: "triage", status: "skipped" })).toBe("agent triage ends skipped");
    expect(say({ agent: "triage", field: "riskScore", gte: 80 })).toBe("triage › riskScore is at least 80");
    expect(say({ agent: "triage", field: "tags", in: ["a", "b"] })).toBe("triage › tags is one of a, b");
    expect(say({ agent: "summary", notContains: "TODO" })).toBe('summary output does not contain "TODO"');
    expect(say({ agent: "triage", field: "extra", exists: true })).toBe("triage › extra is present");
    expect(say({ cost: { lt: 0.5 } })).toBe("the run costs less than $0.5");
    expect(say({ latencyMs: { lte: 9000 } })).toBe("the calls take at most 9000 ms");
    expect(operatorOf({ agent: "a", field: "f", lt: 3 })).toBe("lt");
  });

  it("start new suites and cases under a name nothing else has", () => {
    expect(newSuite([]).name).toBe("New suite");
    expect(newSuite(["New suite", "New suite 2"]).name).toBe("New suite 3");
    expect(newCase(["New case"]).name).toBe("New case 2");
    expect(newCase([])).toEqual({ name: "New case", request: "", expect: [] });
  });

  it("give a pass rate only for a finished run that started something", () => {
    const base = { suiteRunId: "x", startedAt: "", buildVersion: 1 };
    expect(passRate({ ...base, finished: true, passed: 3, total: 4 })).toBe(0.75);
    expect(passRate({ ...base, finished: false })).toBeNull();
    expect(passRate({ ...base, finished: true, passed: 0, total: 0 })).toBeNull();
  });

  it("say why a suite cannot be run here, in order of what the person can change least", () => {
    expect(runBlockReason({ runsHere: true, canStart: true })).toBeUndefined();
    expect(runBlockReason({ runsHere: false, canStart: true })).toMatch(/holds no runs/);
    expect(runBlockReason({ runsHere: true, canStart: false })).toMatch(/not allowed to start runs/);
    expect(runBlockReason({ runsHere: true, canStart: true, account: "123456789012" })).toMatch(/own AWS account/);
    expect(runBlockReason({ runsHere: false, canStart: false })).toMatch(/holds no runs/);
  });
});
