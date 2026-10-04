/** The Builder's edits, and the validator that judges them.
 *
 *  The canvas is drag-and-drop, so the thing worth proving is that no sequence of drops
 *  can produce a `steps` the orchestrator refuses — and that when the user types
 *  something the deploy WOULD refuse, the Builder says so first, in the deploy's words.
 *  The shipped workflow.json is the fixture for "valid": if the validator flagged the
 *  sample the framework itself deploys, it would be wrong, not the sample. */

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

import {
  addAgent, addTool, bindTool, fromFile, insertStage, joinStage, moveStage, newProject,
  removeAgent, removeTool, renameAgent, renameTool, setStageKind, setTools, stepAgents, toBundle,
  unbindTool, unplace,
  type Project, type Workflow,
} from "./model";
import { validate } from "./validate";

const SHIPPED: Workflow = JSON.parse(readFileSync(resolve(process.cwd(), "../app/workflow.json"), "utf8"));

const errors = (wf: Workflow) => validate(wf).filter((i) => i.severity === "error");
const shape = (wf: Workflow) => wf.steps.map((s) => stepAgents(s).join("+"));

function three(): Project {
  let p = newProject("Claims");
  for (const name of ["Triage", "Fraud Check"]) {
    const r = addAgent(p, name);
    p = { ...r.project, workflow: insertStage(r.project.workflow, r.project.workflow.steps.length, r.id) };
  }
  return p;
}

describe("validate", () => {
  it("accepts the shipped sample with no errors", () => {
    expect(errors(SHIPPED)).toEqual([]);
  });

  it("accepts a new project as-is", () => {
    expect(errors(newProject("Claims").workflow)).toEqual([]);
  });

  it("rejects a key outside its runtime, in the framework's own words", () => {
    const wf = structuredClone(SHIPPED);
    wf.agents.analysis.maxTokens = 100;                    // analysis is runtime a2a
    const e = errors(wf);
    expect(e.map((x) => x.path)).toContain("agents.analysis.maxTokens");
    expect(e.find((x) => x.path === "agents.analysis.maxTokens")!.message).toMatch(/does not apply to runtime "a2a"/);
  });

  it("names the illegal id characters for agents and tools", () => {
    const wf = structuredClone(SHIPPED);
    wf.tools["claims_db"] = { type: "websearch" };
    wf.agents["fraud-check"] = { name: "x", maxTokens: 10 };
    const msgs = errors(wf).map((x) => x.message).join("\n");
    expect(msgs).toMatch(/Cedar policy permit_<key>/);
    expect(msgs).toMatch(/no hyphens/);
    expect(msgs).toMatch(/at most one websearch tool/);
  });

  it("catches the references and branch rules no schema can express", () => {
    const wf = structuredClone(SHIPPED);
    wf.agents.intake.tool = "nope";
    wf.steps[1].branch = { when: [{ field: "x", equals: 1, goto: "report" }] };  // on a parallel stage
    wf.steps[2].branch = { default: "intake" };                                   // backwards
    const msgs = errors(wf).map((x) => `${x.path}: ${x.message}`).join("\n");
    expect(msgs).toMatch(/agents\.intake\.tool: "nope" is not a tool/);
    expect(msgs).toMatch(/a parallel stage cannot branch/);
    expect(msgs).toMatch(/not AFTER this stage/);
  });

  it("enforces the exactly-one rules from keys.json", () => {
    const wf = structuredClone(SHIPPED);
    wf.agents.analysis.agentCard = "https://partner.example.com/card.json";  // it already has source
    expect(errors(wf).map((x) => x.message).join()).toMatch(/set exactly one of `agentCard` \/ `source`/);
  });

  it("flags an agent that is defined but never runs", () => {
    const p = three();
    const wf = unplace(p.workflow, "triage");
    expect(errors(wf).map((x) => x.path)).toContain("agents.triage");
  });
});

describe("canvas edits keep steps inside the grammar", () => {
  it("drops new agents into new stages, in order", () => {
    expect(shape(three().workflow)).toEqual(["first_agent", "triage", "fraud_check"]);
  });

  it("joins a stage as a parallel group, and leaves a single step when one member remains", () => {
    const p = three();
    const joined = joinStage(p.workflow, 1, "fraud_check");
    expect(shape(joined)).toEqual(["first_agent", "triage+fraud_check"]);
    expect(joined.steps[1].parallel).toEqual(["triage", "fraud_check"]);
    const split = insertStage(joined, 2, "fraud_check");
    expect(shape(split)).toEqual(["first_agent", "triage", "fraud_check"]);
    expect(split.steps[1]).toEqual({ agent: "triage", hitl: true });
    expect(errors(split)).toEqual([]);
  });

  it("moves a single stage as a whole, keeping its gate and branch", () => {
    const p = three();
    const wf = { ...p.workflow, steps: p.workflow.steps.map((s, i) => (i === 0 ? { ...s, hitl: false } : s)) };
    const moved = insertStage(wf, 3, "first_agent");
    expect(shape(moved)).toEqual(["triage", "fraud_check", "first_agent"]);
    expect(moved.steps[2].hitl).toBe(false);
  });

  it("switches a group between parallel and sequence", () => {
    const p = three();
    const seq = setStageKind(joinStage(p.workflow, 1, "fraud_check"), 1, "sequence");
    expect(seq.steps[1].sequence).toEqual(["triage", "fraud_check"]);
    expect(moveStage(seq, 1, -1).steps[0].sequence).toEqual(["triage", "fraud_check"]);
  });

  it("renames an agent everywhere it is referenced", () => {
    let p = three();
    p = { ...p, workflow: { ...p.workflow, steps: p.workflow.steps.map((s, i) => (i === 0 ? { ...s, branch: { default: "fraud_check" } } : s)) } };
    const r = renameAgent(p, "fraud_check", "fraud");
    expect(Object.keys(r.workflow.agents)).toEqual(["first_agent", "triage", "fraud"]);
    expect(r.workflow.steps[0].branch?.default).toBe("fraud");
    expect(r.prompts.fraud).toBeDefined();
    expect(errors(r.workflow)).toEqual([]);
  });

  it("removes an agent from its stage and from prompts", () => {
    const r = removeAgent(three(), "triage");
    expect(shape(r.workflow)).toEqual(["first_agent", "fraud_check"]);
    expect(r.prompts.triage).toBeUndefined();
  });
});

describe("tools", () => {
  it("binds a dropped tool to an agent, with the KB's first corpus", () => {
    let p = three();
    const t = addTool(p, "Policy Docs", "kb");
    p = bindTool(t.project, "triage", t.key);
    expect(t.key).toBe("policyDocs");
    expect(p.workflow.agents.triage).toMatchObject({ tool: "policyDocs", corpus: "reference" });
    expect(p.workflow.agents.triage.access).toBeUndefined();
    expect(errors(p.workflow)).toEqual([]);
    const gone = removeTool(p, "policyDocs");
    expect(gone.workflow.agents.triage.tool).toBeUndefined();
  });

  it("lets an agent read several tools, and keeps the file in its simplest form", () => {
    let p = three();
    const kb = addTool(p, "Policy Docs", "kb");
    const db = addTool(kb.project, "Claims DB", "mcp");
    p = db.project;
    p = bindTool(p, "triage", kb.key);
    expect(p.workflow.agents.triage.tool).toBe("policyDocs");        // one: a string
    p = bindTool(p, "triage", db.key);
    expect(p.workflow.agents.triage.tool).toEqual(["policyDocs", "claimsDb"]);   // several: a list
    expect(bindTool(p, "triage", db.key)).toBe(p);                    // dropping it twice adds nothing
    expect(p.workflow.agents.triage.corpus).toBe("reference");        // from the KB among them
    expect(errors(p.workflow).filter((e) => e.path.startsWith("agents.triage"))).toEqual([]);

    // Unbinding the KB takes its corpus with it; the last tool goes back to a string.
    const noKb = setTools(p, "triage", ["claimsDb"]);
    expect(noKb.workflow.agents.triage.tool).toBe("claimsDb");
    expect(noKb.workflow.agents.triage.corpus).toBeUndefined();
    const none = unbindTool(noKb, "triage", "claimsDb");
    expect(none.workflow.agents.triage.tool).toBeUndefined();

    // Renaming and deleting a tool reach into lists too.
    const renamed = renameTool(p, "claimsDb", "claims");
    expect(renamed.workflow.agents.triage.tool).toEqual(["policyDocs", "claims"]);
    expect(removeTool(renamed, "policyDocs").workflow.agents.triage).toMatchObject({ tool: "claims" });
  });

  it("names the list mistakes the deploy would reject", () => {
    const p = addTool(three(), "Docs", "mcp").project;
    const wf = structuredClone(p.workflow);
    wf.agents.triage.tool = ["docs", "nope", "docs"];
    wf.agents.triage.corpus = "reference";
    const msgs = errors(wf).map((x) => x.message).join("\n");
    expect(msgs).toMatch(/"nope" is not a tool/);
    expect(msgs).toMatch(/"docs" is listed twice/);
    expect(msgs).toMatch(/`corpus` needs a Knowledge Base among this agent's tools/);
  });

  it("starts a Lambda tool in a shape that only needs its ARN", () => {
    const t = addTool(three(), "Claims DB", "lambda");
    const e = errors(t.project.workflow);
    expect(e.map((x) => x.path)).toEqual(["tools.claimsDb"]);
    expect(e[0].message).toMatch(/`lambdaArn` \/ `source`/);
    const filled = structuredClone(t.project.workflow);
    filled.tools.claimsDb.lambdaArn = "arn:aws:lambda:us-east-1:123456789012:function:claims-db";
    expect(errors(filled)).toEqual([]);
  });
});

describe("import and export", () => {
  it("round-trips a plain workflow.json without losing a key", () => {
    const p = fromFile(JSON.stringify(SHIPPED), "Sample");
    expect(p.workflow).toEqual(SHIPPED);
    expect(p.prompts).toEqual({});
  });

  it("exports what scaffold.py apply reads, and reads it back", () => {
    const p = three();
    const b = toBundle(p);
    expect(b.format).toBe("agentexpress-bundle");
    expect(b.version).toBe(1);
    expect(b.framework?.version).toMatch(/^\d+\.\d+\.\d+/);
    expect(Object.keys(b.prompts)).toEqual(["first_agent", "triage", "fraud_check"]);
    const again = fromFile(JSON.stringify(b), "x");
    expect(again.workflow).toEqual(p.workflow);
    expect(again.prompts).toEqual(p.prompts);
    expect(again.name).toBe("Claims");
  });

  it("refuses a file that is not a workflow", () => {
    expect(() => fromFile("{}", "x")).toThrow(/no `agents` and `steps`/);
    expect(() => fromFile("not json", "x")).toThrow(/Not valid JSON/);
  });
});
