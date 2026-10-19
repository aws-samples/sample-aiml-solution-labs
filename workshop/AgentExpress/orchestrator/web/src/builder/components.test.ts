/** What an agent card lists: every attached component, labelled, read from workflow.json only. */
import { describe, expect, it } from "vitest";
import { componentsOf, kindCounts } from "./components";
import type { Workflow } from "./model";

const wf = {
  agents: {
    full: {
      name: "Full", tool: ["kb", "pay"], skills: ["triage"], askPerson: { maxQuestions: 2 },
      agentcore: {
        guardrails: { input: true, output: true, use: "strict" },
        memory: { longTerm: ["semantic", "summary"], scope: "user" },
        policy: { enabled: true },
        evaluations: { enabled: true, evaluators: ["Builtin.Faithfulness", "Custom.tone"] },
        identity: { outbound: ["crm"] },
      },
    },
    inputOnly: { name: "In", agentcore: { guardrails: { input: true } } },
    deployGuard: { name: "D", agentcore: { guardrails: { output: true } } },
    memTrue: { name: "M", agentcore: { memory: { longTerm: true } } },
    asker: { name: "A", askPerson: true },
    off: { name: "Off", agentcore: { guardrails: { input: false }, evaluations: { enabled: false, evaluators: ["Builtin.x"] } } },
    plain: { name: "Plain" },
    ghostTool: { name: "G", tool: "missing" },
  },
  tools: {
    kb: { type: "kb" },
    pay: { type: "lambda", approval: "person", policies: ["noRefunds", "businessHours"] },
  },
  guardrail: { deniedWords: ["x"] },
  steps: [{ agent: "full" }],
} as unknown as Workflow;

const rows = (id: string) => componentsOf(wf, id).map((c) => [c.kind, c.name, c.detail ?? ""]);

describe("componentsOf", () => {
  it("lists everything attached to an agent, grouped by kind in a fixed order, each named", () => {
    // agentcore.policy.enabled is set on "full" but draws no row: it does nothing (Cedar is
    // enforced at the Gateway), and validate.ts warns about it instead.
    expect(rows("full")).toEqual([
      ["tool", "kb", "Knowledge Base"],
      ["tool", "pay", "Lambda function · HITL approves each call"],
      ["guardrail", "strict", "checks input and output"],
      ["memory", "agent memory", "semantic, summary · per user"],
      ["policy", "noRefunds", "on pay"],
      ["policy", "businessHours", "on pay"],
      ["skill", "triage", ""],
      ["evaluation", "Faithfulness", "built-in"],
      ["evaluation", "tone", "custom"],
      ["identity", "crm", "outbound credential"],
      ["human", "Asks questions", "up to 2 questions a step"],
    ]);
  });
  it("says which side a guardrail checks, and which guardrail", () => {
    expect(rows("inputOnly")).toEqual([["guardrail", "deployment guardrail", "checks input"]]);
    expect(rows("deployGuard")).toEqual([["guardrail", "deployment guardrail", "checks output"]]);
    const noDeploy = { ...wf, guardrail: undefined } as unknown as Workflow;
    expect(componentsOf(noDeploy, "inputOnly")[0].name).toBe("default guardrail");
  });
  it("reads memory: true as semantic, and askPerson: true as three questions", () => {
    expect(rows("memTrue")).toEqual([["memory", "agent memory", "semantic"]]);
    expect(rows("asker")).toEqual([["human", "Asks questions", "up to 3 questions a step"]]);
  });
  it("shows nothing switched off, and nothing for a plain or unknown agent", () => {
    expect(rows("off")).toEqual([]);
    expect(rows("plain")).toEqual([]);
    expect(rows("nobody")).toEqual([]);
  });
  it("still shows a tool the build does not define, and says so", () => {
    expect(rows("ghostTool")).toEqual([["tool", "missing", "not defined"]]);
  });
  it("marks tool rows and per-tool policies with their tool, for drops and clicks", () => {
    const c = componentsOf(wf, "full");
    expect(c.filter((x) => x.tool).map((x) => `${x.kind}:${x.tool}`)).toEqual(["tool:kb", "tool:pay", "policy:pay", "policy:pay"]);
  });
});

describe("kindCounts", () => {
  it("counts agents, not rows", () => {
    const n = kindCounts(wf);
    expect(n.tool).toBe(2);           // full and ghostTool
    expect(n.policy).toBe(1);         // full has three policy rows: one agent
    expect(n.guardrail).toBe(3);
    expect(n.human).toBe(2);
  });
});
