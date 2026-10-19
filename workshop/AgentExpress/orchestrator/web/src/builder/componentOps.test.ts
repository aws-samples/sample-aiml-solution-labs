/** Taking a row off an agent, moving it to another, and the "+ Add" menu (componentOps.ts). */
import { describe, expect, it } from "vitest";

import { addChoices, findComponent, moveComponent, removeComponent } from "./componentOps";
import { componentKey, componentsOf } from "./components";
import type { Project, Workflow } from "./model";

const wf = (): Workflow => ({
  agents: {
    a: {
      name: "A", tool: ["kb", "pay"], skills: ["triage", "tone"], askPerson: { maxQuestions: 2 },
      agentcore: {
        guardrails: { input: true, output: true, use: "strict" },
        memory: { use: "notes" },
        evaluations: { enabled: true, evaluators: ["Builtin.Faithfulness", "Custom.voice"] },
        identity: { outbound: ["crm", "erp"] },
      },
    },
    b: { name: "B" },
    far: { name: "Far", runtime: "a2a" },
  },
  tools: { kb: { type: "kb" }, pay: { type: "lambda", policies: ["noRefunds", "readOnly"] }, web: { type: "websearch" } },
  guardrails: { strict: {} }, memories: { notes: {} }, skills: { triage: {}, tone: {}, other: {} },
  identities: { crm: {}, erp: {}, hr: {} }, policies: { noRefunds: {}, readOnly: {} },
  steps: [{ parallel: ["a", "b"] }],
} as unknown as Workflow);
const project = (w = wf()): Project => ({ id: "p", name: "P", createdAt: "", updatedAt: "", prompts: {}, workflow: w });
const row = (p: Project, agent: string, kind: string, ref?: string) => {
  const c = componentsOf(p.workflow, agent).find((x) => x.kind === kind && (ref === undefined || x.ref === ref));
  if (!c) throw new Error(`no ${kind} ${ref ?? ""} on ${agent}`);
  return c;
};

describe("removeComponent takes one row off, and only that row", () => {
  it.each([
    ["tool", "pay", (w: Workflow) => expect(w.agents.a.tool).toBe("kb")],
    ["guardrail", "strict", (w: Workflow) => expect((w.agents.a.agentcore as never as Record<string, unknown>).guardrails).toBeUndefined()],
    ["memory", "notes", (w: Workflow) => expect((w.agents.a.agentcore as never as Record<string, unknown>).memory).toBeUndefined()],
    ["skill", "tone", (w: Workflow) => expect(w.agents.a.skills).toEqual(["triage"])],
    ["evaluation", "Custom.voice", (w: Workflow) =>
      expect((w.agents.a.agentcore as never as Record<string, Record<string, unknown>>).evaluations.evaluators).toEqual(["Builtin.Faithfulness"])],
    ["identity", "crm", (w: Workflow) =>
      expect((w.agents.a.agentcore as never as Record<string, Record<string, unknown>>).identity.outbound).toEqual(["erp"])],
    ["policy", "noRefunds", (w: Workflow) => expect(w.tools.pay.policies).toEqual(["readOnly"])],
  ])("%s %s", (kind, ref, check) => {
    const p = project();
    const c = row(p, "a", kind, ref);
    const next = removeComponent(p, "a", c);
    check(next.workflow);
    // A tool's Cedar policies are shown on the agents that call it, so they leave with it.
    expect(componentsOf(next.workflow, "a").map(componentKey)).toEqual(componentsOf(p.workflow, "a")
      .filter((x) => componentKey(x) !== componentKey(c) && !(kind === "tool" && x.kind === "policy" && x.tool === ref))
      .map(componentKey));
    expect(p.workflow).toEqual(wf());                                    // the input is not changed
  });
  it("switches evaluations off with the last evaluator, and drops an emptied block", () => {
    let p = project();
    p = removeComponent(p, "a", row(p, "a", "evaluation", "Custom.voice"));
    p = removeComponent(p, "a", row(p, "a", "evaluation", "Builtin.Faithfulness"));
    expect(componentsOf(p.workflow, "a").some((c) => c.kind === "evaluation")).toBe(false);
    for (const c of componentsOf(p.workflow, "a")) p = removeComponent(p, "a", c);
    expect(p.workflow.agents.a).toEqual({ name: "A" });
  });
  it("takes asking a person off", () => {
    let p = project();
    p = removeComponent(p, "a", row(p, "a", "human"));
    expect(p.workflow.agents.a.askPerson).toBeUndefined();
  });
});

describe("moveComponent", () => {
  it.each(["tool", "guardrail", "memory", "skill", "evaluation", "identity", "human"])("moves a %s to another agent", (kind) => {
    const p = project();
    const c = row(p, "a", kind);
    const r = moveComponent(p, "a", "b", c);
    expect(r.issue).toBeUndefined();
    expect(findComponent(r.project, "b", componentKey(c))).toBeTruthy();
    expect(findComponent(r.project, "a", componentKey(c))).toBeUndefined();
  });
  it("refuses a remote agent, and a tool's policy, with why", () => {
    const p = project();
    expect(moveComponent(p, "a", "far", row(p, "a", "memory")).issue).toMatch(/remote \(A2A\)/);
    expect(moveComponent(p, "a", "b", row(p, "a", "policy", "noRefunds")).issue).toMatch(/Tool row/);
    expect(moveComponent(p, "a", "a", row(p, "a", "memory")).project).toBe(p);
  });
});

describe("addChoices: everything that could still go on an agent", () => {
  it("offers unbound tools, unused primitives and identities, and asking a person", () => {
    const p = project();
    const b = addChoices(p, "b").map((c) => `${c.group}:${c.label}`);
    expect(b).toEqual(expect.arrayContaining(["Tools:kb", "Tools:web", "Memory:notes", "Skills:triage", "Identity:hr", "HITL:Ask HITL questions"]));
    const a = addChoices(p, "a").map((c) => `${c.group}:${c.label}`);
    expect(a).toContain("Tools:web");
    expect(a).not.toContain("Tools:kb");
    expect(a).not.toContain("Identity:crm");
    expect(a).toContain("Skills:other");
    expect(a.some((x) => x.startsWith("HITL"))).toBe(false);
    expect(addChoices(p, "far")).toEqual([]);
  });
  it("applies a choice as the inspector would write it", () => {
    const p = project();
    const pick = (label: string) => addChoices(p, "b").find((c) => c.label === label)!.apply(p).workflow.agents.b;
    expect(pick("web").tool).toBe("web");
    expect((pick("hr").agentcore as never as Record<string, Record<string, unknown>>).identity.outbound).toEqual(["hr"]);
    expect(pick("Ask HITL questions").askPerson).toBe(true);
  });
});
