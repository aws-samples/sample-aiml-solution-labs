/** Attaching primitives: a drop writes what the inspector writes for the same choice (each
 *  case below does both, through the real controls, and compares), an invalid target leaves
 *  the workflow as it was and says why, detach undoes attach, and the agent inspector's
 *  Attach select is the keyboard path for every drop. */
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { act, cleanup, render } from "@testing-library/react";
import { useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
vi.mock("../api", () => ({ api: { get: vi.fn(async () => ({ models: [] })), post: vi.fn(), put: vi.fn(), del: vi.fn() } }));
import {
  attach, attachableTo, attachedTo, detach, isAttached, primitivesOf, targetIssue, validTargets,
  type AttachTarget, type Primitive,
} from "./attach";
import type { Selection } from "./Canvas";
import { record, redo, start, undo } from "./history";
import { addChoices, findComponent, removeComponent } from "./componentOps";
import { Inspector } from "./Inspector";
import type { Project, Workflow } from "./model";
import { BuildTriggers } from "./Triggers";
import { validate } from "./validate";

const GW = 'resource == AgentCore::Gateway::"{{gateway}}"';
const base = (): Workflow => ({
  agents: {
    writer: { name: "Writer", tool: "lookup", maxTokens: 1000 },
    helper: { name: "Helper", maxTokens: 1000 },
    remote: { name: "Remote", runtime: "a2a", agentCard: "https://agent.example.com/.well-known/agent.json" },
  },
  tools: {
    lookup: { type: "mcp", endpoint: "https://mcp.example.com/mcp", description: "Look things up" },
    search: { type: "websearch", description: "Search the web" },
  },
  steps: [{ agent: "writer" }, { parallel: ["helper", "remote"] }],
  guardrail: { deniedWords: ["BLOCKED"] },
  guardrails: { strict: { deniedWords: ["SECRET"] } },
  memories: { notes: { strategies: ["semantic"] } },
  policies: {
    noCalls: { statement: `forbid(principal, action in AgentCore::Action::"{{tool}}", ${GW});` },
    noSearch: { statement: `forbid(principal, action == AgentCore::Action::"search___WebSearch", ${GW});` },
  },
  skills: { triage: { description: "Sort requests", instructions: "Read it, then sort it." } },
  evaluators: { tone: { instructions: "Score the tone from 1 to 5." } },
  orchestrator: {
    triggers: { nightly: { type: "schedule", expression: "rate(1 day)", prompt: "Summarise the day", enabled: false } },
    interceptors: { request: { lambdaArn: "arn:aws:lambda:us-east-1:123456789012:function:audit" } },
  },
} as unknown as Workflow);
const project = (workflow = base()): Project => ({ id: "p1", name: "P", createdAt: "", updatedAt: "", prompts: {}, workflow });

const P = {
  guardrail: { kind: "guardrail", key: "strict" },
  deployment: { kind: "guardrail", key: "" },
  memory: { kind: "memory", key: "notes" },
  policy: { kind: "policy", key: "noCalls" },
  skill: { kind: "skill", key: "triage" },
  evaluator: { kind: "evaluator", key: "tone" },
  trigger: { kind: "trigger", key: "nightly" },
  interceptor: { kind: "interceptor", key: "request" },
} satisfies Record<string, Primitive>;
const writer: AttachTarget = { kind: "agent", id: "writer" };
const lookup: AttachTarget = { kind: "tool", id: "lookup" };

afterEach(cleanup);

// --- the inspector, driven through its own controls ------------------------------------------

let latest: Project;
function InspectorHarness({ initial, selection }: { initial: Project; selection: Selection }) {
  const [p, setP] = useState(initial);
  latest = p;
  // Wired as Builder wires them: "+ Add" applies addChoices, Remove applies removeComponent.
  return <Inspector project={p} selection={selection} issues={[]} onChange={setP} onSelect={() => {}}
    onAddChoice={(a, i) => setP((cur) => addChoices(cur, a)[i].apply(cur))}
    onRemoveComponent={(a, key) => setP((cur) => removeComponent(cur, a, findComponent(cur, a, key)!))} />;
}
const w = () => createWrapper(document.body);
const toggle = (label: string) => {
  const t = w().findAllToggles().find((x) => x.getElement().textContent?.includes(label));
  if (!t) throw new Error(`no toggle ${label}`);
  return t;
};
/** A Cloudscape select's ariaLabel, which it renders as a hidden element its trigger is labelled by. */
const ariaOf = (trigger: HTMLElement) => (trigger.getAttribute("aria-labelledby") ?? "").split(" ")
  .filter((id) => /^(select-arialabel|multiselect-ariaLabel)/.test(id)).map((id) => document.getElementById(id)?.textContent ?? "").join("");
const select = (aria: string) => {
  const s = w().findAllSelects().find((x) => ariaOf(x.findTrigger().getElement()) === aria);
  if (!s) throw new Error(`no select ${aria}`);
  return s;
};
const multiselect = (pred: (el: HTMLElement) => boolean) => {
  const m = w().findAllMultiselects().find((x) => pred(x.getElement()));
  if (!m) throw new Error("no multiselect");
  return m;
};
const ariaIs = (aria: string) => (el: HTMLElement) => {
  return [...el.querySelectorAll("[aria-labelledby]")].some((b) => ariaOf(b as HTMLElement) === aria);
};
async function inspect(selection: Selection, initial = project()) {
  await act(async () => { render(<InspectorHarness initial={initial} selection={selection} />); });
}
async function click(el: { click: () => void }) { await act(async () => { el.click(); }); }

describe("a drop writes what the inspector writes", () => {
  it("a named guardrail: both checks on, and the guardrail picked", async () => {
    await inspect({ kind: "agent", id: "writer" });
    await click(toggle("Guardrail: check input").findNativeInput());
    await click(toggle("Guardrail: check output").findNativeInput());
    const s = select("Pick a guardrail");
    await act(async () => { s.openDropdown(); });
    await act(async () => { s.selectOptionByValue("strict"); });
    expect(latest.workflow).toEqual(attach(base(), P.guardrail, writer).workflow);
  });
  it("the deployment guardrail: both checks on, no guardrail named", async () => {
    await inspect({ kind: "agent", id: "writer" });
    await click(toggle("Guardrail: check input").findNativeInput());
    await click(toggle("Guardrail: check output").findNativeInput());
    expect(latest.workflow).toEqual(attach(base(), P.deployment, writer).workflow);
    expect(latest.workflow.agents.writer.agentcore).toEqual({ guardrails: { input: true, output: true } });
  });
  it("a guardrail on an agent already checking one side keeps that side", async () => {
    const wf = base();
    wf.agents.writer.agentcore = { guardrails: { output: true } };
    await inspect({ kind: "agent", id: "writer" }, project(wf));
    const s = select("Pick a guardrail");
    await act(async () => { s.openDropdown(); });
    await act(async () => { s.selectOptionByValue("strict"); });
    expect(latest.workflow).toEqual(attach(wf, P.guardrail, writer).workflow);
    expect(latest.workflow.agents.writer.agentcore).toEqual({ guardrails: { output: true, use: "strict" } });
  });
  it("a memory: switched on, and the memory picked", async () => {
    await inspect({ kind: "agent", id: "writer" });
    await click(toggle("Long-term memory").findNativeInput());
    const s = select("Pick a memory");
    await act(async () => { s.openDropdown(); });
    await act(async () => { s.selectOptionByValue("notes"); });
    expect(latest.workflow).toEqual(attach(base(), P.memory, writer).workflow);
    expect(latest.workflow.agents.writer.agentcore).toEqual({ memory: { use: "notes" } });
  });
  it("a skill: added to the agent's Skills", async () => {
    await inspect({ kind: "agent", id: "writer" });
    const m = multiselect(ariaIs("Skills"));
    await act(async () => { m.openDropdown(); });
    await act(async () => { m.selectOptionByValue("triage"); });
    expect(latest.workflow).toEqual(attach(base(), P.skill, writer).workflow);
    expect(latest.workflow.agents.writer.skills).toEqual(["triage"]);
  });
  it("an evaluator: evaluations on, and the build's evaluator picked", async () => {
    await inspect({ kind: "agent", id: "writer" });
    await click(toggle("Evaluations").findNativeInput());
    const m = multiselect((el) => el.textContent?.includes("Faithfulness (the default)") ?? false);
    await act(async () => { m.openDropdown(); });
    await act(async () => { m.selectOptionByValue("Custom.tone"); });
    expect(latest.workflow).toEqual(attach(base(), P.evaluator, writer).workflow);
    expect(latest.workflow.agents.writer.agentcore).toEqual({ evaluations: { enabled: true, evaluators: ["Custom.tone"] } });
  });
  it("a Cedar policy: added to the tool's Policies", async () => {
    await inspect({ kind: "tool", id: "lookup" });
    const m = multiselect(ariaIs("Policies"));
    await act(async () => { m.openDropdown(); });
    await act(async () => { m.selectOptionByValue("noCalls"); });
    expect(latest.workflow).toEqual(attach(base(), P.policy, lookup).workflow);
    expect(latest.workflow.tools.lookup.policies).toEqual(["noCalls"]);
  });
  it("a trigger: switched on in the Triggers tab", async () => {
    function T() {
      const [p, setP] = useState(project());
      latest = p;
      return <BuildTriggers project={p} setProject={setP} issues={[]} />;
    }
    await act(async () => { render(<T />); });
    await click(toggle("Off").findNativeInput());
    expect(latest.workflow).toEqual(attach(base(), P.trigger, { kind: "stage", index: 0 }).workflow);
    expect((latest.workflow.orchestrator as never as { triggers: Record<string, unknown> }).triggers.nightly)
      .not.toHaveProperty("enabled");
  });
});

describe("invalid targets", () => {
  const cases: [string, Primitive, AttachTarget][] = [
    ["a guardrail on a remote agent", P.guardrail, { kind: "agent", id: "remote" }],
    ["a memory on a stage", P.memory, { kind: "stage", index: 0 }],
    ["a skill between stages", P.skill, { kind: "gap", index: 1 }],
    ["an evaluator on a tool", P.evaluator, lookup],
    ["a skill this build does not have", { kind: "skill", key: "missing" }, writer],
    ["a policy on an agent", P.policy, writer],
    ["a policy on an agent with no tool", P.policy, { kind: "agent", id: "helper" }],
    ["a policy for another tool", { kind: "policy", key: "noSearch" }, lookup],
    ["a trigger on the second stage", P.trigger, { kind: "stage", index: 1 }],
    ["a trigger on an agent of the second stage", P.trigger, { kind: "agent", id: "helper" }],
    ["an interceptor on an agent", P.interceptor, writer],
    ["an interceptor on the first stage", P.interceptor, { kind: "stage", index: 0 }],
  ];
  it.each(cases)("%s: unchanged, with an issue", (_, p, t) => {
    const wf = base();
    const r = attach(wf, p, t);
    expect(r.workflow).toBe(wf);
    expect(r.issue).toMatch(/\w/);
    expect(wf).toEqual(base());
    const d = detach(wf, p, t);
    expect(d.workflow).toBe(wf);
    expect(d.issue).toBe(r.issue);
  });
  it("the deployment guardrail is refused when the build has none", () => {
    const wf = base();
    delete (wf as Record<string, unknown>).guardrail;
    expect(attach(wf, P.deployment, writer).issue).toMatch(/no deployment guardrail/);
  });
  it("a trigger with no stages has nowhere to go", () => {
    const wf = { ...base(), steps: [] };
    expect(attach(wf, P.trigger, { kind: "stage", index: 0 }).issue).toMatch(/add a stage first/);
  });
});

describe("attach and detach", () => {
  const ok: [string, Primitive, AttachTarget][] = [
    ["guardrail", P.guardrail, writer], ["deployment guardrail", P.deployment, writer], ["memory", P.memory, writer],
    ["skill", P.skill, writer], ["evaluator", P.evaluator, writer], ["policy", P.policy, lookup],
    ["trigger", P.trigger, { kind: "stage", index: 0 }],
  ];
  it.each(ok)("%s: attach then detach gives the workflow back", (_, p, t) => {
    const wf = base();
    const on = attach(wf, p, t);
    expect(on.issue).toBeUndefined();
    expect(on.workflow).not.toEqual(wf);
    expect(isAttached(on.workflow, p, t)).toBe(true);
    expect(wf).toEqual(base());                       // the input is not changed in place
    const off = detach(on.workflow, p, t);
    expect(off.issue).toBeUndefined();
    expect(isAttached(off.workflow, p, t)).toBe(false);
    if (p.kind === "evaluator") {
      // As in the inspector: the pick goes, evaluations stay on (with the default judge).
      expect(off.workflow.agents.writer.agentcore).toEqual({ evaluations: { enabled: true } });
    } else expect(off.workflow).toEqual(wf);   // a trigger's off is `enabled: false`, as this one started
  });
  it.each(ok)("%s: a second drop changes nothing", (_, p, t) => {
    const once = attach(base(), p, t).workflow;
    expect(attach(once, p, t).workflow).toBe(once);
  });
  it("detaching what is not attached changes nothing", () => {
    const wf = base();
    expect(detach(wf, P.memory, writer).workflow).toBe(wf);
  });
  it("detaching a guardrail switches guardrails off; detaching one skill keeps the others", () => {
    let wf = base();
    (wf.skills as Record<string, unknown>).second = { description: "x", instructions: "y" };
    wf = attach(wf, P.guardrail, writer).workflow;
    wf = attach(wf, P.skill, writer).workflow;
    wf = attach(wf, { kind: "skill", key: "second" }, writer).workflow;
    wf = detach(wf, P.guardrail, writer).workflow;
    expect(wf.agents.writer.agentcore).toBeUndefined();
    expect(detach(wf, P.skill, writer).workflow.agents.writer.skills).toEqual(["second"]);
  });
  it("an evaluator added to one already picked keeps the first, and detaching leaves evaluations on", () => {
    const wf = base();
    wf.agents.writer.agentcore = { evaluations: { enabled: true, evaluators: ["Builtin.Helpfulness"] } };
    const on = attach(wf, P.evaluator, writer).workflow;
    expect(on.agents.writer.agentcore).toEqual({ evaluations: { enabled: true, evaluators: ["Builtin.Helpfulness", "Custom.tone"] } });
    expect(detach(on, P.evaluator, writer).workflow.agents.writer.agentcore).toEqual(wf.agents.writer.agentcore);
  });
  it("what a drop writes passes the validator", () => {
    let wf = base();
    for (const [, p, t] of ok) wf = attach(wf, p === P.deployment ? P.guardrail : p, t).workflow;
    const errors = validate(wf).filter((i) => i.severity === "error");
    expect(errors.map((e) => `${e.path}: ${e.message}`)).toEqual([]);
  });
});

describe("what exists, and where it may go", () => {
  it("lists only this build's primitives in the tab order, the deployment guardrail heading the guardrails", () => {
    expect(primitivesOf(base()).map((p) => `${p.kind}:${p.key}`)).toEqual([
      "skill:triage", "memory:notes", "guardrail:", "guardrail:strict", "policy:noCalls", "policy:noSearch",
      "interceptor:request", "trigger:nightly", "evaluator:tone",
    ]);
    expect(primitivesOf({ agents: {}, tools: {}, steps: [], guardrail: {} } as Workflow)).toEqual([]);
  });
  it("names the valid targets of each, for highlighting", () => {
    const wf = base();
    expect(validTargets(wf, P.memory)).toEqual({ agents: ["writer", "helper"], stages: [], tools: [] });
    expect(validTargets(wf, P.policy)).toEqual({ agents: [], stages: [], tools: ["lookup", "search"] });
    expect(validTargets(wf, { kind: "policy", key: "noSearch" })).toEqual({ agents: [], stages: [], tools: ["search"] });
    expect(validTargets(wf, P.trigger)).toEqual({ agents: ["writer"], stages: [0], tools: [] });
    expect(validTargets(wf, P.interceptor)).toEqual({ agents: [], stages: [], tools: [] });
  });
  it("says what an agent has and could have", () => {
    let wf = base();
    expect(attachableTo(wf, "writer").map((x) => `${x.primitive.kind}:${x.primitive.key}`)).toEqual([
      "skill:triage", "memory:notes", "guardrail:", "guardrail:strict", "policy:noCalls", "evaluator:tone",
    ]);
    expect(attachableTo(wf, "remote")).toEqual([]);
    wf = attach(wf, P.memory, writer).workflow;
    wf = attach(wf, P.policy, lookup).workflow;
    expect(attachedTo(wf, "writer")).toEqual([
      { primitive: P.memory, target: writer }, { primitive: P.policy, target: lookup },
    ]);
    expect(targetIssue(wf, P.memory, writer)).toBeNull();
  });
});

describe("the agent inspector's Add and Remove are the keyboard path for each drop", () => {
  const pick = async (aria: string, label: string) => {
    const s = select(aria);
    await act(async () => { s.openDropdown(); });
    const opts = s.findDropdown().findOptions();
    const i = opts.findIndex((o) => o.findLabel().getElement().textContent === label);
    if (i < 0) throw new Error(`no option ${label} in ${opts.map((o) => o.getElement().textContent).join(" | ")}`);
    await act(async () => { s.selectOption(i + 1); });
  };
  // [option label, primitive, target, the row's aria name on the agent panel]
  const cases: [string, Primitive, AttachTarget, string][] = [
    ["strict", P.guardrail, writer, "Guardrail: strict"],
    ["Deployment guardrail", P.deployment, writer, "Guardrail: deployment guardrail"],
    ["notes", P.memory, writer, "Memory: notes"], ["noCalls", P.policy, lookup, "Policy: noCalls"],
    ["triage", P.skill, writer, "Skill: triage"], ["tone", P.evaluator, writer, "Eval: tone"],
  ];
  it.each(cases)("add %s", async (label, p, t) => {
    await inspect({ kind: "agent", id: "writer" });
    await pick("Add to this agent", label);
    expect(latest.workflow).toEqual(attach(base(), p, t).workflow);
  });
  it.each(cases)("remove %s", async (_label, p, t, row) => {
    const on = attach(base(), p, t).workflow;
    await inspect({ kind: "agent", id: "writer" }, project(on));
    await click(w().findButton(`[aria-label="Remove ${row} from Writer"]`)!);
    // Removing the last evaluator switches evaluations off (on with none would still score
    // with the default); everything else comes off exactly as a detach takes it off.
    const want = detach(on, p, t).workflow;
    if (p.kind === "evaluator") delete (want.agents.writer.agentcore as Record<string, unknown> | undefined)?.evaluations;
    if (want.agents.writer.agentcore && !Object.keys(want.agents.writer.agentcore).length) delete want.agents.writer.agentcore;
    expect(latest.workflow).toEqual(want);
  });
  it("adds a tool, and asking a person, which are not primitives", async () => {
    await inspect({ kind: "agent", id: "writer" });
    await pick("Add to this agent", "Ask HITL questions");
    expect(latest.workflow.agents.writer.askPerson).toBe(true);
  });
  it("is not offered on a remote agent", async () => {
    await inspect({ kind: "agent", id: "remote" });
    expect(w().findAllSelects().some((s) => ariaOf(s.findTrigger().getElement()) === "Add to this agent")).toBe(false);
  });
});
describe("undo", () => {
  it("takes back each attach and detach, and redo puts it back", () => {
    const p0 = project();
    let h = start(p0, 0);
    const p1 = { ...p0, workflow: attach(p0.workflow, P.memory, writer).workflow };
    h = record(h, p1, 10, { step: true });             // its own step, even right after another edit
    const p2 = { ...p1, workflow: detach(p1.workflow, P.memory, writer).workflow };
    h = record(h, p2, 20, { step: true });
    expect(h.past).toHaveLength(2);
    h = undo(h);
    expect(h.present.workflow).toEqual(p1.workflow);
    h = undo(h);
    expect(h.present.workflow).toEqual(base());
    h = redo(h);
    expect(isAttached(h.present.workflow, P.memory, writer)).toBe(true);
  });
});
