/** Adding, removing and moving what is attached to an agent, one canvas row at a time.
 *
 *  Each row of an agent card (components.ts) can be taken off (its × button, the Delete key,
 *  or dragging it off the agent), moved (dragged onto another agent) and added (the card's
 *  "+ Add" menu, or the inspector's Add select). Every write here is the one the inspector's
 *  own control makes for the same choice: tools through setTools, primitives through
 *  attach.ts, the rest by the same agentcore keys the AgentCore features section writes. */
import { attach, attachableTo, KIND_LABEL, primitiveLabel, type AttachTarget, type Primitive } from "./attach";
import { componentKey, componentsOf, type Component, type ComponentKind } from "./components";
import { bindTool, unbindTool, type Entry, type Json, type Project } from "./model";
import { toolsOf } from "../lib/tools";

const isObj = (v: unknown): v is Record<string, Json> => typeof v === "object" && v !== null && !Array.isArray(v);
const strings = (v: unknown): string[] => (Array.isArray(v) ? v.map(String) : []);

/** Edit agent `id`'s agentcore block; an emptied block goes, as the inspector's setCore. */
function editCore(p: Project, id: string, edit: (core: Record<string, Json>) => void): Project {
  const next = structuredClone(p);
  const a = next.workflow.agents[id];
  const core = isObj(a.agentcore) ? { ...a.agentcore } : {};
  edit(core);
  if (Object.keys(core).length) a.agentcore = core; else delete a.agentcore;
  return next;
}

function editAgent(p: Project, id: string, edit: (a: Entry) => void): Project {
  const next = structuredClone(p);
  edit(next.workflow.agents[id]);
  return next;
}

/** A list in an agentcore group without `name`; the group goes when nothing is left. */
function dropFromList(core: Record<string, Json>, group: string, key: string, name: string) {
  const g = isObj(core[group]) ? { ...core[group] } : {};
  const left = strings(g[key]).filter((x) => x !== name);
  if (left.length) core[group] = { ...g, [key]: left }; else delete core[group];
}

/** Agent `id` without the row `c`. Unknown rows leave the project as it is. */
export function removeComponent(p: Project, id: string, c: Component): Project {
  if (!p.workflow.agents[id]) return p;
  switch (c.kind) {
    case "tool":
      return c.tool ? unbindTool(p, id, c.tool) : p;
    case "guardrail":
      return editCore(p, id, (core) => { delete core.guardrails; });
    case "memory":
      return editCore(p, id, (core) => { delete core.memory; });
    case "policy": {
      if (!c.tool) return editCore(p, id, (core) => { delete core.policy; });
      // A policy is on the TOOL: taking it off changes the tool for every agent that calls it.
      const next = structuredClone(p);
      const t = next.workflow.tools[c.tool];
      if (!t) return p;
      const left = strings(t.policies).filter((x) => x !== c.ref);
      if (left.length) t.policies = left; else delete t.policies;
      return next;
    }
    case "skill":
      return editAgent(p, id, (a) => {
        const left = strings(a.skills).filter((x) => x !== c.ref);
        if (left.length) a.skills = left; else delete a.skills;
      });
    case "evaluation":
      // Removing the last evaluator switches evaluations off: on with none listed would
      // still score with the default (Faithfulness), which is not what "remove" means.
      return editCore(p, id, (core) => {
        if (!c.ref) { delete core.evaluations; return; }
        const ev = isObj(core.evaluations) ? core.evaluations : {};
        const left = strings(ev.evaluators).filter((x) => x !== c.ref);
        if (left.length) core.evaluations = { ...ev, evaluators: left }; else delete core.evaluations;
      });
    case "identity":
      return c.ref ? editCore(p, id, (core) => dropFromList(core, "identity", "outbound", c.ref!)) : p;
    case "human":
      return editAgent(p, id, (a) => { delete a.askPerson; });
    default:
      return p;
  }
}

/** Why row `c` cannot go to agent `to`, or null when it can. */
export function moveIssue(p: Project, _from: string, to: string, c: Component): string | null {
  const target = p.workflow.agents[to];
  if (!target) return `${to} is not an agent of this build.`;
  const name = String(target.name ?? to);
  if (target.runtime === "a2a") return `${name} is a remote (A2A) agent: it runs somewhere else, so nothing can be attached to it here.`;
  if (target.runtime === "task") return `${name} is a task step: it makes no model call, so nothing can be attached to it here.`;
  if (c.kind === "policy" && c.tool) {
    return `${c.name} is a Cedar policy on the tool ${c.tool}, so it applies to every agent that calls that tool. `
      + `Drop it on a Tool row to put it on another tool.`;
  }
  if (c.kind === "human" && target.runtime && target.runtime !== "main") {
    return `${name} runs on its own runtime; only an agent in the main runtime can ask HITL mid-step.`;
  }
  return null;
}

/** Row `c` taken off `from` and put on `to`, written as each agent's own controls write it. */
export function moveComponent(p: Project, from: string, to: string, c: Component): { project: Project; issue?: string } {
  const issue = moveIssue(p, from, to, c);
  if (issue) return { project: p, issue };
  if (from === to) return { project: p };
  const src = p.workflow.agents[from];
  const srcCore = isObj(src?.agentcore) ? src.agentcore : {};
  let next = p;
  switch (c.kind) {
    case "tool":
      if (c.tool) next = bindTool(next, to, c.tool);
      break;
    case "guardrail":
    case "memory":
    case "policy": {
      const group = c.kind === "guardrail" ? "guardrails" : c.kind;
      const block = srcCore[group];
      if (block !== undefined) next = editCore(next, to, (core) => { core[group] = structuredClone(block); });
      break;
    }
    case "skill":
      next = editAgent(next, to, (a) => { a.skills = [...new Set([...strings(a.skills), c.ref!])]; });
      break;
    case "evaluation":
      next = editCore(next, to, (core) => {
        const ev = isObj(core.evaluations) ? core.evaluations : {};
        const list = c.ref ? [...new Set([...strings(ev.evaluators), c.ref])] : strings(ev.evaluators);
        core.evaluations = { ...ev, enabled: true, ...(list.length ? { evaluators: list } : {}) };
      });
      break;
    case "identity":
      next = editCore(next, to, (core) => {
        const g = isObj(core.identity) ? core.identity : {};
        core.identity = { ...g, outbound: [...new Set([...strings(g.outbound), c.ref!])] };
      });
      break;
    case "human":
      next = editAgent(next, to, (a) => { a.askPerson = structuredClone(src.askPerson ?? true); });
      break;
  }
  return { project: removeComponent(next, from, c) };
}

/** One thing the "+ Add" menu offers. */
export interface AddChoice {
  /** The menu group: "Tools", "Guardrails", ... */
  group: string;
  label: string;
  description?: string;
  apply: (p: Project) => Project;
}

/** The kinds a "+ Add" menu offers to create, and the Builder tab (or modal) each opens. */
export const CREATE_KINDS: { kind: ComponentKind; label: string }[] = [
  { kind: "tool", label: "New tool…" },
  { kind: "guardrail", label: "New guardrail…" },
  { kind: "memory", label: "New memory…" },
  { kind: "policy", label: "New Cedar policy…" },
  { kind: "skill", label: "New skill…" },
  { kind: "evaluation", label: "New evaluator…" },
  { kind: "identity", label: "New identity…" },
];

/** Everything that could be added to agent `id` and is not on it yet, grouped by kind. */
export function addChoices(p: Project, id: string): AddChoice[] {
  const wf = p.workflow;
  const a = wf.agents[id];
  if (!a || a.runtime === "a2a" || a.runtime === "task") return [];
  const out: AddChoice[] = [];
  const bound = toolsOf(a.tool);
  // A harness agent calls its tools through the Gateway itself (app/common/harness_agent.py).
  for (const [key, t] of Object.entries(wf.tools ?? {})) {
    if (!bound.includes(key)) out.push({ group: "Tools", label: key, description: String(t?.type ?? ""), apply: (q) => bindTool(q, id, key) });
  }
  for (const { primitive, target } of attachableTo(wf, id)) {
    out.push({
      group: KIND_LABEL[primitive.kind], label: primitiveLabel(primitive),
      ...(target.kind === "tool" ? { description: `On tool ${target.id}, for every agent that calls it` } : {}),
      apply: (q) => attachTo(q, primitive, target),
    });
  }
  if (a.askPerson === undefined && (a.runtime ?? "main") === "main") {
    out.push({
      group: "HITL", label: "Ask HITL questions", description: "Up to 3 questions a step; needs Tool mode \"model\"",
      apply: (q) => editAgent(q, id, (x) => { x.askPerson = true; }),
    });
  }
  return out;
}

function attachTo(p: Project, primitive: Primitive, target: AttachTarget): Project {
  const r = attach(p.workflow, primitive, target);
  return r.workflow === p.workflow ? p : { ...p, workflow: r.workflow };
}

/** Row `key` (componentKey) of agent `id`, if it is still there. */
export function findComponent(p: Project, id: string, key: string): Component | undefined {
  return componentsOf(p.workflow, id).find((c) => componentKey(c) === key);
}
