/** Attaching a primitive (a guardrail, a memory, a skill, ...) by dropping it on the canvas,
 *  and taking it off again.
 *
 *  A drop writes exactly the workflow.json the inspector's own control writes for the same
 *  choice (attach.test.tsx does both and compares), so dragging is a shortcut, never a second
 *  way to configure something. No key here is new. Where each may land, and why:
 *
 *    guardrail, memory,     on an agent that runs here. A remote (A2A) agent runs somewhere
 *    skill, evaluator,      else: nothing in this deployment guards, remembers for, teaches,
 *    identity               scores or signs in for it, and the inspector offers none of
 *                           these for one.
 *    Cedar policy           on a tool. A policy governs a tool's calls at the Gateway, for
 *                           every agent that calls it (tools.<key>.policies), so an agent is
 *                           not a target. On the canvas a tool is a chip of the selected agent.
 *    trigger                on the first stage. A trigger starts the workflow, which runs from
 *                           its first stage; dropping one turns it on, detaching turns it off.
 *    interceptor            nowhere. It runs on every tool call of every agent, and
 *                           workflow.json has no key that scopes one to an agent or a stage.
 *
 *  An invalid target returns an issue and the workflow unchanged (the same object). A valid
 *  target that already has the primitive returns the same object too: nothing to record. */

import * as cedar from "./cedar";
import { interceptorsOf } from "./Interceptors";
import type { DropTarget } from "./layout";
import { stageOf, type Entry, type Json, type Workflow } from "./model";
import { triggersOf } from "./Triggers";

export type PrimitiveKind = "guardrail" | "memory" | "policy" | "skill" | "evaluator" | "identity" | "interceptor" | "trigger";

/** One thing of the build that can be dropped. `key` names it in its map (guardrails,
 *  memories, policies, skills, evaluators, orchestrator.triggers), or the interceptor's point
 *  ("request" or "response"). The deployment guardrail (workflow.guardrail) is key "": no
 *  named guardrail can be called that (names start with a letter). */
export interface Primitive { kind: PrimitiveKind; key: string }

export const DEPLOYMENT_GUARDRAIL = "";

/** What a primitive can land on: what the canvas hit-tests, or a tool chip. */
export type AttachTarget = DropTarget | { kind: "tool"; id: string };

export interface AttachResult { workflow: Workflow; issue?: string }

/** The palette's groups, in the order the Builder's tabs list them. */
export const KIND_LABEL: Record<PrimitiveKind, string> = {
  guardrail: "Guardrails", memory: "Memory", policy: "Policies", skill: "Skills",
  evaluator: "Evals", identity: "Identity", interceptor: "Interceptors", trigger: "Triggers",
};
/** The Builder's tab and library order (Builder.tsx TAB_ORDER): skills, memory, guardrails,
 *  identity, policies, interceptors, triggers, evals. The left pane lists them in this order too. */
export const KIND_ORDER: PrimitiveKind[] = ["skill", "memory", "guardrail", "identity", "policy", "interceptor", "trigger", "evaluator"];

const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);
const named = (wf: Workflow, m: string): Record<string, unknown> => (isObj(wf[m]) ? wf[m] as Record<string, unknown> : {});
const MAP: Record<"guardrail" | "memory" | "policy" | "skill" | "evaluator" | "identity", string> = {
  guardrail: "guardrails", memory: "memories", policy: "policies", skill: "skills", evaluator: "evaluators",
  identity: "identities",
};

/** The deployment guardrail counts as there when it has anything in it: an empty block
 *  enforces nothing, and the Guardrails tab writes `{}` for one never filled in. */
export function hasDeploymentGuardrail(wf: Workflow): boolean {
  const g = (wf as Record<string, unknown>).guardrail;
  return isObj(g) && Object.keys(g).length > 0;
}

/** Everything this build has that can be dropped, in palette order. Only what exists. */
export function primitivesOf(wf: Workflow): Primitive[] {
  const out: Primitive[] = [];
  for (const kind of KIND_ORDER) {
    // The deployment guardrail heads the guardrails, wherever they sit in the order.
    if (kind === "guardrail" && hasDeploymentGuardrail(wf)) out.push({ kind, key: DEPLOYMENT_GUARDRAIL });
    if (kind === "interceptor") {
      const ics = interceptorsOf(wf);
      for (const point of ["request", "response"] as const) if (ics[point]) out.push({ kind, key: point });
    } else if (kind === "trigger") {
      for (const key of Object.keys(triggersOf(wf))) out.push({ kind, key });
    } else {
      for (const key of Object.keys(named(wf, MAP[kind]))) out.push({ kind, key });
    }
  }
  return out;
}

export function primitiveLabel(p: Primitive): string {
  if (p.kind === "guardrail" && p.key === DEPLOYMENT_GUARDRAIL) return "Deployment guardrail";
  if (p.kind === "interceptor") return p.key === "request" ? "Before each request" : "After each answer";
  return p.key;
}

// --- reading an agent ----------------------------------------------------------------

const coreOf = (a: Entry | undefined): Record<string, Record<string, Json>> =>
  (a && isObj(a.agentcore) ? a.agentcore : {}) as Record<string, Record<string, Json>>;
const strings = (v: unknown): string[] => (Array.isArray(v) ? v.map(String) : []);
const toolsOfAgent = (a: Entry | undefined): string[] =>
  (a?.tool === undefined ? [] : Array.isArray(a.tool) ? a.tool.map(String) : [String(a.tool)]);

/** Set one agentcore key; `undefined` removes it, and an emptied group or block goes too.
 *  The same rule as the inspector's setCore: an empty block reads like a setting. */
function withCore(wf: Workflow, id: string, group: string, key: string, v: Json | undefined): Workflow {
  const next = structuredClone(wf);
  const a = next.agents[id];
  const core = { ...coreOf(a) };
  const grp = { ...(core[group] ?? {}) };
  if (v === undefined) delete grp[key]; else grp[key] = v;
  if (Object.keys(grp).length) core[group] = grp; else delete core[group];
  if (Object.keys(core).length) a.agentcore = core as unknown as Json; else delete a.agentcore;
  return next;
}

/** Remove one whole agentcore group, as the inspector's clearCore (a feature switched off). */
function withoutCore(wf: Workflow, id: string, group: string): Workflow {
  const next = structuredClone(wf);
  const a = next.agents[id];
  const core = { ...coreOf(a) };
  delete core[group];
  if (Object.keys(core).length) a.agentcore = core as unknown as Json; else delete a.agentcore;
  return next;
}

// --- where a primitive may land --------------------------------------------------------

/** Why `t` cannot take `p`, or null when it can. */
export function targetIssue(wf: Workflow, p: Primitive, t: AttachTarget): string | null {
  const what = primitiveLabel(p);
  if (p.kind === "interceptor") {
    return `The ${what.toLowerCase()} interceptor runs on every tool call of every agent, at the Gateway: it is on for `
      + "the whole build, not attached to one agent. Change it on the Interceptors tab.";
  }
  if (p.kind === "trigger") {
    if (!(p.key in triggersOf(wf))) return `${what} is not a trigger of this build.`;
    const first = t.kind === "stage" ? t.index === 0 : t.kind === "agent" ? stageOf(wf, t.id) === 0 : false;
    if (!wf.steps.length) return "A trigger starts the workflow: add a stage first, then drop it on the first stage.";
    return first ? null : "A trigger starts the workflow, which runs from its first stage: drop it on the first stage.";
  }
  if (p.kind === "guardrail" && p.key === DEPLOYMENT_GUARDRAIL) {
    if (!hasDeploymentGuardrail(wf)) return "This build has no deployment guardrail: set one on the Guardrails tab.";
  } else if (!(p.key in named(wf, MAP[p.kind]))) {
    return `${what} is not one of this build's ${KIND_LABEL[p.kind].toLowerCase()}.`;
  }
  if (p.kind === "policy") {
    if (t.kind === "tool") {
      if (!(t.id in wf.tools)) return `${t.id} is not a tool of this build.`;
      const entry = named(wf, "policies")[p.key];
      const statement = isObj(entry) ? entry.statement : undefined;
      // A policy written for one tool governs only it; one written for {{tool}} fits any.
      if (typeof statement === "string" && !cedar.isTemplate(statement) && !cedar.governs(statement, t.id)) {
        return `${what} names other tools, not ${t.id}: it can only be attached to the tools it names.`;
      }
      return null;
    }
    if (t.kind === "agent" && wf.agents[t.id]) {
      const name = String(wf.agents[t.id].name ?? t.id);
      return toolsOfAgent(wf.agents[t.id]).length
        ? `A Cedar policy attaches to a tool, and governs it for every agent that calls it: drop ${what} on one of `
          + `${name}'s Tool rows, or pick it in the tool's Policies setting.`
        : `A Cedar policy attaches to a tool, and ${name} has none.`;
    }
    return "A Cedar policy attaches to a tool: drop it on one of an agent's tools.";
  }
  if (t.kind !== "agent") return `Drop ${what} on an agent to attach it.`;
  const a = wf.agents[t.id];
  if (!a) return `${t.id} is not an agent of this build.`;
  if (a.runtime === "a2a") {
    return `${String(a.name ?? t.id)} is a remote (A2A) agent: it runs somewhere else, so this deployment cannot `
      + `attach ${KIND_LABEL[p.kind].toLowerCase()} to it.`;
  }
  if (a.runtime === "task") {
    return `${String(a.name ?? t.id)} is a task step: it makes no model call, so ${KIND_LABEL[p.kind].toLowerCase()} has nothing to work on.`;
  }
  if (a.runtime === "harness" && (p.kind === "skill" || p.kind === "identity")) {
    return `${String(a.name ?? t.id)} runs on an AgentCore Harness, which cannot use this build's ${KIND_LABEL[p.kind].toLowerCase()} yet.`;
  }
  return null;
}

/** Whether `t` already has `p`. */
export function isAttached(wf: Workflow, p: Primitive, t: AttachTarget): boolean {
  if (p.kind === "trigger") return triggersOf(wf)[p.key]?.enabled !== false;
  if (p.kind === "policy") return t.kind === "tool" && strings(wf.tools[t.id]?.policies).includes(p.key);
  if (t.kind !== "agent" || !wf.agents[t.id]) return false;
  const a = wf.agents[t.id];
  const core = coreOf(a);
  switch (p.kind) {
    case "guardrail": {
      const g = core.guardrails ?? {};
      if (!g.input && !g.output) return false;
      return p.key === DEPLOYMENT_GUARDRAIL ? !g.use && !g.guardrailId : g.use === p.key;
    }
    case "memory": return core.memory?.use === p.key;
    case "skill": return strings(a.skills).includes(p.key);
    case "evaluator": return core.evaluations?.enabled === true && strings(core.evaluations?.evaluators).includes(`Custom.${p.key}`);
    case "identity": return strings(core.identity?.outbound).includes(p.key);
    default: return false;
  }
}

/** Every valid target for `p` in this workflow, for highlighting while it is dragged. */
export function validTargets(wf: Workflow, p: Primitive): { agents: string[]; stages: number[]; tools: string[] } {
  const ok = (t: AttachTarget) => targetIssue(wf, p, t) === null;
  return {
    agents: Object.keys(wf.agents).filter((id) => ok({ kind: "agent", id })),
    stages: wf.steps.map((_, index) => index).filter((index) => ok({ kind: "stage", index })),
    tools: Object.keys(wf.tools).filter((id) => ok({ kind: "tool", id })),
  };
}

// --- attach and detach -------------------------------------------------------------------

/** `p` on `t`, written as the inspector writes it. */
export function attach(wf: Workflow, p: Primitive, t: AttachTarget): AttachResult {
  const issue = targetIssue(wf, p, t);
  if (issue) return { workflow: wf, issue };
  if (isAttached(wf, p, t)) return { workflow: wf };
  if (p.kind === "trigger") return { workflow: withTrigger(wf, p.key, true) };
  if (p.kind === "policy") {
    const id = (t as { id: string }).id;
    return { workflow: withToolPolicies(wf, id, [...strings(wf.tools[id].policies), p.key]) };
  }
  const id = (t as { id: string }).id;
  const core = coreOf(wf.agents[id]);
  switch (p.kind) {
    case "guardrail": {
      // The inspector turns a side on before it offers the pick. A drop on an agent that
      // checks neither side checks both, the safer default; one already on is kept.
      let next = wf;
      const g = core.guardrails ?? {};
      if (!g.input && !g.output) {
        next = withCore(next, id, "guardrails", "input", true);
        next = withCore(next, id, "guardrails", "output", true);
      }
      return { workflow: withCore(next, id, "guardrails", "use", p.key === DEPLOYMENT_GUARDRAIL ? undefined : p.key) };
    }
    case "memory":
      return { workflow: withCore(wf, id, "memory", "use", p.key) };
    case "skill": {
      const next = structuredClone(wf);
      next.agents[id] = { ...next.agents[id], skills: [...strings(wf.agents[id].skills), p.key] };
      return { workflow: next };
    }
    case "evaluator": {
      const cur = strings(core.evaluations?.evaluators).filter((e) => e !== `Custom.${p.key}`);
      const on = core.evaluations?.enabled === true ? wf : withCore(wf, id, "evaluations", "enabled", true);
      return { workflow: withCore(on, id, "evaluations", "evaluators", [...cur, `Custom.${p.key}`]) };
    }
    case "identity":
      // As the inspector's Identities pick writes it: agentcore.identity.outbound.
      return { workflow: withCore(wf, id, "identity", "outbound", [...strings(core.identity?.outbound), p.key]) };
    default:
      return { workflow: wf };
  }
}

/** `p` off `t`, as the inspector takes it off: a feature switched off, a pick removed. */
export function detach(wf: Workflow, p: Primitive, t: AttachTarget): AttachResult {
  const issue = targetIssue(wf, p, t);
  if (issue) return { workflow: wf, issue };
  if (!isAttached(wf, p, t)) return { workflow: wf };
  if (p.kind === "trigger") return { workflow: withTrigger(wf, p.key, false) };
  const id = (t as { id: string }).id;
  switch (p.kind) {
    case "policy":
      return { workflow: withToolPolicies(wf, id, strings(wf.tools[id].policies).filter((n) => n !== p.key)) };
    case "guardrail":
      return { workflow: withoutCore(wf, id, "guardrails") };
    case "memory":
      return { workflow: withoutCore(wf, id, "memory") };
    case "skill": {
      const next = structuredClone(wf);
      const left = strings(wf.agents[id].skills).filter((s) => s !== p.key);
      const a = { ...next.agents[id] };
      if (left.length) a.skills = left; else delete a.skills;
      next.agents[id] = a;
      return { workflow: next };
    }
    case "evaluator": {
      const left = strings(coreOf(wf.agents[id]).evaluations?.evaluators).filter((e) => e !== `Custom.${p.key}`);
      return { workflow: withCore(wf, id, "evaluations", "evaluators", left.length ? left : undefined) };
    }
    case "identity": {
      const left = strings(coreOf(wf.agents[id]).identity?.outbound).filter((n) => n !== p.key);
      return { workflow: left.length ? withCore(wf, id, "identity", "outbound", left) : withoutCore(wf, id, "identity") };
    }
    default:
      return { workflow: wf };
  }
}

/** A tool's policies, as the tool inspector writes them: an emptied list goes. */
function withToolPolicies(wf: Workflow, id: string, names: string[]): Workflow {
  const next = structuredClone(wf);
  const t = { ...next.tools[id] };
  if (names.length) t.policies = names; else delete t.policies;
  next.tools[id] = t;
  return next;
}

/** A trigger on or off, as the Triggers tab's toggle writes it: on is the default, so it
 *  leaves no key; off is `enabled: false`. */
function withTrigger(wf: Workflow, name: string, on: boolean): Workflow {
  const orch = { ...((wf.orchestrator ?? {}) as Record<string, Json>) };
  const all = { ...triggersOf(wf) };
  const { enabled: _old, ...rest } = all[name];
  all[name] = on ? rest : { ...rest, enabled: false };
  orch.triggers = all as Json;
  return { ...wf, orchestrator: orch };
}

/** Every primitive attached to agent `id`: on the agent itself, and policies on its tools. */
export function attachedTo(wf: Workflow, id: string): { primitive: Primitive; target: AttachTarget }[] {
  const out: { primitive: Primitive; target: AttachTarget }[] = [];
  for (const p of primitivesOf(wf)) {
    if (p.kind === "policy") {
      for (const tool of toolsOfAgent(wf.agents[id])) {
        if (tool in wf.tools && isAttached(wf, p, { kind: "tool", id: tool })) out.push({ primitive: p, target: { kind: "tool", id: tool } });
      }
    } else if (p.kind !== "trigger" && p.kind !== "interceptor" && isAttached(wf, p, { kind: "agent", id })) {
      out.push({ primitive: p, target: { kind: "agent", id } });
    }
  }
  return out;
}

/** Every primitive that could be attached to agent `id` and is not yet: for the inspector's
 *  Attach control, the keyboard path for each drop. Policies are offered per tool. */
export function attachableTo(wf: Workflow, id: string): { primitive: Primitive; target: AttachTarget }[] {
  const out: { primitive: Primitive; target: AttachTarget }[] = [];
  for (const p of primitivesOf(wf)) {
    if (p.kind === "policy") {
      for (const tool of toolsOfAgent(wf.agents[id])) {
        const t: AttachTarget = { kind: "tool", id: tool };
        if (targetIssue(wf, p, t) === null && !isAttached(wf, p, t)) out.push({ primitive: p, target: t });
      }
    } else if (p.kind !== "trigger" && p.kind !== "interceptor") {
      const t: AttachTarget = { kind: "agent", id };
      if (targetIssue(wf, p, t) === null && !isAttached(wf, p, t)) out.push({ primitive: p, target: t });
    }
  }
  return out;
}
