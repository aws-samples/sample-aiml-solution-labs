/** What is attached to each agent, as named, labelled rows for the Builder canvas.
 *
 *  An agent card lists its components under its name, one row each, always visible: its
 *  tools (with each tool's approval and Cedar policies), guardrail, memory, Cedar check, skills,
 *  evaluators, outbound identities, and whether it asks HITL (a human in the loop). Every row says what KIND of
 *  thing it is in words ("Tool", "Guardrail", ...) and names it, so nobody has to decode a
 *  letter or click to find out. The "Show" menu hides kinds the reader does not want.
 *
 *  Read from workflow.json only: a row appears exactly when the key that attaches the thing is
 *  set (the same keys layers.ts and attach.ts read). Nothing is stored. */
import { toolsOf } from "../lib/tools";
import type { Entry, Workflow } from "./model";

export type ComponentKind = "tool" | "guardrail" | "memory" | "policy" | "skill" | "evaluation" | "identity" | "human";

export interface ComponentType {
  id: ComponentKind;
  /** The group's name (Show menu, palette). */
  label: string;
  /** The word on each row's tag. */
  tag: string;
  hint: string;
}

export const COMPONENT_TYPES: ComponentType[] = [
  { id: "tool", label: "Tools", tag: "Tool", hint: "What the agent can call: a knowledge base, an API, an MCP server, a Lambda, web search" },
  { id: "guardrail", label: "Guardrails", tag: "Guardrail", hint: "A Bedrock guardrail checks the agent's input or output" },
  { id: "memory", label: "Memory", tag: "Memory", hint: "AgentCore Memory: what the agent remembers across runs" },
  { id: "policy", label: "Cedar policies", tag: "Policy", hint: "Cedar policies that decide the agent's tool calls" },
  { id: "skill", label: "Skills", tag: "Skill", hint: "SKILL.md know-how the agent may open" },
  { id: "evaluation", label: "Evaluations", tag: "Eval", hint: "AgentCore Evaluations score the agent's answers" },
  { id: "identity", label: "Identity", tag: "Identity", hint: "Outbound credentials from AgentCore Identity" },
  { id: "human", label: "HITL", tag: "HITL", hint: "HITL (a human in the loop): the agent can ask questions mid-step" },
];
export const ALL_KINDS: ComponentKind[] = COMPONENT_TYPES.map((t) => t.id);

export interface Component {
  kind: ComponentKind;
  name: string;
  /** A few words of what it does here ("input and output", "semantic, summary"). */
  detail?: string;
  /** Set on a tool row, and on a policy that governs one tool: the tool key. */
  tool?: string;
  /** What workflow.json names it by, where it has a name: the tool key, the guardrail or
   *  memory picked ("" for the deployment guardrail), the policy, skill or identity name,
   *  the evaluator id ("Builtin.Faithfulness"). Unset for what is only switched on. */
  ref?: string;
}

/** A row's identity within its agent, stable across edits that do not remove it. */
export const componentKey = (c: Component): string => `${c.kind}:${c.tool ?? ""}:${c.ref ?? c.name}`;

export const TOOL_TYPE_LABEL: Record<string, string> = {
  kb: "Knowledge Base", mcp: "MCP server", openapi: "REST API (OpenAPI)", lambda: "Lambda function",
  websearch: "Web search", apigateway: "REST API (API Gateway)", smithy: "AWS API (Smithy)",
};

const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);
const obj = (v: unknown): Record<string, unknown> => (isObj(v) ? v : {});
const strings = (v: unknown): string[] => (Array.isArray(v) ? v.filter((x) => typeof x === "string" && x) as string[] : []);

function guardrailName(wf: Workflow, g: Record<string, unknown>): string {
  if (typeof g.use === "string" && g.use) return g.use;
  if (typeof g.guardrailId === "string" && g.guardrailId) return "existing guardrail";
  return isObj(wf.guardrail) && Object.keys(wf.guardrail).length ? "deployment guardrail" : "default guardrail";
}

/** An evaluator id as a person reads it: "Builtin.Faithfulness" -> "Faithfulness". */
const evaluatorName = (e: string): { name: string; detail: string } => {
  const [prefix, ...rest] = e.split(".");
  if (rest.length && (prefix === "Builtin" || prefix === "Custom")) {
    return { name: rest.join("."), detail: prefix === "Builtin" ? "built-in" : "custom" };
  }
  return { name: e, detail: "" };
};

/** Every component attached to agent `id`, in COMPONENT_TYPES order, tools in their own order. */
export function componentsOf(wf: Workflow, id: string): Component[] {
  const spec: Entry | undefined = wf.agents?.[id];
  if (!spec) return [];
  const core = obj(spec.agentcore);
  const out: Component[] = [];

  for (const key of toolsOf(spec.tool)) {
    const t = obj(wf.tools?.[key]);
    const type = TOOL_TYPE_LABEL[String(t.type ?? "")] ?? (t.type ? String(t.type) : "not defined");
    const approval = t.approval === "person" || t.approval === "event" ? "HITL approves each call" : "";
    out.push({ kind: "tool", name: key, tool: key, ref: key, detail: [type, approval].filter(Boolean).join(" · ") });
  }

  const g = obj(core.guardrails);
  if (g.input === true || g.output === true) {
    const sides = g.input === true && g.output === true ? "input and output" : g.input === true ? "input" : "output";
    const ref = typeof g.use === "string" && g.use ? g.use : typeof g.guardrailId === "string" && g.guardrailId ? undefined : "";
    out.push({ kind: "guardrail", name: guardrailName(wf, g), detail: `checks ${sides}`, ...(ref === undefined ? {} : { ref }) });
  }

  const m = obj(core.memory);
  const longTerm = m.longTerm === true ? ["semantic"] : strings(m.longTerm);
  if (longTerm.length || strings(m.custom).length || (typeof m.use === "string" && m.use)) {
    const named = typeof m.use === "string" && m.use ? m.use : "agent memory";
    const kinds = [...longTerm, ...strings(m.custom)];
    const scope = typeof m.scope === "string" && m.scope ? `per ${m.scope}` : "";
    out.push({ kind: "memory", name: named, detail: [kinds.join(", "), scope].filter(Boolean).join(" · ") || undefined,
      ...(typeof m.use === "string" && m.use ? { ref: m.use } : {}) });
  }

  for (const key of toolsOf(spec.tool)) {
    for (const p of strings(obj(wf.tools?.[key]).policies)) out.push({ kind: "policy", name: p, detail: `on ${key}`, tool: key, ref: p });
  }

  for (const s of strings(spec.skills)) out.push({ kind: "skill", name: s, ref: s });

  const ev = obj(core.evaluations);
  if (ev.enabled === true) {
    const list = strings(ev.evaluators);
    if (list.length) for (const e of list) out.push({ kind: "evaluation", ...evaluatorName(e), ref: e });
    else out.push({ kind: "evaluation", name: "Evaluations", detail: "no evaluator picked" });
  }

  for (const p of strings(obj(core.identity).outbound)) out.push({ kind: "identity", name: p, detail: "outbound credential", ref: p });

  const ask = spec.askPerson;
  if (ask === true || isObj(ask)) {
    const n = isObj(ask) && typeof ask.maxQuestions === "number" ? ask.maxQuestions : 3;
    out.push({ kind: "human", name: "Asks questions", detail: `up to ${n} question${n === 1 ? "" : "s"} a step` });
  }

  const order = new Map(ALL_KINDS.map((k, i) => [k, i]));
  return out.map((c, i) => ({ c, i })).sort((a, b) => (order.get(a.c.kind)! - order.get(b.c.kind)!) || a.i - b.i).map(({ c }) => c);
}

/** How many agents have at least one component of each kind, for the Show menu. */
export function kindCounts(wf: Workflow): Record<ComponentKind, number> {
  const out = Object.fromEntries(ALL_KINDS.map((k) => [k, 0])) as Record<ComponentKind, number>;
  for (const id of Object.keys(wf.agents ?? {})) {
    for (const k of new Set(componentsOf(wf, id).map((c) => c.kind))) out[k] += 1;
  }
  return out;
}
