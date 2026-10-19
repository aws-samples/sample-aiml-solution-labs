/** What each of an agent's components did in one run, for the run graph's component boxes.
 *
 *  The components come from the app's workflow (bff/workflow.py components(): kinds and
 *  names only); the events are the run's Flow events (lib/flow.ts). Each box gets the events
 *  that belong to it, and one short line: how many calls, how many were refused or failed. */
import type { FlowEvent } from "./flow";

export type RunComponentKind = "tool" | "guardrail" | "memory" | "policy" | "skill" | "evaluation" | "identity" | "human";

/** One component of an agent, as GET /api/workflow ships it. */
export interface RunComponent {
  kind: RunComponentKind | string;
  name: string;
  detail?: string;
  /** A tool's key, or the tool a policy is on. */
  tool?: string;
  /** An evaluator's id ("Builtin.Faithfulness"). */
  evaluator?: string;
  /** What it is deployed as in AWS, by name or id (bff/resource_names.py): the Gateway
   *  target, the guardrail and its version... [] for a kind with no resource of its own;
   *  absent on a run recorded before these were. */
  resources?: { label: string; value: string }[];
}
/** The kinds that are an AWS resource of their own, so a missing name is worth saying. */
export const RESOURCE_KINDS = new Set(["tool", "guardrail", "memory", "policy", "evaluation", "identity"]);

export const KIND_TAG: Record<string, string> = {
  tool: "Tool", guardrail: "Guardrail", memory: "Memory", policy: "Policy", skill: "Skill",
  evaluation: "Eval", identity: "Identity", human: "HITL",
};

/** A tool event's label names the tool by its key, or as the Gateway tool `<key>___<op>`. */
const isToolOf = (label: string, tool: string) =>
  label === tool || label.startsWith(`${tool}___`) || label.startsWith(`${tool}:`) || label.endsWith(`/${tool}`);

/** The events of `agent` that belong to component `c`. Skills and identities record none. */
export function eventsFor(c: RunComponent, agent: string, events: FlowEvent[]): FlowEvent[] {
  const mine = events.filter((e) => e.agent === agent);
  switch (c.kind) {
    case "tool": return mine.filter((e) => e.kind === "tool" && isToolOf(labelOf(e), c.tool ?? c.name));
    case "policy": return mine.filter((e) => e.kind === "policy" && (!c.tool || isToolOf(labelOf(e), c.tool)));
    case "guardrail": return mine.filter((e) => e.kind === "guardrail");
    case "memory": return mine.filter((e) => e.kind === "memory");
    case "evaluation": return mine.filter((e) => e.kind === "eval" && (!c.evaluator || labelOf(e) === c.evaluator || labelOf(e) === c.name));
    case "human": return mine.filter((e) => e.kind === "person");
    default: return [];
  }
}

const labelOf = (e: FlowEvent) => e.label ?? "";

export interface Outcome {
  /** One line for the box: "3 calls · 1 refused", "recalled, stored", "not used in this run". */
  line: string;
  /** The worst tone among its events: what colour the box's status takes. */
  tone: "none" | "ok" | "note" | "stop" | "bad";
  events: FlowEvent[];
}

const RANK = { none: 0, ok: 1, note: 2, stop: 3, bad: 4 } as const;

export function outcomeOf(c: RunComponent, agent: string, events: FlowEvent[] | null): Outcome {
  if (!events) return { line: "", tone: "none", events: [] };
  const list = eventsFor(c, agent, events);
  if (c.kind === "skill" || c.kind === "identity") {
    return { line: c.kind === "skill" ? "available to the agent" : "used when the agent calls its API", tone: "none", events: [] };
  }
  if (!list.length) return { line: "not used in this run", tone: "none", events: [] };
  const tone = list.reduce<Outcome["tone"]>((t, e) => (RANK[e.tone] > RANK[t] ? e.tone : t), "none");
  const stops = list.filter((e) => e.tone === "stop").length;
  const bad = list.filter((e) => e.tone === "bad").length;
  const n = list.length;
  const what = c.kind === "tool" ? `${n} call${n === 1 ? "" : "s"}`
    : c.kind === "policy" ? `${n} decision${n === 1 ? "" : "s"}`
      : c.kind === "guardrail" ? `${n} check${n === 1 ? "" : "s"}`
        : c.kind === "memory" ? [...new Set(list.map((e) => labelOf(e) || "used"))].join(", ")
          : c.kind === "evaluation" ? `${n} score${n === 1 ? "" : "s"}`
            : `${n} event${n === 1 ? "" : "s"}`;
  const stopWord = c.kind === "policy" ? "denied" : c.kind === "guardrail" ? "blocked" : c.kind === "human" ? "not approved" : "refused";
  return { line: [what, stops ? `${stops} ${stopWord}` : "", bad ? `${bad} failed` : ""].filter(Boolean).join(" · "), tone, events: list };
}
