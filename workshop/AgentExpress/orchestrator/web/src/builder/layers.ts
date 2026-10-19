/** What an agent has switched on, read from its own config, for the Builder canvas's badges.
 *
 *  Nothing is stored or computed beyond the config: an agent shows a badge exactly when the
 *  key that turns the capability on is set on it. The canvas draws none of them until the
 *  reader picks a layer from its "Show" menu, so the default canvas is unchanged. */

export type LayerId = "guardrails" | "memory" | "policy" | "skills" | "evaluations" | "identity";

export interface Layer {
  id: LayerId;
  label: string;
  /** One letter on the badge; the label is its accessible name, so a letter is never alone. */
  letter: string;
  /** What switching it on means, for the tooltip. */
  hint: string;
}

export const LAYERS: Layer[] = [
  { id: "guardrails", label: "Guardrails", letter: "G", hint: "Checks this agent's input or output with a Bedrock guardrail" },
  { id: "memory", label: "Memory", letter: "M", hint: "Remembers across runs (AgentCore Memory)" },
  { id: "policy", label: "Cedar policy", letter: "P", hint: "Makes an app-level Cedar policy check" },
  { id: "skills", label: "Skills", letter: "S", hint: "May use skills (SKILL.md know-how)" },
  { id: "evaluations", label: "Evaluations", letter: "E", hint: "Has its answers scored by AgentCore Evaluations" },
  { id: "identity", label: "Identity", letter: "I", hint: "May get an outbound credential from AgentCore Identity" },
];

type Spec = Record<string, unknown> | undefined | null;
const obj = (v: unknown): Record<string, unknown> =>
  (v && typeof v === "object" && !Array.isArray(v) ? v as Record<string, unknown> : {});
const nonEmpty = (v: unknown): boolean => (Array.isArray(v) ? v.length > 0 : Boolean(v));
/** For keys whose value is a list: a string there is a mistake the validator reports, not a layer. */
const filled = (v: unknown): boolean => Array.isArray(v) && v.length > 0;

/** The layers this agent's config turns on, in LAYERS order. */
export function layersOf(spec: Spec): LayerId[] {
  const s = obj(spec);
  const ac = obj(s.agentcore);
  const memory = obj(ac.memory);
  const guardrails = obj(ac.guardrails);
  const on: Record<LayerId, boolean> = {
    guardrails: guardrails.input === true || guardrails.output === true,
    memory: nonEmpty(memory.longTerm) || nonEmpty(memory.custom) || nonEmpty(memory.use),
    policy: obj(ac.policy).enabled === true,
    skills: filled(s.skills),
    evaluations: obj(ac.evaluations).enabled === true,
    identity: filled(obj(ac.identity).outbound),
  };
  return LAYERS.filter((l) => on[l.id]).map((l) => l.id);
}

/** How many agents use each layer, so the menu can say which ones would show anything. */
export function layerCounts(agents: Record<string, Spec>): Record<LayerId, number> {
  const out = Object.fromEntries(LAYERS.map((l) => [l.id, 0])) as Record<LayerId, number>;
  for (const spec of Object.values(agents ?? {})) for (const id of layersOf(spec)) out[id] += 1;
  return out;
}
