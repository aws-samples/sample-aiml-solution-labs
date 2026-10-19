/** A run's model override (orchestrator.models.overrides; bff/model_override.py), as Start run
 *  builds it and the run page shows it. */
import type { ModelOverride } from "../types";

/** The override to send, or undefined when nothing was picked. "" means "the agent's own". */
export function overrideOf(all: string, perAgent: Record<string, string>): ModelOverride | undefined {
  const agents = Object.fromEntries(Object.entries(perAgent).filter(([, m]) => m));
  const out: ModelOverride = {
    ...(all ? { all } : {}),
    ...(Object.keys(agents).length ? { agents } : {}),
  };
  return Object.keys(out).length ? out : undefined;
}

/** One line for the run page: "all agents: X · Intake: Y". "" when the run had none. */
export function overrideLabel(o: ModelOverride | undefined, names: Record<string, string> = {}): string {
  if (!o) return "";
  const parts = [
    ...(o.all ? [`all agents: ${o.all}`] : []),
    ...Object.entries(o.agents ?? {}).map(([id, m]) => `${names[id] ?? id}: ${m}`),
  ];
  return parts.join(" · ");
}
