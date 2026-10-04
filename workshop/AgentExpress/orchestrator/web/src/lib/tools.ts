/** An agent's `tool` as a list. In workflow.json it is one tool key or a list of them;
 *  every screen that shows it wants the list, so this is the one place that reads
 *  either form. */
export function toolsOf(value: unknown): string[] {
  if (!value) return [];
  if (typeof value === "string") return [value];
  return Array.isArray(value) ? value.filter((t): t is string => typeof t === "string" && t !== "") : [];
}

/** "kb · reference, claimsDb" — the tools an agent reads, for a one-line label. */
export function toolsLabel(tool: unknown, corpus?: string): string {
  return toolsOf(tool).map((t, i) => (i === 0 && corpus ? `${t} · ${corpus}` : t)).join(", ");
}
