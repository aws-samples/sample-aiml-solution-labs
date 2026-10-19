/** Live config (orchestrator.liveConfig; bff/liveconfig.py): the shapes the API returns, the
 *  run label, and the diff the Live config page shows before a version is saved. */

/** The settings a version may change, in the order the page shows them (bff/liveconfig.py FIELDS). */
export const LIVE_FIELDS = ["systemPrompt", "model", "temperature", "topP", "maxTokens", "stopSequences"] as const;
export type LiveField = (typeof LIVE_FIELDS)[number];
export const FIELD_LABEL: Record<LiveField, string> = {
  systemPrompt: "System prompt", model: "Model", temperature: "Temperature", topP: "Top P",
  maxTokens: "Max tokens", stopSequences: "Stop sequences",
};

export type AgentOverlay = Partial<Record<LiveField, unknown>>;
/** agent id -> what this version changes for it. */
export type Overlay = Record<string, AgentOverlay>;

export interface LiveAgent {
  name: string;
  runtime: string;
  /** The deployed settings (version 0). The deployed prompt is in the image, not here. */
  deployed: AgentOverlay;
}
export interface LiveState {
  pointer: { active: number; at?: string; by?: string };
  agents: Record<string, LiveAgent>;
  promptMax: number;
  noteMax: number;
}
export interface VersionSummary {
  version: number;
  at: string;
  by: string;
  note: string;
  agents: string[];
}
export interface Version extends VersionSummary {
  overlay: Overlay;
}

/** "config v3" for a run on a live version; "" for one as deployed (0) or from before. */
export function configLabel(v: number | string | null | undefined): string {
  const n = typeof v === "string" ? Number(v) : v;
  return typeof n === "number" && Number.isInteger(n) && n > 0 ? `config v${n}` : "";
}

/** How a value reads in the diff: "(as deployed)" when the version does not set it. */
export function shown(v: unknown): string {
  if (v === undefined) return "(as deployed)";
  if (Array.isArray(v)) return v.length ? v.map(String).join(", ") : "(none)";
  return String(v);
}

export interface DiffRow {
  agent: string;
  field: LiveField;
  active: unknown;
  draft: unknown;
}

/** What `draft` changes against `active`, field by field, in a stable order. */
export function diff(active: Overlay, draft: Overlay): DiffRow[] {
  const agents = [...new Set([...Object.keys(active), ...Object.keys(draft)])].sort();
  const rows: DiffRow[] = [];
  for (const agent of agents) {
    for (const field of LIVE_FIELDS) {
      const a = active[agent]?.[field];
      const d = draft[agent]?.[field];
      if (JSON.stringify(a) !== JSON.stringify(d)) rows.push({ agent, field, active: a, draft: d });
    }
  }
  return rows;
}

/** The editor's text for one field, from a value. */
export function toText(field: LiveField, v: unknown): string {
  if (v === undefined || v === null) return "";
  if (field === "stopSequences") return Array.isArray(v) ? v.map(String).join("\n") : "";
  return String(v);
}

/** One field's value from the editor's text; undefined means "as deployed". Numbers that
 *  do not parse are kept as text, so the server's validator names the problem. */
export function fromText(field: LiveField, text: string): unknown {
  if (field === "systemPrompt") return text.trim() ? text : undefined;
  const t = text.trim();
  if (!t) return undefined;
  if (field === "stopSequences") return t.split("\n").map((s) => s.trim()).filter(Boolean);
  if (field === "model") return t;
  const n = Number(t);
  return Number.isFinite(n) ? n : t;
}

/** The overlay without empty agents, which the server refuses. */
export function compact(o: Overlay): Overlay {
  const out: Overlay = {};
  for (const [agent, fields] of Object.entries(o)) {
    const kept = Object.fromEntries(Object.entries(fields).filter(([, v]) => v !== undefined));
    if (Object.keys(kept).length) out[agent] = kept as AgentOverlay;
  }
  return out;
}
