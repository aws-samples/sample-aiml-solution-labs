/** The event model behind a run's Flow tab, and the replay of it. Pure functions only.
 *
 *  Source: the telemetry rows the runtime already writes for every metered step
 *  (app/features/observability/meter.py, records.py), served WITHOUT their prompt and
 *  answer text by GET /api/sessions/{id}/flow. Nothing new is stored. A row becomes a
 *  FlowEvent: the agent it belongs to, the AgentCore component it touched, how it went,
 *  and ONE plain sentence saying what happened. The sentences are the point of the tab:
 *  a reader who has never met AgentCore learns what Gateway, Memory, Cedar policy and
 *  Guardrails each do by reading what they did in THIS run.
 *
 *  Replay is a reducer over the ordered list, so "the state at step N" can be tested
 *  without a timer; the component's timer only dispatches `tick`.
 */

/** A row as GET /api/sessions/{id}/flow returns it. Every field may be missing on an old row. */
export interface FlowRow {
  agent_id?: string;
  kind?: string;
  label?: string;
  status?: string;
  latency_ms?: number | string;
  ts?: string;
  seq?: number | string;
  version?: number | string;
  mode?: string;
  cost_usd?: number | string;
}

export interface FlowResponse {
  session_id: string;
  events: FlowRow[];
  /** True when the run had more rows than the route returns (it keeps the earliest). */
  capped: boolean;
  max: number;
}

/** The AgentCore piece an event belongs to. `Identity` is absent on purpose: no row kind
 *  records an identity exchange today, and a component with no data would only be a label. */
export type Component =
  | "Model" | "Gateway" | "Memory" | "Policy" | "Guardrails" | "Runtime" | "Evaluations" | "HITL" | "Other";

/** The order the rail lists components in: the ones a run touches most often come first. */
export const COMPONENTS: Component[] = [
  "Model", "Gateway", "Policy", "Guardrails", "Memory", "Evaluations", "Runtime", "HITL", "Other",
];

/** How it went. `stop` is a control doing its job (a block or a denial); `bad` is a failure. */
export type Tone = "ok" | "note" | "stop" | "bad";

export interface FlowEvent {
  /** Position in the ordered list, from 0. Also the key. */
  index: number;
  /** The row's kind when the model knows it; "other" when it does not (still shown). */
  kind: string;
  agent: string;
  component: Component;
  /** The row's label: the tool, evaluator, guardrail side or memory operation it names. */
  label: string;
  status: string;
  tone: Tone;
  sentence: string;
  latencyMs: number;
  costUsd: number;
  ts: string;
  version: number;
}

const num = (v: unknown): number => {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

/** The rows that belong to the run as a whole rather than to an agent: runtime compute is
 *  written under `__session__`. */
const isRunLevel = (id: string) => id === "" || id.startsWith("__");

const COMPONENT_OF: Record<string, Component> = {
  llm: "Model", image: "Model", tool: "Gateway", policy: "Policy", guardrail: "Guardrails",
  memory: "Memory", eval: "Evaluations", agentcore: "Runtime",
  // An agent asking a person mid-step: a question, or a tool call to approve (app/common/asks.py).
  person: "HITL",
};

function secs(ms: number): string {
  return ms >= 1000 ? `${(ms / 1000).toFixed(1)} s` : `${Math.round(ms)} ms`;
}

/** `nameOf` turns an agent id into what the reader calls it; the default is the id. */
export type NameOf = (agentId: string) => string;

function toneOf(kind: string, status: string): Tone {
  if (status === "error") return "bad";
  if (status === "blocked" || status === "denied" || status === "unanswered") return "stop";
  if (status === "anonymized" || status === "log-only") return "note";
  return kind in COMPONENT_OF ? "ok" : "note";
}

/** ONE sentence for one row. Every branch names the AgentCore piece and says what it did,
 *  because that is what a first-time reader is here to learn. */
export function sentence(row: FlowRow, nameOf: NameOf = (id) => id): string {
  const kind = String(row.kind ?? "");
  const label = String(row.label ?? "");
  const status = String(row.status ?? "");
  const id = String(row.agent_id ?? "");
  const who = isRunLevel(id) ? "the run" : nameOf(id);
  const q = label ? `\`${label}\`` : "a tool";
  switch (kind) {
    case "policy":
      if (status === "denied") {
        return `Cedar denied ${q} for ${who}, so the Gateway refused the call.`;
      }
      if (status === "log-only") {
        return `Cedar checked ${q} for ${who} in log-only mode: the decision went to CloudWatch and was not enforced.`;
      }
      if (status === "allowed") return `Cedar allowed ${q} for ${who}.`;
      return `Cedar evaluated ${q} for ${who} (${status || "no decision recorded"}).`;
    case "guardrail": {
      const side = label.toUpperCase() === "OUTPUT" ? "output" : label.toUpperCase() === "INPUT" ? "input" : "";
      const what = side === "output" ? `the output of ${who}` : side === "input" ? `the input to ${who}` : `text for ${who}`;
      if (status === "blocked") return `The guardrail blocked ${what}.`;
      if (status === "anonymized") return `The guardrail masked sensitive details in ${what} and let it through.`;
      if (status === "passed") return `The guardrail checked ${what} and let it through.`;
      return `The guardrail checked ${what} (${status || "no result recorded"}).`;
    }
    case "tool":
      if (status === "error") return `${cap(who)}'s call to ${q} failed.`;
      return String(row.mode ?? "") === "gateway"
        ? `${cap(who)} called ${q} through the Gateway.`
        : `${cap(who)} called ${q}.`;
    case "memory":
      // `status` on a memory row repeats the mode, so an error cannot be told from success here.
      if (label === "recall") return `${cap(who)} recalled what long-term memory holds about this topic.`;
      if (label === "store") return `${cap(who)} saved an insight to long-term memory.`;
      if (label === "recall-filtered") {
        return `Memory returned more than ${who} used: recalled insights that were off-topic were dropped.`;
      }
      return `${cap(who)} used long-term memory${label ? ` (${label})` : ""}.`;
    case "llm":
      return status === "error"
        ? `The model call to ${q} for ${who} failed.`
        : `${cap(who)} asked the model ${q} and got an answer.`;
    case "image":
      return status === "error"
        ? `The image model ${q} failed for ${who}.`
        : `${cap(who)} drew an image with ${q}.`;
    case "eval":
      return status === "error"
        ? `AgentCore Evaluations could not score ${who} with ${q}.`
        : `AgentCore Evaluations scored ${who} with ${q}.`;
    case "agentcore":
      return `AgentCore Runtime was active for ${secs(num(row.latency_ms))} on this run.`;
    case "person":
      if (label === "question") {
        if (status === "asked") return `${cap(who)} paused to ask HITL a question.`;
        if (status === "answered") return `HITL answered ${who}'s question, and it carried on.`;
        return `Nobody answered ${who}'s question, so it carried on without an answer.`;
      }
      if (status === "waiting") return `${cap(who)} asked HITL to approve a call to ${q}.`;
      if (status === "approved") return `HITL approved ${who}'s call to ${q}.`;
      return `${cap(who)}'s call to ${q} was not approved, so it was not made.`;
    default:
      // A kind this version does not know is still an event: shown, not dropped.
      return `${cap(who)} recorded a \`${kind || "unknown"}\` event${label ? ` (${label})` : ""}${status ? `, status ${status}` : ""}.`;
  }
}

function cap(s: string): string {
  return s ? s.charAt(0).toUpperCase() + s.slice(1) : s;
}

/** Rows to events, oldest first: by `ts`, then `seq`, and the order received for a tie
 *  (the route returns them in table order, which already follows ts). Parallel agents
 *  interleave because `ts` is the only clock they share. */
export function buildEvents(rows: FlowRow[] | undefined | null, nameOf: NameOf = (id) => id): FlowEvent[] {
  const list = (rows ?? []).map((row, received) => ({ row, received }));
  list.sort((a, b) =>
    String(a.row.ts ?? "").localeCompare(String(b.row.ts ?? ""))
    || num(a.row.seq) - num(b.row.seq)
    || a.received - b.received);
  return list.map(({ row }, index) => {
    const kind = String(row.kind ?? "");
    const known = kind in COMPONENT_OF;
    const status = String(row.status ?? "");
    return {
      index,
      kind: known ? kind : "other",
      agent: isRunLevel(String(row.agent_id ?? "")) ? "" : String(row.agent_id),
      component: known ? COMPONENT_OF[kind] : "Other",
      label: String(row.label ?? ""),
      status,
      tone: toneOf(kind, status),
      sentence: sentence(row, nameOf),
      latencyMs: num(row.latency_ms),
      costUsd: num(row.cost_usd),
      ts: String(row.ts ?? ""),
      version: num(row.version),
    };
  });
}

/** Events of one agent, in order. */
export const eventsOf = (events: FlowEvent[], agent: string): FlowEvent[] =>
  events.filter((e) => e.agent === agent);

/** Per component: how many events, and how many were a block, a denial or a failure. */
export function railOf(events: FlowEvent[]): { component: Component; count: number; stopped: number; failed: number }[] {
  return COMPONENTS.map((component) => {
    const mine = events.filter((e) => e.component === component);
    return {
      component,
      count: mine.length,
      stopped: mine.filter((e) => e.tone === "stop").length,
      failed: mine.filter((e) => e.tone === "bad").length,
    };
  }).filter((r) => r.count > 0);
}

// --- replay ---------------------------------------------------------------------------

export type Speed = 1 | 2;

/** `at` is how many events have happened: 0 is before the first, `total` is the whole run. */
export interface Replay {
  at: number;
  total: number;
  playing: boolean;
  speed: Speed;
}

export type ReplayAction =
  | { type: "events"; total: number }
  | { type: "play" }
  | { type: "pause" }
  | { type: "back" }
  | { type: "forward" }
  | { type: "seek"; to: number }
  | { type: "speed"; speed: Speed }
  | { type: "tick" };

export const initialReplay: Replay = { at: 0, total: 0, playing: false, speed: 1 };

const clamp = (n: number, total: number) => Math.max(0, Math.min(total, Math.trunc(n) || 0));

export function replayReducer(s: Replay, a: ReplayAction): Replay {
  switch (a.type) {
    case "events": {
      // A run that grows while it is open: a viewer sitting at the end stays at the end.
      const total = Math.max(0, Math.trunc(a.total) || 0);
      const following = s.at >= s.total;
      return { ...s, total, at: following ? total : Math.min(s.at, total), playing: s.playing && total > 0 };
    }
    case "play":
      if (s.total === 0) return s;
      // Pressing play on a finished replay starts it again rather than doing nothing.
      return { ...s, playing: true, at: s.at >= s.total ? 0 : s.at };
    case "pause":
      return { ...s, playing: false };
    case "back":
      return { ...s, playing: false, at: clamp(s.at - 1, s.total) };
    case "forward":
      return { ...s, playing: false, at: clamp(s.at + 1, s.total) };
    case "seek":
      return { ...s, playing: false, at: clamp(a.to, s.total) };
    case "speed":
      return { ...s, speed: a.speed };
    case "tick": {
      if (!s.playing) return s;
      const at = clamp(s.at + 1, s.total);
      return { ...s, at, playing: at < s.total };
    }
  }
}

/** How far one agent has got at a point in the replay. */
export type Phase = "pending" | "running" | "finished";

export interface ReplayView {
  /** Events that have happened by now, oldest first. */
  shown: FlowEvent[];
  /** The latest event, or null before the first. */
  current: FlowEvent | null;
  /** Per agent id: events shown, events in the whole run, and its phase. */
  agents: Record<string, { shown: number; total: number; phase: Phase }>;
}

/** The state of the run after `at` events. A pure function of the list and `at`. */
export function replayAt(events: FlowEvent[], at: number): ReplayView {
  const n = clamp(at, events.length);
  const shown = events.slice(0, n);
  const agents: ReplayView["agents"] = {};
  for (const e of events) {
    const a = (agents[e.agent] ??= { shown: 0, total: 0, phase: "pending" });
    a.total += 1;
  }
  for (const e of shown) agents[e.agent].shown += 1;
  for (const a of Object.values(agents)) {
    a.phase = a.shown === 0 ? "pending" : a.shown < a.total ? "running" : "finished";
  }
  return { shown, current: n > 0 ? events[n - 1] : null, agents };
}
