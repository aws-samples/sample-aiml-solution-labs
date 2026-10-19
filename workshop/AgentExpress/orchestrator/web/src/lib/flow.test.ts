/** The flow event model and the replay reducer: what a row becomes, in what order, and what
 *  the run looks like after N of them. No component, no timer. */
import { describe, expect, it } from "vitest";
import {
  type FlowRow, buildEvents, eventsOf, initialReplay, railOf, replayAt, replayReducer,
  sentence, type Replay,
} from "./flow";

const row = (o: Partial<FlowRow>): FlowRow => ({
  agent_id: "researcher", kind: "llm", label: "claude", status: "ok", latency_ms: 100,
  ts: "2025-01-01 10:00:00", seq: 1, version: 1, mode: "bedrock", cost_usd: 0.001, ...o,
});
const names = (id: string) => ({ researcher: "the researcher", writer: "the writer" }[id] ?? id);

describe("building events", () => {
  it("is empty for an empty run, and for a missing list", () => {
    expect(buildEvents([])).toEqual([]);
    expect(buildEvents(undefined)).toEqual([]);
    expect(buildEvents(null)).toEqual([]);
    expect(railOf([])).toEqual([]);
  });

  it("turns one agent's rows into ordered events with a component, a status and a sentence", () => {
    const ev = buildEvents([
      row({ kind: "tool", label: "docs", mode: "gateway", ts: "2025-01-01 10:00:02", seq: 2 }),
      row({ kind: "llm", ts: "2025-01-01 10:00:01", seq: 1 }),
    ], names);
    expect(ev.map((e) => [e.index, e.kind, e.component, e.agent])).toEqual([
      [0, "llm", "Model", "researcher"],
      [1, "tool", "Gateway", "researcher"],
    ]);
    expect(ev[1].sentence).toBe("The researcher called `docs` through the Gateway.");
    expect(ev[1]).toMatchObject({ latencyMs: 100, costUsd: 0.001, tone: "ok", status: "ok" });
  });

  it("orders by ts, then seq, then arrival, and interleaves parallel agents on ts", () => {
    const ev = buildEvents([
      row({ agent_id: "writer", label: "w1", ts: "2025-01-01 10:00:03", seq: 9 }),
      row({ agent_id: "researcher", label: "r1", ts: "2025-01-01 10:00:01", seq: 5 }),
      row({ agent_id: "writer", label: "w0", ts: "2025-01-01 10:00:02", seq: 2 }),
      row({ agent_id: "researcher", label: "r2", ts: "2025-01-01 10:00:03", seq: 4 }),
      row({ agent_id: "researcher", label: "r3", ts: "2025-01-01 10:00:03", seq: 4 }),
    ]);
    expect(ev.map((e) => e.sentence.match(/`(\w+)`/)?.[1])).toEqual(["r1", "w0", "r2", "r3", "w1"]);
    expect(eventsOf(ev, "writer").map((e) => e.index)).toEqual([1, 4]);
  });

  it("sorts seq as a number, not as text", () => {
    const ev = buildEvents([
      row({ label: "ten", seq: "10" }), row({ label: "two", seq: 2 }),
    ]);
    expect(ev.map((e) => e.sentence.match(/`(\w+)`/)?.[1])).toEqual(["two", "ten"]);
  });

  it("keeps a row that has an unknown kind as a generic event", () => {
    const ev = buildEvents([row({ kind: "hologram", label: "x", status: "weird" })], names);
    expect(ev).toHaveLength(1);
    expect(ev[0]).toMatchObject({ kind: "other", component: "Other", tone: "note" });
    expect(ev[0].sentence).toBe("The researcher recorded a `hologram` event (x), status weird.");
    expect(buildEvents([{}])).toHaveLength(1);     // even a row with nothing in it
  });

  it("puts the runtime's own compute row on the run, not on an agent", () => {
    const [e] = buildEvents([row({ agent_id: "__session__", kind: "agentcore", label: "runtime-compute", latency_ms: 2400 })]);
    expect(e).toMatchObject({ agent: "", component: "Runtime" });
    expect(e.sentence).toBe("AgentCore Runtime was active for 2.4 s on this run.");
  });

  it("reads numbers that arrive as text, and survives ones that do not", () => {
    const [e] = buildEvents([row({ latency_ms: "250", cost_usd: "0.5", version: "2" }), row({ latency_ms: "abc", cost_usd: undefined })]);
    expect(e).toMatchObject({ latencyMs: 250, costUsd: 0.5, version: 2 });
    expect(buildEvents([row({ latency_ms: "abc", cost_usd: undefined })])[0]).toMatchObject({ latencyMs: 0, costUsd: 0 });
  });
});

describe("the sentence for a policy decision", () => {
  it("says Cedar allowed it", () => {
    expect(sentence(row({ kind: "policy", label: "search_documentation", status: "allowed" }), names))
      .toBe("Cedar allowed `search_documentation` for the researcher.");
  });
  it("says Cedar denied it, and never that it was allowed", () => {
    const s = sentence(row({ kind: "policy", label: "search_documentation", status: "denied" }), names);
    expect(s).toBe("Cedar denied `search_documentation` for the researcher, so the Gateway refused the call.");
    expect(s).not.toMatch(/allowed/);
    const [e] = buildEvents([row({ kind: "policy", status: "denied" })]);
    expect(e.tone).toBe("stop");
  });
  it("says a log-only decision was not enforced", () => {
    const s = sentence(row({ kind: "policy", label: "t", status: "log-only" }), names);
    expect(s).toMatch(/log-only/);
    expect(s).toMatch(/not enforced/);
  });
});

describe("the sentence for a guardrail", () => {
  it("says it blocked the input", () => {
    const s = sentence(row({ kind: "guardrail", label: "INPUT", status: "blocked" }), names);
    expect(s).toBe("The guardrail blocked the input to the researcher.");
    expect(buildEvents([row({ kind: "guardrail", label: "INPUT", status: "blocked" })])[0].tone).toBe("stop");
  });
  it("says it passed the output, and that it masked details", () => {
    expect(sentence(row({ kind: "guardrail", label: "OUTPUT", status: "passed" }), names))
      .toBe("The guardrail checked the output of the researcher and let it through.");
    expect(sentence(row({ kind: "guardrail", label: "OUTPUT", status: "anonymized" }), names))
      .toMatch(/masked sensitive details/);
  });
});

describe("the other sentences", () => {
  it("memory", () => {
    expect(sentence(row({ kind: "memory", label: "recall", status: "agentcore" }), names)).toMatch(/recalled/);
    expect(sentence(row({ kind: "memory", label: "store" }), names)).toMatch(/saved an insight/);
    expect(sentence(row({ kind: "memory", label: "recall-filtered" }), names)).toMatch(/off-topic/);
    expect(buildEvents([row({ kind: "memory", label: "recall", status: "agentcore" })])[0].tone).toBe("ok");
  });
  it("a failed tool call is a failure, not a call", () => {
    const [e] = buildEvents([row({ kind: "tool", label: "docs", status: "error", mode: "error" })], names);
    expect(e.sentence).toBe("The researcher's call to `docs` failed.");
    expect(e.tone).toBe("bad");
  });
  it("model, image and evaluation", () => {
    expect(sentence(row({ kind: "llm" }), names)).toMatch(/asked the model `claude`/);
    expect(sentence(row({ kind: "llm", status: "error" }), names)).toMatch(/failed/);
    expect(sentence(row({ kind: "image", label: "nova" }), names)).toMatch(/drew an image/);
    expect(sentence(row({ kind: "eval", label: "Builtin.Faithfulness" }), names)).toMatch(/scored the researcher/);
    expect(sentence(row({ kind: "eval", status: "error" }), names)).toMatch(/could not score/);
  });
  it("falls back to the id when no name is given", () => {
    expect(sentence(row({ kind: "policy", label: "t", status: "allowed" }))).toBe("Cedar allowed `t` for researcher.");
  });
});

describe("the rail", () => {
  it("counts events per component and how many were stopped or failed", () => {
    const ev = buildEvents([
      row({ kind: "tool", seq: 1 }), row({ kind: "tool", status: "error", seq: 2 }),
      row({ kind: "policy", status: "denied", seq: 3 }), row({ kind: "llm", seq: 4 }),
    ]);
    expect(railOf(ev)).toEqual([
      { component: "Model", count: 1, stopped: 0, failed: 0 },
      { component: "Gateway", count: 2, stopped: 0, failed: 1 },
      { component: "Policy", count: 1, stopped: 1, failed: 0 },
    ]);
  });
});

describe("the replay reducer", () => {
  const loaded = (total: number): Replay => replayReducer(initialReplay, { type: "events", total });

  it("starts at the end of the run, so a finished run shows everything", () => {
    expect(loaded(5)).toMatchObject({ at: 5, total: 5, playing: false });
    expect(loaded(0)).toMatchObject({ at: 0, total: 0 });
  });

  it("steps back and forward by exactly one and stops at both ends", () => {
    let s = loaded(3);
    s = replayReducer(s, { type: "back" });
    expect(s.at).toBe(2);
    s = replayReducer(replayReducer(replayReducer(s, { type: "back" }), { type: "back" }), { type: "back" });
    expect(s.at).toBe(0);
    s = replayReducer(s, { type: "forward" });
    expect(s.at).toBe(1);
    s = replayReducer(replayReducer(replayReducer(s, { type: "forward" }), { type: "forward" }), { type: "forward" });
    expect(s.at).toBe(3);
  });

  it("seeks to a position and clamps a position outside the run", () => {
    const s = loaded(4);
    expect(replayReducer(s, { type: "seek", to: 2 }).at).toBe(2);
    expect(replayReducer(s, { type: "seek", to: 99 }).at).toBe(4);
    expect(replayReducer(s, { type: "seek", to: -3 }).at).toBe(0);
    expect(replayReducer(s, { type: "seek", to: Number.NaN }).at).toBe(0);
  });

  it("plays one event per tick and stops by itself on the last", () => {
    let s = replayReducer(loaded(3), { type: "seek", to: 0 });
    s = replayReducer(s, { type: "play" });
    expect(s).toMatchObject({ at: 0, playing: true });
    s = replayReducer(s, { type: "tick" });
    expect(s).toMatchObject({ at: 1, playing: true });
    s = replayReducer(replayReducer(s, { type: "tick" }), { type: "tick" });
    expect(s).toMatchObject({ at: 3, playing: false });
    expect(replayReducer(s, { type: "tick" }).at).toBe(3);
  });

  it("plays a finished replay from the start", () => {
    const s = replayReducer(loaded(3), { type: "play" });
    expect(s).toMatchObject({ at: 0, playing: true });
  });

  it("ignores a tick while paused, and pauses on any manual move", () => {
    const paused = replayReducer(loaded(3), { type: "seek", to: 1 });
    expect(replayReducer(paused, { type: "tick" }).at).toBe(1);
    const playing = replayReducer(paused, { type: "play" });
    for (const a of [{ type: "pause" }, { type: "back" }, { type: "forward" }, { type: "seek", to: 2 }] as const) {
      expect(replayReducer(playing, a).playing).toBe(false);
    }
  });

  it("will not play an empty run", () => {
    expect(replayReducer(loaded(0), { type: "play" }).playing).toBe(false);
  });

  it("keeps the speed across steps, and changes it on its own", () => {
    const s = replayReducer(loaded(3), { type: "speed", speed: 2 });
    expect(s.speed).toBe(2);
    expect(replayReducer(s, { type: "back" }).speed).toBe(2);
  });

  it("follows a growing run when it was at the end, and stays put when it was not", () => {
    const atEnd = loaded(3);
    expect(replayReducer(atEnd, { type: "events", total: 5 }).at).toBe(5);
    const earlier = replayReducer(atEnd, { type: "seek", to: 1 });
    expect(replayReducer(earlier, { type: "events", total: 5 })).toMatchObject({ at: 1, total: 5 });
    expect(replayReducer(earlier, { type: "events", total: 0 })).toMatchObject({ at: 0, total: 0 });
  });
});

describe("the run at step N", () => {
  const ev = buildEvents([
    row({ agent_id: "researcher", seq: 1, ts: "2025-01-01 10:00:01" }),
    row({ agent_id: "writer", seq: 2, ts: "2025-01-01 10:00:02" }),
    row({ agent_id: "researcher", kind: "policy", status: "denied", seq: 3, ts: "2025-01-01 10:00:03" }),
    row({ agent_id: "writer", seq: 4, ts: "2025-01-01 10:00:04" }),
  ]);

  it("shows nothing before the first event", () => {
    const v = replayAt(ev, 0);
    expect(v.shown).toEqual([]);
    expect(v.current).toBeNull();
    expect(Object.values(v.agents).map((a) => a.phase)).toEqual(["pending", "pending"]);
  });

  it("shows exactly N events, the Nth being the current one (no off-by-one)", () => {
    for (const n of [1, 2, 3, 4]) {
      const v = replayAt(ev, n);
      expect(v.shown).toHaveLength(n);
      expect(v.current?.index).toBe(n - 1);
    }
  });

  it("marks an agent running while it has more to do, and finished when it has not", () => {
    expect(replayAt(ev, 1).agents.researcher).toEqual({ shown: 1, total: 2, phase: "running" });
    expect(replayAt(ev, 1).agents.writer).toEqual({ shown: 0, total: 2, phase: "pending" });
    const v = replayAt(ev, 3);
    expect(v.agents.researcher.phase).toBe("finished");
    expect(v.agents.writer.phase).toBe("running");
    expect(v.current?.tone).toBe("stop");
    expect(replayAt(ev, 4).agents.writer.phase).toBe("finished");
  });

  it("clamps a position outside the run", () => {
    expect(replayAt(ev, 99).shown).toHaveLength(4);
    expect(replayAt(ev, -1).shown).toHaveLength(0);
    expect(replayAt([], 3).current).toBeNull();
  });
});
