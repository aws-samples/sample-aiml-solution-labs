/** Which attachment badges an agent gets: exactly the ones its own config turns on. */
import { describe, expect, it } from "vitest";
import { LAYERS, layerCounts, layersOf } from "./layers";

describe("layersOf", () => {
  it("is empty for an agent with nothing switched on, and for no agent at all", () => {
    expect(layersOf({ name: "A" })).toEqual([]);
    expect(layersOf({ agentcore: {} })).toEqual([]);
    expect(layersOf(undefined)).toEqual([]);
    expect(layersOf(null)).toEqual([]);
    expect(layersOf({ agentcore: "nonsense", skills: "nonsense" } as never)).toEqual([]);
  });

  it("reads guardrails from input or output being on, not from the block being there", () => {
    expect(layersOf({ agentcore: { guardrails: { input: true } } })).toEqual(["guardrails"]);
    expect(layersOf({ agentcore: { guardrails: { output: true } } })).toEqual(["guardrails"]);
    expect(layersOf({ agentcore: { guardrails: { input: false, output: false } } })).toEqual([]);
    expect(layersOf({ agentcore: { guardrails: {} } })).toEqual([]);
  });

  it("reads memory from long-term strategies, a custom one or a named memory", () => {
    expect(layersOf({ agentcore: { memory: { longTerm: true } } })).toEqual(["memory"]);
    expect(layersOf({ agentcore: { memory: { longTerm: ["semantic", "summary"] } } })).toEqual(["memory"]);
    expect(layersOf({ agentcore: { memory: { use: "shared" } } })).toEqual(["memory"]);
    expect(layersOf({ agentcore: { memory: { custom: { prompt: "x" } } } })).toEqual(["memory"]);
    expect(layersOf({ agentcore: { memory: { longTerm: [] } } })).toEqual([]);
    expect(layersOf({ agentcore: { memory: { scope: "user" } } })).toEqual([]);
  });

  it("reads policy, evaluations, identity and skills", () => {
    expect(layersOf({ agentcore: { policy: { enabled: true } } })).toEqual(["policy"]);
    expect(layersOf({ agentcore: { policy: { enabled: false } } })).toEqual([]);
    expect(layersOf({ agentcore: { evaluations: { enabled: true, auto: true } } })).toEqual(["evaluations"]);
    expect(layersOf({ agentcore: { evaluations: { auto: true } } })).toEqual([]);
    expect(layersOf({ agentcore: { identity: { outbound: ["github"] } } })).toEqual(["identity"]);
    expect(layersOf({ agentcore: { identity: { outbound: [] } } })).toEqual([]);
    expect(layersOf({ skills: ["triage"] })).toEqual(["skills"]);
    expect(layersOf({ skills: [] })).toEqual([]);
  });

  it("lists several at once in the menu's order", () => {
    const all = layersOf({
      skills: ["s"],
      agentcore: {
        identity: { outbound: ["i"] }, evaluations: { enabled: true }, policy: { enabled: true },
        memory: { longTerm: true }, guardrails: { input: true },
      },
    });
    expect(all).toEqual(LAYERS.map((l) => l.id));
  });
});

describe("layerCounts", () => {
  it("counts how many agents use each layer", () => {
    const counts = layerCounts({
      a: { agentcore: { guardrails: { input: true }, memory: { longTerm: true } } },
      b: { agentcore: { guardrails: { output: true } } },
      c: { name: "plain" },
    });
    expect(counts).toEqual({ guardrails: 2, memory: 1, policy: 0, skills: 0, evaluations: 0, identity: 0 });
    expect(layerCounts({})).toEqual({ guardrails: 0, memory: 0, policy: 0, skills: 0, evaluations: 0, identity: 0 });
  });
});
