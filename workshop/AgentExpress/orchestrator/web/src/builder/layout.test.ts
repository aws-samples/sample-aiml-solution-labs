/** The canvas's geometry: a drop always lands on SOMETHING, and on the right thing. */

import { describe, expect, it } from "vitest";

import { AGENT_H, AGENT_W, COMP_ROW, hitTest, layout } from "./layout";
import type { Workflow } from "./model";

const WF: Workflow = {
  agents: { a: { name: "A" }, b: { name: "B" }, c: { name: "C" } },
  tools: {},
  steps: [{ agent: "a" }, { parallel: ["b", "c"], gateId: "review" }],
};

describe("layout", () => {
  const l = layout(WF);

  it("stacks stages top to bottom and lays a group's members side by side", () => {
    expect(l.stages.map((s) => [s.name, s.kind, s.members])).toEqual([
      ["a", "single", ["a"]], ["review", "parallel", ["b", "c"]]]);
    expect(l.stages[1].y).toBeGreaterThan(l.stages[0].y + l.stages[0].h);
    const [b, c] = l.agents.filter((x) => x.stage === 1);
    expect(c.x).toBeGreaterThan(b.x + AGENT_W);
    expect(l.end.y).toBeGreaterThan(l.stages[1].y);
  });

  it("makes an agent one row taller per component it shows, and still hits it at its last row", () => {
    const rich: Workflow = {
      agents: { a: { name: "A", tool: ["t1", "t2"], agentcore: { guardrails: { input: true } } }, b: { name: "B" } },
      tools: { t1: { type: "mcp" }, t2: { type: "kb" } },
      steps: [{ parallel: ["a", "b"] }],
    };
    const full = layout(rich);
    const a = full.agents.find((x) => x.id === "a")!;
    const b = full.agents.find((x) => x.id === "b")!;
    expect(b.h).toBe(AGENT_H);
    expect(a.h).toBe(AGENT_H + 3 * COMP_ROW + 8);
    expect(full.stages[0].h).toBeGreaterThanOrEqual(a.h);
    const s = full.stages[0];
    expect(hitTest(full, s.x + a.x + 5, s.y + a.y + a.h - 4)).toEqual({ kind: "agent", id: "a" });
    expect(layout(rich, false, ["tool"]).agents.find((x) => x.id === "a")!.h).toBe(AGENT_H + 2 * COMP_ROW + 8);
    const editable = layout(rich, true);
    expect(editable.agents.find((x) => x.id === "a")!.h).toBe(AGENT_H + 4 * COMP_ROW + 8);   // + Add row
    expect(editable.agents.find((x) => x.id === "b")!.h).toBe(AGENT_H + COMP_ROW + 8);       // + Add on an empty agent too
  });

  it("hits an agent first, then its stage, then the nearest gap", () => {
    const s = l.stages[1];
    const b = l.agents.find((x) => x.id === "b")!;
    expect(hitTest(l, s.x + b.x + 5, s.y + b.y + AGENT_H / 2)).toEqual({ kind: "agent", id: "b" });
    expect(hitTest(l, s.x + 2, s.y + 2)).toEqual({ kind: "stage", index: 1 });
    expect(hitTest(l, 0, -500)).toEqual({ kind: "gap", index: 0 });
    expect(hitTest(l, 0, l.stages[0].y + l.stages[0].h + 10)).toEqual({ kind: "gap", index: 1 });
    expect(hitTest(l, 0, 99999)).toEqual({ kind: "gap", index: 2 });
  });
});
