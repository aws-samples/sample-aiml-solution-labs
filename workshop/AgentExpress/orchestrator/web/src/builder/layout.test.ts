/** The canvas's geometry: a drop always lands on SOMETHING, and on the right thing. */

import { describe, expect, it } from "vitest";

import { AGENT_H, AGENT_W, hitTest, layout } from "./layout";
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
