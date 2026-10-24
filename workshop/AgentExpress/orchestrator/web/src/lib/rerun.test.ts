import { describe, expect, it } from "vitest";
import type { Step } from "../types";
import { rerunBlocked } from "./rerun";

/** The shape that showed the defect: a step right after an ungated parallel stage was
 *  offered "Re-run from", the runtime refused it (graph_builder._predecessor_of_step) and
 *  the run did not change. */
const STEPS: Step[] = [
  { agent: "intake", branch: { when: [] } },
  { agent: "fraud" },
  { parallel: ["a", "b"], gateId: "gather" },
  { agent: "drafter" },
  { parallel: ["reviewer", "compliance"], hitl: true },
  { agent: "report" },
  { sequence: ["s1", "s2"] },
  { agent: "last" },
];

describe("rerunBlocked", () => {
  it("refuses the step after an ungated parallel stage, as the runtime does", () => {
    expect(rerunBlocked(STEPS, "drafter")).toMatch(/several agents in parallel with no review gate/);
  });

  it("allows every step the runtime can rewind to", () => {
    for (const id of ["intake", "fraud", "a", "b", "reviewer", "compliance", "report", "s1", "s2", "last"]) {
      expect(rerunBlocked(STEPS, id), id).toBeUndefined();
    }
  });

  it("leaves an unknown step and a workflow with no steps alone", () => {
    expect(rerunBlocked(STEPS, "nobody")).toBeUndefined();
    expect(rerunBlocked(undefined, "drafter")).toBeUndefined();
  });
});
