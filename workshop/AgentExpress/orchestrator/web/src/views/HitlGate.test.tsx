/** The parallel review gate submits a decision for EVERY agent in the stage.
 *
 *  The reported defect: on a five-agent gate, pressing "Submit decisions" did nothing.
 *  Not an error, nothing. The cause was that the screen defaults each row to Approve but
 *  the component only sent the rows a reviewer had CHANGED — so a reviewer who agreed
 *  with all five had changed nothing and the request body was `{"decisions": {}}`. An
 *  empty map is falsy, and no single `decision` was supplied either, so the BFF rejected
 *  it with a 400 (bff/handler.py) and the run stayed parked at the gate.
 *
 *  The fix is one line, which is exactly why it needs a test: the next person to
 *  refactor this component cannot see the empty-map rule from here. */

import { act, fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { SessionSnapshot, Workflow } from "../types";
import { HitlGate } from "./HitlGate";

const IDS = ["knowledge_research", "web_search", "documentation_search",
             "cost_research", "lifecycle_research"];

const WORKFLOW: Workflow = {
  agents: Object.fromEntries(IDS.map((id) => [id, { name: id.replace(/_/g, " ") }])),
  steps: [
    { parallel: IDS, hitl: true, gateId: "research", gateName: "Research" },
  ],
};

const SNAP: SessionSnapshot = {
  session_id: "s1",
  topic: "Design a serverless data pipeline on AWS",
  overall: "waiting_human",
  nodes: Object.fromEntries(
    IDS.map((id) => [id, { status: "done" as const, output: "{}" }])),
  hitl: { node: "research" },
};

function mount(onGroupDecide: ReturnType<typeof vi.fn>) {
  render(
    <HitlGate
      snap={SNAP}
      workflow={WORKFLOW}
      canDecide
      denyReason={null}
      onDecide={vi.fn()}
      onGroupDecide={onGroupDecide}
    />,
  );
}

describe("a parallel review gate", () => {
  it("sends a decision for every agent when the reviewer changes nothing", async () => {
    const onGroupDecide = vi.fn().mockResolvedValue(undefined);
    mount(onGroupDecide);

    // `await act` because Submit is async: it awaits the handler and then clears the
    // busy flag, and that second state update happens after the click returns.
    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: /submit decisions/i }));
    });

    expect(onGroupDecide).toHaveBeenCalledTimes(1);
    const [per] = onGroupDecide.mock.calls[0] as [Record<string, { decision: string }>];
    // All five, not none. This is the assertion the bug would fail.
    expect(Object.keys(per).sort()).toEqual([...IDS].sort());
    // And the default the screen showed is the default that gets sent.
    expect(Object.values(per).map((d) => d.decision)).toEqual(IDS.map(() => "approve"));
  });

  it("offers a per-agent decision control for each agent in the stage", () => {
    mount(vi.fn());
    for (const id of IDS) {
      expect(screen.getAllByText(id.replace(/_/g, " ")).length).toBeGreaterThan(0);
    }
  });
});
