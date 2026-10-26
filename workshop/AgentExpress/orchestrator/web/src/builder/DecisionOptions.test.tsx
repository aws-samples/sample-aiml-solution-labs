/** v139 Track D: the author defines named options in the control plane.
 *
 *  A stage's gate panel ("How the gate decides") writes `hitl.options`, one per line, and
 *  emptying it goes back to a plain gate. A parallel stage does not offer it. The agent panel's
 *  Ask person setting writes `askPerson.options`. Both are checked by the validator. */
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { afterEach, describe, expect, it } from "vitest";
import type { Selection } from "./Canvas";
import { Inspector } from "./Inspector";
import type { Project, Workflow } from "./model";
import { validate } from "./validate";

afterEach(cleanup);

let latest: Project;
function Harness({ initial, selection }: { initial: Project; selection: Selection }) {
  const [p, setP] = useState(initial);
  latest = p;
  return <Inspector project={p} selection={selection} issues={validate(p.workflow)} onChange={setP} onSelect={() => {}} />;
}

const project = (workflow: Workflow): Project =>
  ({ id: "p1", name: "P", createdAt: "", updatedAt: "", prompts: {}, workflow });

const GATED: Workflow = {
  orchestrator: {}, tools: {},
  agents: { check: { name: "Check", runtime: "main" }, deliver: { name: "Deliver", runtime: "main" } },
  steps: [{ agent: "check", hitl: true }, { agent: "deliver" }],
} as unknown as Workflow;

async function inspect(workflow: Workflow, selection: Selection) {
  await act(async () => { render(<Harness initial={project(workflow)} selection={selection} />); });
}

describe("a stage's gate panel", () => {
  it("writes the named options, one per line, and clears them back to a plain gate", async () => {
    await inspect(GATED, { kind: "stage", index: 0 });
    const box = screen.getByLabelText("Named options");
    await act(async () => { fireEvent.change(box, { target: { value: "Override\n\n Abort \n" } }); });
    expect(latest.workflow.steps[0].hitl).toEqual({ options: ["Override", "Abort"] });
    await act(async () => { fireEvent.change(box, { target: { value: "" } }); });
    expect(latest.workflow.steps[0].hitl).toBe(true);
  });

  it("keeps the gate's other settings", async () => {
    const wf = { ...GATED, steps: [{ agent: "check", hitl: { timeout: { after: "24h", action: "deny" } } }, { agent: "deliver" }] };
    await inspect(wf as unknown as Workflow, { kind: "stage", index: 0 });
    await act(async () => { fireEvent.change(screen.getByLabelText("Named options"), { target: { value: "Override\nAbort" } }); });
    expect(latest.workflow.steps[0].hitl).toEqual({ timeout: { after: "24h", action: "deny" }, options: ["Override", "Abort"] });
    // Nothing routes on the choice yet: the validator says so.
    expect(validate(latest.workflow).some((i) => i.path === "steps[0].hitl.options" && i.severity === "warning")).toBe(true);
  });

  it("is not offered on a parallel stage", async () => {
    const wf = { orchestrator: {}, tools: {}, agents: { a: { name: "A" }, b: { name: "B" } },
                 steps: [{ parallel: ["a", "b"], hitl: true, gateId: "g", gateName: "G" }] };
    await inspect(wf as unknown as Workflow, { kind: "stage", index: 0 });
    expect(screen.queryByLabelText("Named options")).toBeNull();
  });
});

describe("an agent's Ask person setting", () => {
  it("writes the decision options under askPerson", async () => {
    const wf = { ...GATED, agents: { ...GATED.agents, check: { name: "Check", runtime: "main", toolMode: "model", askPerson: true } } };
    await inspect(wf as unknown as Workflow, { kind: "agent", id: "check" });
    const box = screen.getByLabelText("Decision options");
    await act(async () => { fireEvent.change(box, { target: { value: "Load images\nContinue" } }); });
    expect(latest.workflow.agents.check.askPerson).toEqual({ options: ["Load images", "Continue"] });
    await act(async () => { fireEvent.change(box, { target: { value: "" } }); });
    expect(latest.workflow.agents.check.askPerson).toBe(true);
  });
});
