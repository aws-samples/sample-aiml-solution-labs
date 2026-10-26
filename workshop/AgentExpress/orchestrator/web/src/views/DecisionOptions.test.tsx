/** v139 Track D: a person chooses one of a run's named options on the run page.
 *
 *  A gate with `hitl.options` shows one button per option, labelled as the author wrote
 *  them, and an optional comment; a choice is sent as approve + the option. An agent's request
 *  for a decision (`hitl.kind` "decision") shows its question and the same buttons. A gate
 *  without options keeps approve / revise / deny. */
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { SessionSnapshot, Workflow } from "../types";
import { HitlGate } from "./HitlGate";

afterEach(cleanup);

const WORKFLOW: Workflow = {
  agents: { check: { name: "Check" }, parse: { name: "Parse" } },
  // The run page reads the options from the waiting run's `hitl`, not from the workflow.
  steps: [{ agent: "check", hitl: true }],
};

function snap(hitl: SessionSnapshot["hitl"]): SessionSnapshot {
  return { session_id: "s1", topic: "t", overall: "waiting_human",
           nodes: { check: { status: "waiting_human", output: '{"passed": false}' } }, hitl };
}

function mount(s: SessionSnapshot, canDecide = true) {
  const onDecide = vi.fn().mockResolvedValue(undefined);
  render(<HitlGate snap={s} workflow={WORKFLOW} canDecide={canDecide} denyReason={null}
                   onDecide={onDecide} onGroupDecide={vi.fn()} />);
  return onDecide;
}

describe("a gate with named options", () => {
  const GATE = snap({ node: "check", question: "Review Check", options: ["Override", "Abort"],
                      timeoutAt: "2030-01-01T00:00:00Z", timeoutAction: "approve" });

  it("shows one button per option and no approve, revise or deny", () => {
    mount(GATE);
    expect(screen.getByText("Decision required — Check")).toBeTruthy();
    expect(screen.getByRole("button", { name: "Override" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Abort" })).toBeTruthy();
    for (const name of ["Approve", "Revise", "Deny"]) expect(screen.queryByRole("button", { name })).toBeNull();
    expect(screen.getByText(/continues with no option chosen/)).toBeTruthy();
  });

  it("sends the chosen option with the comment", async () => {
    const onDecide = mount(GATE);
    fireEvent.change(screen.getByPlaceholderText(/Why, for the run's timeline/), { target: { value: "checked by hand" } });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Override" })); });
    expect(onDecide).toHaveBeenCalledWith("approve", "checked by hand", "Override");
  });

  it("sends a choice without a comment too", async () => {
    const onDecide = mount(GATE);
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Abort" })); });
    expect(onDecide).toHaveBeenCalledWith("approve", "", "Abort");
  });

  it("offers nothing to press without the decision permission", () => {
    mount(GATE, false);
    expect((screen.getByRole("button", { name: "Override" }) as HTMLButtonElement).disabled).toBe(true);
  });
});

describe("an agent's request for a decision", () => {
  const ASKED = snap({ node: "parse", kind: "decision", askId: "parse.v1.d1",
                       question: "Two images are missing. Load them, or continue?",
                       options: ["Load images", "Continue"] });

  it("shows the question and the options, and sends the choice", async () => {
    const onDecide = mount(ASKED);
    expect(screen.getByText("Parse needs a decision")).toBeTruthy();
    expect(screen.getByText("Two images are missing. Load them, or continue?")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Answer" })).toBeNull();
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Continue" })); });
    expect(onDecide).toHaveBeenCalledWith("approve", "", "Continue");
  });
});

describe("a gate without options", () => {
  it("is the gate's own approve / revise / deny form, as before", async () => {
    const onDecide = mount(snap({ node: "check", question: "Review" }));
    expect(screen.getByText(/Review required/)).toBeTruthy();
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Approve" })); });
    expect(onDecide).toHaveBeenCalledWith("approve", "");
  });
});
