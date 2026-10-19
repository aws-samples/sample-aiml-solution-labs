/** An agent waiting for a person mid-step (app/common/asks.py), on the run page.
 *
 *  A question shows the question and a box; the answer is sent as approve + comment, and
 *  Decline as deny. A tool approval shows the tool and its arguments AS TEXT: they come from
 *  a model, so markup in them must never become markup on the page. A review gate is
 *  unchanged: no `kind`, the gate's own form. */
import { act, fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { SessionSnapshot, Workflow } from "../types";
import { HitlGate } from "./HitlGate";

const WORKFLOW: Workflow = {
  agents: { triage: { name: "Triage" } },
  steps: [{ agent: "triage", hitl: true }],
};

function snap(hitl: SessionSnapshot["hitl"]): SessionSnapshot {
  return { session_id: "s1", topic: "t", overall: "waiting_human",
           nodes: { triage: { status: "waiting_human", output: "" } }, hitl };
}

function mount(s: SessionSnapshot, onDecide = vi.fn().mockResolvedValue(undefined)) {
  const { container } = render(
    <HitlGate snap={s} workflow={WORKFLOW} canDecide denyReason={null}
              onDecide={onDecide} onGroupDecide={vi.fn()} />,
  );
  return { onDecide, container };
}

describe("an agent's question", () => {
  const QUESTION = snap({ node: "triage", kind: "question", askId: "triage.v1.q1",
                          question: "Which region did the claimant mean?", timeoutAt: "2030-01-01T00:00:00Z" });

  it("shows the question and sends the answer as approve + comment", async () => {
    const { onDecide } = mount(QUESTION);
    expect(screen.getByText("Which region did the claimant mean?")).toBeTruthy();
    expect(screen.getByText(/carries on without one/)).toBeTruthy();
    const answer = screen.getByRole("button", { name: "Answer" });
    expect((answer as HTMLButtonElement).disabled).toBe(true);     // nothing typed yet
    fireEvent.change(screen.getByPlaceholderText("Answer the question"), { target: { value: "eu-west-1" } });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Answer" })); });
    expect(onDecide).toHaveBeenCalledWith("approve", "eu-west-1");
  });

  it("declines with deny", async () => {
    const { onDecide } = mount(QUESTION);
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Decline" })); });
    expect(onDecide).toHaveBeenCalledWith("deny", "");
  });

  it("does not offer a gate's Revise", () => {
    mount(QUESTION);
    expect(screen.queryByRole("button", { name: "Revise" })).toBeNull();
  });
});

describe("a tool call waiting for approval", () => {
  const ARGS = '{\n  "amount": 40,\n  "note": "<img src=x onerror=alert(1)>"\n}';
  const APPROVAL = snap({ node: "triage", kind: "toolApproval", askId: "triage.v1.t1",
                          tool: "payments___pay", args: ARGS, question: "Approve?" });

  it("shows the tool and its arguments as text, never as markup", () => {
    const { container } = mount(APPROVAL);
    expect(screen.getByText("payments___pay")).toBeTruthy();
    const pre = container.querySelector("pre");
    expect(pre?.textContent).toBe(ARGS);
    expect(container.querySelector("img")).toBeNull();
  });

  it("approves and denies through the decision route", async () => {
    const { onDecide } = mount(APPROVAL);
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Approve" })); });
    expect(onDecide).toHaveBeenLastCalledWith("approve", "");
    fireEvent.change(screen.getByPlaceholderText(/Why/), { target: { value: "too much" } });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Deny" })); });
    expect(onDecide).toHaveBeenLastCalledWith("deny", "too much");
  });
});

describe("a review gate", () => {
  it("is the gate's own form, as before", () => {
    mount(snap({ node: "triage", question: "Review" }));
    expect(screen.getByRole("button", { name: "Revise" })).toBeTruthy();
    expect(screen.getByText(/Review required/)).toBeTruthy();
  });
});
