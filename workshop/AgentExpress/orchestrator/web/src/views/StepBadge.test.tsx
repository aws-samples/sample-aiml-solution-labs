/** Numbered steps in the run views (lib/stepNumbers.ts, StepBadge.tsx), and the deployed
 *  resource each component row names when it is opened (bff/resource_names.py). */
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { stepLabel, stepNumbers, stepTone } from "../lib/stepNumbers";
import type { FlowRow } from "../lib/flow";
import type { Workflow } from "../types";
import { Graph } from "./Graph";
import { NavPane } from "./NavPane";
import { RunFlow } from "./RunFlow";

const WF = {
  agents: {
    intake: { name: "Intake", components: [
      { kind: "guardrail", name: "strict", detail: "checks input",
        resources: [{ label: "Bedrock Guardrail", value: "demo-gr-strict" }, { label: "Version", value: "DRAFT" }] },
      { kind: "memory", name: "notes" },
      { kind: "skill", name: "triage", resources: [] },
    ] },
    left: { name: "Left" }, right: { name: "Right" }, report: { name: "Report" },
  },
  steps: [{ agent: "intake" }, { parallel: ["left", "right"], gateId: "g" }, { agent: "report" }],
} as unknown as Workflow;

describe("step numbers", () => {
  it("number stages in order, and letter the agents of a stage of several", () => {
    expect(stepNumbers(WF.steps)).toEqual({ intake: "1", left: "2a", right: "2b", report: "3" });
    expect(stepNumbers([{ sequence: ["a", "b"] }] as never)).toEqual({ a: "1a", b: "1b" });
    expect(stepNumbers(undefined)).toEqual({});
  });

  it("read as one phrase, with the status in words", () => {
    expect(stepLabel("2", "done")).toBe("Step 2, done");
    expect(stepLabel("2b", "waiting_human")).toBe("Step 2b, waiting for HITL");
    expect(stepLabel("4", undefined)).toBe("Step 4, not started");
    expect(["done", "running", "failed", "denied", "waiting_human", "pending", "skipped"].map(stepTone))
      .toEqual(["done", "running", "failed", "failed", "waiting", "pending", "stopped"]);
  });
});

describe("the run graph", () => {
  it("draws each step's number, coloured by status, where the monogram was", () => {
    render(<Graph workflow={WF} selected={null} onSelect={() => {}}
      nodes={{ intake: { status: "done" }, left: { status: "running" }, right: { status: "failed" } }} />);
    const badge = screen.getByRole("img", { name: "Step 1, done" });
    expect(badge.textContent).toBe("1");
    expect(badge.className).toContain("axs-done");
    expect(screen.getByRole("img", { name: "Step 2a, running" }).className).toContain("axs-running");
    expect(screen.getByRole("img", { name: "Step 2b, failed" }).className).toContain("axs-failed");
    expect(screen.getByRole("img", { name: "Step 3, not started" }).className).toContain("axs-pending");
    // The box itself is announced with its step.
    expect(screen.getByTestId("state-left").getAttribute("aria-label")).toBe("Left — Step 2a, running");
    expect(screen.queryByText("IN")).toBeNull();
  });

  it("names a component's deployed resource when its row is opened, and says when it cannot", () => {
    render(<Graph workflow={WF} nodes={{}} selected={null} onSelect={() => {}} events={[]} />);
    const row = (k: string, n: string) => screen.getByTestId(`run-comp-intake-${k}-${n}`);
    fireEvent.click(within(row("guardrail", "strict")).getByRole("button"));
    const dl = within(row("guardrail", "strict")).getByLabelText("strict as deployed");
    expect(dl.textContent).toContain("Bedrock Guardrail");
    expect(dl.textContent).toContain("demo-gr-strict");
    expect(dl.textContent).toContain("DRAFT");
    // A run recorded before resource names were: said, not left blank.
    fireEvent.click(within(row("memory", "notes")).getByRole("button"));
    expect(row("memory", "notes").textContent).toContain("Deployed resource: not available");
    // A skill is no AWS resource of its own: nothing to say.
    fireEvent.click(within(row("skill", "triage")).getByRole("button"));
    expect(row("skill", "triage").textContent).not.toContain("Deployed resource");
  });
});

describe("the Flow tab", () => {
  it("numbers the agents' boxes and each event mark", async () => {
    const rows: FlowRow[] = [
      { agent_id: "intake", kind: "tool", label: "docs", status: "ok", ts: "1", seq: 1 },
      { agent_id: "intake", kind: "policy", label: "x", status: "denied", ts: "2", seq: 2 },
    ];
    const load = vi.fn().mockResolvedValue({ session_id: "s", events: rows, capped: false, max: 2000 });
    render(<RunFlow sessionId="s" workflow={WF} nodes={{ intake: { status: "done" } }} load={load} defaultView="summary" />);
    await waitFor(() => expect(screen.queryByText(/Reading this run/)).toBeNull());
    expect(screen.getByRole("img", { name: "Step 1, done" })).toBeTruthy();
    expect(screen.getByRole("img", { name: "Step 2b, not started" })).toBeTruthy();
    fireEvent.click(within(screen.getByTestId("flow-agent-intake")).getAllByRole("button")[0]);
    expect(screen.getByTestId("mark-0").textContent).toBe("1");
    expect(screen.getByTestId("mark-1").textContent).toBe("2");
    expect(screen.getByTestId("mark-1").className).toContain("rf-tone-stop");
    expect(screen.getByTestId("mark-1").getAttribute("title")).toMatch(/^Event 2 \(stopped\): /);
  });
});

describe("the side navigation of a build's app", () => {
  const groups = [
    { id: "runs", text: "Runs", href: "#runs", entries: [{ href: "#run:a", text: "A" }] },
    { id: "obs", text: "Observability", href: "#obs", entries: [{ href: "#obs:a", text: "A" }] },
  ];
  it("lets Runs and Observability share the pane, and a collapsed one give its room back", () => {
    const { container } = render(<NavPane groups={groups} links={[{ text: "Files", href: "#files" }]} active="" onFollow={() => {}} fill />);
    expect(container.querySelector("nav")!.className).toBe("axn axn-fill");
    fireEvent.click(screen.getByRole("button", { name: "Collapse Runs" }));
    expect([...container.querySelectorAll(".axn-group")].map((g) => g.className))
      .toEqual(["axn-group axn-closed", "axn-group"]);
    expect(screen.getByRole("link", { name: "Files" })).toBeTruthy();
  });
  it("is unchanged where it is not asked for (the console's Build)", () => {
    const { container } = render(<NavPane groups={groups} links={[]} active="" onFollow={() => {}} />);
    expect(container.querySelector("nav")!.className).toBe("axn");
  });
});
