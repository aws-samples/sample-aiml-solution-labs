import { describe, expect, it } from "vitest";

import type { SessionSnapshot, Workflow } from "../types";
import { endReason } from "./RunDetail";

const wf = { agents: { intake: { name: "Intake Agent" }, researcher: { name: "Researcher" } }, steps: [] } as unknown as Workflow;
const run = (overall: string, ...msgs: string[]): SessionSnapshot => ({
  session_id: "s1", topic: "t", overall,
  logs: msgs.map((msg, i) => ({ ts: `2026-10-08 10:00:0${i}`, msg })),
});

describe("why a run ended where it did", () => {
  it("names the branch rule that sent it to END, and what was skipped", () => {
    const r = endReason(run("done", "Intake started",
      "Branch after intake: destination equals '' -> END · skipping researcher, writer",
      "Workflow finished (done)"), wf);
    expect(r).toEqual({ type: "info", text: 'Ended early: after Intake Agent, the branch rule '
      + `"destination equals ''" sent the run to END, so Researcher, writer did not run.` });
  });

  it("names the branch's default when no rule matched", () => {
    expect(endReason(run("done", "Branch after intake: no rule matched, took `default` -> END · skipping researcher"), wf)?.text)
      .toBe("Ended early: after Intake Agent, the branch's default sent the run to END, so Researcher did not run.");
  });

  it("says nothing for a run that ran to its last step or took another branch", () => {
    expect(endReason(run("done", "Writer complete (v1)"), wf)).toBeNull();
    expect(endReason(run("done", "Branch after intake: days gt 14 -> review"), wf)).toBeNull();
    expect(endReason(run("running", "Branch after intake: x -> END"), wf)).toBeNull();
  });

  it("gives a failed run's guardrail block or agent error, newest first", () => {
    expect(endReason(run("failed", "Intake Agent blocked by guardrail: This request was blocked."), wf))
      .toEqual({ type: "error", text: "Intake Agent blocked by guardrail: This request was blocked." });
    expect(endReason(run("failed", "Writer failed: ModelUnavailable: throttled", "Workflow failed"), wf)?.text)
      .toBe("Writer failed: ModelUnavailable: throttled");
    expect(endReason(run("failed"), wf)?.text).toMatch(/Timeline tab/);
  });
  it("gives a guardrail block with its reasons, not the generic fallback", () => {
    // app/orchestrator/nodes.py writes the reasons in brackets before the message.
    const cases = [
      // An invoice agent stopped by a denied topic.
      "Invoice Checker blocked by guardrail (denied topic taxAdvice): Sorry, this request can't be handled.",
      // An onboarding agent stopped by two filters at once.
      "Onboarding Agent blocked by guardrail (content filter MISCONDUCT, word filter): This request was blocked.",
    ];
    for (const line of cases) {
      expect(endReason(run("failed", "Intake Agent started", line, "Workflow failed"), wf))
        .toEqual({ type: "error", text: line });
    }
    // A bracket in the message itself is not mistaken for the reasons.
    expect(endReason(run("failed", "Report Writer blocked by guardrail: Blocked (policy 7)."), wf)?.text)
      .toBe("Report Writer blocked by guardrail: Blocked (policy 7).");
  });
  it("gives a denied run's gate and the reviewer's note", () => {
    expect(endReason(run("denied", "Intake Agent complete (v1)",
      'Human decision (intake): deny \u2014 note: "Personal trip, not covered by the policy."',
      "intake denied; halting workflow", "Workflow finished (denied)"), wf))
      .toEqual({ type: "info", text: 'Denied at the Intake Agent review gate by a reviewer: "Personal trip, not covered by the policy."' });
    expect(endReason(run("denied", "Research review: deny"), wf)?.text)
      .toBe("Denied at the Research review gate by a reviewer. No note was given.");
    expect(endReason(run("denied"), wf)?.text).toMatch(/Timeline tab/);
  });
});
