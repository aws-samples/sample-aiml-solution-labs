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
});
