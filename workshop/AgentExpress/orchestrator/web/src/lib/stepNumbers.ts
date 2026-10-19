/** Each step's number in a run, as the run views draw it: "1", "2", "3"... in the order of
 *  the workflow's stages, and a stage of several agents (parallel or sequence) gives each
 *  its stage number with a letter, "2a", "2b".
 *
 *  Letters rather than one shared number: the stage number stays the one the Table view's
 *  Stage column and the Builder show, and each box still has a label of its own, so
 *  "step 2b failed" names one agent. The order is the workflow's, not the order this run
 *  happened to reach them: a branch can skip a step, and a skipped step keeps its number. */
import type { Step } from "../types";

export function stepNumbers(steps: Step[] | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  (steps ?? []).forEach((s, i) => {
    const ids = s.parallel ?? s.sequence ?? (s.agent ? [s.agent] : []);
    ids.forEach((id, j) => {
      // 26 agents in one stage is far past anything the Builder lays out; past z the
      // letters would stop meaning anything, so the position is spelled out instead.
      if (!(id in out)) out[id] = ids.length === 1 ? String(i + 1) : `${i + 1}${j < 26 ? String.fromCharCode(97 + j) : `.${j + 1}`}`;
    });
  });
  return out;
}

/** How a screen reader hears a step's status after its number: "Step 2, done". */
const SPOKEN: Record<string, string> = {
  done: "done", running: "running", failed: "failed", denied: "denied", pending: "not started",
  waiting_human: "waiting for HITL", revise: "revising", cancelled: "cancelled", cancelling: "cancelling",
  skipped: "skipped",
};

export function stepLabel(num: string, status: string | undefined): string {
  const s = status ?? "pending";
  return `Step ${num}, ${SPOKEN[s] ?? s.replace(/_/g, " ")}`;
}

/** The badge's colour family: green done, blue running, red failed, amber waiting, grey
 *  not started, and a darker grey for a step that will not run. */
export function stepTone(status: string | undefined): "done" | "running" | "failed" | "waiting" | "pending" | "stopped" {
  switch (status ?? "pending") {
    case "done": return "done";
    case "running": case "revise": return "running";
    case "failed": case "denied": return "failed";
    case "waiting_human": case "cancelling": return "waiting";
    case "cancelled": case "skipped": return "stopped";
    default: return "pending";
  }
}
