/** Whether a step can be the start of a single-step re-run, by the same rule the runtime
 *  applies (app/orchestrator/graph_builder.py `rerun_plan` / `_predecessor_of_step`).
 *
 *  A re-run rewinds the run to the node that routes INTO the step: the start, the gate
 *  or branch of the step before, or the single agent before it. A parallel stage with no
 *  review gate has no single node to rewind to, so the runtime refuses a re-run of the
 *  step right after it; offering the button there would do nothing. */
import type { Step } from "../types";

const agentsIn = (s: Step): string[] => s.parallel ?? s.sequence ?? (s.agent ? [s.agent] : []);

/** Why `agentId` cannot be re-run on its own in this workflow, or undefined when it can. */
export function rerunBlocked(steps: Step[] | undefined, agentId: string): string | undefined {
  const all = steps ?? [];
  const si = all.findIndex((s) => agentsIn(s).includes(agentId));
  if (si <= 0) return undefined;
  const step = all[si];
  // Mid-sequence: entered from the agent before it in the same sequence.
  if (step.sequence && step.sequence[0] !== agentId) return undefined;
  const prev = all[si - 1];
  if (prev.branch || prev.hitl || prev.sequence || agentsIn(prev).length === 1) return undefined;
  return "The step before this one runs several agents in parallel with no review gate, so there "
    + "is no single point to rewind to. Re-run from a step of that stage, or restart the workflow.";
}
