/** One mapping from a run/node status onto Cloudscape's StatusIndicator.
 *
 *  Kept in one file because the status vocabulary appears in the graph, the runs
 *  table, the detail header and the split panel, and four copies would drift. The
 *  indicator pairs a shape with a colour, so state is never carried by hue alone. */

import StatusIndicator, {
  type StatusIndicatorProps,
} from "@cloudscape-design/components/status-indicator";

type Type = StatusIndicatorProps.Type;

const TYPES: Record<string, Type> = {
  done: "success",
  running: "in-progress",
  waiting_human: "pending",
  revise: "pending",
  cancelling: "pending",
  failed: "error",
  denied: "error",
  cancelled: "stopped",
  skipped: "stopped",
  pending: "pending",
};

/** The label a reviewer reads. `waiting_human` is the one worth rewording: the raw
 *  value describes the machine's state, not what the reader has to do. */
const LABELS: Record<string, string> = {
  waiting_human: "Awaiting review",
  done: "Succeeded",
  running: "Running",
  failed: "Failed",
  denied: "Denied",
  cancelled: "Cancelled",
  cancelling: "Cancelling",
  skipped: "Skipped",
  pending: "Not started",
  revise: "Revising",
};

export function statusType(status?: string): Type {
  return TYPES[status ?? "pending"] ?? "pending";
}

export function statusLabel(status?: string): string {
  const s = status ?? "pending";
  return LABELS[s] ?? s.replace(/_/g, " ");
}

export function statusIndicator(status?: string) {
  return <StatusIndicator type={statusType(status)}>{statusLabel(status)}</StatusIndicator>;
}

/** The run has stopped moving, so a re-run has a defined starting point. `waiting_human`
 *  counts: the run is parked at a gate and will not advance until someone acts, which is
 *  exactly when a reviewer wants to send a step back.
 *
 *  Lives beside the vocabulary it is drawn from because three screens ask the question —
 *  the graph, the step panel and the run page — and three copies of this list would
 *  disagree the first time a status was added. */
export function isSettled(overall?: string): boolean {
  return ["done", "denied", "failed", "waiting_human", "cancelled"]
    .includes(String(overall ?? ""));
}
