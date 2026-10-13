/** "v2" for a run or a deployment, "—" when the version is not known.
 *  Called "Workflow" in the UI, not "Version": a run's agents already have versions of
 *  their own (each re-run of an agent is the next one), shown in Observability. */
export function workflowVersionLabel(v: number | string | null | undefined): string {
  const n = Number(v);
  return v !== null && v !== undefined && v !== "" && Number.isInteger(n) && n > 0 ? `v${n}` : "—";
}
