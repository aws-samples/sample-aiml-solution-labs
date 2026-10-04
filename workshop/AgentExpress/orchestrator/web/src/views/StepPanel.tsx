/** What goes in the SplitPanel when a step is selected: Details, Output and Actions,
 *  the way Step Functions splits a selected state's panel.
 *
 *  Version history lives under Output rather than in a tab of its own, because a
 *  revision is a version OF the output — and a reviewer comparing v1 with v2 wants
 *  them adjacent.
 *
 *  RE-RUN LIVES HERE, not in a tab of the run page. It used to be a fifth tab called
 *  "Re-run" listing every step as a button — which meant the graph, the thing a reader
 *  is actually looking at when they decide a step went wrong, offered no way to act on
 *  it. Re-run is an action ON A STEP, so it belongs in the step's own panel, reached by
 *  clicking the step. The whole-workflow restart is on the graph's toolbar. */

import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Checkbox from "@cloudscape-design/components/checkbox";
import ExpandableSection from "@cloudscape-design/components/expandable-section";
import FormField from "@cloudscape-design/components/form-field";
import KeyValuePairs from "@cloudscape-design/components/key-value-pairs";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Tabs from "@cloudscape-design/components/tabs";
import Textarea from "@cloudscape-design/components/textarea";
import { useEffect, useMemo, useState } from "react";

import { AssetView } from "../assets/AssetView";
import { fmtTime } from "../lib/clock";
import { statusIndicator } from "../lib/status";
import type { SessionSnapshot, Workflow } from "../types";
import { toolsLabel, toolsOf } from "../lib/tools";

export function StepPanel({
  agentId, snap, workflow, canRerun, settled, onRerun,
}: {
  agentId: string;
  snap: SessionSnapshot;
  workflow: Workflow;
  canRerun: boolean;
  /** The run has stopped moving, so a re-run has a defined starting point. */
  settled: boolean;
  onRerun: (agents: string[], comment: string) => Promise<void>;
}) {
  const meta = workflow.agents[agentId] ?? { name: agentId };
  const state = snap.nodes?.[agentId] ?? { status: "pending" as const };
  const history = snap.history?.[agentId] ?? [];

  const dataSource = toolsOf(meta.tool).length
    ? toolsLabel(meta.tool, meta.corpus ? `corpus ${meta.corpus}` : undefined)
    : meta.runtime === "a2a"
      ? `remote agent${meta.source ? ` · ${meta.source}` : ""}`
      : (meta.access ?? "—");

  /** The gated parallel stage this step belongs to, if any. A SUBSET re-run is only
   *  offered there, because that stage's gate is what re-reviews the subset together —
   *  anywhere else there is nothing to review them against. */
  const siblings = useMemo(() => {
    const step = (workflow.steps ?? []).find(
      (s) => s.parallel?.includes(agentId) && s.hitl);
    return step?.parallel ?? null;
  }, [workflow.steps, agentId]);

  const ran = (id: string) =>
    Boolean(snap.nodes?.[id]?.output) || snap.nodes?.[id]?.status === "done";

  const [comment, setComment] = useState("");
  const [busy, setBusy] = useState(false);
  const [picked, setPicked] = useState<Record<string, boolean>>({});

  // Selecting a different state is a different action: its feedback and its checkboxes
  // should not be the previous step's.
  useEffect(() => {
    setComment("");
    setPicked({ [agentId]: true });
  }, [agentId]);

  const chosen = (siblings ?? []).filter((id) => picked[id] && ran(id));

  async function submit(agents: string[]) {
    setBusy(true);
    try {
      await onRerun(agents, comment);
      setComment("");
    } finally {
      setBusy(false);
    }
  }

  return (
    <Tabs
      variant="container"
      tabs={[
        {
          id: "details",
          label: "Details",
          content: (
            <KeyValuePairs
              columns={2}
              items={[
                { label: "Status", value: statusIndicator(state.status) },
                { label: "Step ID", value: <Box variant="code" fontSize="body-s">{agentId}</Box> },
                { label: "Produces", value: meta.produces ?? "—" },
                { label: "Placement", value: meta.runtime ?? "main" },
                { label: "Data source", value: dataSource },
                { label: "Model", value: meta.model ?? "deployment default" },
                { label: "Output budget", value: meta.maxTokens ? `${meta.maxTokens} tokens` : "—" },
                { label: "Versions", value: String(history.length || 1) },
                ...(meta.skill ? [{ label: "Remote skill", value: meta.skill }] : []),
                ...(meta.access ? [{ label: "Access", value: meta.access }] : []),
              ]}
            />
          ),
        },
        {
          id: "output",
          label: "Output",
          content:
            history.length > 1
              ? (
                <SpaceBetween size="m">
                  {history.slice().reverse().map((h) => (
                    <ExpandableSection
                      key={h.version}
                      defaultExpanded={h.version === history.length}
                      headerText={`v${h.version} — ${h.version === 1 ? "initial" : "revision"}`}
                      headerDescription={fmtTime(h.at)}
                    >
                      <SpaceBetween size="s">
                        {h.comment ? (
                          <Box variant="p">
                            <Box variant="awsui-key-label">Reviewer note</Box>
                            {h.comment}
                          </Box>
                        ) : null}
                        <AssetView text={h.output ?? ""} />
                      </SpaceBetween>
                    </ExpandableSection>
                  ))}
                </SpaceBetween>
              )
              : <AssetView text={state.output ?? ""} />,
        },
        {
          id: "actions",
          label: "Actions",
          content: !settled
            ? (
              <Alert type="info">
                Re-run becomes available once the run settles. It is currently{" "}
                <Box variant="strong" display="inline">{String(snap.overall)}</Box>.
              </Alert>
            )
            : !ran(agentId)
              ? (
                <Alert type="info">
                  This step has not produced output yet, so there is nothing to re-run
                  from.
                </Alert>
              )
              : (
                <SpaceBetween size="l">
                  {canRerun ? null : (
                    <Alert type="warning">
                      You do not have permission to re-run. Ask for the group that grants
                      the <Box variant="code" display="inline">rerun</Box> action.
                    </Alert>
                  )}

                  <FormField
                    label="Feedback"
                    description="Injected as the agent's feedback, so it knows what to change."
                    stretch
                  >
                    <Textarea
                      value={comment} rows={3}
                      placeholder="What should it change?"
                      onChange={({ detail }) => setComment(detail.value)}
                    />
                  </FormField>

                  <FormField
                    label="Re-run this step and everything after it"
                    description="Each review gate downstream pauses again."
                  >
                    <Button
                      variant="primary" iconName="redo" loading={busy} disabled={!canRerun}
                      onClick={() => void submit([agentId])}
                    >
                      {`Re-run ${meta.name ?? agentId}`}
                    </Button>
                  </FormField>

                  {siblings && siblings.length > 1 ? (
                    <FormField
                      label="Or re-run several of this stage together"
                      description="These agents run concurrently, so a subset can be revised together and re-reviewed at this stage's gate."
                      stretch
                    >
                      <SpaceBetween size="xs">
                        {siblings.map((id) => (
                          <Checkbox
                            key={id}
                            checked={Boolean(picked[id])}
                            disabled={!ran(id)}
                            onChange={({ detail }) =>
                              setPicked((p) => ({ ...p, [id]: detail.checked }))}
                          >
                            {workflow.agents[id]?.name ?? id}
                            {ran(id) ? null : (
                              <Box variant="small" color="text-status-inactive">
                                {" "}— no output to revise
                              </Box>
                            )}
                          </Checkbox>
                        ))}
                        <Button
                          iconName="redo" loading={busy}
                          disabled={!canRerun || chosen.length < 2}
                          onClick={() => void submit(chosen)}
                        >
                          {chosen.length > 1
                            ? `Re-run ${chosen.length} selected together`
                            : "Re-run selected together"}
                        </Button>
                      </SpaceBetween>
                    </FormField>
                  ) : null}
                </SpaceBetween>
              ),
        },
      ]}
    />
  );
}
