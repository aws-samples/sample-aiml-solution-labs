/** The human review gate.
 *
 *  Three shapes, because a gate follows the step it is attached to: one agent, a
 *  SEQUENCE (one decision for the chain), or a PARALLEL group (a decision per agent,
 *  so a reviewer can accept four findings and send one back). Rendered as a Cloudscape
 *  Alert + Form controls so it reads as the console's own "action required". */

import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Container from "@cloudscape-design/components/container";
import ExpandableSection from "@cloudscape-design/components/expandable-section";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import SegmentedControl from "@cloudscape-design/components/segmented-control";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Textarea from "@cloudscape-design/components/textarea";
import { useState } from "react";

import { AssetView } from "../assets/AssetView";
import type { SessionSnapshot, Step, Workflow } from "../types";

export type Decision = "approve" | "revise" | "deny";
export interface GroupDecision { decision: Decision; comment: string }

function gateStep(workflow: Workflow, node: string): Step | undefined {
  return (workflow.steps ?? []).find(
    (s) => (s.parallel || s.sequence) && s.gateId === node);
}

export function HitlGate({
  snap, workflow, canDecide, denyReason, onDecide, onGroupDecide,
}: {
  snap: SessionSnapshot;
  workflow: Workflow;
  canDecide: boolean;
  denyReason: string | null;
  onDecide: (d: Decision, comment: string) => Promise<void>;
  onGroupDecide: (per: Record<string, GroupDecision>, comment: string) => Promise<void>;
}) {
  const node = snap.hitl?.node ?? "";
  const step = gateStep(workflow, node);
  const isGroup = Boolean(step?.parallel);
  const ids = step?.parallel ?? step?.sequence ?? [node];

  const [comment, setComment] = useState("");
  const [busy, setBusy] = useState(false);
  const [per, setPer] = useState<Record<string, GroupDecision>>({});

  const label = step?.gateName ?? workflow.agents[node]?.name ?? node;

  async function run(fn: () => Promise<void>) {
    setBusy(true);
    try { await fn(); setComment(""); setPer({}); } finally { setBusy(false); }
  }

  const perOf = (id: string): GroupDecision => per[id] ?? { decision: "approve", comment: "" };

  /* EVERY agent in the group, not just the ones the reviewer changed. The control
     defaults each row to Approve on screen, so a reviewer who accepts all five and
     presses Submit has touched nothing — and sending only what changed meant sending an
     empty map, which the BFF refuses (an empty `decisions` is falsy, and no single
     `decision` was supplied either). The screen said Approve five times and the request
     said nothing at all. */
  const fullDecisions = (): Record<string, GroupDecision> =>
    Object.fromEntries(ids.map((id) => [id, perOf(id)]));
  const setPerFor = (id: string, patch: Partial<GroupDecision>) =>
    setPer((p) => ({ ...p, [id]: { ...perOf(id), ...patch } }));

  return (
    <Container
      header={
        <Header
          variant="h2"
          description={
            isGroup
              ? "Decide each agent separately. Anything you mark Revise re-runs with your note; the rest are accepted."
              : "Approve to continue, Revise to re-run with your note, or Deny to stop the run."
          }
        >
          {`Review required — ${label}`}
        </Header>
      }
    >
      <SpaceBetween size="l">
        <Alert type="info" statusIconAriaLabel="Info">
          The run is paused at this gate and will stay paused until you decide. Durable
          state is checkpointed, so this survives a reload.
        </Alert>

        {denyReason ? <Alert type="warning">{denyReason}</Alert> : null}

        {isGroup ? (
          <SpaceBetween size="m">
            {ids.map((id) => {
              const meta = workflow.agents[id] ?? { name: id };
              const out = snap.nodes?.[id]?.output ?? "";
              const d = perOf(id);
              return (
                <Container
                  key={id}
                  header={
                    <Header
                      variant="h3"
                      actions={
                        <SegmentedControl
                          selectedId={d.decision}
                          onChange={({ detail }) =>
                            setPerFor(id, { decision: detail.selectedId as Decision })}
                          label={`Decision for ${meta.name ?? id}`}
                          options={[
                            { id: "approve", text: "Approve" },
                            { id: "revise", text: "Revise" },
                          ]}
                        />
                      }
                    >
                      {meta.name ?? id}
                    </Header>
                  }
                >
                  <SpaceBetween size="s">
                    <ExpandableSection headerText="Output">
                      <AssetView text={out} />
                    </ExpandableSection>
                    {d.decision === "revise" ? (
                      <FormField label="What should it change?" stretch>
                        <Textarea
                          value={d.comment} rows={2}
                          placeholder={`Feedback for ${meta.name ?? id}`}
                          onChange={({ detail }) => setPerFor(id, { comment: detail.value })}
                        />
                      </FormField>
                    ) : null}
                  </SpaceBetween>
                </Container>
              );
            })}
            <Box float="right">
              <SpaceBetween direction="horizontal" size="xs">
                <Button disabled={!canDecide || busy}
                        onClick={() => void run(() => onDecide("deny", comment))}>
                  Deny run
                </Button>
                <Button variant="primary" loading={busy} disabled={!canDecide}
                        onClick={() => void run(() => onGroupDecide(fullDecisions(), comment))}>
                  Submit decisions
                </Button>
              </SpaceBetween>
            </Box>
          </SpaceBetween>
        ) : (
          <SpaceBetween size="m">
            {ids.map((id) => (
              <ExpandableSection
                key={id}
                defaultExpanded={ids.length === 1}
                headerText={workflow.agents[id]?.name ?? id}
              >
                <AssetView text={snap.nodes?.[id]?.output ?? ""} />
              </ExpandableSection>
            ))}
            <FormField
              label="Reviewer note"
              description="Required for Revise — it is injected as the agent's feedback. Optional otherwise."
              stretch
            >
              <Textarea
                value={comment} rows={3}
                placeholder="What should change?"
                onChange={({ detail }) => setComment(detail.value)}
              />
            </FormField>
            <Box float="right">
              <SpaceBetween direction="horizontal" size="xs">
                <Button disabled={!canDecide || busy}
                        onClick={() => void run(() => onDecide("deny", comment))}>
                  Deny
                </Button>
                <Button disabled={!canDecide || busy || !comment.trim()}
                        onClick={() => void run(() => onDecide("revise", comment))}>
                  Revise
                </Button>
                <Button variant="primary" loading={busy} disabled={!canDecide}
                        onClick={() => void run(() => onDecide("approve", comment))}>
                  Approve
                </Button>
              </SpaceBetween>
            </Box>
          </SpaceBetween>
        )}
      </SpaceBetween>
    </Container>
  );
}
