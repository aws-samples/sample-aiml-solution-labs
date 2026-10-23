/** An agent waiting for a person in the middle of its step (app/common/asks.py).
 *
 *  Two shapes, told apart by the run's `hitl.kind`:
 *    question      the agent's question and a box for the answer. Answer sends approve with
 *                  the answer as the comment; Decline sends deny.
 *    toolApproval  the tool and the exact arguments it will be called with, and Approve /
 *                  Deny. The arguments are shown as text (React escapes them): never run,
 *                  never rendered as HTML, because they come from a model.
 *  Both go through the same decision route as a review gate (bff/asks.py). */

import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Container from "@cloudscape-design/components/container";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Textarea from "@cloudscape-design/components/textarea";
import { useState } from "react";

import type { SessionSnapshot, Workflow } from "../types";
import type { Decision } from "./HitlGate";
import { useDocsBase } from "../docs/docsLinks";
import { infoSlot } from "../docs/InfoLink";

/** The longest answer the BFF accepts (bff/asks.py ANSWER_MAX). */
export const ANSWER_MAX = 4000;

export function AskPerson({
  snap, workflow, canDecide, denyReason, onDecide,
}: {
  snap: SessionSnapshot;
  workflow: Workflow;
  canDecide: boolean;
  denyReason: string | null;
  onDecide: (d: Decision, comment: string) => Promise<void>;
}) {
  const docs = useDocsBase();
  const hitl = snap.hitl!;
  const agent = workflow.agents[hitl.node]?.name ?? hitl.node;
  const isQuestion = hitl.kind === "question";
  const [text, setText] = useState("");
  const [busy, setBusy] = useState(false);
  const tooLong = text.length > ANSWER_MAX;

  async function send(d: Decision) {
    setBusy(true);
    try { await onDecide(d, text); setText(""); } finally { setBusy(false); }
  }

  const timeout = hitl.timeoutAt
    ? isQuestion
      ? ` With no answer by ${hitl.timeoutAt} (UTC) the agent carries on without one.`
      : ` With no decision by ${hitl.timeoutAt} (UTC) the call is ${hitl.timeoutAction === "approve" ? "approved" : "denied"} automatically.`
    : "";

  return (
    <Container
      header={
        <Header
          variant="h2"
          description={isQuestion
            ? "The agent paused to ask you this. Your answer is given to it as your own words, and it carries on."
            : "The agent wants to call this tool with these arguments. Approve runs the call; Deny tells the agent it was refused, and it carries on."}
          info={infoSlot(docs, "run.ask")}
        >
          {isQuestion ? `${agent} asks` : `Approve a tool call — ${agent}`}
        </Header>
      }
    >
      <SpaceBetween size="l">
        <Alert type="info" statusIconAriaLabel="Info">
          The run is paused and will stay paused until you {isQuestion ? "answer" : "decide"}.
          {hitl.approval === "event" ? " It can also be decided by an AgentExpress Approval Decision event." : ""}
          {timeout}
        </Alert>
        {denyReason ? <Alert type="warning">{denyReason}</Alert> : null}
        {isQuestion ? (
          <SpaceBetween size="m">
            <Box variant="p" fontSize="heading-s">{hitl.question}</Box>
            <FormField
              label="Your answer"
              errorText={tooLong ? `At most ${ANSWER_MAX} characters.` : undefined}
              stretch
            >
              <Textarea value={text} rows={3} placeholder="Answer the question"
                        onChange={({ detail }) => setText(detail.value)} />
            </FormField>
            <Box float="right">
              <SpaceBetween direction="horizontal" size="xs">
                <Button disabled={!canDecide || busy} onClick={() => void send("deny")}>Decline</Button>
                <Button variant="primary" loading={busy}
                        disabled={!canDecide || !text.trim() || tooLong}
                        onClick={() => void send("approve")}>
                  Answer
                </Button>
              </SpaceBetween>
            </Box>
          </SpaceBetween>
        ) : (
          <SpaceBetween size="m">
            <FormField label="Tool">
              <Box variant="code">{hitl.tool}</Box>
            </FormField>
            <FormField label="Arguments">
              <Box variant="pre">{hitl.args ?? "{}"}</Box>
            </FormField>
            <FormField label="Note (optional)" stretch
                       errorText={tooLong ? `At most ${ANSWER_MAX} characters.` : undefined}>
              <Textarea value={text} rows={2} placeholder="Why, for the agent and the activity log"
                        onChange={({ detail }) => setText(detail.value)} />
            </FormField>
            <Box float="right">
              <SpaceBetween direction="horizontal" size="xs">
                <Button disabled={!canDecide || busy || tooLong} onClick={() => void send("deny")}>Deny</Button>
                <Button variant="primary" loading={busy} disabled={!canDecide || tooLong}
                        onClick={() => void send("approve")}>
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
