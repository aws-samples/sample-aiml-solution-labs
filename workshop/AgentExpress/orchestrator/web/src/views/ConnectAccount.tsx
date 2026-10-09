/** A step that needs the person to connect their own account for a tool (auth "user"):
 *  the runtime fails it with where to connect (ToolNeedsConsent). Here that becomes
 *  one button to connect, and one to run the step again once connected. */
import Alert from "@cloudscape-design/components/alert";
import Button from "@cloudscape-design/components/button";
import SpaceBetween from "@cloudscape-design/components/space-between";

import type { SessionSnapshot } from "../types";

const CONSENT = /Connect your account for '([^']+)' first, then run this step again: (https:\/\/bedrock-agentcore\.[a-z0-9-]+\.amazonaws\.com\/identities\/oauth2\/authorize\?\S+)/;

/** The newest "connect your account" a run asked for: the tool, where, and which step. */
export function consentOf(snap: SessionSnapshot): { tool: string; url: string; agent: string } | null {
  const lines = [...(snap.logs ?? [])].reverse();
  for (const l of lines) {
    const m = CONSENT.exec(l.msg ?? "");
    if (!m) continue;
    // The step that asked: the line's own node, else the one that failed.
    const agent = (l.node && snap.nodes?.[l.node] ? l.node : "")
      || (Object.entries(snap.nodes ?? {}).find(([, n]) => n.status === "failed")?.[0] ?? "");
    return { tool: m[1], url: m[2].replace(/[).,]+$/, ""), agent };
  }
  return null;
}

export function ConnectAccount({ snap, canRerun, onRerun }: {
  snap: SessionSnapshot; canRerun: boolean; onRerun: (agents: string[], comment: string) => Promise<void>;
}) {
  const c = consentOf(snap);
  // Only while the run stands failed on it: a re-run that went through clears it.
  if (!c || snap.overall !== "failed") return null;
  return (
    <Alert type="info" header={`Connect your ${c.tool} account`}
      action={
        <SpaceBetween direction="horizontal" size="xs">
          <Button href={c.url} target="_blank" iconName="external" iconAlign="right">Connect</Button>
          {canRerun && c.agent ? <Button onClick={() => void onRerun([c.agent], "")}>Run the step again</Button> : null}
        </SpaceBetween>
      }>
      This step uses your own {c.tool} account. Connect it once, then run the step again.
    </Alert>
  );
}
