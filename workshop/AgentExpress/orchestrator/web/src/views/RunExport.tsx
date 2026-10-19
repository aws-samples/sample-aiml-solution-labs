/** Where this app's runs are sent (`orchestrator.export`): on a run's page, whether the run
 *  reached Langfuse, Datadog... with a link to its trace and a Re-send; on Observability, the
 *  admins' Export card (the last day's sends, a test trace, pause and resume).
 *
 *  All from bff/traceexport.py: GET /api/export, POST /api/export/test, PUT /api/export/pause,
 *  POST /api/sessions/{id}/export. The server checks every action; the page only hides what the
 *  caller may not do. No key or header is ever sent to the page. */
import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import ColumnLayout from "@cloudscape-design/components/column-layout";
import Container from "@cloudscape-design/components/container";
import Header from "@cloudscape-design/components/header";
import Link from "@cloudscape-design/components/link";
import SpaceBetween from "@cloudscape-design/components/space-between";
import StatusIndicator, { type StatusIndicatorProps } from "@cloudscape-design/components/status-indicator";
import { useCallback, useEffect, useState } from "react";
import { api } from "../api";
import { nameOf, type Destination } from "../builder/exportSetup";
import type { SessionSnapshot } from "../types";

export interface ExportStatus {
  on: boolean;
  destination?: Destination;
  label?: string;
  /** A link to one trace, with {traceId} where its id goes; "" when there is none. */
  traceLink?: string;
  paused?: boolean;
  canManage?: boolean;
  pausedBy?: string;
  pausedAt?: string;
  recent?: { sent: number; failed: number; lastError: string; lastErrorAt: string };
}
/** The run item's `export` (export-contract.md section 4). */
export interface RunExportState { status: "sent" | "failed" | "off" | "paused"; traceId?: string; at?: string; error?: string }
interface ActionResult { ok: boolean; status: number; error: string; traceId: string; spans?: number }

export function useExportStatus(): [ExportStatus | null, () => void] {
  const [status, setStatus] = useState<ExportStatus | null>(null);
  const load = useCallback(() => {
    api.get<ExportStatus>("/api/export").then(setStatus, () => setStatus({ on: false }));
  }, []);
  useEffect(load, [load]);
  return [status, load];
}

const CHIP: Record<RunExportState["status"], [StatusIndicatorProps.Type, string]> = {
  sent: ["success", "Sent"], failed: ["error", "Failed"], off: ["stopped", "Off"], paused: ["pending", "Paused"],
};

export function linkTo(template: string | undefined, traceId: string | undefined): string {
  return template && traceId && /^[0-9a-f]{32}$/.test(traceId) ? template.replace("{traceId}", traceId) : "";
}

/** On a run's page. `canResend`: an admin, or the run's owner (the server checks again). */
export function RunExport({ snap, canResend }: { snap: SessionSnapshot; canResend: boolean }) {
  const [status] = useExportStatus();
  const [busy, setBusy] = useState(false);
  const [said, setSaid] = useState<{ ok: boolean; text: string } | null>(null);
  if (!status?.on || !status.destination) return null;
  const tool = nameOf(status.destination);
  const exp = (snap as SessionSnapshot & { export?: RunExportState }).export;
  // A run is sent when it ends; a run parked at a gate has not ended.
  const settled = ["done", "denied", "failed", "cancelled"].includes(String(snap.overall));
  const [type, text]: [StatusIndicatorProps.Type, string] = exp ? CHIP[exp.status] ?? ["info", String(exp.status)]
    : settled ? ["stopped", "Not sent"] : ["pending", "Sends when it ends"];
  const href = linkTo(status.traceLink, exp?.traceId);
  const resend = async () => {
    setBusy(true);
    setSaid(null);
    try {
      const r = await api.post<ActionResult>(`/api/sessions/${encodeURIComponent(snap.session_id)}/export`);
      setSaid(r.ok ? { ok: true, text: `Sent to ${tool}${r.spans ? ` (${r.spans} spans)` : ""}.` }
        : { ok: false, text: r.error || `${tool} did not take it (HTTP ${r.status}).` });
    } catch (e) {
      setSaid({ ok: false, text: (e as Error).message });
    } finally {
      setBusy(false);
    }
  };
  return (
    <SpaceBetween direction="horizontal" size="xs" alignItems="center">
      <span title={exp?.error || undefined}>
        <StatusIndicator type={type}>{`${tool}: ${text}`}</StatusIndicator>
      </span>
      {href ? <Link external href={href}>{`View in ${tool}`}</Link> : null}
      {canResend && settled ? (
        <Button onClick={() => void resend()} loading={busy}>{`Re-send to ${tool}`}</Button>
      ) : null}
      {said ? <StatusIndicator type={said.ok ? "success" : "error"}>{said.text}</StatusIndicator> : null}
    </SpaceBetween>
  );
}

/** The admins' Export card on Observability. Nothing for anyone else, or while it is off. */
export function ExportCard() {
  const [status, reload] = useExportStatus();
  const [busy, setBusy] = useState("");
  const [said, setSaid] = useState<{ ok: boolean; text: string } | null>(null);
  if (!status?.on || !status.canManage || !status.destination) return null;
  const tool = nameOf(status.destination);
  const recent = status.recent ?? { sent: 0, failed: 0, lastError: "", lastErrorAt: "" };
  const testTrace = async () => {
    setBusy("test");
    setSaid(null);
    try {
      const r = await api.post<ActionResult>("/api/export/test");
      setSaid(r.ok ? { ok: true, text: `${tool} accepted a test trace${r.traceId ? ` (trace ID ${r.traceId})` : ""}.` }
        : { ok: false, text: r.error || `${tool} did not take it (HTTP ${r.status}).` });
    } catch (e) {
      setSaid({ ok: false, text: (e as Error).message });
    } finally {
      setBusy("");
    }
  };
  const pause = async (paused: boolean) => {
    if (paused && !window.confirm(`Pause sending runs to ${tool}? Runs that end while it is paused are not sent; you can re-send one from its page.`)) return;
    setBusy("pause");
    try {
      await api.put("/api/export/pause", { paused });
      reload();
    } catch (e) {
      setSaid({ ok: false, text: (e as Error).message });
    } finally {
      setBusy("");
    }
  };
  return (
    <Container header={<Header variant="h2" description="Where this app's runs are sent as they end."
      actions={<SpaceBetween direction="horizontal" size="xs">
        <Button loading={busy === "test"} onClick={() => void testTrace()}>Send test trace</Button>
        <Button loading={busy === "pause"} onClick={() => void pause(!status.paused)}>{status.paused ? "Resume" : "Pause"}</Button>
      </SpaceBetween>}>Export</Header>}>
      <SpaceBetween size="m">
        <ColumnLayout columns={4} variant="text-grid">
          <div><Box variant="awsui-key-label">Destination</Box><div>{status.label}</div></div>
          <div><Box variant="awsui-key-label">Sending</Box>
            <StatusIndicator type={status.paused ? "pending" : "success"}>
              {status.paused ? `Paused${status.pausedBy ? ` by ${status.pausedBy}` : ""}` : "On"}</StatusIndicator></div>
          <div><Box variant="awsui-key-label">Sent, last 24 hours</Box><div>{recent.sent}</div></div>
          <div><Box variant="awsui-key-label">Failed, last 24 hours</Box><div>{recent.failed}</div></div>
        </ColumnLayout>
        {recent.lastError ? (
          <Box variant="small" color="text-status-error">Last error{recent.lastErrorAt ? ` (${recent.lastErrorAt})` : ""}: {recent.lastError}</Box>
        ) : null}
        {said ? <Alert type={said.ok ? "success" : "error"}>{said.text}</Alert> : null}
      </SpaceBetween>
    </Container>
  );
}
