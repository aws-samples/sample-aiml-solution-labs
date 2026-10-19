/** A run someone shared with a link (#shared:<id>): the run page, read-only.
 *
 *  It reads GET /api/shared/{id} and nothing else: no run route, no Flow, no telemetry,
 *  and no write route, so there is no action on it to offer. The server re-checks the
 *  link on every read, so a link that expires or is revoked while the page is open stops
 *  showing the run at the next poll. */

import Alert from "@cloudscape-design/components/alert";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Spinner from "@cloudscape-design/components/spinner";
import { useEffect, useState } from "react";

import { ApiError, api } from "../api";
import { fmtTime } from "../lib/clock";
import { isSettled } from "../lib/status";
import type { SessionSnapshot, Workflow } from "../types";
import { RunDetail } from "./RunDetail";

export interface SharedSnapshot extends SessionSnapshot {
  sharedBy: string;
  expires: string;
  readOnly: true;
}

/** How often a run still in progress is read again. */
const POLL_MS = 5000;

export function SharedRun({ shareId, workflow }: { shareId: string; workflow: Workflow }) {
  const [snap, setSnap] = useState<SharedSnapshot | null>(null);
  const [gone, setGone] = useState("");
  const [tab, setTab] = useState("graph");
  const settled = snap ? isSettled(String(snap.overall ?? "")) : false;

  useEffect(() => {
    setSnap(null);
    setGone("");
    setTab("graph");
  }, [shareId]);

  useEffect(() => {
    if (gone || settled) return;
    let live = true;
    const pull = async () => {
      try {
        const s = await api.get<SharedSnapshot>(`/api/shared/${encodeURIComponent(shareId)}`);
        if (live) setSnap(s);
      } catch (e) {
        // Expired, revoked or unknown: stop showing the run. Anything else is transient.
        if (live && e instanceof ApiError && (e.status === 404 || e.status === 403)) {
          setSnap(null);
          setGone(e.message);
        }
      }
    };
    void pull();
    const t = setInterval(() => void pull(), POLL_MS);
    return () => { live = false; clearInterval(t); };
  }, [shareId, gone, settled]);

  if (gone) {
    return (
      <Alert type="error" header="This link cannot be opened">
        It has expired or been revoked, or it was never a link to a run here. Ask whoever sent it for a new one.
      </Alert>
    );
  }
  if (!snap) return <Spinner size="large" />;
  return (
    <SpaceBetween size="l">
      <Alert type="info" header="Shared run">
        Shared by {snap.sharedBy || "its owner"}, read-only, expires {fmtTime(snap.expires)}.
      </Alert>
      <RunDetail
        snap={snap} workflow={snap.workflow ?? workflow} selected={null} onSelect={() => {}}
        can={() => false} onCancel={() => {}} activeTab={tab} onTabChange={setTab}
        onRerun={async () => {}} readOnly
      />
    </SpaceBetween>
  );
}
