/** Share on a run page: the run's owner makes a read-only link to it, copies it, and
 *  lists and revokes the links it has (bff/runshares.py).
 *
 *  Whoever opens a link must be signed in to this deployment, and sees the run only: not
 *  its cost, prompts or files, and no action on it. The server decides all of that; this
 *  only says so before the owner hands a link out. */

import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import CopyToClipboard from "@cloudscape-design/components/copy-to-clipboard";
import FormField from "@cloudscape-design/components/form-field";
import Modal from "@cloudscape-design/components/modal";
import SegmentedControl from "@cloudscape-design/components/segmented-control";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Table from "@cloudscape-design/components/table";
import { useCallback, useEffect, useState } from "react";

import { ApiError, api } from "../api";
import { fmtTime } from "../lib/clock";

/** One link, as the server returns it. `expires` and `created` are UTC ISO stamps. */
export interface RunShare {
  shareId: string;
  url: string;
  expires: string;
  created: string;
}

export const EXPIRY_CHOICES = [
  { id: "1h", text: "1 hour" }, { id: "1d", text: "1 day" },
  { id: "7d", text: "7 days" }, { id: "30d", text: "30 days" },
];

/** The address a colleague opens: this page, with the link in its hash (App.tsx navigate). */
export function shareLink(shareId: string, loc: Pick<Location, "origin" | "pathname"> = window.location): string {
  return `${loc.origin}${loc.pathname}#shared:${shareId}`;
}

function message(e: unknown): string {
  return e instanceof ApiError ? e.message : String(e);
}

export function ShareRun({ sessionId }: { sessionId: string }) {
  const [open, setOpen] = useState(false);
  const [expiry, setExpiry] = useState("7d");
  const [shares, setShares] = useState<RunShare[] | null>(null);
  const [max, setMax] = useState(10);
  const [made, setMade] = useState<RunShare | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const base = `/api/sessions/${encodeURIComponent(sessionId)}/shares`;

  const load = useCallback(async () => {
    try {
      const r = await api.get<{ shares: RunShare[]; max: number }>(base);
      setShares(r.shares);
      setMax(r.max);
    } catch (e) {
      setError(`Could not list this run's links: ${message(e)}`);
    }
  }, [base]);

  useEffect(() => {
    if (!open) return;
    setMade(null);
    setError("");
    void load();
  }, [open, load]);

  async function create() {
    setBusy(true);
    setError("");
    try {
      setMade(await api.post<RunShare>(base, { expiresIn: expiry }));
      await load();
    } catch (e) {
      setError(`Could not create a link: ${message(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function revoke(s: RunShare) {
    setError("");
    try {
      await api.del(`${base}/${encodeURIComponent(s.shareId)}`);
      if (made?.shareId === s.shareId) setMade(null);
      await load();
    } catch (e) {
      setError(`Could not revoke the link: ${message(e)}`);
    }
  }

  const full = (shares?.length ?? 0) >= max;
  return (
    <>
      <Button iconName="share" onClick={() => setOpen(true)}>Share</Button>
      <Modal
        visible={open}
        onDismiss={() => setOpen(false)}
        header="Share this run"
        footer={<Box float="right"><Button variant="link" onClick={() => setOpen(false)}>Close</Button></Box>}
      >
        <SpaceBetween size="m">
          <Box variant="p">
            Anyone signed in to this application who has the link can read this run until the link
            expires or you revoke it. They see its steps, outputs and timeline, read-only. They cannot
            approve, re-run, stop or delete it, and they do not see its cost, prompts or files.
          </Box>
          <FormField label="Expires after">
            <SegmentedControl
              label="Expires after"
              selectedId={expiry}
              options={EXPIRY_CHOICES}
              onChange={({ detail }) => setExpiry(detail.selectedId)}
            />
          </FormField>
          <Button variant="primary" loading={busy} disabled={full} onClick={() => void create()}>
            Create link
          </Button>
          {full ? <Box color="text-status-inactive">This run has {max} active links, the most it may have. Revoke one to make another.</Box> : null}
          {error ? <Alert type="error">{error}</Alert> : null}
          {made ? (
            <Alert type="success" header="Link created">
              <SpaceBetween size="xxs">
                <CopyToClipboard variant="inline" textToCopy={shareLink(made.shareId)}
                  copyButtonAriaLabel="Copy the link" copySuccessText="Link copied" copyErrorText="Could not copy" />
                <Box fontSize="body-s">Expires {fmtTime(made.expires)}.</Box>
              </SpaceBetween>
            </Alert>
          ) : null}
          <Table
            variant="embedded"
            header={<Box variant="h3">Active links ({shares?.length ?? 0} of {max})</Box>}
            loading={shares === null && !error}
            loadingText="Loading links"
            items={shares ?? []}
            trackBy="shareId"
            empty={<Box textAlign="center" color="text-status-inactive">No active links.</Box>}
            columnDefinitions={[
              {
                id: "link", header: "Link",
                cell: (s) => (
                  <CopyToClipboard variant="inline" textToCopy={shareLink(s.shareId)}
                    textToDisplay={`…${s.shareId.slice(-6)}`}
                    copyButtonAriaLabel={`Copy the link ending ${s.shareId.slice(-6)}`}
                    copySuccessText="Link copied" copyErrorText="Could not copy" />
                ),
              },
              { id: "created", header: "Created", cell: (s) => fmtTime(s.created) },
              { id: "expires", header: "Expires", cell: (s) => fmtTime(s.expires) },
              {
                id: "revoke", header: "",
                cell: (s) => (
                  <Button variant="inline-link" ariaLabel={`Revoke the link ending ${s.shareId.slice(-6)}`}
                    onClick={() => void revoke(s)}>Revoke</Button>
                ),
              },
            ]}
          />
        </SpaceBetween>
      </Modal>
    </>
  );
}
