/** The Files tab of a run: what it was started with, what its agents made, and the code
 *  the sandbox ran, each with Download and, for text, tables, images and PDFs, Preview.
 *
 *  Every file is opened through a 5-minute link the server signs only for a file of a run
 *  the caller may read (bff/runartifacts.py). Text is read through that link and shown as
 *  text: React escapes it, and Markdown is rendered as elements, never as HTML. */

import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Container from "@cloudscape-design/components/container";
import Header from "@cloudscape-design/components/header";
import Modal from "@cloudscape-design/components/modal";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Spinner from "@cloudscape-design/components/spinner";
import StatusIndicator from "@cloudscape-design/components/status-indicator";
import Table from "@cloudscape-design/components/table";
import { useEffect, useState } from "react";

import { api } from "../api";
import { Markdown } from "../builder/markdown";
import { fmtTime } from "../lib/clock";
import {
  CSV_ROWS, type FileGroup, type FileLink, fileLink, fmtSize, parseCsv, prettyJson, readText,
  type RunFile, type RunFiles as RunFilesBody,
} from "../lib/files";
import type { Workflow } from "../types";

const GROUPS: { id: FileGroup; title: string; description: string; empty: string }[] = [
  { id: "input", title: "Inputs", description: "The files the run was started with.", empty: "This run was started without files." },
  { id: "output", title: "Outputs", description: "Files and images the agents made.", empty: "No agent saved a file in this run." },
  { id: "code", title: "Code", description: "Code the sandbox ran, and what it printed (.log).", empty: "No code ran in this run." },
];

function download(url: string): void {
  const a = document.createElement("a");
  a.href = url;
  a.rel = "noopener noreferrer";
  document.body.appendChild(a);
  a.click();
  a.remove();
}

/** Preview and Download for one file. */
export function FileActions({ file, onPreview }: { file: RunFile; onPreview: (f: RunFile) => void }) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const open = async (preview: boolean) => {
    setBusy(true);
    setError(null);
    try {
      if (preview && file.preview !== "pdf") {
        onPreview(file);
        return;
      }
      const link = await fileLink(file.session, file.key);
      // A PDF opens in a new tab; everything else downloads (the link says attachment).
      if (preview) window.open(link.url, "_blank", "noopener,noreferrer");
      else download(link.url);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <SpaceBetween direction="horizontal" size="xs">
      {file.preview ? (
        <Button variant="inline-link" disabled={busy} onClick={() => void open(true)}
          ariaLabel={`Preview ${file.name}`}>
          {file.preview === "pdf" ? "Open" : "Preview"}
        </Button>
      ) : null}
      <Button variant="inline-link" disabled={busy} onClick={() => void open(false)}
        ariaLabel={`Download ${file.name}`}>
        Download
      </Button>
      {error ? <StatusIndicator type="error">{error}</StatusIndicator> : null}
    </SpaceBetween>
  );
}

/** What a preview shows: an image, a table, Markdown, or text. */
function PreviewBody({ file, link }: { file: RunFile; link: FileLink }) {
  const [text, setText] = useState<{ text: string; cut: boolean } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const textual = file.preview !== "image";
  useEffect(() => {
    if (!textual) return;
    let live = true;
    readText(link.url)
      .then((t) => { if (live) setText(t); })
      .catch((e: Error) => { if (live) setError(e.message); });
    return () => { live = false; };
  }, [link.url, textual]);
  if (file.preview === "image") {
    return <img src={link.url} alt={file.name} style={{ maxWidth: "100%", maxHeight: 560, display: "block" }} />;
  }
  if (error) return <StatusIndicator type="error">Preview is not available: {error}. Download it instead.</StatusIndicator>;
  if (!text) return <Spinner />;
  const cutNote = text.cut ? <Box variant="small" color="text-body-secondary">Showing the beginning of the file. Download it for all of it.</Box> : null;
  if (file.preview === "csv") {
    const rows = parseCsv(text.text, CSV_ROWS + 1, text.cut);
    const [head = [], ...body] = rows;
    const width = Math.max(head.length, ...body.map((r) => r.length), 0);
    return (
      <SpaceBetween size="xs">
        <Table
          variant="embedded"
          wrapLines={false}
          items={body.slice(0, CSV_ROWS).map((cells, i) => ({ i, cells }))}
          trackBy="i"
          columnDefinitions={Array.from({ length: width }, (_, c) => ({
            id: String(c), header: head[c] ?? `Column ${c + 1}`, cell: (r: { cells: string[] }) => r.cells[c] ?? "",
          }))}
          empty="No rows."
        />
        {body.length > CSV_ROWS || text.cut
          ? <Box variant="small" color="text-body-secondary">Showing the first {Math.min(body.length, CSV_ROWS)} rows. Download it for all of them.</Box>
          : null}
      </SpaceBetween>
    );
  }
  if (file.preview === "markdown") {
    return <SpaceBetween size="xs"><Markdown text={text.text} />{cutNote}</SpaceBetween>;
  }
  const shown = file.preview === "json" ? prettyJson(text.text, text.cut) : text.text;
  return (
    <SpaceBetween size="xs">
      <pre style={{ whiteSpace: "pre-wrap", wordBreak: "break-word", maxHeight: 560, overflow: "auto", margin: 0 }}>{shown}</pre>
      {cutNote}
    </SpaceBetween>
  );
}

/** The preview window for one file: asks for its link, then shows it. */
export function FilePreview({ file, onClose }: { file: RunFile | null; onClose: () => void }) {
  const [link, setLink] = useState<FileLink | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    setLink(null);
    setError(null);
    if (!file) return;
    let live = true;
    fileLink(file.session, file.key)
      .then((l) => { if (live) setLink(l); })
      .catch((e: Error) => { if (live) setError(e.message); });
    return () => { live = false; };
  }, [file]);
  return (
    <Modal visible={Boolean(file)} onDismiss={onClose} size="max" header={file?.name ?? "File"}
      footer={<Box float="right"><Button onClick={onClose}>Close</Button></Box>}>
      {!file ? null : error ? <StatusIndicator type="error">{error}</StatusIndicator>
        : !link ? <Spinner /> : <PreviewBody file={file} link={link} />}
    </Modal>
  );
}

export function agentName(workflow: Workflow | undefined, id: string): string {
  if (!id) return "—";
  return workflow?.agents?.[id]?.name ?? id;
}

export function RunFiles({ sessionId, workflow, refreshKey }: {
  sessionId: string; workflow: Workflow; refreshKey?: string;
}) {
  const [body, setBody] = useState<RunFilesBody | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [previewing, setPreviewing] = useState<RunFile | null>(null);
  useEffect(() => {
    let live = true;
    api.get<RunFilesBody>(`/api/sessions/${encodeURIComponent(sessionId)}/files`)
      .then((b) => { if (live) { setBody(b); setError(null); } })
      .catch((e: Error) => { if (live) setError(e.message); });
    return () => { live = false; };
  }, [sessionId, refreshKey]);

  if (error) return <Alert type="error" header="Could not list this run's files">{error}</Alert>;
  if (!body) return <Spinner size="large" />;
  if (!body.stored) {
    return (
      <Container header={<Header variant="h2">Files</Header>}>
        <Box color="text-status-inactive">
          {body.note ?? "This deployment keeps no run files. Set orchestrator.files in workflow.json to keep what agents make."}
        </Box>
      </Container>
    );
  }
  return (
    <SpaceBetween size="l">
      {body.truncated ? <Alert type="info">This run has more files than the page lists.</Alert> : null}
      {GROUPS.map((g) => {
        const items = body.files.filter((f) => f.group === g.id);
        return (
          <Table
            key={g.id}
            variant="container"
            items={items}
            trackBy="key"
            header={<Header variant="h2" counter={`(${items.length})`} description={g.description}>{g.title}</Header>}
            empty={<Box color="text-status-inactive">{g.empty}</Box>}
            columnDefinitions={[
              { id: "name", header: "Name", isRowHeader: true, minWidth: 200, cell: (f) => f.name },
              ...(g.id === "input" ? [] : [{ id: "agent", header: "Agent", cell: (f: RunFile) => agentName(workflow, f.agent) }]),
              { id: "size", header: "Size", width: 100, cell: (f) => fmtSize(f.size) },
              { id: "time", header: "Time", width: 190, cell: (f) => fmtTime(f.time) || "—" },
              { id: "actions", header: "Actions", width: 200, cell: (f) => <FileActions file={f} onPreview={setPreviewing} /> },
            ]}
          />
        );
      })}
      <FilePreview file={previewing} onClose={() => setPreviewing(null)} />
    </SpaceBetween>
  );
}
