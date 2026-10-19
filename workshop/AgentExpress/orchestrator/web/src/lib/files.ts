/** A run's files, as GET /api/sessions/{id}/files and GET /api/files return them
 *  (bff/runartifacts.py), and what the page needs to show one: its size, a link, and a
 *  text preview read through that link. */

import { api } from "../api";

export type FileGroup = "input" | "output" | "code";
export type PreviewKind = "" | "image" | "pdf" | "text" | "markdown" | "csv" | "json";

export interface RunFile {
  key: string;
  name: string;
  group: FileGroup;
  /** The agent that made it; "" for an input. */
  agent: string;
  size: number;
  type: string;
  preview: PreviewKind;
  time: string;
  session: string;
  /** The run's request, on the Files page. */
  topic?: string;
}

export interface RunFiles {
  stored: boolean;
  files: RunFile[];
  truncated?: boolean;
  note?: string;
}

export interface FileLink {
  url: string;
  expiresIn: number;
  type: string;
  preview: PreviewKind;
  name: string;
}

/** What a preview reads at most: enough for 200 rows of a wide CSV, little enough that
 *  opening a large log does not stall the page. */
export const PREVIEW_BYTES = 256_000;
export const CSV_ROWS = 200;

export const GROUP_LABEL: Record<FileGroup, string> = { input: "Input", output: "Output", code: "Code" };

export function fmtSize(n: number): string {
  if (!Number.isFinite(n) || n < 0) return "—";
  if (n < 1000) return `${n} B`;
  if (n < 1e6) return `${Math.round(n / 1e3)} KB`;
  return `${(n / 1e6).toFixed(1)} MB`;
}

export function fileLink(sessionId: string, key: string): Promise<FileLink> {
  return api.get<FileLink>(`/api/sessions/${encodeURIComponent(sessionId)}/files/url?key=${encodeURIComponent(key)}`);
}

/** The first `maxBytes` of a file, as text, and whether there was more. The link is a
 *  presigned S3 GET; the bucket allows this page's origin to read it (CORS GET). */
export async function readText(url: string, maxBytes = PREVIEW_BYTES): Promise<{ text: string; cut: boolean }> {
  const res = await fetch(url, { headers: { Range: `bytes=0-${maxBytes - 1}` } });
  if (!res.ok) throw new Error(`could not read the file (${res.status})`);
  const total = Number((res.headers.get("Content-Range") ?? "").split("/")[1] ?? "");
  const buf = new Uint8Array(await res.arrayBuffer());
  const body = buf.length > maxBytes ? buf.slice(0, maxBytes) : buf;
  const cut = buf.length > maxBytes || (Number.isFinite(total) && total > body.length);
  return { text: new TextDecoder("utf-8").decode(body), cut };
}

/** CSV rows (RFC 4180: quoted fields, doubled quotes, newlines inside quotes), at most
 *  `maxRows`. A cut-off last line is dropped rather than shown half. */
export function parseCsv(text: string, maxRows = CSV_ROWS, cut = false): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let quoted = false;
  let i = 0;
  const src = text.replace(/^\uFEFF/, "");
  while (i < src.length && rows.length < maxRows) {
    const c = src[i];
    if (quoted) {
      if (c === '"' && src[i + 1] === '"') { field += '"'; i += 2; continue; }
      if (c === '"') { quoted = false; i += 1; continue; }
      field += c; i += 1; continue;
    }
    if (c === '"' && field === "") { quoted = true; i += 1; continue; }
    if (c === ",") { row.push(field); field = ""; i += 1; continue; }
    if (c === "\r" || c === "\n") {
      row.push(field); field = "";
      rows.push(row); row = [];
      i += c === "\r" && src[i + 1] === "\n" ? 2 : 1;
      continue;
    }
    field += c; i += 1;
  }
  if (rows.length < maxRows && (field !== "" || row.length) && !cut) { row.push(field); rows.push(row); }
  return rows;
}

/** JSON pretty-printed when it parses (all of it was read), else as it came. */
export function prettyJson(text: string, cut: boolean): string {
  if (cut) return text;
  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch {
    return text;
  }
}
