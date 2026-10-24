/** AgentExpress Assistant: say what you want to build, and the designer (a model on the
 *  console's BFF — bff/designer.py) drafts the workflow and keeps it up to date as you
 *  talk. Its changes land in the same draft the manual tab edits, as soon as they are
 *  made; Undo takes one back, like any other edit.
 *
 *  A reply runs in the background and streams into the conversation as it is written, so
 *  this polls every POLL_MS while the designer is replying (the text grows, a phase says
 *  what it is doing), and reloads the build each time a change lands. */
import Avatar from "@cloudscape-design/chat-components/avatar";
import ChatBubble from "@cloudscape-design/chat-components/chat-bubble";
import SupportPromptGroup from "@cloudscape-design/chat-components/support-prompt-group";
import Box from "@cloudscape-design/components/box";
import Button, { type ButtonProps } from "@cloudscape-design/components/button";
import ButtonDropdown from "@cloudscape-design/components/button-dropdown";
import Container from "@cloudscape-design/components/container";
import CopyToClipboard from "@cloudscape-design/components/copy-to-clipboard";
import FileInput from "@cloudscape-design/components/file-input";
import FileTokenGroup from "@cloudscape-design/components/file-token-group";
import Header from "@cloudscape-design/components/header";
import Link from "@cloudscape-design/components/link";
import PromptInput from "@cloudscape-design/components/prompt-input";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Spinner from "@cloudscape-design/components/spinner";
import StatusIndicator from "@cloudscape-design/components/status-indicator";
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { SecretField } from "./BuildResources";
import { Canvas } from "./Canvas";
import { Markdown } from "./markdown";
import type { Project } from "./model";
import { designChanges, designer, type DesignChange, type DesignDoc, type DesignTurn, type ReviewMode } from "./storage";
import type { Issue } from "./validate";
import { gitTokenNames } from "./codeSource";
import { useDocsBase } from "../docs/docsLinks";
import { InfoLink, infoSlot } from "../docs/InfoLink";

/** Literal \\uXXXX sequences as the characters they name. The model sometimes
 *  double-escapes one in a change summary (an arrow arrives as "\\u2192"); summaries
 *  stored before bff/designer.py unescaped them still carry it. */
export function unescape(text: string): string {
  return text.replace(/\\u([0-9a-fA-F]{4})/g, (_m, h: string) => String.fromCharCode(parseInt(h, 16)));
}

export const POLL_MS = 700;
/** Within this many pixels of the bottom, a new line scrolls the log with it; further up,
 *  the reader is looking back, so the log stays put and offers "Jump to latest". */
const STICK_PX = 80;

/** The element that actually scrolls the conversation. Meant to be the log itself, but
 *  the Container's fitHeight content is the one that scrolls when the log's height does
 *  not resolve, so following "the log" did nothing and a reply grew out of sight. The
 *  nearest ancestor (up to the chat's cell) that scrolls wins, else the log. */
export function scrollParent(log: HTMLElement | null, stop: HTMLElement | null): HTMLElement | null {
  for (let e: HTMLElement | null = log; e && e !== stop; e = e.parentElement) {
    const oy = getComputedStyle(e).overflowY;
    if ((oy === "auto" || oy === "scroll") && e.scrollHeight > e.clientHeight + 1) return e;
  }
  return log;
}

/** Review changes: a second model checks the Assistant's change before it is saved
 *  (bff/review.py). The label is this one constant; the reviewer model is never named. */
export const REVIEW_LABEL = "Review changes";
export const REVIEW_HELP = "A second model checks the Assistant's changes before they're applied.";
export const REVIEW_OPTIONS: { id: ReviewMode; text: string }[] = [
  { id: "off", text: "Off" }, { id: "auto", text: "Auto" }, { id: "always", text: "Always" }];
/** The person's last choice, Auto by default. */
const REVIEW_KEY = "agentexpress.assistant.review";
export function readReview(): ReviewMode {
  try {
    const v = localStorage.getItem(REVIEW_KEY);
    return v === "off" || v === "always" ? v : "auto";
  } catch { return "auto"; }
}
function writeReview(v: ReviewMode): void {
  try { localStorage.setItem(REVIEW_KEY, v); } catch { /* private mode: not remembered */ }
}

/** Expanded: the chat takes the whole build area, the workflow beside it hidden. */
const EXPAND_KEY = "agentexpress.assistant.expanded";
export function readExpanded(): boolean {
  try { return localStorage.getItem(EXPAND_KEY) === "1"; } catch { return false; }
}
function writeExpanded(v: boolean): void {
  try { localStorage.setItem(EXPAND_KEY, v ? "1" : "0"); } catch { /* private mode: not remembered */ }
}

/** The Assistant's description: under the header on a new conversation, and always in its ⓘ. */
export const ASSISTANT_HELP = "Say what you want to build. It drafts the workflow, asks what it needs, and updates the build as you talk. Undo any change from the toolbar.";
/** The one line under the input; the full text is in its ⓘ and its tooltip. */
export function hintShort(maxFiles: number): string {
  return `Enter to send · Shift+Enter for a new line · up to ${maxFiles} files or S3 paths`;
}
export function hintFull(maxFiles: number, s3: string[] = []): string {
  return "Enter sends, Shift+Enter adds a line. Attach files for context, or refer to the S3 path where your files "
    + `reside (s3://bucket/file, or s3://bucket/folder/ for the files in it), up to ${maxFiles} files per message.`
    + (s3.length ? ` S3 paths can be in: ${s3.join(", ")}.` : "")
    + " It never deploys; you do that from Deploy.";
}
/** The muted line under a reviewed reply. */
export function reviewLine(r: DesignTurn["review"]): string {
  if (!r) return "";
  const n = r.count ?? 0;
  const s = `${n} suggestion${n === 1 ? "" : "s"}`;
  return r.status === "ok" ? "Reviewed: no issues"
    : r.status === "applied" ? `Reviewed: ${s} applied`
      : r.status === "noted" ? `Reviewed: ${s} noted`
        : r.status === "skipped" ? "Review skipped (small change)"
          : r.status === "unavailable" ? "Review unavailable" : "";
}

const PHASE: Record<string, string> = {
  thinking: "Thinking…",
  writing: "Writing…",
  applying: "Updating the build…",
  checking: "Checking the change…",
};
/** What the bubble says while it works: while a page is fetched ("reading"), the
 *  "Reading <host>" line the designer added last to the progress list. */
export function phaseLabel(t: Pick<DesignTurn, "phase" | "progress" | "changes">): string {
  if (t.phase === "reading") {
    const line = t.progress?.[t.progress.length - 1] ?? "";
    return line.startsWith("Reading ") ? `${line}…` : "Reading the page…";
  }
  return PHASE[t.phase ?? ""] ?? (t.changes?.length ? PHASE.applying : PHASE.thinking);
}

/** 14:05 today; Sep 8, 14:05 before that. */
export function when(at: string, now = new Date()): string {
  const d = new Date(at);
  if (!at || Number.isNaN(d.getTime())) return "";
  const time = d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  return d.toDateString() === now.toDateString() ? time
    : `${d.toLocaleDateString([], { month: "short", day: "numeric" })}, ${time}`;
}

function Turn({ turn: t }: { turn: DesignTurn }) {
  if (t.role === "user") {
    return (
      <div className="axd-turn axd-user" data-testid="design-turn-user">
        <ChatBubble type="outgoing" ariaLabel={`You at ${when(t.at)}`}
          avatar={<Avatar ariaLabel="You" tooltipText="You" iconName="user-profile" />}>
          <div className="axd-user-text">{t.text}</div>
          {t.attachments?.length ? (
            <div className="axd-attached">
              {t.attachments.map((a) => <span key={a.uri ?? a.name} className="axd-attached-item" title={a.uri ?? a.name}>📎 {a.name}</span>)}
            </div>
          ) : null}
        </ChatBubble>
        <div className="axd-meta">{when(t.at)}</div>
      </div>
    );
  }
  const live = t.status === "thinking";
  const phase = phaseLabel(t);
  // How long this reply has been going: a model's reasoning streams no text, so a long
  // think should at least show that time is passing.
  const started = Date.parse(t.at);
  const secs = live && Number.isFinite(started) ? Math.round((Date.now() - started) / 1000) : 0;
  return (
    <div className="axd-turn axd-assistant" data-testid="design-turn-assistant">
      <ChatBubble type="incoming" ariaLabel={`Designer at ${when(t.at)}`} showLoadingBar={live}
        avatar={<Avatar color="gen-ai" iconName="gen-ai" ariaLabel="Designer" tooltipText="Designer" loading={live} />}
        actions={!live && t.text ? (
          <CopyToClipboard variant="icon" textToCopy={t.text} copyButtonAriaLabel="Copy reply"
            copySuccessText="Reply copied" copyErrorText="Couldn't copy the reply" />
        ) : undefined}>
        {t.status === "error" ? (
          <StatusIndicator type="error">{t.text}</StatusIndicator>
        ) : t.text ? <Markdown text={t.text} streaming={live && t.phase === "writing"} /> : null}
        {(t.changes ?? []).map((c) => (
          <div key={c.revision + c.summary} className="axd-change">
            <StatusIndicator type={c.undo ? "info" : "success"}>{unescape(c.summary) || "Updated the build"}</StatusIndicator>
          </div>
        ))}
        {!live && reviewLine(t.review) ? (
          <Box fontSize="body-s" color="text-body-secondary" data-testid="design-review">{reviewLine(t.review)}</Box>
        ) : null}
        {live && t.phase !== "writing" ? (
          <div className="axd-phase"><StatusIndicator type="loading">{phase}{secs >= 5 ? ` ${secs}s` : ""}</StatusIndicator></div>
        ) : null}
        {live && t.phase === "thinking" && t.thinking ? (
          // The model's reasoning as it streams, so a long think is not a bare spinner.
          <div className="axd-thinking" aria-live="polite" data-testid="design-thinking">{t.thinking}</div>
        ) : null}
        {live && (t.phase === "applying" || t.phase === "checking" || t.phase === "reading") && t.progress?.length ? (
          // Each edit of the change being written, named as it streams in.
          <ul className="axd-progress" aria-live="polite" data-testid="design-progress">
            {t.progress.slice(-8).map((step, i) => <li key={`${i}-${step}`}>{step}</li>)}
          </ul>
        ) : null}
      </ChatBubble>
      {!live ? <div className="axd-meta">{when(t.at)}</div> : null}
    </div>
  );
}

/** What the designer asked the user for that is still open: an item stays until the
 *  path it names has no error left (the user filled it in, by chat or by hand). */
export function openInputs(changes: DesignChange[], issues: Issue[]): { path: string; question: string }[] {
  const errors = issues.filter((i) => i.severity === "error").map((i) => i.path);
  const seen = new Set<string>();
  const out: { path: string; question: string }[] = [];
  for (const c of [...changes].reverse()) {
    for (const n of c.needsInput ?? []) {
      if (seen.has(n.path)) continue;
      seen.add(n.path);
      if (errors.some((p) => p === n.path || p.startsWith(`${n.path}.`) || p.startsWith(`${n.path}[`))) out.push(n);
    }
  }
  return out;
}

/** The secrets the designer asked for, newest first, one per tool or agent, and only
 *  for tools and agents still in the build. */
export function secretsNeeded(changes: DesignChange[], tools: Record<string, unknown>,
  agents: Record<string, unknown>, identities: Record<string, unknown> = {},
  gitTokens: string[] = []): NonNullable<DesignChange["secretsNeeded"]> {
  const out: NonNullable<DesignChange["secretsNeeded"]> = [];
  for (const c of [...changes].reverse()) {
    for (const s of c.secretsNeeded ?? []) {
      const present = s.kind === "toolApiKeys" ? s.name in tools
        : s.kind === "identitySecrets" ? s.name in identities
          : s.kind === "gitTokens" ? gitTokens.includes(s.name) : s.name in agents;
      if (present && !out.some((o) => o.kind === s.kind && o.name === s.name)) out.push(s);
    }
  }
  return out;
}

/** A change's defaults as a list of lines. An older server stored a JSON-array STRING from
 *  the model one character per item; such a record is put back together and read here. */
function defaultLines(v: unknown): string[] {
  const parse = (s: string): string[] => {
    const t = s.trim();
    if (t.startsWith("[")) {
      try {
        const p: unknown = JSON.parse(t);
        if (Array.isArray(p)) return p.map(String);
      } catch {
        // A JSON array cut off part-way (the old server kept only its first 30 characters):
        // keep each whole "…" item, and the last unfinished one with an ellipsis.
        const out: string[] = [];
        const rest = t.slice(1).replace(/"((?:[^"\\]|\\.)*)"\s*,?\s*/g, (_m, s: string) => {
          out.push(s.replace(/\\(.)/g, "$1"));
          return "";
        }).trim();
        const tail = rest.replace(/^"/, "").replace(/\]$/, "").trim();
        if (tail) out.push(`${tail}…`);
        return out.filter((s) => s.trim());
      }
    }
    return t ? [t] : [];
  };
  if (typeof v === "string") return parse(v);
  if (!Array.isArray(v)) return [];
  const items = v.map(String);
  if (items.length > 1 && items.every((s) => s.length <= 1)) return parse(items.join(""));
  return items.filter((s) => s.trim());
}

/** The defaults the designer chose, newest first, without repeats. */
export function defaultsUsed(changes: DesignChange[]): string[] {
  const out: string[] = [];
  for (const c of [...changes].reverse()) for (const d of defaultLines(c.defaultsUsed)) if (!out.includes(d)) out.push(d);
  return out;
}

export function DesignChat({ project, issues, onApplied, flush, onShowProblems, demoUrl }: {
  project: Project;
  issues: Issue[];
  /** A change landed on the server: reload the build. */
  onApplied: () => Promise<void> | void;
  /** Save any edit still waiting for autosave, so the designer sees it. */
  flush: () => Promise<void>;
  onShowProblems: () => void;
  /** A demo video to offer before the first message (Builder: a first visit to a console). */
  demoUrl?: string;
}) {
  const docs = useDocsBase();
  const [doc, setDoc] = useState<DesignDoc | null>(null);
  const [text, setText] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [sending, setSending] = useState(false);
  const [review, setReview] = useState<ReviewMode>(readReview);
  const [expanded, setExpandedState] = useState<boolean>(readExpanded);
  const expandRef = useRef<ButtonProps.Ref>(null);
  const setExpanded = useCallback((v: boolean) => {
    setExpandedState(v);
    writeExpanded(v);
    setTimeout(() => {
      // Expanded, the chat's top comes up under the top bar, so it has the window's height.
      const top = cellRef.current?.getBoundingClientRect().top;
      if (v && top !== undefined && top > 80) window.scrollBy?.({ top: top - 72 });
      // The button changes icon in place; keep focus on it so a keyboard user stays put.
      expandRef.current?.focus();
    }, 0);
  }, []);
  // Esc collapses, unless it is closing a popover or a menu inside the page.
  useEffect(() => {
    if (!expanded) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== "Escape" || e.defaultPrevented) return;
      const t = e.target as Element | null;
      if (t?.closest?.('[role="dialog"], [role="menu"], [aria-haspopup][aria-expanded="true"]')) return;
      setExpanded(false);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [expanded, setExpanded]);
  /** Files picked for the next message, each uploaded as soon as it is picked. */
  const [files, setFiles] = useState<{ file: File; key?: string; name?: string; loading: boolean; error?: string }[]>([]);
  const uploading = files.some((f) => f.loading);
  const pick = (picked: File[]) => {
    const room = Math.max(0, (doc?.attach?.maxFiles ?? 5) - files.length);
    const next = picked.slice(0, room);
    if (!next.length) return;
    setFiles((cur) => [...cur, ...next.map((file) => ({ file, loading: true }))]);
    for (const file of next) {
      designer.attach(id, file).then(
        (got) => setFiles((cur) => cur.map((f) => (f.file === file ? { ...f, ...got, loading: false } : f))),
        (e) => setFiles((cur) => cur.map((f) => (f.file === file ? { ...f, loading: false, error: (e as Error).message } : f))));
    }
  };
  const seen = useRef(0);
  const logRef = useRef<HTMLDivElement>(null);
  const cellRef = useRef<HTMLDivElement>(null);
  const [atBottom, setAtBottom] = useState(true);
  const stick = useRef(true);
  const id = project.id;

  const take = useCallback((d: DesignDoc, first = false) => {
    setDoc(d);
    const n = designChanges(d).length;
    if (!first && n > seen.current) void onApplied();
    seen.current = n;
  }, [onApplied]);

  useEffect(() => {
    setDoc(null);
    setError(null);
    seen.current = 0;
    let alive = true;
    // A build made a moment ago may not be on the server yet: save it first, and try
    // again for a few seconds if its first autosave is still on its way.
    const load = async (tries: number): Promise<void> => {
      try {
        await flush();
        const d = await designer.get(id);
        if (alive) take(d, true);
      } catch (e) {
        if (!alive) return;
        if (tries > 0) { setTimeout(() => void load(tries - 1), 1500); return; }
        setError((e as Error).message);
      }
    };
    void load(5);
    return () => { alive = false; };
  }, [id]); // eslint-disable-line react-hooks/exhaustive-deps

  const thinking = doc?.status === "thinking";
  useEffect(() => {
    if (!thinking) return;
    const t = setInterval(() => {
      designer.get(id).then((d) => take(d)).catch(() => undefined);   // retried next tick
    }, POLL_MS);
    return () => clearInterval(t);
  }, [thinking, id, take]);

  const scrollToEnd = () => {
    const el = scrollParent(logRef.current, cellRef.current);
    if (el) el.scrollTop = el.scrollHeight;
    stick.current = true;
    setAtBottom(true);
  };
  // Captured on the chat's cell: the element that scrolls may be the log's ancestor, and
  // a scroll event does not bubble.
  const onScroll = () => {
    const el = scrollParent(logRef.current, cellRef.current);
    if (!el) return;
    const near = el.scrollHeight - el.scrollTop - el.clientHeight < STICK_PX;
    stick.current = near;
    setAtBottom(near);
  };
  // Follow the reply as it grows, unless the reader has scrolled up to look back.
  const last = doc?.turns[doc.turns.length - 1];
  useLayoutEffect(() => {
    if (stick.current) scrollToEnd();
  }, [doc?.turns.length, last?.text, last?.phase, last?.changes?.length]);
  const send = async (message: string) => {
    const m = message.trim();
    const ready = files.filter((f) => f.key).map((f) => ({ key: f.key!, name: f.name ?? f.file.name }));
    if ((!m && !ready.length) || thinking || sending || uploading) return;
    setSending(true);
    setError(null);
    try {
      await flush();
      setText("");
      take(await designer.send(id, m, ready, review));
      setFiles([]);
      scrollToEnd();
      // And the page: the chat's foot, where the reply grows, can be below the window.
      (cellRef.current?.firstElementChild as HTMLElement | null)?.scrollIntoView?.({ block: "nearest", behavior: "smooth" });
    } catch (e) {
      setText(m);                               // give the message back to edit or resend
      setError((e as Error).message);
    } finally {
      setSending(false);
    }
  };

  const startOver = async () => {
    if (!window.confirm("Start the conversation over? The build stays as it is, and Undo still works.")) return;
    try { take(await designer.reset(id), true); } catch (e) { setError((e as Error).message); }
  };

  const changes = useMemo(() => designChanges(doc), [doc]);
  const latest = changes[changes.length - 1];
  const inputs = useMemo(() => openInputs(changes, issues), [changes, issues]);
  const defaults = useMemo(() => defaultsUsed(changes), [changes]);
  const secrets = useMemo(() => secretsNeeded(changes, project.workflow.tools ?? {}, project.workflow.agents ?? {},
    (project.workflow.identities ?? {}) as Record<string, unknown>, gitTokenNames(project.workflow)),
    [changes, project.workflow]);
  const errors = issues.filter((i) => i.severity === "error").length;
  const reviewText = REVIEW_OPTIONS.find((o) => o.id === review)?.text ?? "Auto";
  const hint = hintFull(doc?.attach?.maxFiles ?? 5, doc?.attach?.s3 ?? []);

  if (!doc) {
    return error ? <StatusIndicator type="error">{error}</StatusIndicator> : <Spinner size="large" />;
  }

  return (
    <div className={`axd-layout${expanded ? " axd-expanded" : ""}`}>
      {/* The chat is as tall as the column beside it (the workflow, and whatever shows
          under it): it fills its cell and scrolls, so it never sets the row's height. */}
      <div className="axd-chat-cell" ref={cellRef} onScrollCapture={onScroll}><div className="axd-chat-fill">
      <Container fitHeight
        header={
          <Header variant="h2"
            description={doc.turns.length ? undefined : ASSISTANT_HELP}
            info={<InfoLink feature="assistant" text={ASSISTANT_HELP} always />}
            actions={
              <SpaceBetween direction="horizontal" size="xs" alignItems="center">
                {doc.turns.length ? <Button iconName="refresh" onClick={() => void startOver()} disabled={thinking}>Start over</Button> : null}
                <Button ref={expandRef} variant="icon" iconName={expanded ? "shrink" : "expand"}
                  ariaLabel={expanded ? "Collapse Assistant" : "Expand Assistant"} ariaExpanded={expanded}
                  onClick={() => setExpanded(!expanded)} data-testid="design-expand" />
              </SpaceBetween>
            }>
            AgentExpress Assistant
          </Header>
        }
        footer={
          <SpaceBetween size="xxs">
            <PromptInput
              value={text} minRows={1} maxRows={6}
              disabled={sending}
              placeholder={doc.turns.length ? "Reply, or ask for a change" : "What would you like to build?"}
              ariaLabel="Message to the designer"
              actionButtonIconName="send"
              actionButtonAriaLabel={thinking ? "Wait for the reply to finish" : "Send message"}
              disableActionButton={(!text.trim() && !files.some((f) => f.key)) || thinking || sending || uploading}
              onChange={({ detail }) => setText(detail.value)}
              onAction={() => void send(text)}
              secondaryActions={
                <div className="axd-toolbar">
                  <FileInput variant="icon" multiple value={[]} ariaLabel="Attach files"
                    accept={(doc.attach?.types ?? []).map((t) => `.${t}`).join(",")}
                    onChange={({ detail }) => pick(detail.value)}>Attach files</FileInput>
                  <span className="axd-review" data-testid="design-review-control">
                    <ButtonDropdown variant="normal" expandToViewport
                      ariaLabel={`${REVIEW_LABEL}: ${reviewText}`}
                      items={REVIEW_OPTIONS.map((o) => ({ id: o.id, text: o.text, itemType: "checkbox" as const, checked: o.id === review }))}
                      onItemClick={({ detail }) => { const v = detail.id as ReviewMode; setReview(v); writeReview(v); }}>
                      {`Review: ${reviewText}`}
                    </ButtonDropdown>
                    <InfoLink feature="assistant.review" title={REVIEW_LABEL} text={REVIEW_HELP} always />
                  </span>
                </div>
              }
              secondaryContent={files.length ? (
                <FileTokenGroup alignment="horizontal" showFileSize
                  items={files.map((f) => ({ file: f.file, loading: f.loading, errorText: f.error ?? null }))}
                  onDismiss={({ detail }) => setFiles((cur) => cur.filter((_, i) => i !== detail.fileIndex))}
                  i18nStrings={{ removeFileAriaLabel: (_i, n) => `Remove ${n}`, errorIconAriaLabel: "Error",
                    limitShowFewer: "Show fewer", limitShowMore: "Show more", formatFileSize: (b) => `${(b / 1e6).toFixed(2)} MB` }} />
              ) : undefined}
            />
            <div className="axd-hint">
              <span title={hint} data-testid="design-hint">{hintShort(doc.attach?.maxFiles ?? 5)}</span>
              <InfoLink feature="assistant" title="Sending messages" text={hint} always />
            </div>
            {error ? <StatusIndicator type="error">{error}</StatusIndicator> : null}
          </SpaceBetween>
        }
      >
        <div className="axd-chat">
          <div className="axd-log" ref={logRef} aria-live="polite" aria-busy={thinking}>
            {!doc.turns.length ? (
              <div className="axd-empty">
                <SpaceBetween size="m">
                  <Box variant="h3">What would you like to build?</Box>
                  <Box color="text-body-secondary">
                    Describe the work in your own words: what comes in, what should come out, and where a person
                    should check it. Anything you leave open gets a sensible default, and it tells you which.
                  </Box>
                  {demoUrl ? (
                    <Box fontSize="body-s" data-testid="design-demo-video">
                      New here? <Link href={demoUrl} external variant="primary" fontSize="body-s">Watch the 10-minute demo</Link>
                    </Box>
                  ) : null}
                  <SupportPromptGroup
                    ariaLabel="Starting points"
                    alignment="horizontal"
                    items={doc.starters.map((s) => ({ id: s.title, text: s.title }))}
                    onItemClick={({ detail }) => {
                      const s = doc.starters.find((x) => x.title === detail.id);
                      if (s) void send(s.message);
                    }}
                  />
                </SpaceBetween>
              </div>
            ) : doc.turns.map((t, i) => (
              <div key={t.id}>
                {doc.summarized && i === doc.summarized ? (
                  <div className="axd-meta axd-summarized" role="note">
                    The designer remembers the {doc.summarized} messages above as a summary of what was decided.
                  </div>
                ) : null}
                <Turn turn={t} />
              </div>
            ))}
          </div>
          {!atBottom ? (
            <div className="axd-jump">
              <Button iconName="angle-down" onClick={() => scrollToEnd()}>Jump to latest</Button>
            </div>
          ) : null}
        </div>
      </Container>
      </div></div>

      {expanded ? null : <SpaceBetween size="m">
        <Container header={<Header variant="h2"
          description={latest ? `Last change: ${unescape(latest.summary)}` : "The build, as the conversation shapes it."}
          info={<><StatusIndicator type={errors ? "warning" : "success"}>{errors ? `${errors} to fix` : "Valid"}</StatusIndicator>
            <InfoLink feature="assistant.workflow" /></>}>
          Workflow</Header>}>
          <div className="axd-preview">
            <Canvas workflow={project.workflow} issues={issues} selection={null} onSelect={() => undefined}
              onDropPayload={() => undefined} onMoveAgent={() => undefined}
              highlight={latest && !latest.undo ? latest.changed.agents : []} readOnly />
          </div>
        </Container>
        {secrets.length ? (
          <Container header={<Header variant="h3" description="Enter these here, never in the chat. Each is stored encrypted with this build, used only by the Gateway or the agent that needs it, and never shown again."
            info={infoSlot(docs, "assistant.secrets")}>Secrets</Header>}>
            <SpaceBetween size="m">
              {secrets.map((s) => (
                <SecretField key={`${s.kind}:${s.name}`} buildId={project.id} kind={s.kind} name={s.name}
                  label={`${s.label} for ${s.name}`} description={s.why} />
              ))}
            </SpaceBetween>
          </Container>
        ) : null}
        {inputs.length ? (
          <Container header={<Header variant="h3" description="Only you can give these. Reply in the chat, or fill them in under Build manually. Deploy waits for them."
            info={infoSlot(docs, "assistant.inputs")}>Needs your input</Header>}>
            <ul className="axd-list">
              {inputs.map((n) => <li key={n.path}><code>{n.path}</code> — {n.question}</li>)}
            </ul>
          </Container>
        ) : null}
        {defaults.length ? (
          <Container header={<Header variant="h3" description="Chosen for you where you didn't say. Ask for something else, or change it by hand."
            info={infoSlot(docs, "assistant.defaults")}>Defaults used</Header>}>
            <ul className="axd-list">{defaults.map((d) => <li key={d}>{d}</li>)}</ul>
          </Container>
        ) : null}
        {errors ? (
          <Box>
            <Button variant="inline-link" onClick={onShowProblems}>See the {errors} problem{errors > 1 ? "s" : ""} in Build manually</Button>
          </Box>
        ) : null}
      </SpaceBetween>}
    </div>
  );
}
