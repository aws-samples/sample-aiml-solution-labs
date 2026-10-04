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
import Button from "@cloudscape-design/components/button";
import Container from "@cloudscape-design/components/container";
import CopyToClipboard from "@cloudscape-design/components/copy-to-clipboard";
import FileInput from "@cloudscape-design/components/file-input";
import FileTokenGroup from "@cloudscape-design/components/file-token-group";
import Header from "@cloudscape-design/components/header";
import PromptInput from "@cloudscape-design/components/prompt-input";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Spinner from "@cloudscape-design/components/spinner";
import StatusIndicator from "@cloudscape-design/components/status-indicator";
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { SecretField } from "./BuildResources";
import { Canvas } from "./Canvas";
import { Markdown } from "./markdown";
import type { Project } from "./model";
import { designChanges, designer, type DesignChange, type DesignDoc, type DesignTurn } from "./storage";
import type { Issue } from "./validate";

export const POLL_MS = 700;
/** Within this many pixels of the bottom, a new line scrolls the log with it; further up,
 *  the reader is looking back, so the log stays put and offers "Jump to latest". */
const STICK_PX = 80;

const PHASE: Record<string, string> = {
  thinking: "Thinking…",
  writing: "Writing…",
  applying: "Updating the build…",
  checking: "Checking the change…",
};

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
  const phase = PHASE[t.phase ?? ""] ?? (t.changes?.length ? PHASE.applying : PHASE.thinking);
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
            <StatusIndicator type={c.undo ? "info" : "success"}>{c.summary || "Updated the build"}</StatusIndicator>
          </div>
        ))}
        {live && t.phase !== "writing" ? (
          <div className="axd-phase"><StatusIndicator type="loading">{phase}{secs >= 5 ? ` ${secs}s` : ""}</StatusIndicator></div>
        ) : null}
        {live && t.phase === "thinking" && t.thinking ? (
          // The model's reasoning as it streams, so a long think is not a bare spinner.
          <div className="axd-thinking" aria-live="polite" data-testid="design-thinking">{t.thinking}</div>
        ) : null}
        {live && (t.phase === "applying" || t.phase === "checking") && t.progress?.length ? (
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
  agents: Record<string, unknown>, identities: Record<string, unknown> = {}): NonNullable<DesignChange["secretsNeeded"]> {
  const out: NonNullable<DesignChange["secretsNeeded"]> = [];
  for (const c of [...changes].reverse()) {
    for (const s of c.secretsNeeded ?? []) {
      const present = s.kind === "toolApiKeys" ? s.name in tools
        : s.kind === "identitySecrets" ? s.name in identities : s.name in agents;
      if (present && !out.some((o) => o.kind === s.kind && o.name === s.name)) out.push(s);
    }
  }
  return out;
}

/** The defaults the designer chose, newest first, without repeats. */
export function defaultsUsed(changes: DesignChange[]): string[] {
  const out: string[] = [];
  for (const c of [...changes].reverse()) for (const d of c.defaultsUsed ?? []) if (!out.includes(d)) out.push(d);
  return out;
}

export function DesignChat({ project, issues, onApplied, flush, onShowProblems }: {
  project: Project;
  issues: Issue[];
  /** A change landed on the server: reload the build. */
  onApplied: () => Promise<void> | void;
  /** Save any edit still waiting for autosave, so the designer sees it. */
  flush: () => Promise<void>;
  onShowProblems: () => void;
}) {
  const [doc, setDoc] = useState<DesignDoc | null>(null);
  const [text, setText] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [sending, setSending] = useState(false);
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
    const el = logRef.current;
    if (el) el.scrollTop = el.scrollHeight;
    stick.current = true;
    setAtBottom(true);
  };
  const onScroll = () => {
    const el = logRef.current;
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
      take(await designer.send(id, m, ready));
      setFiles([]);
      scrollToEnd();
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
    (project.workflow.identities ?? {}) as Record<string, unknown>),
    [changes, project.workflow.tools, project.workflow.agents]);
  const errors = issues.filter((i) => i.severity === "error").length;

  if (!doc) {
    return error ? <StatusIndicator type="error">{error}</StatusIndicator> : <Spinner size="large" />;
  }

  return (
    <div className="axd-layout">
      {/* The chat is as tall as the column beside it (the workflow, and whatever shows
          under it): it fills its cell and scrolls, so it never sets the row's height. */}
      <div className="axd-chat-cell"><div className="axd-chat-fill">
      <Container fitHeight
        header={
          <Header variant="h2"
            description="Say what you want to build. It drafts the workflow, asks what it needs, and updates the build as you talk. Undo any change from the toolbar."
            actions={doc.turns.length ? <Button iconName="refresh" onClick={() => void startOver()} disabled={thinking}>Start over</Button> : undefined}>
            AgentExpress Assistant
          </Header>
        }
        footer={
          <SpaceBetween size="xs">
            <PromptInput
              value={text} minRows={2} maxRows={10}
              disabled={sending}
              placeholder={doc.turns.length ? "Reply, or ask for a change" : "What would you like to build?"}
              ariaLabel="Message to the designer"
              actionButtonIconName="send"
              actionButtonAriaLabel={thinking ? "Wait for the reply to finish" : "Send message"}
              disableActionButton={(!text.trim() && !files.some((f) => f.key)) || thinking || sending || uploading}
              onChange={({ detail }) => setText(detail.value)}
              onAction={() => void send(text)}
              secondaryActions={
                <FileInput variant="icon" multiple value={[]} ariaLabel="Attach files"
                  accept={(doc.attach?.types ?? []).map((t) => `.${t}`).join(",")}
                  onChange={({ detail }) => pick(detail.value)}>Attach files</FileInput>
              }
              secondaryContent={files.length ? (
                <FileTokenGroup alignment="horizontal" showFileSize
                  items={files.map((f) => ({ file: f.file, loading: f.loading, errorText: f.error ?? null }))}
                  onDismiss={({ detail }) => setFiles((cur) => cur.filter((_, i) => i !== detail.fileIndex))}
                  i18nStrings={{ removeFileAriaLabel: (_i, n) => `Remove ${n}`, errorIconAriaLabel: "Error",
                    limitShowFewer: "Show fewer", limitShowMore: "Show more", formatFileSize: (b) => `${(b / 1e6).toFixed(2)} MB` }} />
              ) : undefined}
            />
            <Box fontSize="body-s" color="text-body-secondary">
              Enter sends, Shift+Enter adds a line. Attach files for context, or refer to the S3 path where
              your files reside (s3://bucket/file, or s3://bucket/folder/ for the files in it), up to{" "}
              {doc.attach?.maxFiles ?? 5} files per message.
              {doc.attach?.s3?.length ? <> S3 paths can be in: {doc.attach.s3.join(", ")}.</> : null} It never
              deploys; you do that from Deploy.
            </Box>
            {error ? <StatusIndicator type="error">{error}</StatusIndicator> : null}
          </SpaceBetween>
        }
      >
        <div className="axd-chat">
          <div className="axd-log" ref={logRef} onScroll={onScroll} aria-live="polite" aria-busy={thinking}>
            {!doc.turns.length ? (
              <div className="axd-empty">
                <SpaceBetween size="m">
                  <Box variant="h3">What would you like to build?</Box>
                  <Box color="text-body-secondary">
                    Describe the work in your own words: what comes in, what should come out, and where a person
                    should check it. Anything you leave open gets a sensible default, and it tells you which.
                  </Box>
                  <SupportPromptGroup
                    ariaLabel="Starting points"
                    alignment="vertical"
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

      <SpaceBetween size="m">
        <Container header={<Header variant="h2"
          description={latest ? `Last change: ${latest.summary}` : "The build, as the conversation shapes it."}
          info={<StatusIndicator type={errors ? "warning" : "success"}>{errors ? `${errors} to fix` : "Valid"}</StatusIndicator>}>
          Workflow</Header>}>
          <div className="axd-preview">
            <Canvas workflow={project.workflow} issues={issues} selection={null} onSelect={() => undefined}
              onDropPayload={() => undefined} onMoveAgent={() => undefined}
              highlight={latest && !latest.undo ? latest.changed.agents : []} readOnly />
          </div>
        </Container>
        {secrets.length ? (
          <Container header={<Header variant="h3" description="Enter these here, never in the chat. Each is stored encrypted with this build, used only by the Gateway or the agent that needs it, and never shown again.">Secrets</Header>}>
            <SpaceBetween size="m">
              {secrets.map((s) => (
                <SecretField key={`${s.kind}:${s.name}`} buildId={project.id} kind={s.kind} name={s.name}
                  label={`${s.label} for ${s.name}`} description={s.why} />
              ))}
            </SpaceBetween>
          </Container>
        ) : null}
        {inputs.length ? (
          <Container header={<Header variant="h3" description="Only you can give these. Reply in the chat, or fill them in under Build manually. Deploy waits for them.">Needs your input</Header>}>
            <ul className="axd-list">
              {inputs.map((n) => <li key={n.path}><code>{n.path}</code> — {n.question}</li>)}
            </ul>
          </Container>
        ) : null}
        {defaults.length ? (
          <Container header={<Header variant="h3" description="Chosen for you where you didn't say. Ask for something else, or change it by hand.">Defaults used</Header>}>
            <ul className="axd-list">{defaults.map((d) => <li key={d}>{d}</li>)}</ul>
          </Container>
        ) : null}
        {errors ? (
          <Box>
            <Button variant="inline-link" onClick={onShowProblems}>See the {errors} problem{errors > 1 ? "s" : ""} in Build manually</Button>
          </Box>
        ) : null}
      </SpaceBetween>
    </div>
  );
}
