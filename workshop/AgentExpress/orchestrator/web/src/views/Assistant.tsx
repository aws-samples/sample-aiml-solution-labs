/** The in-app assistant.
 *
 *  A Bedrock Converse tool-use loop lives in the BFF (bff/chatbot.py); this is just its
 *  surface. It can TAKE ACTIONS — approve a gate, re-run an agent, start an evaluation —
 *  through the same routes the buttons use, and it is held to the same `authorization`
 *  rules: the BFF withholds the action tools from a caller who lacks the group, so the
 *  assistant is not a way around a gate.
 *
 *  A DOCKED POPUP, not a Modal. The first version was a full Modal opened by a labelled
 *  button, which meant two problems at once: the label ("Run Assistant", from
 *  workflow.json) sat in the bottom-right corner of every page as a slab of text, and
 *  opening the assistant took the whole screen away — so you could not read the run you
 *  were asking about. A chat bubble that expands into a panel beside the content is the
 *  established shape for this, and it keeps the page visible.
 *
 *  Every string still comes from workflow.json `ui` / `orchestrator.chatbot`, so a
 *  customer's wording appears with no code change. The difference is that the wording is
 *  now inside the panel, where it reads as a heading, instead of on the trigger. */

import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Container from "@cloudscape-design/components/container";
import Header from "@cloudscape-design/components/header";
import Input from "@cloudscape-design/components/input";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Spinner from "@cloudscape-design/components/spinner";
import StatusIndicator from "@cloudscape-design/components/status-indicator";
import { useEffect, useRef, useState } from "react";

import { api } from "../api";
import { Markdown } from "../builder/markdown";
import type { ChatbotConfig, UiConfig } from "../types";
import "./assistant.css";

interface Turn { role: "user" | "assistant"; content: string }
interface ChatReply { reply?: string; actions?: string[] | null }

/** The bubble's glyph. Inline rather than a Cloudscape icon because the bubble is a
 *  plain button (see assistant.css) and Cloudscape's Icon expects to size itself to a
 *  component's type scale. */
function ChatGlyph() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path d="M21 12a8 8 0 0 1-8 8H8l-4 3v-4.3A8 8 0 0 1 13 4a8 8 0 0 1 8 8Z" />
      <path d="M9 11h8M9 15h5" />
    </svg>
  );
}

function CloseGlyph() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path d="M6 6l12 12M18 6 6 18" />
    </svg>
  );
}

export function Assistant({
  ui, chatbot, sessionId, onActed, build = "",
}: {
  ui: UiConfig;
  chatbot: ChatbotConfig | null | undefined;
  sessionId: string | null;
  onActed: () => void;
  /** A deployed build to work on ("" = this deployment's own workflow). */
  build?: string;
}) {
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState("");
  const [busy, setBusy] = useState(false);
  const [turns, setTurns] = useState<Turn[]>([]);
  const [actions, setActions] = useState<string[]>([]);
  /** A reply arrived while the popup was closed. Reachable: closing the popup does not
   *  cancel the request, and the answer is worth a dot rather than nothing. */
  const [unread, setUnread] = useState(false);
  const openRef = useRef(false);
  openRef.current = open;
  const endRef = useRef<HTMLDivElement>(null);

  // The assistant is a config flag, so a deployment can ship without it.
  const enabled = chatbot?.enabled !== false && chatbot != null;

  useEffect(() => {
    if (open) endRef.current?.scrollIntoView({ block: "end" });
  }, [turns, busy, open]);

  // Escape closes the popup, which is what a docked panel is expected to do and what a
  // keyboard user reaches for first.
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") setOpen(false); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open]);

  if (!enabled) return null;

  const title = ui.assistantTitle || "Assistant";
  const subtitle = ui.assistantSubtitle
    || "Ask about a run, or tell me to do something.";

  async function send() {
    const message = draft.trim();
    if (!message || busy) return;
    setDraft("");
    const history = turns;
    setTurns([...history, { role: "user", content: message }]);
    setBusy(true);
    try {
      const data = await api.post<ChatReply>("/api/chat", {
        message,
        history,
        session_id: sessionId ?? "",
        ...(build ? { build } : {}),
      });
      setTurns((t) => [...t, { role: "assistant", content: data.reply || "(no response)" }]);
      if (!openRef.current) setUnread(true);
      if (data.actions?.length) {
        setActions(data.actions);
        // An action changed server state, so the page has to catch up.
        onActed();
      }
    } catch (e) {
      setTurns((t) => [...t, {
        role: "assistant",
        content: `I could not reach the assistant: ${e instanceof Error ? e.message : String(e)}`,
      }]);
      if (!openRef.current) setUnread(true);
    } finally {
      setBusy(false);
    }
  }

  return (
    <>
      {open ? (
        <div className="asst-dock" role="dialog" aria-label={title}>
          <Container
            header={
              <Header
                variant="h3"
                description={subtitle}
                actions={
                  <Button
                    variant="icon" iconName="close" ariaLabel="Close the assistant"
                    onClick={() => setOpen(false)}
                  />
                }
              >
                {title}
              </Header>
            }
            footer={
              <SpaceBetween direction="horizontal" size="xs">
                <div style={{ flex: 1, minWidth: 240 }}>
                  <Input
                    value={draft}
                    placeholder={chatbot?.placeholder || "Ask about a run…"}
                    disabled={busy}
                    ariaLabel={`Message ${title}`}
                    onChange={({ detail }) => setDraft(detail.value)}
                    onKeyDown={({ detail }) => { if (detail.key === "Enter") void send(); }}
                  />
                </div>
                <Button
                  variant="primary" iconName="send" loading={busy}
                  disabled={!draft.trim()} ariaLabel="Send"
                  onClick={() => void send()}
                />
              </SpaceBetween>
            }
          >
            <div className="asst-log">
              <SpaceBetween size="s">
                {turns.length === 0 ? (
                  <Box color="text-status-inactive" variant="p">
                    {chatbot?.greeting
                      || "Ask about status, cost, latency, outputs, guardrails or evaluations — "
                         + "or tell me to approve a gate, re-run an agent, or run an evaluation."}
                  </Box>
                ) : null}

                {turns.map((t, i) => (
                  <div
                    key={i}
                    className={`asst-turn${t.role === "user" ? " asst-turn-user" : ""}`}
                  >
                    <Box variant="small" color="text-status-inactive">
                      {t.role === "user" ? "You" : title}
                    </Box>
                    {/* The assistant answers in Markdown (bold, lists, code); what the user
                        typed is shown as typed. */}
                    {t.role === "user" ? <span className="asst-turn-text">{t.content}</span>
                      : <div className="asst-turn-md"><Markdown text={t.content} /></div>}
                  </div>
                ))}

                {busy ? <Box><Spinner /> Thinking…</Box> : null}

                {actions.length > 0 ? (
                  <StatusIndicator type="success">
                    {`Actions taken: ${actions.join(", ")}`}
                  </StatusIndicator>
                ) : null}

                <div ref={endRef} />
              </SpaceBetween>
            </div>
          </Container>
        </div>
      ) : null}

      {/* The trigger: a bubble and nothing else. The name lives in the panel's header
          and in this button's accessible name, not as visible text in the corner. */}
      <button
        type="button"
        className="asst-bubble"
        aria-label={open ? `Close ${title}` : title}
        aria-expanded={open}
        title={open ? `Close ${title}` : title}
        onClick={() => { setOpen((o) => !o); setUnread(false); }}
      >
        {open ? <CloseGlyph /> : <ChatGlyph />}
        {!open && unread ? <span className="asst-bubble-dot" /> : null}
      </button>
    </>
  );
}
