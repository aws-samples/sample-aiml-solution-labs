/** The Flow tab's Story view: the run told top to bottom, like a chat timeline.
 *
 *  One numbered header per stage (the numbers are the Graph view's badges), the agents of a
 *  parallel or sequence stage under it, and one short line per thing that happened: an
 *  emoji, a plain sentence, then time, cost and outcome. It fits the width and scrolls one
 *  way only; no columns, no arrows. The layout is lib/story.ts.
 *
 *  The replay controls are RunFlow's: lines appear as the replay reaches them, the current
 *  one is highlighted and scrolled into view. Every line is a button: click or Enter opens
 *  its detail under it; Arrow Up / Arrow Down move between lines and agent headers. Nothing
 *  animates with prefers-reduced-motion (story.css), and scrolling is never smooth. */
import Box from "@cloudscape-design/components/box";
import LiveRegion from "@cloudscape-design/components/live-region";
import ToggleButton from "@cloudscape-design/components/toggle-button";
import { type KeyboardEvent, useEffect, useMemo, useRef, useState } from "react";
import { type FlowEvent, replayAt } from "../lib/flow";
import { statusLabel } from "../lib/status";
import {
  KIND_LABEL, type StoryAgent, type StoryFilter, type StoryLine, type StoryStage, buildStory,
  filterStory, lineCount, money, span, stageLines, stageStatus,
} from "../lib/story";
import type { HistoryEntry, LogLine, NodeState, Workflow } from "../types";
import { StepBadge } from "./StepBadge";
import "./story.css";

/** Up to this many agents in a stage open by default; more start collapsed. */
const OPEN_UP_TO = 4;

const STATUS_EMOJI: Record<string, string> = {
  done: "✅", failed: "❌", denied: "⛔", waiting_human: "⏸", running: "⏳", revise: "🔁",
  skipped: "⏭️", cancelled: "⏹️", cancelling: "⏹️", pending: "⚪",
};

function Status({ status, durationMs }: { status: string; durationMs: number | null }) {
  return (
    <span className="st-status">
      {status ? <span aria-hidden="true">{STATUS_EMOJI[status] ?? "⚪"}</span> : null}
      {status ? <span className="st-status-word">{statusLabel(status)}</span> : null}
      {durationMs !== null && durationMs > 0 ? <span className="st-status-time">{span(durationMs)}</span> : null}
    </span>
  );
}

export function RunStory({
  events, workflow, nodes, logs, hitl, history, at, total, highlight, picked, onPick,
}: {
  events: FlowEvent[];
  workflow: Workflow;
  nodes: Record<string, NodeState>;
  logs?: LogLine[];
  hitl?: { node: string; kind?: string } | null;
  history?: Record<string, HistoryEntry[]>;
  /** The replay position: how many events have happened. */
  at: number;
  total: number;
  /** Mark the current line: the reader is replaying, or has used a control. */
  highlight: boolean;
  picked: number | null;
  onPick: (index: number | null) => void;
}) {
  const story = useMemo(() => buildStory({ events, workflow, nodes, logs, hitl, history }),
    [events, workflow, nodes, logs, hitl, history]);
  const [filter, setFilter] = useState<StoryFilter>("all");
  const shown = useMemo(() => filterStory(story, filter), [story, filter]);
  const [said, setSaid] = useState("");

  const atEnd = at >= total;
  const phases = useMemo(() => replayAt(events, at).agents, [events, at]);
  // At the end of the replay an agent shows the run's real status; before it, only what the
  // events so far support: not started, in progress, or done with its events.
  const statusOf = (a: StoryAgent): string => {
    if (a.id === "" || atEnd) return a.status;
    const p = phases[a.id]?.phase;
    return !p || p === "pending" ? "pending" : p === "running" ? "running" : "done";
  };

  const [selected, setSelected] = useState<string | null>(null);
  useEffect(() => { setSelected(picked === null ? null : `e${picked}`); }, [picked]);

  // Agents the reader opened or closed; the rest follow the default.
  const [toggled, setToggled] = useState<Record<string, boolean>>({});
  const currentKey = highlight && at > 0 ? `e${at - 1}` : "";
  const currentAgent = highlight && at > 0 ? events[at - 1]?.agent : undefined;
  const isOpen = (stage: StoryStage, a: StoryAgent) => {
    const k = `${stage.key}/${a.id}`;
    if (k in toggled) return toggled[k];
    if (currentAgent !== undefined && currentAgent === a.id) return true;
    return stage.agents.length <= OPEN_UP_TO || a.problem;
  };

  const lineEls = useRef(new Map<string, HTMLElement>());
  useEffect(() => {
    if (!currentKey) return;
    lineEls.current.get(currentKey)?.scrollIntoView?.({ block: "nearest" });
  }, [currentKey]);

  const rootEl = useRef<HTMLDivElement>(null);
  const onKeyDown = (ev: KeyboardEvent<HTMLDivElement>) => {
    if (ev.key !== "ArrowDown" && ev.key !== "ArrowUp") return;
    const list = Array.from(rootEl.current?.querySelectorAll<HTMLElement>("[data-story-nav]") ?? []);
    const i = list.indexOf(document.activeElement as HTMLElement);
    if (i < 0) return;
    ev.preventDefault();
    list[Math.max(0, Math.min(list.length - 1, i + (ev.key === "ArrowDown" ? 1 : -1)))]?.focus();
  };

  const pick = (filterId: StoryFilter) => {
    setFilter(filterId);
    const n = lineCount(filterStory(story, filterId));
    setSaid(`${n} ${n === 1 ? "line" : "lines"} shown`);
  };

  const renderLine = (l: StoryLine) => {
    if (l.at > at) return null;
    if (l.type === "version") {
      return (
        <li key={l.key} className="st-version" data-testid={`story-${l.key}`}>
          <span aria-hidden="true">{l.emoji} </span>{l.text}
        </li>
      );
    }
    const before = l.doneAt !== undefined && at < l.doneAt;
    const text = before ? l.pendingText ?? l.text : l.text;
    const tone = before ? "waiting" : l.tone;
    const open = selected === l.key;
    const current = l.key === currentKey;
    const detailId = `st-detail-${l.key}`;
    return (
      <li key={l.key} className={`st-line st-tone-${tone}${current ? " st-current" : ""}${l.problem && !before ? " st-problem" : ""}`}>
        <button
          type="button" className="st-line-btn" data-story-nav="" title={l.title}
          aria-expanded={open} aria-controls={open ? detailId : undefined}
          data-testid={`story-line-${l.key}`}
          ref={(el) => { if (el) lineEls.current.set(l.key, el); else lineEls.current.delete(l.key); }}
          onClick={() => {
            const next = open ? null : l.key;
            setSelected(next);
            if (l.event) onPick(next === null ? null : l.event.index);
          }}
        >
          <span className="st-emoji" aria-hidden="true">{l.emoji}</span>
          <span className="st-text">{text}</span>
          {l.meta.length ? <span className="st-meta">{l.meta.join(" · ")}</span> : null}
        </button>
        {open ? (
          <div className="st-detail rf-detail" id={detailId} data-testid="flow-detail">
            {l.event ? (
              <>
                <Box variant="p">{l.event.sentence}</Box>
                <Box color="text-body-secondary" fontSize="body-s">
                  {`Latency ${span(l.event.latencyMs)} · Cost ${money(l.event.costUsd)}`}
                  {l.event.kind === "llm" || l.event.kind === "image" ? ` · Model ${l.event.label || "not recorded"}` : ""}
                </Box>
              </>
            ) : <Box variant="p">{text}</Box>}
          </div>
        ) : null}
      </li>
    );
  };

  const renderAgent = (stage: StoryStage, a: StoryAgent) => {
    const open = isOpen(stage, a);
    const id = `st-agent-${stage.key}-${a.id || "run"}`;
    const lines = a.lines.filter((l) => l.at <= at);
    return (
      <li key={a.id || "run"} className={`st-agent${a.problem ? " st-agent-problem" : ""}`} data-testid={`story-agent-${a.id || "run"}`}>
        <button
          type="button" className="st-agent-head" data-story-nav=""
          aria-expanded={open} aria-controls={id}
          onClick={() => setToggled((t) => ({ ...t, [`${stage.key}/${a.id}`]: !open }))}
        >
          <span className="st-chevron" aria-hidden="true">{open ? "▾" : "▸"}</span>
          {a.num ? <StepBadge num={a.num} status={statusOf(a)} /> : null}
          <span className="st-name">
            {a.name} <span aria-hidden="true">{a.emoji}</span>
            {a.role ? <span className="st-role">{a.role}</span> : null}
          </span>
          <Status status={statusOf(a)} durationMs={a.durationMs} />
        </button>
        {open ? (
          <ul className="st-lines" id={id}>
            {lines.length ? lines.map(renderLine) : (
              <li className="st-none">{atEnd ? "Nothing recorded." : "Nothing yet."}</li>
            )}
          </ul>
        ) : null}
      </li>
    );
  };

  const t = story.totals;
  return (
    <div className="st" data-testid="run-story" ref={rootEl} onKeyDown={onKeyDown}>
      <ul className="st-totals" aria-label="Run totals">
        {t.durationMs !== null ? <li><span aria-hidden="true">⏱ </span>{`Took ${span(t.durationMs)}`}</li> : null}
        <li><span aria-hidden="true">💲 </span>{`Cost ${money(t.costUsd)}`}</li>
        <li><span aria-hidden="true">📞 </span>{`${t.calls} ${t.calls === 1 ? "call" : "calls"}`}</li>
        {t.gates ? (
          <li><span aria-hidden="true">⏸ </span>
            {`${t.gates} HITL ${t.gates === 1 ? "review" : "reviews"}${t.waitMs > 0 ? `, waited ${span(t.waitMs)}` : ""}`}</li>
        ) : null}
      </ul>
      <div className="st-chips" role="group" aria-label="Show">
        <ToggleButton pressed={filter === "all"} onChange={() => pick("all")}>All</ToggleButton>
        <ToggleButton pressed={filter === "problems"} onChange={() => pick(filter === "problems" ? "all" : "problems")}>
          Problems only
        </ToggleButton>
        {story.kinds.map((k) => (
          <ToggleButton key={k} pressed={filter === k} onChange={() => pick(filter === k ? "all" : k)}>
            {KIND_LABEL[k] ?? k}
          </ToggleButton>
        ))}
      </div>
      <LiveRegion>{said}</LiveRegion>

      {shown.stages.length === 0 ? (
        <Box color="text-status-inactive" padding="m">
          {filter === "problems" ? "No problems in this run: nothing failed, was denied or was blocked." : "Nothing of this kind in this run."}
        </Box>
      ) : (
        <ol className="st-stages">
          {shown.stages.map((stage) => {
            const statuses = stage.agents.map(statusOf);
            const status = stage.kind === "extra" ? "" : stageStatus(statuses);
            const single = stage.kind === "single" ? stage.agents[0] : undefined;
            return (
              <li key={stage.key} className="st-stage" data-testid={`story-stage-${stage.key}`}>
                <div className="st-stage-head">
                  {stage.num ? <StepBadge num={stage.num} status={status} /> : <span className="st-dot" aria-hidden="true" />}
                  <span className="st-stage-title">
                    {single ? (
                      <>
                        {single.name} <span aria-hidden="true">{single.emoji}</span>
                        {single.role ? <span className="st-role">{single.role}</span> : null}
                      </>
                    ) : stage.title}
                  </span>
                  <Status status={status} durationMs={stage.durationMs} />
                </div>
                {single ? (
                  <ul className="st-lines">{stageLines(stage).map(renderLine)}</ul>
                ) : (
                  <>
                    {stage.agents.length ? <ul className="st-agents">{stage.agents.map((a) => renderAgent(stage, a))}</ul> : null}
                    {stage.lines.length ? <ul className="st-lines">{stage.lines.map(renderLine)}</ul> : null}
                  </>
                )}
              </li>
            );
          })}
        </ol>
      )}
    </div>
  );
}
