/** The Flow tab of a run: what happened, in order, and a replay of it.
 *
 *  Quiet by default. Collapsed, a reader sees the run's agents in its own order, each with
 *  its status and one line of counts, and ONE rail beside them listing the AgentCore pieces
 *  the run touched. Opening an agent shows an edge from it to each piece it used, with one
 *  small mark per event on that edge. Selecting a mark shows its sentence, latency and cost,
 *  and nothing else.
 *
 *  The events are the telemetry rows the runtime already writes, read WITHOUT their prompt
 *  and answer text (GET /api/sessions/{id}/flow). They are stored, so replay is exact; the
 *  run itself is polled, not streamed, so "live" only means the latest state. The tab
 *  re-reads the events when the run's snapshot changes, and a viewer at the end stays at
 *  the end (lib/flow.ts, replayReducer).
 *
 *  Keyboard and screen readers: every mark, the agent boxes and every replay control are
 *  real buttons or a native range input; stepping announces the event in a live region.
 *  With `prefers-reduced-motion`, automatic playback is off and nothing animates: the reader
 *  steps or scrubs instead.
 *
 *  Two views of the same events and the same replay: Story (the default; RunStory.tsx, the
 *  run told top to bottom, stage by stage) and Summary (the agents and the rail, below).
 */
import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import LiveRegion from "@cloudscape-design/components/live-region";
import SegmentedControl from "@cloudscape-design/components/segmented-control";
import Slider from "@cloudscape-design/components/slider";
import Spinner from "@cloudscape-design/components/spinner";
import {
  useCallback, useEffect, useLayoutEffect, useMemo, useReducer, useRef, useState,
} from "react";
import { api } from "../api";
import {
  COMPONENTS, type Component, type FlowEvent, type FlowResponse, type Tone, buildEvents,
  initialReplay, railOf, replayAt, replayReducer,
} from "../lib/flow";
import { statusLabel } from "../lib/status";
import { stepNumbers } from "../lib/stepNumbers";
import type { HistoryEntry, LogLine, NodeState, Workflow } from "../types";
import { monogram } from "./Graph";
import { RunStory } from "./RunStory";
import { StepBadge } from "./StepBadge";
import "./graph.css";
import "./flow.css";

/** One event per this many milliseconds at 1x. Fixed, not the real gaps between events:
 *  a run whose steps took minutes would otherwise replay in minutes. */
const STEP_MS = 900;

/** Each mark shows its event's number in the run, the same number the replay counter says
 *  ("Event 3 of 12"). The tone is carried by colour AND shape (filled or outlined, round or
 *  square: flow.css), never by colour alone. */
const TONE_WORD: Record<Tone, string> = { ok: "", note: "", stop: "stopped", bad: "failed" };

/** `prefers-reduced-motion`, followed live. */
function useReducedMotion(): boolean {
  const query = useMemo(
    () => (typeof window !== "undefined" && typeof window.matchMedia === "function"
      ? window.matchMedia("(prefers-reduced-motion: reduce)") : null), []);
  const [reduced, setReduced] = useState(query?.matches ?? false);
  useEffect(() => {
    if (!query) return undefined;
    const on = () => setReduced(query.matches);
    query.addEventListener?.("change", on);
    return () => query.removeEventListener?.("change", on);
  }, [query]);
  return reduced;
}

const money = (n: number) => (n <= 0 ? "$0" : n < 0.0001 ? "<$0.0001" : `$${n.toFixed(4)}`);
const timing = (ms: number) => (ms >= 1000 ? `${(ms / 1000).toFixed(1)} s` : `${Math.round(ms)} ms`);

interface AgentBox {
  id: string;
  name: string;
  /** The run's own status for this agent; absent for run-level events. */
  status?: string;
}

function Mark({ ev, selected, current, onSelect }: {
  ev: FlowEvent; selected: boolean; current: boolean; onSelect: (i: number) => void;
}) {
  return (
    <button
      type="button"
      className={`rf-mark rf-tone-${ev.tone}${selected ? " rf-selected" : ""}${current ? " rf-current" : ""}`}
      aria-label={ev.sentence}
      aria-pressed={selected}
      title={`Event ${ev.index + 1}${TONE_WORD[ev.tone] ? ` (${TONE_WORD[ev.tone]})` : ""}: ${ev.sentence}`}
      data-testid={`mark-${ev.index}`}
      onClick={() => onSelect(ev.index)}
    >
      <span aria-hidden="true">{ev.index + 1}</span>
    </button>
  );
}

function Agent({ box, num, status, events, open, onToggle, selected, current, onSelect }: {
  box: AgentBox; status: string;
  /** Its step number (lib/stepNumbers.ts); absent for an agent the workflow does not list, and the run. */
  num?: string; events: FlowEvent[]; open: boolean; onToggle: () => void;
  selected: number | null; current: number | null; onSelect: (i: number) => void;
}) {
  const stopped = events.filter((e) => e.tone === "stop").length;
  const failed = events.filter((e) => e.tone === "bad").length;
  const counts = events.length === 0 ? "No events"
    : [`${events.length} ${events.length === 1 ? "event" : "events"}`,
       stopped ? `${stopped} stopped` : "", failed ? `${failed} failed` : ""].filter(Boolean).join(" · ");
  const panelId = `rf-lanes-${box.id || "run"}`;
  const lanes = COMPONENTS
    .map((component) => ({ component, list: events.filter((e) => e.component === component) }))
    .filter((l) => l.list.length > 0);
  return (
    <div className="rf-agent" data-testid={`flow-agent-${box.id || "run"}`}>
      <button
        type="button"
        className={`sfn-state sfn-${status}`}
        aria-expanded={open}
        aria-controls={panelId}
        disabled={events.length === 0}
        onClick={onToggle}
      >
        {num ? <StepBadge num={num} status={status} />
          : <span className={`sfn-icon sfn-icon-${status}`} aria-hidden="true">{monogram(box.name)}</span>}
        <span className="sfn-state-text">
          <span className="sfn-state-name">{box.name}</span>
          <span className="sfn-state-meta">
            {/* The badge carries the status's colour; this says it in words. */}
            {box.status !== undefined ? <span className="rf-status">{statusLabel(status)}</span> : null}
            <span className="sfn-state-sub">{counts}</span>
          </span>
        </span>
        <span className="rf-chevron" aria-hidden="true">{open ? "▾" : "▸"}</span>
      </button>
      {open && lanes.length > 0 ? (
        <div className="rf-lanes" id={panelId}>
          {lanes.map(({ component, list }) => (
            <div key={component} className="rf-lane" role="group"
                 aria-label={`${box.name} to ${component}, ${list.length} ${list.length === 1 ? "event" : "events"}`}>
              <span className="rf-edge" aria-hidden="true" />
              <span className="rf-marks">
                {list.map((ev) => (
                  <Mark key={ev.index} ev={ev} selected={selected === ev.index}
                        current={current === ev.index} onSelect={onSelect} />
                ))}
              </span>
              <span className="rf-lane-end">{component}</span>
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );
}

export function RunFlow({ sessionId, workflow, nodes, refreshKey, load, logs, hitl, history, defaultView = "story" }: {
  sessionId: string;
  workflow: Workflow;
  nodes: Record<string, NodeState>;
  /** The run's log lines: the Story view reads agent starts and ends and HITL gates from them. */
  logs?: LogLine[];
  /** The gate the run is waiting at now, if any. */
  hitl?: { node: string; kind?: string } | null;
  /** Each agent's versions: the Story view names a re-run with its reviewer's note. */
  history?: Record<string, HistoryEntry[]>;
  /** Which view opens first. */
  defaultView?: "story" | "summary";
  /** Any value that changes when the run does (its `updated_at`): the events are read again. */
  refreshKey?: string;
  /** Replaceable in tests; by default the BFF's flow route. */
  load?: (sessionId: string) => Promise<FlowResponse>;
}) {
  const fetchFlow = load ?? ((id: string) =>
    api.get<FlowResponse>(`/api/sessions/${encodeURIComponent(id)}/flow`));
  const fetchRef = useRef(fetchFlow);
  fetchRef.current = fetchFlow;

  const [data, setData] = useState<FlowResponse | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    let alive = true;
    fetchRef.current(sessionId)
      .then((d) => { if (alive) { setData(d); setError(""); } })
      .catch((e: unknown) => { if (alive) setError(e instanceof Error ? e.message : "Could not read this run's events."); })
      .finally(() => { if (alive) setLoading(false); });
    return () => { alive = false; };
  }, [sessionId, refreshKey]);

  const nameOf = useCallback((id: string) => workflow.agents[id]?.name ?? id, [workflow]);
  const events = useMemo(() => buildEvents(data?.events, nameOf), [data, nameOf]);

  const [replay, dispatch] = useReducer(replayReducer, initialReplay);
  useLayoutEffect(() => { dispatch({ type: "events", total: events.length }); }, [events.length]);
  const view = useMemo(() => replayAt(events, replay.at), [events, replay.at]);
  const atEnd = replay.at >= replay.total;

  const reduced = useReducedMotion();
  useEffect(() => { if (reduced && replay.playing) dispatch({ type: "pause" }); }, [reduced, replay.playing]);
  useEffect(() => {
    if (!replay.playing || reduced) return undefined;
    const t = setTimeout(() => dispatch({ type: "tick" }), STEP_MS / replay.speed);
    return () => clearTimeout(t);
  }, [replay.playing, replay.at, replay.speed, reduced]);

  const [mode, setMode] = useState<"story" | "summary">(defaultView);
  const [open, setOpen] = useState<Record<string, boolean>>({});
  const [picked, setPicked] = useState<number | null>(null);
  // Once the reader has used a replay control, the event the replay is at becomes the
  // selected one and its agent opens. Before that nothing is selected or open.
  const touched = useRef(false);
  const current = view.current;
  useEffect(() => {
    if (!touched.current) return;
    setPicked(current ? current.index : null);
    if (current) setOpen((o) => (o[current.agent] ? o : { ...o, [current.agent]: true }));
  }, [current]);
  const control = (a: Parameters<typeof dispatch>[0]) => { touched.current = true; dispatch(a); };

  // The run's agents in the run's order; then any agent the events name that the workflow
  // does not, then the run itself.
  const stages = useMemo(() => (workflow.steps ?? []).map((s) => ({
    parallel: Boolean(s.parallel), ids: s.parallel ?? s.sequence ?? (s.agent ? [s.agent] : []),
  })), [workflow]);
  const inWorkflow = useMemo(() => new Set(stages.flatMap((s) => s.ids)), [stages]);
  const nums = useMemo(() => stepNumbers(workflow.steps), [workflow]);
  const extra = useMemo(() => [...new Set(events.map((e) => e.agent))]
    .filter((a) => a !== "" && !inWorkflow.has(a)), [events, inWorkflow]);
  const hasRunLevel = events.some((e) => e.agent === "");

  const boxOf = (id: string): AgentBox => ({ id, name: nameOf(id), status: nodes[id]?.status ?? "pending" });
  const renderAgent = (box: AgentBox) => {
    const mine = view.shown.filter((e) => e.agent === box.id);
    const phase = view.agents[box.id]?.phase;
    // At the end of the replay the agent shows the run's real status. Before it, only what
    // the events so far can support: not started, in progress, or done with its events.
    const status = box.status === undefined ? "pending"
      : atEnd || !phase ? box.status
      : phase === "pending" ? "pending" : phase === "running" ? "running" : "done";
    return (
      <Agent key={box.id || "run"} box={box} num={nums[box.id]} status={status} events={mine}
             open={Boolean(open[box.id])} onToggle={() => setOpen((o) => ({ ...o, [box.id]: !o[box.id] }))}
             selected={picked} current={!atEnd || touched.current ? (current?.index ?? null) : null}
             onSelect={setPicked} />
    );
  };

  if (loading) return <Box padding="l"><Spinner /> Reading this run's events…</Box>;
  if (error && !data) return <Alert type="error" header="The flow could not be read">{error}</Alert>;

  const rail = railOf(view.shown);
  const detail = picked !== null && picked < view.shown.length ? events[picked] : null;

  return (
    <div className="rf" data-testid="run-flow">
      {error ? <Alert type="warning">{`Showing the events read earlier. ${error}`}</Alert> : null}
      {data?.capped ? (
        <Alert type="info" header="Only the first events are shown">
          {`This run has more than ${data.max.toLocaleString("en-US")} recorded events. The earliest ${data.max.toLocaleString("en-US")} are shown.`}
        </Alert>
      ) : null}
      {events.length === 0 ? (
        <Box color="text-status-inactive" padding="l">
          No events recorded for this run yet. Each model call, tool call, memory recall, guardrail
          check and policy decision adds one.
        </Box>
      ) : replay.total !== events.length ? null : (
        <>
          <SegmentedControl
            label="Flow view" selectedId={mode}
            options={[{ id: "story", text: "Story" }, { id: "summary", text: "Summary" }]}
            onChange={({ detail: d }) => setMode(d.selectedId === "summary" ? "summary" : "story")}
          />
          <div className="rf-bar" role="group" aria-label="Replay">
            <Button variant="icon" iconName="angle-left" ariaLabel="Step back"
                    disabled={replay.at === 0} onClick={() => control({ type: "back" })} />
            <Button variant="icon" iconName={replay.playing ? "pause" : "play"}
                    ariaLabel={replay.playing ? "Pause" : "Play"} disabled={reduced}
                    onClick={() => control({ type: replay.playing ? "pause" : "play" })} />
            <Button variant="icon" iconName="angle-right" ariaLabel="Step forward"
                    disabled={atEnd} onClick={() => control({ type: "forward" })} />
            <SegmentedControl
              label="Replay speed" selectedId={String(replay.speed)}
              options={[{ id: "1", text: "1x" }, { id: "2", text: "2x" }]}
              onChange={({ detail: d }) => control({ type: "speed", speed: d.selectedId === "2" ? 2 : 1 })}
            />
            <div className="rf-scrub">
              <Slider
                ariaLabel="Replay position" min={0} max={replay.total} step={1} value={replay.at}
                valueFormatter={(v) => `${v} of ${replay.total}`}
                onChange={({ detail: d }) => control({ type: "seek", to: d.value })}
              />
            </div>
            <span className="rf-count">{`Event ${replay.at} of ${replay.total}`}</span>
          </div>
          {reduced ? (
            <Box color="text-status-inactive" fontSize="body-s">
              Automatic playback is off because your system asks for reduced motion. Step through
              the events or drag the slider.
            </Box>
          ) : null}
          <LiveRegion>
            {touched.current && current ? `Event ${replay.at} of ${replay.total}: ${current.sentence}` : ""}
          </LiveRegion>

          {mode === "story" ? (
            <RunStory
              events={events} workflow={workflow} nodes={nodes} logs={logs} hitl={hitl} history={history}
              at={replay.at} total={replay.total} highlight={!atEnd || touched.current}
              picked={picked} onPick={setPicked}
            />
          ) : (<>
          <div className="rf-body">
            <div className="rf-stages">
              {stages.map((st, i) => (
                <div key={i} className={`rf-stage${st.parallel ? " rf-parallel" : ""}`}>
                  {st.ids.map((id) => renderAgent(boxOf(id)))}
                </div>
              ))}
              {extra.length > 0 ? (
                <div className="rf-stage">{extra.map((id) => renderAgent({ id, name: nameOf(id) }))}</div>
              ) : null}
              {hasRunLevel ? (
                <div className="rf-stage">{renderAgent({ id: "", name: "The run" })}</div>
              ) : null}
            </div>
            <aside className="rf-rail" aria-label="AgentCore components this run used">
              <span className="rf-rail-title">AgentCore</span>
              <ul>
                {COMPONENTS.map((c: Component) => rail.find((r) => r.component === c)).filter(Boolean).map((r) => (
                  <li key={r!.component}>
                    <span>{r!.component}</span>
                    <span className="rf-rail-n">
                      {r!.count}
                      {r!.stopped ? ` · ${r!.stopped} stopped` : ""}
                      {r!.failed ? ` · ${r!.failed} failed` : ""}
                    </span>
                  </li>
                ))}
              </ul>
            </aside>
          </div>

          {detail ? (
            <div className="rf-detail" data-testid="flow-detail">
              <Box variant="p">{detail.sentence}</Box>
              <Box color="text-body-secondary" fontSize="body-s">
                {`Latency ${timing(detail.latencyMs)} · Cost ${money(detail.costUsd)}`}
              </Box>
            </div>
          ) : null}
          </>)}
        </>
      )}
    </div>
  );
}
