/** The Timeline tab's chart: one run as a Gantt, time left to right (lib/gantt.ts has the
 *  layout and says where each mark comes from).
 *
 *  It reads nothing of its own: the log is the run snapshot's, and the calls are the Flow
 *  events RunDetail already reads for the graph. A shared (read-only) run has no Flow
 *  events, so its chart has the agents' bars and the HITL waits, without the calls.
 *
 *  Every bar, call and wait is focusable and says what it is in its aria-label; a visually
 *  hidden table carries the same data for a screen reader. Colour is never the only cue:
 *  a status also has a glyph and a call kind is named in its label and the legend. */

import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import SpaceBetween from "@cloudscape-design/components/space-between";
import { type PointerEvent, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";

import type { FlowEvent } from "../lib/flow";
import {
  type BarStatus, buildGantt, CALL_KINDS, fmtCost, fmtSpan, type Gantt, type GanttBar, type GanttCall,
  ticks, toX,
} from "../lib/gantt";
import type { SessionSnapshot, Workflow } from "../types";
import "./gantt.css";

const ZOOMS = [1, 2, 4, 8, 16, 32];

const STATUS_WORD: Record<BarStatus, string> = {
  done: "done", running: "running", failed: "failed", waiting: "waiting for HITL", stopped: "stopped",
};
const STATUS_GLYPH: Record<BarStatus, string> = { done: "✓", running: "▶", failed: "✕", waiting: "⏸", stopped: "■" };
const KIND_LABEL = Object.fromEntries(CALL_KINDS.map((k) => [k.kind, k.label]));
const ROW_GLYPH: Record<string, string> = {
  done: "✓", running: "▶", failed: "✕", denied: "✕", waiting_human: "⏸", pending: "○", skipped: "–", cancelled: "■",
};

/** `prefers-reduced-motion`, followed live (the same check RunFlow makes). */
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

const pct = (f: number) => `${(f * 100).toFixed(3)}%`;
const at = (ms: number) => `at ${fmtSpan(ms)}`;

function barText(b: GanttBar, name: string): string {
  return `${name} v${b.version}, ${STATUS_WORD[b.status]}${b.critical ? ", on the critical path" : ""}: `
    + `starts ${at(b.start)}, takes ${fmtSpan(b.end - b.start)}${b.open ? " so far" : ""}, `
    + `${b.calls.length} call${b.calls.length === 1 ? "" : "s"}`;
}

function callText(c: GanttCall): string {
  return `${KIND_LABEL[c.kind]}${c.label ? ` ${c.label}` : ""}${c.status ? `, ${c.status}` : ""}: `
    + `${fmtSpan(c.latencyMs)}, ${fmtCost(c.costUsd)}, ends ${at(c.end)}`;
}

export function RunGantt({ snap, workflow, events, now: fixedNow }: {
  snap: SessionSnapshot;
  workflow: Workflow;
  /** The run's Flow events, or null when not read (a shared run). */
  events: FlowEvent[] | null;
  /** For tests: a fixed "now" instead of the clock. */
  now?: number;
}) {
  const reduced = useReducedMotion();
  const overall = String(snap.overall ?? "");
  const live = ["running", "waiting_human", "cancelling"].includes(overall);
  // While live, a "now" line advances between the page's polls; settled, the clock stops.
  const [clock, setClock] = useState(() => Date.now());
  useEffect(() => {
    if (!live || fixedNow !== undefined) return undefined;
    const t = window.setInterval(() => setClock(Date.now()), 1000);
    return () => window.clearInterval(t);
  }, [live, fixedNow]);
  const now = fixedNow ?? clock;

  const g: Gantt = useMemo(() => buildGantt({
    workflow, logs: snap.logs, nodes: snap.nodes, events, created: snap.created, overall, now,
  }), [workflow, snap.logs, snap.nodes, events, snap.created, overall, now]);

  const [zoom, setZoom] = useState(1);
  const [tip, setTip] = useState<string | null>(null);
  const scroller = useRef<HTMLDivElement>(null);
  const prevZoom = useRef(zoom);
  // Zooming keeps the same moment at the left edge rather than jumping back to the start.
  useLayoutEffect(() => {
    const el = scroller.current;
    if (el && prevZoom.current !== zoom) el.scrollLeft = el.scrollLeft * (zoom / prevZoom.current);
    prevZoom.current = zoom;
  }, [zoom]);
  const drag = useRef<{ x: number; left: number } | null>(null);
  const [dragging, setDragging] = useState(false);
  const onDown = (e: PointerEvent<HTMLDivElement>) => {
    if (e.button !== 0 || !scroller.current) return;
    drag.current = { x: e.clientX, left: scroller.current.scrollLeft };
    setDragging(true);
  };
  const onMove = (e: PointerEvent<HTMLDivElement>) => {
    if (drag.current && scroller.current) scroller.current.scrollLeft = drag.current.left - (e.clientX - drag.current.x);
  };
  const onUp = () => { drag.current = null; setDragging(false); };

  const jump = (c: GanttCall) => {
    const el = document.getElementById(`gantt-${c.key}`);
    el?.scrollIntoView?.({ block: "nearest", inline: "center", behavior: reduced ? "auto" : "smooth" });
    el?.focus({ preventScroll: true });
    setTip(c.key);
  };

  if (!g.rows.length || !Number.isFinite(g.t0)) {
    return <Box color="text-status-inactive" data-testid="run-gantt">Nothing has run yet.</Box>;
  }

  // Hover or focus shows the item's details; the aria-label says the same to a screen reader.
  const hover = (key: string) => ({
    role: "img", tabIndex: 0,
    onMouseEnter: () => setTip(key), onMouseLeave: () => setTip((k) => (k === key ? null : k)),
    onFocus: () => setTip(key), onBlur: () => setTip((k) => (k === key ? null : k)),
  });
  const zi = ZOOMS.indexOf(zoom);
  const nameOf = (id: string) => g.rows.find((r) => r.agent === id)?.name ?? id;
  const known = events !== null;
  const kinds = new Set(g.calls.map((c) => c.kind));

  return (
    <div className={`gantt${reduced ? " gantt-still" : ""}`} data-testid="run-gantt">
      <div className="gantt-toolbar">
        <Box fontSize="body-s" data-testid="gantt-totals">
          <b>Run</b> {fmtSpan(g.totals.durationMs)}{g.live ? " so far" : ""}
          {known ? <> · <b>Cost</b> {fmtCost(g.totals.costUsd)} · <b>Calls</b> {g.totals.calls}</> : null}
        </Box>
        {g.slowest.length ? (
          <div className="gantt-slowest" role="group" aria-label="Slowest calls">
            <span className="gantt-muted">Slowest:</span>
            {g.slowest.map((c) => (
              <button key={c.key} type="button" className="gantt-chip" onClick={() => jump(c)}
                      aria-label={`Show the call: ${nameOf(c.agent)}, ${callText(c)}`}>
                {nameOf(c.agent)} · {c.label || KIND_LABEL[c.kind]} · {fmtSpan(c.latencyMs)}
              </button>
            ))}
          </div>
        ) : null}
        <SpaceBetween direction="horizontal" size="xxs">
          <Button iconName="zoom-out" variant="icon" ariaLabel="Zoom out" disabled={zi <= 0}
                  onClick={() => setZoom(ZOOMS[Math.max(0, zi - 1)])} />
          <Button iconName="zoom-to-fit" variant="icon" ariaLabel="Fit to width" disabled={zoom === 1}
                  onClick={() => setZoom(1)} />
          <Button iconName="zoom-in" variant="icon" ariaLabel="Zoom in" disabled={zi >= ZOOMS.length - 1}
                  onClick={() => setZoom(ZOOMS[Math.min(ZOOMS.length - 1, zi + 1)])} />
          <span className="gantt-muted" aria-live="polite">{zoom}×</span>
        </SpaceBetween>
      </div>
      <div className="gantt-legend" aria-hidden="true">
        {CALL_KINDS.filter((k) => kinds.has(k.kind)).map((k) => (
          <span key={k.kind}><i className={`gantt-swatch gantt-k-${k.kind}`} />{k.emoji} {k.label}</span>
        ))}
        <span><i className="gantt-swatch gantt-swatch-critical" />Critical path (sets the run's length)</span>
        {g.gates.length ? <span><i className="gantt-swatch gantt-swatch-hitl" />HITL wait</span> : null}
      </div>

      <div ref={scroller} className={`gantt-scroll${dragging ? " gantt-dragging" : ""}`}
           onPointerDown={onDown} onPointerMove={onMove} onPointerUp={onUp} onPointerLeave={onUp}>
        <div className="gantt-inner" style={{ ["--gantt-zoom" as string]: zoom }}>
          <div className="gantt-row gantt-axis" aria-hidden="true">
            <div className="gantt-label gantt-muted">Time</div>
            <div className="gantt-track">
              {ticks(g, zoom).map((t) => (
                <span key={t.t} className="gantt-tick" style={{ left: pct(t.x) }}>{t.label}</span>
              ))}
              {g.breaks.map((b) => (
                <span key={b.start} className="gantt-tick gantt-break-label"
                      style={{ left: pct((toX(g, b.start) + toX(g, b.end)) / 2) }}>{b.label}</span>
              ))}
              {g.now !== null ? <span className="gantt-tick gantt-now-label" style={{ left: pct(toX(g, g.now)) }}>now</span> : null}
            </div>
          </div>

          <div className="gantt-body">
            <div className="gantt-overlay">
              {ticks(g, zoom).map((t) => <i key={t.t} className="gantt-grid" style={{ left: pct(t.x) }} />)}
              {g.breaks.map((b) => (
                <i key={b.start} className="gantt-break"
                   style={{ left: pct(toX(g, b.start)), width: pct(toX(g, b.end) - toX(g, b.start)) }} />
              ))}
              {g.gates.map((w) => (
                <div key={w.key} id={`gantt-${w.key}`} className={`gantt-gate${w.open ? " gantt-gate-open" : ""}`}
                     style={{ left: pct(toX(g, w.start)), width: pct(toX(g, w.end) - toX(g, w.start)) }}
                     role="img" tabIndex={0} data-testid={`gantt-gate-${w.node}`}
                     aria-label={`${w.name}: HITL wait of ${fmtSpan(w.end - w.start)}, ${w.outcome}, starts ${at(w.start)}`}>
                  <span className="gantt-gate-label" aria-hidden="true">{w.label}</span>
                </div>
              ))}
              {g.now !== null ? <i className="gantt-now" style={{ left: pct(toX(g, g.now)) }} /> : null}
            </div>

            {g.rows.map((r) => (
              <div key={r.agent}>
                {r.firstInStage ? (
                  <div className="gantt-row gantt-stage">
                    <div className="gantt-label">{r.stageLabel}</div>
                    <div className="gantt-track" />
                  </div>
                ) : null}
                <div className="gantt-row" data-testid={`gantt-row-${r.agent}`}>
                  <div className="gantt-label" title={r.name}>
                    <span aria-hidden="true">{r.emoji}</span>
                    <span className="gantt-name">{r.name}</span>
                    <span className={`gantt-glyph gantt-s-${r.status}`} aria-hidden="true">{ROW_GLYPH[r.status] ?? "○"}</span>
                  </div>
                  <div className="gantt-track">
                    {r.bars.map((b) => {
                      const x0 = toX(g, b.start);
                      const x1 = toX(g, b.end);
                      return (
                        <div key={b.key} id={`gantt-${b.key}`} data-testid={`gantt-bar-${b.key}`}
                             className={`gantt-bar gantt-b-${b.status}${b.critical ? " gantt-critical" : ""}`}
                             style={{ left: pct(x0), width: pct(x1 - x0) }}
                             aria-label={barText(b, r.name)} {...hover(b.key)}>
                          {r.bars.length > 1 ? <span className="gantt-v" aria-hidden="true">{STATUS_GLYPH[b.status]} v{b.version}</span> : null}
                          {tip === b.key ? <span className="gantt-tip" role="tooltip">{barText(b, r.name)}</span> : null}
                        </div>
                      );
                    })}
                    {r.bars.flatMap((b) => b.calls).map((c) => {
                      const x0 = toX(g, c.start);
                      const x1 = toX(g, c.end);
                      return (
                        <div key={c.key} id={`gantt-${c.key}`} data-testid={`gantt-${c.key}`}
                             className={`gantt-call gantt-k-${c.kind} gantt-t-${c.tone}`}
                             style={{ left: pct(x0), width: pct(x1 - x0) }}
                             aria-label={`${r.name}: ${callText(c)}`} {...hover(c.key)}>
                          {tip === c.key ? <span className="gantt-tip" role="tooltip">{callText(c)}</span> : null}
                        </div>
                      );
                    })}
                  </div>
                </div>
              </div>
            ))}

            <div className="gantt-row gantt-total">
              <div className="gantt-label"><b>Totals</b></div>
              <div className="gantt-track gantt-total-text">
                {fmtSpan(g.totals.durationMs)}{g.live ? " so far" : ""}
                {known ? ` · ${fmtCost(g.totals.costUsd)} · ${g.totals.calls} call${g.totals.calls === 1 ? "" : "s"}` : ""}
              </div>
            </div>
          </div>
        </div>
      </div>
      {known ? null : (
        <Box fontSize="body-s" color="text-status-inactive">
          The calls inside each step are not shown on a shared link.
        </Box>
      )}

      <table className="gantt-sr" data-testid="gantt-table">
        <caption>The run's timeline as a table: each step's runs, their calls and the HITL waits.</caption>
        <thead>
          <tr><th scope="col">Step</th><th scope="col">What</th><th scope="col">Status</th>
            <th scope="col">Starts</th><th scope="col">Takes</th><th scope="col">Cost</th></tr>
        </thead>
        <tbody>
          {g.rows.flatMap((r) => r.bars.flatMap((b) => [
            <tr key={b.key}>
              <th scope="row">{r.name}</th><td>Run v{b.version}{b.critical ? " (critical path)" : ""}</td>
              <td>{STATUS_WORD[b.status]}</td><td>{fmtSpan(b.start)}</td><td>{fmtSpan(b.end - b.start)}</td><td>—</td>
            </tr>,
            ...b.calls.map((c) => (
              <tr key={c.key}>
                <th scope="row">{r.name}</th><td>{KIND_LABEL[c.kind]}{c.label ? `: ${c.label}` : ""}</td>
                <td>{c.status || "—"}</td><td>{fmtSpan(c.start)}</td><td>{fmtSpan(c.latencyMs)}</td><td>{fmtCost(c.costUsd)}</td>
              </tr>
            )),
          ]))}
          {g.gates.map((w) => (
            <tr key={w.key}>
              <th scope="row">{w.name}</th><td>HITL wait</td><td>{w.outcome}</td>
              <td>{fmtSpan(w.start)}</td><td>{fmtSpan(w.end - w.start)}</td><td>—</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
