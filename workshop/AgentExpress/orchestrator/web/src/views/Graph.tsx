/** The workflow graph, laid out the way Step Functions draws an execution.
 *
 *  Cloudscape has no graph component — the console's own workflow views are custom
 *  canvases too — so this is bespoke markup INSIDE a Cloudscape Container, using
 *  Cloudscape's design tokens for every colour and radius so it sits in the page
 *  rather than on it. What is borrowed from Step Functions: a tinted dot-grid canvas,
 *  zoom controls, a vertical flow, state boxes whose border carries status, connectors
 *  with arrowheads, and a Parallel state drawn as a frame around its branches.
 */

import Badge from "@cloudscape-design/components/badge";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Popover from "@cloudscape-design/components/popover";
import SpaceBetween from "@cloudscape-design/components/space-between";
import { type CSSProperties, useState } from "react";

import type { FlowEvent } from "../lib/flow";
import { KIND_TAG, RESOURCE_KINDS, outcomeOf } from "../lib/runComponents";
import { stepLabel, stepNumbers } from "../lib/stepNumbers";
import type { AgentMeta, NodeState, NodeStatus, Step, Workflow } from "../types";
import "./graph.css";
import { toolsLabel, toolsOf } from "../lib/tools";
import { StepBadge } from "./StepBadge";

const BRANCH_OPS = ["equals", "notEquals", "in", "contains", "exists", "gt", "gte", "lt", "lte"];

/** The rules of a step's `branch`, as prose. The graph can only draw the CONFIGURED
 *  order of steps; with a branch the real path depends on the output, so a reader has
 *  to be able to see what decides it. */
function branchSummary(b: NonNullable<Step["branch"]>): string[] {
  const lines = (b.when ?? []).map((r) => {
    const cmp = BRANCH_OPS.filter((o) => o in r)
      .map((o) => `${o} ${JSON.stringify(r[o])}`)
      .join(" and ");
    return `if ${r.field ?? "output"} ${cmp} → ${r.goto}`;
  });
  lines.push(b.default ? `otherwise → ${b.default}` : "otherwise → next step");
  return lines;
}

function monogram(name: string): string {
  const words = (name || "?").trim().split(/\s+/).filter(Boolean);
  if (words.length === 0) return "?";
  if (words.length === 1) return words[0].slice(0, 2).toUpperCase();
  return (words[0][0] + words[1][0]).toUpperCase();
}

/** What an agent reads, when it is not bound to a declared tool. */
function sourceLabel(meta: AgentMeta): string | null {
  if (toolsOf(meta.tool).length) return null;
  if (meta.runtime === "a2a") return meta.source ? `a2a · ${meta.source}` : "remote agent";
  // A step with no model says what it does ("Task · Code", from bff/workflow.py).
  if (meta.runtime === "task") return meta.source ?? "task";
  if (meta.runtime === "harness") return meta.source ?? "AgentCore Harness";
  if (meta.access) return meta.access;
  return null;
}

/** One STATE. Compact on purpose: a Step Functions state shows an icon, a name and its
 *  status, and nothing else — its configuration is in the panel. The first version put
 *  every badge (placement, tool, corpus, model) in the box, which made each one a tall
 *  card; five of those in a two-column frame read as a stack of cards rather than a
 *  diagram, and at the frame's width they collided. The badges now live in the split
 *  panel's Details tab, which is where the console puts them. */
/** One agent's tools and AgentCore features, one clickable row each, inside its box (the
 *  same rows as the Builder's agent card). Each says what it did in this run, from the
 *  run's Flow events (lib/runComponents.ts); clicking a row lists those events. Only this
 *  agent's own: another agent's calls to a shared tool are counted on that agent. */
function ComponentRows({ agentId, meta, events }: { agentId: string; meta: AgentMeta; events: FlowEvent[] | null }) {
  const comps = meta.components ?? [];
  const [open, setOpen] = useState<number | null>(null);
  if (!comps.length) return null;
  return (
    <ul className="sfn-rows" aria-label={`Attached to ${meta.name ?? agentId}`} data-testid={`rows-${agentId}`}
        onClick={(e) => e.stopPropagation()}>
      {meta.componentsNow ? <li className="sfn-row-note">As configured now (this run predates the record)</li> : null}
      {comps.map((c, i) => {
        const o = outcomeOf(c, agentId, events);
        return (
          <li key={`${c.kind}:${c.tool ?? ""}:${c.name}:${i}`} className={`sfn-row sfn-tone-${o.tone}`} data-kind={c.kind}
              data-testid={`run-comp-${agentId}-${c.kind}-${c.name}`}>
            <button type="button" className="sfn-row-head" aria-expanded={open === i}
                    title={[c.detail, o.line].filter(Boolean).join(" · ")}
                    onClick={() => setOpen(open === i ? null : i)}>
              <span className={`sfn-tag sfn-tag-${c.kind}`}>{KIND_TAG[c.kind] ?? c.kind}</span>
              <span className="sfn-row-name">{c.name}</span>
              <span className="sfn-row-outcome">{o.line || c.detail || ""}</span>
            </button>
            {open === i ? (
              <div className="sfn-row-more">
                {c.detail ? <div className="sfn-row-detail">{c.detail}</div> : null}
                {/* The real thing in the account, so "strict" can be found in the Bedrock
                    console as the guardrail it is. */}
                {c.resources?.length ? (
                  <dl className="sfn-row-res" aria-label={`${c.name} as deployed`}>
                    {c.resources.map((r) => (
                      <div key={r.label}><dt>{r.label}</dt><dd>{r.value}</dd></div>
                    ))}
                  </dl>
                ) : !c.resources && RESOURCE_KINDS.has(c.kind)
                  ? <div className="sfn-row-detail">Deployed resource: not available</div> : null}
                {o.events.length ? (
                  <ul className="sfn-row-events">
                    {o.events.slice(0, 8).map((e) => <li key={e.index} className={`sfn-ev-${e.tone}`}>{e.sentence}</li>)}
                    {o.events.length > 8 ? <li>{`…and ${o.events.length - 8} more, on the Flow tab.`}</li> : null}
                  </ul>
                ) : <div className="sfn-row-detail">{events ? "Nothing recorded for it in this run." : "Reading this run's events…"}</div>}
              </div>
            ) : null}
          </li>
        );
      })}
    </ul>
  );
}

function StateBox({
  agentId, num, meta, state, selected, onSelect, onRerunFrom, events,
}: {
  events: FlowEvent[] | null;
  /** Its number in the run (lib/stepNumbers.ts): "2", or "2b" in a stage of several. */
  num: string;
  agentId: string;
  meta: AgentMeta;
  state: NodeState;
  selected: boolean;
  onSelect: (id: string) => void;
  /** Provided only when this state can actually be re-run, so the control is absent
   *  rather than present-and-disabled on a running graph. */
  onRerunFrom?: (id: string) => void;
}) {
  const status: NodeStatus = state.status ?? "pending";
  // ONE line of sub-text, the thing that distinguishes this step from its siblings:
  // what it reads. Not six badges.
  const subtitle = toolsOf(meta.tool).length
    ? toolsLabel(meta.tool, meta.corpus)
    : (sourceLabel(meta) ?? meta.produces ?? "");
  return (
    <div
      className={`sfn-state sfn-${status}${selected ? " sfn-selected" : ""}`}
      data-testid={`state-${agentId}`}
      role="button"
      tabIndex={0}
      aria-label={`${meta.name ?? agentId} — ${stepLabel(num, status)}`}
      title={`${meta.name ?? agentId}${subtitle ? ` — ${subtitle}` : ""}`}
      onClick={(e) => { e.stopPropagation(); onSelect(agentId); }}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onSelect(agentId); }
      }}
    >
      {/* The step's number, coloured by its status, where a monogram used to be: a reader
          says "step 3 failed", not "the box with RW in it". */}
      <StepBadge num={num} status={status} />
      {/* The name gets the FULL width of the text column and the placement badge sits on
          the second line beside the subtitle. With the badge on the name's line, a
          260px state truncated "Knowledge Base Research" to "Knowled…" — the badge was
          winning space from the one string a reader needs. */}
      <span className="sfn-state-text">
        <span className="sfn-state-name">{meta.name ?? agentId}</span>
        <span className="sfn-state-meta">
          {subtitle ? <span className="sfn-state-sub">{subtitle}</span> : null}
          {meta.runtime && meta.runtime !== "main"
            ? <Badge color={meta.runtime === "a2a" ? "grey" : "blue"}>{meta.runtime}</Badge>
            : null}
        </span>
      </span>
      {onRerunFrom ? (
        <span className="sfn-state-act" onClick={(e) => e.stopPropagation()}>
          <Button
            variant="inline-icon"
            iconName="redo"
            ariaLabel={`Re-run from ${meta.name ?? agentId}`}
            onClick={() => onRerunFrom(agentId)}
          />
        </span>
      ) : null}
      <ComponentRows agentId={agentId} meta={meta} events={events} />
      {status === "running" ? (
        <div className="sfn-bar"><i style={{ width: `${state.pct || 6}%` }} /></div>
      ) : null}
    </div>
  );
}

function Connector() {
  return <div className="sfn-connector" aria-hidden="true" />;
}

function StageAnnotations({ step }: { step: Step }) {
  if (!step.hitl && !step.branch) return null;
  return (
    <div className="sfn-annotations">
      {step.branch ? (
        <Popover
          dismissButton={false}
          position="top"
          size="medium"
          triggerType="custom"
          header="Branch rules"
          content={
            <SpaceBetween size="xxs">
              {branchSummary(step.branch).map((l, i) => (
                <Box key={i} variant="code" fontSize="body-s">{l}</Box>
              ))}
            </SpaceBetween>
          }
        >
          <span className="sfn-pill sfn-pill-branch" role="note">Branch</span>
        </Popover>
      ) : null}
      {step.hitl ? (
        <Popover
          dismissButton={false}
          position="top"
          size="small"
          triggerType="custom"
          content={<Box variant="p">HITL (a human in the loop) approves, revises or denies this stage before the run continues.</Box>}
        >
          <span className="sfn-pill sfn-pill-gate" role="note">HITL gate</span>
        </Popover>
      ) : null}
    </div>
  );
}

function Stage({
  step, nums, workflow, nodes, selected, onSelect, rerunnable, onRerunFrom, events,
}: {
  step: Step;
  /** Every agent's step number (lib/stepNumbers.ts). */
  nums: Record<string, string>;
  workflow: Workflow;
  nodes: Record<string, NodeState>;
  selected: string | null;
  onSelect: (id: string) => void;
  rerunnable: (id: string) => boolean;
  onRerunFrom?: (id: string) => void;
  /** The run's Flow events, for what each component did; null until read. */
  events?: FlowEvent[] | null;
}) {
  const box = (id: string) => (
    <StateBox
      key={id}
      agentId={id}
      num={nums[id] ?? "?"}
      meta={workflow.agents[id] ?? { name: id }}
      state={nodes[id] ?? { status: "pending" }}
      selected={selected === id}
      onSelect={onSelect}
      onRerunFrom={onRerunFrom && rerunnable(id) ? onRerunFrom : undefined}
      events={events ?? null}
    />
  );

  let body: JSX.Element;
  if (step.parallel) {
    const cols = Math.min(2, step.parallel.length);
    body = (
      <div className="sfn-frame">
        <span className="sfn-frame-label">
          {`${step.gateName ?? "Parallel"} · ${step.parallel.length} branches in parallel`}
        </span>
        {/* The column COUNT is the component's business; the column WIDTH is the
            stylesheet's, and it is the same variable the box sizes itself from. */}
        <div className="sfn-frame-grid" style={{ "--sfn-cols": cols } as CSSProperties}>
          {step.parallel.map(box)}
        </div>
      </div>
    );
  } else if (step.sequence) {
    body = (
      <div className="sfn-frame">
        <span className="sfn-frame-label">
          {`${step.gateName ?? "Sequence"} · ${step.sequence.length} in order`}
        </span>
        <div className="sfn-frame-row">
          {step.sequence.map((id, i) => (
            <div key={id} className="sfn-seq-item">
              {i > 0 ? <div className="sfn-seq-arrow" aria-hidden="true" /> : null}
              {box(id)}
            </div>
          ))}
        </div>
      </div>
    );
  } else {
    body = box(step.agent!);
  }

  return (
    <div className="sfn-stage">
      <StageAnnotations step={step} />
      {body}
    </div>
  );
}

const ZOOMS = [0.6, 0.75, 0.9, 1, 1.15, 1.3];

export function Graph({
  workflow, nodes, selected, onSelect, rerunnable, onRerunFrom, onRestart, events,
}: {
  /** The run's Flow events (lib/flow.ts), for what each component of the selected agent did. */
  events?: FlowEvent[] | null;
  workflow: Workflow;
  nodes: Record<string, NodeState>;
  selected: string | null;
  onSelect: (id: string | null) => void;
  /** Can this state be used as a re-run origin? */
  rerunnable?: (id: string) => boolean;
  onRerunFrom?: (id: string) => void;
  /** Re-run the whole workflow from its first step. */
  onRestart?: () => void;
}) {
  const [zoomIdx, setZoomIdx] = useState(3);
  const zoom = ZOOMS[zoomIdx];
  const steps = workflow.steps ?? [];
  const nums = stepNumbers(steps);
  const can = rerunnable ?? (() => false);

  return (
    <div className="sfn-canvas" onClick={() => onSelect(null)}>
      <div className="sfn-toolbar" onClick={(e) => e.stopPropagation()}>
        {/* Restart lives ON THE GRAPH, next to the zoom controls, because that is where
            a reader is looking when they decide the run went wrong. It used to be a tab
            called "Re-run" three tabs away, which meant nobody found it. */}
        {onRestart ? (
          <>
            <Button iconName="redo" onClick={onRestart}>Restart workflow</Button>
            <span className="sfn-toolbar-sep" aria-hidden="true" />
          </>
        ) : null}
        <Button
          variant="icon" iconName="zoom-out" ariaLabel="Zoom out"
          disabled={zoomIdx === 0} onClick={() => setZoomIdx((i) => Math.max(0, i - 1))}
        />
        <span className="sfn-zoom-label">{Math.round(zoom * 100)}%</span>
        <Button
          variant="icon" iconName="zoom-in" ariaLabel="Zoom in"
          disabled={zoomIdx === ZOOMS.length - 1}
          onClick={() => setZoomIdx((i) => Math.min(ZOOMS.length - 1, i + 1))}
        />
        <Button
          variant="icon" iconName="refresh" ariaLabel="Reset zoom"
          onClick={() => setZoomIdx(3)}
        />
      </div>
      <div className={`sfn-flow${selected ? " sfn-has-selection" : ""}`}
           style={{ transform: `scale(${zoom})` }}>
        {steps.map((step, i) => (
          <div key={step.gateId ?? step.agent ?? i} className="sfn-stage-wrap">
            {i > 0 ? <Connector /> : null}
            <Stage
              step={step} nums={nums} workflow={workflow} nodes={nodes}
              selected={selected} onSelect={onSelect}
              rerunnable={can} onRerunFrom={onRerunFrom} events={events}
            />
          </div>
        ))}
      </div>
    </div>
  );
}

export { branchSummary, monogram };
