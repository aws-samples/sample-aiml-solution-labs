/** The Builder's canvas: stages top to bottom, agents inside them, drag to change.
 *
 *  React Flow draws it (pan, zoom, edges, minimap); everything around it is Cloudscape.
 *  Positions come from `layout()`, which derives them from `steps` — so a drag does not
 *  MOVE a node, it changes which stage the agent is in, and the layout redraws. What
 *  the user sees is therefore always a shape the orchestrator can run.
 *
 *  Drops, by what they land on:
 *    new agent / unplaced agent   on a stage -> joins it (parallel group)
 *                                 between stages -> a new stage there
 *    tool                         on an agent -> binds it as that agent's tool
 *    an agent dragged on canvas   same rules as an agent from the palette
 *
 *  Every drag has a keyboard equivalent in the inspector (the Stage select, the Tool
 *  select, and the move buttons), because drag-and-drop alone is not accessible. */

import {
  Background, Controls, Handle, MarkerType, MiniMap, Position, ReactFlow, ReactFlowProvider,
  useNodesState, useReactFlow, type Edge, type Node, type NodeProps,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useCallback, useEffect, useMemo, useState, type DragEvent } from "react";

import { toolsLabel, toolsOf } from "../lib/tools";
import { branchSummary, monogram } from "../views/Graph";
import { AGENT_H, AGENT_W, hitTest, layout, type DropTarget } from "./layout";
import { stepAgents, type Workflow } from "./model";
import type { Issue } from "./validate";

export const DND_MIME = "application/x-agentexpress";

export type DragPayload =
  | { kind: "new-agent" }
  | { kind: "agent"; id: string }
  | { kind: "tool"; key: string };

export type Selection =
  | { kind: "agent"; id: string }
  | { kind: "tool"; id: string }
  | { kind: "stage"; index: number }
  | null;

interface AgentData extends Record<string, unknown> {
  id: string;
  name: string;
  subtitle: string;
  /** Expanded: its tools as chips (select one, remove it, add another). */
  expanded: boolean;
  tools: string[];
  addable: string[];
  h: number;
  onChip?: (key: string) => void;
  onUnbind?: (key: string) => void;
  onBind?: (key: string) => void;
  errors: number;
  selected: boolean;
  remote: boolean;
  /** Just changed by AgentExpress Assistant: drawn highlighted until the next change. */
  changed: boolean;
}

interface StageData extends Record<string, unknown> {
  index: number;
  label: string;
  gated: boolean;
  branch: string[] | null;
  errors: number;
  selected: boolean;
  w: number;
  h: number;
}

const hidden = { opacity: 0, pointerEvents: "none" as const };

function AgentNode({ data }: NodeProps<Node<AgentData>>) {
  return (
    <div
      className={`axb-agent${data.selected ? " axb-selected" : ""}${data.errors ? " axb-has-errors" : ""}${data.changed ? " axb-changed" : ""}`}
      style={{ width: AGENT_W, height: data.h }}
      data-testid={`builder-agent-${data.id}`}
      title={data.errors ? `${data.errors} problem(s) — select to see them` : data.name}
    >
      <Handle type="target" position={Position.Left} style={hidden} isConnectable={false} />
      <div className="axb-agent-row">
        <span className={`axb-icon${data.remote ? " axb-remote" : ""}`} aria-hidden="true">{monogram(data.name)}</span>
        <span className="axb-text">
          <span className="axb-name">{data.name}</span>
          <span className="axb-sub">{data.subtitle}</span>
        </span>
        {data.errors ? <span className="axb-badge" aria-label={`${data.errors} problems`}>{data.errors}</span> : null}
      </div>
      {data.expanded ? (
        <div className="axb-chips nodrag" onClick={(e) => e.stopPropagation()}>
          {data.tools.map((t) => (
            <span key={t} className="axb-chip">
              <button type="button" className="axb-chip-name" title={`Open tool ${t}`} data-testid={`chip-${data.id}-${t}`}
                onClick={(e) => { e.stopPropagation(); data.onChip?.(t); }}>{t}</button>
              {data.onUnbind ? (
                <button type="button" className="axb-chip-x" aria-label={`Remove ${t} from ${data.name}`}
                  onClick={(e) => { e.stopPropagation(); data.onUnbind?.(t); }}>×</button>
              ) : null}
            </span>
          ))}
          {data.onBind && data.addable.length ? (
            <select className="axb-chip-add" aria-label={`Add a tool to ${data.name}`} value=""
              onClick={(e) => e.stopPropagation()}
              onChange={(e) => { if (e.target.value) data.onBind?.(e.target.value); }}>
              <option value="">+ tool</option>
              {data.addable.map((t) => <option key={t} value={t}>{t}</option>)}
            </select>
          ) : null}
        </div>
      ) : null}
      <Handle type="source" position={Position.Right} style={hidden} isConnectable={false} />
    </div>
  );
}

function StageNode({ data }: NodeProps<Node<StageData>>) {
  return (
    <div
      className={`axb-stage${data.selected ? " axb-selected" : ""}${data.errors ? " axb-has-errors" : ""}`}
      style={{ width: data.w, height: data.h }}
    >
      <Handle type="target" position={Position.Top} style={hidden} isConnectable={false} />
      <div className="axb-stage-head">
        <span className="axb-stage-num">{data.index + 1}</span>
        <span className="axb-stage-label">{data.label}</span>
        {data.gated ? <span className="axb-pill axb-pill-gate">Review gate</span> : null}
        {data.branch ? <span className="axb-pill axb-pill-branch" title={data.branch.join("\n")}>Branch</span> : null}
        {data.errors ? <span className="axb-badge">{data.errors}</span> : null}
      </div>
      <Handle type="source" position={Position.Bottom} style={hidden} isConnectable={false} />
      <Handle id="branch" type="source" position={Position.Right} style={hidden} isConnectable={false} />
    </div>
  );
}

function EndNode() {
  return (
    <div className="axb-end">
      <Handle type="target" position={Position.Top} style={hidden} isConnectable={false} />
      End
    </div>
  );
}

const nodeTypes = { agent: AgentNode, stage: StageNode, end: EndNode };

/** Never zoom in past 100%: one stage filling the canvas at 160% reads as a broken page. */
const FIT = { padding: 0.2, maxZoom: 1, duration: 200 };

/** The rebuilt nodes, each keeping the size React Flow measured for it. React Flow draws
 *  a node with no `measured` size as invisible until its ResizeObserver fires, and that
 *  fires only when the element's size CHANGES, so replacing every node on a click left
 *  all but the one that grew (the clicked agent) invisible until a reload. */
export function keepMeasured(prev: Node[], next: Node[]): Node[] {
  const sizes = new Map(prev.map((n) => [n.id, n.measured]));
  return next.map((n) => {
    const m = sizes.get(n.id);
    return m?.width !== undefined && m?.height !== undefined ? { ...n, measured: m } : n;
  });
}

function CanvasInner({
  workflow, issues, selection, onSelect, onDropPayload, onMoveAgent, highlight, readOnly, onBind, onUnbind,
}: {
  /** Add / remove a tool on an agent, from its chips. */
  onBind?: (agent: string, tool: string) => void;
  onUnbind?: (agent: string, tool: string) => void;
  workflow: Workflow;
  issues: Issue[];
  selection: Selection;
  onSelect: (s: Selection) => void;
  onDropPayload: (p: DragPayload, t: DropTarget) => void;
  onMoveAgent: (id: string, t: DropTarget) => void;
  /** Agents to draw as just changed (AgentExpress Assistant). */
  highlight?: string[];
  /** A preview: nothing can be dragged. */
  readOnly?: boolean;
}) {
  const rf = useReactFlow();
  /** The agent whose tools show as chips: the one last clicked, kept while one of its
   *  chips is selected. */
  const [expanded, setExpanded] = useState<string | null>(null);
  useEffect(() => {
    if (selection?.kind === "agent") setExpanded(selection.id);
    else if (selection === null || selection.kind === "stage") setExpanded(null);
  }, [selection]);
  const l = useMemo(() => layout(workflow, readOnly ? null : expanded), [workflow, expanded, readOnly]);
  const [version, setVersion] = useState(0);

  const derived = useMemo(() => {
    const count = (pred: (i: Issue) => boolean) => issues.filter((i) => i.severity === "error" && pred(i)).length;
    const nodes: Node[] = [];
    for (const s of l.stages) {
      const step = workflow.steps[s.index];
      const label = s.kind === "single"
        ? "Stage"
        : `${step.gateName ?? (s.kind === "parallel" ? "Parallel" : "Sequence")} · ${s.members.length} ${s.kind === "parallel" ? "in parallel" : "in order"}`;
      nodes.push({
        id: `stage-${s.index}`, type: "stage", position: { x: s.x, y: s.y },
        draggable: false, selectable: false,
        data: {
          index: s.index, label, gated: Boolean(step.hitl),
          branch: step.branch ? branchSummary(step.branch as never) : null,
          errors: count((i) => i.where.kind === "step" && i.where.index === s.index),
          selected: selection?.kind === "stage" && selection.index === s.index,
          w: s.w, h: s.h,
        } satisfies StageData,
        style: { width: s.w, height: s.h },
      });
    }
    for (const a of l.agents) {
      const spec = workflow.agents[a.id] ?? {};
      const remote = spec.runtime === "a2a";
      const subtitle = toolsOf(spec.tool).length
        ? toolsLabel(spec.tool, spec.corpus ? String(spec.corpus) : undefined)
        : remote ? "remote agent (A2A)" : String(spec.runtime ?? "main");
      nodes.push({
        id: `agent-${a.id}`, type: "agent", parentId: `stage-${a.stage}`,
        position: { x: a.x, y: a.y }, draggable: !readOnly, selectable: false,
        data: {
          id: a.id, name: String(spec.name ?? a.id), subtitle, remote,
          expanded: !readOnly && expanded === a.id && !remote, h: a.h,
          tools: toolsOf(spec.tool),
          addable: Object.keys(workflow.tools ?? {}).filter((k) => !toolsOf(spec.tool).includes(k)),
          onChip: (k: string) => onSelect({ kind: "tool", id: k }),
          ...(onUnbind ? { onUnbind: (k: string) => onUnbind(a.id, k) } : {}),
          ...(onBind ? { onBind: (k: string) => onBind(a.id, k) } : {}),
          errors: count((i) => i.where.kind === "agent" && i.where.id === a.id),
          selected: selection?.kind === "agent" && selection.id === a.id,
          changed: Boolean(highlight?.includes(a.id)),
        } satisfies AgentData,
      });
    }
    if (l.stages.length) {
      nodes.push({ id: "end", type: "end", position: l.end, draggable: false, selectable: false, data: {} });
    }

    const edges: Edge[] = [];
    const stroke = { stroke: "var(--color-text-body-secondary-cw8ms9, #5f6b7a)" };
    l.stages.forEach((s, i) => {
      const next = i + 1 < l.stages.length ? `stage-${i + 1}` : "end";
      edges.push({ id: `flow-${i}`, source: `stage-${i}`, target: next, type: "smoothstep", style: stroke, markerEnd: { type: MarkerType.ArrowClosed } });
      const step = workflow.steps[s.index];
      if (step.sequence) {
        const m = stepAgents(step);
        for (let k = 1; k < m.length; k++) {
          edges.push({ id: `seq-${i}-${k}`, source: `agent-${m[k - 1]}`, target: `agent-${m[k]}`, style: stroke, markerEnd: { type: MarkerType.ArrowClosed } });
        }
      }
      const b = step.branch;
      if (b) {
        const targets = [...(b.when ?? []).map((r) => String(r.goto ?? "")), ...(b.default ? [b.default] : [])];
        targets.forEach((t, k) => {
          const to = t === "END" ? "end" : l.stages.find((x) => x.name === t);
          if (!to) return;
          edges.push({
            id: `branch-${i}-${k}`, source: `stage-${i}`, sourceHandle: "branch",
            target: to === "end" ? "end" : `stage-${to.index}`, type: "smoothstep",
            label: k < (b.when?.length ?? 0) ? "if" : "otherwise", animated: false,
            style: { strokeDasharray: "6 4", stroke: "#0972d3" }, markerEnd: { type: MarkerType.ArrowClosed },
          });
        });
      }
    });
    return { nodes, edges };
  }, [l, workflow, issues, selection, highlight, readOnly, expanded, onBind, onUnbind, onSelect]);

  const [nodes, setNodes, onNodesChange] = useNodesState<Node>(derived.nodes);
  useEffect(() => { setNodes((prev) => keepMeasured(prev, derived.nodes)); }, [derived, version, setNodes]);

  // Refit when the SHAPE changes (a stage added, removed or widened), so a new stage is
  // never drawn off-screen — but not on every edit, which would fight a user who has
  // deliberately zoomed in to work on one part.
  const shapeKey = l.stages.map((s) => `${s.members.length}:${s.h}`).join(",");
  useEffect(() => {
    const t = setTimeout(() => { void rf.fitView(FIT); }, 30);
    return () => clearTimeout(t);
  }, [shapeKey, rf]);

  const pointFromEvent = (e: { clientX: number; clientY: number }) =>
    rf.screenToFlowPosition({ x: e.clientX, y: e.clientY });

  const onDrop = useCallback((e: DragEvent) => {
    e.preventDefault();
    const raw = e.dataTransfer.getData(DND_MIME);
    if (!raw) return;
    const p = pointFromEvent(e);
    onDropPayload(JSON.parse(raw) as DragPayload, hitTest(l, p.x, p.y));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [l, onDropPayload]);

  return (
    <div
      className="axb-canvas"
      onDragOver={(e) => { e.preventDefault(); e.dataTransfer.dropEffect = "move"; }}
      onDrop={onDrop}
      aria-label="Workflow canvas. Drag agents and tools here; every drag also has a control in the inspector."
    >
      <ReactFlow
        nodes={nodes} edges={derived.edges} nodeTypes={nodeTypes}
        onNodesChange={onNodesChange}
        onNodeClick={(_, n) => {
          if (n.type === "agent") onSelect({ kind: "agent", id: String((n.data as AgentData).id) });
          else if (n.type === "stage") onSelect({ kind: "stage", index: Number((n.data as StageData).index) });
        }}
        onPaneClick={() => onSelect(null)}
        onNodeDragStop={(_, n) => {
          if (n.type !== "agent") return;
          const abs = rf.getInternalNode(n.id)?.internals.positionAbsolute ?? n.position;
          const id = String((n.data as AgentData).id);
          let t = hitTest(l, abs.x + AGENT_W / 2, abs.y + AGENT_H / 2);   // the agent's head row
          if (t.kind === "agent") {
            const a = l.agents.find((x) => x.id === (t as { id: string }).id)!;
            t = { kind: "stage", index: a.stage };
          }
          onMoveAgent(id, t);
          setVersion((v) => v + 1);        // snap back to the derived layout either way
        }}
        nodesConnectable={false} elementsSelectable={false} fitView fitViewOptions={FIT}
        minZoom={0.2} maxZoom={1.6} proOptions={{ hideAttribution: true }}
      >
        <Background gap={16} />
        <Controls showInteractive={false} />
        <MiniMap pannable zoomable />
      </ReactFlow>
      {workflow.steps.length === 0 ? (
        <div className="axb-empty">Drag <b>New agent</b> here to start.</div>
      ) : null}
    </div>
  );
}

export function Canvas(props: Parameters<typeof CanvasInner>[0]) {
  return (
    <ReactFlowProvider>
      <CanvasInner {...props} />
    </ReactFlowProvider>
  );
}
