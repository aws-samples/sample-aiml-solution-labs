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
 *    primitive (guardrail, memory, policy, skill, evaluator, identity, trigger, interceptor)
 *                                 where attach.ts says; while one is dragged, only
 *                                 its valid targets are highlighted
 *    a row of an agent card       on another agent -> moves there (componentOps.ts);
 *                                 anywhere off its agent -> taken off the agent
 *
 *  Each row of an agent card is what is attached to it: click it to see and change its
 *  settings in the inspector, × (or Delete) to take it off. "+ Add" on every agent adds
 *  anything of the build to it, or creates a new one.
 *
 *  Every drag has a keyboard equivalent (the Stage select, the move buttons, the row's ×
 *  and the "+ Add" menu), because drag-and-drop alone is not accessible. */

import {
  Background, Controls, Handle, MarkerType, MiniMap, Panel, Position, ReactFlow, ReactFlowProvider,
  useNodesState, useReactFlow, type Edge, type Node, type NodeProps,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useCallback, useEffect, useMemo, useState, type DragEvent } from "react";

import { toolsLabel, toolsOf } from "../lib/tools";
import { branchSummary, monogram } from "../views/Graph";
import { validTargets, type AttachTarget, type Primitive } from "./attach";
import { ComponentMenu } from "./ComponentMenu";
import { addChoices, CREATE_KINDS, findComponent, moveIssue } from "./componentOps";
import {
  ALL_KINDS, COMPONENT_TYPES, componentKey, componentsOf, kindCounts, type Component, type ComponentKind,
} from "./components";
import { AGENT_H, AGENT_W, hitTest, layout, type DropTarget } from "./layout";
import { imageModelOf } from "./meta";
import type { LayerId } from "./layers";
import { stepAgents, taskLabel, type TaskKind, type Workflow } from "./model";
import { AgentCoreRail, type RailId } from "./Rail";
import type { Issue } from "./validate";

export const DND_MIME = "application/x-agentexpress";

export type DragPayload =
  | { kind: "new-agent" }
  /** A step with no model (runtime "task") of this kind. */
  | { kind: "new-task"; task?: TaskKind }
  | { kind: "agent"; id: string }
  | { kind: "tool"; key: string }
  | { kind: "primitive"; primitive: Primitive }
  /** A row of agent `agent`'s card (componentKey), dragged to move it or take it off. */
  | { kind: "attached"; agent: string; key: string };

export type Selection =
  | { kind: "agent"; id: string }
  | { kind: "tool"; id: string }
  | { kind: "stage"; index: number }
  | { kind: "core"; id: RailId }
  /** One row of an agent's card: a tool, guardrail, memory... as attached to that agent. */
  | { kind: "component"; agent: string; key: string }
  | null;

interface AgentData extends Record<string, unknown> {
  id: string;
  name: string;
  subtitle: string;
  /** Rows can be removed and dragged, and "+ Add" is shown. */
  editable: boolean;
  h: number;
  /** The row selected in the inspector, if it is one of this agent's (componentKey). */
  selectedRow?: string;
  /** The "+ Add" menu: what can be added (in groups), then what can be created. */
  addGroups: { group: string; items: { label: string; index: number }[] }[];
  onRow?: (key: string) => void;
  onRemove?: (key: string) => void;
  onAdd?: (index: number) => void;
  onCreate?: (kind: ComponentKind) => void;
  onRowDrag?: (key: string | null) => void;
  errors: number;
  selected: boolean;
  remote: boolean;
  /** A task step (runtime "task"): drawn with its kind instead of a model's monogram. */
  task?: string;
  /** Just changed by AgentExpress Assistant: drawn highlighted until the next change. */
  changed: boolean;
  /** What is attached to this agent, of the kinds the reader shows (components.ts). */
  components: Component[];
  /** A primitive is being dragged and may be dropped here. */
  dropOk?: boolean;
  /** ...or on these of its tool chips (a Cedar policy). */
  dropTools?: string[];
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
  dropOk?: boolean;
}

const hidden = { opacity: 0, pointerEvents: "none" as const };
/** What a task step's icon shows, by kind. */
const TASK_GLYPH: Record<string, string> = { tool: "⚙", code: "</>", extract: "≡", output: "⤓" };

const TAG: Record<ComponentKind, string> = Object.fromEntries(COMPONENT_TYPES.map((t) => [t.id, t.tag])) as Record<ComponentKind, string>;
/** The rail's layer for each kind of component the cards can show (tools and people have none). */
const LAYER_OF: Partial<Record<ComponentKind, LayerId>> = {
  guardrail: "guardrails", memory: "memory", policy: "policy", skill: "skills", evaluation: "evaluations", identity: "identity",
};

function AgentNode({ data }: NodeProps<Node<AgentData>>) {
  return (
    <div
      className={`axb-agent${data.selected ? " axb-selected" : ""}${data.errors ? " axb-has-errors" : ""}${data.changed ? " axb-changed" : ""}${data.dropOk ? " axb-drop-ok" : ""}`}
      style={{ width: AGENT_W, height: data.h }}
      data-testid={`builder-agent-${data.id}`} data-axb-agent={data.id}
      title={data.errors ? `${data.errors} problem(s) — select to see them` : data.name}
    >
      <Handle type="target" position={Position.Left} style={hidden} isConnectable={false} />
      <div className="axb-agent-row">
        <span className={`axb-icon${data.remote ? " axb-remote" : ""}${data.task ? " axb-task" : ""}`} aria-hidden="true">
          {data.task ? TASK_GLYPH[data.task] ?? "T" : monogram(data.name)}</span>
        <span className="axb-text">
          <span className="axb-name">{data.name}</span>
          <span className="axb-sub">{data.subtitle}</span>
        </span>
        {data.errors ? <span className="axb-badge" aria-label={`${data.errors} problems`}>{data.errors}</span> : null}
      </div>
      {data.components.length || data.editable ? (
        <ul className="axb-comps nodrag" aria-label={`Attached to ${data.name}`} data-testid={`components-${data.id}`}
            onClick={(e) => e.stopPropagation()}>
          {data.components.map((c) => {
            const key = componentKey(c);
            const isTool = c.kind === "tool" && c.tool;
            const label = `${TAG[c.kind]}: ${c.name}`;
            return (
              <li key={key}
                  className={`axb-comp${isTool && data.dropTools?.includes(c.tool!) ? " axb-drop-ok" : ""}${data.selectedRow === key ? " axb-comp-selected" : ""}`}
                  data-kind={c.kind} {...(isTool ? { "data-axb-tool": c.tool } : {})}
                  draggable={data.editable && !c.custom}
                  onDragStart={data.editable && !c.custom ? (e) => {
                    e.stopPropagation();
                    const payload: DragPayload = { kind: "attached", agent: data.id, key };
                    e.dataTransfer.setData(DND_MIME, JSON.stringify(payload));
                    e.dataTransfer.effectAllowed = "move";
                    // After dragstart returns: Chrome cancels a drag whose page changes during
                    // dragstart, and this re-renders every card (move targets, the hint).
                    setTimeout(() => data.onRowDrag?.(key), 0);
                  } : undefined}
                  onDragEnd={data.editable ? () => data.onRowDrag?.(null) : undefined}
                  title={`${label}${c.detail ? ` (${c.detail})` : ""}. Click to see its settings${data.editable ? "; drag it off the agent to remove it" : ""}.`}>
                <span className={`axb-comp-tag axb-comp-${c.kind}`}>{TAG[c.kind]}</span>
                <button type="button" className="axb-comp-name axb-comp-link"
                  data-testid={`chip-${data.id}-${isTool ? c.tool : key}`}
                  aria-label={`${label}, settings`}
                  onClick={(e) => { e.stopPropagation(); data.onRow?.(key); }}>{c.name}</button>
                {c.detail ? <span className="axb-comp-detail">{c.detail}</span> : null}
                {data.editable && data.onRemove && !c.custom ? (
                  <button type="button" className="axb-chip-x" aria-label={`Remove ${label} from ${data.name}`}
                    title={`Remove from ${data.name}`}
                    onClick={(e) => { e.stopPropagation(); data.onRemove?.(key); }}>×</button>
                ) : null}
              </li>
            );
          })}
          {data.editable && (data.onAdd || data.onCreate) && !data.remote ? (
            <li className="axb-comp axb-comp-addrow">
              <select className="axb-comp-add" aria-label={`Add to ${data.name}`} value="" data-testid={`add-${data.id}`}
                onClick={(e) => e.stopPropagation()}
                onChange={(e) => {
                  const v = e.target.value;
                  if (v.startsWith("new:")) data.onCreate?.(v.slice(4) as ComponentKind);
                  else if (v) data.onAdd?.(Number(v));
                }}>
                <option value="">+ Add tool, memory, guardrail…</option>
                {data.addGroups.map((g) => (
                  <optgroup key={g.group} label={g.group}>
                    {g.items.map((it) => <option key={it.index} value={String(it.index)}>{it.label}</option>)}
                  </optgroup>
                ))}
                {data.onCreate ? (
                  <optgroup label="Create new">
                    {CREATE_KINDS.map((k) => <option key={k.kind} value={`new:${k.kind}`}>{k.label}</option>)}
                  </optgroup>
                ) : null}
              </select>
            </li>
          ) : null}
        </ul>
      ) : null}
      <Handle type="source" position={Position.Right} style={hidden} isConnectable={false} />
    </div>
  );
}

function StageNode({ data }: NodeProps<Node<StageData>>) {
  return (
    <div
      className={`axb-stage${data.selected ? " axb-selected" : ""}${data.errors ? " axb-has-errors" : ""}${data.dropOk ? " axb-drop-ok" : ""}`}
      style={{ width: data.w, height: data.h }}
    >
      <Handle type="target" position={Position.Top} style={hidden} isConnectable={false} />
      <div className="axb-stage-head">
        <span className="axb-stage-num">{data.index + 1}</span>
        <span className="axb-stage-label">{data.label}</span>
        {data.gated ? <span className="axb-pill axb-pill-gate">HITL gate</span> : null}
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
  workflow, issues, selection, onSelect, onDropPayload, onMoveAgent, highlight, readOnly, dragging,
  onAddChoice, onRemoveComponent, onMoveComponent, onCreate,
}: {
  /** The primitive being dragged from the palette: its valid targets are highlighted. */
  dragging?: Primitive | null;
  /** "+ Add" on agent `agent`: choice `index` of addChoices(project, agent). */
  onAddChoice?: (agent: string, index: number) => void;
  /** Take row `key` (componentKey) off agent `agent`: its ×, or dragged off the agent. */
  onRemoveComponent?: (agent: string, key: string) => void;
  /** Row `key` of agent `from` dropped on another agent, or (a policy) on a Tool row. */
  onMoveComponent?: (from: string, key: string, to: AttachTarget) => void;
  /** "+ Add" → Create new: open where that kind is made. */
  onCreate?: (agent: string, kind: ComponentKind) => void;
  workflow: Workflow;
  issues: Issue[];
  selection: Selection;
  onSelect: (s: Selection) => void;
  onDropPayload: (p: DragPayload, t: AttachTarget) => void;
  onMoveAgent: (id: string, t: DropTarget) => void;
  /** Agents to draw as just changed (AgentExpress Assistant). */
  highlight?: string[];
  /** A preview: nothing can be dragged. */
  readOnly?: boolean;
}) {
  const rf = useReactFlow();
  /** Which kinds of component the agent cards list. All of them unless the reader hides some. */
  const [shown, setShown] = useState<ComponentKind[]>(ALL_KINDS);
  const editable = !readOnly && Boolean(onRemoveComponent || onAddChoice);
  const l = useMemo(() => layout(workflow, editable, shown), [workflow, editable, shown]);
  /** The row being dragged off its card: { agent, key }, so the canvas can say what a drop does. */
  const [rowDrag, setRowDrag] = useState<{ agent: string; key: string } | null>(null);
  // Memoized: a new object on every render would rebuild the nodes, and that renders again.
  const dragged = useMemo(() => (rowDrag ? findComponent({ workflow } as never, rowDrag.agent, rowDrag.key) : undefined),
    [rowDrag, workflow]);
  const [version, setVersion] = useState(0);
  const counts = useMemo(() => kindCounts(workflow), [workflow]);
  /** The rail marks the pieces whose kinds the cards show. */
  const layers = useMemo(() => shown.map((k) => LAYER_OF[k]).filter((x): x is LayerId => Boolean(x)), [shown]);
  /** The AgentCore rail, turned on in the Show menu. */
  const [rail, setRail] = useState(false);
  const targets = useMemo(() => (dragging && !readOnly ? validTargets(workflow, dragging) : null), [workflow, dragging, readOnly]);

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
          w: s.w, h: s.h, dropOk: Boolean(targets?.stages.includes(s.index)),
        } satisfies StageData,
        style: { width: s.w, height: s.h },
      });
    }
    for (const a of l.agents) {
      const spec = workflow.agents[a.id] ?? {};
      const remote = spec.runtime === "a2a";
      const task = spec.runtime === "task" ? String((spec.task as Record<string, unknown> | undefined)?.kind ?? "") : undefined;
      // An image agent says so, with the model that draws: "image · stability.sd3-5-large-v1:0".
      const image = imageModelOf(spec as Record<string, unknown>);
      const subtitle = toolsOf(spec.tool).length
        ? toolsLabel(spec.tool, spec.corpus ? String(spec.corpus) : undefined)
        : remote ? "remote agent (A2A)" : task !== undefined ? taskLabel(task) : image ? `image · ${image.id}` : String(spec.runtime ?? "main");
      const choices = editable && !remote && task === undefined ? addChoices({ workflow } as never, a.id) : [];
      const addGroups: AgentData["addGroups"] = [];
      choices.forEach((c, index) => {
        const g = addGroups.find((x) => x.group === c.group) ?? (addGroups.push({ group: c.group, items: [] }), addGroups[addGroups.length - 1]);
        g.items.push({ label: c.description ? `${c.label} (${c.description})` : c.label, index });
      });
      const moveOk = rowDrag && dragged && rowDrag.agent !== a.id
        && moveIssue({ workflow } as never, rowDrag.agent, a.id, dragged) === null;
      nodes.push({
        id: `agent-${a.id}`, type: "agent", parentId: `stage-${a.stage}`,
        position: { x: a.x, y: a.y }, draggable: !readOnly, selectable: false,
        data: {
          id: a.id, name: String(spec.name ?? a.id), subtitle, remote, task,
          editable: editable && !remote && task === undefined, h: a.h,
          selectedRow: selection?.kind === "component" && selection.agent === a.id ? selection.key : undefined,
          addGroups,
          onRow: (key: string) => onSelect({ kind: "component", agent: a.id, key }),
          ...(onRemoveComponent ? { onRemove: (key: string) => onRemoveComponent(a.id, key) } : {}),
          ...(onAddChoice ? { onAdd: (i: number) => onAddChoice(a.id, i) } : {}),
          ...(onCreate ? { onCreate: (k: ComponentKind) => onCreate(a.id, k) } : {}),
          onRowDrag: (key: string | null) => setRowDrag(key ? { agent: a.id, key } : null),
          errors: count((i) => i.where.kind === "agent" && i.where.id === a.id),
          selected: (selection?.kind === "agent" && selection.id === a.id)
            || (selection?.kind === "component" && selection.agent === a.id),
          changed: Boolean(highlight?.includes(a.id)),
          components: componentsOf(workflow, a.id).filter((c) => shown.includes(c.kind)),
          dropOk: Boolean(targets?.agents.includes(a.id)) || Boolean(moveOk),
          dropTools: targets ? toolsOf(spec.tool).filter((k) => targets.tools.includes(k)) : undefined,
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
  }, [l, workflow, issues, selection, highlight, editable, onAddChoice, onRemoveComponent, onCreate, onSelect, shown, targets,
      rowDrag, dragged]);

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
    const payload = JSON.parse(raw) as DragPayload;
    // A primitive or a card row lands on what is under the pointer: a Tool row (a Cedar
    // policy) or an agent, read from the element itself, else what the layout says is there.
    const el = (payload.kind === "primitive" || payload.kind === "attached") && e.target instanceof Element ? e.target : null;
    const chip = el?.closest("[data-axb-tool]")?.getAttribute("data-axb-tool");
    const agent = el?.closest("[data-axb-agent]")?.getAttribute("data-axb-agent");
    const target: AttachTarget = chip ? { kind: "tool", id: chip } : agent ? { kind: "agent", id: agent } : hitTest(l, p.x, p.y);
    if (payload.kind === "attached") {
      setRowDrag(null);
      const onItsAgent = target.kind === "agent" ? target.id === payload.agent
        : target.kind === "tool" ? Boolean(el?.closest(`[data-axb-agent="${payload.agent}"]`)) : false;
      const c = findComponent({ workflow } as never, payload.agent, payload.key);
      // A policy row dropped on a Tool row moves to that tool; anything dropped on another
      // agent moves there; dropped back on its own agent nothing changes; anywhere else it
      // comes off the agent.
      if (target.kind === "tool" && c?.kind === "policy" && c.tool !== target.id) onMoveComponent?.(payload.agent, payload.key, target);
      else if (onItsAgent) return;
      else if (target.kind === "agent" || (target.kind === "tool" && agent)) {
        onMoveComponent?.(payload.agent, payload.key, { kind: "agent", id: target.kind === "agent" ? target.id : agent! });
      } else onRemoveComponent?.(payload.agent, payload.key);
      return;
    }
    onDropPayload(payload, target);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [l, onDropPayload, onMoveComponent, onRemoveComponent, workflow]);

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
        <Panel position="top-left"><ComponentMenu counts={counts} shown={shown} onChange={setShown}
          rail={readOnly ? undefined : { on: rail, onChange: setRail }} /></Panel>
        {rowDrag && dragged ? (
          <Panel position="bottom-center">
            <div className="axb-drag-hint" role="status" data-testid="row-drag-hint">
              {`Drop ${TAG[dragged.kind]} "${dragged.name}" anywhere off ${String(workflow.agents[rowDrag.agent]?.name ?? rowDrag.agent)} to remove it, or on another agent to move it there.`}
            </div>
          </Panel>
        ) : null}
        {!readOnly && (rail || dragging) ? (
          <Panel position="top-right">
            <AgentCoreRail workflow={workflow} shown={layers} dragging={dragging?.kind}
              selected={selection?.kind === "core" ? selection.id : null}
              onSelect={(id: RailId) => onSelect({ kind: "core", id })} />
          </Panel>
        ) : null}
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
