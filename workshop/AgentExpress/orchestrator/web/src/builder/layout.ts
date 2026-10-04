/** Where everything sits on the Builder canvas, and what a drop lands on.
 *
 *  Positions are DERIVED from `steps`, never stored. The canvas is a view of the
 *  workflow, not a second document: dragging an agent changes which stage it is in, and
 *  the layout follows. That is what stops the canvas from ever drawing a shape the
 *  orchestrator cannot run — there is no free-form graph to get out of step with the
 *  `steps` grammar, because there is no free-form graph. */

import { toolsOf } from "../lib/tools";
import { stageKind, stepAgents, stepName, type StageKind, type Workflow } from "./model";

export const AGENT_W = 220;
export const AGENT_H = 60;
export const GAP_X = 20;
export const PAD = 16;
export const HEADER = 30;
export const STAGE_GAP = 72;
/** An expanded agent's tool chips: two to a row, plus the "+ tool" chip. */
export const CHIP_ROW = 26;
export function agentHeight(wf: Workflow, id: string, expanded?: string | null): number {
  if (id !== expanded) return AGENT_H;
  const chips = toolsOf(wf.agents[id]?.tool).length + 1;
  return AGENT_H + Math.ceil(chips / 2) * CHIP_ROW + 6;
}

export interface StageBox {
  index: number;
  name: string;
  kind: StageKind;
  x: number;
  y: number;
  w: number;
  h: number;
  members: string[];
}

export interface AgentBox {
  id: string;
  stage: number;
  /** Relative to its stage. */
  x: number;
  y: number;
  /** Taller when expanded to show its tools. */
  h: number;
}

export interface Layout {
  stages: StageBox[];
  agents: AgentBox[];
  /** Where END sits, below the last stage. */
  end: { x: number; y: number };
}

export function layout(wf: Workflow, expanded?: string | null): Layout {
  const stages: StageBox[] = [];
  const agents: AgentBox[] = [];
  let y = 0;
  wf.steps.forEach((step, index) => {
    const members = stepAgents(step);
    const n = Math.max(1, members.length);
    const w = PAD * 2 + n * AGENT_W + (n - 1) * GAP_X;
    const tallest = Math.max(AGENT_H, ...members.map((id) => agentHeight(wf, id, expanded)));
    const h = HEADER + PAD + tallest + PAD;
    stages.push({ index, name: stepName(step, index), kind: stageKind(step), x: -w / 2, y, w, h, members });
    members.forEach((id, i) => {
      agents.push({ id, stage: index, x: PAD + i * (AGENT_W + GAP_X), y: HEADER + PAD, h: agentHeight(wf, id, expanded) });
    });
    y += h + STAGE_GAP;
  });
  return { stages, agents, end: { x: -40, y } };
}

export type DropTarget =
  | { kind: "agent"; id: string }
  | { kind: "stage"; index: number }
  | { kind: "gap"; index: number };

/** What a point on the canvas (flow coordinates) is over.
 *
 *  An agent box first (dropping a TOOL there binds it), then a stage box (dropping an
 *  AGENT there joins the stage as a group), otherwise the gap nearest the point — a new
 *  stage at that position. Gaps are between stage centres, so the whole canvas is a
 *  valid drop target and nothing can be dropped "nowhere". */
export function hitTest(l: Layout, x: number, y: number): DropTarget {
  for (const a of l.agents) {
    const s = l.stages[a.stage];
    const ax = s.x + a.x;
    const ay = s.y + a.y;
    if (x >= ax && x <= ax + AGENT_W && y >= ay && y <= ay + a.h) return { kind: "agent", id: a.id };
  }
  for (const s of l.stages) {
    if (x >= s.x && x <= s.x + s.w && y >= s.y && y <= s.y + s.h) return { kind: "stage", index: s.index };
  }
  let gap = l.stages.length;
  for (const s of l.stages) {
    if (y < s.y + s.h / 2) {
      gap = s.index;
      break;
    }
  }
  return { kind: "gap", index: gap };
}
