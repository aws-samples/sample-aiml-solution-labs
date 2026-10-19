/** Primitives in the Builder: the palette group that drags them onto the canvas, and the
 *  agent inspector's Attach and Detach selects, which do the same by keyboard. Both call
 *  attach.ts, so a drop and a pick write the same workflow.json. */
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import ExpandableSection from "@cloudscape-design/components/expandable-section";
import FormField from "@cloudscape-design/components/form-field";
import Select, { type SelectProps } from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import type { DragEvent, ReactNode } from "react";

import {
  attach, attachableTo, attachedTo, detach, KIND_LABEL, KIND_ORDER, primitiveLabel, primitivesOf,
  type AttachTarget, type Primitive, type PrimitiveKind,
} from "./attach";
import { DND_MIME, type DragPayload } from "./Canvas";
import type { Project, Workflow } from "./model";
import { FEATURE_EMOJI } from "../lib/emoji";

/** The Builder tab where each kind is defined (Builder.tsx TAB_ORDER ids). */
export const PRIMITIVE_TAB: Record<PrimitiveKind, string> = {
  guardrail: "guardrails", memory: "memory", policy: "policies", skill: "skills",
  evaluator: "evals", identity: "identity", interceptor: "interceptors", trigger: "triggers",
};

const HINT: Record<PrimitiveKind, string> = {
  guardrail: "Drag onto an agent", memory: "Drag onto an agent", skill: "Drag onto an agent",
  evaluator: "Drag onto an agent", identity: "Drag onto an agent", policy: "Drag onto a tool row of an agent",
  trigger: "Drag onto the first stage", interceptor: "Runs for every agent",
};

/** One collapsible group of the left pane: a component type, with how many the build has.
 *  A group with something in it starts open; an empty one starts closed and says how to add one. */
export function PaletteGroup({ id, title, count, children, defaultExpanded, emoji }: {
  id: string; title: string; count?: number; children: ReactNode; defaultExpanded?: boolean;
  /** Shown before the title (decoration; lib/emoji.ts). */
  emoji?: string;
}) {
  const head = `${emoji ? `${emoji} ` : ""}${title}`;
  return (
    <div className="axb-palette-group" data-testid={`palette-group-${id}`}>
      {/* The count is in the header text: the compact "footer" variant draws no counter. */}
      <ExpandableSection headerText={count === undefined ? head : `${head} (${count})`}
        defaultExpanded={defaultExpanded ?? Boolean(count)} variant="footer">
        <div className="axb-palette-group-body">{children}</div>
      </ExpandableSection>
    </div>
  );
}

/** The palette's component groups (guardrails, memory, policies, skills, evaluators,
 *  interceptors, triggers): every kind, each collapsible, with what this build has of it.
 *  An item drags onto the canvas; Enter (or a click) opens its tab. */
export function PrimitivePalette({ workflow, onDrag, onOpen }: {
  workflow: Workflow;
  /** The primitive being dragged, so the canvas can highlight where it may go; null when it ends. */
  onDrag: (p: Primitive | null) => void;
  onOpen: (kind: PrimitiveKind) => void;
}) {
  const all = primitivesOf(workflow);
  return (
    <div className="axb-primitives" data-testid="palette-primitives">
      {KIND_ORDER.map((kind) => (
        <PaletteGroup key={kind} id={kind} title={KIND_LABEL[kind]} emoji={FEATURE_EMOJI[kind]} count={all.filter((p) => p.kind === kind).length}>
          {all.some((p) => p.kind === kind) ? null : (
            <Box variant="small" color="text-body-secondary">
              None in this build.{" "}
              <Button variant="inline-link" onClick={() => onOpen(kind)}>Add on the {KIND_LABEL[kind]} tab</Button>
            </Box>
          )}
          {all.filter((p) => p.kind === kind).map((p) => (
            <div key={`${kind}:${p.key}`} className="axb-palette-item axb-palette-primitive" draggable role="button" tabIndex={0}
              data-testid={`palette-${kind}-${p.key || "deployment"}`}
              title={`${HINT[kind]}, or press Enter to open the ${KIND_LABEL[kind]} tab`}
              onDragStart={(e: DragEvent) => {
                const payload: DragPayload = { kind: "primitive", primitive: p };
                e.dataTransfer.setData(DND_MIME, JSON.stringify(payload));
                e.dataTransfer.effectAllowed = "move";
                onDrag(p);
              }}
              onDragEnd={() => onDrag(null)}
              onClick={() => onOpen(kind)}
              onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onOpen(kind); } }}>
              <span className="axb-palette-label"><span className="axb-emoji" aria-hidden="true">{FEATURE_EMOJI[kind]}</span>{primitiveLabel(p)}</span>
              <span className="axb-palette-sub">{HINT[kind]}</span>
            </div>
          ))}
        </PaletteGroup>
      ))}
    </div>
  );
}

type Pair = { primitive: Primitive; target: AttachTarget };

/** Options grouped by kind; a policy says which tool it goes on. */
function optionsOf(pairs: Pair[]): SelectProps.OptionGroup[] {
  return KIND_ORDER.filter((k) => pairs.some((x) => x.primitive.kind === k)).map((kind) => ({
    label: KIND_LABEL[kind],
    options: pairs.map((x, i) => ({ x, i })).filter(({ x }) => x.primitive.kind === kind).map(({ x, i }) => ({
      value: String(i), label: primitiveLabel(x.primitive),
      ...(x.target.kind === "tool" ? { description: `On tool ${x.target.id}, for every agent that calls it` } : {}),
    })),
  }));
}

/** The agent inspector's keyboard path for every drop onto this agent (and onto its tools). */
export function AttachControl({ project, id, onChange }: {
  project: Project; id: string; onChange: (p: Project) => void;
}) {
  const wf = project.workflow;
  const can = attachableTo(wf, id);
  const has = attachedTo(wf, id);
  if (!can.length && !has.length) return null;
  const apply = (r: { workflow: Workflow }) => { if (r.workflow !== wf) onChange({ ...project, workflow: r.workflow }); };
  return (
    <SpaceBetween size="s">
      <FormField label="Attach" description="The same as dragging one from Primitives onto this agent (a policy: onto its tool).">
        <Select selectedOption={null} placeholder={can.length ? "Attach…" : "Everything is attached"} disabled={!can.length}
          ariaLabel="Attach a primitive" options={optionsOf(can)}
          onChange={({ detail }) => {
            const x = can[Number(detail.selectedOption.value)];
            if (x) apply(attach(wf, x.primitive, x.target));
          }} />
      </FormField>
      {has.length ? (
        <FormField label="Detach">
          <Select selectedOption={null} placeholder="Detach…" ariaLabel="Detach a primitive" options={optionsOf(has)}
            onChange={({ detail }) => {
              const x = has[Number(detail.selectedOption.value)];
              if (x) apply(detach(wf, x.primitive, x.target));
            }} />
        </FormField>
      ) : null}
    </SpaceBetween>
  );
}
