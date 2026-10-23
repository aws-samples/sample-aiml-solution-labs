/** The AgentCore rail: one narrow strip beside the canvas listing the deployment-wide pieces
 *  of a build (Gateway, Memory, Policy engine, Guardrail, Identity, Observability, Triggers),
 *  each with a count read from workflow.json. It is one rail, not a node per primitive, so the
 *  graph stays the graph.
 *
 *  It is not drawn on the default canvas. It appears when the reader turns it on in the Show
 *  menu, picks a layer there, or starts dragging a primitive; it opens expanded (each piece's
 *  name and count) and can be collapsed to letters. An item whose layer is shown on the
 *  agents, or whose kind is being dragged, is marked. Selecting an item opens it in the
 *  inspector (CorePanel), which is also how a keyboard reaches it: every item is a button. */
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Header from "@cloudscape-design/components/header";
import SpaceBetween from "@cloudscape-design/components/space-between";
import { useState } from "react";

import type { PrimitiveKind } from "./attach";
import { hasDeploymentGuardrail } from "./attach";
import type { Selection } from "./Canvas";
import { interceptorsOf } from "./Interceptors";
import { layersOf, type LayerId } from "./layers";
import type { Entry, Project, Workflow } from "./model";
import { policyBlock } from "./Policies";
import { triggersOf } from "./Triggers";
import { useDocsBase, type DocsFeature } from "../docs/docsLinks";
import { infoSlot } from "../docs/InfoLink";

export type RailId = "gateway" | "memory" | "policy" | "guardrail" | "identity" | "observability" | "triggers";

export interface RailItem {
  id: RailId;
  label: string;
  letter: string;
  count: number;
  /** One line: what the count is, and what else is worth knowing. */
  note: string;
  /** The Show-menu layer this piece is behind, if any. */
  layer?: LayerId;
  /** The agents using it (empty for Gateway, Observability and Triggers, which act for all). */
  users: string[];
}

const keys = (wf: Workflow, m: string) => Object.keys((wf[m] && typeof wf[m] === "object" ? wf[m] : {}) as object);
const plural = (n: number, one: string, many = `${one}s`) => `${n} ${n === 1 ? one : many}`;

/** The rail's items, in rail order, counted from the config alone (nothing is fetched). */
export function railItems(wf: Workflow): RailItem[] {
  const agents = Object.entries(wf.agents ?? {}) as [string, Entry][];
  const using = (layer: LayerId) => agents.filter(([, a]) => layersOf(a).includes(layer)).map(([id]) => id);
  const ics = interceptorsOf(wf);
  const nIcs = (["request", "response"] as const).filter((p) => ics[p]).length;
  const pol = policyBlock(wf);
  const nPol = keys(wf, "policies").length + (pol.custom?.length ?? 0);
  const triggers = Object.values(triggersOf(wf));
  const local = agents.filter(([, a]) => a.runtime !== "a2a").length;
  const nGuard = keys(wf, "guardrails").length + (hasDeploymentGuardrail(wf) ? 1 : 0);
  const nTools = Object.keys(wf.tools ?? {}).length;
  return [
    { id: "gateway", label: "Gateway", letter: "Gw", count: nTools, users: [],
      note: nTools ? `${plural(nTools, "tool")} behind it, ${plural(nIcs, "interceptor")}` : "No tools, so no Gateway is deployed" },
    { id: "memory", label: "Memory", letter: "M", count: keys(wf, "memories").length, layer: "memory", users: using("memory"),
      note: `${plural(keys(wf, "memories").length, "memory", "memories")}, used by ${plural(using("memory").length, "agent")}` },
    { id: "policy", label: "Policy engine", letter: "P", count: nPol, layer: "policy", users: using("policy"),
      note: `${plural(nPol, "policy", "policies")}; the engine is ${pol.enabled === false ? "off" : "on"}${pol.mode ? ` (${pol.mode})` : ""}` },
    { id: "guardrail", label: "Guardrail", letter: "G", count: nGuard, layer: "guardrails", users: using("guardrails"),
      note: `${plural(nGuard, "guardrail")}, checking ${plural(using("guardrails").length, "agent")}` },
    { id: "identity", label: "Identity", letter: "I", count: keys(wf, "identities").length, layer: "identity", users: using("identity"),
      note: `${plural(keys(wf, "identities").length, "identity", "identities")}, used by ${plural(using("identity").length, "agent")}` },
    { id: "observability", label: "Observability", letter: "O", count: local, users: [],
      note: `Always on: every run of the ${plural(local, "agent")} here is traced` },
    { id: "triggers", label: "Triggers", letter: "T", count: triggers.length, users: [],
      note: `${plural(triggers.length, "trigger")}, ${triggers.filter((t) => t.enabled !== false).length} on` },
  ];
}

/** Which rail item a dragged primitive belongs to (skills and evaluators have none). */
export const RAIL_OF: Partial<Record<PrimitiveKind, RailId>> = {
  guardrail: "guardrail", memory: "memory", policy: "policy", identity: "identity", interceptor: "gateway", trigger: "triggers",
};

export function AgentCoreRail({ workflow, shown, dragging, selected, onSelect }: {
  workflow: Workflow;
  /** The layers chosen in the Show menu. */
  shown: LayerId[];
  /** The kind of primitive being dragged, if one is. */
  dragging?: PrimitiveKind | null;
  selected: RailId | null;
  onSelect: (id: RailId) => void;
}) {
  // Always expanded: full names beside the counts, so nobody has to decode a letter.
  const [open, setOpen] = useState(true);
  const items = railItems(workflow);
  const hot = dragging ? RAIL_OF[dragging] : undefined;
  return (
    <nav className={`axb-rail${open ? " axb-rail-open" : ""}`} aria-label="AgentCore, deployment-wide" data-testid="agentcore-rail">
      <button type="button" className="axb-rail-toggle" aria-expanded={open}
        aria-label={open ? "Collapse the AgentCore rail" : "Expand the AgentCore rail"}
        onClick={() => setOpen((o) => !o)}>{open ? "AgentCore ›" : "‹"}</button>
      <ul>
        {items.map((it) => {
          const mark = (it.layer && shown.includes(it.layer)) || hot === it.id;
          return (
            <li key={it.id}>
              <button type="button" data-testid={`rail-${it.id}`}
                className={`axb-rail-item${mark ? " axb-rail-marked" : ""}${selected === it.id ? " axb-selected" : ""}`}
                aria-label={`${it.label}: ${it.count}. ${it.note}`} aria-pressed={selected === it.id}
                title={it.note} onClick={() => onSelect(it.id)}>
                <span className="axb-rail-letter" aria-hidden="true">{it.letter}</span>
                {open ? <span className="axb-rail-label" aria-hidden="true">{it.label}</span> : null}
                <span className="axb-rail-n" aria-hidden="true">{it.count}</span>
              </button>
            </li>
          );
        })}
      </ul>
    </nav>
  );
}

/** The inspector for a rail item: what it is, what this build has, and who uses it. */
/** Each deployment-wide feature's documentation (docs/docsLinks.ts). */
const RAIL_DOCS: Record<RailId, DocsFeature> = {
  gateway: "gateway", memory: "tab.memory", policy: "tab.policies", guardrail: "tab.guardrails",
  identity: "tab.identity", observability: "observability", triggers: "tab.triggers",
};
export function CorePanel({ project, id, onSelect }: { project: Project; id: RailId; onSelect: (s: Selection) => void }) {
  const docs = useDocsBase();
  const wf = project.workflow;
  const it = railItems(wf).find((x) => x.id === id)!;
  const agentLink = (a: string) => (
    <Button key={a} variant="inline-link" onClick={() => onSelect({ kind: "agent", id: a })}>{String(wf.agents[a]?.name ?? a)}</Button>
  );
  const entries: Record<RailId, string[]> = {
    gateway: [],
    memory: keys(wf, "memories"),
    policy: [...keys(wf, "policies"), ...(policyBlock(wf).custom ?? []).map((c) => c.name)],
    guardrail: [...(hasDeploymentGuardrail(wf) ? ["Deployment guardrail"] : []), ...keys(wf, "guardrails")],
    identity: keys(wf, "identities"),
    observability: [],
    triggers: Object.entries(triggersOf(wf)).map(([n, t]) => `${n} (${String(t.type)}${t.enabled === false ? ", off" : ""})`),
  };
  const where: Record<RailId, string> = {
    gateway: "Every tool call goes through it. Tools are on the Tools tab, interceptors on the Interceptors tab.",
    memory: "Defined on the Memory tab; an agent picks one in its AgentCore features.",
    policy: "Cedar rules checked on every tool call. Written on the Policies tab, attached to tools.",
    guardrail: "Defined on the Guardrails tab; an agent turns checking on in its AgentCore features.",
    identity: "Credentials for calling APIs, on the Identity tab.",
    observability: "Traces, logs and cost for each run, in the deployed app. Nothing to switch on.",
    triggers: "What starts a run without anyone typing it, on the Triggers tab.",
  };
  return (
    <SpaceBetween size="m">
      <Header variant="h2" description="Deployment-wide (AgentCore)" info={infoSlot(docs, RAIL_DOCS[id])}>{it.label}</Header>
      <Box>{it.note}.</Box>
      <Box variant="small" color="text-body-secondary">{where[id]}</Box>
      {id === "gateway" ? (
        <SpaceBetween size="xxs">
          <Box variant="h4">Tools</Box>
          {Object.keys(wf.tools).length ? (
            <SpaceBetween direction="horizontal" size="xs">
              {Object.keys(wf.tools).map((k) => (
                <Button key={k} variant="inline-link" onClick={() => onSelect({ kind: "tool", id: k })}>{k}</Button>
              ))}
            </SpaceBetween>
          ) : <Box color="text-body-secondary">None</Box>}
        </SpaceBetween>
      ) : null}
      {entries[id].length ? (
        <SpaceBetween size="xxs">
          <Box variant="h4">In this build</Box>
          <ul className="axb-core-list">{entries[id].map((e) => <li key={e}>{e}</li>)}</ul>
        </SpaceBetween>
      ) : null}
      {it.layer ? (
        <SpaceBetween size="xxs">
          <Box variant="h4">Agents using it</Box>
          {it.users.length
            ? <SpaceBetween direction="horizontal" size="xs">{it.users.map(agentLink)}</SpaceBetween>
            : <Box color="text-body-secondary">None yet</Box>}
        </SpaceBetween>
      ) : null}
    </SpaceBetween>
  );
}
