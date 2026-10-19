/** The inspector for what is attached to an agent: the agent's list of them (with Add and
 *  Remove), and the panel for one of them, opened by clicking its row on the canvas.
 *
 *  A row's panel shows, in this order: what it is and which agent it is on (with a way back
 *  to the agent); how THIS agent uses it (guardrail sides, auto-scoring, questions a step);
 *  then the definition itself, edited in place with the same form its tab uses (ItemForm),
 *  so nobody has to leave the canvas to see or change what an agent is wired to. */
import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import Input from "@cloudscape-design/components/input";
import Select, { type SelectProps } from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Toggle from "@cloudscape-design/components/toggle";
import { useContext, type ReactNode } from "react";

import { toolsOf } from "../lib/tools";
import type { Selection } from "./Canvas";
import { addChoices, CREATE_KINDS } from "./componentOps";
import { COMPONENT_TYPES, componentKey, componentsOf, type Component, type ComponentKind } from "./components";
import { ItemForm, LibraryContext } from "./Library";
import { vocabExtra } from "./meta";
import type { Entry, Json, Project } from "./model";
import type { LibraryKind } from "./storage";

const TYPE = Object.fromEntries(COMPONENT_TYPES.map((t) => [t.id, t])) as Record<ComponentKind, (typeof COMPONENT_TYPES)[number]>;

/** The Builder tab each kind is defined on (Builder.tsx tab ids). */
export const COMPONENT_TAB: Partial<Record<ComponentKind, { id: string; label: string }>> = {
  tool: { id: "tools", label: "Tools" }, guardrail: { id: "guardrails", label: "Guardrails" },
  memory: { id: "memory", label: "Memory" }, policy: { id: "policies", label: "Policies" },
  skill: { id: "skills", label: "Skills" }, evaluation: { id: "evals", label: "Evals" },
  identity: { id: "identity", label: "Identity" },
};

export interface RowActions {
  onRemoveComponent?: (agent: string, key: string) => void;
  onAddChoice?: (agent: string, index: number) => void;
  onCreate?: (agent: string, kind: ComponentKind) => void;
  onOpenTab?: (tab: string) => void;
}

const isObj = (v: unknown): v is Record<string, Json> => typeof v === "object" && v !== null && !Array.isArray(v);

/** The agent panel's "Attached to this agent": every row of its card, each opening its own
 *  panel, with Remove; and Add, the same menu as the card's "+ Add". */
export function AttachedList({ project, id, onSelect, onRemoveComponent, onAddChoice, onCreate }: {
  project: Project; id: string; onSelect: (s: Selection) => void;
} & RowActions) {
  const rows = componentsOf(project.workflow, id);
  const choices = addChoices(project, id);
  const agentName = String(project.workflow.agents[id]?.name ?? id);
  const groups: SelectProps.OptionGroup[] = [];
  choices.forEach((c, i) => {
    let g = groups.find((x) => x.label === c.group);
    if (!g) { g = { label: c.group, options: [] }; groups.push(g); }
    (g.options as SelectProps.Option[]).push({ value: String(i), label: c.label, ...(c.description ? { description: c.description } : {}) });
  });
  if (onCreate) groups.push({ label: "Create new", options: CREATE_KINDS.map((k) => ({ value: `new:${k.kind}`, label: k.label })) });
  return (
    <SpaceBetween size="s">
      <Header variant="h3" counter={`(${rows.length})`}
        description="Everything this agent uses. Click one to see and change its settings.">
        Attached to this agent
      </Header>
      {rows.length ? (
        <ul className="axb-attached" data-testid={`attached-${id}`}>
          {rows.map((c) => {
            const key = componentKey(c);
            return (
              <li key={key}>
                <span className={`axb-comp-tag axb-comp-${c.kind}`}>{TYPE[c.kind].tag}</span>
                <Button variant="inline-link" ariaLabel={`${TYPE[c.kind].tag}: ${c.name}, settings`}
                  onClick={() => onSelect({ kind: "component", agent: id, key })}>{c.name}</Button>
                {c.detail ? <span className="axb-comp-detail">{c.detail}</span> : null}
                {onRemoveComponent ? (
                  <Button variant="icon" iconName="close" ariaLabel={`Remove ${TYPE[c.kind].tag}: ${c.name} from ${agentName}`}
                    onClick={() => onRemoveComponent(id, key)} />
                ) : null}
              </li>
            );
          })}
        </ul>
      ) : <Box color="text-body-secondary">Nothing yet. Add a tool, memory, guardrail or anything else below.</Box>}
      {onAddChoice || onCreate ? (
        <Select selectedOption={null} placeholder="Add a tool, memory, guardrail…" ariaLabel="Add to this agent"
          options={groups} filteringType="auto" empty="Nothing more to add"
          onChange={({ detail }) => {
            const v = String(detail.selectedOption.value ?? "");
            if (v.startsWith("new:")) onCreate?.(id, v.slice(4) as ComponentKind);
            else if (v) onAddChoice?.(id, Number(v));
          }} />
      ) : null}
    </SpaceBetween>
  );
}

/** Set one agentcore key on agent `id`; undefined removes it, an emptied group goes. */
function setCore(p: Project, id: string, group: string, key: string, v: Json | undefined): Project {
  const next = structuredClone(p);
  const a = next.workflow.agents[id];
  const core = isObj(a.agentcore) ? { ...a.agentcore } : {};
  const g = isObj(core[group]) ? { ...core[group] } : {};
  if (v === undefined) delete g[key]; else g[key] = v;
  if (Object.keys(g).length) core[group] = g; else delete core[group];
  if (Object.keys(core).length) a.agentcore = core; else delete a.agentcore;
  return next;
}

/** A named definition (workflow.<map>.<key>) replaced. */
function setNamed(p: Project, map: string, key: string, entry: Entry | undefined): Project {
  const next = structuredClone(p);
  const m = isObj(next.workflow[map]) ? { ...(next.workflow[map] as Record<string, Entry>) } : {};
  if (entry === undefined) delete m[key]; else m[key] = entry;
  next.workflow[map] = m as never;
  return next;
}

const MAP: Partial<Record<ComponentKind, { map: string; kind: LibraryKind }>> = {
  guardrail: { map: "guardrails", kind: "guardrail" }, memory: { map: "memories", kind: "memory" },
  policy: { map: "policies", kind: "policy" }, skill: { map: "skills", kind: "skill" },
  identity: { map: "identities", kind: "identity" },
};

/** The definition a row names, edited in place; a note when there is none to edit. */
function Definition({ project, c, onChange, onOpenTab }: {
  project: Project; c: Component; onChange: (p: Project) => void; onOpenTab?: (tab: string) => void;
}) {
  const ctx = useContext(LibraryContext);
  const wf = project.workflow;
  const tab = COMPONENT_TAB[c.kind];
  const openTab = tab && onOpenTab ? <Button variant="inline-link" onClick={() => onOpenTab(tab.id)}>{`Open the ${tab.label} tab`}</Button> : null;
  if (c.kind === "guardrail" && c.ref === "") {
    const g = wf.guardrail;
    return isObj(g) && Object.keys(g).length ? (
      <SpaceBetween size="s">
        <Box variant="h4">The deployment guardrail</Box>
        <Box variant="small" color="text-body-secondary">Shared by every agent that checks with it; changing it here changes it for all of them.</Box>
        <ItemForm kind="guardrail" value={g as Entry}
          onChange={(e) => onChange({ ...project, workflow: { ...wf, guardrail: e as never } })} />
      </SpaceBetween>
    ) : <Box color="text-body-secondary">The default guardrail. {openTab}</Box>;
  }
  if (c.kind === "evaluation" && c.ref?.startsWith("Custom.")) {
    const name = c.ref.slice("Custom.".length);
    const def = (wf.evaluators as Record<string, Entry> | undefined)?.[name];
    if (def) {
      return (
        <SpaceBetween size="s">
          <Box variant="h4">Definition</Box>
          <ItemForm kind="evaluator" value={def} onChange={(e) => onChange(setNamed(project, "evaluators", name, e))} />
        </SpaceBetween>
      );
    }
    return <Box color="text-body-secondary">This agent&apos;s own evaluator: edit it under the agent&apos;s AgentCore features.</Box>;
  }
  if (c.kind === "evaluation") {
    const levels = vocabExtra<Record<string, string>>("builtinEvaluators", "levels") ?? {};
    const level = c.ref ? levels[c.ref] : undefined;
    return (
      <Box color="text-body-secondary">
        {c.ref ? `A built-in AgentCore Evaluations judge${level ? `, scored per ${String(level).toLowerCase().replace("_", " ")}` : ""}. Nothing to define.`
          : "No evaluator is picked, so the default (Faithfulness) scores this agent."}
      </Box>
    );
  }
  const m = MAP[c.kind];
  if (m && c.ref) {
    const def = (wf[m.map] as Record<string, Entry> | undefined)?.[c.ref];
    if (!def) return <Alert type="warning">{`${c.ref} is not defined in this build.`} {openTab}</Alert>;
    const live = ctx.liveId(m.map as never, c.ref);
    return (
      <SpaceBetween size="s">
        <Box variant="h4">Definition</Box>
        {live ? (
          <Alert type="info" header="From the library">
            Changing it here makes it this build&apos;s own copy. To change it for every build, edit it in the library.
          </Alert>
        ) : null}
        <ItemForm kind={m.kind} value={def} onChange={(e) => onChange(setNamed(project, m.map, c.ref!, e))} />
        <Box variant="small" color="text-body-secondary">{`Also on the ${tab?.label ?? ""} tab.`}</Box>
      </SpaceBetween>
    );
  }
  if (c.kind === "memory") {
    return <Box color="text-body-secondary">Memory written on this agent itself (its strategies are listed above). To manage it as a named memory, define one on the Memory tab. {openTab}</Box>;
  }
  if (c.kind === "policy") {
    return <Box color="text-body-secondary">The policy engine checks each of this agent&apos;s tool calls against the Cedar policies on its tools. Policies are written on the Policies tab. {openTab}</Box>;
  }
  return null;
}

/** How this agent uses the row: the per-agent settings, where the row has any. */
function Usage({ project, id, c, onChange }: { project: Project; id: string; c: Component; onChange: (p: Project) => void }) {
  const wf = project.workflow;
  const a = wf.agents[id];
  const core = (isObj(a.agentcore) ? a.agentcore : {}) as Record<string, Record<string, Json>>;
  if (c.kind === "guardrail") {
    const g = core.guardrails ?? {};
    const side = (s: "input" | "output", on: boolean) => {
      const other = s === "input" ? "output" : "input";
      if (!on && !g[other]) return; // turning both off is Remove
      onChange(setCore(project, id, "guardrails", s, on || undefined));
    };
    return (
      <SpaceBetween size="xs">
        <Toggle checked={Boolean(g.input)} onChange={({ detail }) => side("input", detail.checked)}>Check what the agent is given (input)</Toggle>
        <Toggle checked={Boolean(g.output)} onChange={({ detail }) => side("output", detail.checked)}>Check what the agent answers (output)</Toggle>
      </SpaceBetween>
    );
  }
  if (c.kind === "evaluation") {
    return (
      <Toggle checked={Boolean(core.evaluations?.auto)}
        onChange={({ detail }) => onChange(setCore(project, id, "evaluations", "auto", detail.checked || undefined))}>
        Score every run automatically (otherwise on demand)
      </Toggle>
    );
  }
  if (c.kind === "human") {
    const ask = isObj(a.askPerson) ? a.askPerson : {};
    const set = (k: "maxQuestions" | "timeout", v: Json | undefined) => {
      const next = structuredClone(project);
      const cur = { ...ask };
      if (v === undefined) delete cur[k]; else cur[k] = v;
      next.workflow.agents[id].askPerson = Object.keys(cur).length ? cur : true;
      onChange(next);
    };
    return (
      <SpaceBetween size="s">
        <FormField label="Questions a step" description="1 to 10. Defaults to 3.">
          <Input type="number" inputMode="numeric" value={ask.maxQuestions === undefined ? "" : String(ask.maxQuestions)} placeholder="3"
            onChange={({ detail }) => {
              const n = Number(detail.value);
              set("maxQuestions", detail.value === "" || !Number.isInteger(n) ? undefined : Math.max(1, Math.min(10, n)));
            }} />
        </FormField>
        <FormField label="Give up after" description="With no answer by then (e.g. 30m, 24h, 7d; at most 30d), the agent carries on without one.">
          <Input value={typeof ask.timeout === "string" ? ask.timeout : ""} placeholder="No time limit"
            onChange={({ detail }) => set("timeout", detail.value.trim() || undefined)} />
        </FormField>
        <Box variant="small" color="text-body-secondary">The agent needs Tool mode &quot;model&quot; to ask.</Box>
      </SpaceBetween>
    );
  }
  if (c.kind === "policy" && c.tool) {
    const users = Object.entries(wf.agents).filter(([, x]) => toolsOf(x.tool).includes(c.tool!)).map(([k, x]) => String(x.name ?? k));
    return (
      <Alert type="info">
        {`This policy is on the tool ${c.tool}, so it decides that tool's calls for every agent that calls it: ${users.join(", ")}.`}
      </Alert>
    );
  }
  return null;
}

/** The panel for one row of an agent's card. */
export function ComponentPanel({ project, agent, componentKey: key, onChange, onSelect, toolPanel, ...actions }: {
  project: Project; agent: string; componentKey: string;
  onChange: (p: Project) => void; onSelect: (s: Selection) => void;
  /** The tool's own panel, for a Tool row. */
  toolPanel: (tool: string) => ReactNode;
} & RowActions) {
  const wf = project.workflow;
  const c = componentsOf(wf, agent).find((x) => componentKey(x) === key);
  const agentName = String(wf.agents[agent]?.name ?? agent);
  if (!c) return null;
  const tab = COMPONENT_TAB[c.kind];
  return (
    <SpaceBetween size="l">
      <Button variant="inline-link" iconName="arrow-left" onClick={() => onSelect({ kind: "agent", id: agent })}>
        {`Back to ${agentName}`}
      </Button>
      <Header variant="h2" description={`${TYPE[c.kind].hint}. Attached to ${agentName}${c.detail ? ` · ${c.detail}` : ""}.`}
        actions={
          <SpaceBetween direction="horizontal" size="xs">
            {actions.onRemoveComponent ? (
              <Button iconName="close" onClick={() => actions.onRemoveComponent!(agent, key)}>{`Remove from ${agentName}`}</Button>
            ) : null}
            {tab && actions.onOpenTab && c.kind !== "tool" ? <Button onClick={() => actions.onOpenTab!(tab.id)}>{`${tab.label} tab`}</Button> : null}
          </SpaceBetween>
        }>
        {`${TYPE[c.kind].tag}: ${c.name}`}
      </Header>
      <Usage project={project} id={agent} c={c} onChange={onChange} />
      {c.kind === "tool" && c.tool && wf.tools[c.tool] ? toolPanel(c.tool) : (
        <Definition project={project} c={c} onChange={onChange} onOpenTab={actions.onOpenTab} />
      )}
      <Box variant="small" color="text-body-secondary">
        {`Tip: drag this row off ${agentName} on the canvas to remove it, or onto another agent to move it. Delete removes the selected row.`}
      </Box>
    </SpaceBetween>
  );
}
