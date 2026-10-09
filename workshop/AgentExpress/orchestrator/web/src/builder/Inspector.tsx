/** The right-hand panel: every setting of whatever is selected on the canvas.
 *
 *  It is also the keyboard path for everything the canvas does by dragging — which
 *  stage an agent is in, which tool it reads, the order inside a group — because a
 *  builder you can only operate with a mouse is not one everybody can use. */

import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Container from "@cloudscape-design/components/container";
import ExpandableSection from "@cloudscape-design/components/expandable-section";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import Autosuggest from "@cloudscape-design/components/autosuggest";
import Input from "@cloudscape-design/components/input";
import Multiselect from "@cloudscape-design/components/multiselect";
import SegmentedControl from "@cloudscape-design/components/segmented-control";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Textarea from "@cloudscape-design/components/textarea";
import Toggle from "@cloudscape-design/components/toggle";
import { useContext, useEffect, useState } from "react";
import { LibraryContext, pickOptions, type LibraryCtx } from "./Library";
import type { LibraryKind } from "./storage";

import { toolsOf } from "../lib/tools";
import type { Selection } from "./Canvas";
import { CodeTool } from "./CodeTool";
import { EntryForm } from "./EntryForm";
import { AGENT_ID_RE, TOOL_KEY_RE, vocab } from "./meta";
import {
  setTools, defaultPrompt, insertStage, joinStage, moveMember, moveStage, removeAgent,
  removeTool, renameAgent, renameTool, setStageKind, stageKind, stageOf, stepAgents, stepName,
  unplace, updateEntry, updateStage, type Entry, type GateSpec, type Json, type Project, type StepSpec,
} from "./model";
import { IssueList } from "./IssueList";
import { KbDocuments, SecretField } from "./BuildResources";
import { ToolConnect } from "./ToolConnect";
import { EvaluatorPicker, type CustomEvaluator } from "./Features";
import { capsLabel, fits, needsOf, useModels } from "./models";

const FRAMEWORK_OPTIONS = [
  { value: "plain", label: "No framework — one ctx.llm call", description: "The simplest agent; add logic to run() as you need it." },
  { value: "strands", label: "Strands Agents", description: "Reasons inside a strands.Agent whose model is ctx.llm." },
  { value: "langgraph", label: "LangGraph", description: "A small graph of its own (draft, check, repair) you can extend." },
];
import { issuesFor, type Issue } from "./validate";

/** An id field that only commits a legal, unused id — on blur or Enter. */
function IdField({ label, value, pattern, taken, hint, onRename }: {
  label: string; value: string; pattern: RegExp; taken: string[]; hint: string;
  onRename: (to: string) => void;
}) {
  const [text, setText] = useState(value);
  useEffect(() => setText(value), [value]);
  const error = text === value ? null
    : !pattern.test(text) ? hint
      : taken.includes(text) ? `"${text}" is already used` : null;
  const commit = () => { if (!error && text !== value) onRename(text); };
  return (
    <FormField label={label} errorText={error} description="Renaming updates every reference to it.">
      <Input value={text} onChange={({ detail }) => setText(detail.value)} onBlur={commit}
        onKeyDown={({ detail }) => { if (detail.key === "Enter") commit(); }} />
    </FormField>
  );
}

/** Remove one whole agentcore group (e.g. "memory") from an agent. */
function clearCore(p: Project, id: string, group: string): Project {
  const a = { ...p.workflow.agents[id] };
  const core = { ...((a.agentcore ?? {}) as Record<string, Json>) };
  delete core[group];
  if (Object.keys(core).length) a.agentcore = core as unknown as Json; else delete a.agentcore;
  return { ...p, workflow: { ...p.workflow, agents: { ...p.workflow.agents, [id]: a } } };
}

/** One AgentCore feature's on/off switch in the agent panel. */
function FeatureToggle({ label, checked, onChange, description }: {
  label: string; checked: boolean; onChange: (on: boolean) => void; description?: string;
}) {
  return (
    <Toggle checked={checked} onChange={({ detail }) => onChange(detail.checked)} description={description}>
      {label}
    </Toggle>
  );
}

/** Set one agentcore key (e.g. "guardrails.use") on an agent, dropping empty groups. */
function setCore(p: Project, id: string, dotted: string, v: Json | undefined): Project {
  const next = structuredClone(p);
  const a = next.workflow.agents[id];
  const core = { ...((a.agentcore ?? {}) as Record<string, Json>) } as Record<string, Record<string, Json>>;
  const [g, k] = dotted.split(".");
  const grp = { ...(core[g] ?? {}) };
  if (v === undefined) delete grp[k]; else grp[k] = v;
  if (Object.keys(grp).length) core[g] = grp; else delete core[g];
  if (Object.keys(core).length) a.agentcore = core as unknown as Json; else delete a.agentcore;
  return next;
}

const keysOf = (wf: Project["workflow"], m: string) => Object.keys((wf[m] ?? {}) as object);

/** A single pick from the build's entries and the library (value "lib:<id>" uses one live). */
function PickOne({ ctx, kind, map, keys, used, value, onPick, onLibrary, none }: {
  ctx: LibraryCtx; kind: LibraryKind; map: string; keys: string[]; used: string[]; value: string;
  onPick: (key: string | undefined) => void; onLibrary: (id: string) => void; none: string;
}) {
  const opts = [{ label: none, value: "" }, ...pickOptions(ctx, kind, keys, used)];
  return (
    <Select selectedOption={opts.find((o) => o.value === value) ?? opts[0]} options={opts} ariaLabel={`Pick a ${kind}`}
      empty={`No ${map} yet`} onChange={({ detail }) => {
        const v = detail.selectedOption.value ?? "";
        if (v.startsWith("lib:")) onLibrary(v.slice(4)); else onPick(v || undefined);
      }} />
  );
}

function AgentPanel({ project, id, issues, onChange, onSelect, server }: {
  project: Project; id: string; issues: Issue[];
  onChange: (p: Project) => void; onSelect: (s: Selection) => void; server?: boolean;
}) {
  const wf = project.workflow;
  const agent = wf.agents[id];
  const mine = issuesFor(issues, { kind: "agent", id });
  const stage = stageOf(wf, id);
  const remote = agent.runtime === "a2a";
  const prompt = project.prompts[id];
  const setWf = (workflow: typeof wf) => onChange({ ...project, workflow });

  const stageOptions = [
    { label: "Not on the canvas", value: "none" },
    ...wf.steps.map((s, i) => ({
      label: `Stage ${i + 1} — ${stepAgents(s).map((a) => wf.agents[a]?.name ?? a).join(", ")}`,
      value: String(i),
    })),
    { label: "A new stage at the end", value: "new" },
  ];
  const tools = Object.entries(wf.tools);
  const bound = toolsOf(agent.tool);
  const kb = bound.map((k) => wf.tools[k]).find((t) => t?.type === "kb");
  const corpora = kb && Array.isArray(kb.corpora) ? kb.corpora.map(String) : [];
  const models = useModels();
  const defaultModel = String((wf.orchestrator as Entry | undefined)?.defaultModel ?? "");
  const ctx = useContext(LibraryContext);
  const used = (m: "tools" | "guardrails" | "memories" | "evaluators" | "identities" | "skills") =>
    keysOf(wf, m).map((k) => ctx.liveId(m, k)).filter(Boolean) as string[];
  const core = (agent.agentcore && typeof agent.agentcore === "object" ? agent.agentcore : {}) as Record<string, Record<string, Json>>;
  // On with nothing picked yet is a real state (the user is about to pick), so kept here.
  const [memoryOn, setMemoryOn] = useState(Boolean(core.memory && Object.keys(core.memory).length));
  const [identityOn, setIdentityOn] = useState(Boolean((core.identity?.outbound as Json[] | undefined)?.length));
  useEffect(() => {
    setMemoryOn(Boolean(core.memory && Object.keys(core.memory).length));
    setIdentityOn(Boolean((core.identity?.outbound as Json[] | undefined)?.length));
  }, [id]); // eslint-disable-line react-hooks/exhaustive-deps
  /** Guardrail input/output on or off; with both off, the guardrail it named goes too. */
  const guard = (p: Project, side: "input" | "output", on: boolean) => {
    const other = side === "input" ? "output" : "input";
    const next = setCore(p, id, `guardrails.${side}`, on || undefined);
    return on || core.guardrails?.[other] ? next : clearCore(next, id, "guardrails");
  };
  const fromLib = (m: "tools" | "guardrails" | "memories" | "evaluators" | "identities" | "skills", libId: string,
    edit: (p: Project, key: string) => Project) => {
    const item = ctx.lib.find((i) => i.id === libId);
    if (item && ctx.withItem) ctx.withItem(m, item, edit);
  };

  return (
    <SpaceBetween size="l">
      <Header
        variant="h2"
        description={remote ? "Remote agent (A2A) — its code is somebody else's" : "Runs in this deployment"}
        actions={
          <SpaceBetween direction="horizontal" size="xs">
            {stage >= 0 ? <Button onClick={() => setWf(unplace(wf, id))}>Remove from canvas</Button> : null}
            <Button onClick={() => { onChange(removeAgent(project, id)); onSelect(null); }}>Delete</Button>
          </SpaceBetween>
        }
      >
        {String(agent.name ?? id)}
      </Header>
      {mine.length ? <IssueList issues={mine} /> : null}
      <IdField
        label="Agent id" value={id} pattern={AGENT_ID_RE} taken={Object.keys(wf.agents)}
        hint="Letters, digits and underscores, starting with a letter — no hyphens (it names an AgentCore Runtime)."
        onRename={(to) => { onChange(renameAgent(project, id, to)); onSelect({ kind: "agent", id: to }); }}
      />
      <FormField label="Stage" description="Where this agent runs. Same as dragging it on the canvas.">
        <Select
          selectedOption={stageOptions.find((o) => o.value === (stage >= 0 ? String(stage) : "none")) ?? null}
          options={stageOptions}
          onChange={({ detail }) => {
            const v = detail.selectedOption.value!;
            if (v === "none") setWf(unplace(wf, id));
            else if (v === "new") setWf(insertStage(wf, wf.steps.length, id));
            else setWf(joinStage(wf, Number(v), id));
          }}
        />
      </FormField>
      {stage >= 0 && stageKind(wf.steps[stage]) !== "single" ? (
        <SpaceBetween direction="horizontal" size="xs">
          <Button iconName="arrow-left" onClick={() => setWf(moveMember(wf, stage, id, -1))}>Earlier in group</Button>
          <Button iconName="arrow-right" onClick={() => setWf(moveMember(wf, stage, id, 1))}>Later in group</Button>
        </SpaceBetween>
      ) : null}

      {!remote ? (
        <ExpandableSection headerText="Prompt" defaultExpanded variant="container"
          headerDescription="Written into app/subagents/<id>/prompts.py by `scaffold.py apply`.">
          <SpaceBetween size="m">
            <FormField label="System prompt" stretch>
              <Textarea rows={10} value={prompt?.systemPrompt ?? ""}
                placeholder="Leave empty to keep the agent's existing prompts.py"
                onChange={({ detail }) => onChange({
                  ...project,
                  prompts: { ...project.prompts, [id]: { ...(prompt ?? defaultPrompt(String(agent.name ?? id))), systemPrompt: detail.value } },
                })} />
            </FormField>
            <FormField label="Output shape" description="The JSON the model must return, described field by field." stretch>
              <Textarea rows={4} value={prompt?.schema ?? ""}
                onChange={({ detail }) => onChange({
                  ...project,
                  prompts: { ...project.prompts, [id]: { ...(prompt ?? defaultPrompt(String(agent.name ?? id))), schema: detail.value } },
                })} />
            </FormField>
          </SpaceBetween>
        </ExpandableSection>
      ) : null}

      <EntryForm
        name="agent" entry={agent} issues={issues} pathPrefix={`agents.${id}`}
        onChange={(e) => onChange(updateEntry(project, "agents", id, e))}
        collapseGroups={["features"]}
        overrides={{
          agentcore: { hidden: true },
          // Where a remote agent came from in an Agent Registry: shown on its own, not edited.
          registry: { hidden: true },
          skills: {
            // The build's skills (and the library's): know-how it opens when a task needs it.
            render: (value, set) => {
              const cur = Array.isArray(value) ? value.map(String) : [];
              const skillDefs = (wf.skills ?? {}) as Record<string, Record<string, Json>>;
              const opts = pickOptions(ctx, "skill", keysOf(wf, "skills"), used("skills"),
                (k) => `${String(skillDefs[k]?.description ?? "")}${ctx.liveId("skills", k) ? " · from the library" : ""}`);
              return (
                <Multiselect selectedOptions={opts.filter((o) => cur.includes(String(o.value)))} options={opts}
                  ariaLabel="Skills" filteringType="auto"
                  placeholder="No skills" empty="No skills yet — add one under Skills, or in the library"
                  onChange={({ detail }) => {
                    const vals = detail.selectedOptions.map((o) => o.value!);
                    const own = vals.filter((v) => !v.startsWith("lib:"));
                    const lib = vals.find((v) => v.startsWith("lib:"));
                    if (lib) {
                      fromLib("skills", lib.slice(4), (p, key) => updateEntry(p, "agents", id,
                        { ...p.workflow.agents[id], skills: [...own, key] }));
                    } else set(own.length ? own : undefined);
                  }} />
              );
            },
          },
          framework: {
            // A workflow.json key (so an uploaded file carries it), shown with what each
            // option means rather than as a bare value.
            render: (value, set) => (
              <Select
                selectedOption={FRAMEWORK_OPTIONS.find((o) => o.value === (value ?? "plain")) ?? FRAMEWORK_OPTIONS[0]}
                options={FRAMEWORK_OPTIONS.filter((o) => vocab("agentFrameworks").includes(o.value))}
                onChange={({ detail }) => set(detail.selectedOption.value === "plain" ? undefined : detail.selectedOption.value)}
              />
            ),
          },
          tool: {
            // Several tools per agent. Each is queried before the agent reasons, in the
            // order shown here, and handed to the model as labelled evidence.
            render: () => (
              <Multiselect
                selectedOptions={bound.map((k) => ({ label: k, value: k, description: String(wf.tools[k]?.type ?? "missing") }))}
                options={pickOptions(ctx, "tool", tools.map(([k]) => k), used("tools"),
                  (k) => `${String(wf.tools[k]?.type ?? "")}${ctx.liveId("tools", k) ? " · from the library" : ""}`)}
                placeholder="No tools — the agent reasons over the request and earlier stages"
                empty="No tools yet — add one under Tools, or in the library"
                filteringType="auto"
                onChange={({ detail }) => {
                  const vals = detail.selectedOptions.map((o) => o.value!);
                  const own = vals.filter((v) => !v.startsWith("lib:"));
                  const lib = vals.find((v) => v.startsWith("lib:"));
                  if (lib) fromLib("tools", lib.slice(4), (p, key) => setTools(p, id, [...own, key]));
                  else onChange(setTools(project, id, own));
                }}
              />
            ),
          },
          model: {
            // The models this account can invoke, listed from Bedrock itself, and
            // still typeable: a model the list does not show (a custom import, a
            // profile in another region) is one the deploy may well accept. Only
            // those that can do what this agent's settings ask: read images
            // (vision) and choose tool calls (toolMode "model").
            render: (value, set) => {
              const needs = needsOf(agent);
              const shown = models.models.filter((m) => fits(m, needs));
              const asks = [needs.vision ? "read images" : "", needs.tools ? "call tools itself" : ""].filter(Boolean);
              return (
                <SpaceBetween size="xxs">
                  <Autosuggest
                    value={typeof value === "string" ? value : ""}
                    onChange={({ detail }) => set(detail.value.trim() ? detail.value.trim() : undefined)}
                    options={shown.map((m) => ({ value: m.id, label: m.id, description: `${m.provider} · ${m.name}`, tags: [capsLabel(m)] }))}
                    enteredTextLabel={(v) => `Use "${v}"`}
                    placeholder={defaultModel ? `Default: ${defaultModel}` : "Choose a model"}
                    statusType={models.loading ? "loading" : models.error ? "error" : "finished"}
                    loadingText="Listing the models this account can use…"
                    errorText={models.error}
                    empty="No models found — type a model id"
                    ariaLabel="Model"
                  />
                  {asks.length && !models.loading ? (
                    <Box variant="small" color="text-body-secondary">
                      {`Showing ${shown.length} of ${models.models.length} models: the ones that can ${asks.join(" and ")}, as this agent does.`}
                    </Box>
                  ) : null}
                </SpaceBetween>
              );
            },
          },
          corpus: {
            render: (value, set) => {
              const opts = [{ label: "(every corpus)", value: "" }, ...corpora.map((c) => ({ label: c, value: c }))];
              return (
                <Select disabled={!corpora.length}
                  selectedOption={opts.find((o) => o.value === (value ?? "")) ?? opts[0]}
                  options={opts} onChange={({ detail }) => set(detail.selectedOption.value || undefined)} />
              );
            },
          },
        }}
      />
      {server && remote && String(agent.auth ?? "") === "bearer" ? (
        <SecretField buildId={project.id} kind="a2aTokens" name={id} label="Bearer token"
          description="Sent to the remote agent on every call. Stored encrypted with this build; never shown again." />
      ) : null}
      {!remote ? (
        <ExpandableSection headerText="AgentCore features" variant="container" defaultExpanded
          headerDescription="Turn a feature on, then pick what it uses. Define guardrails, memories, evaluators and identities in their tabs (or the library).">
          <SpaceBetween size="l">
            <FeatureToggle label="Guardrail: check input" checked={Boolean(core.guardrails?.input)}
              onChange={(on) => onChange(guard(project, "input", on))} />
            <FeatureToggle label="Guardrail: check output" checked={Boolean(core.guardrails?.output)}
              onChange={(on) => onChange(guard(project, "output", on))} />
            {core.guardrails?.input || core.guardrails?.output ? (
              <FormField label="Guardrail" description="The one defined in the Guardrails tab, or the deployment guardrail.">
                <PickOne ctx={ctx} kind="guardrail" map="guardrails" keys={keysOf(wf, "guardrails")} used={used("guardrails")}
                  value={String(core.guardrails?.use ?? "")} none="The deployment guardrail"
                  onPick={(k) => onChange(setCore(project, id, "guardrails.use", k))}
                  onLibrary={(lid) => fromLib("guardrails", lid, (p, key) => setCore(p, id, "guardrails.use", key))} />
              </FormField>
            ) : null}

            <FeatureToggle label="Long-term memory" checked={memoryOn}
              onChange={(on) => { setMemoryOn(on); if (!on) onChange(clearCore(project, id, "memory")); }} />
            {memoryOn && !core.memory?.use && Array.isArray(core.memory?.longTerm) && (core.memory.longTerm as Json[]).length ? (
              // Memory written on the agent itself (an imported workflow.json, an older
              // build): it works, but no Memory-tab entry stands for it, so say what it is.
              <Alert type="info" header="Memory set on this agent">
                {`Strategies: ${(core.memory.longTerm as Json[]).map(String).join(", ")}`}
                {core.memory.scope ? ` · shared per ${String(core.memory.scope)}` : ""}.
                {" "}To manage it in the Memory tab, define a memory there and choose it below.
              </Alert>
            ) : null}
            {memoryOn ? (
              <FormField label="Memory" description="Defined in the Memory tab: strategies, retention and who shares it."
                constraintText={!keysOf(wf, "memories").length && !ctx.lib.some((i) => i.kind === "memory") ? "No memories yet: define one in the Memory tab." : undefined}>
                <PickOne ctx={ctx} kind="memory" map="memories" keys={keysOf(wf, "memories")} used={used("memories")}
                  value={String(core.memory?.use ?? "")} none="Choose a memory"
                  onPick={(k) => onChange(setCore(project, id, "memory.use", k))}
                  onLibrary={(lid) => fromLib("memories", lid, (p, key) => setCore(p, id, "memory.use", key))} />
              </FormField>
            ) : null}

            <FeatureToggle label="Evaluations" checked={Boolean(core.evaluations?.enabled)}
              onChange={(on) => onChange(on ? setCore(project, id, "evaluations.enabled", true) : clearCore(project, id, "evaluations"))} />
            {core.evaluations?.enabled ? (() => {
              const cur = Array.isArray(core.evaluations?.evaluators) ? (core.evaluations.evaluators as Json[]).map(String) : [];
              const shared = keysOf(wf, "evaluators").map((n) => ({ name: n, instructions: "" }));
              const libOpts = pickOptions(ctx, "evaluator", [], used("evaluators"));
              return (
                <SpaceBetween size="xs">
                  <FormField label="Evaluators" description="Built-in ones, and your own from the Evals tab.">
                    <EvaluatorPicker value={cur} custom={[...customEvaluatorsOf(agent), ...shared]}
                      onChange={(v) => onChange(setCore(project, id, "evaluations.evaluators", v?.length ? v as Json : undefined))} />
                  </FormField>
                  {libOpts.length ? (
                    <Select selectedOption={null} placeholder="Add one from the library" options={libOpts} ariaLabel="Add an evaluator from the library"
                      onChange={({ detail }) => fromLib("evaluators", String(detail.selectedOption.value).slice(4),
                        (p, key) => setCore(p, id, "evaluations.evaluators", [...cur, `Custom.${key}`]))} />
                  ) : null}
                  <Toggle checked={Boolean(core.evaluations?.auto)}
                    onChange={({ detail }) => onChange(setCore(project, id, "evaluations.auto", detail.checked || undefined))}>
                    Score every run automatically (otherwise on demand)
                  </Toggle>
                </SpaceBetween>
              );
            })() : null}

            <FeatureToggle label="Outbound identity (calls an API directly)" checked={identityOn}
              onChange={(on) => { setIdentityOn(on); if (!on) onChange(clearCore(project, id, "identity")); }} />
            {identityOn ? (() => {
              const cur = Array.isArray(core.identity?.outbound) ? (core.identity.outbound as Json[]).map(String) : [];
              const keys = [...new Set([...keysOf(wf, "identities"), ...cur])];
              const opts = pickOptions(ctx, "identity", keys, used("identities"),
                (k) => (keysOf(wf, "identities").includes(k) ? String((wf.identities as Record<string, Entry>)[k]?.type ?? "") : "An existing provider"));
              return (
                <FormField label="Identities" description="Defined in the Identity tab.">
                  <Multiselect selectedOptions={opts.filter((o) => cur.includes(o.value))} options={opts} ariaLabel="Identities"
                    placeholder="Choose identities" empty="No identities yet: define one in the Identity tab"
                    onChange={({ detail }) => {
                      const vals = detail.selectedOptions.map((o) => o.value!);
                      const own = vals.filter((v) => !v.startsWith("lib:"));
                      const lib = vals.find((v) => v.startsWith("lib:"));
                      if (lib) fromLib("identities", lib.slice(4), (p, key) => setCore(p, id, "identity.outbound", [...own, key]));
                      else onChange(setCore(project, id, "identity.outbound", own.length ? own : undefined));
                    }} />
                </FormField>
              );
            })() : null}

            <FeatureToggle label="Cedar policy on its tool calls" checked={Boolean(core.policy?.enabled)}
              description="Policies themselves attach to tools, in the Policies tab."
              onChange={(on) => onChange(on ? setCore(project, id, "policy.enabled", true) : clearCore(project, id, "policy"))} />
          </SpaceBetween>
        </ExpandableSection>
      ) : null}
    </SpaceBetween>
  );
}

function StagePanel({ project, index, issues, onChange }: {
  project: Project; index: number; issues: Issue[]; onChange: (p: Project) => void;
}) {
  const wf = project.workflow;
  const step = wf.steps[index];
  const kind = stageKind(step);
  const members = stepAgents(step);
  const mine = issuesFor(issues, { kind: "step", index });
  const set = (patch: Partial<StepSpec>) => onChange({ ...project, workflow: updateStage(wf, index, patch) });
  const [branchText, setBranchText] = useState(step.branch ? JSON.stringify(step.branch, null, 2) : "");
  const [branchErr, setBranchErr] = useState<string | null>(null);
  useEffect(() => {
    setBranchText(step.branch ? JSON.stringify(step.branch, null, 2) : "");
    setBranchErr(null);
  }, [step.branch]);
  const later = wf.steps.slice(index + 1).map((s, i) => stepName(s, index + 1 + i));

  return (
    <SpaceBetween size="l">
      <Header variant="h2" description={`Step ${index + 1} of ${wf.steps.length} · ${members.length} agent(s)`}
        actions={
          <SpaceBetween direction="horizontal" size="xs">
            <Button iconName="angle-up" ariaLabel="Move stage up" disabled={index === 0}
              onClick={() => onChange({ ...project, workflow: moveStage(wf, index, -1) })} />
            <Button iconName="angle-down" ariaLabel="Move stage down" disabled={index === wf.steps.length - 1}
              onClick={() => onChange({ ...project, workflow: moveStage(wf, index, 1) })} />
          </SpaceBetween>
        }
      >
        Stage {index + 1}
      </Header>
      {mine.length ? <IssueList issues={mine} /> : null}
      <FormField label="Shape" description="Parallel agents run side by side; a sequence runs left to right, each seeing the ones before it.">
        <SegmentedControl
          selectedId={kind}
          options={[
            { id: "single", text: "Single", disabled: members.length !== 1 },
            { id: "parallel", text: "Parallel", disabled: members.length < 2 },
            { id: "sequence", text: "Sequence", disabled: members.length < 2 },
          ]}
          onChange={({ detail }) => onChange({ ...project, workflow: setStageKind(wf, index, detail.selectedId as never) })}
        />
      </FormField>
      <FormField label="Review gate" description="Pause after this stage for a human to approve, revise or deny.">
        <Toggle checked={Boolean(step.hitl)} onChange={({ detail }) => set({ hitl: detail.checked || undefined })}>
          {step.hitl ? "A reviewer signs off here" : "No review"}
        </Toggle>
      </FormField>
      {step.hitl ? <GateSettings hitl={step.hitl} onChange={(h) => set({ hitl: h })} /> : null}
      {kind !== "single" ? (
        <>
          <FormField label="Gate id" description="The stage's name for branch targets. Letters, digits, underscores.">
            <Input value={step.gateId ?? ""} onChange={({ detail }) => set({ gateId: detail.value || undefined })} />
          </FormField>
          <FormField label="Gate name" description="What reviewers see on the gate.">
            <Input value={step.gateName ?? ""} onChange={({ detail }) => set({ gateName: detail.value || undefined })} />
          </FormField>
        </>
      ) : null}
      <FormField
        label="Branch"
        description={`Route on this stage's output. Targets: END${later.length ? `, ${later.join(", ")}` : ""}. `
          + 'Example: {"when": [{"field": "risk", "equals": "low", "goto": "END"}]}'}
        errorText={branchErr} stretch
      >
        <Textarea rows={5} value={branchText} placeholder="(no branch — always continue to the next stage)"
          onChange={({ detail }) => setBranchText(detail.value)}
          onBlur={() => {
            if (!branchText.trim()) { set({ branch: undefined }); setBranchErr(null); return; }
            try {
              set({ branch: JSON.parse(branchText) });
              setBranchErr(null);
            } catch (e) {
              setBranchErr(`Not valid JSON: ${(e as Error).message}`);
            }
          }} />
      </FormField>
    </SpaceBetween>
  );
}

/** Read an OpenAPI document from a local .json file into the tool's inline `schema`.
 *  YAML is not parsed here (no dependency for it): convert it to JSON first. */
export function parseOpenApi(text: string): Record<string, unknown> {
  let doc: unknown;
  try { doc = JSON.parse(text); } catch { throw new Error("That file is not JSON. Convert a YAML document to JSON first."); }
  const d = doc as Record<string, unknown>;
  if (!d || typeof d !== "object" || !String(d.openapi ?? "").startsWith("3") || typeof d.paths !== "object") {
    throw new Error("That is not an OpenAPI 3 document (it needs \"openapi\": \"3.x\" and \"paths\").");
  }
  return d;
}

export const OPENAPI_MAX_BYTES = 1024 * 1024;

function OpenApiUpload({ onSchema }: { onSchema: (schema: Record<string, unknown>) => void }) {
  const [error, setError] = useState<string | null>(null);
  const [done, setDone] = useState<string | null>(null);
  return (
    <FormField label="Upload an OpenAPI document"
      description="A local openapi.json (OpenAPI 3). It becomes this tool's inline `schema`, replacing schemaS3Uri or source."
      errorText={error ?? undefined} constraintText={done ?? undefined}>
      <input type="file" accept=".json,application/json" aria-label="OpenAPI document"
        onChange={async (e) => {
          const file = e.target.files?.[0];
          e.target.value = "";
          if (!file) return;
          setError(null); setDone(null);
          if (file.size > OPENAPI_MAX_BYTES) { setError("At most 1 MB: the Gateway takes the document inline."); return; }
          try {
            onSchema(parseOpenApi(await file.text()));
            setDone(`Loaded ${file.name}.`);
          } catch (err) {
            setError((err as Error).message);
          }
        }} />
    </FormField>
  );
}

/** The custom evaluators an agent defines (agentcore.evaluations.custom). */
function customEvaluatorsOf(agent: Entry): CustomEvaluator[] {
  const core = agent.agentcore as Record<string, unknown> | undefined;
  const ev = (core && typeof core === "object" ? core.evaluations : undefined) as Record<string, unknown> | undefined;
  return Array.isArray(ev?.custom) ? (ev!.custom as unknown as CustomEvaluator[]) : [];
}

function ToolPanel({ project, id, issues, onChange, onSelect, server, callbackUrls, agentName }: {
  project: Project; id: string; issues: Issue[];
  onChange: (p: Project) => void; onSelect: (s: Selection) => void; server?: boolean;
  callbackUrls?: Record<string, string>; agentName?: string;
}) {
  const wf = project.workflow;
  const tool = wf.tools[id];
  const users = Object.entries(wf.agents).filter(([, a]) => toolsOf(a.tool).includes(id)).map(([k]) => k);
  const mine = issuesFor(issues, { kind: "tool", id });
  const ctx = useContext(LibraryContext);
  const live = ctx.liveId("tools", id);
  const usedIds = (m: "policies" | "identities") => keysOf(wf, m).map((k) => ctx.liveId(m, k)).filter(Boolean) as string[];
  const setToolKey = (p: Project, k: string, v: Json | undefined) => {
    const next = structuredClone(p);
    const t = { ...next.workflow.tools[id] };
    if (v === undefined || (Array.isArray(v) && !v.length)) delete t[k]; else t[k] = v;
    next.workflow.tools[id] = t;
    return next;
  };
  const fromLib = (m: "policies" | "identities", libId: string, edit: (p: Project, key: string) => Project) => {
    const item = ctx.lib.find((i) => i.id === libId);
    if (item && ctx.withItem) ctx.withItem(m, item, edit);
  };
  return (
    <SpaceBetween size="l">
      <Header variant="h2" description={`${String(tool.type)} tool · used by ${users.join(", ") || "no agent yet"}`}
        actions={<Button onClick={() => { onChange(removeTool(project, id)); onSelect(null); }}>Delete</Button>}>
        {String(tool.description ?? id)}
      </Header>
      {mine.length ? <IssueList issues={mine} /> : null}
      {live ? (
        <Alert type="info" header="From the library">
          Used live: this shows the tool as it is in the library now. Changing it here makes it this build&apos;s own copy;
          to change it for every build that uses it, edit it under Tools in the navigation.
        </Alert>
      ) : null}
      <Container header={<Header variant="h3">How it connects</Header>}>
        <ToolConnect project={project} id={id} onChange={onChange} server={Boolean(server)} callbackUrl={callbackUrls?.[id]} agentName={agentName} />
      </Container>
      <IdField
        label="Tool key" value={id} pattern={TOOL_KEY_RE} taken={Object.keys(wf.tools)}
        hint="Letters and digits, starting with a letter — it names the Gateway target and the Cedar policy. Use camelCase."
        onRename={(to) => { onChange(renameTool(project, id, to)); onSelect({ kind: "tool", id: to }); }}
      />
      <EntryForm name="tool" entry={tool} issues={issues} pathPrefix={`tools.${id}`}
        // A code tool's grants and files have their own panel below (CodeTool).
        // How it connects (auth, service, oauth, identity) has its own panel above (ToolConnect).
        overrides={{ code: { hidden: true }, auth: { hidden: true }, service: { hidden: true }, oauth: { hidden: true },
          registry: { hidden: true },
          identity: { hidden: true },
          policies: {
            render: (value) => {
              const cur = Array.isArray(value) ? value.map(String) : [];
              const opts = pickOptions(ctx, "policy", keysOf(wf, "policies"), usedIds("policies"));
              return (
                <Multiselect selectedOptions={opts.filter((o) => cur.includes(o.value))} options={opts} ariaLabel="Policies"
                  placeholder="None — only the generated permit" empty="No policies yet — write one on the Policies tab"
                  onChange={({ detail }) => {
                    const vals = detail.selectedOptions.map((o) => o.value!);
                    const own = vals.filter((v) => !v.startsWith("lib:"));
                    const lib = vals.find((v) => v.startsWith("lib:"));
                    if (lib) fromLib("policies", lib.slice(4), (p, key) => setToolKey(p, "policies", [...own, key]));
                    else onChange(setToolKey(project, "policies", own));
                  }} />
              );
            },
          },
          // Written here: there is no ARN or shipped source to name.
          ...(tool.code !== undefined ? { lambdaArn: { hidden: true }, source: { hidden: true } } : {}) }}
        onChange={(e) => onChange(updateEntry(project, "tools", id, e))} />
      {String(tool.type) === "lambda" ? <CodeTool project={project} id={id} onChange={onChange} server={server} /> : null}
      {String(tool.type) === "openapi" ? (
        <OpenApiUpload onSchema={(schema) => {
          const { schemaS3Uri: _u, source: _s, ...rest } = tool as Record<string, unknown>;
          onChange(updateEntry(project, "tools", id, { ...rest, schema } as typeof tool));
        }} />
      ) : null}
      {server && String(tool.type) === "kb" ? (
        <ExpandableSection headerText="Documents" variant="container" defaultExpanded
          headerDescription={tool.knowledgeBaseId ? "From your Knowledge Base." : tool.s3Uri ? "From your S3 bucket." : "What each corpus retrieves from."}>
          {tool.knowledgeBaseId ? (
            <Box color="text-body-secondary">
              Retrieves from your Knowledge Base <b>{String(tool.knowledgeBaseId)}</b>, in the account and region this
              build deploys to. Nothing is uploaded, and destroying the build leaves it as it is. Its retrieval function is
              granted bedrock:Retrieve on that one Knowledge Base only.
            </Box>
          ) : tool.s3Uri ? (
            <Box color="text-body-secondary">
              Built from the documents in <b>{String(tool.s3Uri)}</b>, re-synced on every deploy. The Knowledge Base role
              can read only there{tool.kmsKeyArn ? ", and decrypt with the key you named" : ""}. A bucket in another account
              also needs a bucket policy allowing role AgentCoreKB-&lt;build&gt;. Corpora match the <code>{String(tool.corpusKey ?? "doc_type")}</code> attribute
              in your documents&apos; .metadata.json files.
            </Box>
          ) : (
            <KbDocuments buildId={project.id}
              corpora={(Array.isArray(tool.corpora) ? tool.corpora : []).map(String)} />
          )}
        </ExpandableSection>
      ) : null}
    </SpaceBetween>
  );
}

export function Inspector({ project, selection, issues, onChange, onSelect, server, callbackUrls, agentName }: {
  project: Project; selection: Selection; issues: Issue[];
  onChange: (p: Project) => void; onSelect: (s: Selection) => void;
  /** With the console's builds store: secrets and documents can be stored for the build. */
  server?: boolean;
  /** From the last deploy: tool -> the callback URL to register at its provider. */
  callbackUrls?: Record<string, string>;
  /** The build's fixed AWS name (the Gateway role is named from it). */
  agentName?: string;
}) {
  const wf = project.workflow;
  let body;
  if (selection?.kind === "agent" && wf.agents[selection.id]) {
    body = <AgentPanel project={project} id={selection.id} issues={issues} onChange={onChange} onSelect={onSelect} server={server} />;
  } else if (selection?.kind === "stage" && wf.steps[selection.index]) {
    body = <StagePanel project={project} index={selection.index} issues={issues} onChange={onChange} />;
  } else if (selection?.kind === "tool" && wf.tools[selection.id]) {
    body = <ToolPanel project={project} id={selection.id} issues={issues} onChange={onChange} onSelect={onSelect} server={server} callbackUrls={callbackUrls} agentName={agentName} />;
  } else {
    const errors = issues.filter((i) => i.severity === "error").length;
    body = (
      <SpaceBetween size="m">
        <Header variant="h2" description="Select an agent, a stage or a tool to configure it.">Problems</Header>
        {issues.length
          ? <IssueList issues={issues} onPick={(w) => {
            if (w.kind === "agent") onSelect({ kind: "agent", id: w.id });
            if (w.kind === "tool") onSelect({ kind: "tool", id: w.id });
            if (w.kind === "step") onSelect({ kind: "stage", index: w.index });
          }} />
          : <Alert type="success">No problems. This workflow is ready to export.</Alert>}
        {errors ? <Box variant="small" color="text-body-secondary">Export is available once the errors are fixed. Warnings do not block it.</Box> : null}
      </SpaceBetween>
    );
  }
  return <Container>{body}</Container>;
}


/** How a stage's review gate decides (steps[].hitl, app/common/gates.py). All defaults
 *  write `true`, so a plain gate stays a plain gate. */
interface Gate {
  mode?: "always" | "threshold" | "auto";
  when?: Json[];
  approval?: "console" | "event";
  timeout?: { after: string; action: "approve" | "deny" };
}

function GateSettings({ hitl, onChange }: { hitl: boolean | GateSpec; onChange: (h: boolean | GateSpec) => void }) {
  const h = (typeof hitl === "object" ? hitl : {}) as Gate;
  const [rules, setRules] = useState(h.when ? JSON.stringify(h.when, null, 1) : "");
  const [rulesErr, setRulesErr] = useState<string | null>(null);
  const write = (patch: Partial<Gate>) => {
    const next: Record<string, unknown> = { ...h, ...patch };
    for (const k of Object.keys(next)) if (next[k] === undefined) delete next[k];
    if (next.mode === "always") delete next.mode;
    if (next.approval === "console") delete next.approval;
    onChange(Object.keys(next).length ? (next as GateSpec) : true);
  };
  const opt = (v: string, label: string) => ({ value: v, label });
  const modes = [opt("always", "Always: a person decides"), opt("threshold", "Only when a rule matches"),
    opt("auto", "Approves itself (logged)")];
  const approvals = [opt("console", "In the app"), opt("event", "In the app, or by EventBridge event")];
  const mode = h.mode ?? "always";
  return (
    <ExpandableSection headerText="How the gate decides" variant="footer" defaultExpanded={typeof hitl === "object"}>
      <SpaceBetween size="s">
        <FormField label="Who decides">
          <Select selectedOption={modes.find((m) => m.value === mode)!} options={modes}
            onChange={({ detail }) => write({ mode: detail.selectedOption.value as Gate["mode"] })} />
        </FormField>
        {mode === "threshold" ? (
          <FormField label="Rules that call for a person" stretch errorText={rulesErr}
            description={'Branch rules without goto, against this stage\'s output: [{"field": "riskScore", "gte": 80}, {"contains": "URGENT"}]. No match: it approves itself.'}>
            <Textarea rows={3} value={rules} onChange={({ detail }) => setRules(detail.value)}
              onBlur={() => {
                if (!rules.trim()) { write({ when: undefined }); setRulesErr(null); return; }
                try { write({ when: JSON.parse(rules) }); setRulesErr(null); }
                catch (e) { setRulesErr(`Not valid JSON: ${(e as Error).message}`); }
              }} />
          </FormField>
        ) : null}
        {mode !== "auto" ? (
          <>
            <FormField label="Where it is answered" description="By event: an &quot;AgentExpress Approval Requested&quot; event is put on the account's default bus, and an &quot;AgentExpress Approval Decision&quot; event answers it (Slack, ServiceNow, your code).">
              <Select selectedOption={approvals.find((a) => a.value === (h.approval ?? "console"))!} options={approvals}
                onChange={({ detail }) => write({ approval: detail.selectedOption.value as Gate["approval"] })} />
            </FormField>
            <FormField label="Timeout" description="With no decision after this long (30m, 24h, 2d), decide it this way. Empty: wait.">
              <SpaceBetween direction="horizontal" size="xs">
                <Input value={h.timeout?.after ?? ""} placeholder="24h" ariaLabel="Timeout after"
                  onChange={({ detail }) => write({ timeout: detail.value.trim()
                    ? { after: detail.value.trim(), action: h.timeout?.action ?? "deny" } : undefined })} />
                {h.timeout ? (
                  <Select selectedOption={opt(h.timeout.action, h.timeout.action === "deny" ? "Deny" : "Approve")}
                    options={[opt("deny", "Deny"), opt("approve", "Approve")]} ariaLabel="Timeout action"
                    onChange={({ detail }) => write({ timeout: { after: h.timeout!.after,
                      action: detail.selectedOption.value as "approve" | "deny" } })} />
                ) : null}
              </SpaceBetween>
            </FormField>
          </>
        ) : null}
      </SpaceBetween>
    </ExpandableSection>
  );
}
