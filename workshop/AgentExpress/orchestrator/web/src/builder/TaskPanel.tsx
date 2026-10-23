/** The inspector for a task step (runtime "task", app/common/task_agent.py): a step with no
 *  model that calls a tool, runs a script, reads a document or writes a file. Its settings
 *  are the `task` block, so it has its own form rather than the agent's (no prompt, no
 *  model, nothing to attach). Checked as you type by taskCheck.ts. */
import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import Input from "@cloudscape-design/components/input";
import Multiselect from "@cloudscape-design/components/multiselect";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Textarea from "@cloudscape-design/components/textarea";
import Toggle from "@cloudscape-design/components/toggle";
import { useEffect, useState } from "react";
import type { Selection } from "./Canvas";
import { IssueList } from "./IssueList";
import { AGENT_ID_RE, vocab } from "./meta";
import {
  TASK_KINDS, insertStage, joinStage, newTaskEntry, removeAgent, renameAgent, stageOf, stepAgents,
  taskLabel, unplace, updateEntry, type Entry, type Json, type Project, type TaskKind,
} from "./model";
import { issuesFor, upstreamIds, type Issue } from "./validate";
import type { CodeSource } from "./codeSource";
import { CodeSourceField } from "./CodeSourceField";
import { useDocsBase } from "../docs/docsLinks";
import { infoSlot } from "../docs/InfoLink";

const opt = (v: string, label = v) => ({ value: v, label });

/** The task's `arguments`, edited as JSON text: kept as typed until it parses. */
function ArgumentsField({ value, onChange, errorText }: {
  value: Json | undefined; onChange: (v: Entry) => void; errorText?: string;
}) {
  const shown = JSON.stringify(value ?? {}, null, 2);
  const [text, setText] = useState(shown);
  const [bad, setBad] = useState(false);
  useEffect(() => { setText(shown); setBad(false); }, [shown]);
  return (
    <FormField label="Arguments" stretch errorText={bad ? "Not valid JSON: it is kept as you typed it until it is." : errorText}
      description="A JSON object of what to send. A text value may hold {{request}}, {{session}} or {{outputs.<step>.<field>}}.">
      <Textarea rows={6} value={text} ariaLabel="Arguments (JSON)"
        onChange={({ detail }) => {
          setText(detail.value);
          try {
            const v = JSON.parse(detail.value || "{}");
            if (v && typeof v === "object" && !Array.isArray(v)) { setBad(false); onChange(v as Entry); } else setBad(true);
          } catch { setBad(true); }
        }} />
    </FormField>
  );
}

export function TaskPanel({ project, id, issues, onChange, onSelect, server }: {
  project: Project; id: string; issues: Issue[];
  onChange: (p: Project) => void; onSelect: (s: Selection) => void; server?: boolean;
}) {
  const docs = useDocsBase();
  const wf = project.workflow;
  const agent = wf.agents[id];
  const task = (agent.task && typeof agent.task === "object" && !Array.isArray(agent.task) ? agent.task : {}) as Entry;
  const kind = String(task.kind ?? "") as TaskKind;
  const mine = issuesFor(issues, { kind: "agent", id });
  const stage = stageOf(wf, id);
  const before = upstreamIds(wf.steps, id);
  const [newId, setNewId] = useState(id);
  useEffect(() => setNewId(id), [id]);
  const errorAt = (path: string) => mine.filter((i) => i.path === `agents.${id}.${path}` || i.path.startsWith(`agents.${id}.${path}[`))
    .map((i) => i.message).join(" ") || undefined;
  const setAgent = (next: Entry) => onChange(updateEntry(project, "agents", id, next));
  const setTask = (patch: Record<string, Json | undefined>) => {
    const t: Entry = { ...task };
    for (const [k, v] of Object.entries(patch)) { if (v === undefined) delete t[k]; else t[k] = v; }
    setAgent({ ...agent, task: t });
  };
  const setWf = (workflow: typeof wf) => onChange({ ...project, workflow });
  const idError = newId === id ? undefined : !AGENT_ID_RE.test(newId)
    ? "Letters, digits and underscores, starting with a letter." : newId in wf.agents ? `"${newId}" is already used` : undefined;
  const commitId = () => {
    if (!idError && newId !== id) { onChange(renameAgent(project, id, newId)); onSelect({ kind: "agent", id: newId }); }
  };
  const stageOptions = [
    opt("none", "Not on the canvas"),
    ...wf.steps.map((s, i) => opt(String(i), `Stage ${i + 1} — ${stepAgents(s).map((a) => wf.agents[a]?.name ?? a).join(", ")}`)),
    opt("new", "A new stage at the end"),
  ];
  const kinds = TASK_KINDS.map((k) => opt(k.kind, `${k.name} — ${k.description}`));
  const toolOptions = Object.keys(wf.tools).map((k) => opt(k));
  const files = (wf.orchestrator as Entry | undefined)?.files;
  const help = before.length
    ? `Earlier steps: ${before.join(", ")}.`
    : "No step runs before this one yet, so only {{request}} and {{session}} have a value.";

  return (
    <SpaceBetween size="l">
      <Header variant="h2" description={`${taskLabel(kind)} — a step with no model, run in this deployment`}
        info={infoSlot(docs, "task")}
        actions={
          <SpaceBetween direction="horizontal" size="xs">
            {stage >= 0 ? <Button onClick={() => setWf(unplace(wf, id))}>Remove from canvas</Button> : null}
            <Button onClick={() => { onChange(removeAgent(project, id)); onSelect(null); }}>Delete</Button>
          </SpaceBetween>
        }>
        {String(agent.name ?? id)}
      </Header>
      {mine.length ? <IssueList issues={mine} /> : null}
      <FormField label="Name">
        <Input value={String(agent.name ?? "")} onChange={({ detail }) => setAgent({ ...agent, name: detail.value })} />
      </FormField>
      <FormField label="Step id" errorText={idError} description="Later steps read this step's output by it. Renaming updates the stages.">
        <Input value={newId} onChange={({ detail }) => setNewId(detail.value)} onBlur={commitId}
          onKeyDown={({ detail }) => { if (detail.key === "Enter") commitId(); }} />
      </FormField>
      <FormField label="Stage" description="Where this step runs. Same as dragging it on the canvas.">
        <Select selectedOption={stageOptions.find((o) => o.value === (stage >= 0 ? String(stage) : "none")) ?? null}
          options={stageOptions}
          onChange={({ detail }) => {
            const v = detail.selectedOption.value!;
            if (v === "none") setWf(unplace(wf, id));
            else if (v === "new") setWf(insertStage(wf, wf.steps.length, id));
            else setWf(joinStage(wf, Number(v), id));
          }} />
      </FormField>
      <FormField label="What it does" errorText={errorAt("task.kind")}>
        <Select selectedOption={kinds.find((o) => o.value === kind) ?? null} options={kinds} placeholder="Pick what this step does"
          onChange={({ detail }) => {
            const fresh = newTaskEntry(detail.selectedOption.value as TaskKind, wf);
            const next: Entry = { ...agent, task: fresh.task };
            if (fresh.attachments) next.attachments = true;
            setAgent(next);
          }} />
      </FormField>

      {kind === "tool" ? (
        <SpaceBetween size="m">
          <FormField label="Tool" errorText={errorAt("task.tool")} description="The tool this step calls through the Gateway. Its Cedar policy and approval still apply.">
            <Select selectedOption={toolOptions.find((o) => o.value === task.tool) ?? null} options={toolOptions}
              placeholder="Pick a tool" empty="No tools yet — add one under Tools"
              onChange={({ detail }) => setTask({ tool: detail.selectedOption.value })} />
          </FormField>
          <ArgumentsField value={task.arguments} errorText={errorAt("task.arguments")}
            onChange={(v) => setTask({ arguments: v })} />
          <Box variant="small" color="text-body-secondary">{help}</Box>
        </SpaceBetween>
      ) : null}

      {kind === "code" ? (
        <SpaceBetween size="m">
          <FormField label="Language" errorText={errorAt("task.language")}>
            <Select selectedOption={opt(String(task.language ?? "python"))} options={vocab("taskLanguages").map((l) => opt(l))}
              onChange={({ detail }) => setTask({ language: detail.selectedOption.value })} />
          </FormField>
          <CodeSourceField mode="script" language={String(task.language ?? "python")} path={`agents.${id}.task.codeSource`}
            source={task.codeSource as unknown as CodeSource | undefined} buildId={server ? project.id : undefined}
            // A fetched script replaces the written one: a step has one or the other.
            onSource={(s) => setTask(s ? { codeSource: s as unknown as Json, code: undefined }
              : { codeSource: undefined, code: String(task.code ?? "") })}
            onScript={(c) => setTask({ code: c, codeSource: undefined })} />
          {task.codeSource === undefined ? (
            <FormField label="Script" stretch errorText={errorAt("task.code")}
              description="Runs in the AgentCore Code Interpreter sandbox. Read in/request.txt, in/outputs.json and the run's files in in/; print the output (JSON is read as data); write files to keep to out/.">
              <Textarea rows={14} value={String(task.code ?? "")} ariaLabel="Script" spellcheck={false}
                onChange={({ detail }) => setTask({ code: detail.value })} />
            </FormField>
          ) : null}
          <Toggle checked={agent.attachments === true}
            onChange={({ detail }) => { const next = { ...agent }; if (detail.checked) next.attachments = true; else delete next.attachments; setAgent(next); }}>
            Give the script the files the run was started with
          </Toggle>
        </SpaceBetween>
      ) : null}

      {kind === "extract" ? (
        <SpaceBetween size="m">
          <FormField label="File" description="The name of one of the run's files. Leave empty to read every PDF, PNG and JPEG the run brought.">
            <Input value={String(task.file ?? "")} placeholder="Every PDF and image"
              onChange={({ detail }) => setTask({ file: detail.value || undefined })} />
          </FormField>
          <FormField label="Also read" errorText={errorAt("task.features")} description="Text is always read.">
            <Multiselect
              selectedOptions={(Array.isArray(task.features) ? task.features.map(String) : vocab("taskExtractFeatures")).map((f) => opt(f, f === "TABLES" ? "Tables" : "Form fields"))}
              options={vocab("taskExtractFeatures").map((f) => opt(f, f === "TABLES" ? "Tables" : "Form fields"))}
              ariaLabel="Also read"
              onChange={({ detail }) => {
                const picked = detail.selectedOptions.map((o) => o.value!);
                setTask({ features: picked.length === vocab("taskExtractFeatures").length ? undefined : picked });
              }} />
          </FormField>
          {agent.attachments !== true ? (
            <Alert type="warning" action={<Button onClick={() => setAgent({ ...agent, attachments: true })}>Turn on</Button>}>
              This step reads the files a run is started with, so it needs them turned on.
            </Alert>
          ) : null}
        </SpaceBetween>
      ) : null}

      {kind === "output" ? (
        <SpaceBetween size="m">
          <FormField label="Format" errorText={errorAt("task.format")}>
            <Select selectedOption={task.format ? opt(String(task.format)) : null} options={vocab("taskOutputFormats").map((f) => opt(f))}
              placeholder="Pick a format" onChange={({ detail }) => setTask({ format: detail.selectedOption.value })} />
          </FormField>
          <FormField label="File name" errorText={errorAt("task.name")} description={`Default ${id}.<format>. It may hold {{session}} or {{outputs.<step>.<field>}}.`}>
            <Input value={String(task.name ?? "")} placeholder={`${id}.${task.format === "markdown" ? "md" : String(task.format ?? "csv")}`}
              onChange={({ detail }) => setTask({ name: detail.value || undefined })} />
          </FormField>
          <FormField label="From" errorText={errorAt("task.from")} description={`Which earlier outputs to write, separated by commas: a step id, or <step>.<field> for part of one. Empty writes them all. ${help}`}>
            <Input value={Array.isArray(task.from) ? task.from.map(String).join(", ") : ""} placeholder="Every earlier step"
              onChange={({ detail }) => {
                const refs = detail.value.split(",").map((s) => s.trim()).filter(Boolean);
                setTask({ from: refs.length ? refs : undefined });
              }} />
          </FormField>
          {!(files && typeof files === "object") ? (
            <Alert type="warning" action={<Button onClick={() => setWf({ ...wf, orchestrator: { ...((wf.orchestrator ?? {}) as Entry), files: {} } })}>Keep run files</Button>}>
              The file is kept with the run only when the build keeps run files (orchestrator.files).
            </Alert>
          ) : null}
        </SpaceBetween>
      ) : null}

      <FormField label="Produces" description="The name of this step's output, e.g. &quot;price-table&quot;. Optional.">
        <Input value={String(agent.produces ?? "")}
          onChange={({ detail }) => { const next = { ...agent }; if (detail.value) next.produces = detail.value; else delete next.produces; setAgent(next); }} />
      </FormField>
    </SpaceBetween>
  );
}
