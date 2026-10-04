/** Cedar policies: a build's own (workflow.json orchestrator.policy.custom), the permit
 *  generated per tool, the engine's mode, and the user's policy library.
 *
 *  Three ways to write one, all ending as one Cedar statement the user sees before it is
 *  kept: plain English (AgentCore Policy writes it against the deployed build's own
 *  engine and Gateway, via bff/policies.py generate), a guided form (cedar.ts
 *  fromGuided), or raw Cedar. Every
 *  statement is checked as it is typed (cedar.ts problems), and again by the server.
 *
 *  Attaching a library policy COPIES it into the build (with its libraryId): a build is
 *  self-contained, and editing the library later changes no build. */
import Alert from "@cloudscape-design/components/alert";
import Autosuggest from "@cloudscape-design/components/autosuggest";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Checkbox from "@cloudscape-design/components/checkbox";
import ColumnLayout from "@cloudscape-design/components/column-layout";
import Container from "@cloudscape-design/components/container";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import Input from "@cloudscape-design/components/input";
import Modal from "@cloudscape-design/components/modal";
import SegmentedControl from "@cloudscape-design/components/segmented-control";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import StatusIndicator from "@cloudscape-design/components/status-indicator";
import Table from "@cloudscape-design/components/table";
import Tabs from "@cloudscape-design/components/tabs";
import Textarea from "@cloudscape-design/components/textarea";
import Toggle from "@cloudscape-design/components/toggle";
import { useCallback, useEffect, useMemo, useState } from "react";

import { fromGuided, NAME_RE, ARG_RE, problems, summary, type Guided, type Operator } from "./cedar";
import type { Entry, Json, Project, Workflow } from "./model";
import { policyLibrary, type GeneratedPolicy, type LibraryPolicy, type PolicySource } from "./storage";
import type { Issue } from "./validate";
import "./builder.css";

type Notify = (type: "success" | "error" | "info", text: string) => void;

export interface CustomPolicy { name: string; description?: string; statement: string; libraryId?: string }
interface PolicyBlock { enabled?: boolean; mode?: string; custom?: CustomPolicy[] }

export function policyBlock(wf: Workflow): PolicyBlock {
  const orch = (wf.orchestrator ?? {}) as Record<string, unknown>;
  const p = orch.policy;
  return p && typeof p === "object" && !Array.isArray(p) ? p as PolicyBlock : {};
}

/** The workflow with orchestrator.policy changed; an emptied `custom` is dropped. */
export function withPolicy(wf: Workflow, patch: Partial<PolicyBlock>): Workflow {
  const orch = (wf.orchestrator ?? {}) as Record<string, Json>;
  const next: PolicyBlock = { ...policyBlock(wf), ...patch };
  if (!next.custom?.length) delete next.custom;
  return { ...wf, orchestrator: { ...orch, policy: next as unknown as Json } };
}

/** The tools a target publishes and their arguments, when its config says (mirrors
 *  bff/policies.py catalog). An MCP server's tools are only known once it is running. */
export function knownTools(tool: Entry | undefined): { name: string; args: string[] }[] {
  if (!tool) return [];
  const kind = String(tool.type ?? "").toLowerCase();
  const obj = (v: unknown) => (v && typeof v === "object" && !Array.isArray(v) ? v as Record<string, unknown> : {});
  const out: { name: string; args: string[] }[] = [];
  if (kind === "kb") out.push({ name: "retrieve", args: ["query", "filter"] });
  if (kind === "websearch") out.push({ name: "WebSearch", args: ["query"] });
  if (kind === "lambda" && Array.isArray(tool.toolSchema)) {
    for (const s of tool.toolSchema) {
      const o = obj(s);
      if (typeof o.name === "string") out.push({ name: o.name, args: Object.keys(obj(o.properties)).sort() });
    }
  }
  if (kind === "openapi") {
    for (const item of Object.values(obj(obj(tool.schema).paths))) {
      for (const oper of Object.values(obj(item))) {
        const o = obj(oper);
        if (typeof o.operationId === "string") {
          out.push({ name: o.operationId, args: (Array.isArray(o.parameters) ? o.parameters : [])
            .map((p) => obj(p).name).filter((n): n is string => typeof n === "string").sort() });
        }
      }
    }
  }
  if (kind === "apigateway" && Array.isArray(tool.toolOverrides)) {
    for (const o of tool.toolOverrides) if (typeof obj(o).name === "string") out.push({ name: String(obj(o).name), args: [] });
  }
  const pinned = obj(tool.policy).tool;
  if (typeof pinned === "string" && !out.some((t) => t.name === pinned)) out.push({ name: pinned, args: [String(tool.arg ?? "query")] });
  return out;
}

const OPS: { value: Operator | ""; label: string }[] = [
  { value: "", label: "Always (no condition)" },
  { value: "present", label: "when the argument is given" },
  { value: "in", label: "when it is one of" },
  { value: "notIn", label: "when it is not one of" },
  { value: "gt", label: "when it is more than" },
  { value: "lt", label: "when it is less than" },
];

// --- the editor --------------------------------------------------------------------

/** How long the page waits for AgentCore Policy before saying so (it takes ~15 s). */
export const GENERATION_WAIT_MS = 90_000;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** A camelCase policy name from the words of a request. */
export function nameFrom(text: string): string {
  const words = text.replace(/[^A-Za-z0-9 ]/g, " ").split(/\s+/).filter(Boolean).slice(0, 5);
  const s = words.map((w, i) => (i ? w[0].toUpperCase() + w.slice(1).toLowerCase() : w.toLowerCase())).join("");
  return (/^[A-Za-z]/.test(s) ? s : `p${s}`).slice(0, 32) || "myPolicy";
}

export function PolicyEditor({ visible, initial, tools, server, taken, forLibrary = false, buildId, deployed = false,
  pollMs = 2000, onDismiss, onSave }: {
  visible: boolean;
  /** The build: plain English asks AgentCore Policy about ITS deployed engine and Gateway. */
  buildId?: string;
  /** Deployed (so it has an engine and a Gateway to ask). */
  deployed?: boolean;
  pollMs?: number;
  /** Editing this one; null for a new policy. */
  initial: CustomPolicy | null;
  /** The build's tools; {} in the library, where a policy belongs to no build yet. */
  tools: Record<string, Entry>;
  server: boolean;
  /** Names already used where it is going. */
  taken: string[];
  forLibrary?: boolean;
  onDismiss: () => void;
  onSave: (p: CustomPolicy, extra: { toLibrary: boolean; source: PolicySource }) => Promise<void> | void;
}) {
  const keys = Object.keys(tools);
  const [tab, setTab] = useState<PolicySource>("cedar");
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [statement, setStatement] = useState("");
  const [english, setEnglish] = useState("");
  const [drafting, setDrafting] = useState(false);
  const [note, setNote] = useState("");
  const [options, setOptions] = useState<GeneratedPolicy[] | null>(null);
  const [draftError, setDraftError] = useState("");
  const [toLibrary, setToLibrary] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState("");
  const [g, setG] = useState<Guided>({ effect: "forbid", tool: "", toolName: "", arg: "", op: "in", values: "" });
  const [guidedOp, setGuidedOp] = useState<Operator | "">("");

  useEffect(() => {
    if (!visible) return;
    setName(initial?.name ?? "");
    setDescription(initial?.description ?? "");
    setStatement(initial?.statement ?? "");
    setTab(initial ? "cedar" : server && !forLibrary && deployed && keys.length ? "english" : "form");
    setEnglish(""); setNote(""); setDraftError(""); setSaveError(""); setToLibrary(false); setOptions(null);
    const g0: Guided = { effect: "forbid", tool: keys[0] ?? "", toolName: "", arg: "", op: "in", values: "" };
    setG(g0);
    setGuidedOp("");
    const opensOnForm = !initial && !(server && !forLibrary && deployed && keys.length);
    if (opensOnForm && g0.tool) setStatement(fromGuided({ ...g0, arg: "", op: "present" }));
    // Reset only when it opens, not as the tools change underneath it.
  }, [visible, initial]);

  const toolNames = knownTools(tools[g.tool]);
  const argNames = (toolNames.find((t) => t.name === g.toolName)?.args
    ?? [...new Set(toolNames.flatMap((t) => t.args))]);

  const applyGuided = useCallback((next: Guided, op: Operator | "") => {
    setG(next);
    setGuidedOp(op);
    if (next.tool) setStatement(fromGuided({ ...next, arg: op ? next.arg : "", op: op || "present" }));
  }, []);

  const guidedProblem = tab !== "form" ? ""
    : !g.tool ? "Pick the tool it governs."
      : guidedOp && !g.toolName ? "A condition reads one tool's input: pick which of its tools."
      : guidedOp && !ARG_RE.test(g.arg) ? "The argument is a name: letters, digits and _."
        : (guidedOp === "in" || guidedOp === "notIn") && !g.values.trim() ? "List at least one value."
          : (guidedOp === "gt" || guidedOp === "lt") && !/^-?\d+$/.test(g.values.trim()) ? "The limit is a whole number."
            : "";

  const wrong = statement.trim() ? problems(statement, forLibrary ? undefined : keys) : [];
  const nameError = !name ? "" : !NAME_RE.test(name) ? "A letter, then letters and digits (32 at most)."
    : taken.includes(name) && name !== initial?.name ? `"${name}" is already used here.` : "";
  const canSave = Boolean(name) && !nameError && Boolean(statement.trim()) && !wrong.length && !guidedProblem;

  const use = (o: GeneratedPolicy) => {
    setStatement(o.statement);
    const n = nameFrom(o.fragment || english);
    if (!name || !initial) setName(taken.includes(n) ? `${n.slice(0, 30)}2` : n);
    setDescription(o.fragment || english);
  };
  const draft = async () => {
    setDrafting(true); setDraftError(""); setNote(""); setOptions(null);
    try {
      const { generationId } = await policyLibrary.generate(buildId!, english);
      const until = Date.now() + GENERATION_WAIT_MS;
      for (;;) {
        const g = await policyLibrary.generation(buildId!, generationId);
        if (g.status === "GENERATED") {
          const got = g.assets ?? [];
          setOptions(got);
          const usable = got.filter((o) => o.statement && !o.problems.length);
          if (usable.length === 1 && got.length === 1) use(usable[0]);
          if (!got.some((o) => o.statement)) setNote("AgentCore could not express this as a policy. Say it another way, or use the form.");
          break;
        }
        if (g.status !== "GENERATING") {
          setDraftError(`AgentCore Policy could not write it: ${(g.reasons ?? []).join("; ") || g.status}`);
          break;
        }
        if (Date.now() > until) { setDraftError("AgentCore Policy is taking longer than usual. Try again."); break; }
        await sleep(pollMs);
      }
    } catch (e) {
      setDraftError((e as Error).message);
    } finally {
      setDrafting(false);
    }
  };

  const save = async () => {
    setSaving(true); setSaveError("");
    try {
      await onSave({ ...(initial?.libraryId ? { libraryId: initial.libraryId } : {}), name, statement: statement.trim(),
        ...(description.trim() ? { description: description.trim() } : {}) }, { toLibrary, source: tab });
    } catch (e) {
      setSaveError((e as Error).message);
    } finally {
      setSaving(false);
    }
  };

  const toolOptions = keys.map((k) => ({ value: k, label: k, description: String(tools[k].type ?? "") }));
  const tabs = [
    ...(server && !forLibrary ? [{
      id: "english", label: "Describe it",
      content: (
        <SpaceBetween size="s">
          {!deployed ? (
            <Alert type="info" header="Deploy the build first">
              Plain English is written by AgentCore Policy, from this build&apos;s own Gateway and its tools, so it needs the
              build deployed with the policy engine on. Until then, write it with the form or in Cedar.
            </Alert>
          ) : null}
          <FormField label="What should it allow or block?"
            description="In plain words. AgentCore Policy writes the Cedar from your deployed build's tools, one policy per requirement; you review it below before anything is kept.">
            <Textarea value={english} rows={3} onChange={({ detail }) => setEnglish(detail.value)} disabled={!deployed}
              placeholder="Never issue a refund over 500 dollars." />
          </FormField>
          <Button onClick={() => void draft()} loading={drafting} disabled={!english.trim() || !keys.length || !deployed || !buildId}
            disabledReason={!deployed ? "Deploy the build first." : !keys.length ? "Add a tool first: a policy governs a tool." : undefined}>
            {drafting ? "AgentCore Policy is writing it…" : "Write it with AgentCore Policy"}</Button>
          {draftError ? <Alert type="error">{draftError}</Alert> : null}
          {note ? <Alert type="info">{note}</Alert> : null}
          {options && options.length ? (
            <SpaceBetween size="xs">
              {options.map((o, i) => (
                <Box key={i} padding="xs">
                  <SpaceBetween size="xxs">
                    <Box variant="strong">{o.fragment || `Policy ${i + 1}`}</Box>
                    {o.statement ? <pre className="axb-code">{o.statement}</pre> : null}
                    {o.findings.map((f, j) => (
                      <StatusIndicator key={j} type={f.type === "VALID" ? "success" : f.type === "INVALID" ? "error" : "warning"}>
                        {f.type}{f.description ? `: ${f.description}` : ""}</StatusIndicator>
                    ))}
                    {o.problems.map((p) => <StatusIndicator key={p} type="warning">{p}</StatusIndicator>)}
                    {o.statement ? <Button variant="inline-link" onClick={() => use(o)}>Use this one</Button> : null}
                  </SpaceBetween>
                </Box>
              ))}
            </SpaceBetween>
          ) : null}
        </SpaceBetween>
      ),
    }] : []),
    {
      id: "form", label: "Guided",
      content: (
        <SpaceBetween size="s">
          <ColumnLayout columns={2}>
            <FormField label="Effect">
              <SegmentedControl selectedId={g.effect}
                options={[{ id: "forbid", text: "Block" }, { id: "permit", text: "Allow" }]}
                onChange={({ detail }) => applyGuided({ ...g, effect: detail.selectedId as Guided["effect"] }, guidedOp)} />
            </FormField>
            <FormField label="Tool" description={forLibrary ? "The tool key it will govern in a build." : undefined}>
              {forLibrary ? (
                <Input value={g.tool} placeholder="refunds"
                  onChange={({ detail }) => applyGuided({ ...g, tool: detail.value.replace(/[^A-Za-z0-9]/g, "") }, guidedOp)} />
              ) : (
                <Select selectedOption={toolOptions.find((o) => o.value === g.tool) ?? null} options={toolOptions}
                  placeholder="Pick a tool" empty="This build has no tools yet."
                  onChange={({ detail }) => applyGuided({ ...g, tool: detail.selectedOption.value ?? "", toolName: "" }, guidedOp)} />
              )}
            </FormField>
            <FormField label="Which of its tools" description="Empty: all of them.">
              <Autosuggest value={g.toolName} enteredTextLabel={(v) => `Use "${v}"`} placeholder="All of its tools"
                options={toolNames.map((t) => ({ value: t.name }))}
                onChange={({ detail }) => applyGuided({ ...g, toolName: detail.value.replace(/[^A-Za-z0-9_-]/g, "") }, guidedOp)} />
            </FormField>
            <FormField label="Condition">
              <Select selectedOption={OPS.find((o) => o.value === guidedOp) ?? OPS[0]} options={OPS}
                onChange={({ detail }) => applyGuided({ ...g, op: (detail.selectedOption.value || "present") as Operator },
                  (detail.selectedOption.value ?? "") as Operator | "")} />
            </FormField>
            {guidedOp ? (
              <FormField label="Argument" description="The tool input it reads, e.g. amount.">
                <Autosuggest value={g.arg} enteredTextLabel={(v) => `Use "${v}"`} placeholder="amount"
                  options={argNames.map((a) => ({ value: a }))}
                  onChange={({ detail }) => applyGuided({ ...g, arg: detail.value }, guidedOp)} />
              </FormField>
            ) : null}
            {guidedOp && guidedOp !== "present" ? (
              <FormField label={guidedOp === "in" || guidedOp === "notIn" ? "Values, comma-separated" : "Limit (a whole number)"}>
                <Input value={g.values} onChange={({ detail }) => applyGuided({ ...g, values: detail.value }, guidedOp)} />
              </FormField>
            ) : null}
          </ColumnLayout>
          {g.effect === "permit" && g.toolName && !guidedOp ? (
            <Box variant="small" color="text-status-warning">AgentCore rejects an unconditioned permit for one tool: add a condition.</Box>
          ) : null}
          {g.effect === "forbid" && g.tool && !guidedOp ? (
            <Box variant="small" color="text-status-warning">
              With no condition this blocks every call, and the Gateway stops listing {g.toolName ? "this tool" : "these tools"} to agents.
            </Box>
          ) : null}
          {guidedProblem ? <Box variant="small" color="text-status-error">{guidedProblem}</Box> : null}
        </SpaceBetween>
      ),
    },
    {
      id: "cedar", label: "Cedar",
      content: (
        <Box variant="small" color="text-body-secondary">
          Write it below. Actions are AgentCore::Action::&quot;&lt;toolKey&gt;___&lt;toolName&gt;&quot; (one tool, with ==) or
          AgentCore::Action::&quot;&lt;toolKey&gt;&quot; (all of its tools, with in); the resource is always
          AgentCore::Gateway::&quot;{"{{gateway}}"}&quot;, filled in when the build deploys. A forbid wins over any permit.
        </Box>
      ),
    },
  ];

  return (
    <Modal visible={visible} onDismiss={onDismiss} size="large"
      header={initial ? `Edit policy ${initial.name}` : "New policy"}
      footer={
        <Box float="right">
          <SpaceBetween direction="horizontal" size="xs">
            {server && !forLibrary && !initial?.libraryId ? (
              <Checkbox checked={toLibrary} onChange={({ detail }) => setToLibrary(detail.checked)}>Also save to my library</Checkbox>
            ) : null}
            <Button variant="link" onClick={onDismiss} disabled={saving}>Cancel</Button>
            <Button variant="primary" loading={saving} disabled={!canSave} onClick={() => void save()}
              disabledReason={!name ? "Give it a name." : wrong.length ? "Fix the Cedar first." : guidedProblem || undefined}>
              {forLibrary ? "Save" : initial ? "Save" : "Add to build"}
            </Button>
          </SpaceBetween>
        </Box>
      }>
      <SpaceBetween size="m">
        <Tabs activeTabId={tab} tabs={tabs} onChange={({ detail }) => {
          setTab(detail.activeTabId as PolicySource);
          // The form stands for a statement from the start: show it, rather than an empty box.
          if (detail.activeTabId === "form" && !statement.trim()) applyGuided(g, guidedOp);
        }} />
        <ColumnLayout columns={2}>
          <FormField label="Name" errorText={nameError || undefined} description="Letters and digits; names the AgentCore policy.">
            <Input value={name} onChange={({ detail }) => setName(detail.value.replace(/[^A-Za-z0-9]/g, "").slice(0, 32))}
              placeholder="noBigRefunds" />
          </FormField>
          <FormField label="What it does" description="Optional; shown in lists and on the policy in AWS.">
            <Input value={description} onChange={({ detail }) => setDescription(detail.value)} placeholder="Blocks refunds over 500." />
          </FormField>
        </ColumnLayout>
        <FormField label="Cedar" stretch
          errorText={wrong.length ? <ul className="axd-list">{wrong.map((w) => <li key={w}>{w}</li>)}</ul> : undefined}
          constraintText={!wrong.length && statement.trim() ? "Checks out." : undefined}>
          <div className="axp-cedar">
            <Textarea value={statement} rows={9} spellcheck={false} ariaLabel="Cedar statement"
              onChange={({ detail }) => setStatement(detail.value)}
              placeholder={'forbid(\n  principal,\n  action == AgentCore::Action::"refunds___issueRefund",\n  resource == AgentCore::Gateway::"{{gateway}}"\n) when {\n  context.input.amount > 500\n};'} />
          </div>
        </FormField>
        {saveError ? <Alert type="error">{saveError}</Alert> : null}
      </SpaceBetween>
    </Modal>
  );
}

function EffectBadge({ statement }: { statement: string }) {
  const s = summary(statement);
  if (!s) return <StatusIndicator type="error">Not valid</StatusIndicator>;
  return s.effect === "forbid" ? <StatusIndicator type="stopped">Blocks</StatusIndicator>
    : <StatusIndicator type="success">Allows</StatusIndicator>;
}

const actionsOf = (statement: string) => (summary(statement)?.actions ?? []).join(", ") || "—";

// --- a build's policies ------------------------------------------------------------

/** A statement naming exactly one of this build's tools, rewritten for "{{tool}}" so it
 *  can be attached to that tool (tools.<key>.policies) — or to any other. */
export function templatize(statement: string, toolKeys: string[]): { statement: string; tool: string } | null {
  const keys = [...new Set((summary(statement)?.actions ?? []).map((a) => a.split("___")[0]))];
  if (keys.length !== 1 || !toolKeys.includes(keys[0])) return null;
  const k = keys[0];
  return { tool: k, statement: statement.split(`AgentCore::Action::"${k}"`).join('AgentCore::Action::"{{tool}}"')
    .split(`AgentCore::Action::"${k}___`).join('AgentCore::Action::"{{tool}}___') };
}

export function BuildPolicies({ project, setProject, issues, server, notify, deployed = false, list }: {
  /** The build's own policies (Library.tsx NamedTab): each attached to tools. */
  list?: React.ReactNode;
  /** The build is deployed, so AgentCore Policy can write plain English for it. */
  deployed?: boolean;
  project: Project;
  setProject: (p: Project) => void;
  issues: Issue[];
  server: boolean;
  notify: Notify;
}) {
  const wf = project.workflow;
  const block = policyBlock(wf);
  const custom = block.custom ?? [];
  const enabled = block.enabled !== false;
  const mode = String(block.mode ?? "ENFORCE").toUpperCase();
  const tools = wf.tools as Record<string, Entry>;
  const [editing, setEditing] = useState<{ index: number } | null>(null);
  const [picking, setPicking] = useState(false);

  const setBlock = (patch: Partial<PolicyBlock>) => setProject({ ...project, workflow: withPolicy(wf, patch) });
  const setPermit = (key: string, on: boolean) => {
    const t = { ...tools[key] };
    const pol = { ...((t.policy ?? {}) as Record<string, Json>) };
    if (on) delete pol.permit; else pol.permit = false;
    if (Object.keys(pol).length) t.policy = pol; else delete t.policy;
    setProject({ ...project, workflow: { ...wf, tools: { ...wf.tools, [key]: t } } });
  };
  const problemsAt = (i: number) => issues.filter((x) => x.path.startsWith(`orchestrator.policy.custom[${i}]`)).length;

  const save = async (p: CustomPolicy, extra: { toLibrary: boolean; source: PolicySource }, index: number) => {
    let entry = p;
    if (extra.toLibrary) {
      const saved = await policyLibrary.create({ name: p.name, description: p.description, statement: p.statement, source: extra.source });
      entry = { ...p, libraryId: saved.id };
      notify("success", `Saved ${p.name} to your library.`);
    }
    if (index >= 0) {
      // One written before policies were attached to tools: edited where it is.
      const next = [...custom];
      next[index] = entry;
      setBlock({ custom: next });
      setEditing(null);
      return;
    }
    // A new one: into the build's policies, attached to the tool it names.
    const t = templatize(p.statement, Object.keys(tools));
    const pols = { ...((wf.policies ?? {}) as Record<string, Entry>),
      [p.name]: { statement: t ? t.statement : p.statement, ...(p.description ? { description: p.description } : {}) } as Entry };
    const nextTools = { ...wf.tools };
    if (t) {
      const cur = ((nextTools[t.tool].policies ?? []) as string[]).filter((n) => n !== p.name);
      nextTools[t.tool] = { ...nextTools[t.tool], policies: [...cur, p.name] };
    }
    setProject({ ...project, workflow: { ...wf, policies: pols, tools: nextTools } });
    notify("success", t ? `${p.name} is attached to ${t.tool}. Attach it to other tools from each tool's panel.` : `${p.name} added.`);
    setEditing(null);
  };

  const permitted = (key: string) => {
    const pol = (tools[key].policy ?? {}) as Record<string, unknown>;
    const restrict = pol.restrictTo && typeof pol.restrictTo === "object" ? Object.keys(pol.restrictTo as object) : [];
    return `${pol.tool ? `${key}___${String(pol.tool)}` : `all of ${key}`}${restrict.length ? `, only for the allowed ${restrict.join(", ")}` : ""}`;
  };

  return (
    <SpaceBetween size="l">
      <Container header={<Header variant="h2"
        description="Every tool call goes through the Gateway's Cedar engine. Anything no policy permits is denied; a forbid always wins.">
        Policy engine</Header>}>
        <ColumnLayout columns={2}>
          <FormField label="Enabled" description="Off: every declared tool is callable, and none of the policies below deploys.">
            <Toggle checked={enabled} onChange={({ detail }) => setBlock({ enabled: detail.checked })}>
              {enabled ? "On" : "Off"}
            </Toggle>
          </FormField>
          <FormField label="Mode" description="Log only: nothing is blocked, and each decision is logged. Roll a new policy out this way first.">
            <SegmentedControl selectedId={mode} onChange={({ detail }) => setBlock({ mode: detail.selectedId })}
              options={[{ id: "ENFORCE", text: "Enforce" }, { id: "LOG_ONLY", text: "Log only" }]} />
          </FormField>
        </ColumnLayout>
      </Container>

      <Container header={<Header variant="h2"
        description="Say what to allow or block in plain words, pick it on a form, or write Cedar. It is attached to the tool it names; attach it to more tools from each tool's panel."
        actions={<Button variant="primary" iconName="add-plus" onClick={() => setEditing({ index: -1 })}>Write a policy</Button>}>
        Write a policy</Header>} />
      {list}
      {custom.length ? <Table
        header={<Header variant="h2" counter={`(${custom.length})`}
          description="Written before policies were attached to tools. Each deploys as written."
          actions={server ? <Button onClick={() => setPicking(true)}>Add from library</Button> : undefined}>Build-wide policies</Header>}
        items={custom.map((p, index) => ({ p, index }))}
        trackBy={(r) => String(r.index)}
        columnDefinitions={[
          { id: "name", header: "Name", isRowHeader: true, cell: (r) => (
            <Button variant="inline-link" onClick={() => setEditing({ index: r.index })}>{r.p.name || "(no name)"}</Button>) },
          { id: "effect", header: "Effect", cell: (r) => <EffectBadge statement={r.p.statement} /> },
          { id: "tools", header: "Governs", cell: (r) => actionsOf(r.p.statement) },
          { id: "desc", header: "What it does", cell: (r) => r.p.description || "—" },
          { id: "problems", header: "Problems", cell: (r) => {
            const n = problemsAt(r.index);
            return n ? <StatusIndicator type="error">{n}</StatusIndicator> : <StatusIndicator type="success">OK</StatusIndicator>;
          } },
          { id: "actions", header: "", cell: (r) => (
            <Button variant="icon" iconName="remove" ariaLabel={`Remove ${r.p.name}`}
              onClick={() => setBlock({ custom: custom.filter((_, j) => j !== r.index) })} />) },
        ]}
        empty={<Box textAlign="center" color="inherit">No policies of your own. Each tool below is permitted as declared.</Box>}
      /> : null}

      <Table
        header={<Header variant="h2" counter={`(${Object.keys(tools).length})`}
          description="Declaring a tool permits it. Turn one off to allow only what your own policies permit for it.">
          Generated permits</Header>}
        items={Object.keys(tools).map((key) => ({ key }))}
        columnDefinitions={[
          { id: "key", header: "Tool", isRowHeader: true, cell: (r) => r.key },
          { id: "what", header: "Permits", cell: (r) => permitted(r.key) },
          { id: "on", header: "Generated permit", cell: (r) => {
            const on = ((tools[r.key].policy ?? {}) as Record<string, unknown>).permit !== false;
            return (
              <Toggle checked={on} ariaLabel={`Generated permit for ${r.key}`}
                onChange={({ detail }) => setPermit(r.key, detail.checked)}>{on ? "On" : "Off: only your policies"}</Toggle>
            );
          } },
        ]}
        empty={<Box textAlign="center" color="inherit">No tools yet.</Box>}
      />

      <PolicyEditor visible={editing !== null} initial={editing && editing.index >= 0 ? custom[editing.index] : null}
        tools={tools} server={server} taken={custom.map((p) => p.name)} buildId={project.id} deployed={deployed}
        onDismiss={() => setEditing(null)}
        onSave={(p, extra) => save(p, extra, editing?.index ?? -1)} />
      {server ? (
        <LibraryPicker visible={picking} onDismiss={() => setPicking(false)} notify={notify}
          toolKeys={Object.keys(tools)} taken={custom.map((p) => p.name)}
          onAttach={(ps) => {
            setBlock({ custom: [...custom, ...ps.map((p) => ({
              name: p.name, statement: p.statement, libraryId: p.id, ...(p.description ? { description: p.description } : {}) }))] });
            setPicking(false);
            notify("success", `Attached ${ps.map((p) => p.name).join(", ")}.`);
          }} />
      ) : null}
    </SpaceBetween>
  );
}

function LibraryPicker({ visible, toolKeys, taken, onDismiss, onAttach, notify }: {
  visible: boolean; toolKeys: string[]; taken: string[]; notify: Notify;
  onDismiss: () => void; onAttach: (ps: LibraryPolicy[]) => void;
}) {
  const [items, setItems] = useState<LibraryPolicy[] | null>(null);
  const [selected, setSelected] = useState<LibraryPolicy[]>([]);
  useEffect(() => {
    if (!visible) return;
    setSelected([]);
    setItems(null);
    policyLibrary.list().then(setItems, (e) => { notify("error", `Could not load your library: ${(e as Error).message}`); setItems([]); });
  }, [visible, notify]);
  const fits = (p: LibraryPolicy) => !taken.includes(p.name) && !problems(p.statement, toolKeys).length;
  return (
    <Modal visible={visible} onDismiss={onDismiss} size="large" header="Add from your library"
      footer={
        <Box float="right">
          <SpaceBetween direction="horizontal" size="xs">
            <Button variant="link" onClick={onDismiss}>Cancel</Button>
            <Button variant="primary" disabled={!selected.length} onClick={() => onAttach(selected)}>Add {selected.length || ""}</Button>
          </SpaceBetween>
        </Box>
      }>
      <Table selectionType="multi" items={items ?? []} loading={items === null} loadingText="Loading your library"
        selectedItems={selected} onSelectionChange={({ detail }) => setSelected(detail.selectedItems)}
        isItemDisabled={(p) => !fits(p)} trackBy="id"
        columnDefinitions={[
          { id: "name", header: "Name", isRowHeader: true, cell: (p) => p.name },
          { id: "effect", header: "Effect", cell: (p) => <EffectBadge statement={p.statement} /> },
          { id: "tools", header: "Governs", cell: (p) => actionsOf(p.statement) },
          { id: "fit", header: "In this build", cell: (p) => (taken.includes(p.name)
            ? <StatusIndicator type="info">Already added</StatusIndicator>
            : problems(p.statement, toolKeys).length
              ? <StatusIndicator type="warning">{problems(p.statement, toolKeys)[0]}</StatusIndicator>
              : <StatusIndicator type="success">Fits</StatusIndicator>) },
        ]}
        empty={<Box textAlign="center" color="inherit">Your library is empty. Tick &quot;Also save to my library&quot; when you add a policy, or open Policies in the navigation.</Box>}
      />
    </Modal>
  );
}

// --- the library page ----------------------------------------------------------------

export function PolicyLibrary({ notify }: { notify: Notify }) {
  const [items, setItems] = useState<LibraryPolicy[] | null>(null);
  const [editing, setEditing] = useState<LibraryPolicy | "new" | null>(null);
  const load = useCallback(() => {
    policyLibrary.list().then(setItems, (e) => { notify("error", `Could not load your policies: ${(e as Error).message}`); setItems([]); });
  }, [notify]);
  useEffect(load, [load]);
  const names = useMemo(() => (items ?? []).map((p) => p.name), [items]);
  const remove = async (p: LibraryPolicy) => {
    if (!window.confirm(`Delete ${p.name} from your library? Builds it was added to keep their copy.`)) return;
    try {
      await policyLibrary.remove(p.id);
      notify("success", `Deleted ${p.name}.`);
      load();
    } catch (e) {
      notify("error", `Could not delete ${p.name}: ${(e as Error).message}`);
    }
  };
  return (
    <SpaceBetween size="l">
      <Table
        header={<Header variant="h1" counter={items ? `(${items.length})` : undefined}
          description="Cedar policies you keep to reuse. Add one to a build from its Policies tab; the build gets a copy, so editing it here changes no build."
          actions={<Button variant="primary" iconName="add-plus" onClick={() => setEditing("new")}>New policy</Button>}>
          Policies</Header>}
        items={items ?? []} loading={items === null} loadingText="Loading your policies" trackBy="id"
        columnDefinitions={[
          { id: "name", header: "Name", isRowHeader: true, cell: (p) => (
            <Button variant="inline-link" onClick={() => setEditing(p)}>{p.name}</Button>) },
          { id: "effect", header: "Effect", cell: (p) => <EffectBadge statement={p.statement} /> },
          { id: "tools", header: "Governs", cell: (p) => actionsOf(p.statement) },
          { id: "desc", header: "What it does", cell: (p) => p.description || "—" },
          { id: "updated", header: "Updated", cell: (p) => (p.updatedAt ? new Date(p.updatedAt).toLocaleString() : "—") },
          { id: "actions", header: "", cell: (p) => (
            <Button variant="icon" iconName="remove" ariaLabel={`Delete ${p.name}`} onClick={() => void remove(p)} />) },
        ]}
        empty={<Box textAlign="center" color="inherit">No policies yet. Write one here, or tick &quot;Also save to my library&quot; when you add one to a build.</Box>}
      />
      <PolicyEditor visible={editing !== null} initial={editing && editing !== "new" ? editing : null}
        tools={{}} server taken={names} forLibrary
        onDismiss={() => setEditing(null)}
        onSave={async (p, extra) => {
          const body = { name: p.name, description: p.description ?? "", statement: p.statement, source: extra.source };
          if (editing && editing !== "new") await policyLibrary.update(editing.id, body);
          else await policyLibrary.create(body);
          notify("success", `Saved ${p.name}.`);
          setEditing(null);
          load();
        }} />
    </SpaceBetween>
  );
}
