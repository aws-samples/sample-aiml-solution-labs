/** The Builder's project, and every edit it can make to one.
 *
 *  A project IS a workflow.json plus the prompts written for its agents. The workflow is
 *  kept as the raw document — not a parallel model the Builder invents — so importing a
 *  hand-written file and exporting it again loses nothing, including keys the Builder
 *  does not render (`$comment`, a key a newer framework added). That is what makes the
 *  Builder a two-way door.
 *
 *  Every operation here returns a NEW document and keeps `steps` inside the grammar the
 *  framework accepts: a stage is `{agent}`, `{parallel: [...]}` or `{sequence: [...]}`,
 *  and an agent appears in at most one of them. The canvas can therefore only ever
 *  produce a shape the orchestrator can run. */

import { toolsOf } from "../lib/tools";
import { FRAMEWORK_VERSION, defaults } from "./meta";

export type Json = null | boolean | number | string | Json[] | { [k: string]: Json };
export type Entry = Record<string, Json>;

export interface StepSpec {
  agent?: string;
  parallel?: string[];
  sequence?: string[];
  /** true, or how the gate decides (app/common/gates.py). */
  hitl?: boolean | GateSpec;
  gateId?: string;
  gateName?: string;
  branch?: { when?: Entry[]; default?: string };
  [k: string]: Json | undefined;
}

export interface Workflow {
  agents: Record<string, Entry>;
  tools: Record<string, Entry>;
  steps: StepSpec[];
  [k: string]: Json | Record<string, Entry> | StepSpec[] | undefined;
}

/** How a Builder-made agent's agent.py reasons. An authoring choice, so it rides in the
 *  bundle and scaffold.py uses it when it CREATES the folder — it is never written to
 *  workflow.json, because nothing at run time reads it. */
export type Framework = "plain" | "strands" | "langgraph";

export interface AgentPrompt {
  systemPrompt: string;
  schema: string;
  framework?: Framework;
}

export interface Project {
  id: string;
  name: string;
  createdAt: string;
  updatedAt: string;
  workflow: Workflow;
  prompts: Record<string, AgentPrompt>;
  /** The files of each tool written in the build (tools.<key>.code): handler.py,
   *  requirements.txt, any other *.py it imports, and events.json (test events, which
   *  are not deployed). Kept beside the workflow like prompts, and exported with it. */
  toolCode?: Record<string, ToolFiles>;
}

export type ToolFiles = Record<string, string>;
/** steps[].hitl as an object (app/common/gates.py): mode, when, approval, timeout. */
export type GateSpec = Record<string, Json>;

/** The tool keys whose function is written in the build. Mirrors bff/buildstore.py. */
export function codeTools(wf: Workflow): string[] {
  return Object.entries(wf.tools ?? {}).filter(([, t]) => String(t.type ?? "").toLowerCase() === "lambda"
    && t.code !== undefined).map(([k]) => k);
}
/** interceptor-<point> for each Gateway interceptor written in the build: its files are
 *  toolCode["interceptor-<point>"]. Mirrors bff/buildstore.py code_interceptors. */
export function codeInterceptors(wf: Workflow): string[] {
  const orch = wf.orchestrator as Record<string, unknown> | undefined;
  const ics = (orch && typeof orch.interceptors === "object" && orch.interceptors) as Record<string, unknown> | false;
  return (["request", "response"] as const).filter((p) => {
    const ic = ics ? ics[p] : undefined;
    return !!ic && typeof ic === "object" && !Array.isArray(ic) && "code" in (ic as object);
  }).map((p) => `interceptor-${p}`);
}
/** Every function written in the build: code tools, then interceptors. */
export function codeFunctions(wf: Workflow): string[] {
  return [...codeTools(wf), ...codeInterceptors(wf)];
}

export type StageKind = "single" | "parallel" | "sequence";

const clone = <T>(v: T): T => structuredClone(v);

// ---------------------------------------------------------------------------
// Reading steps
// ---------------------------------------------------------------------------

export function stepAgents(step: StepSpec): string[] {
  if (step.parallel) return [...step.parallel];
  if (step.sequence) return [...step.sequence];
  return step.agent ? [step.agent] : [];
}

export function stageKind(step: StepSpec): StageKind {
  if (step.parallel) return "parallel";
  if (step.sequence) return "sequence";
  return "single";
}

/** What a branch `goto` names a step by. Mirrors graph_builder._step_name. */
export function stepName(step: StepSpec, index: number): string {
  return step.agent || step.gateId || `group${index}`;
}

export function stageOf(wf: Workflow, agentId: string): number {
  return wf.steps.findIndex((s) => stepAgents(s).includes(agentId));
}

export function unplaced(wf: Workflow): string[] {
  const placed = new Set(wf.steps.flatMap(stepAgents));
  return Object.keys(wf.agents).filter((id) => !placed.has(id));
}

// ---------------------------------------------------------------------------
// Ids and names
// ---------------------------------------------------------------------------

/** "claims_intake" -> "Claims Intake". Matches scaffold.title_of for plain ids. */
export function titleOf(id: string): string {
  return id.split("_").filter(Boolean)
    .map((w) => w[0].toUpperCase() + w.slice(1).toLowerCase()).join(" ");
}

/** A readable name -> a legal agent id ("Claims Intake" -> "claims_intake"). */
export function idFromName(name: string): string {
  const id = name.trim().toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
  return /^[a-z]/.test(id) ? id : `agent_${id || "new"}`;
}

/** A readable name -> a legal tool key ("Claims DB" -> "claimsDb"). */
export function toolKeyFromName(name: string): string {
  const words = name.trim().split(/[^A-Za-z0-9]+/).filter(Boolean);
  const key = words.map((w, i) => (i === 0 ? w.toLowerCase()
    : w[0].toUpperCase() + w.slice(1).toLowerCase())).join("");
  return /^[A-Za-z]/.test(key) ? key : `tool${key || "New"}`;
}

export function uniqueKey(base: string, taken: Iterable<string>, sep = "_"): string {
  const used = new Set(taken);
  if (!used.has(base)) return base;
  for (let i = 2; ; i++) if (!used.has(`${base}${sep}${i}`)) return `${base}${sep}${i}`;
}

// ---------------------------------------------------------------------------
// A new project
// ---------------------------------------------------------------------------

export const DEFAULT_SCHEMA =
  '{"summary": "one or two sentences on what you found", '
  + '"findings": ["each material point, as its own string"], '
  + '"openQuestions": ["what you could not determine, or []"]}';

export function defaultPrompt(name: string): AgentPrompt {
  return {
    systemPrompt:
      `You are the ${name} agent. <Say what this agent is for, in one or two sentences.>\n\n`
      + "RULES\n"
      + "1. Use ONLY the inputs you are given. Invent nothing.\n"
      + "2. No figure that is not in your inputs — no cost, threshold, percentage or duration.\n"
      + "3. Say plainly what you could not determine, rather than filling the gap.\n"
      + "4. Return ONLY valid JSON.",
    schema: DEFAULT_SCHEMA,
  };
}

/** A local agent entry the way `scaffold.py agent` writes one. */
export function newAgentEntry(name: string): Entry {
  return {
    name,
    runtime: "main",
    produces: `${idFromName(name).replace(/_/g, "-")}-output`,
    maxTokens: Number(defaults().agent?.maxTokens ?? 4000),
    access: ["Upstream assets (orchestrator graph state)"],
    // The shared guardrail is ON for a new agent's input and output: turning it off is
    // the choice to make deliberately, not the one to forget.
    agentcore: { guardrails: { input: true, output: true } },
  };
}

/** The smallest workflow that deploys and passes the customer's own test suite — the
 *  same shape `scaffold.py reset` leaves: one agent, one gated step, no tools, and every
 *  top-level block present (tests/test_config_keys.py requires all of them). */
export function newProject(name: string, now = new Date()): Project {
  const d = defaults();
  const agent = newAgentEntry("First Agent");
  agent.agentcore = {
    ...(agent.agentcore as Entry),
    evaluations: {
      enabled: true, auto: false,
      evaluators: ["Builtin.Faithfulness", "Builtin.ResponseRelevance"],
    },
  };
  const workflow: Workflow = {
    $schema: "./workflow.schema.json",
    $comment: `THIS FILE IS THE WORKFLOW. Designed in the AgentExpress Builder (${name}). `
      + "Both Terraform and CDK read it; see orchestrator/docs/WORKFLOW_REFERENCE.md.",
    // runtimeInvoke is written out because tests/test_runtime_invoke.py requires the
    // block to be visible in the file: it governs retries of a call that is NOT
    // idempotent, and a setting like that should not be an invisible default.
    orchestrator: {
      defaultModel: String(d.orchestrator?.defaultModel ?? ""),
      runtimeInvoke: (d.orchestrator?.runtimeInvoke ?? { maxAttempts: 1, readTimeoutSeconds: 600 }) as Json,
    },
    // No empty strings: the BFF drops them, so the page would fall back to its own
    // text while the file claimed a value.
    ui: { title: name, heading: name, topicPlaceholder: "What should the workflow work on?" },
    guardrail: {
      blockedInputMessage: String(d.guardrail?.blockedInputMessage ?? ""),
      blockedOutputMessage: String(d.guardrail?.blockedOutputMessage ?? ""),
      contentFilters: {
        HATE: "MEDIUM", VIOLENCE: "MEDIUM", SEXUAL: "HIGH",
        INSULTS: "MEDIUM", MISCONDUCT: "MEDIUM", PROMPT_ATTACK: "HIGH",
      },
    },
    // Closed by default: a run action is for the build's `members`, everyone's activity
    // for its `admins`. Deploying creates both groups and puts the owner in each, so
    // the owner can do everything and anyone else signs in with nothing until added.
    authorization: {
      actions: {
        start: ["members"], decision: ["members"], rerun: ["members"], cancel: ["members"],
        evaluate: ["members"], delete: ["members"],
        insights: ["admins"], audit: ["admins"], admin: ["admins"],
      },
    },
    tools: {},
    agents: { first_agent: agent },
    steps: [{ agent: "first_agent", hitl: true }],
  };
  const stamp = now.toISOString();
  return {
    id: `p${now.getTime().toString(36)}${Math.random().toString(36).slice(2, 6)}`,
    name, createdAt: stamp, updatedAt: stamp, workflow,
    prompts: { first_agent: defaultPrompt("First Agent") },
  };
}

// ---------------------------------------------------------------------------
// Steps
// ---------------------------------------------------------------------------

/** Take `agentId` out of whatever stage holds it, tidying what is left behind. */
function detach(steps: StepSpec[], agentId: string): StepSpec[] {
  const out: StepSpec[] = [];
  for (const step of steps) {
    const members = stepAgents(step);
    if (!members.includes(agentId)) { out.push(step); continue; }
    const left = members.filter((m) => m !== agentId);
    if (left.length === 0) continue;                 // the stage goes with its last agent
    const kind = stageKind(step);
    if (left.length === 1) {
      // A group of one is a single step. Its gate stays; a group name does not apply.
      const { parallel: _p, sequence: _s, gateId: _g, gateName: _n, ...rest } = step;
      out.push({ ...rest, agent: left[0] });
    } else {
      out.push({ ...step, [kind]: left });
    }
  }
  return out;
}

/** Place `agentId` in a NEW stage at `index` (moving it if it is already placed). */
export function insertStage(wf: Workflow, index: number, agentId: string): Workflow {
  // `index` is a GAP in the current list (0 = before the first stage, n = after the
  // last). If the agent was alone in a stage above that gap, removing it closes one gap.
  const origin = stageOf(wf, agentId);
  const alone = origin >= 0 && stepAgents(wf.steps[origin]).length === 1;
  const next = clone(wf);
  const steps = detach(next.steps, agentId);
  let at = index - (alone && origin < index ? 1 : 0);
  at = Math.max(0, Math.min(steps.length, at));
  // Moving a whole single stage keeps its gate and branch; a new stage is gated by
  // default, because a human sign-off is the safer thing to forget to remove.
  const moved: StepSpec = alone ? clone(wf.steps[origin]) : { agent: agentId, hitl: true };
  steps.splice(at, 0, moved);
  next.steps = steps;
  return next;
}

/** Add `agentId` to the stage at `index`, turning a single step into a group. */
export function joinStage(wf: Workflow, index: number, agentId: string,
  kind: Exclude<StageKind, "single"> = "parallel"): Workflow {
  const target = wf.steps[index];
  if (!target || stepAgents(target).includes(agentId)) return wf;
  const next = clone(wf);
  // Identify the target by its members, because detaching can shift indices.
  const marker = stepAgents(target)[0];
  const steps = detach(next.steps, agentId);
  const i = steps.findIndex((s) => stepAgents(s).includes(marker));
  const step = steps[i];
  const members = [...stepAgents(step), agentId];
  const existing = stageKind(step);
  const { agent: _a, parallel: _p, sequence: _s, ...rest } = step;
  steps[i] = { [existing === "single" ? kind : existing]: members, ...rest };
  next.steps = steps;
  return next;
}

/** Take an agent off the canvas (it stays defined, just unplaced). */
export function unplace(wf: Workflow, agentId: string): Workflow {
  const next = clone(wf);
  next.steps = detach(next.steps, agentId);
  return next;
}

export function moveStage(wf: Workflow, index: number, delta: number): Workflow {
  const to = index + delta;
  if (to < 0 || to >= wf.steps.length) return wf;
  const next = clone(wf);
  const [s] = next.steps.splice(index, 1);
  next.steps.splice(to, 0, s);
  return next;
}

export function setStageKind(wf: Workflow, index: number, kind: StageKind): Workflow {
  const step = wf.steps[index];
  if (!step) return wf;
  const members = stepAgents(step);
  if (kind === "single" && members.length !== 1) return wf;
  const next = clone(wf);
  const { agent: _a, parallel: _p, sequence: _s, ...rest } = next.steps[index];
  next.steps[index] = kind === "single" ? { agent: members[0], ...rest } : { [kind]: members, ...rest };
  return next;
}

/** Merge `patch` into the stage at `index`; `undefined` removes a key. */
export function updateStage(wf: Workflow, index: number, patch: Partial<StepSpec>): Workflow {
  const next = clone(wf);
  const step: StepSpec = { ...next.steps[index] };
  for (const [k, v] of Object.entries(patch)) {
    if (v === undefined || v === "") delete step[k];
    else step[k] = v as Json;
  }
  next.steps[index] = step;
  return next;
}

/** Reorder the members of a group stage. */
export function moveMember(wf: Workflow, index: number, agentId: string, delta: number): Workflow {
  const step = wf.steps[index];
  const kind = stageKind(step);
  if (kind === "single") return wf;
  const members = stepAgents(step);
  const i = members.indexOf(agentId);
  const to = i + delta;
  if (i < 0 || to < 0 || to >= members.length) return wf;
  members.splice(i, 1);
  members.splice(to, 0, agentId);
  const next = clone(wf);
  next.steps[index] = { ...next.steps[index], [kind]: members };
  return next;
}

// ---------------------------------------------------------------------------
// Agents
// ---------------------------------------------------------------------------

export function addAgent(project: Project, name: string): { project: Project; id: string } {
  const id = uniqueKey(idFromName(name), Object.keys(project.workflow.agents));
  const next = clone(project);
  next.workflow.agents[id] = newAgentEntry(name);
  next.prompts[id] = defaultPrompt(name);
  return { project: next, id };
}

export function updateEntry(project: Project, kind: "agents" | "tools", id: string, entry: Entry): Project {
  const next = clone(project);
  next.workflow[kind][id] = entry;
  return next;
}

export function removeAgent(project: Project, id: string): Project {
  const next = clone(project);
  delete next.workflow.agents[id];
  delete next.prompts[id];
  next.workflow.steps = detach(next.workflow.steps, id);
  return next;
}

/** Rename an agent id everywhere it is referenced: steps, branch targets, prompts. The
 *  key's position in `agents` is kept, so a rename is a one-line diff. */
export function renameAgent(project: Project, from: string, to: string): Project {
  if (from === to || !(from in project.workflow.agents) || to in project.workflow.agents) return project;
  const next = clone(project);
  next.workflow.agents = Object.fromEntries(
    Object.entries(next.workflow.agents).map(([k, v]) => [k === from ? to : k, v]));
  const swap = (x: string) => (x === from ? to : x);
  next.workflow.steps = next.workflow.steps.map((s) => {
    const step: StepSpec = { ...s };
    if (step.agent) step.agent = swap(step.agent);
    if (step.parallel) step.parallel = step.parallel.map(swap);
    if (step.sequence) step.sequence = step.sequence.map(swap);
    if (step.branch) {
      step.branch = {
        ...step.branch,
        ...(step.branch.when ? { when: step.branch.when.map((r) => ({ ...r, goto: swap(String(r.goto ?? "")) })) } : {}),
        ...(step.branch.default ? { default: swap(step.branch.default) } : {}),
      };
    }
    return step;
  });
  if (from in next.prompts) {
    next.prompts[to] = next.prompts[from];
    delete next.prompts[from];
  }
  return next;
}

// ---------------------------------------------------------------------------
// Tools
// ---------------------------------------------------------------------------

export function addTool(project: Project, name: string, type: string): { project: Project; key: string } {
  const key = uniqueKey(toolKeyFromName(name), Object.keys(project.workflow.tools), "");
  const next = clone(project);
  const entry: Entry = { type, description: name };
  if (type === "kb") {
    entry.corpora = ["reference"];
    // Retrieval depth is a cost and quality lever, so it is written out rather than left
    // to a default nobody sees (tests/test_observability_honesty.py holds a kb to it).
    entry.maxResults = Number((defaults().perType as Record<string, Record<string, Record<string, number>>> | undefined)?.tool?.maxResults?.kb ?? 5);
  }
  if (type === "mcp") entry.endpoint = "https://";
  if (type === "lambda") {
    entry.lambdaArn = "";
    entry.toolSchema = [{ name: "run", description: "", properties: { query: { type: "string", required: true, description: "" } } }];
    entry.call = "run";
  }
  if (type === "openapi") entry.schemaS3Uri = "s3://";
  if (type === "apigateway") {
    entry.restApiId = "";
    entry.stage = "prod";
    entry.toolFilters = [{ path: "/*", methods: ["GET"] }];
    entry.auth = "sigv4";
  }
  next.workflow.tools[key] = entry;
  return { project: next, key };
}

/** Set an agent's tools, in order, keeping workflow.json in its simplest form: no key
 *  for none, a string for one, a list only for several. `corpus` follows the agent's
 *  Knowledge Base tool — kept, defaulted to its first corpus, or dropped. */
export function setTools(project: Project, agentId: string, keys: string[]): Project {
  const agent = project.workflow.agents[agentId];
  if (!agent || agent.runtime === "a2a") return project;
  const next = clone(project);
  const a = next.workflow.agents[agentId];
  const wanted = [...new Set(keys)].filter((k) => k in next.workflow.tools);
  if (wanted.length === 0) {
    delete a.tool;
    delete a.toolMode;                   // nothing left to choose among
    delete a.maxToolCalls;
  } else {
    a.tool = wanted.length === 1 ? wanted[0] : wanted;
    delete a.access;                     // `access` and `tool` are exclusive (test_config_keys)
    // An agent built here lets its model choose among its tools unless you said otherwise.
    if (a.toolMode === undefined) a.toolMode = "model";
  }
  const kb = wanted.map((k) => next.workflow.tools[k]).find((t) => t.type === "kb");
  const corpora = kb && Array.isArray(kb.corpora) ? kb.corpora.map(String) : [];
  if (!corpora.length) delete a.corpus;
  else if (!corpora.includes(String(a.corpus ?? ""))) a.corpus = corpora[0];
  return next;
}

/** Add a tool to an agent — the drop of a tool onto an agent node. Adds, never
 *  replaces: an agent can read several tools. */
export function bindTool(project: Project, agentId: string, toolKey: string): Project {
  const agent = project.workflow.agents[agentId];
  if (!agent || !(toolKey in project.workflow.tools)) return project;
  const current = toolsOf(agent.tool);
  if (current.includes(toolKey)) return project;
  return setTools(project, agentId, [...current, toolKey]);
}

export function unbindTool(project: Project, agentId: string, toolKey: string): Project {
  const agent = project.workflow.agents[agentId];
  if (!agent) return project;
  return setTools(project, agentId, toolsOf(agent.tool).filter((t) => t !== toolKey));
}

export function renameTool(project: Project, from: string, to: string): Project {
  if (from === to || !(from in project.workflow.tools) || to in project.workflow.tools) return project;
  const next = clone(project);
  next.workflow.tools = Object.fromEntries(
    Object.entries(next.workflow.tools).map(([k, v]) => [k === from ? to : k, v]));
  for (const a of Object.values(next.workflow.agents)) {
    if (!toolsOf(a.tool).includes(from)) continue;
    const renamed = toolsOf(a.tool).map((t) => (t === from ? to : t));
    a.tool = renamed.length === 1 ? renamed[0] : renamed;
  }
  if (next.toolCode?.[from]) {
    next.toolCode[to] = next.toolCode[from];
    delete next.toolCode[from];
  }
  return next;
}

export function removeTool(project: Project, key: string): Project {
  let next = clone(project);
  delete next.workflow.tools[key];
  if (next.toolCode) delete next.toolCode[key];
  for (const [id, a] of Object.entries(next.workflow.agents)) {
    if (toolsOf(a.tool).includes(key)) {
      // setTools re-derives `corpus` from whatever tools remain.
      a.tool = toolsOf(a.tool).filter((t) => t !== key);
      next = setTools(next, id, toolsOf(next.workflow.agents[id].tool));
    }
  }
  return next;
}

// ---------------------------------------------------------------------------
// Import / export
// ---------------------------------------------------------------------------

export const BUNDLE_FORMAT = "agentexpress-bundle";
export const BUNDLE_VERSION = 1;

export interface Bundle {
  format: typeof BUNDLE_FORMAT;
  version: number;
  /** The framework this page was built from (orchestrator/VERSION). */
  framework?: { version: string };
  project: { id: string; name: string; exportedAt: string };
  workflow: Workflow;
  prompts: Record<string, AgentPrompt>;
  toolCode?: Record<string, ToolFiles>;
}

/** What `scaffold.py apply` reads. Prompts only for agents that still exist, code only
 *  for tools and interceptors still written in the build (mirrors bff/buildstore.py
 *  bundle_of). */
export function toBundle(project: Project, now = new Date()): Bundle {
  const prompts = Object.fromEntries(Object.entries(project.prompts)
    .filter(([id]) => id in project.workflow.agents && project.workflow.agents[id].runtime !== "a2a"));
  const coded = codeFunctions(project.workflow);
  const toolCode = Object.fromEntries(Object.entries(project.toolCode ?? {}).filter(([k]) => coded.includes(k)));
  return {
    format: BUNDLE_FORMAT, version: BUNDLE_VERSION,
    framework: { version: FRAMEWORK_VERSION },
    project: { id: project.id, name: project.name, exportedAt: now.toISOString() },
    workflow: project.workflow, prompts,
    ...(Object.keys(toolCode).length ? { toolCode } : {}),
  };
}

/** Open a file: either a bundle the Builder exported, or a plain workflow.json. */
export function fromFile(text: string, fallbackName: string, now = new Date()): Project {
  let doc: unknown;
  try {
    doc = JSON.parse(text);
  } catch (e) {
    throw new Error(`Not valid JSON: ${(e as Error).message}`);
  }
  if (typeof doc !== "object" || doc === null || Array.isArray(doc)) {
    throw new Error("Expected a JSON object: a workflow.json or an AgentExpress bundle.");
  }
  const obj = doc as Record<string, unknown>;
  const isBundle = obj.format === BUNDLE_FORMAT;
  if (isBundle && obj.version !== BUNDLE_VERSION) {
    throw new Error(`Bundle version ${String(obj.version)} is not supported (expected ${BUNDLE_VERSION}).`);
  }
  const wf = (isBundle ? obj.workflow : obj) as Workflow | undefined;
  if (!wf || typeof wf !== "object" || typeof wf.agents !== "object" || !Array.isArray(wf.steps)) {
    throw new Error("This file has no `agents` and `steps`, so it is not a workflow.json.");
  }
  const workflow: Workflow = { ...wf, tools: wf.tools ?? {}, agents: wf.agents, steps: wf.steps };
  const meta = (isBundle ? obj.project : null) as { name?: string } | null;
  const base = newProject(meta?.name || fallbackName, now);
  return migrateFramework({
    ...base,
    workflow,
    // A plain workflow.json carries no prompts: its agents' code already exists, and
    // `apply` leaves existing folders alone, so there is nothing to invent here.
    prompts: isBundle ? ((obj.prompts as Record<string, AgentPrompt>) ?? {}) : {},
    ...(isBundle && obj.toolCode && typeof obj.toolCode === "object"
      ? { toolCode: obj.toolCode as Record<string, ToolFiles> } : {}),
  });
}

/** Put an uploaded file INTO this build: its workflow replaces the build's, and a
 *  bundle's prompts replace those it carries. The build keeps its id and name — so it
 *  is still the same build, deployable and undoable — and keeps the prompts of agents
 *  the file still has but did not bring prompts for (a plain workflow.json has none). */
/** Older builds kept an agent's framework in its prompt, where a workflow.json could not
 *  carry it. Move it onto the agent (unless the agent already names one). */
export function migrateFramework(project: Project): Project {
  const moved = Object.entries(project.prompts).filter(([id, p]) => p.framework
    && project.workflow.agents[id] && project.workflow.agents[id].framework === undefined);
  const stale = Object.values(project.prompts).some((p) => p.framework !== undefined);
  if (!moved.length && !stale) return project;
  const agents = { ...project.workflow.agents };
  for (const [id, p] of moved) {
    if (p.framework !== "plain") agents[id] = { ...agents[id], framework: p.framework! };
  }
  const prompts = Object.fromEntries(Object.entries(project.prompts)
    .map(([id, p]) => { const { framework: _f, ...rest } = p; return [id, rest]; }));
  return { ...project, workflow: { ...project.workflow, agents }, prompts };
}

export function replaceWorkflow(project: Project, text: string, now = new Date()): Project {
  const got = fromFile(text, project.name, now);
  const kept = Object.fromEntries(Object.entries(project.prompts)
    .filter(([id]) => id in got.workflow.agents));
  const coded = codeFunctions(got.workflow);
  const code = Object.fromEntries(Object.entries({ ...(project.toolCode ?? {}), ...(got.toolCode ?? {}) })
    .filter(([k]) => coded.includes(k)));
  return { ...project, workflow: got.workflow, prompts: { ...kept, ...got.prompts },
    ...(Object.keys(code).length ? { toolCode: code } : { toolCode: undefined }) };
}

// ---------------------------------------------------------------------------
// Library items used live (bff/library.py)
// ---------------------------------------------------------------------------

/** workflow map -> library kind. A build keeps an item it uses as {"library": "<id>"}. */
export const MAP_KIND = { tools: "tool", guardrails: "guardrail", memories: "memory",
  evaluators: "evaluator", identities: "identity", policies: "policy", skills: "skill" } as const;
export type NamedMapName = keyof typeof MAP_KIND;
export interface Ref { id: string; kind: string; name: string; definition: Record<string, unknown>; files?: Record<string, string> }

export const refOf = (entry: unknown): string | null =>
  entry && typeof entry === "object" && !Array.isArray(entry)
  && typeof (entry as Record<string, unknown>).library === "string" ? String((entry as Record<string, unknown>).library) : null;

/** The project as it deploys: each live entry replaced by its item as it is now (and a
 *  code tool's files). An entry whose item is not loaded stays, and the validator says so.
 *  Mirrors bff/library.py resolve. */
export function resolveProject(project: Project, refs: Record<string, Ref>): Project {
  let next: Project | null = null;
  for (const [m, kind] of Object.entries(MAP_KIND)) {
    const entries = project.workflow[m] as Record<string, Entry> | undefined;
    if (!entries || typeof entries !== "object") continue;
    for (const [key, entry] of Object.entries(entries)) {
      const ref = refs[refOf(entry) ?? ""];
      if (!ref || ref.kind !== kind) continue;
      next ??= clone(project);
      (next.workflow[m] as Record<string, Entry>)[key] = clone(ref.definition) as Entry;
      if (kind === "tool" && ref.files) next.toolCode = { ...(next.toolCode ?? {}), [key]: clone(ref.files) };
    }
  }
  return next ?? project;
}

/** An edit made on the resolved project, written back to the stored one: an entry that
 *  was live and still equals its item stays live. One that was edited becomes this
 *  build's own copy — so an edit here never changes the item for other builds; that is
 *  what editing the item itself is for. */
export function relink(raw: Project, edited: Project, refs: Record<string, Ref>): Project {
  const out = clone(edited);
  for (const m of Object.keys(MAP_KIND)) {
    const before = raw.workflow[m] as Record<string, Entry> | undefined;
    const after = out.workflow[m] as Record<string, Entry> | undefined;
    if (!before || !after) continue;
    for (const [key, entry] of Object.entries(before)) {
      const id = refOf(entry);
      const ref = id ? refs[id] : undefined;
      if (!id || !(key in after)) continue;
      if (!ref) { after[key] = entry; continue; }          // not loaded: keep the link as it was
      if (JSON.stringify(after[key]) !== JSON.stringify(ref.definition)) continue;
      after[key] = { library: id };
      if (m === "tools" && out.toolCode?.[key] && JSON.stringify(out.toolCode[key]) === JSON.stringify(ref.files ?? {})) {
        delete out.toolCode[key];
      }
    }
  }
  return out;
}

/** Use a library item in this build, live, under a free key made from its name. */
export function attachItem(project: Project, map: NamedMapName, item: Ref): { project: Project; key: string } {
  const taken = Object.keys((project.workflow[map] as Record<string, Entry> | undefined) ?? {});
  const existing = Object.entries((project.workflow[map] as Record<string, Entry> | undefined) ?? {})
    .find(([, e]) => refOf(e) === item.id);
  if (existing) return { project, key: existing[0] };
  const key = uniqueKey(item.name, taken, "");
  const next = clone(project);
  next.workflow[map] = { ...((next.workflow[map] as Record<string, Entry> | undefined) ?? {}), [key]: { library: item.id } };
  return { project: next, key };
}

/** The build's name and the app's `ui.title` / `ui.heading` start out the same (see the
 *  new-build template above), and a user renaming one expects the others to follow. They
 *  are kept in step while they agree: a field that still shows the old name, or is unset,
 *  follows the change; one the user set to something of its own is left alone. */
function follows(value: unknown, old: string): boolean {
  return value === undefined || value === "" || value === old;
}

/** Rename the build; `ui.title` and `ui.heading` follow while they matched the old name. */
export function renameProject(p: Project, name: string): Project {
  const ui = { ...((p.workflow.ui ?? {}) as Record<string, Json>) };
  for (const k of ["title", "heading"] as const) if (follows(ui[k], p.name)) ui[k] = name;
  return { ...p, name, workflow: { ...p.workflow, ui } };
}

/** Change the `ui` block (the Settings tab). `title` is the build's name as the app shows
 *  it: changing it from a value that matched the name renames the build, and `heading`
 *  follows while it matched too. Editing `heading` changes only the heading — that is how
 *  a build gets a header of its own. A title cleared to empty renames nothing. */
export function setUi(p: Project, next: Record<string, Json>): Project {
  const old = (p.workflow.ui ?? {}) as Record<string, Json>;
  const ui = { ...next };
  const title = typeof ui.title === "string" ? ui.title.trim() : "";
  if (ui.title === old.title || !title || old.title !== p.name) {
    return { ...p, workflow: { ...p.workflow, ui } };
  }
  if (ui.heading === old.heading && follows(old.heading, p.name)) ui.heading = title;
  return { ...p, name: title, workflow: { ...p.workflow, ui } };
}
