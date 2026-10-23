import type { RunComponent } from "./lib/runComponents";
/** The shapes the BFF actually returns. Kept in one file so a projection change
 *  surfaces as a type error rather than as an undefined at runtime.
 *
 *  Note how little the framework asserts about an AGENT'S OUTPUT: it is a string,
 *  because the contract inside it is the customer's. Everything that reads it does so
 *  by SHAPE (see assets/AssetView.tsx), which is what lets a workflow define its own
 *  asset types without touching this file. */

export type NodeStatus =
  | "pending" | "running" | "done" | "waiting_human"
  | "failed" | "denied" | "cancelled" | "cancelling" | "skipped" | "revise";

export interface AgentMeta {
  name?: string;
  kind?: string;
  runtime?: "main" | "dedicated" | "a2a" | string;
  /** One tool key or a list of them (workflow.json `tool`). Read with lib/tools.toolsOf. */
  tool?: string | string[];
  corpus?: string;
  model?: string;
  produces?: string;
  access?: string;
  maxTokens?: number;
  source?: string;
  skill?: string;
  /** A task step's kind (runtime "task": tool, code, extract, output); `source` labels it. */
  task?: string;
  /** What is attached to it, by kind and name (bff/workflow.py components()). */
  components?: RunComponent[];
  /** The components are the agent's current ones: the run predates storing them. */
  componentsNow?: boolean;
}

export interface BranchRule {
  field?: string;
  goto: string;
  [operator: string]: unknown;
}

export interface Step {
  agent?: string;
  parallel?: string[];
  sequence?: string[];
  hitl?: boolean;
  gateId?: string;
  gateName?: string;
  branch?: { when?: BranchRule[]; default?: string };
}

export interface UiConfig {
  title?: string;
  heading?: string;
  defaultTopic?: string;
  topicPlaceholder?: string;
  subjectPlaceholder?: string;
  subjectHint?: string;
  assistantTitle?: string;
  assistantSubtitle?: string;
}

export interface ChatbotConfig {
  enabled?: boolean;
  greeting?: string;
  placeholder?: string;
  tools?: Record<string, boolean>;
}

export interface Workflow {
  agents: Record<string, AgentMeta>;
  steps: Step[];
  ui?: UiConfig;
  authorization?: { actions?: string[] };
  chatbot?: ChatbotConfig | null;
  evalAgents?: string[];
  /** What a run may be started with (bff/runfiles.py); absent when no agent reads files. */
  attachments?: RunAttach | null;
  /** What starts runs besides a person (bff/triggers.py). */
  triggers?: { name: string; type: string; runAs: string }[] | null;
  /** Admins get the Live config page (orchestrator.liveConfig; bff/liveconfig.py). */
  liveConfig?: boolean | null;
  /** The models Start run may offer a person granted `override` (orchestrator.models.overrides). */
  modelOverrides?: string[] | null;
  /** Which version of the workflow is deployed here, set when the Control Plane deployed it
   *  (bff/handler.py WORKFLOW_VERSION). */
  deployment?: { version: number } | null;
}
/** The Start run form's files: limits, readable types, and S3 locations it may name. */
export interface RunAttach {
  maxFiles: number;
  maxBytes: number;
  imageMaxBytes?: number;
  types: string[];
  s3: string[];
}
/** One limit of `orchestrator.limits` and how much of it the caller has used (GET /api/limits). */
export interface LimitUse {
  limit: number;
  used: number;
  /** When it resets, as a phrase: "midnight Eastern time", "15:00 Eastern time". */
  resets: string;
  /** Used 80% or more of it. */
  warn: boolean;
  /** Used all of it: the next run is refused. */
  reached: boolean;
}
/** The caller's use of the limits that are set. `limits: false` when none is. A limit whose
 *  counter could not be read is left out, never shown as zero. */
export interface LimitsUsage {
  limits: boolean;
  daily?: LimitUse;
  hourly?: LimitUse;
  monthly?: LimitUse;
}
/** A file a run was started with, copied into its own folder. */
export interface RunFile {
  key: string;
  name: string;
  kind: "document" | "image";
  format: string;
  size: number;
  uri?: string;
}

export interface NodeState {
  status: NodeStatus;
  pct?: number;
  output?: string;
}

export interface HistoryEntry {
  version: number;
  at: string;
  comment?: string;
  output?: string;
}

export interface LogLine {
  ts: string;
  node?: string;
  msg: string;
}

export interface SessionSummary {
  session_id: string;
  topic: string;
  overall: NodeStatus | string;
  created?: string;
  /** Who started it (the JWT sub). Sent on every run; the admin list shows it. */
  owner?: string;
  user?: string;
  /** Set when a trigger started it, not a person (bff/triggers.py). */
  trigger?: { name: string; type: string; source: string; runAs: string };
  /** The deployed version of the workflow this run ran on. Absent on runs started before
   *  versions were recorded, and on apps deployed outside the Control Plane. */
  workflowVersion?: number;
}

export interface SessionSnapshot extends SessionSummary {
  updated_at?: string;
  /** The workflow this run RAN WITH, stored when it started. Draw the run from this,
   *  not from whatever the workflow is now. Absent on runs older than the snapshot. */
  workflow?: Workflow;
  /** Set when the run was started against a deployed Builder build. */
  build?: { id: string; name: string; version: number };
  user?: string;
  subject_id?: string;
  /** The files the run was started with. */
  attachments?: RunFile[];
  /** The live config version the run started on (bff/liveconfig.py); 0 = as deployed. */
  configVersion?: number;
  /** The models the run was started with (bff/model_override.py), when someone picked them. */
  modelOverride?: ModelOverride;
  nodes?: Record<string, NodeState>;
  history?: Record<string, HistoryEntry[]>;
  logs?: LogLine[];
  result?: string;
  hitl?: { node: string; question?: string; approval?: string; timeoutAt?: string; timeoutAction?: string;
    /** An agent asking mid-step (app/common/asks.py); absent at a review gate. */
    kind?: "question" | "toolApproval"; askId?: string; tool?: string; args?: string } | null;
}

export interface Me {
  user: string;
  /** The caller's id (the JWT sub): whose builds and runs are theirs. */
  owner?: string;
  /** The `admin` permission: read every user's builds and runs, destroy any build. */
  admin?: boolean;
  groups: string[];
  /** null means "not loaded yet or unrestricted" — everything is allowed. */
  permittedActions: string[] | null;
  authzEnabled: boolean;
  /** Whether this console has the Builder plane: server-side builds, deploy, destroy. */
  builder?: boolean;
  /** "builder": a control-plane console — design, build, deploy; every build's runs live
   *  in its own app, so there are no Runs, Observability or assistant here. */
  consoleMode?: "app" | "builder";
  /** Whether this deployment keeps an activity log (a console, or a build's own app). */
  audit?: boolean;
  /** The caller's email: what a share names them by. */
  email?: string;
  /** A control-plane console's link to a product demo video (lib/demoVideo.ts). */
  demoVideoUrl?: string;
  /** The deployment's documentation base URL, when it sets one (docs/docsLinks.ts). */
  docsUrl?: string;
  /** false when the deployment turned documentation links off (-c docsLinks=false). */
  docsLinks?: boolean;
}

/** The mutating actions `authorization` in workflow.json can gate. */
export type Action =
  | "start" | "decision" | "rerun" | "cancel" | "evaluate" | "insights" | "delete"
  | "deploy" | "destroy" | "audit" | "admin" | "override";
/** A run's model override: one model for every agent, and/or one per agent (bff/model_override.py). */
export interface ModelOverride { all?: string; agents?: Record<string, string> }
