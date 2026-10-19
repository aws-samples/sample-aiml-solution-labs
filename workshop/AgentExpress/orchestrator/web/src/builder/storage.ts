/** Where builds are kept.
 *
 *  ON A CONSOLE WITH THE BUILDER PLANE (the default), BUILDS LIVE ON THE SERVER: in the
 *  console's builds store, per user, so a build follows you to any browser — and a
 *  deployed build can be rebuilt exactly, because every deploy freezes a version there
 *  (bff/builds.py). That is `serverStore`.
 *
 *  Without it (`-c builder=false` / `enable_builder = false`, or no backend at all) the
 *  Build view still designs and exports, keeping drafts in this browser: `localStore`.
 *  The first time a server store is available, drafts left in this browser are moved
 *  into it (`migrateLocalDrafts`) and removed here, so nothing is stranded.
 *
 *  An INVALID draft is saved like any other — validation gates deploy and export, never
 *  saving, because "I have not finished yet" is the normal state of a draft. */

import { api } from "../api";
import type { Entry, Project } from "./model";

/** Fired on every save and delete, so the side navigation's list of builds stays
 *  current without the shell importing — and so downloading — the Builder itself. */
export const PROJECTS_EVENT = "agentexpress:projects-changed";

const changed = () => {
  if (typeof window !== "undefined") window.dispatchEvent(new Event(PROJECTS_EVENT));
};

// --- what the server says about a build --------------------------------------------

export type Tool = "cdk" | "terraform";
export type JobStatus = "QUEUED" | "RUNNING" | "SUCCEEDED" | "FAILED";

export interface Job {
  action: "deploy" | "destroy";
  tool: Tool;
  version: number;
  status: JobStatus;
  phase?: string;
  startedAt?: string;
  finishedAt?: string;
  error?: string;
  logsUrl?: string;
  deleteAfter?: boolean;
  by?: string;
  /** "" = this console's account; else the connected account it runs against. */
  account?: string;
  /** The region it deploys to. */
  region?: string;
}
export interface Deployed {
  version: number;
  tool: Tool;
  agentName: string;
  stack?: string;
  runtimeArn?: string;
  uiUrl?: string;
  apiUrl?: string;
  at?: string;
  /** "" = this console's account; else a connected account. */
  account?: string;
  region?: string;
  /** The owner's login on the build's own console (invited on deploy). */
  appUser?: string;
  /** The Cognito user pool of the build's own app (Built-in sign-in). */
  userPoolId?: string;
  /** The framework release that deployed it (orchestrator/VERSION). */
  frameworkVersion?: string;
  /** Tool -> the redirect (callback) URL to register at its provider ("Each person's own account"). */
  callbackUrls?: Record<string, string>;
}

export interface BuildSummary {
  id: string;
  name: string;
  updatedAt: string;
  /** Server only: the fixed AWS name every resource of this build is derived from. */
  agentName?: string;
  /** Versions deployed so far (the next deploy is versions + 1). */
  versions?: number;
  /** The tool this build is (or was partly) deployed with; cleared by a destroy. */
  tool?: Tool;
  /** The account the build is (or was partly) deployed into; "" = this console's. */
  account?: string;
  /** The region it is (or was partly) deployed into. */
  region?: string;
  job?: Job;
  deployed?: Deployed;
  /** Shared with the caller by its owner (who is `ownerEmail`). */
  shared?: boolean;
  ownerEmail?: string;
  shares?: Shares;
  /** Saves so far: a save based on an older one is refused (someone else saved first). */
  rev?: number;
  lastEditor?: string;
  /** When the build was created (ISO 8601). */
  created?: string;
  /** The team whose workspace it is in (bff/teams.py); absent: its creator's own builds. */
  team?: string;
  /** The caller may only look at it: a viewer in its team. */
  readOnly?: boolean;
}

/** Who besides its owner may see and change a build or a library item. */
export interface Shares { emails: string[]; groups: string[]; everyone: boolean }
export const NO_SHARES: Shares = { emails: [], groups: [], everyone: false };

export const jobActive = (b?: { job?: Job }) =>
  b?.job?.status === "QUEUED" || b?.job?.status === "RUNNING";

export interface BuildStore {
  /** True when builds are on the server, which is what makes deploy possible. */
  readonly server: boolean;
  list(): Promise<BuildSummary[]>;
  load(id: string): Promise<{ project: Project; build: BuildSummary; refs?: Record<string, LibraryItem> } | null>;
  /** `rev`: the save this edit is based on; a newer one on the server refuses it (409). */
  save(p: Project, rev?: number): Promise<BuildSummary>;
  /** `destroying` when the build had a stack: it is destroyed first, then deleted. */
  remove(id: string): Promise<{ destroying?: boolean }>;
}

// --- this browser ------------------------------------------------------------------

const KEY = "agentexpress.builder.v1";
const LAST = "agentexpress.builder.last";

interface Local {
  projects: Record<string, Project>;
  last?: string;
}

function read(storage: Storage): Local {
  try {
    const parsed = JSON.parse(storage.getItem(KEY) ?? "") as Local;
    return parsed && typeof parsed.projects === "object" ? parsed : { projects: {} };
  } catch {
    return { projects: {} };
  }
}

function write(storage: Storage, s: Local): void {
  storage.setItem(KEY, JSON.stringify(s));
  changed();
}

export function listProjects(storage: Storage = localStorage): Project[] {
  return Object.values(read(storage).projects)
    .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));
}

export function saveProject(p: Project, storage: Storage = localStorage): Project {
  const s = read(storage);
  const saved = { ...p, updatedAt: new Date().toISOString() };
  s.projects[p.id] = saved;
  s.last = p.id;
  write(storage, s);
  return saved;
}

export function deleteProject(id: string, storage: Storage = localStorage): void {
  const s = read(storage);
  delete s.projects[id];
  if (s.last === id) delete s.last;
  write(storage, s);
}

/** The build last open in this browser — only a pointer, the build itself is elsewhere. */
export function lastOpened(storage: Storage = localStorage): string | null {
  return storage.getItem(LAST);
}

export function setLastOpened(id: string, storage: Storage = localStorage): void {
  storage.setItem(LAST, id);
}

const summaryOf = (p: Project): BuildSummary => ({ id: p.id, name: p.name, updatedAt: p.updatedAt, created: p.createdAt });

/** `storage` is resolved on every call, not once: the global can be replaced (tests do). */
export function localStore(storage?: Storage): BuildStore {
  const st = () => storage ?? localStorage;
  return {
    server: false,
    list: async () => listProjects(st()).map(summaryOf),
    load: async (id) => {
      const p = read(st()).projects[id];
      return p ? { project: p, build: summaryOf(p) } : null;
    },
    save: async (p) => summaryOf(saveProject(p, st())),
    remove: async (id) => { deleteProject(id, st()); return {}; },
  };
}

// --- the console's builds store ----------------------------------------------------

interface ServerBuild extends Omit<BuildSummary, "updatedAt"> {
  updated: string;
  created?: string;
}

const fromServer = (b: ServerBuild): BuildSummary => ({ ...b, updatedAt: b.updated });

/** A build list straight from the server (as the Admin page reads it), with its `updated`
 *  as the page's `updatedAt`. Without this the Admin page's Updated column read a field the
 *  server never sends, and showed "—" for every build. */
export function withUpdatedAt<T extends { updatedAt?: string }>(b: T): T {
  const updated = (b as { updated?: string }).updated;
  return !b.updatedAt && updated ? { ...b, updatedAt: updated } : b;
}

/** ["Created 9 Oct 2026, 11:09 by alice@example.com", "Last edited 10 Oct 2026, 12:46 by
 *  bob@example.com"]: the lines under a build's name, one per bullet. Empty when it has
 *  neither time yet. */
export function editedLines(b: Partial<Pick<BuildSummary, "created" | "updatedAt" | "lastEditor" | "ownerEmail">> | null | undefined,
  fmt: (iso: string) => string = (iso) => new Date(iso).toLocaleString()): string[] {
  if (!b) return [];
  const parts: string[] = [];
  if (b.created) parts.push(`Created ${fmt(b.created)}${b.ownerEmail ? ` by ${b.ownerEmail}` : ""}`);
  if (b.updatedAt) parts.push(`Last edited ${fmt(b.updatedAt)}${b.lastEditor ? ` by ${b.lastEditor}` : ""}`);
  return parts;
}
const enc = encodeURIComponent;

/** The workspace the builds list shows: "" = the caller's own builds, else a team's id
 *  (bff/teams.py). A build created while a team is chosen is created in that team. */
let workspaceId = "";
export const currentWorkspace = () => workspaceId;
export function setWorkspace(id: string) {
  workspaceId = id;
  changed();
}
export const serverStore: BuildStore = {
  server: true,
  list: async () => (await api.get<ServerBuild[]>(`/api/builds${workspaceId ? `?team=${enc(workspaceId)}` : ""}`)).map(fromServer),
  load: async (id) => {
    try {
      const r = await api.get<{ build: ServerBuild; project: Project; refs?: Record<string, LibraryItem> }>(`/api/builds/${enc(id)}`);
      return r.project?.workflow ? { project: r.project, build: fromServer(r.build), refs: r.refs ?? {} } : null;
    } catch {
      return null;
    }
  },
  save: async (p, rev?: number) => {
    // `team` is read only when this save creates the build; an existing one moves by moveBuild.
    const b = fromServer(await api.put<ServerBuild>(`/api/builds/${enc(p.id)}`,
      { project: rev === undefined ? p : { ...p, rev }, ...(workspaceId ? { team: workspaceId } : {}) }));
    changed();
    return b;
  },
  remove: async (id) => {
    const r = await api.del<{ ok?: boolean; destroying?: boolean }>(`/api/builds/${enc(id)}`);
    changed();
    return { destroying: Boolean(r?.destroying) };
  },
};

/** The build's server-side state alone (job, deployed), for polling while a job runs. */
export async function buildStatus(id: string): Promise<BuildSummary> {
  return fromServer((await api.get<{ build: ServerBuild }>(`/api/builds/${enc(id)}`)).build);
}

export async function deploy(id: string, tool: Tool, account = "", region = ""): Promise<Job> {
  const job = await api.post<Job>(`/api/builds/${enc(id)}/deploy`, { tool, account, ...(region ? { region } : {}) });
  changed();
  return job;
}

export async function destroy(id: string): Promise<Job> {
  const job = await api.post<Job>(`/api/builds/${enc(id)}/destroy`);
  changed();
  return job;
}

/** One finished deploy or destroy of a build (bff/builds.py deployments). */
export interface DeploymentEvent {
  at: string;
  action: "deploy" | "destroy";
  result: "succeeded" | "failed";
  version?: number;
  tool?: string;
  /** "console" for this console's account, else the account id. */
  account?: string;
  region?: string;
  by?: string;
  error?: string;
}
export interface DeploymentHistory {
  deployments: DeploymentEvent[];
  running: { action: string; version?: number; tool?: string; by?: string; at?: string; phase?: string } | null;
}
/** Who deployed or destroyed a build, when, and how it ended; newest first. */
export const deploymentHistory = (id: string) =>
  api.get<DeploymentHistory>(`/api/builds/${encodeURIComponent(id)}/deployments`);

export async function jobLog(id: string): Promise<{ lines: string[]; logsUrl?: string }> {
  return api.get(`/api/builds/${enc(id)}/log`);
}

/** Move drafts left in this browser into the server store, once. Returns how many. */
export async function migrateLocalDrafts(storage: Storage = localStorage): Promise<number> {
  const local = listProjects(storage);
  let moved = 0;
  for (const p of local) {
    try {
      await serverStore.save(p);
      deleteProject(p.id, storage);
      moved += 1;
    } catch {
      /* keep it here; the next load tries again */
    }
  }
  return moved;
}

// --- connected AWS accounts (bff/accounts.py) ----------------------------------------

export interface Account {
  accountId: string;
  /** The name the user gave it ("Team sandbox"). */
  label?: string;
  /** The default region: a deploy that names none goes here. */
  region: string;
  status: "pending" | "connected";
  roleArn?: string;
  stackName?: string;
  created?: string;
  verifiedAt?: string;
  updatedAt?: string;
  /** The caller's builds deployed in it, each in the region it was deployed to. */
  builds?: { id: string; name: string; region: string }[];
}
/** "Team sandbox (123456789012)", or "AWS account 123456789012" if it has no name. */
export const accountName = (a: Pick<Account, "accountId" | "label">) =>
  (a.label ? `${a.label} (${a.accountId})` : `AWS account ${a.accountId}`);

export interface AccountLaunch extends Account {
  /** Opens the CloudFormation console in their account, pre-filled. */
  launchUrl: string;
  templateUrl: string;
  cli: string;
  /** Already connected: this updates its role to the console's current permissions. */
  update?: boolean;
}

export const accounts = {
  list: () => api.get<Account[]>("/api/accounts"),
  connect: (accountId: string, region: string, label = "") =>
    api.post<AccountLaunch>("/api/accounts", { accountId, region, label }),
  update: (accountId: string, change: { label?: string; region?: string }) =>
    api.put<Account>(`/api/accounts/${enc(accountId)}`, change),
  launch: (accountId: string) => api.get<AccountLaunch>(`/api/accounts/${enc(accountId)}/launch`),
  verify: (accountId: string) => api.post<Account>(`/api/accounts/${enc(accountId)}/verify`),
  remove: (accountId: string) => api.del(`/api/accounts/${enc(accountId)}`),
};

// --- the policy library (bff/policies.py) ----------------------------------------------
export type PolicySource = "english" | "form" | "cedar";
export interface LibraryPolicy {
  id: string;
  name: string;
  description?: string;
  statement: string;
  source?: PolicySource;
  created?: string;
  updatedAt?: string;
  effect?: string;
  actions?: string[];
}
/** One requirement AgentCore Policy read from the request, and the Cedar it wrote. */
export interface GeneratedPolicy {
  /** The part of the request this answers. */
  fragment: string;
  /** "" when AgentCore could not express it. */
  statement: string;
  /** AgentCore's own analysis: VALID, INVALID, ALLOW_ALL, DENY_NONE... */
  findings: { type: string; description: string }[];
  /** Ours (cedar.ts / bff/cedar.py), against the deployed build's tools. */
  problems: string[];
}
export interface PolicyGeneration {
  status: "GENERATING" | "GENERATED" | "GENERATE_FAILED" | "DELETE_FAILED" | string;
  reasons?: string[];
  assets?: GeneratedPolicy[];
}
// --- the library: items any build uses live, and sharing (bff/library.py, sharing.py) ----

/** "interceptor" is copied into a build, never linked live (bff/library.py COPY_KINDS). */
export type LibraryKind = "tool" | "guardrail" | "memory" | "evaluator" | "identity" | "policy" | "skill" | "interceptor";
export interface LibraryItem {
  id: string;
  kind: LibraryKind;
  name: string;
  description?: string;
  definition: Record<string, unknown>;
  /** A code tool's files. */
  files?: Record<string, string>;
  source?: string;
  ownerEmail?: string;
  shares?: Shares;
  mine?: boolean;
  created?: string;
  updatedAt?: string;
}
export const library = {
  list: (kind?: LibraryKind) => api.get<LibraryItem[]>(`/api/library${kind ? `?kind=${kind}` : ""}`),
  get: (id: string) => api.get<LibraryItem>(`/api/library/${enc(id)}`),
  create: (item: Omit<LibraryItem, "id">) => api.post<LibraryItem>("/api/library", item),
  update: (id: string, patch: Partial<LibraryItem>) => api.put<LibraryItem>(`/api/library/${enc(id)}`, patch),
  /** Builds that used it keep their own copy of it (bff/library.py detach): `keptIn`. */
  remove: (id: string) => api.del<{ ok: boolean; keptIn?: { id: string; name: string }[] }>(`/api/library/${enc(id)}`),
  share: (id: string, shares: Shares) => api.put<Shares>(`/api/library/${enc(id)}/shares`, shares),
};
export const shareBuild = async (id: string, shares: Shares) => {
  const r = await api.put<Shares>(`/api/builds/${enc(id)}/shares`, shares);
  changed();
  return r;
};
/** Put a build in a team's workspace, or ("") back in its creator's own builds. */
export const moveBuild = async (id: string, team: string) => {
  const r = fromServer(await api.put<ServerBuild>(`/api/builds/${enc(id)}/team`, { team }));
  changed();
  return r;
};
export interface Group { name: string; members?: string[]; updatedAt?: string }
export const groups = {
  list: () => api.get<Group[]>("/api/groups"),
  put: (name: string, members: string[]) => api.put<Group>(`/api/groups/${enc(name)}`, { members }),
  remove: (name: string) => api.del(`/api/groups/${enc(name)}`),
};

export const policyLibrary = {
  list: () => api.get<LibraryPolicy[]>("/api/policies"),
  create: (p: Omit<LibraryPolicy, "id">) => api.post<LibraryPolicy>("/api/policies", p),
  update: (id: string, p: Partial<LibraryPolicy>) => api.put<LibraryPolicy>(`/api/policies/${enc(id)}`, p),
  remove: (id: string) => api.del(`/api/policies/${enc(id)}`),
  /** Plain English -> Cedar, written by AgentCore Policy against a DEPLOYED build's engine
   *  and Gateway. Starts it; poll `generation`. Saves nothing. */
  generate: (build: string, text: string) =>
    api.post<{ generationId: string; status: string }>("/api/policies/generate", { build, text }),
  generation: (build: string, id: string) =>
    api.get<PolicyGeneration>(`/api/policies/generate/${enc(id)}?build=${enc(build)}`),
};
// --- a tool written in the build (bff/codecheck.py) ------------------------------------
export interface CodeProblem { severity: "error" | "warning"; file: string; line: number; message: string }
export interface SandboxResult { name: string; ok: boolean; output?: string; error?: string; trace?: string; ms: number }
export interface CodeCheck {
  ok: boolean;
  problems: CodeProblem[];
  sandbox?: { ran: boolean; ms?: number; note: string; results?: SandboxResult[]; output?: string };
}
export interface ToolTest { ok: boolean; status?: number; error: string; output: string; log: string; ms: number }
export const codeTool = {
  /** Static checks, then a run of events.json in an AgentCore Code Interpreter sandbox. */
  check: (key: string, files: Record<string, string>, tool: Record<string, unknown>, run = true) =>
    api.post<CodeCheck>("/api/code/check", { key, files, tool, run }),
  /** Call the deployed function with one event, as the Gateway does. */
  test: (buildId: string, key: string, tool: string, event: Record<string, unknown>) =>
    api.post<ToolTest>(`/api/builds/${enc(buildId)}/test-tool`, { key, tool, event }),
};
// --- a build's secrets and documents ---------------------------------------------------

export type SecretKind = "toolApiKeys" | "a2aTokens" | "identitySecrets";
/** Which secrets are set, by kind — names only; no route ever returns a value. */
export type SecretNames = Record<SecretKind, string[]>;

export const buildSecrets = {
  names: (id: string) => api.get<SecretNames>(`/api/builds/${enc(id)}/secrets`),
  set: (id: string, kind: SecretKind, name: string, value: string) =>
    api.put<SecretNames>(`/api/builds/${enc(id)}/secrets`, { [kind]: { [name]: value } }),
};

// --- AWS Agent Registry (bff/registry.py) --------------------------------------------
export type RegistryKind = "tool" | "agent" | "skill";
export interface Registry { id: string; name: string; description: string; status: string }
/** An approved record, and what the build would hold for it (or why it cannot). */
export interface RegistryHit {
  recordId: string; name: string; displayName: string; description: string;
  type: string; version: string; updatedAt: string;
  kind: RegistryKind | null; key?: string; entry?: Entry; why?: string;
  /** An MCP record's tool names; an agent card's skills. */
  tools?: string[]; skills?: string[];
}
/** A newer approved version of something the build took from a registry. */
export interface RegistryUpdate { map: "tools" | "agents" | "skills"; key: string; sync: boolean; from: string; to: string; entry: Entry }
/** Where a deployed build is published (R2). */
export interface RegistryPublished {
  registryId?: string; registryName?: string; version?: number; at?: string; by?: string;
  records?: Record<string, { recordId: string; name: string; status?: string; statusReason?: string }>;
}
export const registryApi = {
  registries: () => api.get<Registry[]>("/api/registry"),
  search: (registry: string, q: string, kind: RegistryKind) =>
    api.get<RegistryHit[]>(`/api/registry/search?registry=${enc(registry)}&q=${enc(q)}&kind=${kind}`),
  state: (buildId: string) =>
    api.get<{ updates: RegistryUpdate[]; published: RegistryPublished }>(`/api/builds/${enc(buildId)}/registry`),
  publish: (buildId: string, body: { registry: string; what: "build" | "skill"; skill?: string }) =>
    api.post<RegistryPublished>(`/api/builds/${enc(buildId)}/publish`, body),
};

export interface KbDoc {
  corpus: string;
  name: string;
  size: number;
  uploaded: string;
}

export const buildDocs = {
  list: (id: string) => api.get<KbDoc[]>(`/api/builds/${enc(id)}/docs`),
  /** Straight to S3 with a presigned POST; the BFF only signs it. */
  upload: async (id: string, corpus: string, file: File): Promise<void> => {
    const { url, fields } = await api.post<{ url: string; fields: Record<string, string> }>(
      `/api/builds/${enc(id)}/docs`, { corpus, name: file.name });
    const form = new FormData();
    for (const [k, v] of Object.entries(fields)) form.append(k, v);
    form.append("file", file);
    const res = await fetch(url, { method: "POST", body: form });
    if (!res.ok) throw new Error(`upload failed (${res.status}): ${(await res.text()).slice(0, 200)}`);
  },
  remove: (id: string, corpus: string, name: string) =>
    api.del(`/api/builds/${enc(id)}/docs?corpus=${enc(corpus)}&name=${enc(name)}`),
};

// --- AgentExpress Assistant (bff/designer.py) ---------------------------------------------------

export interface DesignChange {
  summary: string;
  revision: number;
  undo?: boolean;
  changed: { agents: string[]; tools: string[]; steps: boolean; blocks: string[]; removed: string[] };
  defaultsUsed?: string[];
  needsInput?: { path: string; question: string }[];
  /** Secrets the designer asked for, entered in a secure field beside the chat. */
  secretsNeeded?: { kind: SecretKind; name: string; label: string; why: string }[];
  problems?: { path: string; message: string }[];
}

export interface DesignTurn {
  id: string;
  role: "user" | "assistant";
  text: string;
  at: string;
  status?: "thinking" | "done" | "error";
  /** While the reply is being written: what the designer is doing right now. */
  phase?: "thinking" | "writing" | "applying" | "checking";
  /** While a change is being written: each edit it has named so far ("Adding agent x"). */
  progress?: string[];
  /** While the designer thinks: the latest part of its reasoning, as it streams. */
  thinking?: string;
  changes?: DesignChange[];
  /** Files the user's message brought: uploads, or objects named by an s3:// path. */
  attachments?: { name: string; source: "upload" | "s3"; uri?: string; size?: number }[];
}

export interface DesignDoc {
  turns: DesignTurn[];
  status: "idle" | "thinking";
  revision: number;
  model: string;
  starters: { title: string; message: string }[];
  /** How many of the earliest turns the designer now reads as a running summary. */
  summarized?: number;
  /** What a message may bring: file limits and the buckets s3:// paths may name. */
  attach?: { maxFiles: number; maxBytes: number; types: string[]; s3: string[] };
}

export const designer = {
  get: (id: string) => api.get<DesignDoc>(`/api/builds/${encodeURIComponent(id)}/design`),
  send: (id: string, message: string, attachments: { key: string; name: string }[] = []) =>
    api.post<DesignDoc>(`/api/builds/${encodeURIComponent(id)}/design`,
      attachments.length ? { message, attachments } : { message }),
  /** Upload one file for the next message, straight to S3 with a presigned POST. */
  attach: async (id: string, file: File): Promise<{ key: string; name: string }> => {
    const { url, fields, key, name } = await api.post<{ url: string; fields: Record<string, string>; key: string; name: string }>(
      `/api/builds/${encodeURIComponent(id)}/design/attachments`, { name: file.name });
    const form = new FormData();
    for (const [k, v] of Object.entries(fields)) form.append(k, v);
    form.append("file", file);
    const res = await fetch(url, { method: "POST", body: form });
    if (!res.ok) throw new Error(`upload failed (${res.status})`);
    return { key, name };
  },
  reset: (id: string) => api.del<DesignDoc>(`/api/builds/${encodeURIComponent(id)}/design`),
};

/** Every change in the conversation, oldest first. */
export function designChanges(doc: DesignDoc | null): DesignChange[] {
  return (doc?.turns ?? []).flatMap((t) => t.changes ?? []);
}

/** The owner's first-sign-in login to a deployed build's own console. */
export interface AppLogin { user: string; password: string; temporary: boolean; at?: string }
export const appLogin = (id: string) => api.get<AppLogin>(`/api/builds/${enc(id)}/login`);
