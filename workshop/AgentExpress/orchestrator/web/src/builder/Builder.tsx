/** The Build view: design a workflow by dragging, configure it in forms generated from
 *  the framework's own key spec, then deploy it and run it — or export it.
 *
 *  What it produces is exactly what a customer would otherwise write by hand — a
 *  workflow.json in canonical form, plus the prompts for each agent. On a console with
 *  the Builder plane, builds autosave to the console's builds store (per user, any
 *  browser) and deploy from here, each as its own stack (DeployPanel.tsx). Without it,
 *  drafts stay in this browser and the bundle is exported for `scaffold.py apply`. */

import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import ButtonDropdown from "@cloudscape-design/components/button-dropdown";
import Container from "@cloudscape-design/components/container";
import CopyToClipboard from "@cloudscape-design/components/copy-to-clipboard";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import Input from "@cloudscape-design/components/input";
import Modal from "@cloudscape-design/components/modal";
import RadioGroup from "@cloudscape-design/components/radio-group";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import StatusIndicator from "@cloudscape-design/components/status-indicator";
import Table from "@cloudscape-design/components/table";
import Tabs from "@cloudscape-design/components/tabs";
import Spinner from "@cloudscape-design/components/spinner";
import {
  useCallback, useEffect, useMemo, useRef, useState, type DragEvent,
  type ReactNode, type SetStateAction,
} from "react";

import type { Action } from "../types";
import { toolsOf } from "../lib/tools";
import { Canvas, DND_MIME, type DragPayload, type Selection } from "./Canvas";
import { DesignChat } from "./DesignChat";
import { BuildPolicies, policyBlock } from "./Policies";
import { record, redo, start, undo, type History } from "./history";
import { EntryForm } from "./EntryForm";
import { formatWorkflow } from "./format";
import { Inspector } from "./Inspector";
import { IssueList } from "./IssueList";
import type { DropTarget } from "./layout";
import { block, vocab } from "./meta";
import {
  addAgent, addTool, bindTool, fromFile, insertStage, joinStage, migrateFramework, newProject,
  refOf, relink, removeTool, replaceWorkflow, resolveProject, attachItem, unbindTool,
  renameProject, setUi, stageOf, toBundle, unplaced, type Entry, type Json, type Project, type Workflow,
} from "./model";
import { LibraryContext, LibraryPicker, NamedTab, ShareDialog, keptNote, publishEntries, sharedLabel, useShareEntries, type LibraryCtx } from "./Library";
import { DeployPanel, TOOL_NAMES } from "./DeployPanel";
import {
  buildStatus, deploy, destroy, jobActive, lastOpened, library, localStore, setLastOpened, shareBuild,
  type BuildStore, type BuildSummary, type LibraryItem, type Tool,
} from "./storage";
import { validate, type Issue } from "./validate";
import { modelIssues, useModels } from "./models";
import "./builder.css";

const TOOL_LABELS: Record<string, string> = {
  kb: "Knowledge Base", mcp: "MCP server", openapi: "REST API (OpenAPI)",
  lambda: "Lambda function", websearch: "Web search", apigateway: "REST API (API Gateway)",
};

/** One way in: a file becomes a new build, or replaces this one (after a confirm). */
function ImportModal({ visible, onDismiss, onPick }: {
  visible: boolean; onDismiss: () => void; onPick: (m: "new" | "replace") => void;
}) {
  const [mode, setMode] = useState<"new" | "replace">("new");
  useEffect(() => { if (visible) setMode("new"); }, [visible]);
  return (
    <Modal visible={visible} onDismiss={onDismiss} header="Import a file"
      footer={<Box float="right"><SpaceBetween direction="horizontal" size="xs">
        <Button variant="link" onClick={onDismiss}>Cancel</Button>
        <Button variant="primary" onClick={() => {
          if (mode === "replace" && !window.confirm("Replace everything in this build with the file? Undo takes it back.")) return;
          onPick(mode);
        }}>Choose file…</Button>
      </SpaceBetween></Box>}>
      <SpaceBetween size="m">
        <Box>A workflow.json, or a bundle exported from here (its prompts and code come with it).</Box>
        <RadioGroup value={mode} onChange={({ detail }) => setMode(detail.value as "new" | "replace")} items={[
          { value: "new", label: "As a new build", description: "This build stays as it is." },
          { value: "replace", label: "Into this build, replacing it", description: "Same build, same deploys; Undo takes it back." },
        ]} />
      </SpaceBetween>
    </Modal>
  );
}

function download(name: string, text: string) {
  const url = URL.createObjectURL(new Blob([text], { type: "application/json" }));
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

const slug = (s: string) => s.trim().toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "") || "workflow";

function place(wf: Workflow, id: string, t: DropTarget): Workflow {
  if (t.kind === "gap") return insertStage(wf, t.index, id);
  const index = t.kind === "stage" ? t.index : stageOf(wf, t.id);
  if (index < 0) return insertStage(wf, wf.steps.length, id);
  if (stageOf(wf, id) === index) return wf;
  return joinStage(wf, index, id);
}

function PaletteItem({ payload, label, sub, onActivate }: {
  payload: DragPayload; label: string; sub?: string; onActivate: () => void;
}) {
  return (
    <div
      className="axb-palette-item" draggable role="button" tabIndex={0}
      onDragStart={(e: DragEvent) => {
        e.dataTransfer.setData(DND_MIME, JSON.stringify(payload));
        e.dataTransfer.effectAllowed = "move";
      }}
      onClick={onActivate}
      onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onActivate(); } }}
      title={payload.kind === "tool" ? "Drag onto an agent to give it this tool" : "Drag onto the canvas, or press Enter to add it as a new stage"}
    >
      <span className="axb-palette-label">{label}</span>
      {sub ? <span className="axb-palette-sub">{sub}</span> : null}
    </div>
  );
}

function AddToolModal({ visible, onDismiss, onAdd }: {
  visible: boolean; onDismiss: () => void; onAdd: (name: string, type: string) => void;
}) {
  const [name, setName] = useState("");
  const [type, setType] = useState("mcp");
  const types = vocab("toolTypes");
  const typeDoc = block("tool").keys.type.doc;
  return (
    <Modal visible={visible} onDismiss={onDismiss} header="Add a tool"
      footer={
        <Box float="right">
          <SpaceBetween direction="horizontal" size="xs">
            <Button variant="link" onClick={onDismiss}>Cancel</Button>
            <Button variant="primary" disabled={!name.trim()}
              onClick={() => { onAdd(name.trim(), type); setName(""); }}>Add</Button>
          </SpaceBetween>
        </Box>
      }>
      <SpaceBetween size="m">
        <FormField label="Name" description="What it is, e.g. “Claims database”. The tool key is derived from it.">
          <Input value={name} onChange={({ detail }) => setName(detail.value)} autoFocus />
        </FormField>
        <FormField label="Type" description={typeDoc}>
          <Select
            selectedOption={{ label: TOOL_LABELS[type] ?? type, value: type }}
            options={types.map((t) => ({ label: TOOL_LABELS[t] ?? t, value: t, description: t }))}
            onChange={({ detail }) => setType(detail.selectedOption.value!)}
          />
        </FormField>
      </SpaceBetween>
    </Modal>
  );
}

function ExportedModal({ name, onDismiss }: { name: string | null; onDismiss: () => void }) {
  return (
    <Modal visible={name !== null} onDismiss={onDismiss} header="Bundle downloaded"
      footer={<Box float="right"><Button variant="primary" onClick={onDismiss}>Done</Button></Box>}>
      <SpaceBetween size="m">
        <Box>
          <b>{name}</b> holds this build: its workflow.json, the prompt you wrote for each agent, and each
          agent&apos;s framework. It is yours to keep.
        </Box>
        <Box>
          To run it, use <b>Deploy</b> on this page — into this console&apos;s account or one of your own.
          Building from the bundle outside this console needs the AgentExpress framework, which is shared on
          request: ask the team that runs this console.
        </Box>
      </SpaceBetween>
    </Modal>
  );
}

/** The build's name, renamable in place: the pencil (or a double-click) turns it into a
 *  field; Enter or leaving the field saves, Escape cancels. Any non-blank name is fine —
 *  it names the draft and the exported file, and nothing in the framework reads it. */
function ProjectTitle({ name, onRename }: { name: string; onRename: (n: string) => void }) {
  const [editing, setEditing] = useState(false);
  const [text, setText] = useState(name);
  useEffect(() => { if (!editing) setText(name); }, [name, editing]);
  const commit = () => {
    const t = text.trim();
    if (t && t !== name) onRename(t);
    setEditing(false);
  };
  if (editing) {
    return (
      <span className="axb-rename">
        <Input value={text} autoFocus ariaLabel="Build name"
          onChange={({ detail }) => setText(detail.value)} onBlur={commit}
          onKeyDown={({ detail }) => {
            if (detail.key === "Enter") commit();
            if (detail.key === "Escape") { setText(name); setEditing(false); }
          }} />
      </span>
    );
  }
  return (
    <span className="axb-title" onDoubleClick={() => setEditing(true)}>
      {name}{" "}
      <Button variant="icon" iconName="edit" ariaLabel="Rename this build" onClick={() => setEditing(true)} />
    </span>
  );
}

/** Which build the side navigation asked for: a saved draft's id, or "new". `nonce`
 *  makes clicking the same entry twice a fresh request. */
export interface BuildRequest {
  id: string;
  nonce: number;
}

/** "My workflow", then "My workflow 2", … — so every build is distinguishable in the
 *  side navigation, which lists them by name. */
function freshProject(names: string[]): Project {
  const taken = new Set(names);
  let name = "My workflow";
  for (let i = 2; taken.has(name); i++) name = `My workflow ${i}`;
  return newProject(name);
}

/** How long typing pauses before the build is saved. */
export const AUTOSAVE_MS = 800;

const defaultStore = localStore();

export function Builder({ notify, request, onCurrent, store = defaultStore, can = () => true, onRun }: {
  notify: (type: "success" | "error" | "info", msg: string) => void;
  request?: BuildRequest | null;
  /** Told which build is open, so the navigation can highlight it. */
  onCurrent?: (id: string) => void;
  /** Where builds live: the console's builds store, or this browser. */
  store?: BuildStore;
  can?: (a: Action) => boolean;
  /** Start a run of this (deployed) build. */
  onRun?: (buildId: string) => void;
}) {
  // The project and its undo history, as one value: every change goes through `record`.
  const [hist, setHist] = useState<History<Project> | null>(null);
  const project = hist?.present ?? null;
  const latest = useRef<Project | null>(null);
  latest.current = project;
  /** A change. `step` makes it its own undo step (an upload, a Design-with-AI change)
   *  rather than merging with the edits just before it. */
  const setProject = useCallback((u: SetStateAction<Project>, step = false) => {
    setHist((h) => (h ? record(h, typeof u === "function" ? (u as (p: Project) => Project)(h.present) : u,
      Date.now(), { step }) : h));
  }, []);
  const [build, setBuild] = useState<BuildSummary | null>(null);
  /** The library items this build uses live, as they are now (GET /api/builds/{id} refs). */
  const [refs, setRefs] = useState<Record<string, LibraryItem>>({});
  const refsNow = useRef(refs);
  refsNow.current = refs;
  const addRefs = useCallback((items: LibraryItem[]) => {
    refsNow.current = { ...refsNow.current, ...Object.fromEntries(items.map((i) => [i.id, i])) };
    setRefs((r) => ({ ...r, ...Object.fromEntries(items.map((i) => [i.id, i])) }));
    setLib((l) => [...l.filter((x) => !items.some((i) => i.id === x.id)), ...items]);
  }, []);
  /** The save this draft is based on: a newer one on the server (a co-builder's) wins. */
  const rev = useRef<number | undefined>(undefined);
  /** Every library item the caller can use, for the pickers. */
  const [lib, setLib] = useState<LibraryItem[]>([]);
  useEffect(() => {
    if (!store.server) return;
    library.list().then(setLib, () => setLib([]));
  }, [store.server]);
  const [builds, setBuilds] = useState<BuildSummary[]>([]);
  const [savedAt, setSavedAt] = useState<string | null>(null);
  const [saveError, setSaveError] = useState<string | null>(null);
  /** The JSON last written, so opening a build does not re-save it unchanged. */
  const lastSaved = useRef("");

  const open = useCallback(async (req?: BuildRequest | null) => {
    const list = await store.list().catch(() => [] as BuildSummary[]);
    setBuilds(list);
    const fresh = () => {
      lastSaved.current = "";
      setHist(start(freshProject(list.map((b) => b.name))));
      setBuild(null);
    };
    if (req?.id === "new") return fresh();
    const id = req?.id ?? (list.some((b) => b.id === lastOpened()) ? lastOpened() : list[0]?.id);
    const got = id ? await store.load(id) : null;
    if (!got) return fresh();
    lastSaved.current = JSON.stringify(got.project);
    setRefs(got.refs ?? {});
    rev.current = got.build.rev;
    setHist(start(migrateFramework(got.project)));
    setBuild(got.build);
  }, [store]);

  // The first open, and a build picked in the side navigation while this view is open.
  const handled = useRef<number | null | undefined>(null);
  useEffect(() => {
    const nonce = request?.nonce;
    if (handled.current !== null && nonce === handled.current) return;
    handled.current = nonce;
    void open(request);
  }, [request, open]);

  useEffect(() => {
    if (!project) return;
    onCurrent?.(project.id);
    setLastOpened(project.id);
  }, [project?.id, onCurrent]); // eslint-disable-line react-hooks/exhaustive-deps

  // Autosave. An invalid draft is saved like any other: unfinished is normal.
  const save = useCallback(async (p: Project) => {
    const body = JSON.stringify(p);
    const b = await store.save(p, store.server ? rev.current : undefined);
    rev.current = b.rev;
    lastSaved.current = body;
    setBuild((cur) => ({ ...(cur ?? {}), ...b }));
    setBuilds((bs) => [b, ...bs.filter((x) => x.id !== b.id)]);
    setSavedAt(new Date().toLocaleTimeString());
    setSaveError(null);
  }, [store]);

  /** Save now if an edit is still waiting for autosave — before the designer reads the
   *  build, so it sees what is on screen. */
  const flush = useCallback(async () => {
    const p = latest.current;
    if (p && JSON.stringify(p) !== lastSaved.current) await save(p);
  }, [save]);

  /** AgentExpress Assistant changed the build on the server: take that version, as one undo step. */
  const reload = useCallback(async () => {
    const id = latest.current?.id;
    if (!id) return;
    const got = await store.load(id);
    if (!got || got.project.id !== latest.current?.id) return;
    lastSaved.current = JSON.stringify(got.project);
    if (got.refs) setRefs(got.refs);
    rev.current = got.build.rev;
    setProject(got.project, true);
    setBuild((cur) => ({ ...(cur ?? {}), ...got.build }));
  }, [store, setProject]);

  /** What the page shows and edits: live items resolved. An edit is written back to the
   *  stored project with them still live (model.ts relink). */
  const view = useMemo(() => (project ? resolveProject(project, refs) : null), [project, refs]);
  /** Use a library item live, then make the edit that names it (model.ts attachItem). */
  const withItem = useCallback<NonNullable<LibraryCtx["withItem"]>>((map, item, edit) => {
    refsNow.current = { ...refsNow.current, [item.id]: item };
    setRefs((r) => ({ ...r, [item.id]: item }));
    setProject((raw) => {
      const used = attachItem(raw, map, item);
      return relink(used.project, edit(resolveProject(used.project, refsNow.current), used.key), refsNow.current);
    }, true);
  }, [setProject]);
  const libCtx = useMemo<LibraryCtx>(() => ({
    lib, withItem,
    liveId: (map, key) => refOf(((project?.workflow[map] ?? {}) as Record<string, unknown>)[key]),
  }), [lib, withItem, project]);
  const setView = useCallback((u: SetStateAction<Project>, step = false) => {
    setProject((raw) => {
      const r = refsNow.current;
      const next = typeof u === "function" ? (u as (p: Project) => Project)(resolveProject(raw, r)) : u;
      return relink(raw, next, r);
    }, step);
  }, [setProject]);

  useEffect(() => {
    if (!project || JSON.stringify(project) === lastSaved.current) return;
    const t = setTimeout(() => {
      save(project).catch((e: Error) => setSaveError(e.message));
    }, AUTOSAVE_MS);
    return () => clearTimeout(t);
  }, [project, save]);

  // While a deploy or destroy runs, follow it — and say when it ends.
  const running = jobActive(build ?? undefined);
  useEffect(() => {
    if (!store.server || !project || !running) return;
    const id = project.id;
    const t = setInterval(() => {
      void buildStatus(id).then((b) => {
        setBuild(b);
        if (jobActive(b) || !b.job) return;
        const what = b.job.action === "destroy" ? "Destroy" : `Deploy of version ${b.job.version}`;
        if (b.job.status === "SUCCEEDED") notify("success", `${what} finished (${TOOL_NAMES[b.job.tool]}).`);
        else notify("error", `${what} failed. The error and the log are in the Deployment panel.`);
      }).catch((e: unknown) => {
        // 404: a destroy-then-delete finished, so the build is gone. Anything else is a
        // missed poll, retried on the next tick.
        if ((e as { status?: number }).status !== 404) return;
        notify("success", "The build was destroyed and deleted.");
        void open(null);   // the next build, or a fresh one if that was the last
      });
    }, 5000);
    return () => clearInterval(t);
  }, [store.server, project?.id, running, notify, open]); // eslint-disable-line react-hooks/exhaustive-deps

  // Deploy is gated on the same validation the Problems tab lists.
  const errorCount = useMemo(() => (view
    ? validate(view.workflow).filter((i) => i.severity === "error").length : 0), [view?.workflow]); // eslint-disable-line react-hooks/exhaustive-deps

  if (!project || !view) return <Spinner size="large" />;

  const onDeploy = async (tool: Tool, account: string, region = "") => {
    try {
      await save(project);        // deploy what is on screen, not the last autosave
      const job = await deploy(project.id, tool, account, region);
      setBuild((b) => (b ? { ...b, tool, account, region: job.region ?? region, job } : b));
      notify("info", `Deploying version ${job.version} with ${TOOL_NAMES[tool]}. This takes 15–30 minutes.`);
    } catch (e) {
      notify("error", `Could not deploy: ${(e as Error).message}`);
    }
  };
  const onDestroy = async () => {
    try {
      const job = await destroy(project.id);
      setBuild((b) => (b ? { ...b, job } : b));
      notify("info", `Destroying with ${TOOL_NAMES[job.tool]}.`);
    } catch (e) {
      notify("error", `Could not destroy: ${(e as Error).message}`);
    }
  };
  const onDelete = async () => {
    const deployed = store.server && build?.tool;
    const msg = deployed
      ? `Delete “${project.name}”? Everything it deployed is destroyed first (with ${TOOL_NAMES[build!.tool!]}), then the build and every stored version are deleted. This cannot be undone.`
      : `Delete “${project.name}”? This cannot be undone.`;
    if (!window.confirm(msg)) return;
    try {
      const r = await store.remove(project.id);
      if (r.destroying) {
        notify("info", `Destroying “${project.name}”; it is deleted when the destroy finishes.`);
        setBuild(await buildStatus(project.id));
        return;
      }
      notify("success", `Build “${project.name}” deleted.`);
      const rest = builds.filter((b) => b.id !== project.id);
      await open(rest[0] ? { id: rest[0].id, nonce: Date.now() } : { id: "new", nonce: Date.now() });
    } catch (e) {
      notify("error", `Could not delete: ${(e as Error).message}`);
    }
  };

  return (
    <LibraryContext.Provider value={libCtx}>
    <Editor
      project={view} setProject={setView} raw={project} setRaw={setProject} refs={refs} addRefs={addRefs}
      build={build} onShared={(shares) => setBuild((b) => (b ? { ...b, shares } : b))}
      onReload={() => void open({ id: project.id, nonce: Date.now() })} notify={notify}
      onUndo={hist?.past.length ? () => setHist((h) => (h ? undo(h) : h)) : undefined}
      onRedo={hist?.future.length ? () => setHist((h) => (h ? redo(h) : h)) : undefined}
      onApplied={reload} flush={flush}
      builds={builds} server={store.server} savedAt={savedAt} saveError={saveError}
      onNew={() => void open({ id: "new", nonce: Date.now() })}
      onOpen={(id) => void open({ id, nonce: Date.now() })}
      onDelete={() => void onDelete()}
      deployed={Boolean(build?.deployed?.version)}
      deployment={store.server ? (
        <DeployPanel build={build} errors={errorCount} can={can}
          onDeploy={onDeploy} onDestroy={onDestroy} onRun={onRun ? () => onRun(project.id) : undefined} />
      ) : null}
    />
    </LibraryContext.Provider>
  );
}

function Editor({
  project, setProject, notify, server, savedAt, saveError, onNew, onDelete, deployment, deployed = false,
  onUndo, onRedo, onApplied, flush, raw, setRaw, refs, addRefs, build, onShared, onReload,
}: {
  /** Resolved: what the page shows. */
  project: Project;
  /** Stored: live library items as {"library": id}. */
  raw: Project;
  setRaw: (u: SetStateAction<Project>, step?: boolean) => void;
  refs: Record<string, LibraryItem>;
  addRefs: (items: LibraryItem[]) => void;
  build: BuildSummary | null;
  onShared: (s: NonNullable<BuildSummary["shares"]>) => void;
  onReload: () => void;
  setProject: (u: SetStateAction<Project>, step?: boolean) => void;
  /** Undefined when there is nothing to undo (or redo). */
  onUndo?: () => void;
  onRedo?: () => void;
  onApplied: () => Promise<void> | void;
  flush: () => Promise<void>;
  notify: (type: "success" | "error" | "info", msg: string) => void;
  builds: BuildSummary[];
  server: boolean;
  savedAt: string | null;
  saveError: string | null;
  onNew: () => void;
  onOpen: (id: string) => void;
  onDelete: () => void;
  deployment: ReactNode;
  /** The build is deployed (plain-English policies need its engine and Gateway). */
  deployed?: boolean;
}) {
  const [selection, setSelection] = useState<Selection>(null);
  const [tab, setTab] = useState("design");
  // AgentExpress Assistant needs the console (the conversation runs on its BFF).
  const [mode, setMode] = useState(server ? "ai" : "manual");
  /** What the file picker does with the file: put it into this build, or open it as a new one. */
  const fileMode = useRef<"replace" | "new">("replace");
  const [addingTool, setAddingTool] = useState(false);
  const [exported, setExported] = useState<string | null>(null);
  const [importing, setImporting] = useState(false);
  const [showProblems, setShowProblems] = useState(false);
  const [sharing, setSharing] = useState(false);
  const [pickingTools, setPickingTools] = useState(false);
  const [toolsSelected, setToolsSelected] = useState<{ key: string }[]>([]);
  const ownTool = (k: string) => k in raw.workflow.tools && !refOf(raw.workflow.tools[k]);
  const toolsToPublish = toolsSelected.map((r) => r.key).filter(ownTool);
  /** Take an item out of the library. The server gives every build that used it its own
   *  copy (bff/library.py detach), this one included: save what is on screen first, then
   *  take the server's copy. */
  const unpublish = async (id: string, name: string) => {
    if (!window.confirm(`Unpublish ${name}? It is deleted from the library; this build, and every other build that uses it, keeps its own copy.`)) return;
    try {
      await flush();
      const r = await library.remove(id);
      await onApplied();
      notify("success", `Unpublished ${name}.${keptNote(r?.keptIn)}`);
    } catch (e) {
      notify("error", (e as Error).message);
    }
  };
  const publishTools = async (keys: string[]) => {
    const r = await publishEntries("tool", keys, raw, project, notify, addRefs);
    if (r) { setRaw(r.project); setToolsSelected([]); }
  };
  const toolSharer = useShareEntries("tool", raw, project, (p) => setRaw(p), refs, addRefs, notify);
  const fileInput = useRef<HTMLInputElement>(null);
  useEffect(() => { setSelection(null); }, [project.id]);

  const models = useModels();
  const issues = useMemo(() => [...validate(project.workflow), ...modelIssues(project.workflow, models.models)],
    [project.workflow, models.models]);
  const errors = issues.filter((i) => i.severity === "error").length;
  const warnings = issues.length - errors;

  const update = useCallback((p: Project) => setProject(p), [setProject]);

  const onDropPayload = useCallback((p: DragPayload, t: DropTarget) => {
    setProject((cur) => {
      if (p.kind === "tool") {
        if (t.kind !== "agent") {
          notify("info", "Drop a tool onto an agent to give it that tool.");
          return cur;
        }
        if (cur.workflow.agents[t.id]?.runtime === "a2a") {
          notify("info", "A remote (A2A) agent reaches its own sources, so it takes no tool.");
          return cur;
        }
        setSelection({ kind: "agent", id: t.id });
        return bindTool(cur, t.id, p.key);
      }
      if (p.kind === "new-agent") {
        const r = addAgent(cur, "New Agent");
        setSelection({ kind: "agent", id: r.id });
        return { ...r.project, workflow: place(r.project.workflow, r.id, t) };
      }
      setSelection({ kind: "agent", id: p.id });
      return { ...cur, workflow: place(cur.workflow, p.id, t) };
    });
  }, [notify, setProject]);

  const onMoveAgent = useCallback((id: string, t: DropTarget) => {
    setProject((cur) => ({ ...cur, workflow: place(cur.workflow, id, t) }));
    setSelection({ kind: "agent", id });
  }, [setProject]);

  const addNewAgentAtEnd = () => onDropPayload({ kind: "new-agent" }, { kind: "gap", index: project.workflow.steps.length });

  const openFile = async (file: File) => {
    try {
      const text = await file.text();
      const p = fileMode.current === "new"
        ? fromFile(text, file.name.replace(/\.(agentexpress\.)?json$/i, ""))
        : replaceWorkflow(project, text);
      setProject(p, true);
      setSelection(null);
      const what = `${Object.keys(p.workflow.agents).length} agents, ${p.workflow.steps.length} stages`;
      notify("success", fileMode.current === "new"
        ? `Opened ${file.name} as a new build: ${what}.`
        : `Loaded ${file.name} into this build: ${what}. Undo takes it back.`);
    } catch (e) {
      notify("error", `Could not open ${file.name}: ${(e as Error).message}`);
    }
  };
  const pickFile = (m: "replace" | "new") => { fileMode.current = m; fileInput.current?.click(); };

  const json = useMemo(() => formatWorkflow(project.workflow as never), [project.workflow]);
  const wf = project.workflow;
  const free = unplaced(wf);

  // Settings holds only what no other tab edits: the app's look, runtime settings and who
  // may do what. The policy engine is on the Policies tab, the guardrail on Guardrails.
  const singleBlocks = (["ui", "orchestrator", "authorization"] as const);
  const pickProblem = (w: Issue["where"]) => {
    setShowProblems(false);
    setMode("manual");
    if (w.kind === "block") {
      const byMap: Record<string, string> = { guardrails: "guardrails", guardrail: "guardrails", memories: "memory",
        evaluators: "evals", identities: "identity", policies: "policies" };
      setTab(byMap[w.name] ?? "settings");
      return;
    }
    setTab("design");
    if (w.kind === "agent") setSelection({ kind: "agent", id: w.id });
    if (w.kind === "tool") setSelection({ kind: "tool", id: w.id });
    if (w.kind === "step") setSelection({ kind: "stage", index: w.index });
  };
  const named = (kind: "guardrail" | "memory" | "evaluator" | "identity" | "policy", extra?: ReactNode) => (
    <NamedTab kind={kind} project={raw} view={project} setProject={(p) => setRaw(p)} refs={refs} addRefs={addRefs}
      issues={issues} server={server} notify={notify} extra={extra}
      onUnpublish={server ? unpublish : undefined} />
  );
  const count = (m: string) => Object.keys((project.workflow[m] ?? {}) as object).length;

  // The manual editor: the canvas, the tools, the settings, the file and its problems.
  const manual = (
      <Tabs
        activeTabId={tab} onChange={({ detail }) => setTab(detail.activeTabId)}
        tabs={[
          {
            id: "design", label: "Design",
            content: (
              <div className="axb-layout">
                <div className="axb-palette">
                  <Box variant="h3">Agents</Box>
                  <PaletteItem payload={{ kind: "new-agent" }} label="+ New agent" sub="runs in this deployment" onActivate={addNewAgentAtEnd} />
                  {free.map((id) => (
                    <PaletteItem key={id} payload={{ kind: "agent", id }} label={String(wf.agents[id].name ?? id)} sub="not on the canvas"
                      onActivate={() => onDropPayload({ kind: "agent", id }, { kind: "gap", index: wf.steps.length })} />
                  ))}
                  <Box variant="h3" padding={{ top: "m" }}>Tools</Box>
                  {Object.entries(wf.tools).map(([key, t]) => (
                    <PaletteItem key={key} payload={{ kind: "tool", key }} label={key} sub={TOOL_LABELS[String(t.type)] ?? String(t.type)}
                      onActivate={() => setSelection({ kind: "tool", id: key })} />
                  ))}
                  <Button variant="inline-link" iconName="add-plus" onClick={() => setAddingTool(true)}>Add a tool</Button>
                  <Box variant="small" color="text-body-secondary" padding={{ top: "s" }}>
                    Drag a tool onto an agent to add it; an agent can read several. Drop an agent on a stage to run it alongside, or
                    between stages to add a new one.
                  </Box>
                </div>
                <Canvas workflow={wf} issues={issues} selection={selection} onSelect={setSelection}
                  onDropPayload={onDropPayload} onMoveAgent={onMoveAgent}
                  onBind={(a, t) => setProject((cur) => bindTool(cur, a, t))}
                  onUnbind={(a, t) => setProject((cur) => unbindTool(cur, a, t))} />
                <div className="axb-inspector">
                  <Inspector project={project} selection={selection} issues={issues} onChange={update} onSelect={setSelection} server={server} />
                </div>
              </div>
            ),
          },
          {
            id: "tools", label: `Tools (${Object.keys(wf.tools).length})`,
            content: (
              <Table
                header={<Header counter={`(${Object.keys(wf.tools).length})`}
                  description="Your data sources. An agent can use several; anything not declared here is refused by Cedar's default-deny."
                  actions={<SpaceBetween direction="horizontal" size="xs">
                    {server ? <Button disabled={!toolsToPublish.length} onClick={() => void publishTools(toolsToPublish)}>
                      Publish to library{toolsToPublish.length ? ` (${toolsToPublish.length})` : ""}</Button> : null}
                    {server ? <Button disabled={!toolsSelected.length} onClick={() => void toolSharer.share(toolsSelected.map((r) => r.key))}>
                      Share{toolsSelected.length ? ` (${toolsSelected.length})` : ""}</Button> : null}
                    {server ? <Button onClick={() => setPickingTools(true)}>Add from library</Button> : null}
                    <Button onClick={() => setAddingTool(true)}>Add a tool</Button>
                  </SpaceBetween>}>Tools</Header>}
                items={Object.entries(wf.tools).map(([key, t]) => ({ key, t }))} trackBy="key"
                wrapLines
                {...(server ? {
                  selectionType: "multi" as const, selectedItems: toolsSelected as { key: string; t: Entry }[],
                  onSelectionChange: ({ detail }: { detail: { selectedItems: { key: string }[] } }) => setToolsSelected(detail.selectedItems),
                  ariaLabels: { selectionGroupLabel: "Select to publish or share",
                    itemSelectionLabel: (_: unknown, r: { key: string }) => `Select ${r.key}`, allItemsSelectionLabel: () => "Select all" },
                } : {})}
                columnDefinitions={[
                  { id: "key", header: "Key", isRowHeader: true,
                    cell: (r) => <span className="axb-nowrap"><Button variant="inline-link" onClick={() => { setSelection({ kind: "tool", id: r.key }); setTab("design"); }}>{r.key}</Button></span> },
                  { id: "type", header: "Type", cell: (r) => TOOL_LABELS[String(r.t.type)] ?? String(r.t.type) },
                  { id: "from", header: "From", cell: (r) => {
                    const id = refOf(raw.workflow.tools[r.key]);
                    return id ? (refs[id] ? `Library · ${refs[id].mine === false ? refs[id].ownerEmail : "yours"}` : "Missing from the library") : "This build";
                  } },
                  // Two lines at most, the rest on hover, so a long description cannot squeeze
                  // the key or push the other columns off-screen.
                  { id: "desc", header: "Description",
                    cell: (r) => <span className="axb-clamp" title={String(r.t.description ?? "")}>{String(r.t.description ?? "") || "—"}</span> },
                  { id: "users", header: "Used by", cell: (r) => Object.entries(wf.agents).filter(([, a]) => toolsOf(a.tool).includes(r.key)).map(([k]) => k).join(", ") || "—" },
                  { id: "issues", header: "Problems", cell: (r) => {
                    const n = issues.filter((i) => i.where.kind === "tool" && i.where.id === r.key && i.severity === "error").length;
                    return n ? <StatusIndicator type="error">{n}</StatusIndicator> : <StatusIndicator type="success">OK</StatusIndicator>;
                  } },
                  { id: "actions", header: "Actions", cell: (r: { key: string }) => {
                    const id = refOf(raw.workflow.tools[r.key]);
                    // Edit and delete on every row: the key link alone was easy to miss,
                    // and deleting was only on the Design tab.
                    const edit = <Button variant="inline-link" ariaLabel={`Edit ${r.key}`}
                      onClick={() => { setSelection({ kind: "tool", id: r.key }); setTab("design"); }}>Edit</Button>;
                    const del = <Button variant="inline-link" ariaLabel={`Delete ${r.key}`} onClick={() => {
                      const users = Object.entries(wf.agents).filter(([, a]) => toolsOf(a.tool).includes(r.key)).map(([k]) => k);
                      if (!window.confirm(`Delete ${r.key} from this build?${users.length ? ` ${users.join(", ")} will no longer use it.` : ""}`)) return;
                      setProject((p) => removeTool(p, r.key));
                      if (selection?.kind === "tool" && selection.id === r.key) setSelection(null);
                    }}>Delete</Button>;
                    return <span className="axb-nowrap"><SpaceBetween direction="horizontal" size="xs">
                      {edit}
                      {server && ownTool(r.key) ? <Button variant="inline-link" ariaLabel={`Publish ${r.key} to the library`} onClick={() => void publishTools([r.key])}>Publish</Button> : null}
                      {server && id && refs[id] ? <>
                        <Button variant="inline-link" onClick={() => setRaw({ ...raw, workflow: { ...raw.workflow, tools: { ...raw.workflow.tools, [r.key]: structuredClone(wf.tools[r.key]) } },
                          ...(project.toolCode?.[r.key] ? { toolCode: { ...(raw.toolCode ?? {}), [r.key]: structuredClone(project.toolCode[r.key]) } } : {}) })}>Make a copy for this build</Button>
                        <Button variant="inline-link" ariaLabel={`Unpublish ${r.key} from the library`} onClick={() => void unpublish(id, refs[id].name)}>Unpublish</Button>
                      </> : null}
                      {del}
                    </SpaceBetween></span>;
                  } },
                ]}
                empty={<Box textAlign="center" color="inherit">No tools yet. An agent with no tool works from the request and the approved output of earlier stages.</Box>}
              />
            ),
          },
          {
            id: "policies", label: `Policies (${count("policies") + (policyBlock(wf).custom?.length ?? 0)})`,
            content: <BuildPolicies project={project} setProject={setProject} issues={issues} server={server} notify={notify}
              deployed={deployed} list={named("policy")} />,
          },
          { id: "guardrails", label: `Guardrails (${count("guardrails")})`, content: named("guardrail", (
            <Container header={<Header variant="h2" description="Applied to an agent that turns guardrails on without naming one below.">
              Deployment guardrail</Header>}>
              <EntryForm name="guardrail" entry={(wf.guardrail ?? {}) as Entry} issues={issues} pathPrefix="guardrail"
                onChange={(e) => setProject({ ...project, workflow: { ...wf, guardrail: e } })} />
            </Container>
          )) },
          { id: "memory", label: `Memory (${count("memories")})`, content: named("memory") },
          { id: "evals", label: `Evals (${count("evaluators")})`, content: named("evaluator") },
          { id: "identity", label: `Identity (${count("identities")})`, content: named("identity") },
          {
            id: "settings", label: "Settings",
            content: (
              <SpaceBetween size="l">
                {singleBlocks.map((name) => (
                  <Container key={name} header={<Header variant="h2" description={block(name).$comment?.split(". ")[0]}>{name}</Header>}>
                    <EntryForm name={name} entry={(wf[name] ?? {}) as Entry} issues={issues} pathPrefix={name}
                      overrides={name === "orchestrator" ? { policy: { hidden: true } } : undefined}
                      onChange={(e) => setProject(name === "ui" ? setUi(project, e as Record<string, Json>)
                        : { ...project, workflow: { ...wf, [name]: e } })} />
                  </Container>
                ))}
              </SpaceBetween>
            ),
          },
          {
            id: "json", label: "workflow.json",
            content: (
              <Container header={<Header variant="h2" description="Exactly what the export writes — already in the canonical order `format_workflow.py --check` expects."
                actions={<CopyToClipboard textToCopy={json} copyButtonText="Copy" copySuccessText="Copied" copyErrorText="Could not copy" />}>
                workflow.json</Header>}>
                <pre className="axb-code axb-json">{json}</pre>
              </Container>
            ),
          },
        ]}
      />
  );

  return (
    <SpaceBetween size="l">
      <Header
        variant="h1"
        description={<>Drag agents onto the canvas, give them tools, gate the stages a human must sign off.
          Every setting comes from the framework&apos;s own key spec.</>}
        info={<Button variant="inline-link" ariaLabel="Show the problems" onClick={() => setShowProblems(true)}>
          <StatusIndicator type={errors ? "error" : warnings ? "warning" : "success"}>
            {errors ? `${errors} error${errors > 1 ? "s" : ""}` : "Valid"}{warnings ? ` · ${warnings} warning${warnings > 1 ? "s" : ""}` : ""}
          </StatusIndicator>
        </Button>}
        actions={
          <SpaceBetween direction="horizontal" size="xs">
            <Button iconName="undo" ariaLabel="Undo" disabled={!onUndo} onClick={() => onUndo?.()}>Undo</Button>
            <Button iconName="redo" ariaLabel="Redo" disabled={!onRedo} onClick={() => onRedo?.()}>Redo</Button>
            {server && build?.id === raw.id ? (
              <Button iconName="share" onClick={() => setSharing(true)}>
                {build?.shared ? `Shared by ${build.ownerEmail ?? "its owner"}` : `Share · ${sharedLabel(build?.shares)}`}
              </Button>
            ) : null}
            <ButtonDropdown
              items={[
                { id: "new", text: "New build" },
                { id: "import", text: "Import a file…", description: "A workflow.json or an exported bundle" },
                { id: "delete", text: "Delete this build" },
              ]}
              onItemClick={({ detail }) => {
                if (detail.id === "new") onNew();
                if (detail.id === "import") setImporting(true);
                if (detail.id === "delete") onDelete();
              }}
            >Project</ButtonDropdown>
            <ButtonDropdown variant="primary"
              items={[
                { id: "bundle", text: "Bundle (workflow, prompts and code)", disabled: errors > 0,
                  disabledReason: errors ? `Fix the ${errors} error${errors > 1 ? "s" : ""} first` : undefined },
                { id: "workflow", text: "workflow.json only" },
              ]}
              onItemClick={({ detail }) => {
                if (detail.id === "workflow") download("workflow.json", json);
                if (detail.id === "bundle") {
                  const name = `${slug(project.name)}.agentexpress.json`;
                  download(name, JSON.stringify(toBundle(project), null, 2) + "\n");
                  setExported(name);
                }
              }}>Export</ButtonDropdown>
          </SpaceBetween>
        }
      >
        <ProjectTitle name={project.name} onRename={(name) => setProject((p) => renameProject(p, name))} />
      </Header>
      {deployment}
      <input ref={fileInput} type="file" accept=".json,application/json" hidden
        onChange={(e) => { const f = e.target.files?.[0]; if (f) void openFile(f); e.target.value = ""; }} />

      {server ? (
        <Tabs
          activeTabId={mode} onChange={({ detail }) => setMode(detail.activeTabId)}
          tabs={[
            {
              id: "ai", label: "AgentExpress Assistant",
              content: <DesignChat project={project} issues={issues} onApplied={onApplied} flush={flush}
                onShowProblems={() => setShowProblems(true)} />,
            },
            { id: "manual", label: "Build manually", content: manual },
          ]}
        />
      ) : manual}
      <Box variant="small" color={saveError ? "text-status-error" : "text-body-secondary"}>
        {saveError ? <>Not saved: {saveError}. It is retried on your next change.{" "}
          {/reload/i.test(saveError) ? <Button variant="inline-link" onClick={onReload}>Reload now</Button> : null}</>
          : server ? (savedAt ? `Saved to your builds at ${savedAt}.` : "Builds save automatically, to your account on this console.")
            : (savedAt ? `Draft saved in this browser at ${savedAt}.` : "Drafts save automatically in this browser.")}
      </Box>

      <AddToolModal visible={addingTool} onDismiss={() => setAddingTool(false)}
        onAdd={(name, type) => {
          const r = addTool(project, name, type);
          setProject(r.project);
          setAddingTool(false);
          setSelection({ kind: "tool", id: r.key });
          setTab("design");
        }} />
      <ExportedModal name={exported} onDismiss={() => setExported(null)} />
      <Modal visible={showProblems} onDismiss={() => setShowProblems(false)} size="large"
        header={`Problems (${issues.length})`}>
        {issues.length ? <IssueList issues={issues} onPick={pickProblem} />
          : <StatusIndicator type="success">No problems. This build is ready to deploy.</StatusIndicator>}
      </Modal>
      <ImportModal visible={importing} onDismiss={() => setImporting(false)}
        onPick={(m) => { setImporting(false); pickFile(m); }} />
      {server && build ? (
        <ShareDialog visible={sharing} title={project.name} shares={build.shares} onDismiss={() => setSharing(false)}
          onSave={async (s) => { onShared(await shareBuild(raw.id, s)); notify("success", "Sharing saved."); }} />
      ) : null}
      {server ? toolSharer.dialog : null}
      {server ? (
        <LibraryPicker kind="tool" visible={pickingTools} onDismiss={() => setPickingTools(false)} notify={notify}
          using={Object.values(raw.workflow.tools).map(refOf).filter(Boolean) as string[]}
          onPick={(items) => {
            addRefs(items);
            let p = raw;
            for (const it of items) p = attachItem(p, "tools", it).project;
            setRaw(p);
            setPickingTools(false);
            notify("success", `This build uses ${items.map((i) => i.name).join(", ")} from the library now.`);
          }} />
      ) : null}
    </SpaceBetween>
  );
}
