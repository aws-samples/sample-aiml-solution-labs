/** The console shell.
 *
 *  AppLayout + TopNavigation + SideNavigation + BreadcrumbGroup + ContentLayout +
 *  SplitPanel + Flashbar — the structure every AWS service page has. The pieces that
 *  make it read as a console are the ones that are easy to leave out: breadcrumbs that
 *  say where you are, a page header that owns the resource's actions, and a collection
 *  in a Table rather than in the navigation panel.
 *
 *  There is no router. The console uses URLs, but adding one would mean a CloudFront
 *  error-page rule to serve index.html for deep links, and this deployment has none —
 *  so navigation is state, and the tradeoff is written down rather than discovered.
 */

import AppLayout from "@cloudscape-design/components/app-layout";
import BreadcrumbGroup from "@cloudscape-design/components/breadcrumb-group";
import ContentLayout from "@cloudscape-design/components/content-layout";
import Flashbar, { type FlashbarProps } from "@cloudscape-design/components/flashbar";
import Select from "@cloudscape-design/components/select";
import Spinner from "@cloudscape-design/components/spinner";
import SplitPanel from "@cloudscape-design/components/split-panel";
import TopNavigation from "@cloudscape-design/components/top-navigation";
import { Suspense, lazy, useCallback, useEffect, useMemo, useRef, useState } from "react";

import { ApiError, api } from "./api";
import { authConfig, authEnabled, initAuth, logout } from "./auth";
import { requestTitle } from "./lib/request";
import { isSettled, statusLabel, statusType } from "./lib/status";
import {
  PROJECTS_EVENT, jobActive, localStore, migrateLocalDrafts, serverStore,
  type BuildStore, type BuildSummary,
} from "./builder/storage";
import { NAV_DIVIDER, NavPane, type NavEntry, type NavGroup } from "./views/NavPane";
import type { BuildRequest } from "./builder/Builder";
import type {
  Action, Me, SessionSnapshot, SessionSummary, Workflow,
} from "./types";
import { AboutPanel } from "./views/AboutPanel";
import { Activity } from "./views/Activity";
import { Assistant } from "./views/Assistant";
import { HitlGate, type Decision, type GroupDecision } from "./views/HitlGate";
import { Observability } from "./views/Observability";
import { RunDetail } from "./views/RunDetail";
import { RunsTable } from "./views/RunsTable";
import { StartRunModal, type RunTarget } from "./views/StartRunModal";
import { StepPanel } from "./views/StepPanel";

/** The product name, and the mark it is shown with, on every build's own app. */
const BRAND = "AgentExpress";
const BRAND_MARK = "🧭";

/** Loaded on first visit to Build. React Flow and the Builder are a third of the bundle,
 *  and a reviewer who only ever approves gates should not download them. */
const Builder = lazy(() => import("./builder/Builder").then((m) => ({ default: m.Builder })));
const Accounts = lazy(() => import("./builder/Accounts").then((m) => ({ default: m.Accounts })));
const Admin = lazy(() => import("./builder/Admin").then((m) => ({ default: m.Admin })));
const LibraryPage = lazy(() => import("./builder/Library").then((m) => ({ default: m.LibraryPage })));
/** The library kinds, in the order the navigation lists them (builder/Library.tsx KINDS). */
const LIBRARY_KINDS = [["tool", "Tools"], ["identity", "Identity"], ["memory", "Memory"], ["evaluator", "Evals"],
  ["policy", "Policies"], ["guardrail", "Guardrails"]] as const;
type LibKind = (typeof LIBRARY_KINDS)[number][0];

/** Where this deployment runs, from auth-config.js (both IaC paths write it). A build
 *  deployed into another region showed "us-east-1" here when this was a constant. */
const REGION = authConfig().region || "us-east-1";
type View = "runs" | "observability" | "build" | "activity" | "accounts" | "admin" | "library";

const EMPTY_WORKFLOW: Workflow = { agents: {}, steps: [] };
const BROWSER_STORE = localStore();

export default function App() {
  const [booted, setBooted] = useState(false);
  const [user, setUser] = useState("");
  const [workflow, setWorkflow] = useState<Workflow>(EMPTY_WORKFLOW);
  const [me, setMe] = useState<Me>({
    user: "", groups: [], permittedActions: null, authzEnabled: false,
  });

  const [view, setView] = useState<View>("runs");
  const [libKind, setLibKind] = useState<LibKind>("tool");
  /** A control-plane console: design, build and deploy. No runs of its own, and each
   *  build's runs are in its own app. */
  const controlPlane = me.consoleMode === "builder";
  /** Where the home link goes. */
  const home = controlPlane ? "#build" : "#runs";
  const [runs, setRuns] = useState<SessionSummary[]>([]);
  const [runsLoading, setRunsLoading] = useState(true);
  const [openRun, setOpenRun] = useState<string | null>(null);
  const [snap, setSnap] = useState<SessionSnapshot | null>(null);
  const [selectedStep, setSelectedStep] = useState<string | null>(null);
  const [tab, setTab] = useState("graph");
  const [startOpen, setStartOpen] = useState(false);
  /** The side navigation lists every build and every run, twice: once under Runs to
   *  open it, once under Observability to inspect it. Builds come from the console's
   *  builds store when it has one (`me.builder`), else from this browser. */
  const [store, setStore] = useState<BuildStore>(BROWSER_STORE);
  const [builds, setBuilds] = useState<BuildSummary[]>([]);
  const [buildsLoaded, setBuildsLoaded] = useState(false);
  /** Whose runs the Runs and Observability lists show: "" is this deployment's own
   *  workflow, anything else a deployed build's id. */
  const [target, setTarget] = useState("");
  const [targetWorkflow, setTargetWorkflow] = useState<Workflow>(EMPTY_WORKFLOW);
  const [buildRequest, setBuildRequest] = useState<BuildRequest | null>(null);
  const [currentBuild, setCurrentBuild] = useState<string | null>(null);
  const [obsSession, setObsSession] = useState<string | null>(null);
  /** The tools drawer. Open on the FIRST visit only: a first-time reader needs to be
   *  told what the application is, and a returning one does not need it in the way. */
  const [toolsOpen, setToolsOpen] = useState(
    () => localStorage.getItem("seen_about") !== "1");
  const [flash, setFlash] = useState<FlashbarProps.MessageDefinition[]>([]);

  const flashId = useRef(0);
  const notify = useCallback((type: FlashbarProps.Type, content: string) => {
    const id = String(++flashId.current);
    setFlash((f) => [
      ...f,
      { id, type, content, dismissible: true, onDismiss: () => setFlash((g) => g.filter((x) => x.id !== id)) },
    ]);
  }, []);

  const fail = useCallback((e: unknown, what: string) => {
    const msg = e instanceof ApiError ? `${what}: ${e.message}` : `${what}: ${String(e)}`;
    notify("error", msg);
  }, [notify]);

  /** RBAC is advisory here — every action is enforced again in bff/authz.py. This only
   *  stops the UI offering a control that would 403. */
  const can = useCallback((a: Action) => {
    const list = me.permittedActions;
    return list === null || list.includes(a);
  }, [me.permittedActions]);

  /** On a run the caller did not start (an admin reading it), nothing is actionable:
   *  the server would refuse every action anyway. */
  const othersRun = Boolean(snap?.owner && me.owner && snap.owner !== me.owner);
  const canOnRun = useCallback((a: Action) => !othersRun && can(a), [othersRun, can]);
  const denyReason = useCallback((a: Action) => {
    if (can(a)) return null;
    return `You do not have permission to ${a}. Your groups: ${me.groups.join(", ") || "none"}.`;
  }, [can, me.groups]);

  // --- boot ---------------------------------------------------------------
  useEffect(() => {
    void (async () => {
      const state = await initAuth();
      // null means a redirect to the IdP is under way. Stop: anything we fetch now is
      // a guaranteed 401 on the way out of the page.
      if (!state) return;
      setUser(state.user);
      try {
        setWorkflow(await api.get<Workflow>("/api/workflow"));
      } catch (e) {
        fail(e, "Could not load the workflow definition");
      }
      try {
        const m = await api.get<Me>("/api/me");
        setMe(m);
        if (m.consoleMode === "builder") setView("build");
        if (m.builder) {
          // Drafts from before builds were stored on the server move there, once.
          const moved = await migrateLocalDrafts().catch(() => 0);
          if (moved) notify("info", `Moved ${moved} draft${moved > 1 ? "s" : ""} from this browser to your builds.`);
          setStore(serverStore);
        }
      } catch {
        /* leave permittedActions null: unknown means "do not hide anything" */
      }
      setBooted(true);
    })();
  }, [fail, notify]);

  const refreshBuilds = useCallback(async () => {
    try {
      setBuilds(await store.list());
      setBuildsLoaded(true);
    } catch { /* the next event or poll retries */ }
  }, [store]);

  useEffect(() => {
    if (!booted) return;
    void refreshBuilds();
    const refresh = () => void refreshBuilds();
    window.addEventListener(PROJECTS_EVENT, refresh);
    return () => window.removeEventListener(PROJECTS_EVENT, refresh);
  }, [booted, refreshBuilds]);

  // A deploy or destroy runs for many minutes; keep its status in the navigation current.
  const anyJob = builds.some((b) => jobActive(b));
  useEffect(() => {
    if (!anyJob) return;
    const t = setInterval(() => void refreshBuilds(), 10000);
    return () => clearInterval(t);
  }, [anyJob, refreshBuilds]);

  /** The builds a run can be started against: deployed, and not being destroyed. */
  // Only builds in THIS console's account: one deployed into a connected account runs
  // from its own app there, so its data never passes through this console.
  const deployedBuilds = builds.filter((b) => b.deployed && !b.deployed.account
    && !(jobActive(b) && b.job?.action === "destroy"));
  const targets: RunTarget[] = [
    { value: "", label: "This deployment's workflow", description: workflow.ui?.title },
    ...deployedBuilds.map((b) => ({
      value: b.id, label: b.name, description: `Build · version ${b.deployed!.version} · ${b.deployed!.tool}`,
    })),
  ];
  // A destroyed (or deleted) build cannot stay picked.
  useEffect(() => {
    if (target && buildsLoaded && !deployedBuilds.some((b) => b.id === target)) setTarget("");
  }, [target, buildsLoaded, builds]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (!target) { setTargetWorkflow(workflow); return; }
    let live = true;
    api.get<Workflow>(`/api/workflow?build=${encodeURIComponent(target)}`)
      .then((w) => { if (live) setTargetWorkflow(w); })
      .catch((e) => fail(e, "Could not load that build's workflow"));
    return () => { live = false; };
  }, [target, workflow, fail]);

  // --- polling ------------------------------------------------------------
  /** Consecutive failed polls. A SINGLE failure is not news.
   *
   *  Measured on the deployed stack over 24 hours: 17,276 API Gateway requests, 17,204
   *  Lambda invocations, ZERO Lambda errors, and two 5xx — transient integration blips
   *  at 0.012%. But this list polls every 5 seconds, so a tab left open all afternoon
   *  will meet one, and the first version raised a sticky red banner for it. Two blips
   *  in seventeen thousand requests presented as "Could not list runs: Internal Server
   *  Error", sitting there until dismissed, which reads as a broken application.
   *
   *  So a poll failure is only worth reporting once it PERSISTS: three in a row is
   *  fifteen seconds of genuinely not working, which is worth saying. Anything shorter
   *  resolves itself before the reader could have acted on it. */
  const pollFailures = useRef(0);
  const FLASH_AFTER = 3;

  const refreshRuns = useCallback(async () => {
    try {
      setRuns(await api.get<SessionSummary[]>(
        target ? `/api/sessions?build=${encodeURIComponent(target)}` : "/api/sessions"));
      pollFailures.current = 0;
    } catch (e) {
      pollFailures.current += 1;
      // `>` not `>=`: report on the third failure and then stay quiet, rather than
      // stacking one flash per failed poll for as long as the outage lasts.
      if (booted && pollFailures.current === FLASH_AFTER) {
        fail(e, `Could not list runs (${FLASH_AFTER} attempts)`);
      }
    } finally {
      setRunsLoading(false);
    }
  }, [booted, fail, target]);

  useEffect(() => {
    if (!booted || controlPlane) return;
    setRunsLoading(true);
    void refreshRuns();
    const t = setInterval(() => void refreshRuns(), 5000);
    return () => clearInterval(t);
  }, [booted, refreshRuns, controlPlane]);

  useEffect(() => {
    if (!booted || !openRun) { setSnap(null); return; }
    let live = true;
    const pull = async () => {
      try {
        const s = await api.get<SessionSnapshot>(`/api/sessions/${openRun}`);
        if (live) setSnap(s);
      } catch { /* a transient failure should not clear the page */ }
    };
    void pull();
    const t = setInterval(() => void pull(), 2000);
    return () => { live = false; clearInterval(t); };
  }, [booted, openRun]);

  const ui = useMemo(() => workflow.ui ?? {}, [workflow.ui]);
  // A build's own app (no Builder) is branded "🧭 AgentExpress - <its title>"; the
  // console, whose workflow IS AgentExpress, keeps its own heading.
  const own = ui.title || ui.heading?.replace(/^\p{Extended_Pictographic}\uFE0F?\s*/u, "") || "";
  const branded = !me.builder && own && own !== BRAND;
  const heading = branded ? `${BRAND_MARK} ${BRAND} - ${own}` : ui.heading || ui.title || BRAND;
  const pageTitle = branded ? `${BRAND} - ${own}` : ui.title || BRAND;

  useEffect(() => {
    document.title = pageTitle;
  }, [pageTitle]);

  useEffect(() => {
    if (toolsOpen) localStorage.setItem("seen_about", "1");
  }, [toolsOpen]);

  const settled = isSettled(String(snap?.overall ?? ""));
  /** A run is drawn from the workflow IT ran with (stored when it started), so an old
   *  run keeps its own graph after the workflow changes. Older runs have none. */
  const runWorkflow = snap?.workflow ?? targetWorkflow;

  // --- actions ------------------------------------------------------------
  async function startRun(topic: string, subject: string) {
    try {
      const body: Record<string, string> = { topic };
      if (subject) body.subjectId = subject;
      if (target) body.build = target;
      const r = await api.post<{ session_id: string }>("/api/sessions", body);
      setStartOpen(false);
      // The + beside Runs can be pressed from any view, so land on the run itself.
      setView("runs");
      setOpenRun(r.session_id);
      setSelectedStep(null);
      setTab("graph");
      notify("success", "Run started.");
      void refreshRuns();
    } catch (e) {
      fail(e, "Could not start the run");
    }
  }

  async function decide(d: Decision, comment: string) {
    if (!openRun) return;
    try {
      await api.post(`/api/sessions/${openRun}/decision`, { decision: d, comment });
      notify("success", `Recorded: ${d}.`);
    } catch (e) {
      fail(e, "Could not submit the decision");
    }
  }

  async function groupDecide(per: Record<string, GroupDecision>, comment: string) {
    if (!openRun) return;
    const decisions: Record<string, { decision: string; comment: string }> = {};
    for (const [id, v] of Object.entries(per)) {
      decisions[id] = { decision: v.decision, comment: v.comment };
    }
    try {
      await api.post(`/api/sessions/${openRun}/decision`, { decisions, comment });
      notify("success", "Decisions recorded.");
    } catch (e) {
      fail(e, "Could not submit the decisions");
    }
  }

  async function rerun(agents: string[], comment: string) {
    if (!openRun || agents.length === 0) return;
    try {
      // Two shapes, and they are not interchangeable (bff/handler.py):
      //   one agent      -> {"agentId": "x", "comment": "..."}
      //   a subset       -> {"agents": [{"agentId": "x", "comment": "..."}, ...]}
      // The subset form takes a list of OBJECTS; sending a list of ids silently matched
      // nothing.
      const body = agents.length === 1
        ? { agentId: agents[0], comment }
        : { agents: agents.map((agentId) => ({ agentId, comment })) };
      await api.post(`/api/sessions/${openRun}/rerun`, body);
      setSelectedStep(null);
      notify("success", agents.length === 1
        ? "Re-running that step and everything after it."
        : `Re-running ${agents.length} agents.`);
    } catch (e) {
      fail(e, "Could not re-run");
    }
  }

  async function cancel() {
    if (!openRun) return;
    try {
      await api.post(`/api/sessions/${openRun}/cancel`);
      notify("info", "Stop requested. The run halts at the next step boundary.");
    } catch (e) {
      fail(e, "Could not stop the run");
    }
  }

  async function del(id: string) {
    if (!window.confirm("Delete this run? Its status and timeline are removed permanently, and it "
      + "disappears from Runs and Observability, including your cost totals.")) return;
    try {
      await api.del(`/api/sessions/${id}`);
      if (openRun === id) { setOpenRun(null); setSnap(null); }
      if (obsSession === id) setObsSession(null);
      notify("success", "Run deleted.");
      void refreshRuns();
    } catch (e) {
      fail(e, "Could not delete the run");
    }
  }

  // --- shell --------------------------------------------------------------
  const crumbs = useMemo(() => {
    const items = [{ text: heading, href: home }];
    if (view === "observability") {
      items.push({ text: "Observability", href: "#obs" });
    } else if (view === "build") {
      items.push({ text: "Build", href: "#build" });
    } else if (view === "activity") {
      items.push({ text: "Activity", href: "#activity" });
    } else if (view === "accounts") {
      items.push({ text: "AWS accounts", href: "#accounts" });
    } else if (view === "admin") {
      items.push({ text: "Admin", href: "#admin" });
    } else if (view === "library") {
      items.push({ text: LIBRARY_KINDS.find(([k]) => k === libKind)?.[1] ?? "Library", href: `#library:${libKind}` });
    } else {
      items.push({ text: "Runs", href: "#runs" });
      if (openRun) items.push({ text: openRun, href: "#run" });
    }
    return items;
  }, [heading, view, openRun, home]);

  async function deleteBuild(b: BuildSummary) {
    const name = b.name || "Untitled";
    const msg = b.tool && store.server
      ? `Delete the build “${name}”? Everything it deployed is destroyed first (with ${b.tool === "cdk" ? "AWS CDK" : "Terraform"}, `
        + "the tool it was deployed with), then the build and every stored version are deleted. This cannot be undone."
      : store.server ? `Delete the build “${name}”? This cannot be undone.`
        : `Delete the build “${name}”? It is saved only in this browser, so this cannot be undone.`;
    if (!window.confirm(msg)) return;
    try {
      const r = await store.remove(b.id);
      if (r.destroying) {
        notify("info", `Destroying “${name}”. It is deleted when the destroy finishes.`);
      } else {
        notify("success", `Build “${name}” deleted.`);
        // The open build went: open the next one, or a fresh one if that was the last.
        if (currentBuild === b.id) {
          const next = builds.find((x) => x.id !== b.id);
          setBuildRequest({ id: next ? next.id : "new", nonce: Date.now() });
        }
      }
      void refreshBuilds();
    } catch (e) {
      fail(e, "Could not delete the build");
    }
  }

  /** Record the sign-out in the audit log, then sign out. The record is best effort: a
   *  sign-out must never be blocked by it. */
  async function signOut() {
    if (me.audit ?? me.builder) {
      try {
        await api.post("/api/audit/logout");
      } catch {
        // Signed out either way.
      }
    }
    logout();
  }

  function runBuild(id: string) {
    setTarget(id);
    setStartOpen(true);
  }

  function buildStatusOf(b: BuildSummary): NavEntry["status"] {
    if (jobActive(b)) {
      return { type: "in-progress", label: b.job!.action === "destroy" ? "Destroying" : "Deploying" };
    }
    if (b.job?.status === "FAILED") return { type: "error", label: `${b.job.action === "destroy" ? "Destroy" : "Deploy"} failed` };
    if (b.deployed) return { type: "success", label: `Version ${b.deployed.version} deployed` };
    return undefined;
  }

  /** One place that knows what every internal link means, so the side navigation and
   *  the breadcrumbs cannot disagree about it. */
  function navigate(href: string) {
    const [what, id] = href.slice(1).split(":");
    if (what === "build" && !me.builder) {
      setView("runs"); setOpenRun(null);
    } else if (what === "build") {
      setView("build");
      setToolsOpen(false);
      if (id) setBuildRequest({ id, nonce: Date.now() });
    } else if (what === "runs") {
      setView("runs"); setOpenRun(null);
    } else if (what === "run" && id) {
      setView("runs"); setOpenRun(id); setSelectedStep(null); setTab("graph");
    } else if (what === "obs") {
      setView("observability"); setObsSession(id ?? null);
    } else if (what === "activity") {
      setView("activity");
    } else if (what === "accounts") {
      setView("accounts");
    } else if (what === "admin") {
      setView("admin");
    } else if (what === "library" || what === "policies") {
      setView("library");
      setLibKind(((LIBRARY_KINDS.find(([k]) => k === id)?.[0]) ?? (what === "policies" ? "policy" : "tool")) as LibKind);
    } else if (what === "about") {
      setToolsOpen(true);
    }
  }

  /** Every run, newest first (the API's order), with its status as an icon. Runs share a
   *  topic often (the same request, re-run), so a repeated topic carries the run id. */
  const topicCount = runs.reduce<Record<string, number>>((m, r) => {
    m[r.topic] = (m[r.topic] ?? 0) + 1;
    return m;
  }, {});
  const runEntries = (prefix: string, deletable: boolean): NavEntry[] => runs.map((r) => {
    const short = requestTitle(r.topic, 60);
    const label = !r.topic ? r.session_id
      : (topicCount[r.topic] ?? 0) > 1 ? `${short} · ${r.session_id.slice(0, 6)}` : short;
    return {
      href: `${prefix}${r.session_id}`,
      text: label,
      title: `${requestTitle(r.topic, 300) || r.session_id} (${r.session_id})`,
      status: { type: statusType(String(r.overall)), label: statusLabel(String(r.overall)) },
      // Delete lives on Runs only: it removes the run, which also takes it out of the
      // Observability list below — two buttons for one action would read as two actions.
      ...(deletable && can("delete")
        ? { onDelete: () => void del(r.session_id), deleteLabel: `Delete run ${label}` } : {}),
    };
  });

  // Build only where this deployment has the Builder (a console). A build's own app is
  // for running and observing it; it is designed and deployed from the console.
  const navGroups: NavGroup[] = [
    ...(!me.builder ? [] : [{
      id: "build", text: "Build", href: "#build", badge: "Preview",
      onAdd: () => navigate("#build:new"), addLabel: "New build",
      entries: builds.map((b) => ({
        href: `#build:${b.id}`, text: `${b.name || "Untitled"}${b.shared ? " · shared" : ""}`,
        ...(b.shared ? { title: `${b.name} — shared with you by ${b.ownerEmail ?? "its owner"}` } : {}),
        status: buildStatusOf(b),
        // Deleting a deployed build destroys its stack, which needs `destroy`.
        ...(b.tool && store.server && !can("destroy") ? {} : {
          onDelete: () => void deleteBuild(b), deleteLabel: `Delete build ${b.name || "Untitled"}`,
        }),
      })),
      empty: "No builds yet — press + to start one.",
    }]),
    ...(controlPlane ? [] : [{
      id: "runs", text: "Runs", href: "#runs",
      ...(can("start") ? { onAdd: () => setStartOpen(true), addLabel: "Start a new run" } : {}),
      entries: runEntries("#run:", true),
      empty: "No runs yet.",
      // Only once there is a choice: with no deployed build, the list is this workflow's.
      ...(targets.length > 1 ? {
        extra: (
          <Select
            ariaLabel="Show the runs of"
            selectedOption={targets.find((t) => t.value === target) ?? targets[0]}
            options={targets}
            onChange={({ detail }) => {
              setTarget(detail.selectedOption.value ?? "");
              setOpenRun(null);
              setObsSession(null);
            }}
          />
        ),
      } : {}),
    },
    {
      id: "obs", text: "Observability", href: "#obs",
      entries: runEntries("#obs:", false),
      empty: "Nothing to inspect until a run starts.",
    }] as NavGroup[]),
  ];

  const activeHref = view === "activity" ? "#activity" : view === "accounts" ? "#accounts"
    : view === "admin" ? "#admin" : view === "library" ? `#library:${libKind}`
    : view === "build" ? (currentBuild ? `#build:${currentBuild}` : "#build")
    : view === "observability" ? (obsSession ? `#obs:${obsSession}` : "#obs")
      : openRun ? `#run:${openRun}` : "#runs";

  const content = view === "activity"
    ? <Activity canSeeEveryone={Boolean(me.permittedActions?.includes("audit"))} />
    : view === "accounts"
    ? (
      <Suspense fallback={<Spinner size="large" />}>
        <Accounts canConnect={can("deploy")} notify={notify} />
      </Suspense>
    )
    : view === "library"
    ? (
      <Suspense fallback={<Spinner size="large" />}>
        <LibraryPage key={libKind} kind={libKind} notify={notify} />
      </Suspense>
    )
    : view === "admin" && me.admin
    ? (
      <Suspense fallback={<Spinner size="large" />}>
        <Admin canDestroy={can("destroy")} notify={notify} runs={!controlPlane}
          onOpenRun={(id) => { setView("runs"); setOpenRun(id); setSelectedStep(null); setTab("graph"); }} />
      </Suspense>
    )
    : view === "build" && me.builder
    ? (
      <Suspense fallback={<Spinner size="large" />}>
        <Builder notify={notify} request={buildRequest} onCurrent={setCurrentBuild}
          store={store} can={can} onRun={controlPlane ? undefined : runBuild} />
      </Suspense>
    )
    : view === "observability"
    ? <Observability workflow={targetWorkflow} session={obsSession} build={target} />
    : openRun && snap
      ? (
        <>
          {snap.hitl ? (
            <div style={{ marginBottom: 20 }}>
              <HitlGate
                snap={snap} workflow={runWorkflow}
                canDecide={canOnRun("decision")}
                denyReason={othersRun ? "This is another user's run: an admin can read it, not act on it." : denyReason("decision")}
                onDecide={decide} onGroupDecide={groupDecide}
              />
            </div>
          ) : null}
          <RunDetail
            snap={snap} workflow={runWorkflow} selected={selectedStep}
            onSelect={setSelectedStep} can={canOnRun} onCancel={cancel}
            activeTab={tab} onTabChange={setTab} onRerun={rerun}
          />
        </>
      )
      : (
        <RunsTable
          runs={runs} loading={runsLoading}
          can={(a) => can(a as Action)}
          onOpen={(id) => { setOpenRun(id); setSelectedStep(null); setTab("graph"); }}
          onDelete={(id) => void del(id)}
          onStartNew={() => setStartOpen(true)}
        />
      );

  return (
    <>
      <div id="top-nav">
        <TopNavigation
          identity={{ href: "#", title: heading, onFollow: (e) => {
            e.preventDefault(); navigate(home);
          } }}
          utilities={[
            {
              type: "button",
              iconName: "status-info",
              text: "Info",
              ariaLabel: "About this application",
              disableUtilityCollapse: true,
              onClick: () => setToolsOpen(true),
            },
            { type: "button", text: `${REGION}`, iconName: "map", disableUtilityCollapse: true },
            ...(authEnabled()
              ? [{
                type: "menu-dropdown" as const,
                text: user || "Account",
                iconName: "user-profile" as const,
                items: [
                  { id: "groups", text: `Groups: ${me.groups.join(", ") || "none"}`, disabled: true },
                  { id: "signout", text: "Sign out" },
                ],
                onItemClick: ({ detail }: { detail: { id: string } }) => {
                  if (detail.id === "signout") void signOut();
                },
              }]
              : []),
          ]}
        />
      </div>

      <AppLayout
        headerSelector="#top-nav"
        // The Build view is a three-pane editor (palette, canvas, inspector); the
        // default reading-width cap would squeeze the canvas to a strip.
        maxContentWidth={view === "build" ? Number.MAX_VALUE : undefined}
        breadcrumbs={
          <BreadcrumbGroup
            items={crumbs}
            onFollow={(e) => {
              e.preventDefault();
              const href = e.detail.href;
              if (href === "#runs") { setView("runs"); setOpenRun(null); }
              if (href === "#obs") { setView("observability"); setObsSession(null); }
              if (href === "#build" && me.builder) setView("build");
              if (href === "#activity") setView("activity");
              if (href === "#accounts") setView("accounts");
              if (href === "#admin") setView("admin");
              if (href.startsWith("#library")) navigate(href);
            }}
          />
        }
        navigation={
          <NavPane
            heading={heading} homeHref={home} groups={navGroups} active={activeHref}
            onFollow={navigate}
            links={[
              // The audit log lives with the Builder's store, so only a Builder console has it.
              // Where builds deploy; then what any build can use (the library); then
              // the log, administration and this page's own help.
              ...(me.builder ? [{ text: "AWS accounts", href: "#accounts" }, NAV_DIVIDER,
                ...LIBRARY_KINDS.map(([k, text]) => ({ text, href: `#library:${k}` })), NAV_DIVIDER] : []),
              // A console's log, or a build app's own (bff/audit.py).
              ...((me.audit ?? me.builder) ? [{ text: "Activity", href: "#activity" }] : []),
              ...(me.builder && me.admin ? [{ text: "Admin", href: "#admin" }] : []),
              { text: "About this application", href: "#about" },
            ]}
          />
        }
        tools={<AboutPanel workflow={workflow} heading={heading} console={controlPlane} />}
        toolsOpen={toolsOpen}
        onToolsChange={({ detail }) => setToolsOpen(detail.open)}
        /* Without these, AppLayout's own open/close controls ship with no accessible
           name — the info panel could be opened and then not closed by anyone using a
           screen reader, and a test could not find the button either. */
        ariaLabels={{
          navigation: "Navigation",
          navigationToggle: "Open the navigation",
          navigationClose: "Close the navigation",
          tools: "About this application",
          toolsToggle: "Open the information panel",
          toolsClose: "Close the information panel",
          notifications: "Notifications",
        }}
        notifications={<Flashbar items={flash} stackItems />}
        splitPanelOpen={Boolean(selectedStep && snap)}
        onSplitPanelToggle={({ detail }) => { if (!detail.open) setSelectedStep(null); }}
        splitPanelPreferences={{ position: "side" }}
        splitPanel={
          selectedStep && snap
            ? (
              <SplitPanel
                header={runWorkflow.agents[selectedStep]?.name ?? selectedStep}
                closeBehavior="hide"
                i18nStrings={{
                  preferencesTitle: "Split panel preferences",
                  preferencesPositionLabel: "Position",
                  preferencesPositionDescription: "Where the panel opens.",
                  preferencesPositionSide: "Side",
                  preferencesPositionBottom: "Bottom",
                  preferencesConfirm: "Confirm",
                  preferencesCancel: "Cancel",
                  closeButtonAriaLabel: "Close panel",
                  openButtonAriaLabel: "Open panel",
                  resizeHandleAriaLabel: "Resize panel",
                }}
              >
                <StepPanel
                  agentId={selectedStep} snap={snap} workflow={runWorkflow}
                  canRerun={can("rerun")} settled={settled} onRerun={rerun}
                />
              </SplitPanel>
            )
            : undefined
        }
        content={
          <ContentLayout>
            {booted ? content : null}
            {controlPlane ? null : (
              <StartRunModal
                visible={startOpen} ui={targetWorkflow.ui ?? ui}
                onDismiss={() => setStartOpen(false)}
                onStart={startRun}
                targets={targets} target={target} onTarget={setTarget}
              />
            )}
            {booted && !controlPlane ? (
              <Assistant
                ui={ui} chatbot={workflow.chatbot} sessionId={openRun} build={target}
                onActed={() => { void refreshRuns(); }}
              />
            ) : null}
          </ContentLayout>
        }
      />
    </>
  );
}
