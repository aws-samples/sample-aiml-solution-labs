/** Admin: every user's builds and runs, read-only, plus Destroy to clean up a build.
 *
 *  Shown only to a caller with the `admin` permission (workflow.json
 *  authorization.actions.admin — a group filled from the backend only; the IaC refuses to
 *  make it the self-sign-up group). The server enforces all of it: every read of someone
 *  else's build or run is logged as `admin.viewed`, and nothing here can save, deploy,
 *  chat, decide, re-run, cancel or delete for another user. */
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Container from "@cloudscape-design/components/container";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import Input from "@cloudscape-design/components/input";
import Link from "@cloudscape-design/components/link";
import Modal from "@cloudscape-design/components/modal";
import SpaceBetween from "@cloudscape-design/components/space-between";
import StatusIndicator from "@cloudscape-design/components/status-indicator";
import Table from "@cloudscape-design/components/table";
import Tabs from "@cloudscape-design/components/tabs";
import TextFilter from "@cloudscape-design/components/text-filter";
import { useCallback, useEffect, useMemo, useState } from "react";

import { api } from "../api";
import { fmtTime } from "../lib/clock";
import { requestTitle } from "../lib/request";
import { statusIndicator } from "../lib/status";
import type { SessionSummary } from "../types";
import { Canvas } from "./Canvas";
import { deployState } from "./DeployPanel";
import { Markdown } from "./markdown";
import type { Project } from "./model";
import type { BuildSummary, DesignDoc } from "./storage";
import { Groups } from "./Groups";
import { validate } from "./validate";
// Admin is its own lazy chunk: without this the canvas has no height when Build was never opened.
import "./builder.css";

export interface AdminBuild extends BuildSummary {
  owner: string;
  ownerEmail?: string;
}

const enc = encodeURIComponent;
const where = (b: BuildSummary) =>
  !b.tool ? "—" : b.account ? `account ${b.account} · ${b.region ?? ""}` : `this console · ${b.region ?? ""}`;

/** Matches a row against a filter, anywhere a person reads it. */
export function matches(text: string, ...fields: (string | undefined)[]): boolean {
  const q = text.trim().toLowerCase();
  return !q || fields.join(" ").toLowerCase().includes(q);
}

function BuildViewer({ build, onClose, onOpenRun, runs: withRuns }: {
  build: AdminBuild | null; onClose: () => void; onOpenRun: (sid: string) => void; runs: boolean;
}) {
  const [project, setProject] = useState<Project | null>(null);
  const [chat, setChat] = useState<DesignDoc | null>(null);
  const [runs, setRuns] = useState<SessionSummary[] | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    setProject(null); setChat(null); setRuns(null); setError("");
    if (!build) return;
    api.get<{ project: Project }>(`/api/builds/${enc(build.id)}`)
      .then((r) => setProject(r.project)).catch((e: Error) => setError(e.message));
    api.get<DesignDoc>(`/api/builds/${enc(build.id)}/design`).then(setChat).catch(() => setChat(null));
    if (withRuns && build.deployed?.runtimeArn && !build.deployed.account) {
      api.get<SessionSummary[]>(`/api/sessions?build=${enc(build.id)}&scope=all`)
        .then(setRuns).catch(() => setRuns([]));
    } else {
      setRuns([]);
    }
  }, [build, withRuns]);
  const issues = useMemo(() => (project ? validate(project.workflow) : []), [project]);
  return (
    <Modal visible={Boolean(build)} onDismiss={onClose} size="max"
      header={build ? `${build.name} — ${build.ownerEmail || build.owner}` : ""}
      footer={<Box float="right"><Button onClick={onClose}>Close</Button></Box>}>
      {error ? <StatusIndicator type="error">{error}</StatusIndicator> : !project ? (
        <StatusIndicator type="loading">Loading the build</StatusIndicator>
      ) : (
        <SpaceBetween size="m">
          <Box color="text-body-secondary">Read-only. You can look, not change: this build is {build?.ownerEmail || "another user"}&apos;s.</Box>
          <Tabs tabs={[
            {
              id: "workflow", label: "Workflow",
              content: (
                <div className="axd-preview">
                  <Canvas workflow={project.workflow} issues={issues} selection={null} onSelect={() => undefined}
                    onDropPayload={() => undefined} onMoveAgent={() => undefined} readOnly />
                </div>
              ),
            },
            {
              id: "json", label: "workflow.json",
              content: <Box variant="code"><pre className="axa-json">{JSON.stringify(project.workflow, null, 2)}</pre></Box>,
            },
            {
              id: "chat", label: `Design chat${chat?.turns.length ? ` (${chat.turns.length})` : ""}`,
              content: !chat?.turns.length ? <Box color="text-body-secondary">No conversation.</Box> : (
                <SpaceBetween size="s">
                  {chat.turns.map((t) => (
                    <Container key={t.id} header={<Header variant="h3" description={t.at ? new Date(t.at).toLocaleString() : undefined}>
                      {t.role === "user" ? "User" : "Designer"}</Header>}>
                      {t.role === "user" ? <Box><span style={{ whiteSpace: "pre-wrap" }}>{t.text}</span></Box> : <Markdown text={t.text} />}
                    </Container>
                  ))}
                </SpaceBetween>
              ),
            },
            ...(!withRuns ? [] : [{
              id: "runs", label: `Runs${runs?.length ? ` (${runs.length})` : ""}`,
              content: build?.deployed?.account ? (
                <Box color="text-body-secondary">This build runs in its own console, in account {build.deployed.account}; its runs are not in this one.</Box>
              ) : (
                <Table items={runs ?? []} loading={runs === null} loadingText="Loading runs" variant="embedded"
                  columnDefinitions={[
                    { id: "topic", header: "Request", cell: (r) => (
                      <Link href="#" onFollow={(e) => { e.preventDefault(); onClose(); onOpenRun(r.session_id); }}>
                        {requestTitle(r.topic, 100) || r.session_id}</Link>) },
                    { id: "status", header: "Status", cell: (r) => statusIndicator(String(r.overall)) },
                    { id: "created", header: "Started", cell: (r) => fmtTime(r.created) || "—" },
                  ]}
                  empty={<Box color="text-body-secondary">No runs.</Box>} />
              ),
            }]),
          ]} />
        </SpaceBetween>
      )}
    </Modal>
  );
}

export function Admin({ canDestroy, notify, onOpenRun, runs: withRuns = true }: {
  canDestroy: boolean;
  notify: (type: "success" | "error" | "info", text: string) => void;
  /** Open a run on the Runs page (read-only there: it is not the admin's). */
  onOpenRun: (sid: string) => void;
  /** False on a control-plane console: it has no runs, and each build's are in its app. */
  runs?: boolean;
}) {
  const [tab, setTab] = useState("builds");
  const [builds, setBuilds] = useState<AdminBuild[] | null>(null);
  const [runs, setRuns] = useState<SessionSummary[] | null>(null);
  const [error, setError] = useState("");
  const [filter, setFilter] = useState("");
  const [viewing, setViewing] = useState<AdminBuild | null>(null);
  const [destroying, setDestroying] = useState<AdminBuild | null>(null);
  const [confirm, setConfirm] = useState("");
  const [busy, setBusy] = useState(false);

  const load = useCallback(async () => {
    setError("");
    try {
      const [b, r] = await Promise.all([
        api.get<AdminBuild[]>("/api/builds?scope=all"),
        withRuns ? api.get<SessionSummary[]>("/api/sessions?scope=all") : Promise.resolve([]),
      ]);
      setBuilds(b); setRuns(r);
    } catch (e) {
      setError((e as Error).message);
      setBuilds([]); setRuns([]);
    }
  }, [withRuns]);
  useEffect(() => { void load(); }, [load]);

  const shownBuilds = (builds ?? []).filter((b) => matches(filter, b.name, b.ownerEmail, b.owner, b.id, b.agentName));
  const shownRuns = (runs ?? []).filter((r) => matches(filter, r.topic, r.user, r.owner, r.session_id));

  const destroy = async () => {
    if (!destroying) return;
    setBusy(true);
    try {
      await api.post(`/api/builds/${enc(destroying.id)}/destroy`);
      notify("info", `Destroying ${destroying.name} (${destroying.ownerEmail || destroying.owner}). Its owner's activity log says you did.`);
      setDestroying(null); setConfirm("");
      await load();
    } catch (e) {
      notify("error", (e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <SpaceBetween size="l">
      <Header variant="h1"
        description={`Every user's builds${withRuns ? " and runs" : ""}, read-only. Destroy removes a build's resources from AWS, to clean up; its owner sees that you did. Everything you open here is in your activity log.`}
        actions={<Button iconName="refresh" ariaLabel="Refresh" onClick={() => void load()} />}>
        Admin
      </Header>
      {error ? <StatusIndicator type="error">{error}</StatusIndicator> : null}
      <TextFilter filteringText={filter} filteringPlaceholder="Find a user, build, request or id"
        filteringAriaLabel="Filter" onChange={({ detail }) => setFilter(detail.filteringText)} />
      <Tabs activeTabId={tab} onChange={({ detail }) => setTab(detail.activeTabId)} tabs={[
        {
          id: "builds", label: `Builds${builds ? ` (${builds.length})` : ""}`,
          content: (
            <Table items={shownBuilds} loading={builds === null} loadingText="Loading builds" trackBy="id"
              columnDefinitions={[
                { id: "name", header: "Build", isRowHeader: true, cell: (b) => (
                  <Link href="#" onFollow={(e) => { e.preventDefault(); setViewing(b); }}>{b.name}</Link>) },
                { id: "owner", header: "Owner", cell: (b) => b.ownerEmail || b.owner },
                { id: "state", header: "Deployment", cell: (b) => {
                  const s = deployState(b);
                  return <StatusIndicator type={s.type}>{s.text}</StatusIndicator>;
                } },
                { id: "where", header: "Where", cell: where },
                { id: "updated", header: "Updated", cell: (b) => (b.updatedAt ? new Date(b.updatedAt).toLocaleString() : "—") },
                { id: "actions", header: "", cell: (b) => (
                  <Button variant="inline-link" disabled={!canDestroy || !b.tool || Boolean(b.job && ["QUEUED", "RUNNING"].includes(String(b.job.status)))}
                    onClick={() => { setDestroying(b); setConfirm(""); }}>Destroy</Button>) },
              ]}
              empty={<Box color="text-body-secondary">No builds.</Box>} />
          ),
        },
        { id: "groups", label: "Groups", content: <Groups notify={notify} /> },
        ...(!withRuns ? [] : [{
          id: "runs", label: `Runs${runs ? ` (${runs.length})` : ""}`,
          content: (
            <Table items={shownRuns} loading={runs === null} loadingText="Loading runs" trackBy="session_id"
              header={<Header variant="h3" description="Runs of this console's own workflow. A build's runs are under the build.">Runs</Header>}
              columnDefinitions={[
                { id: "topic", header: "Request", isRowHeader: true, cell: (r) => (
                  <Link href="#" onFollow={(e) => { e.preventDefault(); onOpenRun(r.session_id); }}>
                    <span title={r.topic}>{requestTitle(r.topic, 100) || r.session_id}</span></Link>) },
                { id: "user", header: "Started by", cell: (r) => r.user || r.owner || "—" },
                { id: "status", header: "Status", cell: (r) => statusIndicator(String(r.overall)) },
                { id: "created", header: "Started", cell: (r) => fmtTime(r.created) || "—" },
              ]}
              empty={<Box color="text-body-secondary">No runs.</Box>} />
          ),
        }]),
      ]} />
      <BuildViewer build={viewing} onClose={() => setViewing(null)} onOpenRun={onOpenRun} runs={withRuns} />
      <Modal visible={Boolean(destroying)} onDismiss={() => setDestroying(null)} header="Destroy another user's build?"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setDestroying(null)} disabled={busy}>Cancel</Button>
              <Button variant="primary" loading={busy} disabled={confirm !== destroying?.name} onClick={() => void destroy()}>Destroy</Button>
            </SpaceBetween>
          </Box>
        }>
        {destroying ? (
          <SpaceBetween size="m">
            <Box>
              This removes every AWS resource of <b>{destroying.name}</b>, owned by {destroying.ownerEmail || destroying.owner},
              from {where(destroying)}. The build itself stays, so its owner can deploy it again. It is logged in your activity and theirs.
            </Box>
            <FormField label={`Type ${destroying.name} to confirm`}>
              <Input value={confirm} onChange={({ detail }) => setConfirm(detail.value)} />
            </FormField>
          </SpaceBetween>
        ) : null}
      </Modal>
    </SpaceBetween>
  );
}
