/** The Live config page (admins; orchestrator.liveConfig): edit an agent's prompt and model
 *  settings, save them as a numbered version, see what changes against the active one, and
 *  activate a version, which is also how to roll back (bff/liveconfig.py). */
import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Container from "@cloudscape-design/components/container";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import Input from "@cloudscape-design/components/input";
import Modal from "@cloudscape-design/components/modal";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Spinner from "@cloudscape-design/components/spinner";
import StatusIndicator from "@cloudscape-design/components/status-indicator";
import Table from "@cloudscape-design/components/table";
import Textarea from "@cloudscape-design/components/textarea";
import { useCallback, useEffect, useMemo, useState } from "react";
import { api } from "../api";
import {
  compact, diff, FIELD_LABEL, fromText, LIVE_FIELDS, shown, toText,
  type LiveField, type LiveState, type Overlay, type Version, type VersionSummary,
} from "../lib/liveConfig";
import { useDocsBase } from "../docs/docsLinks";
import { InfoLink, infoSlot } from "../docs/InfoLink";

const MULTILINE: LiveField[] = ["systemPrompt", "stopSequences"];
const HINT: Partial<Record<LiveField, string>> = {
  systemPrompt: "Replaces the prompt in this agent's code, in full. Empty keeps the deployed one, which lives in the image and is not shown here.",
  model: "A Bedrock model id or inference profile this deployment's runtimes may call.",
  stopSequences: "One per line.",
};

export function LiveConfigPage() {
  const docs = useDocsBase();
  const [state, setState] = useState<LiveState | null>(null);
  const [versions, setVersions] = useState<VersionSummary[]>([]);
  const [active, setActive] = useState<Overlay>({});
  const [draft, setDraft] = useState<Overlay>({});
  const [agent, setAgent] = useState("");
  const [note, setNote] = useState("");
  const [viewing, setViewing] = useState<Version | null>(null);
  const [confirm, setConfirm] = useState<number | null>(null);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");
  const [issues, setIssues] = useState<{ path: string; message: string }[]>([]);
  const [saved, setSaved] = useState<number | null>(null);

  const load = useCallback(async () => {
    try {
      const s = await api.get<LiveState>("/api/liveconfig");
      const v = (await api.get<{ versions: VersionSummary[] }>("/api/liveconfig/versions")).versions;
      const overlay = s.pointer.active ? (await api.get<Version>(`/api/liveconfig/versions/${s.pointer.active}`)).overlay : {};
      setState(s); setVersions(v); setActive(overlay); setDraft(overlay); setError("");
      setAgent((a) => a || Object.keys(s.agents)[0] || "");
    } catch (e) { setError((e as Error).message); }
  }, []);
  useEffect(() => { void load(); }, [load]);

  const changes = useMemo(() => diff(active, compact(draft)), [active, draft]);
  if (!state) return error ? <Alert type="error">{error}</Alert> : <Spinner size="large" />;
  const current = state.pointer.active;
  const deployed = state.agents[agent]?.deployed ?? {};
  const options = Object.entries(state.agents).map(([id, a]) => ({ value: id, label: a.name, description: id }));
  const setField = (field: LiveField, text: string) =>
    setDraft((d) => ({ ...d, [agent]: { ...(d[agent] ?? {}), [field]: fromText(field, text) } }));

  async function save() {
    setBusy("save"); setError(""); setIssues([]); setSaved(null);
    try {
      const v = await api.post<Version>("/api/liveconfig/versions", { agents: compact(draft), note });
      setSaved(v.version); setNote("");
      setVersions((await api.get<{ versions: VersionSummary[] }>("/api/liveconfig/versions")).versions);
    } catch (e) {
      const body = (e as Error & { body?: { issues?: { path: string; message: string }[] } }).body;
      setIssues(body?.issues ?? []);
      setError((e as Error).message);
    } finally { setBusy(""); }
  }
  async function activate(n: number) {
    setBusy("activate"); setError("");
    try {
      await api.put("/api/liveconfig/active", { version: n, from: current });
      setConfirm(null); setSaved(null);
      await load();
    } catch (e) { setError((e as Error).message); setConfirm(null); } finally { setBusy(""); }
  }
  async function view(n: number) {
    try { setViewing(await api.get<Version>(`/api/liveconfig/versions/${n}`)); }
    catch (e) { setError((e as Error).message); }
  }

  const rows = [{ version: 0, at: "", by: "", note: "As deployed", agents: [] as string[] }, ...versions];
  return (
    <SpaceBetween size="l">
      <Header variant="h1" description={"Change an agent's prompt and model settings without a redeploy. A run keeps the "
        + "version that was active when it started, through every gate and re-run; tools, gates, guardrails and "
        + "everything else the deploy provisions are not changed here."} info={infoSlot(docs, "liveConfig")}>Live config</Header>
      {error ? (
        <Alert type="error" header="That did not work">
          {error}
          {issues.length ? <ul>{issues.map((i) => <li key={i.path + i.message}><code>{i.path}</code>: {i.message}</li>)}</ul> : null}
        </Alert>
      ) : null}
      <Container header={<Header variant="h2" info={infoSlot(docs, "liveConfig.active")}
        actions={current ? <Button onClick={() => setConfirm(0)}>Use the deployed config</Button> : undefined}>Active</Header>}>
        {current
          ? <StatusIndicator type="success">Version {current} is active{state.pointer.at ? ` since ${state.pointer.at}` : ""}{state.pointer.by ? `, by ${state.pointer.by}` : ""}. New runs start on it.</StatusIndicator>
          : <StatusIndicator type="info">New runs use the config as deployed (version 0).</StatusIndicator>}
      </Container>
      <Table
        header={<Header variant="h2" counter={`(${versions.length})`} info={infoSlot(docs, "liveConfig.versions")}>Versions</Header>}
        items={rows}
        trackBy="version"
        columnDefinitions={[
          { id: "version", header: "Version", cell: (r) => (r.version === current ? <b>v{r.version} (active)</b> : `v${r.version}`) },
          { id: "at", header: "Saved", cell: (r) => r.at || "—" },
          { id: "by", header: "By", cell: (r) => r.by || "—" },
          { id: "note", header: "Note", cell: (r) => r.note || "—" },
          { id: "agents", header: "Changes", cell: (r) => r.agents.join(", ") || "—" },
          { id: "actions", header: "Actions", cell: (r) => (
            <SpaceBetween direction="horizontal" size="xs">
              {r.version ? <Button variant="inline-link" ariaLabel={`View version ${r.version}`} onClick={() => void view(r.version)}>View</Button> : null}
              <Button variant="inline-link" disabled={r.version === current} ariaLabel={`Activate version ${r.version}`}
                onClick={() => setConfirm(r.version)}>{r.version < current ? "Roll back to it" : "Activate"}</Button>
            </SpaceBetween>
          ) },
        ]}
      />
      <Container header={<Header variant="h2" description={`Starts from the active version (v${current}). Saving makes a new version; it is not active until you activate it.`}
        info={infoSlot(docs, "liveConfig.new")}>New version</Header>}>
        <SpaceBetween size="m">
          <FormField label="Agent">
            <Select selectedOption={options.find((o) => o.value === agent) ?? null} options={options}
              onChange={({ detail }) => setAgent(detail.selectedOption.value ?? "")} ariaLabel="Agent" />
          </FormField>
          {agent ? LIVE_FIELDS.map((field) => {
            const value = toText(field, draft[agent]?.[field]);
            const placeholder = field === "systemPrompt" ? "(the prompt in this agent's code)" : `Deployed: ${shown(deployed[field])}`;
            return (
              <FormField key={field} label={FIELD_LABEL[field]} description={HINT[field]}>
                {MULTILINE.includes(field)
                  ? <Textarea value={value} rows={field === "systemPrompt" ? 10 : 3} placeholder={placeholder}
                      ariaLabel={`${FIELD_LABEL[field]} for ${agent}`} onChange={({ detail }) => setField(field, detail.value)} />
                  : <Input value={value} placeholder={placeholder} ariaLabel={`${FIELD_LABEL[field]} for ${agent}`}
                      inputMode={field === "model" ? undefined : "decimal"} onChange={({ detail }) => setField(field, detail.value)} />}
              </FormField>
            );
          }) : null}
          <FormField label="Note" description="Why, for whoever reads the version list later.">
            <Input value={note} ariaLabel="Note" onChange={({ detail }) => setNote(detail.value.slice(0, state.noteMax))} />
          </FormField>
          <Table
            variant="embedded"
            header={<Header variant="h3">Changes against the active version</Header>}
            items={changes}
            trackBy={(r) => `${r.agent}.${r.field}`}
            empty={<Box color="text-body-secondary">Nothing changed yet.</Box>}
            columnDefinitions={[
              { id: "agent", header: "Agent", cell: (r) => r.agent },
              { id: "field", header: "Setting", cell: (r) => FIELD_LABEL[r.field] },
              { id: "active", header: `Active (v${current})`, cell: (r) => <Box variant="code">{shown(r.active).slice(0, 300)}</Box> },
              { id: "draft", header: "New", cell: (r) => <Box variant="code">{shown(r.draft).slice(0, 300)}</Box> },
            ]}
          />
          <SpaceBetween direction="horizontal" size="xs">
            <Button variant="primary" loading={busy === "save"} disabled={!changes.length || !Object.keys(compact(draft)).length}
              onClick={() => void save()}>Save as a new version</Button>
            <Button disabled={!changes.length} onClick={() => setDraft(active)}>Discard changes</Button>
          </SpaceBetween>
          {saved ? (
            <Alert type="success" action={<Button onClick={() => setConfirm(saved)}>Activate v{saved}</Button>}>
              Saved as version {saved}. It is not active yet.
            </Alert>
          ) : null}
        </SpaceBetween>
      </Container>
      <Modal visible={confirm !== null} onDismiss={() => setConfirm(null)}
        header={confirm ? <>{`Activate version ${confirm}?`}<InfoLink feature="liveConfig.new" /></>
          : <>Use the deployed config?<InfoLink feature="liveConfig.versions" /></>}
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setConfirm(null)}>Cancel</Button>
              <Button variant="primary" loading={busy === "activate"} onClick={() => void activate(confirm ?? 0)}>Activate</Button>
            </SpaceBetween>
          </Box>
        }>
        Runs that start from now on use {confirm ? `version ${confirm}` : "the config as deployed"}. Runs already going keep
        the version they started with. You can go back to version {current} the same way.
      </Modal>
      <Modal visible={viewing !== null} onDismiss={() => setViewing(null)} size="large"
        header={viewing ? <>{`Version ${viewing.version}${viewing.note ? `: ${viewing.note}` : ""}`}<InfoLink feature="liveConfig.versions" /></> : ""}>
        {viewing ? (
          <Table variant="embedded" items={diff({}, viewing.overlay)} trackBy={(r) => `${r.agent}.${r.field}`}
            columnDefinitions={[
              { id: "agent", header: "Agent", cell: (r) => r.agent },
              { id: "field", header: "Setting", cell: (r) => FIELD_LABEL[r.field] },
              { id: "value", header: "Value", cell: (r) => <Box variant="code">{shown(r.draft)}</Box> },
            ]} />
        ) : null}
      </Modal>
    </SpaceBetween>
  );
}
