/** The app's Memory page (admins): what agents remembered across runs (AgentCore Memory
 *  long-term records), searched and deleted one at a time (bff/memories.py).
 *
 *  Record text is model output extracted from earlier runs, so it is shown as TEXT: React
 *  escapes it, and nothing here renders it as HTML or markdown. */
import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import Input from "@cloudscape-design/components/input";
import Modal from "@cloudscape-design/components/modal";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Spinner from "@cloudscape-design/components/spinner";
import Table from "@cloudscape-design/components/table";
import { useCallback, useEffect, useState } from "react";

import { api } from "../api";
import { useDocsBase } from "../docs/docsLinks";
import { InfoLink, infoSlot } from "../docs/InfoLink";

export interface MemoryAgent { id: string; name: string; memory: string; kinds: string[] }
export interface MemoryNamespace { namespace: string; actor: string; kind: string; records: number }
export interface MemoryRecord { id: string; text: string; createdAt: string; strategyId: string; namespace: string; score?: number }

/** Longest search the server accepts (bff/memories.py QUERY_MAX). */
export const QUERY_MAX = 1000;

const qs = (o: Record<string, string>) => new URLSearchParams(o).toString();

/** "a short excerpt…" of a record, for the delete confirmation. */
export function excerpt(text: string, max = 160): string {
  const t = text.replace(/\s+/g, " ").trim();
  return t.length > max ? `${t.slice(0, max - 1)}…` : t;
}

export function MemoryPage({ notify }: { notify: (type: "success" | "error" | "info", text: string) => void }) {
  const docs = useDocsBase();
  const [agents, setAgents] = useState<MemoryAgent[] | null>(null);
  const [deployed, setDeployed] = useState(true);
  const [agent, setAgent] = useState("");
  const [spaces, setSpaces] = useState<MemoryNamespace[] | null>(null);
  const [truncated, setTruncated] = useState(false);
  const [namespace, setNamespace] = useState("");
  const [rows, setRows] = useState<MemoryRecord[] | null>(null);
  const [next, setNext] = useState<string | null>(null);
  const [query, setQuery] = useState("");
  const [searched, setSearched] = useState(false);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");
  const [doomed, setDoomed] = useState<MemoryRecord | null>(null);

  useEffect(() => {
    api.get<{ agents: MemoryAgent[]; deployed: boolean }>("/api/memory/agents").then(
      (r) => { setAgents(r.agents); setDeployed(r.deployed); },
      (e) => { setError((e as Error).message); setAgents([]); });
  }, []);

  const pickAgent = useCallback(async (id: string) => {
    setAgent(id); setSpaces(null); setNamespace(""); setRows(null); setNext(null); setSearched(false); setError("");
    if (!id) return;
    setBusy("namespaces");
    try {
      const r = await api.get<{ namespaces: MemoryNamespace[]; truncated: boolean }>(`/api/memory/namespaces?${qs({ agent: id })}`);
      setSpaces(r.namespaces); setTruncated(r.truncated);
    } catch (e) { setError((e as Error).message); } finally { setBusy(""); }
  }, []);

  const load = useCallback(async (ns: string, token: string | null) => {
    setBusy("records"); setError("");
    try {
      const r = await api.get<{ records: MemoryRecord[]; next: string | null }>(
        `/api/memory/records?${qs({ agent, namespace: ns, ...(token ? { next: token } : {}) })}`);
      setRows((old) => (token && old ? [...old, ...r.records] : r.records)); setNext(r.next);
    } catch (e) { setError((e as Error).message); } finally { setBusy(""); }
  }, [agent]);

  const pickNamespace = (ns: string) => {
    setNamespace(ns); setRows(null); setNext(null); setSearched(false); setQuery("");
    if (ns) void load(ns, null);
  };

  async function search() {
    setBusy("search"); setError("");
    try {
      const r = await api.post<{ records: MemoryRecord[] }>("/api/memory/search", { agent, namespace, query: query.trim() });
      setRows(r.records); setNext(null); setSearched(true);
    } catch (e) { setError((e as Error).message); } finally { setBusy(""); }
  }

  async function remove(r: MemoryRecord) {
    setBusy("delete");
    try {
      await api.del(`/api/memory/records/${encodeURIComponent(r.id)}?${qs({ agent, namespace })}`);
      setRows((old) => (old ?? []).filter((x) => x.id !== r.id));
      setSpaces((old) => (old ?? []).map((s) => (s.namespace === namespace ? { ...s, records: Math.max(0, s.records - 1) } : s)));
      notify("success", `Memory record ${r.id} deleted.`);
      setDoomed(null);
    } catch (e) { notify("error", `Could not delete the record: ${(e as Error).message}`); } finally { setBusy(""); }
  }

  const agentOptions = (agents ?? []).map((a) => ({ value: a.id, label: a.name, description: `${a.id} · memory ${a.memory}` }));
  const spaceOptions = (spaces ?? []).map((s) => ({ value: s.namespace, label: s.namespace,
    description: `${s.kind} · ${s.records} record${s.records === 1 ? "" : "s"}` }));
  const tooLong = query.trim().length > QUERY_MAX;

  return (
    <SpaceBetween size="l">
      <Header variant="h1" description={"What agents remembered across runs (long-term memory). Shown to admins only; "
        + "every look and every delete is in the activity log."} info={infoSlot(docs, "memoryRecords")}>Memory records</Header>
      {error ? <Alert type="error">{error}</Alert> : null}
      {agents === null ? <Spinner size="large" /> : null}
      {agents !== null && !deployed ? <Box color="text-body-secondary">This deployment has no long-term memory.</Box> : null}
      {agents !== null && deployed && agents.length === 0
        ? <Box color="text-body-secondary">No agent here keeps long-term memory.</Box> : null}
      {agents?.length && deployed ? (
        <SpaceBetween direction="horizontal" size="m">
          <FormField label="Agent">
            <Select ariaLabel="Agent" placeholder="Choose an agent"
              selectedOption={agentOptions.find((o) => o.value === agent) ?? null} options={agentOptions}
              onChange={({ detail }) => void pickAgent(detail.selectedOption.value ?? "")} />
          </FormField>
          {agent ? (
            <FormField label="Namespace" description={truncated ? "Only the first records were read; some namespaces may be missing." : undefined}>
              <Select ariaLabel="Namespace" placeholder={busy === "namespaces" ? "Loading…" : "Choose a namespace"}
                statusType={busy === "namespaces" ? "loading" : "finished"}
                empty="No records for this agent yet."
                selectedOption={spaceOptions.find((o) => o.value === namespace) ?? null} options={spaceOptions}
                onChange={({ detail }) => pickNamespace(detail.selectedOption.value ?? "")} />
            </FormField>
          ) : null}
        </SpaceBetween>
      ) : null}
      {namespace ? (
        <Table
          variant="container"
          loading={busy === "records" || busy === "search"}
          loadingText="Loading records"
          items={rows ?? []}
          header={
            <Header variant="h2" counter={rows ? `(${rows.length}${next ? "+" : ""})` : undefined} info={infoSlot(docs, "memoryRecords.namespace")}
              description={searched ? "Search results, most relevant first." : undefined}
              actions={
                <SpaceBetween direction="horizontal" size="xs">
                  <Input type="search" value={query} placeholder="Search this namespace" ariaLabel="Search memory"
                    invalid={tooLong} onChange={({ detail }) => setQuery(detail.value)}
                    onKeyDown={({ detail }) => { if (detail.key === "Enter" && query.trim() && !tooLong) void search(); }} />
                  <Button disabled={!query.trim() || tooLong} onClick={() => void search()}>Search</Button>
                  {searched ? <Button onClick={() => { setQuery(""); setSearched(false); void load(namespace, null); }}>Clear search</Button> : null}
                </SpaceBetween>
              }>{namespace}</Header>
          }
          empty={<Box color="text-body-secondary">{searched ? "Nothing matches." : "No records in this namespace."}</Box>}
          columnDefinitions={[
            { id: "text", header: "Record", cell: (r) => <span style={{ whiteSpace: "pre-wrap" }}>{r.text}</span> },
            { id: "created", header: "Created", cell: (r) => r.createdAt },
            { id: "strategy", header: "Strategy", cell: (r) => r.strategyId },
            ...(searched ? [{ id: "score", header: "Score", cell: (r: MemoryRecord) => (r.score ?? 0).toFixed(2) }] : []),
            { id: "delete", header: "", cell: (r) => (
              <Button variant="icon" iconName="remove" ariaLabel={`Delete memory record ${r.id}`} onClick={() => setDoomed(r)} />) },
          ]}
          footer={next && !searched ? (
            <Box textAlign="center"><Button loading={busy === "records"} onClick={() => void load(namespace, next)}>Load more</Button></Box>
          ) : undefined}
        />
      ) : null}
      {tooLong ? <Box color="text-status-error">A search is at most {QUERY_MAX} characters.</Box> : null}
      <Modal visible={doomed !== null} onDismiss={() => setDoomed(null)} header={<>Delete memory record<InfoLink feature="memoryRecords.delete" /></>}
        footer={<Box float="right"><SpaceBetween direction="horizontal" size="xs">
          <Button variant="link" onClick={() => setDoomed(null)}>Cancel</Button>
          <Button variant="primary" loading={busy === "delete"} onClick={() => doomed && void remove(doomed)}>Delete</Button>
        </SpaceBetween></Box>}>
        {doomed ? (
          <SpaceBetween size="s">
            <Box>Delete <Box variant="code">{doomed.id}</Box> from {doomed.namespace}? This cannot be undone.</Box>
            <Box variant="small" color="text-body-secondary">“{excerpt(doomed.text)}”</Box>
            <Box variant="small" color="text-body-secondary">
              The agent can learn the same fact again if a later run says it again.</Box>
          </SpaceBetween>
        ) : null}
      </Modal>
    </SpaceBetween>
  );
}
