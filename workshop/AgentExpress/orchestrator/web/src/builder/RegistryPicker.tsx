/** Add from registry: what an organization approved in an AWS Agent Registry (MCP
 *  servers, agents, skills), found by search and taken into this build (bff/registry.py).
 *  The only registry is picked by itself; with several, the author picks. Each item can
 *  be kept in sync with its record (refreshed when the build is opened and before each
 *  deploy) or taken once (the Builder says when a newer version exists). */
import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Checkbox from "@cloudscape-design/components/checkbox";
import FormField from "@cloudscape-design/components/form-field";
import Input from "@cloudscape-design/components/input";
import Modal from "@cloudscape-design/components/modal";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import StatusIndicator from "@cloudscape-design/components/status-indicator";
import Table from "@cloudscape-design/components/table";
import { useEffect, useState } from "react";

import type { Entry, Project } from "./model";
import { registryApi, type Registry, type RegistryHit, type RegistryKind, type RegistryUpdate } from "./storage";

const MAP: Record<RegistryKind, "tools" | "agents" | "skills"> = { tool: "tools", agent: "agents", skill: "skills" };
const LABEL: Record<RegistryKind, string> = { tool: "MCP servers", agent: "agents", skill: "skills" };

/** A key not yet used in that map: "orders", then "orders2", ... */
function freeKey(taken: Record<string, unknown>, key: string): string {
  if (!(key in taken)) return key;
  let n = 2;
  while (`${key.slice(0, 30)}${n}` in taken) n += 1;
  return `${key.slice(0, 30)}${n}`;
}

/** The project with these records added (only those it can use), and their keys. */
export function addHits(project: Project, hits: RegistryHit[], sync: boolean): { project: Project; added: string[] } {
  const wf = { ...project.workflow } as Record<string, unknown>;
  const added: string[] = [];
  for (const h of hits) {
    if (!h.kind || !h.entry || !h.key) continue;
    const m = MAP[h.kind];
    const map = { ...((wf[m] ?? {}) as Record<string, Entry>) };
    const key = freeKey(map, h.key);
    const entry = structuredClone(h.entry);
    entry.registry = { ...(entry.registry as Record<string, unknown>), sync } as Entry[string];
    if (h.kind === "agent") entry.produces = key;
    map[key] = entry;
    wf[m] = map;
    added.push(key);
  }
  return { project: { ...project, workflow: wf as Project["workflow"] }, added };
}

/** What a newer version replaces; the rest stays the build's (bff/registry.py _TAKES). */
const TAKES: Record<RegistryUpdate["map"], string[]> = {
  tools: ["description", "endpoint", "toolSchema", "registry"],
  agents: ["agentCard", "registry"],
  skills: ["description", "instructions", "registry"],
};
export function applyUpdate(project: Project, u: RegistryUpdate): Project {
  const wf = { ...project.workflow } as Record<string, unknown>;
  const map = { ...((wf[u.map] ?? {}) as Record<string, Entry>) };
  const cur = map[u.key];
  if (!cur) return project;
  const next: Entry = { ...cur };
  for (const k of TAKES[u.map]) {
    if (k in u.entry) next[k] = structuredClone(u.entry[k]);
    else if (k !== "registry") delete next[k];
  }
  map[u.key] = next;
  wf[u.map] = map;
  return { ...project, workflow: wf as Project["workflow"] };
}

export function RegistryPicker({ kind, visible, onDismiss, onAdd }: {
  kind: RegistryKind; visible: boolean; onDismiss: () => void;
  onAdd: (hits: RegistryHit[], sync: boolean) => void;
}) {
  const [registries, setRegistries] = useState<Registry[] | null>(null);
  const [picked, setPicked] = useState("");
  const [q, setQ] = useState("");
  const [hits, setHits] = useState<RegistryHit[] | null>(null);
  const [selected, setSelected] = useState<RegistryHit[]>([]);
  const [sync, setSync] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const run = async (registry: string, query: string) => {
    setBusy(true); setError("");
    try {
      setHits(await registryApi.search(registry, query, kind));
      setSelected([]);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };
  useEffect(() => {
    if (!visible) return;
    setHits(null); setSelected([]); setQ(""); setError(""); setSync(false);
    registryApi.registries().then((rs) => {
      setRegistries(rs);
      // The only one is used without asking; with several, the first READY one to start.
      const first = rs.find((r) => r.status === "READY") ?? rs[0];
      if (first) { setPicked(first.id); void run(first.id, ""); }
    }, (e) => { setRegistries([]); setError((e as Error).message); });
  }, [visible, kind]); // eslint-disable-line react-hooks/exhaustive-deps

  const usable = (selected ?? []).filter((h) => h.entry);
  const options = (registries ?? []).map((r) => ({ value: r.id, label: r.name, description: r.status === "READY" ? r.description : r.status }));
  const current = (registries ?? []).find((r) => r.id === picked);
  return (
    <Modal visible={visible} onDismiss={onDismiss} size="max" header={`Add ${LABEL[kind]} from the registry`}
      footer={<Box float="right"><SpaceBetween direction="horizontal" size="xs">
        <Button variant="link" onClick={onDismiss}>Cancel</Button>
        <Button variant="primary" disabled={!usable.length} onClick={() => onAdd(usable, sync)}>
          Add{usable.length ? ` (${usable.length})` : ""}</Button>
      </SpaceBetween></Box>}>
      <SpaceBetween size="m">
        {registries === null ? <StatusIndicator type="loading">Looking for registries</StatusIndicator> : null}
        {registries && !registries.length && !error ? (
          <Alert type="info" header="No Agent Registry here">
            This console&apos;s AWS account has no AWS Agent Registry in this region. Create one in the AgentCore console
            (Registry), publish and approve records in it, then add them here.
          </Alert>
        ) : null}
        {registries && registries.length > 1 ? (
          <FormField label="Registry">
            <Select selectedOption={options.find((o) => o.value === picked) ?? null} options={options} ariaLabel="Registry"
              onChange={({ detail }) => { setPicked(String(detail.selectedOption.value)); void run(String(detail.selectedOption.value), q); }} />
          </FormField>
        ) : null}
        {registries && registries.length === 1 && current ? (
          <Box variant="small" color="text-body-secondary">Registry: <b>{current.name}</b>. Only approved records are shown.</Box>
        ) : null}
        {current ? (
          <form onSubmit={(e) => { e.preventDefault(); void run(picked, q); }}>
            <SpaceBetween direction="horizontal" size="xs">
              <Input type="search" value={q} onChange={({ detail }) => setQ(detail.value)} ariaLabel="Search the registry"
                placeholder={kind === "skill" ? "e.g. refund policy" : kind === "agent" ? "e.g. order questions" : "e.g. orders"} />
              <Button formAction="submit" loading={busy}>Search</Button>
            </SpaceBetween>
          </form>
        ) : null}
        {error ? <Alert type="error">{error}</Alert> : null}
        {hits ? (
          <Table items={hits} trackBy="recordId" selectionType="multi" selectedItems={selected}
            isItemDisabled={(h) => !h.entry}
            onSelectionChange={({ detail }) => setSelected(detail.selectedItems)}
            ariaLabels={{ selectionGroupLabel: "Records to add", itemSelectionLabel: (_, h) => `Select ${h.name}`,
              allItemsSelectionLabel: () => "Select all" }}
            empty={<Box textAlign="center">{q ? "Nothing approved matches that." : `No approved ${LABEL[kind]} in this registry.`}</Box>}
            columnDefinitions={[
              { id: "name", header: "Name", isRowHeader: true, cell: (h) => h.displayName || h.name },
              { id: "version", header: "Version", cell: (h) => h.version },
              { id: "about", header: "Description", cell: (h) => <span className="axb-clamp" title={h.description}>{h.description || "—"}</span> },
              { id: "what", header: "Becomes", cell: (h) => (h.entry
                ? <StatusIndicator type="success">{`${kind === "tool" ? "An MCP tool" : kind === "agent" ? "A remote agent (A2A)" : "A skill"} "${h.key}"`}
                  {h.tools?.length ? ` · tools: ${h.tools.join(", ")}` : ""}</StatusIndicator>
                : <StatusIndicator type="warning">{`Cannot add: ${h.why}`}</StatusIndicator>) },
            ]} />
        ) : null}
        {current ? (
          <Checkbox checked={sync} onChange={({ detail }) => setSync(detail.checked)}
            description="Refreshed to each newer approved version when the build is opened and before each deploy. Off: taken as it is now, and you are told when a newer version exists.">
            Keep in sync with the registry
          </Checkbox>
        ) : null}
        {kind === "tool" ? (
          <Box variant="small" color="text-body-secondary">Choose how each one connects on the tool afterwards (How it connects).</Box>
        ) : null}
      </SpaceBetween>
    </Modal>
  );
}
