/** Publish to registry (admins): the deployed build (its workflow, and its Gateway's tools
 *  as an MCP server) or its skills, submitted for approval in an AWS Agent Registry. The
 *  console never approves its own records: a curator does, in the registry. */
import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import FormField from "@cloudscape-design/components/form-field";
import Modal from "@cloudscape-design/components/modal";
import Multiselect from "@cloudscape-design/components/multiselect";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import StatusIndicator, { type StatusIndicatorProps } from "@cloudscape-design/components/status-indicator";
import Table from "@cloudscape-design/components/table";
import { useEffect, useState } from "react";

import { registryApi, type Registry, type RegistryPublished } from "./storage";

type Rec = { recordId: string; name: string; version?: string; status?: string; statusReason?: string };
const SHOWN: Record<string, [StatusIndicatorProps.Type, string]> = {
  DRAFT: ["pending", "Draft"], CREATING: ["loading", "Creating"], UPDATING: ["loading", "Updating"],
  PENDING_APPROVAL: ["pending", "Waiting for approval"], APPROVED: ["success", "Approved"],
  REJECTED: ["error", "Rejected"], DEPRECATED: ["stopped", "Deprecated"], MISSING: ["warning", "Not in the registry"],
  CREATE_FAILED: ["error", "Refused"], UPDATE_FAILED: ["error", "Refused"],
};
const SETTLE_MS = 3000;
export function RecordStatus({ status, reason }: { status?: string; reason?: string }) {
  const [type, label] = SHOWN[String(status)] ?? ["info", String(status ?? "—")];
  return <StatusIndicator type={type}>{label}{reason ? `: ${reason}` : ""}</StatusIndicator>;
}

export function RegistryPublish({ visible, buildId, what, skills, preselect, version, onDismiss, notify }: {
  visible: boolean; buildId: string; what: "build" | "skill";
  /** The build's skills (for what "skill"), and the one to start with. */
  skills?: string[]; preselect?: string;
  /** The deployed version a build publish sends. */
  version?: number;
  onDismiss: () => void;
  notify: (type: "success" | "error" | "info", msg: string) => void;
}) {
  const [registries, setRegistries] = useState<Registry[] | null>(null);
  const [picked, setPicked] = useState("");
  const [chosen, setChosen] = useState<string[]>([]);
  const [published, setPublished] = useState<RegistryPublished>({});
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  useEffect(() => {
    if (!visible) return;
    setError(""); setChosen(preselect ? [preselect] : []);
    registryApi.registries().then((rs) => {
      setRegistries(rs);
      setPicked((p) => p || (rs.find((r) => r.status === "READY") ?? rs[0])?.id || "");
    }, (e) => { setRegistries([]); setError((e as Error).message); });
    registryApi.state(buildId).then((s) => setPublished(s.published ?? {}), () => setPublished({}));
  }, [visible, buildId, preselect]);

  const go = async () => {
    setBusy(true); setError("");
    try {
      let last: RegistryPublished = published;
      if (what === "build") last = await registryApi.publish(buildId, { registry: picked, what: "build" });
      else for (const s of chosen) last = await registryApi.publish(buildId, { registry: picked, what: "skill", skill: s });
      setPublished(last);
      notify("success", what === "build" ? `Version ${version ?? ""} submitted for approval in the registry.`
        : `Submitted for approval: ${chosen.join(", ")}.`);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };
  const rows: (Rec & { key: string })[] = Object.entries((what === "build" ? published.records : (published as { skills?: Record<string, Rec> }).skills) ?? {})
    .map(([key, r]) => ({ key, ...(r as Rec) }));
  // The registry creates and updates records asynchronously: while one is still settling,
  // look again every few seconds so the table ends on its real status and version.
  const settling = rows.some((r) => r.status === "CREATING" || r.status === "UPDATING");
  useEffect(() => {
    if (!visible || !settling || busy) return;
    const t = setTimeout(() => {
      registryApi.state(buildId).then((s) => setPublished(s.published ?? {}), () => undefined);
    }, SETTLE_MS);
    return () => clearTimeout(t);
  }, [visible, settling, busy, buildId, published]);
  const options = (registries ?? []).map((r) => ({ value: r.id, label: r.name, description: r.status }));
  const ready = Boolean(picked) && (what === "build" || chosen.length > 0);
  return (
    <Modal visible={visible} onDismiss={onDismiss} size="large"
      header={what === "build" ? "Publish this build to the registry" : "Publish skills to the registry"}
      footer={<Box float="right"><SpaceBetween direction="horizontal" size="xs">
        <Button variant="link" onClick={onDismiss}>Close</Button>
        <Button variant="primary" loading={busy} disabled={!ready} onClick={() => void go()}>Submit for approval</Button>
      </SpaceBetween></Box>}>
      <SpaceBetween size="m">
        {registries && !registries.length && !error ? (
          <Alert type="info" header="No Agent Registry here">Create one in the AgentCore console (Registry) first.</Alert>
        ) : null}
        {registries && registries.length > 1 ? (
          <FormField label="Registry">
            <Select selectedOption={options.find((o) => o.value === picked) ?? null} options={options} ariaLabel="Registry"
              onChange={({ detail }) => setPicked(String(detail.selectedOption.value))} />
          </FormField>
        ) : registries?.length === 1 ? <Box variant="small" color="text-body-secondary">Registry: <b>{registries[0].name}</b></Box> : null}
        {what === "build" ? (
          <Box>
            Publishes version {version ?? "?"}: the workflow (an agent record naming its app) and, when it has one, its
            Gateway&apos;s tools (an MCP server record). Others find them once a curator approves them. Publishing again
            submits the next version; destroying the build deprecates them.
          </Box>
        ) : (
          <FormField label="Skills" description="Each is published as its SKILL.md. Reference files stay in this build.">
            <Multiselect selectedOptions={chosen.map((s) => ({ value: s, label: s }))} ariaLabel="Skills to publish"
              options={(skills ?? []).map((s) => ({ value: s, label: s }))}
              onChange={({ detail }) => setChosen(detail.selectedOptions.map((o) => String(o.value)))} />
          </FormField>
        )}
        {error ? <Alert type="error">{error}</Alert> : null}
        {rows.length ? (
          <Table items={rows} trackBy="key" variant="embedded" header={<Box variant="h4">In the registry</Box>}
            columnDefinitions={[
              { id: "what", header: "What", isRowHeader: true, cell: (r) => (what === "build"
                ? (r.key === "gateway" ? "Its tools (MCP server)" : "The workflow") : r.key) },
              { id: "name", header: "Record", cell: (r) => r.name },
              { id: "version", header: "Version", cell: (r) => r.version ?? "—" },
              { id: "status", header: "Status", cell: (r) => <RecordStatus status={r.status} reason={r.statusReason} /> },
            ]} />
        ) : null}
      </SpaceBetween>
    </Modal>
  );
}
