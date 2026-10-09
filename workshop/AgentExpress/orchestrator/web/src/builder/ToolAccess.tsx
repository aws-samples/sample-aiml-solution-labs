/** Identity > Tool access: every tool and how it connects, in one table, with its saved
 *  logins below. Read here, changed on the tool (ToolConnect): one place sets it. */
import Button from "@cloudscape-design/components/button";
import Header from "@cloudscape-design/components/header";
import SpaceBetween from "@cloudscape-design/components/space-between";
import StatusIndicator from "@cloudscape-design/components/status-indicator";
import Table from "@cloudscape-design/components/table";
import { useEffect, useState, type ReactNode } from "react";

import type { Entry, Json, Project } from "./model";
import { buildSecrets, type SecretNames } from "./storage";
import { connectionOf, fixedOf, iamNote, METHOD } from "./ToolConnect";

type Row = { key: string; connects: string; sees: string; status: "ready" | "setup" | "unknown"; why?: string;
  /** For an IAM role: which role, on hover. */ tip?: string };

/** Ready, or what is still missing; "unknown" without the server's secret list. */
export function toolRows(project: Project, secrets: SecretNames | null, agentName?: string): Row[] {
  return Object.entries(project.workflow.tools ?? {}).map(([key, raw]) => {
    const tool = raw as Entry & { type?: string; oauth?: Record<string, Json> };
    const c = connectionOf(tool, project);
    if (c.method === "fixed") {
      return { key, connects: fixedOf(String(tool.type ?? "")), sees: "This app", status: "ready", tip: iamNote(tool, agentName) };
    }
    const m = METHOD[c.method];
    const row: Row = { key, connects: m.label,
      sees: m.sees === "person" ? "The person" : "This app", status: "ready" };
    if (c.method === "aws") return { ...row, tip: iamNote(tool, agentName) };
    if (c.method === "none") return row;
    if (!secrets) return { ...row, status: "unknown" };
    if (c.login) {
      return (secrets.identitySecrets ?? []).includes(c.login) ? row
        : { ...row, status: "setup", why: `Set the secret of ${c.login}` };
    }
    if (c.method === "app" || c.method === "user" || c.method === "obo") {
      const o = tool.oauth ?? {};
      if (!o.clientId || !(o.discoveryUrl || o.tokenUrl)) return { ...row, status: "setup", why: "Add the provider address and client ID" };
      if (c.method === "user" && o.tokenUrl && !o.authorizationUrl) return { ...row, status: "setup", why: "Add the sign-in address" };
      if (c.method !== "app" && tool.type === "mcp" && !(tool as { toolSchema?: unknown }).toolSchema) {
        return { ...row, status: "setup", why: "List its tools under Tool schema" };
      }
    }
    return (secrets.toolApiKeys ?? []).includes(key) ? row
      : { ...row, status: "setup", why: c.method === "apikey" ? "Add the API key" : "Add the client secret" };
  });
}

export function ToolAccess({ project, server, onEdit, savedLogins, agentName }: {
  project: Project; server: boolean;
  /** The build's fixed AWS name: names the Gateway role in the IAM role tooltip. */
  agentName?: string;
  /** Open the tool where its connection is set. */
  onEdit: (key: string) => void;
  /** The saved logins list (Identity's named entries). */
  savedLogins: ReactNode;
}) {
  const [secrets, setSecrets] = useState<SecretNames | null>(null);
  useEffect(() => {
    if (!server) return;
    let live = true;
    buildSecrets.names(project.id).then((n) => { if (live) setSecrets(n); }, () => { if (live) setSecrets(null); });
    return () => { live = false; };
  }, [server, project.id]);
  const rows = toolRows(project, server ? secrets : null, agentName);
  return (
    <SpaceBetween size="l">
      <Table items={rows} trackBy="key" variant="container" ariaLabels={{ tableLabel: "How each tool connects" }}
        header={<Header variant="h2" description="Set on each tool. The agent never sees a key or a secret.">Tools</Header>}
        empty="No tools yet."
        columnDefinitions={[
          { id: "tool", header: "Tool", isRowHeader: true, cell: (r) => r.key },
          { id: "connects", header: "Connects with", cell: (r) => <span title={r.tip}>{r.connects}</span> },
          { id: "sees", header: "The tool sees", cell: (r) => r.sees },
          { id: "status", header: "Status", cell: (r) => (r.status === "ready" ? <StatusIndicator>Ready</StatusIndicator>
            : r.status === "setup" ? <StatusIndicator type="warning">{r.why}</StatusIndicator> : "—") },
          { id: "edit", header: "", cell: (r) => <Button variant="inline-link" ariaLabel={`Edit ${r.key}`} onClick={() => onEdit(r.key)}>Edit</Button> },
        ]} />
      <SpaceBetween size="xs">
        <Header variant="h2" description="An API key or OAuth client several tools can share. Rotate it here and every tool using it follows.">
          Saved logins</Header>
        {savedLogins}
      </SpaceBetween>
    </SpaceBetween>
  );
}
