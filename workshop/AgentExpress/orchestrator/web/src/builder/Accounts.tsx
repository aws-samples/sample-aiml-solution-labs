/** Your connected AWS accounts: where your builds can deploy besides this console's own
 *  account. Each has a name you give it and a default region; the Deploy dialog lists
 *  the connected ones to pick from, with any region.
 *
 *  Private to you, like your builds (bff/accounts.py). Disconnecting is refused while a
 *  build of yours is deployed there; destroy those first. */
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import ButtonDropdown from "@cloudscape-design/components/button-dropdown";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import Input from "@cloudscape-design/components/input";
import Modal from "@cloudscape-design/components/modal";
import SpaceBetween from "@cloudscape-design/components/space-between";
import StatusIndicator from "@cloudscape-design/components/status-indicator";
import Table from "@cloudscape-design/components/table";
import { useCallback, useEffect, useState } from "react";

import { ConnectAccountModal, RegionField } from "./DeployPanel";
import { accountName, accounts, type Account } from "./storage";

const at = (ts?: string) => (ts ? new Date(ts).toLocaleString() : "—");

function EditAccountModal({ account, onDismiss, onSaved }: {
  account: Account | null; onDismiss: () => void; onSaved: (a: Account) => void;
}) {
  const [label, setLabel] = useState("");
  const [region, setRegion] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    setLabel(account?.label ?? "");
    setRegion(account?.region ?? "");
    setError(null);
  }, [account]);
  const save = async () => {
    if (!account) return;
    setBusy(true);
    try { onSaved(await accounts.update(account.accountId, { label: label.trim(), region: region.trim() })); }
    catch (e) { setError((e as Error).message); }
    finally { setBusy(false); }
  };
  return (
    <Modal visible={Boolean(account)} onDismiss={onDismiss} header={`Edit AWS account ${account?.accountId ?? ""}`}
      footer={
        <Box float="right">
          <SpaceBetween direction="horizontal" size="xs">
            <Button variant="link" onClick={onDismiss} disabled={busy}>Cancel</Button>
            <Button variant="primary" loading={busy} disabled={label.length > 64 || !region} onClick={() => void save()}>Save</Button>
          </SpaceBetween>
        </Box>
      }>
      <SpaceBetween size="m">
        <FormField label="Name" description="How it shows in lists and in the Deploy dialog."
          errorText={label.length > 64 ? "At most 64 characters" : undefined}>
          <Input value={label} onChange={({ detail }) => setLabel(detail.value)} placeholder="Team sandbox" />
        </FormField>
        <RegionField value={region} onChange={setRegion} label="Default region"
          description="Where a new deploy goes unless you pick another. A build already deployed stays in its region." />
        {error ? <Box color="text-status-error">{error}</Box> : null}
      </SpaceBetween>
    </Modal>
  );
}

export function Accounts({ canConnect, notify }: {
  /** The `deploy` permission: connecting grants a deploy role, so it takes the same. */
  canConnect: boolean;
  notify: (type: "success" | "error" | "info", text: string) => void;
}) {
  const [items, setItems] = useState<Account[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [selected, setSelected] = useState<Account[]>([]);
  const [connecting, setConnecting] = useState(false);
  const [resume, setResume] = useState<Account | null>(null);
  const [editing, setEditing] = useState<Account | null>(null);
  const [removing, setRemoving] = useState<Account | null>(null);
  const [busy, setBusy] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const list = await accounts.list();
      setItems(list);
      setSelected((s) => list.filter((a) => s.some((x) => x.accountId === a.accountId)));
    } catch (e) {
      setError((e as Error).message);
      setItems([]);
    } finally {
      setLoading(false);
    }
  }, []);
  useEffect(() => { void load(); }, [load]);

  const one = selected[0];
  const verify = async (a: Account) => {
    setBusy(true);
    try {
      await accounts.verify(a.accountId);
      notify("success", `${accountName(a)} is connected.`);
      await load();
    } catch (e) {
      notify("error", (e as Error).message);
    } finally {
      setBusy(false);
    }
  };
  const remove = async () => {
    if (!removing) return;
    setBusy(true);
    try {
      await accounts.remove(removing.accountId);
      notify("info", `Disconnected ${accountName(removing)}. Delete stack ${removing.stackName ?? "AgentExpressConnect-…"} in that account to remove its role.`);
      setRemoving(null);
      setSelected([]);
      await load();
    } catch (e) {
      notify("error", (e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <>
      <Table
        variant="full-page"
        loading={loading}
        loadingText="Loading accounts"
        items={items}
        trackBy="accountId"
        selectionType="single"
        selectedItems={selected}
        onSelectionChange={({ detail }) => setSelected(detail.selectedItems)}
        ariaLabels={{ itemSelectionLabel: (_s, a) => accountName(a), selectionGroupLabel: "Accounts" }}
        header={
          <Header
            variant="awsui-h1-sticky"
            counter={loading ? undefined : `(${items.length})`}
            description="AWS accounts of yours that builds can deploy into, so their data, cost and resources stay yours. Only you see these."
            actions={
              <SpaceBetween direction="horizontal" size="xs">
                <Button iconName="refresh" ariaLabel="Refresh accounts" onClick={() => void load()} />
                <ButtonDropdown
                  disabled={!one || busy}
                  items={[
                    { id: "edit", text: "Edit name and region" },
                    one?.status === "connected"
                      ? { id: "verify", text: "Check the connection" }
                      : { id: "finish", text: "Finish connecting", disabled: !canConnect },
                    { id: "update", text: "Update its role", disabled: !canConnect || one?.status !== "connected",
                      disabledReason: one?.status !== "connected" ? "Connect it first" : "Needs the deploy permission" },
                    { id: "remove", text: "Disconnect", disabled: Boolean(one?.builds?.length),
                      disabledReason: "Destroy the builds deployed there first" },
                  ]}
                  onItemClick={({ detail }) => {
                    if (!one) return;
                    if (detail.id === "edit") setEditing(one);
                    if (detail.id === "verify") void verify(one);
                    if (detail.id === "finish" || detail.id === "update") setResume(one);
                    if (detail.id === "remove") setRemoving(one);
                  }}>
                  Actions
                </ButtonDropdown>
                <Button variant="primary" disabled={!canConnect} onClick={() => setConnecting(true)}>
                  Connect an AWS account
                </Button>
              </SpaceBetween>
            }>
            AWS accounts
          </Header>
        }
        columnDefinitions={[
          { id: "name", header: "Name", cell: (a) => a.label || <Box color="text-body-secondary">—</Box> },
          { id: "account", header: "Account ID", cell: (a) => a.accountId },
          { id: "region", header: "Default region", cell: (a) => a.region },
          {
            id: "status", header: "Status",
            cell: (a) => a.status === "connected"
              ? <StatusIndicator type="success">Connected</StatusIndicator>
              : <StatusIndicator type="pending">Waiting for its stack</StatusIndicator>,
          },
          { id: "verified", header: "Verified", cell: (a) => at(a.verifiedAt) },
          {
            id: "builds", header: "Builds deployed",
            cell: (a) => a.builds?.length
              ? a.builds.map((b) => `${b.name} (${b.region})`).join(", ")
              : <Box color="text-body-secondary">None</Box>,
          },
        ]}
        empty={
          <Box textAlign="center" color="inherit">
            {error ? <StatusIndicator type="error">{error}</StatusIndicator> : (
              <SpaceBetween size="s">
                <Box>No accounts connected. Builds deploy to this console&apos;s own account until you connect one.</Box>
                <Button disabled={!canConnect} onClick={() => setConnecting(true)}>Connect an AWS account</Button>
              </SpaceBetween>
            )}
          </Box>
        }
      />
      <ConnectAccountModal visible={connecting || Boolean(resume)} resume={resume}
        onDismiss={() => { setConnecting(false); setResume(null); void load(); }}
        onConnected={(a) => {
          setConnecting(false);
          setResume(null);
          notify("success", `${accountName(a)} is connected.`);
          void load();
        }} />
      <EditAccountModal account={editing} onDismiss={() => setEditing(null)}
        onSaved={(a) => { setEditing(null); notify("success", `Saved ${accountName(a)}.`); void load(); }} />
      <Modal visible={Boolean(removing)} onDismiss={() => setRemoving(null)} header="Disconnect this account?"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setRemoving(null)} disabled={busy}>Cancel</Button>
              <Button variant="primary" loading={busy} onClick={() => void remove()}>Disconnect</Button>
            </SpaceBetween>
          </Box>
        }>
        {removing ? (
          <Box>
            This console forgets {accountName(removing)} and can no longer deploy there. The role stays in that account
            until you delete stack <b>{removing.stackName ?? "AgentExpressConnect-…"}</b> in it.
          </Box>
        ) : null}
      </Modal>
    </>
  );
}
