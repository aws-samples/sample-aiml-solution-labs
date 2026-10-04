/** Groups of email addresses anyone can share a build or a library item with. Only an
 *  admin defines them (bff/sharing.py); shown on the Admin page. */
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import Input from "@cloudscape-design/components/input";
import Link from "@cloudscape-design/components/link";
import Modal from "@cloudscape-design/components/modal";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Table from "@cloudscape-design/components/table";
import Textarea from "@cloudscape-design/components/textarea";
import { useCallback, useEffect, useState } from "react";
import { groups as groupsApi, type Group } from "./storage";

export function Groups({ notify }: { notify: (type: "success" | "error" | "info", text: string) => void }) {
  const [items, setItems] = useState<Group[] | null>(null);
  const [editing, setEditing] = useState<Group | "new" | null>(null);
  const [name, setName] = useState("");
  const [members, setMembers] = useState("");
  const [busy, setBusy] = useState(false);
  const load = useCallback(() => {
    groupsApi.list().then(setItems, (e) => { notify("error", (e as Error).message); setItems([]); });
  }, [notify]);
  useEffect(load, [load]);
  useEffect(() => {
    if (!editing) return;
    setName(editing === "new" ? "" : editing.name);
    setMembers(editing === "new" ? "" : (editing.members ?? []).join("\n"));
  }, [editing]);
  const save = async () => {
    setBusy(true);
    try {
      await groupsApi.put(name.trim(), members.split(/[\s,;]+/).map((m) => m.trim()).filter(Boolean));
      notify("success", `Group ${name} saved.`);
      setEditing(null);
      load();
    } catch (e) {
      notify("error", (e as Error).message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <SpaceBetween size="m">
      <Table items={items ?? []} loading={items === null} loadingText="Loading groups" trackBy="name"
        header={<Header variant="h3" description="Anyone can share a build or a library item with a group; only an admin changes who is in it."
          actions={<Button onClick={() => setEditing("new")}>New group</Button>}>Groups</Header>}
        columnDefinitions={[
          { id: "name", header: "Group", isRowHeader: true, cell: (g) => <Link href="#" onFollow={(e) => { e.preventDefault(); setEditing(g); }}>{g.name}</Link> },
          { id: "members", header: "Members", cell: (g) => (g.members ?? []).join(", ") || "—" },
          { id: "actions", header: "", cell: (g) => (
            <Button variant="icon" iconName="remove" ariaLabel={`Delete group ${g.name}`} onClick={() => {
              if (!window.confirm(`Delete group ${g.name}? What was shared with it is not any more.`)) return;
              groupsApi.remove(g.name).then(() => { notify("success", `Deleted ${g.name}.`); load(); }, (e) => notify("error", (e as Error).message));
            }} />) },
        ]}
        empty={<Box color="text-body-secondary">No groups yet.</Box>} />
      <Modal visible={editing !== null} onDismiss={() => setEditing(null)} header={editing === "new" ? "New group" : `Group ${name}`}
        footer={<Box float="right"><SpaceBetween direction="horizontal" size="xs">
          <Button variant="link" onClick={() => setEditing(null)}>Cancel</Button>
          <Button variant="primary" loading={busy} disabled={!name.trim()} onClick={() => void save()}>Save</Button>
        </SpaceBetween></Box>}>
        <SpaceBetween size="m">
          <FormField label="Name">
            <Input value={name} disabled={editing !== "new"} onChange={({ detail }) => setName(detail.value)} ariaLabel="Group name" />
          </FormField>
          <FormField label="Members" description="Email addresses, one per line.">
            <Textarea value={members} rows={6} onChange={({ detail }) => setMembers(detail.value)} ariaLabel="Members" />
          </FormField>
        </SpaceBetween>
      </Modal>
    </SpaceBetween>
  );
}
