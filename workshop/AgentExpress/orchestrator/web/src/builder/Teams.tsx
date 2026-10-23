/** Teams and their workspaces (bff/teams.py): the Teams page, the workspace switcher above
 *  the builds list, and "Move to team" on a build. An admin creates teams and names their
 *  owners; a team's owners add editors and viewers. The server enforces every rule; this
 *  page only hides what the caller could not do. */
import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import Input from "@cloudscape-design/components/input";
import Link from "@cloudscape-design/components/link";
import Modal from "@cloudscape-design/components/modal";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Table from "@cloudscape-design/components/table";
import { useCallback, useEffect, useState } from "react";
import { api } from "../api";
import { currentWorkspace, moveBuild, setWorkspace, type BuildSummary } from "./storage";
import { useDocsBase } from "../docs/docsLinks";
import { InfoLink, infoSlot } from "../docs/InfoLink";

export type Role = "owner" | "editor" | "viewer";
export interface TeamMember { email: string; role: Role }
export interface TeamGroup { name: string; role: Role }
export interface Team {
  id: string;
  name: string;
  members: TeamMember[];
  groups: TeamGroup[];
  /** The caller's role in it; "" for an admin who is not in it. */
  role?: Role | "";
  updatedAt?: string;
}
type Notify = (type: "success" | "error" | "info", text: string) => void;

const enc = encodeURIComponent;
export const teamsApi = {
  mine: () => api.get<Team[]>("/api/teams"),
  all: () => api.get<Team[]>("/api/teams?scope=all"),
  create: (t: Pick<Team, "name" | "members" | "groups">) => api.post<Team>("/api/teams", t),
  update: (id: string, t: Partial<Pick<Team, "name" | "members" | "groups">>) => api.put<Team>(`/api/teams/${enc(id)}`, t),
  remove: (id: string) => api.del(`/api/teams/${enc(id)}`),
};

export const ROLE_LABELS: Record<Role, string> = {
  owner: "Owner — manages members",
  editor: "Editor — changes and deploys",
  viewer: "Viewer — looks only",
};
const roleOptions = (admin: boolean) => (Object.keys(ROLE_LABELS) as Role[])
  .map((r) => ({ value: r, label: ROLE_LABELS[r], disabled: r === "owner" && !admin }));

/** What a team page row says about who is in it: "3 members · 1 group". */
export function membersLabel(t: Pick<Team, "members" | "groups">): string {
  const m = t.members.length, g = t.groups.length;
  return [`${m} member${m === 1 ? "" : "s"}`, ...(g ? [`${g} group${g === 1 ? "" : "s"}`] : [])].join(" · ");
}

/** Rows of [who, role] with add and remove: the members by email, or the sign-in groups. */
function RoleRows<T extends { role: Role }>({ rows, setRows, keyOf, make, label, placeholder, admin }: {
  rows: T[]; setRows: (r: T[]) => void; keyOf: (r: T) => string; make: (key: string, role: Role) => T;
  label: string; placeholder: string; admin: boolean;
}) {
  return (
    <SpaceBetween size="xs">
      {rows.map((r, i) => {
        // Only an admin adds, removes or changes an owner (bff/teams.py update).
        const locked = r.role === "owner" && !admin;
        return (
          <SpaceBetween key={i} direction="horizontal" size="xs" alignItems="center">
            <Input value={keyOf(r)} placeholder={placeholder} disabled={locked} ariaLabel={`${label} ${i + 1}`}
              onChange={({ detail }) => setRows(rows.map((x, j) => (j === i ? make(detail.value, x.role) : x)))} />
            <Select ariaLabel={`Role of ${keyOf(r) || `${label} ${i + 1}`}`} disabled={locked}
              selectedOption={roleOptions(admin).find((o) => o.value === r.role) ?? null}
              options={roleOptions(admin)}
              onChange={({ detail }) => setRows(rows.map((x, j) => (j === i ? make(keyOf(x), detail.selectedOption.value as Role) : x)))} />
            <Button variant="icon" iconName="remove" disabled={locked} ariaLabel={`Remove ${keyOf(r) || `${label} ${i + 1}`}`}
              onClick={() => setRows(rows.filter((_, j) => j !== i))} />
          </SpaceBetween>
        );
      })}
      <Button iconName="add-plus" onClick={() => setRows([...rows, make("", admin && !rows.length ? "owner" : "editor")])}>
        Add {label.toLowerCase()}
      </Button>
    </SpaceBetween>
  );
}

function TeamModal({ team, admin, onDismiss, onSaved, notify }: {
  team: Team | "new"; admin: boolean; onDismiss: () => void; onSaved: () => void; notify: Notify;
}) {
  const fresh = team === "new";
  const [name, setName] = useState(fresh ? "" : team.name);
  const [members, setMembers] = useState<TeamMember[]>(fresh ? [] : team.members);
  const [groups, setGroups] = useState<TeamGroup[]>(fresh ? [] : team.groups);
  const [busy, setBusy] = useState(false);
  const canManage = admin || (!fresh && team.role === "owner");
  const save = async () => {
    setBusy(true);
    const body = {
      ...(admin ? { name: name.trim() } : {}),
      members: members.filter((m) => m.email.trim()).map((m) => ({ ...m, email: m.email.trim() })),
      groups: groups.filter((g) => g.name.trim()).map((g) => ({ ...g, name: g.name.trim() })),
    };
    try {
      if (fresh) await teamsApi.create({ name: name.trim(), members: body.members, groups: body.groups });
      else await teamsApi.update(team.id, body);
      notify("success", `Team ${name.trim()} saved.`);
      onSaved();
    } catch (e) {
      notify("error", (e as Error).message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <Modal visible onDismiss={onDismiss} size="large" header={<>{fresh ? "New team" : `Team ${team.name}`}<InfoLink feature="teams.edit" /></>}
      footer={<Box float="right"><SpaceBetween direction="horizontal" size="xs">
        <Button variant="link" onClick={onDismiss}>{canManage ? "Cancel" : "Close"}</Button>
        {canManage ? <Button variant="primary" loading={busy} disabled={!name.trim()} onClick={() => void save()}>Save</Button> : null}
      </SpaceBetween></Box>}>
      <SpaceBetween size="l">
        <FormField label="Name">
          <Input value={name} disabled={!admin} onChange={({ detail }) => setName(detail.value)} ariaLabel="Team name" />
        </FormField>
        {canManage ? (
          <SpaceBetween size="l">
            <FormField label="People" description={admin ? "By email address. An owner manages the team's editors and viewers."
              : "By email address. Only an admin adds or removes an owner."}>
              <RoleRows rows={members} setRows={setMembers} keyOf={(m) => m.email} make={(email, role) => ({ email, role })}
                label="Person" placeholder="name@example.com" admin={admin} />
            </FormField>
            <FormField label="Sign-in groups" description="Everyone in a group of your sign-in provider gets its role.">
              <RoleRows rows={groups} setRows={setGroups} keyOf={(g) => g.name} make={(n, role) => ({ name: n, role })}
                label="Group" placeholder="group name" admin={admin} />
            </FormField>
          </SpaceBetween>
        ) : (
          <Table variant="embedded" items={[...members.map((m) => ({ who: m.email, role: m.role })),
            ...groups.map((g) => ({ who: `group ${g.name}`, role: g.role }))]}
            columnDefinitions={[
              { id: "who", header: "Who", isRowHeader: true, cell: (r) => r.who },
              { id: "role", header: "Role", cell: (r) => ROLE_LABELS[r.role] },
            ]}
            empty={<Box color="text-body-secondary">Nobody yet.</Box>} />
        )}
      </SpaceBetween>
    </Modal>
  );
}

export function TeamsPage({ admin, notify }: { admin: boolean; notify: Notify }) {
  const docs = useDocsBase();
  const [items, setItems] = useState<Team[] | null>(null);
  const [editing, setEditing] = useState<Team | "new" | null>(null);
  const load = useCallback(() => {
    (admin ? teamsApi.all() : teamsApi.mine()).then(setItems, (e) => { notify("error", (e as Error).message); setItems([]); });
  }, [admin, notify]);
  useEffect(load, [load]);
  return (
    <SpaceBetween size="m">
      <Table items={items ?? []} loading={items === null} loadingText="Loading teams" trackBy="id"
        header={<Header variant="h1" info={infoSlot(docs, "teams")}
          description="A team shares a workspace of builds. Editors change and deploy them; viewers only look."
          actions={admin ? <Button onClick={() => setEditing("new")}>New team</Button> : undefined}>Teams</Header>}
        columnDefinitions={[
          { id: "name", header: "Team", isRowHeader: true,
            cell: (t) => <Link href="#" onFollow={(e) => { e.preventDefault(); setEditing(t); }}>{t.name}</Link> },
          { id: "role", header: "Your role", cell: (t) => (t.role ? ROLE_LABELS[t.role] : "—") },
          { id: "members", header: "Members", cell: (t) => membersLabel(t) },
          ...(admin ? [{ id: "actions", header: "", cell: (t: Team) => (
            <Button variant="icon" iconName="remove" ariaLabel={`Delete team ${t.name}`} onClick={() => {
              if (!window.confirm(`Delete team ${t.name}? A team that still has builds is not deleted.`)) return;
              teamsApi.remove(t.id).then(() => { notify("success", `Deleted ${t.name}.`); load(); },
                (e) => notify("error", (e as Error).message));
            }} />) }] : []),
        ]}
        empty={<Box color="text-body-secondary">{admin ? "No teams yet." : "You are not in a team yet. An admin adds you to one."}</Box>} />
      {editing ? <TeamModal team={editing} admin={admin} notify={notify}
        onDismiss={() => setEditing(null)} onSaved={() => { setEditing(null); load(); }} /> : null}
    </SpaceBetween>
  );
}

/** "My builds" or one of the caller's teams: which builds the list shows, and where a new
 *  build is created. Nothing at all for someone in no team. */
export function WorkspaceSelect() {
  const [teams, setTeams] = useState<Team[]>([]);
  const [value, setValue] = useState(currentWorkspace());
  useEffect(() => {
    teamsApi.mine().then(setTeams, () => setTeams([]));
  }, []);
  if (!teams.length) return null;
  const options = [{ value: "", label: "My builds" }, ...teams.map((t) => ({ value: t.id, label: t.name, description: t.role ? ROLE_LABELS[t.role] : undefined }))];
  return (
    <Select ariaLabel="Show the builds of" selectedOption={options.find((o) => o.value === value) ?? options[0]}
      options={options}
      onChange={({ detail }) => { const id = detail.selectedOption.value ?? ""; setValue(id); setWorkspace(id); }} />
  );
}

/** Move a build into a team's workspace (one the caller edits in), or back to its creator's
 *  own builds. */
export function MoveToTeam({ build, onDismiss, onMoved, notify }: {
  build: BuildSummary; onDismiss: () => void; onMoved: (b: BuildSummary) => void; notify: Notify;
}) {
  const [teams, setTeams] = useState<Team[] | null>(null);
  const [to, setTo] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    teamsApi.mine().then((ts) => setTeams(ts.filter((t) => t.role === "owner" || t.role === "editor")), () => setTeams([]));
  }, []);
  const options = [
    ...(build.team ? [{ value: "", label: "Its creator's own builds", description: "Only the person who created it can do this" }] : []),
    ...(teams ?? []).filter((t) => t.id !== build.team).map((t) => ({ value: t.id, label: t.name })),
  ];
  const move = async () => {
    if (to === null) return;
    setBusy(true);
    try {
      const moved = await moveBuild(build.id, to);
      notify("success", to ? `Moved “${build.name}” to ${options.find((o) => o.value === to)?.label}.` : `Moved “${build.name}” back to its creator's builds.`);
      onMoved(moved);
    } catch (e) {
      notify("error", (e as Error).message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <Modal visible onDismiss={onDismiss} header={<>Move to team<InfoLink feature="teams.move" /></>}
      footer={<Box float="right"><SpaceBetween direction="horizontal" size="xs">
        <Button variant="link" onClick={onDismiss}>Cancel</Button>
        <Button variant="primary" loading={busy} disabled={to === null} onClick={() => void move()}>Move</Button>
      </SpaceBetween></Box>}>
      <SpaceBetween size="m">
        <Box>Everyone in the team can then see “{build.name}”; its editors can change and deploy it.
          It keeps its deployment, its versions and its sharing.</Box>
        {teams !== null && !options.length
          ? <Alert type="info">You are not an editor in any team. An admin or a team owner can add you.</Alert>
          : (
            <FormField label="Move to">
              <Select ariaLabel="Move to" placeholder="Choose a team" statusType={teams === null ? "loading" : "finished"}
                selectedOption={options.find((o) => o.value === to) ?? null} options={options}
                onChange={({ detail }) => setTo(detail.selectedOption.value ?? "")} />
            </FormField>
          )}
      </SpaceBetween>
    </Modal>
  );
}
