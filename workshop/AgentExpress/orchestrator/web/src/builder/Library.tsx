/** The library and the build's own named items, one component per job:
 *
 *   ShareDialog  who besides the owner may see and change a build or an item
 *   ItemForm     one item's definition, by kind (the same keys a build carries)
 *   LibraryPage  the left pane's Tools / Identity / Memory / Evals / Policies / Guardrails
 *   NamedTab     a build's own list of one kind: add here, add from the library (used
 *                LIVE: the build shows the item as it is now), publish its own ones to
 *                the library (publishEntries), or make a live one this build's own copy
 *
 *  A live item is stored in the build as {"library": "<id>"} (bff/library.py); the Build
 *  view shows it resolved, and edits to it go to the item itself, for every build. */
import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Container from "@cloudscape-design/components/container";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import Input from "@cloudscape-design/components/input";
import Modal from "@cloudscape-design/components/modal";
import Multiselect from "@cloudscape-design/components/multiselect";
import SpaceBetween from "@cloudscape-design/components/space-between";
import StatusIndicator from "@cloudscape-design/components/status-indicator";
import Table from "@cloudscape-design/components/table";
import Textarea from "@cloudscape-design/components/textarea";
import Toggle from "@cloudscape-design/components/toggle";
import { createContext, useCallback, useEffect, useMemo, useState } from "react";
import { SecretField } from "./BuildResources";
import { problems as cedarProblems } from "./cedar";
import { EntryForm } from "./EntryForm";
import type { BlockName } from "./meta";
import { NAME_RE } from "./cedar";
import { refOf, attachItem, uniqueKey, type Entry, type NamedMapName, type Project } from "./model";
import {
  groups as groupsApi, library, NO_SHARES, type Group, type LibraryItem, type LibraryKind, type Shares,
} from "./storage";
import type { Issue } from "./validate";
import { SkillForm } from "./SkillForm";
import "./builder.css";

type Notify = (type: "success" | "error" | "info", msg: string) => void;

export const KINDS: { kind: LibraryKind; map: NamedMapName; label: string; one: string; block: BlockName; about: string }[] = [
  { kind: "tool", map: "tools", label: "Tools", one: "tool", block: "tool", about: "Data sources and actions an agent can call through the Gateway." },
  { kind: "identity", map: "identities", label: "Identity", one: "identity", block: "identity", about: "Credentials a tool or an agent signs in with: an OAuth client or an API key. Each build sets the secret itself." },
  { kind: "memory", map: "memories", label: "Memory", one: "memory", block: "memory", about: "Long-term memory: its strategies, how long events are kept, and whose memories they are." },
  { kind: "evaluator", map: "evaluators", label: "Evals", one: "evaluator", block: "evaluator", about: "Custom LLM-as-a-judge evaluators an agent is scored with." },
  { kind: "policy", map: "policies", label: "Policies", one: "policy", block: "policy", about: "Cedar policies for a tool, written for AgentCore::Action::\"{{tool}}\" and attached to any tool." },
  { kind: "guardrail", map: "guardrails", label: "Guardrails", one: "guardrail", block: "guardrail", about: "Content guardrails an agent's input or output is checked with." },
  { kind: "skill", map: "skills", label: "Skills", one: "skill", block: "skill", about: "Know-how an agent opens when a task needs it: a SKILL.md with optional reference files." },
];
export const kindInfo = (k: LibraryKind) => KINDS.find((x) => x.kind === k)!;

const STARTERS: Record<LibraryKind, Entry> = {
  tool: { type: "mcp", description: "", endpoint: "https://" },
  identity: { type: "oauth2", clientId: "", scopes: [], tokenUrl: "https://" },
  memory: { strategies: ["semantic"], expiryDays: 30, scope: "user" },
  evaluator: { instructions: "" },
  policy: { statement: 'forbid(\n  principal,\n  action in AgentCore::Action::"{{tool}}",\n  resource == AgentCore::Gateway::"{{gateway}}"\n);' },
  guardrail: { contentFilters: { HATE: "HIGH", PROMPT_ATTACK: "HIGH" } },
  skill: { description: "", instructions: "" },
  // Published from a build's Interceptors tab, not made here.
  interceptor: { point: "request", code: {} },
};

// --- sharing -----------------------------------------------------------------------

export function ShareDialog({ visible, title, shares, onDismiss, onSave, note }: {
  visible: boolean; title: string; shares?: Shares; onDismiss: () => void;
  onSave: (s: Shares) => Promise<void>;
  /** Shown above the form (sharing several items at once). */
  note?: string;
}) {
  const [emails, setEmails] = useState("");
  const [picked, setPicked] = useState<string[]>([]);
  const [everyone, setEveryone] = useState(false);
  const [known, setKnown] = useState<Group[]>([]);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    if (!visible) return;
    const s = shares ?? NO_SHARES;
    setEmails((s.emails ?? []).join("\n"));
    setPicked(s.groups ?? []);
    setEveryone(Boolean(s.everyone));
    setError("");
    groupsApi.list().then(setKnown, () => setKnown([]));
  }, [visible, shares]);
  const list = emails.split(/[\s,;]+/).map((e) => e.trim().toLowerCase()).filter(Boolean);
  const bad = list.filter((e) => !/^[^@\s]+@[^@\s]+$/.test(e));
  const save = async () => {
    setBusy(true);
    try {
      await onSave({ emails: list, groups: picked, everyone });
      onDismiss();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <Modal visible={visible} onDismiss={onDismiss} header={`Share ${title}`}
      footer={<Box float="right"><SpaceBetween direction="horizontal" size="xs">
        <Button variant="link" onClick={onDismiss}>Cancel</Button>
        <Button variant="primary" loading={busy} disabled={bad.length > 0} onClick={() => void save()}>Save</Button>
      </SpaceBetween></Box>}>
      <SpaceBetween size="m">
        {note ? <Alert type="info">{note}</Alert> : null}
        <Box color="text-body-secondary">Whoever you share with can do everything you can: open it, change it, deploy it,
          share it on or delete it. You always keep access.</Box>
        <FormField label="People" description="Email addresses, one per line." errorText={bad.length ? `Not an email address: ${bad.join(", ")}` : undefined}>
          <Textarea value={emails} rows={4} onChange={({ detail }) => setEmails(detail.value)} ariaLabel="Share with these email addresses" />
        </FormField>
        <FormField label="Groups" description="Defined by an admin, on the Admin page.">
          <Multiselect selectedOptions={picked.map((g) => ({ label: g, value: g }))}
            options={known.map((g) => ({ label: g.name, value: g.name }))} placeholder="No groups"
            empty="No groups yet" ariaLabel="Share with these groups"
            onChange={({ detail }) => setPicked(detail.selectedOptions.map((o) => o.value!))} />
        </FormField>
        <FormField label="Everyone">
          <Toggle checked={everyone} onChange={({ detail }) => setEveryone(detail.checked)}>
            {everyone ? "Everyone who signs in to this console" : "Only the people and groups above"}
          </Toggle>
        </FormField>
        {error ? <Alert type="error">{error}</Alert> : null}
      </SpaceBetween>
    </Modal>
  );
}

export const sharedLabel = (s?: Shares) => {
  if (!s) return "Only you";
  if (s.everyone) return "Everyone";
  const n = (s.emails?.length ?? 0) + (s.groups?.length ?? 0);
  return n ? `${n} ${n === 1 ? "person or group" : "people and groups"}` : "Only you";
};

// --- one item's definition ---------------------------------------------------------

export function ItemForm({ kind, value, onChange, issues = [], path = "", onName }: {
  kind: LibraryKind; value: Entry; onChange: (e: Entry) => void; issues?: Issue[]; path?: string;
  /** A name an imported file suggests (a skill's SKILL.md). */
  onName?: (name: string) => void;
}) {
  if (kind === "skill") return <SkillForm value={value} onChange={onChange} onName={onName} />;
  if (kind === "policy") {
    const st = String(value.statement ?? "");
    const wrong = st.trim() ? cedarProblems(st) : [];
    return (
      <FormField label="Cedar" description='One permit(...) or forbid(...). Write AgentCore::Action::"{{tool}}" for whichever tool it is attached to.'
        errorText={wrong.join(" ") || undefined} stretch>
        <Textarea value={st} rows={8} spellcheck={false} ariaLabel="Cedar statement"
          onChange={({ detail }) => onChange({ ...value, statement: detail.value })} />
      </FormField>
    );
  }
  return <EntryForm name={kindInfo(kind).block} entry={value} issues={issues} pathPrefix={path}
    onChange={onChange} overrides={kind === "tool" ? { code: { hidden: true }, policies: { hidden: true }, identity: { hidden: true } } : undefined} />;
}

function ItemModal({ kind, visible, initial, taken, onDismiss, onSave, note }: {
  kind: LibraryKind; visible: boolean; initial: { name: string; description?: string; definition: Entry } | null;
  taken: string[]; onDismiss: () => void; note?: string;
  onSave: (v: { name: string; description: string; definition: Entry }) => Promise<void>;
}) {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [def, setDef] = useState<Entry>({});
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    if (!visible) return;
    setName(initial?.name ?? ""); setDescription(initial?.description ?? "");
    setDef(structuredClone(initial?.definition ?? STARTERS[kind])); setError("");
  }, [visible, initial, kind]);
  const nameError = !name ? "" : !(kind === "tool" ? /^[A-Za-z][A-Za-z0-9]*$/ : NAME_RE).test(name)
    ? "A letter, then letters and digits." : taken.includes(name) && name !== initial?.name ? `"${name}" is already used here.` : "";
  const save = async () => {
    setBusy(true);
    try {
      // A skill says when to use it in its own definition: that is its description.
      await onSave({ name, description: kind === "skill" ? String(def.description ?? "") : description, definition: def });
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <Modal visible={visible} onDismiss={onDismiss} size="large"
      header={`${initial ? "Edit" : "New"} ${kindInfo(kind).one}`}
      footer={<Box float="right"><SpaceBetween direction="horizontal" size="xs">
        <Button variant="link" onClick={onDismiss}>Cancel</Button>
        <Button variant="primary" loading={busy} disabled={!name || Boolean(nameError)} onClick={() => void save()}>Save</Button>
      </SpaceBetween></Box>}>
      <SpaceBetween size="m">
        {note ? <Alert type="info">{note}</Alert> : null}
        <FormField label="Name" errorText={nameError || undefined} description="What agents and tools pick it by.">
          <Input value={name} onChange={({ detail }) => setName(detail.value)} ariaLabel="Name" />
        </FormField>
        {kind !== "skill" ? (
          <FormField label="Description">
            <Input value={description} onChange={({ detail }) => setDescription(detail.value)} ariaLabel="Description" />
          </FormField>
        ) : null}
        <ItemForm kind={kind} value={def} onChange={setDef} onName={(n) => { if (!name) setName(n); }} />
        {error ? <Alert type="error">{error}</Alert> : null}
      </SpaceBetween>
    </Modal>
  );
}

// --- the left pane: one kind of library item -----------------------------------------

/** Interceptors are published from a build's Interceptors tab and copied into a build,
 *  never linked live (bff/library.py COPY_KINDS): here they are viewed, shared, deleted. */
const INTERCEPTOR_INFO = { label: "Interceptors", one: "interceptor",
  about: "Gateway interceptors: a Lambda before each tool request or after each answer, with its code. Publish one from a build's Interceptors tab; a build that adds one takes a copy, so changing it never changes another build." };

function InterceptorView({ item, onDismiss }: { item: LibraryItem | null; onDismiss: () => void }) {
  const d = (item?.definition ?? {}) as Record<string, unknown>;
  const templates = Object.keys((d.templates ?? {}) as object);
  return (
    <Modal visible={item !== null} onDismiss={onDismiss} size="large" header={item?.name ?? ""}
      footer={<Box float="right"><Button onClick={onDismiss}>Close</Button></Box>}>
      {item ? (
        <SpaceBetween size="s">
          <Box>{item.description || "No description."}</Box>
          <Box variant="small" color="text-body-secondary">
            {d.point === "response" ? "After each answer" : "Before each request"}
            {d.lambdaArn ? ` · your Lambda ${String(d.lambdaArn)}` : " · written in the build"}
            {templates.length ? ` · from ${templates.join(", ")}` : ""}
            {d.passRequestHeaders ? " · reads the request headers" : ""}
          </Box>
          {item.files?.["handler.py"] ? <pre className="axb-code">{item.files["handler.py"]}</pre> : null}
          <Box variant="small" color="text-body-secondary">To use it, open a build&apos;s Interceptors tab and choose Add from library.</Box>
        </SpaceBetween>
      ) : null}
    </Modal>
  );
}

export function LibraryPage({ kind, notify }: { kind: LibraryKind; notify: Notify }) {
  const copied = kind === "interceptor";
  const info = copied ? INTERCEPTOR_INFO : kindInfo(kind);
  const [viewing, setViewing] = useState<LibraryItem | null>(null);
  const [items, setItems] = useState<LibraryItem[] | null>(null);
  const [editing, setEditing] = useState<LibraryItem | "new" | null>(null);
  const [sharing, setSharing] = useState<LibraryItem[] | null>(null);
  const [selected, setSelected] = useState<LibraryItem[]>([]);
  const load = useCallback(() => {
    library.list(kind).then(setItems, (e) => { notify("error", `Could not load: ${(e as Error).message}`); setItems([]); });
  }, [kind, notify]);
  useEffect(load, [load]);
  const remove = async (it: LibraryItem) => {
    if (!window.confirm(`Delete ${it.name} from the library? Every build that uses it keeps its own copy.`)) return;
    try {
      const r = await library.remove(it.id);
      notify("success", `Deleted ${it.name} from the library.${keptNote(r?.keptIn)}`);
      load();
    } catch (e) {
      notify("error", (e as Error).message);
    }
  };
  return (
    <SpaceBetween size="l">
      <Table
        header={<Header variant="h1" counter={items ? `(${items.length})` : undefined}
          description={copied ? `${info.about} Yours and those shared with you.`
            : `${info.about} Yours and those shared with you, for any build: a build uses one live, so a change here shows in every build that uses it.`}
          actions={<SpaceBetween direction="horizontal" size="xs">
            <Button disabled={!selected.length} onClick={() => setSharing(selected)}>
              Share{selected.length ? ` (${selected.length})` : ""}</Button>
            {copied ? null : <Button variant="primary" iconName="add-plus" onClick={() => setEditing("new")}>New {info.one}</Button>}
          </SpaceBetween>}>
          {info.label}</Header>}
        items={items ?? []} loading={items === null} loadingText="Loading" trackBy="id"
        selectionType="multi" selectedItems={selected} onSelectionChange={({ detail }) => setSelected(detail.selectedItems)}
        ariaLabels={{ selectionGroupLabel: "Select to share", allItemsSelectionLabel: () => "Select all",
          itemSelectionLabel: (_, i) => `Select ${i.name}` }}
        columnDefinitions={[
          { id: "name", header: "Name", isRowHeader: true, cell: (i) => <Button variant="inline-link" onClick={() => (copied ? setViewing(i) : setEditing(i))}>{i.name}</Button> },
          { id: "desc", header: "Description", cell: (i) => i.description || String(i.definition?.description ?? "") || "—" },
          { id: "owner", header: "Owner", cell: (i) => (i.mine ? "You" : i.ownerEmail || "Someone else") },
          { id: "shared", header: "Shared with", cell: (i) => sharedLabel(i.shares) },
          { id: "updated", header: "Updated", cell: (i) => (i.updatedAt ? new Date(i.updatedAt).toLocaleString() : "—") },
          { id: "actions", header: "Actions", cell: (i) => (
            <SpaceBetween direction="horizontal" size="xs">
              {copied
                ? <Button variant="inline-link" ariaLabel={`View ${i.name}`} onClick={() => setViewing(i)}>View</Button>
                : <Button variant="inline-link" ariaLabel={`Edit ${i.name}`} onClick={() => setEditing(i)}>Edit</Button>}
              <Button variant="inline-link" ariaLabel={`Delete ${i.name}`} onClick={() => void remove(i)}>Delete</Button>
            </SpaceBetween>) },
        ]}
        empty={<Box textAlign="center" color="inherit">{copied ? "Nothing yet. Publish one from a build's Interceptors tab."
          : "Nothing yet. Make one here, or save one from a build."}</Box>}
      />
      <InterceptorView item={viewing} onDismiss={() => setViewing(null)} />
      {copied ? null : (
      <ItemModal kind={kind} visible={editing !== null} taken={(items ?? []).map((i) => i.name)}
        initial={editing && editing !== "new" ? { name: editing.name, description: editing.description, definition: editing.definition as Entry } : null}
        note={editing && editing !== "new" ? "Every build that uses it sees the change at once; a deployed one when it is next deployed." : undefined}
        onDismiss={() => setEditing(null)}
        onSave={async (v) => {
          if (editing && editing !== "new") await library.update(editing.id, v);
          else await library.create({ kind, ...v });
          notify("success", `Saved ${v.name}.`);
          setEditing(null);
          load();
        }} />
      )}
      <ShareItems items={sharing} onDismiss={() => setSharing(null)} notify={notify}
        onSaved={() => { setSelected([]); load(); }} />
    </SpaceBetween>
  );
}

// --- a build's own list of one kind ----------------------------------------------------

const sameShares = (a?: Shares, b?: Shares) => {
  const n = (s?: Shares) => JSON.stringify([[...(s?.emails ?? [])].sort(), [...(s?.groups ?? [])].sort(), Boolean(s?.everyone)]);
  return n(a) === n(b);
};

/** Share one or more library items at once, each with the same people, groups or
 *  everyone. Saving replaces who each is shared with. */
export function ShareItems({ items, onDismiss, onSaved, notify }: {
  items: LibraryItem[] | null; onDismiss: () => void; notify: Notify;
  onSaved?: (shares: Shares, items: LibraryItem[]) => void;
}) {
  const list = useMemo(() => items ?? [], [items]);
  const same = list.length > 0 && list.every((i) => sameShares(i.shares, list[0].shares));
  const shares = useMemo(() => (same ? list[0].shares : undefined), [same, list]);
  const names = list.map((i) => i.name).join(", ");
  return (
    <ShareDialog visible={list.length > 0} title={list.length === 1 ? list[0].name : `${list.length} items`} shares={shares}
      note={list.length > 1 ? `${names}: ${same ? "each is shared with the same people now." : "they are shared differently now; saving shares each with exactly what you set here."}` : undefined}
      onDismiss={onDismiss}
      onSave={async (s) => {
        for (const i of list) await library.share(i.id, s);
        notify("success", `Sharing of ${names} saved.`);
        onSaved?.(s, list);
      }} />
  );
}

/** Share entries from a build's tab. One this build defined itself is published to the
 *  library first (asked), since only a library item can be used by other builds; then
 *  every selected one is shared. */
export function useShareEntries(kind: LibraryKind, raw: Project, view: Project, setRaw: (p: Project) => void,
  refs: Record<string, LibraryItem>, addRefs: (items: LibraryItem[]) => void, notify: Notify) {
  const [items, setItems] = useState<LibraryItem[] | null>(null);
  const map = kindInfo(kind).map;
  const share = async (keys: string[]) => {
    const own = (raw.workflow[map] ?? {}) as Record<string, Entry>;
    const local = keys.filter((k) => k in own && !refOf(own[k]));
    let proj = raw;
    let made: LibraryItem[] = [];
    if (local.length) {
      if (!window.confirm(`To share ${local.join(", ")}, ${local.length > 1 ? "they are" : "it is"} published to your library first, and this build uses ${local.length > 1 ? "them" : "it"} from there. Continue?`)) return;
      const r = await publishEntries(kind, local, raw, view, notify, addRefs);
      if (!r) return;
      setRaw(r.project);
      proj = r.project;
      made = r.items;
    }
    const byId: Record<string, LibraryItem> = { ...refs, ...Object.fromEntries(made.map((m) => [m.id, m])) };
    const now = (proj.workflow[map] ?? {}) as Record<string, Entry>;
    const picked = keys.map((k) => byId[refOf(now[k]) ?? ""]).filter(Boolean);
    if (picked.length) setItems(picked);
  };
  const dialog = <ShareItems items={items} onDismiss={() => setItems(null)} notify={notify}
    onSaved={(s, list) => addRefs(list.map((i) => ({ ...i, shares: s })))} />;
  return { share, dialog };
}

/** What deleting an item did to the builds that used it. */
export function keptNote(kept?: { name: string }[]): string {
  if (!kept?.length) return "";
  const names = kept.map((b) => b.name || "Untitled").join(", ");
  return kept.length > 1 ? ` The builds that used it (${names}) each keep their own copy.`
    : ` The build that used it (${names}) keeps its own copy.`;
}

/** A library name for a build key: letters and digits, a letter first, 32 at most
 *  (bff/library.py NAME_RE), and not one of the caller's own items of that kind. */
export function libraryName(key: string, taken: Iterable<string>): string {
  let base = key.replace(/[^A-Za-z0-9]/g, "");
  if (!/^[A-Za-z]/.test(base)) base = `x${base}`;
  base = base.slice(0, 29) || "item";
  return uniqueKey(base, taken, "");
}

/** Publish a build's own entries to the library, and use each from there, live: the
 *  entry becomes {"library": id}, so this build deploys exactly what it did, and other
 *  builds can add it. A code tool takes its files with it. `raw` is the stored project
 *  and `view` the resolved one. Returns the new stored project and the items made, or
 *  null when nothing was published; says what happened either way. */
export async function publishEntries(kind: LibraryKind, keys: string[], raw: Project, view: Project,
  notify: Notify, addRefs: (items: LibraryItem[]) => void): Promise<{ project: Project; items: LibraryItem[] } | null> {
  const map = kindInfo(kind).map;
  const own = (raw.workflow[map] ?? {}) as Record<string, Entry>;
  const shown = (view.workflow[map] ?? {}) as Record<string, Entry>;
  const todo = keys.filter((k) => k in own && !refOf(own[k]) && shown[k]);
  if (!todo.length) return null;
  // Names only have to be free among the caller's OWN items of this kind (a 409 otherwise).
  const taken = await library.list(kind).then((xs) => xs.filter((x) => x.mine !== false).map((x) => x.name), () => [] as string[]);
  const next = structuredClone(raw);
  const made: LibraryItem[] = [];
  const renamed: string[] = [];
  const failed: string[] = [];
  for (const key of todo) {
    const name = libraryName(key, taken);
    try {
      const files = kind === "tool" ? view.toolCode?.[key] : undefined;
      const it = await library.create({ kind, name, definition: structuredClone(shown[key]) as Record<string, unknown>,
        ...(files && Object.keys(files).length ? { files } : {}) });
      taken.push(name);
      made.push(it);
      if (name !== key) renamed.push(`${key} as ${name}`);
      (next.workflow[map] as Record<string, Entry>)[key] = { library: it.id };
      if (kind === "tool" && next.toolCode?.[key]) delete next.toolCode[key];   // the item carries them now
    } catch (e) {
      failed.push(`${key}: ${(e as Error).message}`);
    }
  }
  if (made.length) {
    addRefs(made);
    const notes = [
      renamed.length ? `Published ${renamed.join(", ")}.` : "",
      kind === "identity" ? "Its secret stays with this build: each build that adds it sets its own." : "",
      kind === "tool" && todo.some((k) => (shown[k].policies as unknown[] | undefined)?.length || shown[k].identity)
        ? "A build that adds it needs the policies and identity it names, under the same names." : "",
    ].filter(Boolean).join(" ");
    notify("success", `${made.map((m) => m.name).join(", ")} ${made.length > 1 ? "are" : "is"} in your library now, `
      + `for any build to add; this build uses ${made.length > 1 ? "them" : "it"} from there.${notes ? ` ${notes}` : ""}`);
  }
  if (failed.length) notify("error", `Could not publish ${failed.join("; ")}`);
  return made.length ? { project: next, items: made } : null;
}

export function NamedTab({ kind, project, view, setProject, refs, addRefs, issues, server, notify, extra, onUnpublish }: {
  kind: LibraryKind;
  /** The stored project (live items as {"library": id}). */
  project: Project;
  /** The same, resolved: what it shows. */
  view: Project;
  setProject: (p: Project) => void;
  refs: Record<string, LibraryItem>;
  addRefs: (items: LibraryItem[]) => void;
  issues: Issue[];
  server: boolean;
  notify: Notify;
  /** Shown above the list (the Guardrails tab's deployment guardrail). */
  extra?: React.ReactNode;
  /** Take a live entry's item out of the library; this build (and any other) keeps a copy. */
  onUnpublish?: (id: string, name: string) => Promise<void>;
}) {
  const info = kindInfo(kind);
  const raw = (project.workflow[info.map] ?? {}) as Record<string, Entry>;
  const shown = (view.workflow[info.map] ?? {}) as Record<string, Entry>;
  const [editing, setEditing] = useState<string | "new" | null>(null);
  const [picking, setPicking] = useState(false);
  const [selected, setSelected] = useState<{ key: string }[]>([]);
  const setMap = (next: Record<string, Entry>) => {
    const wf = { ...project.workflow };
    if (Object.keys(next).length) wf[info.map] = next; else delete wf[info.map];
    setProject({ ...project, workflow: wf });
  };
  const usedBy = (key: string) => {
    const a = Object.entries(view.workflow.agents);
    const t = Object.entries(view.workflow.tools);
    const hit = (xs: [string, Entry][], f: (e: Entry) => boolean) => xs.filter(([, e]) => f(e)).map(([k]) => k);
    const core = (e: Entry) => (e.agentcore ?? {}) as Record<string, Record<string, unknown>>;
    if (kind === "tool") return hit(a, (e) => [e.tool ?? []].flat().includes(key));
    if (kind === "guardrail") return hit(a, (e) => core(e).guardrails?.use === key);
    if (kind === "memory") return hit(a, (e) => core(e).memory?.use === key);
    if (kind === "evaluator") return hit(a, (e) => ((core(e).evaluations?.evaluators ?? []) as string[]).includes(`Custom.${key}`));
    if (kind === "identity") return [...hit(a, (e) => ((core(e).identity?.outbound ?? []) as string[]).includes(key)), ...hit(t, (e) => e.identity === key)];
    if (kind === "skill") return hit(a, (e) => ((e.skills ?? []) as string[]).includes(key));
    return hit(t, (e) => ((e.policies ?? []) as string[]).includes(key));
  };
  const errorsAt = (key: string) => issues.filter((i) => i.path === `${info.map}.${key}` || i.path.startsWith(`${info.map}.${key}.`)
    || (kind === "tool" && i.where.kind === "tool" && i.where.id === key)).length;
  const publish = async (keys: string[]) => {
    const r = await publishEntries(kind, keys, project, view, notify, addRefs);
    if (r) { setProject(r.project); setSelected([]); }
  };
  const sharer = useShareEntries(kind, project, view, setProject, refs, addRefs, notify);
  const publishable = selected.map((r) => r.key).filter((k) => k in raw && !refOf(raw[k]));
  const editing0 = editing && editing !== "new" ? editing : null;
  const liveId = editing0 ? refOf(raw[editing0]) : null;
  return (
    <SpaceBetween size="l">
      {extra}
      <Table
        header={<Header variant="h2" counter={`(${Object.keys(raw).length})`} description={info.about}
          actions={<SpaceBetween direction="horizontal" size="xs">
            {server ? <Button disabled={!publishable.length} onClick={() => void publish(publishable)}>
              Publish to library{publishable.length ? ` (${publishable.length})` : ""}</Button> : null}
            {server ? <Button disabled={!selected.length} onClick={() => void sharer.share(selected.map((r) => r.key))}>
              Share{selected.length ? ` (${selected.length})` : ""}</Button> : null}
            {server ? <Button onClick={() => setPicking(true)}>Add from library</Button> : null}
            <Button variant="primary" iconName="add-plus" onClick={() => setEditing("new")}>Add {info.one}</Button>
          </SpaceBetween>}>{`This build's ${info.label.toLowerCase()}`}</Header>}
        items={Object.keys(raw).map((key) => ({ key }))} trackBy="key"
        {...(server ? {
          selectionType: "multi" as const, selectedItems: selected,
          onSelectionChange: ({ detail }: { detail: { selectedItems: { key: string }[] } }) => setSelected(detail.selectedItems),
          ariaLabels: { selectionGroupLabel: "Select to publish or share",
            itemSelectionLabel: (_: unknown, r: { key: string }) => `Select ${r.key}`, allItemsSelectionLabel: () => "Select all" },
        } : {})}
        columnDefinitions={[
          { id: "name", header: "Name", isRowHeader: true, cell: (r) => <Button variant="inline-link" onClick={() => setEditing(r.key)}>{r.key}</Button> },
          { id: "from", header: "From", cell: (r) => {
            const id = refOf(raw[r.key]);
            if (!id) return "This build";
            const it = refs[id];
            return it ? <StatusIndicator type="info">{`Library · ${it.mine === false ? it.ownerEmail : "yours"}`}</StatusIndicator>
              : <StatusIndicator type="error">Missing from the library</StatusIndicator>;
          } },
          { id: "used", header: "Used by", cell: (r) => usedBy(r.key).join(", ") || "—" },
          { id: "issues", header: "Problems", cell: (r) => (errorsAt(r.key)
            ? <StatusIndicator type="error">{errorsAt(r.key)}</StatusIndicator> : <StatusIndicator type="success">OK</StatusIndicator>) },
          { id: "actions", header: "Actions", cell: (r) => (
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="inline-link" ariaLabel={`Edit ${r.key}`} onClick={() => setEditing(r.key)}>Edit</Button>
              {server && !refOf(raw[r.key]) ? <Button variant="inline-link" ariaLabel={`Publish ${r.key} to the library`} onClick={() => void publish([r.key])}>Publish</Button> : null}
              {refOf(raw[r.key]) && shown[r.key] && !refOf(shown[r.key]) ? (
                <Button variant="inline-link" onClick={() => setMap({ ...raw, [r.key]: structuredClone(shown[r.key]) })}>Make a copy for this build</Button>
              ) : null}
              {onUnpublish && refOf(raw[r.key]) && refs[refOf(raw[r.key])!] ? (
                <Button variant="inline-link" ariaLabel={`Unpublish ${r.key} from the library`}
                  onClick={() => void onUnpublish(refOf(raw[r.key])!, refs[refOf(raw[r.key])!].name)}>Unpublish</Button>
              ) : null}
              <Button variant="inline-link" ariaLabel={`Delete ${r.key}`} onClick={() => {
                if (!window.confirm(`Delete ${r.key} from this build?`)) return;
                const next = { ...raw };
                delete next[r.key];
                setMap(next);
              }}>Delete</Button>
            </SpaceBetween>) },
        ]}
        empty={<Box textAlign="center" color="inherit">None in this build.{server ? " Add one here, or from your library." : ""}</Box>}
      />
      {kind === "identity" && server && Object.keys(shown).length ? (
        // An identity holds no secret in its definition (it travels in the library and in
        // exports), so each build enters its own here: the API key, or the OAuth client's
        // secret. Without this field it could only be set through the API.
        <Container header={<Header variant="h3" description="Stored encrypted with this build, vaulted in AgentCore Identity at deploy, and never shown again.">Secrets</Header>}>
          <SpaceBetween size="m">
            {Object.entries(shown).map(([name, def]) => {
              const oauth = String((def as Entry)?.type ?? "") === "oauth2";
              const users = usedBy(name);
              return (
                <SecretField key={name} buildId={project.id} kind="identitySecrets" name={name}
                  label={`${oauth ? "OAuth client secret" : "API key"} for ${name}`}
                  description={users.length ? `Used by ${users.join(", ")}.`
                    : "Nothing uses this identity yet: give it to a tool (its Identity setting) or to an agent (Identity under its features)."} />
              );
            })}
          </SpaceBetween>
        </Container>
      ) : null}
      <ItemModal kind={kind} visible={editing !== null} taken={Object.keys(raw)}
        initial={editing0 ? { name: editing0, definition: (shown[editing0] ?? {}) as Entry,
          description: liveId ? refs[liveId]?.description : undefined } : null}
        note={liveId ? "This one is from the library: saving changes it for every build that uses it." : undefined}
        onDismiss={() => setEditing(null)}
        onSave={async (v) => {
          if (liveId) {
            const it = await library.update(liveId, { name: refs[liveId]?.name ?? v.name, description: v.description, definition: v.definition });
            addRefs([it]);
            if (v.name !== editing0) {
              const next = Object.fromEntries(Object.entries(raw).map(([k, e]) => [k === editing0 ? v.name : k, e]));
              setMap(next);
            }
          } else {
            const next = editing0 && editing0 !== v.name
              ? Object.fromEntries(Object.entries(raw).map(([k, e]) => [k === editing0 ? v.name : k, e])) : { ...raw };
            next[v.name] = v.definition;
            setMap(next);
          }
          setEditing(null);
        }} />
      {server ? sharer.dialog : null}
      {server ? <LibraryPicker kind={kind} visible={picking} onDismiss={() => setPicking(false)} notify={notify}
        using={Object.values(raw).map(refOf).filter(Boolean) as string[]}
        onPick={(items) => {
          addRefs(items);
          let p = project;
          for (const it of items) p = attachItem(p, info.map, it).project;
          setProject(p);
          setPicking(false);
          notify("success", `This build uses ${items.map((i) => i.name).join(", ")} from the library now.`);
        }} /> : null}
    </SpaceBetween>
  );
}

export function LibraryPicker({ kind, visible, using, onDismiss, onPick, notify }: {
  kind: LibraryKind; visible: boolean; using: string[]; onDismiss: () => void;
  onPick: (items: LibraryItem[]) => void; notify: Notify;
}) {
  const [items, setItems] = useState<LibraryItem[] | null>(null);
  const [selected, setSelected] = useState<LibraryItem[]>([]);
  useEffect(() => {
    if (!visible) return;
    setSelected([]); setItems(null);
    library.list(kind).then(setItems, (e) => { notify("error", (e as Error).message); setItems([]); });
  }, [visible, kind, notify]);
  const shown = useMemo(() => (items ?? []).filter((i) => !using.includes(i.id)), [items, using]);
  return (
    <Modal visible={visible} onDismiss={onDismiss} size="large" header={`Add ${kindInfo(kind).label.toLowerCase()} from the library`}
      footer={<Box float="right"><SpaceBetween direction="horizontal" size="xs">
        <Button variant="link" onClick={onDismiss}>Cancel</Button>
        <Button variant="primary" disabled={!selected.length} onClick={() => onPick(selected)}>Add {selected.length || ""}</Button>
      </SpaceBetween></Box>}>
      <Table selectionType="multi" items={shown} loading={items === null} loadingText="Loading" trackBy="id"
        selectedItems={selected} onSelectionChange={({ detail }) => setSelected(detail.selectedItems)}
        columnDefinitions={[
          { id: "name", header: "Name", isRowHeader: true, cell: (i) => i.name },
          { id: "desc", header: "Description", cell: (i) => i.description || "—" },
          { id: "owner", header: "Owner", cell: (i) => (i.mine ? "You" : i.ownerEmail || "—") },
        ]}
        empty={<Box textAlign="center" color="inherit">Nothing (else) in the library yet.</Box>} />
    </Modal>
  );
}

// --- what the Build view's panels pick from ----------------------------------------------

export interface LibraryCtx {
  /** Every library item the caller can use (theirs and shared with them). */
  lib: LibraryItem[];
  /** Use a library item in this build, live, then make an edit that names it by `key`. */
  withItem: ((map: NamedMapName, item: LibraryItem, edit: (p: Project, key: string) => Project) => void) | null;
  /** The library item a build entry is, when it is one. */
  liveId: (map: NamedMapName, key: string) => string | null;
}
export const LibraryContext = createContext<LibraryCtx>({ lib: [], withItem: null, liveId: () => null });

/** Options for a picker: the build's own entries, then library items it does not use yet
 *  (value "lib:<id>"), each labelled with where it comes from. */
export function pickOptions(ctx: LibraryCtx, kind: LibraryKind, buildKeys: string[], usedIds: string[],
  describe: (key: string) => string = () => "This build") {
  return [
    ...buildKeys.map((k) => ({ label: k, value: k, description: describe(k) })),
    ...ctx.lib.filter((i) => i.kind === kind && !usedIds.includes(i.id))
      .map((i) => ({ label: i.name, value: `lib:${i.id}`, description: `Library · ${i.mine === false ? i.ownerEmail : "yours"}` })),
  ];
}
