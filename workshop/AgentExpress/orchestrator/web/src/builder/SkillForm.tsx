/** A skill (workflow.json `skills.<name>`): when to use it, the steps, and its reference
 *  files. A SKILL.md (frontmatter `name` / `description`, then the body) can be imported
 *  into it, and is what a skill is published to an Agent Registry as (toSkillMd).
 *  Scripts are not run, so only text reference files are taken. */
import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import FormField from "@cloudscape-design/components/form-field";
import Input from "@cloudscape-design/components/input";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Textarea from "@cloudscape-design/components/textarea";
import { useRef, useState } from "react";

import type { Entry, Json } from "./model";
import { SKILL_FILE_RE, SKILL_MAX_CHARS, SKILL_MAX_FILES } from "./validate";

export interface SkillMd { name: string; description: string; instructions: string }

/** A picked file's text (FileReader where File.text is missing). */
function readText(f: File): Promise<string> {
  if (typeof f.text === "function") return f.text();
  return new Promise((ok, fail) => {
    const r = new FileReader();
    r.onload = () => ok(String(r.result ?? ""));
    r.onerror = () => fail(r.error);
    r.readAsText(f);
  });
}

/** A SKILL.md's frontmatter and body. Lenient: no frontmatter means it is all body. */
export function parseSkillMd(text: string): SkillMd {
  const m = /^\uFEFF?---\s*\r?\n([\s\S]*?)\r?\n---\s*(?:\r?\n|$)([\s\S]*)$/.exec(text);
  if (!m) return { name: "", description: "", instructions: text.trim() };
  const field = (k: string) => {
    const f = new RegExp(`^${k}\\s*:\\s*(.*)$`, "m").exec(m[1]);
    if (!f) return "";
    const v = f[1].trim();
    // A double-quoted YAML scalar, as toSkillMd writes it.
    if (v.length >= 2 && v.startsWith('"') && v.endsWith('"')) {
      try { return String(JSON.parse(v)); } catch { return v.slice(1, -1); }
    }
    return v.replace(/^'(.*)'$/, "$1");
  };
  return { name: field("name"), description: field("description"), instructions: m[2].trim() };
}

/** The SKILL.md for a skill: what the Agent Registry keeps (its frontmatter name is
 *  kebab-case, as the Agent Skills format asks). */
export function toSkillMd(name: string, skill: Entry): string {
  const kebab = name.replace(/([a-z0-9])([A-Z])/g, "$1-$2").toLowerCase();
  // Quoted: an unquoted description with ": " in it is not valid YAML, and the registry
  // refuses it. Mirrors bff/registry.py skill_md.
  const desc = JSON.stringify(String(skill.description ?? "").replace(/\s+/g, " ").trim());
  return `---\nname: ${kebab}\ndescription: ${desc}\n---\n\n${String(skill.instructions ?? "").trim()}\n`;
}

/** "refund-policy" -> "refundPolicy": a build name (a letter, then letters and digits). */
export function skillKey(name: string): string {
  const parts = name.replace(/[^A-Za-z0-9]+/g, " ").trim().split(/\s+/).filter(Boolean);
  const out = parts.map((p, i) => (i ? p[0].toUpperCase() + p.slice(1) : p[0].toLowerCase() + p.slice(1))).join("");
  const key = /^[A-Za-z]/.test(out) ? out : `skill${out}`;
  return key.slice(0, 32);
}

export function SkillForm({ value, onChange, onName }: {
  value: Entry; onChange: (e: Entry) => void;
  /** A name taken from an imported SKILL.md, offered to the form that owns the name. */
  onName?: (name: string) => void;
}) {
  const md = useRef<HTMLInputElement>(null);
  const refs = useRef<HTMLInputElement>(null);
  const [note, setNote] = useState("");
  const files = (value.files && typeof value.files === "object" && !Array.isArray(value.files)
    ? value.files : {}) as Record<string, Json>;
  const setFiles = (next: Record<string, Json>) => {
    const out = { ...value };
    if (Object.keys(next).length) out.files = next; else delete out.files;
    onChange(out);
  };
  const total = String(value.instructions ?? "").length
    + Object.values(files).reduce<number>((n, t) => n + (typeof t === "string" ? t.length : 0), 0);
  const registry = value.registry as Record<string, Json> | undefined;

  const importMd = async (f: File | undefined) => {
    if (!f) return;
    const got = parseSkillMd(await readText(f));
    onChange({ ...value, description: got.description || value.description || "", instructions: got.instructions });
    if (got.name && onName) onName(skillKey(got.name));
    setNote(`Imported ${f.name}.`);
  };
  const addFiles = async (list: FileList | null) => {
    const next = { ...files };
    const skipped: string[] = [];
    for (const f of Array.from(list ?? [])) {
      if (!SKILL_FILE_RE.test(f.name)) { skipped.push(f.name); continue; }
      next[f.name] = await readText(f);
    }
    setFiles(next);
    setNote(skipped.length ? `Not added (text files only: .md, .txt, .json, .csv, .yaml, .yml): ${skipped.join(", ")}` : "");
  };

  return (
    <SpaceBetween size="m">
      {registry ? (
        <Alert type="info" header={`From the registry: ${String(registry.name ?? registry.recordId ?? "")}`}>
          Version {String(registry.version ?? "?")}
          {registry.sync ? " · kept in sync with the registry" : " · imported once; the Builder says when a newer version exists"}.
        </Alert>
      ) : null}
      <FormField label="When to use it" description="A sentence or two. The agent reads this to decide whether to open the skill.">
        <Input value={String(value.description ?? "")} ariaLabel="When to use it" placeholder="When a customer asks for money back."
          onChange={({ detail }) => onChange({ ...value, description: detail.value })} />
      </FormField>
      <FormField label="Instructions" stretch
        description="The steps the agent follows, in Markdown (the body of a SKILL.md). Name a reference file to have the agent open it."
        secondaryControl={<Button iconName="upload" onClick={() => md.current?.click()}>Import SKILL.md</Button>}>
        <Textarea value={String(value.instructions ?? "")} rows={12} ariaLabel="Instructions" spellcheck
          placeholder={"1. Check the purchase date.\n2. Within 30 days, refund it. Otherwise, offer store credit."}
          onChange={({ detail }) => onChange({ ...value, instructions: detail.value })} />
      </FormField>
      <input ref={md} type="file" accept=".md,text/markdown,text/plain" hidden aria-label="SKILL.md file"
        onChange={(e) => { void importMd(e.target.files?.[0]); e.target.value = ""; }} />

      <SpaceBetween size="xs">
        <Box variant="h4">Reference files</Box>
        <Box variant="small" color="text-body-secondary">
          Optional. Opened only when the agent needs one. Text only, up to {SKILL_MAX_FILES} files and {SKILL_MAX_CHARS / 1000} KB in all
          ({Math.round(total / 1000)} KB used). Scripts are not run.
        </Box>
        {Object.entries(files).map(([name, text]) => (
          <SpaceBetween key={name} size="xxs">
            <SpaceBetween direction="horizontal" size="xs" alignItems="center">
              <Input value={name} ariaLabel={`File name ${name}`}
                invalid={!SKILL_FILE_RE.test(name)}
                onChange={({ detail }) => {
                  const renamed = Object.fromEntries(Object.entries(files).map(([k, v]) => [k === name ? detail.value : k, v]));
                  setFiles(renamed);
                }} />
              <Button variant="inline-link" ariaLabel={`Remove ${name}`}
                onClick={() => { const next = { ...files }; delete next[name]; setFiles(next); }}>Remove</Button>
            </SpaceBetween>
            <Textarea value={typeof text === "string" ? text : ""} rows={5} ariaLabel={`Contents of ${name}`}
              onChange={({ detail }) => setFiles({ ...files, [name]: detail.value })} />
          </SpaceBetween>
        ))}
        <SpaceBetween direction="horizontal" size="xs">
          <Button iconName="add-plus" disabled={Object.keys(files).length >= SKILL_MAX_FILES}
            onClick={() => {
              let n = 1;
              while (`notes${n}.md` in files) n += 1;
              setFiles({ ...files, [`notes${n}.md`]: "" });
            }}>Add a file</Button>
          <Button iconName="upload" disabled={Object.keys(files).length >= SKILL_MAX_FILES}
            onClick={() => refs.current?.click()}>Upload files</Button>
        </SpaceBetween>
        <input ref={refs} type="file" multiple accept=".md,.txt,.json,.csv,.yaml,.yml" hidden aria-label="Reference files"
          onChange={(e) => { void addFiles(e.target.files); e.target.value = ""; }} />
      </SpaceBetween>
      {note ? <Box variant="small" color="text-status-info">{note}</Box> : null}
    </SpaceBetween>
  );
}
