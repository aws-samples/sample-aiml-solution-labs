/** A form for any workflow.json entry, generated from keys.json.
 *
 *  The Builder has no hand-written agent form or tool form. It asks keys.json which keys
 *  apply to this runtime or tool type, in what order, with what help text and what
 *  allowed values, and renders a field for each. So "every setting is configurable" is
 *  true by construction, and a key the framework adds appears here with no UI change —
 *  which is the only way a form like this stays honest as the framework grows. */

import ExpandableSection from "@cloudscape-design/components/expandable-section";
import FormField from "@cloudscape-design/components/form-field";
import Input from "@cloudscape-design/components/input";
import Multiselect from "@cloudscape-design/components/multiselect";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Textarea from "@cloudscape-design/components/textarea";
import Toggle from "@cloudscape-design/components/toggle";
import { useEffect, useState, type ReactNode } from "react";

import { humanizeKey } from "../assets/shape";
import { FORM_LAYOUT, splitLayout, type FormLayout } from "./formLayout";
import {
  allowedValues, applies, block, defaultFor, formGroups, requiredFor, variantOf,
  type BlockName,
} from "./meta";
import type { Entry, Json } from "./model";
import type { Issue } from "./validate";

const isObj = (v: unknown): v is Record<string, Json> =>
  typeof v === "object" && v !== null && !Array.isArray(v);

function getPath(entry: Entry, key: string): Json | undefined {
  const [head, sub] = key.split(".");
  if (!sub) return entry[head];
  const parent = entry[head];
  return isObj(parent) ? parent[sub] : undefined;
}

/** Set `key` (possibly dotted) in `entry`; `undefined` removes it, and an emptied
 *  parent object goes too — an empty block reads like a setting and does nothing. */
function setPath(entry: Entry, key: string, value: Json | undefined): Entry {
  const next: Entry = { ...entry };
  const [head, sub] = key.split(".");
  if (!sub) {
    if (value === undefined) delete next[head];
    else next[head] = value;
    return next;
  }
  const parent: Record<string, Json> = isObj(next[head]) ? { ...(next[head] as Record<string, Json>) } : {};
  if (value === undefined) delete parent[sub];
  else parent[sub] = value;
  if (Object.keys(parent).length) next[head] = parent;
  else delete next[head];
  return next;
}

/** A JSON-valued field (objects, arrays of objects) edited as text, committed on blur. */
function JsonField({ value, onChange, placeholder }: {
  value: Json | undefined; onChange: (v: Json | undefined) => void; placeholder?: string;
}) {
  const shown = value === undefined ? "" : JSON.stringify(value, null, 2);
  const [text, setText] = useState(shown);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => { setText(shown); setError(null); }, [shown]);
  return (
    <FormField errorText={error}>
      <Textarea
        value={text} rows={Math.min(12, Math.max(3, text.split("\n").length))}
        placeholder={placeholder}
        onChange={({ detail }) => setText(detail.value)}
        onBlur={() => {
          if (!text.trim()) { onChange(undefined); setError(null); return; }
          try {
            onChange(JSON.parse(text) as Json);
            setError(null);
          } catch (e) {
            setError(`Not valid JSON: ${(e as Error).message}`);
          }
        }}
      />
    </FormField>
  );
}

export interface FieldOverride {
  /** Replace the generated control. */
  render?: (value: Json | undefined, set: (v: Json | undefined) => void) => ReactNode;
  hidden?: boolean;
}

function Field({ name, keyName, variant, value, onChange, issues, override }: {
  name: BlockName;
  keyName: string;
  variant: string;
  value: Json | undefined;
  onChange: (v: Json | undefined) => void;
  issues: Issue[];
  override?: FieldOverride;
}) {
  const spec = block(name).keys[keyName];
  const label = humanizeKey(keyName.split(".").pop() ?? keyName);
  const required = requiredFor(name, keyName, variant);
  const def = defaultFor(name, keyName, variant);
  const allowed = allowedValues(name, keyName, variant);
  const errorText = issues.map((i) => i.message).join(" ") || undefined;
  const constraint = [required ? "Required" : null,
    def !== undefined ? `Default: ${JSON.stringify(def)}` : null].filter(Boolean).join(" · ") || undefined;

  let control: ReactNode;
  if (override?.render) {
    control = override.render(value, onChange);
  } else if (spec.type === "boolean") {
    const on = typeof value === "boolean" ? value : def === true;
    control = (
      <Toggle checked={on} onChange={({ detail }) => {
        // Writing the default back is noise in the file; leave the key out instead.
        onChange(detail.checked === (def === true) ? undefined : detail.checked);
      }}>{on ? "On" : "Off"}</Toggle>
    );
  } else if (spec.type === "array" && allowed) {
    const selected = Array.isArray(value) ? value.map(String) : [];
    control = (
      <Multiselect
        selectedOptions={selected.map((v) => ({ label: v, value: v }))}
        options={allowed.map((v) => ({ label: v, value: v }))}
        placeholder={def !== undefined ? JSON.stringify(def) : "Choose"}
        onChange={({ detail }) => {
          const vals = detail.selectedOptions.map((o) => o.value!).filter(Boolean);
          onChange(vals.length ? vals : undefined);
        }}
      />
    );
  } else if (allowed) {
    const opts = [{ label: def !== undefined ? `(default: ${String(def)})` : "(not set)", value: "" },
      ...allowed.map((v) => ({ label: v === "" ? '"" (empty)' : v, value: v }))];
    const current = typeof value === "string" ? value : "";
    control = (
      <Select
        selectedOption={opts.find((o) => o.value === current) ?? opts[0]}
        options={opts}
        onChange={({ detail }) => onChange(detail.selectedOption.value ? detail.selectedOption.value : undefined)}
      />
    );
  } else if (spec.type === "number" || spec.type === "integer") {
    control = (
      <Input
        type="number" inputMode={spec.type === "integer" ? "numeric" : "decimal"}
        value={value === undefined ? "" : String(value)}
        placeholder={def !== undefined ? String(def) : undefined}
        onChange={({ detail }) => {
          if (detail.value.trim() === "") return onChange(undefined);
          const n = Number(detail.value);
          onChange(Number.isFinite(n) ? n : (detail.value as Json));
        }}
      />
    );
  } else if (spec.type === "array" && (spec.items ?? "string") === "string") {
    const lines = Array.isArray(value) ? value.map(String).join("\n") : "";
    control = (
      <Textarea
        value={lines} rows={Math.min(6, Math.max(2, lines.split("\n").length))}
        placeholder="One per line"
        onChange={({ detail }) => {
          const vals = detail.value.split("\n").map((s) => s.trim()).filter(Boolean);
          onChange(vals.length ? vals : undefined);
        }}
      />
    );
  } else if (spec.type === "object" || spec.type === "array") {
    control = <JsonField value={value} onChange={onChange}
      placeholder={def !== undefined ? JSON.stringify(def) : spec.type === "array" ? "[ ]" : "{ }"} />;
  } else {
    control = (
      <Input
        value={typeof value === "string" ? value : value === undefined ? "" : JSON.stringify(value)}
        placeholder={def !== undefined ? String(def) : undefined}
        onChange={({ detail }) => onChange(detail.value === "" ? undefined : detail.value)}
      />
    );
  }

  return (
    <FormField label={label} description={spec.doc} constraintText={constraint} errorText={errorText}
      stretch>
      {control}
    </FormField>
  );
}

/** Keys an entry carries that the form has no field for (unknown or not applicable). */
function strays(name: BlockName, entry: Entry, variant: string): string[] {
  const spec = block(name);
  const parents = new Set(Object.keys(spec.keys).filter((k) => k.includes(".")).map((k) => k.split(".")[0]));
  return Object.keys(entry).filter((k) => {
    if (parents.has(k)) return false;
    return !spec.keys[k] || !applies(name, k, variant);
  });
}

export function EntryForm({ name, entry, onChange, issues, pathPrefix, overrides, collapseGroups, layout }: {
  name: BlockName;
  entry: Entry;
  onChange: (e: Entry) => void;
  issues: Issue[];
  /** e.g. `agents.intake` — to match issues to fields. */
  pathPrefix: string;
  overrides?: Record<string, FieldOverride>;
  /** Groups rendered collapsed (by group name). */
  collapseGroups?: string[];
  /** Main fields first, the rest under Advanced (formLayout.ts). `false` for the plain
   *  grouped form. Defaults to the block's layout, when it has one. */
  layout?: FormLayout | false;
}) {
  const spec = block(name);
  const variant = variantOf(name, entry);
  const groups = formGroups(name, variant);

  const setKey = (key: string, value: Json | undefined) => {
    let next = setPath(entry, key, value);
    if (key === spec.$variantKey) {
      // Changing runtime or type changes which keys are legal. Drop the ones that no
      // longer apply rather than leave the user a list of errors they did not cause.
      const v = variantOf(name, next);
      next = Object.fromEntries(Object.entries(next).filter(([k]) =>
        !spec.keys[k] || applies(name, k, v) || k === key)) as Entry;
    }
    onChange(next);
  };

  const fieldIssues = (key: string) => {
    const p = `${pathPrefix}.${key}`;
    return issues.filter((i) => i.path === p || i.path.startsWith(`${p}.`) || i.path.startsWith(`${p}[`));
  };

  const leftovers = strays(name, entry, variant);
  const field = (key: string) => (
    <Field
      key={key} name={name} keyName={key} variant={variant}
      value={getPath(entry, key)} onChange={(v) => setKey(key, v)}
      issues={fieldIssues(key)} override={overrides?.[key]}
    />
  );
  const shape = layout === false ? undefined : (layout ?? FORM_LAYOUT[name]);
  const split = shape ? splitLayout(shape, groups.flatMap((g) => g.keys).filter((k) => !overrides?.[k]?.hidden)) : null;
  // A section holding a field with a problem opens, so the problem is never hidden.
  const flagged = (keys: string[]) => keys.some((k) => fieldIssues(k).length > 0);

  return (
    <SpaceBetween size="l">
      {split ? (
        <>
          {split.main.length ? <SpaceBetween size="m">{split.main.map(field)}</SpaceBetween> : null}
          {split.advanced.length ? (
            <ExpandableSection headerText="Advanced" variant="footer"
              defaultExpanded={flagged(split.advanced.flatMap((a) => a.keys))}>
              <SpaceBetween size="s">
                {split.advanced.map((a) => (
                  <ExpandableSection key={a.title} headerText={a.title} defaultExpanded={flagged(a.keys)}>
                    <SpaceBetween size="m">{a.keys.map(field)}</SpaceBetween>
                  </ExpandableSection>
                ))}
              </SpaceBetween>
            </ExpandableSection>
          ) : null}
        </>
      ) : groups.map((g) => {
        const fields = g.keys.filter((k) => !overrides?.[k]?.hidden).map((key) => (
          <Field
            key={key} name={name} keyName={key} variant={variant}
            value={getPath(entry, key)} onChange={(v) => setKey(key, v)}
            issues={fieldIssues(key)} override={overrides?.[key]}
          />
        ));
        if (!fields.length) return null;
        const title = g.group ? humanizeKey(g.group) : "Settings";
        return (
          <ExpandableSection
            key={g.group || "_"} headerText={title} headerDescription={g.doc}
            defaultExpanded={!collapseGroups?.includes(g.group)} variant="footer"
          >
            <SpaceBetween size="m">{fields}</SpaceBetween>
          </ExpandableSection>
        );
      })}
      {leftovers.length ? (
        <FormField
          label="Other keys"
          description="Keys this entry carries that do not apply here. Kept exactly as imported; remove them to clear the errors."
          errorText={leftovers.map((k) => fieldIssues(k).map((i) => i.message).join(" ")).filter(Boolean).join(" ") || undefined}
        >
          <JsonField
            value={Object.fromEntries(leftovers.map((k) => [k, entry[k]]))}
            onChange={(v) => {
              const kept = Object.fromEntries(Object.entries(entry).filter(([k]) => !leftovers.includes(k)));
              onChange({ ...kept, ...(isObj(v) ? v : {}) } as Entry);
            }}
          />
        </FormField>
      ) : null}
    </SpaceBetween>
  );
}
