/** A Smithy tool's `allow`: the AWS actions, and the resources they act on, that the
 *  build's Gateway role may call for it. One per line; written as tools.<key>.allow. */
import { useEffect, useState } from "react";
import FormField from "@cloudscape-design/components/form-field";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Textarea from "@cloudscape-design/components/textarea";

import type { Entry } from "./model";

const lines = (v: unknown): string[] => (Array.isArray(v) ? v.map(String) : []);
export const parseLines = (text: string): string[] => text.split(/[\n,]+/).map((s) => s.trim()).filter(Boolean);

/** One list as a textarea. The text is kept as typed (a new, empty line included) and
 *  re-read from the tool only when the tool's list stops matching it. */
function ListField({ label, description, placeholder, value, onChange }: {
  label: string; description: string; placeholder: string; value: string[]; onChange: (v: string[]) => void;
}) {
  const joined = value.join("\n");
  const [text, setText] = useState(joined);
  useEffect(() => {
    if (parseLines(text).join("\n") !== joined) setText(joined);
  }, [joined]); // eslint-disable-line react-hooks/exhaustive-deps
  return (
    <FormField label={label} description={description}>
      <Textarea value={text} rows={3} ariaLabel={label} placeholder={placeholder}
        onChange={({ detail }) => { setText(detail.value); onChange(parseLines(detail.value)); }} />
    </FormField>
  );
}

export function SmithyAllow({ tool, onChange }: { tool: Entry; onChange: (next: Entry) => void }) {
  const allow = (tool.allow && typeof tool.allow === "object" && !Array.isArray(tool.allow) ? tool.allow : {}) as Record<string, unknown>;
  const set = (key: "actions" | "resources", v: string[]) =>
    onChange({ ...tool, allow: { actions: lines(allow.actions), resources: lines(allow.resources), [key]: v } });
  return (
    <SpaceBetween size="s">
      <ListField label="AWS actions it may call" description="One per line, each a named action such as dynamodb:GetItem. No *."
        placeholder="dynamodb:GetItem" value={lines(allow.actions)} onChange={(v) => set("actions", v)} />
      <ListField label="On these resources" description="One ARN per line, with its region and account written out."
        placeholder="arn:aws:dynamodb:us-east-1:111122223333:table/orders" value={lines(allow.resources)}
        onChange={(v) => set("resources", v)} />
    </SpaceBetween>
  );
}
