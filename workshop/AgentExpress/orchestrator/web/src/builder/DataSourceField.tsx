/** A Knowledge Base tool's `dataSource`: where its documents come from when they are not
 *  files in S3. "Files" removes the key (uploads, or `s3Uri`); any other choice shows that
 *  connector's settings, each described from app/keys.json. The checks are in
 *  kbDataSource.ts. */
import { useEffect, useState } from "react";
import Alert from "@cloudscape-design/components/alert";
import FormField from "@cloudscape-design/components/form-field";
import Input from "@cloudscape-design/components/input";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Textarea from "@cloudscape-design/components/textarea";
import type { Json } from "./model";
import { block } from "./meta";
import { CONNECTOR_STORE, connectorSettings, settingSpec } from "./kbDataSource";

type Obj = { [k: string]: Json };
const asObj = (v: Json | undefined): Obj => (v && typeof v === "object" && !Array.isArray(v) ? v : {});

export const CONNECTORS: { value: string; label: string }[] = [
  { value: "", label: "Files: uploaded here, or in your S3 bucket" },
  { value: "web", label: "Web pages (crawl a site)" },
  { value: "confluence", label: "Confluence Cloud" },
  { value: "sharepoint", label: "SharePoint Online" },
  { value: "salesforce", label: "Salesforce" },
];

const LABELS: Record<string, string> = {
  seedUrls: "Start URLs", scope: "How far to crawl", inclusionFilters: "Only URLs matching", exclusionFilters: "Skip URLs matching",
  rateLimit: "Pages a minute per host", maxPages: "Most pages", userAgent: "User agent suffix", hostUrl: "Site URL",
  authType: "Sign-in method", credentialsSecretArn: "Credentials secret ARN", domain: "SharePoint domain",
  siteUrls: "Site URLs", tenantId: "Microsoft 365 tenant id", filters: "Content filters (JSON)",
};

/** JSON typed as text, written back only when it parses. */
function JsonText({ label, value, onChange }: { label: string; value: Json | undefined; onChange: (v: Json | undefined) => void }) {
  const shown = value === undefined ? "" : JSON.stringify(value, null, 1);
  const [text, setText] = useState(shown);
  useEffect(() => { try { if (JSON.stringify(JSON.parse(text || "null")) !== JSON.stringify(value ?? null)) setText(shown); } catch { /* typing */ } },
    [shown]); // eslint-disable-line react-hooks/exhaustive-deps
  return (
    <Textarea ariaLabel={label} rows={3} value={text} placeholder='[{"objectType": "Page", "include": [".*Policy.*"]}]'
      onChange={({ detail }) => {
        setText(detail.value);
        if (!detail.value.trim()) return onChange(undefined);
        try { onChange(JSON.parse(detail.value) as Json); } catch { /* not yet valid JSON */ }
      }} />
  );
}

export function DataSourceField({ value, onChange, vectorStore }: {
  value: Json | undefined; onChange: (v: Json | undefined) => void; vectorStore?: Json;
}) {
  const cur = asObj(value);
  const kind = typeof cur.type === "string" ? cur.type : "";
  const selected = CONNECTORS.find((o) => o.value === kind) ?? { value: kind, label: kind };
  const docs = (block("tool").keys.dataSource?.properties ?? {}) as Record<string, { doc?: string }>;
  const { required, optional } = connectorSettings(kind);
  const set = (k: string, v: Json | undefined) => {
    const next: Obj = { ...cur };
    if (v === undefined || v === "" || (Array.isArray(v) && !v.length)) delete next[k]; else next[k] = v;
    onChange(next);
  };
  const control = (k: string) => {
    const spec = settingSpec(k);
    const label = LABELS[k] ?? k;
    if (spec?.type === "enum") {
      const opts = [{ value: "", label: k === "scope" ? "The start URL's host and path (default)" : "(not set)" },
        ...Object.keys(spec.values ?? {}).map((v) => ({ value: v, label: v }))];
      return <Select ariaLabel={label} options={opts} selectedOption={opts.find((o) => o.value === cur[k]) ?? opts[0]}
        onChange={({ detail }) => set(k, detail.selectedOption.value || undefined)} />;
    }
    if (spec?.type === "list") {
      const lines = Array.isArray(cur[k]) ? (cur[k] as Json[]).map(String).join("\n") : "";
      return <Textarea ariaLabel={label} rows={2} value={lines} placeholder="One per line"
        onChange={({ detail }) => set(k, detail.value.split("\n").map((s) => s.trim()).filter(Boolean))} />;
    }
    if (spec?.type === "filters") return <JsonText label={label} value={cur[k]} onChange={(v) => set(k, v)} />;
    const isInt = spec?.type === "int";
    return <Input ariaLabel={label} type={isInt ? "number" : "text"} inputMode={isInt ? "numeric" : undefined}
      value={cur[k] === undefined || cur[k] === null ? "" : String(cur[k])}
      onChange={({ detail }) => {
        const v = detail.value.trim();
        set(k, v === "" ? undefined : isInt && Number.isFinite(Number(v)) ? Number(v) : v);
      }} />;
  };
  const store = asObj(vectorStore).type;
  return (
    <SpaceBetween size="s">
      <Select ariaLabel="Where documents come from" selectedOption={selected} options={CONNECTORS}
        onChange={({ detail }) => {
          const next = detail.selectedOption.value ?? "";
          // A new connector starts empty: one connector's settings mean nothing to another.
          onChange(next ? { type: next } : undefined);
        }} />
      {kind && store !== CONNECTOR_STORE ? (
        <Alert type="warning">
          AWS supports this connector only on an OpenSearch Serverless vector store. Choose one under Vector store.
        </Alert>
      ) : null}
      {[...required, ...optional].map((k) => (
        <FormField key={k} label={LABELS[k] ?? k} description={docs[k]?.doc}
          constraintText={required.includes(k) ? "Required" : "Optional"}>
          {control(k)}
        </FormField>
      ))}
    </SpaceBetween>
  );
}
