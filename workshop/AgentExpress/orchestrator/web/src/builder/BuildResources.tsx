/** What a build needs besides its workflow: credentials and documents.
 *
 *  Neither can live in workflow.json. A tool's API key and a remote agent's bearer
 *  token are secrets — stored in Secrets Manager, write-only: this page can set,
 *  replace and clear one and see WHETHER it is set, never read it back. A knowledge
 *  base's documents are files — uploaded straight to the console's builds bucket and
 *  copied into kb_docs/<corpus>/ when the build deploys. Both only exist with the
 *  console's builds store (the server), so the Inspector shows them only there. */

import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import FormField from "@cloudscape-design/components/form-field";
import Input from "@cloudscape-design/components/input";
import SpaceBetween from "@cloudscape-design/components/space-between";
import StatusIndicator from "@cloudscape-design/components/status-indicator";
import Table from "@cloudscape-design/components/table";
import { useCallback, useEffect, useRef, useState } from "react";

import { buildDocs, buildSecrets, type KbDoc, type SecretKind } from "./storage";

export function SecretField({ buildId, kind, name, label, description }: {
  buildId: string; kind: SecretKind; name: string; label: string; description: string;
}) {
  const [isSet, setIsSet] = useState<boolean | null>(null);
  const [value, setValue] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let live = true;
    buildSecrets.names(buildId)
      .then((n) => { if (live) setIsSet((n[kind] ?? []).includes(name)); })
      .catch(() => { if (live) setIsSet(false); });
    return () => { live = false; };
  }, [buildId, kind, name]);

  const save = async (v: string) => {
    setBusy(true);
    try {
      const n = await buildSecrets.set(buildId, kind, name, v);
      setIsSet((n[kind] ?? []).includes(name));
      setValue("");
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <FormField label={label} description={description} errorText={error}
      info={isSet === null ? undefined
        : <StatusIndicator type={isSet ? "success" : "pending"}>{isSet ? "Set" : "Not set"}</StatusIndicator>}>
      <SpaceBetween direction="horizontal" size="xs">
        <Input type="password" value={value} ariaLabel={label} autoComplete={false}
          placeholder={isSet ? "Enter a new value to replace it" : "Paste the value"}
          onChange={({ detail }) => setValue(detail.value)} />
        <Button loading={busy} disabled={!value} onClick={() => void save(value)}>Save</Button>
        {isSet ? <Button variant="link" disabled={busy} onClick={() => void save("")}>Clear</Button> : null}
      </SpaceBetween>
    </FormField>
  );
}

const fmtSize = (n: number) => (n > 1_048_576 ? `${(n / 1_048_576).toFixed(1)} MB`
  : n > 1024 ? `${Math.round(n / 1024)} KB` : `${n} B`);

export function KbDocuments({ buildId, corpora }: { buildId: string; corpora: string[] }) {
  const [docs, setDocs] = useState<KbDoc[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [uploading, setUploading] = useState<string | null>(null);
  const input = useRef<HTMLInputElement>(null);
  const [into, setInto] = useState("");

  const refresh = useCallback(async () => {
    try { setDocs(await buildDocs.list(buildId)); setError(null); } catch (e) { setError((e as Error).message); }
  }, [buildId]);
  useEffect(() => { void refresh(); }, [refresh]);

  const upload = async (corpus: string, files: FileList) => {
    setUploading(corpus);
    try {
      for (const f of Array.from(files)) await buildDocs.upload(buildId, corpus, f);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setUploading(null);
      void refresh();
    }
  };

  if (!corpora.length) {
    return <Box color="text-body-secondary">Add a corpus name above, then upload its documents here.</Box>;
  }
  const rows = docs ?? [];
  return (
    <SpaceBetween size="s">
      <Box variant="small" color="text-body-secondary">
        Each corpus is a set of documents an agent can retrieve from. Uploads are copied into the knowledge base
        when the build deploys — deploy again after changing them. PDF, text, Markdown, HTML, CSV, Word or
        Excel; up to 50 MB each.
      </Box>
      {error ? <Box color="text-status-error">{error}</Box> : null}
      <input ref={input} type="file" multiple hidden
        accept=".pdf,.txt,.md,.html,.htm,.csv,.doc,.docx,.xls,.xlsx"
        onChange={(e) => { if (e.target.files?.length) void upload(into, e.target.files); e.target.value = ""; }} />
      {corpora.map((c) => {
        const mine = rows.filter((d) => d.corpus === c);
        return (
          <Table key={c} variant="embedded" loading={docs === null} loadingText="Listing documents"
            header={
              <SpaceBetween direction="horizontal" size="xs" alignItems="center">
                <Box variant="h4">{c}</Box>
                <Button iconName="upload" loading={uploading === c}
                  onClick={() => { setInto(c); input.current?.click(); }}>Upload</Button>
              </SpaceBetween>
            }
            items={mine}
            columnDefinitions={[
              { id: "name", header: "Document", cell: (d) => d.name },
              { id: "size", header: "Size", cell: (d) => fmtSize(d.size) },
              {
                id: "del", header: "", cell: (d) => (
                  <Button variant="icon" iconName="remove" ariaLabel={`Delete ${d.name}`}
                    onClick={() => void buildDocs.remove(buildId, d.corpus, d.name).then(refresh)} />
                ),
              },
            ]}
            empty={<Box color="text-body-secondary">No documents yet — a corpus with none fails the deploy.</Box>}
          />
        );
      })}
    </SpaceBetween>
  );
}
