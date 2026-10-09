/** The app's Triggers page (admins): what starts runs here besides a person, each
 *  webhook's URL and secret, a test delivery, and the last deliveries (bff/triggers.py). */
import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import ColumnLayout from "@cloudscape-design/components/column-layout";
import Container from "@cloudscape-design/components/container";
import CopyToClipboard from "@cloudscape-design/components/copy-to-clipboard";
import ExpandableSection from "@cloudscape-design/components/expandable-section";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import Input from "@cloudscape-design/components/input";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Spinner from "@cloudscape-design/components/spinner";
import StatusIndicator from "@cloudscape-design/components/status-indicator";
import Table from "@cloudscape-design/components/table";
import Textarea from "@cloudscape-design/components/textarea";
import { useCallback, useEffect, useState } from "react";

import { api } from "../api";

export interface TriggerRow {
  name: string;
  type: string;
  enabled: boolean;
  runAs: string;
  gates: string;
  prompt: string;
  url?: string;
  signature?: string;
  hasSecret?: boolean;
  expression?: string;
  timezone?: string;
  bucket?: string;
  prefix?: string;
  pattern?: unknown;
  bus?: string;
  deliveries: { at: string; source?: string; outcome?: string; session?: string; reason?: string }[];
}

/** A curl that sends one signed delivery: what a sender's code must do. */
export function curlFor(t: TriggerRow, secret: string): string {
  const body = '{"hello": "world"}';
  if (t.signature === "token") {
    return `curl -X POST '${t.url}' -H 'Content-Type: application/json' -H 'X-AX-Token: ${secret}' -d '${body}'`;
  }
  return [
    `BODY='${body}'; TS=$(date +%s)`,
    `SIG=$(printf '%s.%s' "$TS" "$BODY" | openssl dgst -sha256 -hmac '${secret}' | sed 's/^.* //')`,
    `curl -X POST '${t.url}' -H 'Content-Type: application/json' -H "X-AX-Timestamp: $TS" -H "X-AX-Signature: sha256=$SIG" -d "$BODY"`,
  ].join("\n");
}

const OUTCOME: Record<string, "success" | "warning" | "error" | "info"> = {
  started: "success", duplicate: "info", throttled: "warning", off: "info", unauthorized: "error", failed: "error", empty: "warning",
};

function TriggerCard({ t, onChanged, onOpenRun }: { t: TriggerRow; onChanged: () => void; onOpenRun: (id: string) => void }) {
  const [secret, setSecret] = useState("");
  const [given, setGiven] = useState("");
  const [body, setBody] = useState(t.type === "eventbridge" || t.type === "s3" ? '{"detail": {}}' : '{"text": "a test"}');
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");
  const [result, setResult] = useState<{ started: boolean; session_id?: string; reason?: string } | null>(null);
  const setKey = async (value: string) => {
    setBusy("secret"); setError("");
    try {
      const r = await api.post<{ secret: string }>(`/api/triggers/${encodeURIComponent(t.name)}/secret`, value ? { value } : {});
      setSecret(r.secret); setGiven(""); onChanged();
    } catch (e) { setError((e as Error).message); } finally { setBusy(""); }
  };
  let parsed: unknown = null;
  try { parsed = JSON.parse(body); } catch { parsed = null; }
  return (
    <Container header={<Header variant="h2"
      description={`${t.type}${t.enabled ? "" : " · off"} · runs as ${t.runAs === "service" ? "the trigger" : "the build's owner"}`
        + (t.gates === "auto" ? " · gates approve themselves" : "")}>{t.name}</Header>}>
      <SpaceBetween size="m">
        <Box variant="small" color="text-body-secondary">Request: {t.prompt}</Box>
        {t.type === "webhook" ? (
          <SpaceBetween size="s">
            <FormField label="URL" description={`Signed as ${t.signature}. POST JSON; at most 256 KB.`}>
              <SpaceBetween direction="horizontal" size="xs">
                <Box variant="code">{t.url}</Box>
                <CopyToClipboard textToCopy={t.url ?? ""} variant="icon" copySuccessText="Copied" copyErrorText="Could not copy" />
              </SpaceBetween>
            </FormField>
            <FormField label="Secret" description={t.hasSecret ? "Set. Generate a new one to rotate it; the old one stops working at once."
              : "Not set: every delivery is refused until it is."}>
              <SpaceBetween direction="horizontal" size="xs">
                <Button loading={busy === "secret"} onClick={() => void setKey("")}>{t.hasSecret ? "Rotate" : "Generate"}</Button>
                {t.signature !== "agentexpress" && t.signature !== "token" ? (
                  <>
                    <Input value={given} type="password" placeholder={`Paste ${t.signature}'s signing secret`}
                      ariaLabel={`${t.name} signing secret`} onChange={({ detail }) => setGiven(detail.value)} />
                    <Button disabled={given.length < 16} onClick={() => void setKey(given)}>Save</Button>
                  </>
                ) : null}
              </SpaceBetween>
            </FormField>
            {secret ? (
              <Alert type="success" header="Copy it now: it is not shown again">
                <SpaceBetween size="xs">
                  <SpaceBetween direction="horizontal" size="xs">
                    <Box variant="code">{secret}</Box>
                    <CopyToClipboard textToCopy={secret} variant="icon" copySuccessText="Copied" copyErrorText="Could not copy" />
                  </SpaceBetween>
                  {t.signature === "agentexpress" || t.signature === "token"
                    ? <pre className="axb-code">{curlFor(t, secret)}</pre> : null}
                </SpaceBetween>
              </Alert>
            ) : null}
          </SpaceBetween>
        ) : (
          <ColumnLayout columns={2} variant="text-grid">
            {t.expression ? <div><Box variant="awsui-key-label">Schedule</Box>{t.expression} ({t.timezone ?? "UTC"})</div> : null}
            {t.bucket ? <div><Box variant="awsui-key-label">Bucket</Box>s3://{t.bucket}/{t.prefix ?? ""}</div> : null}
            {t.pattern ? <div><Box variant="awsui-key-label">Pattern</Box><code>{JSON.stringify(t.pattern)}</code></div> : null}
            {t.bus ? <div><Box variant="awsui-key-label">Bus</Box>{t.bus}</div> : null}
          </ColumnLayout>
        )}
        <ExpandableSection headerText="Send a test delivery" variant="footer">
          <SpaceBetween size="s">
            <Box variant="small" color="text-body-secondary">
              Starts a real run, as if it had arrived{t.type === "webhook" ? " (the signature is not checked: you are signed in)" : ""}.
              {t.type === "webhook" || t.type === "sqs" ? " The JSON is the body." : " The JSON is the event."}
            </Box>
            <div className="axp-cedar"><Textarea rows={4} value={body} spellcheck={false} ariaLabel={`${t.name} test delivery`}
              onChange={({ detail }) => setBody(detail.value)} /></div>
            <Button loading={busy === "test"} disabled={parsed === null} onClick={async () => {
              setBusy("test"); setError(""); setResult(null);
              try {
                const payload = t.type === "webhook" || t.type === "sqs" ? { body: parsed } : { event: parsed };
                setResult(await api.post(`/api/triggers/${encodeURIComponent(t.name)}/test`, payload));
                onChanged();
              } catch (e) { setError((e as Error).message); } finally { setBusy(""); }
            }}>Send</Button>
            {result ? (result.started && result.session_id
              ? <StatusIndicator type="success">Started <Button variant="inline-link" onClick={() => onOpenRun(result.session_id!)}>{result.session_id}</Button></StatusIndicator>
              : <StatusIndicator type="warning">Not started: {result.reason}</StatusIndicator>) : null}
          </SpaceBetween>
        </ExpandableSection>
        {error ? <Alert type="error">{error}</Alert> : null}
        <Table variant="embedded" items={t.deliveries} header={<Header variant="h3">Last deliveries</Header>}
          empty={<Box color="text-body-secondary">None yet.</Box>}
          columnDefinitions={[
            { id: "at", header: "When", cell: (d) => d.at },
            { id: "source", header: "From", cell: (d) => d.source ?? "" },
            { id: "outcome", header: "Outcome", cell: (d) => <StatusIndicator type={OUTCOME[d.outcome ?? ""] ?? "info"}>{d.outcome}{d.reason ? `: ${d.reason}` : ""}</StatusIndicator> },
            { id: "run", header: "Run", cell: (d) => d.session
              ? <Button variant="inline-link" onClick={() => onOpenRun(d.session!)}>{d.session}</Button> : "—" },
          ]} />
      </SpaceBetween>
    </Container>
  );
}

export function TriggersPage({ onOpenRun }: { onOpenRun: (id: string) => void }) {
  const [rows, setRows] = useState<TriggerRow[] | null>(null);
  const [error, setError] = useState("");
  const load = useCallback(async () => {
    try { setRows((await api.get<{ triggers: TriggerRow[] }>("/api/triggers")).triggers); setError(""); }
    catch (e) { setError((e as Error).message); }
  }, []);
  useEffect(() => { void load(); }, [load]);
  return (
    <SpaceBetween size="l">
      <Header variant="h1" description="What starts runs here besides a person. Designed in the Builder; secrets and tests here.">
        Triggers</Header>
      {error ? <Alert type="error">{error}</Alert> : null}
      {rows === null && !error ? <Spinner size="large" /> : null}
      {rows?.length === 0 ? <Box color="text-body-secondary">No triggers in this workflow.</Box> : null}
      {rows?.map((t) => <TriggerCard key={t.name} t={t} onChanged={() => void load()} onOpenRun={onOpenRun} />)}
    </SpaceBetween>
  );
}
