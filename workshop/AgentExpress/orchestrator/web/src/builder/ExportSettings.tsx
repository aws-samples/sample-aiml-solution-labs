/** Settings > Export: send every run of this build to Langfuse, Datadog, Grafana Cloud,
 *  Honeycomb or any OTLP backend. Pick the tool, paste the key, test, save, deploy.
 *
 *  Save writes `orchestrator.export` into the workflow and the keys as the build's
 *  `exportHeaders` secret (PUT /api/builds/{id}/secrets), named after the address they are
 *  for (exportSetup.ts secretName). Once saved, a key is never shown again: the page only
 *  says that one is saved. A setup can be kept in the library and used in another build; its
 *  keys are not copied, so that build asks for its own (or reuses an existing secret's ARN). */
import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Checkbox from "@cloudscape-design/components/checkbox";
import Container from "@cloudscape-design/components/container";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import Input from "@cloudscape-design/components/input";
import Link from "@cloudscape-design/components/link";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import StatusIndicator from "@cloudscape-design/components/status-indicator";
import Textarea from "@cloudscape-design/components/textarea";
import Tiles from "@cloudscape-design/components/tiles";
import Toggle from "@cloudscape-design/components/toggle";
import { useEffect, useMemo, useState } from "react";
import type { Project } from "./model";
import {
  composeHeaders, DATADOG_SITE_NAMES, DATADOG_SITES, DEFAULT_REGION, DESTINATIONS, exportApi, exportLabel,
  forLibrary, HELP_LINKS, KEY_FIELDS, nameOf, SECRET_ARN_PATTERN, SEND_DEFAULTS, savedSetups, secretName,
  SetupError, tracesUrl, type Destination, type ExportSetup, type KeyFields, type SavedSetup, type TestResult,
} from "./exportSetup";
import { useDocsBase } from "../docs/docsLinks";
import { InfoLink, infoSlot } from "../docs/InfoLink";

type Notify = (type: "success" | "error" | "info", msg: string) => void;

const REGIONS: Partial<Record<Destination, { label: string; options: { value: string; label: string }[] }>> = {
  langfuse: { label: "Region", options: [{ value: "eu", label: "EU (cloud.langfuse.com)" },
    { value: "us", label: "US (us.cloud.langfuse.com)" }, { value: "self-hosted", label: "Self-hosted, or another Langfuse region" }] },
  datadog: { label: "Datadog site", options: Object.keys(DATADOG_SITES).map((s) => ({ value: s, label: `${DATADOG_SITE_NAMES[s]} (${s})` })) },
  honeycomb: { label: "Region", options: [{ value: "us", label: "US (api.honeycomb.io)" }, { value: "eu", label: "EU (api.eu1.honeycomb.io)" }] },
};

const ENDPOINT_FIELD: Partial<Record<Destination, { label: string; help: string; placeholder: string }>> = {
  langfuse: { label: "Langfuse address", placeholder: "https://langfuse.example.com",
    help: "Where your Langfuse runs. For Langfuse's Japan or HIPAA regions: https://jp.cloud.langfuse.com or https://hipaa.cloud.langfuse.com." },
  grafana: { label: "OTLP endpoint", placeholder: "https://otlp-gateway-prod-us-east-0.grafana.net/otlp",
    help: "On your stack's OpenTelemetry tile in the Grafana Cloud portal (Configure)." },
  otlp: { label: "OTLP endpoint", placeholder: "https://collector.example.com:4318",
    help: "The address your backend takes OTLP over HTTPS at, with or without /v1/traces at the end." },
};

const needsEndpoint = (s: Partial<ExportSetup>) =>
  s.destination === "grafana" || s.destination === "otlp" || (s.destination === "langfuse" && s.region === "self-hosted");

/** The page's words for a server answer. */
function testMessage(r: TestResult): string {
  return r.ok ? (r.message ?? "Accepted.") : r.error || `HTTP ${r.status}`;
}

export function exportOf(project: Project): ExportSetup | null {
  const orch = (project.workflow.orchestrator ?? {}) as Record<string, unknown>;
  const e = orch.export as ExportSetup | undefined;
  return e && typeof e === "object" && e.destination ? e : null;
}

export function withExport(project: Project, setup: ExportSetup | null): Project {
  const orch = { ...((project.workflow.orchestrator ?? {}) as Record<string, unknown>) };
  if (setup) orch.export = setup;
  else delete orch.export;
  return { ...project, workflow: { ...project.workflow, orchestrator: orch } as Project["workflow"] };
}

export function ExportSettings({ project, setProject, buildId, notify }: {
  project: Project; setProject: (p: Project) => void;
  /** The console build's id; absent in this-browser-only mode, where keys cannot be saved. */
  buildId?: string; notify: Notify;
}) {
  const docs = useDocsBase();
  const saved = exportOf(project);
  const [on, setOn] = useState(Boolean(saved));
  const [draft, setDraft] = useState<Partial<ExportSetup>>(saved ?? {});
  const [keys, setKeys] = useState<KeyFields>({});
  const [useArn, setUseArn] = useState(Boolean(saved?.secret?.startsWith("arn:")));
  const [arn, setArn] = useState(saved?.secret?.startsWith("arn:") ? saved.secret : "");
  const [savedNames, setSavedNames] = useState<string[] | null>(null);
  const [keyName, setKeyName] = useState("");
  const [testing, setTesting] = useState(false);
  const [result, setResult] = useState<TestResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [library, setLibrary] = useState<SavedSetup[] | null>(null);
  const [libName, setLibName] = useState("");

  // What is saved in the workflow is what the form starts from, and returns to after a save.
  const savedKey = JSON.stringify(saved);
  useEffect(() => {
    setOn(Boolean(saved));
    setDraft(saved ?? {});
    setUseArn(Boolean(saved?.secret?.startsWith("arn:")));
    setArn(saved?.secret?.startsWith("arn:") ? saved.secret : "");
  }, [savedKey]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (!buildId) return;
    let live = true;
    exportApi.savedNames(buildId).then((n) => { if (live) setSavedNames(n.exportHeaders ?? []); },
      () => { if (live) setSavedNames([]); });
    savedSetups.list().then((l) => { if (live) setLibrary(l); }, () => { if (live) setLibrary([]); });
    return () => { live = false; };
  }, [buildId]);

  const where = useMemo(() => {
    try { return { url: tracesUrl(draft), problem: "" }; } catch (e) {
      return { url: "", problem: e instanceof SetupError ? e.message : String(e) };
    }
  }, [draft]);
  useEffect(() => {
    let live = true;
    if (!where.url) { setKeyName(""); return; }
    void secretName(where.url).then((n) => { if (live) setKeyName(n); });
    return () => { live = false; };
  }, [where.url]);
  const keysSaved = Boolean(keyName && savedNames?.includes(keyName));
  const typed = Object.values(keys).some((v) => String(v ?? "").trim());

  const pick = (d: Destination) => {
    setDraft((s) => ({ destination: d, region: DEFAULT_REGION[d], send: s.send, project: d === "langfuse" ? s.project : undefined }));
    setKeys({});
    setResult(null);
    setError("");
  };
  const change = (patch: Partial<ExportSetup>) => { setDraft((s) => ({ ...s, ...patch })); setResult(null); };
  const send = { ...SEND_DEFAULTS, ...(draft.send ?? {}) };

  const headersNow = (): Record<string, string> | undefined => {
    if (!typed || !draft.destination) return undefined;
    return composeHeaders(draft.destination, keys);
  };

  const test = async () => {
    if (!buildId || !draft.destination) return;
    setTesting(true);
    setResult(null);
    setError("");
    try {
      setResult(await exportApi.test(buildId, draft, headersNow()));
    } catch (e) {
      setResult({ ok: false, status: 0, error: (e as Error).message, traceId: "" });
    } finally {
      setTesting(false);
    }
  };

  const save = async () => {
    if (!draft.destination) return;
    setBusy(true);
    setError("");
    try {
      const name = await secretName(tracesUrl(draft));
      let secret: string;
      if (useArn) {
        if (!SECRET_ARN_PATTERN.test(arn ?? "")) throw new SetupError("the secret ARN must be a full Secrets Manager ARN, with its six-character suffix");
        secret = arn!;
      } else if (typed) {
        if (!buildId) throw new SetupError("keys are saved with the build on the console: use an existing secret's ARN here");
        const headers = composeHeaders(draft.destination, keys);
        const names = await exportApi.saveKeys(buildId, name, headers);
        setSavedNames(names.exportHeaders ?? []);
        secret = name;
      } else if (savedNames?.includes(name)) {
        secret = name;
      } else {
        throw new SetupError(`enter the ${KEY_FIELDS[draft.destination].map((f) => f.label.toLowerCase()).join(" and ")} first`);
      }
      const setup: ExportSetup = { destination: draft.destination, ...(draft.region ? { region: draft.region } : {}),
        ...(needsEndpoint(draft) && draft.endpoint ? { endpoint: draft.endpoint.trim() } : {}),
        secret, ...(draft.project?.trim() ? { project: draft.project.trim() } : {}), send };
      setProject(withExport(project, setup));
      setKeys({});
      notify("success", `Runs will go to ${exportLabel(setup)} once you deploy.`);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const stop = () => {
    if (saved && !window.confirm(`Stop sending runs to ${exportLabel(saved)}? Its saved keys stay with the build until you replace them.`)) return;
    setProject(withExport(project, null));
    setOn(false);
    setDraft({});
    setResult(null);
  };

  const keep = async () => {
    if (!saved) return;
    try {
      const made = await savedSetups.save(libName.trim(), forLibrary(saved));
      setLibrary((l) => [...(l ?? []), made]);
      setLibName("");
      notify("success", `Saved "${made.name}" to your library. Its keys are not copied.`);
    } catch (e) {
      notify("error", (e as Error).message);
    }
  };
  const use = (item: SavedSetup) => {
    const d = item.definition;
    setOn(true);
    setDraft({ ...d, secret: undefined });
    setUseArn(Boolean(d.secret?.startsWith("arn:")));
    setArn(d.secret?.startsWith("arn:") ? d.secret : "");
    setKeys({});
    setResult(null);
    notify("info", `Loaded "${item.name}". Enter the keys for this build, then save.`);
  };

  const dest = draft.destination;
  const regions = dest ? REGIONS[dest] : undefined;
  const endpointField = dest && needsEndpoint(draft) ? ENDPOINT_FIELD[dest] : undefined;

  return (
    <Container header={<Header variant="h2"
      description="Send every run of this build to the observability tool your team uses, as it happens."
      info={saved || docs ? <>{saved ? <StatusIndicator type="success">{`Runs go to ${exportLabel(saved)}`}</StatusIndicator> : null}
        <InfoLink feature="settings.export" /></> : undefined}
      actions={saved ? <Button onClick={stop}>Stop sending runs</Button> : undefined}>
      Export</Header>}>
      <SpaceBetween size="l">
        {!on ? (
          <SpaceBetween size="s">
            <Toggle checked={false} onChange={({ detail }) => setOn(detail.checked)}>Send runs to another tool</Toggle>
            {library?.length ? <UseSaved items={library} onUse={use} /> : null}
          </SpaceBetween>
        ) : (
          <SpaceBetween size="l">
            <FormField label="1. Where to send runs" info={infoSlot(docs, "settings.exportDestination")}>
              <Tiles ariaLabel="Where to send runs" columns={3} value={dest ?? null}
                onChange={({ detail }) => pick(detail.value as Destination)}
                items={DESTINATIONS.map((d) => ({ value: d.id, label: d.name, description: d.about }))} />
            </FormField>
            {library?.length ? <UseSaved items={library} onUse={use} /> : null}

            {dest ? (
              <SpaceBetween size="m">
                <Box variant="h3">2. Connect {nameOf(dest)}<InfoLink feature="settings.exportConnect" /></Box>
                {regions ? (
                  <FormField label={regions.label}
                    description={dest === "datadog" ? "The site is in your Datadog address: app.datadoghq.com is US1, us5.datadoghq.com is US5." : undefined}>
                    <Select ariaLabel={regions.label}
                      selectedOption={regions.options.find((o) => o.value === draft.region) ?? null}
                      options={regions.options}
                      onChange={({ detail }) => change({ region: detail.selectedOption.value })} />
                  </FormField>
                ) : null}
                {dest === "datadog" ? (
                  <Link external href="https://docs.datadoghq.com/getting_started/site/">Which Datadog site am I on?</Link>
                ) : null}
                {endpointField ? (
                  <FormField label={endpointField.label} description={endpointField.help}
                    errorText={draft.endpoint ? where.problem || undefined : undefined}>
                    <Input ariaLabel={endpointField.label} value={draft.endpoint ?? ""} placeholder={endpointField.placeholder}
                      onChange={({ detail }) => change({ endpoint: detail.value })} />
                  </FormField>
                ) : null}

                <Toggle checked={useArn} onChange={({ detail }) => { setUseArn(detail.checked); setResult(null); }}>
                  Use an existing secret (ARN)</Toggle>
                {useArn ? (
                  <FormField label="Secret ARN"
                    description="A Secrets Manager secret in the account the build deploys to, holding a JSON object of header name to value. Its runtimes may read only this secret."
                    errorText={arn && !SECRET_ARN_PATTERN.test(arn) ? "A full secret ARN, ending in its six-character suffix." : undefined}>
                    <Input ariaLabel="Secret ARN" value={arn ?? ""} onChange={({ detail }) => setArn(detail.value)}
                      placeholder="arn:aws:secretsmanager:us-east-1:111122223333:secret:otlp-headers-AbCdEf" />
                  </FormField>
                ) : (
                  <SpaceBetween size="s">
                    {keysSaved ? (
                      <StatusIndicator type="success">Keys are saved for this address. Enter new ones only to replace them.</StatusIndicator>
                    ) : null}
                    {KEY_FIELDS[dest].map((f) => (
                      <FormField key={f.key} label={f.label} description={f.help}>
                        {f.multiline ? (
                          <Textarea ariaLabel={f.label} rows={3} value={keys[f.key] ?? ""} spellcheck={false}
                            onChange={({ detail }) => { setKeys((k) => ({ ...k, [f.key]: detail.value })); setResult(null); }} />
                        ) : (
                          <Input ariaLabel={f.label} type={f.secret ? "password" : "text"} value={keys[f.key] ?? ""}
                            autoComplete={false}
                            onChange={({ detail }) => { setKeys((k) => ({ ...k, [f.key]: detail.value })); setResult(null); }} />
                        )}
                      </FormField>
                    ))}
                    {!buildId ? <Box variant="small" color="text-body-secondary">
                      Keys are saved with the build on the console. Here, use an existing secret&apos;s ARN.</Box> : null}
                  </SpaceBetween>
                )}
                <Link external href={HELP_LINKS[dest].href}>{HELP_LINKS[dest].text}</Link>
                {dest === "langfuse" ? (
                  <FormField label="Project ID (optional)"
                    description="For a View in Langfuse link on each run. In Langfuse: your project's Settings, or the part of its address after /project/.">
                    <Input ariaLabel="Project ID" value={draft.project ?? ""} onChange={({ detail }) => change({ project: detail.value })} />
                  </FormField>
                ) : null}
                {where.url ? <Box variant="small" color="text-body-secondary">Sends to {where.url}</Box> : null}

                <Box variant="h3">3. Test the connection<InfoLink feature="settings.exportTest" /></Box>
                <SpaceBetween direction="horizontal" size="xs" alignItems="center">
                  <Button onClick={() => void test()} loading={testing}
                    disabled={!buildId || !where.url || useArn || (!typed && !keysSaved)}
                    disabledReason={!buildId ? "Testing needs the console." : useArn ? "A secret ARN is tested from the deployed app: Observability, Send test trace."
                      : !where.url ? where.problem : "Enter the keys first."}>
                    Test connection</Button>
                  <Box variant="small" color="text-body-secondary">Sends one small test trace now.</Box>
                </SpaceBetween>
                {result ? (
                  <Alert type={result.ok ? "success" : "error"} header={result.ok ? "Connected" : "Not connected"}>
                    {testMessage(result)}{result.ok && result.traceId ? ` Trace ID ${result.traceId}.` : ""}
                  </Alert>
                ) : null}

                <Box variant="h3">4. What to send<InfoLink feature="settings.exportWhat" /></Box>
                <SpaceBetween size="xs">
                  <Checkbox checked disabled description="Each run, its agents, model calls (model, tokens, cost) and tool calls, with timings and status.">
                    Traces of every run</Checkbox>
                  <Checkbox checked={send.evals} onChange={({ detail }) => change({ send: { ...send, evals: detail.checked } })}
                    description="Each evaluation's score, on its run.">Evaluation scores</Checkbox>
                  <Checkbox checked={send.metrics} onChange={({ detail }) => change({ send: { ...send, metrics: detail.checked } })}
                    description="Run counts, failures, duration, cost and tokens per agent and model, where the tool takes metrics (not Langfuse, which works them out itself).">
                    Metrics</Checkbox>
                  <Checkbox checked={send.content} onChange={({ detail }) => change({ send: { ...send, content: detail.checked } })}
                    description="The request, each agent's prompt and answer, and tool arguments. Off by default.">
                    Prompts and answers</Checkbox>
                  {send.content ? (
                    <Alert type="warning">
                      Prompts and answers can hold personal or confidential data, and they leave your AWS account for {nameOf(dest)}.
                      You are responsible for what you send and for {nameOf(dest)}&apos;s terms and data region. AgentExpress does not mask it.
                    </Alert>
                  ) : null}
                </SpaceBetween>

                {error ? <Alert type="error">{error}</Alert> : null}
                <SpaceBetween direction="horizontal" size="xs">
                  <Button variant="primary" loading={busy} onClick={() => void save()}>Save</Button>
                  {!saved ? <Button variant="link" onClick={() => { setOn(false); setDraft({}); setKeys({}); }}>Cancel</Button> : null}
                </SpaceBetween>
                <Box variant="small" color="text-body-secondary">
                  Saving changes this build; runs start going there when you deploy it. Saved keys are never shown again.</Box>
              </SpaceBetween>
            ) : null}

            {saved && buildId ? (
              <FormField label="Keep this setup in your library" description="To use it in another build. Its keys are not copied: that build asks for its own.">
                <SpaceBetween direction="horizontal" size="xs">
                  <Input ariaLabel="Name in the library" value={libName} placeholder="teamLangfuse"
                    onChange={({ detail }) => setLibName(detail.value)} />
                  <Button disabled={!/^[A-Za-z][A-Za-z0-9]{0,31}$/.test(libName.trim())}
                    disabledReason="A letter, then letters and digits." onClick={() => void keep()}>Save to library</Button>
                </SpaceBetween>
              </FormField>
            ) : null}
          </SpaceBetween>
        )}
      </SpaceBetween>
    </Container>
  );
}

function UseSaved({ items, onUse }: { items: SavedSetup[]; onUse: (s: SavedSetup) => void }) {
  return (
    <FormField label="Or use a saved setup" description="From your library. You enter this build's keys again.">
      <Select ariaLabel="Use a saved setup" selectedOption={null} placeholder="Pick a saved setup"
        options={items.map((i) => ({ value: i.id, label: i.name, description: exportLabel(i.definition) }))}
        onChange={({ detail }) => { const it = items.find((i) => i.id === detail.selectedOption.value); if (it) onUse(it); }} />
    </FormField>
  );
}

/** The Deployment panel's one line about it: "Export: Langfuse (EU)", or nothing when off. */
export function exportSummary(project: Project): string {
  const s = exportOf(project);
  return s ? `Export: ${exportLabel(s)}${s.send?.content ? ", with prompts and answers" : ""}` : "";
}
