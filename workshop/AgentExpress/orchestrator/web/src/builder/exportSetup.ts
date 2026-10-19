/** Sending a build's runs to Langfuse, Datadog, Grafana Cloud, Honeycomb or any OTLP backend:
 *  what the Builder's Export section knows about each destination (`orchestrator.export`).
 *
 *  traces/label/secret naming are a copy of bff/traceexport.py; tests/fixtures/export_presets.json
 *  holds the two to the same answers (exportSetup.test.ts and tests/test_trace_export.py). The
 *  headers each destination needs, and where its values are found, come from the vendors' docs:
 *    Langfuse   https://langfuse.com/integrations/native/opentelemetry (Basic base64(pk:sk) and
 *               x-langfuse-ingestion-version: 4), https://langfuse.com/faq/all/where-are-langfuse-api-keys
 *    Datadog    https://docs.datadoghq.com/opentelemetry/setup/otlp_ingest/traces/ (dd-api-key,
 *               compute_stats: true for trace metrics), https://docs.datadoghq.com/getting_started/site/
 *    Grafana    https://grafana.com/docs/grafana-cloud/send-data/otlp/send-data-otlp/ (the stack's
 *               OpenTelemetry tile: endpoint, instance ID, token; Basic base64(instance:token))
 *    Honeycomb  https://docs.honeycomb.io/send-data/opentelemetry/ (x-honeycomb-team),
 *               https://docs.honeycomb.io/configure/environments/manage-api-keys/ */
import { api } from "../api";

export type Destination = "langfuse" | "datadog" | "grafana" | "honeycomb" | "otlp";
export interface SendChoices { content?: boolean; evals?: boolean; metrics?: boolean }
/** orchestrator.export, as workflow.json holds it (export-contract.md section 1). */
export interface ExportSetup {
  destination: Destination;
  region?: string;
  endpoint?: string;
  /** A build secret's name (kind exportHeaders), or an existing Secrets Manager ARN. */
  secret?: string;
  project?: string;
  send?: SendChoices;
}

export const SEND_DEFAULTS: Required<SendChoices> = { content: false, evals: true, metrics: true };
export const LANGFUSE_HOSTS: Record<string, string> = { us: "https://us.cloud.langfuse.com", eu: "https://cloud.langfuse.com" };
const LANGFUSE_PATH = "/api/public/otel";
/** Datadog site -> the host of its web app (bff/traceexport.py DATADOG_SITES). */
export const DATADOG_SITES: Record<string, string> = {
  "datadoghq.com": "app.datadoghq.com", "us3.datadoghq.com": "us3.datadoghq.com",
  "us5.datadoghq.com": "us5.datadoghq.com", "datadoghq.eu": "app.datadoghq.eu",
  "ap1.datadoghq.com": "ap1.datadoghq.com", "ap2.datadoghq.com": "ap2.datadoghq.com",
  "uk1.datadoghq.com": "uk1.datadoghq.com",
};
/** How people know their Datadog site: by the region name on its sign-in page and in its URL. */
export const DATADOG_SITE_NAMES: Record<string, string> = {
  "datadoghq.com": "US1", "us3.datadoghq.com": "US3", "us5.datadoghq.com": "US5", "datadoghq.eu": "EU",
  "ap1.datadoghq.com": "AP1", "ap2.datadoghq.com": "AP2", "uk1.datadoghq.com": "UK1",
};
export const HONEYCOMB_HOSTS: Record<string, string> = { us: "https://api.honeycomb.io", eu: "https://api.eu1.honeycomb.io" };
/** The deploy settings' endpoint shape (terraform var.otlp_endpoint, cdk/lib/otlp-export.ts). */
export const ENDPOINT_PATTERN = /^https:\/\/[A-Za-z0-9.-]+(:[0-9]{1,5})?(\/[^\s?#]*)?$/;
export const SECRET_ARN_PATTERN = /^arn:aws:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@!-]+-[A-Za-z0-9]{6}$/;

export const DESTINATIONS: { id: Destination; name: string; about: string }[] = [
  { id: "langfuse", name: "Langfuse", about: "Open-source LLM observability: traces, costs and evaluation scores." },
  { id: "datadog", name: "Datadog", about: "Datadog APM: runs as traces next to the rest of your services." },
  { id: "grafana", name: "Grafana Cloud", about: "Traces in Grafana Tempo, from your stack's OTLP endpoint." },
  { id: "honeycomb", name: "Honeycomb", about: "Traces you can query and slice by any field." },
  { id: "otlp", name: "Any OTLP backend", about: "An OpenTelemetry collector or any tool that takes OTLP over HTTPS." },
];
export const nameOf = (d: Destination) => DESTINATIONS.find((x) => x.id === d)?.name ?? d;

/** Thrown with the same words bff/traceexport.py refuses with. */
export class SetupError extends Error {}

function endpointOf(value?: string): string {
  const url = String(value ?? "").trim();
  if (!url) return "";
  if (url.length > 2048 || !ENDPOINT_PATTERN.test(url)) {
    throw new SetupError("the address must be an https:// URL with no user name, password, query or fragment");
  }
  return url.replace(/\/+$/, "");
}

/** With or without /v1/traces (or /v1/metrics): the rule the runtime sends by. */
const baseOf = (url: string) => url.replace(/\/v1\/(traces|metrics)$/, "");
const regionOf = (s: Partial<ExportSetup>) => String(s.region ?? "") || (s.destination ? DEFAULT_REGION[s.destination] ?? "" : "");

/** Where a setup sends traces. Mirrors traceexport.traces_url. */
export function tracesUrl(s: Partial<ExportSetup>): string {
  const region = regionOf(s);
  const endpoint = endpointOf(s.endpoint);
  switch (s.destination) {
    case "langfuse":
      if (!["us", "eu", "self-hosted"].includes(region)) throw new SetupError("the Langfuse region is us, eu or self-hosted");
      if (region !== "self-hosted") return `${LANGFUSE_HOSTS[region]}${LANGFUSE_PATH}/v1/traces`;
      if (!endpoint) throw new SetupError("a self-hosted Langfuse needs its address");
      return `${baseOf(endpoint).replace(/\/api\/public\/otel$/, "")}${LANGFUSE_PATH}/v1/traces`;
    case "datadog":
      if (!(region in DATADOG_SITES)) throw new SetupError(`the Datadog site is one of: ${Object.keys(DATADOG_SITES).join(", ")}`);
      return `https://otlp.${region}/v1/traces`;
    case "honeycomb":
      if (!(region in HONEYCOMB_HOSTS)) throw new SetupError("the Honeycomb region is us or eu");
      return `${HONEYCOMB_HOSTS[region]}/v1/traces`;
    case "grafana":
    case "otlp":
      if (!endpoint) {
        throw new SetupError(s.destination === "grafana" ? "Grafana Cloud needs your stack's OTLP endpoint"
          : "an OTLP backend needs its OTLP endpoint");
      }
      return `${baseOf(endpoint)}/v1/traces`;
    default:
      throw new SetupError("the destination is one of: langfuse, datadog, grafana, honeycomb, otlp");
  }
}

/** "Langfuse (EU)", "Datadog (us5.datadoghq.com)"... Mirrors traceexport.label. */
export function exportLabel(s: Partial<ExportSetup>): string {
  const region = regionOf(s);
  switch (s.destination) {
    case "langfuse": return region === "us" ? "Langfuse (US)" : region === "eu" ? "Langfuse (EU)" : "Langfuse (self-hosted)";
    case "datadog": return `Datadog (${region})`;
    case "honeycomb": return region === "eu" ? "Honeycomb (EU)" : "Honeycomb (US)";
    case "grafana": return "Grafana Cloud";
    default: return "OTLP backend";
  }
}

/** The link to a trace, with {traceId} for its id. Mirrors traceexport.trace_link. */
export function traceLink(s: Partial<ExportSetup>): string {
  const region = regionOf(s);
  if (s.destination === "langfuse") {
    const project = String(s.project ?? "").trim();
    if (!/^[A-Za-z0-9_-]{1,200}$/.test(project)) return "";
    let host = LANGFUSE_HOSTS[region];
    if (!host) {
      try { host = new URL(tracesUrl(s)).origin; } catch { return ""; }
    }
    return `${host}/project/${project}/traces/{traceId}`;
  }
  if (s.destination === "datadog" && region in DATADOG_SITES) {
    return `https://${DATADOG_SITES[region]}/apm/traces?query=trace_id%3A{traceId}`;
  }
  return "";
}

/** The exportHeaders secret a traces URL's headers are saved under: "export-" and the first 16
 *  hex digits of its SHA-256. The console's test uses saved keys only for this exact address. */
export async function secretName(url: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(url));
  const hex = [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
  return `export-${hex.slice(0, 16)}`;
}

const basic = (user: string, password: string) => `Basic ${btoa(`${user}:${password}`)}`;

/** The friendly fields a destination asks for, as typed on the page. Never stored as is. */
export interface KeyFields {
  publicKey?: string; secretKey?: string;     // Langfuse
  apiKey?: string;                            // Datadog, Honeycomb
  instanceId?: string; token?: string;        // Grafana Cloud
  headers?: string;                           // any OTLP backend: "Name: value" per line
}

/** The HTTP headers a destination's keys become (export-contract.md section 2), or why not yet. */
export function composeHeaders(d: Destination, k: KeyFields): Record<string, string> {
  const need = (v: string | undefined, what: string) => {
    const t = String(v ?? "").trim();
    if (!t) throw new SetupError(`enter the ${what}`);
    return t;
  };
  switch (d) {
    case "langfuse":
      return { Authorization: basic(need(k.publicKey, "public key"), need(k.secretKey, "secret key")),
        "x-langfuse-ingestion-version": "4" };
    case "datadog":
      return { "dd-api-key": need(k.apiKey, "API key"), compute_stats: "true" };
    case "grafana":
      return { Authorization: basic(need(k.instanceId, "instance ID"), need(k.token, "token")) };
    case "honeycomb":
      return { "x-honeycomb-team": need(k.apiKey, "API key") };
    default: {
      const out: Record<string, string> = {};
      for (const line of need(k.headers, "headers").split("\n").map((l) => l.trim()).filter(Boolean)) {
        const at = line.indexOf(":");
        if (at < 1) throw new SetupError(`"${line.slice(0, 40)}" is not "Name: value"`);
        out[line.slice(0, at).trim()] = line.slice(at + 1).trim();
      }
      return out;
    }
  }
}

/** One field the page shows for a destination's keys, with where to find it. */
export interface KeyField { key: keyof KeyFields; label: string; help: string; secret: boolean; multiline?: boolean }
export const KEY_FIELDS: Record<Destination, KeyField[]> = {
  langfuse: [
    { key: "publicKey", label: "Public key", secret: false, help: "Starts pk-lf-. In Langfuse: your project, then Settings, then API Keys." },
    { key: "secretKey", label: "Secret key", secret: true, help: "Starts sk-lf-. Shown once, when you create the key pair." },
  ],
  datadog: [{ key: "apiKey", label: "API key", secret: true, help: "In Datadog: Organization Settings, then API Keys. An API key, not an application key." }],
  grafana: [
    { key: "instanceId", label: "Instance ID", secret: false, help: "On your stack's OpenTelemetry tile (Configure), above the token." },
    { key: "token", label: "Token", secret: true, help: "Generate one on the same tile; it needs permission to write traces." },
  ],
  honeycomb: [{ key: "apiKey", label: "API key", secret: true, help: "An ingest key: Environment settings, then API Keys." }],
  otlp: [{ key: "headers", label: "Headers", secret: true, multiline: true, help: "One per line, as Name: value. For example: Authorization: Bearer …" }],
};

export const HELP_LINKS: Record<Destination, { text: string; href: string }> = {
  langfuse: { text: "Where are my Langfuse API keys?", href: "https://langfuse.com/faq/all/where-are-langfuse-api-keys" },
  datadog: { text: "Datadog API keys", href: "https://docs.datadoghq.com/account_management/api-app-keys/" },
  grafana: { text: "Send OTLP data to Grafana Cloud", href: "https://grafana.com/docs/grafana-cloud/send-data/otlp/send-data-otlp/" },
  honeycomb: { text: "Honeycomb API keys", href: "https://docs.honeycomb.io/configure/environments/manage-api-keys/" },
  otlp: { text: "OTLP over HTTP", href: "https://opentelemetry.io/docs/specs/otlp/" },
};

/** The default region a destination starts with. */
export const DEFAULT_REGION: Partial<Record<Destination, string>> = { langfuse: "eu", datadog: "datadoghq.com", honeycomb: "us" };

export interface TestResult { ok: boolean; status: number; error: string; traceId: string; url?: string; message?: string }

/** The console's one test span (POST /api/builds/{id}/export/test). Typed headers are used once;
 *  without them, the build's saved keys for this exact address. */
export const exportApi = {
  test: (buildId: string, s: Partial<ExportSetup>, headers?: Record<string, string>) =>
    api.post<TestResult>(`/api/builds/${encodeURIComponent(buildId)}/export/test`,
      { destination: s.destination, region: s.region, endpoint: s.endpoint, ...(headers ? { headers } : {}) }),
  /** Save the headers as the build's exportHeaders secret; returns the secret names set. */
  saveKeys: (buildId: string, name: string, headers: Record<string, string>) =>
    api.put<{ exportHeaders?: string[] }>(`/api/builds/${encodeURIComponent(buildId)}/secrets`,
      { exportHeaders: { [name]: JSON.stringify(headers) } }),
  savedNames: (buildId: string) =>
    api.get<{ exportHeaders?: string[] }>(`/api/builds/${encodeURIComponent(buildId)}/secrets`),
};

/** A saved setup in the library (bff/library.py kind "export"): no keys, ever. */
export interface SavedSetup { id: string; name: string; description?: string; definition: ExportSetup; mine?: boolean }
export const savedSetups = {
  list: () => api.get<SavedSetup[]>("/api/library?kind=export"),
  save: (name: string, definition: ExportSetup) =>
    api.post<SavedSetup>("/api/library", { kind: "export", name, description: exportLabel(definition), definition }),
  remove: (id: string) => api.del<{ ok: boolean }>(`/api/library/${encodeURIComponent(id)}`),
};

/** A setup as the library keeps it: the build's own secret name dropped, an ARN kept. */
export function forLibrary(s: ExportSetup): ExportSetup {
  const { secret, ...rest } = s;
  return secret && SECRET_ARN_PATTERN.test(secret) ? { ...rest, secret } : rest;
}
