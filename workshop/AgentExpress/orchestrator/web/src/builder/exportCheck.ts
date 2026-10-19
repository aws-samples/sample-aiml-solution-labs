/** Checks for `orchestrator.export`: sending each run to Langfuse, Datadog, Grafana Cloud,
 *  Honeycomb or any OTLP backend (app/features/observability/runexport.py). The generic key
 *  checks (keys.json) say `export` is an object; these are the rest. Mirrors
 *  bff/export_check.py word for word (both run tests/fixtures/validation_cases.json). */
import type { Issue, Where } from "./validate";

type Add = (severity: Issue["severity"], where: Where, path: string, message: string) => void;
const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);

export const EXPORT_DESTINATIONS = ["langfuse", "datadog", "grafana", "honeycomb", "otlp"];
export const LANGFUSE_REGIONS = ["us", "eu", "self-hosted"];
export const DATADOG_SITES = ["datadoghq.com", "us3.datadoghq.com", "us5.datadoghq.com", "datadoghq.eu",
  "ap1.datadoghq.com", "ap2.datadoghq.com", "uk1.datadoghq.com"];
export const HONEYCOMB_REGIONS = ["us", "eu"];
export const METRICS_DESTINATIONS = ["datadog", "honeycomb", "otlp"];
const REGIONS: Record<string, string[]> = {
  langfuse: LANGFUSE_REGIONS, datadog: DATADOG_SITES, honeycomb: HONEYCOMB_REGIONS,
};
const SEND_KEYS = ["content", "evals", "metrics"];
const ENDPOINT_RE = /^https:\/\/[A-Za-z0-9.-]+(:[0-9]{1,5})?(\/[^\s?#]*)?$/;
const SECRET_ARN_RE = /^arn:aws:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@!-]+-[A-Za-z0-9]{6}$/;
const SECRET_NAME_RE = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$/;
const MAX_PROJECT = 200;

const needsEndpoint = (dest: string, region: unknown): boolean =>
  dest === "otlp" || dest === "grafana" || (dest === "langfuse" && region === "self-hosted");

/** The keys' names and JSON types are checked from keys.json (validate.ts checkEntry); a value
 *  of the wrong type is left to that check, so it is reported once. */
export function checkExport(orch: unknown, add: Add): void {
  const ex = isObj(orch) ? orch.export : undefined;
  if (!isObj(ex)) return;
  const where: Where = { kind: "block", name: "orchestrator" };
  const p = "orchestrator.export";
  const dest = ex.destination;
  if (dest === undefined || dest === null) {
    add("error", where, `${p}.destination`, `is required: one of ${EXPORT_DESTINATIONS.join(", ")}`);
    return;
  }
  if (typeof dest !== "string") return;
  if (!EXPORT_DESTINATIONS.includes(dest)) {
    add("error", where, `${p}.destination`, `must be one of: ${EXPORT_DESTINATIONS.join(", ")}`);
    return;
  }
  const region = ex.region;
  if (typeof region === "string") {
    if (!(dest in REGIONS)) add("error", where, `${p}.region`, "applies only to langfuse, datadog and honeycomb; remove it");
    else if (!REGIONS[dest].includes(region)) {
      add("error", where, `${p}.region`, `must be one of: ${REGIONS[dest].join(", ")}`);
    }
  }
  const endpoint = ex.endpoint;
  if (endpoint === undefined || endpoint === null) {
    if (needsEndpoint(dest, region)) add("error", where, `${p}.endpoint`, `is required for ${dest}: the https:// address to send to`);
  } else if (typeof endpoint !== "string") {
    // left to the type check
  } else if (!needsEndpoint(dest, region)) {
    add("error", where, `${p}.endpoint`,
      "is set from destination and region; remove it, or use destination otlp to send anywhere");
  } else if (!ENDPOINT_RE.test(endpoint)) {
    add("error", where, `${p}.endpoint`, "must be an https:// URL with no user info, query or fragment");
  }
  const secret = ex.secret;
  if (secret === undefined || secret === null) {
    add("error", where, `${p}.secret`,
      "is required: the name of the build secret holding the headers, or a Secrets Manager secret ARN");
  } else if (typeof secret !== "string") {
    // left to the type check
  } else if (secret.startsWith("arn:")) {
    if (!SECRET_ARN_RE.test(secret)) {
      add("error", where, `${p}.secret`,
        "must be the full ARN of a Secrets Manager secret, including the six-character suffix");
    }
  } else if (!SECRET_NAME_RE.test(secret)) {
    add("error", where, `${p}.secret`, "must be a secret name: letters, digits, . _ - (up to 64)");
  }
  const project = ex.project;
  // eslint-disable-next-line no-control-regex
  if (typeof project === "string" && (project.length > MAX_PROJECT || /[\x00-\x1f]/.test(project))) {
    add("error", where, `${p}.project`, `must be text, up to ${MAX_PROJECT} characters`);
  }
  const send = ex.send;
  if (!isObj(send)) return;
  for (const [k, v] of Object.entries(send)) {
    if (!SEND_KEYS.includes(k)) add("error", where, `${p}.send.${k}`, `unknown key; send takes: ${SEND_KEYS.join(", ")}`);
    else if (typeof v !== "boolean") add("error", where, `${p}.send.${k}`, "must be true or false");
  }
  if (send.content === true) {
    add("warning", where, `${p}.send.content`,
      `sends prompts, answers and tool input and output to ${dest}, outside your AWS account; ` +
      "check that your data may go there");
  }
  if (send.metrics === true && !METRICS_DESTINATIONS.includes(dest)) {
    add("warning", where, `${p}.send.metrics`,
      `has no effect for ${dest}: metrics go only to ${METRICS_DESTINATIONS.join(", ")}`);
  }
}
