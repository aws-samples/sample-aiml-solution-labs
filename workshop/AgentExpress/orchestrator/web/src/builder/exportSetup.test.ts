/** The Export section's presets agree with the BFF's (tests/fixtures/export_presets.json, also
 *  run by tests/test_trace_export.py), and each destination's keys become the headers its docs
 *  ask for. */
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import {
  composeHeaders, exportLabel, forLibrary, secretName, SetupError, traceLink, tracesUrl, type ExportSetup,
} from "./exportSetup";

interface Case { setup: Partial<ExportSetup>; url?: string; label?: string; link?: string; error?: string }
const FIXTURE: { cases: Case[]; secretNames: { url: string; name: string }[] } = JSON.parse(
  readFileSync(resolve(process.cwd(), "../tests/fixtures/export_presets.json"), "utf8"));

describe("presets, the same as bff/traceexport.py", () => {
  for (const c of FIXTURE.cases) {
    it(JSON.stringify(c.setup), () => {
      if (c.error) {
        expect(() => tracesUrl(c.setup)).toThrow(SetupError);
        expect(() => tracesUrl(c.setup)).toThrow(c.error);
        return;
      }
      expect(tracesUrl(c.setup)).toBe(c.url);
      expect(exportLabel(c.setup)).toBe(c.label);
      expect(traceLink(c.setup)).toBe(c.link);
    });
  }
  it("names the saved keys after the address, as the console looks them up", async () => {
    for (const s of FIXTURE.secretNames) expect(await secretName(s.url)).toBe(s.name);
  });
});

describe("keys become headers", () => {
  it("Langfuse: Basic base64(public:secret), and real-time ingestion", () => {
    expect(composeHeaders("langfuse", { publicKey: " pk-lf-1 ", secretKey: "sk-lf-2" })).toEqual({
      Authorization: `Basic ${Buffer.from("pk-lf-1:sk-lf-2").toString("base64")}`, "x-langfuse-ingestion-version": "4" });
    expect(() => composeHeaders("langfuse", { publicKey: "pk" })).toThrow("enter the secret key");
  });
  it("Datadog, Grafana Cloud and Honeycomb as their docs say", () => {
    expect(composeHeaders("datadog", { apiKey: "dd1" })).toEqual({ "dd-api-key": "dd1", compute_stats: "true" });
    expect(composeHeaders("grafana", { instanceId: "123456", token: "glc_x" }))
      .toEqual({ Authorization: `Basic ${Buffer.from("123456:glc_x").toString("base64")}` });
    expect(composeHeaders("honeycomb", { apiKey: "hc1" })).toEqual({ "x-honeycomb-team": "hc1" });
    expect(() => composeHeaders("honeycomb", {})).toThrow("enter the API key");
  });
  it("any OTLP backend: one Name: value per line", () => {
    expect(composeHeaders("otlp", { headers: "Authorization: Bearer a:b\n\n x-tenant : t1 " }))
      .toEqual({ Authorization: "Bearer a:b", "x-tenant": "t1" });
    expect(() => composeHeaders("otlp", { headers: "no colon here" })).toThrow('is not "Name: value"');
  });
});

describe("a saved setup", () => {
  it("drops this build's key name and keeps an existing secret's ARN", () => {
    const arn = "arn:aws:secretsmanager:us-east-1:111122223333:secret:otlp-headers-AbCdEf";
    expect(forLibrary({ destination: "langfuse", region: "eu", secret: "export-89ecd80e38f1f3f3", project: "p" }))
      .toEqual({ destination: "langfuse", region: "eu", project: "p" });
    expect(forLibrary({ destination: "otlp", endpoint: "https://c.example.com/v1/traces", secret: arn }).secret).toBe(arn);
  });
});
