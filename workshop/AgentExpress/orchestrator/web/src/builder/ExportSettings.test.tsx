/** Settings > Export: pick the tool, paste the key, test, save. Each destination asks only for
 *  what it needs; the keys become the headers its docs name and are saved as the build's
 *  exportHeaders secret, never into the workflow; the test result is said in plain words. */
import { act, fireEvent, render, screen } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { useState } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const calls = vi.hoisted(() => ({
  get: vi.fn(), post: vi.fn(), put: vi.fn(), del: vi.fn(),
}));
vi.mock("../api", () => ({ api: calls }));

import { DeployPanel } from "./DeployPanel";
import { ExportSettings, exportSummary } from "./ExportSettings";
import { secretName } from "./exportSetup";
import type { Project } from "./model";
import type { BuildSummary } from "./storage";

const LF_URL = "https://cloud.langfuse.com/api/public/otel/v1/traces";
const start = (orchestrator?: Record<string, unknown>): Project => ({
  id: "p1", name: "P", createdAt: "", updatedAt: "", prompts: {},
  workflow: { agents: { a: { name: "A" } }, steps: [{ agent: "a" }], tools: {}, ...(orchestrator ? { orchestrator } : {}) } as never,
});
let latest: Project;
const notify = vi.fn();
function Harness({ p, buildId = "pclaims01" }: { p: Project; buildId?: string }) {
  const [project, setProject] = useState(p);
  latest = project;
  return <ExportSettings project={project} setProject={setProject} buildId={buildId || undefined} notify={notify} />;
}
const w = () => createWrapper(document.body);
const exportOf = () => (latest.workflow.orchestrator as Record<string, unknown> | undefined)?.export as Record<string, unknown> | undefined;
const fieldLabels = () => [...document.querySelectorAll("label")].map((l) => l.textContent ?? "").filter(Boolean);
const input = (label: string) => screen.getByLabelText(label) as HTMLInputElement;
const type = async (label: string, value: string) => {
  await act(async () => { fireEvent.change(input(label), { target: { value } }); });
};
const click = async (name: string | RegExp) => {
  await act(async () => { fireEvent.click(screen.getByRole("button", { name })); });
};
const pick = async (name: string) => {
  const tile = w().findTiles()!.findItems().find((t) => t.getElement().textContent?.startsWith(name))!;
  await act(async () => { tile.findNativeInput().click(); });
};
async function turnOn(p = start()) {
  await act(async () => { render(<Harness p={p} />); });
  await act(async () => { w().findToggle()!.findNativeInput().click(); });
}

beforeEach(() => {
  for (const f of Object.values(calls)) f.mockReset();
  notify.mockReset();
  calls.get.mockImplementation(async (path: string) => (path.includes("/secrets") ? { exportHeaders: [] } : []));
  calls.put.mockImplementation(async (_p: string, body: { exportHeaders: Record<string, string> }) =>
    ({ exportHeaders: Object.keys(body.exportHeaders) }));
});

describe("Export section", () => {
  it("is off until turned on, and then offers the five destinations as cards", async () => {
    await turnOn();
    const cards = w().findTiles()!.findItems().map((t) => t.getElement().textContent ?? "");
    expect(cards.map((c) => c.match(/^(Langfuse|Datadog|Grafana Cloud|Honeycomb|Any OTLP backend)/)?.[1]))
      .toEqual(["Langfuse", "Datadog", "Grafana Cloud", "Honeycomb", "Any OTLP backend"]);
    // Each with a one-line description.
    expect(cards[0]).toBe("LangfuseOpen-source LLM observability: traces, costs and evaluation scores.");
    expect(exportOf()).toBeUndefined();
  });

  it.each([
    ["Langfuse", ["Region", "Public key", "Secret key", "Project ID (optional)"]],
    ["Datadog", ["Datadog site", "API key"]],
    ["Grafana Cloud", ["OTLP endpoint", "Instance ID", "Token"]],
    ["Honeycomb", ["Region", "API key"]],
    ["Any OTLP backend", ["OTLP endpoint", "Headers"]],
  ])("%s asks only for what it needs", async (name, want) => {
    await turnOn();
    await pick(name);
    const got = fieldLabels();
    for (const l of want) expect(got).toContain(l);
    for (const l of ["Public key", "Secret key", "Instance ID", "Token", "OTLP endpoint", "Datadog site", "Headers"]
      .filter((l) => !want.includes(l))) expect(got).not.toContain(l);
    // Every destination says where to find its values.
    expect(document.body.textContent).toMatch(/Where are my Langfuse API keys\?|Datadog API keys|Send OTLP data to Grafana Cloud|Honeycomb API keys|OTLP over HTTP/);
  });

  it("tests Langfuse with the composed headers and says what happened", async () => {
    calls.post.mockResolvedValueOnce({ ok: true, status: 200, error: "", traceId: "ab".repeat(16), message: "Langfuse accepted a test trace." });
    await turnOn();
    await pick("Langfuse");
    await type("Public key", "pk-lf-1");
    await type("Secret key", "sk-lf-2");
    expect(input("Secret key").type).toBe("password");
    expect(document.body.textContent).toContain(`Sends to ${LF_URL}`);
    await click("Test connection");
    expect(calls.post).toHaveBeenCalledWith("/api/builds/pclaims01/export/test", {
      destination: "langfuse", region: "eu", endpoint: undefined,
      headers: { Authorization: `Basic ${btoa("pk-lf-1:sk-lf-2")}`, "x-langfuse-ingestion-version": "4" } });
    expect(document.body.textContent).toContain("Langfuse accepted a test trace.");

    calls.post.mockResolvedValueOnce({ ok: false, status: 401, traceId: "",
      error: "Langfuse refused the key (HTTP 401). Check the public and secret keys, and the region." });
    await click("Test connection");
    expect(document.body.textContent).toContain("Not connected");
    expect(document.body.textContent).toContain("Langfuse refused the key (HTTP 401)");
  });

  it("saves the keys as the build's secret for that address, and only the secret's name in the workflow", async () => {
    await turnOn();
    await pick("Langfuse");
    await type("Public key", "pk-lf-1");
    await type("Secret key", "sk-lf-2");
    await type("Project ID", "proj1");
    await click("Save");
    const name = await secretName(LF_URL);
    expect(calls.put).toHaveBeenCalledWith("/api/builds/pclaims01/secrets", { exportHeaders: {
      [name]: JSON.stringify({ Authorization: `Basic ${btoa("pk-lf-1:sk-lf-2")}`, "x-langfuse-ingestion-version": "4" }) } });
    expect(exportOf()).toEqual({ destination: "langfuse", region: "eu", secret: name, project: "proj1",
      send: { content: false, evals: true, metrics: true } });
    expect(JSON.stringify(latest.workflow)).not.toContain("sk-lf-2");
    expect(JSON.stringify(latest.workflow)).not.toContain(btoa("pk-lf-1:sk-lf-2"));
    // The key fields are emptied; a saved key is never shown again.
    expect(input("Secret key").value).toBe("");
    expect(document.body.textContent).toContain("Keys are saved for this address");
    expect(exportSummary(latest)).toBe("Export: Langfuse (EU)");
  });

  it("will not save without keys, and keys saved for one address are not reused for another", async () => {
    calls.get.mockImplementation(async (path: string) => (path.includes("/secrets")
      ? { exportHeaders: [await secretName(LF_URL)] } : []));
    await turnOn();
    await pick("Datadog");
    await click("Save");
    expect(document.body.textContent).toContain("enter the api key first");
    expect(exportOf()).toBeUndefined();
    await pick("Langfuse");
    expect(document.body.textContent).toContain("Keys are saved for this address");
    await click("Save");
    expect(exportOf()?.secret).toBe(await secretName(LF_URL));
    expect(calls.put).not.toHaveBeenCalled();
  });

  it("uses an existing secret's ARN, with nothing saved to the build", async () => {
    await turnOn();
    await pick("Any OTLP backend");
    await type("OTLP endpoint", "https://collector.example.com/v1/traces");
    await act(async () => { w().findAllToggles()[0].findNativeInput().click(); });
    await type("Secret ARN", "arn:aws:secretsmanager:us-east-1:111122223333:secret:otlp-headers-AbCdEf");
    await click("Save");
    expect(calls.put).not.toHaveBeenCalled();
    expect(exportOf()).toMatchObject({ destination: "otlp", endpoint: "https://collector.example.com/v1/traces",
      secret: "arn:aws:secretsmanager:us-east-1:111122223333:secret:otlp-headers-AbCdEf" });
  });

  it("refuses a plain-http address before anything is sent", async () => {
    await turnOn();
    await pick("Grafana Cloud");
    await type("OTLP endpoint", "http://otlp-gateway.example.net/otlp");
    expect(document.body.textContent).toContain("the address must be an https:// URL");
    await click("Save");
    expect(calls.put).not.toHaveBeenCalled();
    expect(calls.post).not.toHaveBeenCalled();
  });

  it("warns when prompts and answers are sent, and saves the choice", async () => {
    calls.get.mockImplementation(async (path: string) => (path.includes("/secrets")
      ? { exportHeaders: [await secretName("https://otlp.datadoghq.com/v1/traces")] } : []));
    await turnOn();
    await pick("Datadog");
    expect(document.body.textContent).not.toContain("leave your AWS account");
    const content = w().findAllCheckboxes().find((c) => c.getElement().textContent?.includes("Prompts and answers"))!;
    await act(async () => { content.findNativeInput().click(); });
    expect(document.body.textContent).toContain("leave your AWS account for Datadog");
    await click("Save");
    expect(exportOf()?.send).toEqual({ content: true, evals: true, metrics: true });
    expect(exportSummary(latest)).toBe("Export: Datadog (datadoghq.com), with prompts and answers");
  });

  it("keeps a setup in the library without its keys, and stops sending when asked", async () => {
    const name = await secretName(LF_URL);
    calls.post.mockImplementation(async (_p: string, body: Record<string, unknown>) => ({ id: "abcd1234", ...body }));
    await act(async () => { render(<Harness p={start({ export: { destination: "langfuse", region: "eu", secret: name } })} />); });
    await type("Name in the library", "teamLangfuse");
    await click("Save to library");
    expect(calls.post).toHaveBeenCalledWith("/api/library", { kind: "export", name: "teamLangfuse",
      description: "Langfuse (EU)", definition: { destination: "langfuse", region: "eu" } });
    vi.spyOn(window, "confirm").mockReturnValue(true);
    await click("Stop sending runs");
    expect(exportOf()).toBeUndefined();
  });

  it("the Deployment panel says where runs go", () => {
    const build = { id: "pclaims01", name: "Claims", agentName: "ax_1a2b3c4d", versions: 0 } as unknown as BuildSummary;
    render(<DeployPanel build={build} errors={0} can={() => true} onDeploy={vi.fn()} onDestroy={vi.fn()}
      exportLabel="Export: Langfuse (EU)" />);
    expect(screen.getByText("Export: Langfuse (EU)")).toBeTruthy();
  });
});
