/** A run's page says whether it reached Langfuse, Datadog..., links to its trace and re-sends it;
 *  the admins' Export card on Observability shows the last day's sends, tests and pauses. */
import { act, fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const calls = vi.hoisted(() => ({ get: vi.fn(), post: vi.fn(), put: vi.fn(), del: vi.fn() }));
vi.mock("../api", () => ({ api: calls }));

import type { SessionSnapshot } from "../types";
import { ExportCard, linkTo, RunExport, type ExportStatus } from "./RunExport";

const TRACE = "0af7651916cd43dd8448eb211c80319c";
const LANGFUSE: ExportStatus = { on: true, destination: "langfuse", label: "Langfuse (EU)", paused: false, canManage: false,
  traceLink: "https://cloud.langfuse.com/project/proj1/traces/{traceId}" };
const run = (over: Record<string, unknown> = {}) =>
  ({ session_id: "aaaaaaaaaaaa", topic: "t", overall: "done", ...over }) as unknown as SessionSnapshot;
const status = (s: ExportStatus) => calls.get.mockResolvedValue(s);
const show = async (ui: React.ReactElement) => { await act(async () => { render(ui); }); };
const text = () => document.body.textContent ?? "";

beforeEach(() => { for (const f of Object.values(calls)) f.mockReset(); });

describe("on a run's page", () => {
  it("shows nothing while the app sends runs nowhere", async () => {
    status({ on: false });
    await show(<RunExport snap={run({ export: { status: "sent", traceId: TRACE } })} canResend />);
    expect(text()).toBe("");
    expect(calls.get).toHaveBeenCalledWith("/api/export");
  });

  it("says Sent with a link to the trace", async () => {
    status(LANGFUSE);
    await show(<RunExport snap={run({ export: { status: "sent", traceId: TRACE, at: "2026-01-01T10:00:00Z" } })} canResend={false} />);
    expect(text()).toContain("Langfuse: Sent");
    const link = screen.getByRole("link", { name: /View in Langfuse/ }) as HTMLAnchorElement;
    expect(link.getAttribute("href")).toBe(`https://cloud.langfuse.com/project/proj1/traces/${TRACE}`);
    expect(screen.queryByRole("button", { name: /Re-send/ })).toBeNull();
  });

  it.each([
    [{ status: "failed", error: "HTTP 401" }, "Langfuse: Failed"],
    [{ status: "paused" }, "Langfuse: Paused"],
    [{ status: "off" }, "Langfuse: Off"],
    [undefined, "Langfuse: Not sent"],
  ])("a run whose export is %j says %s", async (exp, words) => {
    status(LANGFUSE);
    await show(<RunExport snap={run(exp ? { export: exp } : {})} canResend={false} />);
    expect(text()).toContain(words);
    expect(screen.queryByRole("link")).toBeNull();
  });

  it("waits for a running run, and offers no re-send until it ends", async () => {
    status(LANGFUSE);
    await show(<RunExport snap={run({ overall: "waiting_human" })} canResend />);
    expect(text()).toContain("Sends when it ends");
    expect(screen.queryByRole("button", { name: /Re-send/ })).toBeNull();
  });

  it("re-sends for its owner or an admin, and says how it went", async () => {
    status(LANGFUSE);
    calls.post.mockResolvedValueOnce({ ok: true, status: 200, error: "", traceId: TRACE, spans: 12 });
    await show(<RunExport snap={run({ export: { status: "failed" } })} canResend />);
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Re-send to Langfuse" })); });
    expect(calls.post).toHaveBeenCalledWith("/api/sessions/aaaaaaaaaaaa/export");
    expect(text()).toContain("Sent to Langfuse (12 spans).");
    calls.post.mockResolvedValueOnce({ ok: false, status: 0, error: "the runtime could not be reached", traceId: "" });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Re-send to Langfuse" })); });
    expect(text()).toContain("the runtime could not be reached");
  });

  it("links only a real trace id", () => {
    expect(linkTo("https://x/{traceId}", TRACE)).toBe(`https://x/${TRACE}`);
    expect(linkTo("https://x/{traceId}", "javascript:alert(1)")).toBe("");
    expect(linkTo("", TRACE)).toBe("");
  });

  it("a Datadog run links to the Trace Explorer on its site", async () => {
    status({ on: true, destination: "datadog", label: "Datadog (datadoghq.eu)",
      traceLink: "https://app.datadoghq.eu/apm/traces?query=trace_id%3A{traceId}" });
    await show(<RunExport snap={run({ export: { status: "sent", traceId: TRACE } })} canResend={false} />);
    expect((screen.getByRole("link", { name: /View in Datadog/ }) as HTMLAnchorElement).getAttribute("href"))
      .toBe(`https://app.datadoghq.eu/apm/traces?query=trace_id%3A${TRACE}`);
  });
});

describe("the Export card", () => {
  const ADMIN: ExportStatus = { ...LANGFUSE, canManage: true,
    recent: { sent: 41, failed: 2, lastError: "Langfuse refused the key (HTTP 401).", lastErrorAt: "2026-01-01T10:00:00Z" } };

  it("is for admins only", async () => {
    status(LANGFUSE);
    await show(<ExportCard />);
    expect(text()).toBe("");
  });

  it("shows where runs go and the last day's sends", async () => {
    status(ADMIN);
    await show(<ExportCard />);
    expect(text()).toContain("Langfuse (EU)");
    expect(text()).toContain("41");
    expect(text()).toContain("Last error (2026-01-01T10:00:00Z): Langfuse refused the key (HTTP 401).");
  });

  it("sends a test trace and says so", async () => {
    status(ADMIN);
    calls.post.mockResolvedValueOnce({ ok: true, status: 200, error: "", traceId: TRACE });
    await show(<ExportCard />);
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Send test trace" })); });
    expect(calls.post).toHaveBeenCalledWith("/api/export/test");
    expect(text()).toContain(`Langfuse accepted a test trace (trace ID ${TRACE}).`);
  });

  it("pauses after asking, and resumes", async () => {
    status(ADMIN);
    calls.put.mockResolvedValue({});
    const confirm = vi.spyOn(window, "confirm").mockReturnValue(true);
    await show(<ExportCard />);
    status({ ...ADMIN, paused: true, pausedBy: "admin@example.com" });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Pause" })); });
    expect(confirm).toHaveBeenCalled();
    expect(calls.put).toHaveBeenCalledWith("/api/export/pause", { paused: true });
    expect(text()).toContain("Paused by admin@example.com");
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Resume" })); });
    expect(calls.put).toHaveBeenLastCalledWith("/api/export/pause", { paused: false });
  });
});
