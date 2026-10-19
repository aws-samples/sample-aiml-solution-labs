/** The Analytics view: what it asks for, and how it shows what came back. */
import { render, screen, waitFor } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

const get = vi.fn();
vi.mock("../api", () => ({ api: { get: (p: string) => get(p) } }));

import { dayBefore } from "./Activity";
import {
  Analytics, analyticsPath, healthType, ms, pct, warningsOf,
  type AgentStats, type AnalyticsReport, type Coverage,
} from "./Analytics";

const agent = (over: Partial<AgentStats>): AgentStats => ({
  agentId: "a", rows: 1, calls: 1, errors: 0, otherStatus: 0, other: 0, guardrailChecks: 0,
  guardrailBlocks: 0, allowed: 0, denied: 0, logOnly: 0, errorRate: 0, guardrailBlockRate: null,
  denialRate: null, p50Ms: 100, p95Ms: 100, health: 100, reasons: [], ...over,
});

const COVERAGE: Coverage = {
  agents: [
    { agentId: "intake", name: "Intake", input: true, output: false, guardrail: "the deployment guardrail", warning: null },
    { agentId: "report", name: "Report", input: false, output: false, guardrail: null,
      warning: "no guardrail: neither its input nor its output is screened" },
  ],
  tools: [{ tool: "kb", type: "kb", warning: null },
    { tool: "pricing", type: "lambda", warning: "Cedar runs in LOG_ONLY: decisions are logged to CloudWatch, not enforced" }],
  policyEngine: { enabled: true, mode: "ENFORCE" },
};

const REPORT: AnalyticsReport = {
  from: "2025-03-01", to: "2025-03-07", rowsRead: 42, capped: false, maxRows: 20000,
  percentiles: "exact over the rows read (nearest rank)", kinds: { llm: 30, tool: 12 },
  tools: [{ tool: "kb", calls: 12, errors: 3, otherStatus: 0, allowed: 8, denied: 2, logOnly: 0,
    errorRate: 0.25, denialRate: 0.2, p50Ms: 80, p95Ms: 400 }],
  agents: [
    agent({ agentId: "intake", health: 100 }),
    agent({ agentId: "idle", calls: 0, health: null, reasons: ["no calls in this range"], p50Ms: null, p95Ms: null, errorRate: null }),
    agent({ agentId: "report", calls: 20, errors: 2, errorRate: 0.1, allowed: 3, denied: 1, health: 89,
      reasons: ["error rate 10% (2 of 20 calls) (-6)", "Cedar denied 25% of tool calls (1 of 4) (-5)"] }),
  ],
  coverage: COVERAGE,
};

describe("reading the figures", () => {
  it("shows a rate, a latency and an empty one plainly", () => {
    expect([pct(0.125), pct(0), pct(1), pct(null), pct(undefined)]).toEqual(["12.5%", "0%", "100%", "—", "—"]);
    expect([ms(420), ms(null)]).toEqual(["420 ms", "—"]);
  });
  it("bands a score for the eye only", () => {
    expect([healthType(100), healthType(90), healthType(89), healthType(70), healthType(69), healthType(null)])
      .toEqual(["success", "success", "warning", "warning", "error", "stopped"]);
  });
  it("asks for the range, and for a build only when one is picked", () => {
    expect(analyticsPath("2025-03-01", "2025-03-07")).toBe("/api/analytics?from=2025-03-01&to=2025-03-07");
    expect(analyticsPath("2025-03-01", "2025-03-07", "pclaims01"))
      .toBe("/api/analytics?from=2025-03-01&to=2025-03-07&build=pclaims01");
  });
  it("lists every warning, naming what it is about", () => {
    expect(warningsOf(COVERAGE)).toEqual([
      "Agent Report (report): no guardrail: neither its input nor its output is screened",
      "Tool pricing: Cedar runs in LOG_ONLY: decisions are logged to CloudWatch, not enforced",
    ]);
    expect(warningsOf(undefined)).toEqual([]);
  });
});

describe("the view", () => {
  beforeEach(() => { get.mockReset(); });

  it("asks for the last 7 days and shows the worst agent first, with its reasons", async () => {
    get.mockResolvedValue(REPORT);
    render(<Analytics />);
    await waitFor(() => expect(screen.getByText(/42 telemetry rows read/)).toBeTruthy());
    expect(get).toHaveBeenCalledWith(analyticsPath(dayBefore(6), dayBefore(0)));
    const [agents, tools] = createWrapper(document.body).findAllTables();
    const first = (row: number) => agents.findBodyCell(row, 1)!.getElement().textContent;
    // Lowest score first; an agent with no score last.
    expect([first(1), first(2), first(3)]).toEqual(["Report (report)", "Intake (intake)", "idle"]);
    expect(agents.findBodyCell(1, 3)!.getElement().textContent)
      .toBe("error rate 10% (2 of 20 calls) (-6); Cedar denied 25% of tool calls (1 of 4) (-5)");
    expect(agents.findBodyCell(1, 2)!.getElement().textContent).toBe("89");
    expect(tools.findBodyCell(1, 3)!.getElement().textContent).toBe("25%");
    expect(tools.findBodyCell(1, 6)!.getElement().textContent).toBe("20%");
    expect(screen.getByText(/Agent Report \(report\): no guardrail/)).toBeTruthy();
    expect(screen.getByText(/Tool pricing: Cedar runs in LOG_ONLY/)).toBeTruthy();
    expect(screen.queryByText(/Only part of the range was read/)).toBeNull();
  });

  it("re-sorts a table when a header is pressed", async () => {
    get.mockResolvedValue(REPORT);
    render(<Analytics />);
    await waitFor(() => expect(screen.getByText(/42 telemetry rows read/)).toBeTruthy());
    const agents = createWrapper(document.body).findAllTables()[0];
    const calls = agents.findColumnSortingArea(4)!;
    calls.click();
    expect(agents.findBodyCell(1, 4)!.getElement().textContent).toBe("0");
    calls.click();
    expect(agents.findBodyCell(1, 4)!.getElement().textContent).toBe("20");
  });

  it("says when it read only part of the range, and counts unknown rows", async () => {
    get.mockResolvedValue({ ...REPORT, capped: true, maxRows: 500, kinds: { llm: 1, other: 2 } });
    render(<Analytics build="pclaims01" />);
    await waitFor(() => expect(screen.getByText(/Only part of the range was read/)).toBeTruthy());
    expect(screen.getByText(/The newest 500 rows were read/)).toBeTruthy();
    expect(screen.getByText(/2 rows are of a kind this console does not know/)).toBeTruthy();
    expect(get).toHaveBeenCalledWith(analyticsPath(dayBefore(6), dayBefore(0), "pclaims01"));
  });

  it("says so when nothing needs a warning", async () => {
    get.mockResolvedValue({ ...REPORT, coverage: { ...COVERAGE, agents: [COVERAGE.agents[0]], tools: [COVERAGE.tools[0]] } });
    render(<Analytics />);
    await waitFor(() => expect(screen.getByText(/Every agent is screened by a guardrail/)).toBeTruthy());
  });

  it("shows the server's refusal", async () => {
    get.mockRejectedValue(new Error("not authorized to perform 'admin'"));
    render(<Analytics />);
    await waitFor(() => expect(screen.getByText("not authorized to perform 'admin'")).toBeTruthy());
  });
});
