/** The Tests tab: write a case with forms, run it, and read what failed.
 *
 *  What a person needs from it: to author without typing JSON, to be told plainly when running
 *  is not possible (and still be able to write), and when a run ends, to see which case failed,
 *  why in the server's words, and a way into the run. */
import { act, fireEvent, render, screen } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { useState } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("./suitesApi", () => ({ suitesApi: { trends: vi.fn(), start: vi.fn(), result: vi.fn() } }));
import { BuildSuites, POLL_MS } from "./Suites";
import { suitesApi } from "./suitesApi";
import type { Project } from "./model";
import type { Suite, SuiteRunResult } from "./suiteSpec";

const api = vi.mocked(suitesApi);
const base = (suites?: Suite[]): Project => ({
  id: "p1", name: "P", createdAt: "", updatedAt: "", prompts: {}, ...(suites ? { suites } : {}),
  workflow: { agents: { triage: { name: "Triage" }, summary: { name: "Summary" } }, steps: [{ agent: "triage" }, { agent: "summary" }], tools: {} } as never,
});
const SAVED: Suite[] = [{ name: "Triage", cases: [
  { name: "urgent", request: "Claim 1", expect: [{ agent: "triage", field: "riskScore", gte: 80 }] },
  { name: "routine", request: "Claim 2", expect: [] },
] }];
let latest: Project;
const flush = vi.fn(async () => {});
const notify = vi.fn();

function Harness({ start, ...props }: { start?: Project } & Partial<React.ComponentProps<typeof BuildSuites>>) {
  const [p, setP] = useState(start ?? base());
  latest = p;
  return <BuildSuites project={p} setProject={setP} server buildId="p1" deployed flush={flush} notify={notify} {...props} />;
}
const w = () => createWrapper(document.body);
const input = (label: string) => w().findAllInputs().find((i) => i.findNativeInput().getElement().getAttribute("aria-label") === label)!;
const area = (label: string) => w().findAllTextareas().find((i) => i.findNativeTextarea().getElement().getAttribute("aria-label") === label)!;
/** A Select's aria-label is the text of a hidden element its trigger points to. */
const labelOf = (s: ReturnType<ReturnType<typeof w>["findAllSelects"]>[number]) => (s.findTrigger().getElement().getAttribute("aria-labelledby") ?? "")
  .split(" ").map((id) => document.getElementById(id)?.textContent ?? "").join(" ");
const select = (label: string) => w().findAllSelects().find((s) => labelOf(s).startsWith(label))!;
const click = async (name: string | RegExp) => { await act(async () => { fireEvent.click(screen.getByRole("button", { name })); }); };

function result(over: Partial<SuiteRunResult> = {}): SuiteRunResult {
  return {
    suiteRunId: "r1", suite: "Triage", buildVersion: 2, startedAt: "2026-09-10T10:00:00Z", state: "finished",
    summary: { passed: 1, failed: 1, running: 0, notStarted: 0, total: 2 },
    cases: [
      { case: "urgent", sessionId: "s1", status: "passed", message: "All 2 checks passed.", checks: { passed: 2, total: 2 }, failed: [] },
      { case: "routine", sessionId: "s2", status: "failed", checks: { passed: 1, total: 2 },
        message: "1 of 2 checks failed: Agent triage, field riskScore was 55, expected lt 30.",
        failed: [{ expected: "agent triage riskScore lt 30", actual: "55", message: "Agent triage, field riskScore was 55, expected lt 30." }] },
    ],
    trend: [{ suiteRunId: "r0", startedAt: "", buildVersion: 1, finished: true, passed: 2, failed: 0, total: 2, notStarted: 0 },
      { suiteRunId: "r1", startedAt: "", buildVersion: 2, finished: true, passed: 1, failed: 1, total: 2, notStarted: 0 }],
    ...over,
  };
}

beforeEach(() => {
  api.trends.mockResolvedValue({});
  api.start.mockResolvedValue({ suiteRunId: "r1", suite: "Triage", buildVersion: 2, startedAt: "", started: 2, notStarted: 0 });
  api.result.mockResolvedValue(result());
});
afterEach(() => { vi.clearAllMocks(); vi.useRealTimers(); });

describe("writing a suite", () => {
  it("builds a suite, a case and a check with forms, and the project holds exactly what the server accepts", async () => {
    await act(async () => { render(<Harness />); });
    await click("New suite");
    await click("Add a case");
    await act(async () => { area("Case 1 request").setTextareaValue("Claim 88213: water damage"); });
    await click("Add check");
    await act(async () => { select("Check 1 agent").openDropdown(); });
    await act(async () => { select("Check 1 agent").selectOptionByValue("triage"); });
    await act(async () => { input("Check 1 field").setInputValue("riskScore"); });
    await act(async () => { select("Check 1 comparison").openDropdown(); });
    await act(async () => { select("Check 1 comparison").selectOptionByValue("gte"); });
    await act(async () => { input("Check 1 value").setInputValue("80"); });
    expect(latest.suites).toEqual([{
      name: "New suite",
      cases: [{ name: "New case", request: "Claim 88213: water damage",
        expect: [{ agent: "triage", field: "riskScore", gte: 80 }] }],
    }]);
  });

  it("adds a cost limit and a run outcome as numbers and statuses, not text", async () => {
    await act(async () => { render(<Harness start={base([{ name: "S", cases: [{ name: "c", request: "r", expect: [] }] }])} />); });
    await act(async () => { select("Case 1 kind").openDropdown(); });
    await act(async () => { select("Case 1 kind").selectOptionByValue("cost"); });
    await click("Add check");
    await act(async () => { input("Check 1 limit").setInputValue("0.25"); });
    await act(async () => { select("Case 1 kind").openDropdown(); });
    await act(async () => { select("Case 1 kind").selectOptionByValue("run"); });
    await click("Add check");
    expect(latest.suites![0].cases[0].expect).toEqual([{ cost: { lt: 0.25 } }, { run: "done" }]);
  });

  it("says so, and still lets you write, when the build is not deployed", async () => {
    await act(async () => { render(<Harness deployed={false} start={base(SAVED)} />); });
    expect(screen.getByTestId("tests-cannot-run").textContent).toContain("Deploy the build to run its tests");
    expect((screen.getByRole("button", { name: "Run Triage" }) as HTMLButtonElement).disabled).toBe(true);
    await click("New suite");
    expect(latest.suites).toHaveLength(2);
    expect(api.start).not.toHaveBeenCalled();
  });

  it("says why running is not possible here, when something other than deploying is the reason", async () => {
    await act(async () => { render(<Harness runBlock="You are not allowed to start runs, which is what a test run does." start={base(SAVED)} />); });
    expect(screen.getByTestId("tests-cannot-run").textContent).toContain("not allowed to start runs");
    expect((screen.getByRole("button", { name: "Run Triage" }) as HTMLButtonElement).disabled).toBe(true);
    await act(async () => { render(<Harness server={false} start={base(SAVED)} />); });
    expect(screen.getAllByTestId("tests-cannot-run").pop()!.textContent).toContain("kept in your browser");
  });

  it("removes a suite after asking", async () => {
    vi.spyOn(window, "confirm").mockReturnValue(true);
    await act(async () => { render(<Harness start={base(SAVED)} />); });
    await click("Remove suite Triage");
    expect(latest.suites).toEqual([]);
  });
});

describe("running a suite", () => {
  it("saves what is on screen first, runs it, and shows each case with what failed and a way into the run", async () => {
    const order: string[] = [];
    flush.mockImplementationOnce(async () => { order.push("flush"); });
    api.start.mockImplementationOnce(async () => { order.push("start"); return { suiteRunId: "r1", suite: "Triage", buildVersion: 2, startedAt: "", started: 2, notStarted: 0 }; });
    await act(async () => { render(<Harness start={base(SAVED)} />); });
    await click("Run Triage");
    expect(order).toEqual(["flush", "start"]);
    expect(api.start).toHaveBeenCalledWith("p1", "Triage");
    expect(api.result).toHaveBeenCalledWith("p1", "r1");
    expect(screen.getByTestId("suite-summary").textContent).toContain("1 of 2 passed");
    expect(screen.getByTestId("suite-summary").textContent).toContain("build version 2");
    // The failure, in the server's words, beside the case that failed.
    expect(screen.getAllByText("Agent triage, field riskScore was 55, expected lt 30.").length).toBeGreaterThan(0);
    const links = screen.getAllByRole("link", { name: "Open run" }) as HTMLAnchorElement[];
    expect(links.map((a) => a.getAttribute("href"))).toEqual(["#run:s1", "#run:s2"]);
    expect(screen.getAllByText("Passed").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Failed").length).toBeGreaterThan(0);
    // The trend of the suite, by build version.
    expect(screen.getByTestId("suite-trend").textContent).toContain("v1: 2/2 · v2: 1/2");
  });

  it("follows a run that is still going until it ends, then stops asking", async () => {
    vi.useFakeTimers();
    api.result
      .mockResolvedValueOnce(result({ state: "running", summary: { passed: 0, failed: 0, running: 2, notStarted: 0, total: 2 },
        cases: result().cases.map((c) => ({ ...c, status: "running" as const, message: "The run is running.", failed: [] })) }))
      .mockResolvedValue(result());
    await act(async () => { render(<Harness start={base(SAVED)} />); });
    await click("Run Triage");
    expect(screen.getByTestId("suite-summary").textContent).toContain("Running: 0 of 2 cases done");
    expect((screen.getByRole("button", { name: "Run Triage" }) as HTMLButtonElement).disabled).toBe(true);
    await act(async () => { await vi.advanceTimersByTimeAsync(POLL_MS); });
    expect(screen.getByTestId("suite-summary").textContent).toContain("1 of 2 passed");
    const calls = api.result.mock.calls.length;
    await act(async () => { await vi.advanceTimersByTimeAsync(POLL_MS * 3); });
    expect(api.result.mock.calls.length).toBe(calls);
    expect((screen.getByRole("button", { name: "Run Triage" }) as HTMLButtonElement).disabled).toBe(false);
  });

  it("shows a case the limits refused as not started, and does not call it a failure", async () => {
    api.result.mockResolvedValue(result({
      summary: { passed: 1, failed: 0, running: 0, notStarted: 1, total: 1 },
      cases: [result().cases[0], { case: "routine", sessionId: null, status: "not_started", checks: { passed: 0, total: 0 }, failed: [],
        message: "Not started: You have started 1 runs this hour, which is the limit." }],
    }));
    await act(async () => { render(<Harness start={base(SAVED)} />); });
    await click("Run Triage");
    expect(screen.getByTestId("suite-summary").textContent).toContain("1 of 1 passed · 1 not started");
    expect(screen.getByText("Not started: You have started 1 runs this hour, which is the limit.")).toBeTruthy();
    expect(screen.getAllByText("Not started").length).toBeGreaterThan(0);
  });

  it("tells you when the run could not be started", async () => {
    api.start.mockRejectedValueOnce(new Error("this build is not deployed"));
    await act(async () => { render(<Harness start={base(SAVED)} />); });
    await click("Run Triage");
    expect(notify).toHaveBeenCalledWith("error", "Could not run Triage: this build is not deployed");
    expect(screen.queryByTestId("suite-summary")).toBeNull();
  });

  it("loads the history of each suite when it opens", async () => {
    api.trends.mockResolvedValue({ Triage: result().trend });
    await act(async () => { render(<Harness start={base(SAVED)} />); });
    expect(api.trends).toHaveBeenCalledWith("p1");
    expect(screen.getByTestId("suite-trend").textContent).toContain("v1: 2/2 · v2: 1/2");
  });

  it("does not ask the server for history when the build is kept in the browser", async () => {
    await act(async () => { render(<Harness server={false} start={base(SAVED)} />); });
    expect(api.trends).not.toHaveBeenCalled();
  });
});
