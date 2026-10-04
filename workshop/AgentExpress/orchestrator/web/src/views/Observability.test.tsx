/** The island contract, asserted from both sides.
 *
 *  The reported defect: a completed run's "Prompts & I/O" drawer showed the system
 *  prompt, the input, the output and the guardrail checks, and then stopped. No
 *  Evaluation block, no Evaluate button — for any agent, on any run. AgentCore
 *  Evaluations had run, been BILLED, and written `kind="eval"` telemetry the whole time
 *  (six rows on the run that was reported), so this read as "evaluations are broken"
 *  when nothing about evaluations was broken.
 *
 *  The cause was a dependency that went missing without anything failing.
 *  `legacy/observability.js` gates the Evaluation block on `evalAgents`, which it read
 *  as a BARE GLOBAL named `workflow` — published by the pre-React index.html. The
 *  Cloudscape shell holds the workflow in React state and never assigns that global, so
 *  the read resolved to undefined, `evalAgents` defaulted to `[]`, and every agent
 *  looked like one with evaluations switched off. An absent global is not an error in
 *  JavaScript, so the panel rendered happily with a whole feature missing.
 *
 *  Both halves are tested because either alone would have let it through:
 *    - the React half, that the workflow is actually HANDED to mount();
 *    - the legacy half, that nothing in the module still reaches for the global —
 *      passing it is pointless if the consumer keeps reading the ambient name. */

import { act, render } from "@testing-library/react";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it, vi } from "vitest";

import type { Workflow } from "../types";
import { Observability } from "./Observability";

const WORKFLOW: Workflow = {
  agents: { intake: { name: "Request Intake" }, report: { name: "Report" } },
  steps: [{ agent: "intake" }, { agent: "report" }],
  evalAgents: ["intake", "report"],
};

describe("the Observability island", () => {
  it("hands the workflow to the legacy module, not just the host and token", async () => {
    const mount = vi.fn();
    window.ObservabilityIsland = { mount };

    // loadIsland() resolves immediately when the module is already registered, but the
    // mount lands in a promise callback that also calls setReady — so the render has to
    // be awaited inside act() or React warns about the state update.
    await act(async () => { render(<Observability workflow={WORKFLOW} />); });
    expect(mount).toHaveBeenCalled();

    const [el, , wf] = mount.mock.calls[0] as [HTMLElement, string | null, Workflow];
    expect(el).toBeInstanceOf(HTMLElement);
    // The assertion that matters: WITHOUT this argument the module cannot tell which
    // agents have evaluations enabled, and silently renders none of them.
    expect(wf).toBe(WORKFLOW);
    expect(wf.evalAgents).toEqual(["intake", "report"]);
  });
});

describe("legacy/observability.js", () => {
  // Resolved from the vitest root (web/), not from import.meta.url: under the Vite
  // transform import.meta.url is not a file: URL and readFileSync rejects it.
  const src = readFileSync(resolve(process.cwd(), "legacy/observability.js"), "utf8");

  it("reads the workflow it was given, never an ambient global", () => {
    // The exact expression that caused the defect, in both places it appeared.
    expect(src).not.toContain("typeof workflow !== \"undefined\"");
    // `wfDef` is the handed-in copy; the two consumers are agentFlags (evalAgents) and
    // friendlyAgent (agent display names).
    expect(src).toContain("const ev = wfDef.evalAgents || []");
    expect(src).toContain("const meta = (wfDef.agents || {})[id]");
  });

  it("takes the workflow as the third argument to mount", () => {
    expect(src).toContain("function mount(el, token, wf, opts)");
    expect(src).toContain("wfDef = wf || {}");
  });

  it("scopes only the two collection reads to a picked build", () => {
    // Per-run reads find their build on the server by run id; the run list and the cost
    // rollups have no run id, so they must say which build they mean.
    expect(src).toContain('p === "/api/sessions" || p.startsWith("/api/telemetry/aggregate")');
    // Insights too: a build's findings cover only that build's runs.
    expect(src).toContain('p.startsWith("/api/insights")');
    expect(src).toContain('buildId = (opts && opts.build) || ""');
  });
});
