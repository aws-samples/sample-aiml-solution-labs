/** Policies: a build's own Cedar (plain English, the form or Cedar), the generated
 *  permits, the engine's mode, and the library a build copies from. */
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { useState } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const calls: { method: string; path: string; body?: unknown }[] = [];
let library: unknown[] = [];
let polls = 0;
let generated: unknown[] = [];
const GW = 'resource == AgentCore::Gateway::"{{gateway}}"';
const FORBID = `forbid(principal, action == AgentCore::Action::"refunds___issueRefund", ${GW}) when { context.input.amount > 500 };`;
vi.mock("../api", () => {
  const answer = (method: string, path: string, body?: unknown) => {
    calls.push({ method, path, body });
    if (method === "GET" && path === "/api/policies") return Promise.resolve(library);
    if (path === "/api/policies/generate") return Promise.resolve({ generationId: "ax_1-abc", status: "GENERATING" });
    if (path.startsWith("/api/policies/generate/ax_1-abc")) {
      polls += 1;
      if (polls < 2) return Promise.resolve({ status: "GENERATING" });
      return Promise.resolve({ status: "GENERATED", assets: generated });
    }
    if (method === "POST" && path === "/api/policies") return Promise.resolve({ id: "a1b2c3d4", ...(body as object) });
    return Promise.resolve({ ok: true });
  };
  return {
    api: {
      get: (p: string) => answer("GET", p),
      post: (p: string, b?: unknown) => answer("POST", p, b),
      put: (p: string, b?: unknown) => answer("PUT", p, b),
      del: (p: string) => answer("DELETE", p),
    },
  };
});
import { fromGuided, problems } from "./cedar";
import type { Project } from "./model";
import { BuildPolicies, knownTools, PolicyLibrary, policyBlock, withPolicy } from "./Policies";

const TOOLS = {
  refunds: { type: "lambda", description: "Refunds.", lambdaArn: "arn:aws:lambda:us-east-1:123456789012:function:r",
    toolSchema: [{ name: "issueRefund", properties: { amount: { type: "integer" }, orderId: { type: "string" } } }] },
  kb: { type: "kb", corpora: ["policies"], policy: { tool: "retrieve", restrictTo: { filter: ["policies"] } } },
};
const base = (): Project => ({ id: "p1", name: "P", createdAt: "", updatedAt: "", prompts: {},
  workflow: { agents: {}, steps: [], tools: structuredClone(TOOLS), orchestrator: { defaultModel: "m" } } as never });

let latest: Project;
function Harness({ server = true, deployed = true, legacy = false }: { server?: boolean; deployed?: boolean; legacy?: boolean }) {
  const [p, setP] = useState(() => {
    const b = base();
    if (legacy) (b.workflow.orchestrator as Record<string, unknown>).policy = { custom: [{ name: "old", statement: FORBID }] };
    return b;
  });
  latest = p;
  return <BuildPolicies project={p} setProject={setP} issues={[]} server={server} notify={vi.fn()} deployed={deployed} />;
}
/** The open one: every modal is mounted, hidden until visible. */
const modal = (title = "") => createWrapper(document.body).findAllModals()
  .find((m) => m.findHeader().getElement().textContent?.includes(title)
    && !m.getElement().className.includes("hidden") && m.isVisible())!;
/** Cloudscape marks a button with a disabledReason aria-disabled, not disabled. */
const off = (b: HTMLElement) => (b as HTMLButtonElement).disabled || b.getAttribute("aria-disabled") === "true";

beforeEach(() => {
  calls.length = 0; library = []; polls = 0;
  generated = [{ fragment: "Never refund more than 500 dollars.", statement: FORBID, findings: [], problems: [] }];
});

describe("cedar.ts, the form", () => {
  it("writes statements that check out", () => {
    const keys = ["refunds", "kb"];
    const cases = [
      fromGuided({ effect: "forbid", tool: "refunds", toolName: "issueRefund", arg: "amount", op: "gt", values: "500" }),
      fromGuided({ effect: "permit", tool: "kb", toolName: "retrieve", arg: "filter", op: "in", values: "a, b" }),
      fromGuided({ effect: "forbid", tool: "kb", toolName: "retrieve", arg: "filter", op: "notIn", values: "secret" }),
      fromGuided({ effect: "forbid", tool: "refunds", toolName: "", arg: "", op: "present", values: "" }),
    ];
    for (const s of cases) expect(problems(s, keys)).toEqual([]);
    // Live: AgentCore failed a deploy on a condition over a whole target ("attribute
    // `input` in context for AgentCore::Action::"fxConvert" not found").
    const group = fromGuided({ effect: "forbid", tool: "kb", toolName: "", arg: "filter", op: "notIn", values: "x" });
    expect(problems(group, keys)).toEqual([
      'a condition on context.input needs one tool, action == AgentCore::Action::"kb___<toolName>": '
      + 'all of "kb"\'s tools together have no input to read']);
    expect(cases[0]).toContain("context.input has amount && context.input.amount > 500");
    expect(cases[1]).toContain('["a","b"].contains(context.input.filter)');
    expect(cases[2]).toContain("!");
    expect(cases[3]).toContain('action in AgentCore::Action::"refunds"');
    // Parses and deploys, and never matches: AgentCore's context holds `input`.
    const args = 'forbid(principal, action == AgentCore::Action::"kb___retrieve", resource == AgentCore::Gateway::"{{gateway}}") '
      + 'when { context.arguments.query like "*Acme*" };';
    expect(problems(args, keys).some((m) => m.includes("not context.arguments"))).toBe(true);
    // Fails the deploy: "attribute `query` in context ... not found. Did you mean `input`?"
    const bare = 'forbid(principal, action in AgentCore::Action::"kb", resource == AgentCore::Gateway::"{{gateway}}") '
      + 'when { context.query like "*Acme*" };';
    expect(problems(bare, keys).some((m) => m.includes("not context.query"))).toBe(true);
  });
  it("knows the tools a target publishes from its config", () => {
    expect(knownTools(TOOLS.refunds as never)).toEqual([{ name: "issueRefund", args: ["amount", "orderId"] }]);
    expect(knownTools(TOOLS.kb as never)[0].name).toBe("retrieve");
    expect(knownTools({ type: "mcp" } as never)).toEqual([]);
  });
  it("keeps the policy block tidy", () => {
    const wf = withPolicy(base().workflow, { custom: [] });
    expect(policyBlock(wf)).toEqual({});
    expect((wf.orchestrator as Record<string, unknown>).defaultModel).toBe("m");
  });
});

describe("a build's policies", () => {
  it("has AgentCore Policy write one from plain English, polls it, and adds it for review", async () => {
    await act(async () => { render(<Harness />); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Write a policy" })); });
    const m = modal();
    await act(async () => { m.findContent().findTextarea()!.setTextareaValue("never refund more than 500 dollars"); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Write it with AgentCore Policy" })); });
    await waitFor(() => expect(screen.getByText("Checks out.")).toBeTruthy(), { timeout: 8000 });
    expect(calls.find((c) => c.path === "/api/policies/generate")!.body).toEqual({
      build: "p1", text: "never refund more than 500 dollars" });
    expect(polls).toBe(2);
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Add to build" })); });
    // Into the build's policies, written for {{tool}}, and attached to the tool it names.
    expect(latest.workflow.policies).toEqual({ neverRefundMoreThan500: {
      statement: FORBID.replace('"refunds___', '"{{tool}}___'), description: "Never refund more than 500 dollars." } });
    expect((latest.workflow.tools.refunds as Record<string, unknown>).policies).toEqual(["neverRefundMoreThan500"]);
    expect(policyBlock(latest.workflow).custom).toBeUndefined();
    // Nothing went to the library: that was not ticked.
    expect(calls.some((c) => c.method === "POST" && c.path === "/api/policies")).toBe(false);
  }, 15000);
  it("shows every policy AgentCore wrote, with its findings, to pick from", async () => {
    generated = [
      { fragment: "No refunds over 500.", statement: FORBID, findings: [{ type: "VALID", description: "" }], problems: [] },
      { fragment: "Only managers approve.", statement: "", findings: [{ type: "INVALID", description: "Non-translatable" }],
        problems: ["AgentCore could not turn this part into a policy: say it another way"] },
    ];
    await act(async () => { render(<Harness />); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Write a policy" })); });
    await act(async () => { modal().findContent().findTextarea()!.setTextareaValue("two things"); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Write it with AgentCore Policy" })); });
    await waitFor(() => expect(screen.getByText(/INVALID: Non-translatable/)).toBeTruthy(), { timeout: 8000 });
    // Two answers: nothing is picked for the user.
    expect(screen.queryByText("Checks out.")).toBeNull();
    await act(async () => { fireEvent.click(screen.getAllByRole("button", { name: "Use this one" })[0]); });
    expect(screen.getByText("Checks out.")).toBeTruthy();
  }, 15000);
  it("needs the build deployed before plain English, and says so", async () => {
    await act(async () => { render(<Harness deployed={false} />); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Write a policy" })); });
    await act(async () => { modal().findContent().findTabs()!.findTabLinkById("english")!.click(); });
    expect(screen.getByText("Deploy the build first")).toBeTruthy();
    expect(off(screen.getByRole("button", { name: "Write it with AgentCore Policy" }))).toBe(true);
  });
  it("refuses Cedar that would not deploy, and saves to the library when asked", async () => {
    await act(async () => { render(<Harness />); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Write a policy" })); });
    const m = modal();
    await act(async () => { m.findContent().findTabs()!.findTabLinkById("cedar")!.click(); });
    await act(async () => { m.findContent().findInput()!.setInputValue("mine"); });
    const cedarBox = () => m.findContent().findAllTextareas().at(-1)!;
    await act(async () => { cedarBox().setTextareaValue(`permit(principal, action == AgentCore::Action::"refunds___issueRefund", ${GW});`); });
    expect(screen.getByText(/needs a `when` condition/)).toBeTruthy();
    const add = () => screen.getByRole("button", { name: "Add to build" });
    expect(off(add())).toBe(true);
    await act(async () => { cedarBox().setTextareaValue(FORBID); });
    expect(off(add())).toBe(false);
    await act(async () => { m.findFooter()!.findCheckbox()!.findNativeInput().click(); });
    await act(async () => { fireEvent.click(add()); });
    expect(calls.find((c) => c.method === "POST" && c.path === "/api/policies")!.body)
      .toMatchObject({ name: "mine", statement: FORBID, source: "cedar" });
    expect((latest.workflow.policies as Record<string, Record<string, unknown>>).mine.statement).toBe(FORBID.replace('"refunds___', '"{{tool}}___'));
  });
  it("sets the mode and turns a tool's generated permit off", async () => {
    await act(async () => { render(<Harness />); });
    const w = createWrapper(document.body);
    await act(async () => { w.findSegmentedControl()!.findSegments()[1].click(); });
    expect(policyBlock(latest.workflow).mode).toBe("LOG_ONLY");
    await act(async () => { screen.getByRole("checkbox", { name: "Generated permit for refunds" }).click(); });
    expect((latest.workflow.tools.refunds as Record<string, unknown>).policy).toEqual({ permit: false });
    await act(async () => { screen.getByRole("checkbox", { name: "Generated permit for kb" }).click(); });
    expect((latest.workflow.tools.kb as Record<string, unknown>).policy).toEqual({ tool: "retrieve", restrictTo: { filter: ["policies"] }, permit: false });
    await act(async () => { screen.getByRole("checkbox", { name: "Generated permit for refunds" }).click(); });
    expect((latest.workflow.tools.refunds as Record<string, unknown>).policy).toBeUndefined();
  });
  it("keeps build-wide policies written before, and edits them where they are", async () => {
    await act(async () => { render(<Harness legacy />); });
    expect(screen.getByText("Build-wide policies")).toBeTruthy();
    expect(screen.getByRole("button", { name: "Add from library" })).toBeTruthy();
  });
  it("offers plain English and the library only on the console", async () => {
    await act(async () => { render(<Harness server={false} legacy />); });
    expect(screen.queryByRole("button", { name: "Add from library" })).toBeNull();
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Write a policy" })); });
    expect(modal().findContent().findTabs()!.findTabLinkById("english")).toBeNull();
  });
});

describe("the library page", () => {
  it("lists, adds and deletes your policies", async () => {
    library = [{ id: "11111111", name: "noBigRefunds", statement: FORBID, description: "Over 500.", updatedAt: "2026-09-09T10:00:00Z" }];
    const confirm = vi.spyOn(window, "confirm").mockReturnValue(true);
    await act(async () => { render(<PolicyLibrary notify={vi.fn()} />); });
    await waitFor(() => expect(screen.getByText("Over 500.")).toBeTruthy());
    expect(screen.getByText("Blocks")).toBeTruthy();
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Delete noBigRefunds" })); });
    expect(calls.some((c) => c.method === "DELETE" && c.path === "/api/policies/11111111")).toBe(true);
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "New policy" })); });
    // A library policy belongs to no build yet, so no plain English (it needs a build's tools).
    expect(modal().findContent().findTabs()!.findTabLinkById("english")).toBeNull();
    confirm.mockRestore();
  });
});
