/** A Lambda tool written in the build: its files travel with it, its grants are a fixed
 *  menu, and it is checked and run in a sandbox before it deploys. */
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { useState } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const calls: { method: string; path: string; body?: unknown }[] = [];
vi.mock("../api", () => {
  const answer = (method: string, path: string, body?: unknown) => {
    calls.push({ method, path, body });
    if (path === "/api/code/check") {
      return Promise.resolve({ ok: true, problems: [{ severity: "warning", file: "handler.py", line: 1, message: "`json` is imported and never used" }],
        sandbox: { ran: true, ms: 2100, note: "Run in an isolated sandbox.", results: [
          { name: "small refund", ok: true, output: "{\"refunded\": 20}", ms: 1 },
          { name: "big refund", ok: false, error: "ValueError: too big", trace: "Traceback…", ms: 0 }] } });
    }
    if (path.endsWith("/test-tool")) return Promise.resolve({ ok: true, status: 200, error: "", output: "{\"ok\": 1}", log: "START", ms: 40 });
    return Promise.resolve({ ok: true });
  };
  return { api: {
    get: (p: string) => answer("GET", p), post: (p: string, b?: unknown) => answer("POST", p, b),
    put: (p: string, b?: unknown) => answer("PUT", p, b), del: (p: string) => answer("DELETE", p),
  } };
});
import { CodeTool, starterFiles } from "./CodeTool";
import { codeTools, removeTool, renameTool, toBundle, type Project } from "./model";
import { validate } from "./validate";

const SCHEMA = [{ name: "issueRefund", properties: { amount: { type: "integer", required: true } } },
  { name: "lookupOrder", properties: { orderId: { type: "string", required: true } } }];
const base = (): Project => ({ id: "p1", name: "P", createdAt: "", updatedAt: "", prompts: {},
  workflow: { agents: {}, steps: [], tools: { refunds: { type: "lambda", description: "Refunds.", lambdaArn: "", toolSchema: SCHEMA, call: "issueRefund", arg: "amount" } } } as never });

let latest: Project;
function Harness({ start = base() }: { start?: Project }) {
  const [p, setP] = useState(start);
  latest = p;
  return <CodeTool project={p} id="refunds" onChange={setP} server />;
}
beforeEach(() => { calls.length = 0; });

describe("a code tool", () => {
  it("starts from a handler for each of its tools, and test events", () => {
    const f = starterFiles("refunds", base().workflow.tools.refunds);
    expect(f["handler.py"]).toContain("def lambda_handler(event, context):");
    expect(f["handler.py"]).toContain('if tool == "issueRefund":');
    expect(f["handler.py"]).toContain('if tool == "lookupOrder":');
    expect(JSON.parse(f["events.json"])[0].tool).toBe("issueRefund");
  });
  it("switches a Lambda tool to one written here, with no grants and its files", async () => {
    await act(async () => { render(<Harness />); });
    await act(async () => { createWrapper(document.body).findSegmentedControl()!.findSegments()[1].click(); });
    const t = latest.workflow.tools.refunds as Record<string, unknown>;
    expect(t.code).toEqual({});
    expect(t.lambdaArn).toBeUndefined();
    expect(Object.keys(latest.toolCode!.refunds).sort()).toEqual(["events.json", "handler.py", "requirements.txt"]);
    expect(codeTools(latest.workflow)).toEqual(["refunds"]);
    expect(validate(latest.workflow).filter((i) => i.path.startsWith("tools.refunds") && i.severity === "error")).toEqual([]);
  });
  it("grants only what is picked, and edits the files", async () => {
    const p = base();
    p.workflow.tools.refunds = { ...p.workflow.tools.refunds, code: {} } as never;
    delete (p.workflow.tools.refunds as Record<string, unknown>).lambdaArn;
    p.toolCode = { refunds: starterFiles("refunds", p.workflow.tools.refunds) };
    await act(async () => { render(<Harness start={p} />); });
    const w = createWrapper(document.body);
    const input = (ph: string) => w.findAllInputs().find((i) => i.findNativeInput().getElement().placeholder === ph)!;
    await act(async () => { input("orders").setInputValue("orders"); });
    await act(async () => { input("payments/api-key").setInputValue("payments/key"); });
    expect((latest.workflow.tools.refunds as Record<string, unknown>).code).toEqual({ grants: { table: "orders", secret: "payments/key" } });
    await act(async () => { input("orders").setInputValue(""); });
    expect((latest.workflow.tools.refunds as Record<string, unknown>).code).toEqual({ grants: { secret: "payments/key" } });
    const handler = w.findAllTextareas().find((a) => a.findNativeTextarea().getElement().getAttribute("aria-label") === "handler.py")!;
    await act(async () => { handler.setTextareaValue("def lambda_handler(event, context):\n    return 1\n"); });
    expect(latest.toolCode!.refunds["handler.py"]).toContain("return 1");
    await act(async () => { input("helpers.py").setInputValue("helpers.py"); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Add a file" })); });
    expect(latest.toolCode!.refunds["helpers.py"]).toBe("");
  });
  it("checks it, and shows the sandbox's run of each event", async () => {
    const p = base();
    p.workflow.tools.refunds = { type: "lambda", description: "Refunds.", toolSchema: SCHEMA, code: {} } as never;
    p.toolCode = { refunds: starterFiles("refunds", p.workflow.tools.refunds) };
    await act(async () => { render(<Harness start={p} />); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Check and run in the sandbox" })); });
    await waitFor(() => expect(screen.getByText(/small refund · 1 ms/)).toBeTruthy());
    expect(screen.getByText(/imported and never used/)).toBeTruthy();
    expect(screen.getByText(/ValueError: too big/)).toBeTruthy();
    expect(calls[0].body).toMatchObject({ key: "refunds", files: { "handler.py": expect.any(String) }, tool: { code: {} } });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Test tool" })); });
    await waitFor(() => expect(screen.getByText(/Returned · 40 ms/)).toBeTruthy());
    expect(calls.at(-1)).toMatchObject({ path: "/api/builds/p1/test-tool", body: { key: "refunds", tool: "issueRefund", event: {} } });
  });
});

describe("the code in the model", () => {
  it("travels in the bundle, and follows a rename and a delete", () => {
    const p = base();
    p.workflow.tools.refunds = { type: "lambda", description: "x", toolSchema: SCHEMA, code: {} } as never;
    p.workflow.tools.other = { type: "lambda", description: "x", toolSchema: SCHEMA, lambdaArn: "arn:aws:lambda:us-east-1:123456789012:function:x" } as never;
    p.toolCode = { refunds: { "handler.py": "x" }, other: { "handler.py": "stale" } };
    expect(toBundle(p).toolCode).toEqual({ refunds: { "handler.py": "x" } });
    const renamed = renameTool(p, "refunds", "payments");
    expect(renamed.toolCode!.payments).toEqual({ "handler.py": "x" });
    expect(renamed.toolCode!.refunds).toBeUndefined();
    expect(removeTool(renamed, "payments").toolCode!.payments).toBeUndefined();
  });
});
