/** The Triggers tab writes orchestrator.triggers the validators accept, and the app's
 *  Triggers page signs a test delivery the way bff/triggers.py checks it. */
import { act, fireEvent, render, screen } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { createHmac } from "node:crypto";
import { execSync } from "node:child_process";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";

vi.mock("../api", () => ({ api: { get: vi.fn(), post: vi.fn(), put: vi.fn(), del: vi.fn() } }));
import { BuildTriggers, triggersOf } from "./Triggers";
import type { Project } from "./model";
import { validate } from "./validate";
import { curlFor, type TriggerRow } from "../views/TriggersPage";

const base = (): Project => ({ id: "p1", name: "P", createdAt: "", updatedAt: "", prompts: {},
  workflow: { agents: { a: { name: "A" } }, steps: [{ agent: "a" }], tools: {} } as never });

let latest: Project;
function Harness() {
  const [p, setP] = useState(base());
  latest = p;
  return <BuildTriggers project={p} setProject={setP} issues={validate(p.workflow)} />;
}
const w = () => createWrapper(document.body);

describe("the Triggers tab", () => {
  it.each(["webhook", "schedule", "eventbridge", "sqs"])("adds a %s trigger that validates", async (type) => {
    await act(async () => { render(<Harness />); });
    const name = w().findAllInputs().find((i) => i.findNativeInput().getElement().getAttribute("aria-label") === "Trigger name")!;
    await act(async () => { name.setInputValue("myTrigger"); });
    const select = w().findAllSelects().find((s) => s.getElement().textContent?.includes("Webhook"))!;
    await act(async () => { select.openDropdown(); });
    const label = { webhook: "Webhook", schedule: "Schedule", eventbridge: "EventBridge event", sqs: "SQS message" }[type]!;
    await act(async () => { select.selectOptionByValue(type); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Add" })); });
    const t = triggersOf(latest.workflow).myTrigger;
    expect(t.type).toBe(type);
    expect(label).toBeTruthy();
    expect(validate(latest.workflow).filter((i) => i.path.startsWith("orchestrator.triggers") && i.severity === "error")).toEqual([]);
  });
  it("turns a trigger off and back on, and removes it", async () => {
    await act(async () => { render(<Harness />); });
    const name = w().findAllInputs().find((i) => i.findNativeInput().getElement().getAttribute("aria-label") === "Trigger name")!;
    await act(async () => { name.setInputValue("hook"); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Add" })); });
    await act(async () => { w().findAllToggles()[0].findNativeInput().click(); });
    expect(triggersOf(latest.workflow).hook.enabled).toBe(false);
    vi.spyOn(window, "confirm").mockReturnValue(true);
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Remove hook" })); });
    expect((latest.workflow.orchestrator as Record<string, unknown> | undefined)?.triggers).toBeUndefined();
  });
});

describe("the app's curl sample", () => {
  it("signs the way bff/triggers.py verifies", () => {
    const t = { name: "hook", type: "webhook", signature: "agentexpress", url: "https://x/api/hooks/hook" } as TriggerRow;
    const curl = curlFor(t, "secret-123");
    expect(curl).toContain("openssl dgst -sha256 -hmac 'secret-123'");
    // Run the signing half of it and compare with node's HMAC of "<ts>.<body>".
    const sig = execSync(`bash -c "TS=100; BODY='{\\"hello\\": \\"world\\"}'; printf '%s.%s' \\"\\$TS\\" \\"\\$BODY\\" | openssl dgst -sha256 -hmac 'secret-123' | sed 's/^.* //'"`).toString().trim();
    expect(sig).toBe(createHmac("sha256", "secret-123").update('100.{"hello": "world"}').digest("hex"));
    expect(curlFor({ ...t, signature: "token" }, "tok")).toContain("X-AX-Token: tok");
  });
});
