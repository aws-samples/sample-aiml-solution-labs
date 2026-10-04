/** The AWS accounts page: your connections, named, with a default region and the builds
 *  deployed in each; edit, check, update the role, disconnect. */
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

const calls: { method: string; path: string; body?: unknown }[] = [];
let list: unknown[] = [];
vi.mock("../api", () => {
  const answer = (method: string, path: string, body?: unknown) => {
    calls.push({ method, path, body });
    if (method === "GET" && path === "/api/accounts") return Promise.resolve(list);
    if (method === "PUT") return Promise.resolve({ accountId: "111122223333", ...(body as object), status: "connected" });
    if (path.endsWith("/launch")) {
      return Promise.resolve({ accountId: "444455556666", region: "us-east-1", status: "pending",
        launchUrl: "https://example.com/launch", templateUrl: "https://example.com/t", cli: "aws cloudformation deploy",
        stackName: "AgentExpressConnect-abcd" });
    }
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
import { Accounts } from "./Accounts";

const CONNECTED = { accountId: "111122223333", label: "Team sandbox", region: "eu-west-1", status: "connected",
  verifiedAt: "2026-09-09T10:00:00Z", stackName: "AgentExpressConnect-1111",
  builds: [{ id: "p1", name: "Claims", region: "us-west-2" }] };
const PENDING = { accountId: "444455556666", region: "us-east-1", status: "pending", builds: [] };

beforeEach(() => { calls.length = 0; list = [CONNECTED, PENDING]; });

async function select(name: string) {
  const table = createWrapper(document.body).findTable()!;
  const row = table.findRows().findIndex((r) => r.getElement().textContent?.includes(name));
  await act(async () => { table.findRowSelectionArea(row + 1)!.click(); });
}
/** Open Actions and return the item `id`, or null if it isn't there in that state. */
async function action(id: string, disabled = false) {
  const menu = createWrapper(document.body).findButtonDropdown()!;
  await act(async () => { menu.openDropdown(); });
  return menu.findItemById(id, { disabled });
}
const modal = (title: string) => createWrapper(document.body).findAllModals()
  .find((m) => m.findHeader().getElement().textContent?.includes(title))!;

describe("AWS accounts", () => {
  it("lists each connection with its name, default region, status and the builds in it", async () => {
    await act(async () => { render(<Accounts canConnect notify={vi.fn()} />); });
    expect(screen.getByText("Team sandbox")).toBeTruthy();
    expect(screen.getByText("Connected")).toBeTruthy();
    expect(screen.getByText("Waiting for its stack")).toBeTruthy();
    expect(screen.getByText("Claims (us-west-2)")).toBeTruthy();
    expect(screen.getByText("(2)")).toBeTruthy();
  });
  it("renames one and changes its default region", async () => {
    const notify = vi.fn();
    await act(async () => { render(<Accounts canConnect notify={notify} />); });
    await select("111122223333");
    const edit = (await action("edit"))!;
    await act(async () => { edit.click(); });
    const m = modal("Edit AWS account");
    await act(async () => { m.findContent().findInput()!.setInputValue("Prod"); });
    await act(async () => { m.findContent().findAutosuggest()!.setInputValue("ap-southeast-2"); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Save" })); });
    expect(calls.find((c) => c.method === "PUT")).toEqual({ method: "PUT", path: "/api/accounts/111122223333",
      body: { label: "Prod", region: "ap-southeast-2" } });
    await waitFor(() => expect(notify).toHaveBeenCalledWith("success", "Saved Prod (111122223333)."));
  });
  it("won't disconnect an account with builds deployed in it, and finishes a pending one", async () => {
    await act(async () => { render(<Accounts canConnect notify={vi.fn()} />); });
    await select("111122223333");
    expect(await action("remove", true)).toBeTruthy();
    await act(async () => { createWrapper(document.body).findButtonDropdown()!.openDropdown(); });  // close it
    await select("444455556666");
    const finish = (await action("finish"))!;
    await act(async () => { finish.click(); });
    await waitFor(() => expect(screen.getByText("Launch stack in AWS")).toBeTruthy());
    expect(calls.some((c) => c.path === "/api/accounts/444455556666/launch")).toBe(true);
  });
  it("connects nothing without the deploy permission", async () => {
    await act(async () => { render(<Accounts canConnect={false} notify={vi.fn()} />); });
    const button = screen.getByRole("button", { name: "Connect an AWS account" }) as HTMLButtonElement;
    expect(button.disabled).toBe(true);
  });
});
