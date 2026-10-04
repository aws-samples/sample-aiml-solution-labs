/** The Deployment panel: what it says about a build, and what it lets you do. */
import { act, fireEvent, render, screen } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

// The Deploy dialog lists the caller's connected accounts.
beforeEach(() => {
  vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify([
    { accountId: "111122223333", label: "Team sandbox", region: "eu-west-1", status: "connected",
      builds: [{ id: "pother1", name: "Other", region: "eu-west-1" }] },
  ]), { status: 200 })));
});

import { DeployPanel, deployState, stackOf } from "./DeployPanel";
import type { BuildSummary } from "./storage";

const base: BuildSummary = { id: "pabc123", name: "Claims", updatedAt: "2026-09-09T00:00:00Z", agentName: "ax_1a2b3c4d", versions: 0 };
const all = () => true;

describe("deployState", () => {
  it("names the version and the tool at every stage", () => {
    expect(deployState(base)).toEqual({ type: "stopped", text: "Not deployed" });
    expect(deployState({ ...base, job: { action: "deploy", tool: "terraform", version: 3, status: "RUNNING", phase: "deploying" } }).text)
      .toBe("Deploying version 3 with Terraform — creating and updating resources");
    expect(deployState({ ...base, job: { action: "deploy", tool: "cdk", version: 2, status: "FAILED" } }))
      .toEqual({ type: "error", text: "Deploying version 2 with AWS CDK failed" });
    expect(deployState({ ...base, deployed: { version: 2, tool: "cdk", agentName: "ax_1a2b3c4d" } }).text)
      .toBe("Version 2 deployed with AWS CDK");
    expect(deployState({ ...base, job: { action: "destroy", tool: "cdk", version: 2, status: "QUEUED", deleteAfter: true } }).text)
      .toMatch(/^Destroying, then deleting with AWS CDK/);
  });
});

describe("DeployPanel", () => {
  it("offers Run here only where this console runs builds; otherwise Open app", () => {
    const deployed = { ...base, versions: 1, tool: "cdk" as const,
      deployed: { version: 1, tool: "cdk" as const, agentName: "ax_1a2b3c4d", uiUrl: "https://app.example.com" } };
    const { unmount } = render(<DeployPanel build={deployed} errors={0} can={all}
      onDeploy={vi.fn()} onDestroy={vi.fn()} onRun={vi.fn()} />);
    expect(screen.getByRole("button", { name: "Run here" })).toBeTruthy();
    unmount();
    render(<DeployPanel build={deployed} errors={0} can={all} onDeploy={vi.fn()} onDestroy={vi.fn()} />);
    expect(screen.queryByRole("button", { name: "Run here" })).toBeNull();
    expect(screen.getByText("Open app")).toBeTruthy();
    expect(screen.getByText(/Its runs, observability and assistant are in its app/)).toBeTruthy();
  });

  it("names what a build deploys as — a stack with CDK, a Terraform deployment without one", () => {
    expect(stackOf(base, "cdk")).toBe("the stack ax-1a2b3c4d-stack");
    expect(stackOf(base, "terraform")).toBe("the Terraform deployment ax_1a2b3c4d");
    expect(stackOf({ ...base, tool: "terraform" })).toBe("the Terraform deployment ax_1a2b3c4d");
    const { unmount } = render(<DeployPanel build={{ ...base, tool: "terraform" }} errors={0} can={all}
      onDeploy={vi.fn()} onDestroy={vi.fn()} onRun={vi.fn()} />);
    expect(screen.getAllByText("the Terraform deployment ax_1a2b3c4d").length).toBeGreaterThan(0);
    unmount();
  });

  it("offers both tools for a first deploy, and deploys with the one picked", async () => {
    const onDeploy = vi.fn().mockResolvedValue(undefined);
    render(<DeployPanel build={base} errors={0} can={all} onDeploy={onDeploy} onDestroy={vi.fn()} onRun={vi.fn()} />);
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Deploy" })); });
    expect(screen.getByText("Deploy version 1")).toBeTruthy();
    await act(async () => { fireEvent.click(screen.getByLabelText(/Terraform/)); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Deploy with Terraform" })); });
    expect(onDeploy).toHaveBeenCalledWith("terraform", "", "");
  });

  it("locks the tool once the build is deployed with one", async () => {
    const onDeploy = vi.fn().mockResolvedValue(undefined);
    const deployed = { ...base, versions: 1, tool: "terraform" as const,
      deployed: { version: 1, tool: "terraform" as const, agentName: "ax_1a2b3c4d" } };
    render(<DeployPanel build={deployed} errors={0} can={all} onDeploy={onDeploy} onDestroy={vi.fn()} onRun={vi.fn()} />);
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Deploy" })); });
    expect(screen.getAllByText(/destroy it first/).length).toBe(2);   // the tool AND the account
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Deploy with Terraform" })); });
    expect(onDeploy).toHaveBeenCalledWith("terraform", "", "");
    // And a deployed build can be run and destroyed from here.
    expect(screen.getByRole("button", { name: "Run here" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Destroy" })).toBeTruthy();
  });

  it("will not deploy with validation errors, without permission, or while a job runs", () => {
    const { rerender } = render(<DeployPanel build={base} errors={2} can={all} onDeploy={vi.fn()} onDestroy={vi.fn()} onRun={vi.fn()} />);
    const button = () => screen.getByRole("button", { name: "Deploy" }) as HTMLButtonElement;
    expect(button().getAttribute("aria-disabled")).toBe("true");
    rerender(<DeployPanel build={base} errors={0} can={(a) => a !== "deploy"} onDeploy={vi.fn()} onDestroy={vi.fn()} onRun={vi.fn()} />);
    expect(button().getAttribute("aria-disabled")).toBe("true");
    rerender(<DeployPanel build={{ ...base, job: { action: "deploy", tool: "cdk", version: 1, status: "RUNNING" } }}
      errors={0} can={all} onDeploy={vi.fn()} onDestroy={vi.fn()} onRun={vi.fn()} />);
    expect(button().getAttribute("aria-disabled")).toBe("true");
    rerender(<DeployPanel build={base} errors={0} can={all} onDeploy={vi.fn()} onDestroy={vi.fn()} onRun={vi.fn()} />);
    expect(button().getAttribute("aria-disabled")).toBeNull();
  });

  it("shows why a deploy failed, in place", () => {
    render(<DeployPanel build={{ ...base, tool: "cdk", job: { action: "deploy", tool: "cdk", version: 1, status: "FAILED",
      error: "`npx cdk deploy` failed (exit 1):\nResource limit exceeded" } }}
      errors={0} can={all} onDeploy={vi.fn()} onDestroy={vi.fn()} onRun={vi.fn()} />);
    expect(screen.getByText(/Resource limit exceeded/)).toBeTruthy();
  });

  it("deploys into a connected account when one is picked", async () => {
    const onDeploy = vi.fn().mockResolvedValue(undefined);
    render(<DeployPanel build={base} errors={0} can={all} onDeploy={onDeploy} onDestroy={vi.fn()} onRun={vi.fn()} />);
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Deploy" })); });
    const select = createWrapper(document.body).findModal()!.findContent().findSelect()!;
    await act(async () => { select.openDropdown(); });
    // Listed by the name it was given, with its default region and what is deployed there.
    expect(screen.getByText("Team sandbox (111122223333)")).toBeTruthy();
    expect(screen.getByText("Connected · default region eu-west-1 · 1 build deployed")).toBeTruthy();
    await act(async () => { select.selectOptionByValue("111122223333"); });
    // Its default region is filled in.
    expect(createWrapper(document.body).findModal()!.findContent().findAutosuggest()!.findNativeInput().getElement().value)
      .toBe("eu-west-1");
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Deploy with AWS CDK" })); });
    expect(onDeploy).toHaveBeenCalledWith("cdk", "111122223333", "eu-west-1");
  });

  it("opens a deployed build's own app, and runs one in another account only there", () => {
    const deployed = { ...base, versions: 1, tool: "cdk" as const, account: "111122223333",
      deployed: { version: 1, tool: "cdk" as const, agentName: "ax_1a2b3c4d", account: "111122223333",
        region: "eu-west-1", uiUrl: "https://d1.cloudfront.net", appUser: "me@example.com" } };
    render(<DeployPanel build={deployed} errors={0} can={all} onDeploy={vi.fn()} onDestroy={vi.fn()} onRun={vi.fn()} />);
    const open = screen.getByRole("link", { name: /Open app/ }) as HTMLAnchorElement;
    expect(open.getAttribute("href")).toBe("https://d1.cloudfront.net");
    expect(screen.queryByRole("button", { name: "Run here" })).toBeNull();
    expect(screen.getByText("me@example.com")).toBeTruthy();
  });
  it("deploys a connected account's build to the region picked", async () => {
    const onDeploy = vi.fn().mockResolvedValue(undefined);
    render(<DeployPanel build={base} errors={0} can={all} onDeploy={onDeploy} onDestroy={vi.fn()} onRun={vi.fn()} />);
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Deploy" })); });
    const modal = createWrapper(document.body).findModal()!.findContent();
    const account = modal.findSelect()!;
    await act(async () => { account.openDropdown(); });
    await act(async () => { account.selectOptionByValue("111122223333"); });
    const region = modal.findAutosuggest()!;
    await act(async () => { region.setInputValue("ap-southeast-2"); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Deploy with AWS CDK" })); });
    expect(onDeploy).toHaveBeenCalledWith("cdk", "111122223333", "ap-southeast-2");
  });
  it("shows the owner the temporary password for the build's own app when asked", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify(
      { user: "me@example.com", password: "Tmp9Pass8word7X", temporary: true }), { status: 200 })));
    const deployed = { ...base, versions: 1, tool: "cdk" as const,
      deployed: { version: 1, tool: "cdk" as const, agentName: "ax_1a2b3c4d", uiUrl: "https://d1.cloudfront.net",
        appUser: "me@example.com" } };
    render(<DeployPanel build={deployed} errors={0} can={all} onDeploy={vi.fn()} onDestroy={vi.fn()} onRun={vi.fn()} />);
    expect(screen.queryByText("Tmp9Pass8word7X")).toBeNull();
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Show the temporary password" })); });
    expect(screen.getAllByText("Tmp9Pass8word7X").length).toBeGreaterThan(0);
    expect(screen.getByText(/first sign-in asks you to set your own/)).toBeTruthy();
  });
});
