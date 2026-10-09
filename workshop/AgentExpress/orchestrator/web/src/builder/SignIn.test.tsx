/** Identity > Sign-in: pick where people sign in, copy two values from the provider,
 *  paste one address back. What it writes is what the deploy runner reads. */
import { act, render, screen } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { useState } from "react";
import { describe, expect, it } from "vitest";

import type { Project } from "./model";
import { hostOnly, SignInSettings } from "./SignIn";
import { validate } from "./validate";

const base = (actions = true): Project => ({ id: "p1", name: "P", createdAt: "", updatedAt: "", prompts: {},
  workflow: { agents: { intake: { name: "Intake" } }, steps: [{ agent: "intake" }], tools: {},
  authorization: { groupsClaim: "cognito:groups", ...(actions ? { actions: { decision: ["approvers"] } } : {}) } } as never });

let latest: Project;
function Harness({ start = base(), appUrl }: { start?: Project; appUrl?: string }) {
  const [p, setP] = useState(start);
  latest = p;
  return <SignInSettings project={p} setProject={setP} appUrl={appUrl} />;
}
const w = () => createWrapper(document.body);
const auth = () => latest.workflow.authorization as Record<string, unknown>;
const pick = async (label: string) => {
  const tile = w().findTiles()!.findItems().find((t) => t.getElement().textContent?.includes(label))!;
  await act(async () => { tile.findNativeInput().click(); });
};
const field = (label: string) => w().findAllInputs().find((i) => i.findNativeInput().getElement().getAttribute("aria-label") === label)!;
const signInIssues = () => validate(latest.workflow).filter((i) => i.path.startsWith("authorization"));

describe("Identity > Sign-in", () => {
  it("starts on Built in, with nothing to fill in", async () => {
    await act(async () => { render(<Harness />); });
    const tiles = w().findTiles()!.findItems().map((t) => t.getElement().textContent ?? "");
    expect(tiles.map((t) => ["Built in", "Okta", "Microsoft Entra ID", "Auth0"].find((n) => t.startsWith(n))))
      .toEqual(["Built in", "Okta", "Microsoft Entra ID", "Auth0"]);
    expect(auth().signIn).toBeUndefined();
    expect(w().findAllInputs()).toHaveLength(0);
  });

  it("Okta: two values named as Okta names them, the domain cleaned of whatever was pasted round it", async () => {
    await act(async () => { render(<Harness />); });
    await pick("Okta");
    expect(auth().signIn).toEqual({ provider: "okta" });
    expect(auth().groupsClaim).toBe("groups");
    expect(w().findAllInputs().map((i) => i.findNativeInput().getElement().getAttribute("aria-label")))
      .toEqual(["Okta domain", "Client ID"]);
    await act(async () => { field("Okta domain").setInputValue("https://dev-123456.okta.com/oauth2/default"); });
    await act(async () => { field("Client ID").setInputValue("  0oaAbc123  "); });
    expect(auth().signIn).toEqual({ provider: "okta", domain: "dev-123456.okta.com", clientId: "0oaAbc123" });
    expect(signInIssues()).toEqual([]);
    expect(screen.getByText(/Sign-in redirect URI, Sign-out redirect URI and Trusted Origins/)).toBeTruthy();
  });

  it("Entra ID: says plainly when the tenant ID is not one, and validates clean when it is", async () => {
    await act(async () => { render(<Harness />); });
    await pick("Microsoft Entra ID");
    expect(auth().groupsClaim).toBe("roles");
    await act(async () => { field("Directory (tenant) ID").setInputValue("contoso.onmicrosoft.com"); });
    expect(screen.getByText(/A GUID, like/)).toBeTruthy();
    await act(async () => { field("Directory (tenant) ID").setInputValue("72f988bf-86f1-41af-91ab-2d7cd011db47"); });
    await act(async () => { field("Application (client) ID").setInputValue("11111111-2222-3333-4444-555555555555"); });
    expect(screen.queryByText(/A GUID, like/)).toBeNull();
    expect(signInIssues()).toEqual([]);
  });

  it("gives the address to paste back once the app is deployed", async () => {
    await act(async () => { render(<Harness appUrl="https://d123.cloudfront.net" />); });
    await pick("Auth0");
    expect(screen.getByText("https://d123.cloudfront.net")).toBeTruthy();
    expect(screen.getByText(/Allowed Callback URLs, Allowed Logout URLs and Allowed Web Origins/)).toBeTruthy();
  });

  it("before a deploy, says where the address will come from", async () => {
    await act(async () => { render(<Harness />); });
    await pick("Okta");
    expect(screen.getByText(/Deploy\. The app's address appears here/)).toBeTruthy();
  });

  it("Auth0 asks for its roles claim only when the app has roles", async () => {
    const first = await act(async () => render(<Harness start={base(false)} />));
    await pick("Auth0");
    expect(field("Roles claim")).toBeUndefined();
    first.unmount();
    await act(async () => { render(<Harness />); });
    await pick("Auth0");
    await act(async () => { field("Roles claim").setInputValue("https://app/roles"); });
    expect(auth().groupsClaim).toBe("https://app/roles");
  });

  it("back to Built in drops the provider's values and its roles claim", async () => {
    await act(async () => { render(<Harness />); });
    await pick("Okta");
    await act(async () => { field("Okta domain").setInputValue("dev-1.okta.com"); });
    await pick("Built in");
    expect(auth().signIn).toBeUndefined();
    expect(auth().groupsClaim).toBe("cognito:groups");
    expect(signInIssues()).toEqual([]);
  });

  it("keeps the host of whatever is pasted", () => {
    expect(hostOnly(" https://acme.okta.com/ ")).toBe("acme.okta.com");
    expect(hostOnly("acme.us.auth0.com/authorize?x=1")).toBe("acme.us.auth0.com");
  });
});
