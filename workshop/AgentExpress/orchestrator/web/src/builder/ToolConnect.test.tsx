/** How a tool connects: only the choices its kind allows, written as the keys the IaC
 *  already reads, with the overview on Identity > Tool access agreeing. */
import { act, render, screen } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";

vi.mock("../api", () => ({ api: {
  get: async () => ({ toolApiKeys: ["crm"], a2aTokens: [], identitySecrets: [] }),
  put: async () => ({}), post: async () => ({}), del: async () => ({}),
} }));
import type { Project } from "./model";
import { toolRows } from "./ToolAccess";
import { connectionOf, iamNote, methodsFor, ToolConnect, withMethod } from "./ToolConnect";
import { validate } from "./validate";

const base = (tool: Record<string, unknown>, identities?: Record<string, unknown>): Project => ({
  id: "p1", name: "P", createdAt: "", updatedAt: "", prompts: {},
  workflow: { agents: { intake: { name: "Intake", tool: "crm" } }, steps: [{ agent: "intake" }],
    tools: { crm: { description: "CRM.", ...tool } }, ...(identities ? { identities } : {}) } as never });

let latest: Project;
function Harness({ start, callbackUrl, agentName }: { start: Project; callbackUrl?: string; agentName?: string }) {
  const [p, setP] = useState(start);
  latest = p;
  return <ToolConnect project={p} id="crm" onChange={setP} server callbackUrl={callbackUrl} agentName={agentName} />;
}
const w = () => createWrapper(document.body);
const tiles = () => w().findTiles()?.findItems().map((t) => t.getElement().textContent ?? "") ?? [];
const pick = async (label: string) => {
  const t = w().findTiles()!.findItems().find((i) => i.getElement().textContent?.startsWith(label))!;
  await act(async () => { t.findNativeInput().click(); });
};
const crm = () => latest.workflow.tools.crm as Record<string, unknown>;
const toolIssues = () => validate(latest.workflow).filter((i) => i.path.startsWith("tools.crm"));

describe("How it connects", () => {
  it("offers each kind only what it supports", () => {
    expect(methodsFor("mcp")).toEqual(["none", "aws", "apikey", "app", "user", "obo"]);
    expect(methodsFor("openapi")).toEqual(["none", "aws", "apikey", "app", "user", "obo"]);
    expect(methodsFor("apigateway")).toEqual(["none", "aws", "apikey"]);
    expect(methodsFor("lambda")).toEqual([]);
    expect(methodsFor("websearch")).toEqual([]);
  });

  it("a Lambda tool has nothing to choose, and says which role calls it", async () => {
    await act(async () => { render(<Harness agentName="ax_12345678"
      start={base({ type: "lambda", lambdaArn: "arn:aws:lambda:us-east-1:123456789012:function:f" })} />); });
    expect(w().findTiles()).toBeNull();
    expect(screen.getByText(/IAM role \(SigV4\)\. Nothing to set up/)).toBeTruthy();
    expect(document.body.textContent).toContain("AgentCoreGateway-ax_12345678, which may invoke only this function");
  });

  it("names the Gateway role for each kind, and says so plainly before a name exists", () => {
    expect(iamNote({ type: "apigateway" }, "ax_1")).toBe(
      "Signed by this build's Gateway role, AgentCoreGateway-ax_1, which may call only this API and stage.");
    expect(iamNote({ type: "kb" }, "ax_1")).toContain("may query only this Knowledge Base");
    expect(iamNote({ type: "openapi", service: "bedrock" }, "ax_1")).toBe(
      "Signed by this build's Gateway role, AgentCoreGateway-ax_1, for bedrock.");
    expect(iamNote({ type: "apigateway" })).toBe("Signed by this build's Gateway role, which may call only this API and stage.");
  });

  it("an MCP tool: picks write the keys the deploy reads, and validate clean", async () => {
    await act(async () => { render(<Harness start={base({ type: "mcp", endpoint: "https://mcp.example.com/mcp" })} />); });
    expect(tiles().map((t) => t.split(/(?=[A-Z][a-z]+ )/)[0])).toHaveLength(6);
    await pick("API key");
    expect(crm().auth).toBe("apikey");
    expect(w().findAllInputs().some((i) => i.findNativeInput().getElement().getAttribute("type") === "password")).toBe(true);
    await pick("IAM role");
    expect(crm().auth).toBe("sigv4");
    await pick("Public");
    expect(crm().auth).toBe("none");
    expect(toolIssues()).toEqual([]);
  });

  it("App login: one address field takes a token URL or a discovery document", async () => {
    await act(async () => { render(<Harness start={base({ type: "openapi", schemaS3Uri: "s3://my-bucket/specs/k.json" })} />); });
    await pick("App login");
    const field = (l: string) => w().findAllInputs().find((i) => i.findNativeInput().getElement().getAttribute("aria-label") === l)!;
    await act(async () => { field("Provider address").setInputValue("https://idp.example.com/.well-known/openid-configuration"); });
    await act(async () => { field("Client ID").setInputValue("abc"); });
    await act(async () => { field("Scopes").setInputValue("orders.read, orders.write"); });
    expect(crm().auth).toBe("oauth2");
    expect(crm().oauth).toEqual({ clientId: "abc", scopes: ["orders.read", "orders.write"],
      discoveryUrl: "https://idp.example.com/.well-known/openid-configuration" });
    await act(async () => { field("Provider address").setInputValue("https://idp.example.com/oauth2/token"); });
    expect((crm().oauth as Record<string, unknown>).tokenUrl).toBe("https://idp.example.com/oauth2/token");
    expect((crm().oauth as Record<string, unknown>).discoveryUrl).toBeUndefined();
    expect(toolIssues()).toEqual([]);
  });

  it("a saved login replaces the tool's own, and switching back drops it", async () => {
    const ids = { crmKey: { type: "apikey", description: "CRM key" } };
    await act(async () => { render(<Harness start={base({ type: "mcp", endpoint: "https://mcp.example.com/mcp", auth: "apikey" }, ids)} />); });
    const login = w().findSelect()!;
    await act(async () => { login.openDropdown(); });
    await act(async () => { login.selectOptionByValue("crmKey"); });
    expect(crm().identity).toBe("crmKey");
    expect(crm().auth).toBeUndefined();
    expect(connectionOf(crm() as never, latest)).toEqual({ method: "apikey", login: "crmKey" });
    expect(toolIssues()).toEqual([]);
    await pick("IAM role");
    expect(crm().identity).toBeUndefined();
    expect(crm().auth).toBe("sigv4");
  });

  it("withMethod keeps the rest of the tool untouched", () => {
    const t = { type: "mcp", endpoint: "e", auth: "oauth2", oauth: { clientId: "x" }, service: "s", call: "find" };
    expect(withMethod(t as never, "none")).toEqual({ type: "mcp", endpoint: "e", call: "find", auth: "none" });
  });
});

describe("Acting as the person", () => {
  const field = (l: string) => w().findAllInputs().find((i) => i.findNativeInput().getElement().getAttribute("aria-label") === l);

  it("User login: a token URL also asks where people sign in, and shows the callback URL", async () => {
    await act(async () => { render(<Harness callbackUrl="https://bedrock-agentcore.us-east-1.amazonaws.com/identities/oauth2/callback/abc"
      start={base({ type: "openapi", schemaS3Uri: "s3://my-bucket/specs/k.json" })} />); });
    await pick("User login");
    expect(crm().auth).toBe("user");
    expect(field("Sign-in address")).toBeUndefined();
    await act(async () => { field("Provider address")!.setInputValue("https://idp.example.com/oauth2/token"); });
    await act(async () => { field("Client ID")!.setInputValue("abc"); });
    expect(toolIssues().map((i) => i.path)).toContain("tools.crm.oauth.authorizationUrl");
    await act(async () => { field("Sign-in address")!.setInputValue("https://idp.example.com/oauth2/authorize"); });
    expect(crm().oauth).toEqual({ clientId: "abc", scopes: [], tokenUrl: "https://idp.example.com/oauth2/token",
      authorizationUrl: "https://idp.example.com/oauth2/authorize" });
    expect(toolIssues()).toEqual([]);
    expect(document.body.textContent).toContain("/identities/oauth2/callback/abc");
    // Back to App login: the sign-in address is not used there, so it goes.
    await pick("App login");
    expect((crm().oauth as Record<string, unknown>).authorizationUrl).toBeUndefined();
  });

  it("an MCP tool used as the person must list its tools", async () => {
    await act(async () => { render(<Harness start={base({ type: "mcp", endpoint: "https://mcp.example.com/mcp" })} />); });
    await pick("User login");
    await act(async () => { field("Provider address")!.setInputValue("https://idp.example.com/.well-known/openid-configuration"); });
    await act(async () => { field("Client ID")!.setInputValue("abc"); });
    expect(toolIssues().map((i) => i.path)).toEqual(["tools.crm.toolSchema"]);
    expect(document.body.textContent).toContain("appears here");
  });

  it("On behalf of user: writes obo with the tool's API, and is off with Entra ID sign-in", async () => {
    await act(async () => { render(<Harness start={base({ type: "openapi", schemaS3Uri: "s3://my-bucket/specs/k.json" })} />); });
    await pick("On behalf of user");
    await act(async () => { field("Provider address")!.setInputValue("https://idp.example.com/oauth2/token"); });
    await act(async () => { field("Client ID")!.setInputValue("abc"); });
    await act(async () => { field("Tool's API")!.setInputValue("api://orders"); });
    expect(crm().auth).toBe("obo");
    expect((crm().oauth as Record<string, unknown>).audience).toBe("api://orders");
    expect(toolIssues()).toEqual([]);
  });

  it("with Entra ID sign-in the person's sign-in tile is disabled", async () => {
    const p = base({ type: "openapi", schemaS3Uri: "s3://my-bucket/specs/k.json" });
    (p.workflow as Record<string, unknown>).authorization = { signIn: { provider: "entra", tenantId: "t", clientId: "c" } };
    await act(async () => { render(<Harness start={p} />); });
    const obo = w().findTiles()!.findItems().find((i) => i.getElement().textContent?.startsWith("On behalf of user"))!;
    expect((obo.findNativeInput().getElement() as HTMLInputElement).disabled).toBe(true);
    expect(obo.getElement().textContent).toContain("Not with Entra ID sign-in yet");
  });
});

describe("Identity > Tool access", () => {
  it("person tools say the tool sees the person, and what they still need", () => {
    const p = base({ type: "mcp", endpoint: "https://mcp.example.com/mcp", auth: "user",
      oauth: { clientId: "a", tokenUrl: "https://idp/token" } });
    (p.workflow.tools as Record<string, unknown>).hr = { type: "openapi", description: "HR.", schemaS3Uri: "s3://b/k.json",
      auth: "obo", oauth: { clientId: "a", discoveryUrl: "https://idp/.well-known/openid-configuration" } };
    const rows = toolRows(p, { toolApiKeys: ["crm", "hr"], a2aTokens: [], identitySecrets: [] });
    expect(rows.map((r) => [r.key, r.connects, r.sees, r.why ?? ""])).toEqual([
      ["crm", "User login", "The person", "Add the sign-in address"],
      ["hr", "On behalf of user", "The person", ""],
    ]);
  });

  it("says what each tool connects with, and what is still missing", () => {
    const p = base({ type: "mcp", endpoint: "https://mcp.example.com/mcp", auth: "apikey" });
    (p.workflow.tools as Record<string, unknown>).docs = { type: "lambda", description: "Docs.", lambdaArn: "arn:aws:lambda:us-east-1:1:function:f" };
    (p.workflow.tools as Record<string, unknown>).cal = { type: "mcp", description: "Cal.", endpoint: "https://c", auth: "oauth2", oauth: { clientId: "" } };
    const rows = toolRows(p, { toolApiKeys: [], a2aTokens: [], identitySecrets: [] });
    expect(rows.map((r) => [r.key, r.connects, r.status, r.why ?? ""])).toEqual([
      ["crm", "API key", "setup", "Add the API key"],
      ["docs", "IAM role", "ready", ""],
      ["cal", "App login", "setup", "Add the provider address and client ID"],
    ]);
    expect(toolRows(p, { toolApiKeys: ["crm"], a2aTokens: [], identitySecrets: [] })[0].status).toBe("ready");
  });
});
