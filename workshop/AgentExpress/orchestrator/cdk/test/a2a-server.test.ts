/**
 * The A2A endpoint (lib/a2a-server.ts, terraform/a2a_server.tf, bff/a2a_server.py), as
 * synthesized.
 *
 * What is asserted is what API Gateway will be asked to enforce, because the authorizer
 * itself cannot run here: the A2A routes have their own JWT authorizer whose audience is
 * the A2A client only and they require the A2A scope, every other route keeps the
 * people's authorizer (audience: the sign-in client) and no scope. So a person's token
 * cannot pass the A2A routes and the A2A client's token cannot pass any other route.
 *
 * The baseline is the Lab 5 context (-c idp=cognito -c createCognito=true
 * -c enableGateway=true), synthesized with the feature off and on: off must add nothing,
 * and on must add exactly the A2A resources.
 */
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import * as fs from "fs";
import * as path from "path";
import { A2A_ROUTES, A2A_SCOPE, a2aProblem } from "../lib/a2a-server";
import { OrchestratorStack } from "../lib/orchestrator-stack";

const TF = path.join(__dirname, "..", "..", "terraform");

function synth(overrides: Record<string, any> = {}) {
  const app = new cdk.App();
  const stack = new OrchestratorStack(app, "TestStack", {
    env: { account: "123456789012", region: "us-east-1" },
    agentName: "multiagent_orchestrator",
    modelId: "us.anthropic.claude-haiku-4-5-20251001-v1:0",
    memoryEventExpiryDays: 30,
    idp: "cognito",
    createCognito: true,
    cognitoUserPoolId: "",
    cognitoClientId: "",
    cognitoDomainPrefix: "",
    auth0Domain: "",
    auth0ClientId: "",
    enableGateway: true,
    gatewayClientId: "",
    gatewayClientSecret: "dummy",
    gatewayAudience: "",
    toolApiKeys: {},
    a2aTokens: {},
    transactionSearchIndexingPercentage: 100,
    ...overrides,
  });
  return Template.fromStack(stack);
}

let off: Template;
let on: Template;
beforeAll(() => {
  off = synth();
  on = synth({ exposeA2a: true, a2aApprovers: ["reviewers"] });
});

const routes = (t: Template) => Object.values<any>(t.findResources("AWS::ApiGatewayV2::Route"));
const authorizers = (t: Template) => t.findResources("AWS::ApiGatewayV2::Authorizer");
function bffEnv(t: Template): Record<string, any> {
  const [fn] = Object.values<any>(t.findResources("AWS::Lambda::Function")).filter(
    (f) => String(f.Properties?.FunctionName ?? "").startsWith("AgentCoreBFF-"));
  return fn.Properties.Environment.Variables;
}
function authorizerIdOf(t: Template, name: string): string {
  const [id] = Object.entries<any>(authorizers(t)).find(([, a]) => a.Properties.Name === name) ?? [];
  expect(id).toBeDefined();
  return id!;
}

describe("off (the default)", () => {
  it("has no A2A route, authorizer, client or environment", () => {
    expect(routes(off).map((r) => r.Properties.RouteKey).filter((k) => k.includes("/api/a2a"))).toEqual([]);
    expect(Object.values<any>(authorizers(off)).map((a) => a.Properties.Name)).toEqual(["JwtAuthorizer"]);
    expect(Object.values<any>(off.findResources("AWS::Cognito::UserPoolResourceServer"))
      .map((r) => r.Properties.Identifier)).toEqual(["gateway"]);
    expect(Object.keys(bffEnv(off)).filter((k) => k.startsWith("A2A_"))).toEqual([]);
    expect(Object.keys(off.toJSON().Outputs ?? {}).filter((k) => k.startsWith("a2a"))).toEqual([]);
  });
  it("on adds exactly the A2A resources and changes no other logical id", () => {
    const before = Object.keys(off.toJSON().Resources).sort();
    const after = Object.keys(on.toJSON().Resources).sort();
    expect(after.filter((id) => !before.includes(id)).length).toBeGreaterThan(0);
    expect(before.filter((id) => !after.includes(id))).toEqual([]);
    const added = after.filter((id) => !before.includes(id))
      .map((id) => on.toJSON().Resources[id].Type).sort();
    expect(added).toEqual([
      "AWS::ApiGatewayV2::Authorizer",
      "AWS::ApiGatewayV2::Route",
      "AWS::ApiGatewayV2::Route",
      "AWS::Cognito::UserPoolClient",
      "AWS::Cognito::UserPoolResourceServer",
    ]);
  });
});

describe("on", () => {
  it("adds the two routes, with the A2A authorizer and the scope", () => {
    const a2a = routes(on).filter((r) => String(r.Properties.RouteKey).includes("/api/a2a"));
    expect(a2a.map((r) => r.Properties.RouteKey).sort()).toEqual([...A2A_ROUTES].sort());
    const a2aAuthorizer = authorizerIdOf(on, "a2a-multiagent_orchestrator");
    for (const r of a2a) {
      expect(r.Properties.AuthorizationType).toBe("JWT");
      expect(r.Properties.AuthorizerId).toEqual({ Ref: a2aAuthorizer });
      expect(r.Properties.AuthorizationScopes).toEqual([A2A_SCOPE]);
    }
  });
  it("keeps every other route on the people's authorizer, with no scope", () => {
    const main = authorizerIdOf(on, "JwtAuthorizer");
    const others = routes(on).filter((r) => !String(r.Properties.RouteKey).includes("/api/a2a")
      && r.Properties.AuthorizationType === "JWT");
    expect(others.length).toBeGreaterThan(10);
    for (const r of others) {
      expect(r.Properties.AuthorizerId).toEqual({ Ref: main });
      expect(r.Properties.AuthorizationScopes).toBeUndefined();
    }
  });
  it("gives the two authorizers disjoint audiences: the A2A client, and the sign-in client", () => {
    const auth = authorizers(on);
    const a2aAud = auth[authorizerIdOf(on, "a2a-multiagent_orchestrator")].Properties.JwtConfiguration.Audience;
    const mainAud = auth[authorizerIdOf(on, "JwtAuthorizer")].Properties.JwtConfiguration.Audience;
    const clients = on.findResources("AWS::Cognito::UserPoolClient");
    const idOf = (name: string) => Object.entries<any>(clients).find(([, c]) => c.Properties.ClientName === name)![0];
    expect(a2aAud).toEqual([{ Ref: idOf("multiagent_orchestrator-a2a") }]);
    expect(mainAud).toEqual([{ Ref: idOf("multiagent_orchestrator-spa") }]);
    // Same issuer: the deployment's own pool.
    expect(auth[authorizerIdOf(on, "a2a-multiagent_orchestrator")].Properties.JwtConfiguration.Issuer)
      .toEqual(auth[authorizerIdOf(on, "JwtAuthorizer")].Properties.JwtConfiguration.Issuer);
  });
  it("creates a client-credentials client with the a2a/invoke scope and nothing a person could use", () => {
    const [server] = Object.values<any>(on.findResources("AWS::Cognito::UserPoolResourceServer"))
      .filter((r) => r.Properties.Identifier === "a2a");
    expect(server.Properties.Scopes.map((x: any) => x.ScopeName)).toEqual(["invoke"]);
    const [client] = Object.values<any>(on.findResources("AWS::Cognito::UserPoolClient"))
      .filter((c) => c.Properties.ClientName === "multiagent_orchestrator-a2a");
    expect(client.Properties.GenerateSecret).toBe(true);
    expect(client.Properties.AllowedOAuthFlows).toEqual(["client_credentials"]);
    expect(client.Properties.AllowedOAuthFlowsUserPoolClient).toBe(true);
    expect(client.Properties.SupportedIdentityProviders).toEqual(["COGNITO"]);
    expect(JSON.stringify(client.Properties.AllowedOAuthScopes)).toContain("/invoke");
    expect(client.Properties.CallbackURLs).toBeUndefined();
    // The SPA client cannot ask for the A2A scope.
    const [spa] = Object.values<any>(on.findResources("AWS::Cognito::UserPoolClient"))
      .filter((c) => c.Properties.ClientName === "multiagent_orchestrator-spa");
    expect(JSON.stringify(spa.Properties.AllowedOAuthScopes)).not.toContain("invoke");
  });
  it("tells the BFF who may call, and outputs where to get a token but never the secret", () => {
    const env = bffEnv(on);
    expect(env.A2A_ENABLED).toBe("true");
    expect(env.A2A_SCOPE).toBe(A2A_SCOPE);
    expect(env.A2A_APPROVERS).toBe('["reviewers"]');
    expect(JSON.stringify(env.A2A_CLIENT_IDS)).toContain("A2aClient");
    expect(JSON.stringify(env)).not.toMatch(/ClientSecret/);
    const outputs = on.toJSON().Outputs;
    expect(Object.keys(outputs).filter((k) => k.startsWith("a2a")).sort())
      .toEqual(["a2aClientId", "a2aEndpoint", "a2aTokenUrl"]);
    expect(JSON.stringify(outputs.a2aTokenUrl.Value)).toContain("amazoncognito.com/oauth2/token");
    expect(JSON.stringify(outputs)).not.toMatch(/ClientSecret/);
  });
});

describe("with another identity provider", () => {
  const okta = {
    idp: "okta", createCognito: false, oktaDomain: "acme.okta.com", oktaClientId: "spa-okta",
    enableGateway: false,
  };
  it("refuses to synthesize until a2aIdentity says whose tokens to accept", () => {
    expect(() => synth({ ...okta, exposeA2a: true })).toThrow(/a2aIdentity/);
  });
  it("uses the configured audience, scope and issuer, and creates no Cognito client", () => {
    const t = synth({
      ...okta, exposeA2a: true,
      a2aIdentity: { audience: "api://claims", scope: "a2a.invoke", clientIds: ["0oa1"], issuer: "https://acme.okta.com/oauth2/m2m" },
    });
    const [a] = Object.values<any>(authorizers(t)).filter((x) => x.Properties.Name === "a2a-multiagent_orchestrator");
    expect(a.Properties.JwtConfiguration).toEqual({ Audience: ["api://claims"], Issuer: "https://acme.okta.com/oauth2/m2m" });
    const a2a = routes(t).filter((r) => String(r.Properties.RouteKey).includes("/api/a2a"));
    expect(a2a.map((r) => r.Properties.AuthorizationScopes)).toEqual([["a2a.invoke"], ["a2a.invoke"]]);
    expect(bffEnv(t).A2A_CLIENT_IDS).toBe('["0oa1"]');
    expect(Object.keys(t.findResources("AWS::Cognito::UserPoolClient"))).toEqual([]);
  });
});

describe("refusals, the twins of the Terraform preconditions", () => {
  const base = { authEnabled: true, workflowPlane: true, createCognito: true, jwtAudience: "spa" };
  it("refuses idp=none, a control-plane console, a missing identity and a shared audience", () => {
    expect(a2aProblem(base)).toBeUndefined();
    expect(a2aProblem({ ...base, authEnabled: false })).toMatch(/sign-in provider/);
    expect(a2aProblem({ ...base, workflowPlane: false })).toMatch(/control-plane console/);
    expect(a2aProblem({ ...base, createCognito: false })).toMatch(/a2aIdentity/);
    expect(a2aProblem({ ...base, createCognito: false, identity: { audience: "spa", scope: "s", clientIds: ["c"] } }))
      .toMatch(/must not be the sign-in client/);
    expect(a2aProblem({ ...base, createCognito: false, identity: { audience: "api", scope: "s", clientIds: ["c"] } }))
      .toBeUndefined();
  });
  it("says the same things as Terraform", () => {
    const tf = fs.readFileSync(path.join(TF, "a2a_server.tf"), "utf8");
    for (const phrase of ["sign-in provider", "control-plane console", "must not be the sign-in client"]) {
      expect(tf).toContain(phrase);
    }
  });
});

describe("parity with Terraform", () => {
  it("declares the same routes and scope", () => {
    const tf = fs.readFileSync(path.join(TF, "a2a_server.tf"), "utf8");
    const list = tf.slice(tf.indexOf("a2a_routes = ["), tf.indexOf("]", tf.indexOf("a2a_routes = [")));
    expect([...list.matchAll(/"((?:GET|POST) [^"]+)"/g)].map((m) => m[1])).toEqual(A2A_ROUTES);
    expect(tf).toContain(`"${A2A_SCOPE}"`);
    expect(tf).toMatch(/identifier\s+= "a2a"/);
  });
  it("is off by default on both paths", () => {
    const vars = fs.readFileSync(path.join(TF, "variables.tf"), "utf8");
    expect(vars).toMatch(/variable "expose_a2a" \{[^}]*default\s*=\s*false/s);
    const bin = fs.readFileSync(path.join(__dirname, "..", "bin", "orchestrator.ts"), "utf8");
    expect(bin).toContain('exposeA2a: String(app.node.tryGetContext("exposeA2a")) === "true"');
  });
  it("sets the same BFF environment names", () => {
    const tf = fs.readFileSync(path.join(TF, "a2a_server.tf"), "utf8");
    const tfNames = [...tf.matchAll(/^\s+(A2A_[A-Z_]+)\s*=/gm)].map((m) => m[1]).sort();
    expect(tfNames).toEqual(Object.keys(bffEnv(on)).filter((k) => k.startsWith("A2A_")).sort());
  });
});
