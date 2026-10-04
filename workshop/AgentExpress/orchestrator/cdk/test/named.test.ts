/** The build-level named blocks (guardrails, memories, evaluators, identities, policies)
 *  and orchestrator.gatewayIdentity, synthesized. Terraform's side is held to the same
 *  names by tests/test_named_iac.py. */
import * as fs from "fs";
import * as path from "path";
import * as cdk from "aws-cdk-lib";
import { Match, Template } from "aws-cdk-lib/assertions";
import {
  OrchestratorStack, agentClientIdsOf, agentIdentitiesOf, customPoliciesOf, inheritIdentities,
  namedPolicyName, sharedEvaluatorUses,
} from "../lib/orchestrator-stack";

const ORCH = path.resolve(__dirname, "..", "..");
const SAMPLE = JSON.parse(fs.readFileSync(path.join(ORCH, "app", "workflow.json"), "utf8"));
const GW = 'resource == AgentCore::Gateway::"{{gateway}}"';

function named(): any {
  const wf = structuredClone(SAMPLE);
  wf.guardrails = { strict: { contentFilters: { HATE: "HIGH" }, deniedWords: ["secret"] } };
  wf.memories = { shared: { strategies: ["semantic", "userPreference"], expiryDays: 90, scope: "agent" } };
  wf.evaluators = { clarity: { instructions: "Score clarity." } };
  wf.identities = {
    partner: { type: "oauth2", clientId: "abc", scopes: ["r"], tokenUrl: "https://auth.example.com/oauth2/token" },
    keyed: { type: "apikey" },
  };
  wf.policies = {
    noSecrets: { statement: `forbid(principal, action in AgentCore::Action::"{{tool}}", ${GW});` },
    kbOnly: { statement: `forbid(principal, action in AgentCore::Action::"docs", ${GW});` },
  };
  wf.orchestrator.gatewayIdentity = "perAgent";
  wf.tools.docs.policies = ["noSecrets"];
  wf.tools.docs.identity = "partner";
  delete wf.tools.docs.auth;
  const a = wf.agents;
  a.intake.agentcore = { ...(a.intake.agentcore ?? {}), guardrails: { input: true, use: "strict" } };
  a.analysis.agentcore = { ...(a.analysis.agentcore ?? {}), memory: { use: "shared" },
    evaluations: { enabled: true, evaluators: ["Custom.clarity"] } };
  a.report.agentcore = { ...(a.report.agentcore ?? {}), identity: { outbound: ["partner", "keyed"] } };
  return wf;
}

function synth(wf: any, extra: Record<string, any> = {}) {
  const app = new cdk.App();
  const stack = new OrchestratorStack(app, "NamedStack", {
    env: { account: "123456789012", region: "us-east-1" },
    agentName: "ax_12345678", modelId: "us.anthropic.claude-haiku-4-5-20251001-v1:0",
    memoryEventExpiryDays: 30, idp: "cognito", createCognito: true, cognitoUserPoolId: "",
    cognitoClientId: "", cognitoDomainPrefix: "", auth0Domain: "", auth0ClientId: "",
    enableGateway: true, gatewayClientId: "", gatewayClientSecret: "dummy", gatewayAudience: "",
    toolApiKeys: {}, a2aTokens: {}, identitySecrets: { partner: "s3cret", keyed: "k3y" },
    transactionSearchIndexingPercentage: 100, workflow: wf, ...extra,
  } as any);
  return Template.fromStack(stack);
}

describe("the build's named blocks", () => {
  let t: Template;
  beforeAll(() => { t = synth(named()); });

  it("makes a guardrail per name, and lets every runtime apply it", () => {
    t.hasResourceProperties("AWS::Bedrock::Guardrail", { Name: "ax_12345678-gr-strict",
      ContentPolicyConfig: { FiltersConfig: [Match.objectLike({ Type: "HATE", InputStrength: "HIGH" })] } });
    t.resourceCountIs("AWS::Bedrock::Guardrail", 2);
    t.hasResourceProperties("AWS::BedrockAgentCore::Runtime", { EnvironmentVariables: Match.objectLike({ GUARDRAILS: Match.anyValue() }) });
  });

  it("makes a memory per name, with its strategies and expiry", () => {
    t.hasResourceProperties("AWS::BedrockAgentCore::Memory", { Name: "ax_12345678_m_shared", EventExpiryDuration: 90,
      MemoryStrategies: [Match.objectLike({ SemanticMemoryStrategy: Match.anyValue() }),
        Match.objectLike({ UserPreferenceMemoryStrategy: Match.anyValue() })] });
  });

  it("makes one evaluator per shared definition and points each agent that runs it there", () => {
    t.hasResourceProperties("AWS::BedrockAgentCore::Evaluator", { EvaluatorName: Match.stringLikeRegexp("^ax_12345678_[0-9a-f]{6}_clarity$") });
    expect(sharedEvaluatorUses(named())).toEqual({ "analysis.clarity": "clarity" });
  });

  it("creates the providers agents use directly, and one workload identity", () => {
    t.hasResourceProperties("AWS::BedrockAgentCore::OAuth2CredentialProvider", { Name: "bedrock-agentcore-ax-12345678-id-partner" });
    t.hasResourceProperties("AWS::BedrockAgentCore::ApiKeyCredentialProvider", { Name: "bedrock-agentcore-ax-12345678-id-keyed" });
    t.hasResourceProperties("AWS::BedrockAgentCore::WorkloadIdentity", { Name: "ax_12345678-agents" });
    expect(agentIdentitiesOf(named()).sort()).toEqual(["keyed", "partner"]);
  });

  it("gives a tool with an identity that identity's auth and secret", () => {
    const got = inheritIdentities(named().tools, named().identities, { partner: "s3cret" }, {});
    expect(got.tools.docs.auth).toBe("oauth2");
    expect(got.tools.docs.oauth.clientId).toBe("abc");
    expect(got.tools.docs.identity).toBeUndefined();
    expect(got.toolApiKeys.docs).toBe("s3cret");
    t.hasResourceProperties("AWS::BedrockAgentCore::OAuth2CredentialProvider", { Name: "bedrock-agentcore-ax-12345678-docs" });
  });

  it("deploys a {{tool}} policy once per attached tool, and a written one once", () => {
    const names = customPoliciesOf(named()).map((p) => p.name);
    expect(names).toEqual(["kbOnly", namedPolicyName("noSecrets", "docs")]);
    expect(customPoliciesOf(named())[1].statement).toContain('AgentCore::Action::"docs"');
    t.resourceCountIs("AWS::BedrockAgentCore::Policy", Object.keys(named().tools).length + 2);
  });

  it("gives each agent with a tool its own Gateway client, all allowed by the Gateway", () => {
    const ids = agentClientIdsOf(named());
    expect(ids.length).toBeGreaterThan(1);
    t.resourceCountIs("AWS::Cognito::UserPoolClient", 2 + ids.length);   // SPA + shared M2M + one each
    const gw = Object.values<any>(t.findResources("AWS::BedrockAgentCore::Gateway"))[0];
    expect(gw.Properties.AuthorizerConfiguration.CustomJWTAuthorizer.AllowedClients.length).toBe(1 + ids.length);
  });

  it("refuses perAgent without the pool this deployment creates", () => {
    expect(() => synth(named(), { createCognito: false, cognitoUserPoolId: "p", cognitoClientId: "c",
      cognitoDomainPrefix: "d", gatewayClientId: "x", gatewayAudience: "a" })).toThrow(/perAgent/);
  });

  it("refuses an identity an agent uses without its secret", () => {
    expect(() => synth(named(), { identitySecrets: {} })).toThrow(/IDENTITY_SECRETS.partner/);
  });

  it("gives the BFF one API-wide invoke permission, not one per route (Lambda's 20 KB policy cap)", () => {
    const perms = Object.values<any>(t.findResources("AWS::Lambda::Permission"))
      .filter((p) => p.Properties.Principal === "apigateway.amazonaws.com");
    expect(perms.length).toBe(1);
  });

  it("lets the runtime log how each run ended in the activity log", () => {
    const rt = Object.values<any>(t.findResources("AWS::BedrockAgentCore::Runtime"))
      .find((r) => r.Properties.EnvironmentVariables.AUDIT_TABLE);
    expect(rt.Properties.EnvironmentVariables.AUDIT_TABLE).toMatch(/_(audit|builds)$/);
    expect(JSON.stringify(t.findResources("AWS::IAM::Policy"))).toContain("RunOutcomesToActivityLog");
  });
  it("adds nothing for a workflow that uses none of them", () => {
    const plain = synth(structuredClone(SAMPLE));
    plain.resourceCountIs("AWS::Bedrock::Guardrail", 1);
    plain.resourceCountIs("AWS::BedrockAgentCore::WorkloadIdentity", 0);
    plain.resourceCountIs("AWS::BedrockAgentCore::Memory", 2);
  });
  it("makes no build-wide guardrail when its block enforces nothing (Bedrock rejects an empty one)", () => {
    const wf = named();
    delete wf.guardrail;
    const t2 = synth(wf);
    t2.resourceCountIs("AWS::Bedrock::Guardrail", 1); // only the named one
    const rt = Object.values<any>(t2.findResources("AWS::BedrockAgentCore::Runtime"))[0];
    expect(rt.Properties.EnvironmentVariables.GUARDRAIL_ID).toBe("");
    const none = structuredClone(SAMPLE);
    delete none.guardrail;
    const t3 = synth(none);
    t3.resourceCountIs("AWS::Bedrock::Guardrail", 0);
    expect(JSON.stringify(t3.findResources("AWS::IAM::Policy"))).toContain(":guardrail/none");
  });
});
