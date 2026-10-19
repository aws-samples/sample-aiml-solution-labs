/**
 * Non-Bedrock models through a LiteLLM proxy (lib/litellm.ts): off adds nothing; on sets the
 * same two environment variables on every runtime and the BFF, lets each of their roles read
 * that one secret, and passes both settings to the Builder's deploy project, as
 * terraform/litellm.tf does.
 *
 * Synthesizes the real stack against the shipped workflow.json (which has dedicated agents),
 * once with the settings and once without. No credentials, no Docker (see stack.test.ts).
 */
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import * as fs from "fs";
import * as path from "path";
import { OrchestratorStack } from "../lib/orchestrator-stack";
import { litellmProxy } from "../lib/litellm";

const ORCH_ROOT = path.join(__dirname, "..", "..");
const ARN = "arn:aws:secretsmanager:eu-west-1:210987654321:secret:litellm-key-AbC123";
const BASE = "https://litellm.example.com";
const ENV_KEYS = ["LITELLM_BASE_URL", "LITELLM_KEY_SECRET_ARN"];
const SIDS = ["ReadLitellmKeySecret", "DecryptLitellmKeySecret"];

function synth(overrides: Record<string, any> = {}): Template {
  const app = new cdk.App();
  const stack = new OrchestratorStack(app, "LitellmStack", {
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

const runtimes = (t: Template) => Object.values<any>(t.findResources("AWS::BedrockAgentCore::Runtime"));
const bffFn = (t: Template) => Object.values<any>(t.findResources("AWS::Lambda::Function"))
  .find((f) => f.Properties.FunctionName === "AgentCoreBFF-multiagent_orchestrator");

/** Every statement attached to the role with logical id `id`. */
function statementsOfId(t: Template, id: string): any[] {
  return Object.values<any>(t.findResources("AWS::IAM::Policy"))
    .filter((p) => (p.Properties.Roles ?? []).some((r: any) => r.Ref === id))
    .flatMap((p) => p.Properties.PolicyDocument.Statement);
}
function statementsOf(t: Template, roleName: string): any[] {
  const roles = t.findResources("AWS::IAM::Role");
  const id = Object.keys(roles).find((k) => roles[k].Properties.RoleName === roleName);
  expect(id).toBeDefined();
  return statementsOfId(t, id!);
}
const litellmStatements = (t: Template) => Object.values<any>(t.findResources("AWS::IAM::Policy"))
  .flatMap((p) => p.Properties.PolicyDocument.Statement)
  .filter((s) => SIDS.includes(s.Sid));
const deployEnv = (t: Template) => Object.values<any>(t.findResources("AWS::CodeBuild::Project"))[0]
  .Properties.Environment.EnvironmentVariables;

let off: Template;
let on: Template;
beforeAll(() => {
  off = synth();
  on = synth({ litellmBaseUrl: BASE, litellmKeySecretArn: ARN });
});

describe("LiteLLM, off (the default)", () => {
  it("sets no LiteLLM variable on any runtime, the BFF or the deploy project", () => {
    expect(runtimes(off).length).toBeGreaterThan(1);
    for (const r of runtimes(off)) {
      for (const k of ENV_KEYS) expect(r.Properties.EnvironmentVariables).not.toHaveProperty(k);
    }
    for (const k of ENV_KEYS) expect(bffFn(off).Properties.Environment.Variables).not.toHaveProperty(k);
    expect(deployEnv(off).map((v: any) => v.Name)).not.toContain("LITELLM_BASE_URL");
  });
  it("grants nothing", () => {
    expect(litellmStatements(off)).toEqual([]);
  });
});

describe("LiteLLM, on", () => {
  const shipped = JSON.parse(fs.readFileSync(path.join(ORCH_ROOT, "app", "workflow.json"), "utf8"));
  const dedicated = Object.entries<any>(shipped.agents).filter(([, a]) => a.runtime === "dedicated");
  it("sets the same two variables on the orchestrator, every dedicated runtime and the BFF", () => {
    expect(dedicated.length).toBeGreaterThan(0);
    expect(runtimes(on).length).toBe(1 + dedicated.length);
    for (const r of runtimes(on)) {
      expect(r.Properties.EnvironmentVariables).toMatchObject({ LITELLM_BASE_URL: BASE, LITELLM_KEY_SECRET_ARN: ARN });
    }
    expect(bffFn(on).Properties.Environment.Variables).toMatchObject({ LITELLM_BASE_URL: BASE, LITELLM_KEY_SECRET_ARN: ARN });
  });
  it("leaves every other variable as it was", () => {
    const byName = (t: Template) => Object.fromEntries(runtimes(t).map((r) => [r.Properties.AgentRuntimeName,
      Object.keys(r.Properties.EnvironmentVariables).filter((k) => !ENV_KEYS.includes(k)).sort()]));
    expect(byName(on)).toEqual(byName(off));
  });
  it("lets each runtime role and the BFF read that one secret, and decrypt it only through Secrets Manager", () => {
    const roles = ["AgentCoreRuntime-multiagent_orchestrator",
      ...dedicated.map(([id]) => `AgentCoreSubagent-multiagent_orchestrator-${id}`)];
    const bffRole = bffFn(on).Properties.Role["Fn::GetAtt"][0];
    const sets = [...roles.map((r) => statementsOf(on, r)), statementsOfId(on, bffRole)];
    for (const st of sets) {
      expect(st.find((s) => s.Sid === "ReadLitellmKeySecret")).toEqual({
        Sid: "ReadLitellmKeySecret", Effect: "Allow", Action: "secretsmanager:GetSecretValue", Resource: ARN });
      expect(st.find((s) => s.Sid === "DecryptLitellmKeySecret")).toEqual({
        Sid: "DecryptLitellmKeySecret", Effect: "Allow", Action: "kms:Decrypt",
        Resource: "arn:aws:kms:eu-west-1:210987654321:key/*",
        Condition: { StringEquals: {
          "kms:ViaService": "secretsmanager.eu-west-1.amazonaws.com",
          "kms:EncryptionContext:SecretARN": ARN,
        } },
      });
    }
    // On those roles and no other.
    expect(litellmStatements(on).length).toBe(2 * sets.length);
  });
  it("passes both settings to the Builder's deploy project, for builds", () => {
    expect(deployEnv(on)).toEqual(expect.arrayContaining([
      { Name: "LITELLM_BASE_URL", Type: "PLAINTEXT", Value: BASE },
      { Name: "LITELLM_KEY_SECRET_ARN", Type: "PLAINTEXT", Value: ARN },
    ]));
  });
});

describe("the settings are checked at synth, as Terraform checks them at plan", () => {
  it.each([
    [{ litellmBaseUrl: BASE }, "together, or neither"],
    [{ litellmKeySecretArn: ARN }, "together, or neither"],
    [{ litellmBaseUrl: "http://litellm.example.com", litellmKeySecretArn: ARN }, "https://"],
    [{ litellmBaseUrl: "https://u:p@litellm.example.com", litellmKeySecretArn: ARN }, "https://"],
    [{ litellmBaseUrl: "https://litellm.example.com/?k=v", litellmKeySecretArn: ARN }, "https://"],
    [{ litellmBaseUrl: BASE, litellmKeySecretArn: "arn:aws:secretsmanager:eu-west-1:210987654321:secret:key" }, "full ARN"],
    [{ litellmBaseUrl: BASE, litellmKeySecretArn: "*" }, "full ARN"],
  ])("refuses %j", (settings, message) => {
    expect(() => litellmProxy(settings)).toThrow(message);
  });
  it("adds nothing when neither is set", () => {
    expect(litellmProxy({})).toEqual({ env: {}, statements: [] });
    expect(litellmProxy({ litellmBaseUrl: "", litellmKeySecretArn: "" })).toEqual({ env: {}, statements: [] });
  });
});

describe("parity with Terraform", () => {
  const tf = fs.readFileSync(path.join(ORCH_ROOT, "terraform", "litellm.tf"), "utf8");
  it("sets the same environment variable names", () => {
    const tfNames = [...tf.matchAll(/^\s+(LITELLM_[A-Z_]+)\s+=/gm)].map((m) => m[1]).sort();
    expect(tfNames).toEqual([...ENV_KEYS].sort());
  });
  it("grants the same statements", () => {
    for (const sid of SIDS) expect(tf).toContain(`Sid      = "${sid}"`);
    expect(tf).toContain('"kms:EncryptionContext:SecretARN" = var.litellm_key_secret_arn');
    expect(tf).toContain('"secretsmanager.${local.litellm_secret_arn_parts[3]}.amazonaws.com"');
  });
});
