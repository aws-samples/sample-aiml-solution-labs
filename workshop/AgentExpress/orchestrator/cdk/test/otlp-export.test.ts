/**
 * The optional OTLP export (lib/otlp-export.ts): off adds nothing; on sets the same three
 * environment variables on every runtime and lets each runtime role read that one secret,
 * as terraform/otlp_export.tf does.
 *
 * Synthesizes the real stack against the shipped workflow.json (which has dedicated agents),
 * once with the settings and once without. No credentials, no Docker (see stack.test.ts).
 */
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import * as fs from "fs";
import * as path from "path";
import { OrchestratorStack } from "../lib/orchestrator-stack";
import { otlpExport } from "../lib/otlp-export";

const ORCH_ROOT = path.join(__dirname, "..", "..");
const ARN = "arn:aws:secretsmanager:eu-west-1:210987654321:secret:otlp-headers-AbC123";
const ENDPOINT = "https://cloud.langfuse.com/api/public/otel/v1/traces";
const ENV_KEYS = ["OTLP_EXPORT_ENDPOINT", "OTLP_EXPORT_HEADERS_SECRET_ARN", "OTLP_EXPORT_INCLUDE_CONTENT"];

function synth(overrides: Record<string, any> = {}): Template {
  const app = new cdk.App();
  const stack = new OrchestratorStack(app, "OtlpStack", {
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

/** Every statement attached to the role named `roleName`. */
function statementsOf(t: Template, roleName: string): any[] {
  const roles = t.findResources("AWS::IAM::Role");
  const id = Object.keys(roles).find((k) => roles[k].Properties.RoleName === roleName);
  expect(id).toBeDefined();
  return Object.values<any>(t.findResources("AWS::IAM::Policy"))
    .filter((p) => (p.Properties.Roles ?? []).some((r: any) => r.Ref === id))
    .flatMap((p) => p.Properties.PolicyDocument.Statement);
}

const otlpStatements = (t: Template) => Object.values<any>(t.findResources("AWS::IAM::Policy"))
  .flatMap((p) => p.Properties.PolicyDocument.Statement)
  .filter((s) => ["ReadOtlpHeadersSecret", "DecryptOtlpHeadersSecret"].includes(s.Sid));

let off: Template;
let on: Template;
beforeAll(() => {
  off = synth();
  on = synth({ otlpEndpoint: ENDPOINT, otlpHeadersSecretArn: ARN });
});

describe("the OTLP export, off (the default)", () => {
  it("sets no OTLP variable on any runtime", () => {
    expect(runtimes(off).length).toBeGreaterThan(1);
    for (const r of runtimes(off)) {
      for (const k of ENV_KEYS) expect(r.Properties.EnvironmentVariables).not.toHaveProperty(k);
    }
  });
  it("grants nothing", () => {
    expect(otlpStatements(off)).toEqual([]);
    expect(JSON.stringify(off.toJSON())).not.toContain("OtlpHeaders");
  });
});

describe("the OTLP export, on", () => {
  it("sets the same three variables on the orchestrator and every dedicated runtime", () => {
    const shipped = JSON.parse(fs.readFileSync(path.join(ORCH_ROOT, "app", "workflow.json"), "utf8"));
    const dedicated = Object.entries<any>(shipped.agents).filter(([, a]) => a.runtime === "dedicated");
    expect(dedicated.length).toBeGreaterThan(0);
    expect(runtimes(on).length).toBe(1 + dedicated.length);
    for (const r of runtimes(on)) {
      expect(r.Properties.EnvironmentVariables).toMatchObject({
        OTLP_EXPORT_ENDPOINT: ENDPOINT,
        OTLP_EXPORT_HEADERS_SECRET_ARN: ARN,
        OTLP_EXPORT_INCLUDE_CONTENT: "false",
      });
    }
  });
  it("leaves every other variable as it was", () => {
    const byName = (t: Template) => Object.fromEntries(runtimes(t).map((r) => [r.Properties.AgentRuntimeName,
      Object.keys(r.Properties.EnvironmentVariables).filter((k) => !ENV_KEYS.includes(k)).sort()]));
    expect(byName(on)).toEqual(byName(off));
  });
  it("lets each runtime role read that one secret, and decrypt it only through Secrets Manager", () => {
    const shipped = JSON.parse(fs.readFileSync(path.join(ORCH_ROOT, "app", "workflow.json"), "utf8"));
    const roles = ["AgentCoreRuntime-multiagent_orchestrator", ...Object.entries<any>(shipped.agents)
      .filter(([, a]) => a.runtime === "dedicated").map(([id]) => `AgentCoreSubagent-multiagent_orchestrator-${id}`)];
    for (const role of roles) {
      const st = statementsOf(on, role);
      const read = st.find((s) => s.Sid === "ReadOtlpHeadersSecret");
      expect(read).toEqual({ Sid: "ReadOtlpHeadersSecret", Effect: "Allow", Action: "secretsmanager:GetSecretValue",
        Resource: ARN });
      const decrypt = st.find((s) => s.Sid === "DecryptOtlpHeadersSecret");
      expect(decrypt).toEqual({
        Sid: "DecryptOtlpHeadersSecret", Effect: "Allow", Action: "kms:Decrypt",
        // The secret's own region and account (it may be in neither of the stack's).
        Resource: "arn:aws:kms:eu-west-1:210987654321:key/*",
        Condition: { StringEquals: {
          "kms:ViaService": "secretsmanager.eu-west-1.amazonaws.com",
          "kms:EncryptionContext:SecretARN": ARN,
        } },
      });
    }
    // On those roles and no other.
    expect(otlpStatements(on).length).toBe(2 * roles.length);
  });
  it("sends content only with otlpIncludeContent", () => {
    expect(otlpExport({ otlpEndpoint: ENDPOINT, otlpHeadersSecretArn: ARN, otlpIncludeContent: true })
      .env.OTLP_EXPORT_INCLUDE_CONTENT).toBe("true");
  });
});

describe("the settings are checked at synth, as Terraform checks them at plan", () => {
  it.each([
    [{ otlpEndpoint: ENDPOINT }, "together, or neither"],
    [{ otlpHeadersSecretArn: ARN }, "together, or neither"],
    [{ otlpEndpoint: "http://cloud.langfuse.com/api/public/otel/v1/traces", otlpHeadersSecretArn: ARN }, "https://"],
    [{ otlpEndpoint: "https://u:p@collector.example.com/v1/traces", otlpHeadersSecretArn: ARN }, "https://"],
    [{ otlpEndpoint: "https://collector.example.com/v1/traces?k=v", otlpHeadersSecretArn: ARN }, "https://"],
    [{ otlpEndpoint: ENDPOINT, otlpHeadersSecretArn: "arn:aws:secretsmanager:eu-west-1:210987654321:secret:otlp" }, "full ARN"],
    [{ otlpEndpoint: ENDPOINT, otlpHeadersSecretArn: "*" }, "full ARN"],
  ])("refuses %j", (settings, message) => {
    expect(() => otlpExport(settings)).toThrow(message);
  });
  it("adds nothing when neither is set", () => {
    expect(otlpExport({})).toEqual({ env: {}, statements: [] });
    expect(otlpExport({ otlpEndpoint: "", otlpHeadersSecretArn: "", otlpIncludeContent: true }))
      .toEqual({ env: {}, statements: [] });
  });
});

describe("parity with Terraform", () => {
  const tf = fs.readFileSync(path.join(ORCH_ROOT, "terraform", "otlp_export.tf"), "utf8");
  const vars = fs.readFileSync(path.join(ORCH_ROOT, "terraform", "variables.tf"), "utf8");
  const bin = fs.readFileSync(path.join(__dirname, "..", "bin", "orchestrator.ts"), "utf8");
  it("sets the same environment variable names", () => {
    const tfNames = [...tf.matchAll(/^\s+(OTLP_EXPORT_[A-Z_]+)\s+=/gm)].map((m) => m[1]).sort();
    expect(tfNames).toEqual([...ENV_KEYS].sort());
  });
  it("grants the same statements", () => {
    for (const sid of ["ReadOtlpHeadersSecret", "DecryptOtlpHeadersSecret"]) expect(tf).toContain(`Sid      = "${sid}"`);
    expect(tf).toContain('"kms:EncryptionContext:SecretARN" = var.otlp_headers_secret_arn');
    expect(tf).toContain('"secretsmanager.${local.otlp_secret_arn_parts[3]}.amazonaws.com"');
  });
  it("is off by default on both paths", () => {
    for (const v of ["otlp_endpoint", "otlp_headers_secret_arn"]) {
      expect(vars.split(`variable "${v}"`)[1].split("\nvariable ")[0]).toMatch(/default\s+= ""\n/);
    }
    expect(vars.split('variable "otlp_include_content"')[1]).toMatch(/default\s+= false\n/);
    expect(bin).toContain('otlpEndpoint: ctx("otlpEndpoint", ""),');
    expect(bin).toContain('otlpHeadersSecretArn: ctx("otlpHeadersSecretArn", ""),');
    expect(bin).toContain('otlpIncludeContent: String(app.node.tryGetContext("otlpIncludeContent")) === "true",');
  });
});
