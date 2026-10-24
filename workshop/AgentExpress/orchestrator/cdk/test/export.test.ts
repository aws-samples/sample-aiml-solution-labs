/**
 * Sending runs to an observability tool (workflow.json orchestrator.export, lib/export.ts):
 * off adds nothing; on, the ORCHESTRATOR runtime alone gets the three variables, its role may
 * read exactly the headers secret, and that secret is created from EXPORT_HEADERS when the
 * workflow names one. Mirrors terraform/export.tf (tests/test_run_export.py pins the twin).
 *
 * Synthesizes the real stack against the shipped workflow.json plus an export block. No
 * credentials, no Docker (see stack.test.ts).
 */
import * as cdk from "aws-cdk-lib";
import { Annotations, Match, Template } from "aws-cdk-lib/assertions";
import * as fs from "fs";
import * as path from "path";
import { OrchestratorStack } from "../lib/orchestrator-stack";
import { exportPlan } from "../lib/export";

const ORCH_ROOT = path.join(__dirname, "..", "..");
const SHIPPED = JSON.parse(fs.readFileSync(path.join(ORCH_ROOT, "app", "workflow.json"), "utf8"));
const ARN = "arn:aws:secretsmanager:eu-west-1:444455556666:secret:langfuse-headers-AbC123";
// A fake header value, joined at run time so secret scanners do not read it as a real one.
const HEADERS = JSON.stringify({ Authorization: "Basic " + "cGstbGY6c2stbGY=" });
const ENV_KEYS = ["OTLP_EXPORT_DESTINATION", "OTLP_EXPORT_HEADERS_SECRET_ARN", "OTLP_EXPORT_INCLUDE_CONTENT"];

function withExport(exp: unknown): any {
  const wf = JSON.parse(JSON.stringify(SHIPPED));
  wf.orchestrator.export = exp;
  return wf;
}

function stackOf(overrides: Record<string, any> = {}): cdk.Stack {
  const app = new cdk.App();
  return new OrchestratorStack(app, "ExportStack", {
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
  } as any);
}

const runtimes = (t: Template) => t.findResources("AWS::BedrockAgentCore::Runtime");
const orchestratorOf = (t: Template) => Object.values<any>(runtimes(t))
  .find((r) => r.Properties.AgentRuntimeName === "multiagent_orchestrator");
const exportStatements = (t: Template) => Object.values<any>(t.findResources("AWS::IAM::Policy"))
  .flatMap((p) => p.Properties.PolicyDocument.Statement.map((s: any) => ({ ...s, roles: p.Properties.Roles })))
  .filter((s) => ["ReadExportHeadersSecret", "DecryptExportHeadersSecret"].includes(s.Sid));
const secrets = (t: Template) => Object.entries<any>(t.findResources("AWS::SecretsManager::Secret"))
  .filter(([, s]) => String(s.Properties.Description ?? "").startsWith("HTTP headers for sending runs"));

let off: Template;
let named: Template;
let byArn: Template;
let both: cdk.Stack;
beforeAll(() => {
  off = Template.fromStack(stackOf());
  named = Template.fromStack(stackOf({
    workflow: withExport({ destination: "langfuse", region: "us", secret: "langfuse", send: { content: true } }),
    exportHeaders: { langfuse: HEADERS },
  }));
  byArn = Template.fromStack(stackOf({ workflow: withExport({ destination: "datadog", secret: ARN }) }));
  both = stackOf({
    workflow: withExport({ destination: "datadog", secret: ARN }),
    otlpEndpoint: "https://collector.example.com/v1/traces", otlpHeadersSecretArn: ARN,
  });
});

describe("orchestrator.export, off (the default)", () => {
  it("sets no export variable on any runtime, creates no secret and grants nothing", () => {
    for (const r of Object.values<any>(runtimes(off))) {
      for (const k of ENV_KEYS) expect(r.Properties.EnvironmentVariables).not.toHaveProperty(k);
    }
    expect(secrets(off)).toEqual([]);
    expect(exportStatements(off)).toEqual([]);
  });
});

describe("orchestrator.export naming a build secret", () => {
  it("creates the headers secret from EXPORT_HEADERS, on the data key", () => {
    const made = secrets(named);
    expect(made.length).toBe(1);
    expect(made[0][1].Properties.SecretString).toBe(HEADERS);
    expect(made[0][1].Properties.KmsKeyId).toBeDefined();
  });
  it("sets the three variables on the orchestrator only", () => {
    const env = orchestratorOf(named).Properties.EnvironmentVariables;
    expect(env.OTLP_EXPORT_DESTINATION).toBe("langfuse");
    expect(env.OTLP_EXPORT_INCLUDE_CONTENT).toBe("true");
    expect(env.OTLP_EXPORT_HEADERS_SECRET_ARN).toEqual({ Ref: made(named) });
    for (const r of Object.values<any>(runtimes(named))) {
      if (r.Properties.AgentRuntimeName === "multiagent_orchestrator") continue;
      for (const k of ENV_KEYS) expect(r.Properties.EnvironmentVariables).not.toHaveProperty(k);
    }
  });
  it("lets the runtime role read that one secret, and nothing more", () => {
    const st = exportStatements(named);
    expect(st.map((s) => s.Sid)).toEqual(["ReadExportHeadersSecret"]);
    expect(st[0].Action).toBe("secretsmanager:GetSecretValue");
    expect(st[0].Resource).toEqual({ Ref: made(named) });
    expect(JSON.stringify(st[0].roles)).toContain("RuntimeRole");
  });
});

function made(t: Template): string {
  return secrets(t)[0][0];
}

describe("orchestrator.export giving a secret ARN", () => {
  it("creates no secret and grants read and a KMS decrypt held to that secret", () => {
    expect(secrets(byArn)).toEqual([]);
    const env = orchestratorOf(byArn).Properties.EnvironmentVariables;
    expect(env).toMatchObject({ OTLP_EXPORT_DESTINATION: "datadog", OTLP_EXPORT_HEADERS_SECRET_ARN: ARN,
      OTLP_EXPORT_INCLUDE_CONTENT: "false" });
    const st = exportStatements(byArn);
    expect(st.map((s) => s.Sid)).toEqual(["ReadExportHeadersSecret", "DecryptExportHeadersSecret"]);
    expect(st[0].Resource).toBe(ARN);
    expect(st[1].Resource).toBe("arn:aws:kms:eu-west-1:444455556666:key/*");
    expect(st[1].Condition).toEqual({ StringEquals: {
      "kms:ViaService": "secretsmanager.eu-west-1.amazonaws.com", "kms:EncryptionContext:SecretARN": ARN } });
  });
});

describe("with the otlp* deploy settings as well", () => {
  it("uses orchestrator.export, leaves the otlp* settings out, and says so", () => {
    const t = Template.fromStack(both);
    for (const r of Object.values<any>(runtimes(t))) expect(r.Properties.EnvironmentVariables).not.toHaveProperty("OTLP_EXPORT_ENDPOINT");
    expect(orchestratorOf(t).Properties.EnvironmentVariables.OTLP_EXPORT_DESTINATION).toBe("datadog");
    const warnings = Annotations.fromStack(both).findWarning("*", Match.stringLikeRegexp("orchestrator.export is set"));
    expect(warnings.length).toBe(1);
  });
});

describe("the settings are checked at synth, as Terraform checks them at plan", () => {
  it.each([
    [{ destination: "splunk", secret: "x" }, {}, "destination must be one of"],
    [{ destination: "otlp" }, {}, "export.secret must name"],
    [{ destination: "otlp", secret: "arn:aws:secretsmanager:eu-west-1:444455556666:secret:short" }, {}, "export.secret must name"],
    [{ destination: "otlp", secret: "missing" }, {}, "no exportHeaders secret by that name"],
    [{ destination: "otlp", secret: "h" }, { h: "not json" }, "must be a JSON object of HTTP headers"],
    [{ destination: "otlp", secret: "h" }, { h: "[1]" }, "must be a JSON object of HTTP headers"],
  ])("refuses %j", (exp, headers, message) => {
    expect(() => exportPlan({ orchestrator: { export: exp } }, headers as Record<string, string>)).toThrow(message);
  });
  it("is nothing when the key is absent", () => {
    expect(exportPlan({ orchestrator: {} })).toBeNull();
    expect(exportPlan({})).toBeNull();
  });
});
