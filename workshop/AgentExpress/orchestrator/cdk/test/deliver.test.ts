/**
 * Deliver steps (v139 Track C deliver; task.kind "deliver", lib/deliver.ts): the runtime role
 * reads only the run's saved files, puts only under each s3 target's prefix and sends only
 * from each email target's SES identity (From address pinned); a webhook's signing secret
 * reaches the runtime secret only when a target names one; a workflow without a deliver step
 * synthesizes byte-identically to one built without this code. Same as terraform/deliver.tf.
 */
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import * as fs from "fs";
import * as path from "path";
import * as deliver from "../lib/deliver";
import { OrchestratorStack } from "../lib/orchestrator-stack";

const ORCH_ROOT = path.join(__dirname, "..", "..");
const shipped = require(`${ORCH_ROOT}/app/workflow.json`);
function synth(workflow: any, deliverSecrets?: Record<string, string>): Template {
  const app = new cdk.App();
  const stack = new OrchestratorStack(app, "TestStack", {
    env: { account: "123456789012", region: "us-east-1" },
    agentName: "multiagent_orchestrator",
    modelId: "us.anthropic.claude-haiku-4-5-20251001-v1:0",
    memoryEventExpiryDays: 30,
    idp: "cognito", createCognito: true, cognitoUserPoolId: "", cognitoClientId: "",
    cognitoDomainPrefix: "", auth0Domain: "", auth0ClientId: "",
    enableGateway: true, gatewayClientId: "", gatewayClientSecret: "dummy", gatewayAudience: "",
    toolApiKeys: {}, a2aTokens: {}, transactionSearchIndexingPercentage: 100,
    ...(deliverSecrets ? { deliverSecrets } : {}),
    workflow,
  } as any);
  return Template.fromStack(stack);
}
const IDENTITY = "arn:aws:ses:us-east-1:123456789012:identity/example.com";
const TARGETS = [
  { type: "s3", uri: "s3://amzn-s3-demo-bucket/reports/", files: ["*.pdf"] },
  { type: "s3", uri: "s3://amzn-s3-demo-bucket/" },
  { type: "email", identityArn: IDENTITY, fromAddress: "reports@example.com", to: ["team@example.com"] },
  { type: "webhook", url: "https://hooks.example.com/in", secret: "hook" },
];
const withDeliver = (targets: any[] = TARGETS, files = true) => ({
  ...shipped,
  orchestrator: { ...shipped.orchestrator, ...(files ? { files: {} } : {}) },
  agents: { ...shipped.agents, send: { name: "Send", runtime: "task", task: { kind: "deliver", targets } } },
  steps: [...shipped.steps, { agent: "send" }],
});
const runtimeStatements = (t: Template) => Object.entries<any>(t.findResources("AWS::IAM::Policy"))
  .filter(([k]) => k.startsWith("RuntimeRole"))
  .flatMap(([, p]) => p.Properties.PolicyDocument.Statement as any[]);
const runtimeSecret = (t: Template) => JSON.stringify(Object.entries<any>(t.findResources("AWS::SecretsManager::Secret"))
  .filter(([k]) => k.startsWith("OrchestratorSecret")).map(([, r]) => r.Properties.SecretString));
const asList = (v: unknown) => (Array.isArray(v) ? v : [v]);

describe("deliver steps", () => {
  afterEach(() => jest.restoreAllMocks());

  it("synthesizes a workflow without a deliver step byte-identically to a build without this code", () => {
    // With the deliver code a no-op, the template is what a build before it made; with it
    // live, the same. Also with a deliverSecrets secret saved that no target names.
    for (const wf of [shipped, { ...shipped, orchestrator: { ...shipped.orchestrator, files: {} } }]) {
      const live = JSON.stringify(synth(wf, { unused: "x" }).toJSON());
      jest.spyOn(deliver, "deliverStatements").mockReturnValue([]);
      jest.spyOn(deliver, "deliverSecretEnv").mockReturnValue(null);
      jest.spyOn(deliver, "validateDeliver").mockImplementation(() => undefined);
      const without = JSON.stringify(synth(wf).toJSON());
      jest.restoreAllMocks();
      expect(live).toEqual(without);
      expect(live).not.toMatch(/"Sid":"Deliver/);
      expect(live).not.toContain("DELIVER_SECRETS");
    }
    expect(deliver.deliverStatements(shipped, "arn:aws:s3:::b")).toEqual([]);
    expect(deliver.deliverSecretEnv(shipped, { hook: "x" })).toBeNull();
  });

  it("reads only the run's saved files, puts only under the targets' prefixes, sends only from the identity", () => {
    const st = runtimeStatements(synth(withDeliver(), { hook: "s3cret" }));
    const read = st.find((s) => s.Sid === "DeliverReadRunFiles");
    expect(read.Action).toBe("s3:GetObject");
    expect(JSON.stringify(read.Resource)).toContain("/runs/*/outputs/*");
    const list = st.find((s) => s.Sid === "DeliverListRunFiles");
    expect(list.Action).toBe("s3:ListBucket");
    expect(list.Condition).toEqual({ StringLike: { "s3:prefix": ["runs/*/outputs/*"] } });
    const put = st.find((s) => s.Sid === "DeliverS3Put");
    expect(put.Action).toBe("s3:PutObject");
    expect(asList(put.Resource)).toEqual(["arn:aws:s3:::amzn-s3-demo-bucket/reports/*", "arn:aws:s3:::amzn-s3-demo-bucket/*"]);
    const ses = st.find((s) => s.Sid === "DeliverSesSend");
    expect(ses.Action).toBe("ses:SendEmail");
    expect(asList(ses.Resource)).toEqual([IDENTITY]);
    expect(ses.Condition).toEqual({ StringEquals: { "ses:FromAddress": ["reports@example.com"] } });
    // No wildcard resource for any of them.
    for (const s of [read, list, put, ses]) expect(JSON.stringify(s.Resource)).not.toBe('"*"');
  });

  it("puts only what a target needs: no SES without an email target, no put without an s3 one", () => {
    const st = runtimeStatements(synth(withDeliver([{ type: "webhook", url: "https://hooks.example.com/in" }])));
    const sids = st.map((s) => s.Sid);
    expect(sids).toEqual(expect.arrayContaining(["DeliverReadRunFiles", "DeliverListRunFiles"]));
    expect(sids).not.toContain("DeliverS3Put");
    expect(sids).not.toContain("DeliverSesSend");
  });

  it("deploys the named signing secret, and only it, in the runtime secret", () => {
    const secret = runtimeSecret(synth(withDeliver(), { hook: "s3cret", other: "nope" }));
    expect(secret).toContain("DELIVER_SECRETS");
    expect(secret).toContain("s3cret");
    expect(secret).not.toContain("nope");
    expect(deliver.deliverSecretEnv(withDeliver(), { hook: "s3cret", other: "nope" })).toEqual({ hook: "s3cret" });
    expect(runtimeSecret(synth(withDeliver([TARGETS[0]])))).not.toContain("DELIVER_SECRETS");
  });

  it("fails the synth for what the runtime could not deliver with", () => {
    expect(() => synth(withDeliver(TARGETS, true), {})).toThrow(/deliverSecrets secret\(s\) hook, but the build has none/);
    expect(() => synth(withDeliver([TARGETS[0]], false))).toThrow(/set orchestrator.files/);
    // ${storage} without orchestrator.storage: refused where the token is resolved (lib/storage.ts).
    expect(() => synth(withDeliver([{ type: "s3", uri: "${storage}/out/" }])))
      .toThrow(/task\.targets\[0\]\.uri uses \$\{storage\}, but this build creates no storage/);
  });

  it("matches the Terraform path", () => {
    // Whitespace folded: terraform fmt aligns the `=` of each block.
    const tf = fs.readFileSync(path.join(ORCH_ROOT, "terraform", "deliver.tf"), "utf8").replace(/\s+/g, " ");
    for (const s of ['Sid = "DeliverReadRunFiles", Effect = "Allow", Action = ["s3:GetObject"]',
      '"${aws_s3_bucket.assets[0].arn}/runs/*/outputs/*"',
      'Sid = "DeliverListRunFiles", Effect = "Allow", Action = ["s3:ListBucket"]',
      'Condition = { StringLike = { "s3:prefix" = ["runs/*/outputs/*"] } }',
      'Sid = "DeliverS3Put", Effect = "Allow", Action = ["s3:PutObject"]',
      '"arn:aws:s3:::${trimprefix(t.uri, "s3://")}*"',
      'Sid = "DeliverSesSend", Effect = "Allow", Action = ["ses:SendEmail"]',
      'Condition = { StringEquals = { "ses:FromAddress" = local.deliver_from } }',
      "set orchestrator.files so the run keeps them", "which needs the build's storage (orchestrator.storage)",
      "DELIVER_SECRETS = jsonencode"]) {
      expect(tf).toContain(s);
    }
    expect(fs.readFileSync(path.join(ORCH_ROOT, "terraform", "secrets.tf"), "utf8")).toContain("}, local.deliver_secrets_env))");
    const vars = fs.readFileSync(path.join(ORCH_ROOT, "terraform", "variables.tf"), "utf8");
    expect(vars).toMatch(/variable "deliver_secrets" \{[\s\S]*?default\s+= \{\}\s+sensitive\s+= true\n\}/);
    const runner = fs.readFileSync(path.join(ORCH_ROOT, "deployer", "runner.py"), "utf8");
    expect(runner).toContain('out["DELIVER_SECRETS"] = out["TF_VAR_deliver_secrets"]');
    expect(fs.readFileSync(path.join(__dirname, "..", "bin", "orchestrator.ts"), "utf8"))
      .toContain('parseSecretMap("DELIVER_SECRETS"');
  });
});
