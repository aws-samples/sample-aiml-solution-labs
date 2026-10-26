/**
 * v1.3.9's tracks, wired together in CDK (tests/test_v139_wiring.py is the same for Terraform):
 * the `${storage}` token (lib/storage.ts) resolves to the build's bucket in the Document
 * generator's settings (lib/docgen-tool.ts) and in a deliver step's s3 targets
 * (lib/deliver.ts), so their grants name the bucket's folders; and a build that names it in a
 * deliver target without creating storage is refused.
 */
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import * as path from "path";
import { deliverStatements } from "../lib/deliver";
import { docgenStatements } from "../lib/docgen-tool";
import { OrchestratorStack } from "../lib/orchestrator-stack";
import { resolveStorage, STORAGE_TOKEN, storageOf } from "../lib/storage";

const ORCH_ROOT = path.join(__dirname, "..", "..");
const shipped = require(`${ORCH_ROOT}/app/workflow.json`);
const ACCOUNT = "123456789012";
const AGENT = "multiagent_orchestrator";
const BUCKET = `multiagent-orchestrator-storage-${ACCOUNT}`;
const ASSETS = "arn:aws:s3:::assets";

const docgen = {
  type: "lambda", description: "Writes the report.", source: "docgen", call: "generate_document", arg: "data",
  toolSchema: [{ name: "generate_document", properties: { format: { type: "string", required: true }, data: { type: "object", required: true } } }],
  formats: ["pdf"], templateS3Uri: `${STORAGE_TOKEN}/templates/`, outputPrefix: `${STORAGE_TOKEN}/outputs/`,
};
const send = { name: "Send", runtime: "task", task: { kind: "deliver", targets: [{ type: "s3", uri: `${STORAGE_TOKEN}/delivered/` }] } };
function workflow(storage: boolean, tools: Record<string, any> = {}): any {
  return {
    ...shipped,
    orchestrator: { ...shipped.orchestrator, files: {}, ...(storage ? { storage: { create: true } } : {}) },
    tools: { ...shipped.tools, ...tools },
    agents: { ...shipped.agents, send },
    steps: [...shipped.steps, { agent: "send" }],
  };
}
function synth(wf: any): Template {
  const app = new cdk.App();
  const stack = new OrchestratorStack(app, "TestStack", {
    env: { account: ACCOUNT, region: "us-east-1" },
    agentName: AGENT,
    modelId: "us.anthropic.claude-haiku-4-5-20251001-v1:0",
    memoryEventExpiryDays: 30,
    idp: "cognito", createCognito: true, cognitoUserPoolId: "", cognitoClientId: "",
    cognitoDomainPrefix: "", auth0Domain: "", auth0ClientId: "",
    enableGateway: true, gatewayClientId: "", gatewayClientSecret: "dummy", gatewayAudience: "",
    toolApiKeys: {}, a2aTokens: {}, transactionSearchIndexingPercentage: 100,
    workflow: wf,
  } as any);
  return Template.fromStack(stack);
}
const asList = (v: unknown) => (Array.isArray(v) ? v : [v]);

describe("v1.3.9 wiring", () => {
  it("resolves ${storage} in the Document generator's locations and grants them on the build's bucket", () => {
    const wf = workflow(true, { reportDoc: docgen });
    const out = resolveStorage(wf, storageOf(wf, AGENT, ACCOUNT));
    expect(out.tools.reportDoc.templateS3Uri).toBe(`s3://${BUCKET}/templates/`);
    expect(out.tools.reportDoc.outputPrefix).toBe(`s3://${BUCKET}/outputs/`);
    const resources = docgenStatements(out.tools.reportDoc, ASSETS).flatMap((s) => s.toJSON().Resource);
    expect(resources).toEqual(expect.arrayContaining([`arn:aws:s3:::${BUCKET}/templates/*`, `arn:aws:s3:::${BUCKET}/outputs/*`]));
    expect(JSON.stringify(resources)).not.toContain(STORAGE_TOKEN);
  });
  it("resolves ${storage} in a deliver target, so the runtime may put under that folder of the build's bucket", () => {
    const wf = workflow(true);
    const out = resolveStorage(wf, storageOf(wf, AGENT, ACCOUNT));
    expect(out.agents.send.task.targets[0].uri).toBe(`s3://${BUCKET}/delivered/`);
    const put = deliverStatements(out, ASSETS).map((s) => s.toJSON()).find((s) => s.Sid === "DeliverS3Put");
    expect(asList(put.Resource)).toEqual([`arn:aws:s3:::${BUCKET}/delivered/*`]);
  });
  it("synthesizes a deliver step into the build's storage, and refuses one when the build creates none", () => {
    const t = synth(workflow(true));
    const statements = Object.entries<any>(t.findResources("AWS::IAM::Policy"))
      .filter(([k]) => k.startsWith("RuntimeRole"))
      .flatMap(([, p]) => p.Properties.PolicyDocument.Statement as any[]);
    const put = statements.find((s) => s.Sid === "DeliverS3Put");
    expect(JSON.stringify(put.Resource)).toContain(`arn:aws:s3:::${BUCKET}/delivered/*`);
    expect(JSON.stringify(statements)).not.toContain(STORAGE_TOKEN);
    expect(Object.values<any>(t.findResources("AWS::S3::Bucket")).some((b) => b.Properties.BucketName === BUCKET)).toBe(true);
    expect(() => synth(workflow(false))).toThrow(/task\.targets\[0\]\.uri uses \$\{storage\}, but this build creates no storage/);
  });
});
