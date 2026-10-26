/**
 * Track B docgen. The built-in Document generator (source: "docgen"), synthesized through
 * the ToolPlane.
 *
 * What is worth pinning is the role: read only the template object or folder, write only
 * run outputs and the output prefix, read back only the copy it links to, write its own
 * log group, and nothing else. Mirrors tests/test_docgen_tool.py, which pins the same facts
 * in terraform/docgen_tool.tf and terraform/tools.tf.
 */
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { ToolPlane, ToolSpec } from "../lib/tool-plane";
import { validateTools } from "../lib/orchestrator-stack";
import { s3ObjectArn } from "../lib/docgen-tool";

const ORCH_ROOT = path.join(__dirname, "..", "..");
/** A copy of app/tools/docgen/ with its libraries "installed" (a stand-in package), as a
 *  deploy leaves it: the synth refuses a folder without them. */
const INSTALLED = (() => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "docgen-"));
  const dir = path.join(root, "app", "tools", "docgen");
  fs.mkdirSync(path.join(dir, "pptx"), { recursive: true });
  for (const f of ["handler.py", "render.py", "requirements.txt"]) {
    fs.copyFileSync(path.join(ORCH_ROOT, "app", "tools", "docgen", f), path.join(dir, f));
  }
  fs.writeFileSync(path.join(dir, "pptx", "__init__.py"), "");
  return root;
})();
afterAll(() => fs.rmSync(INSTALLED, { recursive: true, force: true }));

const RUN_FILES = { bucketName: "agentcore-test-orch-assets-123456789012", bucketArn: "arn:aws:s3:::agentcore-test-orch-assets-123456789012" };
function plane(tools: Record<string, ToolSpec>, opts: { orchRoot?: string; runFiles?: typeof RUN_FILES | null } = {}) {
  const app = new cdk.App();
  const stack = new cdk.Stack(app, "T", { env: { account: "123456789012", region: "us-east-1" } });
  new ToolPlane(stack, "ToolPlane", {
    agentName: "test_orch",
    tools,
    toolApiKeys: {},
    gatewayDiscoveryUrl: "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc/.well-known/openid-configuration",
    gatewayClientId: "client-abc",
    gatewayAudience: "gateway/invoke",
    isCognito: true,
    policyEnabled: true,
    policyMode: "ENFORCE",
    customPolicies: [],
    orchRoot: opts.orchRoot ?? INSTALLED,
    ...(opts.runFiles === null ? {} : { runFiles: opts.runFiles ?? RUN_FILES }),
  });
  return Template.fromStack(stack);
}
const asList = (v: any): any[] => (Array.isArray(v) ? v : [v]);
const DOCS: ToolSpec = {
  type: "lambda",
  source: "docgen",
  description: "Write documents.",
  call: "generate_document",
  arg: "data",
  formats: ["pdf", "docx"],
  toolSchema: [{ name: "generate_document", properties: {
    format: { type: "string", required: true }, data: { type: "object", required: true } } }],
};
const FN_NAME = "ToolLambda-test_orch-docs";
const RUN_OUTPUTS = `${RUN_FILES.bucketArn}/runs/*/outputs/*`;

describe("the built-in Document generator (source: docgen)", () => {
  const template = plane({ docs: DOCS });
  const fn = (t: Template = template): any =>
    Object.values<any>(t.findResources("AWS::Lambda::Function")).find((f) => f.Properties.FunctionName === FN_NAME);
  const own = (t: Template = template): any[] => {
    const roleId = fn(t).Properties.Role["Fn::GetAtt"][0];
    return Object.values<any>(t.findResources("AWS::IAM::Policy"))
      .filter((p) => (p.Properties.Roles ?? []).some((r: any) => r.Ref === roleId))
      .flatMap((p) => p.Properties.PolicyDocument.Statement);
  };
  const sid = (st: any[], s: string) => st.find((x) => x.Sid === s);

  it("deploys app/tools/docgen/ with 60 s and 512 MB, told its formats and the run files bucket", () => {
    template.hasResourceProperties("AWS::Lambda::Function", {
      FunctionName: FN_NAME, Handler: "handler.lambda_handler", Runtime: "python3.12", Timeout: 60, MemorySize: 512,
    });
    expect(fn().Properties.Environment).toEqual({ Variables: { FORMATS: "pdf,docx", ASSETS_BUCKET: RUN_FILES.bucketName } });
  });

  it("without a template or output prefix: saves with the run and reads back only that copy", () => {
    const st = own();
    expect(sid(st, "ReadItsTemplates")).toBeUndefined();
    expect(sid(st, "WriteItsOutputs")).toBeUndefined();
    const save = sid(st, "SaveWithTheRun");
    expect(asList(save.Action).sort()).toEqual(["s3:GetObject", "s3:PutObject"]);
    expect(asList(save.Resource)).toEqual([RUN_OUTPUTS]);
  });

  it("reads exactly one template object, or only under a template folder", () => {
    const one = plane({ docs: { ...DOCS, formats: ["docx"], templateS3Uri: "s3://my-templates/forms/report.docx" } });
    expect(fn(one).Properties.Environment.Variables.TEMPLATE_S3_URI).toBe("s3://my-templates/forms/report.docx");
    const read = sid(own(one), "ReadItsTemplates");
    expect(asList(read.Action)).toEqual(["s3:GetObject"]);
    expect(asList(read.Resource)).toEqual(["arn:aws:s3:::my-templates/forms/report.docx"]);
    const folder = plane({ docs: { ...DOCS, templateS3Uri: "s3://my-templates/forms/" } });
    expect(asList(sid(own(folder), "ReadItsTemplates").Resource)).toEqual(["arn:aws:s3:::my-templates/forms/*"]);
  });

  it("writes under the output prefix and links to that copy, and reads who wrote a run file", () => {
    const t = plane({ docs: { ...DOCS, outputPrefix: "s3://my-outputs/reports/" } });
    expect(fn(t).Properties.Environment.Variables.OUTPUT_PREFIX).toBe("s3://my-outputs/reports/");
    const st = own(t);
    const out = sid(st, "WriteItsOutputs");
    expect(asList(out.Action).sort()).toEqual(["s3:GetObject", "s3:PutObject"]);
    expect(asList(out.Resource)).toEqual(["arn:aws:s3:::my-outputs/reports/*"]);
    // HeadObject on a taken name, so a step's re-run replaces only its own earlier file.
    const save = sid(st, "SaveWithTheRun");
    expect(asList(save.Action).sort()).toEqual(["s3:GetObject", "s3:PutObject"]);
    expect(asList(save.Resource)).toEqual([RUN_OUTPUTS]);
  });

  it("publishes the attempt the runtime sets, without the config declaring it", () => {
    const lambdaTools = (t: Template) => Object.values<any>(t.findResources("AWS::BedrockAgentCore::GatewayTarget"))
      .map((r) => r.Properties.TargetConfiguration?.Mcp?.Lambda?.ToolSchema?.InlinePayload).filter(Boolean);
    const [docs] = lambdaTools(template);
    expect(Object.keys(docs[0].InputSchema.Properties).sort()).toEqual(["attempt", "data", "format"]);
    expect(docs[0].InputSchema.Properties.attempt).toEqual({ Type: "string", Description: "Set by the workflow on every call; leave it out." });
    expect(docs[0].InputSchema.Required).toEqual(["format", "data"]);
    // One the config declares is kept as written; another Lambda tool gets nothing.
    const declared = plane({ docs: { ...DOCS, toolSchema: [{ name: "generate_document", properties: {
      ...DOCS.toolSchema![0].properties, attempt: { type: "string", description: "mine" } } }] } });
    expect(lambdaTools(declared)[0][0].InputSchema.Properties.attempt.Description).toBe("mine");
    const other = plane({ sandbox: { type: "lambda", source: "codeInterpreter", call: "execute_code", arg: "code",
      toolSchema: [{ name: "execute_code", properties: { code: { type: "string", required: true } } }] } },
    { orchRoot: ORCH_ROOT });
    expect(Object.keys(lambdaTools(other)[0][0].InputSchema.Properties)).toEqual(["code"]);
  });

  it("may do nothing else: no wildcard action or resource, no ListBucket, no managed policy", () => {
    const st = own(plane({ docs: { ...DOCS, templateS3Uri: "s3://my-templates/t/", outputPrefix: "s3://my-outputs/" } }));
    const actions = st.flatMap((s) => asList(s.Action)).map(String);
    expect([...new Set(actions)].sort()).toEqual(["logs:CreateLogStream", "logs:PutLogEvents", "s3:GetObject", "s3:PutObject"]);
    expect(st.flatMap((s) => asList(s.Resource)).some((r) => r === "*")).toBe(false);
    expect(JSON.stringify(st.find((s) => asList(s.Action).includes("logs:PutLogEvents")).Resource)).toContain("ToolLambdaLogGroupdocs");
    const roleId = fn().Properties.Role["Fn::GetAtt"][0];
    expect(template.toJSON().Resources[roleId].Properties.ManagedPolicyArns).toBeUndefined();
  });

  it("is registered as a Gateway target the Gateway may invoke", () => {
    const target = Object.values<any>(template.findResources("AWS::BedrockAgentCore::GatewayTarget"))
      .find((r) => r.Properties.TargetConfiguration?.Mcp?.Lambda);
    expect(target.Properties.TargetConfiguration.Mcp.Lambda.LambdaArn["Fn::GetAtt"][0]).toMatch(/ToolLambdadocs/);
  });

  it("refuses a deployment without run files, without its libraries, or naming ${storage} before it resolves", () => {
    expect(() => plane({ docs: DOCS }, { runFiles: null })).toThrow(/set orchestrator.files/);
    expect(() => plane({ docs: DOCS }, { orchRoot: ORCH_ROOT + path.sep + "no-such-root" })).toThrow(/libraries are not installed/);
    expect(() => plane({ docs: { ...DOCS, outputPrefix: "${storage}/out/" } })).toThrow(/needs the build's storage/);
  });

  it("adds nothing at all when no tool asks for it", () => {
    const none = plane({ sandbox: { type: "lambda", source: "codeInterpreter", call: "execute_code", arg: "code",
      toolSchema: [{ name: "execute_code", properties: { code: { type: "string", required: true } } }] } },
    { orchRoot: ORCH_ROOT });
    const text = JSON.stringify(none.toJSON());
    for (const s of ["FORMATS", "TEMPLATE_S3_URI", "OUTPUT_PREFIX", "SaveWithTheRun", "ReadItsTemplates", "/outputs/"]) {
      expect(text).not.toContain(s);
    }
    const sandbox = Object.values<any>(none.findResources("AWS::Lambda::Function"))
      .find((f) => f.Properties.FunctionName === "ToolLambda-test_orch-sandbox");
    expect(sandbox.Properties.MemorySize).toBe(256);
  });

  it("is a source the synth accepts", () => {
    expect(() => validateTools({ docs: DOCS }, {}, "us-east-1", ORCH_ROOT)).not.toThrow();
    expect(() => validateTools({ docs: { ...DOCS, source: "documents" } }, {}, "us-east-1")).toThrow(/"docgen"/);
  });

  it("covers one object or everything under a folder, the same way as Terraform", () => {
    expect(s3ObjectArn("s3://b/a/t.pptx")).toBe("arn:aws:s3:::b/a/t.pptx");
    expect(s3ObjectArn("s3://b/a/")).toBe("arn:aws:s3:::b/a/*");
    expect(s3ObjectArn("s3://b/")).toBe("arn:aws:s3:::b/*");
  });
});
