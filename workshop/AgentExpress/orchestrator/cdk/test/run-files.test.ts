/**
 * Keeping the files runs make (workflow.json `orchestrator.files`, lib/run-files.ts):
 * opt-in, so the shipped workflow gets nothing new; set, it gets the assets bucket, a
 * retention rule on runs/, a CORS GET for previews, the BFF's list grant and each
 * dedicated runtime's write grant on outputs/ and code/ only. Same as terraform/run_files.tf.
 */
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import * as fs from "fs";
import * as path from "path";

import { OrchestratorStack } from "../lib/orchestrator-stack";
import { filesRetentionDays } from "../lib/run-files";

const ORCH_ROOT = path.join(__dirname, "..", "..");
const shipped = require(`${ORCH_ROOT}/app/workflow.json`);

function synth(workflow: any): Template {
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
    workflow,
  } as any);
  return Template.fromStack(stack);
}

const withFiles = (files: any) => ({ ...shipped, orchestrator: { ...shipped.orchestrator, files } });
const assetsBucket = (t: Template) => Object.values<any>(t.findResources("AWS::S3::Bucket"))
  .find((b) => String(JSON.stringify(b.Properties?.BucketName ?? "")).includes("-assets-"));
const statements = (t: Template, prefix = "") => Object.entries<any>(t.findResources("AWS::IAM::Policy"))
  .filter(([k]) => k.startsWith(prefix))
  .flatMap(([, p]) => p.Properties.PolicyDocument.Statement as any[]);

describe("orchestrator.files", () => {
  it("reads the retention, 90 days by default, and refuses what S3 would", () => {
    expect(filesRetentionDays(shipped)).toBeNull();
    expect(filesRetentionDays(withFiles({}))).toBe(90);
    expect(filesRetentionDays(withFiles({ retentionDays: 30 }))).toBe(30);
    for (const bad of [0, 3651, 1.5, "90"]) {
      expect(() => filesRetentionDays(withFiles({ retentionDays: bad }))).toThrow(/1 to 3650/);
    }
  });

  let off: Template;
  let on: Template;
  beforeAll(() => {
    off = synth(shipped);
    on = synth(withFiles({ retentionDays: 30 }));
  });

  it("adds nothing to a workflow that does not set it", () => {
    expect(assetsBucket(off)).toBeUndefined();
    const all = JSON.stringify(statements(off));
    expect(all).not.toContain("ListRunFiles");
    expect(all).not.toContain("SaveRunFiles");
  });

  it("creates the bucket, expiring everything under runs/ after the retention", () => {
    const b = assetsBucket(on);
    expect(b.Properties.LifecycleConfiguration.Rules).toEqual([
      { Id: "expire-run-files", Prefix: "runs/", ExpirationInDays: 30, Status: "Enabled" }]);
    expect(b.Properties.CorsConfiguration.CorsRules.map((r: any) => r.AllowedMethods)).toEqual([["GET"]]);
    expect(b.Properties.PublicAccessBlockConfiguration).toMatchObject({ BlockPublicAcls: true, RestrictPublicBuckets: true });
  });

  it("lets the BFF list runs/ only, and each dedicated runtime write outputs and code only", () => {
    const list = statements(on, "BffServiceRoleDefaultPolicy").find((s) => s.Sid === "ListRunFiles");
    expect(list.Action).toBe("s3:ListBucket");
    expect(list.Condition).toEqual({ StringLike: { "s3:prefix": ["runs/*"] } });
    const save = statements(on, "SubagentRole").filter((s) => s.Sid === "SaveRunFiles");
    const dedicated = Object.values<any>(shipped.agents).filter((a) => a.runtime === "dedicated").length;
    expect(save).toHaveLength(dedicated);
    expect(JSON.stringify(save[0].Resource)).toContain("/runs/*/outputs/*");
    expect(JSON.stringify(save[0].Resource)).toContain("/runs/*/code/*");
    expect(save[0].Action).toBe("s3:PutObject");
  });

  it("routes the Files tab and page behind the JWT authorizer", () => {
    const routes = Object.values<any>(on.findResources("AWS::ApiGatewayV2::Route"));
    for (const key of ["GET /api/sessions/{id}/files", "GET /api/sessions/{id}/files/url", "GET /api/files"]) {
      const r = routes.find((x) => x.Properties.RouteKey === key);
      expect(r?.Properties.AuthorizationType).toBe("JWT");
    }
  });

  it("matches the Terraform path", () => {
    const tf = fs.readFileSync(path.join(ORCH_ROOT, "terraform", "run_files.tf"), "utf8");
    const images = fs.readFileSync(path.join(ORCH_ROOT, "terraform", "images.tf"), "utf8");
    for (const s of ["SaveRunFiles", "ListRunFiles", '"s3:prefix" = ["runs/*"]', "runs/*/outputs/*", "runs/*/code/*",
      "local.workflow_def.orchestrator.files", "1 to 3650"]) expect(tf).toContain(s);
    for (const s of ['id     = "expire-run-files"', 'prefix = "runs/"', 'allowed_methods = ["GET"]', "local.files_on"]) {
      expect(images).toContain(s);
    }
  });
});
