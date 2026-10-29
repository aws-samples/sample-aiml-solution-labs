/**
 * Bucket names (lib/bucket-names.ts), and the UI / KB / schema uploads waiting for their
 * bucket's emptier (lib/auto-delete.ts).
 *
 * The names are held to ../tests/fixtures/bucket_names.json, the vectors bff/bucket_names.py
 * and terraform/bucket_names.tf are held to too (tests/test_bucket_names.py), so the three
 * agree. "legacy" is every deployment's name before; "regional" ends each name with the
 * Region, and cuts a long agent name (with a digest of it) to stay within S3's 63 characters.
 */
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import * as fs from "fs";
import * as path from "path";
import { bucketName, bucketNamingOf, BucketKind } from "../lib/bucket-names";
import { OrchestratorStack } from "../lib/orchestrator-stack";

const ORCH_ROOT = path.join(__dirname, "..", "..");
const shipped = require(`${ORCH_ROOT}/app/workflow.json`);
const VECTORS: any[] = JSON.parse(fs.readFileSync(path.join(ORCH_ROOT, "tests", "fixtures", "bucket_names.json"), "utf8"));
const ACCOUNT = "123456789012";

describe("bucket names", () => {
  it("are the shared vectors' names", () => {
    // ui-logs is named by CloudFormation on this path, and the UI bucket's legacy name
    // differs between the two paths (CDK's always had the Region).
    const mine = VECTORS.filter((v) => v.kind !== "ui-logs" && v.tool !== "terraform");
    expect(mine.length).toBeGreaterThan(100);
    for (const v of mine) {
      expect([v.kind, v.agent, v.region, v.scheme, bucketName(v.kind, v.agent, v.account, v.region, v.scheme)])
        .toEqual([v.kind, v.agent, v.region, v.scheme, v.name]);
    }
  });

  it("are valid S3 names of at most 63 characters in the regional scheme", () => {
    const agents = ["ax_1a2b3c4d", "multiagent_orchestrator", "a".repeat(48), "Upper_Case_And_Long_Agent_Name_x"];
    const regions = ["us-east-1", "ap-southeast-3", "us-gov-west-1", "us-isof-south-1", "cn-northwest-1"];
    for (const agent of agents) for (const region of regions) {
      for (const kind of ["assets", "kbdocs", "schemas", "ui", "storage"] as BucketKind[]) {
        const n = bucketName(kind, agent, ACCOUNT, region, "regional");
        expect(n.length).toBeLessThanOrEqual(63);
        expect(n).toMatch(/^[a-z0-9][a-z0-9-]*[a-z0-9]$/);
        expect(n.endsWith(`-${kind}-${ACCOUNT}-${region}`)).toBe(true);
      }
    }
    // A console build (ax_ + 8 hex) never needs the fallback, even in the longest Region.
    expect(bucketName("schemas", "ax_1a2b3c4d", ACCOUNT, "us-isof-south-1", "regional"))
      .toBe(`agentcore-ax-1a2b3c4d-schemas-${ACCOUNT}-us-isof-south-1`);
  });

  it("refuse a name S3 reserves, and a naming value that is not one", () => {
    expect(() => bucketName("storage", "xn__a", ACCOUNT, "us-east-1", "regional")).toThrow(/reserves/);
    expect(() => bucketName("storage", "sthree_a", ACCOUNT, "us-east-1", "regional")).toThrow(/reserves/);
    expect(bucketNamingOf(undefined)).toBe("legacy");
    expect(bucketNamingOf("regional")).toBe("regional");
    expect(() => bucketNamingOf("global")).toThrow(/legacy, regional/);
  });

  it("keep a token account in the name, counted as 12 digits", () => {
    const app = new cdk.App();
    const stack = new cdk.Stack(app, "S", { env: { region: "us-east-1" } });
    const n = bucketName("assets", "multiagent_orchestrator", stack.account, "us-east-1", "regional");
    expect(cdk.Token.isUnresolved(n)).toBe(true);
  });
});

describe("a stack with bucketNaming regional", () => {
  // A Region that serves every tool the shipped workflow has (web search is in few).
  const REGION = "eu-west-1";
  const AGENT = "ax_1a2b3c4d";
  let t: Template;
  let resources: Record<string, any>;
  beforeAll(() => {
    const app = new cdk.App();
    const stack = new OrchestratorStack(app, "TestStack", {
      env: { account: ACCOUNT, region: REGION },
      agentName: AGENT, modelId: "us.anthropic.claude-haiku-4-5-20251001-v1:0", memoryEventExpiryDays: 30,
      idp: "cognito", createCognito: true, cognitoUserPoolId: "", cognitoClientId: "", cognitoDomainPrefix: "",
      auth0Domain: "", auth0ClientId: "", enableGateway: true, gatewayClientId: "", gatewayClientSecret: "dummy",
      gatewayAudience: "", toolApiKeys: {}, a2aTokens: {}, transactionSearchIndexingPercentage: 100,
      bucketNaming: "regional",
      workflow: { ...shipped, orchestrator: { ...shipped.orchestrator, files: {}, storage: { create: true } } },
    } as any);
    t = Template.fromStack(stack);
    resources = t.toJSON().Resources;
  });

  it("gives every bucket it names the Region", () => {
    const names = Object.values<any>(t.findResources("AWS::S3::Bucket"))
      .map((b) => b.Properties?.BucketName).filter((n) => typeof n === "string");
    for (const kind of ["assets", "kbdocs", "schemas", "ui", "storage"] as BucketKind[]) {
      expect(names).toContain(bucketName(kind, AGENT, ACCOUNT, REGION, "regional"));
    }
    expect(names.every((n: string) => n.endsWith(`-${ACCOUNT}-${REGION}`))).toBe(true);
  });

  it("lets the console's BFF read a build's images in either scheme", () => {
    const statements = Object.values<any>(t.findResources("AWS::IAM::Policy"))
      .flatMap((p) => p.Properties.PolicyDocument.Statement);
    const images = statements.find((s) => JSON.stringify(s.Resource ?? "").includes("-assets-") && s.Action === "s3:GetObject"
      && JSON.stringify(s.Resource).includes("agentcore-ax-*"));
    expect(images.Resource).toEqual([
      `arn:aws:s3:::agentcore-ax-*-assets-${ACCOUNT}/runs/*`,
      `arn:aws:s3:::agentcore-ax-*-assets-${ACCOUNT}-${REGION}/runs/*`,
    ]);
  });

  it("uploads into a bucket only once that bucket's emptier exists", () => {
    const deployments = Object.entries<any>(resources).filter(([, r]) => r.Type === "Custom::CDKBucketDeployment");
    const ids = deployments.map(([id]) => id);
    expect(ids.some((id) => id.startsWith("UiDeploy"))).toBe(true);
    expect(ids.some((id) => id.includes("KbDocsDeploy"))).toBe(true);
    expect(ids.some((id) => id.includes("ToolSchema"))).toBe(true);
    for (const [id, r] of deployments) {
      const bucket = JSON.stringify(r.Properties.DestinationBucketName);
      const logical = /"Ref":"([A-Za-z0-9]+)"/.exec(bucket)![1];
      const emptier = Object.entries<any>(resources).find(([, x]) => x.Type === "Custom::S3AutoDeleteObjects"
        && JSON.stringify(x.Properties.BucketName).includes(`"${logical}"`))![0];
      expect([id, r.DependsOn ?? []]).toEqual([id, expect.arrayContaining([emptier])]);
    }
  });
});
