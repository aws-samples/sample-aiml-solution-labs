/**
 * Organisation guardrails (lib/hardening.ts): a permissions boundary and a path on EVERY
 * IAM role, the CDK framework's own included; a custom domain with TLS 1.2; AWS WAF. All off
 * by default, and unset they leave the template as it was. The Terraform half is
 * terraform/tests/hardening.tftest.hcl, and tests/test_hardening.py holds the two to the
 * same values.
 */
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { OrchestratorStack } from "../lib/orchestrator-stack";
import { ToolPlane } from "../lib/tool-plane";
import * as hardening from "../lib/hardening";

const ORCH_ROOT = path.join(__dirname, "..", "..");
const BOUNDARY = "arn:aws:iam::123456789012:policy/OrgBoundary";
const CERT = "arn:aws:acm:us-east-1:123456789012:certificate/0f1e2d3c-4b5a-6978-8a9b-0c1d2e3f4a5b";

function synth(overrides: Record<string, any> = {}, region = "us-east-1") {
  const app = new cdk.App();
  const stack = new OrchestratorStack(app, "TestStack", {
    env: { account: "123456789012", region },
    agentName: "multiagent_orchestrator", modelId: "", memoryEventExpiryDays: 30,
    idp: "cognito", createCognito: true, cognitoUserPoolId: "", cognitoClientId: "",
    cognitoDomainPrefix: "", auth0Domain: "", auth0ClientId: "", enableGateway: true,
    gatewayClientId: "", gatewayClientSecret: "", gatewayAudience: "", toolApiKeys: {},
    a2aTokens: {}, transactionSearchIndexingPercentage: 100, ...overrides,
  });
  return Template.fromStack(stack);
}
const roles = (t: Template) => Object.entries<any>(t.findResources("AWS::IAM::Role"));
const deployRolePolicies = (t: Template) => JSON.stringify(Object.values<any>(t.findResources("AWS::IAM::Policy"))
  .filter((p) => JSON.stringify(p.Properties.Roles ?? []).includes("BuilderDeployRole")));
const codeBuildEnv = (t: Template) => Object.values<any>(t.findResources("AWS::CodeBuild::Project"))[0]
  .Properties.Environment.EnvironmentVariables.map((v: any) => v.Name);

describe("unset (the default), nothing changes", () => {
  let t: Template;
  beforeAll(() => { t = synth(); });
  it("gives no role a path or a boundary", () => {
    expect(roles(t).length).toBeGreaterThan(10);
    for (const [id, r] of roles(t)) {
      expect([id, r.Properties.Path]).toEqual([id, undefined]);
      expect([id, r.Properties.PermissionsBoundary]).toEqual([id, undefined]);
    }
  });
  it("keeps CloudFront's own certificate, no alias, no web ACL", () => {
    const d = Object.values<any>(t.findResources("AWS::CloudFront::Distribution"))[0].Properties.DistributionConfig;
    expect(d.Aliases).toBeUndefined();
    expect(d.ViewerCertificate).toBeUndefined();
    expect(d.WebACLId).toBeUndefined();
    t.resourceCountIs("AWS::WAFv2::WebACL", 0);
  });
  it("passes builds nothing new and asks nothing new of the deploy role", () => {
    expect(codeBuildEnv(t)).not.toEqual(expect.arrayContaining(["PERMISSIONS_BOUNDARY_ARN"]));
    expect(codeBuildEnv(t)).not.toEqual(expect.arrayContaining(["IAM_ROLE_PATH"]));
    expect(deployRolePolicies(t)).not.toContain("iam:PermissionsBoundary");
  });
  it("registers only the CloudFront URL for sign-in", () => {
    const spa = Object.values<any>(t.findResources("AWS::Cognito::UserPoolClient"))
      .find((c) => c.Properties.ClientName === "multiagent_orchestrator-spa");
    expect(spa.Properties.CallbackURLs).toHaveLength(1);
  });
});

describe("with every guardrail set", () => {
  let t: Template;
  beforeAll(() => {
    t = synth({ permissionsBoundaryArn: BOUNDARY, iamRolePath: "/agentexpress/", customDomain: "agents.example.com",
      acmCertificateArn: CERT, enableWaf: true, wafRateLimit: 5000 });
  });
  it("puts the boundary and the path on EVERY role, CDK's own framework roles too", () => {
    const all = roles(t);
    expect(all.length).toBeGreaterThan(10);
    for (const [id, r] of all) {
      expect([id, r.Properties.PermissionsBoundary]).toEqual([id, BOUNDARY]);
      expect([id, r.Properties.Path]).toEqual([id, "/agentexpress/"]);
    }
    // The roles CDK makes for its own Lambdas, not declared anywhere in lib/: the
    // AwsCustomResource singleton, the UI deployment's handler, the auto-delete provider.
    const ids = all.map(([id]) => id).join(" ");
    expect(ids).toMatch(/AWS679f53fac002430cb0da5b7982bd2287/);
    expect(ids).toMatch(/CustomCDKBucketDeployment/);
    expect(ids).toMatch(/CustomS3AutoDeleteObjectsCustomResourceProvider/);
  });
  it("makes the deploy role create roles only with the boundary, and keep it", () => {
    const p = deployRolePolicies(t);
    expect(p).toContain('"ArnNotEquals":{"iam:PermissionsBoundary":"' + BOUNDARY + '"}');
    for (const sid of ["RolesOnlyWithTheBoundary", "KeepTheBoundary", "LeaveTheBoundaryPolicyAlone"]) expect(p).toContain(sid);
  });
  it("passes the boundary and the path on to every build", () => {
    const env = Object.values<any>(t.findResources("AWS::CodeBuild::Project"))[0].Properties.Environment.EnvironmentVariables;
    expect(env).toEqual(expect.arrayContaining([{ Name: "PERMISSIONS_BOUNDARY_ARN", Type: "PLAINTEXT", Value: BOUNDARY },
      { Name: "IAM_ROLE_PATH", Type: "PLAINTEXT", Value: "/agentexpress/" }]));
  });
  it("names the deploy policy's roles under the path", () => {
    const managed = JSON.stringify(Object.values<any>(t.findResources("AWS::IAM::ManagedPolicy"))
      .filter((p) => String(p.Properties.ManagedPolicyName).startsWith("AgentExpressDeploy-")));
    expect(managed).toContain(":role/agentexpress/AgentCoreRuntime-*");
    expect(managed).not.toMatch(/:role\/AgentCore/);
  });
  it("serves the custom domain with its certificate, SNI and TLS 1.2 (2021)", () => {
    const d = Object.values<any>(t.findResources("AWS::CloudFront::Distribution"))[0].Properties.DistributionConfig;
    expect(d.Aliases).toEqual(["agents.example.com"]);
    expect(d.ViewerCertificate).toEqual({ AcmCertificateArn: CERT, MinimumProtocolVersion: "TLSv1.2_2021", SslSupportMethod: "sni-only" });
  });
  it("lets people sign in and upload from the custom domain as well as the CloudFront URL", () => {
    const spa = Object.values<any>(t.findResources("AWS::Cognito::UserPoolClient"))
      .find((c) => c.Properties.ClientName === "multiagent_orchestrator-spa");
    expect(spa.Properties.CallbackURLs).toHaveLength(2);
    expect(spa.Properties.CallbackURLs).toContain("https://agents.example.com");
    expect(spa.Properties.LogoutURLs).toContain("https://agents.example.com");
    const cors = JSON.stringify(Object.values<any>(t.findResources("AWS::S3::Bucket"))
      .map((b) => b.Properties.CorsConfiguration).filter(Boolean));
    expect(cors).toContain("https://agents.example.com");
    expect(t.toJSON().Outputs.uiUrl.Value).toBe("https://agents.example.com");
  });
  it("puts a CLOUDFRONT web ACL on the distribution: a rate limit and three managed groups", () => {
    const acls = Object.entries<any>(t.findResources("AWS::WAFv2::WebACL"));
    expect(acls).toHaveLength(1);
    const [aclId, acl] = acls[0];
    expect(acl.Properties.Scope).toBe("CLOUDFRONT");
    expect(acl.Properties.DefaultAction).toEqual({ Allow: {} });
    const rules = acl.Properties.Rules;
    expect(rules[0].Statement.RateBasedStatement).toEqual({ Limit: 5000, AggregateKeyType: "IP" });
    expect(rules.slice(1).map((r: any) => r.Statement.ManagedRuleGroupStatement.Name)).toEqual(hardening.WAF_MANAGED_RULE_GROUPS);
    const crs = rules.find((r: any) => r.Name === "AWSManagedRulesCommonRuleSet");
    expect(crs.Statement.ManagedRuleGroupStatement.RuleActionOverrides).toEqual([
      { Name: "SizeRestrictions_BODY", ActionToUse: { Count: {} } }]);
    const d = Object.values<any>(t.findResources("AWS::CloudFront::Distribution"))[0].Properties.DistributionConfig;
    expect(d.WebACLId).toEqual({ "Fn::GetAtt": [aclId, "Arn"] });
  });
});

describe("a code tool's role under the organisation's boundary", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "ax-hard-"));
  fs.mkdirSync(path.join(root, "app", "tools", "_code", "refunds"), { recursive: true });
  fs.writeFileSync(path.join(root, "app", "tools", "_code", "refunds", "handler.py"), "def lambda_handler(e, c):\n    return {}\n");
  const plane = (boundary: string) => {
    const app = new cdk.App();
    const stack = new cdk.Stack(app, "C", { env: { account: "123456789012", region: "us-east-1" } });
    new ToolPlane(stack, "ToolPlane", {
      agentName: "ax_1a2b3c4d", toolApiKeys: {},
      tools: { refunds: { type: "lambda", description: "Refunds", toolSchema: [{ name: "q", description: "q",
        properties: { q: { type: "string", required: true, description: "q" } } }], code: { grants: { table: "orders" } } } as any },
      gatewayDiscoveryUrl: "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc/.well-known/openid-configuration",
      gatewayClientId: "c", gatewayAudience: "gateway/invoke", isCognito: true,
      policyEnabled: false, policyMode: "ENFORCE", orchRoot: root,
    });
    const h = hardening.hardeningOf({ permissionsBoundaryArn: boundary }, "us-east-1");
    hardening.applyIamGuardrails(stack, h);
    hardening.denyFrameworkDataToCodeTools(stack, h);
    const t = Template.fromStack(stack);
    const [roleId, role] = Object.entries<any>(t.findResources("AWS::IAM::Role"))
      .find(([, r]) => r.Properties.RoleName === "ToolLambda-ax_1a2b3c4d-refunds")!;
    const attached = JSON.stringify(Object.values<any>(t.findResources("AWS::IAM::Policy"))
      .filter((p) => JSON.stringify(p.Properties.Roles).includes(roleId)));
    return { role, attached };
  };
  it("keeps its own boundary when none is set", () => {
    const { role, attached } = plane("");
    expect(role.Properties.PermissionsBoundary).toEqual({ Ref: expect.stringMatching(/^ToolPlaneToolCodeBoundaryrefunds/) });
    expect(attached).not.toContain("NeverFrameworkTables");
  });
  it("takes the organisation's, and keeps denying the framework's data in its own policy", () => {
    const { role, attached } = plane(BOUNDARY);
    expect(role.Properties.PermissionsBoundary).toBe(BOUNDARY);
    for (const sid of ["NeverFrameworkTables", "NeverFrameworkSecrets", "NeverFrameworkBuckets"]) expect(attached).toContain(sid);
    expect(attached).toContain('"Effect":"Deny"');
  });
});

describe("the settings are checked", () => {
  it.each([
    [{ permissionsBoundaryArn: "arn:aws:iam::123456789012:role/NotAPolicy" }, /permissionsBoundaryArn/],
    [{ iamRolePath: "agentexpress" }, /iamRolePath/],
    [{ iamRolePath: "/a*b/" }, /iamRolePath/],
    [{ customDomain: "agents.example.com" }, /go together/],
    [{ acmCertificateArn: CERT }, /go together/],
    [{ customDomain: "https://agents.example.com", acmCertificateArn: CERT }, /customDomain/],
    [{ customDomain: "agents.example.com", acmCertificateArn: CERT.replace("us-east-1", "eu-west-1") }, /us-east-1/],
    [{ wafRateLimit: 50 }, /wafRateLimit/],
  ])("%j is refused", (props, message) => {
    expect(() => hardening.hardeningOf(props as any, "us-east-1")).toThrow(message);
  });
  it("refuses WAF outside us-east-1, on the stack itself", () => {
    expect(() => synth({ enableWaf: true }, "eu-west-1")).toThrow(/enableWaf needs the stack in us-east-1/);
  });
  it("reads every setting from context in bin/orchestrator.ts, with the Terraform defaults", () => {
    const bin = fs.readFileSync(path.join(__dirname, "..", "bin", "orchestrator.ts"), "utf8");
    for (const [key, d] of [["permissionsBoundaryArn", '""'], ["iamRolePath", '"/"'], ["customDomain", '""'],
      ["acmCertificateArn", '""'], ["enableWaf", '"false"'], ["wafRateLimit", '"2000"']]) {
      expect(bin).toContain(`ctx("${key}", ${d})`);
    }
    const tf = fs.readFileSync(path.join(ORCH_ROOT, "terraform", "variables.tf"), "utf8");
    expect(tf).toMatch(/variable "iam_role_path" \{[^}]*default\s+= "\/"/);
    expect(tf).toMatch(/variable "enable_waf" \{[^}]*default\s+= false/);
  });
});
