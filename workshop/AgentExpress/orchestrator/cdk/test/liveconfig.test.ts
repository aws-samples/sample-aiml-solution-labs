/** orchestrator.liveConfig (bff/liveconfig.py), synthesized: off, a stack gains its five
 *  authorized routes and nothing else; on, the BFF alone gets one statement, held to the
 *  _liveconfig# rows. Terraform's side is held to the same by tests/test_liveconfig.py. */
import * as fs from "fs";
import * as path from "path";
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import { OrchestratorStack } from "../lib/orchestrator-stack";

const ORCH = path.resolve(__dirname, "..", "..");
const SAMPLE = JSON.parse(fs.readFileSync(path.join(ORCH, "app", "workflow.json"), "utf8"));
const ROUTES = ["GET /api/liveconfig", "GET /api/liveconfig/versions", "POST /api/liveconfig/versions",
  "GET /api/liveconfig/versions/{version}", "PUT /api/liveconfig/active"];

/** The Lab 5 context: -c agentName=x -c idp=cognito -c createCognito=true -c enableGateway=true. */
function synth(wf: any): Template {
  const app = new cdk.App();
  const stack = new OrchestratorStack(app, "LiveConfigStack", {
    env: { account: "123456789012", region: "us-east-1" },
    agentName: "x", modelId: "us.anthropic.claude-haiku-4-5-20251001-v1:0",
    memoryEventExpiryDays: 30, idp: "cognito", createCognito: true, cognitoUserPoolId: "",
    cognitoClientId: "", cognitoDomainPrefix: "", auth0Domain: "", auth0ClientId: "",
    enableGateway: true, gatewayClientId: "", gatewayClientSecret: "dummy", gatewayAudience: "",
    toolApiKeys: {}, a2aTokens: {}, transactionSearchIndexingPercentage: 100, workflow: wf,
  } as any);
  return Template.fromStack(stack);
}

const statements = (t: Template): { policy: any; s: any }[] =>
  Object.values(t.findResources("AWS::IAM::Policy")).flatMap((p: any) =>
    p.Properties.PolicyDocument.Statement.map((s: any) => ({ policy: p, s })));

describe("orchestrator.liveConfig", () => {
  let off: Template;
  let on: Template;
  beforeAll(() => {
    off = synth(SAMPLE);
    const wf = structuredClone(SAMPLE);
    wf.orchestrator = { ...wf.orchestrator, liveConfig: true };
    on = synth(wf);
  });

  it("declares the five routes behind the JWT authorizer, on or off", () => {
    for (const t of [off, on]) {
      const routes = Object.values(t.findResources("AWS::ApiGatewayV2::Route")) as any[];
      for (const key of ROUTES) {
        const r = routes.find((x) => x.Properties.RouteKey === key);
        expect(r).toBeDefined();
        expect(r.Properties.AuthorizationType).toBe("JWT");
      }
    }
  });

  it("adds nothing else when off: no statement and no other resource names it", () => {
    expect(statements(off).filter(({ s }) => s.Sid === "LiveConfig")).toHaveLength(0);
    const named = Object.entries(off.toJSON().Resources as Record<string, any>)
      .filter(([, r]) => JSON.stringify(r).includes("liveconfig") || JSON.stringify(r).includes("_liveconfig"))
      .map(([, r]) => r.Type);
    expect(named.every((type) => type === "AWS::ApiGatewayV2::Route")).toBe(true);
    expect(named).toHaveLength(ROUTES.length);
  });

  it("when on, gives the BFF one statement on the reserved rows only", () => {
    const got = statements(on).filter(({ s }) => s.Sid === "LiveConfig");
    expect(got).toHaveLength(1);
    const { policy, s } = got[0];
    expect(s.Effect).toBe("Allow");
    expect(s.Action).toEqual(["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:Query"]);
    expect(s.Condition).toEqual({ "ForAllValues:StringLike": { "dynamodb:LeadingKeys": ["_liveconfig#*"] } });
    expect(JSON.stringify(s.Resource)).toContain("TelemetryTable");
    expect(JSON.stringify(s.Resource)).not.toContain("*");
    expect(JSON.stringify(s.Resource)).not.toContain("/index/");
    expect(JSON.stringify(policy.Properties.Roles)).toMatch(/Bff/);
  });

  it("gives no runtime role anything on those rows: the version rides in the start payload", () => {
    const touching = statements(on).filter(({ s }) => JSON.stringify(s).includes("_liveconfig"));
    expect(touching).toHaveLength(1);
    expect(JSON.stringify(touching[0].policy.Properties.Roles)).not.toMatch(/Runtime|Subagent/i);
  });
});
