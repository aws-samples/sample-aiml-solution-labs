/**
 * type="smithy": the target shape, the Gateway role's grants and the synth-time checks.
 * Mirrors tests/test_smithy_tool.py (the Terraform path) and the fixture cases the two
 * Build view validators share.
 */
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import * as path from "path";
import { ToolPlane, ToolSpec } from "../lib/tool-plane";
import { validateTools } from "../lib/orchestrator-stack";
import { smithyStatements, validateSmithy } from "../lib/smithy-tool";

const ORCH_ROOT = path.join(__dirname, "..", "..");
const ACCOUNT = "123456789012";
const MODEL = {
  smithy: "2.0",
  shapes: {
    "example.places#Places": { type: "service", version: "1.0", traits: { "aws.protocols#restJson1": {} },
      operations: [{ target: "example.places#Search" }] },
    "example.places#Search": { type: "operation", input: { target: "example.places#SearchInput" } },
    "example.places#SearchInput": { type: "structure", members: { Text: { target: "smithy.api#String" } } },
  },
};
const ALLOW = { actions: ["geo:SearchPlaceIndexForText"], resources: [`arn:aws:geo:us-east-1:${ACCOUNT}:place-index/stores`] };
const INLINE: ToolSpec = { type: "smithy", description: "Places", schema: MODEL, allow: ALLOW, call: "Search", arg: "Text" } as ToolSpec;
const HOSTED: ToolSpec = { type: "smithy", description: "Places", schemaS3Uri: "s3://amzn-s3-demo-bucket/models/places.json", allow: ALLOW } as ToolSpec;

function plane(tools: Record<string, ToolSpec>) {
  const app = new cdk.App();
  const stack = new cdk.Stack(app, "T", { env: { account: ACCOUNT, region: "us-east-1" } });
  new ToolPlane(stack, "ToolPlane", {
    agentName: "test_orch", tools, toolApiKeys: {},
    gatewayDiscoveryUrl: "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc/.well-known/openid-configuration",
    gatewayClientId: "client-abc", gatewayAudience: "gateway/invoke", isCognito: true,
    policyEnabled: true, policyMode: "ENFORCE", customPolicies: [], orchRoot: ORCH_ROOT,
  });
  return Template.fromStack(stack);
}
const statements = (t: Template): any[] => Object.values<any>(t.findResources("AWS::IAM::Policy"))
  .flatMap((p) => p.Properties.PolicyDocument.Statement);
const target = (t: Template): any => Object.values<any>(t.findResources("AWS::BedrockAgentCore::GatewayTarget"))[0].Properties;

describe("type=smithy", () => {
  it("sends an inline model as smithyModel.inlinePayload, signed by the Gateway role alone", () => {
    const p = target(plane({ places: INLINE }));
    expect(JSON.parse(p.TargetConfiguration.Mcp.SmithyModel.InlinePayload)).toEqual(MODEL);
    // "do not include the iamCredentialProvider field" for Smithy targets.
    expect(p.CredentialProviderConfigurations).toEqual([{ CredentialProviderType: "GATEWAY_IAM_ROLE" }]);
  });

  it("points a hosted model at its S3 object and may read exactly that object", () => {
    const t = plane({ places: HOSTED });
    expect(target(t).TargetConfiguration.Mcp.SmithyModel.S3.Uri).toBe("s3://amzn-s3-demo-bucket/models/places.json");
    const read = statements(t).find((s) => s.Sid === "ReadSmithyModels");
    expect(read.Action).toBe("s3:GetObject");
    expect(read.Resource).toBe("arn:aws:s3:::amzn-s3-demo-bucket/models/places.json");
  });

  it("grants the Gateway role exactly the tool's allow, and nothing without a smithy tool", () => {
    const calls = statements(plane({ places: INLINE })).find((s) => s.Sid === "SmithyCallsplaces");
    expect(calls).toMatchObject({ Effect: "Allow", Action: "geo:SearchPlaceIndexForText", Resource: ALLOW.resources[0] });
    expect(statements(plane({ places: INLINE })).find((s) => s.Sid === "ReadSmithyModels")).toBeUndefined();
    expect(smithyStatements([["docs", { type: "mcp" }]])).toEqual([]);
  });

  it("permits the target in Cedar like any other tool", () => {
    const policies = Object.values<any>(plane({ places: INLINE }).findResources("AWS::BedrockAgentCore::Policy"));
    // The statement embeds the Gateway ARN, so it is a CFN join rather than a plain string.
    expect(JSON.stringify(policies.map((p) => p.Properties.Definition.Cedar.Statement)))
      .toContain('action in AgentCore::Action::\\"places\\"');
  });

  it("refuses at synth what the Build view refuses", () => {
    const ok = { type: "smithy", schema: MODEL, allow: ALLOW };
    expect(() => validateSmithy("p", ok)).not.toThrow();
    expect(() => validateTools({ p: ok }, {}, "us-east-1")).not.toThrow();
    expect(() => validateSmithy("p", { ...ok, schemaS3Uri: "s3://b/k.json" })).toThrow(/EXACTLY ONE/);
    expect(() => validateSmithy("p", { ...ok, schema: { smithy: "2.0", shapes: { "a#S": { type: "service" } } } }))
      .toThrow(/at least one operation/);
    expect(() => validateSmithy("p", { ...ok, allow: undefined })).toThrow(/allow.actions/);
    expect(() => validateSmithy("p", { ...ok, allow: { ...ALLOW, actions: ["geo:*"] } })).toThrow(/allow.actions/);
    expect(() => validateSmithy("p", { ...ok, allow: { ...ALLOW, actions: ["iam:CreateUser"] } })).toThrow(/may not name iam:CreateUser/);
    expect(() => validateSmithy("p", { ...ok, allow: { ...ALLOW, resources: ["*"] } })).toThrow(/allow.resources/);
    expect(() => validateSmithy("p", { ...ok, allow: { ...ALLOW, resources: ["arn:aws:geo:*:123456789012:place-index/x"] } }))
      .toThrow(/allow.resources/);
  });
});
