/**
 * cdk-nag's AwsSolutions pack over the synthesized stack: no finding that lib/nag.ts has
 * not acknowledged with a reason. A new resource that trips a rule fails here until it is
 * fixed or acknowledged there.
 */
import * as cdk from "aws-cdk-lib";
import { Annotations, Match } from "aws-cdk-lib/assertions";
import { AwsSolutionsChecks } from "cdk-nag";
import { acknowledgeNagFindings, NAG_ACKNOWLEDGED } from "../lib/nag";
import { OrchestratorStack } from "../lib/orchestrator-stack";

function nagged(overrides: Record<string, any> = {}) {
  const app = new cdk.App();
  const stack = new OrchestratorStack(app, "NagStack", {
    env: { account: "123456789012", region: "us-east-1" },
    agentName: "multiagent_orchestrator",
    modelId: "us.anthropic.claude-haiku-4-5-20251001-v1:0",
    memoryEventExpiryDays: 30, idp: "cognito", createCognito: true,
    cognitoUserPoolId: "", cognitoClientId: "", cognitoDomainPrefix: "",
    auth0Domain: "", auth0ClientId: "", enableGateway: true, gatewayClientId: "",
    gatewayClientSecret: "dummy", gatewayAudience: "", toolApiKeys: {}, a2aTokens: {},
    transactionSearchIndexingPercentage: 100,
    ...overrides,
  } as any);
  acknowledgeNagFindings(stack);
  cdk.Aspects.of(app).add(new AwsSolutionsChecks());
  app.synth();
  return Annotations.fromStack(stack);
}

describe("cdk-nag AwsSolutions", () => {
  it("has no unacknowledged finding on a console", () => {
    const errors = nagged().findError("*", Match.stringLikeRegexp("AwsSolutions-.*"));
    expect(errors.map((e) => `${e.id}: ${String(e.entry.data).split("\n")[0]}`)).toEqual([]);
  });
  it("has no unacknowledged finding on a build (no Builder plane)", () => {
    const errors = nagged({ agentName: "ax_0123abcd", builder: false })
      .findError("*", Match.stringLikeRegexp("AwsSolutions-.*"));
    expect(errors.map((e) => `${e.id}: ${String(e.entry.data).split("\n")[0]}`)).toEqual([]);
  });
  it("gives every acknowledgement a reason", () => {
    for (const a of NAG_ACKNOWLEDGED) expect(a.reason.length).toBeGreaterThan(40);
  });
});
