/**
 * Extra tags for every build a console deploys (lib/build-tags.ts, `-c buildTags`): parsed
 * and checked at synth, and passed to the deploy project as BUILD_EXTRA_TAGS. Unset, the
 * project gets nothing new. The Terraform half is terraform/tests/build_tags.tftest.hcl and
 * the runner's is tests/test_build_tags.py, which also holds the three to the same rules.
 */
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import { aws_kms as kms, aws_logs as logs } from "aws-cdk-lib";
import * as fs from "fs";
import * as path from "path";
import { BuilderPlane } from "../lib/builder-plane";
import { buildTagsEnv, buildTagsOf, MAX_BUILD_TAGS } from "../lib/build-tags";
import { OrchestratorStack } from "../lib/orchestrator-stack";

const ORCH_ROOT = path.join(__dirname, "..", "..");

const many = (n: number) => Object.fromEntries(Array.from({ length: n }, (_, i) => [`k${String(i).padStart(2, "0")}`, "v"]));

/** The deploy project's environment variables, by name: the one project that has BUILDS_TABLE. */
function projectEnv(t: Template): Record<string, string> {
  const envs = Object.values<any>(t.findResources("AWS::CodeBuild::Project"))
    .map((p) => Object.fromEntries((p.Properties.Environment.EnvironmentVariables ?? [])
      .map((v: any) => [v.Name, v.Value])) as Record<string, string>)
    .filter((e) => "BUILDS_TABLE" in e);
  expect(envs).toHaveLength(1);
  return envs[0];
}

function plane(buildTags?: Record<string, string>): Template {
  const stack = new cdk.Stack(new cdk.App(), "PlaneStack", { env: { account: "123456789012", region: "us-east-1" } });
  const dataKey = new kms.Key(stack, "DataKey");
  new BuilderPlane(stack, "Builder", {
    agentName: "console", orchRoot: ORCH_ROOT, logRetention: logs.RetentionDays.ONE_WEEK, dataKey, buildTags,
  });
  return Template.fromStack(stack);
}

function consoleStack(overrides: Record<string, any>): OrchestratorStack {
  return new OrchestratorStack(new cdk.App(), "TestStack", {
    env: { account: "123456789012", region: "us-east-1" },
    agentName: "multiagent_orchestrator", modelId: "", memoryEventExpiryDays: 30,
    idp: "cognito", createCognito: true, cognitoUserPoolId: "", cognitoClientId: "",
    cognitoDomainPrefix: "", auth0Domain: "", auth0ClientId: "", enableGateway: true,
    gatewayClientId: "", gatewayClientSecret: "", gatewayAudience: "", toolApiKeys: {},
    a2aTokens: {}, transactionSearchIndexingPercentage: 100, consoleMode: "builder", ...overrides,
  });
}

describe("buildTagsOf", () => {
  it("is no tags when unset, empty or {}", () => {
    expect(buildTagsOf(undefined)).toEqual({});
    expect(buildTagsOf("")).toEqual({});
    expect(buildTagsOf("{}")).toEqual({});
  });

  it("reads a JSON string (-c) and an object (cdk.json) the same way", () => {
    const tags = { "cost-center": "1234", team: "data platform", path: "a/b=c+d@e:f_g.h-i", empty: "" };
    expect(buildTagsOf(JSON.stringify(tags))).toEqual(tags);
    expect(buildTagsOf(tags)).toEqual(tags);
  });

  it("turns numbers and booleans into strings, as Terraform's map(string) does", () => {
    expect(buildTagsOf('{"n":1234,"b":true}')).toEqual({ n: "1234", b: "true" });
  });

  it(`accepts exactly ${MAX_BUILD_TAGS} tags`, () => {
    expect(Object.keys(buildTagsOf(many(40)))).toHaveLength(40);
  });

  it.each([
    ["invalid JSON", "{oops", /must be a JSON object of tag key -> value/],
    ["a list", "[]", /must be a JSON object of tag key -> value/],
    ["a string", '"x"', /must be a JSON object of tag key -> value/],
    ["a null value", '{"k":null}', /buildTags\["k"\] must be a string, number or boolean/],
    ["an object value", '{"k":{"x":1}}', /buildTags\["k"\] must be a string, number or boolean/],
    ["aws:", '{"aws:x":"y"}', /buildTags key "aws:x" is reserved/],
    ["AWS: in upper case", '{"AWS:x":"y"}', /buildTags key "AWS:x" is reserved/],
    ["agentexpress:", '{"agentexpress:build":"y"}', /is reserved: keys starting with "aws:" or "agentexpress:"/],
    ["AgentExpress: in mixed case", '{"AgentExpress:x":"y"}', /is reserved/],
    ["an empty key", '{"":"v"}', /buildTags key "" must be 1 to 128 characters/],
    ["a 129-character key", JSON.stringify({ ["k".repeat(129)]: "v" }), /must be 1 to 128 characters/],
    ["a 257-character value", JSON.stringify({ k: "v".repeat(257) }), /buildTags\["k"\] must be at most 256 characters/],
    ["a # in a key", '{"a#b":"v"}', /buildTags key "a#b" may use only letters A-Z a-z, digits, spaces and/],
    ["a < in a value", '{"k":"a<b"}', /buildTags\["k"\] may use only letters/],
    ["a non-ASCII key", '{"équipe":"v"}', /may use only letters A-Z a-z/],
    ["keys differing only in case", '{"Team":"a","team":"b"}', /keys "Team" and "team" differ only in case/],
    ["41 tags", JSON.stringify(many(41)), /buildTags has 41 tags; at most 40 are allowed/],
  ])("refuses %s", (_name, raw, message) => {
    expect(() => buildTagsOf(raw)).toThrow(message);
  });
});

describe("buildTagsEnv", () => {
  it("is nothing for no tags", () => {
    expect(buildTagsEnv({})).toEqual({});
  });

  it("is compact JSON with sorted keys, the bytes Terraform's jsonencode writes", () => {
    expect(buildTagsEnv({ team: "a", "cost-center": "1" })).toEqual({ BUILD_EXTRA_TAGS: '{"cost-center":"1","team":"a"}' });
  });
});

describe("the deploy project", () => {
  it("gets no BUILD_EXTRA_TAGS when unset", () => {
    expect(projectEnv(plane())).not.toHaveProperty("BUILD_EXTRA_TAGS");
    expect(projectEnv(plane({}))).not.toHaveProperty("BUILD_EXTRA_TAGS");
  });

  it("passes the tags on as sorted JSON", () => {
    const env = projectEnv(plane({ team: "data platform", "cost-center": "1234" }));
    expect(env.BUILD_EXTRA_TAGS).toBe('{"cost-center":"1234","team":"data platform"}');
  });

  it("is reached from -c buildTags on a console stack", () => {
    const t = Template.fromStack(consoleStack({ buildTags: '{"team":"data"}' }));
    expect(projectEnv(t).BUILD_EXTRA_TAGS).toBe('{"team":"data"}');
  });

  it("refuses an invalid -c buildTags when the stack is made", () => {
    expect(() => consoleStack({ buildTags: '{"aws:x":"y"}' })).toThrow(/reserved/);
  });

  it("reads buildTags from context in bin/orchestrator.ts", () => {
    const bin = fs.readFileSync(path.join(__dirname, "..", "bin", "orchestrator.ts"), "utf8");
    expect(bin).toContain('ctx("buildTags", "")');
  });
});
