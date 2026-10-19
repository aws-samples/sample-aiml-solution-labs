/**
 * The built-in Browser function (source: "browser"), synthesized through the ToolPlane.
 *
 * It opens pages the MODEL chooses, so what is worth pinning is the role: start and stop
 * sessions on the one system browser, connect to an automation stream, write its own log
 * group, and nothing else. Mirrors tests/test_browser_tool.py, which pins the same facts in
 * terraform/browser_tool.tf and terraform/tools.tf.
 */
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import * as path from "path";
import { ToolPlane, ToolSpec } from "../lib/tool-plane";
import { validateTools } from "../lib/orchestrator-stack";

const ORCH_ROOT = path.join(__dirname, "..", "..");

function plane(tools: Record<string, ToolSpec>) {
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
    orchRoot: ORCH_ROOT,
  });
  return Template.fromStack(stack);
}
const asList = (v: any): any[] => (Array.isArray(v) ? v : [v]);

const WEB: ToolSpec = {
  type: "lambda",
  source: "browser",
  description: "Open a web page and read it.",
  call: "open_page",
  arg: "url",
  toolSchema: [{ name: "open_page", properties: { url: { type: "string", required: true } } }],
};
const FN_NAME = "ToolLambda-test_orch-web";
const BROWSER_ARN = "arn:aws:bedrock-agentcore:us-east-1:aws:browser/aws.browser.v1";

describe("the built-in Browser function (source: browser)", () => {
  const template = plane({ web: WEB });
  const fn = (t: Template = template): any =>
    Object.values<any>(t.findResources("AWS::Lambda::Function")).find((f) => f.Properties.FunctionName === FN_NAME);
  const own = (t: Template = template): any[] => {
    const roleId = fn(t).Properties.Role["Fn::GetAtt"][0];
    return Object.values<any>(t.findResources("AWS::IAM::Policy"))
      .filter((p) => (p.Properties.Roles ?? []).some((r: any) => r.Ref === roleId))
      .flatMap((p) => p.Properties.PolicyDocument.Statement);
  };

  it("deploys app/tools/browser/ with room for a 40 s call", () => {
    template.hasResourceProperties("AWS::Lambda::Function", {
      FunctionName: FN_NAME, Handler: "handler.lambda_handler", Runtime: "python3.12", Timeout: 60, MemorySize: 256,
    });
  });

  it("passes no environment without allowDomains, and ALLOW_DOMAINS with it", () => {
    expect(fn().Properties.Environment).toBeUndefined();
    const t = plane({ web: { ...WEB, allowDomains: ["docs.aws.amazon.com", "example.org"] } });
    expect(fn(t).Properties.Environment).toEqual({ Variables: { ALLOW_DOMAINS: "docs.aws.amazon.com,example.org" } });
  });

  it("may start and stop sessions on the system browser and connect to an automation stream, nothing else", () => {
    const st = own();
    const use = st.find((s) => s.Sid === "UseSystemBrowser");
    expect(asList(use.Action).sort()).toEqual(["bedrock-agentcore:StartBrowserSession", "bedrock-agentcore:StopBrowserSession"]);
    expect(asList(use.Resource)).toEqual([BROWSER_ARN]);
    const drive = st.find((s) => s.Sid === "DriveTheSessionItStarted");
    expect(asList(drive.Action)).toEqual(["bedrock-agentcore:ConnectBrowserAutomationStream"]);
    expect(drive.Resource).toBe("*");
    const actions = st.flatMap((s) => asList(s.Action)).map(String);
    expect(actions.filter((a) => !a.startsWith("bedrock-agentcore:")).sort()).toEqual(["logs:CreateLogStream", "logs:PutLogEvents"]);
    expect(actions.some((a) => a.includes("*"))).toBe(false);
    // The only "*" resource is the action IAM gives no resource type.
    expect(st.filter((s) => asList(s.Resource).includes("*")).map((s) => s.Sid)).toEqual(["DriveTheSessionItStarted"]);
    expect(JSON.stringify(st.find((s) => asList(s.Action).includes("logs:PutLogEvents")).Resource)).toContain("ToolLambdaLogGroupweb");
  });

  it("has no managed policy, because the default one can create log groups anywhere", () => {
    const roleId = fn().Properties.Role["Fn::GetAtt"][0];
    expect(template.toJSON().Resources[roleId].Properties.ManagedPolicyArns).toBeUndefined();
  });

  it("is registered as a Gateway target the Gateway may invoke, behind a Cedar permit", () => {
    const target = Object.values<any>(template.findResources("AWS::BedrockAgentCore::GatewayTarget"))
      .find((r) => r.Properties.TargetConfiguration?.Mcp?.Lambda);
    expect(target.Properties.TargetConfiguration.Mcp.Lambda.LambdaArn["Fn::GetAtt"][0]).toMatch(/ToolLambdaweb/);
    const invoke = Object.values<any>(template.findResources("AWS::IAM::Policy"))
      .flatMap((p) => p.Properties.PolicyDocument.Statement).find((s) => s.Sid === "InvokeToolLambdas");
    expect(asList(invoke.Resource)).toHaveLength(1);
    const statement = Object.values<any>(template.findResources("AWS::BedrockAgentCore::Policy"))[0]
      .Properties.Definition.Cedar.Statement;
    const text = (statement["Fn::Join"][1] as any[]).map((p) => (typeof p === "string" ? p : "")).join("");
    expect(text).toContain('action in AgentCore::Action::"web"');
  });

  it("changes nothing for the code interpreter function next to it", () => {
    const both = plane({
      web: WEB,
      sandbox: { type: "lambda", source: "codeInterpreter", call: "execute_code", arg: "code",
        toolSchema: [{ name: "execute_code", properties: { code: { type: "string", required: true } } }] },
    });
    const all = Object.values<any>(both.findResources("AWS::IAM::Policy")).flatMap((p) => p.Properties.PolicyDocument.Statement);
    const actions = all.flatMap((s) => asList(s.Action)).map(String);
    expect(actions.filter((a) => a === "bedrock-agentcore:StartBrowserSession")).toHaveLength(1);
    expect(actions.filter((a) => a === "bedrock-agentcore:InvokeCodeInterpreter")).toHaveLength(1);
  });

  it("adds nothing at all when no tool asks for it", () => {
    const none = plane({ sandbox: { type: "lambda", source: "codeInterpreter", call: "execute_code", arg: "code",
      toolSchema: [{ name: "execute_code", properties: { code: { type: "string", required: true } } }] } });
    const text = JSON.stringify(none.toJSON());
    expect(text).not.toContain("BrowserSession");
    expect(text).not.toContain("aws.browser.v1");
    expect(text).not.toContain("ALLOW_DOMAINS");
  });

  it("is a source the synth accepts, and the refusal for others names it", () => {
    expect(() => validateTools({ web: WEB }, {}, "us-east-1", ORCH_ROOT)).not.toThrow();
    expect(() => validateTools({ web: { ...WEB, source: "chrome" } }, {}, "us-east-1")).toThrow(/"browser"/);
  });
});
