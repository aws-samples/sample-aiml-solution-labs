/**
 * A hosted MCP server (tools.<key>.hosted, lib/hosted-mcp.ts): its own AgentCore Runtime with
 * protocol MCP, registered as an MCP server target of the Gateway with SigV4 as the gateway role.
 */
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import { ToolPlane, ToolSpec } from "../lib/tool-plane";
import { validateTools } from "../lib/orchestrator-stack";
import { ecrRepositoryArn, hostedMcpRuntimeName } from "../lib/hosted-mcp";

const ACCOUNT = "123456789012";
const IMAGE = `${ACCOUNT}.dkr.ecr.us-east-1.amazonaws.com/team/orders-mcp:1.2`;

function plane(tools: Record<string, ToolSpec>, orchRoot = path.join(__dirname, "..", "..")) {
  const app = new cdk.App();
  const stack = new cdk.Stack(app, "T", { env: { account: ACCOUNT, region: "us-east-1" } });
  new ToolPlane(stack, "ToolPlane", {
    agentName: "test_orch", tools, toolApiKeys: {},
    gatewayDiscoveryUrl: "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc/.well-known/openid-configuration",
    gatewayClientId: "client-abc", gatewayAudience: "gateway/invoke", isCognito: true,
    policyEnabled: true, policyMode: "ENFORCE", customPolicies: [], orchRoot,
  } as any);
  return Template.fromStack(stack);
}

const only = (t: Template, type: string) => Object.values<any>(t.findResources(type));

describe("hosted MCP server", () => {
  test("an image the build brings: an MCP Runtime and a SigV4 MCP server target", () => {
    const t = plane({ orders: { type: "mcp", description: "Orders", hosted: { code: { image: IMAGE } } } as ToolSpec });
    const [rt] = only(t, "AWS::BedrockAgentCore::Runtime");
    expect(rt.Properties.ProtocolConfiguration).toBe("MCP");
    expect(rt.Properties.AgentRuntimeName).toBe("test_orch_mcp_orders");
    expect(rt.Properties.AgentRuntimeArtifact).toEqual({ ContainerConfiguration: { ContainerUri: IMAGE } });
    expect(rt.Properties.NetworkConfiguration).toEqual({ NetworkMode: "PUBLIC" });
    expect(rt.Properties.AuthorizerConfiguration).toBeUndefined();     // IAM SigV4 inbound

    const target = only(t, "AWS::BedrockAgentCore::GatewayTarget").find((r) => r.Properties.Name === "orders");
    const server = target.Properties.TargetConfiguration.Mcp.McpServer;
    const joined = JSON.stringify(server.Endpoint);
    expect(joined).toContain("https://bedrock-agentcore.us-east-1.amazonaws.com/runtimes/arn%3A");
    expect(joined).toContain("%3Aruntime%2F");
    expect(joined).toContain("/invocations?qualifier=DEFAULT");
    expect(target.Properties.CredentialProviderConfigurations).toEqual([{
      CredentialProviderType: "GATEWAY_IAM_ROLE",
      CredentialProvider: { IamCredentialProvider: { Service: "bedrock-agentcore", Region: "us-east-1" } },
    }]);
    expect(target.Properties.MetadataConfiguration).toEqual({
      AllowedRequestHeaders: ["Mcp-Session-Id"], AllowedResponseHeaders: ["Mcp-Session-Id"],
    });
    expect(target.DependsOn.some((d: string) => d.startsWith("ToolPlaneHostedMcporders"))).toBe(true);

    const stmts = only(t, "AWS::IAM::Policy").flatMap((p) => p.Properties.PolicyDocument.Statement);
    const invoke = stmts.find((s) => s.Sid === "InvokeHostedMcporders");
    expect(invoke.Action).toBe("bedrock-agentcore:InvokeAgentRuntime");
    const pull = stmts.find((s) => JSON.stringify(s.Action).includes("ecr:BatchGetImage"));
    expect(pull.Resource).toBe(`arn:aws:ecr:us-east-1:${ACCOUNT}:repository/team/orders-mcp`);
  });

  test("code written in the build is an ARM64 image asset from app/tools/_code/<key>/, in a VPC", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "hosted-mcp-"));
    try {
      const dir = path.join(root, "app", "tools", "_code", "orders");
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(dir, "Dockerfile"), "FROM scratch\n");
      fs.mkdirSync(path.join(root, "app", "tools", "_code"), { recursive: true });
      const network = { mode: "VPC", subnetIds: ["subnet-0123456789abcdef0"], securityGroupIds: ["sg-0123456789abcdef0"] };
      const t = plane({ orders: { type: "mcp", hosted: { network } } as ToolSpec }, root);
      const [rt] = only(t, "AWS::BedrockAgentCore::Runtime");
      expect(JSON.stringify(rt.Properties.AgentRuntimeArtifact.ContainerConfiguration.ContainerUri)).toContain("cdk-hnb659fds-container-assets");
      expect(rt.Properties.NetworkConfiguration).toEqual({
        NetworkMode: "VPC",
        NetworkModeConfig: { Subnets: network.subnetIds, SecurityGroups: network.securityGroupIds },
      });
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test("the validator refuses an endpoint, another auth, unknown keys or a non-ECR image", () => {
    const v = (t: any) => () => validateTools({ x: t }, {}, "us-east-1", path.join(__dirname, "..", ".."));
    expect(v({ type: "mcp", hosted: {} })).not.toThrow();
    expect(v({ type: "mcp", hosted: { code: { image: IMAGE } } })).not.toThrow();
    expect(v({ type: "mcp", hosted: {}, endpoint: "https://x.test/mcp" })).toThrow(/hosted/);
    expect(v({ type: "mcp", hosted: {}, auth: "apikey" })).toThrow(/hosted/);
    expect(v({ type: "mcp", hosted: { port: 8000 } })).toThrow(/hosted/);
    expect(v({ type: "mcp", hosted: { code: { image: "docker.io/library/x:1" } } })).toThrow(/hosted/);
    expect(v({ type: "openapi", hosted: {} })).toThrow(/hosted/);
  });

  test("names: the Runtime name rule and the ECR repository ARN", () => {
    expect(() => hostedMcpRuntimeName("ax_12345678", "a".repeat(40))).toThrow(/AgentRuntimeName/);
    expect(ecrRepositoryArn(`${ACCOUNT}.dkr.ecr.eu-west-1.amazonaws.com/r@sha256:${"a".repeat(64)}`))
      .toBe(`arn:aws:ecr:eu-west-1:${ACCOUNT}:repository/r`);
    expect(() => ecrRepositoryArn("public.ecr.aws/x/y:1")).toThrow(/ECR/);
  });
});
