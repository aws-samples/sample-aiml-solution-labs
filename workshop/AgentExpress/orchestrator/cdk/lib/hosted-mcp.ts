/**
 * A hosted MCP server: `tools.<key> = { type: "mcp", hosted: { code?, network? } }`.
 *
 * AgentExpress runs the server on its own AgentCore Runtime (protocol MCP) and registers that
 * Runtime as an MCP server target of the build's Gateway, so agents call its tools like any
 * other Gateway tool (Cedar, interceptors, guardrails and Observability are unchanged). Mirrors
 * terraform/tools_hosted_mcp.tf.
 *
 * Every AWS fact below is from the docs saved in .agents/tasks/hosted-mcp-2026-10-22/docs/:
 *
 * The Runtime (AWS::BedrockAgentCore::Runtime):
 *   ProtocolConfiguration is a String, MCP | HTTP | A2A | AGUI; Ref is the runtime ARN; GetAtt
 *   AgentRuntimeId / AgentRuntimeArn; AgentRuntimeName [a-zA-Z][a-zA-Z0-9_]{0,47}.
 *   https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-bedrockagentcore-runtime.html
 *   A container, not S3 code: the MCP contract says the server "must be deployed as a
 *   containerized application" on ARM64, listening on 0.0.0.0:8000 at /mcp with streamable
 *   HTTP. MCP with CodeConfiguration is not stated in the reference docs (FACTS.md (e) 1).
 *   https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-mcp-protocol-contract.html
 *   Inbound auth stays the default, IAM SigV4 (no AuthorizerConfiguration).
 *   https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-oauth.html
 *
 * The Gateway target (AWS::BedrockAgentCore::GatewayTarget):
 *   TargetConfiguration.Mcp.McpServer.Endpoint (^https://.*), the runtime's URL-encoded ARN in
 *   https://bedrock-agentcore.{region}.amazonaws.com/runtimes/{ARN}/invocations?qualifier=DEFAULT.
 *   https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-mcp.html
 *   https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-properties-bedrockagentcore-gatewaytarget-mcpservertargetconfiguration.html
 *   Outbound GATEWAY_IAM_ROLE with IamCredentialProvider.Service "bedrock-agentcore" ("For MCP
 *   servers hosted on Amazon Bedrock AgentCore, such as the runtime").
 *   https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-building-adding-targets-authorization.html
 *   Mcp-Session-Id allowed as a request and response header (MetadataConfiguration), the tip for
 *   a server on AgentCore Runtime on protocol 2025-11-25 and earlier (the Gateway's version).
 *   https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-target-MCPservers.html
 *   https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-properties-bedrockagentcore-gatewaytarget-metadataconfiguration.html
 *   The gateway role is the principal that invokes the runtime, so it gets
 *   bedrock-agentcore:InvokeAgentRuntime on it (runtime-oauth.html;
 *   https://docs.aws.amazon.com/bedrock-agentcore/latest/APIReference/API_InvokeAgentRuntime.html).
 */
import * as path from "path";
import * as cdk from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import * as ecrAssets from "aws-cdk-lib/aws-ecr-assets";
import { Construct } from "constructs";
import { runtimeNetwork } from "./runtime-network";

/** tools.<key>.hosted. `code` is a code source (Track B's shape) or absent (written in the build). */
export interface HostedMcp {
  code?: { git?: unknown; s3?: unknown; image?: string };
  network?: { mode: string; subnetIds?: string[]; securityGroupIds?: string[] };
}

/** The Runtime's name: AgentRuntimeName allows [a-zA-Z][a-zA-Z0-9_]{0,47}. */
export function hostedMcpRuntimeName(agentName: string, key: string): string {
  const name = `${agentName}_mcp_${key}`;
  if (!/^[a-zA-Z][a-zA-Z0-9_]{0,47}$/.test(name)) {
    throw new Error(`workflow.json tools.${key}.hosted: the runtime name "${name}" must match ` +
      `[a-zA-Z][a-zA-Z0-9_]{0,47} (AgentRuntimeName); use a shorter tool key.`);
  }
  return name;
}

/** A private ECR image URI -> its repository ARN, for the pull grant. */
export function ecrRepositoryArn(uri: string): string {
  const m = /^(\d{12})\.dkr\.ecr\.([a-z0-9-]+)\.amazonaws\.com\/([a-z0-9._\/-]+?)(?::[\w.-]+|@sha256:[0-9a-f]{64})?$/.exec(uri);
  if (!m) throw new Error(`not a private ECR image URI: ${JSON.stringify(uri)}`);
  return `arn:aws:ecr:${m[2]}:${m[1]}:repository/${m[3]}`;
}

/** Builds the hosted server's Runtime and its role; returns the Gateway target endpoint. */
export function hostedMcpRuntime(
  scope: Construct, key: string, hosted: HostedMcp, agentName: string, orchRoot: string,
  gatewayRole: iam.Role,
): { endpoint: string; runtime: cdk.CfnResource } {
  const { region, account, partition } = cdk.Stack.of(scope);
  const role = new iam.Role(scope, `HostedMcpRole-${key}`, {
    roleName: `AgentCoreMcp-${agentName}-${key}`.slice(0, 64),
    assumedBy: new iam.ServicePrincipal("bedrock-agentcore.amazonaws.com", {
      conditions: {
        StringEquals: { "aws:SourceAccount": account },
        ArnLike: { "aws:SourceArn": `arn:aws:bedrock-agentcore:${region}:${account}:*` },
      },
    }),
  });
  // Logs, spans and metrics, as every Runtime of the build gets (orchestrator-stack.ts
  // observabilityPerms). Nothing else: the server's own AWS calls are not in v1.
  role.addToPolicy(new iam.PolicyStatement({
    actions: ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents",
      "logs:DescribeLogStreams", "logs:DescribeLogGroups"],
    resources: [`arn:aws:logs:${region}:${account}:log-group:/aws/bedrock-agentcore/runtimes/*`],
  }));
  role.addToPolicy(new iam.PolicyStatement({
    actions: ["xray:PutTraceSegments", "xray:PutTelemetryRecords", "xray:GetSamplingRules", "xray:GetSamplingTargets"],
    resources: ["*"],
  }));
  role.addToPolicy(new iam.PolicyStatement({
    actions: ["cloudwatch:PutMetricData"], resources: ["*"],
    conditions: { StringEquals: { "cloudwatch:namespace": "bedrock-agentcore" } },
  }));

  let containerUri: string;
  if (typeof hosted.code?.image === "string") {
    // An image the build brings, used as is. AgentCore pulls it as the execution role, with
    // the ECR actions of https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-permissions.html
    // (the same list as terraform/tools_hosted_mcp.tf).
    containerUri = hosted.code.image;
    role.addToPolicy(new iam.PolicyStatement({ actions: ["ecr:GetAuthorizationToken"], resources: ["*"] }));
    role.addToPolicy(new iam.PolicyStatement({
      actions: ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"],
      resources: [ecrRepositoryArn(containerUri)],
    }));
  } else {
    // Written in the build, or fetched by the deployer from git / S3: app/tools/_code/<key>/,
    // with a Dockerfile (the deployer adds the default one when the code has none).
    // Build with finch by exporting CDK_DOCKER=finch, as for the orchestrator image.
    const image = new ecrAssets.DockerImageAsset(scope, `HostedMcpImage-${key}`, {
      directory: path.join(orchRoot, "app", "tools", "_code", key),
      platform: ecrAssets.Platform.LINUX_ARM64,
      exclude: ["__pycache__", ".from-builder"],
    });
    image.repository.grantPull(role);
    containerUri = image.imageUri;
  }

  const runtime = new cdk.CfnResource(scope, `HostedMcp-${key}`, {
    type: "AWS::BedrockAgentCore::Runtime",
    properties: {
      AgentRuntimeName: hostedMcpRuntimeName(agentName, key),
      Description: `Hosted MCP server for tool ${key}`,
      RoleArn: role.roleArn,
      AgentRuntimeArtifact: { ContainerConfiguration: { ContainerUri: containerUri } },
      ProtocolConfiguration: "MCP",
      NetworkConfiguration: runtimeNetwork(hosted.network, `tools.${key}.hosted.network`),
      EnvironmentVariables: { AWS_REGION: region },
    },
  });
  const policy = role.node.tryFindChild("DefaultPolicy");
  if (policy) runtime.node.addDependency(policy);
  runtime.node.addDependency(role);

  const arn = runtime.getAtt("AgentRuntimeArn").toString();
  gatewayRole.addToPolicy(new iam.PolicyStatement({
    sid: `InvokeHostedMcp${key}`,
    actions: ["bedrock-agentcore:InvokeAgentRuntime"],
    resources: [arn, `${arn}/*`],
  }));
  // The ARN, URL-encoded (":" -> %3A, "/" -> %2F): arn:<partition>:bedrock-agentcore:<region>:
  // <account>:runtime/<id> (API_GetAgentRuntime agentRuntimeArn pattern).
  const endpoint = cdk.Fn.join("", [
    `https://bedrock-agentcore.${region}.amazonaws.com/runtimes/arn%3A${partition}%3Abedrock-agentcore%3A`,
    `${region}%3A${account}%3Aruntime%2F`, runtime.getAtt("AgentRuntimeId").toString(),
    "/invocations?qualifier=DEFAULT",
  ]);
  return { endpoint, runtime };
}

/** The Gateway target's MetadataConfiguration for a hosted server (see the header). */
export const HOSTED_MCP_METADATA = {
  allowedRequestHeaders: ["Mcp-Session-Id"],
  allowedResponseHeaders: ["Mcp-Session-Id"],
};
