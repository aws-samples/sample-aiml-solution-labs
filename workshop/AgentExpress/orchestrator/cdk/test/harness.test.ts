/**
 * AgentCore Harness agents (runtime "harness", lib/harness.ts): one harness and one role per
 * harness agent, built from its entry, memory off, no tool unless `harness.shell`; the
 * orchestrator may invoke them and gets HARNESS_ARNS; the shipped workflow gets nothing new;
 * a harness agent that cannot be built fails the synth. Same as terraform/harness.tf.
 */
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import * as fs from "fs";
import * as path from "path";
import { BUILTIN_TOOLS, NO_TOOLS, validateHarness } from "../lib/harness";
import { OrchestratorStack } from "../lib/orchestrator-stack";

const ORCH_ROOT = path.join(__dirname, "..", "..");
const shipped = require(`${ORCH_ROOT}/app/workflow.json`);

function synth(workflow: any): Template {
  const app = new cdk.App();
  const stack = new OrchestratorStack(app, "TestStack", {
    env: { account: "123456789012", region: "eu-west-1" },
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

const withAgents = (agents: Record<string, any>) => ({
  ...shipped,
  agents: { ...shipped.agents, ...agents },
  steps: [...shipped.steps, ...Object.keys(agents).map((id) => ({ agent: id }))],
});
const WRITER = { writer: { name: "Writer", runtime: "harness", produces: "draft", instructions: "Write the draft.",
  topP: 0.9, harness: { maxIterations: 8, timeoutSeconds: 300 } } };
const CODER = { coder: { name: "Coder", runtime: "harness", instructions: "Crunch it.", model: "us.amazon.nova-pro-v1:0",
  maxTokens: 1500, harness: { shell: true } } };
const statementsOf = (t: Template, prefix: string) => Object.entries<any>(t.findResources("AWS::IAM::Policy"))
  .filter(([k]) => k.startsWith(prefix)).flatMap(([, p]) => p.Properties.PolicyDocument.Statement as any[]);
const orchestratorEnv = (t: Template) => Object.values<any>(t.findResources("AWS::BedrockAgentCore::Runtime"))
  .find((r) => r.Properties.AgentRuntimeName === "multiagent_orchestrator").Properties.EnvironmentVariables;

describe("harness agents", () => {
  it("gives the shipped workflow nothing new", () => {
    const t = synth(shipped);
    t.resourceCountIs("AWS::BedrockAgentCore::Harness", 0);
    expect(orchestratorEnv(t).HARNESS_ARNS).toBeUndefined();
    expect(JSON.stringify(statementsOf(t, "RuntimeRole"))).not.toContain("InvokeHarness");
  });

  it("builds each harness from its entry, with memory off and only the tools it may use", () => {
    const t = synth(withAgents({ ...WRITER, ...CODER }));
    const harnesses = Object.values<any>(t.findResources("AWS::BedrockAgentCore::Harness")).map((r) => r.Properties);
    const writer = harnesses.find((h) => h.HarnessName === "multiagent_orchestrator_writer");
    const coder = harnesses.find((h) => h.HarnessName === "multiagent_orchestrator_coder");
    expect(writer.SystemPrompt).toEqual([{ Text: "Write the draft." }]);
    // The deployment default, in this region's geography; topP alone leaves the temperature out.
    expect(writer.Model.BedrockModelConfig).toEqual({ ModelId: "eu.anthropic.claude-haiku-4-5-20251001-v1:0", MaxTokens: 4000, TopP: 0.9 });
    expect(writer.Memory).toEqual({ Disabled: {} });
    expect(writer.AllowedTools).toEqual([NO_TOOLS]);
    expect([writer.MaxIterations, writer.TimeoutSeconds]).toEqual([8, 300]);
    expect(coder.Model.BedrockModelConfig).toEqual({ ModelId: "eu.amazon.nova-pro-v1:0", MaxTokens: 1500, Temperature: 0 });
    expect(coder.AllowedTools).toEqual([BUILTIN_TOOLS]);
    expect(coder.MaxIterations).toBeUndefined();
  });

  it("gives each harness its own narrow role and the orchestrator InvokeHarness on them alone", () => {
    const t = synth(withAgents(WRITER));
    const role = Object.values<any>(t.findResources("AWS::IAM::Role"))
      .find((r) => r.Properties.RoleName === "AgentCoreHarness-multiagent_orchestrator-writer");
    expect(role.Properties.AssumeRolePolicyDocument.Statement[0].Principal.Service).toBe("bedrock-agentcore.amazonaws.com");
    const sids = statementsOf(t, "HarnessRolewriter").map((s) => s.Sid);
    expect(sids).toEqual(["HarnessModelInvocation", "HarnessEcrPublicPull", "HarnessLogGroup", "HarnessDescribeLogGroups",
      "HarnessLogStream", "HarnessSpanDelivery", "HarnessTracing", "HarnessMetrics", "HarnessWorkloadIdentity"]);
    const invoke = statementsOf(t, "RuntimeRole").find((s) => s.Sid === "InvokeHarnessAgents");
    expect(invoke.Action).toEqual(["bedrock-agentcore:InvokeHarness", "bedrock-agentcore:InvokeAgentRuntime"]);
    expect(JSON.stringify(invoke.Resource)).toContain("Harnesswriter");
    expect(JSON.stringify(orchestratorEnv(t).HARNESS_ARNS)).toContain("Harnesswriter");
  });

  it("refuses at synth a harness agent that cannot be built", () => {
    const bad = (a: any, name = "app", env: any = {}) => () =>
      validateHarness({ ...withAgents({ writer: { name: "W", runtime: "harness", ...a } }), memories: { notes: { strategies: ["semantic"] } } }, name, env);
    expect(bad({})).toThrow(/harness agent\(s\) writer: a harness agent needs `instructions`/);
    expect(bad({ instructions: "x", harness: { timeoutSeconds: 901 } })).toThrow(/timeoutSeconds 1-900/);
    expect(bad({ instructions: "x" }, "a_very_long_deployment_name_for_this")).toThrow(/at most 40 letters/);
    // Gateway tools: the Gateway, Cognito's machine client, and tools that are the build's.
    expect(bad({ instructions: "x", tool: "docs" })).toThrow(/`tool` needs the Gateway/);
    expect(bad({ instructions: "x", tool: "docs" }, "app", { gatewayFlow: "auth0", tools: { docs: {} } })).toThrow(/Cognito machine client/);
    expect(bad({ instructions: "x", tool: "docs" }, "app", { gatewayFlow: "cognito", tools: { docs: { auth: "user" } } })).toThrow(/act as the person/);
    expect(bad({ instructions: "x", tool: "docs" }, "app", { gatewayFlow: "cognito", tools: { docs: {} } })).not.toThrow();
    expect(bad({ instructions: "x", harness: { memory: "nope" } })).toThrow(/harness.memory must name one of `memories`/);
    expect(bad({ instructions: "x", model: "gpt-5", harness: { provider: "openai", apiKey: "oa" } })).toThrow(/IDENTITY_SECRETS/);
    expect(bad({ instructions: "x", model: "gpt-5", harness: { provider: "openai", apiKey: "oa" } }, "app", { identitySecrets: { oa: "k" } })).not.toThrow();
    // A harness you bring: this account and region only.
    const byo = (arn: string) => bad({ harness: { arn } }, "app", { region: "eu-west-1", account: "123456789012" });
    expect(byo("arn:aws:bedrock-agentcore:eu-west-1:999999999999:harness/h-AbCdEf1234")).toThrow(/this account \(123456789012\)/);
    expect(byo("arn:aws:bedrock-agentcore:us-east-1:123456789012:harness/h-AbCdEf1234")).toThrow(/another account or region/);
    expect(byo("arn:aws:bedrock-agentcore:eu-west-1:123456789012:harness/h-AbCdEf1234")).not.toThrow();
    expect(bad({ instructions: "x" })).not.toThrow();
  });

  it("gives a harness agent its Gateway tools through an OAuth2 credential provider of the agents' machine client", () => {
    const t = synth(withAgents({ writer: { name: "Writer", runtime: "harness", instructions: "Write.", tool: ["docs", "pricing"] } }));
    const h = Object.values<any>(t.findResources("AWS::BedrockAgentCore::Harness"))[0].Properties;
    expect(h.Tools).toHaveLength(1);
    expect(h.Tools[0]).toMatchObject({ Type: "agentcore_gateway", Name: "gateway" });
    const gw = h.Tools[0].Config.AgentCoreGateway;
    expect(JSON.stringify(gw.GatewayArn)).toContain("Gateway");
    expect(gw.OutboundAuth.Oauth).toMatchObject({ Scopes: ["gateway/invoke"], GrantType: "CLIENT_CREDENTIALS" });
    expect(h.AllowedTools).toEqual(["@gateway/docs___*", "@gateway/pricing___*"]);
    const providers = Object.values<any>(t.findResources("AWS::BedrockAgentCore::OAuth2CredentialProvider"))
      .map((p) => p.Properties).filter((p) => p.Name === "bedrock-agentcore-multiagent-orchestrator-hgw-writer");
    expect(providers).toHaveLength(1);
    const cfg = providers[0].Oauth2ProviderConfigInput.CustomOauth2ProviderConfig;
    // The shared machine client the other agents use (gatewayIdentity "shared").
    expect(JSON.stringify(cfg.ClientId)).toContain("M2mClient");
    expect(cfg.OauthDiscovery.AuthorizationServerMetadata.TokenEndpoint).toBeDefined();
    const sids = statementsOf(t, "HarnessRolewriter").map((s) => s.Sid);
    expect(sids.slice(9)).toEqual(["HarnessGatewayToken", "HarnessGatewaySecret"]);
    // With gatewayIdentity "perAgent" it signs in as itself, so Cedar sees which agent called.
    const own = synth({ ...withAgents({ writer: { name: "Writer", runtime: "harness", instructions: "Write.", tool: "docs" } }),
      orchestrator: { ...shipped.orchestrator, gatewayIdentity: "perAgent" } });
    const mine = Object.values<any>(own.findResources("AWS::BedrockAgentCore::OAuth2CredentialProvider"))
      .map((p) => p.Properties).find((p) => p.Name === "bedrock-agentcore-multiagent-orchestrator-hgw-writer");
    expect(JSON.stringify(mine.Oauth2ProviderConfigInput.CustomOauth2ProviderConfig.ClientId)).toContain("AgentClientwriter");
  });

  it("builds every harness setting both paths can express, each with only its own grant", () => {
    const wf = {
      ...withAgents({ writer: { name: "Writer", runtime: "harness", instructions: "Write.", model: "gpt-5.4", maxTokens: 1000,
        harness: {
          provider: "openai", apiKey: "oa", shell: true, browser: true,
          codeInterpreter: "arn:aws:bedrock-agentcore:eu-west-1:123456789012:code-interpreter-custom/my_ci-AbCdEf1234",
          mcp: [{ name: "wiki", url: "https://wiki.example.com/mcp" }],
          skills: [{ aws: [] }, { git: "https://github.com/org/skills", path: "x", credential: "gh" }, { s3: "s3://skills-bkt/style/" }],
          memory: "notes", container: "123456789012.dkr.ecr.eu-west-1.amazonaws.com/team/env:v1",
          mounts: [{ session: "/mnt/data" }, { efs: "arn:aws:elasticfilesystem:eu-west-1:123456789012:access-point/fsap-0123456789abcdef0", path: "/mnt/efs" }],
          network: { subnetIds: ["subnet-0123456789abcdef0"], securityGroupIds: ["sg-0123456789abcdef0"] },
          endpoint: "live" } } }),
      memories: { notes: { strategies: ["semantic"] } },
      identities: { oa: { type: "apikey" }, gh: { type: "apikey" } },
    };
    const app = new cdk.App();
    const stack = new OrchestratorStack(app, "TestStack", {
      env: { account: "123456789012", region: "eu-west-1" }, agentName: "multiagent_orchestrator",
      modelId: "us.anthropic.claude-haiku-4-5-20251001-v1:0", memoryEventExpiryDays: 30,
      idp: "cognito", createCognito: true, cognitoUserPoolId: "", cognitoClientId: "", cognitoDomainPrefix: "", auth0Domain: "",
      auth0ClientId: "", enableGateway: true, gatewayClientId: "", gatewayClientSecret: "dummy", gatewayAudience: "",
      toolApiKeys: {}, a2aTokens: {}, transactionSearchIndexingPercentage: 100, identitySecrets: { oa: "sk-1", gh: "ghp-1" }, workflow: wf,
    } as any);
    const t = Template.fromStack(stack);
    const h = Object.values<any>(t.findResources("AWS::BedrockAgentCore::Harness"))[0].Properties;
    expect(h.Model.OpenAiModelConfig).toMatchObject({ ModelId: "gpt-5.4", MaxTokens: 1000 });
    expect(h.Model.OpenAiModelConfig.Temperature).toBeUndefined();
    expect(h.Tools.map((x: any) => x.Type)).toEqual(["remote_mcp", "agentcore_browser", "agentcore_code_interpreter"]);
    expect(h.Tools[0]).toEqual({ Type: "remote_mcp", Name: "wiki", Config: { RemoteMcp: { Url: "https://wiki.example.com/mcp" } } });
    expect(h.AllowedTools).toEqual([BUILTIN_TOOLS, "@wiki", "browser", "@browser", "code_interpreter", "@code_interpreter", "skills", "@builtin/skills"]);
    expect(h.Skills[0]).toEqual({ AwsSkills: {} });
    expect(h.Skills[1].Git).toMatchObject({ Url: "https://github.com/org/skills", Path: "x" });
    expect(h.Skills[2]).toEqual({ S3: { Uri: "s3://skills-bkt/style/" } });
    expect(JSON.stringify(h.Memory.AgentCoreMemoryConfiguration.Arn)).toContain("Memorynotes");
    expect(h.EnvironmentArtifact).toEqual({ ContainerConfiguration: { ContainerUri: "123456789012.dkr.ecr.eu-west-1.amazonaws.com/team/env:v1" } });
    const env = h.Environment.AgentCoreRuntimeEnvironment;
    expect(env.NetworkConfiguration).toEqual({ NetworkMode: "VPC", NetworkModeConfig: { SecurityGroups: ["sg-0123456789abcdef0"], Subnets: ["subnet-0123456789abcdef0"] } });
    expect(env.FilesystemConfigurations[0]).toEqual({ SessionStorage: { MountPath: "/mnt/data" } });
    // Keys made from the build's secrets, never in the template's harness.
    const keys = Object.values<any>(t.findResources("AWS::BedrockAgentCore::ApiKeyCredentialProvider")).map((p) => p.Properties.Name);
    expect(keys).toEqual(expect.arrayContaining(["bedrock-agentcore-multiagent-orchestrator-hk-oa", "bedrock-agentcore-multiagent-orchestrator-hk-gh"]));
    expect(JSON.stringify(h)).not.toContain("sk-1");
    // The endpoint follows the harness's version.
    const ep = Object.values<any>(t.findResources("AWS::BedrockAgentCore::HarnessEndpoint"))[0].Properties;
    expect(ep.EndpointName).toBe("live");
    expect(JSON.stringify(ep.TargetVersion)).toContain("Version");
    const statements = statementsOf(t, "HarnessRolewriter");
    expect(statements.map((s) => s.Sid).slice(9)).toEqual(["HarnessApiKeys", "HarnessApiKeySecrets", "HarnessBrowser",
      "HarnessCodeInterpreter", "HarnessMemory", "HarnessSkillObjects", "HarnessSkillBuckets", "HarnessContainerPull",
      "HarnessVpcImagePull", "HarnessEcrToken", "HarnessEfsMounts"]);
    const by = (sid: string) => statements.find((s) => s.Sid === sid);
    expect(by("HarnessContainerPull").Resource).toBe("arn:aws:ecr:eu-west-1:123456789012:repository/team/env");
    expect(by("HarnessSkillObjects").Resource).toBe("arn:aws:s3:::skills-bkt/style/*");
    expect(by("HarnessEfsMounts").Condition).toEqual({ StringEquals: {
      "elasticfilesystem:AccessPointArn": ["arn:aws:elasticfilesystem:eu-west-1:123456789012:access-point/fsap-0123456789abcdef0"] } });
    expect(by("HarnessBrowser").Resource).toBe("arn:aws:bedrock-agentcore:eu-west-1:aws:browser/*");
    const invoke = statementsOf(t, "RuntimeRole").find((s) => s.Sid === "InvokeHarnessAgents");
    expect(JSON.stringify(invoke.Resource)).toContain("/harness-endpoint/live");
  });

  it("creates nothing for a harness you bring, and lets the orchestrator invoke exactly it", () => {
    const arn = "arn:aws:bedrock-agentcore:eu-west-1:123456789012:harness/research_agent-AbCdEf1234";
    const t = synth(withAgents({ writer: { name: "Writer", runtime: "harness", harness: { arn, endpoint: "prod" } } }));
    t.resourceCountIs("AWS::BedrockAgentCore::Harness", 0);
    expect(Object.values<any>(t.findResources("AWS::IAM::Role")).some((r) => String(r.Properties.RoleName).startsWith("AgentCoreHarness-"))).toBe(false);
    const invoke = statementsOf(t, "RuntimeRole").find((s) => s.Sid === "InvokeHarnessAgents");
    expect(invoke.Resource).toEqual([arn, `${arn}/harness-endpoint/prod`]);
    expect(orchestratorEnv(t).HARNESS_ARNS).toBeUndefined();
  });

  it("matches the Terraform path", () => {
    const tf = fs.readFileSync(path.join(ORCH_ROOT, "terraform", "harness.tf"), "utf8");
    for (const s of [`"${NO_TOOLS}"`, `"${BUILTIN_TOOLS}"`, 'dynamic "disabled"', 'harness_name       = local.harness_names[each.key]',
      '"AgentCoreHarness-${var.agent_name}-${id}"', '"${var.agent_name}_${id}"', 'Sid      = "InvokeHarnessAgents"',
      '"bedrock-agentcore:InvokeHarness", "bedrock-agentcore:InvokeAgentRuntime"', "harness.timeoutSeconds 1-900",
      '"@gateway/${k}___*"', '["browser", "@browser"]', '["code_interpreter", "@code_interpreter"]', '["skills", "@builtin/skills"]',
      'name                       = "bedrock-agentcore-${local.harness_dash}-hgw-${each.key}"', 'grant_type   = "CLIENT_CREDENTIALS"',
      'name       = "bedrock-agentcore-${local.harness_dash}-hk-${each.key}"', 'type = "remote_mcp"', 'type = "agentcore_browser"',
      'type = "agentcore_code_interpreter"', 'network_mode = "VPC"', "/harness-endpoint/", "replace_triggered_by"]) {
      expect(tf).toContain(s);
    }
    // The extra statements in the same order.
    const tfSids = [...tf.matchAll(/Sid\s+= "(Harness\w+)"/g)].map((m) => m[1]);
    const ts = fs.readFileSync(path.join(ORCH_ROOT, "cdk", "lib", "harness.ts"), "utf8");
    const tsSids = [...ts.matchAll(/(?:sid: |statement\()"(Harness\w+)"/g)].map((m) => m[1]);
    // harness.tf declares the optional statements (a local) above the base policy.
    expect(tfSids).toEqual([...tsSids.slice(9), ...tsSids.slice(0, 9)]);
  });
});
