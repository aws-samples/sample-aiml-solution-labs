/**
 * An AgentCore Runtime's network (lib/runtime-network.ts): `orchestrator.network` places the
 * shared orchestrator Runtime and `agents.<id>.network` a dedicated agent's own Runtime. Absent,
 * every Runtime keeps { NetworkMode: "PUBLIC" }, as before the setting. Same cases as
 * tests/test_runtime_network.py on the Terraform path (terraform/runtime_network.tf).
 */
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import * as fs from "fs";
import * as path from "path";
import { OrchestratorStack } from "../lib/orchestrator-stack";
import { runtimeNetwork, validateAgentNetworks } from "../lib/runtime-network";

const ORCH_ROOT = path.join(__dirname, "..", "..");
const shipped = require(`${ORCH_ROOT}/app/workflow.json`);
const SUBNETS = ["subnet-0123456789abcdef0", "subnet-0fedcba9876543210"];
const GROUPS = ["sg-0123456789abcdef0"];

function synth(workflow: any): Template {
  const app = new cdk.App();
  const stack = new OrchestratorStack(app, "TestStack", {
    env: { account: "123456789012", region: "us-east-1" },
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
const networks = (t: Template): Record<string, any> => Object.fromEntries(
  Object.values<any>(t.findResources("AWS::BedrockAgentCore::Runtime"))
    .map((r) => [r.Properties.AgentRuntimeName, r.Properties.NetworkConfiguration]));

describe("runtime network", () => {
  it("keeps every Runtime PUBLIC when workflow.json sets no network", () => {
    const got = networks(synth(shipped));
    expect(Object.keys(got).sort()).toEqual(["multiagent_orchestrator", "multiagent_orchestrator_documentation_search",
      "multiagent_orchestrator_knowledge_research", "multiagent_orchestrator_web_search"]);
    for (const n of Object.values(got)) expect(n).toEqual({ NetworkMode: "PUBLIC" });
  });

  it("puts the orchestrator and a dedicated agent in their own VPC settings, not inherited", () => {
    const wf = structuredClone(shipped);
    wf.orchestrator.network = { mode: "VPC", subnetIds: SUBNETS, securityGroupIds: GROUPS };
    wf.agents.knowledge_research.network = { mode: "VPC", subnetIds: SUBNETS.slice(0, 1), securityGroupIds: GROUPS };
    wf.agents.web_search.network = { mode: "PUBLIC" };
    const got = networks(synth(wf));
    expect(got.multiagent_orchestrator).toEqual({ NetworkMode: "VPC", NetworkModeConfig: { Subnets: SUBNETS, SecurityGroups: GROUPS } });
    expect(got.multiagent_orchestrator_knowledge_research).toEqual(
      { NetworkMode: "VPC", NetworkModeConfig: { Subnets: SUBNETS.slice(0, 1), SecurityGroups: GROUPS } });
    expect(got.multiagent_orchestrator_web_search).toEqual({ NetworkMode: "PUBLIC" });
    expect(got.multiagent_orchestrator_documentation_search).toEqual({ NetworkMode: "PUBLIC" });
  });

  it("refuses at synth what the API refuses, naming where", () => {
    expect(() => runtimeNetwork({ mode: "VPC" }, "orchestrator.network")).toThrow(/orchestrator.network.subnetIds: mode "VPC" needs 1-16 ids/);
    expect(() => runtimeNetwork({ mode: "VPC", subnetIds: SUBNETS }, "x")).toThrow(/x.securityGroupIds/);
    expect(() => runtimeNetwork({ mode: "VPC", subnetIds: ["subnet-xyz"], securityGroupIds: GROUPS }, "x")).toThrow(/x.subnetIds/);
    const many = Array.from({ length: 17 }, (_, i) => `sg-0123456789abcd${String(i).padStart(3, "0")}`);
    expect(() => runtimeNetwork({ mode: "VPC", subnetIds: SUBNETS, securityGroupIds: many }, "x")).toThrow(/x.securityGroupIds/);
    expect(() => runtimeNetwork({ mode: "public" }, "x")).toThrow(/x.mode must be one of PUBLIC, VPC/);
    expect(() => runtimeNetwork({ mode: "PUBLIC", subnetIds: SUBNETS }, "x")).toThrow(/apply to mode "VPC" only/);
    expect(runtimeNetwork(undefined, "x")).toEqual({ NetworkMode: "PUBLIC" });
    expect(() => validateAgentNetworks({ intake: { name: "I", network: { mode: "PUBLIC" } } })).toThrow(/agent "intake" sets "network" but its runtime is "main"/);
    expect(() => validateAgentNetworks({ r: { name: "R", runtime: "dedicated", network: { mode: "PUBLIC" } } })).not.toThrow();
  });

  it("matches the Terraform path", () => {
    const tf = (f: string) => fs.readFileSync(path.join(ORCH_ROOT, "terraform", f), "utf8");
    expect(tf("main.tf")).toContain('network_configuration = local.runtime_network_config["orchestrator.network"]');
    expect(tf("subagent_runtimes.tf")).toContain('network_configuration = local.runtime_network_config["agents.${each.key}.network"]');
    const net = tf("runtime_network.tf");
    for (const s of ['jsondecode(file("${path.module}/../app/vocabulary.json")).networkModes', "subnets         = n.subnets",
      "security_groups = n.groups", 'n.mode == "VPC" ? {', "try(local.workflow_def.orchestrator.network, null)",
      'lower(try(a.runtime, local.agent_defaults.runtime)) != "dedicated"']) {
      expect(net).toContain(s);
    }
    const ts = fs.readFileSync(path.join(__dirname, "..", "lib", "runtime-network.ts"), "utf8");
    expect(ts).toContain('vocab.values("networkModes")');
  });
});
