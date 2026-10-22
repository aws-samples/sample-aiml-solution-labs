/**
 * An AgentCore Runtime's NetworkConfiguration, from workflow.json: `orchestrator.network` for
 * the shared orchestrator Runtime (every main and task agent runs in it) and
 * `agents.<id>.network` for a runtime "dedicated" agent's own Runtime. Mirrors
 * terraform/main.tf (orchestrator) and terraform/subagent_runtimes.tf (dedicated).
 *
 * The CloudFormation shape, AWS::BedrockAgentCore::Runtime NetworkConfiguration:
 *   NetworkMode PUBLIC | VPC (Required), NetworkModeConfig (VpcConfig, Required: No) with
 *   Subnets and SecurityGroups (each Required, 1-16 items).
 *   https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-properties-bedrockagentcore-runtime-networkconfiguration.html
 *   https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-properties-bedrockagentcore-runtime-vpcconfig.html
 * Absent, it stays { NetworkMode: "PUBLIC" }: the template is what it was before the setting.
 *
 * No IAM is added here. AgentCore creates the VPC's network interfaces with the service-linked
 * role AWSServiceRoleForBedrockAgentCoreNetwork, created on the first VPC Runtime; the
 * deploying principal needs iam:CreateServiceLinkedRole for network.bedrock-agentcore.amazonaws.com
 * (terraform/deploy-role-policy.json IamServiceLinkedRoles), and the execution role needs no
 * EC2 permission.
 *   https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/agentcore-vpc.html
 *   https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/service-linked-roles.html
 */
import { keyDefault } from "./defaults";
import * as vocab from "./vocabulary";

const MODES = vocab.values("networkModes");
const MAX_IDS = vocab.extra<number>("networkModes", "maxIds") ?? 16;
const SUBNET_RE = new RegExp(String(vocab.extra<string>("networkModes", "subnetPattern")));
const SG_RE = new RegExp(String(vocab.extra<string>("networkModes", "securityGroupPattern")));

/** The CloudFormation NetworkConfiguration for one Runtime. Throws, naming `where`, on a
 *  setting the docs say the API refuses (the BFF validators say the same before a deploy:
 *  bff/network_check.py). */
export function runtimeNetwork(net: any, where: string): Record<string, any> {
  if (net === undefined || net === null) return { NetworkMode: "PUBLIC" };
  const mode = String(net.mode ?? "");
  if (!MODES.includes(mode)) {
    throw new Error(`workflow.json ${where}.mode must be one of ${MODES.join(", ")} (got "${net.mode ?? ""}").`);
  }
  const subnets: unknown[] = Array.isArray(net.subnetIds) ? net.subnetIds : [];
  const groups: unknown[] = Array.isArray(net.securityGroupIds) ? net.securityGroupIds : [];
  if (mode !== "VPC") {
    if (subnets.length || groups.length) {
      throw new Error(`workflow.json ${where}: subnetIds and securityGroupIds apply to mode "VPC" only.`);
    }
    return { NetworkMode: mode };
  }
  for (const [what, ids, re] of [["subnetIds", subnets, SUBNET_RE], ["securityGroupIds", groups, SG_RE]] as const) {
    if (!ids.length || ids.length > MAX_IDS || !ids.every((x) => typeof x === "string" && re.test(x))) {
      throw new Error(`workflow.json ${where}.${what}: mode "VPC" needs 1-${MAX_IDS} ids matching ${re.source}.`);
    }
  }
  return { NetworkMode: mode, NetworkModeConfig: { Subnets: subnets, SecurityGroups: groups } };
}

/** `network` on an agent that has no Runtime of its own would configure nothing. */
export function validateAgentNetworks(agents: Record<string, any>): void {
  for (const [id, a] of Object.entries<any>(agents)) {
    if (a?.network !== undefined && String(a?.runtime ?? "").toLowerCase() !== "dedicated") {
      throw new Error(
        `workflow.json agent "${id}" sets "network" but its runtime is "${a?.runtime ?? keyDefault<string>("agent", "runtime")}". Only a ` +
          `"dedicated" agent has its own Runtime; main and task agents use orchestrator.network, a ` +
          `harness agent uses harness.network, and an a2a agent runs outside this build.`
      );
    }
  }
}
