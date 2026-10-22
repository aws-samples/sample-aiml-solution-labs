# --- AgentCore Runtime network (PUBLIC or VPC) ---------------------------------
# workflow.json `orchestrator.network` places the shared orchestrator Runtime (main.tf), where
# every main and task agent runs; `agents.<id>.network` places a runtime "dedicated" agent's
# own Runtime (subagent_runtimes.tf). Absent, a Runtime stays network_mode = "PUBLIC" with no
# network_mode_config: the plan is what it was before the setting. Mirrors
# cdk/lib/runtime-network.ts.
#
# awscc_bedrockagentcore_runtime network_configuration: network_mode, network_mode_config
# { subnets, security_groups } (https://registry.terraform.io/providers/hashicorp/awscc/1.101.0/docs/resources/bedrockagentcore_runtime).
# Allowed values and limits are the API's: networkMode PUBLIC | VPC, Required
# (https://docs.aws.amazon.com/bedrock-agentcore-control/latest/APIReference/API_NetworkConfiguration.html);
# subnets and securityGroups each Required, 1-16, subnet-[0-9a-zA-Z]{8,17} / sg-[0-9a-zA-Z]{8,17}
# (https://docs.aws.amazon.com/bedrock-agentcore-control/latest/APIReference/API_VpcConfig.html),
# held once in app/vocabulary.json networkModes.
#
# No IAM here: AgentCore creates the network interfaces with the service-linked role
# AWSServiceRoleForBedrockAgentCoreNetwork, made on the first VPC Runtime; the deployer needs
# iam:CreateServiceLinkedRole for network.bedrock-agentcore.amazonaws.com, which
# deploy-role-policy.json IamServiceLinkedRoles already allows
# (https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/agentcore-vpc.html,
# https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/service-linked-roles.html).
locals {
  network_vocab = jsondecode(file("${path.module}/../app/vocabulary.json")).networkModes
  # Each Runtime's setting, keyed by where it is written, in one shape.
  runtime_networks = { for where, n in merge(
    { "orchestrator.network" = try(local.workflow_def.orchestrator.network, null) },
    { for id, a in local.dedicated_agents : "agents.${id}.network" => try(a.network, null) },
    ) : where => {
    mode    = n == null ? "PUBLIC" : tostring(try(n.mode, ""))
    subnets = [for s in try(n.subnetIds, []) : tostring(s)]
    groups  = [for s in try(n.securityGroupIds, []) : tostring(s)]
  } }
  # What the API would refuse, named (bff/network_check.py says the same before a deploy).
  runtime_network_problems = { for where, n in local.runtime_networks : where => concat(
    contains(local.network_vocab.values, n.mode) ? [] : ["${where}.mode must be one of ${join(", ", local.network_vocab.values)} (got \"${n.mode}\")"],
    n.mode == "VPC" ? [for k, ids in { subnetIds = n.subnets, securityGroupIds = n.groups } :
      "${where}.${k}: mode \"VPC\" needs 1-${local.network_vocab.maxIds} ids matching ${k == "subnetIds" ? local.network_vocab.subnetPattern : local.network_vocab.securityGroupPattern}"
    if length(ids) < 1 || length(ids) > local.network_vocab.maxIds || anytrue([for x in ids : length(regexall(k == "subnetIds" ? local.network_vocab.subnetPattern : local.network_vocab.securityGroupPattern, x)) == 0])] : [],
    n.mode != "VPC" && length(concat(n.subnets, n.groups)) > 0 ? ["${where}: subnetIds and securityGroupIds apply to mode \"VPC\" only"] : [],
  ) }
  # `network` on an agent with no Runtime of its own would configure nothing.
  misplaced_networks = [for id, a in local.workflow_def.agents : id
  if can(a.network) && lower(try(a.runtime, local.agent_defaults.runtime)) != "dedicated"]
  # The awscc network_configuration attribute for each Runtime.
  runtime_network_config = { for where, n in local.runtime_networks : where => {
    network_mode = n.mode
    network_mode_config = n.mode == "VPC" ? {
      subnets         = n.subnets
      security_groups = n.groups
    } : null
  } }
}
