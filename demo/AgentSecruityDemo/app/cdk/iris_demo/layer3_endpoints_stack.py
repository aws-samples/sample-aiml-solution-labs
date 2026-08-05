"""
Layer 3 Endpoints stack — stable, deploy-once VPC endpoints for Gateway access.

Kept SEPARATE from the (timestamped) Layer 3 tools stack so that redeploying
tools/Gateway never deletes+recreates these endpoints. Private-DNS interface
endpoints are unique per service per VPC — recreating them on every deploy
causes a "conflicting DNS domain" error. This stack has a STABLE name and is
never deleted during a Layer 3 redeploy, so the tools stack can be recreated
freely without touching them.

Creates:
  - bedrock-agentcore.gateway  → agent calls Gateway MCP via PrivateLink
  - bedrock-agentcore          → data plane (token exchange, invocations)
"""
from aws_cdk import (
    Stack, Tags, CfnOutput,
    aws_ec2 as ec2,
)
from constructs import Construct


class Layer3EndpointsStack(Stack):
    def __init__(self, scope: Construct, cid: str, **kwargs) -> None:
        super().__init__(scope, cid, **kwargs)

        Tags.of(self).add("demo", "iris-security")
        Tags.of(self).add("demo-layer", "layer3")

        vpc_id = self.node.try_get_context("vpcId")
        sg_id = self.node.try_get_context("securityGroupId")

        # This stack is only meaningful when BOTH the VPC and the Layer 1 SG are
        # provided (i.e. during the Layer 3 deploy). When CDK synthesizes it as a
        # side effect of another layer's deploy (e.g. Layer 1 passes only vpcId),
        # skip building endpoints so synth doesn't fail. Passing an empty
        # security_groups list to add_interface_endpoint also errors, so only pass
        # it when we actually have an SG.
        if vpc_id and sg_id:
            vpc = ec2.Vpc.from_lookup(self, "BaselineVpc", vpc_id=vpc_id)
            sg = ec2.SecurityGroup.from_security_group_id(self, "L1SG", sg_id)
            gw_ep = vpc.add_interface_endpoint("AgentCoreGatewayEP",
                service=ec2.InterfaceVpcEndpointAwsService("bedrock-agentcore.gateway"),
                security_groups=[sg],
                private_dns_enabled=True,
            )
            data_ep = vpc.add_interface_endpoint("AgentCoreDataEP",
                service=ec2.InterfaceVpcEndpointAwsService("bedrock-agentcore"),
                security_groups=[sg],
                private_dns_enabled=True,
            )
            CfnOutput(self, "GatewayEndpointId", value=gw_ep.vpc_endpoint_id)
            CfnOutput(self, "DataEndpointId", value=data_ep.vpc_endpoint_id)
