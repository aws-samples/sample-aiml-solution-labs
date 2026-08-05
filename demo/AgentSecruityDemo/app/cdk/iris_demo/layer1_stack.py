"""
Layer 1 (Platform) stack — network controls.

Reuses the shared VPC from baseline (which now has a NAT gateway + a
PRIVATE_WITH_EGRESS subnet tier). Layer 1 places the agent runtime in the
egress subnets so it CAN reach the internet — but egress is FILTERED by the
Route 53 DNS Firewall built in infra, which allows ONLY the sanctioned shipment
Function URL host and denies every other domain (including the attacker's exfil
URL from a prompt injection).

So the difference from baseline is purely the network perimeter:
  - baseline: agent is PUBLIC (no VPC) → http_request reaches ANY url → exfil works
  - layer 1 : agent is in the VPC, egress filtered by DNS Firewall → the shipment
              lookup still works, but the exfil URL fails to resolve (NXDOMAIN)

Same agent code/image as baseline.
"""
from aws_cdk import (
    Stack, Tags, CfnOutput,
    aws_ec2 as ec2,
)
from constructs import Construct


class Layer1Stack(Stack):
    def __init__(self, scope: Construct, cid: str, **kwargs) -> None:
        super().__init__(scope, cid, **kwargs)

        Tags.of(self).add("demo", "iris-security")
        Tags.of(self).add("demo-layer", "layer1")

        # Import baseline's VPC by ID (passed via context)
        vpc_id = self.node.try_get_context("vpcId")
        vpc = ec2.Vpc.from_lookup(self, "BaselineVpc", vpc_id=vpc_id) if vpc_id else None
        if not vpc:
            # Fallback minimal VPC (shouldn't happen once infra is deployed)
            vpc = ec2.Vpc(self, "L1Vpc", max_azs=2, nat_gateways=1)

        # Security Group — allow HTTPS egress to the internet. We deliberately DO
        # NOT try to block the exfil host here (an SG can't tell two Lambda URLs
        # apart — same shared AWS IP ranges). The selective allow/deny is done by
        # the DNS Firewall (allow shipment host, block everything else).
        sg = ec2.SecurityGroup(
            self, "AgentSG",
            vpc=vpc,
            description="Iris agent Layer 1 - HTTPS egress (filtered by DNS Firewall)",
            allow_all_outbound=False,
        )
        sg.add_egress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.tcp(443),
            "HTTPS egress - DNS Firewall decides which hosts resolve",
        )
        sg.add_egress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.udp(53),
            "DNS - resolved through the VPC resolver (DNS Firewall inspects)",
        )
        sg.add_egress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.tcp(53),
            "DNS TCP",
        )
        sg.add_ingress_rule(
            ec2.Peer.ipv4(vpc.vpc_cidr_block),
            ec2.Port.tcp(443),
            "HTTPS from VPC for endpoint ENIs",
        )

        # Interface endpoints for the AWS services the agent uses — keeps that
        # traffic on PrivateLink (cheaper/faster than NAT) and independent of the
        # DNS Firewall's public-domain rules.
        for name, svc in [
            ("BedrockRuntimeEP", ec2.InterfaceVpcEndpointAwsService.BEDROCK_RUNTIME),
            ("CloudWatchLogsEP", ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS),
            ("StsEP", ec2.InterfaceVpcEndpointAwsService.STS),
            ("EcrApiEP", ec2.InterfaceVpcEndpointAwsService.ECR),
            ("EcrDockerEP", ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER),
            ("RdsDataEP", ec2.InterfaceVpcEndpointAwsService("rds-data")),
        ]:
            vpc.add_interface_endpoint(name, service=svc, security_groups=[sg])
        vpc.add_gateway_endpoint("S3EP", service=ec2.GatewayVpcEndpointAwsService.S3)

        # The agent runs in the PRIVATE_WITH_EGRESS subnets (NAT → IGW), so it has
        # a real internet path — which the DNS Firewall then filters.
        egress_subnets = vpc.select_subnets(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS)

        CfnOutput(self, "VpcId", value=vpc.vpc_id)
        CfnOutput(self, "SubnetIds", value=",".join([s.subnet_id for s in egress_subnets.subnets]))
        CfnOutput(self, "SecurityGroupId", value=sg.security_group_id)
