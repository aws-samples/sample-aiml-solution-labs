#!/usr/bin/env python3
"""
Layer 6 (capstone) — SELF-CONTAINED full-stack CDK app.

This is a COMPLETE, PARALLEL copy of the security controls proven across the demo's
Infra + Layer 1..Layer 5, packaged as its own CDK project so someone can lift it and
deploy a production-shaped secure agent out of the box. It is INDEPENDENT of the
demo's cdk/ project — all resources are L6-named (Iris* / iris-*) so the two can
coexist in one account without collision.

What it carries forward (security controls only — NOT the demo's attack scaffolding):
  - Infra: VPC + private subnets + NAT + Aurora (system of record) + ECR + broad-less
    exec role + sanctioned shipment service.  (NO attacker collector.)
  - Layer 1 (network): agent security group + VPC interface endpoints + (DNS Firewall
    built by the server as an ALLOWLIST — only the sanctioned shipment host resolves,
    everything else NXDOMAINs).
  - Layer 3 (tools): typed Gateway tool Lambdas + REQUEST interceptor (Gateway + Cedar
    policy engine + OBO providers are created by the server control-plane).
  - Layer 4 (memory): CMK + memory execution role + self-managed PRE-WRITE gate
    (S3 payload bucket + SNS + extractor/validator Lambda + per-actor gate flag table).
  - Layer 5 (models): scoped exec role (IAM model allowlist) + approved Application
    Inference Profile + default Bedrock Guardrail (+version); account-level enforcement
    is registered by the server.

The AgentCore resources (Gateway, Policy, OBO credential providers, Memory resource,
guardrail enforcement, and the ONE consolidated runtime running the goal-fenced agent)
are created by the separate Layer 6 server deploy module, exactly as the demo does —
this CDK provides the durable AWS resources those steps depend on.

DROPPED vs the demo: the collector (attacker) stack and the Cognito Layer 2 stack —
both are demo scaffolding (Layer 2 identity here is Okta, an external SaaS reused
as-is; there is no attacker infra in a takeaway package).
"""
import os
import aws_cdk as cdk
from iris_demo.baseline_stack import BaselineStack
from iris_demo.layer1_stack import Layer1Stack
from iris_demo.layer3_stack import Layer3Stack
from iris_demo.layer3_endpoints_stack import Layer3EndpointsStack
from iris_demo.layer4_stack import Layer4Stack
from iris_demo.layer5_stack import Layer5Stack

app = cdk.App()
env = cdk.Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=os.environ.get("CDK_DEFAULT_REGION", "us-east-1"),
)

# All stacks are FIXED-NAME and update in place. Nothing downstream depends on a
# physical resource name — the server resolves every resource by ARN via stack
# OUTPUTS — so CloudFormation manages resource names/replacement. This is the
# consolidated model: one stack set deployed + updated together, no per-deploy
# suffix, no orphaned generations.

# Durable infra (VPC/Aurora/ECR/exec-role/shipment/collector).
BaselineStack(app, "IrisInfra", env=env,
              description="Iris - infra: Aurora + VPC + NAT + ECR + shipment + collector - demo:iris-security")

# Network controls (agent SG + egress subnets).
Layer1Stack(app, "IrisNetwork", env=env,
            description="Iris - network controls (SG + egress subnets) - demo:iris-security")

# Gateway PrivateLink VPC endpoints.
Layer3EndpointsStack(app, "IrisEndpoints", env=env,
            description="Iris - Gateway VPC endpoints (stable) - demo:iris-security")

# Tools stack (typed tool Lambdas + interceptor).
Layer3Stack(app, "IrisTools", env=env,
            description="Iris - typed Gateway tools + interceptor - demo:iris-security")

# Memory stack (CMK + exec role + self-managed pre-write gate).
Layer4Stack(app, "IrisMemory", env=env,
            description="Iris - memory CMK + pre-write gate - demo:iris-security")

# Models stack (scoped exec role + guardrail + AIP).
Layer5Stack(app, "IrisModels", env=env,
            description="Iris - model allowlist + guardrail + AIP - demo:iris-security")

app.synth()
