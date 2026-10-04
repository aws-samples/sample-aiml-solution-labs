/**
 * cdk-nag (AwsSolutions) for this stack, and the findings it accepts, each with why.
 *
 * Run with `cdk synth -c nag=true` (bin/orchestrator.ts): an unacknowledged finding fails
 * the synth. test/nag.test.ts runs the same pack on every synth of the test suite, so a
 * new resource that trips a rule fails CI until it is fixed or acknowledged here.
 *
 * Acknowledged at stack level, not per resource: each one is a deliberate property of
 * the design (or a cost a sample should not impose), true of every resource it matches.
 */
import * as cdk from "aws-cdk-lib";
import { NagSuppressions } from "cdk-nag";

export const NAG_ACKNOWLEDGED: Array<{ id: string; reason: string }> = [
  { id: "AwsSolutions-IAM4",
    reason: "AWSLambdaBasicExecutionRole only, on CDK-managed singleton Lambdas (BucketDeployment, " +
      "AwsCustomResource, log retention) and the Cognito trigger: logs to its own group, nothing more." },
  { id: "AwsSolutions-IAM5",
    reason: "Wildcards are scoped by name prefix (agentName_*, ax_*, agentcore-*) or by an id only known " +
      "after create (AgentCore runtimes, memories, gateways, user pools, log streams). Account-wide reads " +
      "(bedrock:InvokeModel on foundation models, pricing:GetProducts, xray) have no resource form." },
  { id: "AwsSolutions-L1",
    reason: "Lambda runtimes are pinned to the version both IaC paths test against (python3.12 for the " +
      "code tools and BFF); CDK-managed singletons use the runtime CDK chooses." },
  { id: "AwsSolutions-S1",
    reason: "S3 server access logs are off by default for cost; CloudTrail data events cover audit. " +
      "Every bucket blocks public access, is encrypted and refuses plain HTTP." },
  { id: "AwsSolutions-CFR3",
    reason: "CloudFront standard logs are off by default for cost; API access logs are on." },
  { id: "AwsSolutions-CFR4",
    reason: "The default *.cloudfront.net certificate, so TLS 1.2 cannot be pinned without a custom " +
      "domain; bring one with an ACM certificate to set the minimum protocol." },
  { id: "AwsSolutions-COG8",
    reason: "Cognito threat protection needs the Plus feature plan (per-MAU cost). Passwords are 12+ " +
      "characters with symbols and TOTP MFA is offered." },
  { id: "AwsSolutions-CB4",
    reason: "The deploy project's artifacts and logs use the AWS-managed keys of S3 and CloudWatch Logs; " +
      "it holds no long-lived data of its own." },
  { id: "AwsSolutions-SMG4",
    reason: "Runtime secrets hold Cognito client secrets and third-party tokens that rotate at their " +
      "source; a redeploy writes the current values." },
];

export function acknowledgeNagFindings(stack: cdk.Stack): void {
  NagSuppressions.addStackSuppressions(stack, NAG_ACKNOWLEDGED);
}
