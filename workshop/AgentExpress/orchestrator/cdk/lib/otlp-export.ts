/**
 * Optional second trace destination: any OTLP/HTTP endpoint (off by default).
 *
 * When `-c otlpEndpoint` is set, every runtime (the orchestrator and each dedicated agent;
 * one image, one entrypoint) adds a span processor beside ADOT's and sends a copy of each
 * span there, without prompt or answer content unless `-c otlpIncludeContent=true`
 * (app/features/observability/export.py). AgentCore Observability in CloudWatch is
 * unchanged. Mirrors terraform/otlp_export.tf and the otlp_* variables.
 *
 * Off, this returns no environment and no statements, so the stack synthesizes exactly as
 * it did before.
 */
import { aws_iam as iam } from "aws-cdk-lib";

/** The traces URL, https only, no user info, query or fragment. The same pattern as
 *  var.otlp_endpoint and ENDPOINT_PATTERN in export.py (tests/test_otlp_export.py). */
export const OTLP_ENDPOINT_PATTERN = /^https:\/\/[A-Za-z0-9.-]+(:[0-9]{1,5})?(\/[^\s?#]*)?$/;
/** A full secret ARN, six-character suffix included: IAM grants on exactly this ARN. */
export const OTLP_SECRET_ARN_PATTERN = /^arn:aws:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@!-]+-[A-Za-z0-9]{6}$/;

export interface OtlpExportSettings {
  otlpEndpoint?: string;
  otlpHeadersSecretArn?: string;
  otlpIncludeContent?: boolean;
}

/** The runtime environment and the execution-role statements for the settings, or nothing
 *  when off. Throws at synth on a setting Terraform's validation would refuse at plan. */
export function otlpExport(s: OtlpExportSettings): { env: Record<string, string>; statements: iam.PolicyStatement[] } {
  const endpoint = s.otlpEndpoint ?? "";
  const arn = s.otlpHeadersSecretArn ?? "";
  if ((endpoint === "") !== (arn === "")) {
    throw new Error("Set otlpEndpoint and otlpHeadersSecretArn together, or neither.");
  }
  if (!endpoint) return { env: {}, statements: [] };
  if (!OTLP_ENDPOINT_PATTERN.test(endpoint)) {
    throw new Error("otlpEndpoint must be an https:// URL with no user info, query or fragment, " +
      "e.g. https://cloud.langfuse.com/api/public/otel/v1/traces.");
  }
  if (!OTLP_SECRET_ARN_PATTERN.test(arn)) {
    throw new Error("otlpHeadersSecretArn must be the full ARN of a Secrets Manager secret, including the " +
      "six-character suffix (arn:aws:secretsmanager:<region>:<account>:secret:<name>-AbCdEf).");
  }
  // arn:aws:secretsmanager:<region>:<account>:secret:<name>
  const [, , , region, account] = arn.split(":");
  return {
    env: {
      OTLP_EXPORT_ENDPOINT: endpoint,
      OTLP_EXPORT_HEADERS_SECRET_ARN: arn,
      OTLP_EXPORT_INCLUDE_CONTENT: s.otlpIncludeContent ? "true" : "false",
    },
    statements: [
      new iam.PolicyStatement({
        sid: "ReadOtlpHeadersSecret",
        actions: ["secretsmanager:GetSecretValue"],
        resources: [arn],
      }),
      // Only needed when the secret uses a customer managed key (the aws/secretsmanager key
      // policy already allows use through Secrets Manager). The key is not a setting, so the
      // grant names the secret's account and region and is held to decrypting THIS secret
      // through Secrets Manager: its encryption context carries the secret's ARN.
      new iam.PolicyStatement({
        sid: "DecryptOtlpHeadersSecret",
        actions: ["kms:Decrypt"],
        resources: [`arn:aws:kms:${region}:${account}:key/*`],
        conditions: {
          StringEquals: {
            "kms:ViaService": `secretsmanager.${region}.amazonaws.com`,
            "kms:EncryptionContext:SecretARN": arn,
          },
        },
      }),
    ],
  };
}
