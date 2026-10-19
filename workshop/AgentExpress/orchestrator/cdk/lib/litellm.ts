/**
 * Optional: non-Bedrock models through the customer's LiteLLM proxy (off by default).
 *
 * When `-c litellmBaseUrl` is set, every runtime (the orchestrator and each dedicated agent)
 * can answer an agent whose model is litellm/<model> by calling the proxy's
 * /v1/chat/completions with the virtual key in `-c litellmKeySecretArn`
 * (app/common/litellm_proxy.py), and the BFF lists the proxy's models for the Build view's
 * picker (GET /api/models/litellm, bff/litellm_models.py). Mirrors terraform/litellm.tf and
 * the litellm_* variables.
 *
 * Off, this returns no environment and no statements, so the stack synthesizes exactly as
 * it did before.
 */
import { aws_iam as iam } from "aws-cdk-lib";

/** The proxy, https only, no user info, query or fragment. The same pattern as
 *  var.litellm_base_url and BASE_URL_PATTERN in litellm_proxy.py (tests/test_litellm_iac.py). */
export const LITELLM_BASE_URL_PATTERN = /^https:\/\/[A-Za-z0-9.-]+(:[0-9]{1,5})?(\/[^\s?#]*)?$/;
/** A full secret ARN, six-character suffix included: IAM grants on exactly this ARN. */
export const LITELLM_SECRET_ARN_PATTERN = /^arn:aws:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@!-]+-[A-Za-z0-9]{6}$/;

export interface LitellmSettings {
  litellmBaseUrl?: string;
  litellmKeySecretArn?: string;
}

/** The environment (runtimes and BFF) and the statements (their roles) for the settings,
 *  or nothing when off. Throws at synth on a setting Terraform's validation refuses at plan. */
export function litellmProxy(s: LitellmSettings): { env: Record<string, string>; statements: iam.PolicyStatement[] } {
  const base = s.litellmBaseUrl ?? "";
  const arn = s.litellmKeySecretArn ?? "";
  if ((base === "") !== (arn === "")) {
    throw new Error("Set litellmBaseUrl and litellmKeySecretArn together, or neither.");
  }
  if (!base) return { env: {}, statements: [] };
  if (!LITELLM_BASE_URL_PATTERN.test(base)) {
    throw new Error("litellmBaseUrl must be an https:// URL with no user info, query or fragment, " +
      "e.g. https://litellm.example.com.");
  }
  if (!LITELLM_SECRET_ARN_PATTERN.test(arn)) {
    throw new Error("litellmKeySecretArn must be the full ARN of a Secrets Manager secret, including the " +
      "six-character suffix (arn:aws:secretsmanager:<region>:<account>:secret:<name>-AbCdEf).");
  }
  // arn:aws:secretsmanager:<region>:<account>:secret:<name>
  const [, , , region, account] = arn.split(":");
  return {
    env: { LITELLM_BASE_URL: base, LITELLM_KEY_SECRET_ARN: arn },
    statements: [
      new iam.PolicyStatement({
        sid: "ReadLitellmKeySecret",
        actions: ["secretsmanager:GetSecretValue"],
        resources: [arn],
      }),
      // Only needed when the secret uses a customer managed key (the aws/secretsmanager key
      // policy already allows use through Secrets Manager). The key is not a setting, so the
      // grant names the secret's account and region and is held to decrypting THIS secret
      // through Secrets Manager: its encryption context carries the secret's ARN.
      new iam.PolicyStatement({
        sid: "DecryptLitellmKeySecret",
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
