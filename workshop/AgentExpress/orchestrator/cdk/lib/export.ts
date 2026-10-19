/**
 * Send each run to an observability tool (workflow.json `orchestrator.export`, off by default).
 *
 * When app/workflow.json has orchestrator.export, the ORCHESTRATOR runtime sends each run,
 * once it ends, to Langfuse, Datadog, Grafana Cloud, Honeycomb or an OTLP backend
 * (app/features/observability/runexport.py). Only the orchestrator: it rebuilds the run from
 * the telemetry table, which every runtime already writes. Mirrors terraform/export.tf.
 *
 * The headers that carry the tool's API key are a Secrets Manager secret the runtime reads by
 * ARN: one created here from EXPORT_HEADERS[<secret>] (the Builder's exportHeaders build
 * secret, by the name orchestrator.export.secret gives; from the environment, never context),
 * or an existing one when orchestrator.export.secret is an ARN.
 *
 * Off, exportPlan returns null and the stack synthesizes exactly as before.
 */
import { aws_iam as iam, aws_kms as kms, aws_secretsmanager as secretsmanager, RemovalPolicy, SecretValue } from "aws-cdk-lib";
import { Construct } from "constructs";

export const EXPORT_DESTINATIONS = ["langfuse", "datadog", "grafana", "honeycomb", "otlp"];
/** The same pattern as export.SECRET_ARN_PATTERN and terraform/export.tf. */
export const EXPORT_SECRET_ARN_PATTERN = /^arn:aws:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@!-]+-[A-Za-z0-9]{6}$/;

export interface ExportPlan {
  destination: string;
  /** Set when orchestrator.export.secret is an ARN; otherwise the secret is created. */
  secretArn?: string;
  /** The headers JSON for the secret this stack creates. */
  headers?: string;
  includeContent: boolean;
}

/** What orchestrator.export asks for, or null when it is not set. Throws at synth on what
 *  Terraform's preconditions refuse at plan, with the same words. */
export function exportPlan(workflow: any, exportHeaders: Record<string, string> = {}): ExportPlan | null {
  const cfg = workflow?.orchestrator?.export;
  if (cfg === undefined || cfg === null) return null;
  const destination = typeof cfg.destination === "string" ? cfg.destination : "";
  if (!EXPORT_DESTINATIONS.includes(destination)) {
    throw new Error(`app/workflow.json orchestrator.export.destination must be one of: ${EXPORT_DESTINATIONS.join(", ")}.`);
  }
  const secret = typeof cfg.secret === "string" ? cfg.secret : "";
  const byArn = secret.startsWith("arn:");
  if (!secret || (byArn && !EXPORT_SECRET_ARN_PATTERN.test(secret))) {
    throw new Error("app/workflow.json orchestrator.export.secret must name the build's exportHeaders secret, " +
      "or be the full ARN of a Secrets Manager secret, including the six-character suffix.");
  }
  const includeContent = cfg.send?.content === true;
  if (byArn) return { destination, secretArn: secret, includeContent };
  const headers = exportHeaders[secret] ?? "";
  if (!headers) {
    throw new Error(`app/workflow.json orchestrator.export.secret names "${secret}", but the build has no exportHeaders ` +
      "secret by that name (var.export_headers). Save the tool's key in the console, or give a Secrets Manager secret ARN.");
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(headers);
  } catch {
    parsed = undefined;
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error(`The exportHeaders secret "${secret}" must be a JSON object of HTTP headers.`);
  }
  return { destination, headers, includeContent };
}

/** The statements that let the runtime role read the headers secret at `arn`. */
export function exportStatements(arn: string, byArn: boolean): iam.PolicyStatement[] {
  const statements = [new iam.PolicyStatement({
    sid: "ReadExportHeadersSecret", actions: ["secretsmanager:GetSecretValue"], resources: [arn],
  })];
  if (byArn) {
    // A secret given by ARN may use a customer managed key, which is not a setting: the grant
    // names the secret's account and region and is held to decrypting THIS secret through
    // Secrets Manager (as otlp-export.ts does). A secret created here uses the deployment's
    // data key, whose policy already allows that.
    const [, , , region, account] = arn.split(":");
    statements.push(new iam.PolicyStatement({
      sid: "DecryptExportHeadersSecret",
      actions: ["kms:Decrypt"],
      resources: [`arn:aws:kms:${region}:${account}:key/*`],
      conditions: {
        StringEquals: {
          "kms:ViaService": `secretsmanager.${region}.amazonaws.com`,
          "kms:EncryptionContext:SecretARN": arn,
        },
      },
    }));
  }
  return statements;
}

/** Create the headers secret (when the plan carries headers), grant the runtime role, and
 *  return the orchestrator runtime's environment. Nothing when `plan` is null. */
export function addRunExport(scope: Construct, plan: ExportPlan | null, dataKey: kms.IKey,
  role: iam.Role): Record<string, string> {
  if (!plan) return {};
  let arn = plan.secretArn ?? "";
  if (!arn) {
    const secret = new secretsmanager.Secret(scope, "ExportHeadersSecret", {
      description: "HTTP headers for sending runs (app/features/observability/runexport.py)",
      secretStringValue: SecretValue.unsafePlainText(plan.headers ?? ""),
      encryptionKey: dataKey,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    arn = secret.secretArn;
  }
  exportStatements(arn, Boolean(plan.secretArn)).forEach((s) => role.addToPolicy(s));
  return {
    OTLP_EXPORT_DESTINATION: plan.destination,
    OTLP_EXPORT_HEADERS_SECRET_ARN: arn,
    OTLP_EXPORT_INCLUDE_CONTENT: plan.includeContent ? "true" : "false",
  };
}
