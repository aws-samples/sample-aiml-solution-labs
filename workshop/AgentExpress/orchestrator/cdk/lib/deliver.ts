/**
 * Deliver steps (v139 Track C deliver): a task step with `task.kind: "deliver"`
 * (app/common/deliver.py) sends the run's files to S3, by email with Amazon SES, or to a
 * webhook. It runs in the orchestrator's own runtime, so only that role gets anything, and
 * only when a deliver step exists:
 *   DeliverReadRunFiles  s3:GetObject on the assets bucket's runs/<session>/outputs/ only;
 *   DeliverListRunFiles  s3:ListBucket on the assets bucket, for prefixes under runs/*\/outputs/;
 *   DeliverS3Put         s3:PutObject under each s3 target's prefix only;
 *   DeliverSesSend       ses:SendEmail on each email target's identity ARN only, with
 *                        ses:FromAddress pinned to the configured From addresses
 *                        (https://docs.aws.amazon.com/service-authorization/latest/reference/list_sesv2.html,
 *                        https://docs.aws.amazon.com/ses/latest/dg/control-user-access.html).
 * A webhook target needs no AWS access; its optional signing secret is a deliverSecrets
 * build secret, deployed in the runtime secret as DELIVER_SECRETS only when a target names
 * one. A workflow with no deliver step gets nothing. Mirrors terraform/deliver.tf.
 */
import * as iam from "aws-cdk-lib/aws-iam";

/** The build-storage token an s3 target's uri may start with (orchestrator.storage). */
export const STORAGE_TOKEN = "${storage}";

/** Every target of every deliver step, in workflow order. */
export function deliverTargets(workflow: any): any[] {
  return Object.values<any>(workflow?.agents ?? {})
    .filter((a) => a?.runtime === "task" && a?.task?.kind === "deliver")
    .flatMap((a) => (Array.isArray(a.task.targets) ? a.task.targets : []));
}

/** Whether the workflow has a deliver step. */
export function hasDeliver(workflow: any): boolean {
  return Object.values<any>(workflow?.agents ?? {}).some((a) => a?.runtime === "task" && a?.task?.kind === "deliver");
}

/** The deliverSecrets names the webhook targets sign with. */
export function deliverSecretNames(workflow: any): string[] {
  return [...new Set(deliverTargets(workflow)
    .filter((t) => t?.type === "webhook" && typeof t?.secret === "string" && t.secret)
    .map((t) => String(t.secret)))].sort();
}

/** Throws at synth for what the runtime could not deliver with. */
export function validateDeliver(workflow: any, assetsBucket: boolean, secrets: Record<string, string>): void {
  if (!hasDeliver(workflow)) return;
  if (!assetsBucket) {
    throw new Error("app/workflow.json has a deliver step, which sends the run's files: set orchestrator.files so the run keeps them.");
  }
  const storage = deliverTargets(workflow).filter((t) => t?.type === "s3" && String(t?.uri ?? "").startsWith(STORAGE_TOKEN));
  if (storage.length) {
    throw new Error(`app/workflow.json deliver target uri ${storage[0].uri} uses ${STORAGE_TOKEN}, which needs the build's storage (orchestrator.storage).`);
  }
  const missing = deliverSecretNames(workflow).filter((n) => !secrets[n]);
  if (missing.length) {
    throw new Error(`app/workflow.json deliver target(s) sign with the deliverSecrets secret(s) ${missing.join(", ")}, ` +
      `but the build has none by that name (DELIVER_SECRETS). Save it on the step's panel, or remove \`secret\`.`);
  }
}

/** (bucket, prefix) of an s3 target's uri, s3://<bucket>/<prefix>/. */
function bucketAndPrefix(uri: string): [string, string] {
  const [bucket, ...rest] = uri.replace(/^s3:\/\//, "").split("/");
  return [bucket, rest.join("/")];
}

/** What the runtime role needs for the workflow's deliver steps (none without them). */
export function deliverStatements(workflow: any, assetsBucketArn: string): iam.PolicyStatement[] {
  if (!hasDeliver(workflow)) return [];
  const targets = deliverTargets(workflow);
  const out = [
    new iam.PolicyStatement({
      sid: "DeliverReadRunFiles", actions: ["s3:GetObject"], resources: [`${assetsBucketArn}/runs/*/outputs/*`],
    }),
    new iam.PolicyStatement({
      sid: "DeliverListRunFiles", actions: ["s3:ListBucket"], resources: [assetsBucketArn],
      conditions: { StringLike: { "s3:prefix": ["runs/*/outputs/*"] } },
    }),
  ];
  const puts = [...new Set(targets.filter((t) => t?.type === "s3" && typeof t?.uri === "string")
    .map((t) => { const [b, p] = bucketAndPrefix(t.uri); return `arn:aws:s3:::${b}/${p}*`; }))];
  if (puts.length) {
    out.push(new iam.PolicyStatement({ sid: "DeliverS3Put", actions: ["s3:PutObject"], resources: puts }));
  }
  const emails = targets.filter((t) => t?.type === "email" && typeof t?.identityArn === "string");
  if (emails.length) {
    out.push(new iam.PolicyStatement({
      sid: "DeliverSesSend", actions: ["ses:SendEmail"],
      resources: [...new Set(emails.map((t) => String(t.identityArn)))],
      conditions: { StringEquals: { "ses:FromAddress": [...new Set(emails.map((t) => String(t.fromAddress ?? "")))] } },
    }));
  }
  return out;
}

/** DELIVER_SECRETS for the runtime secret: name -> value, only the names a webhook target
 *  signs with; null when none does, so the secret is what it was without deliver steps. */
export function deliverSecretEnv(workflow: any, secrets: Record<string, string>): Record<string, string> | null {
  const names = deliverSecretNames(workflow);
  return names.length ? Object.fromEntries(names.map((n) => [n, secrets[n] ?? ""])) : null;
}
