/**
 * Build storage (workflow.json `orchestrator.storage`): an S3 bucket the deploy creates for
 * the build, which tools reach through the `${storage}` token (app/vocabulary.json
 * storageToken). Mirrors terraform/storage.tf.
 *
 * Opt-in. `{"create": true}` creates `<build name>-storage-<account>`:
 *   * private: all four Block Public Access settings, and a bucket policy refusing plain HTTP
 *     https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-control-block-public-access.html
 *   * encrypted by default with the build's own KMS key (DataKey), with an S3 Bucket Key
 *     https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html
 *     https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucket-key.html
 *     With `retain`, S3-managed keys instead: the build's key is deleted with the build, which
 *     would leave what the bucket keeps unreadable.
 *   * versioned; a lifecycle rule deletes a version 30 days after it is replaced, clears
 *     delete markers left with no versions, and aborts uploads unfinished after 7 days
 *     https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-configuration-examples.html
 *   * tagged agentexpress:code-tools=true with ABAC on, so a code tool's grant (conditioned
 *     on that tag, lib/tool-plane.ts CODE_GRANTABLE) reaches it
 *     https://docs.aws.amazon.com/AmazonS3/latest/userguide/buckets-tagging-enable-abac.html
 *     https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-s3-bucket.html#cfn-s3-bucket-abacstatus
 *
 * Nothing is granted on the bucket here: each tool that names it gets its own scoped grant
 * where tools get theirs (a code tool's s3Prefix, a kb tool's s3Uri).
 *
 * Destroy: by default the bucket is emptied (every version) and deleted with the stack, like
 * the assets bucket. `retain: true` keeps it and its contents (RemovalPolicy.RETAIN); a later
 * deploy of the same build then finds the name taken, so delete or import the kept bucket
 * first. A workflow without `orchestrator.storage` synthesizes exactly as before
 * (test/storage-identity.test.ts).
 */
import * as cdk from "aws-cdk-lib";
import * as kms from "aws-cdk-lib/aws-kms";
import * as s3 from "aws-cdk-lib/aws-s3";
import { Construct } from "constructs";

import { bucketName, BucketNaming } from "./bucket-names";
import { keyDefault } from "./defaults";
import { CODE_GRANTABLE_TAG } from "./tool-plane";
import { extra, values } from "./vocabulary";

/** What a value writes for the build's bucket: s3://<the bucket>. */
export const STORAGE_TOKEN = values("storageToken")[0];
/** Where the token is read (a `*` matches any key or list index). */
export const STORAGE_FIELDS: string[] = extra<string[]>("storageToken", "fields") ?? [];
/** Days an old version is kept after it is replaced or deleted. */
export const STORAGE_NONCURRENT_DAYS = 30;
/** Days before an unfinished multipart upload is aborted. */
export const STORAGE_ABORT_UPLOAD_DAYS = 7;

export interface StorageSpec {
  bucketName: string;
  /** What the token stands for: s3://<bucketName>. */
  uri: string;
  retain: boolean;
}

/** `<build name>-storage-<account>` (legacy) or `<build name>-storage-<account>-<region>`
 *  (regional, lib/bucket-names.ts), dashed like the build's other buckets (an S3 bucket name
 *  takes no underscore). Not agentcore-*: that prefix is the framework's own, which every code
 *  tool's boundary denies. */
export function storageBucketName(agentName: string, account: string, region = "",
                                  scheme: BucketNaming = "legacy"): string {
  return bucketName("storage", agentName, account, region, scheme);
}

/** The build's storage, or null when the workflow does not set orchestrator.storage.create.
 *  Throws at synth for a value the validators refuse. */
export function storageOf(workflow: any, agentName: string, account: string, region = "",
                          scheme: BucketNaming = "legacy"): StorageSpec | null {
  const st = workflow?.orchestrator?.storage;
  if (st === undefined || st === null) return null;
  const defaults = keyDefault<{ create: boolean; retain: boolean }>("orchestrator", "storage");
  const create = st.create ?? defaults.create;
  const retain = st.retain ?? defaults.retain;
  if (typeof create !== "boolean" || typeof retain !== "boolean") {
    throw new Error("orchestrator.storage.create and orchestrator.storage.retain must be true or false.");
  }
  if (!create) return null;
  const bucketName = storageBucketName(agentName, account, region, scheme);
  // https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html: 3 to 63.
  if (!cdk.Token.isUnresolved(account) && bucketName.length > 63) {
    throw new Error(`orchestrator.storage: the bucket name ${bucketName} is over S3's 63 characters; use a shorter agent name.`);
  }
  return { bucketName, uri: `s3://${bucketName}`, retain };
}

/** Each [path, value] of the workflow a pattern of STORAGE_FIELDS names, path as segments. */
function matches(node: any, pattern: string[], at: (string | number)[] = []): [(string | number)[], any][] {
  if (!pattern.length) return [[at, node]];
  if (node === null || typeof node !== "object") return [];
  const [head, ...rest] = pattern;
  const keys: (string | number)[] = head === "*"
    ? (Array.isArray(node) ? node.map((_, i) => i) : Object.keys(node))
    : (Object.prototype.hasOwnProperty.call(node, head) ? [Array.isArray(node) ? Number(head) : head] : []);
  return keys.flatMap((k) => matches(node[k as any], rest, [...at, k]));
}

const dotted = (at: (string | number)[]) =>
  at.map((k, i) => (typeof k === "number" ? `[${k}]` : `${i ? "." : ""}${k}`)).join("");

/** The workflow with the token resolved where it is read: the same object when nothing uses
 *  it, so a workflow without storage is untouched. Throws when a value uses the token and the
 *  build creates no storage, rather than deploy the literal text. */
export function resolveStorage(workflow: any, storage: StorageSpec | null): any {
  const hits = STORAGE_FIELDS.flatMap((f) => matches(workflow, f.split(".")))
    .filter(([, v]) => typeof v === "string" && v.includes(STORAGE_TOKEN));
  if (!hits.length) return workflow;
  if (!storage) {
    throw new Error(`workflow.json ${dotted(hits[0][0])} uses ${STORAGE_TOKEN}, but this build creates no storage: ` +
      "set orchestrator.storage.create to true.");
  }
  const out = JSON.parse(JSON.stringify(workflow));
  for (const [at, v] of hits) {
    let node = out;
    for (const k of at.slice(0, -1)) node = node[k as any];
    node[at[at.length - 1] as any] = String(v).split(STORAGE_TOKEN).join(storage.uri);
  }
  return out;
}

/** The bucket. `dataKey` is the build's KMS key (DataKey in orchestrator-stack.ts). */
export function addBuildStorage(scope: Construct, storage: StorageSpec, dataKey: kms.IKey): s3.Bucket {
  const bucket = new s3.Bucket(scope, "BuildStorage", {
    bucketName: storage.bucketName,
    blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    ...(storage.retain
      ? { encryption: s3.BucketEncryption.S3_MANAGED }
      : { encryption: s3.BucketEncryption.KMS, encryptionKey: dataKey, bucketKeyEnabled: true }),
    enforceSSL: true,
    versioned: true,
    abacStatus: true,
    lifecycleRules: [{
      id: "storage-housekeeping",
      noncurrentVersionExpiration: cdk.Duration.days(STORAGE_NONCURRENT_DAYS),
      expiredObjectDeleteMarker: true,
      abortIncompleteMultipartUploadAfter: cdk.Duration.days(STORAGE_ABORT_UPLOAD_DAYS),
    }],
    removalPolicy: storage.retain ? cdk.RemovalPolicy.RETAIN : cdk.RemovalPolicy.DESTROY,
    autoDeleteObjects: !storage.retain,
  });
  // A kept bucket keeps its HTTPS-only policy too.
  if (storage.retain) bucket.policy?.applyRemovalPolicy(cdk.RemovalPolicy.RETAIN);
  cdk.Tags.of(bucket).add(CODE_GRANTABLE_TAG, "true");
  return bucket;
}

/** The orchestrator runtime's environment: where the token points, for a step that resolves
 *  it at run time (app/common/storage.py). {} without storage. */
export function storageEnv(storage: StorageSpec | null): Record<string, string> {
  return storage ? { STORAGE_BUCKET: storage.bucketName } : {};
}
