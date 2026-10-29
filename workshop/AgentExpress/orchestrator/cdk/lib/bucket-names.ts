/**
 * The S3 bucket names a deployment gives its own buckets, in both naming schemes. Mirrors
 * bff/bucket_names.py and terraform/bucket_names.tf; orchestrator/tests/fixtures/
 * bucket_names.json holds all three to the same names (test/bucket-names.test.ts).
 *
 * A general purpose bucket name is unique across a partition, and a deleted name "might not
 * become available immediately"
 * (https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html), so a build
 * destroyed in one Region and deployed in another minutes later failed to create its bucket.
 *   * "legacy" (the default, `-c bucketNaming` unset): the names every deployment had before.
 *     Upgrading never renames a bucket: CloudFormation REPLACES a bucket whose name changes.
 *   * "regional": the Region ends every name. The console's deploy runner chooses it only for
 *     a build with nothing deployed, and records it on the build (deployer/runner.py).
 * A regional name over 63 characters keeps its prefix, `-<kind>-<account>-<region>` and an
 * 8-character digest of the agent name, and cuts the agent name to fit.
 *
 * The account may be a token (an environment-agnostic synth): an account ID is always 12
 * digits, so the length is computed with 12 and the token stays in the name.
 */
import * as crypto from "crypto";
import * as cdk from "aws-cdk-lib";

export const BUCKET_NAMING_SCHEMES = ["legacy", "regional"] as const;
export type BucketNaming = (typeof BUCKET_NAMING_SCHEMES)[number];
export type BucketKind = "assets" | "kbdocs" | "schemas" | "ui" | "storage";

/** kind -> the prefix before the agent name (storage is not agentcore-*: lib/storage.ts). */
const PREFIX: Record<BucketKind, string> = {
  assets: "agentcore-", kbdocs: "agentcore-", schemas: "agentcore-", ui: "agentcore-", storage: "",
};
const MAX_LEN = 63;
const ACCOUNT_LEN = 12;
const DIGEST_LEN = 8;
const RESERVED_PREFIXES = ["xn--", "sthree-", "amzn-s3-demo-"];

/** The `bucketNaming` context value, or a synth error naming the values allowed. */
export function bucketNamingOf(value: unknown): BucketNaming {
  const v = String(value ?? "legacy");
  if (!(BUCKET_NAMING_SCHEMES as readonly string[]).includes(v)) {
    throw new Error(`bucketNaming must be one of ${BUCKET_NAMING_SCHEMES.join(", ")}, not ${JSON.stringify(v)}.`);
  }
  return v as BucketNaming;
}

/** The bucket of `kind` for `agentName` in `account` and `region`, under `scheme`. */
export function bucketName(kind: BucketKind, agentName: string, account: string, region: string,
                           scheme: BucketNaming = "legacy"): string {
  const pre = PREFIX[kind];
  if (scheme === "legacy") {
    const d = agentName.replace(/_/g, "-");
    // The UI bucket always had the Region in this path (UiBucket, lib/orchestrator-stack.ts).
    return kind === "ui" ? `${pre}${d}-ui-${account}-${region}` : `${pre}${d}-${kind}-${account}`;
  }
  const d = agentName.replace(/_/g, "-").toLowerCase();
  const tail = `-${kind}-${account}-${region}`;
  const accountLen = cdk.Token.isUnresolved(account) ? ACCOUNT_LEN : account.length;
  const tailLen = tail.length - account.length + accountLen;
  let out = `${pre}${d}${tail}`;
  let len = pre.length + d.length + tailLen;
  if (len > MAX_LEN) {
    const h = crypto.createHash("sha256").update(agentName).digest("hex").slice(0, DIGEST_LEN);
    const keep = Math.max(0, MAX_LEN - pre.length - tailLen - DIGEST_LEN - 1);
    const t = d.slice(0, keep).replace(/-+$/, "");
    out = t ? `${pre}${t}-${h}${tail}` : `${pre}${h}${tail}`;
    len = out.length - account.length + accountLen;
  }
  if (len > MAX_LEN) {
    throw new Error(`the bucket name ${out} is over S3's ${MAX_LEN} characters.`);
  }
  if (RESERVED_PREFIXES.some((p) => out.startsWith(p))) {
    throw new Error(`the bucket name ${out} starts with a prefix S3 reserves (${RESERVED_PREFIXES.join(", ")}); use another agent name.`);
  }
  return out;
}
