/**
 * Keeping the files runs make (workflow.json `orchestrator.files`, app/common/artifacts.py):
 * what agents save under runs/<session>/outputs/ and the code the codeInterpreter tool ran
 * under runs/<session>/code/, beside the inputs and images already kept there. Shown on a
 * run's Files tab and the app's Files page (bff/runartifacts.py).
 *
 * Opt-in. Setting `orchestrator.files` (even {}) creates the assets bucket when no agent
 * needed it yet, lets every dedicated runtime write outputs and code, lets the page read a
 * text file through its link (CORS GET), and expires everything under runs/ after
 * retentionDays (default 90). A deployment without it keeps exactly what it kept before,
 * with no expiry. Mirrors terraform/run_files.tf.
 */
import * as iam from "aws-cdk-lib/aws-iam";

import { keyDefault } from "./defaults";

/** S3 lifecycle expiration takes a positive whole number of days; ten years only catches a
 *  typo. Same bound as FILES_MAX_DAYS in bff/validate_build.py and validate.ts. */
export const FILES_MAX_DAYS = 3650;

/** The days a run's files are kept, or null when the workflow does not set
 *  `orchestrator.files`. Throws at synth for a value S3 would refuse, with the words the
 *  validators use. */
export function filesRetentionDays(workflow: any): number | null {
  const files = workflow?.orchestrator?.files;
  if (files === undefined || files === null) return null;
  const days = files?.retentionDays ?? keyDefault<Record<string, number>>("orchestrator", "files").retentionDays;
  if (!(typeof days === "number" && Number.isInteger(days) && days >= 1 && days <= FILES_MAX_DAYS)) {
    throw new Error(`orchestrator.files.retentionDays must be a whole number of days, 1 to ${FILES_MAX_DAYS}.`);
  }
  return days;
}

/** A dedicated agent saves its files and keeps its sandbox code from its own runtime: only
 *  those two folders. Mirrors aws_iam_role_policy.subagent_save_files. */
export function saveRunFilesStatement(bucketArn: string): iam.PolicyStatement {
  return new iam.PolicyStatement({
    sid: "SaveRunFiles", actions: ["s3:PutObject"],
    resources: [`${bucketArn}/runs/*/outputs/*`, `${bucketArn}/runs/*/code/*`],
  });
}

/** The Files tab lists a run's folder; the BFF already reads runs/*. Only keys under runs/.
 *  Mirrors aws_iam_role_policy.bff_list_run_files. */
export function listRunFilesStatement(bucketArn: string): iam.PolicyStatement {
  return new iam.PolicyStatement({
    sid: "ListRunFiles", actions: ["s3:ListBucket"], resources: [bucketArn],
    conditions: { StringLike: { "s3:prefix": ["runs/*"] } },
  });
}
