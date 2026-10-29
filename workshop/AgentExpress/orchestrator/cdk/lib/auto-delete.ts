/**
 * A BucketDeployment into a bucket with `autoDeleteObjects: true` uploads only once the
 * bucket's emptier (its Custom::S3AutoDeleteObjects resource) exists.
 *
 * Without this the two are created side by side: the deployment depends on the bucket, the
 * emptier on the bucket policy, and neither on the other. Seen live: a first create failed
 * after the UI files had landed; the rollback could not delete the UI bucket (BucketNotEmpty,
 * "Deletion fails for buckets that have contents":
 * https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-s3-bucket.html)
 * and the stack ended ROLLBACK_FAILED. With the dependency, the files never land before the
 * emptier exists, and a delete or rollback removes the deployment, then the emptier (which
 * empties the bucket), then the bucket. For a bucket that only a deployment fills, the CDK
 * docs recommend autoDeleteObjects over turning retainOnDelete off
 * (https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.aws_s3_deployment-readme.html).
 */
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";

/** The construct id aws-cdk-lib gives a bucket's emptier (aws-s3 BucketAutoDeleteObjects). */
export const AUTO_DELETE_CHILD = "AutoDeleteObjectsCustomResource";
/** ...and a BucketDeployment's upload (its Custom::CDKBucketDeployment). Only the upload waits:
 *  the deployment's AWS CLI layer holds nothing of the bucket's. */
export const DEPLOYMENT_CHILD = "CustomResource";

export function deployAfterAutoDelete(dep: s3deploy.BucketDeployment, bucket: s3.Bucket): void {
  const emptier = bucket.node.tryFindChild(AUTO_DELETE_CHILD);
  const upload = dep.node.tryFindChild(DEPLOYMENT_CHILD);
  if (!emptier || !upload) {
    throw new Error(`${bucket.node.path} has no ${AUTO_DELETE_CHILD} or ${dep.node.path} no ${DEPLOYMENT_CHILD}: ` +
      "set autoDeleteObjects on the bucket, or (after an aws-cdk-lib upgrade) update the ids in lib/auto-delete.ts.");
  }
  upload.node.addDependency(emptier);
}
