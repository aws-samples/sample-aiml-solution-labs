/**
 * Track B docgen. The Document generator (`source: "docgen"` -> app/tools/docgen/): a Gateway
 * Lambda target that renders a PDF, PPTX or DOCX from JSON data, into a template from S3
 * or a built-in layout, and saves it with the run (runs/<run>/outputs/ in the assets
 * bucket) and under `outputPrefix`.
 *
 * Its role may write its own log group; read ONLY the template object, or the objects
 * under the template folder (`templateS3Uri`); write ONLY run outputs in the assets bucket
 * and objects under `outputPrefix`; read back the file it links to (a presigned link
 * works only if its signer may GetObject it) and who wrote a run file whose name it was
 * about to take (HeadObject, so a step's re-run replaces only its own earlier file).
 * Nothing else: no table, secret or other
 * bucket, and no s3:ListBucket. Mirrors terraform/docgen_tool.tf and the shared built-in
 * function resources in terraform/tools.tf.
 *   https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html
 *
 * Its libraries (requirements.txt) are installed into app/tools/docgen/ before the zip is
 * made (deployer/runner.py install_builtin_requirements): about 16 MB zipped and 43 MB
 * unpacked, inside Lambda's 50 MB zipped / 250 MB unzipped .zip limits, so no layer or
 * container image is needed.
 *   https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html
 *   https://docs.aws.amazon.com/lambda/latest/dg/python-package.html
 */
import * as fs from "fs";
import * as path from "path";
import * as cdk from "aws-cdk-lib";
import { aws_iam as iam, aws_lambda as lambda, aws_logs as logs } from "aws-cdk-lib";
import { Construct } from "constructs";

export const DOCGEN_SOURCE = "docgen";
/** The one tool the function publishes (handler.py TOOL). */
export const DOCGEN_TOOL = "generate_document";
/** The argument the runtime sets on every call (app/features/gateway/client.py _run_args)
 *  that the deploy publishes itself, so no workflow.json has to declare it: this run of the
 *  step, so its new attempt replaces the file its earlier attempt wrote. Mirrors
 *  docgen_published_args in terraform/tools.tf. */
export const DOCGEN_PUBLISHED_ARGS: Record<string, { type: string; required?: boolean; description: string }> = {
  attempt: { type: "string", required: false, description: "Set by the workflow on every call; leave it out." },
};

/** A Document generator's toolSchema as the deploy publishes it: generate_document with the
 *  arguments the runtime sets added when the config leaves them out. Any other entry, and
 *  an argument the config declares, is kept as written. */
export function withDocgenArgs<T extends { name: string; properties?: Record<string, any> }>(schema: T[]): T[] {
  return schema.map((s) => (s.name !== DOCGEN_TOOL ? s : {
    ...s, properties: { ...Object.fromEntries(Object.entries(DOCGEN_PUBLISHED_ARGS).filter(([k]) => !(k in (s.properties ?? {})))), ...(s.properties ?? {}) },
  }));
}

/** The Track E build-storage token (`${storage}/...`). The stack resolves it to the build's
 *  bucket before the tools reach the tool plane (lib/storage.ts resolveStorage, which
 *  refuses it when the build creates no storage). A location still naming it here was not
 *  resolved, and is refused at synth rather than granted on a bucket that does not exist. */
export const STORAGE_TOKEN = "${storage}";
export function resolveStorage(uri: string, key: string): string {
  if (uri.startsWith(STORAGE_TOKEN)) {
    throw new Error(`tools.${key}: ${STORAGE_TOKEN} needs the build's storage (orchestrator.storage): set orchestrator.storage.create to true.`);
  }
  return uri;
}

/** s3://bucket/key -> [bucket, key]. */
function splitS3(uri: string): [string, string] {
  const [bucket, ...rest] = uri.replace(/^s3:\/\//, "").split("/");
  return [bucket, rest.join("/")];
}

/** The object ARN pattern a location covers: one object, or everything under a folder. */
export function s3ObjectArn(uri: string): string {
  const [bucket, key] = splitS3(uri);
  return key === "" || key.endsWith("/") ? `arn:aws:s3:::${bucket}/${key}*` : `arn:aws:s3:::${bucket}/${key}`;
}

export interface DocgenSpec {
  formats?: string[];
  templateS3Uri?: string;
  outputPrefix?: string;
}

/** The policy statements of the function's own role, besides its log writes. The link it
 *  returns is to the output prefix's copy when there is one, else to the run's copy, so
 *  only that one is readable. */
export function docgenStatements(spec: DocgenSpec, assetsBucketArn: string): iam.PolicyStatement[] {
  const out: iam.PolicyStatement[] = [];
  if (spec.templateS3Uri) {
    out.push(new iam.PolicyStatement({
      sid: "ReadItsTemplates", actions: ["s3:GetObject"], resources: [s3ObjectArn(spec.templateS3Uri)],
    }));
  }
  const runOutputs = `${assetsBucketArn}/runs/*/outputs/*`;
  // GetObject on the run's files with or without an output prefix: before it replaces a
  // taken name it reads (HeadObject) which step and attempt wrote it (handler.py _claim).
  out.push(new iam.PolicyStatement({
    sid: "SaveWithTheRun",
    actions: ["s3:PutObject", "s3:GetObject"],
    resources: [runOutputs],
  }));
  if (spec.outputPrefix) {
    out.push(new iam.PolicyStatement({
      sid: "WriteItsOutputs", actions: ["s3:PutObject", "s3:GetObject"], resources: [s3ObjectArn(spec.outputPrefix)],
    }));
  }
  return out;
}

/** The function for tool `name`, named and logged like the other built-in ones
 *  (ToolLambda-<agentName>-<name>, /aws/lambda/ToolLambda-<agentName>-<name>). */
export function docgenToolFunction(scope: Construct, name: string, raw: DocgenSpec, agentName: string,
  orchRoot: string, retention: logs.RetentionDays, assets: { bucketName: string; bucketArn: string } | undefined): lambda.Function {
  // The file is always saved with its run, so the run files must exist (orchestrator.files).
  if (!assets) {
    throw new Error(`tools.${name}: the Document generator saves each file with its run: set orchestrator.files (even {}) so this deployment keeps run files.`);
  }
  const folder = path.join(orchRoot, "app", "tools", DOCGEN_SOURCE);
  // Its libraries must be in the folder the zip is made from, or every call fails on import.
  if (!fs.existsSync(path.join(folder, "pptx", "__init__.py"))) {
    throw new Error(`tools.${name}: the Document generator's libraries are not installed in app/tools/docgen/. `
      + `A console deploy installs them; from a checkout run: python3 -m pip install -r app/tools/docgen/requirements.txt `
      + `-t app/tools/docgen --platform manylinux2014_x86_64 --implementation cp --python-version 3.12 --only-binary=:all:`);
  }
  const spec: DocgenSpec = {
    formats: raw.formats,
    ...(raw.templateS3Uri ? { templateS3Uri: resolveStorage(raw.templateS3Uri, name) } : {}),
    ...(raw.outputPrefix ? { outputPrefix: resolveStorage(raw.outputPrefix, name) } : {}),
  };
  const logGroup = new logs.LogGroup(scope, `ToolLambdaLogGroup-${name}`, {
    logGroupName: `/aws/lambda/ToolLambda-${agentName}-${name}`,
    retention,
    removalPolicy: cdk.RemovalPolicy.DESTROY,
  });
  // Not the default role: AWSLambdaBasicExecutionRole can create log groups anywhere.
  const role = new iam.Role(scope, `ToolLambdaRole-${name}`, {
    assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
  });
  logGroup.grantWrite(role);
  const fn = new lambda.Function(scope, `ToolLambda-${name}`, {
    logGroup,
    role,
    functionName: `ToolLambda-${agentName}-${name}`,
    runtime: lambda.Runtime.PYTHON_3_12,
    handler: "handler.lambda_handler",
    code: lambda.Code.fromAsset(folder),
    // Reading a template and rendering a file of up to 25 MB. Mirrors terraform/tools.tf.
    timeout: cdk.Duration.seconds(60),
    memorySize: 512,
    environment: {
      FORMATS: (spec.formats ?? []).join(","),
      ASSETS_BUCKET: assets.bucketName,
      ...(spec.templateS3Uri ? { TEMPLATE_S3_URI: spec.templateS3Uri } : {}),
      ...(spec.outputPrefix ? { OUTPUT_PREFIX: spec.outputPrefix } : {}),
    },
  });
  for (const s of docgenStatements(spec, assets.bucketArn)) fn.addToRolePolicy(s);
  return fn;
}
