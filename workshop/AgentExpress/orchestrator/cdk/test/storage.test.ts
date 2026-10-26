/**
 * Build storage (workflow.json `orchestrator.storage`, lib/storage.ts): set, a private,
 * versioned, encrypted, tagged bucket with ABAC on, the ${storage} token resolved where tools
 * read it, the code tool's grant on its prefix (and the build's key when it encrypts the
 * bucket), STORAGE_BUCKET on the orchestrator runtime; kept on destroy with `retain`. Not set,
 * nothing changes (storage-identity.test.ts). Same as terraform/storage.tf.
 */
import * as cdk from "aws-cdk-lib";
import { Annotations, Match, Template } from "aws-cdk-lib/assertions";
import { AwsSolutionsChecks } from "cdk-nag";
import * as fs from "fs";
import * as path from "path";

import { acknowledgeNagFindings } from "../lib/nag";
import { OrchestratorStack } from "../lib/orchestrator-stack";
import {
  resolveStorage, STORAGE_ABORT_UPLOAD_DAYS, STORAGE_FIELDS, STORAGE_NONCURRENT_DAYS, STORAGE_TOKEN,
  storageBucketName, storageOf,
} from "../lib/storage";

const ORCH_ROOT = path.join(__dirname, "..", "..");
const TF = path.join(ORCH_ROOT, "terraform");
const shipped = require(`${ORCH_ROOT}/app/workflow.json`);
const ACCOUNT = "123456789012";
const AGENT = "ax_1a2b3c4d";
const BUCKET = `ax-1a2b3c4d-storage-${ACCOUNT}`;
const CODE_TOOL = "renderer";
const CODE_DIR = path.join(ORCH_ROOT, "app", "tools", "_code", CODE_TOOL);
const kbKey = Object.keys(shipped.tools).find((k) => shipped.tools[k].type === "kb")!;

function withStorage(storage: any): any {
  return {
    ...shipped,
    orchestrator: { ...shipped.orchestrator, storage },
    tools: {
      ...shipped.tools,
      [kbKey]: { ...shipped.tools[kbKey], s3Uri: `${STORAGE_TOKEN}/kb/` },
      [CODE_TOOL]: {
        type: "lambda", description: "Renders files, written in the build.", arg: "query",
        toolSchema: [{ name: "render", description: "Render a file.", properties: { query: { type: "string", required: true } } }],
        code: { grants: { s3Prefix: `${STORAGE_TOKEN}/templates/`, s3Access: "readwrite" } },
      },
    },
  };
}

function synth(workflow: any): { t: Template; nag: string[] } {
  const app = new cdk.App();
  const stack = new OrchestratorStack(app, "TestStack", {
    env: { account: ACCOUNT, region: "us-east-1" },
    agentName: AGENT, builder: false,
    modelId: "us.anthropic.claude-haiku-4-5-20251001-v1:0",
    memoryEventExpiryDays: 30,
    idp: "cognito", createCognito: true, cognitoUserPoolId: "", cognitoClientId: "",
    cognitoDomainPrefix: "", auth0Domain: "", auth0ClientId: "",
    enableGateway: true, gatewayClientId: "", gatewayClientSecret: "dummy", gatewayAudience: "",
    toolApiKeys: {}, a2aTokens: {}, transactionSearchIndexingPercentage: 100,
    workflow,
  } as any);
  // As test/nag.test.ts runs the pack.
  acknowledgeNagFindings(stack);
  cdk.Aspects.of(app).add(new AwsSolutionsChecks());
  app.synth();
  const t = Template.fromStack(stack);
  const nag = Annotations.fromStack(stack).findError("*", Match.stringLikeRegexp("AwsSolutions-.*"))
    .map((e) => `${e.id}: ${String(e.entry.data).split("\n")[0]}`);
  return { t, nag };
}

const storageBucket = (t: Template) => Object.entries<any>(t.findResources("AWS::S3::Bucket"))
  .find(([, b]) => b.Properties?.BucketName === BUCKET);
const codeRole = (t: Template) => Object.values<any>(t.findResources("AWS::IAM::Role"))
  .find((r) => r.Properties.RoleName === `ToolLambda-${AGENT}-${CODE_TOOL}`);
const codeStatements = (t: Template) => codeRole(t).Properties.Policies.flatMap((p: any) => p.PolicyDocument.Statement);

describe("orchestrator.storage, unit", () => {
  it("is null unless create is true, and names the bucket after the build, outside agentcore-*", () => {
    expect(storageOf(shipped, AGENT, ACCOUNT)).toBeNull();
    expect(storageOf({ orchestrator: { storage: {} } }, AGENT, ACCOUNT)).toBeNull();
    expect(storageOf({ orchestrator: { storage: { create: false, retain: true } } }, AGENT, ACCOUNT)).toBeNull();
    expect(storageOf({ orchestrator: { storage: { create: true } } }, AGENT, ACCOUNT))
      .toEqual({ bucketName: BUCKET, uri: `s3://${BUCKET}`, retain: false });
    expect(storageOf({ orchestrator: { storage: { create: true, retain: true } } }, AGENT, ACCOUNT)!.retain).toBe(true);
    expect(storageBucketName("multiagent_orchestrator", ACCOUNT)).toBe(`multiagent-orchestrator-storage-${ACCOUNT}`);
    expect(BUCKET.startsWith("agentcore-")).toBe(false);
    expect(() => storageOf({ orchestrator: { storage: { create: "yes" } } }, AGENT, ACCOUNT)).toThrow(/true or false/);
    expect(() => storageOf({ orchestrator: { storage: { create: true } } }, "a".repeat(43), ACCOUNT)).toThrow(/63 characters/);
  });

  it("resolves the token only where it is read, and leaves a workflow without it the same object", () => {
    expect(resolveStorage(shipped, null)).toBe(shipped);
    const st = storageOf({ orchestrator: { storage: { create: true } } }, AGENT, ACCOUNT);
    expect(resolveStorage(shipped, st)).toBe(shipped);
    const wf = withStorage({ create: true });
    const out = resolveStorage({ ...wf, tools: { ...wf.tools, note: { type: "mcp", description: `see ${STORAGE_TOKEN}` } },
      orchestrator: { ...wf.orchestrator, interceptors: { request: { code: { grants: { s3Prefix: `${STORAGE_TOKEN}/i/` } } } } } }, st);
    expect(out.tools[CODE_TOOL].code.grants.s3Prefix).toBe(`s3://${BUCKET}/templates/`);
    expect(out.tools[kbKey].s3Uri).toBe(`s3://${BUCKET}/kb/`);
    expect(out.orchestrator.interceptors.request.code.grants.s3Prefix).toBe(`s3://${BUCKET}/i/`);
    expect(out.tools.note.description).toBe(`see ${STORAGE_TOKEN}`);       // not a field it is read in
    expect(wf.tools[CODE_TOOL].code.grants.s3Prefix).toBe(`${STORAGE_TOKEN}/templates/`); // the input is untouched
    expect(STORAGE_FIELDS).toEqual(["tools.*.code.grants.s3Prefix", "tools.*.s3Uri", "orchestrator.interceptors.*.code.grants.s3Prefix",
      "tools.*.templateS3Uri", "tools.*.outputPrefix", "agents.*.task.targets.*.uri"]);
  });

  it("refuses the token at synth when the build creates no storage", () => {
    expect(() => resolveStorage(withStorage({ create: false }), null))
      .toThrow(`workflow.json tools.${CODE_TOOL}.code.grants.s3Prefix uses ${STORAGE_TOKEN}, but this build creates no storage`);
  });
});

describe("orchestrator.storage, synthesized", () => {
  let on: { t: Template; nag: string[] };
  let kept: { t: Template; nag: string[] };
  beforeAll(() => {
    fs.mkdirSync(CODE_DIR, { recursive: true });
    fs.writeFileSync(path.join(CODE_DIR, "handler.py"), "def lambda_handler(event, context):\n    return {}\n");
    on = synth(withStorage({ create: true }));
    kept = synth(withStorage({ create: true, retain: true }));
  });
  afterAll(() => fs.rmSync(CODE_DIR, { recursive: true, force: true }));

  it("creates a private, versioned bucket encrypted with the build's key, tagged for code tools, ABAC on", () => {
    const [id, b] = storageBucket(on.t)!;
    const p = b.Properties;
    expect(p.PublicAccessBlockConfiguration).toEqual({ BlockPublicAcls: true, BlockPublicPolicy: true,
      IgnorePublicAcls: true, RestrictPublicBuckets: true });
    expect(p.VersioningConfiguration).toEqual({ Status: "Enabled" });
    expect(p.AbacStatus).toBe("Enabled");
    const sse = p.BucketEncryption.ServerSideEncryptionConfiguration[0];
    expect(sse.ServerSideEncryptionByDefault.SSEAlgorithm).toBe("aws:kms");
    expect(JSON.stringify(sse.ServerSideEncryptionByDefault.KMSMasterKeyID)).toContain("DataKey");
    expect(sse.BucketKeyEnabled).toBe(true);
    expect(p.Tags).toEqual(expect.arrayContaining([{ Key: "agentexpress:code-tools", Value: "true" }]));
    expect(p.LifecycleConfiguration.Rules).toEqual([{ Id: "storage-housekeeping", Status: "Enabled",
      ExpiredObjectDeleteMarker: true, NoncurrentVersionExpiration: { NoncurrentDays: STORAGE_NONCURRENT_DAYS },
      AbortIncompleteMultipartUpload: { DaysAfterInitiation: STORAGE_ABORT_UPLOAD_DAYS } }]);
    // Emptied (every version) and deleted with the stack.
    expect(b.DeletionPolicy).toBe("Delete");
    const autoDelete = Object.values<any>(on.t.findResources("Custom::S3AutoDeleteObjects"))
      .find((r) => JSON.stringify(r.Properties.BucketName).includes(id));
    expect(autoDelete).toBeDefined();
    // HTTPS only, and no grant to anyone on the bucket itself.
    const policy = Object.values<any>(on.t.findResources("AWS::S3::BucketPolicy"))
      .find((r) => JSON.stringify(r.Properties.Bucket).includes(id));
    const statements = policy.Properties.PolicyDocument.Statement;
    const deny = statements.find((s: any) => s.Effect === "Deny");
    expect(deny.Condition).toEqual({ Bool: { "aws:SecureTransport": "false" } });
    for (const s of statements.filter((x: any) => x.Effect === "Allow")) {
      expect(JSON.stringify(s.Principal)).not.toBe('{"AWS":"*"}');
      expect(s.Principal).not.toBe("*");
    }
  });

  it("keeps it on destroy with retain, encrypted with S3-managed keys, its policy too", () => {
    const [id, b] = storageBucket(kept.t)!;
    expect(b.DeletionPolicy).toBe("Retain");
    expect(b.Properties.BucketEncryption.ServerSideEncryptionConfiguration[0].ServerSideEncryptionByDefault)
      .toEqual({ SSEAlgorithm: "AES256" });
    expect(Object.values<any>(kept.t.findResources("Custom::S3AutoDeleteObjects"))
      .find((r) => JSON.stringify(r.Properties.BucketName).includes(id))).toBeUndefined();
    const policy = Object.values<any>(kept.t.findResources("AWS::S3::BucketPolicy"))
      .find((r) => JSON.stringify(r.Properties.Bucket).includes(id));
    expect(policy.DeletionPolicy).toBe("Retain");
  });

  it("grants a code tool its prefix of the bucket, and the build's key through S3 only when it encrypts it", () => {
    const st = codeStatements(on.t);
    const prefix = st.find((s: any) => s.Sid === "ItsPrefix");
    expect(prefix.Resource).toBe(`arn:aws:s3:::${BUCKET}/templates/*`);
    expect(prefix.Condition.StringEquals).toEqual({ "aws:ResourceTag/agentexpress:code-tools": "true" });
    const key = st.find((s: any) => s.Sid === "UseStorageKey");
    expect(key.Action).toEqual(["kms:Decrypt", "kms:GenerateDataKey"]);
    expect(JSON.stringify(key.Resource)).toContain("DataKey");
    expect(key.Condition).toEqual({ StringEquals: { "kms:ViaService": "s3.us-east-1.amazonaws.com" } });
    const fn = Object.values<any>(on.t.findResources("AWS::Lambda::Function"))
      .find((f) => f.Properties.FunctionName === `ToolLambda-${AGENT}-${CODE_TOOL}`);
    expect(fn.Properties.Environment.Variables.S3_PREFIX).toBe(`s3://${BUCKET}/templates/`);
    expect(codeStatements(kept.t).find((s: any) => s.Sid === "UseStorageKey")).toBeUndefined();
    // Its boundary holds the same grant, so the boundary does not cut it off.
    const boundary = Object.values<any>(on.t.findResources("AWS::IAM::ManagedPolicy"))
      .find((p) => p.Properties.ManagedPolicyName === `ToolLambda-${AGENT}-${CODE_TOOL}-boundary`);
    expect(boundary.Properties.PolicyDocument.Statement.map((s: any) => s.Sid)).toContain("UseStorageKey");
  });

  it("reads the kb tool's documents from the bucket, and tells the runtime where it is", () => {
    expect(JSON.stringify(on.t.toJSON())).toContain(`arn:aws:s3:::${BUCKET}/kb/*`);
    const runtime = Object.values<any>(on.t.findResources("AWS::BedrockAgentCore::Runtime"))
      .find((r) => r.Properties.AgentRuntimeName === AGENT);
    expect(runtime.Properties.EnvironmentVariables.STORAGE_BUCKET).toBe(BUCKET);
  });

  it("has no unacknowledged cdk-nag finding", () => {
    expect(on.nag).toEqual([]);
    expect(kept.nag).toEqual([]);
  });

  it("matches the Terraform path", () => {
    const tf = fs.readFileSync(path.join(TF, "storage.tf"), "utf8");
    for (const s of ['"${replace(var.agent_name, "_", "-")}-storage-${local.account_id}"', "force_destroy = !local.storage_retain",
      '"agentexpress:code-tools" = "true"', 'status = "Enabled"', "block_public_acls       = true",
      "block_public_policy     = true", "ignore_public_acls      = true", "restrict_public_buckets = true",
      'local.storage_kms ? "aws:kms" : "AES256"', "aws_kms_key.data.arn", "bucket_key_enabled = local.storage_kms ? true : null",
      'id     = "storage-housekeeping"', "expired_object_delete_marker = true",
      `noncurrent_days = ${STORAGE_NONCURRENT_DAYS}`, `days_after_initiation = ${STORAGE_ABORT_UPLOAD_DAYS}`,
      '"aws:SecureTransport" = "false"', "STORAGE_BUCKET = local.storage_bucket_name", "local.vocab.storageToken.values[0]",
      "but this build creates no storage: set orchestrator.storage.create to true."]) expect(tf).toContain(s);
    const code = fs.readFileSync(path.join(TF, "tools_code.tf"), "utf8");
    for (const s of ['"UseStorageKey"', '["kms:Decrypt", "kms:GenerateDataKey"]', '"kms:ViaService" = "s3.${var.region}.amazonaws.com"',
      "replace(try(g.s3Prefix, \"\"), local.storage_token, local.storage_uri)"]) expect(code).toContain(s);
    expect(fs.readFileSync(path.join(TF, "kb.tf"), "utf8"))
      .toContain('replace(try(local.kb_spec.s3Uri, ""), local.storage_token, local.storage_uri)');
    expect(fs.readFileSync(path.join(TF, "main.tf"), "utf8")).toContain("local.export_env, local.storage_env)");
  });

  it("lets a build's deploy role create and empty the bucket, and nothing else of the account's", () => {
    const doc = JSON.parse(fs.readFileSync(path.join(TF, "deploy-role-policy.json"), "utf8"));
    const st = doc.Statement.find((s: any) => s.Sid === "S3ManageBuildStorage");
    expect(st.Resource).toEqual(["arn:aws:s3:::ax-*-storage-*", "arn:aws:s3:::ax-*-storage-*/*"]);
    const glob = new RegExp(`^${st.Resource[0].replace("arn:aws:s3:::", "").split("*").join(".*")}$`);
    expect(glob.test(BUCKET)).toBe(true);
    for (const a of ["s3:CreateBucket", "s3:DeleteBucket", "s3:PutBucketAbac", "s3:TagResource", "s3:PutBucketVersioning",
      "s3:PutEncryptionConfiguration", "s3:PutBucketPublicAccessBlock", "s3:PutLifecycleConfiguration", "s3:PutBucketPolicy",
      "s3:ListBucketVersions", "s3:DeleteObjectVersion"]) expect(st.Action).toContain(a);
    // Not the objects' contents: a deploy never reads or writes what the tools keep there.
    expect(st.Action).not.toContain("s3:GetObject");
    expect(st.Action).not.toContain("s3:PutObject");
  });
});
