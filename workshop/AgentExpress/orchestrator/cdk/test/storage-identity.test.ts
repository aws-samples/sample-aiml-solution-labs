/**
 * Build storage is OFF unless configured (workflow.json `orchestrator.storage`, lib/storage.ts):
 * a workflow that does not set it synthesizes byte-identically to the release before the
 * feature existed (v1.3.8, 7ae5e19). So are v1.3.9's other features, the Document generator
 * (source "docgen"), the deliver step (task.kind "deliver") and named gate options
 * (hitl.options, askPerson.options): these cases use none of them, and the templates are
 * compared whole. tests/test_v139_identity.py is the same for the Terraform plan.
 *
 * test/fixtures/storage-off/<case>.json is each case's template as that release synthesized
 * it, with asset hashes masked: an asset's hash is a digest of files any change to the app
 * moves, and is not part of what a workflow deploys. Everything else must be equal byte for
 * byte. Regenerate (only from a checkout of a release, never to make this pass):
 *   AX_WRITE_STORAGE_GOLDEN=1 npx jest test/storage-identity.test.ts
 *
 * The console's own deploy role (lib/builder-plane.ts, terraform/deploy-role-policy.json)
 * is the one expected difference, and only on the console: it must be allowed to create a
 * build's storage. A build's own stack (builder=false, what deployer/runner.py deploys)
 * has no deploy role and is compared whole.
 *
 * The region fixes add two more expected differences, removed by `withoutRegionFixes` and
 * nothing else: each Custom::CDKBucketDeployment depends on its bucket's
 * Custom::S3AutoDeleteObjects (lib/auto-delete.ts), and the console BFF may read a build's
 * images under the regional bucket name too (lib/builder-plane.ts). Every bucket name is
 * unchanged: the default bucketNaming is "legacy" (lib/bucket-names.ts), which renames nothing.
 */
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import * as fs from "fs";
import * as path from "path";

import { OrchestratorStack } from "../lib/orchestrator-stack";

const ORCH_ROOT = path.join(__dirname, "..", "..");
const GOLDEN = path.join(__dirname, "fixtures", "storage-off");
const shipped = require(`${ORCH_ROOT}/app/workflow.json`);
const WRITE = process.env.AX_WRITE_STORAGE_GOLDEN === "1";

function synth(workflow: any, overrides: Record<string, any> = {}): any {
  const app = new cdk.App();
  const stack = new OrchestratorStack(app, "TestStack", {
    env: { account: "123456789012", region: "us-east-1" },
    agentName: "multiagent_orchestrator",
    modelId: "us.anthropic.claude-haiku-4-5-20251001-v1:0",
    memoryEventExpiryDays: 30,
    idp: "cognito", createCognito: true, cognitoUserPoolId: "", cognitoClientId: "",
    cognitoDomainPrefix: "", auth0Domain: "", auth0ClientId: "",
    enableGateway: true, gatewayClientId: "", gatewayClientSecret: "dummy", gatewayAudience: "",
    toolApiKeys: {}, a2aTokens: {}, transactionSearchIndexingPercentage: 100,
    workflow,
    ...overrides,
  } as any);
  return Template.fromStack(stack).toJSON();
}

/** The template as text, with every asset hash (64 hex digits) masked, and the one value
 *  that differs on every synth by design: a Knowledge Base on your own bucket (s3Uri)
 *  ingests again on each deploy, keyed by the time (tool-plane.ts corpusHash `s3-<ms>`).
 *  The release tag in the default docs URL (docs_default.json `ref`) is masked too: every
 *  release moves it to its own tag. */
const DOCS_REF = /agentexpress-v\d+\.\d+\.\d+/g;
const normalized = (template: any) => JSON.stringify(template, null, 1)
  .replace(/[0-9a-f]{64}/g, "<asset-hash>").replace(/kb-ingest-s3-\d+/g, "kb-ingest-s3-<synth-time>")
  .replace(DOCS_REF, "<docs-ref>") + "\n";

/** A code tool, written in the build, granted an S3 prefix in a bucket of the customer's. */
const CODE_TOOL = "refundsCode";
const CODE_DIR = path.join(ORCH_ROOT, "app", "tools", "_code", CODE_TOOL);
const kbKey = Object.keys(shipped.tools ?? {}).find((k) => shipped.tools[k]?.type === "kb");

const BUILD = { agentName: "ax_1a2b3c4d", builder: false };
const CASES: Record<string, { workflow: any; props: Record<string, any> }> = {
  // The shipped workflow, as the console and as a build.
  console: { workflow: shipped, props: {} },
  build: { workflow: shipped, props: BUILD },
  // A build that keeps its run files (the assets bucket beside where storage would go).
  "build-files": { workflow: { ...shipped, orchestrator: { ...shipped.orchestrator, files: {} } }, props: BUILD },
  // A code tool granted s3://<bucket>/<prefix>/ in the customer's own bucket.
  "build-code-tool": {
    workflow: { ...shipped, tools: { ...shipped.tools, [CODE_TOOL]: {
      type: "lambda", description: "Refunds, written in the build.", arg: "question",
      toolSchema: [{ name: "lookup", description: "Look up a refund.", properties: { question: { type: "string", required: true } } }],
      code: { grants: { s3Prefix: "s3://amzn-s3-demo-bucket/refunds/", s3Access: "readwrite" } },
    } } },
    props: BUILD,
  },
  // The Knowledge Base reading the customer's own bucket (s3Uri).
  ...(kbKey ? { "build-kb-s3": {
    workflow: { ...shipped, tools: { ...shipped.tools, [kbKey]: { ...shipped.tools[kbKey], s3Uri: "s3://amzn-s3-demo-bucket/kb/" } } },
    props: BUILD,
  } } : {}),
};

/** Resources that may differ on the console only: the deploy role's managed policies. */
function withoutDeployPolicies(template: any): any {
  const out = JSON.parse(JSON.stringify(template));
  for (const [id, r] of Object.entries<any>(out.Resources ?? {})) {
    if (r.Type === "AWS::IAM::ManagedPolicy" && /^BuilderTerraformDeploy\d/.test(id)) delete out.Resources[id];
  }
  return out;
}

/** The region fixes' two intended changes, taken back out (see the header); nothing else. */
function withoutRegionFixes(template: any): any {
  const out = JSON.parse(JSON.stringify(template));
  for (const r of Object.values<any>(out.Resources ?? {})) {
    if (r.Type === "Custom::CDKBucketDeployment" && Array.isArray(r.DependsOn)) {
      r.DependsOn = r.DependsOn.filter((d: string) => !/AutoDeleteObjectsCustomResource[0-9A-F]{8}$/.test(d));
      if (!r.DependsOn.length) delete r.DependsOn;
    }
    for (const s of r.Type === "AWS::IAM::Policy" ? r.Properties.PolicyDocument.Statement : []) {
      const regional = /^arn:aws:s3:::agentcore-ax-\*-assets-\d{12}-[a-z0-9-]+\/runs\/\*$/;
      if (s.Action === "s3:GetObject" && Array.isArray(s.Resource) && s.Resource.length === 2 && regional.test(s.Resource[1])) {
        s.Resource = s.Resource[0];
      }
    }
  }
  return out;
}

describe("a workflow without orchestrator.storage", () => {
  beforeAll(() => {
    fs.mkdirSync(CODE_DIR, { recursive: true });
    fs.writeFileSync(path.join(CODE_DIR, "handler.py"), "def lambda_handler(event, context):\n    return {}\n");
  });
  afterAll(() => fs.rmSync(CODE_DIR, { recursive: true, force: true }));

  for (const [name, c] of Object.entries(CASES)) {
    it(`synthesizes byte-identically to v1.3.8: ${name}`, () => {
      const file = path.join(GOLDEN, `${name}.json`);
      const raw = synth(c.workflow, c.props);
      const got = WRITE ? raw : withoutRegionFixes(raw);
      if (WRITE) {
        fs.mkdirSync(GOLDEN, { recursive: true });
        fs.writeFileSync(file, normalized(got));
        return;
      }
      const want = fs.readFileSync(file, "utf8");
      if (name === "console") {
        // Everything but the deploy role's policies is unchanged.
        expect(normalized(withoutDeployPolicies(got))).toBe(normalized(withoutDeployPolicies(JSON.parse(want))));
      } else {
        expect(normalized(got)).toBe(want.replace(DOCS_REF, "<docs-ref>"));
      }
    });
  }
});
