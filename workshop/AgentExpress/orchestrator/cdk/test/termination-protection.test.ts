/**
 * Termination protection as `cdk deploy` sees it: bin/orchestrator.ts synthesized with the
 * context the CLI would pass (cdk.json's context, then each -c value as a string), read back
 * from the cloud assembly manifest. The CLI sets the deployed stack's protection to the
 * artifact's `terminationProtection ?? false` on every deploy, so an unset value turns it
 * OFF. A control plane must come out true; a workflow app and a build stack false.
 */
import * as cxapi from "aws-cdk-lib/cx-api";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

const CDK_ROOT = path.join(__dirname, "..");
const cdkJsonContext = JSON.parse(fs.readFileSync(path.join(CDK_ROOT, "cdk.json"), "utf8")).context;

/** Synthesize bin/orchestrator.ts with `-c` flags; the named stack's artifact. */
function deployedStack(flags: Record<string, string>, stackName: string): cxapi.CloudFormationStackArtifact {
  const outdir = fs.mkdtempSync(path.join(os.tmpdir(), "cdk-tp-"));
  const saved = { ...process.env };
  try {
    process.env.CDK_OUTDIR = outdir;
    process.env.CDK_DEFAULT_ACCOUNT = "123456789012";
    process.env.CDK_DEFAULT_REGION = "us-east-1";
    process.env.CDK_CONTEXT_JSON = JSON.stringify({
      ...cdkJsonContext, ...flags, "aws:cdk:bundling-stacks": [],
    });
    jest.isolateModules(() => { require("../bin/orchestrator"); });
    return new cxapi.CloudAssembly(outdir).getStackByName(stackName);
  } finally {
    process.env = saved;
    fs.rmSync(outdir, { recursive: true, force: true });
  }
}

// The documented control-plane deploy (DEPLOYMENT.md), flag for flag.
const CONTROL_PLANE = {
  agentName: "agentexpress", idp: "cognito", createCognito: "true", enableGateway: "true",
  consoleMode: "builder", hostedUiProviders: "FederateOIDC", demoVideoUrl: "https://example.com/demo",
};

describe("termination protection on cdk deploy", () => {
  jest.setTimeout(300000);
  it("is on for the control-plane deploy command, so a deploy keeps it on", () => {
    expect(deployedStack(CONTROL_PLANE, "agentexpress-stack").terminationProtection).toBe(true);
  });
  it("is off for that command with -c terminationProtection=false", () => {
    expect(deployedStack({ ...CONTROL_PLANE, terminationProtection: "false" }, "agentexpress-stack")
      .terminationProtection).toBe(false);
  });
  it("is off for a workflow app (consoleMode=app)", () => {
    expect(deployedStack({ consoleMode: "app", createCognito: "true" }, "multiagent-orchestrator-stack").terminationProtection).toBe(false);
  });
  it("is off for a build stack as deployer/runner.py deploys it", () => {
    const build = { agentName: "ax_test", idp: "cognito", createCognito: "true", enableGateway: "false", builder: "false" };
    expect(deployedStack(build, "ax-test-stack").terminationProtection).toBe(false);
  });
});
