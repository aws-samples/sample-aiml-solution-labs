/**
 * Synthesized-template assertions.
 *
 * The unit tests in config-plane.test.ts check the projections; this checks that
 * what they produce actually reaches CloudFormation. It synthesizes the real stack
 * against the real `workflow.json`, so it also covers the construct code that has
 * no testable pure function behind it (the Cognito groups, the route wiring, the
 * env var the BFF reads).
 *
 * No credentials and no container builder are needed. The account and region are
 * pinned below (so nothing is an unresolved token), and `Template.fromStack` renders
 * the template without staging the cloud assembly — which is the step that would
 * bundle the Docker image. Verified by running this file with
 * `CDK_DOCKER=/nonexistent/builder`.
 */

import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import * as path from "path";
import * as fs from "fs";
import * as os from "os";

import {
  a2aLambdaAgents, customEvaluatorsOf, customPoliciesOf, memoryStrategiesFor, OrchestratorStack, PRIVILEGED_ACTIONS,
  privilegedGrants, stageBffPackage,
} from "../lib/orchestrator-stack";

const ORCH_ROOT = path.join(__dirname, "..", "..");
const shipped = require(`${ORCH_ROOT}/app/workflow.json`);

function synth(overrides: Record<string, any> = {}) {
  const app = new cdk.App();
  const stack = new OrchestratorStack(app, "TestStack", {
    env: { account: "123456789012", region: "us-east-1" },
    agentName: "multiagent_orchestrator",
    modelId: "us.anthropic.claude-haiku-4-5-20251001-v1:0",
    memoryEventExpiryDays: 30,
    idp: "cognito",
    createCognito: true,
    cognitoUserPoolId: "",
    cognitoClientId: "",
    cognitoDomainPrefix: "",
    auth0Domain: "",
    auth0ClientId: "",
    enableGateway: true,
    gatewayClientId: "",
    gatewayClientSecret: "dummy",
    gatewayAudience: "",
    toolApiKeys: {},
    a2aTokens: {},
    transactionSearchIndexingPercentage: 100,
    ...overrides,
  });
  return Template.fromStack(stack);
}

// Synthesizing is the slow part, so do it once and assert many things.
let template: Template;
beforeAll(() => {
  template = synth();
});

describe("Cognito", () => {
  it("creates one group per group named in authorization.actions", () => {
    const groups = template.findResources("AWS::Cognito::UserPoolGroup");
    const names = Object.values(groups)
      .map((g: any) => g.Properties.GroupName)
      .sort();
    const expected = [
      ...new Set(Object.values<string[]>(shipped.authorization.actions).flat()),
    ].sort();
    expect(names).toEqual(expected);
  });

  it("describes each group with the actions it grants", () => {
    // So an operator reading the Cognito console can see what a group is for
    // without opening workflow.json.
    const groups = template.findResources("AWS::Cognito::UserPoolGroup");
    const byName: Record<string, string> = {};
    for (const g of Object.values<any>(groups)) {
      byName[g.Properties.GroupName] = g.Properties.Description;
    }
    for (const [action, allowed] of Object.entries<string[]>(shipped.authorization.actions)) {
      for (const group of allowed) {
        expect(byName[group]).toContain(action);
      }
    }
  });

  it("disables self-signup", () => {
    // The UI sits on a public CloudFront URL. This was `true`, diverging from the
    // Terraform path and leaving open registration on a public endpoint.
    template.hasResourceProperties("AWS::Cognito::UserPool", {
      AdminCreateUserConfig: { AllowAdminCreateUserOnly: true },
    });
  });
});

describe("HTTP API", () => {
  it("exposes /api/me", () => {
    template.hasResourceProperties("AWS::ApiGatewayV2::Route", {
      RouteKey: "GET /api/me",
    });
  });

  it("puts the JWT authorizer on every /api route", () => {
    // A route deployed without an authorizer is open to the internet. Every one of
    // them must carry the authorizer, not just most.
    const routes = template.findResources("AWS::ApiGatewayV2::Route");
    const apiRoutes = Object.values<any>(routes).filter((r) =>
      String(r.Properties.RouteKey).includes("/api/")
    );
    expect(apiRoutes.length).toBeGreaterThan(10);
    for (const r of apiRoutes) {
      expect(r.Properties.AuthorizationType).toBe("JWT");
      expect(r.Properties.AuthorizerId).toBeDefined();
    }
  });

  it("wires every mutating route the RBAC guards cover", () => {
    const keys = Object.values<any>(template.findResources("AWS::ApiGatewayV2::Route")).map(
      (r) => r.Properties.RouteKey
    );
    for (const k of [
      "POST /api/sessions/{id}/decision",
      "POST /api/sessions/{id}/rerun",
      "POST /api/sessions/{id}/cancel",
      "POST /api/sessions/{id}/evaluate",
      "POST /api/insights/run",
      "DELETE /api/sessions/{id}",
    ]) {
      expect(keys).toContain(k);
    }
  });
});

describe("the BFF deployment package", () => {
  /**
   * The workflow reaches the BFF in its PACKAGE, not in its environment.
   *
   * It used to travel in a `WORKFLOW_JSON` env var holding a projection this file
   * built. Lambda caps the whole environment at 4 KB and that quota cannot be
   * raised, so both IaC paths carried a 3400-byte guard and the shipped ten-agent
   * workflow measured 3153 bytes — about eleven agents before a customer's deploy
   * failed telling them to shorten an agent name. bff/workflow.py does the
   * projection now, at request time, from the file staged here.
   *
   * What the projection CONTAINS is asserted in tests/test_bff_projection.py, next
   * to the implementation. What this file owns is that the file gets there.
   */
  function bffFunction(): any {
    const fns = Object.values<any>(template.findResources("AWS::Lambda::Function")).filter(
      (f) => f.Properties?.Handler === "handler.handler"
        && String(f.Properties?.FunctionName ?? "").startsWith("AgentCoreBFF-")
    );
    expect(fns).toHaveLength(1);
    return fns[0];
  }

  it("ships no WORKFLOW_JSON, because that was an eleven-agent ceiling", () => {
    const env = bffFunction().Properties.Environment.Variables;
    expect(env).not.toHaveProperty("WORKFLOW_JSON");
    // The vars that remain are ARNs, table names and the model id.
    // The BUILDS_* and DEPLOY_PROJECT names are the builder plane's (lib/builder-plane.ts).
    expect(Object.keys(env).sort()).toEqual([
      "BFF_ROLE_ARN",
      "BUILDS_BUCKET",
      "BUILDS_TABLE",
      "DEPLOY_PROJECT",
      "DEPLOY_ROLE_ARN",
      "EVENTS_TABLE",
      "MODEL_ID",
      "RUNTIME_ARN",
      "SECRETS_PREFIX",
      "STATUS_TABLE",
      "TELEMETRY_TABLE",
    ]);
  });

  it("keeps the whole environment well inside Lambda's 4 KB limit", () => {
    // Trivially true now, and kept because the limit is the reason the projection
    // moved: if anything large is ever put back in here, this is where it shows up.
    const env = bffFunction().Properties.Environment.Variables;
    const bytes = Object.entries(env).reduce((n, [k, v]) => n + k.length + String(v).length, 0);
    expect(bytes).toBeLessThan(2048);
  });

  it("stages workflow.json next to the handler, byte-identical to the source", () => {
    const staged = fs.mkdtempSync(path.join(os.tmpdir(), "bff-stage-"));
    stageBffPackage(shipped, staged);

    const files = fs.readdirSync(staged).sort();
    expect(files).toContain("handler.handler".split(".")[0] + ".py");
    expect(files).toContain("workflow.py");
    expect(files).toContain("workflow.json");
    // The whole point: bff/workflow.py finds the customer's config beside it.
    expect(JSON.parse(fs.readFileSync(path.join(staged, "workflow.json"), "utf8"))).toEqual(
      shipped
    );
  });

  it("stages no __pycache__, which fromAsset(bff/) used to ship", () => {
    // Including .pyc files built by a different Python minor version than the
    // Lambda's runtime.
    const staged = fs.mkdtempSync(path.join(os.tmpdir(), "bff-stage-"));
    stageBffPackage(shipped, staged);
    expect(fs.readdirSync(staged)).not.toContain("__pycache__");
  });

  it("synthesizes a workflow far past the old eleven-agent ceiling", () => {
    // THE HEADLINE CLAIM, asserted rather than argued. Twenty-five agents with long
    // names is roughly twice what the WORKFLOW_JSON environment variable could hold;
    // under the old design this threw at synth with "shorten your agent names".
    //
    // 25 is not the new limit — there isn't one worth asserting. It is far enough
    // past 11 to show the constraint was REMOVED rather than raised, which is all
    // SSM Parameter Store would have done (4 KB standard, 8 KB advanced and billed).
    const many: Record<string, any> = {};
    for (let n = 0; n < 25; n++) {
      many[`specialist_review_agent_${String(n).padStart(2, "0")}`] = {
        name: `Specialist Review and Escalation Agent number ${String(n).padStart(2, "0")}`,
        runtime: "main",
        maxTokens: 4000,
        access: ["Upstream assets (orchestrator graph state)"],
        agentcore: { evaluations: { enabled: true, auto: false } },
      };
    }
    const big = {
      ...shipped,
      agents: { ...shipped.agents, ...many },
      steps: [...shipped.steps, { parallel: Object.keys(many), gateId: "bulk" }],
    };
    expect(() => synth({ workflow: big, a2aTokens: {} })).not.toThrow();

    // And the projection that WOULD have been shipped in the env var is comfortably
    // over the old budget, which is what makes the point concrete.
    const staged = fs.mkdtempSync(path.join(os.tmpdir(), "bff-stage-big-"));
    stageBffPackage(big, staged);
    const bundled = fs.readFileSync(path.join(staged, "workflow.json"), "utf8");
    expect(bundled.length).toBeGreaterThan(3400);
    expect(Object.keys(JSON.parse(bundled).agents)).toHaveLength(
      Object.keys(shipped.agents).length + 25
    );
  });

  it("stages the same python files Terraform's archive_file does", () => {
    // Two IaC paths, one package. A module present in one and not the other is an
    // ImportError on half your deployments.
    const staged = fs.mkdtempSync(path.join(os.tmpdir(), "bff-stage-"));
    stageBffPackage(shipped, staged);
    const stagedPy = fs
      .readdirSync(staged)
      .filter((f) => f.endsWith(".py"))
      .sort();
    const sourcePy = fs
      .readdirSync(path.join(__dirname, "..", "..", "bff"))
      .filter((f) => f.endsWith(".py"))
      .sort();
    expect(stagedPy).toEqual(sourcePy);
    // And Terraform's fileset pattern must be the recursive one, or a subpackage
    // added later is silently dropped from that path only.
    const bffTf = fs.readFileSync(
      path.join(__dirname, "..", "..", "terraform", "bff.tf"),
      "utf8"
    );
    expect(bffTf).toContain('fileset("${path.module}/../bff", "**/*.py")');
    expect(bffTf).toContain('filename = "workflow.json"');
  });
});

describe("the tool plane", () => {
  it("creates one Gateway target per tools entry", () => {
    const targets = Object.values<any>(
      template.findResources("AWS::BedrockAgentCore::GatewayTarget")
    ).map((t) => t.Properties.Name);
    expect(targets.sort()).toEqual(Object.keys(shipped.tools).sort());
  });

  /**
   * The Cedar statements, flattened to text.
   *
   * Each one is an `Fn::Join` rather than a literal, because the gateway ARN is a
   * `Fn::GetAtt` resolved at deploy time — so the fragments have to be concatenated
   * before the policy text can be read.
   */
  // TOOL NAMES RESOLVED BY TYPE, not written down. These tests said `shipped.tools.kb`
  // and `"docs"`, which made them assertions about the SAMPLE's tool set: a foreign
  // workflow with one `policy_docs` Knowledge Base and no web search failed three of
  // them, reporting a framework defect where there was none. `itIfTool` skips instead,
  // because "this deployment has no MCP target" is not a bug.
  const toolOfType = (t: string): string | undefined =>
    Object.keys(shipped.tools ?? {}).find(
      (n) => String(shipped.tools[n].type ?? "").toLowerCase() === t
    );
  const itIfTool = (t: string) => (toolOfType(t) ? it : it.skip);

  function cedarStatements(): string[] {
    return Object.values<any>(template.findResources("AWS::BedrockAgentCore::Policy")).map((p) => {
      const s = p.Properties.Definition.Cedar.Statement;
      if (typeof s === "string") return s;
      return (s["Fn::Join"][1] as any[])
        .map((part) => (typeof part === "string" ? part : "<resolved-at-deploy>"))
        .join("");
    });
  }

  it("emits one Cedar permit per declared tool, attached to a policy engine", () => {
    template.resourceCountIs("AWS::BedrockAgentCore::PolicyEngine", 1);
    const statements = cedarStatements();
    expect(statements).toHaveLength(Object.keys(shipped.tools).length);
    for (const name of Object.keys(shipped.tools)) {
      // Every declared tool is permitted, by name or as a target action group.
      // Anything NOT declared is denied by Cedar's default-deny.
      expect(statements.some((s) => s.includes(`AgentCore::Action::"${name}`))).toBe(true);
    }
  });

  itIfTool("websearch")("permits web search BY NAME and the MCP target at target level", () => {
    // Not cosmetic. Measured on a live gateway: a target-level permit did NOT
    // authorize the connector's tool — `action in AgentCore::Action::"websearch"`
    // produced ToolDenied for websearch___WebSearch. A remote MCP server's tool
    // names are unknown at deploy time, so that one has to stay target-level.
    const statements = cedarStatements();
    const ws = toolOfType("websearch")!;
    expect(
      statements.some((s) => s.includes(`action == AgentCore::Action::"${ws}___WebSearch"`))
    ).toBe(true);
    const mcp = toolOfType("mcp");
    if (mcp) {
      expect(statements.some((s) => s.includes(`action in AgentCore::Action::"${mcp}"`))).toBe(true);
    }
    const kb = toolOfType("kb");
    if (kb) {
      expect(
        statements.some((s) => s.includes(`action == AgentCore::Action::"${kb}___retrieve"`))
      ).toBe(true);
    }
  });

  itIfTool("kb")("carries the kb corpus restriction into the permit", () => {
    const name = toolOfType("kb")!;
    const kb = cedarStatements().find((s) => s.includes(`${name}___retrieve`))!;
    expect(kb).toBeDefined();
    for (const corpus of shipped.tools[name].corpora) expect(kb).toContain(corpus);
    expect(kb).toContain("context.input has filter");
  });

  it("scopes every permit to this gateway", () => {
    for (const s of cedarStatements()) {
      expect(s).toContain("resource == AgentCore::Gateway::");
    }
  });

  itIfTool("mcp")("sets the MCP target's listingMode from config", () => {
    const name = toolOfType("mcp")!;
    const target = Object.values<any>(
      template.findResources("AWS::BedrockAgentCore::GatewayTarget")
    ).find((t) => t.Properties.Name === name);
    expect(JSON.stringify(target)).toContain(shipped.tools[name].listingMode ?? "DEFAULT");
  });
});

describe("idp=none", () => {
  it("is rejected while authorization.actions is non-empty", () => {
    // Belt and braces: config-plane.test.ts asserts the validator, this asserts the
    // stack actually calls it.
    expect(() => synth({ idp: "none", createCognito: false, enableGateway: false })).toThrow(
      /idp = "none" deploys the API with no authorizer/
    );
  });
});

describe("a remote (a2a) agent", () => {
  // `runtime: "a2a"` is an agent this deployment does NOT operate. The thing to prove
  // at the stack level is a negative: it must provision no compute, no IAM and no log
  // group, because there is nothing of ours to run. A dedicated runtime accidentally
  // created for it would be a container that boots, finds no module under
  // app/subagents/<id>/, and crash-loops — while the orchestrator happily called the
  // partner's URL and the run succeeded, so nothing would point at the waste.
  const withRemote = {
    ...shipped,
    agents: {
      ...shipped.agents,
      partner_check: {
        name: "Partner Check",
        runtime: "a2a",
        agentCard: "https://agents.partner.example/check",
        produces: "partner-assessment",
      },
    },
    steps: [...shipped.steps, { agent: "partner_check" }],
  };

  let remoteTemplate: Template;
  beforeAll(() => {
    remoteTemplate = synth({ workflow: withRemote, a2aTokens: {} });
  });

  it("provisions no AgentCore Runtime of its own", () => {
    const runtimes = Object.values<any>(
      remoteTemplate.findResources("AWS::BedrockAgentCore::Runtime")
    ).map((r) => r.Properties.AgentRuntimeName);
    // One per `dedicated` agent, plus the orchestrator. Never one for a2a.
    const dedicated = Object.entries<any>(withRemote.agents)
      .filter(([, a]) => (a.runtime ?? "main") === "dedicated")
      .map(([id]) => id);
    expect(runtimes).toHaveLength(dedicated.length + 1);
    expect(runtimes.join(" ")).not.toContain("partner_check");
  });

  it("still reaches the BFF, so the UI can draw it", () => {
    // How it is LABELLED is bff/workflow.py's job and is asserted in
    // tests/test_bff_projection.py. What this path owns is that a remote agent —
    // which provisions no runtime of its own — is still in the config the API reads.
    const staged = fs.mkdtempSync(path.join(os.tmpdir(), "bff-stage-remote-"));
    stageBffPackage(withRemote, staged);
    const bundled = JSON.parse(fs.readFileSync(path.join(staged, "workflow.json"), "utf8"));
    expect(bundled.agents.partner_check.runtime).toBe("a2a");
    expect(bundled.agents.partner_check.agentCard).toBe(
      withRemote.agents.partner_check.agentCard
    );
  });

  it("ships A2A_TOKENS in the orchestrator's runtime secret, not its environment", () => {
    const orchestrator = Object.values<any>(
      remoteTemplate.findResources("AWS::BedrockAgentCore::Runtime")
    ).find((r) => r.Properties.AgentRuntimeName === "multiagent_orchestrator");
    const env = orchestrator.Properties.EnvironmentVariables;
    expect(env).not.toHaveProperty("A2A_TOKENS");
    expect(env).toHaveProperty("RUNTIME_SECRET_ARN");
    const secrets = Object.values<any>(remoteTemplate.findResources("AWS::SecretsManager::Secret"));
    expect(secrets.some((x) => JSON.stringify(x.Properties.SecretString).includes("A2A_TOKENS"))).toBe(true);
  });

  it("refuses to synth when a bearer agent's token was not supplied", () => {
    // The alternative is a 401 from a service you do not control, which is a far
    // harder failure to read than a synth error naming the agent.
    const bearer = {
      ...withRemote,
      agents: {
        ...withRemote.agents,
        partner_check: { ...withRemote.agents.partner_check, auth: "bearer" },
      },
    };
    expect(() => synth({ workflow: bearer, a2aTokens: {} })).toThrow(
      /declare auth "bearer" but no token was supplied for them: partner_check/
    );
    expect(() => synth({ workflow: bearer, a2aTokens: { partner_check: "t0k" } })).not.toThrow();
  });
});

describe("the stand-in A2A agent", () => {
  // The shipped workflow points two agents at it via `source`, so the default template
  // must contain it. These assert the security posture and the wiring, because both are
  // easy to get subtly wrong: a Function URL defaults to being PUBLIC, and an endpoint
  // map that is absent or misassembled fails only at run time, on the partner's side of
  // a boundary where the error is hardest to read.

  it("deploys one Lambda behind an IAM-authed Function URL", () => {
    const names = Object.values<any>(template.findResources("AWS::Lambda::Function"))
      .map((f) => f.Properties.FunctionName)
      .filter(Boolean);
    expect(names).toContain("A2AAgent-multiagent_orchestrator");

    const urls = Object.values<any>(template.findResources("AWS::Lambda::Url"));
    expect(urls).toHaveLength(1);
    // NEVER "NONE". The client's whole `auth: "sigv4"` mode exists so this endpoint can
    // require a signature instead of a shared token.
    expect(urls[0].Properties.AuthType).toBe("AWS_IAM");
  });

  it("gives the orchestrator the only permission to call it", () => {
    expect(JSON.stringify(template.findResources("AWS::IAM::Policy")))
      .toContain("lambda:InvokeFunctionUrl");
  });

  it("injects one endpoint per remote agent, with the skill as a path segment", () => {
    const orchestrator = Object.values<any>(
      template.findResources("AWS::BedrockAgentCore::Runtime")
    ).find((r) => r.Properties.AgentRuntimeName === "multiagent_orchestrator");
    // A CloudFormation token, so assert on the assembled pieces rather than a string.
    const endpoints = JSON.stringify(orchestrator.Properties.EnvironmentVariables.A2A_ENDPOINTS);
    for (const [id, skill] of Object.entries(a2aLambdaAgents(shipped.agents))) {
      expect(endpoints).toContain(id);
      // A path, not `?skill=`: a client appends /.well-known/agent-card.json to this.
      expect(endpoints).toContain(`/${skill}`);
    }
    expect(endpoints).not.toContain("?skill=");
  });

  it("gives the remote agents no runtime of their own", () => {
    // They are somebody else's service. A runtime created for one would be a container
    // that boots, finds no module under app/subagents/, and crash-loops — while the run
    // succeeded, so nothing would point at the waste.
    const runtimes = Object.values<any>(
      template.findResources("AWS::BedrockAgentCore::Runtime")
    ).map((r) => r.Properties.AgentRuntimeName);
    for (const id of Object.keys(a2aLambdaAgents(shipped.agents))) {
      expect(runtimes.join(" ")).not.toContain(id);
    }
  });

  it("is not deployed at all when no agent asks for it", () => {
    // Point the agents at a real partner and none of this infrastructure exists — the
    // same deal a `tools` entry gets from lambdaArn vs source.
    const external = {
      ...shipped,
      agents: Object.fromEntries(
        Object.entries<any>(shipped.agents).map(([id, a]) =>
          a.source === "a2a_lambda"
            ? [id, { ...a, source: undefined, skill: undefined,
                     agentCard: "https://agents.partner.example/x" }]
            : [id, a]
        )
      ),
    };
    const t = synth({ workflow: JSON.parse(JSON.stringify(external)) });
    expect(Object.values<any>(t.findResources("AWS::Lambda::Url"))).toHaveLength(0);
    const names = Object.values<any>(t.findResources("AWS::Lambda::Function"))
      .map((f) => f.Properties.FunctionName)
      .filter(Boolean);
    expect(names).not.toContain("A2AAgent-multiagent_orchestrator");
  });
});

// ===========================================================================
// One execution role per dedicated agent
// ===========================================================================
// This was a SINGLE shared role for every dedicated runtime, and nothing anywhere
// asserted its contents — so the shape was invisible and the drift below went unnoticed:
// an agent that enables no features still carried the union of every other agent's
// permissions. In the shipped workflow `knowledge_research` uses no guardrails and NO
// dedicated agent uses long-term memory, yet all three could call ApplyGuardrail and read
// the semantic memory store.
//
// What makes this the framework's job rather than the customer's is that the grants are
// DERIVED from the same config that switches the feature on. A customer enables memory for
// an agent; the framework grants that agent memory. There is no IAM to write.
describe("dedicated agent execution roles", () => {
  const roles = (t: Template) =>
    Object.entries<any>(t.findResources("AWS::IAM::Role")).filter(([, r]) =>
      String(r.Properties.RoleName ?? "").startsWith("AgentCoreSubagent-")
    );
  /** The inline policy document attached to the role at logical id `logicalId`. */
  const policyFor = (t: Template, logicalId: string) =>
    JSON.stringify(
      Object.values<any>(t.findResources("AWS::IAM::Policy")).find((p) =>
        JSON.stringify(p.Properties.Roles ?? []).includes(logicalId)
      )?.Properties.PolicyDocument ?? {}
    );

  const dedicated = Object.entries<any>(shipped.agents)
    .filter(([, a]) => (a.runtime ?? "main") === "dedicated")
    .map(([id]) => id);

  it("creates one role per dedicated agent, named for that agent", () => {
    const names = roles(template).map(([, r]) => r.Properties.RoleName);
    expect(names).toHaveLength(dedicated.length);
    for (const id of dedicated) {
      expect(names).toContain(`AgentCoreSubagent-multiagent_orchestrator-${id}`);
    }
  });

  it("gives each runtime its OWN role, not a shared one", () => {
    // The property this whole change buys. If two runtimes point at one role, scoping the
    // role per agent achieves nothing.
    const used = Object.values<any>(template.findResources("AWS::BedrockAgentCore::Runtime"))
      .filter((r) => String(r.Properties.AgentRuntimeName).includes("_"))
      .map((r) => JSON.stringify(r.Properties.RoleArn));
    expect(new Set(used).size).toEqual(used.length);
  });

  it("grants ApplyGuardrail only to agents whose config enables guardrails", () => {
    for (const [logicalId] of roles(template)) {
      const id = dedicated.find((a) => logicalId.includes(a.replace(/_/g, "")));
      const ac = shipped.agents[id!].agentcore ?? {};
      const wants = Boolean(ac.guardrails?.input || ac.guardrails?.output);
      expect(policyFor(template, logicalId).includes("bedrock:ApplyGuardrail")).toEqual(wants);
    }
    // And the sample really does exercise both branches, or the test above is vacuous.
    const flags = dedicated.map((id) => {
      const g = (shipped.agents[id].agentcore ?? {}).guardrails ?? {};
      return Boolean(g.input || g.output);
    });
    expect(new Set(flags).size).toBeGreaterThan(1);
  });

  it("grants long-term memory only to agents that declare it", () => {
    for (const [logicalId] of roles(template)) {
      const id = dedicated.find((a) => logicalId.includes(a.replace(/_/g, "")));
      const wants = ((shipped.agents[id!].agentcore ?? {}).memory?.longTerm ?? []).length > 0;
      expect(policyFor(template, logicalId).includes("RetrieveMemoryRecords")).toEqual(wants);
    }
  });

  it("still grants every agent what it unconditionally needs", () => {
    // Scoping down must not remove the permissions a container needs merely to run: pull
    // the image, emit spans and metrics, call a model, write its telemetry row.
    for (const [logicalId] of roles(template)) {
      const doc = policyFor(template, logicalId);
      for (const needed of [
        "ecr:BatchGetImage",
        "logs:PutLogEvents",
        "xray:PutTraceSegments",
        "cloudwatch:PutMetricData",
        "bedrock:InvokeModel",
        "dynamodb:PutItem",
      ]) {
        expect(doc).toContain(needed);
      }
    }
  });

  it("scopes model invocation to inference profiles, never account-wide bedrock:*", () => {
    // The Terraform side of this role carried "arn:aws:bedrock:<region>:<acct>:*", which
    // also covered custom models, provisioned throughput, agents, guardrails and prompts —
    // broader than the CDK role AND broader than Terraform's own orchestrator role, so it
    // was drift rather than a decision. Asserted here so the two cannot diverge again.
    for (const [logicalId] of roles(template)) {
      const doc = policyFor(template, logicalId);
      expect(doc).toContain("inference-profile/*");
      expect(doc).not.toMatch(/"arn:aws:bedrock:us-east-1:123456789012:\*"/);
    }
  });

  it("refuses an agent id that fits the runtime name but not the role name", () => {
    // The role name is the TIGHTER limit, and this is the window that proves it matters:
    // "AgentCoreSubagent-" is an 18-character prefix the runtime name does not carry, so
    // with a 23-character agentName an id of 23 passes the 48-char runtime check and
    // overruns IAM's 64. Caught rather than truncated, because a shortened name can
    // collide with another agent's and put two runtimes back on one role.
    const id = "a".repeat(23);
    expect(`multiagent_orchestrator_${id}`.length).toBeLessThanOrEqual(48);
    expect(`AgentCoreSubagent-multiagent_orchestrator-${id}`.length).toBeGreaterThan(64);
    const wf = {
      ...shipped,
      agents: { ...shipped.agents, [id]: { name: "X", runtime: "dedicated" } },
      steps: [...shipped.steps, { agent: id }],
    };
    expect(() => synth({ workflow: wf })).toThrow(/must be 64 characters or fewer/);
  });
});

describe("the Builder control plane", () => {
  it("stores builds in a table with an owner index, and a versioned bucket", () => {
    template.hasResourceProperties("AWS::DynamoDB::Table", {
      TableName: "multiagent_orchestrator_builds",
      KeySchema: [
        { AttributeName: "pk", KeyType: "HASH" },
        { AttributeName: "sk", KeyType: "RANGE" },
      ],
      GlobalSecondaryIndexes: [
        {
          IndexName: "by_owner",
          KeySchema: [
            { AttributeName: "owner", KeyType: "HASH" },
            { AttributeName: "updated", KeyType: "RANGE" },
          ],
        },
      ],
    });
    template.hasResourceProperties("AWS::S3::Bucket", {
      VersioningConfiguration: { Status: "Enabled" },
      PublicAccessBlockConfiguration: {
        BlockPublicAcls: true, BlockPublicPolicy: true, IgnorePublicAcls: true, RestrictPublicBuckets: true,
      },
    });
  });

  it("deploys builds from an ARM, privileged CodeBuild project running the runner", () => {
    const projects = template.findResources("AWS::CodeBuild::Project");
    const [project] = Object.values(projects) as any[];
    expect(project.Properties.Name).toBe("multiagent-orchestrator-deploy");
    expect(project.Properties.Environment.Type).toBe("ARM_CONTAINER");
    expect(project.Properties.Environment.PrivilegedMode).toBe(true);
    expect(project.Properties.TimeoutInMinutes).toBe(120);
    const spec = JSON.parse(project.Properties.Source.BuildSpec);
    expect(spec.phases.build.commands.join("\n")).toContain("python3 deployer/runner.py");
    const names = project.Properties.Environment.EnvironmentVariables.map((v: any) => v.Name).sort();
    expect(names).toEqual(["BUILDS_BUCKET", "BUILDS_TABLE", "CONSOLE_NAME", "SECRETS_PREFIX", "SOURCE_URI",
      "TERRAFORM_VERSION"]);
  });

  it("lets the console run deployed builds only through the ax_ prefix", () => {
    const policies = JSON.stringify(template.findResources("AWS::IAM::Policy"));
    expect(policies).toContain(":table/ax_*");
    expect(policies).toContain(":runtime/ax_*");
    expect(policies).toContain("codebuild:StartBuild");
  });

  it("splits the Terraform deploy permissions into policies small enough to attach", () => {
    const managed = Object.values(template.findResources("AWS::IAM::ManagedPolicy")) as any[];
    expect(managed.length).toBeGreaterThanOrEqual(2);
    for (const m of managed) {
      expect(JSON.stringify(m.Properties.PolicyDocument).length).toBeLessThan(6144);
    }
    const sids = managed.flatMap((m) => m.Properties.PolicyDocument.Statement.map((s: any) => s.Sid));
    const file = require(`${ORCH_ROOT}/terraform/deploy-role-policy.json`);
    expect(sids.sort()).toEqual(file.Statement.map((s: any) => s.Sid).sort());
  });

  it("is left out with builder=false, which is how every build stack is deployed", () => {
    const t = synth({ builder: false });
    t.resourceCountIs("AWS::CodeBuild::Project", 0);
    expect(JSON.stringify(t.findResources("AWS::DynamoDB::Table"))).not.toContain("_builds");
  });
});

/** The trigger logs sign-ins into the builds table and can do nothing else. */
function t_expectSignInLogOnly(t: Template): void {
  t.hasResourceProperties("AWS::Lambda::Function", {
    FunctionName: "AgentCoreSignup-multiagent_orchestrator",
    Environment: { Variables: { AUDIT_TABLE: "multiagent_orchestrator_builds" } },
  });
  const policies = JSON.stringify(Object.entries(t.findResources("AWS::IAM::Policy"))
    .filter(([id]) => id.startsWith("SignupTrigger")));
  expect(policies).toContain("dynamodb:PutItem");
  expect(policies).not.toContain("AdminAddUserToGroup");
}

describe("self sign-up", () => {
  it("is closed by default: admin-created users only, and nobody is put in a group", () => {
    template.hasResourceProperties("AWS::Cognito::UserPool", {
      AdminCreateUserConfig: { AllowAdminCreateUserOnly: true },
    });
    const [pool] = Object.values(template.findResources("AWS::Cognito::UserPool")) as any[];
    expect(pool.Properties.LambdaConfig.PostConfirmation).toBeUndefined();
    // The trigger is there only to log sign-ins (a Builder console's audit log).
    expect(pool.Properties.LambdaConfig.PostAuthentication["Fn::GetAtt"][0]).toMatch(/^SignupTrigger/);
    t_expectSignInLogOnly(template);
  });
  it("logs a build app's sign-ins and runs in its own audit table, without the Builder", () => {
    const t = synth({ builder: false });
    t.hasResourceProperties("AWS::Lambda::Function", {
      FunctionName: "AgentCoreSignup-multiagent_orchestrator",
      Environment: { Variables: { AUDIT_TABLE: "multiagent_orchestrator_audit" } },
    });
    t.hasResourceProperties("AWS::DynamoDB::Table", {
      TableName: "multiagent_orchestrator_audit",
      KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }, { AttributeName: "sk", KeyType: "RANGE" }],
    });
    const bff = Object.values<any>(t.findResources("AWS::Lambda::Function"))
      .find((f) => f.Properties.FunctionName === "AgentCoreBFF-multiagent_orchestrator");
    expect(Object.keys(bff.Properties.Environment.Variables)).toContain("AUDIT_TABLE");
    const policies = JSON.stringify(t.findResources("AWS::IAM::Policy"));
    expect(policies).toContain('"Sid":"AuditLog"');
    expect(policies).not.toContain("AdminAddUserToGroup");
  });
  it("is a control plane with consoleMode=builder, and refuses it without the Builder", () => {
    const t = synth({ consoleMode: "builder" });
    const bff = Object.values<any>(t.findResources("AWS::Lambda::Function"))
      .find((f) => f.Properties.FunctionName === "AgentCoreBFF-multiagent_orchestrator");
    expect(bff.Properties.Environment.Variables.CONSOLE_MODE).toBe("builder");
    // No workflow plane of its own: nothing that runs a workflow is deployed.
    for (const type of ["AWS::BedrockAgentCore::Runtime", "AWS::BedrockAgentCore::Memory",
      "AWS::BedrockAgentCore::Gateway", "AWS::BedrockAgentCore::GatewayTarget", "AWS::BedrockAgentCore::PolicyEngine",
      "AWS::BedrockAgentCore::Policy", "AWS::Bedrock::Guardrail", "AWS::Bedrock::KnowledgeBase",
      "AWS::Logs::ResourcePolicy"]) t.resourceCountIs(type, 0);
    const tables = Object.values<any>(t.findResources("AWS::DynamoDB::Table")).map((x) => x.Properties.TableName);
    expect(tables).toEqual(["multiagent_orchestrator_builds"]);
    for (const k of ["STATUS_TABLE", "EVENTS_TABLE", "TELEMETRY_TABLE", "RUNTIME_ARN"]) {
      expect(bff.Properties.Environment.Variables[k]).toBeUndefined();
    }
    expect(JSON.stringify(t.findResources("AWS::IAM::Policy"))).not.toContain("_status");
    expect(JSON.stringify(t.findResources("Custom::AWS"))).not.toContain("TransactionSearch");
    // No M2M Gateway client either; still the console: the UI, the BFF, the builds, sign-in.
    expect(JSON.stringify(t.findResources("AWS::Cognito::UserPoolClient"))).not.toContain("ClientCredentials");
    t.resourceCountIs("AWS::CodeBuild::Project", 1);
    t.resourceCountIs("AWS::CloudFront::Distribution", 1);
    t.hasOutput("consoleMode", { Value: "builder" });
    expect(Object.keys(t.toJSON().Outputs)).not.toContain("agentRuntimeArn");
    // A console keeps its log with its builds: no separate audit table.
    expect(JSON.stringify(t.findResources("AWS::DynamoDB::Table"))).not.toContain("_audit");
    expect(() => synth({ consoleMode: "builder", builder: false })).toThrow(/needs the Builder/);
    expect(() => synth({ consoleMode: "console" })).toThrow(/"app" or "builder"/);
  });

  it("opened, puts every confirmed user in the default group", () => {
    const t = synth({ selfSignUp: true, selfSignUpGroup: "members" });
    const [pool] = Object.values(t.findResources("AWS::Cognito::UserPool")) as any[];
    expect(pool.Properties.AdminCreateUserConfig.AllowAdminCreateUserOnly).toBe(false);
    expect(pool.Properties.LambdaConfig.PostConfirmation["Fn::GetAtt"][0]).toMatch(/^SignupTrigger/);
    t.hasResourceProperties("AWS::Lambda::Function", {
      FunctionName: "AgentCoreSignup-multiagent_orchestrator",
      Environment: { Variables: { DEFAULT_GROUP: "members" } },
    });
  });

  it("refuses a default group workflow.json does not name", () => {
    expect(() => synth({ selfSignUp: true, selfSignUpGroup: "nobody" })).toThrow(/authorization.actions names it/);
  });

  it("never makes a sign-up an admin: the default group may not hold admin, audit or insights", () => {
    // The sample grants those to `admins`, a group filled only from the backend.
    expect(() => synth({ selfSignUp: true, selfSignUpGroup: "admins" }))
      .toThrow(/grants that group "admin", "audit", "insights"/);
    expect(privilegedGrants({ authorization: { actions: { audit: ["members"], deploy: ["members"] } } }, "members"))
      .toEqual(["audit"]);
    expect(PRIVILEGED_ACTIONS).toEqual(["admin", "audit", "insights"]);
  });
});

describe("connected accounts, secrets and documents", () => {
  it("lets the deploy project and the BFF assume only a connected account's deploy role", () => {
    const policies = JSON.stringify(template.findResources("AWS::IAM::Policy"));
    expect(policies).toContain("arn:aws:iam::*:role/AgentExpressDeploy-*");
    expect(policies).not.toMatch(/"sts:AssumeRole","Effect":"Allow","Resource":"\*"/);
  });

  it("scopes build secrets to this console's prefix", () => {
    const policies = JSON.stringify(template.findResources("AWS::IAM::Policy"));
    expect(policies).toContain("secret:agentexpress/multiagent_orchestrator/builds/*");
  });

  it("accepts document uploads only from this console's own origin", () => {
    const buckets = Object.values(template.findResources("AWS::S3::Bucket")) as any[];
    const withCors = buckets.filter((b) => b.Properties.CorsConfiguration);
    expect(withCors.length).toBe(1);     // the builds bucket, and nothing else
    const rule = withCors[0].Properties.CorsConfiguration.CorsRules[0];
    expect(rule.AllowedMethods).toEqual(["POST"]);
    expect(JSON.stringify(rule.AllowedOrigins)).toContain("DomainName");
  });
});

describe("image agents", () => {
  it("get an assets bucket the runtime writes to and the BFF reads from — only when one draws", () => {
    expect(JSON.stringify(template.findResources("AWS::S3::Bucket"))).not.toContain("-assets-");
    const wf = JSON.parse(JSON.stringify(shipped));
    wf.agents.report = { ...wf.agents.report, output: "image", image: { aspectRatio: "16:9" } };
    const t = synth({ workflow: wf });
    t.hasResourceProperties("AWS::S3::Bucket", { BucketName: "agentcore-multiagent-orchestrator-assets-123456789012" });
    const runtime = Object.values<any>(t.findResources("AWS::BedrockAgentCore::Runtime"))
      .find((r) => r.Properties.AgentRuntimeName === "multiagent_orchestrator");
    expect(Object.keys(runtime.Properties.EnvironmentVariables)).toContain("ASSETS_BUCKET");
    const policies = JSON.stringify(t.findResources("AWS::IAM::Policy"));
    expect(policies).toContain('"s3:PutObject"');
    expect(policies).toContain('"s3:GetObject"');
    const routes = JSON.stringify(t.findResources("AWS::ApiGatewayV2::Route"));
    expect(routes).toContain("GET /api/images");
    expect(routes).toContain("POST /api/builds/{id}/design");
  });
  it("refuse a deploy whose image brief would leave the region's geography, unless allowed", () => {
    const wf = JSON.parse(JSON.stringify(shipped));
    wf.agents.report = { ...wf.agents.report, output: "image" };
    const eu = { account: "123456789012", region: "eu-west-1" };
    expect(() => synth({ workflow: wf, env: eu })).toThrow(/agents\.report: .*allowCrossRegion/);
    wf.agents.report.image = { allowCrossRegion: true };
    expect(() => synth({ workflow: wf, env: eu })).not.toThrow();
  });
  it("let an agent with `vision` read the run's images — and only then", () => {
    const statements = (t: Template) => Object.values<any>(t.findResources("AWS::IAM::Policy"))
      .flatMap((p) => p.Properties.PolicyDocument.Statement as any[]);
    const assets = (st: any) => JSON.stringify(st.Resource).includes("runs/*");
    const wf = JSON.parse(JSON.stringify(shipped));
    wf.agents.intake = { ...wf.agents.intake, output: "image" };
    // Drawing only: the runtime writes images and never reads them, as before.
    const drawOnly = statements(synth({ workflow: wf })).filter(assets);
    expect(drawOnly.some((st) => [st.Action].flat().includes("s3:GetObject") && st.Sid !== undefined)).toBe(false);
    expect(drawOnly.filter((st) => [st.Action].flat().includes("s3:PutObject"))
      .every((st) => [st.Action].flat().length === 1)).toBe(true);
    // An in-process reader (report) and a dedicated one (knowledge_research).
    wf.agents.report = { ...wf.agents.report, vision: { from: ["intake"] } };
    wf.agents.knowledge_research = { ...wf.agents.knowledge_research, vision: { from: ["intake"], maxImages: 2 } };
    const both = statements(synth({ workflow: wf })).filter(assets);
    expect(both.some((st) => JSON.stringify([st.Action].flat()) === JSON.stringify(["s3:PutObject", "s3:GetObject"]))).toBe(true);
    expect(both.filter((st) => st.Sid === "ReadRunImages")).toHaveLength(1);
  });
});

describe("the AgentExpress Assistant's attachments", () => {
  it("reads s3:// paths only from the buckets the console allows", () => {
    const none = JSON.stringify(template.findResources("AWS::IAM::Policy"));
    expect(none).not.toContain("AssistantReadsS3Object");
    const t = synth({ designerS3Buckets: ["data-bucket", "specs-bucket/team/specs/"] });
    const bff = Object.values<any>(t.findResources("AWS::Lambda::Function"))
      .find((f) => f.Properties.FunctionName === "AgentCoreBFF-multiagent_orchestrator");
    expect(bff.Properties.Environment.Variables.DESIGNER_S3_BUCKETS).toBe("data-bucket,specs-bucket/team/specs/");
    const st = Object.values<any>(t.findResources("AWS::IAM::Policy"))
      .flatMap((p) => p.Properties.PolicyDocument.Statement as any[]);
    expect(st.find((x) => x.Sid === "AssistantReadsS3Object0").Resource).toBe("arn:aws:s3:::data-bucket/*");
    expect(st.find((x) => x.Sid === "AssistantReadsS3Object1").Resource).toBe("arn:aws:s3:::specs-bucket/team/specs/*");
    expect(st.find((x) => x.Sid === "AssistantListsS31").Condition).toEqual(
      { StringLike: { "s3:prefix": ["team/specs/*", "team/specs"] } });
    expect(JSON.stringify(t.findResources("AWS::ApiGatewayV2::Route"))).toContain("POST /api/builds/{id}/design/attachments");
  });
});

describe("long-term memory strategies and custom evaluators, from workflow.json", () => {
  it("keeps semantic + summary, and adds the rest only when an agent names them", () => {
    const base = memoryStrategiesFor({ agents: { a: { agentcore: { memory: { longTerm: ["semantic"] } } } } }, "m");
    expect(base.strategies.map((s) => Object.keys(s)[0])).toEqual(["SemanticMemoryStrategy", "SummaryMemoryStrategy"]);
    expect(base.custom).toBe(false);
    const all = memoryStrategiesFor({ orchestrator: { defaultModel: "dflt" }, agents: {
      a: { agentcore: { memory: { longTerm: ["userPreference", "episodic"] } } },
      b: { agentcore: { memory: { custom: { base: "semantic", instructions: "Keep configs." } } } },
    } }, "m");
    expect(all.strategies.map((s) => Object.keys(s)[0])).toEqual([
      "SemanticMemoryStrategy", "SummaryMemoryStrategy", "UserPreferenceMemoryStrategy",
      "EpisodicMemoryStrategy", "CustomMemoryStrategy"]);
    expect(all.strategies[3].EpisodicMemoryStrategy.ReflectionConfiguration.Namespaces).toEqual(["episodes/{actorId}"]);
    expect(all.strategies[4].CustomMemoryStrategy).toEqual({ Name: "custom_b", Namespaces: ["custom-b/{actorId}"],
      Configuration: { SemanticOverride: {
        Extraction: { AppendToPrompt: "Keep configs.", ModelId: "dflt" },
        Consolidation: { AppendToPrompt: "Keep configs.", ModelId: "dflt" } } } });
    expect(all.custom).toBe(true);
  });
  it("makes one evaluator per custom judge, with its placeholders and a default scale", () => {
    const evs = customEvaluatorsOf({ agents: { report: { agentcore: { evaluations: { custom: [
      { name: "tone", instructions: "Score the tone." },
      { name: "facts", instructions: "Given {context}, score {assistant_turn}.", model: "judge",
        scale: [{ value: 1, label: "Yes", definition: "d" }, { value: 0, label: "No", definition: "d" }] }] } } } } },
    "ax_1a2b3c4d", "m");
    expect(evs.map((e) => e.key)).toEqual(["report.tone", "report.facts"]);
    expect(evs[0].instructions).toMatch(/Score the tone\.\n\nContext: \{context\}\nResponse to score: \{assistant_turn\}$/);
    expect(evs[1].instructions).toBe("Given {context}, score {assistant_turn}.");
    expect(evs[0].scale.length).toBe(5);
    expect(evs[1].model).toBe("judge");
    expect(evs[0].evaluatorName).toMatch(/^ax_1a2b3c4d_[0-9a-f]{6}_tone$/);
  });
  it("checks orchestrator.policy.custom before a policy is made of it", () => {
    const gw = 'resource == AgentCore::Gateway::"{{gateway}}"';
    const ok = `// why\nforbid(principal, action in AgentCore::Action::"kb", ${gw});`;
    expect(customPoliciesOf({})).toEqual([]);
    expect(customPoliciesOf({ orchestrator: { policy: { custom: [{ name: "a", statement: ok, description: "d" }] } } }))
      .toEqual([{ name: "a", statement: ok, description: "d" }]);
    const bad = (custom: unknown) => () => customPoliciesOf({ orchestrator: { policy: { custom } } });
    expect(bad({})).toThrow(/must be a list/);
    expect(bad([{ name: "bad name", statement: ok }])).toThrow(/custom\[0\]\.name/);
    expect(bad([{ name: "a", statement: ok }, { name: "a", statement: ok }])).toThrow(/used twice/);
    expect(bad([{ name: "a", statement: "allow(principal, action, resource);" }])).toThrow(/permit\(\.\.\.\) or forbid/);
    expect(bad([{ name: "a", statement: "permit(principal, action in AgentCore::Action::\"kb\", resource);" }]))
      .toThrow(/\{\{gateway\}\}/);
  });
  it("adds no custom policy for the shipped workflow", () => {
    expect(Object.keys(template.findResources("AWS::BedrockAgentCore::Policy")).some((k) => k.includes("CustomPolicy"))).toBe(false);
  });
  it("adds nothing for the shipped workflow: no memory role, no evaluator", () => {
    expect(Object.keys(template.findResources("AWS::BedrockAgentCore::Evaluator"))).toEqual([]);
    expect(Object.keys(template.findResources("AWS::IAM::Role")).some((k) => k.startsWith("MemoryExecutionRole"))).toBe(false);
  });
});

describe("inline Lambda code", () => {
  it("is valid Python: every function the stack writes inline compiles", () => {
    const { execFileSync } = require("child_process");
    const inline = Object.values<any>(template.findResources("AWS::Lambda::Function"))
      .map((f) => f.Properties.Code?.ZipFile).filter((c) => typeof c === "string");
    expect(inline.length).toBeGreaterThan(0);
    for (const code of inline) {
      // An indented module ("unexpected indent") failed the Transaction Search custom
      // resource on every build: compile each one exactly as Lambda would load it.
      execFileSync("python3", ["-c", "import sys; compile(sys.stdin.read(), 'index.py', 'exec')"], { input: code });
    }
  });
});
describe("baseline hardening (mirrors the Terraform path)", () => {
  it("writes each runtime secret as valid JSON, nested objects included", () => {
    // Fn::Join with tokens in it: stand a plain string in for each token, then parse.
    const flat = (v: any): string => typeof v === "string" ? v
      : v?.["Fn::Join"] ? v["Fn::Join"][1].map((p: any) => typeof p === "string" ? p : "TOKEN").join(v["Fn::Join"][0])
      : "TOKEN";
    const secrets = Object.values<any>(template.findResources("AWS::SecretsManager::Secret"))
      .filter((x) => JSON.stringify(x.Properties.SecretString ?? "").includes("GATEWAY_CLIENT_SECRET"));
    expect(secrets.length).toBeGreaterThan(1);
    for (const x of secrets) {
      const parsed = JSON.parse(flat(x.Properties.SecretString));
      expect(parsed).toHaveProperty("GATEWAY_CLIENT_SECRET");
      if ("GATEWAY_AGENT_CLIENTS" in parsed) expect(typeof parsed.GATEWAY_AGENT_CLIENTS).toBe("object");
    }
  });
  it("puts no credential in any runtime's environment variables", () => {
    const runtimes = Object.values<any>(template.findResources("AWS::BedrockAgentCore::Runtime"));
    expect(runtimes.length).toBeGreaterThan(0);
    for (const r of runtimes) {
      const env = r.Properties.EnvironmentVariables ?? {};
      for (const k of ["GATEWAY_CLIENT_SECRET", "GATEWAY_AGENT_CLIENTS", "A2A_TOKENS"]) {
        expect(env).not.toHaveProperty(k);
      }
      expect(env).toHaveProperty("RUNTIME_SECRET_ARN");
    }
  });
  it("turns on point-in-time recovery for every table", () => {
    const tables = Object.values<any>(template.findResources("AWS::DynamoDB::Table"));
    expect(tables.length).toBeGreaterThan(0);
    for (const t of tables) {
      expect(t.Properties.PointInTimeRecoverySpecification).toEqual({ PointInTimeRecoveryEnabled: true });
    }
  });
  it("has TTL on the status and events tables", () => {
    for (const name of ["multiagent_orchestrator_status", "multiagent_orchestrator_events"]) {
      template.hasResourceProperties("AWS::DynamoDB::Table", {
        TableName: name, TimeToLiveSpecification: { AttributeName: "ttl", Enabled: true },
      });
    }
  });
  it("refuses plain HTTP on every bucket", () => {
    const buckets = Object.keys(template.findResources("AWS::S3::Bucket"));
    const policies = Object.values<any>(template.findResources("AWS::S3::BucketPolicy"));
    for (const b of buckets) {
      const p = policies.find((x) => x.Properties.Bucket?.Ref === b);
      expect(p).toBeDefined();
      expect(JSON.stringify(p.Properties.PolicyDocument)).toContain("aws:SecureTransport");
    }
  });
  it("logs and throttles the BFF API", () => {
    const stage = Object.values<any>(template.findResources("AWS::ApiGatewayV2::Stage"))[0];
    expect(stage.Properties.AccessLogSettings.DestinationArn).toBeDefined();
    expect(stage.Properties.DefaultRouteSettings).toEqual({ ThrottlingBurstLimit: 1000, ThrottlingRateLimit: 500 });
  });
  it("sends security headers on every CloudFront behaviour", () => {
    template.hasResourceProperties("AWS::CloudFront::ResponseHeadersPolicy", {
      ResponseHeadersPolicyConfig: {
        SecurityHeadersConfig: { FrameOptions: { FrameOption: "DENY", Override: true } },
      },
    });
    const dist = Object.values<any>(template.findResources("AWS::CloudFront::Distribution"))[0];
    const cfg = dist.Properties.DistributionConfig;
    for (const b of [cfg.DefaultCacheBehavior, ...cfg.CacheBehaviors]) {
      expect(b.ResponseHeadersPolicyId).toBeDefined();
    }
  });
  it("asks for a 12-character password with symbols, offers TOTP, and has no localhost callback", () => {
    template.hasResourceProperties("AWS::Cognito::UserPool", {
      Policies: { PasswordPolicy: { MinimumLength: 12, RequireSymbols: true } },
      MfaConfiguration: "OPTIONAL",
      EnabledMfas: ["SOFTWARE_TOKEN_MFA"],
    });
    for (const c of Object.values<any>(template.findResources("AWS::Cognito::UserPoolClient"))) {
      expect(JSON.stringify(c.Properties.CallbackURLs ?? [])).not.toContain("localhost");
    }
  });
});
describe("Hosted UI sign-in options", () => {
  it("offers the pool's own users by default, and a federated provider when named", () => {
    const clients = (t: Template) => Object.values<any>(t.findResources("AWS::Cognito::UserPoolClient"))
      .filter((c) => c.Properties.AllowedOAuthFlows?.includes("code"));
    expect(clients(template)[0].Properties.SupportedIdentityProviders).toEqual(["COGNITO"]);
    const app = new cdk.App({ context: { hostedUiProviders: "MyCompanyOidc" } });
    const stack = new OrchestratorStack(app, "IdpStack", {
      env: { account: "123456789012", region: "us-east-1" }, agentName: "multiagent_orchestrator",
      modelId: "us.anthropic.claude-haiku-4-5-20251001-v1:0", memoryEventExpiryDays: 30, idp: "cognito",
      createCognito: true, cognitoUserPoolId: "", cognitoClientId: "", cognitoDomainPrefix: "",
      auth0Domain: "", auth0ClientId: "", enableGateway: false, gatewayClientId: "", gatewayClientSecret: "",
      gatewayAudience: "", toolApiKeys: {}, a2aTokens: {}, transactionSearchIndexingPercentage: 100,
    } as any);
    expect(clients(Template.fromStack(stack))[0].Properties.SupportedIdentityProviders).toEqual(["MyCompanyOidc"]);
  });
});
