/**
 * Terraform ↔ CDK parity.
 *
 * The project promises both IaC paths deploy the same thing from the same
 * `workflow.json`. Nothing enforces that: they are two independent
 * implementations of the same projection, and they HAVE drifted — `buildBffWorkflow`
 * once emitted `mcp`/`rag` after workflow.json renamed them to `tool`/`corpus`, and
 * omitted `evalAgents` and `chatbot` entirely, so CDK deployments silently lost the
 * data-source chips, the Evaluate buttons and the in-app assistant while Terraform
 * ones kept them. Nobody noticed until a live CDK deploy was compared to a live
 * Terraform one.
 *
 * These tests read the HCL as text and compare the things a drift would show up in:
 * the key set of the BFF projection, the API route list, the tool types, the web
 * search regions, and the RBAC action list (which is duplicated in three places by
 * necessity — Terraform cannot read the Python, and TypeScript cannot read either).
 *
 * Reading HCL with regexes is crude, but the alternative is an HCL parser
 * dependency for a handful of lists, and each check below asserts it actually
 * extracted something before comparing — so a regex that stops matching fails loudly
 * rather than passing vacuously.
 */

import * as crypto from "crypto";
import * as fs from "fs";
import * as path from "path";


const ORCH_ROOT = path.join(__dirname, "..", "..");
const TF = path.join(ORCH_ROOT, "terraform");

const read = (p: string) => fs.readFileSync(p, "utf8");
const shipped = JSON.parse(read(path.join(ORCH_ROOT, "app", "workflow.json")));

/** The agent ids in one `steps` entry, whatever its shape. */
const stepIds = (s: any): string[] => s.parallel ?? s.sequence ?? [s.agent];

/**
 * Agent ids resolved from the shipped TOPOLOGY rather than written out.
 *
 * These tests used to name `agents.intake` directly, which quietly made the test
 * suite part of the customer-editable surface: renaming an agent in workflow.json
 * turned them red even though the framework was fine. The promise is that
 * workflow.json + app/subagents/ is all you touch.
 */
const firstAgent: string = stepIds(shipped.steps[0])[0];
/** The first agent whose data source is the tool type `t`, or "" if none. */
const agentWithToolType = (t: string): string =>
  Object.entries<any>(shipped.agents).find(
    ([, a]) => a.tool && shipped.tools[a.tool]?.type === t
  )?.[0] ?? "";
/**
 * The first agent that has no tool and reads its upstream peers.
 *
 * `runtime: "a2a"` is excluded: a remote agent also has no tool, but its label comes
 * from the A2A branch of the projection, not from `access`. Without that exclusion this
 * resolved to a remote agent as soon as the sample gained one, and the assertion about
 * the "Upstream agent outputs" label started checking the wrong branch.
 */
const upstreamAgent: string =
  Object.entries<any>(shipped.agents).find(
    ([id, a]) => !a.tool && id !== firstAgent && (a.runtime ?? "main") !== "a2a"
  )?.[0] ?? "";

/** Balance braces from the `{` at or after `from`, returning the body between them. */
function balanced(src: string, from: number): string {
  let depth = 0;
  let i = src.indexOf("{", from);
  const open = i;
  for (; i < src.length; i++) {
    if (src[i] === "{") depth++;
    else if (src[i] === "}" && --depth === 0) return src.slice(open + 1, i);
  }
  throw new Error(`unbalanced HCL block at offset ${from}`);
}

/** The body of a `<name> = { ... }` HCL block. */
function hclBlock(src: string, name: string): string {
  const start = src.indexOf(`${name} = {`);
  expect(start).toBeGreaterThan(-1);
  return balanced(src, start);
}

/**
 * The body of the OBJECT a `<name> = { for ... : k => { ... } }` comprehension
 * produces — i.e. the per-item shape, not the comprehension's own braces.
 */
function hclForValue(src: string, name: string): string {
  const start = src.indexOf(`${name} = {`);
  expect(start).toBeGreaterThan(-1);
  const arrow = src.indexOf("=> {", start);
  expect(arrow).toBeGreaterThan(start);
  return balanced(src, arrow);
}

/** Top-level `key =` names inside an HCL block body, ignoring comments. */
function hclKeys(body: string): string[] {
  const keys = new Set<string>();
  let depth = 0;
  for (const line of body.split("\n")) {
    const code = line.replace(/#.*$/, "");
    if (depth === 0) {
      const m = code.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=/);
      if (m) keys.add(m[1]);
    }
    depth += (code.match(/[{[(]/g) ?? []).length - (code.match(/[}\])]/g) ?? []).length;
  }
  return [...keys].sort();
}

// ---------------------------------------------------------------------------
// API routes
// ---------------------------------------------------------------------------

describe("API routes", () => {
  it("both paths expose exactly the same route set", () => {
    // A route present in one path and not the other is a 404 that only appears on
    // one kind of deployment — which is how /api/me could have gone missing.
    const tfRoutes = [
      ...read(path.join(TF, "bff.tf")).matchAll(
        /^\s*"((?:GET|POST|PUT|DELETE|PATCH) \/api\/[^"]*)",/gm
      ),
    ]
      .map((m) => m[1])
      .sort();

    const stack = read(path.join(__dirname, "..", "lib", "orchestrator-stack.ts"));
    const cdkRoutes = [
      ...stack.matchAll(/\{\s*path:\s*"(\/api\/[^"]*)",\s*methods:\s*\[([^\]]*)\]/g),
    ]
      .flatMap(([, p, methods]) =>
        [...methods.matchAll(/HttpMethod\.(\w+)/g)].map((m) => `${m[1]} ${p}`)
      )
      .sort();

    expect(tfRoutes.length).toBeGreaterThan(10);
    expect(cdkRoutes).toEqual(tfRoutes);
  });

  it("includes the RBAC-relevant routes", () => {
    const stack = read(path.join(__dirname, "..", "lib", "orchestrator-stack.ts"));
    for (const p of ["/api/me", "/api/sessions/{id}/decision", "/api/insights/run"]) {
      expect(stack).toContain(`"${p}"`);
    }
  });
});

// ---------------------------------------------------------------------------
// Duplicated constants
// ---------------------------------------------------------------------------

describe("the framework vocabulary has ONE home", () => {
  /**
   * This describe used to be called "constants duplicated across languages", and it
   * compared hand-written copies of every closed value set against each other: the tool
   * types in Python, TypeScript and HCL; the RBAC action names in all three; the schema
   * property types; the a2a auth modes; the web search regions.
   *
   * It was a test that the duplicates had not drifted — not a reason for them to exist.
   * And the drift was real: a value added to one copy and missed in another rejected a
   * config the other planes accepted, so whether a workflow deployed depended on which
   * IaC path you used.
   *
   * There is one copy now, app/vocabulary.json, and JSON is the one format all three
   * planes read natively. So these assert that each plane READS it, which is a much
   * stronger guarantee than three lists agreeing on one particular day.
   */
  const VOCAB_PATH = path.join(ORCH_ROOT, "app", "vocabulary.json");
  const vocab = JSON.parse(read(VOCAB_PATH));

  it("declares every set with values and an explanation of why it is closed", () => {
    const names = Object.keys(vocab).filter((k) => !k.startsWith("$"));
    expect(names.length).toBeGreaterThan(10);
    for (const name of names) {
      expect(Array.isArray(vocab[name].values)).toBe(true);
      expect(vocab[name].values.length).toBeGreaterThan(0);
      // A closed set a customer can trip over has to say why it is closed, or the
      // error message is a dead end.
      expect(String(vocab[name].$comment ?? "").length).toBeGreaterThan(20);
    }
  });

  it("is reached by all three planes through the file, not through an import of nothing", () => {
    const stack = read(path.join(__dirname, "..", "lib", "orchestrator-stack.ts"));
    expect(stack).toContain('from "./vocabulary"');
    expect(read(path.join(__dirname, "..", "lib", "vocabulary.ts"))).toContain("vocabulary.json");
    expect(read(path.join(ORCH_ROOT, "app", "orchestrator", "registry.py"))).toContain("vocabulary.");
    expect(read(path.join(ORCH_ROOT, "bff", "authz.py"))).toContain("authorizationActions");
    for (const f of ["tools.tf", "identity.tf", "guardrail.tf"]) {
      expect(read(path.join(TF, f))).toContain("app/vocabulary.json");
    }
  });

  it("is not ALSO written out as a literal anywhere in any plane", () => {
    // GENERATED from the vocabulary, over every source file, rather than a list of
    // patterns maintained by hand. The hand-written version checked five of fourteen
    // sets and missed `a2aLambdaSkills`, which stayed a literal pair in
    // terraform/tools.tf while the other two planes read four values from the file —
    // so `skill: "analysis"` synthesized under CDK and was rejected by Terraform. That
    // is the precise failure this vocabulary exists to prevent, surviving inside the
    // test that is supposed to prove it cannot happen.
    const sources: [string, string][] = [
      ...["tools.tf", "identity.tf", "guardrail.tf", "a2a.tf", "main.tf", "kb.tf"].map(
        (f) => [`terraform/${f}`, read(path.join(TF, f))] as [string, string]
      ),
      ["cdk/lib/orchestrator-stack.ts", read(path.join(__dirname, "..", "lib", "orchestrator-stack.ts"))],
      ["cdk/lib/tool-plane.ts", read(path.join(__dirname, "..", "lib", "tool-plane.ts"))],
      ["app/orchestrator/registry.py", read(path.join(ORCH_ROOT, "app", "orchestrator", "registry.py"))],
      ["bff/authz.py", read(path.join(ORCH_ROOT, "bff", "authz.py"))],
    ];
    // COMMENTS ARE STRIPPED FIRST. This is about code that VALIDATES against a copy —
    // the thing that rejects a config the other planes accept. A comment illustrating
    // what a customer might write (`memory.longTerm (["semantic","summary"])`) is
    // documentation, and policing it here would only teach people to reword comments.
    const strip = (src: string) =>
      src.replace(/\/\*[\s\S]*?\*\//g, "").replace(/^\s*(?:\/\/|#).*$/gm, "");

    const offenders: string[] = [];
    for (const name of Object.keys(vocab).filter((k) => !k.startsWith("$"))) {
      const values: string[] = vocab[name].values.filter((v: string) => v !== "");
      // A one-value set has no ORDER to drift and its single string appears all over
      // these files legitimately (`t.lambda_source == "pricing"`), so a literal
      // check on it is all false positives.
      if (values.length < 2) continue;
      // An array or tuple literal holding exactly these strings, in order: `["a", "b"]`
      // in HCL and TypeScript, `("a", "b")` in Python. Prose that happens to name the
      // values is fine — an error message listing them is the point.
      const body = values.map((v) => `"${v.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}"`).join(",\\s*");
      const literal = new RegExp(`[[(]\\s*${body}\\s*[)\\]]`);
      for (const [file, src] of sources) {
        if (literal.test(strip(src))) offenders.push(`${name} in ${file}`);
      }
    }
    expect(offenders).toEqual([]);
  });

  it("ships the file to the BFF, which reads the action names from it", () => {
    // authz.py enforces RBAC inside the Lambda, so the file has to travel with it.
    const stack = read(path.join(__dirname, "..", "lib", "orchestrator-stack.ts"));
    expect(stack).toContain('"vocabulary.json"');
    const bffTf = read(path.join(TF, "bff.tf"));
    expect(bffTf).toContain('filename = "vocabulary.json"');
  });
  it("ships the key spec to the BFF, which validates a build against it before a deploy", () => {
    const { stageBffPackage } = require("../lib/orchestrator-stack");
    const out = fs.mkdtempSync(path.join(require("os").tmpdir(), "bff-"));
    const staged = stageBffPackage(shipped, path.join(out, "pkg"));
    for (const f of ["keys.json", "vocabulary.json", "validate_build.py", "VERSION",
      "deploy-role-policy.json"]) {
      expect(fs.existsSync(path.join(staged, f))).toBe(true);
    }
    fs.rmSync(out, { recursive: true, force: true });
    expect(read(path.join(TF, "bff.tf"))).toContain('filename = "keys.json"');
    expect(read(path.join(TF, "bff.tf"))).toContain('filename = "VERSION"');
    expect(read(path.join(TF, "bff.tf"))).toContain('filename = "deploy-role-policy.json"');
  });

  it("still holds the values the shipped workflow relies on", () => {
    // A vocabulary that lost a value would pass every test above and reject the
    // shipped config, so pin the ones workflow.json actually uses.
    expect(vocab.runtimes.values).toEqual(expect.arrayContaining(["main", "dedicated", "a2a"]));
    expect(vocab.toolTypes.values).toEqual(
      expect.arrayContaining(["kb", "websearch", "mcp", "openapi", "lambda"])
    );
    expect(vocab.a2aAuthModes.values).toContain("sigv4");
    // Read off the shipped agents rather than written out here. As a literal pair this
    // had to be edited every time the sample's remote steps changed, which is the
    // hand-maintained list this whole file exists to get rid of.
    const skills = Object.values<any>(shipped.agents)
      .filter((a) => a.skill)
      .map((a) => a.skill);
    expect(skills.length).toBeGreaterThan(0);
    expect(vocab.a2aLambdaSkills.values).toEqual(expect.arrayContaining(skills));
    expect(vocab.memoryStrategies.values).toEqual(["semantic", "summary", "userPreference", "episodic"]);
    expect(vocab.authorizationActions.values.sort()).toEqual([
      "admin", "audit", "cancel", "decision", "delete", "deploy", "destroy", "evaluate", "insights",
      "rerun", "start",
    ]);
  });
});

// ---------------------------------------------------------------------------
// The shipped config is coherent for both paths
// ---------------------------------------------------------------------------

describe("web search domain filtering", () => {
  // It lived in FOUR keys — a request-level includeDomains/excludeDomains pair and a
  // target-level targetIncludeDomains/targetExcludeDomains pair — for one intent, and
  // the pair with the obvious name was the weaker of the two. Now one `domains` key,
  // applied on the target by both planes. These assert the collapse actually happened
  // everywhere rather than in the plane somebody remembered.
  const REMOVED = [
    "includeDomains",
    "excludeDomains",
    "targetIncludeDomains",
    "targetExcludeDomains",
  ];

  it("is one key in the spec, with the old four recorded as removed", () => {
    const keys = JSON.parse(read(path.join(ORCH_ROOT, "app", "keys.json")));
    expect(keys.tool.keys.domains).toBeDefined();
    for (const old of REMOVED) {
      expect(keys.tool.keys[old]).toBeUndefined();
      // A closed key set REJECTS an old key rather than ignoring it, so the message has
      // to name the replacement or a customer upgrading is left reading a diff.
      expect(keys.tool.$removed[old]).toContain("domains");
    }
  });

  it("is read from `domains` by both IaC planes, and by neither of the old names", () => {
    const tf = read(path.join(TF, "tools.tf"));
    const cdkPlane = read(path.join(__dirname, "..", "lib", "tool-plane.ts"));
    expect(tf).toContain("t.domains.include");
    expect(tf).toContain("t.domains.exclude");
    expect(cdkPlane).toContain("spec.domains?.include");
    expect(cdkPlane).toContain("spec.domains?.exclude");
    // Prose may still explain what was replaced; a READ of an old key may not exist.
    for (const old of REMOVED) {
      expect(tf).not.toMatch(new RegExp(`t\\.${old}`));
      expect(cdkPlane).not.toMatch(new RegExp(`spec\\.${old}`));
    }
  });

  it("is never projected to the runtime by either plane", () => {
    // The point of moving it to the target: the app cannot send a domain filter of its
    // own, so the filter is a boundary rather than a preference. Both projections have
    // to agree on withholding it, or a CDK deployment would scope differently from a
    // Terraform one.
    const tf = read(path.join(TF, "tools.tf"));
    const stack = read(path.join(__dirname, "..", "lib", "orchestrator-stack.ts"));
    // Comments stripped: both projections carry a note SAYING they withhold the domain
    // lists, and the check is about the code, not about whether the code is explained.
    const strip = (src: string) => src.replace(/^\s*(?:\/\/|#).*$/gm, "");
    const window = (src: string, from: string) =>
      strip(src.slice(src.indexOf(from), src.indexOf(from) + 1400));
    // Anchored on the DECLARATIONS, not on a bare name: prose elsewhere in the file
    // may mention the projection by name, and a loose anchor picks the wrong region.
    for (const src of [window(tf, "tools_env = jsonencode("),
      window(stack, "export function toolsEnv")]) {
      expect(src).not.toContain("domains");
      expect(src).toContain("maxResults");   // the projection is not simply empty
    }
    const client = read(path.join(ORCH_ROOT, "app", "features", "gateway", "client.py"));
    expect(client).not.toContain('spec.get("includeDomains")');
    expect(client).not.toContain('spec.get("domains")');
  });
});

describe("the shipped workflow.json", () => {
  it("declares an authorization block with groups for every restricted action", () => {
    const actions: Record<string, string[]> = shipped.authorization.actions;
    expect(Object.keys(actions).length).toBeGreaterThan(0);
    for (const [action, groups] of Object.entries(actions)) {
      expect(Array.isArray(groups)).toBe(true);
      expect(groups.length).toBeGreaterThan(0); // no accidentally-closed action
    }
  });

  it("names every agent's tool in the tools block", () => {
    for (const [id, a] of Object.entries<any>(shipped.agents)) {
      if (a.tool) expect(Object.keys(shipped.tools)).toContain(a.tool);
    }
  });

});

describe("the kb tool's retrieval settings", () => {
  /**
   * Both IaC planes build the KB Lambda's environment independently — `local.kb_*` in
   * terraform/kb.tf and the `environment` block in cdk/lib/tool-plane.ts — and the handler
   * reads it as `os.environ`. Three implementations of one decision, which is the shape
   * that has already drifted twice in this repo (`publishedFrom`/`publishedTo`, then
   * `rowPath`), both times silently and both times found by a human reading wrong output.
   *
   * The failure mode here is worse than a dropped tuning knob. `KB_STATIC_FILTER` is the
   * target-level retrieval filter — the one the agent cannot influence and therefore the
   * one that is a security boundary. A plane that forgets to set it retrieves from OUTSIDE
   * the configured scope, successfully, with no error anywhere.
   */
  const HANDLER_ENV = [
    "KB_NUM_RESULTS",
    "KB_CORPUS_KEY",
    "KB_CORPUS_OPERATOR",
    "KB_STATIC_FILTER",
    "KB_RERANK",
  ];

  it("is read from the environment by the handler, for every variable both planes set", () => {
    const handler = read(path.join(ORCH_ROOT, "kb_lambda", "handler.py"));
    for (const key of HANDLER_ENV) {
      expect(handler).toContain(key);
    }
  });

  it("is set by BOTH IaC planes, so neither silently drops a setting", () => {
    const planes = {
      "terraform/kb.tf": read(path.join(TF, "kb.tf")),
      "cdk/lib/tool-plane.ts": read(path.join(__dirname, "..", "lib", "tool-plane.ts")),
    };
    for (const key of HANDLER_ENV) {
      for (const [where, text] of Object.entries(planes)) {
        expect(text.includes(key)).toBe(true);
        if (!text.includes(key)) throw new Error(`${where} never sets ${key}`);
      }
    }
  });

  it("derives the embedding model and dimension from the vocabulary in both planes", () => {
    // The pair is the dangerous one: a mismatch is not rejected at deploy, Bedrock fails
    // at INGESTION afterwards while the stack reports success and the corpus is empty. So
    // the per-model dimension table has to be the same table on both sides.
    expect(read(path.join(TF, "kb.tf"))).toContain("dimensionsByModel");
    expect(read(path.join(__dirname, "..", "lib", "vocabulary.ts"))).toContain("dimensionsByModel");
    const vocab = JSON.parse(read(path.join(ORCH_ROOT, "app", "vocabulary.json")));
    for (const model of vocab.embeddingModels.values) {
      const dims = vocab.embeddingModels.dimensionsByModel[model];
      expect(Array.isArray(dims) && dims.length).toBeTruthy();
    }
  });

  it("gives each dedicated agent its own role in both planes", () => {
    // Terraform uses for_each on the agents map; CDK creates the Role inside the
    // per-agent loop. Either reverting to one shared role would be invisible without this.
    expect(read(path.join(TF, "subagent_runtimes.tf"))).toContain(
      "for_each = local.dedicated_agents"
    );
    expect(read(path.join(TF, "subagent_runtimes.tf"))).toContain(
      "aws_iam_role.subagent[each.key].arn"
    );
    const stack = read(path.join(__dirname, "..", "lib", "orchestrator-stack.ts"));
    expect(stack).toContain("`SubagentRole-${id}`");
    // And the feature gates, which are what makes the split worth having.
    expect(read(path.join(TF, "subagent_runtimes.tf"))).toContain("subagent_features");
    expect(stack).toMatch(/ac\.guardrails\?\.input \|\| ac\.guardrails\?\.output/);
  });

  it("grants BOTH Function URL actions for the a2a stand-in in both planes", () => {
    // Learned the hard way: a principal holding only lambda:InvokeFunctionUrl is
    // rejected with a bare 403 BEFORE the function is entered, so there is no log line
    // to read and nothing on the function to inspect. lambda:InvokeFunction is required
    // as well. CDK gets the pair from grantInvokeUrl(); Terraform has to spell it out.
    // Dropping either action in either plane brings the whole A2A stage down, which is
    // exactly the failure this asserts against.
    const tf = read(path.join(TF, "a2a.tf"));
    expect(tf).toContain('"lambda:InvokeFunctionUrl"');
    expect(tf).toContain('"lambda:InvokeFunction"');
    expect(tf).toContain('"lambda:InvokedViaFunctionUrl" = "true"');
    const stack = read(path.join(__dirname, "..", "lib", "orchestrator-stack.ts"));
    expect(stack).toContain("grantInvokeUrl(runtimeRole)");
  });

  it("grants the full ECR pull set to every runtime role in both planes", () => {
    // BatchCheckLayerAvailability is part of a pull and its absence only shows up as an
    // intermittent cold-start failure, so both planes state it explicitly.
    for (const f of ["main.tf", "subagent_runtimes.tf"]) {
      expect(read(path.join(TF, f))).toContain('"ecr:BatchCheckLayerAvailability"');
    }
    // CDK gets the same set from grantPull() on the asset's repository.
    expect(read(path.join(__dirname, "..", "lib", "orchestrator-stack.ts")))
      .toMatch(/grantPull\(/);
  });

  it("spells out DynamoDB actions in both planes instead of using CDK's grant macros", () => {
    // grantReadWriteData() also hands over BatchGetItem/BatchWriteItem/
    // ConditionCheckItem/DescribeTable and the stream reads. Nothing in the app calls
    // any of them, and Terraform never granted them — so the macro was both over-broad
    // and a silent divergence between the two deployments.
    const stack = read(path.join(__dirname, "..", "lib", "orchestrator-stack.ts"));
    // Comments stripped: each explicit grant NAMES the macro it replaced, and the check
    // is about the code, not about whether the code is explained.
    const code = stack.replace(/^\s*\/\/.*$/gm, "");
    for (const macro of ["grantReadWriteData", "grantWriteData", "grantReadData"]) {
      expect(code).not.toContain(macro);
    }
    expect(stack).toContain('sid: "ProgressStoreWrite"');
    expect(read(path.join(TF, "main.tf"))).toContain('Sid      = "ProgressStoreWrite"');
  });
});

describe("Cedar policy names", () => {
  it("are unique per deployment, the same way on both planes", () => {
    // Policy names are unique in the ACCOUNT, so two deployments with the same tool key
    // must not both be permit_<key>.
    const { cedarPolicyName } = require("../lib/tool-plane");
    const a = cedarPolicyName("pricing", "agentexpress");
    expect(a).toMatch(/^permit_pricing_[0-9a-f]{8}$/);
    expect(cedarPolicyName("pricing", "ax_4f67c298")).not.toBe(a);
    const sha = crypto.createHash("sha1").update("agentexpress").digest("hex").slice(0, 8);
    expect(a).toBe(`permit_pricing_${sha}`);
    const policyTf = fs.readFileSync(path.join(TF, "policy.tf"), "utf8");
    expect(policyTf).toContain("cedar_policy_suffix = substr(sha1(var.agent_name), 0, 8)");
    expect(policyTf).toContain('name             = "permit_${each.key}_${local.cedar_policy_suffix}"');
  });
  it("name a custom policy custom_<name>_<suffix> and fill in the Gateway on both planes", () => {
    const { customPolicyName, GATEWAY_PLACEHOLDER } = require("../lib/tool-plane");
    const sha = crypto.createHash("sha1").update("agentexpress").digest("hex").slice(0, 8);
    expect(customPolicyName("noBigRefunds", "agentexpress")).toBe(`custom_noBigRefunds_${sha}`);
    const policyTf = fs.readFileSync(path.join(TF, "policy.tf"), "utf8");
    expect(policyTf).toContain('name             = "custom_${each.key}_${local.cedar_policy_suffix}"');
    expect(policyTf).toContain(`replace(each.value.statement, "${GATEWAY_PLACEHOLDER}", local.gateway_arn_for_policy)`);
    // Every Gateway target type orders before the custom policies, as before the permits.
    const targets = [...fs.readdirSync(TF).filter((f) => f.endsWith(".tf"))
      .map((f) => fs.readFileSync(path.join(TF, f), "utf8")).join("\n")
      .matchAll(/resource "aws_bedrockagentcore_gateway_target" "(\w+)"/g)].map((m) => m[1]);
    const custom = policyTf.split('resource "aws_bedrockagentcore_policy" "custom"')[1];
    const permits = policyTf.split('resource "aws_bedrockagentcore_policy" "tool"')[1].split("\n}\n")[0];
    for (const t of targets) {
      expect(custom).toContain(`aws_bedrockagentcore_gateway_target.${t},`);
      expect(permits).toContain(`aws_bedrockagentcore_gateway_target.${t},`);
    }
  });
});
