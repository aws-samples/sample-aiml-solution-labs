/**
 * The ToolPlane construct, synthesized directly.
 *
 * `stack.test.ts` covers the whole stack against the shipped `workflow.json`, which
 * declares no `lambda` tool — a real `lambdaArn` is account-specific and would pin
 * the committed config to one AWS account. ToolPlane takes its `tools` as a prop,
 * though, so it can be instantiated with any tools block. That is what this file
 * does: it exercises the target shapes, the IAM grants and the Cedar permits for
 * configurations the sample does not itself ship.
 */

import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import { aws_dynamodb as dynamodb } from "aws-cdk-lib";
import * as path from "path";

import { CODE_BOUNDARY_DENY, customPolicyName, CustomPolicy, interceptorFunctionName, parseS3Location, ToolPlane, ToolSpec } from "../lib/tool-plane";
import { validateTools } from "../lib/orchestrator-stack";

const ORCH_ROOT = path.join(__dirname, "..", "..");
const ACCOUNT = "123456789012";

function plane(tools: Record<string, ToolSpec>, toolApiKeys: Record<string, string> = {},
  customPolicies: CustomPolicy[] = [], extra: Record<string, unknown> = {}) {
  const app = new cdk.App();
  const stack = new cdk.Stack(app, "T", { env: { account: ACCOUNT, region: "us-east-1" } });
  new ToolPlane(stack, "ToolPlane", {
    ...extra,
    agentName: "test_orch",
    tools,
    toolApiKeys,
    gatewayDiscoveryUrl:
      "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc/.well-known/openid-configuration",
    gatewayClientId: "client-abc",
    gatewayAudience: "gateway/invoke",
    isCognito: true,
    policyEnabled: true,
    policyMode: "ENFORCE",
    customPolicies,
    orchRoot: ORCH_ROOT,
  });
  return Template.fromStack(stack);
}

const QUERY_CLAIMS = {
  name: "query_claims",
  description: "Answer a question about claims using the warehouse.",
  properties: {
    question: { type: "string", required: true, description: "The question to answer." },
    limit: { type: "integer", required: false, description: "Max rows to consider." },
  },
};

const LAMBDA_TOOL: ToolSpec = {
  type: "lambda",
  description: "Read-only questions answered from the claims warehouse.",
  lambdaArn: `arn:aws:lambda:us-east-1:${ACCOUNT}:function:query-claims`,
  arg: "question",
  toolSchema: [QUERY_CLAIMS],
};

/** A statement's Resource, always as a list (CFN collapses a single element). */
function asList(v: any): any[] {
  return Array.isArray(v) ? v : [v];
}

/** All statements from every inline policy in the template, flattened. */
function statements(t: Template): any[] {
  return Object.values<any>(t.findResources("AWS::IAM::Policy")).flatMap(
    (p) => p.Properties.PolicyDocument.Statement
  );
}

function lambdaTarget(t: Template): any {
  const target = Object.values<any>(
    t.findResources("AWS::BedrockAgentCore::GatewayTarget")
  ).find((r) => r.Properties.TargetConfiguration?.Mcp?.Lambda);
  expect(target).toBeDefined();
  return target.Properties.TargetConfiguration.Mcp.Lambda;
}

function connectorTarget(t: Template): any {
  const target = Object.values<any>(
    t.findResources("AWS::BedrockAgentCore::GatewayTarget")
  ).find((r) => r.Properties.TargetConfiguration?.Mcp?.Connector);
  expect(target).toBeDefined();
  return target.Properties.TargetConfiguration.Mcp.Connector;
}

// The websearch connector target had NO coverage here at all, which is how collapsing
// four domain keys into `domains` passed this suite without a single change. A filter
// that silently stops being applied is the worst case for this particular key: results
// keep coming back and nothing says they are no longer scoped.
describe("type=websearch target", () => {
  it("applies `domains` on the TARGET, where the agent cannot reach it", () => {
    const connector = connectorTarget(
      plane({
        ws: {
          type: "websearch",
          description: "Managed web search.",
          domains: { include: ["docs.aws.amazon.com"], exclude: ["spam.example"] },
        },
      })
    );
    expect(connector.Source.ConnectorId).toBe("web-search");
    expect(connector.Configurations[0].ParameterValues).toEqual({
      domainFilter: { include: ["docs.aws.amazon.com"], exclude: ["spam.example"] },
    });
  });

  it("accepts either half on its own", () => {
    for (const domains of [{ include: ["a.example"] }, { exclude: ["b.example"] }]) {
      const values = connectorTarget(plane({ ws: { type: "websearch", domains } }))
        .Configurations[0].ParameterValues;
      expect(values.domainFilter).toEqual(domains);
    }
  });

  it("emits {} rather than an empty filter when no domains are configured", () => {
    // Not cosmetic: the API DISCARDS a configuration entry with no ParameterValues and
    // then reports "Connector configurations must not be empty", which reads as if the
    // list were absent rather than its single entry dropped.
    const connector = connectorTarget(plane({ ws: { type: "websearch" } }));
    expect(connector.Configurations[0].ParameterValues).toEqual({});
    expect(connector.Configurations[0].Name).toBe("WebSearch");
  });
});

describe("type=lambda target", () => {
  const template = plane({ claims: LAMBDA_TOOL });

  it("registers the function by the ARN from config", () => {
    expect(lambdaTarget(template).LambdaArn).toBe(LAMBDA_TOOL.lambdaArn);
  });

  it("publishes the declared tool with its description", () => {
    const payload = lambdaTarget(template).ToolSchema.InlinePayload;
    expect(payload).toHaveLength(1);
    expect(payload[0].Name).toBe("query_claims");
    expect(payload[0].Description).toBe(QUERY_CLAIMS.description);
  });

  it("turns per-property `required` into JSON Schema's object-level list", () => {
    // The config (and the Terraform provider's flattened `property` block) put
    // `required` on each property; JSON Schema puts it on the object as a list of
    // names. Both paths therefore accept the same JSON.
    //
    // The nested keys render PascalCase (Type/Description/Required) because CDK maps
    // the L1 property tree that way. That is the same shape the KB target has
    // produced all along, which is deployed and working — so it is the known-good
    // form rather than an assumption.
    const schema = lambdaTarget(template).ToolSchema.InlinePayload[0].InputSchema;
    expect(schema.Type).toBe("object");
    expect(Object.keys(schema.Properties).sort()).toEqual(["limit", "question"]);
    expect(schema.Properties.question).toEqual({
      Type: "string",
      Description: "The question to answer.",
    });
    expect(schema.Required).toEqual(["question"]);
  });

  it("produces the same target shape as the KB Lambda target", () => {
    // The KB target is the one Lambda target that has been deployed and exercised
    // end to end. Matching its shape is the strongest available evidence that a
    // customer's own function will register and invoke correctly.
    const lt = lambdaTarget(template);
    expect(Object.keys(lt).sort()).toEqual(["LambdaArn", "ToolSchema"]);
    expect(Object.keys(lt.ToolSchema)).toEqual(["InlinePayload"]);
    expect(Object.keys(lt.ToolSchema.InlinePayload[0]).sort()).toEqual([
      "Description",
      "InputSchema",
      "Name",
    ]);
  });

  it("lets the Gateway invoke the function with its own role", () => {
    // No secret is involved: the credential provider is GATEWAY_IAM_ROLE.
    const target = Object.values<any>(
      template.findResources("AWS::BedrockAgentCore::GatewayTarget")
    ).find((r) => r.Properties.TargetConfiguration?.Mcp?.Lambda);
    expect(target.Properties.CredentialProviderConfigurations).toEqual([
      { CredentialProviderType: "GATEWAY_IAM_ROLE" },
    ]);
  });

  it("grants lambda:InvokeFunction on exactly that function", () => {
    const invoke = statements(template).find((s) => s.Sid === "InvokeToolLambdas");
    expect(invoke).toBeDefined();
    expect(asList(invoke.Action)).toEqual(["lambda:InvokeFunction"]);
    expect(asList(invoke.Resource)).toEqual([LAMBDA_TOOL.lambdaArn]);
  });

  it("adds the function's resource policy statement for a same-account function", () => {
    template.hasResourceProperties("AWS::Lambda::Permission", {
      FunctionName: LAMBDA_TOOL.lambdaArn,
      Action: "lambda:InvokeFunction",
      Principal: "bedrock-agentcore.amazonaws.com",
      SourceAccount: ACCOUNT,
    });
  });

  it("emits a Cedar permit naming the declared tool", () => {
    const statement = Object.values<any>(
      template.findResources("AWS::BedrockAgentCore::Policy")
    )[0].Properties.Definition.Cedar.Statement;
    const text = (statement["Fn::Join"][1] as any[])
      .map((p) => (typeof p === "string" ? p : ""))
      .join("");
    // No `policy.tool`, so the whole target is permitted as an action group — right
    // for a function that may publish several tools.
    expect(text).toContain('action in AgentCore::Action::"claims"');
  });

  it("can be narrowed to one tool with an argument restriction", () => {
    const t = plane({
      claims: {
        ...LAMBDA_TOOL,
        policy: { tool: "query_claims", restrictTo: { region: ["emea", "apac"] } },
      },
    });
    const statement = Object.values<any>(t.findResources("AWS::BedrockAgentCore::Policy"))[0]
      .Properties.Definition.Cedar.Statement;
    const text = (statement["Fn::Join"][1] as any[])
      .map((p) => (typeof p === "string" ? p : ""))
      .join("");
    expect(text).toContain('action == AgentCore::Action::"claims___query_claims"');
    expect(text).toContain('["emea","apac"].contains(context.input.region)');
  });
});

describe("the built-in demo function (source: pricing)", () => {
  // This is what makes `type: "lambda"` demonstrable out of the box: the framework
  // deploys one function so the tool type has a live agent, without asking anyone to
  // stand up a database first. A customer's own function goes in via `lambdaArn` and
  // none of this applies to it.
  const BUILTIN: ToolSpec = {
    type: "lambda",
    source: "pricing",
    call: "aws_prices",
    arg: "services",
    toolSchema: [
      { name: "aws_prices", properties: { services: { type: "string", required: true } } },
    ],
  };

  function withTables() {
    const app = new cdk.App();
    const stack = new cdk.Stack(app, "T", { env: { account: ACCOUNT, region: "us-east-1" } });
    const status = new dynamodb.Table(stack, "Status", {
      partitionKey: { name: "session_id", type: dynamodb.AttributeType.STRING },
    });
    const telemetry = new dynamodb.Table(stack, "Telemetry", {
      partitionKey: { name: "session_id", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "sk", type: dynamodb.AttributeType.STRING },
    });
    new ToolPlane(stack, "ToolPlane", {
      agentName: "test_orch",
      tools: { pricing: BUILTIN },
      toolApiKeys: {},
      gatewayDiscoveryUrl: "https://example.test/.well-known/openid-configuration",
      gatewayClientId: "client-abc",
      gatewayAudience: "gateway/invoke",
      isCognito: true,
      policyEnabled: false,
      policyMode: "ENFORCE",
      orchRoot: ORCH_ROOT,
      statusTable: status,
      telemetryTable: telemetry,
    });
    return Template.fromStack(stack);
  }

  const template = withTables();

  it("deploys the function from orchestrator/app/tools/pricing/", () => {
    // Name prefixed with ToolLambda- so a scoped deploy policy can express it
    // without granting lambda:* on every function in the account.
    template.hasResourceProperties("AWS::Lambda::Function", {
      FunctionName: "ToolLambda-test_orch-pricing",
      Handler: "handler.lambda_handler",
      Runtime: "python3.12",
    });
  });

  it("pins the Price List API endpoint region, and passes nothing else", () => {
    // The Price List Query API is published in only two regions and describes prices
    // for all of them, so this is where the ENDPOINT lives — not the region being
    // priced. Pinned so the tool works on a deployment anywhere.
    const fn = Object.values<any>(template.findResources("AWS::Lambda::Function")).find(
      (f) => f.Properties.FunctionName === "ToolLambda-test_orch-pricing"
    );
    const vars = fn.Properties.Environment.Variables;
    expect(Object.keys(vars).sort()).toEqual(["PRICING_API_REGION"]);
    expect(vars.PRICING_API_REGION).toBe("us-east-1");
  });

  it("reads the PUBLIC price list and touches no customer data", () => {
    // This is what lets `source` deploy a framework-owned function at all: its
    // execution role is fixed and reads nothing belonging to the customer. A demo
    // function with access to the framework's own state would be a poor example.
    const actions = statements(template).flatMap((s) => asList(s.Action)).map(String);
    expect(actions).toContain("pricing:GetProducts");
    expect(actions).toContain("pricing:DescribeServices");
    expect(actions).not.toContain("pricing:*");
    // And NOTHING anywhere in this template touches the framework's own datastore.
    // The demo function used to hold a DynamoDB read grant on the run tables; it
    // reads the public price list now, so a `dynamodb:` action reappearing here
    // means a data-plane grant crept back into a framework-deployed function.
    expect(actions.filter((a) => /^dynamodb:/i.test(a))).toEqual([]);
  });

  it("registers the target against the function it just deployed", () => {
    // Not a literal ARN from config — a Fn::GetAtt on the function in this stack,
    // which is what keeps the committed workflow.json account-neutral.
    const arn = lambdaTarget(template).LambdaArn;
    expect(arn["Fn::GetAtt"][0]).toMatch(/ToolLambdapricing/);
    expect(arn["Fn::GetAtt"][1]).toBe("Arn");
  });

  it("grants the Gateway invoke on it and adds the resource policy", () => {
    const invoke = statements(template).find((s) => s.Sid === "InvokeToolLambdas");
    expect(invoke).toBeDefined();
    template.hasResourceProperties("AWS::Lambda::Permission", {
      Principal: "bedrock-agentcore.amazonaws.com",
      SourceAccount: ACCOUNT,
    });
  });

  it("needs no deployment tables at all", () => {
    // It reads only the public price list, so it deploys without being handed any
    // of this deployment's state. That is what makes the fixed execution role
    // behind `source` defensible: there is nothing of the customer's in it.
    expect(() => plane({ pricing: BUILTIN })).not.toThrow();
  });
});

describe("a cross-account lambda tool", () => {
  const CROSS = "arn:aws:lambda:us-east-1:999999999999:function:shared-tool";
  const template = plane({ shared: { ...LAMBDA_TOOL, lambdaArn: CROSS } });

  it("is still granted on the Gateway role", () => {
    const invoke = statements(template).find((s) => s.Sid === "InvokeToolLambdas");
    expect(asList(invoke.Resource)).toEqual([CROSS]);
  });

  it("gets NO resource-policy statement, because we cannot edit another account's", () => {
    // Emitting one would fail the deploy. The owning account adds it instead — which
    // is documented rather than silently required.
    const perms = Object.values<any>(template.findResources("AWS::Lambda::Permission")).map(
      (p) => p.Properties.FunctionName
    );
    expect(perms).not.toContain(CROSS);
  });
});

describe("several lambda tools", () => {
  const A = `arn:aws:lambda:us-east-1:${ACCOUNT}:function:tool-a`;
  const B = `arn:aws:lambda:us-east-1:${ACCOUNT}:function:tool-b`;
  const template = plane({
    b: { ...LAMBDA_TOOL, lambdaArn: B },
    a: { ...LAMBDA_TOOL, lambdaArn: A },
  });

  it("collects every function into ONE sorted IAM statement", () => {
    // One statement rather than one policy per tool, so the inline policy stays well
    // inside its size limit as the tool count grows. Sorted so the template is
    // stable and a no-op redeploy shows no diff.
    const invoke = statements(template).find((s) => s.Sid === "InvokeToolLambdas");
    expect(asList(invoke.Resource)).toEqual([A, B]);
  });

  it("creates a target and a permission for each", () => {
    template.resourceCountIs("AWS::BedrockAgentCore::GatewayTarget", 2);
    template.resourceCountIs("AWS::Lambda::Permission", 2);
  });
});

describe("one function publishing several tools", () => {
  const template = plane({
    warehouse: {
      ...LAMBDA_TOOL,
      call: "query_claims",
      toolSchema: [
        QUERY_CLAIMS,
        {
          name: "list_tables",
          properties: { schema: { type: "string", required: true } },
        },
      ],
    },
  });

  it("publishes both, so the handler can dispatch on the tool name", () => {
    const payload = lambdaTarget(template).ToolSchema.InlinePayload;
    expect(payload.map((p: any) => p.Name)).toEqual(["query_claims", "list_tables"]);
  });

  it("falls back to the tool name as its description", () => {
    const payload = lambdaTarget(template).ToolSchema.InlinePayload;
    expect(payload[1].Description).toBe("list_tables");
  });

  it("still grants only one function", () => {
    const invoke = statements(template).find((s) => s.Sid === "InvokeToolLambdas");
    expect(asList(invoke.Resource)).toEqual([LAMBDA_TOOL.lambdaArn]);
  });
});

describe("no lambda tools declared", () => {
  it("grants nothing and creates no permission", () => {
    // The grant is conditional, so a deployment with no lambda tool carries no
    // lambda:InvokeFunction for one.
    const template = plane({ docs: { type: "mcp", endpoint: "https://x.test/mcp" } });
    expect(statements(template).find((s) => s.Sid === "InvokeToolLambdas")).toBeUndefined();
    template.resourceCountIs("AWS::Lambda::Permission", 0);
  });
});

// ===========================================================================
// type=openapi
// ===========================================================================
// The shipped workflow.json now declares one of these, so stack.test.ts covers the
// `source` path end to end. What is here is the part that file cannot reach: the
// customer-hosted `schemaS3Uri` path, and the IAM grant that made the whole tool type
// work. That grant did not exist before — the Gateway role had no s3:GetObject at all,
// so an openapi target failed when the Gateway tried to LOAD its schema, with a message
// naming neither the bucket nor the permission. A tool type that cannot work is worse
// than one that is absent, because the config accepts it.
describe("type=openapi", () => {
  const SOURCE_TOOL: ToolSpec = {
    type: "openapi",
    description: "Product lifecycle dates, from a schema the framework uploads.",
    source: "lifecycle",
    call: "getProductLifecycle",
    arg: "product",
  };
  const HOSTED_TOOL: ToolSpec = {
    type: "openapi",
    description: "An API whose schema the customer already hosts.",
    schemaS3Uri: "s3://customer-schemas/orders/v2.json",
    call: "listOrders",
    arg: "query",
  };

  it("uploads a `source` schema and points the target at the derived URI", () => {
    // The whole reason `source` exists: a bucket name is as account-specific as a
    // function ARN, so a committed workflow.json cannot carry one. The URI is
    // therefore DERIVED, and the target must read the derived value rather than a
    // literal — otherwise the sample only deploys in the account that wrote it.
    const template = plane({ lifecycle: SOURCE_TOOL });
    template.hasResourceProperties("AWS::S3::Bucket", {
      BucketName: `agentcore-test-orch-schemas-${ACCOUNT}`,
      PublicAccessBlockConfiguration: {
        BlockPublicAcls: true,
        BlockPublicPolicy: true,
        IgnorePublicAcls: true,
        RestrictPublicBuckets: true,
      },
      VersioningConfiguration: { Status: "Enabled" },
    });
    const target = Object.values<any>(
      template.findResources("AWS::BedrockAgentCore::GatewayTarget")
    )[0];
    const uri = target.Properties.TargetConfiguration.Mcp.OpenApiSchema.S3.Uri;
    // Built from the bucket ref, so it is a CFN join rather than a plain string.
    expect(JSON.stringify(uri)).toContain("lifecycle/openapi.json");
  });

  it("creates no bucket when every schema is customer-hosted", () => {
    // A deployment that hosts its own schemas should pay for nothing. The bucket is
    // conditional on some tool actually using `source`.
    const template = plane({ orders: HOSTED_TOOL });
    const buckets = Object.values<any>(template.findResources("AWS::S3::Bucket")).filter(
      (b) => String(b.Properties?.BucketName ?? "").includes("schemas")
    );
    expect(buckets).toHaveLength(0);
    const target = Object.values<any>(
      template.findResources("AWS::BedrockAgentCore::GatewayTarget")
    )[0];
    expect(target.Properties.TargetConfiguration.Mcp.OpenApiSchema.S3.Uri).toEqual(
      "s3://customer-schemas/orders/v2.json"
    );
  });

  it("grants the Gateway role GetObject on exactly the schema objects declared", () => {
    // Scoped to the object, not the bucket and not "*". A schema enumerates which
    // operations an agent may reach, so read access to all of them is a wider grant
    // than this tool type needs.
    const template = plane({ lifecycle: SOURCE_TOOL, orders: HOSTED_TOOL });
    const grant = statements(template).find((s) => s.Sid === "ReadOpenApiSchemas");
    expect(grant).toBeDefined();
    expect(grant.Action).toEqual("s3:GetObject");
    const resources = asList(grant.Resource);
    // The customer-hosted one is a literal ARN built from the parsed URI.
    expect(resources).toContain("arn:aws:s3:::customer-schemas/orders/v2.json");
    // The framework-uploaded one is a ref to the bucket it just created.
    expect(JSON.stringify(resources)).toContain("ToolSchemasBucket");
  });

  it("adds no grant at all when no openapi tool is declared", () => {
    const template = plane({ fn: LAMBDA_TOOL });
    expect(statements(template).find((s) => s.Sid === "ReadOpenApiSchemas")).toBeUndefined();
  });

  it("publishes a Cedar permit for an openapi tool like any other type", () => {
    // The tool type must not be a hole in authorization. `openapi` is the one type
    // whose tool NAMES come from the schema's operationIds rather than from config,
    // which is exactly the case where a permit could plausibly have been skipped.
    const template = plane({ lifecycle: SOURCE_TOOL });
    const policies = Object.values<any>(
      template.findResources("AWS::BedrockAgentCore::Policy")
    );
    expect(JSON.stringify(policies)).toContain("permit_lifecycle");
  });
});

// ===========================================================================
// type=kb — embedding model, dimensions, and the retrieval shape
// ===========================================================================
// All of this was hardcoded: the embedding model and dimension here and in
// terraform/kb.tf, and the retrieval request shape inside kb_lambda/handler.py. The
// dimension is the dangerous one to get wrong, because a model/dimension mismatch is not
// rejected at deploy — Bedrock fails at INGESTION afterwards, so the stack reports success
// and the corpus is silently empty. That is why the pair is validated at synth.
describe("type=kb config", () => {
  const kb = (over: Record<string, any> = {}): Record<string, ToolSpec> => ({
    kb: { type: "kb", description: "Corpus", corpora: ["reference"], ...over } as ToolSpec,
  });
  const kbLambdaEnv = (template: Template) =>
    Object.values<any>(template.findResources("AWS::Lambda::Function")).find((f) =>
      String(f.Properties.FunctionName ?? "").includes("KBRetrieve")
    ).Properties.Environment.Variables;

  it("defaults to Titan v2 at 1024 dimensions when nothing is declared", () => {
    // The previous hardcoded values, so an existing workflow.json deploys unchanged.
    const template = plane(kb());
    template.hasResourceProperties("AWS::S3Vectors::Index", { Dimension: 1024 });
    const knowledgeBase = Object.values<any>(
      template.findResources("AWS::Bedrock::KnowledgeBase")
    )[0];
    expect(
      JSON.stringify(knowledgeBase.Properties.KnowledgeBaseConfiguration)
    ).toContain("amazon.titan-embed-text-v2:0");
  });

  it("takes the model and dimension from config", () => {
    const template = plane(kb({ embeddingModel: "amazon.titan-embed-text-v2:0", dimensions: 256 }));
    template.hasResourceProperties("AWS::S3Vectors::Index", { Dimension: 256 });
  });

  it("derives the index and KB names from the dimension, so a change REPLACES them", () => {
    // Neither the dimension nor the model can be altered in place, and CloudFormation
    // refuses to replace a resource with a fixed custom name. Digest-derived names are
    // what make changing the embedding model a working deploy instead of a stuck one.
    const nameOf = (t: Template, type: string, prop: string) =>
      Object.values<any>(t.findResources(type))[0].Properties[prop];
    const a = plane(kb({ dimensions: 1024 }));
    const b = plane(kb({ dimensions: 256 }));
    expect(nameOf(a, "AWS::S3Vectors::Index", "IndexName")).not.toEqual(
      nameOf(b, "AWS::S3Vectors::Index", "IndexName")
    );
    expect(nameOf(a, "AWS::Bedrock::KnowledgeBase", "Name")).not.toEqual(
      nameOf(b, "AWS::Bedrock::KnowledgeBase", "Name")
    );
  });

  it("refuses a dimension the chosen model does not support", () => {
    // Caught at synth because the alternative is a green deploy and an empty corpus.
    expect(() => plane(kb({ embeddingModel: "cohere.embed-english-v3", dimensions: 256 }))).toThrow(
      /does not support .* accepts \[1024\]/s
    );
    // Titan v1 is 1536-only, so even the usual default is wrong for it — which is the
    // case a per-model dimension table exists to catch.
    expect(() => plane(kb({ embeddingModel: "amazon.titan-embed-text-v1", dimensions: 1024 }))).toThrow(
      /does not support/
    );
    expect(() => plane(kb({ embeddingModel: "amazon.titan-embed-text-v1" }))).not.toThrow();
  });

  it("passes the retrieval shape to the Lambda, with documented defaults", () => {
    const env = kbLambdaEnv(plane(kb()));
    expect(env.KB_CORPUS_KEY).toEqual("doc_type");
    expect(env.KB_CORPUS_OPERATOR).toEqual("equals");
    // Empty means "keep the handler's default", so the IaC does not restate a default
    // that already has one home. Mirrors terraform/kb.tf.
    expect(env.KB_STATIC_FILTER).toEqual("");
    expect(env.KB_RERANK).toEqual("");
  });

  it("passes a configured corpus key and target filter through", () => {
    const filter = { andAll: [{ equals: { key: "tier", value: "public" } }] };
    const env = kbLambdaEnv(
      plane(kb({ corpusKey: "product_line", corpusOperator: "startsWith", filter }))
    );
    expect(env.KB_CORPUS_KEY).toEqual("product_line");
    expect(env.KB_CORPUS_OPERATOR).toEqual("startsWith");
    expect(JSON.parse(env.KB_STATIC_FILTER)).toEqual(filter);
  });

  it("grants the reranking model only when reranking is configured, and only that model", () => {
    // Reranking is a second model call Bedrock makes on the function's behalf, so the
    // function's own role needs it — but granting foundation-model/* to a retrieve Lambda
    // would hand it every model in the account.
    const withRerank = plane(kb({ rerank: { model: "amazon.rerank-v1:0", count: 3 } }));
    const grant = statements(withRerank).find((s) => s.Sid === "InvokeRerankingModel");
    expect(grant).toBeDefined();
    expect(asList(grant.Resource)[0]).toContain("foundation-model/amazon.rerank-v1:0");
    expect(JSON.stringify(grant.Resource)).not.toContain("foundation-model/*");

    expect(statements(plane(kb())).find((s) => s.Sid === "InvokeRerankingModel")).toBeUndefined();
  });
});

describe("type=kb document sources", () => {
  const kb = (over: Record<string, any> = {}): Record<string, ToolSpec> => ({
    kb: { type: "kb", description: "Corpus", corpora: ["reference"], ...over } as ToolSpec,
  });
  const kbRolePolicy = (t: Template) => JSON.stringify(Object.entries(t.findResources("AWS::IAM::Policy"))
    .filter(([id]) => id.includes("KbRole")).map(([, p]) => p));
  const retrieveEnv = (t: Template) => Object.values<any>(t.findResources("AWS::Lambda::Function"))
    .find((f) => String(f.Properties.FunctionName ?? "").includes("KBRetrieve")).Properties.Environment.Variables;

  it("parses an s3 location into bucket and prefix", () => {
    expect(parseS3Location("s3://my-docs")).toEqual({ bucket: "my-docs", prefix: "" });
    expect(parseS3Location("s3://my-docs/")).toEqual({ bucket: "my-docs", prefix: "" });
    expect(parseS3Location("s3://my-docs/policies")).toEqual({ bucket: "my-docs", prefix: "policies/" });
    expect(parseS3Location("s3://my-docs/a/b/")).toEqual({ bucket: "my-docs", prefix: "a/b/" });
  });

  it("builds from YOUR bucket: reads only that prefix, decrypts with your key, uploads nothing", () => {
    const key = `arn:aws:kms:us-east-1:${ACCOUNT}:key/1234abcd-12ab`;
    const t = plane(kb({ s3Uri: "s3://my-docs/policies", kmsKeyArn: key }));
    expect(Object.keys(t.findResources("AWS::S3::Bucket")).filter((k) => k.startsWith("ToolPlaneKbDocs"))).toEqual([]);
    t.hasResourceProperties("AWS::Bedrock::DataSource", {
      DataSourceConfiguration: { Type: "S3", S3Configuration: { BucketArn: "arn:aws:s3:::my-docs", InclusionPrefixes: ["policies/"] } },
    });
    const policy = kbRolePolicy(t);
    expect(policy).toContain("arn:aws:s3:::my-docs/policies/*");
    expect(policy).toContain('"s3:prefix":["policies/*"]');
    expect(policy).toContain(key);
    expect(policy).not.toContain("s3:PutObject");
    t.resourceCountIs("AWS::Bedrock::KnowledgeBase", 1);
  });

  it("uses YOUR Knowledge Base: creates none, retrieves from exactly that one", () => {
    const t = plane(kb({ knowledgeBaseId: "ABCDEFGH12" }));
    t.resourceCountIs("AWS::Bedrock::KnowledgeBase", 0);
    t.resourceCountIs("AWS::Bedrock::DataSource", 0);
    t.resourceCountIs("AWS::S3Vectors::VectorBucket", 0);
    expect(retrieveEnv(t).KB_ID).toBe("ABCDEFGH12");
    const policies = JSON.stringify(t.findResources("AWS::IAM::Policy"));
    expect(policies).toContain(`arn:aws:bedrock:us-east-1:${ACCOUNT}:knowledge-base/ABCDEFGH12`);
    // Still a Gateway target like any kb.
    expect(Object.keys(t.findResources("AWS::BedrockAgentCore::GatewayTarget")).length).toBe(1);
  });

  it("refuses a source mix that cannot work", () => {
    const tools = (over: Record<string, any>) => ({ kb: { type: "kb", corpora: ["r"], ...over } });
    expect(() => validateTools(tools({ knowledgeBaseId: "ABCDEFGH12", s3Uri: "s3://b1b" }), {}, "us-east-1"))
      .toThrow(/do not apply/);
    expect(() => validateTools(tools({ knowledgeBaseId: "kb-1" }), {}, "us-east-1")).toThrow(/10 capital/);
    expect(() => validateTools(tools({ kmsKeyArn: `arn:aws:kms:us-east-1:${ACCOUNT}:key/x` }), {}, "us-east-1"))
      .toThrow(/only applies with s3Uri/);
    expect(() => validateTools(tools({ s3Uri: "https://x" }), {}, "us-east-1")).toThrow(/s3:\/\/<bucket>/);
  });
});

describe("type=apigateway, an inline OpenAPI schema, and OAuth client credentials", () => {
  const target = (t: Template, name: string) => Object.values<any>(t.findResources("AWS::BedrockAgentCore::GatewayTarget"))
    .find((r) => r.Properties.Name === name).Properties;
  const API: ToolSpec = {
    type: "apigateway", description: "Orders", restApiId: "a1b2c3d4e5", stage: "prod",
    toolFilters: [{ path: "/orders/*", methods: ["GET"] }],
    toolOverrides: [{ path: "/orders/{id}", method: "GET", name: "getOrder" }], auth: "sigv4",
  } as ToolSpec;
  const DOC = { openapi: "3.0.1", info: { title: "Orders", version: "1" },
    paths: { "/orders/{id}": { get: { operationId: "getOrder", responses: { 200: { description: "ok" } } } } } };

  it("points an API Gateway target at the stage, the filters and the overrides, signed as the Gateway", () => {
    const t = plane({ orders: API });
    const p = target(t, "orders");
    expect(p.TargetConfiguration.Mcp.ApiGateway).toEqual({
      RestApiId: "a1b2c3d4e5", Stage: "prod",
      ApiGatewayToolConfiguration: {
        ToolFilters: [{ FilterPath: "/orders/*", Methods: ["GET"] }],
        ToolOverrides: [{ Path: "/orders/{id}", Method: "GET", Name: "getOrder" }],
      },
    });
    expect(p.CredentialProviderConfigurations[0].CredentialProviderType).toBe("GATEWAY_IAM_ROLE");
    // The role alone: AgentCore refuses an IamCredentialProvider on this target type.
    expect(p.CredentialProviderConfigurations[0].CredentialProvider).toBeUndefined();
    const policies = JSON.stringify(t.findResources("AWS::IAM::Policy"));
    expect(policies).toContain(`arn:aws:execute-api:us-east-1:${ACCOUNT}:a1b2c3d4e5/prod/*/*`);
  });

  it("takes an API key for an API Gateway target like any keyed target", () => {
    const t = plane({ orders: { ...API, auth: "apikey" } as ToolSpec }, { orders: "k-123" });
    expect(target(t, "orders").CredentialProviderConfigurations[0].CredentialProviderType).toBe("API_KEY");
    expect(JSON.stringify(t.findResources("AWS::IAM::Policy"))).not.toContain("execute-api:Invoke");
    // The Gateway must be allowed to read the key: without it every call answered
    // "An internal error occurred" (observed live).
    expect(JSON.stringify(t.findResources("AWS::IAM::Policy"))).toContain("bedrock-agentcore:GetResourceApiKey");
    const plain = plane({ ordersApi: { type: "openapi", description: "Orders", schema: DOC } as ToolSpec });
    expect(JSON.stringify(plain.findResources("AWS::IAM::Policy"))).not.toContain("GetResourceApiKey");
  });

  it("sends an inline schema with the target: no bucket, no S3 read", () => {
    const t = plane({ ordersApi: { type: "openapi", description: "Orders", schema: DOC } as ToolSpec });
    expect(JSON.parse(target(t, "ordersApi").TargetConfiguration.Mcp.OpenApiSchema.InlinePayload)).toEqual(DOC);
    expect(Object.keys(t.findResources("AWS::S3::Bucket")).some((k) => k.includes("ToolSchemas"))).toBe(false);
    expect(JSON.stringify(t.findResources("AWS::IAM::Policy"))).not.toContain("ReadOpenApiSchemas");
  });

  it("gets tokens with OAuth client credentials through a provider it creates", () => {
    const t = plane({ ordersApi: { type: "openapi", description: "Orders", schema: DOC, auth: "oauth2",
      oauth: { clientId: "abc", scopes: ["orders.read"], tokenUrl: "https://auth.example.com/oauth2/token" } } as ToolSpec },
    { ordersApi: "s3cr3t" });
    t.hasResourceProperties("AWS::BedrockAgentCore::OAuth2CredentialProvider", {
      CredentialProviderVendor: "CustomOauth2",
      Oauth2ProviderConfigInput: { CustomOauth2ProviderConfig: {
        ClientId: "abc", ClientSecret: "s3cr3t",
        OauthDiscovery: { AuthorizationServerMetadata: {
          Issuer: "https://auth.example.com", TokenEndpoint: "https://auth.example.com/oauth2/token" } } } },
    });
    const c = target(t, "ordersApi").CredentialProviderConfigurations[0];
    expect(c.CredentialProviderType).toBe("OAUTH");
    expect(c.CredentialProvider.OauthCredentialProvider).toMatchObject({ Scopes: ["orders.read"], GrantType: "CLIENT_CREDENTIALS" });
    expect(JSON.stringify(t.findResources("AWS::IAM::Policy"))).toContain("bedrock-agentcore:GetResourceOauth2Token");
    // No client secret, no deploy.
    expect(() => plane({ x: { type: "mcp", endpoint: "https://m.example.com/mcp", auth: "oauth2",
      oauth: { clientId: "a", discoveryUrl: "https://i.example.com/.well-known/openid-configuration" } } as ToolSpec }))
      .toThrow(/no client secret/);
  });

  it("refuses what the Gateway cannot take", () => {
    const v = (t: Record<string, any>) => () => validateTools({ x: t }, {}, "us-east-1");
    expect(v({ ...API, restApiId: "nope" })).toThrow(/restApiId/);
    expect(v({ ...API, toolFilters: [] })).toThrow(/toolFilters/);
    expect(v({ ...API, auth: "oauth2" })).toThrow(/not oauth2/);
    expect(v({ type: "openapi", schema: DOC, schemaS3Uri: "s3://bucket/k.json" })).toThrow(/EXACTLY ONE/);
    expect(v({ type: "mcp", endpoint: "https://m.example.com/mcp", auth: "oauth2", oauth: { clientId: "a" } })).toThrow(/discoveryUrl/);
  });
});

describe("tools that act as the person (auth user / obo)", () => {
  const target = (t: Template, name: string) => Object.values<any>(t.findResources("AWS::BedrockAgentCore::GatewayTarget"))
    .find((r) => r.Properties.Name === name).Properties;
  const DOC = { openapi: "3.0.1", info: { title: "HR", version: "1" },
    paths: { "/me": { get: { operationId: "getMe", responses: { "200": { description: "ok" } } } } } };
  const PERSON = { personAuth: { discoveryUrl: "https://login.example.com/.well-known/openid-configuration", audience: "spa-client" },
    returnUrl: "https://app.example.com" };
  const OA = { clientId: "abc", scopes: ["read"], tokenUrl: "https://idp.example.com/oauth2/token",
    authorizationUrl: "https://idp.example.com/oauth2/authorize" };
  const ORDERS_SCHEMA = [{ name: "listOrders", description: "My orders.",
    properties: { status: { type: "string", required: true, description: "Which orders." } } }];
  const tools = {
    claims: LAMBDA_TOOL,
    hr: { type: "openapi", description: "HR", schema: DOC, auth: "user", oauth: OA } as ToolSpec,
    orders: { type: "mcp", description: "Orders", endpoint: "https://m.example.com/mcp", auth: "obo",
      oauth: { clientId: "abc", tokenUrl: "https://idp.example.com/oauth2/token", audience: "api://orders" },
      toolSchema: ORDERS_SCHEMA } as unknown as ToolSpec,
  };
  const t = plane(tools, { hr: "s1", orders: "s2" }, [], PERSON);
  const gateways = Object.values<any>(t.findResources("AWS::BedrockAgentCore::Gateway"));
  const personGw = gateways.find((g) => String(g.Properties.Name).endsWith("-gwu"));

  it("get a Gateway of their own that trusts the app's sign-in, and can ask to connect", () => {
    expect(gateways.length).toBe(2);
    expect(personGw.Properties.Name).toBe("test-orch-gwu");
    expect(personGw.Properties.ProtocolConfiguration.Mcp.SupportedVersions).toEqual(["2025-11-25"]);
    expect(personGw.Properties.AuthorizerConfiguration.CustomJWTAuthorizer).toEqual({
      DiscoveryUrl: PERSON.personAuth.discoveryUrl, AllowedAudience: ["spa-client"] });
    // No Cedar engine on it: the tool's own provider authorizes each person.
    expect(personGw.Properties.PolicyEngineConfiguration).toBeUndefined();
  });

  it("each person's own account: authorization code, back to the app", () => {
    const c = target(t, "hr").CredentialProviderConfigurations[0].CredentialProvider.OauthCredentialProvider;
    expect(c).toMatchObject({ GrantType: "AUTHORIZATION_CODE", DefaultReturnUrl: "https://app.example.com", Scopes: ["read"] });
    t.hasResourceProperties("AWS::BedrockAgentCore::OAuth2CredentialProvider", {
      Name: "bedrock-agentcore-test-orch-hr",
      Oauth2ProviderConfigInput: { CustomOauth2ProviderConfig: { OauthDiscovery: { AuthorizationServerMetadata: {
        AuthorizationEndpoint: OA.authorizationUrl } } } } });
  });

  it("the person's sign-in: a token exchange of the ID token, for the tool's API", () => {
    const c = target(t, "orders").CredentialProviderConfigurations[0].CredentialProvider.OauthCredentialProvider;
    expect(c.GrantType).toBe("TOKEN_EXCHANGE");
    expect(c.CustomParameters).toEqual({ subject_token_type: "urn:ietf:params:oauth:token-type:id_token", audience: "api://orders" });
    t.hasResourceProperties("AWS::BedrockAgentCore::OAuth2CredentialProvider", {
      Name: "bedrock-agentcore-test-orch-orders",
      Oauth2ProviderConfigInput: { CustomOauth2ProviderConfig: { OnBehalfOfTokenExchangeConfig: {
        GrantType: "TOKEN_EXCHANGE", TokenExchangeGrantTypeConfig: { ActorTokenContent: "NONE" } } } } });
    // An MCP server reached as the person cannot be listed before anyone has signed in.
    expect(JSON.parse(target(t, "orders").TargetConfiguration.Mcp.McpServer.McpToolSchema.InlinePayload)).toEqual([
      { name: "listOrders", description: "My orders.", inputSchema: { type: "object",
        properties: { status: { type: "string", description: "Which orders." } }, required: ["status"] } }]);
  });

  it("sit on the person Gateway, outside the machine Gateway's Cedar permits", () => {
    const personRef = Object.entries<any>(t.findResources("AWS::BedrockAgentCore::Gateway"))
      .find(([, g]) => String(g.Properties.Name).endsWith("-gwu"))![0];
    expect(JSON.stringify(target(t, "hr").GatewayIdentifier)).toContain(personRef);
    expect(JSON.stringify(target(t, "claims").GatewayIdentifier)).not.toContain(personRef);
    const cedar = JSON.stringify(t.findResources("AWS::BedrockAgentCore::Policy"));
    expect(cedar).toContain('AgentCore::Action::\\"claims');
    expect(cedar).not.toContain('AgentCore::Action::\\"hr');
    expect(cedar).not.toContain('AgentCore::Action::\\"orders');
  });

  it("need the app's sign-in, and no person tool means no person Gateway", () => {
    expect(() => plane(tools, { hr: "s1", orders: "s2" })).toThrow(/act as the person/);
    const plain = plane({ claims: LAMBDA_TOOL }, {}, [], PERSON);
    expect(Object.keys(plain.findResources("AWS::BedrockAgentCore::Gateway")).length).toBe(1);
  });
});
describe("custom Cedar policies (orchestrator.policy.custom)", () => {
  const statement = 'forbid(principal, action == AgentCore::Action::"claims___query_claims", '
    + 'resource == AgentCore::Gateway::"{{gateway}}") when { context.input.limit > 100 };';
  const t = plane({ claims: LAMBDA_TOOL }, {}, [{ name: "noBigScans", description: "No scans over 100 rows.", statement }]);
  const policies = t.findResources("AWS::BedrockAgentCore::Policy");
  const custom = Object.entries<any>(policies).filter(([id]) => id.includes("CustomPolicy"));
  it("creates one policy per entry, named custom_<name>_<suffix>, next to the generated permit", () => {
    expect(Object.keys(policies).length).toBe(2);
    expect(custom.length).toBe(1);
    expect(custom[0][1].Properties.Name).toBe(customPolicyName("noBigScans", "test_orch"));
    expect(custom[0][1].Properties.Name).toMatch(/^custom_noBigScans_[0-9a-f]{8}$/);
    expect(custom[0][1].Properties.Description).toBe("No scans over 100 rows.");
    expect(custom[0][1].Properties.ValidationMode).toBe("IGNORE_ALL_FINDINGS");
  });
  it("fills in the Gateway ARN for {{gateway}}", () => {
    const s = JSON.stringify(custom[0][1].Properties.Definition.Cedar.Statement);
    expect(s).not.toContain("{{gateway}}");
    expect(s).toContain("GatewayArn");
    expect(s).toContain("context.input.limit > 100");
  });
  it("is created after every Gateway target", () => {
    const deps: string[] = custom[0][1].DependsOn ?? [];
    const targets = Object.keys(t.findResources("AWS::BedrockAgentCore::GatewayTarget"));
    expect(targets.length).toBeGreaterThan(0);
    for (const id of targets) expect(deps).toContain(id);
  });
});

describe("a tool written in the build (tools.<key>.code)", () => {
  const os = require("os");
  const fs = require("fs");
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "ax-code-"));
  fs.mkdirSync(path.join(root, "app", "tools", "_code", "refunds"), { recursive: true });
  fs.writeFileSync(path.join(root, "app", "tools", "_code", "refunds", "handler.py"),
    "def lambda_handler(event, context):\n    return {}\n");
  const spec: ToolSpec = {
    type: "lambda", description: "Refunds, written in the build.", arg: "question", toolSchema: [QUERY_CLAIMS],
    code: { grants: { secret: "payments/key", table: "orders", tableAccess: "readwrite", s3Prefix: "s3://my-bucket/in/",
      vpc: { subnetIds: ["subnet-0abc1234"], securityGroupIds: ["sg-0abc1234"] } },
      timeoutSeconds: 60, memoryMB: 512, environment: { CURRENCY: "USD" } },
  };
  const app = new cdk.App();
  const stack = new cdk.Stack(app, "C", { env: { account: ACCOUNT, region: "us-east-1" } });
  new ToolPlane(stack, "ToolPlane", {
    agentName: "ax_1a2b3c4d", tools: { refunds: spec }, toolApiKeys: {},
    gatewayDiscoveryUrl: "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc/.well-known/openid-configuration",
    gatewayClientId: "client-abc", gatewayAudience: "gateway/invoke", isCognito: true,
    policyEnabled: true, policyMode: "ENFORCE", orchRoot: root,
  });
  const t = Template.fromStack(stack);
  const fn = Object.values<any>(t.findResources("AWS::Lambda::Function"))
    .find((f) => f.Properties.FunctionName === "ToolLambda-ax_1a2b3c4d-refunds");
  const role = Object.values<any>(t.findResources("AWS::IAM::Role"))
    .find((r) => r.Properties.RoleName === "ToolLambda-ax_1a2b3c4d-refunds");
  const boundary = Object.values<any>(t.findResources("AWS::IAM::ManagedPolicy"))
    .find((p) => p.Properties.ManagedPolicyName === "ToolLambda-ax_1a2b3c4d-refunds-boundary");
  it("deploys the function from app/tools/_code/<key>/ with its settings and where its grants are", () => {
    expect(fn.Properties.Runtime).toBe("python3.12");
    expect(fn.Properties.Handler).toBe("handler.lambda_handler");
    expect(fn.Properties.Timeout).toBe(60);
    expect(fn.Properties.MemorySize).toBe(512);
    expect(fn.Properties.Environment.Variables).toEqual({ CURRENCY: "USD", SECRET_NAME: "payments/key",
      TABLE_NAME: "orders", S3_PREFIX: "s3://my-bucket/in/" });
    expect(fn.Properties.VpcConfig).toEqual({ SubnetIds: ["subnet-0abc1234"], SecurityGroupIds: ["sg-0abc1234"] });
  });
  it("gives it a role of only its grants, capped by a boundary that denies the framework's data", () => {
    expect(role.Properties.PermissionsBoundary).toEqual({ Ref: expect.stringMatching(/^ToolPlaneToolCodeBoundaryrefunds/) });
    const grants = JSON.stringify(role.Properties.Policies);
    expect(grants).toContain("secret:payments/key-??????");
    expect(grants).toContain("table/orders");
    expect(grants).toContain("dynamodb:PutItem");
    expect(grants).toContain("arn:aws:s3:::my-bucket/in/*");
    expect(grants).toContain("ec2:CreateNetworkInterface");
    expect(grants).not.toContain("s3:PutObject");           // read, by default
    const sids = boundary.Properties.PolicyDocument.Statement.map((s: any) => `${s.Sid}:${s.Effect}`);
    expect(sids).toEqual(expect.arrayContaining(["OwnLogs:Allow", "ItsTable:Allow", "NeverFrameworkTables:Deny",
      "NeverFrameworkSecrets:Deny", "NeverFrameworkBuckets:Deny"]));
    expect(JSON.stringify(boundary)).toContain("table/*_builds");
  });
  it("registers it as a Gateway target the Gateway may invoke, with a Cedar permit", () => {
    const target = Object.values<any>(t.findResources("AWS::BedrockAgentCore::GatewayTarget"))[0];
    expect(JSON.stringify(target.Properties.TargetConfiguration)).toContain("ToolLambdarefunds");
    expect(Object.keys(t.findResources("AWS::Lambda::Permission")).length).toBe(1);
    expect(Object.keys(t.findResources("AWS::BedrockAgentCore::Policy")).length).toBe(1);
  });
  it("mirrors Terraform's statements and deny list", () => {
    const tf = fs.readFileSync(path.join(ORCH_ROOT, "terraform", "tools_code.tf"), "utf8");
    for (const sid of ["OwnLogs", "ReadItsSecret", "ItsTable", "ItsPrefix", "ListItsPrefix", "AttachToItsVpc",
      "NeverFrameworkTables", "NeverFrameworkSecrets", "NeverFrameworkBuckets"]) expect(tf).toContain(`"${sid}"`);
    for (const d of CODE_BOUNDARY_DENY) for (const r of d.resources.filter((x: string) => !x.includes("dynamodb"))) expect(tf).toContain(r);
    expect(tf).toContain('name                 = local.code_tool_fn[each.key]');
  });
  it("reaches only what its owner tagged grantable, on both paths", () => {
    const statements = role.Properties.Policies.flatMap((p: any) => p.PolicyDocument.Statement);
    for (const sid of ["ReadItsSecret", "ItsTable", "ItsPrefix", "ListItsPrefix"]) {
      const s = statements.find((x: any) => x.Sid === sid);
      expect(s.Condition.StringEquals).toEqual({ "aws:ResourceTag/agentexpress:code-tools": "true" });
    }
    const tf = fs.readFileSync(path.join(ORCH_ROOT, "terraform", "tools_code.tf"), "utf8");
    expect(tf).toContain('"aws:ResourceTag/agentexpress:code-tools" = "true"');
  });
});

describe("Gateway interceptors (orchestrator.interceptors)", () => {
  const os = require("os");
  const fs = require("fs");
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "ax-icpt-"));
  fs.mkdirSync(path.join(root, "app", "tools", "_code", "interceptor-request"), { recursive: true });
  fs.writeFileSync(path.join(root, "app", "tools", "_code", "interceptor-request", "handler.py"),
    "def lambda_handler(event, context):\n    return {}\n");
  const MINE = `arn:aws:lambda:us-east-1:${ACCOUNT}:function:my-redactor`;
  const synth = (interceptors: any) => {
    const app = new cdk.App();
    const stack = new cdk.Stack(app, "I", { env: { account: ACCOUNT, region: "us-east-1" } });
    new ToolPlane(stack, "ToolPlane", {
      agentName: "ax_1a2b3c4d", tools: { claims: LAMBDA_TOOL }, toolApiKeys: {},
      gatewayDiscoveryUrl: "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc/.well-known/openid-configuration",
      gatewayClientId: "client-abc", gatewayAudience: "gateway/invoke", isCognito: true,
      policyEnabled: true, policyMode: "ENFORCE", orchRoot: root, interceptors,
    });
    return Template.fromStack(stack);
  };
  const t = synth({ request: { code: { timeoutSeconds: 5 }, passRequestHeaders: true, templates: { audit: {} } },
    response: { lambdaArn: MINE } });
  const gateway = Object.values<any>(t.findResources("AWS::BedrockAgentCore::Gateway"))[0].Properties;
  it("configures one interceptor per point, with its headers setting", () => {
    const ics = gateway.InterceptorConfigurations;
    expect(ics.map((c: any) => c.InterceptionPoints)).toEqual([["REQUEST"], ["RESPONSE"]]);
    expect(ics.map((c: any) => c.InputConfiguration.PassRequestHeaders)).toEqual([true, false]);
    expect(JSON.stringify(ics[0].Interceptor.Lambda.Arn)).toContain("ToolLambdainterceptorrequest");
    expect(ics[1].Interceptor.Lambda.Arn).toBe(MINE);
  });
  it("deploys the one written in the build like a code tool, inside the ToolLambda- prefix", () => {
    const fn = Object.values<any>(t.findResources("AWS::Lambda::Function"))
      .find((f) => f.Properties.FunctionName === interceptorFunctionName("ax_1a2b3c4d", "request"));
    expect(fn.Properties.FunctionName).toBe("ToolLambda-ax_1a2b3c4d-interceptor-request");
    expect(fn.Properties.Timeout).toBe(5);
    const boundary = Object.values<any>(t.findResources("AWS::IAM::ManagedPolicy"))
      .find((p) => p.Properties.ManagedPolicyName === "ToolLambda-ax_1a2b3c4d-interceptor-request-boundary");
    expect(JSON.stringify(boundary)).toContain("NeverFrameworkTables");
  });
  it("lets the Gateway's role invoke exactly the interceptors, and permits the service on both", () => {
    const policies = Object.values<any>(t.findResources("AWS::IAM::Policy"));
    const stmt = policies.flatMap((p) => p.Properties.PolicyDocument.Statement).find((s: any) => s.Sid === "InvokeInterceptors");
    expect(stmt.Action).toBe("lambda:InvokeFunction");
    expect(stmt.Resource).toHaveLength(2);
    expect(JSON.stringify(stmt.Resource)).toContain(MINE);
    const perms = Object.values<any>(t.findResources("AWS::Lambda::Permission"))
      .filter((p) => JSON.stringify(p.Properties.FunctionName).match(/interceptor|my-redactor/));
    expect(perms).toHaveLength(2);
    expect(perms.every((p) => p.Properties.Principal === "bedrock-agentcore.amazonaws.com")).toBe(true);
  });
  it("leaves the Gateway as it was without any", () => {
    const plain = Object.values<any>(synth({}).findResources("AWS::BedrockAgentCore::Gateway"))[0].Properties;
    expect(plain.InterceptorConfigurations).toBeUndefined();
  });
  it("is checked at synth: exactly one of code or lambdaArn, and the handler on disk", () => {
    const { interceptorsOf } = require("../lib/orchestrator-stack");
    expect(() => interceptorsOf({ orchestrator: { interceptors: { request: {} } } })).toThrow(/EXACTLY ONE/);
    expect(() => interceptorsOf({ orchestrator: { interceptors: { request: { lambdaArn: "x" } } } })).toThrow(/not a Lambda/);
    expect(() => interceptorsOf({ orchestrator: { interceptors: { response: { code: {} } } } }, root)).toThrow(/handler.py/);
    expect(Object.keys(interceptorsOf({ orchestrator: { interceptors: { request: { code: {} } } } }, root))).toEqual(["request"]);
  });
  it("mirrors Terraform", () => {
    const tf = fs.readFileSync(path.join(ORCH_ROOT, "terraform", "interceptors.tf"), "utf8");
    expect(tf).toContain('"InvokeInterceptors"');
    const gw = fs.readFileSync(path.join(ORCH_ROOT, "terraform", "gateway.tf"), "utf8");
    expect(gw).toContain('dynamic "interceptor_configuration"');
    expect(gw).toContain("pass_request_headers");
    expect(fs.readFileSync(path.join(ORCH_ROOT, "terraform", "tools_code.tf"), "utf8")).toContain("interceptor-");
  });
});

// ===========================================================================
// Every tool type × the policy engine on or off × its generated permit on or off × a
// custom policy or none × ENFORCE or LOG_ONLY. Each combination synthesizes, registers
// exactly its target, and creates exactly the Cedar policies that combination asks for.
// ===========================================================================
describe("the tool and policy matrix", () => {
  const fs = require("fs");
  const codeDir = path.join(ORCH_ROOT, "app", "tools", "_code", "codeTool");
  const madeCode = !fs.existsSync(codeDir);
  beforeAll(() => {
    fs.mkdirSync(codeDir, { recursive: true });
    fs.writeFileSync(path.join(codeDir, "handler.py"), "def lambda_handler(event, context):\n    return {}\n");
  });
  afterAll(() => {
    if (!madeCode) return;
    fs.rmSync(codeDir, { recursive: true, force: true });
    const parent = path.dirname(codeDir);
    if (fs.existsSync(parent) && !fs.readdirSync(parent).length) fs.rmdirSync(parent);
  });
  const SCHEMA = [{ name: "doIt", description: "Do it.", properties: { query: { type: "string", required: true, description: "q" } } }];
  const DOC = { openapi: "3.0.1", info: { title: "T", version: "1" },
    paths: { "/x": { get: { operationId: "getX", responses: { 200: { description: "ok" } } } } } };
  const TYPES: Record<string, ToolSpec> = {
    kbTool: { type: "kb", description: "Corpus", corpora: ["reference"], policy: { tool: "retrieve", restrictTo: { filter: ["reference"] } } } as ToolSpec,
    webTool: { type: "websearch", description: "Web" } as ToolSpec,
    mcpTool: { type: "mcp", description: "MCP", endpoint: "https://mcp.example.com/mcp" } as ToolSpec,
    apiTool: { type: "openapi", description: "API", schema: DOC, call: "getX", arg: "query" } as ToolSpec,
    restTool: { type: "apigateway", description: "REST", restApiId: "a1b2c3d4e5", stage: "prod",
      toolFilters: [{ path: "/x", methods: ["GET"] }], auth: "sigv4" } as ToolSpec,
    arnTool: { ...LAMBDA_TOOL, description: "ARN" },
    pricingTool: { type: "lambda", description: "Pricing", source: "pricing", toolSchema: SCHEMA } as ToolSpec,
    codeTool: { type: "lambda", description: "Code", toolSchema: SCHEMA, code: { grants: { table: "orders" } } } as ToolSpec,
  };
  const combos: [string, boolean, boolean, boolean, string][] = [];
  for (const key of Object.keys(TYPES)) {
    combos.push([key, true, true, true, "ENFORCE"], [key, true, false, true, "LOG_ONLY"],
      [key, true, true, false, "ENFORCE"], [key, false, true, true, "ENFORCE"]);
  }
  it.each(combos)("%s: engine %s, permit %s, custom %s, %s", (key, engine, permit, custom, mode) => {
    const spec = { ...TYPES[key], ...(permit ? {} : { policy: { ...(TYPES[key].policy ?? {}), permit: false } }) } as ToolSpec;
    const app = new cdk.App();
    const stack = new cdk.Stack(app, "M", { env: { account: ACCOUNT, region: "us-east-1" } });
    new ToolPlane(stack, "ToolPlane", {
      agentName: "ax_1a2b3c4d", tools: { [key]: spec }, toolApiKeys: {},
      gatewayDiscoveryUrl: "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc/.well-known/openid-configuration",
      gatewayClientId: "c", gatewayAudience: "gateway/invoke", isCognito: true,
      policyEnabled: engine, policyMode: mode, orchRoot: ORCH_ROOT,
      customPolicies: engine && custom ? [{ name: "blockSecrets", statement:
        `forbid(principal, action == AgentCore::Action::"${key}___run", resource == AgentCore::Gateway::"{{gateway}}") when { context.input has secret };` }] : [],
    });
    const t = Template.fromStack(stack);
    const targets = Object.values<any>(t.findResources("AWS::BedrockAgentCore::GatewayTarget"));
    expect(targets.map((x) => x.Properties.Name)).toEqual([key]);
    const gateway = Object.values<any>(t.findResources("AWS::BedrockAgentCore::Gateway"))[0].Properties;
    const policies = Object.entries<any>(t.findResources("AWS::BedrockAgentCore::Policy"));
    if (!engine) {
      t.resourceCountIs("AWS::BedrockAgentCore::PolicyEngine", 0);
      expect(gateway.PolicyEngineConfiguration).toBeUndefined();
      expect(policies.length).toBe(0);
      return;
    }
    t.resourceCountIs("AWS::BedrockAgentCore::PolicyEngine", 1);
    expect(gateway.PolicyEngineConfiguration.Mode).toBe(mode);
    expect(policies.filter(([id]) => id.includes("CustomPolicy")).length).toBe(custom ? 1 : 0);
    expect(policies.filter(([id]) => !id.includes("CustomPolicy")).length).toBe(permit ? 1 : 0);
    for (const [, p] of policies) {
      const s = JSON.stringify(p.Properties.Definition.Cedar.Statement);
      expect(s).not.toContain("{{gateway}}");
      expect(s).toContain("GatewayArn");
      // Every policy waits for the target it names.
      expect(p.DependsOn ?? []).toEqual(expect.arrayContaining(
        Object.keys(t.findResources("AWS::BedrockAgentCore::GatewayTarget"))));
    }
  });
});
