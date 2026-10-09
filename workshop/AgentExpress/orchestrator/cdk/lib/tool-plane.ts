import * as crypto from "crypto";
import * as fs from "fs";
import * as path from "path";
import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as vocab from "./vocabulary";
import { keyDefault, keyDefaultFor } from "./defaults";
import {
  aws_dynamodb as dynamodb,
  aws_iam as iam,
  aws_lambda as lambda,
  aws_logs as logs,
  aws_s3 as s3,
  aws_s3_deployment as s3deploy,
  aws_s3vectors as s3vectors,
  aws_bedrock as bedrock,
  aws_bedrockagentcore as agentcore,
  custom_resources as cr,
} from "aws-cdk-lib";

/** The tool types the framework knows how to provision. */

/**
 * A tool's Cedar policy name: `permit_<key>_<8 hex of the agent name>`.
 *
 * Policy names are unique in the ACCOUNT, not in their policy engine, so a bare
 * `permit_<key>` made the second deployment in an account with the same tool key fail
 * ("Policy with the same name already exists") — a Builder build next to its console,
 * or two builds. The suffix keeps it within the 48-character limit whatever the agent
 * name, and the key still leads, so a denial in CloudWatch names its tool. Mirrors
 * local.cedar_policy_suffix in terraform/policy.tf.
 */
export function cedarPolicyName(key: string, agentName: string): string {
  return `permit_${key}_${crypto.createHash("sha1").update(agentName).digest("hex").slice(0, 8)}`;
}

/**
 * A custom policy's name: `custom_<name>_<8 hex of the agent name>` — account-unique
 * like cedarPolicyName, and within 48 characters for a name of up to 32. Mirrors
 * terraform/policy.tf aws_bedrockagentcore_policy.custom.
 */
export function customPolicyName(name: string, agentName: string): string {
  return `custom_${name}_${crypto.createHash("sha1").update(agentName).digest("hex").slice(0, 8)}`;
}
/** Where a custom policy names the build's Gateway; replaced by its ARN at deploy. */
export const GATEWAY_PLACEHOLDER = "{{gateway}}";
/** One workflow.json orchestrator.policy.custom entry (checked by customPoliciesOf). */
export interface CustomPolicy {
  name: string;
  description?: string;
  statement: string;
}
/** tools.<key>.code (validated by bff/validate_build.py _check_code before a deploy). */
export interface CodeSpec {
  grants?: {
    secret?: string;
    table?: string;
    tableAccess?: "read" | "readwrite";
    s3Prefix?: string;
    s3Access?: "read" | "readwrite";
    vpc?: { subnetIds: string[]; securityGroupIds: string[] };
  };
  timeoutSeconds?: number;
  memoryMB?: number;
  environment?: Record<string, string>;
}

/** Where the Gateway calls an interceptor: before a request reaches a tool, or after
 *  the tool answers. */
export type InterceptionPoint = "request" | "response";
export const INTERCEPTION_POINTS: InterceptionPoint[] = ["request", "response"];
/** orchestrator.interceptors.<point> (validated by bff/validate_build.py
 *  _check_interceptors before a deploy, and by interceptorsOf at synth). */
export interface InterceptorSpec {
  /** A function written in the build: app/tools/_code/interceptor-<point>/. */
  code?: CodeSpec;
  /** A function you own. */
  lambdaArn?: string;
  /** Hand the interceptor the request headers (default false): the caller's token,
   *  and the x-ax-session / x-ax-agent / x-ax-user the runtime sends. */
  passRequestHeaders?: boolean;
  /** What the Builder generated the code from. Read only by the Builder. */
  templates?: Record<string, unknown>;
}
/** The function name of an interceptor written in the build. The ToolLambda- prefix
 *  keeps it inside the deploy role's scoped grants, like a code tool. Mirrors
 *  local.code_functions in terraform/tools_code.tf. */
export function interceptorFunctionName(agentName: string, point: InterceptionPoint): string {
  return `ToolLambda-${agentName}-interceptor-${point}`;
}

/** What no code tool may reach, in any account: the framework's own tables, secrets and
 *  buckets. An explicit Deny in every code tool's permissions boundary, so a grant can
 *  never widen into them. Mirrors local.code_boundary_deny in terraform/tools.tf and the
 *  FRAMEWORK_*_RE checks in bff/validate_build.py. */
export const CODE_BOUNDARY_DENY = [
  {
    sid: "NeverFrameworkTables", actions: ["dynamodb:*"],
    resources: ["ax_*", "*_builds", "*_audit", "*_status", "*_events", "*_telemetry", "*_insights"]
      .flatMap((t) => [`arn:aws:dynamodb:*:*:table/${t}`, `arn:aws:dynamodb:*:*:table/${t}/*`]),
  },
  {
    sid: "NeverFrameworkSecrets", actions: ["secretsmanager:*"],
    resources: ["arn:aws:secretsmanager:*:*:secret:agentexpress/*", "arn:aws:secretsmanager:*:*:secret:bedrock-agentcore*"],
  },
  {
    sid: "NeverFrameworkBuckets", actions: ["s3:*"],
    resources: ["arn:aws:s3:::agentcore-*", "arn:aws:s3:::*builderbuildsbucket*"],
  },
];

/** An allowlist, not just the deny above: a code tool reaches a secret, table or bucket
 *  only if its owner tagged it agentexpress:code-tools=true (a bucket also needs ABAC
 *  turned on: aws s3api put-bucket-abac). Anything else in the account stays out of reach
 *  whatever a build names. Mirrors local.code_grantable in terraform/tools_code.tf. */
export const CODE_GRANTABLE_TAG = "agentexpress:code-tools";
const CODE_GRANTABLE = { StringEquals: { [`aws:ResourceTag/${CODE_GRANTABLE_TAG}`]: "true" } };

/** The grants of a code tool, as IAM statements: logs on its own log group, then only
 *  what `grants` names. The same list is its inline policy AND its permissions boundary,
 *  so nothing attached to the role later can reach further. Mirrors
 *  local.code_tool_statements in terraform/tools.tf. */
export function codeToolStatements(fnName: string, code: CodeSpec, region: string, account: string): any[] {
  const g = code.grants ?? {};
  const out: any[] = [{
    sid: "OwnLogs", actions: ["logs:CreateLogStream", "logs:PutLogEvents"],
    resources: [`arn:aws:logs:${region}:${account}:log-group:/aws/lambda/${fnName}:*`],
  }];
  if (g.secret) {
    out.push({ sid: "ReadItsSecret", actions: ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"],
      resources: [`arn:aws:secretsmanager:${region}:${account}:secret:${g.secret}-??????`],
      conditions: CODE_GRANTABLE });
  }
  if (g.table) {
    const t = `arn:aws:dynamodb:${region}:${account}:table/${g.table}`;
    const read = ["dynamodb:GetItem", "dynamodb:BatchGetItem", "dynamodb:Query", "dynamodb:Scan", "dynamodb:DescribeTable"];
    const write = ["dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem", "dynamodb:BatchWriteItem",
      "dynamodb:ConditionCheckItem"];
    out.push({ sid: "ItsTable", actions: g.tableAccess === "readwrite" ? [...read, ...write] : read,
      resources: [t, `${t}/index/*`], conditions: CODE_GRANTABLE });
  }
  if (g.s3Prefix) {
    const [bucket, ...rest] = g.s3Prefix.replace(/^s3:\/\//, "").split("/");
    const prefix = rest.join("/");
    out.push({ sid: "ItsPrefix", actions: g.s3Access === "readwrite" ? ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"] : ["s3:GetObject"],
      resources: [`arn:aws:s3:::${bucket}/${prefix}*`], conditions: CODE_GRANTABLE });
    out.push({ sid: "ListItsPrefix", actions: ["s3:ListBucket"], resources: [`arn:aws:s3:::${bucket}`],
      conditions: { ...CODE_GRANTABLE, StringLike: { "s3:prefix": [`${prefix}*`] } } });
  }
  if (g.vpc) {
    // What Lambda needs to attach the function to your subnets (AWSLambdaVPCAccessExecutionRole).
    out.push({ sid: "AttachToItsVpc", actions: ["ec2:CreateNetworkInterface", "ec2:DescribeNetworkInterfaces",
      "ec2:DescribeSubnets", "ec2:DeleteNetworkInterface", "ec2:AssignPrivateIpAddresses", "ec2:UnassignPrivateIpAddresses"],
      resources: ["*"] });
  }
  return out;
}

/** The plain environment of a code tool, plus where its grants are. */
export function codeToolEnvironment(code: CodeSpec): Record<string, string> {
  const g = code.grants ?? {};
  return {
    ...(code.environment ?? {}),
    ...(g.secret ? { SECRET_NAME: g.secret } : {}),
    ...(g.table ? { TABLE_NAME: g.table } : {}),
    ...(g.s3Prefix ? { S3_PREFIX: g.s3Prefix } : {}),
  };
}

export type ToolType = "kb" | "websearch" | "mcp" | "openapi" | "lambda" | "apigateway";

/** One tool published by a `type: "lambda"` target. */
export interface LambdaToolDef {
  name: string;
  description?: string;
  /** Argument name -> its schema. `required` is per-property, JSON-Schema style. */
  properties: Record<
    string,
    { type?: string; required?: boolean; description?: string }
  >;
}

/**
 * One entry from the `tools` block in app/workflow.json — the single place a
 * customer declares a data source. The KEY is the Gateway target name AND the
 * label an agent references via its `tool` field.
 */
/** s3://bucket or s3://bucket/prefix/ -> its parts (the prefix "" or ending in "/"). */
export function parseS3Location(uri: string): { bucket: string; prefix: string } {
  const m = /^s3:\/\/([a-z0-9][a-z0-9.-]{1,61}[a-z0-9])(?:\/(.*))?$/.exec(uri);
  if (!m) throw new Error(`not an s3:// location: ${JSON.stringify(uri)}`);
  const rest = m[2] ?? "";
  return { bucket: m[1], prefix: rest && !rest.endsWith("/") ? `${rest}/` : rest };
}

export interface ToolSpec {
  type: ToolType;
  description?: string;
  /** type=kb: top-level folders under kb_docs/, each a filterable doc_type. */
  corpora?: string[];
  /** type=kb: build the KB from YOUR documents in S3 (s3://bucket[/prefix/]). */
  s3Uri?: string;
  /** type=kb with s3Uri: the customer managed KMS key that bucket is encrypted with. */
  kmsKeyArn?: string;
  /** type=kb: use a Knowledge Base you already have, in this account and region. */
  knowledgeBaseId?: string;
  /** type=openapi: the OpenAPI 3 document itself, sent to the Gateway inline. */
  schema?: Record<string, unknown>;
  /** type=apigateway: an API Gateway REST API in this account and region. */
  restApiId?: string;
  stage?: string;
  toolFilters?: { path: string; methods: string[] }[];
  toolOverrides?: { path: string; method: string; name: string; description?: string }[];
  /** auth=oauth2 | user | obo: the OAuth client (its secret is in toolApiKeys). `authorizationUrl`
   *  for "user" with a tokenUrl; `audience` for "obo": the API the exchanged token is for. */
  oauth?: { clientId: string; scopes?: string[]; discoveryUrl?: string; tokenUrl?: string; issuer?: string;
    authorizationUrl?: string; audience?: string };
  /**
   * Which field of this target's result ROWS carries which role, for an agent that
   * consumes the tool's data rather than its prose rendering (ctx.call_tool_rows).
   *
   * Optional, and only a DETERMINISTIC agent needs it — one whose output is a
   * transformation of the rows (counting them, quoting their fields) rather than a
   * model's reading of them. It is what keeps such an agent config-driven: repoint
   * the tool at another Lambda, warehouse or API and set these to its field names,
   * with no agent code change. Without it the agent would have to know one target's
   * field names and swapping the target would need an edit.
   *
   * Keys are the roles the agent asks for; values are this target's field names:
   *   { "id": "sessionId", "label": "topic", "outcome": "overall", "timestamp": "created" }
   */
  rowFields?: Record<string, string>;
  /**
   * Dot path to the LIST of records in this target's response, e.g. "result.releases".
   * Needed only when the list is somewhere the framework does not already probe for
   * (results, result, items, documents, content.result, content.results) — which is
   * common for a REST API you did not design. Without it an agent that computes sees
   * zero rows and reports "nothing found" for a response that was full of data.
   */
  rowPath?: string;
  /**
   * type=kb: which Bedrock model embeds the corpus. Defaults to the first entry in
   * `embeddingModels` (vocabulary.json). Changing it REPLACES the index and the KB,
   * which the digest-derived names make work rather than fail.
   */
  embeddingModel?: string;
  /** type=kb: embedding vector size. Defaults to the chosen model's first dimension. */
  dimensions?: number;
  /** type=kb: metadata attribute an agent's `corpus` is matched against ("doc_type"). */
  corpusKey?: string;
  /** type=kb: how `corpus` is compared. Single-value operators only — see vocabulary.json. */
  corpusOperator?: string;
  /**
   * type=kb: a Bedrock-shaped retrieval filter applied to EVERY call, set on the target
   * and invisible to the agent. ANDed with the agent's corpus filter, so it can only
   * narrow. This is where multi-condition narrowing goes; the agent's own filter stays a
   * scalar so the generated Cedar permit can still enforce it.
   */
  filter?: Record<string, any>;
  /** type=kb: rerank retrieved chunks with a second model. `{ model, count }`. */
  rerank?: { model?: string; count?: number };
  /** type=mcp: the MCP server's Streamable HTTP URL. Change this, nothing else. */
  endpoint?: string;
  /** type=openapi: s3:// URI of the OpenAPI schema. */
  schemaS3Uri?: string;
  /**
   * type=lambda: the ARN of an EXISTING function. The framework registers it; it
   * does not create or deploy it. This is the general-purpose escape hatch for
   * anything the Gateway cannot reach directly — a database (Redshift, Snowflake,
   * any RDBMS), an internal service, a resource inside a VPC.
   */
  lambdaArn?: string;
  /**
   * type=lambda alternative to `lambdaArn`: name a function the FRAMEWORK ships and
   * deploys, so the committed config stays account-neutral (a real ARN would pin
   * workflow.json to one AWS account).
   *
   * The value is a FOLDER NAME under orchestrator/app/tools/ — `source: "pricing"`
   * means app/tools/pricing/ — and the validator accepts only the one the repo
   * ships, because the execution role is fixed and cannot be configured. This is
   * deliberately NOT a general "deploy any directory" feature: a framework-deployed
   * function needs an execution role that config cannot express. A function of your
   * own goes in via `lambdaArn`, and the framework then touches neither its code nor
   * its role.
   */
  source?: string;
  /**
   * type=lambda: a function WRITTEN IN THE BUILD (its files are app/tools/_code/<name>/,
   * written there by scaffold.py apply). Deployed with a role of logs plus only these
   * grants, capped by a permissions boundary of the same. See codeToolStatements.
   */
  code?: CodeSpec;
  /**
   * type=lambda: the tools this function publishes. DECLARED, not discovered —
   * unlike an MCP server there is no tools/list for the Gateway to call. One
   * function may publish several; the Gateway passes the tool name through so the
   * handler can dispatch on it (set `call` to say which one an agent invokes).
   */
  toolSchema?: LambdaToolDef[];
  /**
   * type=websearch: which domains web search may return. Applied on the Gateway
   * TARGET, so it is hidden from the calling agent and enforced on every request —
   * a prompt-injected instruction cannot widen it.
   *
   * ONE key, replacing four. There used to be a request-level
   * includeDomains/excludeDomains pair beside a target-level
   * targetIncludeDomains/targetExcludeDomains pair; they expressed one intent, and
   * the pair with the obvious name was the weaker one (caller-supplied, so scoping
   * rather than a boundary, and needing connector v1.2.0+). Both came from the same
   * config file, so setting both only produced their intersection.
   */
  domains?: { include?: string[]; exclude?: string[] };
  /** type=websearch: pin the connector version, e.g. "1.2.0". */
  connectorVersion?: string;
  /** type=websearch: request-level published-date bounds (ISO-8601 UTC). */
  publishedFrom?: string;
  publishedTo?: string;
  /** type=websearch: results per call (1-25). */
  maxResults?: number;
  /**
   * Which tool on the target to invoke. A target may publish several (the AWS
   * Documentation MCP server publishes five), in which case this is required.
   */
  call?: string;
  /** The parameter the query goes into. Default "query". */
  arg?: string;
  /** Fixed extra arguments sent on every call to this tool. */
  args?: Record<string, unknown>;
  /**
   * How the Gateway discovers the server's tools.
   *   DEFAULT — synchronise + cache the catalog at create/update time.
   *   DYNAMIC — forward tools/list to the server at invocation time instead.
   * DYNAMIC avoids a stale catalogue when a server's tool set changes. When
   * verifying with tools/list, follow nextCursor — the response is PAGINATED, and a
   * target whose tools sit on page two otherwise looks empty.
   */
  listingMode?: "DEFAULT" | "DYNAMIC";
  /**
   * OUTBOUND auth from the Gateway to the endpoint.
   *   "apikey" — vaulted key sent as X-API-Key (supply it via $TOOL_API_KEYS)
   *   "sigv4"  — the Gateway signs with its own execution role, so there is no
   *              secret. Only works behind a service that verifies SigV4:
   *              AgentCore Runtime/Gateway, API Gateway, Lambda Function URLs.
   */
  auth?: "none" | "apikey" | "sigv4" | "oauth2" | "user" | "obo";
  /** SigV4 signing name; auto-detected from the hostname when omitted. */
  service?: string;
  /** Cedar shape. Omit for a target-level permit (all the target's tools). */
  policy?: {
    /** Narrow the permit to ONE tool name within this target. */
    tool?: string;
    /** Argument allow-lists, e.g. { filter: ["reference"] }. */
    restrictTo?: Record<string, string[]>;
    /** false = register the target but do NOT permit it (default-deny demo). */
    permit?: boolean;
  };
}

export interface ToolPlaneProps {
  agentName: string;
  /** orchestrator.interceptors, as interceptorsOf returned it. */
  interceptors?: Partial<Record<InterceptionPoint, InterceptorSpec>>;
  /** The parsed `tools` block from workflow.json. */
  tools: Record<string, ToolSpec>;
  /** API keys by tool name, from $TOOL_API_KEYS. Never from workflow.json. */
  toolApiKeys: Record<string, string>;
  /** OIDC discovery URL of the configured IdP. */
  gatewayDiscoveryUrl: string;
  gatewayClientId: string;
  /** Each agent's own client, with orchestrator.gatewayIdentity "perAgent". */
  agentClientIds?: string[];
  gatewayAudience: string;
  /** Cognito pins the caller by `client_id` (allowedClients); every other idp by the
   *  token's `aud` plus the claim naming the client (clientClaim). */
  isCognito: boolean;
  /** The claim carrying the calling client on a non-Cognito token: "azp" (Auth0,
   *  Entra, the default) or "cid" (Okta). */
  clientClaim?: string;
  /** For tools that act as the PERSON (auth "user" / "obo"): the sign-in provider's
   *  discovery URL and the app's client id, which the person Gateway trusts. */
  personAuth?: { discoveryUrl: string; audience: string };
  /** Where a person is sent back after connecting their account (the app's address). */
  returnUrl?: string;
  /** Cedar policy engine on/off (workflow.json orchestrator.policy.enabled). */
  policyEnabled: boolean;
  /** "ENFORCE" | "LOG_ONLY". */
  policyMode: string;
  /** workflow.json orchestrator.policy.custom: the user's own Cedar, next to the permits. */
  customPolicies?: CustomPolicy[];
  /** orchestrator/ root, for the kb_docs corpus and the kb_lambda / app/tools sources. */
  orchRoot: string;
  /**
   * This deployment's own run-data tables. Needed only by the built-in
   * `source: "pricing"` demo function, which reports on run history and is
   * granted READ-ONLY access to them. Omit when no tool asks for it.
   */
  statusTable?: dynamodb.ITable;
  telemetryTable?: dynamodb.ITable;
}

/**
 * The Gateway-backed TOOL PLANE — the CDK counterpart to terraform/tools.tf +
 * gateway.tf + kb.tf + policy.tf.
 *
 * Everything here is GENERATED from the `tools` block in app/workflow.json:
 * one Gateway target per entry, a vaulted credential provider for any entry with
 * an API key, and the Cedar permits that authorize exactly those tools. A
 * customer adds a data source by adding a JSON block — no TypeScript edits.
 */
export class ToolPlane extends Construct {
  /** The person Gateway's MCP endpoint (GATEWAY_USER_URL), or "" when no tool acts as the person. */
  public readonly personGatewayUrl: string;
  /** tool name -> the redirect (callback) URL to register at its provider, for each
   *  tool where each person connects their own account (auth "user"). */
  public readonly callbackUrls: Record<string, string> = {};
  /** MCP endpoint the runtimes call (GATEWAY_URL). */
  public readonly gatewayUrl: string;
  public readonly gatewayId: string;
  /** tool name -> ARN, for each function the framework deployed from `source`. */
  private readonly builtinLambdaArns: Record<string, string> = {};
  /** tool name -> s3:// URI, for each openapi schema the framework uploaded from `source`. */
  private readonly uploadedSchemaUris: Record<string, string> = {};
  public readonly gatewayArn: string;
  /** Policy mode to report in the UI, "" when policy is off. */
  public readonly policyModeEnv: string;
  /** Empty when no tool declares type=kb. */
  public readonly knowledgeBaseId: string;

  constructor(scope: Construct, id: string, props: ToolPlaneProps) {
    super(scope, id);

    const stack = cdk.Stack.of(this);
    const { region, account } = stack;
    const { agentName, orchRoot, tools } = props;
    const dashName = agentName.replace(/_/g, "-");


    // ---- Split the declared tools by type ---------------------------------
    const entries = Object.entries(tools);
    const byType = (t: ToolType) => entries.filter(([, v]) => v.type === t);
    const kbEntry = byType("kb")[0];
    const kbName = kbEntry?.[0] ?? "";

    // ---- Embedding model + dimension, from config -------------------------
    // Both were hardcoded here and in terraform/kb.tf. They are a PAIR: a model supports
    // only certain dimensions, and a mismatch is not rejected at deploy — it fails at
    // Bedrock INGESTION afterwards, while the deploy reports success and the corpus is
    // silently empty. So the model is closed to a known set (vocabulary.json) and the
    // dimension defaults from it, which means declaring `embeddingModel` alone is enough
    // and declaring neither keeps the previous behaviour exactly.
    // Mirrors local.kb_embed_model_id / local.kb_dims in terraform/kb.tf.
    const kbSpec: ToolSpec = (kbEntry?.[1] ?? {}) as ToolSpec;
    const embedModelId = kbSpec.embeddingModel ?? vocab.EMBEDDING_MODELS[0];
    const allowedDims = vocab.embeddingDimensions(embedModelId);
    const KB_DIMS = kbSpec.dimensions ?? allowedDims[0];
    if (kbName && !allowedDims.includes(KB_DIMS)) {
      throw new Error(
        `workflow.json tools.${kbName} has "dimensions": ${KB_DIMS}, which ` +
          `"embeddingModel": ${JSON.stringify(embedModelId)} does not support — it accepts ` +
          `${JSON.stringify(allowedDims)} (first is the default, so omitting "dimensions" ` +
          `is usually right). A mismatch is NOT rejected at deploy time: Bedrock fails at ` +
          `ingestion afterwards while the deploy reports success, leaving an empty corpus.`
      );
    }
    const embedModel = `arn:aws:bedrock:${region}::foundation-model/${embedModelId}`;

    // ======================================================================
    // Gateway service role
    // ======================================================================
    const gatewayRole = new iam.Role(this, "GatewayRole", {
      roleName: `AgentCoreGateway-${agentName}`,
      assumedBy: new iam.ServicePrincipal("bedrock-agentcore.amazonaws.com", {
        conditions: {
          StringEquals: { "aws:SourceAccount": account },
          ArnLike: { "aws:SourceArn": `arn:aws:bedrock-agentcore:${region}:${account}:*` },
        },
      }),
    });
    gatewayRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
        resources: [`arn:aws:logs:${region}:${account}:*`],
      })
    );
    // Needed when a target uses an API-key / OAuth credential provider.
    gatewayRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["secretsmanager:GetSecretValue"],
        resources: [`arn:aws:secretsmanager:${region}:${account}:secret:bedrock-agentcore*`],
      })
    );
    // OAuth client credentials (auth: oauth2): the Gateway asks AgentCore Identity for a
    // token as its own workload identity. Granted only when a tool uses it. Both ARN
    // forms of the token vault are listed: the provider ARN comes back in the acps
    // namespace while the documented policy uses bedrock-agentcore. Mirrors
    // GatewayOAuthTokens in terraform/gateway.tf.
    // An API-key provider (auth: apikey, or an apikey identity) is read the same way.
    // Without this the Gateway answers every call to such a tool with "An internal error
    // occurred" (observed live; nothing else in the stack names the cause). Mirrors
    // GatewayApiKeys in terraform/gateway.tf.
    if (Object.values(props.tools).some((t) => t.auth === "apikey")) {
      gatewayRole.addToPolicy(
        new iam.PolicyStatement({
          sid: "GatewayApiKeys",
          actions: ["bedrock-agentcore:GetResourceApiKey", "bedrock-agentcore:GetWorkloadAccessToken"],
          resources: [
            `arn:aws:bedrock-agentcore:${region}:${account}:token-vault/*`,
            `arn:aws:acps:${region}:${account}:token-vault/*`,
            `arn:aws:bedrock-agentcore:${region}:${account}:workload-identity-directory/*`,
          ],
        })
      );
    }
    if (Object.values(props.tools).some((t) => t.auth === "oauth2")) {
      gatewayRole.addToPolicy(
        new iam.PolicyStatement({
          sid: "GatewayOAuthTokens",
          actions: ["bedrock-agentcore:GetResourceOauth2Token", "bedrock-agentcore:GetWorkloadAccessToken"],
          resources: [
            `arn:aws:bedrock-agentcore:${region}:${account}:token-vault/*`,
            `arn:aws:acps:${region}:${account}:token-vault/*`,
            `arn:aws:bedrock-agentcore:${region}:${account}:workload-identity-directory/*`,
          ],
        })
      );
    }
    // A SigV4 API Gateway target: invoke exactly that API and stage, nothing else.
    // Mirrors GatewayInvokeRestApis in terraform/gateway.tf.
    const signedApis = Object.values(props.tools)
      .filter((t) => t.type === "apigateway" && t.auth === "sigv4" && t.restApiId && t.stage)
      .map((t) => `arn:aws:execute-api:${region}:${account}:${t.restApiId}/${t.stage}/*/*`)
      .sort();
    if (signedApis.length) {
      gatewayRole.addToPolicy(
        new iam.PolicyStatement({ sid: "GatewayInvokeRestApis", actions: ["execute-api:Invoke"], resources: signedApis })
      );
    }
    // Read + evaluate the attached Cedar engine on every tool call.
    // AuthorizeAction is checked against BOTH the engine and the gateway.
    gatewayRole.addToPolicy(
      new iam.PolicyStatement({
        sid: "PolicyEngineEvaluate",
        actions: [
          "bedrock-agentcore:GetPolicyEngine",
          "bedrock-agentcore:ListPolicies",
          "bedrock-agentcore:GetPolicy",
          "bedrock-agentcore:*Authorize*",
        ],
        resources: [
          `arn:aws:bedrock-agentcore:${region}:${account}:policy-engine/*`,
          `arn:aws:bedrock-agentcore:${region}:${account}:gateway/*`,
        ],
      })
    );

    // OUTBOUND permission for the managed web-search connector. The connector runs
    // inside AWS and the Gateway reaches it as itself, so without this a call fails
    // at INVOKE time (not at deploy time) with
    //   -32002 "Execution role is not authorized for connector web-search"
    // Granted only when a tools entry actually declares type=websearch.
    if (Object.values(props.tools).some((t) => t.type === "websearch")) {
      gatewayRole.addToPolicy(
        new iam.PolicyStatement({
          // Scoped to the AWS-OWNED tool ARN exactly as documented (note the
          // literal "aws" where the account id would normally be — authorization
          // is enforced per invocation against that ARN). This was "*" before,
          // which worked but granted more than the docs call for.
          sid: "InvokeWebSearch",
          actions: ["bedrock-agentcore:InvokeWebSearch"],
          resources: [`arn:aws:bedrock-agentcore:${region}:aws:tool/web-search.v1`],
        })
      );
      // The documented Web Search service-role policy pairs InvokeWebSearch with
      // InvokeGateway on the gateway.
      //
      // Scoped to gateway/* in this account and region rather than the exact ARN.
      // Using the concrete ARN creates a CloudFormation CIRCULAR DEPENDENCY: the
      // Gateway must wait for this role's policy to exist (dependOnDefaultPolicy
      // below), so the policy cannot in turn reference the Gateway. The existing
      // PolicyEngineEvaluate statement is scoped the same way for the same reason.
      gatewayRole.addToPolicy(
        new iam.PolicyStatement({
          sid: "InvokeGateway",
          actions: ["bedrock-agentcore:InvokeGateway"],
          resources: [`arn:aws:bedrock-agentcore:${region}:${account}:gateway/*`],
        })
      );
    }

    // ======================================================================
    // Cedar policy engine (before the Gateway, so it can be attached)
    // ======================================================================
    let policyEngine: agentcore.CfnPolicyEngine | undefined;
    if (props.policyEnabled) {
      policyEngine = new agentcore.CfnPolicyEngine(this, "PolicyEngine", {
        name: `${agentName}_policy`,
        description: "Cedar policy engine governing AgentCore Gateway tool access",
      });
    }
    this.policyModeEnv = props.policyEnabled ? props.policyMode : "";

    // ======================================================================
    // Interceptors (orchestrator.interceptors): a Lambda the Gateway calls before each
    // request and/or after each response. One written in the build is deployed like a
    // code tool; one you own is only registered. The Gateway invokes it with its own
    // role, so the role may invoke exactly these. Mirrors terraform/interceptors.tf.
    // ======================================================================
    const interceptorArns: Partial<Record<InterceptionPoint, string>> = {};
    for (const point of INTERCEPTION_POINTS) {
      const ic = props.interceptors?.[point];
      if (!ic) continue;
      interceptorArns[point] = ic.code !== undefined
        ? this.codeFunction(`interceptor-${point}`, ic.code, agentName, orchRoot).functionArn
        : ic.lambdaArn!;
    }
    const interceptorList = INTERCEPTION_POINTS.filter((p) => interceptorArns[p]);
    if (interceptorList.length) {
      gatewayRole.addToPolicy(new iam.PolicyStatement({
        sid: "InvokeInterceptors", actions: ["lambda:InvokeFunction"],
        resources: interceptorList.map((p) => interceptorArns[p]!),
      }));
      for (const point of interceptorList) {
        const ic = props.interceptors![point]!;
        // Same-account only: a function's resource policy is edited from its account.
        if (ic.code === undefined && !cdk.Token.isUnresolved(account) && ic.lambdaArn!.split(":")[4] !== account) continue;
        new lambda.CfnPermission(this, `InterceptorPermission-${point}`, {
          functionName: interceptorArns[point]!,
          action: "lambda:InvokeFunction",
          principal: "bedrock-agentcore.amazonaws.com",
          sourceAccount: account,
        });
      }
    }

    // ======================================================================
    // Gateway
    // ======================================================================
    // Inbound auth is provider-specific because the token formats differ:
    // Cognito client-credentials tokens carry `client_id` + `scope` and no
    // `aud`, so pin allowedClients. Auth0 M2M tokens are the mirror image —
    // `aud` + `azp`, no `client_id` — so pin the audience and the `azp` claim.
    // Okta (`aud` + `cid`) and Entra v2 (`aud` + `azp`) take the Auth0 shape.
    const gateway = new agentcore.CfnGateway(this, "Gateway", {
      name: `${dashName}-gw`,
      roleArn: gatewayRole.roleArn,
      protocolType: "MCP",
      authorizerType: "CUSTOM_JWT",
      authorizerConfiguration: {
        customJwtAuthorizer: {
          discoveryUrl: props.gatewayDiscoveryUrl,
          ...(props.isCognito ? { allowedClients: [props.gatewayClientId, ...(props.agentClientIds ?? [])] } : {}),
          ...(!props.isCognito
            ? {
                allowedAudience: [props.gatewayAudience],
                customClaims: [
                  {
                    inboundTokenClaimName: props.clientClaim || "azp",
                    inboundTokenClaimValueType: "STRING",
                    authorizingClaimMatchValue: {
                      claimMatchOperator: "EQUALS",
                      claimMatchValue: { matchValueString: props.gatewayClientId },
                    },
                  },
                ],
              }
            : {}),
        },
      },
      ...(policyEngine
        ? {
            policyEngineConfiguration: {
              arn: policyEngine.attrPolicyEngineArn,
              mode: props.policyMode,
            },
          }
        : {}),
      ...(interceptorList.length
        ? {
            interceptorConfigurations: interceptorList.map((point) => ({
              interceptionPoints: [point.toUpperCase()],
              interceptor: { lambda: { arn: interceptorArns[point]! } },
              inputConfiguration: { passRequestHeaders: props.interceptors![point]!.passRequestHeaders === true },
            })),
          }
        : {}),
    });
    gateway.node.addDependency(gatewayRole);
    this.gatewayUrl = gateway.attrGatewayUrl;
    this.gatewayId = gateway.attrGatewayIdentifier;
    this.gatewayArn = gateway.attrGatewayArn;

    // ======================================================================
    // Person Gateway: tools that act as the PERSON using the app
    // ======================================================================
    // auth "user" (each person's own account, OAuth authorization code) and "obo" (the
    // person's sign-in exchanged on-behalf-of) need the PERSON's token at the Gateway:
    // AgentCore Identity keeps a 3LO grant per person, keyed by that token's subject, and
    // exchanges that token for OBO. The machine Gateway above only ever sees the agents'
    // shared client, so these tools get a Gateway of their own that trusts the app's
    // sign-in instead. MCP 2025-11-25 is what lets it answer "connect your account first"
    // (URL elicitation, -32042). No Cedar engine or interceptors here: the tool's own
    // provider authorizes each person.
    const personTools = entries.filter(([, s]) => s.auth === "user" || s.auth === "obo");
    let personGateway: agentcore.CfnGateway | undefined;
    if (personTools.length) {
      if (!props.personAuth?.discoveryUrl || !props.personAuth.audience) {
        throw new Error(`tools ${personTools.map(([n]) => n).join(", ")} act as the person, which needs the app's ` +
          "sign-in: deploy with a sign-in provider (not idp=none).");
      }
      personGateway = new agentcore.CfnGateway(this, "PersonGateway", {
        name: `${dashName}-gwu`,
        roleArn: gatewayRole.roleArn,
        protocolType: "MCP",
        protocolConfiguration: { mcp: { supportedVersions: ["2025-11-25"] } },
        authorizerType: "CUSTOM_JWT",
        authorizerConfiguration: {
          customJwtAuthorizer: {
            discoveryUrl: props.personAuth.discoveryUrl,
            allowedAudience: [props.personAuth.audience],
          },
        },
      });
      personGateway.node.addDependency(gatewayRole);
      gatewayRole.addToPolicy(new iam.PolicyStatement({
        sid: "PersonGatewayTokens",
        actions: ["bedrock-agentcore:GetResourceOauth2Token", "bedrock-agentcore:GetWorkloadAccessToken",
          "bedrock-agentcore:GetWorkloadAccessTokenForJWT"],
        resources: [
          `arn:aws:bedrock-agentcore:${region}:${account}:token-vault/*`,
          `arn:aws:acps:${region}:${account}:token-vault/*`,
          `arn:aws:bedrock-agentcore:${region}:${account}:workload-identity-directory/*`,
        ],
      }));
    }
    this.personGatewayUrl = personGateway?.attrGatewayUrl ?? "";



    // ======================================================================
    // Knowledge Base (only when a tool declares type=kb)
    // ======================================================================
    // Three sources, from the tool's keys (mirrors local.kb_source in terraform/kb.tf):
    //   upload   — kb_docs/ (and a Builder build's uploads): the framework makes it all.
    //   s3       — `s3Uri`: YOUR bucket is the data source; the framework makes the
    //              vector store and KB, and reads only that bucket/prefix (+ `kmsKeyArn`).
    //   existing — `knowledgeBaseId`: YOUR Knowledge Base; the framework makes nothing
    //              but the retrieval function, and never deletes it.
    const kbSource = kbSpec.knowledgeBaseId ? "existing" : kbSpec.s3Uri ? "s3" : "upload";
    let kbId = "";
    let kbArn = "";
    let kbLambda: lambda.Function | undefined;
    this.knowledgeBaseId = "";

    if (kbName && kbSource === "existing") {
      kbId = String(kbSpec.knowledgeBaseId);
      kbArn = `arn:aws:bedrock:${region}:${account}:knowledge-base/${kbId}`;
      this.knowledgeBaseId = kbId;
    }

    if (kbName && kbSource !== "existing") {
      let kb: bedrock.CfnKnowledgeBase;
      const vectorBucketName = `agentcore-${dashName}-kb-${account}`;
      const vectorBucket = new s3vectors.CfnVectorBucket(this, "KbVectorBucket", {
        vectorBucketName,
      });

      const vectorIndex = new s3vectors.CfnIndex(this, "KbVectorIndex", {
        // The NAME, not vectorBucket.ref: Ref on AWS::S3Vectors::VectorBucket
        // returns the ARN, and CfnIndex caps VectorBucketName at 63 chars, so
        // the ref fails validation at DEPLOY time (synth cannot catch it):
        //   #/VectorBucketName: expected maxLength: 63, actual: 97
        // The explicit addDependency below preserves the ordering the ref implied.
        vectorBucketName,
        // The name carries a digest of this index's IMMUTABLE properties (see
        // kbIndexName). It was the fixed literal "kb-index", and changing any
        // immutable property then failed the deploy outright:
        //   "CloudFormation cannot update a stack when a custom-named resource
        //    requires replacing. Rename ... and update the stack again."
        // Deriving the name means a change to those properties yields a NEW name,
        // so CloudFormation can replace the index cleanly. Both the Knowledge Base
        // and Terraform reference the index by ARN, so the rename propagates.
        indexName: kbIndexName(KB_DIMS, KB_NON_FILTERABLE),
        dataType: "float32",
        dimension: KB_DIMS,
        distanceMetric: "cosine",
        // S3 Vectors caps FILTERABLE metadata at 2048 bytes per vector, and both
        // of these grow with the document, so both must be excluded:
        //   AMAZON_BEDROCK_TEXT     — the chunk's own text.
        //   AMAZON_BEDROCK_METADATA — a JSON blob carrying `text`, `parentText`,
        //                             the source location and a document id.
        //
        // Only TEXT was listed here, which is why ingestion silently accepted the
        // two small sample documents and then FAILED on a larger one with
        //   "Invalid record ...: Filterable metadata must have at most 2048 bytes
        //    (Service: S3Vectors, Status Code: 400)"
        // — one document failed, the job went to FAILED, and the KB simply never
        // returned that content. Nothing filters on either key (the only filter
        // this framework uses is `doc_type`), so excluding both costs nothing.
        metadataConfiguration: { nonFilterableMetadataKeys: KB_NON_FILTERABLE },
      });
      vectorIndex.node.addDependency(vectorBucket);

      // The data source: this deployment's own docs bucket (uploads), or yours (s3Uri).
      const kbDocsDir = path.join(orchRoot, "kb_docs");
      let kbFiles: string[] = [];
      let docsBucketArn: string;
      let docsPrefix = "";
      let docsDeployment: s3deploy.BucketDeployment | undefined;
      if (kbSource === "upload") {
        const docsBucket = new s3.Bucket(this, "KbDocsBucket", {
          bucketName: `agentcore-${dashName}-kbdocs-${account}`,
          blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
          encryption: s3.BucketEncryption.S3_MANAGED,
          enforceSSL: true,
          removalPolicy: cdk.RemovalPolicy.DESTROY,
          autoDeleteObjects: true,
        });
        docsBucketArn = docsBucket.bucketArn;

        // Per-document metadata sidecars. Bedrock reads "<key>.metadata.json"
        // beside each source object and attaches its attributes to every chunk.
        // doc_type = the document's TOP-LEVEL FOLDER, which is what lets an agent
        // (and the Cedar permit) scope retrieval to one corpus.
        kbFiles = listFilesRecursive(kbDocsDir).filter((f) => !f.endsWith(".DS_Store"));
        const sidecars = kbFiles.map((rel) =>
          s3deploy.Source.data(
            `${rel}.metadata.json`,
            JSON.stringify({ metadataAttributes: { doc_type: rel.split("/")[0] } })
          )
        );

        docsDeployment = new s3deploy.BucketDeployment(this, "KbDocsDeploy", {
          logGroup: new logs.LogGroup(this, "KbDocsDeployLogGroup", {
            retention: LOG_RETENTION,
            removalPolicy: cdk.RemovalPolicy.DESTROY,
          }),
          destinationBucket: docsBucket,
          sources: [
            s3deploy.Source.asset(kbDocsDir, { exclude: [".DS_Store", "**/.DS_Store"] }),
            ...sidecars,
          ],
        });
      } else {
        const loc = parseS3Location(String(kbSpec.s3Uri));
        docsBucketArn = `arn:aws:s3:::${loc.bucket}`;
        docsPrefix = loc.prefix;
      }

      const kbRole = new iam.Role(this, "KbRole", {
        roleName: `AgentCoreKB-${agentName}`,
        assumedBy: new iam.ServicePrincipal("bedrock.amazonaws.com", {
          conditions: { StringEquals: { "aws:SourceAccount": account } },
        }),
      });
      kbRole.addToPolicy(
        new iam.PolicyStatement({ sid: "Embeddings", actions: ["bedrock:InvokeModel"], resources: [embedModel] })
      );
      kbRole.addToPolicy(
        new iam.PolicyStatement({
          sid: "S3VectorsData",
          // The verbs a Knowledge Base uses. The wildcard also granted DeleteIndex and
          // DeleteVectorBucket, which a KB never calls. Mirrors terraform/kb.tf.
          actions: [
            "s3vectors:GetVectorBucket", "s3vectors:GetIndex", "s3vectors:ListIndexes",
            "s3vectors:PutVectors", "s3vectors:GetVectors", "s3vectors:ListVectors",
            "s3vectors:QueryVectors", "s3vectors:DeleteVectors",
          ],
          resources: [vectorBucket.attrVectorBucketArn, `${vectorBucket.attrVectorBucketArn}/*`],
        })
      );
      if (kbSource === "upload") {
        kbRole.addToPolicy(
          new iam.PolicyStatement({
            sid: "DocsRead",
            actions: ["s3:GetObject", "s3:ListBucket"],
            resources: [docsBucketArn, `${docsBucketArn}/*`],
          })
        );
      } else {
        // YOUR bucket: read only, and only under the prefix you named. Mirrors the
        // DocsRead/DocsList statements in terraform/kb.tf.
        kbRole.addToPolicy(
          new iam.PolicyStatement({
            sid: "DocsRead",
            actions: ["s3:GetObject"],
            resources: [`${docsBucketArn}/${docsPrefix}*`],
          })
        );
        kbRole.addToPolicy(
          new iam.PolicyStatement({
            sid: "DocsList",
            actions: ["s3:ListBucket"],
            resources: [docsBucketArn],
            ...(docsPrefix ? { conditions: { StringLike: { "s3:prefix": [`${docsPrefix}*`] } } } : {}),
          })
        );
        if (kbSpec.kmsKeyArn) {
          kbRole.addToPolicy(
            new iam.PolicyStatement({
              sid: "KmsDecrypt",
              actions: ["kms:Decrypt"],
              resources: [String(kbSpec.kmsKeyArn)],
            })
          );
        }
      }

      kb = new bedrock.CfnKnowledgeBase(this, "KnowledgeBase", {
        // Digest-derived for the same reason as the index name, one level up: the
        // KB's storageConfiguration points at the index ARN and is immutable, so
        // replacing the index replaces the KB. See knowledgeBaseName.
        name: knowledgeBaseName(dashName, KB_DIMS, KB_NON_FILTERABLE),
        roleArn: kbRole.roleArn,
        knowledgeBaseConfiguration: {
          type: "VECTOR",
          vectorKnowledgeBaseConfiguration: {
            embeddingModelArn: embedModel,
            embeddingModelConfiguration: {
              bedrockEmbeddingModelConfiguration: {
                dimensions: KB_DIMS,
                embeddingDataType: "FLOAT32",
              },
            },
          },
        },
        storageConfiguration: {
          type: "S3_VECTORS",
          s3VectorsConfiguration: { indexArn: vectorIndex.attrIndexArn },
        },
      });
      // Bedrock validates the role's permissions at CREATE time, so the inline
      // POLICY (not just the role) must exist first.
      kb.node.addDependency(kbRole);
      dependOnDefaultPolicy(kb, kbRole);
      kb.node.addDependency(vectorIndex);
      this.knowledgeBaseId = kb.attrKnowledgeBaseId;
      kbId = kb.attrKnowledgeBaseId;
      kbArn = kb.attrKnowledgeBaseArn;

      const dataSource = new bedrock.CfnDataSource(this, "KbDataSource", {
        knowledgeBaseId: kb.attrKnowledgeBaseId,
        name: "docs",
        dataSourceConfiguration: {
          type: "S3",
          s3Configuration: {
            bucketArn: docsBucketArn,
            ...(docsPrefix ? { inclusionPrefixes: [docsPrefix] } : {}),
          },
        },
      });

      // StartIngestionJob has no declarative equivalent. For uploads the corpus hash is
      // baked into the physical id, so editing kb_docs/ re-ingests on the next deploy and
      // nothing else does. YOUR bucket changes without this stack knowing, so it is
      // re-synced on every deploy (ingestion is incremental: unchanged files are skipped).
      const corpusHash = kbSource === "upload" ? hashCorpus(kbDocsDir, kbFiles) : `s3-${Date.now()}`;
      const ingestCall = {
        service: "bedrock-agent",
        action: "StartIngestionJob",
        parameters: {
          knowledgeBaseId: kb.attrKnowledgeBaseId,
          dataSourceId: dataSource.attrDataSourceId,
        },
        physicalResourceId: cr.PhysicalResourceId.of(`kb-ingest-${corpusHash}`),
      };
      const ingest = new cr.AwsCustomResource(this, "KbIngest", {
        logGroup: new logs.LogGroup(this, "KbIngestLogGroup", {
          retention: LOG_RETENTION,
          removalPolicy: cdk.RemovalPolicy.DESTROY,
        }),
        onCreate: ingestCall,
        onUpdate: ingestCall,
        policy: cr.AwsCustomResourcePolicy.fromStatements([
          new iam.PolicyStatement({
            actions: ["bedrock:StartIngestionJob"],
            resources: [kb.attrKnowledgeBaseArn],
          }),
        ]),
        installLatestAwsSdk: false,
      });
      if (docsDeployment) ingest.node.addDependency(docsDeployment);
      ingest.node.addDependency(dataSource);
    }

    if (kbName) {
      kbLambda = new lambda.Function(this, "KbRetrieve", {
        // Owned explicitly so `destroy` removes it — see LOG_RETENTION in
        // lib/orchestrator-stack.ts for why an implicit group is a cost leak.
        logGroup: new logs.LogGroup(this, "KbRetrieveLogGroup", {
          logGroupName: `/aws/lambda/AgentCoreKBRetrieve-${agentName}`,
          retention: LOG_RETENTION,
          removalPolicy: cdk.RemovalPolicy.DESTROY,
        }),
        functionName: `AgentCoreKBRetrieve-${agentName}`,
        runtime: lambda.Runtime.PYTHON_3_13,
        handler: "handler.lambda_handler",
        code: lambda.Code.fromAsset(path.join(orchRoot, "kb_lambda")),
        timeout: cdk.Duration.seconds(30),
        memorySize: 256,
        environment: {
          KB_ID: kbId,
          // Retrieval depth, from `tools.<kb>.maxResults` in app/workflow.json. It was
          // a Lambda-only env var that NO IaC set, so depth was frozen at the
          // handler's default of 5 and the only way to change it was hand-editing a
          // deployed function — while `tools.<websearch>.maxResults` was a config key.
          // Same key name on both tool types now. Mirrors local.kb_max_results in
          // terraform/tools.tf.
          KB_NUM_RESULTS: String(kbSpec.maxResults ?? keyDefaultFor<number>("tool", "maxResults", "kb")),
          // The rest of the retrieval shape, also from the tool's entry. An empty string
          // means "keep the handler's default", so this block does not restate defaults
          // that already have one documented home (kb_lambda/handler.py).
          // Mirrors the same five env vars in terraform/kb.tf.
          KB_CORPUS_KEY: kbSpec.corpusKey ?? keyDefault<string>("tool", "corpusKey"),
          KB_CORPUS_OPERATOR: kbSpec.corpusOperator ?? keyDefault<string>("tool", "corpusOperator"),
          KB_STATIC_FILTER: kbSpec.filter ? JSON.stringify(kbSpec.filter) : "",
          KB_RERANK: kbSpec.rerank ? JSON.stringify(kbSpec.rerank) : "",
        },
      });
      kbLambda.addToRolePolicy(
        new iam.PolicyStatement({ actions: ["bedrock:Retrieve"], resources: [kbArn] })
      );
      // Reranking is a SECOND model call Bedrock makes on this function's behalf, so the
      // function's own role needs it — granted only when the tool asks for reranking, and
      // on exactly the model it named rather than foundation-model/*.
      if (kbSpec.rerank?.model) {
        const m = kbSpec.rerank.model;
        kbLambda.addToRolePolicy(
          new iam.PolicyStatement({
            sid: "InvokeRerankingModel",
            actions: ["bedrock:Rerank", "bedrock:InvokeModel"],
            resources: [
              m.startsWith("arn:") ? m : `arn:aws:bedrock:${region}::foundation-model/${m}`,
            ],
          })
        );
      }
      gatewayRole.addToPolicy(
        new iam.PolicyStatement({ actions: ["lambda:InvokeFunction"], resources: [kbLambda.functionArn] })
      );
      kbLambda.addPermission("AllowAgentCoreGatewayInvoke", {
        principal: new iam.ServicePrincipal("bedrock-agentcore.amazonaws.com"),
        action: "lambda:InvokeFunction",
        sourceAccount: account,
      });
    }

    // ---- type=openapi -----------------------------------------------------
    // Mirrors aws_s3_bucket.tool_schemas + aws_s3_object.openapi_schema and the
    // ReadOpenApiSchemas statement in terraform/gateway.tf.
    //
    // The Gateway loads an OpenAPI schema from S3 and nowhere else, and it loads it AS
    // THE GATEWAY ROLE. Before this existed the role had no s3:GetObject at all, so
    // `type: "openapi"` could not work in either plane: target creation failed on a
    // schema it was not allowed to read, and the message named neither the bucket nor
    // the permission.
    const openApiTools = entries.filter(([, s]) => s.type === "openapi");
    const schemaSourceTools = openApiTools.filter(([, s]) => s.source);
    const schemaReadArns: string[] = [];
    /** tool name -> its upload, so the target can depend on the object existing. */
    const schemaUploads: Record<string, s3deploy.BucketDeployment> = {};

    if (schemaSourceTools.length) {
      // Created only when a tool actually uses `source`, so a deployment that hosts
      // its own schemas — or declares no openapi tool — pays for no bucket.
      //
      // Private and encrypted, and NOT because these schemas are secret (the one this
      // sample ships describes a public API). A schema enumerates which operations an
      // agent may reach, so it is part of the authorization surface, and a
      // world-readable copy is an inventory of your endpoints for anyone who finds the
      // bucket.
      const schemaBucket = new s3.Bucket(this, "ToolSchemasBucket", {
        // Same shape as the KB buckets: `agentcore-<dashed agent name>-<what>-<account>`.
        // The agent name is dashed because an S3 bucket name may not contain an
        // underscore, and the region is NOT in it because the first version of this
        // name was 64 characters — one over the limit — which fails at synth.
        bucketName: `agentcore-${dashName}-schemas-${account}`,
        blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
        encryption: s3.BucketEncryption.S3_MANAGED,
        enforceSSL: true,
        versioned: true,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
        autoDeleteObjects: true,
      });
      schemaReadArns.push(schemaBucket.arnForObjects("*"));

      for (const [name, spec] of schemaSourceTools) {
        // One deployment per tool, keyed by the tool name rather than by `source`, so
        // two tools may legitimately share one schema folder and still get their own
        // object and their own target.
        schemaUploads[name] = new s3deploy.BucketDeployment(this, `ToolSchema-${name}`, {
          destinationBucket: schemaBucket,
          destinationKeyPrefix: name,
          sources: [
            s3deploy.Source.asset(path.join(orchRoot, "app", "tools", spec.source!), {
              exclude: ["*", "!openapi.json"],
            }),
          ],
          prune: false,
        });
        this.uploadedSchemaUris[name] = schemaBucket.s3UrlForObject(`${name}/openapi.json`);
      }
    }

    // A schema the CUSTOMER hosts still needs the grant, scoped to that exact object
    // rather than to "*" — the URI is parsed rather than trusted wholesale so a
    // malformed one cannot widen the grant to a whole bucket.
    for (const [, spec] of openApiTools.filter(([, s]) => s.schemaS3Uri)) {
      const m = /^s3:\/\/([^/]+)\/(.+)$/.exec(String(spec.schemaS3Uri));
      if (m) schemaReadArns.push(`arn:aws:s3:::${m[1]}/${m[2]}`);
    }

    if (schemaReadArns.length) {
      gatewayRole.addToPolicy(
        new iam.PolicyStatement({
          sid: "ReadOpenApiSchemas",
          actions: ["s3:GetObject"],
          resources: schemaReadArns,
        })
      );
    }

    // ---- type=lambda ------------------------------------------------------
    // Mirrors the tool_lambda resources + aws_iam_role_policy.gateway_invoke_lambda
    // + aws_lambda_permission.gateway_invoke_tool_lambda in terraform/tools.tf
    // (the Terraform resource names kept their old spelling; the SOURCE moved).
    //
    // Two ways a function arrives: `source` (one the framework ships and deploys)
    // or `lambdaArn` (one you already own, which is only registered).
    const lambdaTools = entries.filter(([, s]) => s.type === "lambda");
    if (lambdaTools.length) {
      // The framework-deployed built-in. `source` is a FOLDER NAME under
      // app/tools/ — `source: "pricing"` is app/tools/pricing/ — and its execution
      // role is FIXED: logs plus READ-ONLY on the PUBLIC AWS price list. That role is
      // exactly why `source` accepts no other value. It is right for this one
      // function and wrong for almost any other: a warehouse connector needs VPC
      // config and a secret, an RDBMS connector needs credentials, and none of that
      // is expressible in workflow.json. YOUR connector is a function you deploy,
      // declared with `lambdaArn`.
      for (const [name, spec] of lambdaTools.filter(([, s]) => s.source)) {
        const fn = new lambda.Function(this, `ToolLambda-${name}`, {
          logGroup: new logs.LogGroup(this, `ToolLambdaLogGroup-${name}`, {
            logGroupName: `/aws/lambda/ToolLambda-${agentName}-${name}`,
            retention: LOG_RETENTION,
            removalPolicy: cdk.RemovalPolicy.DESTROY,
          }),
          // Prefixed to match this function's own IAM role (ToolLambda-…) and the
          // other framework-owned functions (AgentCoreBFF-…, AgentCoreKBRetrieve-…).
          // Without a prefix the name is just "<agentName>-<tool>", which a scoped
          // deploy policy cannot express without granting lambda:* on every
          // function in the account. Mirrors terraform/tools.tf.
          functionName: `ToolLambda-${agentName}-${name}`,
          runtime: lambda.Runtime.PYTHON_3_12,
          handler: "handler.lambda_handler",
          code: lambda.Code.fromAsset(path.join(props.orchRoot, "app", "tools", spec.source!)),
          timeout: cdk.Duration.seconds(30),
          memorySize: 256,
          environment: {
            // Where the Price List Query API endpoint lives, which is NOT where
            // prices are being asked about: the API is published in only two
            // regions and describes prices for all of them. Pinned so the tool
            // works on a deployment in any region. Mirrors terraform/tools.tf.
            PRICING_API_REGION: "us-east-1",
          },
        });
        // Read only, and the only thing it reads is the PUBLIC price list — no
        // customer data, no account spend. The Price List Query API has no
        // resource-level permissions, so "*" is the only grant it accepts.
        // Mirrors aws_iam_role_policy.tool_lambda in terraform/tools.tf.
        fn.addToRolePolicy(
          new iam.PolicyStatement({
            sid: "ReadPublicPriceList",
            actions: ["pricing:GetProducts", "pricing:DescribeServices"],
            resources: ["*"],
          })
        );
        this.builtinLambdaArns[name] = fn.functionArn;
      }

      // ---- type=lambda with `code`: a function written in the build ------------
      // Its files are app/tools/_code/<name>/ (scaffold.py apply wrote them from the
      // bundle; the runner pip-installed its requirements into the same folder). Its
      // role is logs plus ONLY the grants the build names — and the same statements are
      // its permissions boundary, with an explicit Deny on the framework's own tables,
      // secrets and buckets, so nothing added to the role later reaches further.
      // Mirrors aws_lambda_function.tool_code in terraform/tools.tf.
      for (const [name, spec] of lambdaTools.filter(([, s]) => s.code)) {
        this.builtinLambdaArns[name] = this.codeFunction(name, spec.code!, agentName, props.orchRoot).functionArn;
      }

      // Every declared lambda tool's effective ARN, whichever way it was supplied,
      // so the target and the IAM grant below need not care.
      const arnOf = (name: string, spec: ToolSpec) =>
        spec.source || spec.code ? this.builtinLambdaArns[name] : spec.lambdaArn!;

      // One statement listing every function, rather than a policy per tool, so the
      // inline policy stays small as the tool count grows.
      gatewayRole.addToPolicy(
        new iam.PolicyStatement({
          sid: "InvokeToolLambdas",
          actions: ["lambda:InvokeFunction"],
          resources: lambdaTools.map(([n, s]) => arnOf(n, s)).sort(),
        })
      );
      for (const [name, spec] of lambdaTools) {
        // A Lambda's resource policy can only be edited from the account that owns
        // the function, so this is emitted for same-account functions only. A
        // cross-account function still works — its owner adds the statement. A
        // framework-deployed function is always local.
        // Only SKIP when we can prove the function belongs to another account. With
        // an env-agnostic stack `account` is an unresolved token, which never equals a
        // real 12-digit id — so this used to skip a same-account function silently and
        // the tool failed at first invoke with an authorization error. Line ~436 guards
        // the same hazard for the Cognito domain prefix; this site did not.
        if (!spec.source && !spec.code && !cdk.Token.isUnresolved(account)
            && spec.lambdaArn!.split(":")[4] !== account) continue;
        new lambda.CfnPermission(this, `ToolLambdaPermission-${name}`, {
          functionName: arnOf(name, spec),
          action: "lambda:InvokeFunction",
          principal: "bedrock-agentcore.amazonaws.com",
          sourceAccount: account,
        });
      }
    }

    // ======================================================================
    // Gateway targets — one per declared tool
    // ======================================================================
    // Registered one at a time: CloudFormation would otherwise create them in
    // parallel, and concurrent target registration on a single Gateway is not
    // something the service guarantees.
    let previousTarget: agentcore.CfnGatewayTarget | undefined;
    const targets: agentcore.CfnGatewayTarget[] = [];

    for (const [name, spec] of entries) {
      // A vaulted key, for the types that reach a third-party endpoint. The name
      // MUST start with "bedrock-agentcore" so the Gateway role's Secrets Manager
      // statement above can read it.
      const apiKey = props.toolApiKeys[name];
      let credProviderArn: string | undefined;
      let oauthProviderArn: string | undefined;
      const person = spec.auth === "user" || spec.auth === "obo";
      if (spec.auth === "oauth2" || person) {
        // OAuth client credentials: the client SECRET arrives like an API key (the
        // build's secrets -> TOOL_API_KEYS), the rest from `oauth`. Mirrors
        // aws_bedrockagentcore_oauth2_credential_provider.tool in terraform/tools.tf.
        if (!apiKey) {
          throw new Error(
            `tools.${name} has auth "${spec.auth}" but no client secret: set it in the build's secrets ` +
              `(tool_api_keys.${name}), the same place as an API key.`
          );
        }
        const oa = spec.oauth!;
        const issuer = oa.issuer ?? (oa.tokenUrl ? new URL(oa.tokenUrl).origin : "");
        const cp = new agentcore.CfnOAuth2CredentialProvider(this, `OAuth-${name}`, {
          name: `bedrock-agentcore-${dashName}-${name}`,
          credentialProviderVendor: "CustomOauth2",
          oauth2ProviderConfigInput: {
            customOauth2ProviderConfig: {
              clientId: oa.clientId,
              clientSecret: apiKey,
              oauthDiscovery: oa.discoveryUrl
                ? { discoveryUrl: oa.discoveryUrl }
                : {
                    authorizationServerMetadata: {
                      issuer,
                      tokenEndpoint: String(oa.tokenUrl),
                      // Each person's sign-in goes there; unused by client credentials and
                      // the exchange, but required by the API.
                      authorizationEndpoint: String(oa.authorizationUrl ?? oa.tokenUrl),
                    },
                  },
              // OBO: the person's sign-in token is the subject; the provider authenticates
              // this client and needs no separate actor token.
              ...(spec.auth === "obo"
                ? { onBehalfOfTokenExchangeConfig: { grantType: "TOKEN_EXCHANGE",
                    tokenExchangeGrantTypeConfig: { actorTokenContent: "NONE" } } }
                : {}),
            },
          },
        });
        oauthProviderArn = cp.attrCredentialProviderArn;
        if (spec.auth === "user") this.callbackUrls[name] = cp.attrCallbackUrl;
      } else if (apiKey && spec.auth !== "sigv4" && vocab.API_KEY_TOOL_TYPES.includes(spec.type)) {
        const cp = new agentcore.CfnApiKeyCredentialProvider(this, `Cred-${name}`, {
          name: `bedrock-agentcore-${dashName}-${name}`,
          apiKey,
        });
        credProviderArn = cp.attrCredentialProviderArn;
      }

      // How the Gateway authenticates OUTBOUND to this tool: a vaulted API key
      // when one was supplied, its own execution role for the KB Lambda, and
      // nothing for a public endpoint or a managed connector.
      const credentialProviderConfigurations = oauthProviderArn
        ? [
            {
              credentialProviderType: "OAUTH",
              credentialProvider: {
                oauthCredentialProvider: {
                  providerArn: oauthProviderArn,
                  scopes: spec.oauth?.scopes ?? [],
                  ...(spec.auth === "user"
                    // The person connects once; then back to the app, which binds the
                    // grant to them (POST /api/connect).
                    ? { grantType: "AUTHORIZATION_CODE", defaultReturnUrl: props.returnUrl ?? "" }
                    : spec.auth === "obo"
                    ? { grantType: "TOKEN_EXCHANGE", customParameters: {
                        // The subject is the app's sign-in: an ID token (bff -> runtime).
                        subject_token_type: "urn:ietf:params:oauth:token-type:id_token",
                        ...(spec.oauth?.audience ? { audience: spec.oauth.audience } : {}) } }
                    : { grantType: "CLIENT_CREDENTIALS" }),
                },
              },
            },
          ]
        : credProviderArn
        ? [
            {
              credentialProviderType: "API_KEY",
              credentialProvider: {
                apiKeyCredentialProvider: {
                  providerArn: credProviderArn,
                  credentialLocation: "HEADER",
                  credentialParameterName: "X-API-Key",
                },
              },
            },
          ]
        : // SigV4: the Gateway signs with its own execution role — no secret at
          // all. Also the mode for BOTH Lambda target kinds (the KB retrieve
          // function and a customer's own `type: "lambda"` function): the Gateway
          // invokes them as itself.
          // A `websearch` connector needs one too: without it CreateGatewayTarget
          // fails with "Credential provider configurations is not defined". The
          // connector runs inside AWS, so the Gateway uses its OWN execution role.
          spec.auth === "sigv4" ||
            spec.type === "kb" ||
            spec.type === "lambda" ||
            spec.type === "websearch"
          ? [
              {
                credentialProviderType: "GATEWAY_IAM_ROLE",
                // Signing for a named AWS service (mcp / openapi `service`). An API Gateway
                // target takes the role alone: AgentCore refuses an iamCredentialProvider
                // there ("not supported for this target type", seen live) and signs for
                // execute-api itself.
                ...(spec.auth === "sigv4" && spec.service && spec.type !== "apigateway"
                  ? {
                      credentialProvider: {
                        iamCredentialProvider: {
                          service: spec.service ?? "execute-api",
                          region: cdk.Stack.of(this).region,
                        },
                      },
                    }
                  : {}),
              },
            ]
          : undefined;

      const target = new agentcore.CfnGatewayTarget(this, `Target-${name}`, {
        gatewayIdentifier: person ? personGateway!.attrGatewayIdentifier : this.gatewayId,
        name, // MUST equal the agent's `tool` label in workflow.json
        // workflow.json descriptions are written to explain the tool to whoever
        // edits the config, so they can be long; CloudFormation caps this field
        // at 200 characters.
        description: truncate(spec.description ?? `Tool target ${name}`, 190),
        targetConfiguration: { mcp: this.mcpConfigFor(name, spec, kbLambda) },
        ...(credentialProviderConfigurations ? { credentialProviderConfigurations } : {}),
      });

      if (spec.type === "kb") dependOnDefaultPolicy(target, gatewayRole);
      // An openapi target is the one kind whose creation READS something: the Gateway
      // fetches and parses the schema as the gateway role. So both the grant and the
      // upload have to land first, or creation fails on a schema that is missing or
      // unreadable — and it fails at deploy, after a clean synth.
      if (spec.type === "openapi") {
        dependOnDefaultPolicy(target, gatewayRole);
        const upload = schemaUploads[name];
        if (upload) target.node.addDependency(upload);
      }
      if (previousTarget) target.node.addDependency(previousTarget);
      previousTarget = target;
      targets.push(target);
    }

    // ======================================================================
    // Cedar policy — generated from the same config
    // ======================================================================
    // Declaring a tool permits it. Anything NOT declared — including a tool name
    // a prompt-injected instruction invents — is refused by Cedar's default-deny.
    if (policyEngine) {
      for (const [name, spec] of entries) {
        if ((spec.policy?.permit ?? keyDefault<boolean>("tool", "policy.permit")) === false) continue;
        if (spec.auth === "user" || spec.auth === "obo") continue;   // on the person Gateway, which has no engine
        const policy = new agentcore.CfnPolicy(this, `Policy-${name}`, {
          name: cedarPolicyName(name, agentName),
          description: `Generated from workflow.json tools.${name}. Anything not permitted here is denied by Cedar's default-deny.`,
          policyEngineId: policyEngine.attrPolicyEngineId,
          definition: { cedar: { statement: cedarStatement(name, spec, this.gatewayArn) } },
          // The engine's analyser emits ADVISORY findings, and the default
          // (FAIL_ON_ANY_FINDINGS) turns one into a hard CloudFormation failure:
          //   "Overly Permissive: Policy Engine will allow every request for the
          //    specified principal (AgentCore::IamEntity), action
          //    (websearch___WebSearch) and resource ... combination"
          //    (HandlerErrorCode: NotStabilized)
          // That finding is CORRECT and intended: every agent shares one M2M
          // identity, so a tool permit deliberately applies to any authenticated
          // caller. The authorization boundary being demonstrated is per-TOOL (and
          // per-argument for the KB corpus), not per-principal. Findings are still
          // recorded; they just no longer block the deploy.
          validationMode: "IGNORE_ALL_FINDINGS",
        });
        // Cedar validates action names against the gateway's REGISTERED targets,
        // so every target must exist before any policy is created.
        targets.forEach((t) => policy.node.addDependency(t));
      }
      // The user's own policies (a forbid wins over any permit; see bff/cedar.py for
      // what was checked before they got here). Same findings mode, same ordering.
      for (const c of props.customPolicies ?? []) {
        const policy = new agentcore.CfnPolicy(this, `CustomPolicy-${c.name}`, {
          name: customPolicyName(c.name, agentName),
          description: (c.description || `workflow.json orchestrator.policy.custom ${c.name}.`).slice(0, 400),
          policyEngineId: policyEngine.attrPolicyEngineId,
          definition: { cedar: { statement: c.statement.split(GATEWAY_PLACEHOLDER).join(this.gatewayArn) } },
          validationMode: "IGNORE_ALL_FINDINGS",
        });
        targets.forEach((t) => policy.node.addDependency(t));
      }
    }

    dependOnDefaultPolicy(gateway, gatewayRole);
  }

  /** A function written in the build: a code tool (tools.<name>.code) or an interceptor
   *  (name interceptor-<point>). Its files are app/tools/_code/<name>/ (scaffold.py
   *  apply wrote them from the bundle; the runner pip-installed its requirements into
   *  the same folder). Its role is logs plus ONLY the grants the build names — and the
   *  same statements are its permissions boundary, with an explicit Deny on the
   *  framework's own tables, secrets and buckets, so nothing added to the role later
   *  reaches further. Mirrors terraform/tools_code.tf. */
  private codeFunction(name: string, code: CodeSpec, agentName: string, orchRoot: string): lambda.Function {
    const { region, account } = cdk.Stack.of(this);
    const fnName = `ToolLambda-${agentName}-${name}`;
    const toStatement = (s: any) => new iam.PolicyStatement({
      sid: s.sid, effect: s.effect === "Deny" ? iam.Effect.DENY : iam.Effect.ALLOW,
      actions: s.actions, resources: s.resources, ...(s.conditions ? { conditions: s.conditions } : {}),
    });
    const grants = codeToolStatements(fnName, code, region, account);
    const boundary = new iam.ManagedPolicy(this, `ToolCodeBoundary-${name}`, {
      managedPolicyName: `${fnName}-boundary`,
      description: `The most ${fnName} may ever do: its grants in workflow.json, and never the framework's own data.`,
      statements: [...grants, ...CODE_BOUNDARY_DENY.map((d) => ({ ...d, effect: "Deny" }))].map(toStatement),
    });
    const role = new iam.Role(this, `ToolCodeRole-${name}`, {
      roleName: fnName,
      assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
      permissionsBoundary: boundary,
      inlinePolicies: { grants: new iam.PolicyDocument({ statements: grants.map(toStatement) }) },
    });
    const fn = new lambda.Function(this, `ToolLambda-${name}`, {
      logGroup: new logs.LogGroup(this, `ToolLambdaLogGroup-${name}`, {
        logGroupName: `/aws/lambda/${fnName}`,
        retention: LOG_RETENTION,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
      functionName: fnName,
      role,
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: "handler.lambda_handler",
      code: lambda.Code.fromAsset(path.join(orchRoot, "app", "tools", "_code", name),
        { exclude: ["__pycache__", ".from-builder", "events.json"] }),
      timeout: cdk.Duration.seconds(code.timeoutSeconds ?? 30),
      memorySize: code.memoryMB ?? 256,
      environment: codeToolEnvironment(code),
    });
    if (code.grants?.vpc) {
      // By id: a VPC lookup would need this account's context at synth time.
      (fn.node.defaultChild as lambda.CfnFunction).vpcConfig = {
        subnetIds: code.grants.vpc.subnetIds, securityGroupIds: code.grants.vpc.securityGroupIds,
      };
    }
    return fn;
  }

  /** The `mcp` target configuration for one tool, by type. */
  private mcpConfigFor(
    name: string,
    spec: ToolSpec,
    kbLambda?: lambda.Function
  ): agentcore.CfnGatewayTarget.McpTargetConfigurationProperty {
    switch (spec.type) {
      case "kb":
        if (!kbLambda) throw new Error(`tools.${name} is type=kb but the KB Lambda was not created.`);
        return {
          lambda: {
            lambdaArn: kbLambda.functionArn,
            toolSchema: {
              inlinePayload: [
                {
                  name: "retrieve",
                  description:
                    "Retrieve relevant document chunks from the knowledge base for a query.",
                  inputSchema: {
                    type: "object",
                    properties: {
                      query: { type: "string", description: "The natural-language search query." },
                      filter: {
                        type: "string",
                        description: `Optional corpus (doc_type) to restrict retrieval to, e.g. ${(spec.corpora ?? []).join(" | ")}.`,
                      },
                    },
                    required: ["query"],
                  },
                },
              ],
            },
          },
        };

      case "websearch": {
        // A managed built-in connector: no endpoint, no schema, no API key.
        // The tool surfaces as "<name>___WebSearch".
        //
        // Domain filtering is ONE key and ONE place: `domains` in workflow.json,
        // applied here on the target, so the Gateway enforces it on every request and
        // the agent never sees it. See the longer note in terraform/tools.tf for what
        // this replaced and why nothing was lost. `publishedFrom`/`publishedTo` stay
        // request-level because the target has no equivalent.
        const domainFilter: Record<string, string[]> = {};
        if (spec.domains?.include?.length) domainFilter.include = spec.domains.include;
        if (spec.domains?.exclude?.length) domainFilter.exclude = spec.domains.exclude;
        return {
          connector: {
            // NOTE: no version pin here. The CreateGatewayTarget API and the
            // Terraform provider both accept `version` on the connector source,
            // but the CloudFormation resource does NOT — ConnectorSourceProperty
            // exposes only connectorId. So `connectorVersion` is a Terraform-only
            // option; validateTools rejects it on this path rather than accepting
            // it and silently ignoring it.
            source: { connectorId: "web-search" },
            configurations: [
              {
                name: "WebSearch",
                description: truncate(spec.description ?? `Tool target ${name}`, 190),
                // REQUIRED, even when empty. The API DISCARDS a configuration entry
                // that has no parameterValues and then reports "Connector
                // configurations must not be empty" — which reads as if the list
                // were absent rather than its single entry dropped. Verified
                // directly against CreateGatewayTarget: "{}" is what makes it count.
                parameterValues: Object.keys(domainFilter).length ? { domainFilter } : {},
              },
            ],
          },
        };
      }

      case "mcp":
        if (!spec.endpoint) {
          throw new Error(`tools.${name} has type="mcp" and so requires "endpoint".`);
        }
        return {
          mcpServer: {
            endpoint: spec.endpoint,
            listingMode: spec.listingMode ?? keyDefault<string>("tool", "listingMode"),
            // Used as the person: its tools are given here, since the Gateway cannot list
            // them from the server before anyone has connected an account.
            ...((spec.auth === "user" || spec.auth === "obo") && spec.toolSchema?.length
              ? { mcpToolSchema: { inlinePayload: JSON.stringify(inlineTools(spec.toolSchema)) } }
              : {}),
          },
        };

      case "openapi": {
        // Either a schema you host (by URI) or one the framework uploaded from
        // `source`. Mirrors the lambdaArn/source split below, for the same reason:
        // a bucket name is as account-specific as a function ARN, so a committed
        // workflow.json cannot carry one and stay deployable anywhere else.
        // Or the document itself, inline: nothing to host, nothing to grant.
        if (spec.schema) return { openApiSchema: { inlinePayload: JSON.stringify(spec.schema) } };
        const uri = spec.source ? this.uploadedSchemaUris[name] : spec.schemaS3Uri;
        if (!uri)
          throw new Error(
            `tools.${name} has type="openapi" and so requires "schemaS3Uri", "source" or "schema".`
          );
        return { openApiSchema: { s3: { uri } } };
      }

      case "apigateway":
        // A REST API stage in this account and region. The Gateway reads its definition
        // with GetExport (as the deployer, when the target is created) and exposes only
        // the operations the filters select.
        return {
          apiGateway: {
            restApiId: String(spec.restApiId),
            stage: String(spec.stage),
            apiGatewayToolConfiguration: {
              toolFilters: (spec.toolFilters ?? []).map((f) => ({ filterPath: f.path, methods: f.methods })),
              ...(spec.toolOverrides?.length
                ? {
                    toolOverrides: spec.toolOverrides.map((o) => ({
                      path: o.path, method: o.method, name: o.name,
                      ...(o.description ? { description: o.description } : {}),
                    })),
                  }
                : {}),
            },
          },
        };

      case "lambda": {
        // Either a function you own (by ARN) or one the framework deployed from
        // `source`. The schema has to be spelled out either way, because there is no
        // tools/list to discover it from.
        const lambdaArn = spec.source || spec.code ? this.builtinLambdaArns[name] : spec.lambdaArn;
        if (!lambdaArn)
          throw new Error(
            `tools.${name} has type="lambda" and so requires "lambdaArn" or "source".`
          );
        if (!spec.toolSchema?.length)
          throw new Error(`tools.${name} has type="lambda" and so requires a non-empty "toolSchema".`);
        return {
          lambda: {
            lambdaArn,
            toolSchema: { inlinePayload: inlineTools(spec.toolSchema) },
          },
        };
      }
    }
  }
}

/**
 * Build the Cedar permit for one tool.
 *
 * With `policy.tool`, emit a fine-grained permit on that single tool, optionally
 * argument-restricted. Without it, emit a TARGET-level permit: Cedar has no
 * wildcard actions, but a Gateway target IS an action group, so
 * `action in AgentCore::Action::"<target>"` covers every tool the target
 * publishes — which is the only workable form for a remote MCP server whose tool
 * names we cannot know at deploy time.
 */
export function cedarStatement(name: string, spec: ToolSpec, gatewayArn: string): string {
  const resource = `resource == AgentCore::Gateway::"${gatewayArn}"`;
  // The managed web-search connector always publishes exactly ONE tool, named
  // "WebSearch", so default to permitting it BY NAME. Not cosmetic: a
  // target-level permit does NOT authorize it. Verified on a live gateway —
  // `action in AgentCore::Action::"websearch"` produced
  // "ToolDenied: websearch___WebSearch was denied by the Cedar policy engine".
  const toolName = spec.policy?.tool ?? (spec.type === "websearch" ? "WebSearch" : undefined);
  if (!toolName) {
    return [
      "permit(",
      "  principal,",
      `  action in AgentCore::Action::"${name}",`,
      `  ${resource}`,
      ");",
    ].join("\n");
  }
  const restrict = spec.policy?.restrictTo ?? {};
  const conditions = Object.entries(restrict).map(
    ([arg, allowed]) =>
      `  context.input has ${arg} && ${JSON.stringify(allowed)}.contains(context.input.${arg})`
  );
  // The `when` clause is NOT optional. The policy engine REFUSES to create a
  // permit that is unconditioned for an unconstrained principal:
  //   "Overly Permissive: Policy Engine will allow every request for the
  //    specified principal (AgentCore::IamEntity), action (...) and resource ...
  //    combination"   (HandlerErrorCode: NotStabilized)
  // So when a tool declares no `policy.restrictTo`, still require the query
  // argument to be PRESENT: a real constraint (a call with no query is refused)
  // and enough to make the permit acceptable.
  if (conditions.length === 0) {
    conditions.push(`  context.input has ${spec.arg || keyDefault<string>("tool", "arg")}`);
  }
  const head = [
    "permit(",
    "  principal,",
    `  action == AgentCore::Action::"${name}___${toolName}",`,
    `  ${resource}`,
    ")",
  ].join("\n");
  return `${head} when {\n${conditions.join(" &&\n")}\n};`;
}

/**
 * Metadata keys S3 Vectors must NOT treat as filterable.
 *
 * It caps filterable metadata at 2048 bytes per vector, and both of these grow
 * with the document, so both must be excluded:
 *   AMAZON_BEDROCK_TEXT     — the chunk's own text.
 *   AMAZON_BEDROCK_METADATA — a JSON blob carrying `text`, `parentText`, the source
 *                             location and a document id.
 * Nothing filters on either (this framework's only filter is `doc_type`).
 */
/**
 * Retention for the log groups of the Lambdas this construct creates.
 *
 * Duplicated rather than imported from lib/orchestrator-stack.ts, which imports THIS
 * file — the reverse import would be circular. cdk/test/parity.test.ts asserts the two
 * stay equal.
 */
export const LOG_RETENTION = logs.RetentionDays.ONE_MONTH;

export const KB_NON_FILTERABLE = ["AMAZON_BEDROCK_TEXT", "AMAZON_BEDROCK_METADATA"];

/**
 * Digest of the vector-store properties that CANNOT be changed in place.
 *
 * S3 Vectors rejects an update to an index's `dimension` or `metadataConfiguration`,
 * so a change to either requires REPLACING the index — and CloudFormation refuses to
 * replace a resource that carries a fixed custom name:
 *   "CloudFormation cannot update a stack when a custom-named resource requires
 *    replacing. Rename ... and update the stack again."
 * Folding those properties into the NAME means any change to them yields a new name,
 * so the replacement just works. Keep in step with terraform/kb.tf.
 */
function kbStorageDigest(dims: number, nonFilterable: string[]): string {
  return crypto
    .createHash("sha256")
    .update(`${dims}|${[...nonFilterable].sort().join(",")}`)
    .digest("hex")
    .slice(0, 8);
}

/** The vector index name (see kbStorageDigest). */
export function kbIndexName(dims: number, nonFilterable: string[]): string {
  return `kb-index-${kbStorageDigest(dims, nonFilterable)}`;
}

/**
 * The Knowledge Base name, carrying the SAME digest as its index.
 *
 * The index name alone is not enough. A replaced index has a new ARN, the KB's
 * `storageConfiguration` is itself immutable, so the KB is replaced too — and it hit
 * the identical wall one level up, as a 409 rather than a rename hint:
 *   "KnowledgeBase with name multiagent-orchestrator-kb already exists.
 *    (Service: BedrockAgent, Status Code: 409)"
 * because CloudFormation creates the replacement before deleting the original.
 */
export function knowledgeBaseName(dashName: string, dims: number, nonFilterable: string[]): string {
  return `${dashName}-kb-${kbStorageDigest(dims, nonFilterable)}`;
}

/** Clip a string to `max` characters, for fields with a CloudFormation limit. */
/** workflow.json `toolSchema` as the Gateway's inline tool definitions (a Lambda's, or an
 *  MCP tool's used as the person). */
function inlineTools(tools: LambdaToolDef[]) {
  return tools.map((t) => ({
    name: t.name,
    description: truncate(t.description ?? t.name, 190),
    inputSchema: {
      type: "object",
      properties: Object.fromEntries(
        Object.entries(t.properties ?? {}).map(([p, def]) => [
          p,
          { type: def.type ?? "string", description: def.description ?? p },
        ])
      ),
      // JSON Schema puts `required` on the OBJECT as a list of names, while the config
      // (and the Terraform provider's flattened `property` block) put it on each
      // property. Collect them here so both paths accept the same JSON.
      required: Object.entries(t.properties ?? {})
        .filter(([, def]) => def.required)
        .map(([p]) => p),
    },
  }));
}

function truncate(s: string, max: number): string {
  return s.length <= max ? s : s.slice(0, max - 1).trimEnd() + "\u2026";
}

/** Relative paths of every file under `dir`, recursively (POSIX separators). */
function listFilesRecursive(dir: string, prefix = ""): string[] {
  if (!fs.existsSync(dir)) return [];
  const out: string[] = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const rel = prefix ? `${prefix}/${entry.name}` : entry.name;
    if (entry.isDirectory()) out.push(...listFilesRecursive(path.join(dir, entry.name), rel));
    else out.push(rel);
  }
  return out;
}

/** Stable hash of the corpus contents + doc_type mapping, for ingestion triggering. */
function hashCorpus(dir: string, files: string[]): string {
  const h = crypto.createHash("sha1");
  for (const f of [...files].sort()) {
    h.update(f);
    h.update(fs.readFileSync(path.join(dir, f)));
    h.update(f.split("/")[0]); // doc_type
  }
  return h.digest("hex").slice(0, 16);
}

/**
 * Make `resource` wait for a role's inline default policy, not just the role.
 * CDK only creates the implicit role -> resource dependency; several services
 * validate permissions at CREATE time and fail if the policy lands afterwards.
 */
function dependOnDefaultPolicy(resource: cdk.CfnResource, role: iam.Role): void {
  const policy = role.node.tryFindChild("DefaultPolicy") as iam.Policy | undefined;
  if (policy) resource.node.addDependency(policy);
}
