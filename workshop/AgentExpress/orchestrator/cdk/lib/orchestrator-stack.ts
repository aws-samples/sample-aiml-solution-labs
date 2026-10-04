import * as path from "path";
import * as fs from "fs";
import * as crypto from "crypto";
import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import {
  aws_dynamodb as dynamodb,
  aws_iam as iam,
  aws_lambda as lambda,
  aws_ecr_assets as ecrAssets,
  aws_s3 as s3,
  aws_s3_deployment as s3deploy,
  aws_cloudfront as cf,
  aws_cloudfront_origins as origins,
  aws_cognito as cognito,
  aws_logs as logs,
  aws_secretsmanager as secretsmanager,
  custom_resources as cr,
  aws_bedrockagentcore as agentcore,
} from "aws-cdk-lib";
import { CfnStage, HttpApi, HttpMethod } from "aws-cdk-lib/aws-apigatewayv2";
import { HttpLambdaIntegration } from "aws-cdk-lib/aws-apigatewayv2-integrations";
import { HttpJwtAuthorizer } from "aws-cdk-lib/aws-apigatewayv2-authorizers";
import { CustomPolicy, GATEWAY_PLACEHOLDER, ToolPlane, ToolSpec, ToolType } from "./tool-plane";
import { BuilderPlane } from "./builder-plane";
import * as vocab from "./vocabulary";
import { keyDefault, keyDefaultFor } from "./defaults";

/** Regions where the managed AgentCore Web Search connector is available. */
/**
 * Retention for the log groups of the Lambdas this stack creates.
 *
 * Every Lambda here declares its log group explicitly. Left implicit, Lambda creates
 * `/aws/lambda/<name>` itself with NEVER-EXPIRE retention and no stack ownership, so
 * `cdk destroy` leaves it behind accruing cost forever — a full destroy of this stack
 * was verified to orphan eight such groups. Keep in step with
 * terraform/variables.tf var.log_retention_days.
 */
const LOG_RETENTION = logs.RetentionDays.ONE_MONTH;

/** The BFF API's $default stage throttle (requests/s, burst). Keep in step with
 *  terraform/variables.tf var.api_throttle_rate / var.api_throttle_burst. */
const API_THROTTLE_RATE = 500;
const API_THROTTLE_BURST = 1000;
/** One JSON line per request: who, what, how it ended. No bodies, no headers. */
const API_ACCESS_LOG_FORMAT = {
  requestId: "$context.requestId",
  ip: "$context.identity.sourceIp",
  requestTime: "$context.requestTime",
  routeKey: "$context.routeKey",
  status: "$context.status",
  responseLength: "$context.responseLength",
  latency: "$context.responseLatency",
  integrationError: "$context.integrationErrorMessage",
  authorizerError: "$context.authorizer.error",
  sub: "$context.authorizer.claims.sub",
};

const WEB_SEARCH_REGIONS = vocab.WEB_SEARCH_REGIONS;
const TOOL_TYPES = vocab.TOOL_TYPES as ToolType[];
/** Property types the AgentCore inline tool schema accepts. */
const SCHEMA_TYPES = vocab.TOOL_SCHEMA_PROPERTY_TYPES;
const LAMBDA_ARN =
  /^arn:aws[a-z-]*:lambda:[a-z0-9-]+:[0-9]{12}:function:[a-zA-Z0-9-_]+(:[a-zA-Z0-9-_$]+)?$/;
/** The only value `source` accepts — see the ToolSpec docs for why it is not open. */
const BUILTIN_LAMBDA_SOURCE = vocab.BUILTIN_LAMBDA_SOURCE;

/**
 * Validate the `tools` block from workflow.json, mirroring the preconditions in
 * terraform/tools.tf so both IaC paths reject the same mistakes with the same
 * message — at synth, before anything is deployed.
 */
/**
 * What a `tools` KEY may be — narrower than either AWS constraint alone, because the key
 * builds TWO names with INCOMPATIBLE rules:
 *
 *   Gateway target   `<key>`          ^([0-9a-zA-Z][-]?){1,100}$   no underscores
 *   Cedar policy     `permit_<key>`   ^[A-Za-z][A-Za-z0-9_]*$      no hyphens
 *
 * Neither separator survives both, so the intersection is alphanumeric. camelCase is
 * already the house style in workflow.json, so this costs a customer nothing.
 *
 * Both constraints were found by DEPLOYING a foreign workflow, one after the other, each
 * after a clean `cdk synth` — CloudFormation refused the change set, the last possible
 * place to find out. Every tool in the shipped sample is one alphanumeric word.
 */
export const TARGET_NAME_RE = /^[A-Za-z][A-Za-z0-9]*$/;

export function validateTools(
  raw: Record<string, any>,
  agents: Record<string, any>,
  region: string,
  /**
   * The orchestrator/ directory, needed only to check that a `source` a tool names
   * really has a file in it. Optional because most callers are asking "is this config
   * self-consistent", which is answerable without a filesystem; the stack passes it, so
   * a real synth still gets the check.
   */
  orchRoot?: string
): Record<string, ToolSpec> {
  const tools: Record<string, ToolSpec> = {};
  for (const [name, t] of Object.entries(raw)) {
    // A TOOL KEY MUST BE ALPHANUMERIC. See TARGET_NAME_RE above for why it is that
    // narrow; checked here because otherwise CloudFormation is the first thing to say so.
    if (!TARGET_NAME_RE.test(name)) {
      const suggestion =
        name.replace(/[^A-Za-z0-9_-]/g, "").replace(/[_-](\w)/g, (_m, c) => c.toUpperCase()) ||
        "myTool";
      throw new Error(
        `workflow.json tools.${name} — a tool's key must match ${TARGET_NAME_RE.source} ` +
          `(letters and digits, starting with a letter). Try "${suggestion}", and update ` +
          `the \`tool\` field of any agent bound to it.\nWHY IT IS THIS NARROW: the key ` +
          `builds two AWS names whose rules contradict each other — the Gateway target is ` +
          `"${name}" and forbids underscores, while the Cedar policy is "permit_${name}" ` +
          `and forbids hyphens. camelCase matches the rest of workflow.json anyway. An ` +
          `AGENT id is different and may contain underscores, because it becomes part of a ` +
          `runtime name instead.`
      );
    }
    if (!TOOL_TYPES.includes(t?.type)) {
      throw new Error(`workflow.json tools.${name} needs "type" = ${TOOL_TYPES.join(" | ")}.`);
    }
    if (t.type === "mcp" && !t.endpoint) {
      throw new Error(
        `workflow.json tools.${name} has type="mcp" and so requires "endpoint" (your MCP server's Streamable HTTP URL).`
      );
    }
    // --- type=kb: where its documents come from ---------------------------
    // Mirrors bff/validate_build.py and the kb_source preconditions in terraform/tools.tf.
    if (t.type === "kb") {
      if (t.knowledgeBaseId !== undefined && !/^[0-9A-Z]{10}$/.test(String(t.knowledgeBaseId))) {
        throw new Error(`workflow.json tools.${name}.knowledgeBaseId must be a Knowledge Base id: 10 capital letters and digits.`);
      }
      if (t.s3Uri !== undefined && !/^s3:\/\/[a-z0-9][a-z0-9.-]{1,61}[a-z0-9](\/.*)?$/.test(String(t.s3Uri))) {
        throw new Error(`workflow.json tools.${name}.s3Uri must be s3://<bucket> or s3://<bucket>/<prefix>/.`);
      }
      if (t.kmsKeyArn !== undefined && !/^arn:aws[a-z-]*:kms:[a-z0-9-]+:[0-9]{12}:key\/[A-Za-z0-9-]+$/.test(String(t.kmsKeyArn))) {
        throw new Error(`workflow.json tools.${name}.kmsKeyArn must be a KMS key ARN, arn:aws:kms:<region>:<account>:key/<id>.`);
      }
      if (t.knowledgeBaseId) {
        const extra = ["s3Uri", "kmsKeyArn", "embeddingModel", "dimensions"].filter((k) => t[k] !== undefined);
        if (extra.length) {
          throw new Error(
            `workflow.json tools.${name} uses an existing Knowledge Base (knowledgeBaseId), so ` +
              `${extra.join(", ")} do not apply: that Knowledge Base already has its documents and embeddings.`
          );
        }
      } else if (t.kmsKeyArn && !t.s3Uri) {
        throw new Error(`workflow.json tools.${name}.kmsKeyArn only applies with s3Uri: it decrypts that bucket.`);
      }
    }
    // --- type=apigateway ------------------------------------------------
    // Mirrors bff/validate_build.py and the apigateway preconditions in terraform/tools.tf.
    if (t.type === "apigateway") {
      if (!/^[a-z0-9]{10}$/.test(String(t.restApiId ?? ""))) {
        throw new Error(`workflow.json tools.${name}.restApiId must be a REST API id: 10 lowercase letters and digits.`);
      }
      if (!/^[A-Za-z0-9_-]{1,128}$/.test(String(t.stage ?? ""))) {
        throw new Error(`workflow.json tools.${name}.stage must be the REST API stage to call, e.g. "prod".`);
      }
      const filters = Array.isArray(t.toolFilters) ? t.toolFilters : [];
      const methods = vocab.values("httpMethods");
      if (!filters.length || filters.some((f: any) => typeof f?.path !== "string" || !f.path.startsWith("/")
          || !Array.isArray(f.methods) || !f.methods.length || f.methods.some((m: string) => !methods.includes(m)))) {
        throw new Error(
          `workflow.json tools.${name}.toolFilters must be a non-empty list of {"path": "/...", "methods": [...]} ` +
            `with methods from ${methods.join(", ")}: only what they select becomes a tool.`
        );
      }
      if (t.auth === "oauth2") {
        throw new Error(`workflow.json tools.${name}: an API Gateway target takes auth apikey, sigv4 or none, not oauth2.`);
      }
    }
    // --- auth: oauth2 (client credentials) ------------------------------
    if (t.auth === "oauth2") {
      const oa = t.oauth ?? {};
      const urls = ["discoveryUrl", "tokenUrl"].filter((k) => oa[k]);
      if (!vocab.values("oauthToolTypes").includes(t.type) || !oa.clientId || urls.length !== 1
          || urls.some((k) => !/^https:\/\/\S+\.\S+/.test(String(oa[k])))) {
        throw new Error(
          `workflow.json tools.${name} has auth "oauth2", which needs type ${vocab.values("oauthToolTypes").join(" or ")} and ` +
            `"oauth": {"clientId", "scopes", and exactly one https "discoveryUrl" or "tokenUrl"}. ` +
            `The client secret goes in the build's secrets, never in workflow.json.`
        );
      }
    }
    // --- type=openapi ---------------------------------------------------
    // One of three keys has to say where the OpenAPI document is. Mirrors terraform/tools.tf.
    if (t.type === "openapi") {
      if ([t.schemaS3Uri, t.source, t.schema].filter(Boolean).length !== 1) {
        throw new Error(
          `workflow.json tools.${name} needs EXACTLY ONE of "schemaS3Uri" (an s3:// object you ` +
            `already host), "source" (a folder under orchestrator/app/tools/ holding ` +
            `openapi.json, which the framework uploads for you) or "schema" (the OpenAPI ` +
            `document itself, inline).`
        );
      }
      if (t.schema && (typeof t.schema !== "object" || !String(t.schema.openapi ?? "").startsWith("3"))) {
        throw new Error(`workflow.json tools.${name}.schema must be an OpenAPI 3 document.`);
      }
      // Unlike type="lambda", `source` here is NOT closed to a known list: uploading a
      // schema needs no permissions, so any folder name is legitimate and the only
      // meaningful check is that the file is really there. Caught at synth; otherwise
      // the asset is empty and the target fails when the Gateway loads it.
      if (t.source && orchRoot) {
        const schemaPath = path.join(orchRoot, "app", "tools", String(t.source), "openapi.json");
        if (!fs.existsSync(schemaPath)) {
          throw new Error(
            `workflow.json tools.${name} has "source": ${JSON.stringify(t.source)}, but ` +
              `orchestrator/app/tools/${t.source}/openapi.json does not exist. A type="openapi" ` +
              `tool's "source" names a folder under orchestrator/app/tools/ containing that file.`
          );
        }
      }
      if (t.schemaS3Uri && !/^s3:\/\/[a-z0-9.-]{3,63}\/.+/.test(String(t.schemaS3Uri))) {
        throw new Error(
          `workflow.json tools.${name} has type="openapi" and so requires "schemaS3Uri": a full ` +
            `s3:// URI including the object key, e.g. "s3://my-bucket/schemas/orders.json". ` +
            `Got: ${JSON.stringify(t.schemaS3Uri)}.`
        );
      }
    }
    // --- type=lambda ----------------------------------------------------
    // The Gateway will not discover a Lambda's tools for itself — there is no
    // tools/list to call — so the schema is declared. A missing or malformed one is
    // accepted by the API and then publishes a tool the agent cannot call, which
    // surfaces as an empty answer rather than an error.
    if (t.type === "lambda") {
      // Exactly one source of truth for WHICH function to call. Both set is
      // ambiguous; neither means there is nothing to register.
      if ([t.lambdaArn, t.source, t.code].filter((v) => v !== undefined && v !== null && v !== "").length !== 1) {
        throw new Error(
          `workflow.json tools.${name} needs EXACTLY ONE of "lambdaArn" (a function you already own — ` +
            `the framework only registers it), "source" (a function the framework ships and deploys) ` +
            `or "code" (a function written in the build, in app/tools/_code/${name}/).`
        );
      }
      if (t.code !== undefined && orchRoot) {
        const dir = path.join(orchRoot, "app", "tools", "_code", name);
        if (!fs.existsSync(path.join(dir, "handler.py"))) {
          throw new Error(
            `workflow.json tools.${name} is written in the build ("code"), but ${dir}/handler.py ` +
              `is not there. scaffold.py apply writes it from the build's bundle.`
          );
        }
      }
      // `source` names a FOLDER UNDER app/tools/, and is deliberately NOT a general
      // "deploy any directory" feature: a framework-deployed function needs an execution
      // role config cannot express.
      if (t.source && t.source !== BUILTIN_LAMBDA_SOURCE) {
        throw new Error(
          `workflow.json tools.${name} has "source": ${JSON.stringify(t.source)}. ` +
            `"source" names a folder under orchestrator/app/tools/, and the only one the ` +
            `framework ships is "${BUILTIN_LAMBDA_SOURCE}" ` +
            `(orchestrator/app/tools/${BUILTIN_LAMBDA_SOURCE}/). It is not a general ` +
            `"deploy any directory" option: the execution role is fixed at logs plus ` +
            `read-only on the PUBLIC AWS price list, which is right for that function and ` +
            `wrong for a connector that needs VPC config or a secret. To use a function of ` +
            `your own, deploy it yourself and set "lambdaArn" instead — the framework then ` +
            `registers it as a Gateway target and touches neither its code nor its role.`
        );
      }
      if (t.lambdaArn && !LAMBDA_ARN.test(String(t.lambdaArn))) {
        throw new Error(
          `workflow.json tools.${name} has type="lambda" and so requires "lambdaArn": the full ARN of an ` +
            `EXISTING function, e.g. "arn:aws:lambda:us-east-1:123456789012:function:my-tool" ` +
            `(an alias or version qualifier is allowed). Got: ${JSON.stringify(t.lambdaArn)}.`
        );
      }
      const schema: any[] = t.toolSchema ?? [];
      if (!schema.length) {
        throw new Error(
          `workflow.json tools.${name} has type="lambda" and so requires a non-empty "toolSchema" list. ` +
            `The Gateway cannot discover a Lambda's tools, so each must be declared: ` +
            `{ "name": "...", "description": "...", "properties": { "<arg>": { "type": "string", ` +
            `"required": true, "description": "..." } } }.`
        );
      }
      for (const s of schema) {
        if (!s?.name || !Object.keys(s.properties ?? {}).length) {
          throw new Error(
            `Every "toolSchema" entry in workflow.json tools.${name} needs a "name" and at least one ` +
              `entry in "properties" — a tool with no arguments has nothing for the agent's query to go into.`
          );
        }
        for (const [p, def] of Object.entries<any>(s.properties)) {
          const ty = String(def?.type ?? "string").toLowerCase();
          if (!SCHEMA_TYPES.includes(ty)) {
            throw new Error(
              `workflow.json tools.${name} toolSchema.${s.name}.properties.${p} has type "${def?.type}"; ` +
                `use one of ${SCHEMA_TYPES.join(", ")}.`
            );
          }
        }
      }
      const names = schema.map((s: any) => s.name);
      // Mirrors _select_tool in app/features/gateway/client.py, which REFUSES to
      // guess between several published tools.
      if (schema.length > 1 && !t.call) {
        throw new Error(
          `workflow.json tools.${name} declares ${schema.length} "toolSchema" entries (${names.join(", ")}), ` +
            `so it must also set "call" naming which one an agent invokes — the app refuses to guess.`
        );
      }
      if (t.call && !names.includes(t.call)) {
        throw new Error(
          `workflow.json tools.${name} has "call": "${t.call}", which is not one of its own "toolSchema" ` +
            `entries (${names.join(", ")}).`
        );
      }
      // `arg` decides which property the agent's query is written into, so it has
      // to exist — otherwise the query goes under a name the function ignores and
      // the tool answers as though none had been supplied.
      const called = t.call ? schema.find((s: any) => s.name === t.call) : schema[0];
      if (t.arg && called && !Object.keys(called.properties ?? {}).includes(t.arg)) {
        throw new Error(
          `workflow.json tools.${name} has "arg": "${t.arg}", which is not a property of "${called.name}" ` +
            `(${Object.keys(called.properties ?? {}).join(", ")}). That is where the agent's query is placed.`
        );
      }
    }
    if (t.type === "websearch" && t.connectorVersion) {
      // Fail loudly rather than accept-and-ignore: CloudFormation's
      // ConnectorSourceProperty has only connectorId, so a version pin cannot be
      // expressed on this path even though the API and Terraform support it.
      throw new Error(
        `workflow.json tools.${name} sets "connectorVersion", which CloudFormation cannot express ` +
          `(AWS::BedrockAgentCore::GatewayTarget's connector source accepts only connectorId). ` +
          `Remove it to track the latest connector, or deploy this stack with Terraform, which does ` +
          `support the pin.`
      );
    }
    if (t.listingMode && !vocab.TOOL_LISTING_MODES.includes(t.listingMode)) {
      throw new Error(
        `workflow.json tools.${name} has an invalid "listingMode" (${t.listingMode}); use "DEFAULT" or "DYNAMIC".`
      );
    }
    if (t.auth && !vocab.TOOL_AUTH_MODES.includes(t.auth)) {
      throw new Error(
        `workflow.json tools.${name} has an invalid "auth" (${t.auth}); use "none", "apikey", "sigv4" or "oauth2".`
      );
    }
    // An API key is only vaulted for the target kinds that GET a credential provider.
    // On any other type the key was accepted, stored nowhere, and the endpoint then
    // called unauthenticated — a silent security downgrade rather than an error.
    //
    // Terraform has refused this since the credential provider existed
    // (terraform/tools.tf, the `keyed_tools` precondition). This path did not, so the
    // same config synthesized cleanly under CDK and was rejected under Terraform. Found
    // by noticing that `API_KEY_TOOL_TYPES` was imported into cdk/lib/vocabulary.ts and
    // never read — an unused vocabulary binding is a good signal that one plane is
    // missing a check the others have.
    if (t.auth === "apikey" && !vocab.API_KEY_TOOL_TYPES.includes(t.type)) {
      throw new Error(
        `workflow.json tools.${name} has type="${t.type}" with auth="apikey", but only ` +
          `${vocab.API_KEY_TOOL_TYPES.map((x) => `"${x}"`).join(" and ")} targets get a ` +
          `credential provider. On any other type the key would be accepted, vaulted ` +
          `nowhere, and the endpoint called unauthenticated — so this is refused rather ` +
          `than silently downgraded.`
      );
    }
    tools[name] = t as ToolSpec;
  }

  const ofType = (ty: ToolType) => Object.entries(tools).filter(([, v]) => v.type === ty);

  const kbs = ofType("kb");
  if (kbs.length > 1) {
    throw new Error(
      'Only one workflow.json tools entry may have type="kb" (there is a single Knowledge Base per deployment). Scope agents to different document sets with `corpora` + each agent\'s `corpus` instead.'
    );
  }
  if (kbs.length === 1 && (kbs[0][1].corpora ?? []).length === 0) {
    throw new Error(
      `workflow.json tools.${kbs[0][0]} has type="kb" and so requires a non-empty "corpora" list naming the top-level folders under kb_docs/.`
    );
  }

  const ws = ofType("websearch");
  if (ws.length > 1) {
    throw new Error(
      'Only one workflow.json tools entry may have type="websearch" (the managed connector is a single target).'
    );
  }
  if (ws.length === 1 && !cdk.Token.isUnresolved(region) && !WEB_SEARCH_REGIONS.includes(region)) {
    throw new Error(
      `workflow.json tools.${ws[0][0]} has type="websearch", which is only available in ${WEB_SEARCH_REGIONS.join(", ")} (got ${region}). Remove the entry or deploy in one of those regions.`
    );
  }

  // Every agent's `tool` must resolve, or that agent silently runs with no data.
  // `tool` is one key or a list of them; every entry is checked.
  for (const [id, a] of Object.entries(agents)) {
    const raw = (a as any)?.tool;
    const refs: string[] = Array.isArray(raw) ? raw : raw ? [raw] : [];
    for (const ref of refs) {
      if (typeof ref !== "string" || !(ref in tools)) {
        throw new Error(
          `workflow.json agent "${id}" references tool "${ref}", which is not a key in the "tools" block. The label must match exactly.`
        );
      }
    }
  }
  return tools;
}

/**
 * Validate the AGENTS and STEPS, mirroring `terraform_data.workflow_validation`
 * in terraform/tools.tf so both IaC paths reject the same mistakes at synth.
 *
 * These used to go unchecked, which meant a bad id surfaced as a runtime
 * graph-build crash, an opaque deploy-time AgentCore error, or — worst — silently
 * empty retrievals with no error anywhere.
 */
/**
 * Bearer tokens to ship to the container: only for agents that are `runtime: "a2a"`
 * AND declare `auth: "bearer"`.
 *
 * Projected rather than passed whole, for the same reason the tools projection is: a
 * token for an agent that no longer exists, or for one using OAuth, would be shipped
 * to a running container for no reason. Throws when a bearer agent has no token,
 * because the alternative is a 401 from a service you do not control — a much harder
 * failure to read than a synth error.
 */
export function a2aTokenEnv(
  agents: Record<string, any>,
  tokens: Record<string, string>
): Record<string, string> {
  const out: Record<string, string> = {};
  const missing: string[] = [];
  for (const [id, a] of Object.entries<any>(agents)) {
    if (String(a.runtime ?? keyDefault<string>("agent", "runtime")) !== "a2a") continue;
    if (String(a.auth ?? keyDefault<string>("agent", "auth")).toLowerCase() !== "bearer") continue;
    const token = tokens[id];
    if (!token) missing.push(id);
    else out[id] = token;
  }
  if (missing.length) {
    throw new Error(
      `workflow.json agent(s) declare auth "bearer" but no token was supplied for them: ` +
        `${missing.join(", ")}. Tokens never go in workflow.json — pass them keyed by agent id: ` +
        `export A2A_TOKENS='{"${missing[0]}":"..."}'.`
    );
  }
  return out;
}

/**
 * Agents that want the stand-in A2A server, mapped to the skill each one is.
 *
 * Mirrors `local.a2a_lambda_agents` in terraform/a2a.tf. Empty means the function is
 * not deployed at all — point the agents at a real partner's `agentCard` instead and
 * none of that infrastructure exists, the same deal a `tools` entry gets from
 * `lambdaArn` vs `source`.
 */
export function a2aLambdaAgents(agents: Record<string, any>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [id, a] of Object.entries<any>(agents)) {
    if (String(a.runtime ?? keyDefault<string>("agent", "runtime")) === "a2a" && a.source === "a2a_lambda") {
      out[id] = String(a.skill ?? keyDefault<string>("agent", "skill")).toLowerCase();
    }
  }
  return out;
}

/** Skills the shipped stand-in publishes (a2a_lambda/handler.py SKILLS). */
export const A2A_LAMBDA_SKILLS = vocab.A2A_LAMBDA_SKILLS;

/** The only `source` values: stand-ins this repo ships and deploys. */
export const A2A_SOURCES = vocab.A2A_SOURCES;

export const RUNTIMES = vocab.RUNTIMES;
export const A2A_AUTH_MODES = vocab.A2A_AUTH_MODES;

/**
 * Validate each agent's `runtime` placement, mirroring registry.validate_runtimes
 * and the preconditions in terraform/tools.tf.
 *
 * `a2a` is the placement where the agent is NOT ours — somebody else's service,
 * reached at its Agent Card URL. That makes the wrong config here expensive: a
 * misspelled `runtime` falls through to "main" and dies on a missing module; an
 * `agentCard` on a non-a2a agent reads like a setting and does nothing; a non-https
 * card puts a bearer token on the wire in plaintext.
 */
export function validateRuntimes(agents: Record<string, any>): void {
  for (const [id, a] of Object.entries<any>(agents)) {
    const placement = String(a.runtime ?? keyDefault<string>("agent", "runtime"));
    if (!RUNTIMES.includes(placement)) {
      throw new Error(
        `workflow.json agent "${id}" has runtime "${placement}"; valid values are ` +
          `${RUNTIMES.join(", ")}. A misspelling would otherwise be treated as "main" and fail ` +
          `on a missing module under app/subagents/.`
      );
    }
    if (placement !== "a2a") {
      for (const key of ["agentCard", "auth"]) {
        if (a[key]) {
          throw new Error(
            `workflow.json agent "${id}" sets "${key}" but its runtime is "${placement}". ` +
              `${key} is only read for runtime "a2a" — an agent this workflow does not operate.`
          );
        }
      }
      // NOT checked here: that app/subagents/<id>/ exists. It was, briefly, and it made
      // ten honest tests of this function fail for the wrong reason — `validateRuntimes`
      // is a pure CONFIG validator, unit-tested against fabricated workflows whose agent
      // ids deliberately have no folders, and a filesystem check does not belong in it.
      // Folder existence is a property of the REPO rather than of a config, so it is
      // asserted by tests/test_subagents.py (which runs in CI and in a customer's own
      // `pytest`), and `registry.build_agent_module` reports it with the fix named if it
      // ever reaches a container.
      continue;
    }
    const card = String(a.agentCard ?? "");
    const source = String(a.source ?? "");
    if (Boolean(card) === Boolean(source)) {
      // Exactly one, for the same reason a `tools` entry needs exactly one of
      // lambdaArn/source: an external agent's URL is a stable value you write down,
      // whereas a framework-deployed one is generated by the deploy. Both set is
      // ambiguous; neither leaves the agent with nowhere to call.
      throw new Error(
        `workflow.json agent "${id}" has runtime "a2a" and needs EXACTLY ONE of "agentCard" ` +
          `(an agent that already exists — its base URL, or its card URL) or "source" (one of ` +
          `${A2A_SOURCES.join(", ")}, the stand-in this repo deploys for you, whose URL only ` +
          `exists after a deploy). Got ${card && source ? "both" : "neither"}.`
      );
    }
    if (source === "a2a_lambda" && String(a.auth ?? "").toLowerCase() !== "sigv4") {
      throw new Error(
        `workflow.json agent "${id}" has source "a2a_lambda" and needs auth "sigv4" (got ` +
          `"${a.auth ?? keyDefault<string>("agent", "auth")}"). That stand-in is deployed behind an AWS_IAM Function URL, so ` +
          `the request must be SigV4-signed with the orchestrator's own role — there is no ` +
          `token, by design.`
      );
    }
    if (source && !A2A_SOURCES.includes(source)) {
      throw new Error(
        `workflow.json agent "${id}" has source "${source}"; the only value is ` +
          `${A2A_SOURCES.join(", ")}. A framework-deployed agent needs infrastructure config ` +
          `cannot express, so this is not an arbitrary string.`
      );
    }
    if (card && !card.toLowerCase().startsWith("https://")) {
      throw new Error(
        `workflow.json agent "${id}" agentCard must be an https:// URL — the request may carry ` +
          `a bearer token. Got "${card}".`
      );
    }
    const auth = String(a.auth ?? keyDefault<string>("agent", "auth")).toLowerCase();
    if (!A2A_AUTH_MODES.includes(auth)) {
      throw new Error(
        `workflow.json agent "${id}" has auth "${a.auth}"; valid values are ` +
          `${A2A_AUTH_MODES.join(", ")}.`
      );
    }
    if (auth === "oauth2" && !(a.agentcore?.identity?.outbound ?? []).length) {
      throw new Error(
        `workflow.json agent "${id}" has auth "oauth2" but no agentcore.identity.outbound ` +
          `provider to get a token from. Add one, or use auth "bearer" with a token injected ` +
          `by the IaC.`
      );
    }
    const skill = String(a.skill ?? "");
    if (skill && !a.source) {
      throw new Error(
        `workflow.json agent "${id}" sets "skill" but points at an external "agentCard". ` +
          `\`skill\` selects among the skills of the stand-in this repo deploys (source), and a ` +
          `third party's agent advertises its own in its card.`
      );
    }
    if (a.source === "a2a_lambda" && skill && !A2A_LAMBDA_SKILLS.includes(skill.toLowerCase())) {
      throw new Error(
        `workflow.json agent "${id}" has skill "${skill}"; the stand-in A2A server publishes ` +
          `${A2A_LAMBDA_SKILLS.join(", ")} (see a2a_lambda/handler.py SKILLS). An unrecognised ` +
          `one would silently fall back to the default reviewer.`
      );
    }
    if (a.tool || a.corpus) {
      throw new Error(
        `workflow.json agent "${id}" has runtime "a2a" and also a tool/corpus binding. A remote ` +
          `agent reaches its own data sources; binding one here would imply this deployment's ` +
          `Gateway and Cedar policy govern those calls, which they cannot.`
      );
    }
    // `access` too: the BFF projection matches runtime "a2a" first and derives
    // "A2A · <host>", so a label set here is never read.
    const decorative = ["model", "temperature", "maxTokens", "access", "toolMode", "maxToolCalls"].filter((k) => k in a);
    if (decorative.length) {
      throw new Error(
        `workflow.json agent "${id}" has runtime "a2a" and also ${decorative.join(", ")}. Those ` +
          `configure this deployment's model call, which a remote agent does not make — it ` +
          `chooses its own model and owns its own token budget. Remove them.`
      );
    }
  }
}

/** The name a step is addressed by, and what a `branch` target names.
 *  Mirrors graph_builder._step_name and the same expression in terraform/tools.tf. */
export const stepName = (s: any, i: number): string => s.agent ?? s.gateId ?? `group${i}`;

const BRANCH_OPS = ["equals", "notEquals", "in", "contains", "exists", "gt", "gte", "lt", "lte"];
const BRANCH_NUM_OPS = ["gt", "gte", "lt", "lte"];
const BRANCH_TEXT_OPS = ["equals", "notEquals", "contains"];

/**
 * Validate every `branch` in `steps`, mirroring app/common/branching.validate_spec
 * and graph_builder.validate_branches.
 *
 * Each of these is a mistake that FAILS SILENTLY at runtime: a misspelled operator,
 * a rule with no comparison, or a target that names nothing all evaluate to "no
 * match", so the run quietly takes the default on every request and the branch looks
 * like it works. Catching them at synth is the difference between a typo and a
 * misrouted workload nobody notices.
 */
export function validateBranches(steps: any[]): void {
  const names = new Map<string, number>(steps.map((s, i) => [stepName(s, i), i]));
  if (steps.some((s) => s.branch) && names.size !== steps.length) {
    // A branch target names a step, so two steps sharing a name make a target
    // ambiguous — and the lookup above would silently resolve it to the later one.
    throw new Error(
      `app/workflow.json \`steps\` has duplicate step name(s), which makes a \`branch\` target ` +
        `ambiguous. Each step is named by its \`agent\` id or its \`gateId\`; give every step a ` +
        `distinct one. Names in order: ${steps.map((s, i) => stepName(s, i)).join(", ")}.`
    );
  }
  steps.forEach((step, i) => {
    const spec = step.branch;
    if (spec === undefined) return;
    const where = `app/workflow.json steps[${i}] (${stepName(step, i)})`;
    if (step.parallel) {
      throw new Error(
        `${where}: \`branch\` is not supported on a \`parallel\` step, because a group has no ` +
          `single agent whose output decides. Put the branch on a single-agent step, or on a ` +
          `\`sequence\` step (its LAST agent decides).`
      );
    }
    if (i === steps.length - 1) {
      throw new Error(
        `${where}: \`branch\` on the LAST step has nowhere to route. Use it on an earlier step, ` +
          `or drop it — "END" is already where the last step goes.`
      );
    }
    if (typeof spec !== "object" || Array.isArray(spec)) {
      throw new Error(`${where}: \`branch\` must be an object with \`when\` (and optionally \`default\`).`);
    }
    const unknown = Object.keys(spec).filter((k) => k !== "when" && k !== "default");
    if (unknown.length) {
      throw new Error(`${where}: \`branch\` has unknown key(s) ${unknown.join(", ")}. Valid keys: default, when.`);
    }
    if (spec.when === undefined && !String(spec.default ?? "").trim()) {
      throw new Error(
        `${where}: \`branch\` needs \`when\` (conditional rules), \`default\` (an unconditional ` +
          `jump to a later step), or both.`
      );
    }
    if (spec.when !== undefined && (!Array.isArray(spec.when) || !spec.when.length)) {
      throw new Error(
        `${where}: \`branch.when\` must be a non-empty list of rules. Omit it entirely for an ` +
          `unconditional jump via \`default\` alone.`
      );
    }
    (spec.when ?? []).forEach((rule: any, r: number) => {
      const at = `${where} branch.when[${r}]`;
      if (typeof rule !== "object" || rule === null || Array.isArray(rule)) {
        throw new Error(`${at}: each rule must be an object.`);
      }
      const bad = Object.keys(rule).filter((k) => k !== "field" && k !== "goto" && !BRANCH_OPS.includes(k));
      if (bad.length) {
        throw new Error(
          `${at}: unknown key(s) ${bad.join(", ")}. A rule is \`goto\`, an optional \`field\`, and ` +
            `one or more of ${BRANCH_OPS.join(", ")}.`
        );
      }
      if (!String(rule.goto ?? "").trim()) {
        throw new Error(`${at}: needs a \`goto\` naming the step to run when it matches (or "END").`);
      }
      if (!BRANCH_OPS.some((o) => o in rule)) {
        throw new Error(
          `${at}: has no comparison. Add one or more of ${BRANCH_OPS.join(", ")} — a rule with none ` +
            `would never match, so the branch would always take \`default\`.`
        );
      }
      if ("in" in rule && !Array.isArray(rule.in)) {
        throw new Error(`${at}: \`in\` takes a list of alternatives.`);
      }
      if ("exists" in rule && typeof rule.exists !== "boolean") {
        throw new Error(`${at}: \`exists\` takes true or false.`);
      }
      for (const op of BRANCH_NUM_OPS) {
        if (op in rule && !Number.isFinite(Number(rule[op]))) {
          throw new Error(`${at}: \`${op}\` takes a number, got ${JSON.stringify(rule[op])}.`);
        }
      }
      for (const op of BRANCH_TEXT_OPS) {
        if (op in rule && typeof rule[op] === "object" && rule[op] !== null) {
          throw new Error(
            `${at}: \`${op}\` takes a single value, not an object or list. Use \`in\` for a list of ` +
              `alternatives.`
          );
        }
      }
    });
    if ("default" in spec && !String(spec.default ?? "").trim()) {
      throw new Error(`${where}: \`branch.default\` is empty. Omit it to continue to the next step.`);
    }

    const targets = [
      ...(spec.when ?? []).map((r: any) => String(r.goto)),
      ...(spec.default ? [String(spec.default)] : []),
    ];
    for (const t of new Set(targets)) {
      if (t === "END") continue;
      const j = names.get(t);
      if (j === undefined) {
        throw new Error(
          `${where}: branch target "${t}" names no step. A target is "END", a single-agent step's ` +
            `\`agent\` id, or a group step's \`gateId\`. Known step names: ${[...names.keys()].join(", ")}.`
        );
      }
      if (j <= i) {
        throw new Error(
          `${where}: branch target "${t}" is step ${j}, at or before this one. Targets must be LATER ` +
            `steps — a backward edge is a cycle the run could not leave, and re-running earlier work ` +
            `is what a review gate's \`revise\` is for.`
        );
      }
    }
  });
}

/**
 * workflow.json orchestrator.policy.custom, checked for what would fail the deploy: a
 * name that makes a valid, unique AgentCore policy name, and a statement that is a
 * permit/forbid naming the build's Gateway by its placeholder. bff/cedar.py and
 * web/src/builder/cedar.ts check the rest before a build gets here. Mirrors the
 * custom-policy preconditions in terraform/policy.tf.
 */
export function customPoliciesOf(workflow: any): CustomPolicy[] {
  const custom = workflow?.orchestrator?.policy?.custom;
  if (custom === undefined) return namedPoliciesOf(workflow);
  if (!Array.isArray(custom)) throw new Error("orchestrator.policy.custom must be a list of {name, description, statement}");
  const seen = new Set<string>();
  return custom.map((c: any, i: number) => {
    const p = `orchestrator.policy.custom[${i}]`;
    if (typeof c?.name !== "string" || !/^[A-Za-z][A-Za-z0-9]{0,31}$/.test(c.name)) {
      throw new Error(`${p}.name must be a letter, then letters and digits (32 at most)`);
    }
    if (seen.has(c.name)) throw new Error(`${p}.name "${c.name}" is used twice`);
    seen.add(c.name);
    const s = c.statement;
    if (typeof s !== "string" || !/^\s*(@|permit\s*\(|forbid\s*\()/.test(s.replace(/^\s*\/\/[^\n]*\n/gm, ""))
      || !s.includes(`AgentCore::Gateway::"${GATEWAY_PLACEHOLDER}"`)) {
      throw new Error(`${p}.statement must be one permit(...) or forbid(...) whose resource is `
        + `AgentCore::Gateway::"${GATEWAY_PLACEHOLDER}"`);
    }
    return { name: c.name, statement: s, ...(typeof c.description === "string" ? { description: c.description } : {}) };
  }).concat(namedPoliciesOf(workflow));
}

/** A policy template's name on one tool: account-unique per pair, within 32. Mirrors
 *  local.named_policy_list in terraform/policy.tf. */
export function namedPolicyName(policy: string, tool: string): string {
  return `${policy.slice(0, 20)}${crypto.createHash("sha1").update(`${policy}.${tool}`).digest("hex").slice(0, 8)}`;
}

/** workflow.json `policies`: one written for "{{tool}}" once per tool that attaches it
 *  (tools.<key>.policies), one written with real tool keys once, as written. */
/** An agent whose model reads earlier agents' images (workflow.json `vision.from`). */
export function readsImages(a: any): boolean {
  return Array.isArray(a?.vision?.from) && a.vision.from.length > 0;
}

export function namedPoliciesOf(workflow: any): CustomPolicy[] {
  const defs: Record<string, any> = workflow?.policies ?? {};
  const out: CustomPolicy[] = Object.entries(defs)
    .filter(([, p]) => typeof p?.statement === "string" && !p.statement.includes("{{tool}}"))
    .map(([n, p]) => ({ name: n, statement: p.statement, description: String(p.description ?? "") }));
  for (const [k, t] of Object.entries<any>(workflow?.tools ?? {})) {
    for (const n of Array.isArray(t?.policies) ? t.policies : []) {
      const st = defs[n]?.statement;
      if (typeof st !== "string" || !st.includes("{{tool}}")) continue;
      out.push({ name: namedPolicyName(n, k), statement: st.split("{{tool}}").join(k),
        description: `${n} on ${k}: ${String(defs[n].description ?? "")}` });
    }
  }
  return out;
}

/** The identities some agent reaches directly (agentcore.identity.outbound names from
 *  `identities`): each gets its own credential provider. Mirrors
 *  local.agent_identity_names in terraform/identities.tf. */
export function agentIdentitiesOf(workflow: any): string[] {
  const defs = workflow?.identities ?? {};
  return [...new Set(Object.values<any>(workflow?.agents ?? {}).flatMap((a) =>
    (a?.agentcore?.identity?.outbound ?? []).filter((n: string) => n in defs)))];
}

/** Tools with `identity` take its auth: an oauth2 identity's client (auth "oauth2" +
 *  `oauth`), an apikey identity's key (auth "apikey"), with the identity's secret as the
 *  tool's key. Mirrors the identity fallbacks in terraform/tools.tf. */
export function inheritIdentities(tools: Record<string, any>, identities: Record<string, any>,
  identitySecrets: Record<string, string>, toolApiKeys: Record<string, string>):
  { tools: Record<string, any>; toolApiKeys: Record<string, string> } {
  const out: Record<string, any> = {};
  const keys = { ...toolApiKeys };
  for (const [k, t] of Object.entries<any>(tools)) {
    const id = t?.identity ? identities[t.identity] : undefined;
    if (!id) { out[k] = t; continue; }
    const { identity: _i, ...rest } = t;
    if (id.type === "oauth2") {
      out[k] = { ...rest, auth: "oauth2", oauth: { clientId: id.clientId, scopes: id.scopes ?? [],
        ...(id.discoveryUrl ? { discoveryUrl: id.discoveryUrl } : {}), ...(id.tokenUrl ? { tokenUrl: id.tokenUrl } : {}),
        ...(id.issuer ? { issuer: id.issuer } : {}) } };
    } else {
      out[k] = { ...rest, auth: "apikey" };
    }
    if (identitySecrets[t.identity]) keys[k] = identitySecrets[t.identity];
    else delete keys[k];
  }
  return { tools: out, toolApiKeys: keys };
}

/** Which agents get their own Gateway client (orchestrator.gatewayIdentity "perAgent"):
 *  every local agent with a tool. Mirrors local.agent_client_ids in terraform/cognito.tf. */
export function agentClientIdsOf(workflow: any): string[] {
  if (String(workflow?.orchestrator?.gatewayIdentity ?? keyDefault<string>("orchestrator", "gatewayIdentity")) !== "perAgent") return [];
  return Object.entries<any>(workflow?.agents ?? {})
    .filter(([, a]) => (a?.runtime ?? keyDefault<string>("agent", "runtime")) !== "a2a" && [a?.tool ?? []].flat().filter(Boolean).length)
    .map(([id]) => id);
}

export function validateWorkflow(workflow: any, orchRoot: string, agentName: string, idp: string): void {
  const agents: Record<string, any> = workflow.agents ?? {};
  const steps: any[] = workflow.steps ?? [];
  if (!steps.length) throw new Error("app/workflow.json `steps` is empty — there is no pipeline to run.");

  const stepIds = [
    ...new Set(steps.flatMap((s) => s.parallel ?? s.sequence ?? [s.agent])),
  ].filter(Boolean) as string[];

  const missing = stepIds.filter((id) => !(id in agents));
  if (missing.length) {
    throw new Error(
      `app/workflow.json \`steps\` names agent id(s) that are not in \`agents\`: ${missing.join(", ")}.`
    );
  }
  const orphans = Object.keys(agents).filter((id) => !stepIds.includes(id));
  if (orphans.length) {
    throw new Error(
      `app/workflow.json declares agent(s) that no \`steps\` entry runs: ${orphans.join(", ")}. ` +
        `Add them to \`steps\` or remove them.`
    );
  }

  validateBranches(steps);
  validateRuntimes(agents);

  // The id becomes part of the AgentCore Runtime name, which only accepts
  // [a-zA-Z][a-zA-Z0-9_]* — a hyphen fails at DEPLOY time with an opaque error.
  const badIds = Object.keys(agents).filter((id) => !/^[a-zA-Z][a-zA-Z0-9_]*$/.test(id));
  if (badIds.length) {
    throw new Error(
      `Agent ids in app/workflow.json must match ^[a-zA-Z][a-zA-Z0-9_]*$ (letters, digits and ` +
        `underscores; no hyphens) because the id becomes part of the AgentCore Runtime name. ` +
        `Offending: ${badIds.join(", ")}.`
    );
  }
  const tooLong = Object.entries(agents)
    .filter(([, a]) => (a.runtime ?? keyDefault<string>("agent", "runtime")) === "dedicated")
    .map(([id]) => `${agentName}_${id}`)
    .filter((n) => n.length > 48);
  if (tooLong.length) {
    throw new Error(
      `A dedicated agent's AgentCore Runtime name is "<agentName>_<agent id>" and must be 48 ` +
        `characters or fewer. Too long: ${tooLong.join(", ")}. Shorten agentName or the agent id.`
    );
  }
  // A dedicated agent also gets its OWN execution role now, and that name is the TIGHTER
  // of the two limits: the role carries an 18-character prefix where the runtime name
  // carries none, so for a 23-character agentName an id of 23-24 characters passes the
  // check above and fails this one. Both are reported from here so a customer gets the
  // real constraint at validation rather than part of it now and the rest at synth.
  //
  // Checked, never truncated: a shortened role name can collide with another agent's, and
  // two runtimes sharing one role is exactly what per-agent roles exist to prevent.
  const roleTooLong = Object.entries(agents)
    .filter(([, a]) => (a.runtime ?? keyDefault<string>("agent", "runtime")) === "dedicated")
    .map(([id]) => `AgentCoreSubagent-${agentName}-${id}`)
    .filter((n) => n.length > 64);
  if (roleTooLong.length) {
    throw new Error(
      `A dedicated agent's execution role is named "AgentCoreSubagent-<agentName>-<agent id>" ` +
        `and must be 64 characters or fewer, which is IAM's limit. Too long: ` +
        `${roleTooLong.join(", ")}. Shorten agentName or the agent id.`
    );
  }

  // Corpora must be real top-level folders under kb_docs/, or the generated Cedar
  // permit filters on a doc_type no chunk carries and retrieval returns NOTHING
  // with no error at all.
  const kbEntry = Object.entries<any>(workflow.tools ?? {}).find(([, t]) => t?.type === "kb");
  const corpora: string[] = kbEntry ? kbEntry[1].corpora ?? [] : [];
  const kbDir = path.join(orchRoot, "kb_docs");
  // Only for UPLOADED documents: with `s3Uri` or `knowledgeBaseId` the corpora are values
  // in the customer's own metadata, which cannot be checked from here.
  const uploaded = kbEntry ? !kbEntry[1].s3Uri && !kbEntry[1].knowledgeBaseId : false;
  if (uploaded && corpora.length && fs.existsSync(kbDir)) {
    const entries = fs.readdirSync(kbDir, { withFileTypes: true });
    const folders = entries.filter((e) => e.isDirectory()).map((e) => e.name);
    const rootFiles = entries
      .filter((e) => e.isFile() && e.name !== ".DS_Store")
      .map((e) => e.name);
    const unknown = corpora.filter((c) => !folders.includes(c));
    if (unknown.length) {
      throw new Error(
        `tools.${kbEntry![0]}.corpora names folder(s) that do not exist under orchestrator/kb_docs/: ` +
          `${unknown.join(", ")}. Found: ${folders.join(", ") || "(none)"}.`
      );
    }
    if (rootFiles.length) {
      throw new Error(
        `These files sit at the root of orchestrator/kb_docs/: ${rootFiles.join(", ")}. Each document ` +
          `must live in a top-level FOLDER, because that folder name becomes its corpus (doc_type).`
      );
    }
  }

  // An agent's `corpus` must be one of the declared corpora.
  for (const [id, a] of Object.entries<any>(agents)) {
    if (a.corpus && !corpora.includes(a.corpus)) {
      throw new Error(
        `workflow.json agent "${id}" has corpus "${a.corpus}", which is not in ` +
          `tools.<kb>.corpora (${corpora.join(", ") || "none declared"}).`
      );
    }
  }

  // --- RBAC (authorization) ------------------------------------------------
  const authzActions: Record<string, string[]> = workflow.authorization?.actions ?? {};
  const restricted = Object.keys(authzActions);
  if (restricted.length && idp === "none") {
    // No authorizer means no claims, means no groups: every rule would evaluate
    // against an empty group set and DENY, locking the UI out of its own buttons.
    throw new Error(
      `app/workflow.json restricts ${restricted.join(", ")} in \`authorization.actions\`, but ` +
        `idp = "none" deploys the API with no authorizer, so there are no JWT claims to ` +
        `authorize against and every one of those actions would be denied. Use -c idp=cognito ` +
        `or -c idp=auth0, or remove \`authorization.actions\`.`
    );
  }
  // Mirrors ACTIONS in bff/authz.py. A typo'd key looks like a restriction but
  // gates nothing, leaving the real action wide open — so reject it at synth.
  const knownActions = vocab.AUTHORIZATION_ACTIONS;
  const unknownActions = restricted.filter((a) => !knownActions.includes(a));
  if (unknownActions.length) {
    throw new Error(
      `app/workflow.json \`authorization.actions\` names unknown action(s): ` +
        `${unknownActions.join(", ")}. Valid actions are ${knownActions.join(", ")} (see ACTIONS ` +
        `in bff/authz.py). An unrecognised key gates nothing.`
    );
  }
}

/** Every distinct group named anywhere in `authorization.actions`. */
/** The long-term memory strategies a workflow needs, as AWS::BedrockAgentCore::Memory
 *  MemoryStrategies. semantic and summary always (as before); userPreference and
 *  episodic only when some agent names them, and one custom strategy per agent that
 *  defines `memory.custom` — each strategy is a model call per stored turn. Namespaces
 *  match app/common/context.py _namespace_for. Mirrors local.memory_strategies in
 *  terraform/main.tf. */
export function memoryStrategiesFor(workflow: any, defaultModel: string, region?: string): { strategies: any[]; custom: boolean } {
  const agents = Object.entries<any>(workflow.agents ?? {});
  const named = new Set<string>(agents.flatMap(([, a]) => {
    const lt = a?.agentcore?.memory?.longTerm;
    return Array.isArray(lt) ? lt : lt ? ["semantic"] : [];
  }));
  const strategies: any[] = [
    { SemanticMemoryStrategy: { Name: "insights", Namespaces: ["insights/{actorId}"] } },
    // Summarization is inherently per-session, so AgentCore REQUIRES
    // {sessionId} here (unlike semantic, which is actor-scoped).
    { SummaryMemoryStrategy: { Name: "summary", Namespaces: ["summary/{actorId}/{sessionId}"] } },
  ];
  if (named.has("userPreference")) {
    strategies.push({ UserPreferenceMemoryStrategy: { Name: "preferences", Namespaces: ["preferences/{actorId}"] } });
  }
  if (named.has("episodic")) {
    strategies.push({ EpisodicMemoryStrategy: {
      Name: "episodes", Namespaces: ["episodes/{actorId}/{sessionId}"],
      ReflectionConfiguration: { Namespaces: ["episodes/{actorId}"] } } });
  }
  const customs = agents.filter(([, a]) => a?.agentcore?.memory?.custom);
  for (const [id, a] of customs) {
    const c = a.agentcore.memory.custom;
    const model = vocab.regionalModelId(String(c.model ?? workflow.orchestrator?.defaultModel ?? defaultModel), region);
    const step = { AppendToPrompt: String(c.instructions), ModelId: model };
    const override = c.base === "summary" ? { SummaryOverride: { Consolidation: step } }
      : c.base === "userPreference" ? { UserPreferenceOverride: { Extraction: step, Consolidation: step } }
      : { SemanticOverride: { Extraction: step, Consolidation: step } };
    strategies.push({ CustomMemoryStrategy: {
      Name: `custom_${id}`.slice(0, 48), Namespaces: [`custom-${id}/{actorId}`], Configuration: override } });
  }
  return { strategies, custom: customs.length > 0 };
}

/** Every custom evaluator a workflow defines: key "<agent>.<name>", and what
 *  AWS::BedrockAgentCore::Evaluator needs. Mirrors local.custom_evaluators in
 *  terraform/evaluations.tf. */
export function customEvaluatorsOf(workflow: any, agentName: string, defaultModel: string, region?: string): {
  key: string; evaluatorName: string; instructions: string; model: string;
  scale: { value: number; label: string; definition: string }[];
}[] {
  const scale = vocab.extra<any[]>("builtinEvaluators", "customScale") ?? [];
  const tail = vocab.extra<string>("builtinEvaluators", "customPlaceholders") ?? "";
  const out = [];
  for (const [id, a] of Object.entries<any>(workflow.agents ?? {})) {
    for (const c of a?.agentcore?.evaluations?.custom ?? []) {
      const text = String(c.instructions ?? "");
      const digest = crypto.createHash("sha256").update(`${id}.${c.name}`).digest("hex").slice(0, 6);
      out.push({
        key: `${id}.${c.name}`,
        // Unique per region: this deployment's name, a digest of agent + name, the name.
        evaluatorName: `${agentName}_${digest}_${c.name}`.slice(0, 48),
        instructions: /\{(context|assistant_turn)\}/.test(text) ? text : text + tail,
        model: vocab.regionalModelId(String(c.model ?? workflow.orchestrator?.defaultModel ?? defaultModel), region),
        scale: Array.isArray(c.scale) && c.scale.length ? c.scale : scale,
      });
    }
  }
  // The build's own evaluators (`evaluators`), keyed "*.<name>": one each, whichever
  // agents run them (sharedEvaluatorUses).
  for (const [n, c] of Object.entries<any>(workflow.evaluators ?? {})) {
    const text = String(c?.instructions ?? "");
    const digest = crypto.createHash("sha256").update(`*.${n}`).digest("hex").slice(0, 6);
    out.push({
      key: `*.${n}`,
      evaluatorName: `${agentName}_${digest}_${n}`.slice(0, 48),
      instructions: /\{(context|assistant_turn)\}/.test(text) ? text : text + tail,
      model: vocab.regionalModelId(String(c?.model ?? workflow.orchestrator?.defaultModel ?? defaultModel), region),
      scale: Array.isArray(c?.scale) && c.scale.length ? c.scale : scale,
    });
  }
  return out;
}

/** "<agent>.<name>" -> name, for every agent that runs one of the build's own evaluators.
 *  Mirrors local.shared_evaluator_uses in terraform/evaluations.tf. */
export function sharedEvaluatorUses(workflow: any): Record<string, string> {
  const defs = workflow.evaluators ?? {};
  const out: Record<string, string> = {};
  for (const [id, a] of Object.entries<any>(workflow.agents ?? {})) {
    const own = (a?.agentcore?.evaluations?.custom ?? []).map((c: any) => c?.name);
    for (const e of a?.agentcore?.evaluations?.evaluators ?? []) {
      const n = String(e).startsWith("Custom.") ? String(e).slice(7) : "";
      if (n && n in defs && !own.includes(n)) out[`${id}.${n}`] = n;
    }
  }
  return out;
}

export function authzGroups(workflow: any): string[] {
  const actions: Record<string, string[]> = workflow.authorization?.actions ?? {};
  return [...new Set(Object.values(actions).flat())].sort();
}

/** Actions that reach EVERY user's data. The group self sign-up puts new users in must
 *  hold none of them, so an admin is only ever made from the backend. Mirrors
 *  local.privileged_actions in terraform/signup.tf. */
export const PRIVILEGED_ACTIONS = ["admin", "audit", "insights"];

/** The privileged actions `group` is granted in workflow.json. */
export function privilegedGrants(workflow: any, group: string): string[] {
  const actions: Record<string, string[]> = workflow.authorization?.actions ?? {};
  return PRIVILEGED_ACTIONS.filter((a) => (actions[a] ?? []).includes(group));
}

export interface OrchestratorStackProps extends cdk.StackProps {
  agentName: string;
  modelId: string;
  memoryEventExpiryDays: number;
  /** "cognito" | "auth0" | "none" — see terraform/identity.tf for the same switch. */
  idp: string;
  cognitoUserPoolId: string;
  cognitoClientId: string;
  cognitoDomainPrefix: string;
  auth0Domain: string;
  auth0ClientId: string;
  /** Create the AgentCore Gateway + Knowledge Base + Cedar policy (live MCP/RAG). */
  enableGateway: boolean;
  createCognito: boolean;
  /**
   * Let anyone register on the Hosted UI (email verified), and put each new user in
   * `selfSignUpGroup` via a post-confirmation trigger. Off by default: the UI is on a
   * public URL, and every user can spend the account's Bedrock budget. Only with
   * createCognito. Mirrors terraform `cognito.self_signup`.
   */
  selfSignUp?: boolean;
  /** The group self-registered users join. Must be named in workflow.json authorization. */
  selfSignUpGroup?: string;
  /** Bring-your-own M2M client id (unused when Cognito is created here). */
  gatewayClientId: string;
  /** Bring-your-own M2M secret, from $GATEWAY_CLIENT_SECRET. Never a context key. */
  gatewayClientSecret: string;
  /** OAuth2 scope (Cognito) or API identifier (Auth0) for the M2M token. */
  gatewayAudience: string;
  /**
   * API keys for tools that need one, keyed by the workflow.json tool name.
   * From $TOOL_API_KEYS, never a context key (cdk.json is committed).
   */
  toolApiKeys: Record<string, string>;
  /** Each workflow.json identity's secret, from $IDENTITY_SECRETS. Never a context key. */
  identitySecrets?: Record<string, string>;
  /**
   * Bearer tokens for `runtime: "a2a"` agents, keyed by the workflow.json agent id.
   * From $A2A_TOKENS, never a context key (cdk.json is committed). These are
   * credentials for an agent this deployment does not operate.
   */
  a2aTokens: Record<string, string>;
  /** Percentage of spans indexed by CloudWatch Transaction Search (0-100). */
  transactionSearchIndexingPercentage: number;
  /**
   * The parsed workflow. Defaults to `app/workflow.json`, which is what every real
   * deployment uses; supplied only by tests that need a topology the shipped file
   * does not contain.
   */
  workflow?: any;
  /**
   * Deploy the Builder's control plane: the builds store and the project that deploys
   * each build as its own stack (lib/builder-plane.ts). On by default; the deploy
   * runner turns it OFF for the stacks it creates, because a build is a workflow, not
   * another console. `-c builder=false`.
   */
  builder?: boolean;
  /**
   * "builder": this console is a control plane — people design, build and deploy here,
   * and each build's runs, observability and assistant live in that build's own app.
   * The BFF refuses every run route and the UI shows no Runs or Observability. Needs the
   * Builder. "app" (the default): a workflow's own app, with the Builder beside it.
   * `-c consoleMode=builder`. Mirrors var.console_mode in terraform/builder.tf.
   */
  consoleMode?: string;
  /** Buckets, or bucket/prefix, the AgentExpress Assistant may read files from when a
   *  message names an s3:// path. `-c designerS3Buckets=a,b/prefix`. Mirrors
   *  var.designer_s3_buckets in terraform/builder.tf. */
  designerS3Buckets?: string[];
  /** The AgentExpress Assistant's model (`-c designerModel`) and its conversation-summary
   *  model (`-c designerSummaryModel`). Empty = bff/designer.py's defaults. Mirrors
   *  Terraform's designer_model / designer_summary_model. */
  designerModel?: string;
  designerSummaryModel?: string;
  /** The Assistant's reasoning effort: medium (the default) | low | high | model. Mirrors
   *  Terraform's designer_effort. */
  designerEffort?: string;
}

// Repo layout: this file is orchestrator/cdk/lib/, so orchestrator/ is two levels up.
const ORCH_ROOT = path.join(__dirname, "..", "..");

/**
 * Full CDK equivalent of the Terraform deployment:
 *
 *   * the orchestrator AgentCore Runtime + one dedicated runtime per `dedicated` agent
 *   * AgentCore Memory ×2 (LangGraph checkpointer + long-term semantic/summary)
 *   * a Bedrock Guardrail, DynamoDB status/events/telemetry/insights stores
 *   * the BFF Lambda + HTTP API (JWT authorizer for the configured IdP)
 *   * the S3 + CloudFront UI
 *   * CloudWatch Transaction Search (so agent spans reach the aws/spans group)
 *   * with `enableGateway`: the AgentCore **Gateway**, a Bedrock **Knowledge Base**
 *     on S3 Vectors, the KB retrieve Lambda, and the **Cedar policy engine**
 *     (see ToolPlane in lib/tool-plane.ts)
 *
 * `enableGateway=false` is the same reduced mode as Terraform's
 * `enable_gateway = false`: no tool plane is provisioned, so any agent with a
 * `tool` raises ToolUnavailable rather than inventing evidence.
 */
export class OrchestratorStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: OrchestratorStackProps) {
    super(scope, id, props);

    const { agentName, memoryEventExpiryDays } = props;
    // ---- What this deployment is --------------------------------------------
    // "builder": a control-plane console. It designs, builds and deploys, and runs no
    // workflow of its own: no image, runtime, memory, guardrail, Gateway, Knowledge Base,
    // run tables or Transaction Search — each build's app has its own. "app" (the
    // default): this workflow's own app, with the Builder beside it unless builder=false.
    const consoleMode = props.consoleMode ?? "app";
    if (!["app", "builder"].includes(consoleMode)) {
      throw new Error(`consoleMode must be "app" or "builder", not "${consoleMode}"`);
    }
    if (consoleMode === "builder" && props.builder === false) {
      throw new Error("consoleMode=builder needs the Builder: drop -c builder=false");
    }
    const workflowPlane = consoleMode !== "builder";
    const enableGateway = props.enableGateway && workflowPlane;

    // ---- workflow.json: the single source of truth for agents + topology ----
    // Overridable so the stack is a pure function of its props, like every other
    // input here. Tests use it to synthesize topologies the shipped file does not
    // contain — chiefly to prove a NEGATIVE, that a `runtime: "a2a"` agent
    // provisions no compute of its own.
    const workflow =
      props.workflow ??
      JSON.parse(fs.readFileSync(path.join(ORCH_ROOT, "app", "workflow.json"), "utf8"));
    const agents: Record<string, any> = workflow.agents;
    // The deployment's default model: an explicit `-c modelId`, else this workflow's
    // `orchestrator.defaultModel`, else the framework default from app/defaults.json — in
    // this region's inference-profile geography. It reaches every plane as
    // BEDROCK_MODEL_ID / MODEL_ID, which the app reads FIRST, so it has to be the same
    // answer the app would reach on its own. It used to be the cdk.json literal, which
    // silently overrode the workflow's defaultModel on every deployment.
    const region = cdk.Stack.of(this).region;
    const modelId = vocab.regionalModelId(
      props.modelId || workflow.orchestrator?.defaultModel || keyDefault<string>("orchestrator", "defaultModel"),
      cdk.Token.isUnresolved(region) ? undefined : region
    );
    const dedicatedIds = Object.keys(agents).filter(
      (id) => (agents[id].runtime ?? keyDefault<string>("agent", "runtime")) === "dedicated"
    );

    // ---- Identity provider ----------------------------------------------------
    // Mirrors terraform/identity.tf: props.idp selects the provider, and the rest
    // of the stack consumes only the provider-agnostic values derived here
    // (authEnabled / jwtIssuer / jwtAudience / authConfig for the SPA).
    const isCognito = props.idp === "cognito";
    const isAuth0 = props.idp === "auth0";

    let cognitoUserPoolId = "";
    let cognitoClientId = "";
    let cognitoDomainPrefix = "";
    // The SPA client is created BEFORE the CloudFront distribution exists, so its
    // callback URLs can only be wired once we know the distribution domain. Held
    // here and completed further down (search: wire the Hosted UI callbacks).
    let spaClientL1: cognito.CfnUserPoolClient | undefined;
    // Machine-to-machine identity for agent -> Gateway. Created here only when
    // we're also creating the pool AND the Gateway is on (Terraform's
    // local.create_m2m); otherwise it's bring-your-own via gatewayIdentity*.
    let m2mClientId = "";
    let m2mClientSecret = "";
    const agentClients: Record<string, { id: string; secret: string }> = {};
    if (agentClientIdsOf(workflow).length && !(isCognito && props.createCognito && enableGateway)) {
      throw new Error('orchestrator.gatewayIdentity "perAgent" needs the Cognito pool this deployment creates '
        + "(-c idp=cognito -c createCognito=true) and the Gateway: with your own IdP there is no pool to create "
        + 'a client per agent in. Use "shared".');
    }
    let m2mAudience = "";

    if (isCognito && props.createCognito) {
      const pool = new cognito.UserPool(this, "UserPool", {
        userPoolName: `${agentName}-users`,
        // Admin-only user creation, matching terraform/cognito.tf
        // (allow_admin_create_user_only = true). This was `true`, which diverged
        // from the Terraform path and left OPEN self-registration on a public
        // CloudFront URL — anyone who found the URL could register and spend the
        // account's Bedrock budget. Create users deliberately:
        //   aws cognito-idp admin-create-user --user-pool-id <id> \
        //       --username you@example.com --message-action SUPPRESS \
        //       --user-attributes Name=email,Value=you@example.com Name=email_verified,Value=true
        //
        // `-c selfSignUp=true` opens it deliberately: email-verified registration, and
        // the new user joins `selfSignUpGroup` (post-confirmation trigger below). Every
        // build and run is private to its creator, so that group reaches only their own.
        selfSignUpEnabled: Boolean(props.selfSignUp),
        signInAliases: { email: true },
        autoVerify: { email: true },
        passwordPolicy: {
          minLength: 12,
          requireLowercase: true,
          requireUppercase: true,
          requireDigits: true,
          requireSymbols: true,
        },
        // Users may enrol an authenticator app (TOTP). SMS is off: it needs an SNS role
        // and is the weaker factor. Matches terraform/cognito.tf.
        mfa: cognito.Mfa.OPTIONAL,
        mfaSecondFactor: { sms: false, otp: true },
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });

      // The Hosted UI domain prefix must be globally unique, so it's suffixed with
      // the account. It also has to be a CONCRETE string at synth time (CDK
      // validates the charset), so fall back to a placeholder when synthesizing
      // without credentials — where this.account is an unresolved token.
      const acctSuffix = cdk.Token.isUnresolved(this.account) ? "local" : this.account.slice(-6);
      const domainPrefix = `${agentName.replace(/_/g, "-")}-${acctSuffix}`;
      pool.addDomain("CognitoDomain", {
        cognitoDomain: { domainPrefix },
      });

      const client = pool.addClient("SpaClient", {
        userPoolClientName: `${agentName}-spa`,
        generateSecret: false,
        authFlows: { userSrp: true },
        oAuth: {
          flows: { authorizationCodeGrant: true },
          scopes: [
            cognito.OAuthScope.OPENID,
            cognito.OAuthScope.EMAIL,
            cognito.OAuthScope.PROFILE,
          ],
          callbackUrls: ["https://localhost"],
          logoutUrls: ["https://localhost"],
        },
      });

      spaClientL1 = client.node.defaultChild as cognito.CfnUserPoolClient;
      cognitoUserPoolId = pool.userPoolId;
      cognitoClientId = client.userPoolClientId;
      cognitoDomainPrefix = domainPrefix;

      // Groups named by workflow.json -> authorization.actions, so declaring RBAC
      // provisions the groups it needs (mirrors aws_cognito_user_group.authz).
      // Membership is NOT managed here — it is per-person and changes far more
      // often than a deploy:
      //   aws cognito-idp admin-add-user-to-group --user-pool-id <id> \
      //       --username you@example.com --group-name approvers
      // The Cognito trigger (signup_lambda/handler.py), for two jobs. Mirrors
      // terraform/signup.tf. Self sign-up: post-confirmation puts each new user in the
      // default group. The activity log: post-authentication records each sign-in — on
      // a console in the builds table, next to the deploys and destroys; in a build's
      // own app in its <agentName>_audit table (below), next to its runs.
      const auditSignIns = true;
      const auditTableName = props.builder === false ? `${agentName}_audit` : `${agentName}_builds`;
      const group = props.selfSignUpGroup || "members";
      if (props.selfSignUp) {
        if (!authzGroups(workflow).includes(group)) {
          throw new Error(
            `selfSignUp puts every new user in group "${group}", but no action in ` +
              `app/workflow.json authorization.actions names it, so the group would not ` +
              `exist and sign-up confirmation would fail. Name it on the actions a new ` +
              `user may take (e.g. "deploy": ["operators", "${group}"]), or pass ` +
              `-c selfSignUpGroup=<a group that is named>.`
          );
        }
        const privileged = privilegedGrants(workflow, group);
        if (privileged.length) {
          throw new Error(
            `selfSignUp puts every new user in group "${group}", and app/workflow.json ` +
              `grants that group ${privileged.map((a) => `"${a}"`).join(", ")} — which reach ` +
              `every user's builds, runs or activity. Anyone who signs up would get them. ` +
              `Grant those to a group you fill from the backend only (e.g. "admins"), or ` +
              `pass -c selfSignUpGroup=<a group without them>.`
          );
        }
      }
      if (props.selfSignUp || auditSignIns) {
        const fnName = `AgentCoreSignup-${agentName}`;
        const signup = new lambda.Function(this, "SignupTrigger", {
          functionName: fnName,
          logGroup: new logs.LogGroup(this, "SignupTriggerLogGroup", {
            logGroupName: `/aws/lambda/${fnName}`,
            retention: LOG_RETENTION,
            removalPolicy: cdk.RemovalPolicy.DESTROY,
          }),
          runtime: lambda.Runtime.PYTHON_3_13,
          handler: "handler.handler",
          code: lambda.Code.fromAsset(path.join(ORCH_ROOT, "signup_lambda"), { exclude: ["__pycache__"] }),
          timeout: cdk.Duration.seconds(10),
          memorySize: 128,
          environment: {
            ...(props.selfSignUp ? { DEFAULT_GROUP: group } : {}),
            // By name: the table is created further down (lib/builder-plane.ts, or the
            // AuditTable of a build's app).
            ...(auditSignIns ? { AUDIT_TABLE: auditTableName } : {}),
          },
        });
        if (props.selfSignUp) {
          // Any pool in this account/region, not pool.userPoolArn: the pool references
          // the function as its trigger, so naming the pool here would be a cycle.
          signup.addToRolePolicy(new iam.PolicyStatement({
            actions: ["cognito-idp:AdminAddUserToGroup"],
            resources: [`arn:aws:cognito-idp:${this.region}:${this.account}:userpool/*`],
          }));
          pool.addTrigger(cognito.UserPoolOperation.POST_CONFIRMATION, signup);
        }
        if (auditSignIns) {
          signup.addToRolePolicy(new iam.PolicyStatement({
            actions: ["dynamodb:PutItem"],
            resources: [`arn:aws:dynamodb:${this.region}:${this.account}:table/${auditTableName}`],
          }));
          pool.addTrigger(cognito.UserPoolOperation.POST_AUTHENTICATION, signup);
        }
      }

      const authzActionMap: Record<string, string[]> = workflow.authorization?.actions ?? {};
      for (const g of authzGroups(workflow)) {
        const grants = Object.entries(authzActionMap)
          .filter(([, gs]) => gs.includes(g))
          .map(([a]) => a);
        new cognito.CfnUserPoolGroup(this, `AuthzGroup${g.replace(/[^A-Za-z0-9]/g, "")}`, {
          userPoolId: pool.userPoolId,
          groupName: g,
          description: `Grants: ${grants.join(", ")} (from app/workflow.json authorization.actions).`,
        });
      }

      // ---- Cognito machine-to-machine identity (agent -> Gateway) ----------
      // The SPA client above logs USERS in. Gateway calls are a different
      // boundary: the runtime needs a client-credentials token, which requires a
      // Resource Server (to define the custom scope) plus a CONFIDENTIAL client.
      if (enableGateway) {
        const scopeName = "invoke";
        const resourceServer = pool.addResourceServer("GatewayResourceServer", {
          identifier: "gateway",
          userPoolResourceServerName: `${agentName}-gateway`,
          scopes: [
            new cognito.ResourceServerScope({
              scopeName,
              scopeDescription: "Invoke tools through the AgentCore Gateway",
            }),
          ],
        });
        const m2m = pool.addClient("M2mClient", {
          userPoolClientName: `${agentName}-m2m`,
          generateSecret: true,
          authFlows: {},
          oAuth: {
            flows: { clientCredentials: true },
            scopes: [
              cognito.OAuthScope.resourceServer(
                resourceServer,
                new cognito.ResourceServerScope({ scopeName, scopeDescription: "" })
              ),
            ],
          },
        });
        m2mClientId = m2m.userPoolClientId;
        // orchestrator.gatewayIdentity "perAgent": one client per agent with a tool.
        for (const id of agentClientIdsOf(workflow)) {
          const c = pool.addClient(`AgentClient-${id}`, {
            userPoolClientName: `${agentName}-a-${id}`,
            generateSecret: true,
            authFlows: {},
            oAuth: { flows: { clientCredentials: true }, scopes: [cognito.OAuthScope.resourceServer(
              resourceServer, new cognito.ResourceServerScope({ scopeName, scopeDescription: "" }))] },
          });
          agentClients[id] = { id: c.userPoolClientId, secret: c.userPoolClientSecret.unsafeUnwrap() };
        }
        // Resolved via a describe-user-pool-client lookup at deploy time, so the
        // secret never appears in the template body.
        m2mClientSecret = m2m.userPoolClientSecret.unsafeUnwrap();
        m2mAudience = "gateway/invoke";
      }

      new cdk.CfnOutput(this, "cognitoUserPoolId", { value: pool.userPoolId });
      new cdk.CfnOutput(this, "cognitoClientId", { value: client.userPoolClientId });
      new cdk.CfnOutput(this, "cognitoDomainPrefix", { value: domainPrefix });
    } else if (isCognito) {
      cognitoUserPoolId = props.cognitoUserPoolId;
      cognitoClientId = props.cognitoClientId;
      cognitoDomainPrefix = props.cognitoDomainPrefix;
    }

    // "none" is the only configuration that deploys without auth.
    const authEnabled = props.idp !== "none";

    // Fail at synth with a precise message rather than deploying a broken authorizer.
    if (isCognito && !props.createCognito && (!cognitoUserPoolId || !cognitoClientId || !cognitoDomainPrefix)) {
      throw new Error(
        'idp=cognito without -c createCognito=true requires -c cognitoUserPoolId=... -c cognitoClientId=... -c cognitoDomainPrefix=...'
      );
    }
    if (isAuth0 && (!props.auth0Domain || !props.auth0ClientId)) {
      throw new Error('idp=auth0 requires -c auth0Domain=... -c auth0ClientId=...');
    }

    // Cognito issues `iss` as the user-pool endpoint; Auth0 as the tenant domain
    // WITH a trailing slash. Both put the SPA client id in the ID token's `aud`.
    const jwtIssuer = isCognito
      ? `https://cognito-idp.${this.region}.amazonaws.com/${cognitoUserPoolId}`
      : isAuth0
        ? `https://${props.auth0Domain}/`
        : "";
    const jwtAudience = isCognito ? cognitoClientId : props.auth0ClientId;

    // ---- Agent -> Gateway machine identity (provider-agnostic) --------------
    // Either created above (Cognito + createCognito) or supplied by the operator.
    const createdM2m = m2mClientId !== "";
    const gatewayClientId = createdM2m ? m2mClientId : props.gatewayClientId;
    const gatewayClientSecret = createdM2m ? m2mClientSecret : props.gatewayClientSecret;
    // Cognito -> the OAuth2 scope; Auth0 -> the API identifier.
    const gatewayAudience = createdM2m ? m2mAudience : props.gatewayAudience;

    // OIDC discovery + token endpoints, per provider.
    const gatewayDiscoveryUrl = isCognito
      ? `https://cognito-idp.${this.region}.amazonaws.com/${cognitoUserPoolId}/.well-known/openid-configuration`
      : isAuth0
        ? `https://${props.auth0Domain}/.well-known/openid-configuration`
        : "";
    const gatewayTokenUrl = isCognito
      ? `https://${cognitoDomainPrefix}.auth.${this.region}.amazoncognito.com/oauth2/token`
      : isAuth0
        ? `https://${props.auth0Domain}/oauth/token`
        : "";

    // Same guardrails as terraform/identity.tf, enforced at synth.
    if (enableGateway) {
      if (props.idp === "none") {
        throw new Error(
          'idp=none cannot be combined with -c enableGateway=true: the Gateway\'s CUSTOM_JWT ' +
            "authorizer needs an OIDC provider. Use -c enableGateway=false (no tool plane is " +
            "then provisioned, so agents with a `tool` fail loudly), or pick an idp."
        );
      }
      if (!createdM2m && (!gatewayClientId || !gatewayAudience || !gatewayClientSecret)) {
        throw new Error(
          "enableGateway=true requires -c gatewayClientId=... -c gatewayAudience=... and " +
            "GATEWAY_CLIENT_SECRET in the environment (the OAuth2 scope for Cognito, or the API " +
            "identifier for Auth0), unless idp=cognito with -c createCognito=true."
        );
      }
    }

    // ======================================================================
    // The workflow plane: everything that RUNS this deployment's workflow. Absent on a
    // control-plane console (consoleMode "builder"). Mirrors local.workflow_plane in
    // terraform/main.tf.
    // ======================================================================
    let plane: {
      statusTable: dynamodb.Table; eventsTable: dynamodb.Table; telemetryTable: dynamodb.Table;
      orchestratorArn: string; memoryId: string; imageUri: string;
      assetsBucket?: s3.Bucket; toolPlane?: ToolPlane;
    } | undefined;
    if (workflowPlane) {
      // ---- Container image (ARM64) built from orchestrator/Dockerfile ----------
      // Build with finch by exporting CDK_DOCKER=finch (default is docker).
      const image = new ecrAssets.DockerImageAsset(this, "Image", {
        directory: ORCH_ROOT,
        file: "Dockerfile",
        platform: ecrAssets.Platform.LINUX_ARM64,
      });

      // ---- DynamoDB: status, events, telemetry --------------------------------
      const statusTable = new dynamodb.Table(this, "StatusTable", {
        tableName: `${agentName}_status`,
        partitionKey: { name: "session_id", type: dynamodb.AttributeType.STRING },
        billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
        pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
        // Rows carry `ttl` only when RUN_TTL_DAYS is set (app/common/sink.py).
        timeToLiveAttribute: "ttl",
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });

      const eventsTable = new dynamodb.Table(this, "EventsTable", {
        tableName: `${agentName}_events`,
        partitionKey: { name: "session_id", type: dynamodb.AttributeType.STRING },
        sortKey: { name: "ts", type: dynamodb.AttributeType.STRING },
        billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
        pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
        // Rows carry `ttl` only when RUN_TTL_DAYS is set (app/common/sink.py).
        timeToLiveAttribute: "ttl",
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });

      const telemetryTable = new dynamodb.Table(this, "TelemetryTable", {
        tableName: `${agentName}_telemetry`,
        partitionKey: { name: "session_id", type: dynamodb.AttributeType.STRING },
        sortKey: { name: "sk", type: dynamodb.AttributeType.STRING },
        billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
        pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
        // The writer stamps `ttl` on every row (app/features/observability/store.py,
        // TELEMETRY_TTL_DAYS, default 90 days). Mirrors terraform/observability.tf.
        timeToLiveAttribute: "ttl",
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });
      telemetryTable.addGlobalSecondaryIndex({
        indexName: "by_date",
        partitionKey: { name: "date", type: dynamodb.AttributeType.STRING },
        sortKey: { name: "sk", type: dynamodb.AttributeType.STRING },
        projectionType: dynamodb.ProjectionType.ALL,
      });

      // ---- Insights findings store (AgentCore Optimization) -------------------
      // One reserved item (id="latest") holding the newest cross-run findings.
      const insightsTable = new dynamodb.Table(this, "InsightsTable", {
        tableName: `${agentName}_insights`,
        partitionKey: { name: "id", type: dynamodb.AttributeType.STRING },
        billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
        pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });

      // ---- AgentCore Memory ×2 -----------------------------------------------
      // No stable L2 for AgentCore yet; use the CloudFormation resource types
      // directly (the same types the Terraform awscc provider drives).
      //
      // 1) the durable LangGraph checkpointer (short-term run state / HITL resume)
      const memory = new cdk.CfnResource(this, "Memory", {
        type: "AWS::BedrockAgentCore::Memory",
        properties: {
          Name: `${agentName}_memory`,
          EventExpiryDuration: memoryEventExpiryDays,
          Description: "Checkpoint/state store for the multi-agent orchestrator",
        },
      });
      const memoryId = memory.getAtt("MemoryId").toString();
      const memoryArn = memory.getAtt("MemoryArn").toString();

      // 2) LONG-TERM memory with extraction strategies. Separate resource: agents
      // that enable memory.longTerm in workflow.json recall past insights before
      // running and store their output after. Namespaces are derived at run time
      // from actorId = "<agentId>-<subjectSlug>", so adding an agent or subject
      // needs no infrastructure change.
      const memoryPlan = memoryStrategiesFor(workflow, modelId, cdk.Token.isUnresolved(region) ? undefined : region);
      // A custom strategy's extraction runs YOUR model on your behalf, so the memory needs
      // an execution role that may invoke it. Only when some agent defines one.
      const memoryRole = memoryPlan.custom ? new iam.Role(this, "MemoryExecutionRole", {
        roleName: `AgentCoreMemory-${agentName}`,
        assumedBy: new iam.ServicePrincipal("bedrock-agentcore.amazonaws.com", {
          conditions: { StringEquals: { "aws:SourceAccount": this.account } },
        }),
        inlinePolicies: { invoke: new iam.PolicyDocument({ statements: [new iam.PolicyStatement({
          actions: ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
          resources: ["arn:aws:bedrock:*::foundation-model/*",
            `arn:aws:bedrock:${this.region}:${this.account}:inference-profile/*`],
        })] }) },
      }) : undefined;
      const semanticMemory = new cdk.CfnResource(this, "SemanticMemory", {
        type: "AWS::BedrockAgentCore::Memory",
        properties: {
          Name: `${agentName}_semantic`,
          EventExpiryDuration: memoryEventExpiryDays,
          Description: "Long-term semantic memory (per-agent, per-subject insights)",
          MemoryStrategies: memoryPlan.strategies,
          ...(memoryRole ? { MemoryExecutionRoleArn: memoryRole.roleArn } : {}),
        },
      });
      if (memoryRole) semanticMemory.node.addDependency(memoryRole);

      // Custom evaluators (agentcore.evaluations.custom): one AgentCore evaluator each,
      // scored like a built-in; the runtime finds its id in CUSTOM_EVALUATORS.
      const customEvaluatorIds: Record<string, string> = {};
      for (const ev of customEvaluatorsOf(workflow, agentName, modelId, cdk.Token.isUnresolved(region) ? undefined : region)) {
        const e = new agentcore.CfnEvaluator(this, `Evaluator-${ev.key.replace(/[^A-Za-z0-9]/g, "")}`, {
          evaluatorName: ev.evaluatorName,
          level: "TRACE",
          description: `Custom evaluator ${ev.key}`,
          evaluatorConfig: { llmAsAJudge: {
            instructions: ev.instructions,
            modelConfig: { bedrockEvaluatorModelConfig: { modelId: ev.model } },
            ratingScale: { numerical: ev.scale },
          } },
        });
        customEvaluatorIds[ev.key] = e.attrEvaluatorId;
      }
      for (const [use, n] of Object.entries(sharedEvaluatorUses(workflow))) customEvaluatorIds[use] = customEvaluatorIds[`*.${n}`];
      for (const k of Object.keys(customEvaluatorIds)) if (k.startsWith("*.")) delete customEvaluatorIds[k];
      const semanticMemoryId = semanticMemory.getAtt("MemoryId").toString();
      const semanticMemoryArn = semanticMemory.getAtt("MemoryArn").toString();

      // Guardrail policy, projected from workflow.json (see buildGuardrail).
      const gr = buildGuardrail(workflow);

      // ---- Bedrock Guardrail (content safety) --------------------------------
      // The POLICY comes from the `guardrail` block in workflow.json, so adapting
      // content safety to a customer's domain is a CONFIG edit — nothing here is
      // domain-specific. Mirrors terraform/guardrail.tf.
      //
      // Its id is injected as GUARDRAIL_ID, so workflow.json only needs the
      // per-agent flip ("guardrails": {"input": true, "output": true}).
      // Every sub-policy is optional: omit a key and that block is not emitted at
      // all, because Bedrock rejects an empty policy config.
      // Bedrock rejects a guardrail with no policy, so the build-wide one exists only when
      // its block enforces something; otherwise GUARDRAIL_ID is "" (a no-op unless an agent
      // names one with agentcore.guardrails.use). Mirrors gr_has_policy in guardrail.tf.
      const grHasPolicy = !!(gr.contentFilters.length || gr.deniedWords.length ||
        gr.managedWordLists.length || gr.deniedTopics.length || gr.piiEntities.length);
      const guardrail = !grHasPolicy ? undefined : new cdk.CfnResource(this, "Guardrail", {
        type: "AWS::Bedrock::Guardrail",
        properties: {
          Name: `${agentName}-guardrail`,
          Description: "Content-safety guardrail generated from app/workflow.json",
          BlockedInputMessaging: gr.blockedInputMessage,
          BlockedOutputsMessaging: gr.blockedOutputMessage,
          ...(gr.contentFilters.length
            ? { ContentPolicyConfig: { FiltersConfig: gr.contentFilters } }
            : {}),
          ...(gr.deniedWords.length || gr.managedWordLists.length
            ? {
                WordPolicyConfig: {
                  ...(gr.deniedWords.length ? { WordsConfig: gr.deniedWords } : {}),
                  ...(gr.managedWordLists.length
                    ? { ManagedWordListsConfig: gr.managedWordLists }
                    : {}),
                },
              }
            : {}),
          ...(gr.deniedTopics.length
            ? { TopicPolicyConfig: { TopicsConfig: gr.deniedTopics } }
            : {}),
          ...(gr.piiEntities.length
            ? { SensitiveInformationPolicyConfig: { PiiEntitiesConfig: gr.piiEntities } }
            : {}),
        },
      });
      const guardrailId = guardrail ? guardrail.getAtt("GuardrailId").toString() : "";

      // Named guardrails (workflow.json `guardrails`): each its own, for
      // agentcore.guardrails.use. Mirrors aws_bedrock_guardrail.named in terraform.
      const namedGuardrails: Record<string, { id: string; version: string }> = {};
      const guardrailArns = guardrail ? [guardrail.getAtt("GuardrailArn").toString()] : [];
      for (const [n, def] of Object.entries<any>(workflow.guardrails ?? {})) {
        const g2 = buildGuardrail({ guardrail: def });
        const r = new cdk.CfnResource(this, `Guardrail-${n}`, {
          type: "AWS::Bedrock::Guardrail",
          properties: {
            Name: `${agentName}-gr-${n}`,
            Description: `Guardrail ${n} from app/workflow.json guardrails`,
            BlockedInputMessaging: g2.blockedInputMessage,
            BlockedOutputsMessaging: g2.blockedOutputMessage,
            ...(g2.contentFilters.length ? { ContentPolicyConfig: { FiltersConfig: g2.contentFilters } } : {}),
            ...(g2.deniedWords.length || g2.managedWordLists.length ? { WordPolicyConfig: {
              ...(g2.deniedWords.length ? { WordsConfig: g2.deniedWords } : {}),
              ...(g2.managedWordLists.length ? { ManagedWordListsConfig: g2.managedWordLists } : {}) } } : {}),
            ...(g2.deniedTopics.length ? { TopicPolicyConfig: { TopicsConfig: g2.deniedTopics } } : {}),
            ...(g2.piiEntities.length ? { SensitiveInformationPolicyConfig: { PiiEntitiesConfig: g2.piiEntities } } : {}),
          },
        });
        namedGuardrails[n] = { id: r.getAtt("GuardrailId").toString(), version: "DRAFT" };
        guardrailArns.push(r.getAtt("GuardrailArn").toString());
      }

      // Named memories (workflow.json `memories`): each its own AgentCore Memory with its
      // strategies and expiry, for agentcore.memory.use. Mirrors
      // awscc_bedrockagentcore_memory.named in terraform/evaluations.tf.
      const namedMemories: Record<string, string> = {};
      const namedMemoryArns: string[] = [];
      for (const [n, def] of Object.entries<any>(workflow.memories ?? {})) {
        const want: string[] = def?.strategies ?? [];
        const strategies = [
          ...(want.includes("semantic") ? [{ SemanticMemoryStrategy: { Name: "insights", Namespaces: ["insights/{actorId}"] } }] : []),
          ...(want.includes("summary") ? [{ SummaryMemoryStrategy: { Name: "summary", Namespaces: ["summary/{actorId}/{sessionId}"] } }] : []),
          ...(want.includes("userPreference") ? [{ UserPreferenceMemoryStrategy: { Name: "preferences", Namespaces: ["preferences/{actorId}"] } }] : []),
          ...(want.includes("episodic") ? [{ EpisodicMemoryStrategy: { Name: "episodes", Namespaces: ["episodes/{actorId}/{sessionId}"],
            ReflectionConfiguration: { Namespaces: ["episodes/{actorId}"] } } }] : []),
        ];
        const m = new cdk.CfnResource(this, `Memory-${n}`, {
          type: "AWS::BedrockAgentCore::Memory",
          properties: {
            Name: `${agentName}_m_${n}`,
            EventExpiryDuration: Number(def?.expiryDays ?? keyDefault<number>("memory", "expiryDays")),
            Description: String(def?.description || `Memory ${n} from workflow.json memories`).slice(0, 200),
            MemoryStrategies: strategies,
          },
        });
        namedMemories[n] = m.getAtt("MemoryId").toString();
        const arn = m.getAtt("MemoryArn").toString();
        namedMemoryArns.push(arn, `${arn}/*`);
      }

      // Identities agents reach directly (agentcore.identity.outbound names from
      // `identities`): a credential provider each, and the workload identity the runtimes
      // exchange for their credentials. Mirrors terraform/identities.tf.
      const identityDefs: Record<string, any> = workflow.identities ?? {};
      const agentIdentities = agentIdentitiesOf(workflow);
      const identityProviders: Record<string, { provider: string; type: string; scopes: string[] }> = {};
      const dash = agentName.replace(/_/g, "-");
      for (const n of agentIdentities) {
        const def = identityDefs[n];
        const secret = props.identitySecrets?.[n] ?? "";
        if (!secret) {
          throw new Error(`identities.${n} is used by an agent and needs its secret in the build's secrets (IDENTITY_SECRETS.${n}).`);
        }
        const provider = `bedrock-agentcore-${dash}-id-${n}`;
        if (def.type === "oauth2") {
          new agentcore.CfnOAuth2CredentialProvider(this, `Identity-${n}`, {
            name: provider,
            credentialProviderVendor: "CustomOauth2",
            oauth2ProviderConfigInput: { customOauth2ProviderConfig: {
              clientId: def.clientId, clientSecret: secret,
              oauthDiscovery: def.discoveryUrl ? { discoveryUrl: def.discoveryUrl } : { authorizationServerMetadata: {
                issuer: def.issuer ?? new URL(def.tokenUrl).origin, tokenEndpoint: def.tokenUrl, authorizationEndpoint: def.tokenUrl } },
            } },
          });
        } else {
          new agentcore.CfnApiKeyCredentialProvider(this, `Identity-${n}`, { name: provider, apiKey: secret });
        }
        identityProviders[n] = { provider, type: String(def.type), scopes: def.scopes ?? [] };
      }
      const workloadIdentity = agentIdentities.length ? `${agentName}-agents` : "";
      if (workloadIdentity) new agentcore.CfnWorkloadIdentity(this, "AgentsWorkloadIdentity", { name: workloadIdentity });
      const identityPerms = [
        new iam.PolicyStatement({
          actions: ["bedrock-agentcore:GetResourceOauth2Token", "bedrock-agentcore:GetResourceApiKey",
            "bedrock-agentcore:GetWorkloadAccessToken"],
          resources: [
            `arn:aws:bedrock-agentcore:${this.region}:${this.account}:token-vault/default`,
            `arn:aws:bedrock-agentcore:${this.region}:${this.account}:token-vault/default/*`,
            `arn:aws:bedrock-agentcore:${this.region}:${this.account}:workload-identity-directory/default`,
            `arn:aws:bedrock-agentcore:${this.region}:${this.account}:workload-identity-directory/default/workload-identity/${agentName}-*`,
          ],
        }),
        new iam.PolicyStatement({
          actions: ["secretsmanager:GetSecretValue"],
          resources: [`arn:aws:secretsmanager:${this.region}:${this.account}:secret:bedrock-agentcore-identity!*`],
        }),
      ];

      // Feature env vars shared by the orchestrator and every dedicated runtime.
      // GenAI Observability: AgentCore injects the ADOT distro + OTLP endpoint;
      // always_on defeats the sampled=0 context the UI path propagates; the
      // bundled LangChain instrumentation is disabled because it emits a second,
      // token-less LLM span. (Same reasoning as terraform/main.tf.)
      const observabilityEnv = {
        AGENT_OBSERVABILITY_ENABLED: "true",
        OTEL_TRACES_SAMPLER: "always_on",
        OTEL_PYTHON_DISABLED_INSTRUMENTATIONS: "aws_langchain",
      };
      // Image agents (workflow.json `output: "image"`, app/common/images.py) store what they
      // render here; the BFF hands out short-lived links to it. Only when some agent draws,
      // and emptied and deleted with the stack. Mirrors aws_s3_bucket.assets in
      // terraform/images.tf.
      const imageAgents = Object.entries(agents).filter(([, a]) => a?.output === "image").map(([id]) => id);
      // Refuse a deploy that would send an image brief out of this region's geography
      // without the agent saying so (app/common/images.py enforces it again per render).
      if (!cdk.Token.isUnresolved(region)) {
        for (const id of imageAgents) {
          const img = agents[id].image ?? {};
          const why = vocab.imageResidencyError(
            String(img.model ?? vocab.values("imageModels")[0]), region, img.allowCrossRegion === true);
          if (why) throw new Error(`agents.${id}: ${why}`);
        }
      }
      const assetsBucket = imageAgents.length ? new s3.Bucket(this, "AssetsBucket", {
        bucketName: `agentcore-${agentName.replace(/_/g, "-")}-assets-${this.account}`,
        blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
        encryption: s3.BucketEncryption.S3_MANAGED,
        enforceSSL: true,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
        autoDeleteObjects: true,
      }) : undefined;
      const featureEnv = {
        ...observabilityEnv,
        ...(assetsBucket ? { ASSETS_BUCKET: assetsBucket.bucketName } : {}),
        SEMANTIC_MEMORY_ID: semanticMemoryId,
        ...(Object.keys(customEvaluatorIds).length
          ? { CUSTOM_EVALUATORS: this.toJsonString(customEvaluatorIds) } : {}),
        GUARDRAIL_ID: guardrailId,
        GUARDRAIL_VERSION: "DRAFT",
        GUARDRAILS: this.toJsonString(namedGuardrails),
        MEMORIES: this.toJsonString(namedMemories),
        IDENTITY_PROVIDERS: JSON.stringify(identityProviders),
        WORKLOAD_IDENTITY: workloadIdentity,
        // Evaluations reads spans from the runtime log group(s); the exact
        // id-suffixed name is only known after the runtime exists, so inject the
        // stable prefix and discover the match at query time.
        SPAN_LOG_GROUP_PREFIX: `/aws/bedrock-agentcore/runtimes/${agentName}-`,
      };

      // Permissions the AgentCore features need, on top of observabilityPerms.
      const guardrailApply = new iam.PolicyStatement({
        actions: ["bedrock:ApplyGuardrail"],
        // IAM rejects an empty resource list: with no guardrail at all, grant on an id no
        // guardrail has (ids are service-generated). Mirrors guardrail_arns in terraform.
        resources: guardrailArns.length ? guardrailArns
          : [`arn:aws:bedrock:${this.region}:${this.account}:guardrail/none`],
      });
      const longTermMemory = new iam.PolicyStatement({
        actions: [
          "bedrock-agentcore:CreateEvent",
          "bedrock-agentcore:RetrieveMemories",
          "bedrock-agentcore:RetrieveMemoryRecords",
          "bedrock-agentcore:ListMemoryRecords",
          "bedrock-agentcore:GetMemoryRecord",
        ],
        resources: [semanticMemoryArn, `${semanticMemoryArn}/*`, ...namedMemoryArns],
      });
      // Evaluations + Insights: Evaluate/BatchEvaluation resources are created
      // dynamically and the Logs query verbs aren't resource-scopable, so "*".
      const evaluationsAndInsights = new iam.PolicyStatement({
        actions: [
          "bedrock-agentcore:Evaluate",
          "bedrock-agentcore:StartBatchEvaluation",
          "bedrock-agentcore:GetBatchEvaluation",
          "bedrock-agentcore:ListBatchEvaluations",
          // Read the spans Evaluations/Insights analyze.
          "logs:DescribeLogGroups",
          "logs:StartQuery",
          "logs:GetQueryResults",
          "logs:StopQuery",
          "logs:FilterLogEvents",
          "logs:GetLogEvents",
          // StartBatchEvaluation WRITES its output to a CloudWatch log group it
          // creates on the caller's behalf (a forward access session), so these are
          // required too — without them Insights fails with
          // "FAS credentials do not have permission to create CloudWatch log groups".
          // Note this cannot be scoped to the runtime log-group prefix: the group
          // BatchEvaluation creates is outside it.
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:PutRetentionPolicy",
        ],
        resources: ["*"],
      });

      // ---- Tool plane: Gateway + Knowledge Base + Cedar policy ----------------
      // The whole subsystem lives in lib/tool-plane.ts (mirroring Terraform's
      // gateway.tf + kb.tf + policy.tf). Absent it, any agent with a `tool` fails
      // loudly (app/common/errors.py: ToolUnavailable) instead of inventing evidence.
      //
      // Cedar policy is on/off from workflow.json, exactly like Terraform:
      //   orchestrator.policy.enabled (default true), .mode (default ENFORCE).
      const policyEnabled =
        enableGateway && (workflow.orchestrator?.policy?.enabled ?? keyDefault<any>("orchestrator", "policy").enabled);
      const policyMode = String(workflow.orchestrator?.policy?.mode ?? keyDefault<any>("orchestrator", "policy").mode).toUpperCase();

      // Every tool the agents may call is declared in workflow.json — the same
      // block Terraform reads. Validated before use so a typo fails at synth.
      const inherited = inheritIdentities(workflow.tools ?? {}, workflow.identities ?? {},
        props.identitySecrets ?? {}, props.toolApiKeys);
      const declaredTools: Record<string, ToolSpec> = validateTools(
        inherited.tools,
        workflow.agents ?? {},
        this.region,
        ORCH_ROOT
      );
      // Agents + steps + kb_docs corpora (see terraform_data.workflow_validation).
      validateWorkflow(workflow, ORCH_ROOT, agentName, props.idp);

      const toolPlane = enableGateway
        ? new ToolPlane(this, "ToolPlane", {
            agentName,
            tools: declaredTools,
            toolApiKeys: inherited.toolApiKeys,
            // Only used by the built-in `source: "pricing"` demo function, which
            // reports on run history and gets READ-ONLY access to them.
            statusTable,
            telemetryTable,
            gatewayDiscoveryUrl,
            gatewayClientId,
            agentClientIds: Object.values(agentClients).map((c) => c.id),
            gatewayAudience,
            isCognito,
            isAuth0,
            policyEnabled,
            policyMode,
            customPolicies: policyEnabled ? customPoliciesOf(workflow) : [],
            orchRoot: ORCH_ROOT,
          })
        : undefined;

      // Gateway-backed MCP access. Empty strings when the Gateway is off, in which
      // case any agent with a `tool` fails loudly rather than inventing an answer
      // (see app/common/errors.py: ToolUnavailable).
      const gatewayEnv = {
        GATEWAY_URL: toolPlane?.gatewayUrl ?? "",
        GATEWAY_TOKEN_URL: enableGateway ? gatewayTokenUrl : "",
        GATEWAY_CLIENT_ID: enableGateway ? gatewayClientId : "",
        // Which client-credentials request shape to build ("cognito" | "auth0").
        GATEWAY_AUTH_FLOW: enableGateway ? props.idp : "",
        GATEWAY_AUDIENCE: enableGateway ? gatewayAudience : "",
        // Cedar mode in effect at the Gateway, so the UI can label decisions.
        // Enforcement itself is server-side; this is display-only.
        GATEWAY_POLICY_MODE: toolPlane?.policyModeEnv ?? "",
        // How to CALL each tool (argument shape, corpus, per-type options), so an
        // agent reaches a newly declared data source with no code change. This was
        // MISSING on the CDK path: app/common/config.py then falls back to the `tools`
        // block of the workflow.json baked into the image, which keeps only a subset of
        // the fields — so a request-level option set in config was silently dropped
        // here while Terraform honoured it. Mirrors terraform/main.tf TOOLS_JSON.
        TOOLS_JSON: JSON.stringify(toolsEnv(declaredTools)),
      };

      // Credentials go in one secret per runtime, not in its environment variables, where
      // anyone allowed GetAgentRuntime could read them. The runtime gets the ARN only and
      // copies the keys into its environment at start (app/common/runtime_secret.py).
      // Generated names: a destroyed secret waits out a recovery window, and a fixed name
      // would block the next deploy of the same build. Mirrors terraform/secrets.tf.
      // toJsonString, not secretObjectValue: the values carry tokens (the Cognito client
      // secrets), and secretObjectValue joins them into the JSON unescaped, so a nested
      // object came out as invalid JSON. Objects stay objects; runtime_secret.py encodes them.
      const runtimeSecret = (cid: string, values: Record<string, unknown>, role: iam.IRole): string => {
        const secret = new secretsmanager.Secret(this, cid, {
          description: `Runtime credentials for ${agentName} (app/common/runtime_secret.py)`,
          secretStringValue: cdk.SecretValue.unsafePlainText(this.toJsonString(values)),
          removalPolicy: cdk.RemovalPolicy.DESTROY,
        });
        secret.grantRead(role);
        return secret.secretArn;
      };

      // ---- Shared IAM trust: bedrock-agentcore assumes runtime roles ----------
      const agentcorePrincipal = new iam.ServicePrincipal("bedrock-agentcore.amazonaws.com", {
        conditions: {
          StringEquals: { "aws:SourceAccount": this.account },
          ArnLike: { "aws:SourceArn": `arn:aws:bedrock-agentcore:${this.region}:${this.account}:*` },
        },
      });

      const bedrockInvoke = new iam.PolicyStatement({
        actions: [
          "bedrock:InvokeModel",
          "bedrock:InvokeModelWithResponseStream",
          "bedrock:CountTokens",
        ],
        // Inference profiles, not account-wide bedrock:* — that also covered custom
        // models, provisioned throughput, agents, guardrails and prompts, none of which
        // the runtime invokes. Mirrors terraform/main.tf.
        resources: [
          "arn:aws:bedrock:*::foundation-model/*",
          `arn:aws:bedrock:${this.region}:${this.account}:inference-profile/*`,
          `arn:aws:bedrock:${this.region}:${this.account}:application-inference-profile/*`,
        ],
      });
      // The cost telemetry prices each model call from the AWS Price List
      // (app/features/observability/pricelist.py). Read-only public price data; the API
      // has no resource-level permissions, so "*". Mirrors terraform/main.tf.
      const priceListRead = new iam.PolicyStatement({
        sid: "PriceListRead",
        actions: ["pricing:GetProducts"],
        resources: ["*"],
      });
      const observabilityPerms = (workloadSuffix: string): iam.PolicyStatement[] => [
        new iam.PolicyStatement({
          actions: [
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents",
            "logs:DescribeLogStreams",
            "logs:DescribeLogGroups",
          ],
          resources: [`arn:aws:logs:${this.region}:${this.account}:log-group:/aws/bedrock-agentcore/runtimes/*`],
        }),
        new iam.PolicyStatement({
          // AgentCore "unified" telemetry: AgentCore adds a CloudWatch Logs resource
          // policy so X-Ray can deliver spans to the agent's own log group. Without
          // this, unified spans never reach CloudWatch. Account-scoped API, so "*".
          // Mirrors terraform/main.tf AgentCoreUnifiedSpanDelivery.
          actions: ["logs:PutResourcePolicy"],
          resources: ["*"],
        }),
        new iam.PolicyStatement({
          actions: ["xray:PutTraceSegments", "xray:PutTelemetryRecords", "xray:GetSamplingRules", "xray:GetSamplingTargets"],
          resources: ["*"],
        }),
        new iam.PolicyStatement({
          actions: ["cloudwatch:PutMetricData"],
          resources: ["*"],
          conditions: { StringEquals: { "cloudwatch:namespace": "bedrock-agentcore" } },
        }),
        new iam.PolicyStatement({
          actions: [
            "bedrock-agentcore:GetWorkloadAccessToken",
            "bedrock-agentcore:GetWorkloadAccessTokenForJWT",
            "bedrock-agentcore:GetWorkloadAccessTokenForUserId",
          ],
          resources: [
            `arn:aws:bedrock-agentcore:${this.region}:${this.account}:workload-identity-directory/default`,
            `arn:aws:bedrock-agentcore:${this.region}:${this.account}:workload-identity-directory/default/workload-identity/${workloadSuffix}`,
          ],
        }),
      ];

      // ---- Dedicated agent runtimes (one per `dedicated` agent) ---------------
      // ONE EXECUTION ROLE PER AGENT, scoped to what that agent's workflow.json entry asks
      // for. This was a single shared role, so an agent that enables nothing still carried
      // the union of every other agent's permissions — in the shipped workflow
      // `knowledge_research` uses no guardrails and no dedicated agent uses long-term
      // memory, yet all three could call ApplyGuardrail and read the semantic memory store.
      //
      // Nothing here is for the customer to write: the grants are DERIVED from the same
      // config that switches the feature on. Mirrors aws_iam_role.subagent in
      // terraform/subagent_runtimes.tf.
      const dedicatedArns: Record<string, string> = {};
      for (const id of dedicatedIds) {
        const ac = agents[id].agentcore ?? {};
        // IAM caps a role name at 64 characters, and this one carries the agent id where
        // the shared name did not. Checked rather than truncated: a silently shortened name
        // can collide with another agent's, and two runtimes sharing a role is exactly what
        // this change exists to stop.
        // Length is validated in validateWorkflow, alongside the runtime-name limit, so both
        // name rules are reported from one place.
        const roleName = `AgentCoreSubagent-${agentName}-${id}`;
        const subagentRole = new iam.Role(this, `SubagentRole-${id}`, {
          roleName,
          assumedBy: agentcorePrincipal,
        });
        image.repository.grantPull(subagentRole);
        // Unconditional: every dedicated container pulls the image, emits spans and metrics,
        // calls a model, and writes a telemetry row per model call.
        [bedrockInvoke, priceListRead, ...observabilityPerms(`${agentName}*`)].forEach((s) =>
          subagentRole.addToPolicy(s)
        );
        // PutItem only — grantWriteData() would also add DeleteItem/UpdateItem and the
        // batch writes. A dedicated agent appends telemetry rows; it never edits them.
        subagentRole.addToPolicy(
          new iam.PolicyStatement({
            actions: ["dynamodb:PutItem"],
            resources: [telemetryTable.tableArn],
          })
        );
        // Feature-gated: granted only because THIS agent's config enables the feature.
        // Without the grant, GUARDRAIL_ID still resolves and ApplyGuardrail is denied —
        // the correct outcome for an agent that never calls it.
        if (ac.guardrails?.input || ac.guardrails?.output) subagentRole.addToPolicy(guardrailApply);
        if (assetsBucket && agents[id].output === "image") {
          subagentRole.addToPolicy(new iam.PolicyStatement({
            actions: ["s3:PutObject"], resources: [assetsBucket.arnForObjects("runs/*")],
          }));
        }
        // An agent that reads earlier agents' images (workflow.json `vision`) reads them
        // from its own runtime. Mirrors aws_iam_role_policy.subagent_vision.
        if (assetsBucket && readsImages(agents[id])) {
          subagentRole.addToPolicy(new iam.PolicyStatement({
            sid: "ReadRunImages", actions: ["s3:GetObject"], resources: [assetsBucket.arnForObjects("runs/*")],
          }));
        }
        if ((ac.memory?.longTerm ?? []).length || ac.memory?.use) subagentRole.addToPolicy(longTermMemory);
        if ((ac.identity?.outbound ?? []).some((n: string) => agentIdentities.includes(n))) {
          identityPerms.forEach((st) => subagentRole.addToPolicy(st));
        }

        const rt = new cdk.CfnResource(this, `Subagent-${id}`, {
          type: "AWS::BedrockAgentCore::Runtime",
          properties: {
            AgentRuntimeName: `${agentName}_${id}`,
            Description: `Dedicated runtime for agent ${id} (${agents[id].name})`,
            RoleArn: subagentRole.roleArn,
            AgentRuntimeArtifact: { ContainerConfiguration: { ContainerUri: image.imageUri } },
            NetworkConfiguration: { NetworkMode: "PUBLIC" },
            EnvironmentVariables: {
              AWS_REGION: this.region,
              BEDROCK_MODEL_ID: modelId,
              AGENT_ID: id,
              TELEMETRY_TABLE: telemetryTable.tableName,
              ...featureEnv,
              ...gatewayEnv,
              // This agent's own Gateway client when orchestrator.gatewayIdentity is "perAgent".
              ...(agentClients[id] ? { GATEWAY_CLIENT_ID: agentClients[id].id } : {}),
              ...(enableGateway ? {
                RUNTIME_SECRET_ARN: runtimeSecret(`SubagentSecret-${id}`, {
                  GATEWAY_CLIENT_SECRET: agentClients[id] ? agentClients[id].secret : gatewayClientSecret,
                }, subagentRole),
              } : {}),
            },
          },
        });
        rt.node.addDependency(subagentRole);
        dedicatedArns[id] = rt.getAtt("AgentRuntimeArn").toString();
      }

      // JSON map {agentId: runtimeArn} for the orchestrator (Fn.join keeps tokens intact).
      const arnMapJson =
        dedicatedIds.length === 0
          ? "{}"
          : cdk.Fn.join("", [
              "{",
              ...dedicatedIds.flatMap((id, i) => [
                i ? "," : "",
                `"${id}":"`,
                dedicatedArns[id],
                '"',
              ]),
              "}",
            ]);
      const invokeRuntimeResources =
        dedicatedIds.length === 0
          ? [`arn:aws:bedrock-agentcore:${this.region}:${this.account}:runtime/none`]
          : dedicatedIds.flatMap((id) => [dedicatedArns[id], `${dedicatedArns[id]}/*`]);

      // ---- The stand-in A2A agent (mirrors terraform/a2a.tf) -----------------
      // A real Agent2Agent server in its own Lambda behind its own Function URL, so
      // `runtime: "a2a"` ships with something live. See a2a_lambda/handler.py. Created
      // only when an agent asks for it with `source: "a2a_lambda"`.
      const a2aAgents = a2aLambdaAgents(agents);
      let a2aEndpoints: Record<string, string> = {};
      let a2aFunction: lambda.Function | undefined;
      let a2aFunctionUrl: lambda.FunctionUrl | undefined;
      if (Object.keys(a2aAgents).length) {
        const a2aName = `A2AAgent-${agentName}`;
        a2aFunction = new lambda.Function(this, "A2AAgent", {
          functionName: a2aName,
          description: 'Stand-in A2A (Agent2Agent) agent for runtime = "a2a"',
          logGroup: new logs.LogGroup(this, "A2AAgentLogGroup", {
            logGroupName: `/aws/lambda/${a2aName}`,
            retention: LOG_RETENTION,
            removalPolicy: cdk.RemovalPolicy.DESTROY,
          }),
          runtime: lambda.Runtime.PYTHON_3_13,
          handler: "handler.lambda_handler",
          code: lambda.Code.fromAsset(path.join(ORCH_ROOT, "a2a_lambda")),
          // It makes a model call, so it needs far more than the 3s default — and a skill
          // that synthesizes a full asset (`analysis`, `recommendation`) is a 6000-token
          // generation, not a short review.
          //
          // DELIBERATELY SHORTER than the client's own a2aInvoke.timeoutSeconds. The side
          // that gives up first should be the side that knows why: this function returns
          // an error naming the skill and the budget, whereas a client that abandons the
          // request first leaves no diagnosis and cannot tell slow from dead.
          timeout: cdk.Duration.seconds(120),
          memorySize: 512,
          environment: {
            BEDROCK_MODEL_ID: modelId,
            // The card's `provider.organization` — who OPERATES these agents. Each skill
            // names itself; this names the party behind them.
            A2A_AGENT_NAME: "Independent Agent Services",
            // The DEFAULT output budget. A skill that needs more declares its own
            // (a2a_lambda/handler.py `maxTokens`), so this is never tuned per agent.
            A2A_MAX_TOKENS: "2000",
          },
        });
        // It is an agent, so it calls a model.
        a2aFunction.addToRolePolicy(bedrockInvoke);
        const a2aUrl = (a2aFunctionUrl = a2aFunction.addFunctionUrl({
          // AWS_IAM, never NONE. This is why the client has an `auth: "sigv4"` mode: the
          // endpoint is not public, the caller must present a SigV4 signature from a
          // principal allowed to invoke it, and no bearer token exists to leak or rotate.
          authType: lambda.FunctionUrlAuthType.AWS_IAM,
        }));
        // The skill is a PATH SEGMENT, not a query string: a client appends
        // `/.well-known/agent-card.json` to the base URL it is given, and
        // `https://host/?skill=x` + that path resolves to nothing.
        //
        // "/" + skill without stripping any trailing slash from the Function URL. The
        // url attribute is a CloudFormation token, so it cannot be inspected at synth to
        // find out whether it ends in one — and the result is safe either way, because a
        // doubled slash is just an empty path segment that the handler's
        // `_skill_from_path` skips. Guessing wrong about the token would have produced
        // `https://hostcompliance`, which fails only at run time.
        a2aEndpoints = Object.fromEntries(
          Object.entries(a2aAgents).map(([id, skill]) => [
            id,
            cdk.Fn.join("", [a2aUrl.url, `/${skill}`]),
          ])
        );
      }

      // ---- Orchestrator runtime ----------------------------------------------
      const runtimeRole = new iam.Role(this, "RuntimeRole", {
        roleName: `AgentCoreRuntime-${agentName}`,
        assumedBy: agentcorePrincipal,
      });
      image.repository.grantPull(runtimeRole);
      [
        bedrockInvoke,
        priceListRead,
        ...observabilityPerms(`${agentName}-*`),
        guardrailApply,
        longTermMemory,
        evaluationsAndInsights,
        ...(agentIdentities.length ? identityPerms : []),
      ].forEach((s) => runtimeRole.addToPolicy(s));
      if (assetsBucket) {
        // Read too, only when an in-process agent reads images (`vision`). Mirrors
        // aws_iam_role_policy.runtime_assets.
        const mainReads = Object.entries(agents).some(([id, a]) => !dedicatedIds.includes(id)
          && a?.runtime !== "a2a" && readsImages(a));
        runtimeRole.addToPolicy(new iam.PolicyStatement({
          actions: ["s3:PutObject", ...(mainReads ? ["s3:GetObject"] : [])],
          resources: [assetsBucket.arnForObjects("runs/*")],
        }));
      }
      runtimeRole.addToPolicy(
        new iam.PolicyStatement({
          actions: [
            "bedrock-agentcore:CreateEvent",
            "bedrock-agentcore:ListEvents",
            "bedrock-agentcore:GetEvent",
            "bedrock-agentcore:ListSessions",
            "bedrock-agentcore:RetrieveMemories",
          ],
          resources: [memoryArn, `${memoryArn}/*`],
        })
      );
      runtimeRole.addToPolicy(
        new iam.PolicyStatement({
          actions: ["bedrock-agentcore:InvokeAgentRuntime"],
          resources: invokeRuntimeResources,
        })
      );
      // The orchestrator is the only principal allowed to call the stand-in — granted on
      // BOTH sides. Identity-based alone is enough for a same-account caller, but the
      // resource policy is what makes the permission visible on the function when a 403
      // has to be diagnosed, and it is what CDK's grantInvokeUrl exists to write.
      if (a2aFunction && a2aFunctionUrl) {
        runtimeRole.addToPolicy(
          new iam.PolicyStatement({
            actions: ["lambda:InvokeFunctionUrl"],
            resources: [a2aFunction.functionArn],
          })
        );
        a2aFunctionUrl.grantInvokeUrl(runtimeRole);
      }
      // Spelled out rather than grantReadWriteData(), which also hands over
      // BatchGetItem/BatchWriteItem/ConditionCheckItem/DescribeTable and the stream
      // reads — none of which this app calls. The Terraform path already granted only
      // what the code uses, so the macro was both over-broad and a parity gap.
      runtimeRole.addToPolicy(
        new iam.PolicyStatement({
          sid: "ProgressStoreWrite",
          actions: ["dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:GetItem"],
          resources: [statusTable.tableArn, eventsTable.tableArn],
        })
      );
      // The activity log: the runtime records how each run ended there (the BFF logged it
      // starting). A build's own <agentName>_audit, or this console's builds table.
      // Mirrors RunOutcomesToActivityLog in terraform/main.tf.
      const runAuditTable = props.builder === false ? `${agentName}_audit` : `${agentName}_builds`;
      runtimeRole.addToPolicy(
        new iam.PolicyStatement({
          sid: "RunOutcomesToActivityLog",
          actions: ["dynamodb:PutItem"],
          resources: [`arn:aws:dynamodb:${this.region}:${this.account}:table/${runAuditTable}`],
        })
      );
      runtimeRole.addToPolicy(
        // Scan on the STATUS table only: Insights flags whether each analyzed session
        // still exists so the UI can disable dead links.
        new iam.PolicyStatement({
          sid: "ProgressStoreScanStatus",
          actions: ["dynamodb:Scan"],
          resources: [statusTable.tableArn],
        })
      );
      runtimeRole.addToPolicy(
        // Read as well as write: Evaluations scores each prompt from its persisted
        // model-call I/O, which means querying the telemetry table back.
        new iam.PolicyStatement({
          sid: "TelemetryReadWrite",
          actions: ["dynamodb:PutItem", "dynamodb:Query", "dynamodb:GetItem"],
          resources: [telemetryTable.tableArn, `${telemetryTable.tableArn}/index/*`],
        })
      );
      runtimeRole.addToPolicy(
        new iam.PolicyStatement({
          sid: "InsightsFindingsStore",
          actions: ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem",
            "dynamodb:DeleteItem"],
          resources: [insightsTable.tableArn],
        })
      );

      const orchestrator = new cdk.CfnResource(this, "Orchestrator", {
        type: "AWS::BedrockAgentCore::Runtime",
        properties: {
          AgentRuntimeName: agentName,
          Description: "Multi-agent LangGraph orchestrator (CDK-managed)",
          RoleArn: runtimeRole.roleArn,
          AgentRuntimeArtifact: { ContainerConfiguration: { ContainerUri: image.imageUri } },
          NetworkConfiguration: { NetworkMode: "PUBLIC" },
          EnvironmentVariables: {
            AWS_REGION: this.region,
            BEDROCK_MODEL_ID: modelId,
            MEMORY_ID: memoryId,
            STATUS_TABLE: statusTable.tableName,
            EVENTS_TABLE: eventsTable.tableName,
            TELEMETRY_TABLE: telemetryTable.tableName,
            INSIGHTS_TABLE: insightsTable.tableName,
            // How each run ended, in the activity log (app/common/audit.py).
            AUDIT_TABLE: runAuditTable,
            AGENT_RUNTIME_ARNS: arnMapJson,
            RUNTIME_SECRET_ARN: runtimeSecret("OrchestratorSecret", {
              GATEWAY_CLIENT_SECRET: enableGateway ? gatewayClientSecret : "",
              // orchestrator.gatewayIdentity "perAgent": each in-process agent's own client.
              GATEWAY_AGENT_CLIENTS: agentClients,
              A2A_TOKENS: a2aTokenEnv(agents, props.a2aTokens),
            }, runtimeRole),
            A2A_ENDPOINTS: JSON.stringify(a2aEndpoints),
            ...featureEnv,
            ...gatewayEnv,
          },
        },
      });
      orchestrator.node.addDependency(runtimeRole);
      const orchestratorArn = orchestrator.getAtt("AgentRuntimeArn").toString();
      plane = { statusTable, eventsTable, telemetryTable, orchestratorArn, memoryId, imageUri: image.imageUri,
        assetsBucket, toolPlane };
    }
    const assetsBucket = plane?.assetsBucket;
    const toolPlane = plane?.toolPlane;

    // ---- BFF Lambda + HTTP API ---------------------------------------------
    const bffFunctionName = `AgentCoreBFF-${agentName}`;
    // Constructed ARN (not bff.functionArn) so the self-invoke policy below does not
    // create a function -> role-policy -> function circular dependency.
    const bffArn = `arn:aws:lambda:${this.region}:${this.account}:function:${bffFunctionName}`;
    // Own every Lambda log group explicitly. Left implicit, Lambda creates
    // /aws/lambda/<name> itself with NEVER-EXPIRE retention and no stack ownership,
    // so `cdk destroy` leaves it behind accruing cost forever — verified: a full
    // destroy of this stack orphaned eight such groups. Mirrors the
    // aws_cloudwatch_log_group resources on the Terraform path (var.log_retention_days).
    const lambdaLogGroup = (id: string, fnName: string) =>
      new logs.LogGroup(this, id, {
        logGroupName: `/aws/lambda/${fnName}`,
        retention: LOG_RETENTION,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });

    const bff = new lambda.Function(this, "Bff", {
      functionName: bffFunctionName,
      logGroup: lambdaLogGroup("BffLogGroup", bffFunctionName),
      runtime: lambda.Runtime.PYTHON_3_13,
      handler: "handler.handler",
      code: lambda.Code.fromAsset(stageBffPackage(workflow)),
      // 300 s, not 60: a Design-with-AI turn (bff/designer.py) runs in a background
      // invocation of this function and can take a few model calls. API requests are still
      // cut off by API Gateway at 30 s, so nothing interactive waits longer.
      timeout: cdk.Duration.seconds(300),
      memorySize: 256,
      environment: {
        // This deployment's own runs: none on a control-plane console.
        ...(plane ? {
          STATUS_TABLE: plane.statusTable.tableName,
          EVENTS_TABLE: plane.eventsTable.tableName,
          TELEMETRY_TABLE: plane.telemetryTable.tableName,
          RUNTIME_ARN: plane.orchestratorArn,
        } : {}),
        // No WORKFLOW_JSON: the workflow travels IN the package (stageBffPackage)
        // because Lambda's whole environment is capped at 4 KB, which held about
        // eleven agents' worth of the old projection.
        // The assistant's tool-use loop runs in this Lambda, so it needs the
        // deployment's model rather than its own copy of the id.
        MODEL_ID: modelId,
      },
    });
    // Explicit, not grantReadWriteData()/grantReadData() — same reason as the
    // runtime role above: the macros add batch and stream actions the BFF never calls.
    if (plane) {
      bff.addToRolePolicy(
        new iam.PolicyStatement({
          actions: ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem",
            "dynamodb:DeleteItem", "dynamodb:Query", "dynamodb:Scan"],
          resources: [plane.statusTable.tableArn, plane.eventsTable.tableArn],
        })
      );
      bff.addToRolePolicy(
        // Session detail query plus the by_date GSI the aggregation reads.
        new iam.PolicyStatement({
          actions: ["dynamodb:Query"],
          resources: [plane.telemetryTable.tableArn, `${plane.telemetryTable.tableArn}/index/*`],
        })
      );
      bff.addToRolePolicy(
        new iam.PolicyStatement({
          actions: ["bedrock-agentcore:InvokeAgentRuntime"],
          resources: [plane.orchestratorArn, `${plane.orchestratorArn}/*`],
        })
      );
    }
    // The BFF self-invokes (async runner mode). Use the constructed ARN to avoid a
    // circular dependency with its own execution role.
    bff.addToRolePolicy(
      new iam.PolicyStatement({ actions: ["lambda:InvokeFunction"], resources: [bffArn] })
    );
    // The in-app assistant's tool-use loop runs IN the BFF (bff/chatbot.py calls
    // bedrock-runtime Converse), so the BFF needs its own model grant. Without it
    // the chat UI deploys, accepts a message, and every reply is
    // "couldn't reach the model (AccessDeniedException)" — which is exactly what a
    // CDK deployment did, because only the runtime roles carried `bedrockInvoke`.
    // Mirrors terraform/bff.tf. Scoped to the inference profile + foundation
    // models, not the account-wide bedrock:* the runtime roles use.
    bff.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
        resources: [
          "arn:aws:bedrock:*::foundation-model/*",
          `arn:aws:bedrock:${this.region}:${this.account}:inference-profile/*`,
        ],
      })
    );

    // GET /api/models lists what this account can invoke, for the Build view's model
    // picker. Read-only list calls, which take no resource ARN. Mirrors terraform/bff.tf.
    bff.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["bedrock:ListFoundationModels", "bedrock:ListInferenceProfiles"],
        resources: ["*"],
      })
    );

    // ---- Builder control plane (builds store + deploy project) ----------------
    const builderPlane = props.builder === false ? undefined
      : new BuilderPlane(this, "Builder", { agentName, orchRoot: ORCH_ROOT, logRetention: LOG_RETENTION });
    builderPlane?.grantConsole(bff);
    // s3:// paths in an AgentExpress Assistant message: read only these (bff/designer.py).
    const designerS3 = builderPlane ? (props.designerS3Buckets ?? []) : [];
    if (designerS3.length) {
      bff.addEnvironment("DESIGNER_S3_BUCKETS", designerS3.join(","));
      designerS3.forEach((entry, i) => {
        const [bucket, ...rest] = entry.replace(/^s3:\/\//, "").replace(/\/+$/, "").split("/");
        const prefix = rest.join("/");
        bff.addToRolePolicy(new iam.PolicyStatement({
          sid: `AssistantReadsS3Object${i}`, actions: ["s3:GetObject"],
          resources: [`arn:aws:s3:::${bucket}/${prefix ? `${prefix}/` : ""}*`],
        }));
        bff.addToRolePolicy(new iam.PolicyStatement({
          sid: `AssistantListsS3${i}`, actions: ["s3:ListBucket"], resources: [`arn:aws:s3:::${bucket}`],
          ...(prefix ? { conditions: { StringLike: { "s3:prefix": [`${prefix}/*`, prefix] } } } : {}),
        }));
      });
    }
    // The Assistant's models. Only set when chosen: the BFF's own defaults are already
    // moved to this region's inference-profile geography, so an empty value is not a
    // `us.` literal in disguise.
    if (builderPlane && props.designerModel) bff.addEnvironment("DESIGNER_MODEL", props.designerModel);
    if (builderPlane && props.designerSummaryModel) {
      bff.addEnvironment("DESIGNER_SUMMARY_MODEL", props.designerSummaryModel);
    }
    if (builderPlane && props.designerEffort) {
      if (!["low", "medium", "high", "model"].includes(props.designerEffort)) {
        throw new Error(`designerEffort must be low, medium, high or model, not "${props.designerEffort}"`);
      }
      bff.addEnvironment("DESIGNER_EFFORT", props.designerEffort);
    }
    // A control-plane console (see consoleMode): the BFF refuses the run routes.
    if (consoleMode === "builder") bff.addEnvironment("CONSOLE_MODE", "builder");
    // A build's own app keeps its own activity log (bff/audit.py): who signed in, and who
    // started, decided, cancelled, re-ran or deleted a run. A console keeps it with its
    // builds. Mirrors aws_dynamodb_table.audit in terraform/audit.tf.
    if (!builderPlane) {
      const auditTable = new dynamodb.Table(this, "AuditTable", {
        tableName: `${agentName}_audit`,
        partitionKey: { name: "pk", type: dynamodb.AttributeType.STRING },
        sortKey: { name: "sk", type: dynamodb.AttributeType.STRING },
        billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
        pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });
      bff.addEnvironment("AUDIT_TABLE", auditTable.tableName);
      bff.addToRolePolicy(new iam.PolicyStatement({
        sid: "AuditLog", actions: ["dynamodb:PutItem", "dynamodb:Query"], resources: [auditTable.tableArn],
      }));
    }
    // Links to the images an image agent rendered (GET /api/images), for runs it owns.
    if (assetsBucket) {
      bff.addEnvironment("ASSETS_BUCKET", assetsBucket.bucketName);
      bff.addToRolePolicy(new iam.PolicyStatement({
        actions: ["s3:GetObject"], resources: [assetsBucket.arnForObjects("runs/*")],
      }));
    }

    const api = new HttpApi(this, "HttpApi", { apiName: `AgentCoreBFF-${agentName}` });
    // ONE invoke permission for the whole API, as terraform/bff.tf has, instead of one per
    // route: Lambda caps a function's resource policy at 20 KB, and ~70 per-route
    // statements passed it ("The final policy size (20681) is bigger than the limit").
    const integration = new HttpLambdaIntegration("BffIntegration", bff, { scopePermissionToRoute: false });
    // Provider-agnostic JWT authorizer; omitted entirely when idp = "none",
    // which leaves /api/* OPEN.
    const authorizer = authEnabled
      ? new HttpJwtAuthorizer("JwtAuthorizer", jwtIssuer, { jwtAudience: [jwtAudience] })
      : undefined;

    const routes: Array<{ path: string; methods: HttpMethod[] }> = [
      { path: "/api/workflow", methods: [HttpMethod.GET] },
      // Who the caller is and which run actions they may take, so the UI can
      // disable controls instead of offering buttons that 403.
      { path: "/api/me", methods: [HttpMethod.GET] },
      // The Bedrock text models this account can invoke, for the Build view's picker.
      { path: "/api/models", methods: [HttpMethod.GET] },
      { path: "/api/sessions", methods: [HttpMethod.GET, HttpMethod.POST] },
      { path: "/api/sessions/{id}", methods: [HttpMethod.GET, HttpMethod.DELETE] },
      { path: "/api/sessions/{id}/decision", methods: [HttpMethod.POST] },
      { path: "/api/sessions/{id}/cancel", methods: [HttpMethod.POST] },
      // Rewind & re-run a settled run from a chosen agent.
      { path: "/api/sessions/{id}/rerun", methods: [HttpMethod.POST] },
      // AgentCore Evaluations on demand (auto-eval also runs at completion).
      { path: "/api/sessions/{id}/evaluate", methods: [HttpMethod.POST] },
      // AgentCore Optimization / Insights: cross-run findings.
      { path: "/api/insights", methods: [HttpMethod.GET] },
      { path: "/api/insights/run", methods: [HttpMethod.POST] },
      { path: "/api/sessions/{id}/telemetry", methods: [HttpMethod.GET] },
      { path: "/api/telemetry/aggregate", methods: [HttpMethod.GET] },
      // In-app assistant (bff/chatbot.py).
      { path: "/api/chat", methods: [HttpMethod.POST] },
      // Builder builds (bff/builds.py). Declared even with the builder plane off, where
      // the BFF answers 404 — so the route set is the same on every deployment.
      { path: "/api/builds", methods: [HttpMethod.GET] },
      { path: "/api/builds/{id}", methods: [HttpMethod.GET, HttpMethod.PUT, HttpMethod.DELETE] },
      { path: "/api/builds/{id}/deploy", methods: [HttpMethod.POST] },
      { path: "/api/builds/{id}/destroy", methods: [HttpMethod.POST] },
      { path: "/api/builds/{id}/log", methods: [HttpMethod.GET] },
      { path: "/api/builds/{id}/login", methods: [HttpMethod.GET] },
      // AgentExpress Assistant: the conversation that edits a build (bff/designer.py).
      { path: "/api/builds/{id}/design", methods: [HttpMethod.GET, HttpMethod.POST, HttpMethod.DELETE] },
      { path: "/api/builds/{id}/design/attachments", methods: [HttpMethod.POST] },
      // A link to an image an image agent rendered (AssetsBucket).
      { path: "/api/images", methods: [HttpMethod.GET] },
      // A build's write-only secrets, and its knowledge-base documents.
      { path: "/api/builds/{id}/secrets", methods: [HttpMethod.GET, HttpMethod.PUT] },
      { path: "/api/builds/{id}/docs", methods: [HttpMethod.GET, HttpMethod.POST, HttpMethod.DELETE] },
      // Connected AWS accounts (bff/accounts.py).
      { path: "/api/accounts", methods: [HttpMethod.GET, HttpMethod.POST] },
      { path: "/api/accounts/{id}", methods: [HttpMethod.PUT, HttpMethod.DELETE] },
      { path: "/api/accounts/{id}/verify", methods: [HttpMethod.POST] },
      { path: "/api/accounts/{id}/launch", methods: [HttpMethod.GET] },
      // A tool written in the build: checked (bff/codecheck.py), and tested once deployed.
      { path: "/api/code/check", methods: [HttpMethod.POST] },
      { path: "/api/builds/{id}/test-tool", methods: [HttpMethod.POST] },
      // The policy library (bff/policies.py), and plain English -> Cedar.
      { path: "/api/policies", methods: [HttpMethod.GET, HttpMethod.POST] },
      { path: "/api/policies/generate", methods: [HttpMethod.POST] },
      { path: "/api/policies/generate/{id}", methods: [HttpMethod.GET] },
      { path: "/api/policies/{id}", methods: [HttpMethod.PUT, HttpMethod.DELETE] },
      // Sharing, the library and admin-defined groups (bff/sharing.py, bff/library.py).
      { path: "/api/builds/{id}/shares", methods: [HttpMethod.PUT] },
      { path: "/api/library", methods: [HttpMethod.GET, HttpMethod.POST] },
      { path: "/api/library/{id}", methods: [HttpMethod.GET, HttpMethod.PUT, HttpMethod.DELETE] },
      { path: "/api/library/{id}/shares", methods: [HttpMethod.PUT] },
      { path: "/api/groups", methods: [HttpMethod.GET] },
      { path: "/api/groups/{id}", methods: [HttpMethod.PUT, HttpMethod.DELETE] },
      // The audit log, and the sign-out the page reports before it signs out.
      { path: "/api/audit", methods: [HttpMethod.GET] },
      { path: "/api/audit/logout", methods: [HttpMethod.POST] },
    ];
    for (const r of routes) {
      api.addRoutes({ path: r.path, methods: r.methods, integration, authorizer });
    }

    // Access logs and a throttle on the $default stage. Mirrors terraform/bff.tf. The
    // throttle is per API, generous for a console: the UI polls a run every few seconds.
    const apiAccessLogs = new logs.LogGroup(this, "ApiAccessLogs", {
      logGroupName: `/aws/apigateway/AgentCoreBFF-${agentName}`,
      retention: LOG_RETENTION,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    const defaultStage = api.defaultStage!.node.defaultChild as CfnStage;
    defaultStage.accessLogSettings = {
      destinationArn: apiAccessLogs.logGroupArn,
      format: JSON.stringify(API_ACCESS_LOG_FORMAT),
    };
    defaultStage.defaultRouteSettings = {
      throttlingBurstLimit: API_THROTTLE_BURST,
      throttlingRateLimit: API_THROTTLE_RATE,
    };

    // ---- Static UI: private S3 + CloudFront (OAC) --------------------------
    const uiBucket = new s3.Bucket(this, "UiBucket", {
      bucketName: `agentcore-${agentName.replace(/_/g, "-")}-ui-${this.account}-${this.region}`,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    const apiDomain = cdk.Fn.select(2, cdk.Fn.split("/", api.apiEndpoint));
    // Security headers on every response, UI and /api alike. No script-src policy: the
    // SPA talks to the configured IdP and to presigned S3 URLs, which vary by deploy,
    // so the CSP only forbids framing, plugins and <base> rewrites. Mirrors ui.tf.
    const securityHeaders = new cf.ResponseHeadersPolicy(this, "UiSecurityHeaders", {
      securityHeadersBehavior: {
        strictTransportSecurity: {
          accessControlMaxAge: cdk.Duration.days(365), includeSubdomains: true, preload: true, override: true,
        },
        contentTypeOptions: { override: true },
        frameOptions: { frameOption: cf.HeadersFrameOption.DENY, override: true },
        referrerPolicy: {
          referrerPolicy: cf.HeadersReferrerPolicy.STRICT_ORIGIN_WHEN_CROSS_ORIGIN, override: true,
        },
        contentSecurityPolicy: {
          contentSecurityPolicy: "frame-ancestors 'none'; object-src 'none'; base-uri 'self'",
          override: true,
        },
      },
    });
    const distribution = new cf.Distribution(this, "Ui", {
      comment: "Multi-agent orchestrator UI",
      defaultRootObject: "index.html",
      defaultBehavior: {
        origin: origins.S3BucketOrigin.withOriginAccessControl(uiBucket),
        viewerProtocolPolicy: cf.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
        cachePolicy: cf.CachePolicy.CACHING_OPTIMIZED,
        responseHeadersPolicy: securityHeaders,
      },
      additionalBehaviors: {
        "/api/*": {
          origin: new origins.HttpOrigin(apiDomain, { protocolPolicy: cf.OriginProtocolPolicy.HTTPS_ONLY }),
          viewerProtocolPolicy: cf.ViewerProtocolPolicy.HTTPS_ONLY,
          allowedMethods: cf.AllowedMethods.ALLOW_ALL,
          cachePolicy: cf.CachePolicy.CACHING_DISABLED,
          originRequestPolicy: cf.OriginRequestPolicy.ALL_VIEWER_EXCEPT_HOST_HEADER,
          responseHeadersPolicy: securityHeaders,
        },
      },
    });

    builderPlane?.allowUploadsFrom(`https://${distribution.distributionDomainName}`);

    // ---- wire the Hosted UI callbacks -------------------------------------
    // Cognito rejects an /authorize request whose redirect_uri is not registered
    // on the client, and the Hosted UI shows only "An error was encountered with
    // the requested page." So the CloudFront URL MUST be on the SPA client.
    // Terraform does this in cognito.tf; without it CDK login is simply broken.
    // Only the CloudFront URL: a localhost redirect lets any local listener receive a
    // sign-in code. For local development against the same pool, add one deliberately
    // with `-c extraCallbackUrls=https://localhost:5173` (comma-separated).
    if (spaClientL1) {
      const uiOrigin = `https://${distribution.distributionDomainName}`;
      const extra = String(this.node.tryGetContext("extraCallbackUrls") ?? "")
        .split(",").map((u) => u.trim()).filter(Boolean);
      spaClientL1.callbackUrLs = [uiOrigin, ...extra];
      spaClientL1.logoutUrLs = [uiOrigin, ...extra];
      // Which sign-in the Hosted UI offers: COGNITO (its own users) by default, or a
      // federated provider you added to the pool, e.g. `-c hostedUiProviders=MyOidc`.
      // Set here, not by hand: a manual change is reverted whenever the client updates.
      // Mirrors terraform var.hosted_ui_providers.
      const providers = String(this.node.tryGetContext("hostedUiProviders") ?? "")
        .split(",").map((p) => p.trim()).filter(Boolean);
      if (providers.length) spaClientL1.supportedIdentityProviders = providers;
    }

    // Non-secret IdP config the SPA reads. Same shape as the Terraform template
    // (web/auth-config.js.tftpl) so web/index.html works under either IaC:
    // `provider` selects the login strategy, and unused fields stay empty.
    const authConfigJs =
      `// Generated by CDK. Non-secret IdP settings for the SPA.\n` +
      `// provider: "cognito" | "auth0" | "none" (enabled=false).\n` +
      `window.AUTH_CONFIG = {\n` +
      `  enabled: ${authEnabled},\n` +
      `  provider: ${JSON.stringify(props.idp)},\n` +
      `  region: ${JSON.stringify(this.region)},\n` +
      `  userPoolId: ${JSON.stringify(cognitoUserPoolId)},\n` +
      `  domainPrefix: ${JSON.stringify(cognitoDomainPrefix)},\n` +
      `  domain: ${JSON.stringify(isAuth0 ? props.auth0Domain : "")},\n` +
      `  clientId: ${JSON.stringify(jwtAudience)}\n` +
      `};\n`;

    new s3deploy.BucketDeployment(this, "UiDeploy", {
      // The CDK framework Lambda behind this construct also leaves a never-expire log
      // group behind on destroy unless we own it.
      logGroup: new logs.LogGroup(this, "UiDeployLogGroup", {
        retention: LOG_RETENTION,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
      destinationBucket: uiBucket,
      distribution,
      distributionPaths: ["/*"],
      sources: [
        // web/ is a Vite + React + Cloudscape app, so what ships is the BUILD OUTPUT.
        // `bundling` runs the build inside the asset staging step, which means the same
        // npm script CI runs produces the bundle here — a stale dist/ cannot be deployed
        // by accident, and `tsc --noEmit` inside `npm run build` fails the synth rather
        // than shipping a broken page.
        s3deploy.Source.asset(path.join(ORCH_ROOT, "web"), {
          // Only the sources: node_modules and a previous dist/ must not become part of
          // the asset hash, or every local build would invalidate the deployment.
          exclude: ["node_modules", "dist", "*.tftpl", ".vite"],
          bundling: {
            // A Node image, because the build is npm. Pinned to 22 while web/package.json
            // requires only >=20: the floor is what the app needs, this is what CI builds
            // it with, and pinning means a new Node major cannot change the bundle without
            // someone choosing it.
            image: cdk.DockerImage.fromRegistry("public.ecr.aws/docker/library/node:22-alpine"),
            command: [
              "sh", "-c",
              [
                "cd /asset-input",
                // A writable HOME for npm's cache inside the container.
                "export HOME=/tmp npm_config_cache=/tmp/.npm",
                // `--include=dev` is NOT redundant. npm skips devDependencies when
                // NODE_ENV=production, and typescript and vite are devDependencies — so
                // an install inherited from a shell with NODE_ENV=production removes the
                // very tools the next line needs, and the build dies with
                // "sh: tsc: not found". The flag makes the install independent of the
                // caller's environment, which is the only way this is reliable.
                // (Which React gets bundled is settled in web/vite.config.ts, not here.)
                "if [ -f package-lock.json ]; then npm ci --include=dev --no-fund --no-audit; " +
                  "else npm install --include=dev --no-fund --no-audit; fi",
                "npm run build",
                // The Observability tab is still the legacy island; it is copied in
                // beside the bundle rather than imported, so it is served as its own
                // file exactly as before.
                "cp legacy/observability.js dist/observability.js",
                "cp -r dist/. /asset-output/",
              ].join(" && "),
            ],
            // Falls back to a local build when no container engine is available, which
            // keeps `cdk synth` working on a machine without Docker/Finch.
            local: {
              tryBundle(outputDir: string): boolean {
                const { spawnSync } = require("node:child_process") as typeof import("node:child_process");
                const web = path.join(ORCH_ROOT, "web");
                const run = (cmd: string, args: string[]) =>
                  spawnSync(cmd, args, { cwd: web, stdio: "inherit", shell: false });
                if (spawnSync("npm", ["--version"], { stdio: "ignore" }).status !== 0) return false;
                // `--include=dev`, for the reason given in the container command above:
                // typescript and vite are devDependencies, and npm drops those when the
                // caller's NODE_ENV is "production".
                const install = fs.existsSync(path.join(web, "package-lock.json"))
                  ? ["ci", "--include=dev", "--no-fund", "--no-audit"]
                  : ["install", "--include=dev", "--no-fund", "--no-audit"];
                if (run("npm", install).status !== 0) return false;
                if (run("npm", ["run", "build"]).status !== 0) return false;
                fs.cpSync(path.join(web, "dist"), outputDir, { recursive: true });
                fs.copyFileSync(
                  path.join(web, "legacy", "observability.js"),
                  path.join(outputDir, "observability.js"));
                return true;
              },
            },
          },
        }),
        s3deploy.Source.data("auth-config.js", authConfigJs),
      ],
      // ONE deployment, everything `no-cache`, and that is a deliberate simplification.
      // The tempting version is two deployments — hashed assets cached `immutable`,
      // index.html revalidating — but they would fight over `prune`: the pruning one
      // deletes whatever the other just uploaded, and CDK does not order them unless you
      // say so. A revalidation request per asset returning 304 is a small price for
      // removing that class of bug from a reference sample, and CloudFront still caches
      // at the edge under its own TTL. Terraform's ui.tf does split the two, because
      // there each object is declared individually and the ordering question does not
      // arise.
      cacheControl: [s3deploy.CacheControl.fromString("no-cache")],
      prune: true,
    });

    // Only where runtimes emit spans: not on a control-plane console.
    if (workflowPlane) {
      // ---- CloudWatch Transaction Search --------------------------------------
      // AgentCore Observability and the Evaluations/Insights features read agent
      // OTEL spans from the CloudWatch "aws/spans" log group. Spans only land there
      // once Transaction Search is on for the account (it switches X-Ray span
      // ingestion into CloudWatch Logs) — normally a manual console step per new
      // account, provisioned here so a fresh-account deploy is self-contained.

      // Resource policy letting X-Ray deliver spans to those log groups. Keyed by
      // name and updated in place, so it is safe to manage declaratively.
      const tsLogPolicy = new logs.CfnResourcePolicy(this, "TransactionSearchLogPolicy", {
        policyName: `${agentName}-transaction-search`,
        policyDocument: JSON.stringify({
          Version: "2012-10-17",
          Statement: [
            {
              Sid: "TransactionSearchXRayAccess",
              Effect: "Allow",
              Principal: { Service: "xray.amazonaws.com" },
              Action: "logs:PutLogEvents",
              Resource: [
                `arn:aws:logs:${this.region}:${this.account}:log-group:aws/spans:*`,
                `arn:aws:logs:${this.region}:${this.account}:log-group:/aws/application-signals/data:*`,
              ],
              Condition: {
                ArnLike: { "aws:SourceArn": `arn:aws:xray:${this.region}:${this.account}:*` },
                StringEquals: { "aws:SourceAccount": this.account },
              },
            },
          ],
        }),
      });

      // Enablement needs a READ-THEN-WRITE, so it is a small Lambda-backed custom
      // resource rather than anything declarative. Two dead ends ruled this out:
      //
      //   * AWS::XRay::TransactionSearchConfig — Transaction Search is an
      //     ACCOUNT-WIDE SINGLETON, so that resource can only ever CREATE it and
      //     fails with AlreadyExists wherever it is already on.
      //   * a bare UpdateTraceSegmentDestination call — NOT idempotent: it raises
      //     InvalidRequestException("The destination is already set to
      //     CloudWatchLogs") on a second call, which fails the whole stack.
      //
      // So this checks the current destination first and only writes when it needs
      // to change — the same logic as the Terraform null_resource. Re-deploying, or
      // deploying into an account where Transaction Search is already on, is a
      // clean no-op instead of an error.
      const tsHandler = new lambda.Function(this, "TransactionSearchHandler", {
        // Unnamed function -> CDK generates the name, so let it generate the log group
        // too and just pin retention + removal.
        logGroup: new logs.LogGroup(this, "TransactionSearchHandlerLogGroup", {
          retention: LOG_RETENTION,
          removalPolicy: cdk.RemovalPolicy.DESTROY,
        }),
        runtime: lambda.Runtime.PYTHON_3_13,
        handler: "index.handler",
        timeout: cdk.Duration.minutes(2),
        description: "Idempotently enables CloudWatch Transaction Search for this account/region",
        code: lambda.Code.fromInline(`
import boto3

def handler(event, context):
    # Deletion is a deliberate no-op: Transaction Search is account-wide and
    # other stacks or services may depend on it, so tearing it down on a
    # \`cdk destroy\` of this stack would be a surprise.
    if event["RequestType"] == "Delete":
        return {"PhysicalResourceId": event["PhysicalResourceId"]}

    pct = int(event["ResourceProperties"]["IndexingPercentage"])
    xray = boto3.client("xray")

    current = xray.get_trace_segment_destination().get("Destination")
    changed = False
    if current != "CloudWatchLogs":
        # Enabling Transaction Search the FIRST time in an account makes X-Ray call
        # Application Signals and CloudTrail on your behalf, so it needs more than
        # the xray:* actions (see the role policy below). Those calls can still be
        # refused by an SCP or permissions boundary we cannot see from here.
        #
        # Transaction Search is an OBSERVABILITY enhancement, not something the
        # agents need in order to run, so a refusal must NOT fail the deployment
        # and take the whole stack with it. Degrade and report instead.
        try:
            xray.update_trace_segment_destination(Destination="CloudWatchLogs")
            changed = True
        except Exception as e:
            return {
                "PhysicalResourceId": "transaction-search",
                "Data": {
                    "Destination": current or "None",
                    "Changed": "False",
                    "Indexing": "skipped",
                    "Warning": (
                        "Transaction Search NOT enabled: "
                        + type(e).__name__ + ": " + str(e)[:180]
                        + " -- tracing still works; enable it later with "
                        + "'aws xray update-trace-segment-destination "
                        + "--destination CloudWatchLogs'."
                    ),
                },
            }

    # Best effort: never fail the deploy over the indexed percentage, since
    # Transaction Search itself is on by this point.
    try:
        xray.update_indexing_rule(
            Name="Default",
            Rule={"Probabilistic": {"DesiredSamplingPercentage": pct}},
        )
        indexing = "set"
    except Exception as e:
        indexing = f"skipped ({type(e).__name__})"

    return {
        "PhysicalResourceId": "transaction-search",
        "Data": {
            "Destination": "CloudWatchLogs",
            "Changed": str(changed),
            "Indexing": indexing,
        },
    }
  `),
      });
      tsHandler.addToRolePolicy(
        new iam.PolicyStatement({
          actions: [
            "xray:GetTraceSegmentDestination",
            "xray:UpdateTraceSegmentDestination",
            "xray:UpdateIndexingRule",
            // UpdateTraceSegmentDestination calls Application Signals on your
            // behalf the FIRST time Transaction Search is enabled in an account, so
            // without this the custom resource fails with
            //   AccessDeniedException ... not authorized to perform:
            //   application-signals:StartDiscovery
            // It is easy to miss because an account where Application Signals is
            // already discovered never triggers the call — this only bites on a
            // clean account. Terraform doesn't need the grant: it runs the
            // equivalent step via local-exec using the deployer's own credentials.
            "application-signals:StartDiscovery",
            "xray:GetIndexingRules",
            // StartDiscovery creates a service-linked CloudTrail event channel and
            // the Application Signals service-linked role, so both of these are
            // required on a first-time enable.
            "cloudtrail:CreateServiceLinkedChannel",
            "iam:CreateServiceLinkedRole",
          ],
          resources: ["*"],
        })
      );
      // The first enable in a region also creates the aws/spans log group and its
      // `default` stream with the CALLER's permissions ("no identity-based policy allows
      // the logs:CreateLogStream action"). Mirrors TransactionSearchSpansLogGroup in
      // terraform/deploy-role-policy.json.
      tsHandler.addToRolePolicy(
        new iam.PolicyStatement({
          actions: ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutRetentionPolicy"],
          resources: [
            `arn:aws:logs:${this.region}:${this.account}:log-group:aws/spans`,
            `arn:aws:logs:${this.region}:${this.account}:log-group:aws/spans:*`,
            `arn:aws:logs:${this.region}:${this.account}:log-group:/aws/application-signals/data`,
            `arn:aws:logs:${this.region}:${this.account}:log-group:/aws/application-signals/data:*`,
          ],
        })
      );

      const tsProvider = new cr.Provider(this, "TransactionSearchProvider", {
        onEventHandler: tsHandler,
      });
      const tsEnable = new cdk.CustomResource(this, "TransactionSearchEnable", {
        serviceToken: tsProvider.serviceToken,
        properties: {
          // Included so a changed percentage triggers an Update.
          IndexingPercentage: props.transactionSearchIndexingPercentage,
        },
      });
      tsEnable.node.addDependency(tsLogPolicy);
    }

    // ---- Outputs ------------------------------------------------------------
    new cdk.CfnOutput(this, "uiUrl", { value: `https://${distribution.distributionDomainName}` });
    new cdk.CfnOutput(this, "apiEndpoint", { value: api.apiEndpoint });
    new cdk.CfnOutput(this, "idp", { value: props.idp });
    if (toolPlane) {
      new cdk.CfnOutput(this, "gatewayUrl", { value: toolPlane.gatewayUrl });
      new cdk.CfnOutput(this, "gatewayId", { value: toolPlane.gatewayId });
      new cdk.CfnOutput(this, "knowledgeBaseId", { value: toolPlane.knowledgeBaseId });
    }
    if (plane) {
      new cdk.CfnOutput(this, "agentRuntimeArn", { value: plane.orchestratorArn });
      new cdk.CfnOutput(this, "memoryId", { value: plane.memoryId });
      new cdk.CfnOutput(this, "imageUri", { value: plane.imageUri });
    }
    new cdk.CfnOutput(this, "consoleMode", { value: consoleMode });
    if (plane) new cdk.CfnOutput(this, "dedicatedAgents", { value: dedicatedIds.join(", ") || "(none)" });
    new cdk.CfnOutput(this, "authEnabled", { value: String(authEnabled) });
  }
}

/**
 * Project the `guardrail` block of workflow.json into CloudFormation shapes for
 * AWS::Bedrock::Guardrail, mirroring the dynamic blocks in terraform/guardrail.tf.
 *
 * Returns already-shaped arrays so the caller can omit an empty policy entirely —
 * Bedrock rejects an empty *PolicyConfig rather than treating it as "no policy".
 */
export function buildGuardrail(workflow: any): {
  blockedInputMessage: string;
  blockedOutputMessage: string;
  contentFilters: Array<Record<string, string>>;
  deniedWords: Array<{ Text: string }>;
  managedWordLists: Array<{ Type: string }>;
  deniedTopics: Array<Record<string, unknown>>;
  piiEntities: Array<{ Type: string; Action: string }>;
} {
  const g = workflow.guardrail ?? {};
  // "NONE, LOW, MEDIUM or HIGH" rather than a bare join, because these strings are
  // generated from the vocabulary now and an error a customer reads should still read
  // like a sentence.
  const orList = (v: string[]) =>
    v.length < 2 ? v.join("") : `${v.slice(0, -1).join(", ")} or ${v[v.length - 1]}`;

  const contentFilters = Object.entries<string>(g.contentFilters ?? {}).map(([type, strength]) => {
    const t = type.toUpperCase();
    const s = String(strength).toUpperCase();
    if (!vocab.GUARDRAIL_FILTER_STRENGTHS.includes(s)) {
      throw new Error(
        `workflow.json guardrail.contentFilters.${type} is "${strength}"; use ` +
          `${orList(vocab.GUARDRAIL_FILTER_STRENGTHS)}.`
      );
    }
    return {
      Type: t,
      InputStrength: s,
      // Prompt-attack filtering is input-only; Bedrock rejects an output strength.
      OutputStrength: t === "PROMPT_ATTACK" ? "NONE" : s,
    };
  });

  const piiEntities = Object.entries<string>(g.piiEntities ?? {}).map(([type, action]) => {
    const a = String(action).toUpperCase();
    if (!vocab.GUARDRAIL_PII_ACTIONS.includes(a)) {
      throw new Error(
        `workflow.json guardrail.piiEntities.${type} is "${action}"; use ` +
          `${orList(vocab.GUARDRAIL_PII_ACTIONS)}.`
      );
    }
    return { Type: type.toUpperCase(), Action: a };
  });

  const deniedTopics = (g.deniedTopics ?? []).map((t: any) => {
    if (!t?.name || !t?.definition) {
      throw new Error(
        'Each workflow.json guardrail.deniedTopics entry needs a "name" and a "definition".'
      );
    }
    return {
      Name: t.name,
      Definition: t.definition,
      ...(t.examples?.length ? { Examples: t.examples } : {}),
      Type: "DENY",
    };
  });

  return {
    blockedInputMessage:
      g.blockedInputMessage ?? keyDefault<string>("guardrail", "blockedInputMessage"),
    blockedOutputMessage:
      g.blockedOutputMessage ?? keyDefault<string>("guardrail", "blockedOutputMessage"),
    contentFilters,
    deniedWords: (g.deniedWords ?? []).map((w: string) => ({ Text: w })),
    managedWordLists: (g.managedWordLists ?? []).map((w: string) => ({ Type: String(w).toUpperCase() })),
    deniedTopics,
    piiEntities,
  };
}

/**
 * Stage the BFF's deployment package: bff/*.py PLUS app/workflow.json, so the API
 * reads the same file the customer edits and the same file the runtime reads.
 *
 * WHY THERE IS A STAGING STEP AT ALL. `lambda.Code.fromAsset` takes one directory,
 * and the two inputs live in different ones. The alternative was what this code used
 * to do: build a trimmed PROJECTION of the workflow here in TypeScript, build the
 * same projection a second time in HCL for the Terraform path, and ship the result
 * in a `WORKFLOW_JSON` environment variable.
 *
 * Both halves of that were wrong. The two projections drifted silently — the
 * TypeScript one once emitted `mcp`/`rag` after workflow.json had renamed them to
 * `tool`/`corpus`, and omitted `evalAgents` and `chatbot` entirely, so CDK
 * deployments quietly lost the data-source chips, the Evaluate buttons and the
 * assistant. And the environment variable was a CEILING: Lambda caps the whole
 * environment at 4 KB and the quota cannot be raised, so both paths carried a
 * 3400-byte guard, the shipped ten-agent workflow measured 3153 bytes, and a
 * customer's twelfth agent failed the deploy.
 *
 * `bff/workflow.py` now does the projection once, in Python, at request time. This
 * function's only job is to put the file where that module can read it.
 *
 * Staged under cdk.out so it is a build artifact, never a mutation of the source
 * tree, and so CDK's own asset hashing sees a stable directory.
 */
export function stageBffPackage(workflow: any, outDir?: string): string {
  // One per jest worker: parallel test files synthesizing at once would otherwise delete
  // each other's staging directory mid-copy (ENOTEMPTY).
  const worker = process.env.JEST_WORKER_ID ? `-${process.env.JEST_WORKER_ID}` : "";
  const staged = outDir ?? path.join(ORCH_ROOT, "cdk", "cdk.out", `bff-package${worker}`);
  fs.rmSync(staged, { recursive: true, force: true });
  fs.mkdirSync(staged, { recursive: true });
  // Only .py, and NOT __pycache__: `fromAsset` on bff/ used to ship every stale .pyc
  // in the working tree, including ones built by a different Python minor version.
  const src = path.join(ORCH_ROOT, "bff");
  for (const entry of fs.readdirSync(src, { withFileTypes: true })) {
    if (entry.isFile() && entry.name.endsWith(".py")) {
      fs.copyFileSync(path.join(src, entry.name), path.join(staged, entry.name));
    } else if (entry.isDirectory() && entry.name !== "__pycache__") {
      // A subpackage. Mirrors `**/*.py` in the Terraform archive_file.
      fs.cpSync(path.join(src, entry.name), path.join(staged, entry.name), {
        recursive: true,
        filter: (s) => !s.includes("__pycache__"),
      });
    }
  }
  // Written from the parsed object rather than copied, so a syntactically broken
  // workflow.json fails here at synth instead of inside the Lambda at run time.
  fs.writeFileSync(path.join(staged, "workflow.json"), JSON.stringify(workflow, null, 2));
  // The framework's closed value sets. bff/authz.py reads the RBAC action names from
  // here rather than keeping a third copy of them. Mirrors the archive_file source in
  // terraform/bff.tf.
  fs.copyFileSync(
    path.join(ORCH_ROOT, "app", "vocabulary.json"),
    path.join(staged, "vocabulary.json")
  );
  // Every key's DEFAULT (generated from app/keys.json by build_schema.py). bff/authz.py
  // reads groupsClaim's default from here rather than keeping a fourth copy of it.
  // Mirrors the archive_file source in terraform/bff.tf.
  fs.copyFileSync(
    path.join(ORCH_ROOT, "app", "defaults.json"),
    path.join(staged, "defaults.json")
  );
  // The key spec. bff/validate_build.py checks a build against it before a deploy,
  // the same rules the Build view shows as you type. Mirrors terraform/bff.tf.
  fs.copyFileSync(path.join(ORCH_ROOT, "app", "keys.json"), path.join(staged, "keys.json"));
  // The framework version, recorded in every bundle the BFF freezes for a deploy.
  fs.copyFileSync(path.join(ORCH_ROOT, "VERSION"), path.join(staged, "VERSION"));
  // The Terraform path's deploy permissions: bff/accounts.py builds a connected
  // account's deploy role from them instead of granting AdministratorAccess.
  fs.copyFileSync(path.join(ORCH_ROOT, "terraform", "deploy-role-policy.json"),
    path.join(staged, "deploy-role-policy.json"));
  return staged;
}

/**
 * The `tools` block projected down to what the APP needs: how to call each tool.
 *
 * Deploy-time detail (endpoint, policy, schema, credentials) is deliberately left
 * out — the IaC consumes that, and the app must never send it in a request. Mirrors
 * `tools_env` in terraform/tools.tf; `cdk/test/parity.test.ts` asserts the key sets
 * agree, because a field present on one path and absent on the other is a config
 * key that silently does nothing on half your deployments.
 */
export function toolsEnv(tools: Record<string, ToolSpec>): Record<string, any> {
  const out: Record<string, any> = {};
  for (const [name, t] of Object.entries(tools)) {
    const spec: Record<string, any> = { type: t.type };
    if (t.type === "kb") spec.corpora = t.corpora ?? [];
    if (t.type === "websearch") {
      spec.maxResults = t.maxResults ?? keyDefaultFor<number>("tool", "maxResults", "websearch");
      // `domains` is deliberately NOT projected: it is applied on the Gateway target,
      // so the runtime neither needs it nor should be able to send one of its own.
      if (t.publishedFrom) spec.publishedFrom = t.publishedFrom;
      if (t.publishedTo) spec.publishedTo = t.publishedTo;
    }
    if (t.call) spec.call = t.call;
    if (t.arg) spec.arg = t.arg;
    if (t.args && Object.keys(t.args).length) spec.args = t.args;
    // rowFields: which result-row field carries which role, for an agent that reads
    // the tool's DATA rather than its prose rendering (ctx.call_tool_rows). Keeps a
    // deterministic agent config-driven: repoint the tool at another source and set
    // these to its field names, with no agent code change.
    if (t.rowFields && Object.keys(t.rowFields).length) spec.rowFields = t.rowFields;
    // rowPath: WHERE those rows are in the response, for a target that nests them
    // somewhere the framework does not probe for. Projected alongside rowFields
    // because the two are useless apart — field names for rows you cannot find.
    if (t.rowPath) spec.rowPath = t.rowPath;
    out[name] = spec;
  }
  return out;
}

