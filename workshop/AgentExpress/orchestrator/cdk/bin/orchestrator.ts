#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { AwsSolutionsChecks } from "cdk-nag";
import { acknowledgeNagFindings } from "../lib/nag";
import { OrchestratorStack } from "../lib/orchestrator-stack";

const app = new cdk.App();

// Config resolution order: `-c key=value` on the CLI overrides cdk.json context.
const ctx = (k: string, d?: string) => (app.node.tryGetContext(k) ?? d) as string;

const agentName = ctx("agentName", "multiagent_orchestrator");
// `-c region` first: it is the explicit choice on the command line. It used to come
// AFTER CDK_DEFAULT_REGION, which the CLI always sets from the profile, so `-c region`
// was silently ignored whenever credentials were present — i.e. on every real deploy.
const region = ctx("region") || process.env.CDK_DEFAULT_REGION || "us-east-1";

const idp = ctx("idp", "cognito");
if (!["cognito", "auth0", "okta", "entra", "none"].includes(idp)) {
  throw new Error(`idp must be one of "cognito", "auth0", "okta", "entra", "none" (got "${idp}")`);
}
// idp=none deploys the UI and /api/* with NO sign-in on a public CloudFront URL, where
// anyone who finds it can run agents on this account's Bedrock budget. Say so twice.
if (idp === "none" && String(ctx("allowUnauthenticated", "false")) !== "true") {
  throw new Error(
    'idp=none deploys the console with no authentication on a public URL. ' +
    'Add -c allowUnauthenticated=true to deploy it anyway, or use idp=cognito.');
}

/** Parse a JSON object of name -> secret from an env var. Never a context key: cdk.json
 *  is committed, and both of these carry credentials. */
function parseSecretMap(envName: string, keyedBy: string, raw?: string): Record<string, string> {
  if (!raw) return {};
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error(`${envName} must be a JSON object, e.g. '{"billing":"sk-live-..."}'.`);
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error(`${envName} must be a JSON object keyed by the workflow.json ${keyedBy}.`);
  }
  return parsed as Record<string, string>;
}

// Tags on every resource: `agentexpress:app` always, plus any from `-c tags='{"k":"v"}'`
// (the deploy runner adds agentexpress:build / :version / :owner for a build).
const tags: Record<string, string> = { "agentexpress:app": agentName, ...parseTags(ctx("tags", "")) };
for (const [k, v] of Object.entries(tags)) cdk.Tags.of(app).add(k, v);

function parseTags(raw: string): Record<string, string> {
  if (!raw) return {};
  const parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error(`-c tags must be a JSON object of tag key -> value, e.g. '{"team":"claims"}'.`);
  }
  return Object.fromEntries(Object.entries(parsed).map(([k, v]) => [k, String(v)]));
}

const stack = new OrchestratorStack(app, `${agentName.replace(/_/g, "-")}-stack`, {
  // A concrete account/region is required for the AgentCore + CloudFront resources
  // and for the Docker image asset. Falls back to us-east-1 for `cdk synth` without creds.
  env: { account: process.env.CDK_DEFAULT_ACCOUNT, region },
  description: "Multi-agent LangGraph orchestrator on Amazon Bedrock AgentCore (CDK)",
  agentName,
  // Empty = this workflow's orchestrator.defaultModel, else the framework default from
  // app/defaults.json; the stack resolves it and moves it to the region's geography.
  modelId: ctx("modelId", ""),
  memoryEventExpiryDays: Number(ctx("memoryEventExpiryDays", "30")),
  // --- Identity provider: one switch, same semantics as Terraform's `idp` ---
  //   -c idp=cognito   (default) Cognito; add -c createCognito=true to have CDK
  //                    provision the pool, or pass the cognito* ids below.
  //   -c idp=auth0     existing Auth0 tenant: -c auth0Domain=... -c auth0ClientId=...
  //   -c idp=okta      existing Okta org: -c oktaDomain=... -c oktaClientId=...
  //                    [-c oktaAuthServer=default]
  //   -c idp=entra     existing Microsoft Entra ID tenant: -c entraTenantId=<GUID>
  //                    -c entraClientId=...
  //   -c idp=none      NO authentication — the UI and /api/* deploy open.
  idp: idp,
  cognitoUserPoolId: ctx("cognitoUserPoolId", ""),
  cognitoClientId: ctx("cognitoClientId", ""),
  cognitoDomainPrefix: ctx("cognitoDomainPrefix", ""),
  auth0Domain: ctx("auth0Domain", ""),
  auth0ClientId: ctx("auth0ClientId", ""),
  oktaDomain: ctx("oktaDomain", ""),
  oktaClientId: ctx("oktaClientId", ""),
  oktaAuthServer: ctx("oktaAuthServer", "default"),
  entraTenantId: ctx("entraTenantId", ""),
  entraClientId: ctx("entraClientId", ""),
  // Only meaningful for idp=cognito.
  createCognito: idp === "cognito" && String(app.node.tryGetContext("createCognito")) === "true",
  // Open registration on the Hosted UI (email verified); new users join selfSignUpGroup.
  // Off unless asked for: -c selfSignUp=true [-c selfSignUpGroup=members]
  selfSignUp: String(app.node.tryGetContext("selfSignUp")) === "true",
  selfSignUpGroup: ctx("selfSignUpGroup", "members"),

  // --- Tool plane: AgentCore Gateway + Knowledge Base + Cedar policy --------
  // Same switch as Terraform's `enable_gateway`. true = live MCP + RAG + policy.
  // false provisions no tool plane, so any agent with a `tool` fails loudly with
  // ToolUnavailable — nothing is simulated. Only tool-less agents can run.
  enableGateway: String(app.node.tryGetContext("enableGateway")) === "true",
  // Bring-your-own M2M client (not needed for idp=cognito + createCognito=true,
  // where CDK creates the resource server + confidential client itself).
  gatewayClientId: ctx("gatewayClientId", ""),
  gatewayAudience: ctx("gatewayAudience", ""),
  // Okta: the custom scope to request (required); Entra: default "<gatewayAudience>/.default".
  gatewayScope: ctx("gatewayScope", ""),
  // Secret comes from the ENVIRONMENT, never a context key: `cdk.context.json`
  // is committed, and -c values land in cdk.out. Mirrors TF_VAR_gateway_client_secret.
  gatewayClientSecret: process.env.GATEWAY_CLIENT_SECRET ?? "",
  // API keys for tools that need one, keyed by the workflow.json tool name.
  // From the ENVIRONMENT, never context: cdk.json is committed and -c values
  // land in cdk.out. Mirrors TF_VAR_tool_api_keys.
  //   export TOOL_API_KEYS='{"billing":"sk-live-..."}'
  toolApiKeys: parseSecretMap("TOOL_API_KEYS", "tool name", process.env.TOOL_API_KEYS),
  // Each workflow.json identity's secret (an OAuth client secret or an API key).
  identitySecrets: parseSecretMap("IDENTITY_SECRETS", "identity name", process.env.IDENTITY_SECRETS),
  // orchestrator.export's headers by secret name (the Builder's exportHeaders); lib/export.ts.
  exportHeaders: parseSecretMap("EXPORT_HEADERS", "orchestrator.export.secret name", process.env.EXPORT_HEADERS),
  // Bearer tokens for `runtime: "a2a"` agents, keyed by agent id. Credentials for
  // somebody ELSE's agent, so they come from the environment and never from config.
  a2aTokens: parseSecretMap("A2A_TOKENS", "agent id", process.env.A2A_TOKENS),

  transactionSearchIndexingPercentage: Number(ctx("transactionSearchIndexingPercentage", "100")),

  // The Builder's builds store and deploy project. `-c builder=false` leaves them out;
  // the deploy runner does that for every build stack it creates.
  builder: String(ctx("builder", "true")) !== "false",
  // "builder": a control-plane console (design, build, deploy; runs live in each build's
  // app). "app" (the default): this workflow's own app.
  consoleMode: String(ctx("consoleMode", "app")),
  // A link to a product demo video, shown on a control-plane console (consoleMode=builder):
  // -c demoVideoUrl=https://... An https URL or empty (the default: no link).
  demoVideoUrl: ctx("demoVideoUrl", ""),
  // The documentation an info icon beside each feature links to, and the top bar's
  // Documentation link: -c docsUrl=https://... An https URL, or empty for none. Unset, the
  // public user guide at the release tag (lib/docs-default.ts). Mirrors var.docs_url in
  // terraform/builder.tf.
  docsUrl: app.node.tryGetContext("docsUrl") as string | undefined,
  // -c docsLinks=false: no info icons and no Documentation link anywhere. Mirrors
  // var.docs_links in terraform/builder.tf.
  docsLinks: String(ctx("docsLinks", "true")) !== "false",
  // A destroy of this stack clears the builds table's deletion protection just before
  // deleting it. -c keepBuildsTableProtected=true makes a destroy stop at the table
  // instead. Mirrors var.keep_builds_table_protected in terraform/builder.tf.
  keepBuildsTableProtected: String(app.node.tryGetContext("keepBuildsTableProtected")) === "true",
  // Buckets (or bucket/prefix) the AgentExpress Assistant may read when a message names
  // s3://... (bff/designer.py). Comma-separated; none by default.
  designerS3Buckets: String(ctx("designerS3Buckets", "")).split(",").map((s) => s.trim()).filter(Boolean),
  // S3 code sources a build in this console's own account may read (bff/code_source.py).
  codeSourceBuckets: String(ctx("codeSourceBuckets", "")).split(",").map((s) => s.trim()).filter(Boolean),
  // The AgentExpress Assistant's model and the small model that condenses long
  // conversations. Empty = bff/designer.py's defaults (Claude Sonnet 5.5; the deployment's
  // default model), in the console region's inference-profile geography.
  designerModel: ctx("designerModel", ""),
  designerSummaryModel: ctx("designerSummaryModel", ""),
  // How hard the Assistant's model reasons: medium (default) | low | high | model.
  designerEffort: ctx("designerEffort", ""),
  // The model that reviews the Assistant's changes. Empty = bff/review.py's default.
  designerReviewModel: ctx("designerReviewModel", ""),
  // Serve this workflow to other teams' agents over A2A (lib/a2a-server.ts). Off unless
  // -c exposeA2a=true. Without a created Cognito pool, -c a2aIdentity='{"audience":...,
  // "scope":...,"clientIds":[...]}' says whose machine tokens to accept. -c a2aApprovers=a,b
  // names the groups who decide A2A runs' gates. Mirrors Terraform's expose_a2a etc.
  exposeA2a: String(app.node.tryGetContext("exposeA2a")) === "true",
  a2aIdentity: parseA2aIdentity(ctx("a2aIdentity", "")),
  a2aApprovers: String(ctx("a2aApprovers", "")).split(",").map((s) => s.trim()).filter(Boolean),

  // Also send the runtimes' spans to an OTLP/HTTP backend (DEPLOYMENT.md). Off by default:
  //   -c otlpEndpoint=https://.../v1/traces -c otlpHeadersSecretArn=arn:aws:secretsmanager:...
  //   [-c otlpIncludeContent=true]   (prompt and answer content; stripped otherwise)
  // The headers stay in Secrets Manager; only the secret's ARN is a context value.
  otlpEndpoint: ctx("otlpEndpoint", ""),
  otlpHeadersSecretArn: ctx("otlpHeadersSecretArn", ""),
  otlpIncludeContent: String(app.node.tryGetContext("otlpIncludeContent")) === "true",
  // Non-Bedrock models (an agent's model litellm/<model>) through your LiteLLM proxy
  // (DEPLOYMENT.md). Off by default:
  //   -c litellmBaseUrl=https://litellm.example.com -c litellmKeySecretArn=arn:aws:secretsmanager:...
  // The virtual key stays in Secrets Manager; only the secret's ARN is a context value.
  litellmBaseUrl: ctx("litellmBaseUrl", ""),
  litellmKeySecretArn: ctx("litellmKeySecretArn", ""),

  // Organisation guardrails, all off unless set (lib/hardening.ts; terraform/hardening.tf):
  //   -c permissionsBoundaryArn=arn:aws:iam::<acct>:policy/<name>  on every role, builds' too
  //   -c iamRolePath=/agentexpress/   every role's path; changing it REPLACES the roles
  //   -c customDomain=agents.example.com -c acmCertificateArn=<us-east-1 ACM ARN>
  //   -c enableWaf=true [-c wafRateLimit=2000]   us-east-1 only
  permissionsBoundaryArn: ctx("permissionsBoundaryArn", ""),
  iamRolePath: ctx("iamRolePath", "/"),
  customDomain: ctx("customDomain", ""),
  acmCertificateArn: ctx("acmCertificateArn", ""),
  enableWaf: String(ctx("enableWaf", "false")) === "true",
  wafRateLimit: Number(ctx("wafRateLimit", "2000")),
});

function parseA2aIdentity(raw: string) {
  if (!raw) return undefined;
  const parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error(`-c a2aIdentity must be a JSON object: {"audience":...,"scope":...,"clientIds":[...]}.`);
  }
  return parsed as { audience?: string; scope?: string; clientIds?: string[]; issuer?: string; tokenUrl?: string };
}

// `-c nag=true`: run cdk-nag's AwsSolutions pack; an unacknowledged finding fails the
// synth. What is accepted, and why, is lib/nag.ts.
acknowledgeNagFindings(stack);
if (String(ctx("nag", "false")) === "true") cdk.Aspects.of(app).add(new AwsSolutionsChecks());

app.synth();
