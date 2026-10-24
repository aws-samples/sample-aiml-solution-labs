/**
 * AgentCore Harness agents (agents with runtime "harness", app/common/harness_agent.py).
 * A harness is an agent with no code: AgentCore runs the model loop and its session from
 * configuration. One AWS::BedrockAgentCore::Harness per harness agent, built from its
 * workflow.json entry:
 *   system prompt  <- instructions
 *   model          <- model (else the deployment default), temperature, topP, maxTokens, on
 *                     harness.provider: Bedrock (default), or OpenAI / Gemini / LiteLLM with
 *                     harness.apiKey (an apikey identity -> a credential provider made here,
 *                     or a credential provider ARN)
 *   limits         <- harness.maxIterations, harness.timeoutSeconds
 *   tools          <- `tool` (the build's Gateway, through an OAuth2 credential provider
 *                     holding the agent's Gateway machine client), harness.mcp,
 *                     harness.browser, harness.codeInterpreter
 *   allowedTools   <- exactly those, plus the built-in shell and file tools with harness.shell
 *   skills         <- harness.skills (AWS catalog, Git, S3)
 *   memory         <- disabled (CreateHarness provisions a billed managed memory when the
 *                     field is omitted, devguide/harness-memory.html), or harness.memory:
 *                     one of the build's `memories`
 *   environment    <- harness.container, harness.mounts, harness.network
 *   endpoint       <- harness.endpoint, an AWS::BedrockAgentCore::HarnessEndpoint on the
 *                     harness's current Version, so each deploy that makes one moves it
 * Each gets its own execution role, scoped to the documented sample for a harness on the
 * public network (devguide/harness-security.html) plus, only for what it uses, that page's
 * optional statements narrowed to its resources. harness.arn brings a harness made
 * elsewhere: nothing is created for it, the orchestrator may invoke exactly that ARN. A
 * workflow with no harness agent gets nothing. Mirrors terraform/harness.tf.
 */
import * as cdk from "aws-cdk-lib";
import * as agentcore from "aws-cdk-lib/aws-bedrockagentcore";
import * as iam from "aws-cdk-lib/aws-iam";
import { Construct } from "constructs";
import { keyDefault } from "./defaults";
import { regionalModelId } from "./vocabulary";

/** allowedTools for an agent with no tools: an MCP server name the harness does not have,
 *  so it matches nothing (omitted, every tool is allowed). Same literal in harness.tf and
 *  app/common/harness_agent.py. */
export const NO_TOOLS = "@no_tools";
/** allowedTools with harness.shell: the built-in shell and file_operations tools. */
export const BUILTIN_TOOLS = "@builtin";
const NAME_RE = /^[a-zA-Z][a-zA-Z0-9_]{0,39}$/;
const HARNESS_ARN_RE = /^arn:aws[a-z-]*:bedrock-agentcore:([a-z0-9-]+):([0-9]{12}):harness\/[a-zA-Z][a-zA-Z0-9_]{0,39}-[a-zA-Z0-9]{10}$/;

/** The workflow's harness agents, by id: the ones this deployment creates. */
export function harnessAgentsOf(workflow: any): Record<string, any> {
  return Object.fromEntries(Object.entries<any>(workflow?.agents ?? {})
    .filter(([, a]) => a?.runtime === "harness" && !a?.harness?.arn));
}
/** Harnesses brought from elsewhere (harness.arn), by agent id. */
export function broughtHarnessesOf(workflow: any): Record<string, { arn: string; endpoint?: string }> {
  return Object.fromEntries(Object.entries<any>(workflow?.agents ?? {})
    .filter(([, a]) => a?.runtime === "harness" && a?.harness?.arn)
    .map(([id, a]) => [id, { arn: String(a.harness.arn), ...(a.harness.endpoint ? { endpoint: String(a.harness.endpoint) } : {}) }]));
}

export const harnessName = (agentName: string, id: string) => `${agentName}_${id}`;
export const harnessRoleName = (agentName: string, id: string) => `AgentCoreHarness-${agentName}-${id}`;

/** One harness agent's settings, in one shape whatever the entry holds (harness.tf harness_spec). */
export function harnessSpec(a: any) {
  const h = a?.harness ?? {};
  const arnOr = (v: unknown) => (typeof v === "string" && v.startsWith("arn:") ? v : "");
  return {
    provider: String(h.provider ?? "bedrock").toLowerCase(),
    tools: [a?.tool ?? []].flat().filter(Boolean).map(String) as string[],
    mcp: (h.mcp ?? []).map((m: any) => ({ name: String(m.name), url: String(m.url) })) as { name: string; url: string }[],
    browser: h.browser === true || arnOr(h.browser) !== "",
    browserArn: arnOr(h.browser),
    code: h.codeInterpreter === true || arnOr(h.codeInterpreter) !== "",
    codeArn: arnOr(h.codeInterpreter),
    skills: (h.skills ?? []).map((s: any) => ({
      kind: "aws" in s ? "aws" : "git" in s ? "git" : "s3",
      paths: (s.aws ?? []).map(String) as string[], url: String(s.git ?? ""), path: String(s.path ?? ""),
      credential: String(s.credential ?? ""), uri: String(s.s3 ?? ""),
    })) as { kind: string; paths: string[]; url: string; path: string; credential: string; uri: string }[],
    memory: String(h.memory ?? ""),
    container: String(h.container ?? ""),
    mounts: (h.mounts ?? []).map((m: any) => ({
      kind: "session" in m ? "session" : "efs" in m ? "efs" : "s3Files",
      path: String(m.session ?? m.path ?? ""), arn: String(m.efs ?? m.s3Files ?? ""),
    })) as { kind: string; path: string; arn: string }[],
    subnets: (h.network?.subnetIds ?? []).map(String) as string[],
    groups: (h.network?.securityGroupIds ?? []).map(String) as string[],
    endpoint: String(h.endpoint ?? ""),
    apiKey: String(h.apiKey ?? ""),
    apiBase: String(h.apiBase ?? ""),
  };
}

/** allowedTools, in the order app/common/harness_agent.allowed_tools gives them. */
export function harnessTools(a: any): string[] {
  const s = harnessSpec(a);
  const out = [
    ...(a?.harness?.shell === true ? [BUILTIN_TOOLS] : []),
    ...s.tools.flatMap((k) => [...new Set([`@gateway/${k}___*`, `@gateway/${k.toLowerCase()}___*`])]),
    ...s.mcp.map((m) => `@${m.name}`),
    ...(s.browser ? ["browser", "@browser"] : []),
    ...(s.code ? ["code_interpreter", "@code_interpreter"] : []),
    ...(s.skills.length ? ["skills", "@builtin/skills"] : []),
  ];
  return out.length ? out : [NO_TOOLS];
}

/** API key references a harness reads: its model key and Git skill credentials. */
const keyRefs = (s: ReturnType<typeof harnessSpec>) =>
  [...new Set([...(s.apiKey ? [s.apiKey] : []), ...s.skills.map((k) => k.credential).filter(Boolean)])];

/** What the plan/synth checks need beyond the workflow (harness.tf's preconditions). */
export interface HarnessEnv {
  region?: string;
  account?: string;
  /** The Gateway, when the deployment has one: how its machine client signs in. */
  gatewayFlow?: string;
  /** The build's tools, with their resolved `auth`. */
  tools?: Record<string, any>;
  identitySecrets?: Record<string, string>;
}

/** Throws at synth for a harness agent that cannot be built. Same rules as harness.tf. */
export function validateHarness(workflow: any, agentName: string, env: HarnessEnv = {}): void {
  const inRange = (v: unknown, high: number) => v === undefined || (typeof v === "number" && v >= 1 && v <= high);
  const agents = harnessAgentsOf(workflow);
  const bad = Object.entries(agents).filter(([, a]) => !String(a.instructions ?? "").trim()
    || !inRange(a.harness?.maxIterations, 100) || !inRange(a.harness?.timeoutSeconds, 900)).map(([id]) => id);
  if (bad.length) {
    throw new Error(`app/workflow.json harness agent(s) ${bad.join(", ")}: a harness agent needs \`instructions\`, `
      + "and harness.maxIterations is 1-100, harness.timeoutSeconds 1-900.");
  }
  const names = Object.keys(agents).filter((id) => !NAME_RE.test(harnessName(agentName, id)) || harnessRoleName(agentName, id).length > 64);
  if (names.length) {
    throw new Error(`app/workflow.json harness agent(s) ${names.join(", ")}: the harness is named <agentName>_<agent id>, which must be at `
      + "most 40 letters, digits and underscores, starting with a letter (and its role AgentCoreHarness-<agentName>-<id> at most 64 "
      + "characters). Shorten agentName or the agent id.");
  }
  const tools = env.tools ?? {};
  const gateway = Object.entries(agents).filter(([, a]) => {
    const s = harnessSpec(a);
    return s.tools.length && (env.gatewayFlow !== "cognito"
      || s.tools.some((k) => !tools[k] || ["user", "obo"].includes(String(tools[k].auth ?? ""))));
  }).map(([id]) => id);
  if (gateway.length) {
    throw new Error(`app/workflow.json harness agent(s) ${gateway.join(", ")}: a harness agent's \`tool\` needs the Gateway `
      + "(-c enableGateway=true) signed in with the Cognito machine client this deployment makes or is given (idp=cognito, or a "
      + 'machine pool with Okta, Auth0 or Entra), and tools of `tools` that do not act as the person (auth "user" / "obo").');
  }
  const memories = workflow?.memories ?? {};
  const refs = Object.entries(agents).filter(([, a]) => {
    const s = harnessSpec(a);
    return (s.memory && !memories[s.memory]) || keyRefs(s).some((n) => !n.startsWith("arn:") && !env.identitySecrets?.[n]);
  }).map(([id]) => id);
  if (refs.length) {
    throw new Error(`app/workflow.json harness agent(s) ${refs.join(", ")}: harness.memory must name one of \`memories\`, and an `
      + "apikey identity named by harness.apiKey or a skill's credential needs its key in the build's secrets (IDENTITY_SECRETS).");
  }
  const known = (v?: string) => v !== undefined && !cdk.Token.isUnresolved(v);
  const byo = Object.entries(broughtHarnessesOf(workflow)).filter(([, b]) => {
    const m = HARNESS_ARN_RE.exec(b.arn);
    return !m || (known(env.region) && m[1] !== env.region) || (known(env.account) && m[2] !== env.account);
  }).map(([id]) => id);
  if (byo.length) {
    throw new Error(`app/workflow.json harness agent(s) ${byo.join(", ")}: harness.arn must be a harness in this account `
      + `(${env.account}) and region (${env.region}); a harness in another account or region cannot be invoked from here.`);
  }
}

export interface HarnessPlane {
  /** agent id -> harness ARN (a token). */
  arns: Record<string, string>;
  /** What the orchestrator's role needs to invoke them; none without harness agents. */
  invoke: iam.PolicyStatement[];
}

/** The Gateway a harness agent's `tool` reaches, and the clients it may sign in with. */
export interface HarnessGateway {
  arn: string;
  flow: string;
  discoveryUrl: string;
  tokenUrl: string;
  /** The OAuth2 scope the client asks for (Cognito: gateway/invoke). */
  scope: string;
  clientId: string;
  clientSecret: string;
  /** orchestrator.gatewayIdentity "perAgent": each agent's own client. */
  agentClients: Record<string, { id: string; secret: string }>;
}

/** The harnesses, their roles, and what the orchestrator needs to invoke them. */
export function harnessAgents(scope: Construct, props: {
  workflow: any; agentName: string; modelId: string; region: string; account: string; principal: iam.IPrincipal;
  gateway?: HarnessGateway; tools?: Record<string, any>; identitySecrets?: Record<string, string>;
  /** The build's `memories`: name -> memory id. */
  namedMemories?: Record<string, string>;
}): HarnessPlane {
  const { workflow, agentName, region, account } = props;
  validateHarness(workflow, agentName, { region, account, gatewayFlow: props.gateway?.flow, tools: props.tools,
    identitySecrets: props.identitySecrets });
  const dash = agentName.replace(/_/g, "-");
  const arns: Record<string, string> = {};
  const endpoints: string[] = [];
  const secretBase = `arn:aws:secretsmanager:${region}:${account}:secret:bedrock-agentcore-identity!default`;
  // Model and private Git skill API keys made from the build's secrets, once each.
  const keyArns: Record<string, string> = {};
  for (const n of [...new Set(Object.values(harnessAgentsOf(workflow)).flatMap((a) => keyRefs(harnessSpec(a))))]) {
    if (n.startsWith("arn:")) continue;
    keyArns[n] = new agentcore.CfnApiKeyCredentialProvider(scope, `HarnessKey-${n}`, {
      name: `bedrock-agentcore-${dash}-hk-${n}`, apiKey: props.identitySecrets![n],
    }).attrCredentialProviderArn;
  }
  const keyArn = (n: string) => (n.startsWith("arn:") ? n : keyArns[n]);
  const memoryArn = (n: string) => `arn:aws:bedrock-agentcore:${region}:${account}:memory/${props.namedMemories?.[n]}`;

  for (const [id, a] of Object.entries(harnessAgentsOf(workflow))) {
    const name = harnessName(agentName, id);
    const s = harnessSpec(a);
    const role = new iam.Role(scope, `HarnessRole-${id}`, { roleName: harnessRoleName(agentName, id), assumedBy: props.principal });
    const workload = [`arn:aws:bedrock-agentcore:${region}:${account}:workload-identity-directory/default`,
      `arn:aws:bedrock-agentcore:${region}:${account}:workload-identity-directory/default/workload-identity/harness_${name}-*`];
    const vault = `arn:aws:bedrock-agentcore:${region}:${account}:token-vault/default`;

    // The Gateway: an OAuth2 credential provider holding the client the agent signs in
    // with (its own with gatewayIdentity "perAgent"), so the Gateway's Cedar policy and
    // interceptors apply to it as to the other agents. Mirrors harness_gateway in harness.tf.
    let gatewayProvider: string | undefined;
    if (s.tools.length && props.gateway) {
      const g = props.gateway;
      const client = g.agentClients[id] ?? { id: g.clientId, secret: g.clientSecret };
      gatewayProvider = new agentcore.CfnOAuth2CredentialProvider(scope, `HarnessGateway-${id}`, {
        name: `bedrock-agentcore-${dash}-hgw-${id}`,
        credentialProviderVendor: "CustomOauth2",
        oauth2ProviderConfigInput: { customOauth2ProviderConfig: {
          clientId: client.id, clientSecret: client.secret,
          oauthDiscovery: { authorizationServerMetadata: {
            issuer: cdk.Fn.select(0, cdk.Fn.split("/.well-known/openid-configuration", g.discoveryUrl)),
            tokenEndpoint: g.tokenUrl, authorizationEndpoint: g.tokenUrl } },
        } },
      }).attrCredentialProviderArn;
    }
    const refs = keyRefs(s);
    const efs = s.mounts.filter((m) => m.kind === "efs");
    const s3files = s.mounts.filter((m) => m.kind === "s3Files");
    const s3skills = s.skills.filter((k) => k.kind === "s3");
    const ecr = /^([0-9]{12})\.dkr\.ecr\.([a-z0-9-]+)\.[^/]+\/([^:@]+)/.exec(s.container);
    const statement = (sid: string, actions: string[], resources: string[], conditions?: any) =>
      new iam.PolicyStatement({ sid, actions, resources, ...(conditions ? { conditions } : {}) });
    [
      new iam.PolicyStatement({
        sid: "HarnessModelInvocation",
        actions: ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
        // Models and inference profiles only, as for a dedicated agent's role.
        resources: ["arn:aws:bedrock:*::foundation-model/*", `arn:aws:bedrock:${region}:${account}:inference-profile/*`,
          `arn:aws:bedrock:${region}:${account}:application-inference-profile/*`],
      }),
      // On the public network the harness pulls its managed image from Amazon ECR Public.
      // Neither action has a resource type, so "*"; the bearer token only for ECR Public.
      new iam.PolicyStatement({
        sid: "HarnessEcrPublicPull", actions: ["ecr-public:GetAuthorizationToken", "sts:GetServiceBearerToken"], resources: ["*"],
        conditions: { StringEquals: { "sts:AWSServiceName": "ecr-public.amazonaws.com" } },
      }),
      new iam.PolicyStatement({
        sid: "HarnessLogGroup", actions: ["logs:CreateLogGroup", "logs:DescribeLogStreams"],
        resources: [`arn:aws:logs:${region}:${account}:log-group:/aws/bedrock-agentcore/runtimes/*`],
      }),
      new iam.PolicyStatement({
        sid: "HarnessDescribeLogGroups", actions: ["logs:DescribeLogGroups"],
        resources: [`arn:aws:logs:${region}:${account}:log-group:*`],
      }),
      new iam.PolicyStatement({
        sid: "HarnessLogStream", actions: ["logs:CreateLogStream", "logs:PutLogEvents"],
        resources: [`arn:aws:logs:${region}:${account}:log-group:/aws/bedrock-agentcore/runtimes/*:log-stream:*`],
      }),
      // Span delivery to its own log group. Scope per
      // https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-permissions.html
      // (mirrors terraform/harness.tf HarnessSpanDelivery).
      new iam.PolicyStatement({
        sid: "HarnessSpanDelivery", actions: ["logs:PutResourcePolicy"],
        resources: [`arn:aws:logs:${region}:${account}:log-group:/aws/bedrock-agentcore/runtimes/*`],
      }),
      // X-Ray has no resource types for these.
      new iam.PolicyStatement({
        sid: "HarnessTracing", resources: ["*"],
        actions: ["xray:PutTraceSegments", "xray:PutTelemetryRecords", "xray:GetSamplingRules", "xray:GetSamplingTargets"],
      }),
      new iam.PolicyStatement({
        sid: "HarnessMetrics", actions: ["cloudwatch:PutMetricData"], resources: ["*"],
        conditions: { StringEquals: { "cloudwatch:namespace": "bedrock-agentcore" } },
      }),
      new iam.PolicyStatement({
        sid: "HarnessWorkloadIdentity",
        actions: ["bedrock-agentcore:GetWorkloadAccessToken", "bedrock-agentcore:GetWorkloadAccessTokenForJWT"],
        resources: workload,
      }),
      // The optional statements of devguide/harness-security.html, only for what this agent
      // uses and narrowed to its own resources. In the order harness.tf gives them.
      ...(gatewayProvider ? [
        statement("HarnessGatewayToken", ["bedrock-agentcore:GetResourceOauth2Token"], [vault, gatewayProvider, ...workload]),
        statement("HarnessGatewaySecret", ["secretsmanager:GetSecretValue"], [`${secretBase}/oauth2/bedrock-agentcore-${dash}-hgw-${id}-*`]),
      ] : []),
      ...(refs.length ? [
        statement("HarnessApiKeys", ["bedrock-agentcore:GetResourceApiKey"], [vault, ...workload, ...refs.map(keyArn)]),
        // The key's secret, named after its provider (the last part of the provider ARN).
        statement("HarnessApiKeySecrets", ["secretsmanager:GetSecretValue"], refs.map((n) => (n.startsWith("arn:")
          ? `arn:aws:secretsmanager:${n.split(":")[3]}:${n.split(":")[4]}:secret:bedrock-agentcore-identity!default/apikey/${n.split("/").pop()}-*`
          : `${secretBase}/apikey/bedrock-agentcore-${dash}-hk-${n}-*`))),
      ] : []),
      ...(s.browser ? [statement("HarnessBrowser", ["bedrock-agentcore:StartBrowserSession", "bedrock-agentcore:StopBrowserSession",
        "bedrock-agentcore:GetBrowserSession", "bedrock-agentcore:ListBrowserSessions", "bedrock-agentcore:UpdateBrowserStream",
        "bedrock-agentcore:ConnectBrowserAutomationStream", "bedrock-agentcore:ConnectBrowserLiveViewStream"],
      [s.browserArn || `arn:aws:bedrock-agentcore:${region}:aws:browser/*`])] : []),
      ...(s.code ? [statement("HarnessCodeInterpreter", ["bedrock-agentcore:StartCodeInterpreterSession",
        "bedrock-agentcore:StopCodeInterpreterSession", "bedrock-agentcore:GetCodeInterpreterSession",
        "bedrock-agentcore:ListCodeInterpreterSessions", "bedrock-agentcore:InvokeCodeInterpreter"],
      [s.codeArn || `arn:aws:bedrock-agentcore:${region}:aws:code-interpreter/*`])] : []),
      ...(s.memory ? [statement("HarnessMemory", ["bedrock-agentcore:CreateEvent", "bedrock-agentcore:DeleteEvent",
        "bedrock-agentcore:GetEvent", "bedrock-agentcore:ListEvents", "bedrock-agentcore:RetrieveMemoryRecords"], [memoryArn(s.memory)])] : []),
      // The skill objects under each prefix, and the listing of their buckets.
      ...(s3skills.length ? [
        statement("HarnessSkillObjects", ["s3:GetObject"], s3skills.map((k) => `arn:aws:s3:::${k.uri.replace(/^s3:\/\//, "")}*`)),
        statement("HarnessSkillBuckets", ["s3:ListBucket"], [...new Set(s3skills.map((k) => `arn:aws:s3:::${k.uri.replace(/^s3:\/\//, "").split("/")[0]}`))]),
      ] : []),
      // Your image's repository only.
      ...(ecr ? [statement("HarnessContainerPull", ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"],
        [`arn:aws:ecr:${ecr[2]}:${ecr[1]}:repository/${ecr[3]}`])] : []),
      // In a VPC the harness pulls its managed image from the region's private repository,
      // owned by an AWS service account (devguide/harness-security.html).
      ...(s.subnets.length ? [statement("HarnessVpcImagePull", ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer",
        "ecr:BatchCheckLayerAvailability"], [`arn:aws:ecr:${region}:*:repository/harness-*`])] : []),
      // ecr:GetAuthorizationToken has no resource type.
      ...(ecr || s.subnets.length ? [statement("HarnessEcrToken", ["ecr:GetAuthorizationToken"], ["*"])] : []),
      // Mount through exactly these access points (the file system is the access point's).
      ...(efs.length ? [statement("HarnessEfsMounts", ["elasticfilesystem:ClientMount", "elasticfilesystem:ClientWrite"],
        [...new Set(efs.map((m) => `arn:aws:elasticfilesystem:${m.arn.split(":")[3]}:${m.arn.split(":")[4]}:file-system/*`))],
        { StringEquals: { "elasticfilesystem:AccessPointArn": efs.map((m) => m.arn) } })] : []),
      ...(s3files.length ? [statement("HarnessS3FilesMounts", ["s3files:ClientMount", "s3files:ClientWrite", "s3files:GetAccessPoint"],
        [...new Set(s3files.map((m) => m.arn.split("/access-point/")[0]))],
        { StringEquals: { "s3files:AccessPointArn": s3files.map((m) => m.arn) } })] : []),
    ].forEach((st) => role.addToPolicy(st));

    // The framework's default temperature, except with topP alone: several Claude models
    // refuse both, and an in-process agent drops the temperature in that case too.
    const temperature = a.temperature ?? (a.topP === undefined ? keyDefault<number>("agent", "temperature") : undefined);
    const maxTokens = a.maxTokens ?? keyDefault<number>("agent", "maxTokens");
    // Another provider's model: its own id, the key from AgentCore Identity, and only the
    // sampling settings the entry gives (their defaults are the provider's).
    const sampling = { maxTokens, ...(a.temperature !== undefined ? { temperature: a.temperature } : {}),
      ...(a.topP !== undefined ? { topP: a.topP } : {}) };
    const model: agentcore.CfnHarness.HarnessModelConfigurationProperty =
      s.provider === "openai" ? { openAiModelConfig: { modelId: String(a.model), apiKeyArn: keyArn(s.apiKey), ...sampling } }
      : s.provider === "gemini" ? { geminiModelConfig: { modelId: String(a.model), apiKeyArn: keyArn(s.apiKey), ...sampling } }
      : s.provider === "litellm" ? { liteLlmModelConfig: { modelId: String(a.model), ...(s.apiKey ? { apiKeyArn: keyArn(s.apiKey) } : {}),
        ...(s.apiBase ? { apiBase: s.apiBase } : {}), ...sampling } }
      : { bedrockModelConfig: {
        modelId: a.model ? regionalModelId(String(a.model), region) : props.modelId,
        maxTokens,
        ...(temperature !== undefined ? { temperature } : {}),
        ...(a.topP !== undefined ? { topP: a.topP } : {}),
      } };
    // Tools, in the order harness.tf gives them: the Gateway, MCP servers, browser, code interpreter.
    const tools: agentcore.CfnHarness.HarnessToolProperty[] = [
      ...(gatewayProvider ? [{ type: "agentcore_gateway", name: "gateway", config: { agentCoreGateway: {
        gatewayArn: props.gateway!.arn,
        outboundAuth: { oauth: { providerArn: gatewayProvider, scopes: [props.gateway!.scope], grantType: "CLIENT_CREDENTIALS" } },
      } } }] : []),
      ...s.mcp.map((m) => ({ type: "remote_mcp", name: m.name, config: { remoteMcp: { url: m.url } } })),
      ...(s.browser ? [{ type: "agentcore_browser", name: "browser",
        config: { agentCoreBrowser: s.browserArn ? { browserArn: s.browserArn } : {} } }] : []),
      ...(s.code ? [{ type: "agentcore_code_interpreter", name: "code_interpreter",
        config: { agentCoreCodeInterpreter: s.codeArn ? { codeInterpreterArn: s.codeArn } : {} } }] : []),
    ];
    const skills: agentcore.CfnHarness.HarnessSkillProperty[] = s.skills.map((k) => (k.kind === "aws"
      // No paths: every skill in the AWS catalog.
      ? { awsSkills: k.paths.length ? { paths: k.paths } : {} }
      : k.kind === "git"
        ? { git: { url: k.url, ...(k.path ? { path: k.path } : {}), ...(k.credential ? { auth: { credentialArn: keyArn(k.credential) } } : {}) } }
        : { s3: { uri: k.uri } }));
    const environment = s.mounts.length || s.subnets.length ? { agentCoreRuntimeEnvironment: {
      ...(s.subnets.length ? { networkConfiguration: { networkMode: "VPC", networkModeConfig: { securityGroups: s.groups, subnets: s.subnets } } } : {}),
      ...(s.mounts.length ? { filesystemConfigurations: s.mounts.map((m) => (m.kind === "session"
        ? { sessionStorage: { mountPath: m.path } }
        : m.kind === "efs" ? { efsAccessPoint: { accessPointArn: m.arn, mountPath: m.path } }
          : { s3FilesAccessPoint: { accessPointArn: m.arn, mountPath: m.path } })) } : {}),
    } } : undefined;
    const harness = new agentcore.CfnHarness(scope, `Harness-${id}`, {
      harnessName: name,
      executionRoleArn: role.roleArn,
      systemPrompt: [{ text: String(a.instructions) }],
      model,
      // One of the build's memories (the harness derives retrieval from its strategies), else none.
      memory: s.memory ? { agentCoreMemoryConfiguration: { arn: memoryArn(s.memory) } } : { disabled: {} },
      allowedTools: harnessTools(a),
      ...(a.harness?.maxIterations !== undefined ? { maxIterations: a.harness.maxIterations } : {}),
      ...(a.harness?.timeoutSeconds !== undefined ? { timeoutSeconds: a.harness.timeoutSeconds } : {}),
      ...(tools.length ? { tools } : {}),
      ...(skills.length ? { skills } : {}),
      ...(environment ? { environment } : {}),
      ...(s.container ? { environmentArtifact: { containerConfiguration: { containerUri: s.container } } } : {}),
    });
    // The harness validates its role when it is created.
    harness.node.addDependency(role);
    arns[id] = harness.attrArn;
    if (s.endpoint) {
      // On the version CloudFormation reports, so an update that makes a new version moves it.
      new agentcore.CfnHarnessEndpoint(scope, `HarnessEndpoint-${id}`, {
        harnessId: harness.attrHarnessId, endpointName: s.endpoint, targetVersion: harness.attrVersion,
        description: "Follows each version this deployment makes",
      });
      endpoints.push(`${harness.attrArn}/harness-endpoint/${s.endpoint}`);
    }
  }
  const brought = Object.values(broughtHarnessesOf(workflow));
  const resources = [...Object.values(arns), ...endpoints, ...brought.map((b) => b.arn),
    ...brought.filter((b) => b.endpoint).map((b) => `${b.arn}/harness-endpoint/${b.endpoint}`)];
  // InvokeHarness needs InvokeHarness AND InvokeAgentRuntime on the harness ARN, and the
  // endpoint ARN too for a named endpoint (devguide/harness-security.html, "Required IAM
  // permissions for callers").
  const invoke = resources.length ? [new iam.PolicyStatement({
    sid: "InvokeHarnessAgents", actions: ["bedrock-agentcore:InvokeHarness", "bedrock-agentcore:InvokeAgentRuntime"],
    resources,
  })] : [];
  return { arns, invoke };
}
