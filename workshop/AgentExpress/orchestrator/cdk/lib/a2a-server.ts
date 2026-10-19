/**
 * This workflow, served over A2A to other teams' agents (bff/a2a_server.py). Only with
 * `-c exposeA2a=true`. Mirrors terraform/a2a_server.tf.
 *
 * Callers are machines and sign in with the deployment's identity provider, never with a
 * key minted here. The two routes have their OWN JWT authorizer, whose audience is the A2A
 * client(s) only, and they require the A2A scope:
 *   - a person's token (the SPA client's) has the wrong audience and no such scope, so it
 *     is refused on these routes;
 *   - an A2A client's token has the wrong audience for the main authorizer, so it is
 *     refused on every other route (and bff/handler.py refuses it there too).
 */
import * as cdk from "aws-cdk-lib";
import * as cognito from "aws-cdk-lib/aws-cognito";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { HttpApi, HttpMethod, HttpRouteIntegration } from "aws-cdk-lib/aws-apigatewayv2";
import { HttpJwtAuthorizer } from "aws-cdk-lib/aws-apigatewayv2-authorizers";
import { Construct } from "constructs";

/** The route keys, as terraform/a2a_server.tf and bff/a2a_server.py ROUTES spell them. */
export const A2A_ROUTES = ["GET /api/a2a/.well-known/agent-card.json", "POST /api/a2a"];
/** The scope of the A2A client a created Cognito pool gets (resource server "a2a"). */
export const A2A_SCOPE = "a2a/invoke";

/** `-c a2aIdentity='{...}'`: the A2A callers' identity when this stack does not create the
 *  Cognito pool. Terraform's var.a2a_identity, in camelCase. */
export interface A2aIdentity {
  audience?: string;
  scope?: string;
  clientIds?: string[];
  issuer?: string;
  tokenUrl?: string;
}

export interface A2aServerOptions {
  api: HttpApi;
  integration: HttpRouteIntegration;
  bff: lambda.Function;
  agentName: string;
  region: string;
  authEnabled: boolean;
  workflowPlane: boolean;
  /** idp=cognito with createCognito: the pool to add the A2A client to. */
  createCognito: boolean;
  userPoolId: string;
  domainPrefix: string;
  jwtIssuer: string;
  /** The sign-in client's audience, which the A2A audience must not be. */
  jwtAudience: string;
  identity?: A2aIdentity;
  approvers?: string[];
}

/** Why `-c exposeA2a=true` cannot deploy as configured, or undefined. The twins of the
 *  preconditions on aws_apigatewayv2_authorizer.a2a. */
export function a2aProblem(o: Pick<A2aServerOptions, "authEnabled" | "workflowPlane" | "createCognito" | "jwtAudience" | "identity">): string | undefined {
  const id = o.identity ?? {};
  if (!o.authEnabled) {
    return 'exposeA2a needs a sign-in provider: with idp=none the A2A routes would have no authorizer. '
      + "Use idp=cognito (or another provider with -c a2aIdentity).";
  }
  if (!o.workflowPlane) {
    return "exposeA2a serves this deployment's workflow, and a control-plane console (consoleMode builder) "
      + "runs none. Expose it from the build's own app.";
  }
  if (!o.createCognito && !(id.audience && id.scope && (id.clientIds ?? []).length)) {
    return "exposeA2a without a Cognito pool this deployment creates needs -c a2aIdentity='{\"audience\":...,"
      + "\"scope\":...,\"clientIds\":[...]}' from your identity provider: the audience and scope of the machine "
      + "tokens your A2A callers get, and their client ids.";
  }
  if (!o.createCognito && id.audience === o.jwtAudience) {
    return "a2aIdentity.audience must not be the sign-in client's audience: people's tokens would then pass "
      + "the A2A routes' authorizer.";
  }
  return undefined;
}

export function addA2aServer(scope: Construct, o: A2aServerOptions): void {
  const problem = a2aProblem(o);
  if (problem) throw new Error(problem);
  const id = o.identity ?? {};
  let audience = id.audience ?? "";
  let clientIds = id.clientIds ?? [];
  let a2aScope = id.scope ?? "";
  let tokenUrl = id.tokenUrl ?? "";
  if (o.createCognito) {
    const pool = cognito.UserPool.fromUserPoolId(scope, "A2aUserPool", o.userPoolId);
    const invoke = new cognito.ResourceServerScope({
      scopeName: "invoke", scopeDescription: "Start, read and cancel runs of this workflow over A2A",
    });
    const server = new cognito.UserPoolResourceServer(scope, "A2aResourceServer", {
      userPool: pool, identifier: "a2a", userPoolResourceServerName: `${o.agentName}-a2a`, scopes: [invoke],
    });
    // A CONFIDENTIAL client: client credentials require a secret. The secret stays in
    // Cognito; it is not an output and is not given to the BFF, which needs only the id.
    const client = new cognito.UserPoolClient(scope, "A2aClient", {
      userPool: pool, userPoolClientName: `${o.agentName}-a2a`, generateSecret: true, authFlows: {},
      oAuth: { flows: { clientCredentials: true }, scopes: [cognito.OAuthScope.resourceServer(server, invoke)] },
      supportedIdentityProviders: [cognito.UserPoolClientIdentityProvider.COGNITO],
    });
    audience = client.userPoolClientId;
    clientIds = [client.userPoolClientId];
    a2aScope = A2A_SCOPE;
    tokenUrl = `https://${o.domainPrefix}.auth.${o.region}.amazoncognito.com/oauth2/token`;
    new cdk.CfnOutput(scope, "a2aClientId", { value: client.userPoolClientId });
  }
  // API Gateway matches the audience against the token's `aud` or, for an access token
  // without one (Cognito's), its `client_id`.
  const authorizer = new HttpJwtAuthorizer("A2aJwtAuthorizer", o.createCognito ? o.jwtIssuer : id.issuer || o.jwtIssuer, {
    jwtAudience: [audience], authorizerName: `a2a-${o.agentName}`,
  });
  for (const key of A2A_ROUTES) {
    const [method, routePath] = key.split(" ");
    o.api.addRoutes({
      path: routePath, methods: [method as HttpMethod], integration: o.integration,
      authorizer, authorizationScopes: [a2aScope],
    });
  }
  o.bff.addEnvironment("A2A_ENABLED", "true");
  o.bff.addEnvironment("A2A_CLIENT_IDS", cdk.Stack.of(scope).toJsonString(clientIds));
  o.bff.addEnvironment("A2A_SCOPE", a2aScope);
  o.bff.addEnvironment("A2A_TOKEN_URL", tokenUrl);
  o.bff.addEnvironment("A2A_APPROVERS", JSON.stringify(o.approvers ?? []));
  new cdk.CfnOutput(scope, "a2aEndpoint", { value: `${o.api.apiEndpoint}/api/a2a` });
  new cdk.CfnOutput(scope, "a2aTokenUrl", { value: tokenUrl || "(from your identity provider)" });
}
