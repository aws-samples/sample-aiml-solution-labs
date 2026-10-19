/**
 * The Browser tool (`source: "browser"` -> app/tools/browser/): a Gateway Lambda target that
 * opens a web page in the AWS-managed AgentCore Browser and returns its text.
 *
 * The function drives the browser over the session's automation stream (Chrome DevTools
 * Protocol over a SigV4-signed WebSocket), so its role may start a session on the system
 * browser, connect to its automation stream and stop it, and write to its own log group.
 * Nothing else: no table, bucket or secret. Mirrors terraform/browser_tool.tf (the policy)
 * and the shared built-in function resources in terraform/tools.tf.
 *   https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/browser-resource-session-management.html
 */
import * as path from "path";
import * as cdk from "aws-cdk-lib";
import { aws_iam as iam, aws_lambda as lambda, aws_logs as logs } from "aws-cdk-lib";
import { Construct } from "constructs";

export const BROWSER_SOURCE = "browser";

/** The policy statements of the function's own role, besides its log writes. */
export function browserStatements(region: string): iam.PolicyStatement[] {
  return [
    // The AWS-managed ("system") browser, whose account field is the literal `aws`.
    // Resource form: AgentCore Browser "Resource management" (System ARNs).
    new iam.PolicyStatement({
      sid: "UseSystemBrowser",
      actions: ["bedrock-agentcore:StartBrowserSession", "bedrock-agentcore:StopBrowserSession"],
      resources: [`arn:aws:bedrock-agentcore:${region}:aws:browser/aws.browser.v1`],
    }),
    // "*" because the action has no resource type: the Service Authorization Reference
    // lists ConnectBrowserAutomationStream with an empty resource column, so a statement
    // naming the browser's ARN would not match it. The function only connects to the
    // session it started a moment before, on the system browser.
    // https://docs.aws.amazon.com/service-authorization/latest/reference/list_bedrock-agentcore.html
    new iam.PolicyStatement({
      sid: "DriveTheSessionItStarted",
      actions: ["bedrock-agentcore:ConnectBrowserAutomationStream"],
      resources: ["*"],
    }),
  ];
}

/** The function for tool `name`, named and logged like the other built-in ones
 *  (ToolLambda-<agentName>-<name>, /aws/lambda/ToolLambda-<agentName>-<name>). */
export function browserToolFunction(scope: Construct, name: string, allowDomains: string[] | undefined,
  agentName: string, orchRoot: string, retention: logs.RetentionDays): lambda.Function {
  const logGroup = new logs.LogGroup(scope, `ToolLambdaLogGroup-${name}`, {
    logGroupName: `/aws/lambda/ToolLambda-${agentName}-${name}`,
    retention,
    removalPolicy: cdk.RemovalPolicy.DESTROY,
  });
  // Not the default role: AWSLambdaBasicExecutionRole can create log groups anywhere.
  const role = new iam.Role(scope, `ToolLambdaRole-${name}`, {
    assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
  });
  logGroup.grantWrite(role);
  const domains = allowDomains ?? [];
  const fn = new lambda.Function(scope, `ToolLambda-${name}`, {
    logGroup,
    role,
    functionName: `ToolLambda-${agentName}-${name}`,
    runtime: lambda.Runtime.PYTHON_3_12,
    handler: "handler.lambda_handler",
    code: lambda.Code.fromAsset(path.join(orchRoot, "app", "tools", BROWSER_SOURCE)),
    // A page may take up to 40 s (CALL_SECONDS in the handler) plus starting and stopping
    // the session. Mirrors terraform/tools.tf.
    timeout: cdk.Duration.seconds(60),
    memorySize: 256,
    // The domains it may open, only when some are named. Mirrors terraform/tools.tf.
    ...(domains.length ? { environment: { ALLOW_DOMAINS: domains.join(",") } } : {}),
  });
  for (const s of browserStatements(cdk.Stack.of(scope).region)) fn.addToRolePolicy(s);
  return fn;
}
