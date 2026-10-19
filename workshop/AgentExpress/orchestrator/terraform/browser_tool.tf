# --- The Browser tool (`source: "browser"` -> app/tools/browser/) --------------
# Opens a web page in the AWS-managed AgentCore Browser and returns its text. Its zip,
# role, log group and function are the shared built-in ones in tools.tf (for_each over
# local.builtin_lambda_tools); this file is its role's policy. Nothing here exists unless
# a tool names `source: "browser"`. Mirrors cdk/lib/browser-tool.ts.
#
# The function drives the browser over the session's automation stream (Chrome DevTools
# Protocol over a SigV4-signed WebSocket), so it needs three actions: start a session,
# connect to its automation stream, stop it. It reads nothing of this deployment's: no
# table, bucket or secret.
#   https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/browser-resource-session-management.html
resource "aws_iam_role_policy" "tool_lambda_browser" {
  for_each = local.builtin_lambda_browser_tools
  name     = "ToolLambdaBrowser"
  role     = aws_iam_role.tool_lambda[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # The log group is created in tools.tf (aws_cloudwatch_log_group.tool), so the
        # function needs to write streams to it and nothing else.
        Sid      = "Logs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.tool[each.key].arn}:*"
      },
      {
        # The AWS-managed ("system") browser, whose account field is the literal `aws`.
        # Resource form: AgentCore Browser "Resource management" (System ARNs).
        Sid      = "UseSystemBrowser"
        Effect   = "Allow"
        Action   = ["bedrock-agentcore:StartBrowserSession", "bedrock-agentcore:StopBrowserSession"]
        Resource = ["arn:aws:bedrock-agentcore:${var.region}:aws:browser/aws.browser.v1"]
      },
      {
        # "*" because the action has no resource type: the Service Authorization
        # Reference lists ConnectBrowserAutomationStream with an empty resource column,
        # so a statement naming the browser's ARN would not match it. The function only
        # connects to the session it started a moment before, on the system browser.
        # https://docs.aws.amazon.com/service-authorization/latest/reference/list_bedrock-agentcore.html
        Sid      = "DriveTheSessionItStarted"
        Effect   = "Allow"
        Action   = ["bedrock-agentcore:ConnectBrowserAutomationStream"]
        Resource = "*"
      },
    ]
  })
}
