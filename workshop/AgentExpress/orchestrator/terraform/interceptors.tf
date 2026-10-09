# --- Gateway interceptors (orchestrator.interceptors) ----------------------------------
# A Lambda the Gateway calls before each MCP request reaches a tool (REQUEST) and/or
# after each tool answers (RESPONSE): at most one of each per Gateway. One written in
# the build (`code`) is deployed by tools_code.tf exactly like a code tool, as
# ToolLambda-<agent>-interceptor-<point> from app/tools/_code/interceptor-<point>/; one
# you own (`lambdaArn`) is only registered. The Gateway invokes it with its own role,
# which may invoke exactly these (gateway.tf). Mirrors the interceptors in
# cdk/lib/tool-plane.ts and interceptorsOf in cdk/lib/orchestrator-stack.ts.
locals {
  # try(), not a conditional: `{}` and the decoded object are different types.
  interceptors_cfg = try(local.workflow_def.orchestrator.interceptors, {})
  # Only with a Gateway to put them on: a build with no tools deploys none.
  interceptor_specs = { for p in ["request", "response"] : p => local.interceptors_cfg[p]
  if local.gateway_enabled && try(local.interceptors_cfg[p], null) != null }
  interceptor_has_code = { for p, c in local.interceptor_specs : p => try(c.code, null) != null }
  # Read by tools_code.tf (local.code_functions), keyed as its folder is named.
  code_interceptors = { for p, c in local.interceptor_specs : "interceptor-${p}" => { code = c.code }
  if local.interceptor_has_code[p] }
  interceptor_arns = { for p, c in local.interceptor_specs : p => (
    local.interceptor_has_code[p] ? try(aws_lambda_function.tool_code["interceptor-${p}"].arn, "") : try(c.lambdaArn, "")
  ) }
  # A function's resource policy can only be edited from its own account, so the
  # service permission is added for functions here only (yours elsewhere: add it there).
  # Keyed from the config alone, so the set is known at plan time.
  interceptor_local_arns = { for p, c in local.interceptor_specs : p => local.interceptor_arns[p]
  if local.interceptor_has_code[p] || try(split(":", c.lambdaArn)[4], "") == local.account_id }
  # What the Gateway role may invoke (gateway.tf InvokeInterceptors).
  interceptor_statements = length(local.interceptor_arns) == 0 ? [] : [{
    Sid      = "InvokeInterceptors"
    Effect   = "Allow"
    Action   = ["lambda:InvokeFunction"]
    Resource = [for p in ["request", "response"] : local.interceptor_arns[p] if contains(keys(local.interceptor_arns), p)]
  }]
}

resource "terraform_data" "interceptors_check" {
  count = length(local.interceptor_specs) == 0 ? 0 : 1
  lifecycle {
    precondition {
      condition = alltrue([for p, c in local.interceptor_specs :
      local.interceptor_has_code[p] != (try(c.lambdaArn, "") != "")])
      error_message = "orchestrator.interceptors.<point> needs EXACTLY ONE of \"code\" (a function written in the build, in app/tools/_code/interceptor-<point>/) or \"lambdaArn\" (a function you own). Offending: ${join(", ", [for p, c in local.interceptor_specs : p if local.interceptor_has_code[p] == (try(c.lambdaArn, "") != "")])}."
    }
    precondition {
      condition = alltrue([for p, c in local.interceptor_specs :
      local.interceptor_has_code[p] || can(regex("^arn:aws[a-z-]*:lambda:[a-z0-9-]+:[0-9]{12}:function:[a-zA-Z0-9-_]+(:[a-zA-Z0-9-_$]+)?$", try(c.lambdaArn, "")))])
      error_message = "orchestrator.interceptors.<point>.lambdaArn must be a Lambda function ARN: arn:aws:lambda:<region>:<account>:function:<name>."
    }
    precondition {
      condition = alltrue([for p, has in local.interceptor_has_code :
      !has || fileexists("${path.module}/../app/tools/_code/interceptor-${p}/handler.py")])
      error_message = "An interceptor written in the build (\"code\") needs app/tools/_code/interceptor-<point>/handler.py, which scaffold.py apply writes from the build's bundle."
    }
  }
}

resource "aws_lambda_permission" "gateway_invoke_interceptor" {
  for_each       = local.interceptor_local_arns
  statement_id   = "AllowAgentCoreGatewayInterceptor-${var.agent_name}"
  action         = "lambda:InvokeFunction"
  function_name  = each.value
  principal      = "bedrock-agentcore.amazonaws.com"
  source_account = local.account_id
}
