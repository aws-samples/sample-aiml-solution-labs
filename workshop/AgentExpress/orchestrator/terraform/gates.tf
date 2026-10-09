# --- Review gates answered without the run page (steps[].hitl, bff/gates.py) -------------
# A gate with approval "event" asks on the account's default bus (the runtime may put
# events there) and takes an "AgentExpress Approval Decision" event for this app as its
# answer; a gate with a timeout is decided by a sweep every five minutes. Created only
# when a step asks for them. Mirrors gateSteps and the GateDecisions / GateTimeouts rules
# in cdk/lib/orchestrator-stack.ts.
locals {
  gate_specs    = [for s in try(local.workflow_def.steps, []) : s.hitl if can(keys(s.hitl))]
  gate_on       = local.workflow_plane && var.console_mode != "builder"
  gate_events   = local.gate_on && length([for h in local.gate_specs : h if try(h.approval, "") == "event"]) > 0
  gate_timeouts = local.gate_on && length([for h in local.gate_specs : h if can(keys(h.timeout))]) > 0
}

resource "aws_iam_role_policy" "runtime_gate_events" {
  count = local.gate_events ? 1 : 0
  name  = "gate-approval-requests"
  role  = aws_iam_role.runtime[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "GateApprovalRequests"
      Effect   = "Allow"
      Action   = ["events:PutEvents"]
      Resource = "arn:aws:events:${var.region}:${local.account_id}:event-bus/default"
    }]
  })
}

resource "aws_cloudwatch_event_rule" "gate_decisions" {
  count = local.gate_events ? 1 : 0
  name  = "AgentCoreTrigger-${var.agent_name}-gatedecisions"
  event_pattern = jsonencode({
    "detail-type" = ["AgentExpress Approval Decision"]
    detail        = { app = [var.agent_name] }
  })
}

resource "aws_cloudwatch_event_target" "gate_decisions" {
  count = local.gate_events ? 1 : 0
  rule  = aws_cloudwatch_event_rule.gate_decisions[0].name
  arn   = aws_lambda_function.bff.arn
  input_transformer {
    input_paths    = { e = "$" }
    input_template = "{\"axGate\": \"decision\", \"event\": <e>}"
  }
  retry_policy {
    maximum_retry_attempts       = 2
    maximum_event_age_in_seconds = 3600
  }
}

resource "aws_cloudwatch_event_rule" "gate_timeouts" {
  count               = local.gate_timeouts ? 1 : 0
  name                = "AgentCoreTrigger-${var.agent_name}-gatetimeouts"
  schedule_expression = "rate(5 minutes)"
}

resource "aws_cloudwatch_event_target" "gate_timeouts" {
  count = local.gate_timeouts ? 1 : 0
  rule  = aws_cloudwatch_event_rule.gate_timeouts[0].name
  arn   = aws_lambda_function.bff.arn
  input = jsonencode({ axGate = "sweep" })
}

resource "aws_lambda_permission" "gate_rules" {
  for_each = merge(
    local.gate_events ? { decisions = aws_cloudwatch_event_rule.gate_decisions[0].arn } : {},
    local.gate_timeouts ? { timeouts = aws_cloudwatch_event_rule.gate_timeouts[0].arn } : {},
  )
  statement_id  = "AllowGate-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.bff.function_name
  principal     = "events.amazonaws.com"
  source_arn    = each.value
}
