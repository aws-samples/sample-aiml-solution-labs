# --- External triggers (orchestrator.triggers) -----------------------------------------
# What starts a run without anyone typing it. Every delivery reaches the BFF function
# (bff/triggers.py): a webhook through POST /api/hooks/{name} (bff.tf), a schedule
# through EventBridge Scheduler, an event or an S3 object through an EventBridge rule, a
# message through an SQS event source mapping. Mirrors the triggers in
# cdk/lib/orchestrator-stack.ts (triggersOf / triggerPattern).
locals {
  # A for-expression, not a conditional: `{}` and the decoded object are different types.
  triggers_cfg = { for n, t in try(local.workflow_def.orchestrator.triggers, {}) : n => t
  if local.workflow_plane && var.console_mode != "builder" }
  trigger_type          = { for n, t in local.triggers_cfg : n => try(t.type, "") }
  trigger_on            = { for n, t in local.triggers_cfg : n => try(t.enabled, true) != false }
  trigger_resource_name = { for n, t in local.triggers_cfg : n => "AgentCoreTrigger-${var.agent_name}-${n}" }

  webhook_triggers  = [for n, ty in local.trigger_type : n if ty == "webhook"]
  schedule_triggers = { for n, ty in local.trigger_type : n => local.triggers_cfg[n] if ty == "schedule" }
  rule_triggers     = { for n, ty in local.trigger_type : n => local.triggers_cfg[n] if contains(["eventbridge", "s3"], ty) }
  sqs_triggers      = { for n, ty in local.trigger_type : n => local.triggers_cfg[n] if ty == "sqs" }
  created_queues    = { for n, t in local.sqs_triggers : n => t if try(t.queueArn, "") == "" }

  # An S3 trigger listens for the bucket's "Object Created" events (the bucket must
  # send its events to EventBridge); an eventbridge trigger with its own pattern.
  trigger_patterns = { for n, t in local.rule_triggers : n => (
    local.trigger_type[n] == "s3"
    ? jsonencode({
      source        = ["aws.s3"]
      "detail-type" = ["Object Created"]
      detail = merge({ bucket = { name = [t.bucket] } },
      try(t.prefix, "") == "" ? {} : { object = { key = [{ prefix = t.prefix }] } })
    })
    : jsonencode(t.pattern)
  ) }
  trigger_queue_arns = merge(
    { for n, t in local.sqs_triggers : n => t.queueArn if try(t.queueArn, "") != "" },
    { for n, t in local.created_queues : n => aws_sqs_queue.trigger[n].arn },
  )
  trigger_bff_env = length(local.triggers_cfg) == 0 ? {} : merge(
    length(local.webhook_triggers) == 0 ? {} : { TRIGGER_SECRET_ARN = aws_secretsmanager_secret.triggers[0].arn },
    length(local.trigger_queue_arns) == 0 ? {} : { TRIGGER_QUEUES = jsonencode({ for n, arn in local.trigger_queue_arns : arn => n }) },
    var.run_owner_email == "" ? {} : { RUN_OWNER_EMAIL = var.run_owner_email },
    local.is_cognito ? { USER_POOL_ID = local.cognito_pool_id } : {},
  )
  # Read by bff/gates.py when a gate is answered by event or times out (gates.tf).
  gate_bff_env = local.gate_events || local.gate_timeouts ? { APP_NAME = var.agent_name } : {}
  # For-filters, not conditionals: the statements differ in shape, so two branches of
  # a conditional would be tuples of different types.
  bff_trigger_statements = concat(
    [for st in [{
      Sid      = "TriggerSecrets"
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue", "secretsmanager:PutSecretValue", "secretsmanager:DescribeSecret"]
      Resource = try(aws_secretsmanager_secret.triggers[0].arn, "")
    }] : st if length(local.webhook_triggers) > 0],
    [for st in [{
      Sid      = "TriggerQueues"
      Effect   = "Allow"
      Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility"]
      Resource = values(local.trigger_queue_arns)
    }] : st if length(local.sqs_triggers) > 0],
    [for st in [{
      Sid      = "TriggerRunOwner"
      Effect   = "Allow"
      Action   = ["cognito-idp:AdminGetUser"]
      Resource = "arn:aws:cognito-idp:${var.region}:${local.account_id}:userpool/${local.cognito_pool_id}"
    }] : st if length(local.triggers_cfg) > 0 && local.is_cognito],
  )
}

resource "terraform_data" "triggers_check" {
  count = length(local.triggers_cfg) == 0 ? 0 : 1
  lifecycle {
    precondition {
      condition     = alltrue([for n, ty in local.trigger_type : can(regex("^[A-Za-z][A-Za-z0-9]{0,31}$", n)) && contains(local.vocab.triggerTypes.values, ty)])
      error_message = "Each orchestrator.triggers entry needs a name of letters and digits (at most 32, starting with a letter) and a type: ${join(", ", local.vocab.triggerTypes.values)}."
    }
    precondition {
      condition     = alltrue([for n, t in local.schedule_triggers : can(regex("^(cron|rate)\\(.+\\)$", try(t.expression, "")))])
      error_message = "A schedule trigger needs \"expression\": cron(...) or rate(...)."
    }
    precondition {
      condition     = alltrue([for n, t in local.rule_triggers : local.trigger_type[n] == "s3" ? can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", try(t.bucket, ""))) : length(try(t.pattern, {})) > 0])
      error_message = "An eventbridge trigger needs an event \"pattern\"; an s3 trigger needs a \"bucket\"."
    }
    precondition {
      condition     = alltrue([for n, t in local.sqs_triggers : try(t.queueArn, "") == "" || can(regex("^arn:aws[a-z-]*:sqs:[a-z0-9-]+:[0-9]{12}:[A-Za-z0-9_.-]{1,80}$", t.queueArn))])
      error_message = "An sqs trigger's queueArn must be an SQS queue ARN (or left out, for a queue the framework creates)."
    }
  }
}

# --- webhook: each trigger's secret, set from the app's Triggers page --------------------
resource "aws_secretsmanager_secret" "triggers" {
  count                   = length(local.webhook_triggers) == 0 ? 0 : 1
  name_prefix             = "agentexpress/${var.agent_name}/triggers-"
  description             = "Webhook trigger secrets for ${var.agent_name} (bff/triggers.py)"
  recovery_window_in_days = 0
}

# --- schedule ----------------------------------------------------------------------------
resource "aws_iam_role" "trigger_scheduler" {
  count = length(local.schedule_triggers) == 0 ? 0 : 1
  name  = "AgentCoreBFF-${var.agent_name}-scheduler"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "aws:SourceAccount" = local.account_id } }
    }]
  })
}

resource "aws_iam_role_policy" "trigger_scheduler" {
  count = length(local.schedule_triggers) == 0 ? 0 : 1
  name  = "invoke"
  role  = aws_iam_role.trigger_scheduler[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["lambda:InvokeFunction"]
      Resource = aws_lambda_function.bff.arn
    }]
  })
}

resource "aws_scheduler_schedule" "trigger" {
  # checkov:skip=CKV_AWS_297:The schedule's input is the trigger's name and two Scheduler placeholders, nothing secret; AWS-owned encryption applies.
  for_each                     = local.schedule_triggers
  name                         = local.trigger_resource_name[each.key]
  schedule_expression          = each.value.expression
  schedule_expression_timezone = try(each.value.timezone, "UTC")
  state                        = local.trigger_on[each.key] ? "ENABLED" : "DISABLED"
  flexible_time_window {
    mode = "OFF"
  }
  target {
    arn      = aws_lambda_function.bff.arn
    role_arn = aws_iam_role.trigger_scheduler[0].arn
    input = jsonencode({
      axTrigger = each.key
      time      = "<aws.scheduler.scheduled-time>"
      id        = "<aws.scheduler.execution-id>"
    })
    retry_policy {
      maximum_retry_attempts = 2
    }
  }
  depends_on = [aws_iam_role_policy.trigger_scheduler]
}

# --- eventbridge and s3 --------------------------------------------------------------------
resource "aws_cloudwatch_event_rule" "trigger" {
  for_each       = local.rule_triggers
  name           = local.trigger_resource_name[each.key]
  event_bus_name = try(each.value.bus, "default")
  event_pattern  = local.trigger_patterns[each.key]
  state          = local.trigger_on[each.key] ? "ENABLED" : "DISABLED"
}

resource "aws_cloudwatch_event_target" "trigger" {
  for_each       = local.rule_triggers
  rule           = aws_cloudwatch_event_rule.trigger[each.key].name
  event_bus_name = try(each.value.bus, "default")
  arn            = aws_lambda_function.bff.arn
  input_transformer {
    input_paths    = { e = "$" }
    input_template = "{\"axTrigger\": \"${each.key}\", \"event\": <e>}"
  }
  retry_policy {
    maximum_retry_attempts       = 2
    maximum_event_age_in_seconds = 3600
  }
}

resource "aws_lambda_permission" "trigger_rule" {
  for_each      = local.rule_triggers
  statement_id  = "AllowTrigger-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.bff.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.trigger[each.key].arn
}

# --- sqs -----------------------------------------------------------------------------------
resource "aws_sqs_queue" "trigger_dlq" {
  for_each                  = local.created_queues
  name                      = "agentcore-${var.agent_name}-trigger-${each.key}-dlq"
  sqs_managed_sse_enabled   = true
  message_retention_seconds = 1209600
}

resource "aws_sqs_queue" "trigger" {
  for_each                = local.created_queues
  name                    = "agentcore-${var.agent_name}-trigger-${each.key}"
  sqs_managed_sse_enabled = true
  # Above the function's 300 s, so a message being handled is not handed out twice.
  visibility_timeout_seconds = 330
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.trigger_dlq[each.key].arn
    maxReceiveCount     = 3
  })
}

resource "aws_sqs_queue_policy" "trigger" {
  for_each  = local.created_queues
  queue_url = aws_sqs_queue.trigger[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "TlsOnly"
      Effect    = "Deny"
      Principal = "*"
      Action    = "sqs:*"
      Resource  = aws_sqs_queue.trigger[each.key].arn
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

resource "aws_lambda_event_source_mapping" "trigger" {
  for_each                = local.trigger_queue_arns
  event_source_arn        = each.value
  function_name           = aws_lambda_function.bff.arn
  batch_size              = 1
  function_response_types = ["ReportBatchItemFailures"]
  enabled                 = local.trigger_on[each.key]
  depends_on              = [aws_iam_role_policy.bff_triggers]
}

# --- what the BFF may do for them ----------------------------------------------------------
resource "aws_iam_role_policy" "bff_triggers" {
  count = length(local.triggers_cfg) > 0 && (length(local.webhook_triggers) + length(local.sqs_triggers) > 0 || local.is_cognito) ? 1 : 0
  name  = "triggers"
  role  = aws_iam_role.bff.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = local.bff_trigger_statements
  })
}
