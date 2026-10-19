# --- Task steps (agents with runtime "task", app/common/task_agent.py) -------------------
# A step with no model: it calls a tool, runs a script in the Code Interpreter sandbox,
# reads the run's documents with Textract, or writes a file. It runs in the orchestrator's
# own runtime, so only that role gets anything, and only what a step of the workflow uses:
#   code    -> start, use and stop sessions on the AWS-managed interpreter, nothing else;
#   extract -> Textract's document analysis. Textract has no resource type for these
#              actions (service authorization reference, Amazon Textract), so "*".
# A tool step uses the Gateway like any agent, and an output step writes under runs/*, which
# the runtime may already do (images.tf runtime_assets). An extract step reads the run's
# files from S3 itself: it needs `attachments`, which grants the runtime GetObject on runs/*.
# A workflow with no task step gets nothing here. Mirrors cdk/lib/tasks.ts.
locals {
  task_specs   = { for id, a in try(local.workflow_def.agents, {}) : id => try(a.task, {}) if try(a.runtime, "") == "task" }
  task_kinds   = distinct([for id, t in local.task_specs : try(t.kind, "")])
  task_code    = local.workflow_plane && contains(local.task_kinds, "code")
  task_extract = local.workflow_plane && contains(local.task_kinds, "extract")
  bad_tasks    = [for id, t in local.task_specs : id if !contains(local.vocab.taskKinds.values, try(t.kind, ""))]
}
resource "terraform_data" "task_steps" {
  count = length(local.task_specs) > 0 ? 1 : 0
  lifecycle {
    precondition {
      condition     = length(local.bad_tasks) == 0
      error_message = "app/workflow.json task step(s) ${join(", ", local.bad_tasks)}: task.kind must be one of ${join(", ", local.vocab.taskKinds.values)}."
    }
  }
}
resource "aws_iam_role_policy" "runtime_tasks" {
  count = local.task_code || local.task_extract ? 1 : 0
  name  = "AgentCoreRuntimeTasks-${var.agent_name}"
  role  = aws_iam_role.runtime[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      local.task_code ? [{
        Sid    = "TaskCodeInterpreter", Effect = "Allow",
        Action = ["bedrock-agentcore:StartCodeInterpreterSession", "bedrock-agentcore:InvokeCodeInterpreter", "bedrock-agentcore:StopCodeInterpreterSession"]
        # The AWS-managed interpreter only (account "aws" in its ARN), as for the codeInterpreter tool.
        Resource = ["arn:aws:bedrock-agentcore:${var.region}:aws:code-interpreter/aws.codeinterpreter.v1"]
      }] : [],
      local.task_extract ? [{
        Sid    = "TaskTextract", Effect = "Allow",
        Action = ["textract:AnalyzeDocument", "textract:StartDocumentAnalysis", "textract:GetDocumentAnalysis"]
        # No resource type: Textract authorizes these on "*" only.
        Resource = "*"
      }] : [],
    )
  })
}
