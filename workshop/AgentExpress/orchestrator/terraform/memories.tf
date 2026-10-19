# --- The memory inspector (bff/memories.py) ---------------------------------------------
# Admins browse, search and delete long-term memory records from the app. The BFF gets the
# LONG-TERM memories only: the semantic one and the named ones (evaluations.tf). Never the
# checkpointer (awscc_bedrockagentcore_memory.orchestrator), so run checkpoints cannot be
# read or deleted from here. Mirrors the MemoryInspector* statements in
# cdk/lib/orchestrator-stack.ts.

locals {
  memory_bff_env = !local.workflow_plane ? {} : {
    SEMANTIC_MEMORY_ID = awscc_bedrockagentcore_memory.semantic[0].memory_id
    MEMORIES           = local.memories_env
  }
  # The memory resources themselves: these data-plane actions take the memory ARN, with no
  # sub-resource (Service Authorization Reference, resource type `memory`).
  inspected_memory_arns = !local.workflow_plane ? [] : concat(
    [awscc_bedrockagentcore_memory.semantic[0].memory_arn],
    [for m in awscc_bedrockagentcore_memory.named : m.memory_arn],
  )
}

resource "aws_iam_role_policy" "bff_memories" {
  count = local.workflow_plane ? 1 : 0
  name  = "AgentCoreBFFMemories-${var.agent_name}"
  role  = aws_iam_role.bff.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "MemoryInspectorRead"
        Effect   = "Allow"
        Action   = ["bedrock-agentcore:ListActors", "bedrock-agentcore:ListMemoryRecords", "bedrock-agentcore:RetrieveMemoryRecords", "bedrock-agentcore:GetMemoryRecord"]
        Resource = local.inspected_memory_arns
      },
      {
        # Its own statement, so a deployment that wants a read-only inspector removes it.
        Sid      = "MemoryInspectorDelete"
        Effect   = "Allow"
        Action   = ["bedrock-agentcore:DeleteMemoryRecord"]
        Resource = local.inspected_memory_arns
      },
    ]
  })
}
