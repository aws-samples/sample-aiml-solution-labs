# --- Long-term memory strategies and custom evaluators, from workflow.json ------------
#
# Mirrors memoryStrategiesFor / customEvaluatorsOf in cdk/lib/orchestrator-stack.ts.

locals {
  agents_def = try(local.workflow_def.agents, {})

  # Strategies agents name in agentcore.memory.longTerm (a list, or `true` = semantic).
  memory_named = distinct(flatten([
    for id, a in local.agents_def :
    try(tolist(a.agentcore.memory.longTerm), try(a.agentcore.memory.longTerm, false) == true ? ["semantic"] : [])
  ]))
  # Agents with their own custom strategy (agentcore.memory.custom).
  memory_customs = { for id, a in local.agents_def : id => a.agentcore.memory.custom if local.workflow_plane && can(a.agentcore.memory.custom.base) }
  # The resolved default (terraform/models.tf), already in this region's geography.
  memory_default_model = local.model_id

  memory_strategies = concat(
    [
      {
        semantic_memory_strategy = {
          name       = "insights"
          namespaces = ["insights/{actorId}"]
        }
      },
      {
        # Summarization is inherently per-session, so AgentCore REQUIRES {sessionId}
        # in its namespace (unlike semantic, which is actor-scoped and cross-session).
        summary_memory_strategy = {
          name       = "summary"
          namespaces = ["summary/{actorId}/{sessionId}"]
        }
      },
    ],
    contains(local.memory_named, "userPreference") ? [{
      user_preference_memory_strategy = {
        name       = "preferences"
        namespaces = ["preferences/{actorId}"]
      }
    }] : [],
    contains(local.memory_named, "episodic") ? [{
      episodic_memory_strategy = {
        name                     = "episodes"
        namespaces               = ["episodes/{actorId}/{sessionId}"]
        reflection_configuration = { namespaces = ["episodes/{actorId}"] }
      }
    }] : [],
    [for id, c in local.memory_customs : {
      custom_memory_strategy = {
        name       = substr("custom_${id}", 0, 48)
        namespaces = ["custom-${id}/{actorId}"]
        configuration = {
          summary_override = c.base == "summary" ? {
            consolidation = { append_to_prompt = c.instructions, model_id = try(local.regional_model[c.model], local.memory_default_model) }
          } : null
          user_preference_override = c.base == "userPreference" ? {
            extraction    = { append_to_prompt = c.instructions, model_id = try(local.regional_model[c.model], local.memory_default_model) }
            consolidation = { append_to_prompt = c.instructions, model_id = try(local.regional_model[c.model], local.memory_default_model) }
          } : null
          semantic_override = c.base == "semantic" ? {
            extraction    = { append_to_prompt = c.instructions, model_id = try(local.regional_model[c.model], local.memory_default_model) }
            consolidation = { append_to_prompt = c.instructions, model_id = try(local.regional_model[c.model], local.memory_default_model) }
          } : null
        }
      }
    }],
  )

  # Named memories (workflow.json `memories`): each its own AgentCore Memory, with the
  # strategies it names and its own event expiry. An agent uses one with
  # agentcore.memory.use; the runtime finds its id in MEMORIES. The namespaces are the
  # ones the semantic memory uses, so recall reads them the same way (context.py).
  named_memories = local.workflow_plane ? try(local.workflow_def.memories, {}) : {}
  named_memory_strategies = { for n, m in local.named_memories : n => concat(
    contains(m.strategies, "semantic") ? [{ semantic_memory_strategy = { name = "insights", namespaces = ["insights/{actorId}"] } }] : [],
    contains(m.strategies, "summary") ? [{ summary_memory_strategy = { name = "summary", namespaces = ["summary/{actorId}/{sessionId}"] } }] : [],
    contains(m.strategies, "userPreference") ? [{ user_preference_memory_strategy = { name = "preferences", namespaces = ["preferences/{actorId}"] } }] : [],
    contains(m.strategies, "episodic") ? [{ episodic_memory_strategy = {
      name = "episodes", namespaces = ["episodes/{actorId}/{sessionId}"], reflection_configuration = { namespaces = ["episodes/{actorId}"] }
    } }] : [],
  ) }
  memories_env      = jsonencode({ for n, m in awscc_bedrockagentcore_memory.named : n => m.memory_id })
  named_memory_arns = flatten([for m in awscc_bedrockagentcore_memory.named : [m.memory_arn, "${m.memory_arn}/*"]])

  # The build's own evaluators (workflow.json `evaluators`): one AgentCore evaluator each,
  # and every agent that lists Custom.<name> is scored by it (keys "<agent>.<name>").
  shared_evaluator_defs = local.workflow_plane ? try(local.workflow_def.evaluators, {}) : {}
  shared_evaluator_uses = merge([
    for id, a in local.agents_def : {
      for e in try(a.agentcore.evaluations.evaluators, []) : "${id}.${substr(e, 7, -1)}" => substr(e, 7, -1)
      if startswith(e, "Custom.") && contains(keys(local.shared_evaluator_defs), substr(e, 7, -1))
      && !contains([for c in try(a.agentcore.evaluations.custom, []) : c.name], substr(e, 7, -1))
    }
  ]...)

  # Custom evaluators (agentcore.evaluations.custom), keyed "<agent>.<name>".
  evaluator_tail  = local.vocab.builtinEvaluators.customPlaceholders
  evaluator_scale = local.vocab.builtinEvaluators.customScale
  custom_evaluators = merge([
    for id, a in local.agents_def : {
      for c in(local.workflow_plane ? try(a.agentcore.evaluations.custom, []) : []) : "${id}.${c.name}" => {
        name         = substr("${var.agent_name}_${substr(sha256("${id}.${c.name}"), 0, 6)}_${c.name}", 0, 48)
        instructions = can(regex("\\{(context|assistant_turn)\\}", c.instructions)) ? c.instructions : "${c.instructions}${local.evaluator_tail}"
        model        = try(local.regional_model[c.model], local.memory_default_model)
        scale        = try(length(c.scale), 0) > 0 ? c.scale : local.evaluator_scale
      }
    }
  ]...)
  custom_evaluator_ids = jsonencode(merge(
    { for k, e in aws_bedrockagentcore_evaluator.custom : k => e.evaluator_id },
    { for k, n in local.shared_evaluator_uses : k => aws_bedrockagentcore_evaluator.shared[n].evaluator_id },
  ))
}

# The memory's execution role: only when some agent defines a custom strategy.
resource "aws_iam_role" "memory" {
  count = length(local.memory_customs) > 0 ? 1 : 0
  name  = "AgentCoreMemory-${var.agent_name}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "bedrock-agentcore.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "aws:SourceAccount" = local.account_id } }
    }]
  })
}

resource "aws_iam_role_policy" "memory" {
  count = length(local.memory_customs) > 0 ? 1 : 0
  name  = "invoke"
  role  = aws_iam_role.memory[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"]
      Resource = [
        "arn:aws:bedrock:*::foundation-model/*",
        "arn:aws:bedrock:${var.region}:${local.account_id}:inference-profile/*"
      ]
    }]
  })
}

resource "aws_bedrockagentcore_evaluator" "custom" {
  for_each       = local.custom_evaluators
  evaluator_name = each.value.name
  level          = "TRACE"
  description    = "Custom evaluator ${each.key}"
  evaluator_config {
    llm_as_a_judge {
      instructions = each.value.instructions
      model_config {
        bedrock_evaluator_model_config {
          model_id = each.value.model
        }
      }
      rating_scale {
        dynamic "numerical" {
          for_each = each.value.scale
          content {
            value      = numerical.value.value
            label      = numerical.value.label
            definition = numerical.value.definition
          }
        }
      }
    }
  }
}

resource "aws_bedrockagentcore_evaluator" "shared" {
  for_each       = local.shared_evaluator_defs
  evaluator_name = substr("${var.agent_name}_${substr(sha256("*.${each.key}"), 0, 6)}_${each.key}", 0, 48)
  level          = "TRACE"
  description    = "Evaluator ${each.key} from workflow.json evaluators"
  evaluator_config {
    llm_as_a_judge {
      instructions = can(regex("\\{(context|assistant_turn)\\}", each.value.instructions)) ? each.value.instructions : "${each.value.instructions}${local.evaluator_tail}"
      model_config {
        bedrock_evaluator_model_config {
          model_id = try(local.regional_model[each.value.model], local.memory_default_model)
        }
      }
      rating_scale {
        dynamic "numerical" {
          for_each = try(length(each.value.scale), 0) > 0 ? each.value.scale : local.evaluator_scale
          content {
            value      = numerical.value.value
            label      = numerical.value.label
            definition = numerical.value.definition
          }
        }
      }
    }
  }
}

resource "awscc_bedrockagentcore_memory" "named" {
  for_each              = local.named_memories
  name                  = "${var.agent_name}_m_${each.key}"
  event_expiry_duration = try(each.value.expiryDays, local.key_defaults.memory.expiryDays)
  description           = substr(try(each.value.description, "") != "" ? each.value.description : "Memory ${each.key} from workflow.json memories", 0, 200)
  memory_strategies     = local.named_memory_strategies[each.key]
}
