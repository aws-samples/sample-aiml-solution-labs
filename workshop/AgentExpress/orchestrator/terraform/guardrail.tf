# --- Bedrock Guardrail (content safety) ------------------------------------
# The guardrail's POLICY comes from the `guardrail` block in app/workflow.json,
# so adapting content safety to a customer's domain is a CONFIG edit — nothing in
# this file is domain-specific. Its id is injected into every runtime as
# GUARDRAIL_ID, so workflow.json only needs the per-agent flip
# ("guardrails": {"input": true, "output": true}) and never an account-specific
# id. An agent MAY override with its own "guardrailId"; otherwise it uses this.
#
# Every sub-policy is optional: omit a key in workflow.json and that policy block
# is not emitted at all (Bedrock rejects an empty *_policy_config, hence the
# dynamic blocks below rather than empty lists).

locals {
  # Closed value sets from app/vocabulary.json, shared with the Python and TypeScript
  # planes rather than written out a second time here.
  gr_vocab = jsondecode(file("${path.module}/../app/vocabulary.json"))

  gr = try(local.workflow_def.guardrail, {})

  # type -> strength, e.g. {"HATE" = "MEDIUM"}. Applied to input AND output.
  # PROMPT_ATTACK is input-only in Bedrock, so its output strength must be NONE.
  gr_content_filters = try(local.gr.contentFilters, {})
  gr_denied_words    = try(local.gr.deniedWords, [])
  gr_managed_words   = try(local.gr.managedWordLists, [])
  gr_denied_topics   = try(local.gr.deniedTopics, [])
  # entity type -> action (BLOCK | ANONYMIZE).
  gr_pii = try(local.gr.piiEntities, {})

  # Defaults from app/defaults.json (generated from app/keys.json), like every other
  # key's. Both messages were written out here AND in cdk/lib/orchestrator-stack.ts, so a
  # reworded one changed what a blocked caller saw on only one deploy path.
  gr_blocked_input  = try(local.gr.blockedInputMessage, local.key_defaults.guardrail.blockedInputMessage)
  gr_blocked_output = try(local.gr.blockedOutputMessage, local.key_defaults.guardrail.blockedOutputMessage)

  # Bedrock rejects a guardrail with no policy ("Guardrail must have at least one
  # policy"), so the build-wide one exists only when its block enforces something.
  # Without it GUARDRAIL_ID is "" and an agent's guardrails flag is a no-op unless it
  # names a guardrail with agentcore.guardrails.use.
  gr_has_policy = length(local.gr_content_filters) + length(local.gr_denied_words) + length(local.gr_managed_words) + length(local.gr_denied_topics) + length(local.gr_pii) > 0
  guardrail_id  = try(aws_bedrock_guardrail.main[0].guardrail_id, "")
}

# Fail at PLAN on a malformed guardrail block rather than at apply, when Bedrock
# returns a less obvious error.
resource "terraform_data" "guardrail_validation" {
  lifecycle {
    precondition {
      condition = alltrue([
        for t, s in local.gr_content_filters :
        contains(local.gr_vocab.guardrailFilterStrengths.values, upper(s))
      ])
      error_message = "Each workflow.json guardrail.contentFilters strength must be NONE, LOW, MEDIUM or HIGH."
    }
    precondition {
      condition = alltrue([
        for e, a in local.gr_pii : contains(local.gr_vocab.guardrailPiiActions.values, upper(a))
      ])
      error_message = "Each workflow.json guardrail.piiEntities action must be BLOCK or ANONYMIZE."
    }
    precondition {
      condition = alltrue([
        for t in local.gr_denied_topics : try(t.name, "") != "" && try(t.definition, "") != ""
      ])
      error_message = "Each workflow.json guardrail.deniedTopics entry needs a \"name\" and a \"definition\"."
    }
  }
}

resource "aws_bedrock_guardrail" "main" {
  count                     = local.workflow_plane && local.gr_has_policy ? 1 : 0
  name                      = "${var.agent_name}-guardrail"
  description               = "Content-safety guardrail generated from app/workflow.json"
  blocked_input_messaging   = local.gr_blocked_input
  blocked_outputs_messaging = local.gr_blocked_output

  dynamic "content_policy_config" {
    for_each = length(local.gr_content_filters) > 0 ? [1] : []
    content {
      dynamic "filters_config" {
        for_each = local.gr_content_filters
        content {
          type           = upper(filters_config.key)
          input_strength = upper(filters_config.value)
          # Prompt-attack filtering is input-only; Bedrock rejects an output
          # strength on it.
          output_strength = upper(filters_config.key) == "PROMPT_ATTACK" ? "NONE" : upper(filters_config.value)
        }
      }
    }
  }

  dynamic "word_policy_config" {
    for_each = (length(local.gr_denied_words) + length(local.gr_managed_words)) > 0 ? [1] : []
    content {
      dynamic "words_config" {
        for_each = local.gr_denied_words
        content {
          text = words_config.value
        }
      }
      dynamic "managed_word_lists_config" {
        for_each = local.gr_managed_words
        content {
          type = upper(managed_word_lists_config.value)
        }
      }
    }
  }

  dynamic "topic_policy_config" {
    for_each = length(local.gr_denied_topics) > 0 ? [1] : []
    content {
      dynamic "topics_config" {
        for_each = local.gr_denied_topics
        content {
          name       = topics_config.value.name
          definition = topics_config.value.definition
          examples   = try(topics_config.value.examples, [])
          type       = "DENY"
        }
      }
    }
  }

  dynamic "sensitive_information_policy_config" {
    for_each = length(local.gr_pii) > 0 ? [1] : []
    content {
      dynamic "pii_entities_config" {
        for_each = local.gr_pii
        content {
          type   = upper(pii_entities_config.key)
          action = upper(pii_entities_config.value)
        }
      }
    }
  }
}

# --- Named guardrails: workflow.json `guardrails.<name>` --------------------------------
# Each is its own Bedrock Guardrail with the keys of the `guardrail` block; an agent
# applies one with agentcore.guardrails.use, and the runtime finds its id in GUARDRAILS.
# Mirrors the named guardrails in cdk/lib/orchestrator-stack.ts.
locals {
  named_guardrails = local.workflow_plane ? try(local.workflow_def.guardrails, {}) : {}
  guardrails_env = jsonencode({ for n, g in aws_bedrock_guardrail.named : n => {
    id = g.guardrail_id, version = "DRAFT"
  } })
  # IAM rejects an empty Resource list, so a build with no guardrail at all grants
  # ApplyGuardrail on an id no guardrail has (ids are service-generated).
  guardrail_arns = coalescelist(concat(
    [for g in aws_bedrock_guardrail.main : g.guardrail_arn],
    [for g in aws_bedrock_guardrail.named : g.guardrail_arn],
  ), ["arn:aws:bedrock:${var.region}:${local.account_id}:guardrail/none"])
}

resource "aws_bedrock_guardrail" "named" {
  for_each                  = local.named_guardrails
  name                      = "${var.agent_name}-gr-${each.key}"
  description               = "Guardrail ${each.key} from app/workflow.json guardrails"
  blocked_input_messaging   = try(each.value.blockedInputMessage, local.key_defaults.guardrail.blockedInputMessage)
  blocked_outputs_messaging = try(each.value.blockedOutputMessage, local.key_defaults.guardrail.blockedOutputMessage)

  dynamic "content_policy_config" {
    for_each = length(try(each.value.contentFilters, {})) > 0 ? [1] : []
    content {
      dynamic "filters_config" {
        for_each = try(each.value.contentFilters, {})
        content {
          type            = upper(filters_config.key)
          input_strength  = upper(filters_config.value)
          output_strength = upper(filters_config.key) == "PROMPT_ATTACK" ? "NONE" : upper(filters_config.value)
        }
      }
    }
  }

  dynamic "word_policy_config" {
    for_each = (length(try(each.value.deniedWords, [])) + length(try(each.value.managedWordLists, []))) > 0 ? [1] : []
    content {
      dynamic "words_config" {
        for_each = try(each.value.deniedWords, [])
        content {
          text = words_config.value
        }
      }
      dynamic "managed_word_lists_config" {
        for_each = try(each.value.managedWordLists, [])
        content {
          type = upper(managed_word_lists_config.value)
        }
      }
    }
  }

  dynamic "topic_policy_config" {
    for_each = length(try(each.value.deniedTopics, [])) > 0 ? [1] : []
    content {
      dynamic "topics_config" {
        for_each = try(each.value.deniedTopics, [])
        content {
          name       = topics_config.value.name
          definition = topics_config.value.definition
          examples   = try(topics_config.value.examples, [])
          type       = "DENY"
        }
      }
    }
  }

  dynamic "sensitive_information_policy_config" {
    for_each = length(try(each.value.piiEntities, {})) > 0 ? [1] : []
    content {
      dynamic "pii_entities_config" {
        for_each = try(each.value.piiEntities, {})
        content {
          type   = upper(pii_entities_config.key)
          action = upper(pii_entities_config.value)
        }
      }
    }
  }
}
