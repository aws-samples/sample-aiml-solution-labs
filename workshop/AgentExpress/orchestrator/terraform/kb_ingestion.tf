# --- How the Knowledge Base reads and splits its documents ------------------
#
# A type="kb" tool's `chunking` and `parsing` keys become the data source's
# vector_ingestion_configuration (kb.tf). Both are optional: with neither, the data
# source is exactly what it was (name "docs", no ingestion block), so an existing
# deployment plans no change. Mirrors cdk/lib/kb-ingestion.ts.
#
# Both settings force a NEW data source (provider: "Forces new resource"; CloudFormation:
# ChunkingConfiguration and ParsingConfiguration "Update requires: Replacement"). The
# name carries a digest of them, as the vector index's does, so a change is a clean
# replacement on both paths and the ingestion step re-runs for the new data source id.
#
# https://docs.aws.amazon.com/bedrock/latest/userguide/kb-chunking.html
# https://docs.aws.amazon.com/bedrock/latest/userguide/kb-advanced-parsing.html

locals {
  kb_chunking       = try(local.kb_spec.chunking, null)
  kb_parsing        = try(local.kb_spec.parsing, null)
  kb_chunk_strategy = try(tostring(local.kb_chunking.strategy), "")
  kb_parse_strategy = try(tostring(local.kb_parsing.strategy), "")
  kb_chunk_params   = try(local.vocab.kbChunkingStrategies.params[local.kb_chunk_strategy], [])
  kb_chunk_ranges   = local.vocab.kbChunkingStrategies.ranges
  # One value per setting the strategy requires, null when missing (the precondition on
  # the data source names it). Numbers only: a string here fails the precondition too.
  kb_chunk = { for p in local.kb_chunk_params : p => try(tonumber(local.kb_chunking[p]), null) }

  # The parsing model: a foundation model id, or a system inference profile id whose
  # first segment is a geography (us., eu., global., ...). A profile is invoked through
  # its own ARN and routes to the model in any region of its geography, so it needs both.
  kb_parse_model_id   = try(tostring(local.kb_parsing.model), "")
  kb_parse_prompt     = try(tostring(local.kb_parsing.prompt), "")
  kb_parse_is_profile = contains(local.vocab.inferenceProfileGeos.values, split(".", local.kb_parse_model_id)[0])
  kb_parse_base_model = local.kb_parse_is_profile ? join(".", slice(split(".", local.kb_parse_model_id), 1, length(split(".", local.kb_parse_model_id)))) : local.kb_parse_model_id
  kb_parse_model_arn = local.kb_parse_is_profile ? (
    "arn:aws:bedrock:${var.region}:${local.account_id}:inference-profile/${local.kb_parse_model_id}"
  ) : "arn:aws:bedrock:${var.region}::foundation-model/${local.kb_parse_model_id}"

  # The KB role may invoke the parsing model, and nothing else beyond the embedding model.
  # Concatenated into aws_iam_role_policy.kb; empty when parsing is off.
  kb_parsing_statements = !contains(local.vocab.kbParsingStrategies.values, local.kb_parse_strategy) ? [] : [
    {
      Sid    = "ParsingModel"
      Effect = "Allow"
      Action = ["bedrock:InvokeModel"]
      Resource = local.kb_parse_is_profile ? [
        local.kb_parse_model_arn,
        "arn:aws:bedrock:*::foundation-model/${local.kb_parse_base_model}",
      ] : [local.kb_parse_model_arn]
    }
  ]

  # Neptune Analytics' graph construction model (kb_vector_store.tf), granted the same way.
  kb_graph_is_profile = contains(local.vocab.inferenceProfileGeos.values, split(".", local.kb_graph_model)[0])
  kb_graph_model_arn = local.kb_graph_is_profile ? (
    "arn:aws:bedrock:${var.region}:${local.account_id}:inference-profile/${local.kb_graph_model}"
  ) : "arn:aws:bedrock:${var.region}::foundation-model/${local.kb_graph_model}"
  kb_graph_statements = local.kb_graph_model == "" ? [] : [
    {
      Sid    = "GraphModel"
      Effect = "Allow"
      Action = ["bedrock:InvokeModel"]
      Resource = local.kb_graph_is_profile ? [
        local.kb_graph_model_arn,
        "arn:aws:bedrock:*::foundation-model/${join(".", slice(split(".", local.kb_graph_model), 1, length(split(".", local.kb_graph_model))))}",
      ] : [local.kb_graph_model_arn]
    }
  ]

  # The same ingestion settings in awscc's shape, for awscc_bedrock_data_source.kb_graph.
  kb_ingestion_awscc = {
    chunking_configuration = local.kb_chunk_strategy == "" ? null : {
      chunking_strategy = local.vocab.kbChunkingStrategies.api[local.kb_chunk_strategy]
      fixed_size_chunking_configuration = local.kb_chunk_strategy != "fixedSize" ? null : {
        max_tokens = local.kb_chunk.maxTokens, overlap_percentage = local.kb_chunk.overlapPercentage
      }
      hierarchical_chunking_configuration = local.kb_chunk_strategy != "hierarchical" ? null : {
        level_configurations = [{ max_tokens = local.kb_chunk.parentMaxTokens }, { max_tokens = local.kb_chunk.childMaxTokens }]
        overlap_tokens       = local.kb_chunk.overlapTokens
      }
      semantic_chunking_configuration = local.kb_chunk_strategy != "semantic" ? null : {
        max_tokens                      = local.kb_chunk.maxTokens, buffer_size = local.kb_chunk.bufferSize
        breakpoint_percentile_threshold = local.kb_chunk.breakpointPercentileThreshold
      }
    }
    parsing_configuration = local.kb_parse_strategy == "" ? null : {
      parsing_strategy = local.vocab.kbParsingStrategies.api[local.kb_parse_strategy]
      bedrock_foundation_model_configuration = {
        model_arn      = local.kb_parse_model_arn
        parsing_prompt = local.kb_parse_prompt == "" ? null : { parsing_prompt_text = local.kb_parse_prompt }
      }
    }
    context_enrichment_configuration = local.kb_graph_model == "" ? null : {
      type = "BEDROCK_FOUNDATION_MODEL"
      bedrock_foundation_model_configuration = {
        model_arn                         = local.kb_graph_model_arn
        enrichment_strategy_configuration = { method = "CHUNK_ENTITY_EXTRACTION" }
      }
    }
  }

  # Digest of everything that forces a new data source. The canonical string must match
  # kbIngestionKey() in cdk/lib/kb-ingestion.ts character for character.
  kb_ingestion_key    = "c=${local.kb_chunk_strategy}:${join(",", [for p in local.kb_chunk_params : try(tostring(local.kb_chunking[p]), "")])}|p=${local.kb_parse_strategy}:${local.kb_parse_model_id}:${local.kb_parse_prompt}${local.kb_graph_model == "" ? "" : "|e=${local.kb_graph_model}"}"
  kb_ingestion_set    = local.kb_chunk_strategy != "" || local.kb_parse_strategy != "" || local.kb_graph_model != ""
  kb_data_source_name = local.kb_ingestion_set ? "docs-${substr(sha256(local.kb_ingestion_key), 0, 8)}" : "docs"

  # The checks bff/validate_build.py and validateKbIngestion make, as one list of
  # problems for the data source's precondition. Empty when the settings are valid.
  # Comparisons sit inside try(): HCL does not short-circuit `||`, so a null setting
  # would otherwise fail the comparison instead of being reported as missing.
  kb_ingestion_problems = concat(
    local.kb_chunking == null || contains(local.vocab.kbChunkingStrategies.values, local.kb_chunk_strategy) ? [] : [
      "chunking.strategy must be one of: ${join(", ", local.vocab.kbChunkingStrategies.values)}"
    ],
    [for p, v in local.kb_chunk : "chunking.${p} is required for ${local.kb_chunk_strategy} chunking, as a whole number" if v == null],
    [for p, v in local.kb_chunk : (local.kb_chunk_ranges[p][1] == null
      ? "chunking.${p} must be a whole number, ${local.kb_chunk_ranges[p][0]} or more"
      : "chunking.${p} must be a whole number from ${local.kb_chunk_ranges[p][0]} to ${local.kb_chunk_ranges[p][1]}")
      if v != null && (try(v != floor(v) || v < local.kb_chunk_ranges[p][0], false) || try(v > local.kb_chunk_ranges[p][1], false))
    ],
    local.kb_chunk_strategy == "hierarchical" && try(local.kb_chunk.childMaxTokens >= local.kb_chunk.parentMaxTokens, false) ? [
      "chunking.childMaxTokens must be smaller than parentMaxTokens: the second layer holds the smaller chunks"
    ] : [],
    local.kb_parsing == null || contains(local.vocab.kbParsingStrategies.values, local.kb_parse_strategy) ? [] : [
      "parsing.strategy must be one of: ${join(", ", local.vocab.kbParsingStrategies.values)}"
    ],
    !contains(local.vocab.kbParsingStrategies.values, local.kb_parse_strategy) || can(regex("^[a-z0-9-]+(\\.[a-z0-9-]+)+(:[0-9]+)?$", local.kb_parse_model_id)) ? [] : [
      "parsing.model must be a Bedrock model or inference profile id, like anthropic.claude-3-haiku-20240307-v1:0"
    ],
    local.kb_parse_prompt == "" || length(local.kb_parse_prompt) <= 10000 ? [] : [
      "parsing.prompt must be 1 to 10000 characters"
    ],
  )
}
