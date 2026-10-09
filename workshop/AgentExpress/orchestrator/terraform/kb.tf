# --- Bedrock Knowledge Base (S3 Vectors) fronted by the Gateway -----------
#
# Created only when app/workflow.json declares a tool with type="kb"; the tool's
# KEY becomes the Gateway target name. Replace the contents of kb_docs/ with your
# own documents — each TOP-LEVEL FOLDER becomes a corpus (a filterable doc_type),
# and an agent is scoped to one via its `corpus` field. No code or IaC change.
#
# An agent bound to the `kb` tool retrieves from a Bedrock Knowledge Base backed by
# S3 Vectors (serverless, no OCU floor). Retrieval is exposed as a tool through the
# same JWT-authed AgentCore Gateway (whichever idp — see identity.tf) via a Lambda
# target. All declarative: no vector-index bootstrap script, because the S3 Vectors
# index is a native resource.

locals {
  # Provisioned only when workflow.json declares a tool with type="kb".
  kb_enabled = local.kb_tool_name != ""
  # The kb tool's own entry, so the retrieval settings below read from ONE place.
  # `try` rather than a conditional: the two branches of `enabled ? tools_raw[name] : {}`
  # are an object with the kb tool's attributes and an empty object, and HCL refuses to
  # unify those ("Inconsistent conditional result types"). An absent key fails the index,
  # which is exactly the condition we want to fall back on.
  kb_spec = try(local.tools_raw[local.kb_tool_name], {})

  # --- Where the documents come from ----------------------------------------
  # Mirrors kbSource in cdk/lib/tool-plane.ts:
  #   upload   - kb_docs/ (and a Builder build's uploads): everything is made here.
  #   s3       - `s3Uri`: YOUR bucket is the data source; the vector store and KB are
  #              made here and read only that bucket/prefix (plus `kmsKeyArn`).
  #   existing - `knowledgeBaseId`: YOUR Knowledge Base; only the retrieval function
  #              is made, and nothing here can delete it.
  kb_existing_id = try(local.kb_spec.knowledgeBaseId, "")
  kb_s3_uri      = try(local.kb_spec.s3Uri, "")
  kb_kms_key     = try(local.kb_spec.kmsKeyArn, "")
  kb_source      = local.kb_existing_id != "" ? "existing" : (local.kb_s3_uri != "" ? "s3" : "upload")
  kb_create      = local.kb_enabled && local.kb_source != "existing"
  kb_upload      = local.kb_enabled && local.kb_source == "upload"
  kb_s3_bucket   = local.kb_s3_uri == "" ? "" : split("/", trimprefix(local.kb_s3_uri, "s3://"))[0]
  kb_s3_rest     = local.kb_s3_uri == "" ? "" : trimprefix(trimprefix(local.kb_s3_uri, "s3://${local.kb_s3_bucket}"), "/")
  kb_s3_prefix   = local.kb_s3_rest == "" || endswith(local.kb_s3_rest, "/") ? local.kb_s3_rest : "${local.kb_s3_rest}/"

  # --- Embedding model + dimension, from config -----------------------------
  # Both were hardcoded here and in cdk/lib/tool-plane.ts. They are a PAIR: a model
  # supports only certain dimensions, and a mismatch is not rejected at deploy - it
  # fails at Bedrock INGESTION, after a deploy that reported success. So the model is
  # closed to a known set and the dimension is defaulted from it, which means declaring
  # `embeddingModel` alone is enough and declaring neither keeps today's behaviour.
  kb_embed_model_id = try(local.kb_spec.embeddingModel, local.vocab.embeddingModels.values[0])
  kb_allowed_dims   = try(local.vocab.embeddingModels.dimensionsByModel[local.kb_embed_model_id], [])
  kb_dims           = try(local.kb_spec.dimensions, try(local.kb_allowed_dims[0], 1024))
  # S3 Vectors caps FILTERABLE metadata at 2048 bytes per vector, and both of these
  # grow with the document, so both must be excluded. Mirrors KB_NON_FILTERABLE in
  # cdk/lib/tool-plane.ts.
  kb_non_filterable = ["AMAZON_BEDROCK_TEXT", "AMAZON_BEDROCK_METADATA"]
  # Digest of the vector store's IMMUTABLE properties, so changing one yields NEW
  # names and the replacement succeeds instead of erroring out. Must match
  # kbStorageDigest() in cdk/lib/tool-plane.ts.
  kb_storage_digest = substr(sha256("${local.kb_dims}|${join(",", sort(local.kb_non_filterable))}"), 0, 8)
  kb_index_name     = "kb-index-${local.kb_storage_digest}"
  # The KB carries the SAME digest: its storage_configuration points at the index ARN
  # and is immutable, so replacing the index replaces the KB too. Matches
  # knowledgeBaseName() in cdk/lib/tool-plane.ts.
  kb_name     = "${replace(var.agent_name, "_", "-")}-kb-${local.kb_storage_digest}"
  embed_model = "arn:aws:bedrock:${var.region}::foundation-model/${local.kb_embed_model_id}"

  # --- Retrieval settings passed to the KB Lambda ---------------------------
  # Defaults live in kb_lambda/handler.py, so an absent key sends an empty string and
  # the function keeps its documented default rather than this file restating it.
  kb_corpus_key      = try(local.kb_spec.corpusKey, local.tool_defaults.corpusKey)
  kb_corpus_operator = try(local.kb_spec.corpusOperator, local.tool_defaults.corpusOperator)
  # Target-level, agent-invisible. See the kb_lambda docstring: the agent's filter is a
  # scalar the Cedar permit enforces, THIS one may be arbitrary because nothing the
  # caller sends can influence it.
  kb_static_filter = try(jsonencode(local.kb_spec.filter), "")
  kb_rerank        = try(jsonencode(local.kb_spec.rerank), "")
  # The reranking model, for the IAM grant. Empty when reranking is off, so the
  # statement is omitted rather than granted on a resource nobody calls.
  kb_rerank_model = try(local.kb_spec.rerank.model, "")
  kb_rerank_arn = local.kb_rerank_model == "" ? "" : (
    startswith(local.kb_rerank_model, "arn:")
    ? local.kb_rerank_model
    : "arn:aws:bedrock:${var.region}::foundation-model/${local.kb_rerank_model}"
  )

  # Corpus files, recursive (subfolders included), excluding macOS noise that
  # Bedrock ingestion would reject.
  # Corpus documents. Sidecars are EXCLUDED: this module generates one
  # "<key>.metadata.json" per document, so treating a hand-written sidecar as a
  # document would ingest it as content and then give it its own sidecar.
  kb_files = [
    for f in fileset("${path.module}/../kb_docs", "**") : f
    if !endswith(f, ".DS_Store") && !endswith(f, ".metadata.json")
    && !startswith(basename(f), ".")
  ]
}

# --- S3 Vectors store -----------------------------------------------------

resource "aws_s3vectors_vector_bucket" "kb" {
  count              = local.kb_create ? 1 : 0
  vector_bucket_name = "agentcore-${replace(var.agent_name, "_", "-")}-kb-${local.account_id}"
  force_destroy      = true
}

resource "aws_s3vectors_index" "kb" {
  count              = local.kb_create ? 1 : 0
  vector_bucket_name = aws_s3vectors_vector_bucket.kb[0].vector_bucket_name
  # The name carries a digest of this index's IMMUTABLE properties (dimension +
  # the non-filterable key list), matching kbIndexName() in cdk/lib/tool-plane.ts.
  # Neither can be changed in place, so a change to either must REPLACE the index,
  # and CloudFormation refuses to replace a resource with a fixed custom name
  # ("cannot update a stack when a custom-named resource requires replacing").
  # Deriving the name means the replacement just works on both paths.
  index_name      = local.kb_index_name
  data_type       = "float32"
  dimension       = local.kb_dims
  distance_metric = "cosine"

  # See local.kb_non_filterable above for why both keys are excluded. Miss one and
  # ingestion accepts small documents, then FAILS on a larger one while the deploy
  # still reports success.
  metadata_configuration {
    non_filterable_metadata_keys = local.kb_non_filterable
  }
}

# --- Documents bucket + sample corpus -------------------------------------

resource "aws_s3_bucket" "kb_docs" {
  count         = local.kb_upload ? 1 : 0
  bucket        = "agentcore-${replace(var.agent_name, "_", "-")}-kbdocs-${local.account_id}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "kb_docs" {
  count                   = local.kb_upload ? 1 : 0
  bucket                  = aws_s3_bucket.kb_docs[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "kb_docs" {
  count  = local.kb_upload ? 1 : 0
  bucket = aws_s3_bucket.kb_docs[0].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
resource "aws_s3_bucket_policy" "kb_docs_tls" {
  count  = local.kb_upload ? 1 : 0
  bucket = aws_s3_bucket.kb_docs[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.kb_docs[0].arn, "${aws_s3_bucket.kb_docs[0].arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.kb_docs]
}
resource "aws_s3_object" "kb_docs" {
  for_each = local.kb_upload ? toset(local.kb_files) : []
  bucket   = aws_s3_bucket.kb_docs[0].id
  key      = each.value
  source   = "${path.module}/../kb_docs/${each.value}"
  etag     = filemd5("${path.module}/../kb_docs/${each.value}")
}

# Per-document metadata sidecars. Bedrock reads "<key>.metadata.json" next to
# each source object and attaches its attributes to every chunk (it does not
# ingest the sidecar itself as a document). We tag each doc with a filterable
# `doc_type` derived from its top-level folder (e.g. "reference") so a RAG agent
# can restrict retrieval to its own corpus instead of searching the whole shared
# index. Add more folders to add more corpora.
resource "aws_s3_object" "kb_docs_metadata" {
  for_each     = local.kb_upload ? toset(local.kb_files) : []
  bucket       = aws_s3_bucket.kb_docs[0].id
  key          = "${each.value}.metadata.json"
  content      = jsonencode({ metadataAttributes = { doc_type = split("/", each.value)[0] } })
  content_type = "application/json"
}

# --- KB service role ------------------------------------------------------

resource "aws_iam_role" "kb" {
  count = local.kb_create ? 1 : 0
  name  = "AgentCoreKB-${var.agent_name}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "bedrock.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "aws:SourceAccount" = local.account_id } }
    }]
  })
}

resource "aws_iam_role_policy" "kb" {
  count = local.kb_create ? 1 : 0
  name  = "KBPolicy-${var.agent_name}"
  role  = aws_iam_role.kb[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Sid      = "Embeddings"
        Effect   = "Allow"
        Action   = ["bedrock:InvokeModel"]
        Resource = [local.embed_model]
      },
      {
        Sid    = "S3VectorsData"
        Effect = "Allow"
        # The verbs a Knowledge Base uses. The wildcard also granted DeleteIndex and
        # DeleteVectorBucket, which a KB never calls. Mirrors cdk/lib/tool-plane.ts.
        Action = [
          "s3vectors:GetVectorBucket", "s3vectors:GetIndex", "s3vectors:ListIndexes",
          "s3vectors:PutVectors", "s3vectors:GetVectors", "s3vectors:ListVectors",
          "s3vectors:QueryVectors", "s3vectors:DeleteVectors"
        ]
        Resource = [aws_s3vectors_vector_bucket.kb[0].vector_bucket_arn, "${aws_s3vectors_vector_bucket.kb[0].vector_bucket_arn}/*"]
      },
      ], local.kb_upload ? [
      {
        Sid      = "DocsRead"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.kb_docs[0].arn, "${aws_s3_bucket.kb_docs[0].arn}/*"]
      }
      ] : [], local.kb_source == "s3" ? [
      # YOUR bucket: read only, and only under the prefix named. Mirrors the DocsRead /
      # DocsList / KmsDecrypt statements in cdk/lib/tool-plane.ts.
      {
        Sid      = "DocsRead"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["arn:aws:s3:::${local.kb_s3_bucket}/${local.kb_s3_prefix}*"]
      }
      ] : [], local.kb_source == "s3" && local.kb_s3_prefix == "" ? [
      {
        Sid      = "DocsList"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = ["arn:aws:s3:::${local.kb_s3_bucket}"]
      }
      ] : [], local.kb_source == "s3" && local.kb_s3_prefix != "" ? [
      {
        Sid       = "DocsList"
        Effect    = "Allow"
        Action    = ["s3:ListBucket"]
        Resource  = ["arn:aws:s3:::${local.kb_s3_bucket}"]
        Condition = { StringLike = { "s3:prefix" = ["${local.kb_s3_prefix}*"] } }
      }
      ] : [], local.kb_kms_key == "" ? [] : [
      {
        Sid      = "KmsDecrypt"
        Effect   = "Allow"
        Action   = ["kms:Decrypt"]
        Resource = [local.kb_kms_key]
      }
    ])
  })
}

# --- Knowledge Base + data source -----------------------------------------

resource "aws_bedrockagent_knowledge_base" "kb" {
  count    = local.kb_create ? 1 : 0
  name     = local.kb_name
  role_arn = aws_iam_role.kb[0].arn

  knowledge_base_configuration {
    type = "VECTOR"
    vector_knowledge_base_configuration {
      embedding_model_arn = local.embed_model
      embedding_model_configuration {
        bedrock_embedding_model_configuration {
          dimensions          = local.kb_dims
          embedding_data_type = "FLOAT32"
        }
      }
    }
  }

  storage_configuration {
    type = "S3_VECTORS"
    s3_vectors_configuration {
      index_arn = aws_s3vectors_index.kb[0].index_arn
    }
  }

  depends_on = [aws_iam_role_policy.kb]
}

resource "aws_bedrockagent_data_source" "kb" {
  count             = local.kb_create ? 1 : 0
  knowledge_base_id = aws_bedrockagent_knowledge_base.kb[0].id
  name              = "docs"

  data_source_configuration {
    type = "S3"
    s3_configuration {
      bucket_arn         = local.kb_upload ? aws_s3_bucket.kb_docs[0].arn : "arn:aws:s3:::${local.kb_s3_bucket}"
      inclusion_prefixes = local.kb_s3_prefix == "" ? null : [local.kb_s3_prefix]
    }
  }
}

# Trigger ingestion when the corpus changes. No declarative equivalent exists for
# StartIngestionJob; this mirrors the existing build_push local-exec pattern.
resource "null_resource" "kb_ingest" {
  count = local.kb_create ? 1 : 0
  # Uploads re-ingest when they change. YOUR bucket changes without Terraform knowing, so
  # it is re-synced on every apply (ingestion is incremental: unchanged files are skipped).
  triggers = {
    docs_hash = local.kb_upload ? sha1(join(",", [for f in sort(local.kb_files) : filemd5("${path.module}/../kb_docs/${f}")])) : timestamp()
    meta_hash = sha1(jsonencode({ for f in local.kb_files : f => split("/", f)[0] }))
    ds_id     = aws_bedrockagent_data_source.kb[0].data_source_id
  }
  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      aws bedrock-agent start-ingestion-job --region ${var.region} \
        --knowledge-base-id ${aws_bedrockagent_knowledge_base.kb[0].id} \
        --data-source-id ${aws_bedrockagent_data_source.kb[0].data_source_id}
    EOT
  }
  depends_on = [aws_s3_object.kb_docs, aws_s3_object.kb_docs_metadata]
}

locals {
  # The Knowledge Base retrieval reads: the one made here, or yours.
  kb_id  = local.kb_source == "existing" ? local.kb_existing_id : try(aws_bedrockagent_knowledge_base.kb[0].id, "")
  kb_arn = local.kb_source == "existing" ? "arn:aws:bedrock:${var.region}:${local.account_id}:knowledge-base/${local.kb_existing_id}" : try(aws_bedrockagent_knowledge_base.kb[0].arn, "")
}

# --- Retrieve Lambda (Gateway Lambda target) ------------------------------

data "archive_file" "kb_lambda" {
  count       = local.kb_enabled ? 1 : 0
  type        = "zip"
  source_dir  = "${path.module}/../kb_lambda"
  output_path = "${path.module}/.build/kb_lambda.zip"
}

resource "aws_iam_role" "kb_lambda" {
  count = local.kb_enabled ? 1 : 0
  name  = "AgentCoreKBLambda-${var.agent_name}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "kb_lambda" {
  count = local.kb_enabled ? 1 : 0
  name  = "KBLambdaPolicy-${var.agent_name}"
  role  = aws_iam_role.kb_lambda[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:${var.region}:${local.account_id}:*"
      },
      {
        Effect   = "Allow"
        Action   = ["bedrock:Retrieve"]
        Resource = [local.kb_arn]
      },
      ], local.kb_rerank_arn == "" ? [] : [
      {
        # Reranking is a SECOND model call made by Bedrock on this function's behalf, so
        # the function's own role needs it. Granted only when the tool asks for
        # reranking, and on exactly the model it named — not on foundation-model/* .
        Sid      = "InvokeRerankingModel"
        Effect   = "Allow"
        Action   = ["bedrock:Rerank", "bedrock:InvokeModel"]
        Resource = [local.kb_rerank_arn]
      }
    ])
  })
}

resource "aws_cloudwatch_log_group" "kb_retrieve" {
  count             = local.kb_enabled ? 1 : 0
  name              = "/aws/lambda/AgentCoreKBRetrieve-${var.agent_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "kb_retrieve" {
  count            = local.kb_enabled ? 1 : 0
  function_name    = "AgentCoreKBRetrieve-${var.agent_name}"
  role             = aws_iam_role.kb_lambda[0].arn
  runtime          = "python3.13"
  handler          = "handler.lambda_handler"
  filename         = data.archive_file.kb_lambda[0].output_path
  source_code_hash = data.archive_file.kb_lambda[0].output_base64sha256
  timeout          = 30
  memory_size      = 256

  environment {
    variables = {
      KB_ID = local.kb_id
      # Retrieval depth, from `tools.<kb>.maxResults` in app/workflow.json. It was a
      # Lambda-only env var that no IaC set, so depth was frozen at the handler's
      # default of 5 and the only way to change it was to hand-edit a deployed
      # function — while `tools.<websearch>.maxResults` was a config key. Same key
      # name on both tool types now.
      KB_NUM_RESULTS = tostring(local.kb_max_results)
      # The rest of the retrieval shape, also from the tool's entry. Empty means "keep
      # the handler's default", so this block does not restate defaults that already
      # have one documented home.
      KB_CORPUS_KEY      = local.kb_corpus_key
      KB_CORPUS_OPERATOR = local.kb_corpus_operator
      KB_STATIC_FILTER   = local.kb_static_filter
      KB_RERANK          = local.kb_rerank
    }
  }
  # The log group must exist BEFORE the function, or Lambda creates
  # /aws/lambda/<name> itself and Terraform's CreateLogGroup then fails with
  # ResourceAlreadyExistsException. Nothing in the function's arguments references the
  # group, so without this they are created in parallel and the apply is a race — which
  # is exactly how it failed on the first real apply, for two of the four functions.
  # (The CDK path gets this ordering for free by passing the group as `logGroup:`.)
  depends_on = [aws_cloudwatch_log_group.kb_retrieve[0]]
}

# Allow the Gateway (via its execution role) to invoke the retrieve Lambda.
resource "aws_iam_role_policy" "gateway_invoke_kb" {
  count = local.kb_enabled ? 1 : 0
  name  = "GatewayInvokeKB-${var.agent_name}"
  role  = aws_iam_role.gateway[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["lambda:InvokeFunction"]
      Resource = [aws_lambda_function.kb_retrieve[0].arn]
    }]
  })
}

resource "aws_lambda_permission" "gateway_invoke_kb" {
  count          = local.kb_enabled ? 1 : 0
  statement_id   = "AllowAgentCoreGatewayInvoke"
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.kb_retrieve[0].function_name
  principal      = "bedrock-agentcore.amazonaws.com"
  source_account = local.account_id
}

# --- Gateway target: KB retrieve tool -------------------------------------

resource "aws_bedrockagentcore_gateway_target" "kb" {
  count              = local.kb_enabled ? 1 : 0
  gateway_identifier = aws_bedrockagentcore_gateway.mcp[0].gateway_id
  # Never "", even with count = 0: the provider validates this block's literal values
  # whether or not an instance exists, so a workflow with NO knowledge-base tool failed
  # validation on an empty name. Found deploying a tool-less build with Terraform.
  name        = local.kb_enabled ? local.kb_tool_name : "no-kb-tool"
  description = "Bedrock Knowledge Base retrieval tool"

  target_configuration {
    mcp {
      lambda {
        lambda_arn = aws_lambda_function.kb_retrieve[0].arn
        tool_schema {
          inline_payload {
            name        = "retrieve"
            description = "Retrieve relevant document chunks from the knowledge base for a query."
            input_schema {
              type = "object"
              property {
                name        = "query"
                type        = "string"
                required    = true
                description = "The natural-language search query."
              }
              property {
                name        = "filter"
                type        = "string"
                required    = false
                description = "Optional doc_type to restrict retrieval to one corpus (e.g. reference)."
              }
            }
          }
        }
      }
    }
  }

  credential_provider_configuration {
    gateway_iam_role {}
  }
}
