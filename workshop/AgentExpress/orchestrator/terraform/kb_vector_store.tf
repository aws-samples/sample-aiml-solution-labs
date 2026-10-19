# --- A vector store you already have -----------------------------------------
#
# A type="kb" tool's `vectorStore` points the Knowledge Base kb.tf creates at YOUR store
# (OpenSearch Serverless, an OpenSearch domain, Aurora PostgreSQL, Neptune Analytics,
# Pinecone or MongoDB Atlas) instead of the S3 Vectors bucket and
# index it would otherwise create, and grants the Knowledge Base role what AWS documents
# for that store, on exactly the resource named. Without it nothing here applies and
# kb.tf plans exactly as before. Mirrors cdk/lib/kb-vector-store.ts.
#
# The storage configuration cannot be changed in place ("Update requires: Replacement"),
# so the Knowledge Base name carries a digest of it, as the S3 Vectors one does.
#
# https://docs.aws.amazon.com/bedrock/latest/userguide/knowledge-base-setup.html
# https://docs.aws.amazon.com/bedrock/latest/userguide/kb-permissions.html

locals {
  kb_vs          = try(local.kb_spec.vectorStore, null)
  kb_vs_set      = local.kb_create && local.kb_vs != null
  kb_vs_type     = try(tostring(local.kb_vs.type), "")
  kb_vs_vocab    = local.vocab.kbVectorStores
  kb_vs_known    = contains(local.kb_vs_vocab.values, local.kb_vs_type)
  kb_vs_required = try(local.kb_vs_vocab.required[local.kb_vs_type], [])
  kb_vs_settings = concat(local.kb_vs_required, try(local.kb_vs_vocab.optional[local.kb_vs_type], []))
  # Every setting the store takes, "" when absent: the blocks below read from here.
  kb_vs_v = { for k in local.kb_vs_settings : k => try(tostring(local.kb_vs[k]), "") }
  # S3 Vectors only when the tool names no store of its own.
  kb_s3vectors = local.kb_create && local.kb_vs == null

  # Must match kbVectorStoreKey() in cdk/lib/kb-vector-store.ts character for character.
  kb_vs_key  = "vs=${local.kb_vs_type}:${join(",", [for k in local.kb_vs_settings : local.kb_vs_v[k]])}|d=${local.kb_dims}|m=${local.kb_embed_model_id}"
  kb_vs_name = "${replace(var.agent_name, "_", "-")}-kb-${substr(sha256(local.kb_vs_key), 0, 8)}"

  # Neptune Analytics (GraphRAG): the graph construction model, which the data source
  # sends as its context enrichment (kb.tf, kb_ingestion.tf).
  kb_graph_model = local.kb_vs_type == "neptuneAnalytics" ? try(local.kb_vs_v.graphModel, "") : ""

  # The Knowledge Base role's grants on the store, from "Create a service role for Amazon
  # Bedrock Knowledge Bases". Concatenated into aws_iam_role_policy.kb.
  kb_vs_secret = !local.kb_vs_set || try(local.kb_vs_v.credentialsSecretArn, "") == "" ? [] : [
    {
      Sid      = "VectorStoreSecret"
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = [local.kb_vs_v.credentialsSecretArn]
    }
  ]
  kb_vs_statements = !local.kb_vs_set ? [] : concat(
    local.kb_vs_type == "opensearchServerless" ? [
      { Sid = "VectorStore", Effect = "Allow", Action = ["aoss:APIAccessAll"], Resource = [local.kb_vs_v.collectionArn] }
    ] : [],
    local.kb_vs_type == "opensearchManaged" ? [
      # The index and the requests under it (_search, _doc, ...): that index only.
      {
        Sid      = "VectorStore"
        Effect   = "Allow"
        Action   = ["es:ESHttpGet", "es:ESHttpPost", "es:ESHttpPut", "es:ESHttpDelete"]
        Resource = ["${local.kb_vs_v.domainArn}/${local.kb_vs_v.indexName}", "${local.kb_vs_v.domainArn}/${local.kb_vs_v.indexName}/*"]
      },
      { Sid = "VectorStoreDomain", Effect = "Allow", Action = ["es:DescribeDomain"], Resource = [local.kb_vs_v.domainArn] }
    ] : [],
    local.kb_vs_type == "aurora" ? [
      {
        Sid      = "VectorStore"
        Effect   = "Allow"
        Action   = ["rds:DescribeDBClusters", "rds-data:BatchExecuteStatement", "rds-data:ExecuteStatement"]
        Resource = [local.kb_vs_v.resourceArn]
      }
    ] : [],
    local.kb_vs_type == "neptuneAnalytics" ? [
      {
        Sid      = "VectorStore"
        Effect   = "Allow"
        Action   = ["neptune-graph:GetGraph", "neptune-graph:ReadDataViaQuery", "neptune-graph:WriteDataViaQuery", "neptune-graph:DeleteDataViaQuery"]
        Resource = [local.kb_vs_v.graphArn]
      }
    ] : [],
    local.kb_vs_secret,
  )

  # The checks bff/validate_build.py and validateKbVectorStore make, for the Knowledge
  # Base's precondition (kb.tf). Empty when the store is valid.
  kb_vs_pattern = { for k in local.kb_vs_settings : k => (
    local.kb_vs_type == "aurora" && contains(["vectorField", "textField", "metadataField"], k)
    ? local.kb_vs_vocab.columnPattern : local.kb_vs_vocab.patterns[k]
  ) }
  kb_vs_problems = !local.kb_vs_set ? [] : (!local.kb_vs_known ? [
    "vectorStore.type must be one of: ${join(", ", local.kb_vs_vocab.values)}"
    ] : concat(
    [for k in local.kb_vs_required : "vectorStore.${k} is required for the ${local.kb_vs_type} store" if local.kb_vs_v[k] == ""],
    [for k, v in local.kb_vs : "vectorStore.${k} does not apply to the ${local.kb_vs_type} store" if k != "type" && !contains(local.kb_vs_settings, k)],
    [for k in local.kb_vs_settings : "vectorStore.${k} \"${local.kb_vs_v[k]}\" does not match ${local.kb_vs_pattern[k]}"
    if local.kb_vs_v[k] != "" && !can(regex(local.kb_vs_pattern[k], local.kb_vs_v[k]))],
  ))
}
