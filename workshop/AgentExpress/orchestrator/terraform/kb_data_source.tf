# --- Documents from a web crawl, Confluence, SharePoint or Salesforce ----------
#
# A type="kb" tool's `dataSource` replaces the S3 data source of kb.tf with a connector:
# the web crawler, or a Confluence, SharePoint or Salesforce site. No documents bucket is
# made, and the Knowledge Base role may read the connector's secret and nothing else.
# AWS supports these connectors only on an OpenSearch Serverless vector store, so the
# tool's `vectorStore` must be one (kb_vector_store.tf). Without `dataSource` nothing here
# applies and kb.tf plans exactly as before. Mirrors cdk/lib/kb-data-source.ts.
#
# https://docs.aws.amazon.com/bedrock/latest/userguide/data-source-connectors.html
# https://docs.aws.amazon.com/bedrock/latest/userguide/kb-permissions.html

locals {
  kb_ds        = try(local.kb_spec.dataSource, null)
  kb_ds_set    = local.kb_create && local.kb_ds != null
  kb_ds_type   = try(tostring(local.kb_ds.type), "")
  kb_ds_vocab  = local.vocab.kbDataSources
  kb_ds_known  = contains(local.kb_ds_vocab.values, local.kb_ds_type)
  kb_ds_specs  = local.kb_ds_vocab.settings
  kb_ds_needed = try(local.kb_ds_vocab.required[local.kb_ds_type], [])
  kb_ds_takes  = concat(local.kb_ds_needed, try(local.kb_ds_vocab.optional[local.kb_ds_type], []))

  # Each setting read once, in the shape the data source blocks (kb.tf) need.
  kb_ds_seed_urls  = try([for u in local.kb_ds.seedUrls : tostring(u)], [])
  kb_ds_site_urls  = try([for u in local.kb_ds.siteUrls : tostring(u)], [])
  kb_ds_inclusions = try([for u in local.kb_ds.inclusionFilters : tostring(u)], null)
  kb_ds_exclusions = try([for u in local.kb_ds.exclusionFilters : tostring(u)], null)
  kb_ds_rate_limit = try(tonumber(local.kb_ds.rateLimit), null)
  kb_ds_max_pages  = try(tonumber(local.kb_ds.maxPages), null)
  kb_ds_scope      = try(local.kb_ds_specs.scope.values[local.kb_ds.scope], null)
  kb_ds_user_agent = try(tostring(local.kb_ds.userAgent), null)
  kb_ds_host_url   = try(tostring(local.kb_ds.hostUrl), "")
  kb_ds_auth_type  = try(local.kb_ds_specs.authType.values[local.kb_ds.authType], "")
  kb_ds_secret     = try(tostring(local.kb_ds.credentialsSecretArn), "")
  kb_ds_domain     = try(tostring(local.kb_ds.domain), "")
  kb_ds_tenant_id  = try(tostring(local.kb_ds.tenantId), null)
  kb_ds_filters = try([for f in local.kb_ds.filters : {
    object_type = tostring(f.objectType)
    include     = try([for x in f.include : tostring(x)], null)
    exclude     = try([for x in f.exclude : tostring(x)], null)
  }], [])
  kb_ds_crawler = local.kb_ds_type == "web" && length(compact([
    for v in [local.kb_ds_rate_limit, local.kb_ds_max_pages, local.kb_ds_scope, local.kb_ds_user_agent] : try(tostring(v), "")
  ])) + length(coalesce(local.kb_ds_inclusions, [])) + length(coalesce(local.kb_ds_exclusions, [])) > 0

  # The Knowledge Base role's grant: the connector's secret. Confluence with OAuth also
  # writes it, because Bedrock stores the refreshed token there.
  kb_ds_statements = !local.kb_ds_set || local.kb_ds_secret == "" ? [] : [
    {
      Sid      = "ConnectorSecret"
      Effect   = "Allow"
      Action   = local.kb_ds_type == "confluence" && try(local.kb_ds.authType, "") == "oauth2" ? ["secretsmanager:GetSecretValue", "secretsmanager:PutSecretValue"] : ["secretsmanager:GetSecretValue"]
      Resource = [local.kb_ds_secret]
    }
  ]

  # The checks bff/validate_build.py and validateKbConnector make, for the Knowledge Base's
  # precondition (kb.tf). Comparisons sit inside try(): HCL does not short-circuit.
  kb_ds_problems = !local.kb_ds_set ? [] : (!local.kb_ds_known ? [
    "dataSource.type must be one of: ${join(", ", local.kb_ds_vocab.values)}"
    ] : concat(
    local.kb_vs_type == "opensearchServerless" ? [] : [
      "dataSource needs vectorStore.type \"opensearchServerless\": AWS supports the ${local.kb_ds_type} connector only on OpenSearch Serverless"
    ],
    local.kb_s3_uri == "" ? [] : ["dataSource takes the place of s3Uri: the documents come from one or the other"],
    [for k in local.kb_ds_needed : "dataSource.${k} is required for the ${local.kb_ds_type} connector"
    if try(local.kb_ds[k], null) == null || try(length(local.kb_ds[k]) == 0, false)],
    [for k, v in local.kb_ds : "dataSource.${k} does not apply to the ${local.kb_ds_type} connector" if k != "type" && !contains(local.kb_ds_takes, k)],
    [for k, v in local.kb_ds : "dataSource.${k} \"${try(tostring(v), "")}\" does not match ${local.kb_ds_specs[k].pattern}"
    if contains(local.kb_ds_takes, k) && local.kb_ds_specs[k].type == "string" && !can(regex(local.kb_ds_specs[k].pattern, tostring(v)))],
    [for k, v in local.kb_ds : "dataSource.${k} must be one of: ${join(", ", keys(local.kb_ds_specs[k].values))}"
    if contains(local.kb_ds_takes, k) && local.kb_ds_specs[k].type == "enum" && !can(local.kb_ds_specs[k].values[v])],
    [for k, v in local.kb_ds : "dataSource.${k} must be a whole number from ${local.kb_ds_specs[k].min} to ${local.kb_ds_specs[k].max}"
    if contains(local.kb_ds_takes, k) && local.kb_ds_specs[k].type == "int" && !try(v == floor(v) && v >= local.kb_ds_specs[k].min && v <= local.kb_ds_specs[k].max, false)],
    [for k, v in local.kb_ds : "dataSource.${k} must list ${local.kb_ds_specs[k].min} to ${local.kb_ds_specs[k].max}"
      if contains(local.kb_ds_takes, k) && local.kb_ds_specs[k].type == "list" && !try(length(v) >= local.kb_ds_specs[k].min && length(v) <= local.kb_ds_specs[k].max
    && alltrue([for x in v : try(local.kb_ds_specs[k].pattern, "") == "" || can(regex(local.kb_ds_specs[k].pattern, tostring(x)))]), false)],
    [for k, v in local.kb_ds : "dataSource.${k} must list ${local.kb_ds_specs[k].min} to ${local.kb_ds_specs[k].max} filters, each with an objectType of 1 to 50 characters and include/exclude lists of 1 to 25 patterns"
      if contains(local.kb_ds_takes, k) && local.kb_ds_specs[k].type == "filters" && !try(length(v) >= local.kb_ds_specs[k].min && length(v) <= local.kb_ds_specs[k].max
        && alltrue([for f in v : length(setsubtract(keys(f), ["objectType", "include", "exclude"])) == 0
          && length(tostring(f.objectType)) >= 1 && length(tostring(f.objectType)) <= 50
    && alltrue([for x in ["include", "exclude"] : !contains(keys(f), x) || try(length(f[x]) >= 1 && length(f[x]) <= 25, false)])]), false)],
  ))
}
