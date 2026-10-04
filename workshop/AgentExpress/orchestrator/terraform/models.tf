# The deployment's default model, and every model id the workflow names, moved to this
# region's cross-region inference-profile geography.
#
# A profile (`us.` in `us.anthropic.claude-...`) answers only from a source region in its
# own geography. The framework default and the sample workflow are `us.` profiles, so the
# same config deployed in eu-west-1 failed every model call. The rule and the region ->
# geography table live once in app/vocabulary.json (`inferenceProfileGeos`); the same rule
# is implemented in app/common/vocabulary.py, bff/workflow.py and cdk/lib/vocabulary.ts.
# `global.` profiles and bare foundation-model ids are left alone.
locals {
  model_geos = local.vocab.inferenceProfileGeos
  region_geo = try(
    [for r in local.model_geos.byRegionPrefix : r[1] if startswith(var.region, r[0])][0],
    local.model_geos.fallback
  )

  # An explicit model_id, else this workflow's orchestrator.defaultModel, else the
  # framework default from app/defaults.json. var.model_id used to default to the literal,
  # and BEDROCK_MODEL_ID (which the app reads first) silently overrode the workflow's own
  # defaultModel on every deployment.
  model_requested = coalesce(
    var.model_id,
    try(local.workflow_def.orchestrator.defaultModel, ""),
    local.key_defaults.orchestrator.defaultModel
  )

  _named_models = distinct(compact(concat(
    [local.model_requested],
    [for id, c in local.memory_customs : try(c.model, "")],
    flatten([for id, a in local.agents_def : [for c in try(a.agentcore.evaluations.custom, []) : try(c.model, "")]]),
    [for n, c in local.shared_evaluator_defs : try(c.model, "")],
  )))
  regional_model = {
    for m in local._named_models : m => (
      contains(local.model_geos.values, split(".", m)[0])
      && split(".", m)[0] != "global"
      && split(".", m)[0] != local.region_geo
      ? "${local.region_geo}.${join(".", slice(split(".", m), 1, length(split(".", m))))}"
      : m
    )
  }

  model_id = local.regional_model[local.model_requested]
}
