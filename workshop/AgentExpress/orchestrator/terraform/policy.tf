# --- AgentCore Policy (Cedar authorization at the Gateway) -----------------
#
# Enforcement is SERVER-SIDE: when an agent calls a tool through the Gateway, the
# attached policy engine evaluates these Cedar rules and allows or denies it.
#
# YOU DO NOT HAVE TO WRITE CEDAR. The permits below are generated from the `tools`
# block in app/workflow.json (see terraform/tools.tf → local.cedar_statements), and
# orchestrator.policy.custom adds your own (aws_bedrockagentcore_policy.custom):
#
#   * declaring a tool permits that tool
#   * a tool with `policy.tool` + `policy.restrictTo` gets a fine-grained permit
#     with an argument restriction (the KB uses this to pin retrieval to its
#     declared corpora)
#   * anything NOT declared — including a tool name a prompt-injected instruction
#     invents — matches no permit and is refused by Cedar's DEFAULT-DENY
#
# orchestrator.policy in workflow.json controls the whole feature:
#   enabled = true  -> engine created + attached, decisions OBEYED (DENY blocks)
#   enabled = false -> no engine created or attached; no policy evaluation at all
#   mode    = "ENFORCE" (default) | "LOG_ONLY" (evaluate + log without blocking)

locals {
  # Policy only applies when there is a Gateway to attach it to.
  policy_enabled = local.gateway_enabled && try(local.workflow_def.orchestrator.policy.enabled, local.key_defaults.orchestrator.policy.enabled)
  policy_mode    = upper(try(local.workflow_def.orchestrator.policy.mode, local.key_defaults.orchestrator.policy.mode))

  # The Gateway ARN every generated statement is scoped to. A tool-specific
  # policy REQUIRES a specific Gateway ARN — AgentCore rejects a wildcard
  # resource or a bare `resource is AgentCore::Gateway` type constraint.
  gateway_arn_for_policy = local.gateway_enabled ? aws_bedrockagentcore_gateway.mcp[0].gateway_arn : ""
}

resource "aws_bedrockagentcore_policy_engine" "main" {
  count       = local.policy_enabled ? 1 : 0
  name        = "${var.agent_name}_policy"
  description = "Cedar policy engine governing AgentCore Gateway tool access"
}

# One policy per declared tool. Named after the tool so a denial in CloudWatch
# points straight at the config entry that governs it.
# Named permit_<key>_<8 hex of the agent name>: policy names are unique in the
# ACCOUNT, not in their engine, so a bare permit_<key> failed the second deployment in
# an account with the same tool key ("Policy with the same name already exists"). The
# suffix keeps the name within 48 characters whatever the agent name. Mirrors
# cedarPolicyName in cdk/lib/tool-plane.ts.
locals {
  cedar_policy_suffix = substr(sha1(var.agent_name), 0, 8)
}

resource "aws_bedrockagentcore_policy" "tool" {
  for_each         = local.policy_enabled ? local.cedar_statements : {}
  name             = "permit_${each.key}_${local.cedar_policy_suffix}"
  description      = "Generated from workflow.json tools.${each.key}. Anything not permitted here is denied by Cedar's default-deny."
  policy_engine_id = aws_bedrockagentcore_policy_engine.main[0].policy_engine_id

  definition {
    cedar {
      statement = each.value
    }
  }

  # The engine's analyser emits ADVISORY findings, and the default
  # (FAIL_ON_ANY_FINDINGS) turns one into a hard apply failure:
  #   "Overly Permissive: Policy Engine will allow every request for the specified
  #    principal (AgentCore::IamEntity), action (websearch___WebSearch) and resource
  #    ... combination if the policy is added or updated"
  # with the resource never reaching ACTIVE.
  #
  # That finding is CORRECT and intended: every agent shares one M2M identity, so a tool
  # permit deliberately applies to any authenticated caller. The authorization boundary
  # being demonstrated is per-TOOL (and per-argument for the KB corpus), not
  # per-principal. Findings are still recorded; they just no longer block the apply.
  #
  # cdk/lib/tool-plane.ts has set the CDK equivalent (validationMode) since the policy
  # engine was added; this path did not, so the SAME config deployed under CDK and failed
  # under Terraform. Found by the first real `terraform apply` — `validate` and `plan`
  # both pass, because the finding only exists once the engine evaluates the statement.
  validation_mode = "IGNORE_ALL_FINDINGS"

  # The Cedar engine validates action names against the gateway's REGISTERED
  # targets, so a policy can only be created after the target it governs exists.
  # Without this a fresh single apply can order the policy first and fail with
  # "unrecognized action".
  depends_on = [
    aws_bedrockagentcore_gateway_target.kb,
    aws_bedrockagentcore_gateway_target.websearch,
    aws_bedrockagentcore_gateway_target.mcp_server,
    aws_bedrockagentcore_gateway_target.openapi,
    # A type="lambda" tool produces a Cedar statement like any other, so it belongs
    # here too. Omitting it left the exact "unrecognized action" ordering failure
    # this list exists to prevent reachable on a fresh single apply.
    aws_bedrockagentcore_gateway_target.lambda_fn,
    aws_bedrockagentcore_gateway_target.apigateway,
  ]
}

# --- Your own policies: orchestrator.policy.custom --------------------------------
# One AgentCore policy each, custom_<name>_<suffix>, next to the generated permits. A
# forbid wins over any permit. The statement names the Gateway as "{{gateway}}", which
# only has an ARN once it exists; it is filled in here. bff/cedar.py (and the Build
# view) checked the rest before a build got here. Mirrors customPoliciesOf and
# customPolicyName in cdk/lib.
locals {
  # workflow.json `policies`: one written for "{{tool}}" is deployed once per tool that
  # attaches it (tools.<key>.policies), named <policy><8 hex of policy.tool> so the pair is
  # unique; one written with real tool keys is deployed once, as written. Mirrors
  # customPoliciesOf in cdk/lib/tool-plane.ts.
  named_policy_defs = try(local.workflow_def.policies, {})
  named_policy_list = concat(
    [for n, p in local.named_policy_defs : { name = n, description = try(p.description, ""), statement = p.statement }
    if !strcontains(p.statement, "{{tool}}")],
    flatten([for k, t in local.tools_raw : [
      for n in try(t.policies, []) : {
        name        = "${substr(n, 0, 20)}${substr(sha1("${n}.${k}"), 0, 8)}"
        description = "${n} on ${k}: ${try(local.named_policy_defs[n].description, "")}"
        statement   = replace(local.named_policy_defs[n].statement, "{{tool}}", k)
      } if strcontains(try(local.named_policy_defs[n].statement, ""), "{{tool}}")
    ]]),
  )
  custom_policy_list = concat(try(local.workflow_def.orchestrator.policy.custom, []), local.named_policy_list)
  # Grouped (`...`), so a name used twice reaches the precondition below instead of
  # failing the for expression with a less helpful "duplicate object key".
  custom_policy_groups = { for c in local.custom_policy_list : tostring(try(c.name, "")) => c... }
  custom_policies      = { for n, cs in local.custom_policy_groups : n => cs[0] }
}

resource "terraform_data" "custom_policy_validation" {
  lifecycle {
    precondition {
      condition     = alltrue([for n, cs in local.custom_policy_groups : can(regex("^[A-Za-z][A-Za-z0-9]{0,31}$", n)) && length(cs) == 1])
      error_message = "orchestrator.policy.custom: each policy needs a unique name, a letter then letters and digits (32 at most)."
    }
    precondition {
      condition = alltrue([for c in local.custom_policy_list :
        can(regex("^\\s*(@|permit\\s*\\(|forbid\\s*\\()", replace(tostring(try(c.statement, "")), "/(?m)^\\s*//[^\\n]*\\n/", "")))
      && strcontains(tostring(try(c.statement, "")), "AgentCore::Gateway::\"{{gateway}}\"")])
      error_message = "orchestrator.policy.custom: each statement must be one permit(...) or forbid(...) whose resource is AgentCore::Gateway::\"{{gateway}}\"."
    }
  }
}

resource "aws_bedrockagentcore_policy" "custom" {
  for_each         = local.policy_enabled ? local.custom_policies : {}
  name             = "custom_${each.key}_${local.cedar_policy_suffix}"
  description      = substr(try(each.value.description, "") != "" ? each.value.description : "workflow.json orchestrator.policy.custom ${each.key}.", 0, 400)
  policy_engine_id = aws_bedrockagentcore_policy_engine.main[0].policy_engine_id
  definition {
    cedar {
      statement = replace(each.value.statement, "{{gateway}}", local.gateway_arn_for_policy)
    }
  }
  # As for the generated permits, above.
  validation_mode = "IGNORE_ALL_FINDINGS"
  depends_on = [
    terraform_data.custom_policy_validation,
    aws_bedrockagentcore_gateway_target.kb,
    aws_bedrockagentcore_gateway_target.websearch,
    aws_bedrockagentcore_gateway_target.mcp_server,
    aws_bedrockagentcore_gateway_target.openapi,
    aws_bedrockagentcore_gateway_target.lambda_fn,
    aws_bedrockagentcore_gateway_target.apigateway,
  ]
}
