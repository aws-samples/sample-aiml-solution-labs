# ===========================================================================
# type = "smithy" — an AWS service described by a Smithy model, as a Gateway target
# ===========================================================================
# The Gateway turns each operation in the model into a tool ("<target>___<operation>")
# and calls the service signed by this build's Gateway role. So the tool's `allow` (AWS
# actions on resources) is granted to that role (gateway.tf, local.smithy_statements),
# and nothing else is. Mirrors cdk/lib/smithy-tool.ts; the Build view's checks are
# bff/smithy_tool.py and web/src/builder/smithy.ts.
#
# From the docs and the provider schema (hashicorp/aws 6.67.0):
#   * target_configuration.mcp.smithy_model { inline_payload { payload } | s3 { uri } }.
#   * Outbound auth: the Gateway role alone. "For Lambda, API Gateway, and Smithy targets,
#     do not include the iamCredentialProvider field", so gateway_iam_role {} is empty.
#   * Custom Smithy models are for AWS services only, RestJson protocol, up to 10 MB.
# https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-building-smithy-targets.html
# https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-outbound-auth.html

locals {
  smithy_tools = { for n, t in local.tools : n => t if t.type == "smithy" }
  # Each tool's grant, read from the raw entry: `allow` is smithy-only, so it is not part
  # of the normalised local.tools shape every other type shares.
  smithy_allow = { for n, t in local.smithy_tools : n => {
    actions   = try([for a in local.tools_raw[n].allow.actions : tostring(a)], [])
    resources = try([for r in local.tools_raw[n].allow.resources : tostring(r)], [])
  } }
  # A hosted model is read by the Gateway, as its role, when the target is created.
  smithy_model_arns = [for n, t in local.smithy_tools :
    "arn:aws:s3:::${replace(t.schema_s3_uri, "s3://", "")}" if t.schema_json == "" && t.schema_s3_uri != ""
  ]
  # Same patterns as bff/smithy_tool.py ACTION_RE / ARN_RE.
  smithy_action_re = "^[a-z0-9-]+:[A-Za-z0-9]+$"
  smithy_arn_re    = "^arn:aws[a-z-]*:[a-z0-9-]+:([a-z]{2}(-[a-z]+)+-[0-9]+)?:([0-9]{12})?:[^*\\s]\\S*$"
  smithy_denied    = local.vocab.smithyDeniedServices.values

  # Statements for the Gateway role, concatenated into aws_iam_role_policy.gateway so the
  # IAM wait before targets are created covers them. Empty with no smithy tool.
  smithy_statements = concat(
    length(local.smithy_model_arns) == 0 ? [] : [{
      Sid      = "ReadSmithyModels"
      Effect   = "Allow"
      Action   = ["s3:GetObject"]
      Resource = local.smithy_model_arns
    }],
    [for n, a in local.smithy_allow : {
      Sid      = "SmithyCalls${n}"
      Effect   = "Allow"
      Action   = a.actions
      Resource = a.resources
    } if length(a.actions) > 0 && length(a.resources) > 0],
  )
}

resource "terraform_data" "smithy_validation" {
  count = length(local.smithy_tools) > 0 ? 1 : 0
  input = keys(local.smithy_tools)

  lifecycle {
    precondition {
      condition = alltrue([for n, t in local.smithy_tools :
      length([for v in [t.schema_s3_uri, t.schema_json] : v if v != ""]) == 1])
      error_message = "A tools entry with type=\"smithy\" needs EXACTLY ONE of \"schemaS3Uri\" (an s3:// Smithy JSON model you already host) or \"schema\" (the model itself, inline). Offending: ${join(", ", [for n, t in local.smithy_tools : n if length([for v in [t.schema_s3_uri, t.schema_json] : v if v != ""]) != 1])}."
    }
    precondition {
      condition = alltrue([for n, t in local.smithy_tools :
      t.schema_s3_uri == "" || can(regex("^s3://[a-z0-9.-]{3,63}/.+", t.schema_s3_uri))])
      error_message = "A type=\"smithy\" tool's \"schemaS3Uri\" must be a full s3:// URI including the object key, e.g. \"s3://amzn-s3-demo-bucket/models/orders.json\"."
    }
    precondition {
      condition = alltrue([for n, t in local.smithy_tools : t.schema_json == "" || (
        can(tostring(local.tools_raw[n].schema.smithy))
        && anytrue([for s in values(try(local.tools_raw[n].schema.shapes, {})) : try(s.type, "") == "service"])
        && anytrue([for s in values(try(local.tools_raw[n].schema.shapes, {})) : try(s.type, "") == "operation"])
      )])
      error_message = "A type=\"smithy\" tool's \"schema\" must be a Smithy JSON model ({\"smithy\": \"2.0\", \"shapes\": {...}}) with a service shape and at least one operation."
    }
    precondition {
      condition = alltrue([for n, a in local.smithy_allow :
        length(a.actions) > 0 && length(a.actions) <= 20
      && alltrue([for x in a.actions : can(regex(local.smithy_action_re, x))])])
      error_message = "A type=\"smithy\" tool's \"allow.actions\" must list 1 to 20 named IAM actions, like \"dynamodb:GetItem\" (no *): what the Gateway role may call for this tool."
    }
    precondition {
      condition = alltrue([for n, a in local.smithy_allow :
      alltrue([for x in a.actions : !contains(local.smithy_denied, split(":", x)[0])])])
      error_message = "A type=\"smithy\" tool's \"allow.actions\" may not name ${join(", ", local.smithy_denied)}: a Smithy tool's calls run as the Gateway role every tool shares."
    }
    precondition {
      condition = alltrue([for n, a in local.smithy_allow :
        length(a.resources) > 0 && length(a.resources) <= 20
      && alltrue([for x in a.resources : can(regex(local.smithy_arn_re, x))])])
      error_message = "A type=\"smithy\" tool's \"allow.resources\" must list 1 to 20 full ARNs, region and account written out (or empty where the service has none), and no bare *."
    }
  }
}

resource "aws_bedrockagentcore_gateway_target" "smithy" {
  for_each           = local.smithy_tools
  gateway_identifier = aws_bedrockagentcore_gateway.mcp[0].gateway_id
  name               = each.key
  description        = each.value.description

  target_configuration {
    mcp {
      smithy_model {
        dynamic "inline_payload" {
          for_each = each.value.schema_json != "" ? [1] : []
          content {
            payload = each.value.schema_json
          }
        }
        dynamic "s3" {
          for_each = each.value.schema_json == "" ? [1] : []
          content {
            uri = each.value.schema_s3_uri
          }
        }
      }
    }
  }

  # The Gateway role, alone: no service or region (see the note at the top).
  credential_provider_configuration {
    gateway_iam_role {}
  }

  # The role reads a hosted model when the target is created, and makes the calls.
  depends_on = [aws_iam_role_policy.gateway, terraform_data.smithy_validation]
}
