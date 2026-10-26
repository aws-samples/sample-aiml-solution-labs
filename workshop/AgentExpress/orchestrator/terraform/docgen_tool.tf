# --- Track B docgen: the Document generator (`source: "docgen"` -> app/tools/docgen/) ---------
# Renders a PDF, PPTX or DOCX from JSON data, into a template from S3 or a built-in layout,
# and saves it with the run (runs/<run>/outputs/ in the assets bucket) and under
# `outputPrefix`. Its zip, role, log group and function are the shared built-in ones in
# tools.tf (for_each over local.builtin_lambda_tools); this file is its role's policy and
# its checks. Nothing here exists unless a tool names `source: "docgen"`. Mirrors
# cdk/lib/docgen-tool.ts.
#
# The role may write its own log group; read ONLY the template object, or the objects under
# the template folder; write ONLY run outputs in the assets bucket and objects under
# `outputPrefix`; read back the one copy it links to (a presigned link works only if its
# signer may GetObject it) and who wrote a run file whose name it was about to take
# (HeadObject, so a step's re-run replaces only its own earlier file). No s3:ListBucket, no
# other bucket, table or secret.
#   https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html
#
# Its libraries (requirements.txt) are installed into app/tools/docgen/ before the zip is
# made (deployer/runner.py install_builtin_requirements): about 16 MB zipped and 43 MB
# unpacked, inside Lambda's 50 MB zipped (direct upload, which aws_lambda_function's
# `filename` is) and 250 MB unzipped limits, so no layer or container image is needed.
#   https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html
#   https://docs.aws.amazon.com/lambda/latest/dg/python-package.html
locals {
  # The argument the runtime sets on every call (app/features/gateway/client.py _run_args)
  # that the deploy publishes itself in generate_document's schema (tools.tf tool_schema),
  # so no workflow.json has to declare it: this run of the step, so its new attempt
  # replaces the file its earlier attempt wrote. Mirrors DOCGEN_PUBLISHED_ARGS in
  # cdk/lib/docgen-tool.ts.
  docgen_published_args = {
    attempt = { type = "string", description = "Set by the workflow on every call; leave it out." }
  }
  # The object ARN pattern each location covers: one object, or everything under a folder.
  docgen_template_arn = { for n, t in local.builtin_lambda_docgen_tools : n => (
    endswith(t.template_s3_uri, "/") ? "arn:aws:s3:::${trimprefix(t.template_s3_uri, "s3://")}*" : "arn:aws:s3:::${trimprefix(t.template_s3_uri, "s3://")}"
  ) if t.template_s3_uri != "" }
  docgen_output_arn = { for n, t in local.builtin_lambda_docgen_tools : n => "arn:aws:s3:::${trimprefix(t.output_prefix, "s3://")}*" if t.output_prefix != "" }
  docgen_statements = { for n, t in local.builtin_lambda_docgen_tools : n => concat(
    [{
      # The log group is created in tools.tf (aws_cloudwatch_log_group.tool), so the
      # function needs to write streams to it and nothing else.
      Sid      = "Logs"
      Effect   = "Allow"
      Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
      Resource = ["${aws_cloudwatch_log_group.tool[n].arn}:*"]
    }],
    # Only when set. A for-filter, not a conditional: the two results would be tuples of
    # different types, which Terraform refuses to unify.
    [for arn in [lookup(local.docgen_template_arn, n, "")] : {
      Sid      = "ReadItsTemplates"
      Effect   = "Allow"
      Action   = ["s3:GetObject"]
      Resource = [arn]
    } if arn != ""],
    [{
      # GetObject on the run's files with or without an output prefix: before it replaces a
      # taken name it reads (HeadObject) which step and attempt wrote it (handler.py _claim).
      Sid      = "SaveWithTheRun"
      Effect   = "Allow"
      Action   = ["s3:PutObject", "s3:GetObject"]
      Resource = ["${try(aws_s3_bucket.assets[0].arn, "")}/runs/*/outputs/*"]
    }],
    [for arn in [lookup(local.docgen_output_arn, n, "")] : {
      Sid      = "WriteItsOutputs"
      Effect   = "Allow"
      Action   = ["s3:PutObject", "s3:GetObject"]
      Resource = [arn]
    } if arn != ""],
  ) }
}

resource "aws_iam_role_policy" "tool_lambda_docgen" {
  for_each = local.builtin_lambda_docgen_tools
  name     = "ToolLambdaDocgen"
  role     = aws_iam_role.tool_lambda[each.key].id
  policy   = jsonencode({ Version = "2012-10-17", Statement = local.docgen_statements[each.key] })
}

# Checked at plan, with the validators' words (bff/docgen_tool.py, web/src/builder/docgenTool.ts).
resource "terraform_data" "docgen_validation" {
  count = length(local.builtin_lambda_docgen_tools) > 0 ? 1 : 0
  lifecycle {
    precondition {
      # The file is always saved with its run, so the run files must exist.
      condition     = local.files_on
      error_message = "The Document generator saves each file with its run: set orchestrator.files (even {}) so this deployment keeps run files. Tools: ${join(", ", keys(local.builtin_lambda_docgen_tools))}."
    }
    precondition {
      # Its libraries must be in the folder the zip is made from, or every call fails on import.
      condition     = fileexists("${path.module}/../app/tools/docgen/pptx/__init__.py")
      error_message = "The Document generator's libraries are not installed in app/tools/docgen/. A console deploy installs them; from a checkout run: python3 -m pip install -r app/tools/docgen/requirements.txt -t app/tools/docgen --platform manylinux2014_x86_64 --implementation cp --python-version 3.12 --only-binary=:all:"
    }
    precondition {
      # The Track E build-storage token. tools.tf resolves it to the build's bucket
      # (storage.tf storage_uri); it is left as written only when the build creates no
      # storage, and then a location naming it is refused rather than granted on a bucket
      # that does not exist (terraform_data.storage_token says the same).
      condition     = alltrue([for n, t in local.builtin_lambda_docgen_tools : !startswith(t.template_s3_uri, "$${storage}") && !startswith(t.output_prefix, "$${storage}")])
      error_message = "$${storage} needs the build's storage (orchestrator.storage): set orchestrator.storage.create to true."
    }
  }
}
