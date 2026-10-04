# --- Static UI on S3 + CloudFront -----------------------------------------

resource "aws_s3_bucket" "ui" {
  # S3 bucket names must be lowercase and cannot contain underscores.
  bucket        = "agentcore-${replace(var.agent_name, "_", "-")}-ui-${local.account_id}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "ui" {
  bucket                  = aws_s3_bucket.ui.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "ui" {
  bucket = aws_s3_bucket.ui.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# --- build the UI ----------------------------------------------------------
# web/ is a Vite + React + Cloudscape app, so there is a build step now. It runs here
# rather than being a thing you have to remember: `terraform apply` produces the same
# bundle as CI, and a stale dist/ cannot be uploaded by accident.
#
# The trigger is a hash of the SOURCE, not a timestamp, so an apply that changes nothing
# in web/ does not rebuild — and one that changes a single component does.
locals {
  ui_src_files = [
    for f in fileset("${path.module}/../web", "{src,legacy}/**") : f
  ]
  ui_src_hash = sha1(join("", concat(
    [
      filesha1("${path.module}/../web/package.json"),
      filesha1("${path.module}/../web/vite.config.ts"),
      filesha1("${path.module}/../web/tsconfig.json"),
      filesha1("${path.module}/../web/index.html"),
    ],
    [for f in sort(local.ui_src_files) : filesha1("${path.module}/../web/${f}")]
  )))
  ui_dist = "${path.module}/../web/dist"
}

resource "null_resource" "ui_build" {
  triggers = { src = local.ui_src_hash }

  provisioner "local-exec" {
    working_dir = "${path.module}/../web"
    # `npm ci` when the lockfile is present, so a deploy installs exactly what was
    # tested. The build runs `tsc --noEmit` first, so a type error fails the apply
    # instead of shipping a broken bundle.
    # `--include=dev` is not redundant: npm skips devDependencies when NODE_ENV is
    # "production", and typescript and vite ARE devDependencies — so an apply from a
    # shell with NODE_ENV=production would remove the tools the next line needs and fail
    # with "tsc: not found". The flag makes the install independent of the caller's
    # environment. Which React ends up in the bundle is settled in web/vite.config.ts.
    command     = <<-EOT
      set -e
      if [ -f package-lock.json ]; then npm ci --include=dev --no-fund --no-audit; else npm install --include=dev --no-fund --no-audit; fi
      npm run build
      cp legacy/observability.js dist/observability.js
    EOT
    interpreter = ["/bin/bash", "-c"]
  }
}

# --- upload the built bundle -----------------------------------------------
# Every file Vite emitted. `for_each` over a fileset computed AFTER the build, so the
# hashed asset names are picked up without being listed anywhere.
data "external" "ui_dist" {
  depends_on = [null_resource.ui_build]
  program = ["/bin/bash", "-c", <<-EOT
    cd "${local.ui_dist}" 2>/dev/null || { echo '{}'; exit 0; }
    # name -> md5, as a flat JSON object (the only shape `external` accepts).
    printf '{'
    first=1
    while IFS= read -r f; do
      f="$${f#./}"
      [ $first -eq 0 ] && printf ','
      first=0
      printf '"%s":"%s"' "$f" "$(md5 -q "$f" 2>/dev/null || md5sum "$f" | cut -d' ' -f1)"
    done < <(find . -type f | sort)
    printf '}'
  EOT
  ]
}

resource "aws_s3_object" "ui_asset" {
  for_each = data.external.ui_dist.result

  bucket = aws_s3_bucket.ui.id
  key    = each.key
  source = "${local.ui_dist}/${each.key}"
  etag   = each.value

  content_type = lookup({
    "html"  = "text/html",
    "js"    = "application/javascript",
    "css"   = "text/css",
    "map"   = "application/json",
    "json"  = "application/json",
    "svg"   = "image/svg+xml",
    "png"   = "image/png",
    "ico"   = "image/x-icon",
    "woff"  = "font/woff",
    "woff2" = "font/woff2",
  }, element(reverse(split(".", each.key)), 0), "application/octet-stream")

  # index.html must revalidate so a deploy lands immediately. Everything else carries a
  # content hash in its NAME, so it can be cached hard — which is the whole point of
  # hashed asset names, and what stops a stale bundle being served against fresh HTML.
  cache_control = each.key == "index.html" ? "no-cache" : (
    each.key == "observability.js" ? "no-cache" : "public, max-age=31536000, immutable"
  )
}

# Non-secret Cognito settings injected into the SPA at deploy time. Rendered from
# a template so index.html stays static (its JS uses ${...} literals that must
# not be interpreted by Terraform).
# Non-secret IdP settings the SPA reads at load time. local.ui_auth is built in
# identity.tf and already carries the right fields for the selected provider, so
# this stays provider-agnostic.
resource "aws_s3_object" "auth_config" {
  bucket        = aws_s3_bucket.ui.id
  key           = "auth-config.js"
  content_type  = "application/javascript"
  cache_control = "no-cache"
  content       = templatefile("${path.module}/../web/auth-config.js.tftpl", local.ui_auth)
  etag          = md5(templatefile("${path.module}/../web/auth-config.js.tftpl", local.ui_auth))
}

resource "aws_cloudfront_origin_access_control" "ui" {
  name                              = "agentcore-${var.agent_name}-oac"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

locals {
  api_host = replace(aws_apigatewayv2_api.bff.api_endpoint, "https://", "")
  # AWS managed policies
  cache_disabled_id   = "4135ea2d-6df8-44a3-9df3-4b5a84be39ad"
  cache_optimized_id  = "658327ea-f89d-4fab-a63d-7e88639e58f6"
  orp_all_except_host = "b689b0a8-53d0-40ab-baf2-68738e2966ac"
}

# Security headers on every response, UI and /api alike. No script-src policy: the SPA
# talks to the configured IdP and to presigned S3 URLs, which vary by deploy, so the CSP
# only forbids framing, plugins and <base> rewrites. Mirrors the CDK UiSecurityHeaders.
resource "aws_cloudfront_response_headers_policy" "ui_security" {
  name = "agentcore-${replace(var.agent_name, "_", "-")}-security-headers"
  security_headers_config {
    strict_transport_security {
      access_control_max_age_sec = 31536000
      include_subdomains         = true
      preload                    = true
      override                   = true
    }
    content_type_options {
      override = true
    }
    frame_options {
      frame_option = "DENY"
      override     = true
    }
    referrer_policy {
      referrer_policy = "strict-origin-when-cross-origin"
      override        = true
    }
    content_security_policy {
      content_security_policy = "frame-ancestors 'none'; object-src 'none'; base-uri 'self'"
      override                = true
    }
  }
}

resource "aws_cloudfront_distribution" "ui" {
  enabled             = true
  default_root_object = "index.html"
  comment             = "Multi-agent orchestrator UI"

  origin {
    origin_id                = "s3-ui"
    domain_name              = aws_s3_bucket.ui.bucket_regional_domain_name
    origin_access_control_id = aws_cloudfront_origin_access_control.ui.id
  }

  origin {
    origin_id   = "api-bff"
    domain_name = local.api_host
    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "https-only"
      origin_ssl_protocols   = ["TLSv1.2"]
    }
  }

  default_cache_behavior {
    target_origin_id           = "s3-ui"
    viewer_protocol_policy     = "redirect-to-https"
    allowed_methods            = ["GET", "HEAD"]
    cached_methods             = ["GET", "HEAD"]
    cache_policy_id            = local.cache_optimized_id
    response_headers_policy_id = aws_cloudfront_response_headers_policy.ui_security.id
  }

  ordered_cache_behavior {
    path_pattern               = "/api/*"
    target_origin_id           = "api-bff"
    viewer_protocol_policy     = "https-only"
    allowed_methods            = ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"]
    cached_methods             = ["GET", "HEAD"]
    cache_policy_id            = local.cache_disabled_id
    origin_request_policy_id   = local.orp_all_except_host
    response_headers_policy_id = aws_cloudfront_response_headers_policy.ui_security.id
  }

  restrictions {
    geo_restriction { restriction_type = "none" }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }
}

resource "aws_s3_bucket_policy" "ui" {
  bucket = aws_s3_bucket.ui.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowCloudFrontRead"
      Effect    = "Allow"
      Principal = { Service = "cloudfront.amazonaws.com" }
      Action    = "s3:GetObject"
      Resource  = "${aws_s3_bucket.ui.arn}/*"
      Condition = {
        StringEquals = { "AWS:SourceArn" = aws_cloudfront_distribution.ui.arn }
      }
      }, {
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.ui.arn, "${aws_s3_bucket.ui.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.ui]
}


