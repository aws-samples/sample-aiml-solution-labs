terraform {
  # 1.10+ for S3-native state locking (use_lockfile), which removes the need for
  # a separate DynamoDB lock table.
  required_version = ">= 1.10.0"

  # Central remote state, so anyone with deploy credentials can plan/apply the
  # SAME stack. S3 stores the state (versioned + encrypted); `use_lockfile` uses
  # S3-native locking so concurrent applies can't clobber each other.
  #
  # The bucket + region are NOT hardcoded here — they are per-account. Run
  # ./bootstrap-state.sh once: it derives the account id from your AWS
  # credentials, creates the bucket, writes backend.hcl (git-ignored), and wires
  # this working directory to it.
  #
  # Prefer local state instead (a solo trial)? Comment this block out and run
  # `terraform init -migrate-state`.
  backend "s3" {
    key          = "orchestrator/terraform.tfstate"
    encrypt      = true
    use_lockfile = true
  }

  required_providers {
    aws = {
      # >= 6.64 is required: that release added the `connector` block on
      # aws_bedrockagentcore_gateway_target, which the managed AgentCore Web
      # Search tool needs (terraform/tools.tf).
      source  = "hashicorp/aws"
      version = ">= 6.64"
    }
    awscc = {
      source  = "hashicorp/awscc"
      version = ">= 1.0"
    }
    null = {
      source  = "hashicorp/null"
      version = ">= 3.2"
    }
    archive = {
      source  = "hashicorp/archive"
      version = ">= 2.4"
    }
    time = {
      source  = "hashicorp/time"
      version = ">= 0.11"
    }
    # Reads the list of files the UI build produced (ui.tf). A data source is used
    # rather than `fileset()` because fileset is evaluated at PLAN time, before the
    # build has run — so on a first apply the directory does not exist yet. With
    # `depends_on` this defers to apply time, which is when the answer exists.
    external = {
      source  = "hashicorp/external"
      version = ">= 2.3"
    }
  }
}

provider "aws" {
  region = var.region
  # Every resource: agentexpress:app always, plus var.tags (a build's are set by the
  # deploy runner). Mirrors cdk/bin/orchestrator.ts. The awscc provider has no default
  # tags, so the AgentCore runtimes and memories are the exception.
  default_tags {
    tags = merge({ "agentexpress:app" = var.agent_name }, var.tags)
  }
}

provider "awscc" {
  region = var.region
}
