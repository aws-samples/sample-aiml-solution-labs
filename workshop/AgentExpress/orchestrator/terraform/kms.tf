# One customer managed KMS key per deployment, for data at rest: the DynamoDB tables,
# the Secrets Manager secrets, the orchestrator's ECR repository, the Builder's CodeBuild
# project and scheduled-trigger payloads. Mirrors DataKey in cdk/lib/orchestrator-stack.ts.
#
# The key policy has the shape of an AWS managed key's (aws/dynamodb, aws/secretsmanager):
# any principal in THIS account may use the key, but only through one of those services
# acting on its behalf (kms:ViaService). So every role that can already read a table or a
# secret keeps working with no KMS statement of its own, and nobody can call KMS with the
# key directly. Administration stays with the account through IAM (the first statement).
#
# EventBridge Scheduler is the exception: it does not use grants, so the principal that
# creates a schedule and the schedule's execution role need kms permissions in IAM
# (triggers.tf, and the KmsDataKey statements in deploy-role-policy.json).
#
# Costs about $1 a month plus requests. The key is deleted with the stack, after a
# 7-day waiting period during which it can be recovered.
resource "aws_kms_key" "data" {
  description             = "AgentExpress ${var.agent_name}: tables, secrets, images, builds and schedules"
  enable_key_rotation     = true
  deletion_window_in_days = 7
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AccountAdministersKeyThroughIam"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${local.account_id}:root" }
        Action    = "kms:*"
        Resource  = "*"
      },
      {
        Sid       = "AccountUsesKeyThroughItsServices"
        Effect    = "Allow"
        Principal = { AWS = "*" }
        Action = [
          "kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*",
          "kms:CreateGrant", "kms:DescribeKey"
        ]
        Resource = "*"
        Condition = {
          StringEquals = { "kms:CallerAccount" = local.account_id }
          # dynamodb.* (any Region), as DynamoDB's own guidance requires for its grants.
          StringLike = {
            "kms:ViaService" = [
              "dynamodb.*.amazonaws.com",
              "secretsmanager.${var.region}.amazonaws.com",
              "ecr.${var.region}.amazonaws.com",
              "codebuild.${var.region}.amazonaws.com",
              "s3.${var.region}.amazonaws.com"
            ]
          }
        }
      }
    ]
  })
}

resource "aws_kms_alias" "data" {
  name          = "alias/agentexpress-${replace(var.agent_name, "_", "-")}"
  target_key_id = aws_kms_key.data.key_id
}
