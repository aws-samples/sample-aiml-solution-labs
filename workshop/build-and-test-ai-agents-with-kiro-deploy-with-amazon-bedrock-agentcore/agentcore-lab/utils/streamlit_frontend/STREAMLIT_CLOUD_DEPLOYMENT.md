# Streamlit Cloud Deployment Guide

## Problem
When deploying to Streamlit Cloud, you get `botocore.exceptions.NoCredentialsError` because the app can't access AWS credentials that are available locally.

## Solution Overview
Streamlit Cloud doesn't have access to your local AWS credentials. You need to:
1. Store AWS credentials and configuration in Streamlit Secrets
2. Modify the code to read from Streamlit Secrets instead of boto3

## Step-by-Step Fix

### Option 1: Quick Fix - Use Streamlit Secrets (Recommended)

#### 1. Add Secrets to Streamlit Cloud

Go to your Streamlit Cloud app:
- Click "Manage app" (bottom right corner in your screenshot)
- Navigate to "Secrets" section
- Add the following in TOML format:

```toml
# AWS Credentials
AWS_ACCESS_KEY_ID = "your_access_key_id"
AWS_SECRET_ACCESS_KEY = "your_secret_access_key"
AWS_DEFAULT_REGION = "us-west-2"

# SSM Parameters (get these values from AWS SSM Parameter Store)
RUNTIME_ARN = "your_agent_runtime_arn"
```

To get the RUNTIME_ARN value:
```bash
# Run this locally where you have AWS credentials
aws ssm get-parameter --name /app/returnsrefunds/agentcore/runtime_arn --query Parameter.Value --output text
```

#### 2. Verify chat.py is using cloud-compatible utils

The `chat.py` file should already be configured to use cloud-compatible utilities:
```python
from chat_utils_cloud import make_urls_clickable, create_safe_markdown_text, get_aws_region, get_ssm_parameter
```

If you see `from chat_utils import ...` instead, update it to use `chat_utils_cloud`.

#### 3. Redeploy

Commit and push your changes. Streamlit Cloud will automatically redeploy.

### Option 2: Environment Variables (Alternative)

If you prefer not to use Streamlit Secrets, you can set environment variables:

1. In Streamlit Cloud app settings, go to "Advanced settings"
2. Add environment variables:
   - `AWS_ACCESS_KEY_ID`
   - `AWS_SECRET_ACCESS_KEY`
   - `AWS_DEFAULT_REGION`

Then modify the code to read from `os.environ` instead of boto3.

### Option 3: Use IAM Role (For AWS-hosted Streamlit)

If you're running Streamlit on an EC2 instance or ECS:
1. Attach an IAM role with necessary permissions
2. Remove explicit credential configuration
3. boto3 will automatically use the instance role

## Security Best Practices

### DO NOT:
- ❌ Hardcode credentials in your code
- ❌ Commit credentials to Git
- ❌ Share your secrets publicly

### DO:
- ✅ Use Streamlit Secrets for sensitive data
- ✅ Use IAM roles when possible
- ✅ Rotate credentials regularly
- ✅ Use least-privilege IAM policies

## Required IAM Permissions

Your AWS credentials need these permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel",
        "bedrock:Retrieve",
        "ssm:GetParameter"
      ],
      "Resource": "*"
    }
  ]
}
```

## Testing Locally with Secrets

Create `.streamlit/secrets.toml` in your project root:

```toml
AWS_ACCESS_KEY_ID = "your_access_key_id"
AWS_SECRET_ACCESS_KEY = "your_secret_access_key"
AWS_DEFAULT_REGION = "us-west-2"
RUNTIME_ARN = "your_agent_runtime_arn"
```

**Important**: Add `.streamlit/secrets.toml` to your `.gitignore`!

## Troubleshooting

### Error: "NoCredentialsError"
- Check that secrets are properly configured in Streamlit Cloud
- Verify secret names match exactly (case-sensitive)
- Ensure you've redeployed after adding secrets

### Error: "AccessDeniedException"
- Your AWS credentials don't have sufficient permissions
- Add the required IAM permissions listed above

### Error: "ExpiredToken"
- Your AWS credentials have expired
- Generate new credentials and update secrets

### Error: "Parameter not found"
- The SSM parameter doesn't exist in your AWS account
- Verify the parameter name is correct
- Check you're using the correct AWS region

## Alternative: Deploy to AWS Instead

Consider deploying to AWS services that have native IAM role support:
- **AWS App Runner**: Easiest, supports IAM roles
- **ECS Fargate**: Container-based, full IAM support
- **EC2**: Traditional VM, attach IAM role
- **Lambda + API Gateway**: Serverless option

These eliminate the need to manage credentials manually.

## Code Changes Summary

The `chat_utils_cloud.py` file provides a drop-in replacement that:
1. Tries Streamlit secrets first (for cloud deployment)
2. Falls back to boto3 (for local development)
3. Provides helpful error messages

This allows the same code to work both locally and on Streamlit Cloud.

### Key Functions in chat_utils_cloud.py

#### `get_aws_region()`
Returns the AWS region from:
- Streamlit secrets (`AWS_DEFAULT_REGION`) if available
- Environment variable (`AWS_DEFAULT_REGION`) as fallback
- Defaults to `us-west-2` if neither is set

#### `get_ssm_parameter(name, with_decryption=True)`
Retrieves SSM parameters with dual-mode support:
- **Cloud mode**: Reads from Streamlit secrets using the last segment of the parameter name
  - Example: `/app/returnsrefunds/agentcore/runtime_arn` → looks for `RUNTIME_ARN` in secrets
- **Local mode**: Falls back to boto3 SSM client for local development
- Provides clear error messages if parameter is not found

#### `make_urls_clickable(text)`
Converts URLs in text to clickable HTML links (same as original)

#### `create_safe_markdown_text(text, message_placeholder)`
Safely renders markdown with proper encoding and newline handling (same as original)
