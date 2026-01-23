# Streamlit Cloud Deployment Guide

## Problem
When deploying to Streamlit Cloud, you get `botocore.exceptions.NoCredentialsError` because the app can't access AWS credentials that are available locally.

## Solution Overview
Streamlit Cloud doesn't have access to your local AWS credentials. You need to:
1. Store AWS credentials and configuration in Streamlit Secrets
2. Modify the code to read from Streamlit Secrets instead of boto3

## Step-by-Step Fix

### Option 1: Quick Fix - Use Streamlit Secrets (Recommended)

This is the simplest approach - store all values directly in Streamlit secrets without needing AWS API access.

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

#### 2. Verify main.py is using cloud-compatible utils

The `main.py` file should already be configured to use cloud-compatible utilities:
```python
from chat_utils_cloud import make_urls_clickable
```

If you see `from chat_utils import ...` instead, update it to use `chat_utils_cloud`.

#### 3. Redeploy

Commit and push your changes. Streamlit Cloud will automatically redeploy.

### Option 1.5: Hybrid Mode - Use Streamlit Secrets with SSM API (Advanced)

This approach stores AWS credentials in secrets and allows the app to fetch parameters from SSM Parameter Store dynamically.

#### When to use this:
- You have many SSM parameters and don't want to copy them all to secrets
- You want parameters to stay in sync with SSM Parameter Store
- You're comfortable managing AWS credentials in Streamlit secrets

#### Setup:

Add only AWS credentials to Streamlit secrets:

```toml
# AWS Credentials only
AWS_ACCESS_KEY_ID = "your_access_key_id"
AWS_SECRET_ACCESS_KEY = "your_secret_access_key"
AWS_DEFAULT_REGION = "us-west-2"
```

The app will automatically use these credentials to fetch SSM parameters via boto3. No need to add `RUNTIME_ARN` or other SSM values to secrets.

#### Benefits:
- ✅ Parameters stay in sync with AWS SSM
- ✅ Fewer secrets to manage in Streamlit
- ✅ Works with dynamic parameter updates

#### Tradeoffs:
- ⚠️ Requires AWS credentials in Streamlit secrets
- ⚠️ Slightly slower (API calls vs. local secrets)
- ⚠️ Requires IAM permissions for SSM access

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
- **Cause**: The SSM parameter doesn't exist in your AWS account, or credentials are insufficient
- **Solutions**:
  - **Option 1 (Recommended for Cloud)**: Add the parameter value directly to Streamlit secrets
    - The app will show you the exact secret key name (e.g., `RUNTIME_ARN`)
    - Add it in TOML format: `RUNTIME_ARN = "your_value_here"`
  - **Option 2 (Hybrid)**: Provide AWS credentials in secrets to enable SSM API calls
    - Add `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to secrets
    - The app will use these to fetch from SSM Parameter Store
  - **Option 3 (Local)**: Verify the parameter exists and your local credentials have access
    - Run: `aws ssm get-parameter --name /your/parameter/name`
- Check you're using the correct AWS region in your secrets or environment

## Alternative: Deploy to AWS Instead

Consider deploying to AWS services that have native IAM role support:
- **AWS App Runner**: Easiest, supports IAM roles
- **ECS Fargate**: Container-based, full IAM support
- **EC2**: Traditional VM, attach IAM role
- **Lambda + API Gateway**: Serverless option

These eliminate the need to manage credentials manually.

## Code Changes Summary

The `chat_utils_cloud.py` file provides a drop-in replacement with intelligent credential management:
1. **Robust error handling**: Gracefully handles missing secrets files with try-except blocks
2. **Multi-tier credential strategy**: 
   - Tries Streamlit secrets first (for cloud deployment)
   - Falls back to boto3 with secrets-based credentials (hybrid mode)
   - Falls back to default boto3 credential chain (for local development)
3. **Enhanced error messages**: Provides actionable guidance with exact secret key names and TOML examples
4. **Seamless compatibility**: Works in local development, Streamlit Cloud, and hybrid environments

This allows the same code to work across multiple deployment scenarios without modification.

### Key Functions in chat_utils_cloud.py

#### `get_aws_region()`
Returns the AWS region with robust error handling:
- **Primary**: Streamlit secrets (`AWS_DEFAULT_REGION`) if available
- **Fallback**: Environment variable (`AWS_DEFAULT_REGION`)
- **Default**: `us-west-2` if neither is set
- Gracefully handles `AttributeError` and `FileNotFoundError` when secrets are unavailable

#### `get_ssm_parameter(name, with_decryption=True)`
Retrieves SSM parameters with intelligent multi-mode support:
- **Cloud mode (Priority 1)**: Reads from Streamlit secrets using the last segment of the parameter name
  - Example: `/app/returnsrefunds/agentcore/runtime_arn` → looks for `RUNTIME_ARN` in secrets
  - Gracefully handles missing secrets file
- **Hybrid mode (Priority 2)**: Uses boto3 with credentials from Streamlit secrets if available
  - Allows SSM API calls even in cloud environments when credentials are provided
  - Falls back to default boto3 credential chain if secrets are unavailable
- **Local mode (Priority 3)**: Uses boto3 with default credential chain (IAM roles, ~/.aws/credentials)
- **Enhanced error messages**: Provides helpful guidance with exact secret key names and TOML format examples when parameters are not found

#### `make_urls_clickable(text)`
Converts URLs in text to clickable HTML links (same as original)

#### `create_safe_markdown_text(text, message_placeholder)`
Safely renders markdown with proper encoding and newline handling (same as original)
