# Quick Fix for Streamlit Cloud NoCredentialsError

## Immediate Steps

### Current Error Explained
The `ClientError` you're seeing means the code is trying to call AWS SSM (Systems Manager) to get the agent ARN, but either:
- The AWS credentials in secrets are invalid/expired
- The credentials don't have permission to access SSM
- The secret key name doesn't match what the code expects

**Good news**: The updated `chat_utils_cloud.py` now provides helpful error messages that show you exactly which secret key to add and in what format. If you see an error, it will guide you to the solution.

### 1. Get Your Agent ARN
Run this locally (where AWS credentials work):
```bash
aws ssm get-parameter --name /app/returnsrefunds/agentcore/runtime_arn --query Parameter.Value --output text
```

### 2. Add Secrets to Streamlit Cloud
1. Click "Manage app" (bottom right in your Streamlit app)
2. Go to "Secrets" section
3. Paste this (replace with your actual values):

```toml
# AWS Credentials
AWS_ACCESS_KEY_ID = "AKIA..."
AWS_SECRET_ACCESS_KEY = "your_secret_key"
AWS_DEFAULT_REGION = "us-west-2"

# Agent Configuration (the value from step 1)
RUNTIME_ARN = "arn:aws:bedrock-agentcore:us-west-2:123456789012:runtime/your-runtime-id"
```

**Important**: The secret key must be `RUNTIME_ARN` (all caps, matches the last part of the SSM parameter path)

### 3. Update Your Code
In `chat.py`, change line 9:
```python
# OLD:
from chat_utils import make_urls_clickable, create_safe_markdown_text, get_aws_region, get_ssm_parameter

# NEW:
from chat_utils_cloud import make_urls_clickable, create_safe_markdown_text, get_aws_region, get_ssm_parameter
```

**Note**: This change has already been made in the file. Just verify it's correct.

### 3.5. Test Your Secrets (Optional but Recommended)
Before deploying the main app, test your secrets configuration:
1. Temporarily change your Streamlit app to use `test_secrets.py` as the main file
2. Or add `test_secrets.py` to your repo and navigate to it
3. This will show you exactly which secrets are missing or misconfigured

### 4. Commit and Push
```bash
git add .
git commit -m "Fix: Use Streamlit secrets for AWS credentials"
git push
```

Streamlit Cloud will auto-redeploy and the error should be gone!

## Files Created
- `chat_utils_cloud.py` - Cloud-compatible version with intelligent credential management:
  - Robust error handling for missing secrets files
  - Multi-tier credential strategy (secrets → boto3 with secrets → default boto3)
  - Enhanced error messages with actionable guidance
- `STREAMLIT_CLOUD_DEPLOYMENT.md` - Full deployment guide with hybrid mode options

## Security Note
Never commit your actual credentials to Git! Always use Streamlit Secrets or environment variables.


## Troubleshooting Checklist

If you still see errors after following the steps above:

### ✅ Verify Secrets Format
Make sure your secrets in Streamlit Cloud look exactly like this (no extra spaces, quotes, or formatting):
```toml
AWS_ACCESS_KEY_ID = "AKIAIOSFODNN7EXAMPLE"
AWS_SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
AWS_DEFAULT_REGION = "us-west-2"
RUNTIME_ARN = "arn:aws:bedrock-agentcore:us-west-2:123456789012:runtime/abc123"
```

### ✅ Check Secret Key Names
The secret keys are **case-sensitive** and must match exactly:
- `AWS_ACCESS_KEY_ID` (not `aws_access_key_id`)
- `AWS_SECRET_ACCESS_KEY` (not `aws_secret_access_key`)
- `AWS_DEFAULT_REGION` (not `aws_region`)
- `RUNTIME_ARN` (not `runtime_arn` or `agent_arn`)

### ✅ Verify AWS Credentials
Test your credentials locally:
```bash
# Set the credentials from your secrets
export AWS_ACCESS_KEY_ID="your_key"
export AWS_SECRET_ACCESS_KEY="your_secret"
export AWS_DEFAULT_REGION="us-west-2"

# Test if they work
aws sts get-caller-identity
```

If this fails, your credentials are invalid. Generate new ones in AWS IAM.

### ✅ Check IAM Permissions
Your AWS credentials need these permissions:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "bedrock-agentcore:InvokeRuntime",
        "ssm:GetParameter"
      ],
      "Resource": "*"
    }
  ]
}
```

### ✅ Verify the Agent ARN Value
Make sure the RUNTIME_ARN value is correct:
```bash
# This should return your agent ARN
aws ssm get-parameter --name /app/returnsrefunds/agentcore/runtime_arn --query Parameter.Value --output text
```

### ✅ Check Streamlit Logs
Click "Manage app" → "Logs" to see the full error message. Look for:
- `AccessDeniedException` → IAM permissions issue
- `InvalidClientTokenId` → Invalid AWS credentials
- `ParameterNotFound` → Wrong SSM parameter name or doesn't exist

### ✅ Redeploy After Changes
After updating secrets, Streamlit Cloud should auto-redeploy. If not:
1. Make a small change to any file (add a comment)
2. Commit and push
3. This will trigger a redeploy

## Still Not Working?

If you've tried everything above and it still doesn't work, you have two options:

### Option A: Skip SSM, Use Direct ARN (Recommended)
Instead of fetching from SSM, provide the ARN directly in secrets:
1. Get your agent ARN value (from step 1 above)
2. Add it directly to secrets as `RUNTIME_ARN`
3. The code will use it directly without calling SSM API

**This is now the recommended approach** - it's simpler and doesn't require SSM permissions.

### Option B: Use Hybrid Mode (Advanced)
Keep only AWS credentials in secrets and let the app fetch from SSM:
1. Remove `RUNTIME_ARN` from secrets
2. Keep `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_DEFAULT_REGION`
3. The app will automatically use these credentials to call SSM API
4. Requires IAM permissions for `ssm:GetParameter`

**Benefits**: Parameters stay in sync with AWS SSM
**Tradeoffs**: Requires managing AWS credentials and IAM permissions

### Option C: Deploy to AWS Instead
Consider deploying to AWS App Runner or ECS where IAM roles work natively:
- No need to manage credentials
- More secure
- Better integration with AWS services
