# Streamlit Cloud Deployment - Complete Guide

## 🎯 Quick Start

Your app is failing because it can't access AWS credentials on Streamlit Cloud. Here's the fix:

### Step 1: Get Your Agent ARN
```bash
aws ssm get-parameter --name /app/returnsrefunds/agentcore/runtime_arn --query Parameter.Value --output text
```

### Step 2: Configure Secrets in Streamlit Cloud
1. Go to your app: https://sample-aiml-solution-labs-idakupw6yhy24mpjbrx.streamlit.app
2. Click "Manage app" (bottom right)
3. Click "Secrets" in the left sidebar
4. Copy the template from `secrets.toml.template` and paste it
5. Replace all placeholder values with your actual values
6. Click "Save"

### Step 3: Verify and Deploy
The app will automatically redeploy. If you see errors:
1. Check the logs: Manage app → Logs
2. Run the test page: Navigate to `test_secrets.py` in your app
3. Follow the troubleshooting guide below

---

## 📁 Files Overview

### Core Files (Already Updated)
- **`chat_utils_cloud.py`** - Cloud-compatible utilities that read from Streamlit secrets
- **`chat.py`** - Already updated to import from `chat_utils_cloud`

### Helper Files (New)
- **`secrets.toml.template`** - Template for your secrets configuration
- **`test_secrets.py`** - Diagnostic tool to verify secrets are configured correctly
- **`STREAMLIT_CLOUD_DEPLOYMENT.md`** - Detailed deployment guide
- **`README_DEPLOYMENT.md`** - This file

---

## 🔧 How It Works

### The Problem
```python
# Old code (chat_utils.py)
import boto3
REGION = boto3.session.Session().region_name  # ❌ Fails on Streamlit Cloud
```

Streamlit Cloud doesn't have access to your local AWS credentials, so boto3 fails.

### The Solution
```python
# New code (chat_utils_cloud.py)
import streamlit as st

def get_aws_region():
    # Try Streamlit secrets first (for cloud)
    if 'AWS_DEFAULT_REGION' in st.secrets:
        return st.secrets['AWS_DEFAULT_REGION']
    # Fall back to boto3 (for local dev)
    return os.environ.get('AWS_DEFAULT_REGION', 'us-west-2')
```

This approach:
- ✅ Works on Streamlit Cloud (uses secrets)
- ✅ Works locally (uses boto3 or environment variables)
- ✅ No code changes needed between environments

---

## 🔐 Security Best Practices

### DO:
- ✅ Use Streamlit Secrets for sensitive data
- ✅ Use IAM roles when possible (AWS deployments)
- ✅ Rotate credentials regularly
- ✅ Use least-privilege IAM policies
- ✅ Add `.streamlit/secrets.toml` to `.gitignore`

### DON'T:
- ❌ Hardcode credentials in code
- ❌ Commit secrets to Git
- ❌ Share secrets publicly
- ❌ Use root AWS credentials
- ❌ Give overly broad IAM permissions

---

## 🧪 Testing

### Local Testing
Create `.streamlit/secrets.toml` in your project root:
```toml
AWS_ACCESS_KEY_ID = "your_key"
AWS_SECRET_ACCESS_KEY = "your_secret"
AWS_DEFAULT_REGION = "us-west-2"
RUNTIME_ARN = "your_arn"
```

Run locally:
```bash
streamlit run main.py
```

### Cloud Testing
1. Deploy to Streamlit Cloud
2. Navigate to `test_secrets.py` in your app
3. Verify all checks pass ✅
4. If any fail, follow the error messages

---

## 🐛 Troubleshooting

### Error: "NoCredentialsError"
**Cause**: AWS credentials not configured in secrets

**Fix**: Add `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to secrets

### Error: "ClientError" or "AccessDeniedException"
**Cause**: Invalid credentials or insufficient IAM permissions

**Fix**: 
1. Verify credentials work locally:
   ```bash
   aws sts get-caller-identity
   ```
2. Check IAM permissions include:
   - `bedrock-agentcore:InvokeRuntime`
   - `ssm:GetParameter` (if using SSM)

### Error: "KeyError: 'RUNTIME_ARN'"
**Cause**: Secret key name doesn't match

**Fix**: Ensure secret is named exactly `RUNTIME_ARN` (all caps)

### Error: "ParameterNotFound"
**Cause**: SSM parameter doesn't exist or wrong name

**Fix**: 
1. Verify parameter exists:
   ```bash
   aws ssm get-parameter --name /app/returnsrefunds/agentcore/runtime_arn
   ```
2. Or add `RUNTIME_ARN` directly to secrets (skip SSM)

### App Still Not Working?
1. Check Streamlit logs: Manage app → Logs
2. Run `test_secrets.py` to diagnose
3. Verify all secrets are spelled correctly (case-sensitive!)
4. Try redeploying: Make a small change, commit, push

---

## 📊 Required IAM Permissions

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

For production, restrict resources:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "bedrock-agentcore:InvokeRuntime",
      "Resource": "arn:aws:bedrock-agentcore:us-west-2:123456789012:runtime/*"
    },
    {
      "Effect": "Allow",
      "Action": "ssm:GetParameter",
      "Resource": "arn:aws:ssm:us-west-2:123456789012:parameter/app/returnsrefunds/*"
    }
  ]
}
```

---

## 🚀 Alternative Deployment Options

If Streamlit Cloud continues to have issues, consider:

### AWS App Runner
- Native IAM role support (no credential management!)
- Auto-scaling
- Simple deployment from GitHub
- Cost: ~$5-20/month

### AWS ECS Fargate
- Full container control
- IAM roles for tasks
- Integrates with other AWS services
- Cost: ~$10-30/month

### AWS EC2
- Traditional VM approach
- Attach IAM role to instance
- Full control
- Cost: ~$5-50/month depending on instance type

### AWS Lambda + API Gateway
- Serverless option
- Native IAM support
- Pay per request
- Cost: ~$0-5/month for low traffic

All of these eliminate credential management since they use IAM roles.

---

## 📚 Additional Resources

- [Streamlit Secrets Documentation](https://docs.streamlit.io/streamlit-community-cloud/deploy-your-app/secrets-management)
- [AWS IAM Best Practices](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html)
- [Boto3 Credentials Configuration](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html)
- [AWS App Runner Documentation](https://docs.aws.amazon.com/apprunner/)

---

## 💡 Tips

1. **Use the test script**: `test_secrets.py` will save you hours of debugging
2. **Check logs frequently**: Streamlit Cloud logs show the actual error messages
3. **Start simple**: Get credentials working first, then add complexity
4. **Consider AWS deployment**: If you're heavily using AWS services, deploy on AWS for better integration
5. **Rotate credentials**: Set a reminder to rotate AWS credentials every 90 days

---

## ✅ Checklist

Before asking for help, verify:
- [ ] All 4 required secrets are configured in Streamlit Cloud
- [ ] Secret names are exactly: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, `RUNTIME_ARN`
- [ ] AWS credentials work locally: `aws sts get-caller-identity`
- [ ] IAM permissions include `bedrock-agentcore:InvokeRuntime` and `ssm:GetParameter`
- [ ] `chat.py` imports from `chat_utils_cloud` (not `chat_utils`)
- [ ] You've checked Streamlit Cloud logs for specific error messages
- [ ] You've run `test_secrets.py` and all checks pass

If all checks pass and it still doesn't work, there may be a different issue. Check the full error in logs.
