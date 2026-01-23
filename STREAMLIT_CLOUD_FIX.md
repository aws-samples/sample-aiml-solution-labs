# Quick Fix for Streamlit Cloud NoCredentialsError

## Immediate Steps

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
AWS_ACCESS_KEY_ID = "AKIA..."
AWS_SECRET_ACCESS_KEY = "your_secret_key"
AWS_DEFAULT_REGION = "us-west-2"
RUNTIME_ARN = "arn:aws:bedrock-agentcore:..."
```

### 3. Update Your Code
In `chat.py`, change line 9:
```python
# OLD:
from chat_utils import make_urls_clickable, create_safe_markdown_text, get_aws_region, get_ssm_parameter

# NEW:
from chat_utils_cloud import make_urls_clickable, create_safe_markdown_text, get_aws_region, get_ssm_parameter
```

### 4. Commit and Push
```bash
git add .
git commit -m "Fix: Use Streamlit secrets for AWS credentials"
git push
```

Streamlit Cloud will auto-redeploy and the error should be gone!

## Files Created
- `chat_utils_cloud.py` - Cloud-compatible version that reads from Streamlit secrets
- `STREAMLIT_CLOUD_DEPLOYMENT.md` - Full deployment guide

## Security Note
Never commit your actual credentials to Git! Always use Streamlit Secrets or environment variables.
