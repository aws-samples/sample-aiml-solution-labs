# 🚀 Deployment Checklist

## ✅ Changes Made to Fix the Error

### Issue Identified
The error was caused by **two files** importing from the old `chat_utils.py`:
1. ❌ `chat.py` - Already fixed (was importing from `chat_utils_cloud`)
2. ❌ `main.py` - **NOW FIXED** (was importing from `chat_utils`)

### Files Updated
1. ✅ `main.py` - Changed import from `chat_utils` to `chat_utils_cloud`
2. ✅ `chat_utils.py` - Made safe (lazy initialization, no immediate boto3 call)

---

## 📋 Pre-Deployment Checklist

### 1. Verify All Imports ✅
```bash
# Check no files import from old chat_utils
cd workshop/build-and-test-ai-agents-with-kiro-deploy-with-amazon-bedrock-agentcore/agentcore-lab/utils/streamlit_frontend
grep -r "from chat_utils import" *.py
# Should return: No matches ✅
```

### 2. Verify Secrets Are Configured
In Streamlit Cloud Dashboard:
- [ ] `AWS_ACCESS_KEY_ID` is set
- [ ] `AWS_SECRET_ACCESS_KEY` is set
- [ ] `AWS_DEFAULT_REGION` is set (e.g., "us-west-2")
- [ ] `RUNTIME_ARN` is set (your agent ARN)

### 3. Commit and Push Changes
```bash
# Check what will be committed
git status

# Add the fixed files
git add workshop/build-and-test-ai-agents-with-kiro-deploy-with-amazon-bedrock-agentcore/agentcore-lab/utils/streamlit_frontend/main.py
git add workshop/build-and-test-ai-agents-with-kiro-deploy-with-amazon-bedrock-agentcore/agentcore-lab/utils/streamlit_frontend/chat_utils.py

# Commit
git commit -m "Fix: Update main.py to use chat_utils_cloud"

# Push
git push
```

### 4. Wait for Auto-Deploy
- Streamlit Cloud will automatically detect the push
- Wait 1-2 minutes for rebuild
- Check the app URL

---

## 🔍 Verification Steps

### After Deployment

1. **Check App Loads**
   - Visit your Streamlit app URL
   - Should see login page (not error)

2. **Check Logs** (if still errors)
   - Click "Manage app" (bottom right)
   - Click "Logs"
   - Look for specific error messages

3. **Test Secrets** (optional)
   - Navigate to `test_secrets.py` in your app
   - Verify all checks pass ✅

---

## 🐛 Troubleshooting

### Error: Still seeing "chat_utils.py line 16"
**Cause**: Streamlit Cloud is using cached version

**Fix**:
1. Go to Streamlit Cloud dashboard
2. Click "Manage app"
3. Click "Reboot app" (forces fresh start)
4. Wait for reboot to complete

### Error: "KeyError: 'AWS_ACCESS_KEY_ID'"
**Cause**: Secrets not configured in Streamlit Cloud

**Fix**:
1. Go to Streamlit Cloud dashboard
2. Click "Manage app" → "Secrets"
3. Add all 4 required secrets
4. Click "Save"

### Error: "AccessDeniedException"
**Cause**: AWS credentials don't have required permissions

**Fix**:
1. Check IAM permissions include:
   - `bedrock-agentcore:InvokeRuntime`
   - `ssm:GetParameter`
2. Test credentials locally:
   ```bash
   aws sts get-caller-identity
   ```

### Error: "ParameterNotFound"
**Cause**: SSM parameter doesn't exist OR you're using RUNTIME_ARN from secrets

**Fix**:
- If using secrets (recommended): This is expected, ignore this error
- If using SSM: Verify parameter exists:
  ```bash
  aws ssm get-parameter --name /app/returnsrefunds/agentcore/runtime_arn
  ```

---

## 📊 What Changed

### Before (Broken)
```python
# main.py
from chat_utils import make_urls_clickable  # ❌ Old import

# chat_utils.py (executed at import time)
import boto3
REGION = boto3.session.Session().region_name  # ❌ Fails on Streamlit Cloud
```

### After (Fixed)
```python
# main.py
from chat_utils_cloud import make_urls_clickable  # ✅ New import

# chat_utils_cloud.py (lazy initialization)
def get_aws_region():
    if 'AWS_DEFAULT_REGION' in st.secrets:  # ✅ Checks secrets first
        return st.secrets['AWS_DEFAULT_REGION']
    return os.environ.get('AWS_DEFAULT_REGION', 'us-west-2')
```

---

## 🎯 Expected Behavior

### Successful Deployment
```
1. User visits app URL
   ↓
2. App loads (no errors)
   ↓
3. Cognito login page appears
   ↓
4. User logs in
   ↓
5. Chat interface loads
   ↓
6. User can send messages
   ↓
7. Agent responds successfully
```

### What Happens Behind the Scenes
```
1. Streamlit Cloud pulls code from Git
   ↓
2. Installs dependencies
   ↓
3. Runs main.py
   ↓
4. Imports chat_utils_cloud (not chat_utils)
   ↓
5. chat_utils_cloud checks st.secrets
   ↓
6. Finds AWS_DEFAULT_REGION in secrets ✅
   ↓
7. Finds RUNTIME_ARN in secrets ✅
   ↓
8. App initializes successfully
   ↓
9. Ready to handle user requests
```

---

## 📝 Summary of Changes

| File | Change | Why |
|------|--------|-----|
| `main.py` | Import from `chat_utils_cloud` | Use cloud-compatible version |
| `chat_utils.py` | Lazy initialization | Prevent immediate boto3 call |
| `chat.py` | Already correct | Was using `chat_utils_cloud` |

---

## ✅ Final Checklist

Before marking as complete:
- [ ] All files import from `chat_utils_cloud` (not `chat_utils`)
- [ ] Secrets configured in Streamlit Cloud
- [ ] Changes committed and pushed to Git
- [ ] Streamlit Cloud auto-deployed
- [ ] App loads without errors
- [ ] Can log in successfully
- [ ] Can send messages and get responses

---

## 🆘 Still Having Issues?

If you've followed all steps and still see errors:

1. **Check the exact error message** in Streamlit logs
2. **Run test_secrets.py** to diagnose secrets issues
3. **Try rebooting the app** in Streamlit Cloud
4. **Verify Git push succeeded** - check GitHub/GitLab
5. **Check Streamlit Cloud build logs** for deployment errors

Common issues:
- Secrets not saved properly (check for typos)
- Git push didn't complete (check remote)
- Streamlit Cloud didn't detect push (manual reboot)
- Cached Python modules (reboot app)

---

**Next Step**: Commit and push the changes, then wait for Streamlit Cloud to redeploy! 🚀
