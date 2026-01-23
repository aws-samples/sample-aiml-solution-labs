# 🔐 Security Guide: Protecting Your Secrets

## ✅ Your Secrets Are Safe!

Good news: **No secrets have been committed to your Git repository.** Here's why:

### What We Checked
1. ✅ No `.streamlit/secrets.toml` file exists in your repo
2. ✅ No actual secrets in any committed files
3. ✅ Only template files (`secrets.toml.template`) are committed
4. ✅ Template files contain placeholder values, not real credentials

### Files That ARE Safe to Commit
- ✅ `secrets.toml.template` - Contains only placeholders
- ✅ `chat_utils_cloud.py` - Contains no secrets, only code to read them
- ✅ `test_secrets.py` - Diagnostic tool, no secrets
- ✅ All documentation files

### Files That Should NEVER Be Committed
- ❌ `.streamlit/secrets.toml` - Your actual secrets
- ❌ `secrets.toml` - Any file with real credentials
- ❌ `.env` files with real values
- ❌ `cognito_config.json` with real values
- ❌ Any file with AWS credentials

---

## 🛡️ How Your Secrets Are Protected

### 1. .gitignore Files Created
I've created two `.gitignore` files to protect your secrets:

**Root `.gitignore`** (repository-wide protection):
```
.streamlit/
**/secrets.toml
.env
.aws/
credentials
*.pem
*.key
```

**Streamlit Frontend `.gitignore`** (directory-specific):
```
.streamlit/
secrets.toml
*.secrets.toml
.env
.aws/
```

### 2. How .gitignore Works
```
You create: .streamlit/secrets.toml
              ↓
Git checks: .gitignore
              ↓
Finds match: .streamlit/
              ↓
Result: File is IGNORED by Git
              ↓
git add . → Skips this file
git commit → Never includes it
git push → Never uploads it
```

### 3. Verify Protection
Run these commands to verify secrets are ignored:

```bash
# Check what Git sees
git status

# Check if secrets.toml would be tracked
git check-ignore .streamlit/secrets.toml
# Should output: .streamlit/secrets.toml (means it's ignored)

# List all ignored files
git status --ignored
```

---

## 📋 Where Secrets Live

### Local Development
```
Your Computer
├── .streamlit/
│   └── secrets.toml          ← Local only, never committed
├── .gitignore                ← Tells Git to ignore secrets
└── secrets.toml.template     ← Safe template, can commit
```

### Streamlit Cloud
```
Streamlit Cloud Dashboard
└── Manage app
    └── Secrets
        └── [Your secrets here]  ← Encrypted, never in Git
```

### Git Repository (GitHub/GitLab)
```
Git Repository
├── .gitignore                ← Committed ✅
├── secrets.toml.template     ← Committed ✅ (placeholders only)
├── chat_utils_cloud.py       ← Committed ✅ (code only)
└── .streamlit/secrets.toml   ← NEVER HERE ❌
```

---

## 🚨 What If Secrets Were Accidentally Committed?

If you accidentally committed secrets in the past, here's how to fix it:

### Step 1: Check Git History
```bash
# Search for potential secrets in history
git log --all --full-history -- "*secrets.toml"
git log --all --full-history -- "*.env"

# Search for AWS keys in commits
git log -S "AWS_ACCESS_KEY_ID" --all
```

### Step 2: Remove from History (if found)
```bash
# WARNING: This rewrites history!
# Coordinate with your team first

# Remove a specific file from all history
git filter-branch --force --index-filter \
  "git rm --cached --ignore-unmatch .streamlit/secrets.toml" \
  --prune-empty --tag-name-filter cat -- --all

# Or use BFG Repo-Cleaner (easier)
# Download from: https://rtyley.github.io/bfg-repo-cleaner/
java -jar bfg.jar --delete-files secrets.toml
git reflog expire --expire=now --all
git gc --prune=now --aggressive
```

### Step 3: Force Push (if you removed secrets)
```bash
# WARNING: This overwrites remote history
git push origin --force --all
git push origin --force --tags
```

### Step 4: Rotate Compromised Credentials
If secrets were committed, assume they're compromised:

1. **AWS Credentials**:
   - Go to AWS IAM Console
   - Delete the old access key
   - Create a new access key
   - Update Streamlit secrets

2. **Other Secrets**:
   - Change passwords
   - Regenerate API keys
   - Update all references

---

## ✅ Best Practices Checklist

### Before Committing
- [ ] Run `git status` to see what will be committed
- [ ] Verify `.gitignore` is in place
- [ ] Check no secrets in staged files: `git diff --cached`
- [ ] Use `git add` selectively, not `git add .` blindly

### When Creating Secrets
- [ ] Always use `.streamlit/secrets.toml` (ignored by Git)
- [ ] Never use `secrets.toml` in root directory
- [ ] Use template files for documentation
- [ ] Add new secret patterns to `.gitignore`

### Regular Audits
- [ ] Review `.gitignore` monthly
- [ ] Check Git history for leaks: `git log -S "AWS_ACCESS_KEY"`
- [ ] Rotate credentials every 90 days
- [ ] Use AWS IAM Access Analyzer

### Team Collaboration
- [ ] Share `.gitignore` with team
- [ ] Document where secrets should go
- [ ] Use pre-commit hooks to prevent leaks
- [ ] Review pull requests for secrets

---

## 🔍 How to Verify Your Secrets Are Safe

### Test 1: Check Current Status
```bash
cd workshop/build-and-test-ai-agents-with-kiro-deploy-with-amazon-bedrock-agentcore/agentcore-lab/utils/streamlit_frontend

# Create a test secrets file
mkdir -p .streamlit
echo "test" > .streamlit/secrets.toml

# Check if Git ignores it
git status
# Should NOT show .streamlit/secrets.toml

# Clean up
rm -rf .streamlit
```

### Test 2: Check Git History
```bash
# Search for any secrets.toml in history
git log --all --full-history -- "*secrets.toml"
# Should only show secrets.toml.template

# Search for AWS keys
git log -S "AKIA" --all
# Should show no results (or only template files)
```

### Test 3: Check What Would Be Committed
```bash
# Stage all files
git add .

# See what would be committed
git status
# Should NOT include any secrets files

# Unstage
git reset
```

---

## 🎓 Understanding Git Ignore Patterns

### Pattern Examples
```gitignore
# Exact filename
secrets.toml

# Any directory named .streamlit
.streamlit/

# Any secrets.toml in any subdirectory
**/secrets.toml

# Any file ending in .env
*.env

# Any file starting with secret_
secret_*

# Specific path
workshop/streamlit_frontend/.streamlit/
```

### What Gets Ignored
```
✅ .streamlit/secrets.toml
✅ .streamlit/config.toml
✅ app/.streamlit/secrets.toml
✅ any/path/.streamlit/secrets.toml
✅ secrets.toml
✅ my_secrets.toml
✅ .env
✅ .env.local
```

### What Doesn't Get Ignored
```
❌ secrets.toml.template (template is safe)
❌ README_SECRETS.md (documentation is safe)
❌ chat_utils_cloud.py (code is safe)
```

---

## 🚀 Streamlit Cloud Secrets

### How Streamlit Secrets Work
1. **Encrypted Storage**: Secrets are encrypted at rest
2. **Environment Isolation**: Each app has isolated secrets
3. **No Git Sync**: Secrets never sync to Git
4. **Access Control**: Only app owner can view/edit

### Adding Secrets to Streamlit Cloud
```
1. Go to: https://share.streamlit.io/
2. Click your app
3. Click "Manage app" (bottom right)
4. Click "Secrets" in sidebar
5. Paste your secrets in TOML format
6. Click "Save"
7. App auto-redeploys with new secrets
```

### Secrets Are Separate from Code
```
Git Repository          Streamlit Cloud
├── code files     →    ├── code files (from Git)
├── .gitignore     →    ├── .gitignore (from Git)
└── (no secrets)        └── secrets (from dashboard)
                             ↑
                        NOT from Git!
                        Configured separately
```

---

## 📞 Emergency Response

### If You Accidentally Committed Secrets

**IMMEDIATE ACTIONS:**
1. ⚠️ **Assume compromised** - Don't wait to confirm
2. 🔄 **Rotate immediately** - Generate new credentials
3. 🗑️ **Delete from Git** - Remove from history
4. 🔍 **Check for abuse** - Review AWS CloudTrail logs
5. 📢 **Notify team** - Alert security team if applicable

**AWS Credential Rotation:**
```bash
# 1. Create new credentials
aws iam create-access-key --user-name your-username

# 2. Update Streamlit secrets with new credentials

# 3. Test new credentials work
aws sts get-caller-identity

# 4. Delete old credentials
aws iam delete-access-key --access-key-id OLD_KEY_ID --user-name your-username
```

**Check for Unauthorized Access:**
```bash
# Check recent AWS API calls
aws cloudtrail lookup-events --max-results 50

# Check for unusual activity
aws cloudtrail lookup-events --lookup-attributes AttributeKey=Username,AttributeValue=your-user
```

---

## 🎯 Summary

### ✅ You're Protected Because:
1. `.gitignore` files are in place
2. No actual secrets in your Git history
3. Only template files are committed
4. Streamlit Cloud secrets are separate from Git

### 🔒 Keep It Safe By:
1. Never committing `.streamlit/secrets.toml`
2. Using templates for documentation
3. Rotating credentials regularly
4. Reviewing Git status before commits
5. Using `git check-ignore` to verify

### 📚 Resources
- [GitHub: Removing Sensitive Data](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/removing-sensitive-data-from-a-repository)
- [Streamlit Secrets Management](https://docs.streamlit.io/streamlit-community-cloud/deploy-your-app/secrets-management)
- [AWS Security Best Practices](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html)
- [Git Ignore Documentation](https://git-scm.com/docs/gitignore)

---

**Remember**: The best security is prevention. Always check before you commit! 🛡️
