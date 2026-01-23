# Architecture: Local vs Cloud Deployment

## 🏠 Local Development Flow

```
┌─────────────────────────────────────────────────────────────┐
│                     Local Development                        │
└─────────────────────────────────────────────────────────────┘

User runs: streamlit run main.py
                    ↓
            main.py loads
                    ↓
        main.py imports chat_utils_cloud
                    ↓
    chat_utils_cloud.get_aws_region()
                    ↓
    ┌──────────────────────────────────┐
    │ Try st.secrets first             │
    │   ↓ (not found)                  │
    │ Fall back to boto3               │
    │   ↓                              │
    │ boto3.session.Session()          │
    │   ↓                              │
    │ Uses ~/.aws/credentials          │ ← Your local AWS config
    └──────────────────────────────────┘
                    ↓
            ✅ Works locally!
```

## ☁️ Streamlit Cloud Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    Streamlit Cloud                           │
└─────────────────────────────────────────────────────────────┘

User visits: https://your-app.streamlit.app
                    ↓
            main.py loads
                    ↓
        main.py imports chat_utils_cloud
                    ↓
    chat_utils_cloud.get_aws_region()
                    ↓
    ┌──────────────────────────────────┐
    │ Try st.secrets first             │
    │   ↓                              │
    │ st.secrets['AWS_DEFAULT_REGION'] │ ← From Streamlit Secrets
    │   ↓                              │
    │ ✅ Returns "us-west-2"           │
    └──────────────────────────────────┘
                    ↓
    chat_utils_cloud.get_ssm_parameter()
                    ↓
    ┌──────────────────────────────────┐
    │ Try st.secrets first             │
    │   ↓                              │
    │ st.secrets['RUNTIME_ARN']        │ ← From Streamlit Secrets
    │   ↓                              │
    │ ✅ Returns agent ARN             │
    └──────────────────────────────────┘
                    ↓
            ✅ Works on cloud!
```

## ❌ Old Flow (Why It Failed)

```
┌─────────────────────────────────────────────────────────────┐
│              Streamlit Cloud (OLD CODE)                      │
└─────────────────────────────────────────────────────────────┘

User visits: https://your-app.streamlit.app
                    ↓
            main.py loads
                    ↓
        main.py imports chat_utils  ← OLD import
                    ↓
    chat_utils.py runs at import time:
    REGION = boto3.session.Session().region_name
                    ↓
    ┌──────────────────────────────────┐
    │ boto3 looks for credentials:     │
    │   1. Environment variables ❌    │
    │   2. ~/.aws/credentials ❌       │
    │   3. IAM role ❌                 │
    │   4. Nothing found!              │
    └──────────────────────────────────┘
                    ↓
        ❌ NoCredentialsError!
```

## 🔄 Credential Resolution Order

### chat_utils_cloud.py (NEW)
```
1. Check st.secrets (Streamlit Cloud) ✅
   ↓ (if not found)
2. Check environment variables ✅
   ↓ (if not found)
3. Check boto3 (local AWS config) ✅
   ↓ (if not found)
4. Use default value ✅
```

### chat_utils.py (OLD)
```
1. boto3.session.Session() immediately ❌
   ↓ (fails on Streamlit Cloud)
2. NoCredentialsError ❌
```

## 🔐 Secrets Flow

```
┌─────────────────────────────────────────────────────────────┐
│                  Streamlit Cloud Secrets                     │
└─────────────────────────────────────────────────────────────┘

You configure in UI:
┌──────────────────────────────────┐
│ AWS_ACCESS_KEY_ID = "AKIA..."   │
│ AWS_SECRET_ACCESS_KEY = "..."   │
│ AWS_DEFAULT_REGION = "us-west-2"│
│ RUNTIME_ARN = "arn:aws:..."     │
└──────────────────────────────────┘
                ↓
        Encrypted storage
                ↓
        Available as st.secrets
                ↓
┌──────────────────────────────────┐
│ Your app code:                   │
│ region = st.secrets['AWS_...']  │
│ arn = st.secrets['RUNTIME_ARN'] │
└──────────────────────────────────┘
                ↓
        Used to call AWS APIs
                ↓
┌──────────────────────────────────┐
│ boto3.client(                    │
│   'bedrock-agentcore',           │
│   aws_access_key_id=...,         │
│   aws_secret_access_key=...      │
│ )                                │
└──────────────────────────────────┘
                ↓
        ✅ Successful API call!
```

## 🏗️ Component Interaction

```
┌─────────────────────────────────────────────────────────────┐
│                         main.py                              │
│  - Handles Cognito authentication                            │
│  - Manages chat UI                                           │
│  - Calls ChatManager                                         │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                         chat.py                              │
│  - ChatManager class                                         │
│  - invoke_endpoint_streaming()                               │
│  - Manages session state                                     │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    chat_utils_cloud.py                       │
│  - get_aws_region() → Returns region from secrets           │
│  - get_ssm_parameter() → Returns ARN from secrets           │
│  - make_urls_clickable() → Formats response                 │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    Streamlit Secrets                         │
│  - AWS_ACCESS_KEY_ID                                         │
│  - AWS_SECRET_ACCESS_KEY                                     │
│  - AWS_DEFAULT_REGION                                        │
│  - RUNTIME_ARN                                               │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    AWS Bedrock AgentCore                     │
│  - Receives authenticated requests                           │
│  - Processes agent invocations                               │
│  - Returns streaming responses                               │
└─────────────────────────────────────────────────────────────┘
```

## 🔍 Debugging Flow

```
Problem: App shows error on Streamlit Cloud
                    ↓
Step 1: Check which error
    ├─ NoCredentialsError → Secrets not configured
    ├─ ClientError → Invalid credentials or permissions
    ├─ KeyError → Secret name mismatch
    └─ ParameterNotFound → SSM parameter issue
                    ↓
Step 2: Run test_secrets.py
    ├─ Shows which secrets are missing
    ├─ Tests AWS credential validity
    └─ Verifies module imports work
                    ↓
Step 3: Check Streamlit logs
    ├─ Manage app → Logs
    └─ Look for specific error messages
                    ↓
Step 4: Verify secrets configuration
    ├─ Check spelling (case-sensitive!)
    ├─ Check values are correct
    └─ Check no extra quotes or spaces
                    ↓
Step 5: Test locally
    ├─ Create .streamlit/secrets.toml
    ├─ Run: streamlit run main.py
    └─ Verify it works locally
                    ↓
Step 6: Redeploy
    ├─ Make small change
    ├─ Commit and push
    └─ Wait for auto-deploy
                    ↓
            ✅ Should work now!
```

## 📊 Data Flow

```
User Input
    ↓
┌─────────────────────────────────┐
│ Streamlit UI (main.py)          │
│ - User types message            │
│ - Clicks send                   │
└─────────────────────────────────┘
    ↓
┌─────────────────────────────────┐
│ ChatManager (chat.py)           │
│ - Builds context                │
│ - Prepares payload              │
└─────────────────────────────────┘
    ↓
┌─────────────────────────────────┐
│ Get Configuration               │
│ - Region from secrets           │
│ - Agent ARN from secrets        │
│ - Bearer token from Cognito     │
└─────────────────────────────────┘
    ↓
┌─────────────────────────────────┐
│ HTTP Request                    │
│ POST to AgentCore endpoint      │
│ - Headers: Authorization        │
│ - Body: User prompt             │
└─────────────────────────────────┘
    ↓
┌─────────────────────────────────┐
│ AWS Bedrock AgentCore           │
│ - Authenticates request         │
│ - Invokes agent                 │
│ - Streams response              │
└─────────────────────────────────┘
    ↓
┌─────────────────────────────────┐
│ Response Processing             │
│ - Receives streaming chunks     │
│ - Formats markdown              │
│ - Makes URLs clickable          │
└─────────────────────────────────┘
    ↓
┌─────────────────────────────────┐
│ Display to User                 │
│ - Shows in chat UI              │
│ - Adds to message history       │
└─────────────────────────────────┘
```

## 🎯 Key Takeaways

1. **Streamlit Cloud ≠ Local Environment**
   - No access to ~/.aws/credentials
   - Must use Streamlit Secrets

2. **chat_utils_cloud.py is the Bridge**
   - Works in both environments
   - Tries secrets first, falls back to boto3

3. **Secrets Must Be Exact**
   - Case-sensitive names
   - No extra quotes or spaces
   - Must match what code expects

4. **Test Before Deploy**
   - Use test_secrets.py
   - Verify locally first
   - Check logs on cloud

5. **Security First**
   - Never commit secrets
   - Use least-privilege IAM
   - Rotate credentials regularly
