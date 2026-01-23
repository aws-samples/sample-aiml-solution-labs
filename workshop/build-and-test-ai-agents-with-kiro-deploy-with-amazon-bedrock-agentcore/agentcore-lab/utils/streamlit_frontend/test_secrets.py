"""
Test script to verify Streamlit secrets are configured correctly.
Run this on Streamlit Cloud to debug credential issues.
"""
import streamlit as st

st.title("🔍 Secrets Configuration Test")

st.write("This page helps you verify that your Streamlit secrets are configured correctly.")

# Check if secrets exist
st.header("1. Secrets Availability")
try:
    secrets_available = hasattr(st, 'secrets') and st.secrets is not None
    if secrets_available:
        st.success("✅ Streamlit secrets are available")
    else:
        st.error("❌ Streamlit secrets are not available")
        st.stop()
except Exception as e:
    st.error(f"❌ Error accessing secrets: {e}")
    st.stop()

# Check required secrets
st.header("2. Required Secrets")
required_secrets = [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY", 
    "AWS_DEFAULT_REGION",
    "RUNTIME_ARN"
]

missing_secrets = []
for secret_name in required_secrets:
    if secret_name in st.secrets:
        # Show partial value for security
        value = str(st.secrets[secret_name])
        if secret_name == "AWS_SECRET_ACCESS_KEY":
            masked_value = value[:4] + "..." + value[-4:] if len(value) > 8 else "***"
        elif secret_name == "AWS_ACCESS_KEY_ID":
            masked_value = value[:8] + "..." if len(value) > 8 else value
        else:
            masked_value = value
        st.success(f"✅ {secret_name} = `{masked_value}`")
    else:
        st.error(f"❌ {secret_name} is missing")
        missing_secrets.append(secret_name)

if missing_secrets:
    st.error(f"Missing secrets: {', '.join(missing_secrets)}")
    st.info("Add these secrets in Streamlit Cloud: Manage app → Secrets")
    st.code("""
# Add these to your secrets:
AWS_ACCESS_KEY_ID = "your_key"
AWS_SECRET_ACCESS_KEY = "your_secret"
AWS_DEFAULT_REGION = "us-west-2"
RUNTIME_ARN = "your_agent_arn"
""", language="toml")
    st.stop()

# Test AWS credentials
st.header("3. AWS Credentials Test")
try:
    import boto3
    
    # Create STS client with credentials from secrets
    sts = boto3.client(
        'sts',
        region_name=st.secrets['AWS_DEFAULT_REGION'],
        aws_access_key_id=st.secrets['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=st.secrets['AWS_SECRET_ACCESS_KEY']
    )
    
    # Try to get caller identity
    identity = sts.get_caller_identity()
    st.success("✅ AWS credentials are valid!")
    st.json({
        "Account": identity['Account'],
        "UserId": identity['UserId'],
        "Arn": identity['Arn']
    })
except Exception as e:
    st.error(f"❌ AWS credentials test failed: {str(e)}")
    st.info("Check that your AWS credentials are valid and not expired")

# Test region
st.header("4. AWS Region")
try:
    region = st.secrets['AWS_DEFAULT_REGION']
    st.success(f"✅ Region configured: {region}")
except Exception as e:
    st.error(f"❌ Region test failed: {e}")

# Test Runtime ARN format
st.header("5. Runtime ARN Format")
try:
    runtime_arn = st.secrets['RUNTIME_ARN']
    if runtime_arn.startswith('arn:aws:bedrock-agentcore:'):
        st.success(f"✅ Runtime ARN format looks correct")
        st.code(runtime_arn)
    else:
        st.warning("⚠️ Runtime ARN format may be incorrect")
        st.write(f"Current value: `{runtime_arn}`")
        st.write("Expected format: `arn:aws:bedrock-agentcore:REGION:ACCOUNT:runtime/ID`")
except Exception as e:
    st.error(f"❌ Runtime ARN test failed: {e}")

# Test chat_utils_cloud import
st.header("6. Module Import Test")
try:
    from chat_utils_cloud import get_aws_region, get_ssm_parameter
    st.success("✅ chat_utils_cloud module imported successfully")
    
    # Test get_aws_region
    region = get_aws_region()
    st.success(f"✅ get_aws_region() returned: {region}")
    
    # Test get_ssm_parameter (should use RUNTIME_ARN from secrets)
    try:
        runtime_arn = get_ssm_parameter("/app/returnsrefunds/agentcore/runtime_arn")
        st.success(f"✅ get_ssm_parameter() returned: {runtime_arn}")
    except Exception as e:
        st.error(f"❌ get_ssm_parameter() failed: {e}")
        
except Exception as e:
    st.error(f"❌ Module import failed: {e}")
    st.info("Make sure chat_utils_cloud.py is in the same directory")

# Summary
st.header("📋 Summary")
if not missing_secrets:
    st.success("🎉 All secrets are configured correctly! Your app should work now.")
    st.info("If you still see errors, check the Streamlit logs: Manage app → Logs")
else:
    st.error("❌ Some secrets are missing. Add them and redeploy.")
