import re
import os
import streamlit as st

def get_aws_region() -> str:
    """Get the current AWS region from Streamlit secrets or environment."""
    # Try Streamlit secrets first (for Streamlit Cloud)
    try:
        if 'AWS_DEFAULT_REGION' in st.secrets:
            return st.secrets['AWS_DEFAULT_REGION']
    except (AttributeError, FileNotFoundError):
        pass
    
    # Fall back to environment variable
    region = os.environ.get('AWS_DEFAULT_REGION', 'us-west-2')
    return region


def get_ssm_parameter(name: str, with_decryption: bool = True) -> str:
    """Get SSM parameter from Streamlit secrets or boto3."""
    # Try Streamlit secrets first (for Streamlit Cloud)
    try:
        # Convert SSM parameter name to secret key
        # e.g., /app/returnsrefunds/agentcore/runtime_arn -> RUNTIME_ARN
        secret_key = name.split('/')[-1].upper()
        if secret_key in st.secrets:
            return st.secrets[secret_key]
    except (AttributeError, FileNotFoundError):
        pass
    
    # Fall back to boto3 for local development
    try:
        import boto3
        region = get_aws_region()
        
        # Configure boto3 with credentials from secrets if available
        try:
            if 'AWS_ACCESS_KEY_ID' in st.secrets and 'AWS_SECRET_ACCESS_KEY' in st.secrets:
                ssm = boto3.client(
                    "ssm",
                    region_name=region,
                    aws_access_key_id=st.secrets['AWS_ACCESS_KEY_ID'],
                    aws_secret_access_key=st.secrets['AWS_SECRET_ACCESS_KEY']
                )
            else:
                ssm = boto3.client("ssm", region_name=region)
        except (AttributeError, FileNotFoundError):
            ssm = boto3.client("ssm", region_name=region)
            
        response = ssm.get_parameter(Name=name, WithDecryption=with_decryption)
        return response["Parameter"]["Value"]
    except Exception as e:
        secret_key = name.split('/')[-1].upper()
        st.error(f"❌ Failed to get parameter {name}: {str(e)}")
        st.info(f"💡 For Streamlit Cloud deployment, add this parameter to secrets as: **{secret_key}**")
        st.code(f'{secret_key} = "your_value_here"', language="toml")
        raise


def make_urls_clickable(text):
    """Convert URLs in text to clickable HTML links."""
    url_pattern = r"https?://(?:[-\w.])+(?:\:[0-9]+)?(?:/(?:[\w/_.])*(?:\?(?:[\w&=%.])*)?(?:\#(?:[\w.])*)?)?"

    def replace_url(match):
        url = match.group(0)
        return f'<a href="{url}" target="_blank" style="color:#4fc3f7;text-decoration:underline;">{url}</a>'

    return re.sub(url_pattern, replace_url, text)


def create_safe_markdown_text(text, message_placeholder):
    """Create safe markdown text with proper encoding and newline handling"""
    # First encode/decode for safety
    safe_text = text.encode("utf-16", "surrogatepass").decode("utf-16")
    
    # Convert newlines to HTML breaks for proper rendering
    # This handles both actual newlines and any remaining escaped ones
    safe_text = safe_text.replace('\n', '<br>')
    safe_text = safe_text.replace('\\n', '<br>')
    
    message_placeholder.markdown(safe_text, unsafe_allow_html=True)
