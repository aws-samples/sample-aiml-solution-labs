import re
import os

# DEPRECATED: Use chat_utils_cloud.py instead
# This file is kept for backwards compatibility but should not be used

# Get AWS region with fallback - lazy initialization to avoid credential errors
_REGION = None

def get_aws_region() -> str:
    """Get the current AWS region."""
    global _REGION
    if _REGION is None:
        try:
            import boto3
            _REGION = boto3.session.Session().region_name or "us-west-2"
        except Exception:
            _REGION = os.environ.get('AWS_DEFAULT_REGION', 'us-west-2')
    return _REGION


def get_ssm_parameter(name: str, with_decryption: bool = True) -> str:
    """DEPRECATED: Use chat_utils_cloud.get_ssm_parameter instead"""
    try:
        import boto3
        region = get_aws_region()
        ssm = boto3.client("ssm", region_name=region)
        response = ssm.get_parameter(Name=name, WithDecryption=with_decryption)
        return response["Parameter"]["Value"]
    except Exception as e:
        raise RuntimeError(
            f"chat_utils.py is deprecated. Please use chat_utils_cloud.py instead. "
            f"Original error: {e}"
        )


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