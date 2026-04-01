import os
from strands.models import BedrockModel, CacheConfig

MODEL_ID = os.environ.get("MODEL_ID", "us.anthropic.claude-sonnet-4-5-20250929-v1:0")


def load_model() -> BedrockModel:
    """Get Bedrock model client using IAM credentials."""
    return BedrockModel(model_id=MODEL_ID, temperature=0.1, cache_config=CacheConfig(strategy="auto"))
