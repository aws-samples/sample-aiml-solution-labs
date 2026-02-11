"""
AWS TCO & BVA Analyst HTTP Endpoint - Minimal Version for AgentCore.

This version uses minimal dependencies for fast initialization.
"""

import os
from bedrock_agentcore.runtime import BedrockAgentCoreApp
from aws_tco_bva_analyst_minimal import AwsTcoBvaAnalystMinimal

# Configuration
os.environ["BYPASS_TOOL_CONSENT"] = "true"

# Create BedrockAgentCore application
app = BedrockAgentCoreApp()

# Lazy initialization
analyst = None


def get_analyst():
    """Get or create analyst instance."""
    global analyst
    if analyst is None:
        analyst = AwsTcoBvaAnalystMinimal()
    return analyst


@app.entrypoint
def handle_request(prompt: str) -> str:
    """
    Handle incoming requests.
    
    Args:
        prompt: User query about AWS costs or TCO analysis
    
    Returns:
        Analysis response
    """
    try:
        return get_analyst().analyze(prompt)
    except Exception as e:
        return f"Error: {str(e)}"
