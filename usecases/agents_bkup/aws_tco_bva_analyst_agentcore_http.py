"""
AWS TCO & BVA Analyst HTTP Endpoint for AgentCore Deployment.

This script defines an HTTP endpoint that wraps the AwsTcoBvaAnalyst
for deployment to Amazon Bedrock AgentCore using HTTP protocol.

Uses fast initialization to avoid 30-second timeout.
"""

import os
from bedrock_agentcore.runtime import BedrockAgentCoreApp
from aws_tco_bva_analyst_fast import AwsTcoBvaAnalystFast

# =============================================================================
# CONFIGURATION
# =============================================================================
os.environ["BYPASS_TOOL_CONSENT"] = "true"

# Create BedrockAgentCore application
app = BedrockAgentCoreApp()

# Agent will be initialized on first request (lazy)
tco_bva_analyst = None


def get_analyst():
    """Lazy initialization of the analyst."""
    global tco_bva_analyst
    if tco_bva_analyst is None:
        print("Initializing AWS TCO & BVA Analyst...")
        tco_bva_analyst = AwsTcoBvaAnalystFast()
        print("Agent initialized successfully!")
    return tco_bva_analyst


@app.entrypoint
def handle_request(prompt: str) -> str:
    """
    Handle incoming requests and route to the TCO & BVA Analyst.
    
    Args:
        prompt: User query describing the AWS cost analysis needed.
               Examples:
               - "Calculate monthly cost for Bedrock Claude Haiku with 10K requests"
               - "What's the TCO for running an AI agent with 50K questions/month?"
               - "Compare costs between Claude Sonnet and Haiku for my use case"
    
    Returns:
        Detailed cost analysis with pricing breakdown and calculations.
    """
    try:
        analyst = get_analyst()
        response = analyst.analyze(prompt)
        
        # Extract text content from response
        if hasattr(response, 'message') and 'content' in response.message:
            return response.message['content'][0]['text']
        return str(response)
    except Exception as e:
        return f"Error processing request: {str(e)}"

