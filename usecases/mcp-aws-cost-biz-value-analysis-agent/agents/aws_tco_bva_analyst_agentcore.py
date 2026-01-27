"""
AWS TCO & BVA Analyst MCP Endpoint for AgentCore Deployment.

This script defines an MCP endpoint that wraps the AwsTcoBvaAnalyst
for deployment to Amazon Bedrock AgentCore.
"""

import os
import time
from mcp.server.fastmcp import FastMCP
from bedrock_agentcore.runtime import BedrockAgentCoreApp

from aws_tco_bva_analyst import AwsTcoBvaAnalyst

# =============================================================================
# CONFIGURATION
# =============================================================================
os.environ["BYPASS_TOOL_CONSENT"] = "true"

app = BedrockAgentCoreApp()

# Create FastMCP application
mcp = FastMCP(host="0.0.0.0", stateless_http=True)

# Initialize the AWS TCO & BVA Analyst Agent
tco_bva_analyst = AwsTcoBvaAnalyst()


def invoke_with_retry(agent, query: str, max_retries: int = 3, base_delay: int = 1):
    """
    Invoke agent with exponential backoff retry for throttling errors.
    
    Args:
        agent: The agent to invoke
        query: User query string
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds for exponential backoff
        
    Returns:
        Agent response
    """
    for attempt in range(max_retries + 1):
        try:
            return agent(query)
        except Exception as e:
            error_str = str(e).lower()
            is_retryable = any(err in error_str for err in [
                "serviceunavailableexception",
                "modelthrottledexception", 
                "throttling"
            ])
            
            if is_retryable and attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                print(f"Retry {attempt + 1}/{max_retries} after {delay}s: {type(e).__name__}")
                time.sleep(delay)
                continue
            raise


@mcp.tool()
def read_only_analyze_aws_costs(query: str) -> str:
    """
    Analyze AWS costs and perform TCO calculations.
    
    This tool retrieves AWS pricing from a Knowledge Base and performs
    cost calculations using calculator tools for accurate TCO analysis.
    
    Args:
        query: User query describing the AWS cost analysis needed.
               Examples:
               - "Calculate monthly cost for Bedrock Claude Haiku with 10K requests"
               - "What's the TCO for running an AI agent with 50K questions/month?"
               - "Compare costs between Claude Sonnet and Haiku for my use case"
    
    Returns:
        Detailed cost analysis with pricing breakdown and calculations.
    """
    response = invoke_with_retry(tco_bva_analyst, query)
    
    # Extract text content from response
    if hasattr(response, 'message') and 'content' in response.message:
        return response.message['content'][0]['text']
    return str(response)


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
