"""
AgentCore entrypoint using bedrock-agentcore SDK with Strands framework.
"""

import os
import sys
import time
import logging
from bedrock_agentcore.runtime import BedrockAgentCoreApp
from aws_tco_bva_analyst import AwsTcoBvaAnalyst

# Configuration
os.environ["BYPASS_TOOL_CONSENT"] = "true"

# Setup logging so tool calls appear in CloudWatch
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger("agent_entrypoint")

# Create BedrockAgentCore application
app = BedrockAgentCoreApp()

# Lazy initialization
analyst = None


def get_analyst():
    """Get or create analyst instance."""
    global analyst
    if analyst is None:
        analyst = AwsTcoBvaAnalyst()
    return analyst


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


@app.entrypoint
def handle_request(prompt: str) -> str:
    """
    Handle incoming requests using Strands agent.
    
    Args:
        prompt: User query about AWS costs or TCO analysis
    
    Returns:
        Analysis response
    """
    try:
        agent = get_analyst()
        
        # Ensure prompt is a string
        if not isinstance(prompt, str):
            prompt = str(prompt)
        
        response = invoke_with_retry(agent, prompt)
        
        # AgentResult.__str__() extracts text content from the final message
        return str(response)
    except Exception as e:
        import traceback
        return f"Error: {str(e)}\n\nTraceback:\n{traceback.format_exc()}"


# Make module runnable
if __name__ == "__main__":
    app.run()
