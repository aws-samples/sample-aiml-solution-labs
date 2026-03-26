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
              (may be JSON with 'prompt' and optional 'model_id' fields)
    
    Returns:
        Analysis response
    """
    try:
        import json as _json
        
        # Parse payload — may be JSON with model_id or plain string
        model_id = None
        if isinstance(prompt, str):
            try:
                payload = _json.loads(prompt)
                if isinstance(payload, dict) and "prompt" in payload:
                    model_id = payload.get("model_id")
                    prompt = payload["prompt"]
            except (ValueError, _json.JSONDecodeError):
                pass
        
        if not isinstance(prompt, str):
            prompt = str(prompt)
        
        agent = get_analyst()
        
        # If admin specified a model, swap it for this request
        if model_id:
            agent._model = None  # Reset cached model
            agent._tools = None  # Reset cached tools (they reference model)
            original_model_id = agent.model_id
            agent.model_id = model_id
            try:
                response = invoke_with_retry(agent, prompt)
            finally:
                # Restore default model for next request
                agent.model_id = original_model_id
                agent._model = None
                agent._tools = None
        else:
            response = invoke_with_retry(agent, prompt)
        
        return str(response)
    except Exception as e:
        import traceback
        return f"Error: {str(e)}\n\nTraceback:\n{traceback.format_exc()}"


# Make module runnable
if __name__ == "__main__":
    app.run()
