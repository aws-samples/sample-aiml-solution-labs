import os
import json
from bedrock_agentcore.runtime import BedrockAgentCoreApp
from strands import Agent, tool
from strands.models import BedrockModel
from strands_tools import retrieve, current_time
from strands.tools.mcp import MCPClient
from mcp.client.streamable_http import streamablehttp_client
from bedrock_agentcore.memory.integrations.strands.config import AgentCoreMemoryConfig, RetrievalConfig
from bedrock_agentcore.memory.integrations.strands.session_manager import AgentCoreMemorySessionManager
from utils.agent_memory import REGION, SESSION_ID, ACTOR_ID
from utils.identity_ssm_utils import get_cognito_token_with_scope

MODEL_ID = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
bedrock_model = BedrockModel(model_id=MODEL_ID, temperature=0.3)
app = BedrockAgentCoreApp()

# Get configuration from environment variables
kb_id = os.environ.get("KNOWLEDGE_BASE_ID", "NOT AVAILABLE")
memory_id = os.environ.get("MEMORY_ID")
gateway_url = os.environ.get("GATEWAY_URL")
cognito_client_id = os.environ.get("COGNITO_CLIENT_ID")
cognito_client_secret = os.environ.get("COGNITO_CLIENT_SECRET")
cognito_discovery_url = os.environ.get("COGNITO_DISCOVERY_URL")

if not memory_id:
    raise Exception("Environment variable MEMORY_ID is required")

def create_mcp_client():
    """Create MCP client for gateway access"""
    token = get_cognito_token_with_scope(
        cognito_client_id,
        cognito_client_secret,
        cognito_discovery_url,
        "workshop-api/read workshop-api/write"
    )
    return MCPClient(
        lambda: streamablehttp_client(
            gateway_url,
            headers={"Authorization": f"Bearer {token}"},
        )
    )

system_prompt = f"""You are an Amazon Returns & Refunds assistant with access to:
- Knowledge Base (retrieve tool with knowledgeBaseId="{kb_id}") for policy questions
- Gateway tools for refund management (create, list, approve refund requests)
- Customer conversation history and preferences through memory

Use conversation history to provide personalized assistance. Reference past interactions when relevant.
For refund operations, use user_id 'user456' as default if not specified."""

@app.entrypoint
def invoke(payload, context=None):
    """AgentCore Runtime entrypoint"""
    session_id = context.session_id if context else SESSION_ID
    actor_id = payload.get("actor_id", ACTOR_ID)

    # Configure memory with retrieval settings
    agentcore_memory_config = AgentCoreMemoryConfig(
        memory_id=memory_id,
        session_id=session_id,
        actor_id=actor_id,
        retrieval_config={
            f"returns/customer/{actor_id}/semantic": RetrievalConfig(top_k=3, relevance_score=0.2),
            f"returns/customer/{actor_id}/preferences": RetrievalConfig(top_k=3, relevance_score=0.2),
            f"returns/customer/{actor_id}/{session_id}/summary": RetrievalConfig(top_k=2, relevance_score=0.2)
        }
    )

    session_manager = AgentCoreMemorySessionManager(
        agentcore_memory_config=agentcore_memory_config,
        region_name=REGION
    )

    # Create MCP client and use it within context manager
    mcp_client = create_mcp_client()

    with mcp_client:
        # Get gateway tools from MCP client while it's active
        gateway_tools = list(mcp_client.list_tools_sync())

        # Create agent with all tools
        agent = Agent(
            model=bedrock_model,
            tools=[retrieve, current_time] + gateway_tools,
            system_prompt=system_prompt,
            session_manager=session_manager
        )

        user_input = payload.get("prompt", "")
        response = agent(user_input)
        return response.message["content"][0]["text"]

if __name__ == "__main__":
    app.run()
