import logging
from mcp.client.streamable_http import streamablehttp_client
from strands.tools.mcp.mcp_client import MCPClient

logger = logging.getLogger(__name__)

AWS_KNOWLEDGE_MCP_URL = "https://knowledge-mcp.global.api.aws"


def get_streamable_http_mcp_client() -> MCPClient:
    """Returns an MCP Client for the AWS Knowledge MCP server."""
    return MCPClient(lambda: streamablehttp_client(AWS_KNOWLEDGE_MCP_URL))
