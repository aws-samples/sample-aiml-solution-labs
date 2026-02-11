"""
AWS TCO & Business Value Analyst Agent using Strands Agents framework.

This agent orchestrates TCO analysis by combining pricing search capabilities
with calculator tools. It uses the pricing_search_agent as a tool to retrieve 
AWS pricing data and calculator tools for Bedrock/AgentCore/BVA cost computations.
Also integrates with AWS Knowledge MCP server for AWS documentation search.
"""

import os
from strands import Agent
from strands.models import BedrockModel
from strands.tools.mcp import MCPClient
from mcp.client.streamable_http import streamablehttp_client
from pricing_search_assistant import call_pricing_search_agent
from calculator_bva import bva_calculator, bva_what_if_analysis
from calculator_agentcore import use_agentcore_calculator, agentcore_what_if_analysis
from calculator_bedrock import use_bedrock_calculator, bedrock_what_if_analysis
from system_prompt import TCO_ANALYST_PROMPT

# =============================================================================
# CONFIGURATION
# =============================================================================
REGION = os.environ.get("AWS_REGION", "us-east-1")
MODEL_ID = os.environ.get("MODEL_ID", "global.anthropic.claude-haiku-4-5-20251001-v1:0")

# AWS Knowledge MCP Server
AWS_KNOWLEDGE_MCP_URL = "https://knowledge-mcp.global.api.aws"


class AwsTcoBvaAnalyst:
    """
    AWS TCO & Business Value Analyst Agent.
    
    Combines pricing search from Bedrock Knowledge Base with
    calculator tools for comprehensive cost and business value analysis.
    Also uses AWS Knowledge MCP server for AWS documentation search.
    """
    
    def __init__(self, region: str = None, model_id: str = None):
        """
        Initialize the AWS TCO & BVA Analyst Agent.
        
        Args:
            region: AWS region for services
            model_id: Bedrock model ID to use
        """
        self.region = region or REGION
        self.model_id = model_id or MODEL_ID
        self._mcp_client = None
        self._agent = None
    
    def _create_mcp_client(self) -> MCPClient:
        """Create MCP client for AWS Knowledge server."""
        return MCPClient(
            lambda: streamablehttp_client(AWS_KNOWLEDGE_MCP_URL)
        )
    
    def _create_agent(self, mcp_tools: list = None) -> Agent:
        """Create and configure the Strands agent."""
        model = BedrockModel(
            model_id=self.model_id,
            temperature=0.1,
        )
        
        tools = [
            # Pricing search
            call_pricing_search_agent,
            # BVA calculators
            bva_calculator,
            bva_what_if_analysis,
            # AgentCore calculators
            use_agentcore_calculator,
            agentcore_what_if_analysis,
            # Bedrock calculators
            use_bedrock_calculator,
            bedrock_what_if_analysis,
        ]
        
        # Add MCP tools if available
        if mcp_tools:
            tools.extend(mcp_tools)
        
        return Agent(
            model=model,
            system_prompt=TCO_ANALYST_PROMPT,
            tools=tools,
        )
    
    def analyze(self, query: str) -> str:
        """
        Perform TCO analysis based on user query.
        
        Args:
            query: User query about AWS costs or TCO analysis
            
        Returns:
            Analysis response from the agent
        """
        # Use MCP client context manager for proper lifecycle management
        self._mcp_client = self._create_mcp_client()
        
        with self._mcp_client:
            mcp_tools = self._mcp_client.list_tools_sync()
            self._agent = self._create_agent(mcp_tools)
            return self._agent(query)
    
    def __call__(self, query: str) -> str:
        """Allow calling the agent directly."""
        return self.analyze(query)
    
    def run_interactive(self):
        """Run the agent in interactive chat mode."""
        print("AWS TCO & Business Value Analyst Agent")
        print("=" * 60)
        print(f"Region: {self.region}")
        print(f"Model: {self.model_id}")
        print(f"AWS Knowledge MCP: {AWS_KNOWLEDGE_MCP_URL}")
        print("\nCapabilities:")
        print("  - AWS pricing search via Knowledge Base")
        print("  - AWS documentation search via MCP")
        print("  - BVA calculator (ROI, cost savings, revenue impact)")
        print("  - Bedrock cost calculator with what-if analysis")
        print("  - AgentCore cost calculator with what-if analysis")
        print("\nType 'quit' to exit\n")
        
        # Create MCP client for interactive session
        self._mcp_client = self._create_mcp_client()
        
        with self._mcp_client:
            mcp_tools = self._mcp_client.list_tools_sync()
            print(f"Loaded {len(mcp_tools)} tools from AWS Knowledge MCP server\n")
            self._agent = self._create_agent(mcp_tools)
            
            while True:
                user_input = input("You: ").strip()
                if user_input.lower() in ["quit", "exit", "q"]:
                    print("Goodbye!")
                    break
                if not user_input:
                    continue
                
                print("\nAnalyzing...\n")
                response = self._agent(user_input)
                print(f"Agent: {response}\n")


if __name__ == "__main__":
    agent = AwsTcoBvaAnalyst()
    agent.run_interactive()
