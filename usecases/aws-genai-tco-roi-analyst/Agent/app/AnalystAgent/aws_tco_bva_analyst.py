"""
AWS TCO & Business Value Analyst Agent using Strands Agents framework.

This agent orchestrates TCO analysis by combining pricing search capabilities
with calculator tools. It uses the pricing_search_agent as a tool to retrieve 
AWS pricing data and calculator tools for Bedrock/AgentCore/BVA cost computations.
Also integrates with AWS Knowledge MCP server for AWS documentation search.
"""

import os
import logging
from strands import Agent
from strands.models import BedrockModel
from strands.tools.mcp import MCPClient
from mcp.client.streamable_http import streamablehttp_client
from search_pricing_info import call_pricing_search_agent
from calculator_bva import bva_calculator, bva_what_if_analysis
from calculator_agentcore import use_agentcore_calculator, agentcore_what_if_analysis
from calculator_bedrock import use_bedrock_calculator, bedrock_what_if_analysis
from calculator_capacity_planning import capacity_planning_calculator
from search_bedrock_quota import call_bedrock_quota_agent
from system_prompt import TCO_ANALYST_PROMPT
from tool_throttle_hook import ToolThrottleHook

logger = logging.getLogger("aws_tco_bva_analyst")

# =============================================================================
# CONFIGURATION
# =============================================================================
REGION = os.environ.get("AWS_REGION", "us-east-1")
# Default model for the agent. Override via MODEL_ID env var to switch models
# without code changes (e.g., MODEL_ID=us.anthropic.claude-haiku-4-5-20251001-v1:0)
MODEL_ID = os.environ.get("MODEL_ID", "us.anthropic.claude-sonnet-4-5-20250929-v1:0")

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
        self._mcp_tools = None
        self._agent = None
        self._model = None
        self._tools = None
        
        # Generic tool throttle — prevents runaway tool call loops
        self._tool_throttle = ToolThrottleHook(
            max_calls_per_tool=10,
            total_max_calls=50,
        )
    
    def _create_mcp_client(self) -> MCPClient:
        """Create MCP client for AWS Knowledge server."""
        return MCPClient(
            lambda: streamablehttp_client(AWS_KNOWLEDGE_MCP_URL)
        )
    
    def _create_agent(self, mcp_tools: list = None, messages: list = None) -> Agent:
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
            # Capacity planning
            capacity_planning_calculator,
            # Bedrock quota lookup
            call_bedrock_quota_agent,
        ]
        
        # Add MCP tools if available
        if mcp_tools:
            tools.extend(mcp_tools)
        
        kwargs = dict(
            model=model,
            system_prompt=TCO_ANALYST_PROMPT,
            tools=tools,
        )
        if messages:
            kwargs['messages'] = messages
        
        return Agent(**kwargs)
    
    @staticmethod
    def _compact_messages(messages: list, keep_last_tool_pairs: int = 3) -> list:
        """Compact conversation history to reduce token overhead.
        
        Keeps user messages, assistant text responses, and the most recent
        N tool call/result pairs. Older tool call/result blocks are stripped
        to save tokens while preserving recent tool context that the model
        may need for follow-up turns.
        
        Args:
            messages: Raw conversation history from the agent.
            keep_last_tool_pairs: Number of recent tool call/result pairs to
                preserve. Set to 0 to strip all tool messages (original behavior).
        """
        if keep_last_tool_pairs <= 0:
            # Original behavior: strip all tool messages
            compacted = []
            for msg in messages:
                role = msg.get("role")
                content = msg.get("content", [])
                text_blocks = [b for b in content if "text" in b]
                if text_blocks and role in ("user", "assistant"):
                    compacted.append({"role": role, "content": text_blocks})
            return compacted

        # Collect all toolUse IDs in order of appearance, then keep the last N
        all_tool_use_ids = []
        for msg in messages:
            if msg.get("role") == "assistant":
                for block in msg.get("content", []):
                    if "toolUse" in block:
                        all_tool_use_ids.append(block["toolUse"].get("toolUseId"))

        keep_ids = set(all_tool_use_ids[-keep_last_tool_pairs:]) if all_tool_use_ids else set()

        compacted = []
        for msg in messages:
            role = msg.get("role")
            content = msg.get("content", [])

            if role == "user":
                kept = []
                for b in content:
                    if "text" in b:
                        kept.append(b)
                    elif "toolResult" in b:
                        if b["toolResult"].get("toolUseId") in keep_ids:
                            kept.append(b)
                if kept:
                    compacted.append({"role": "user", "content": kept})
            elif role == "assistant":
                kept = []
                for b in content:
                    if "text" in b:
                        kept.append(b)
                    elif "toolUse" in b:
                        if b["toolUse"].get("toolUseId") in keep_ids:
                            kept.append(b)
                if kept:
                    compacted.append({"role": "assistant", "content": kept})

        return compacted
    
    def analyze(self, query: str):
        """
        Perform TCO analysis based on user query.
        Maintains conversation history across calls by creating a fresh agent
        each time (avoids ConcurrencyException) but reusing model/tools.
        
        Args:
            query: User query about AWS costs or TCO analysis
            
        Returns:
            Analysis response from the agent (AgentResult)
        """
        # Initialize MCP client once
        if self._mcp_client is None:
            self._mcp_client = self._create_mcp_client()
            self._mcp_client.__enter__()
            self._mcp_tools = self._mcp_client.list_tools_sync()
        
        # Compact conversation history — strip tool call/result messages
        # to reduce token overhead while preserving context
        prev_messages = []
        if self._agent:
            prev_messages = self._compact_messages(self._agent.messages)
        
        # Create fresh agent with compacted conversation history
        # This avoids ConcurrencyException while preserving context
        self._agent = self._create_agent(self._mcp_tools, messages=prev_messages)
        
        return self._agent(query)
    
    def __call__(self, query: str):
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
        print("  - Bedrock quota lookup (RPM/TPM limits)")
        print("\nType 'quit' to exit\n")
        
        # Create MCP client for interactive session
        self._mcp_client = self._create_mcp_client()
        
        with self._mcp_client:
            mcp_tools = self._mcp_client.list_tools_sync()
            print(f"Loaded {len(mcp_tools)} tools from AWS Knowledge MCP server\n")
            self._mcp_tools = mcp_tools
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
