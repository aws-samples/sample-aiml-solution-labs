"""
AWS TCO & Business Value Analyst Agent - Fast Initialization Version.

This version uses lazy initialization for MCP connections to avoid
AgentCore runtime timeout issues. MCP tools are loaded on-demand.
"""

import os
from strands import Agent
from strands.models import BedrockModel
from pricing_search_assistant import call_pricing_search_agent
from calculator_bva import bva_calculator, bva_what_if_analysis
from calculator_agentcore import use_agentcore_calculator, agentcore_what_if_analysis
from calculator_bedrock import use_bedrock_calculator, bedrock_what_if_analysis
from system_prompt import TCO_ANALYST_PROMPT

# =============================================================================
# CONFIGURATION
# =============================================================================
REGION = os.environ.get("AWS_REGION", "us-east-1")
MODEL_ID = os.environ.get("MODEL_ID", "us.anthropic.claude-haiku-4-5-20251001-v1:0")


class AwsTcoBvaAnalystFast:
    """
    AWS TCO & Business Value Analyst Agent - Fast initialization.
    
    This version initializes quickly without MCP connections,
    making it suitable for AgentCore runtime deployment.
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
        self._agent = None
        
        # Initialize agent immediately without MCP
        self._create_agent()
    
    def _create_agent(self) -> Agent:
        """Create and configure the Strands agent without MCP tools."""
        model = BedrockModel(
            model_id=self.model_id,
            temperature=0.1,
        )
        
        # Only include local tools - no MCP initialization
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
        
        self._agent = Agent(
            model=model,
            system_prompt=TCO_ANALYST_PROMPT,
            tools=tools,
        )
        
        return self._agent
    
    def analyze(self, query: str) -> str:
        """
        Perform TCO analysis based on user query.
        
        Args:
            query: User query about AWS costs or TCO analysis
            
        Returns:
            Analysis response from the agent
        """
        return self._agent(query)
    
    def __call__(self, query: str) -> str:
        """Allow calling the agent directly."""
        return self.analyze(query)


if __name__ == "__main__":
    agent = AwsTcoBvaAnalystFast()
    print("Agent initialized successfully!")
