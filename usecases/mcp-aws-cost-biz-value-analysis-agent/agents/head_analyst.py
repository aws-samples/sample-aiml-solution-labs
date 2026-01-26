"""
Head Analyst Agent using Strands Agents framework.

This agent orchestrates TCO analysis by combining pricing search capabilities
with code interpreter for calculations. It uses the pricing_search_agent as a
tool to retrieve AWS pricing data and AgentCore Code Interpreter for computations.
"""

import os
from strands import Agent
from strands.models import BedrockModel
from strands_tools.code_interpreter import AgentCoreCodeInterpreter
from pricing_search_assistant import call_pricing_search_agent
from scenario_analyst_genai import invoke_genai_scenario_analyst_agent
from chart_analysis_agent import generate_chart_data
from system_prompt import TCO_ANALYST_PROMPT

# =============================================================================
# CONFIGURATION
# =============================================================================
REGION = os.environ.get("AWS_REGION", "us-east-1")
MODEL_ID = os.environ.get("MODEL_ID", "global.anthropic.claude-sonnet-4-5-20250929-v1:0")


class HeadAnalystAgent:
    """
    Head Analyst Agent for TCO and business value analysis.
    
    Combines pricing search from Bedrock Knowledge Base with
    AgentCore Code Interpreter for comprehensive cost analysis.
    """
    
    def __init__(self, region: str = None, model_id: str = None):
        """
        Initialize the Head Analyst Agent.
        
        Args:
            region: AWS region for services
            model_id: Bedrock model ID to use
        """
        self.region = region or REGION
        self.model_id = model_id or MODEL_ID
        self._agent = self._create_agent()
    
    def _create_agent(self) -> Agent:
        """Create and configure the Strands agent."""
        model = BedrockModel(
            model_id=self.model_id,
            temperature=0.1,
        )
        
        code_interpreter = AgentCoreCodeInterpreter(region=self.region)
        
        return Agent(
            model=model,
            system_prompt=TCO_ANALYST_PROMPT,
            tools=[
                call_pricing_search_agent,
                invoke_genai_scenario_analyst_agent,
                generate_chart_data,
            ],
        )
    
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
    
    def run_interactive(self):
        """Run the agent in interactive chat mode."""
        print("Head Analyst Agent - TCO & Business Value Analysis")
        print("=" * 60)
        print(f"Region: {self.region}")
        print(f"Model: {self.model_id}")
        print("\nCapabilities:")
        print("  - AWS pricing search via Knowledge Base")
        print("  - Scenario analysis for TCO and business value")
        print("  - Chart data generation for QuickSight")
        print("\nType 'quit' to exit\n")
        
        while True:
            user_input = input("You: ").strip()
            if user_input.lower() in ["quit", "exit", "q"]:
                print("Goodbye!")
                break
            if not user_input:
                continue
            
            print("\nAnalyzing...\n")
            response = self.analyze(user_input)
            print(f"Agent: {response}\n")


if __name__ == "__main__":
    agent = HeadAnalystAgent()
    agent.run_interactive()
