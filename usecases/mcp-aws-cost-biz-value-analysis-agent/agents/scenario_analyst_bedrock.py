"""
Business Value Analyst Agent using Strands Agents framework.

This agent performs comprehensive business value analysis (BVA) calculations
including cost savings, revenue growth, customer churn reduction, and ROI analysis.
Uses AgentCore Code Interpreter for numerical calculations.
"""

import os
from strands import Agent, tool
from strands.models import BedrockModel
from system_prompt import SCENARIO_ANALYST_GENAI_PROMPT
from strands_tools.code_interpreter import AgentCoreCodeInterpreter

REGION = os.environ.get("AWS_REGION", "us-east-1")
MODEL_ID = "global.anthropic.claude-sonnet-4-5-20250929-v1:0"


def create_agent():
    """Create and configure the Business Value Analyst agent."""
    model = BedrockModel(
        model_id=MODEL_ID,
        temperature=0.1,
    )
    
    code_interpreter = AgentCoreCodeInterpreter(region=REGION)
    
    agent = Agent(
        model=model,
        system_prompt=SCENARIO_ANALYST_GENAI_PROMPT,
        #tools=[code_interpreter.code_interpreter],
    )
    
    return agent


@tool
def invoke_genai_scenario_analyst_agent(query: str):
    """
    Invoke the GenAI Scenario Analyst Agent for AWS cost and business value analysis.
    
    This agent generates comprehensive metrics including TCO (Total Cost of Ownership)
    analysis, business value calculations, and what-if scenario results for Amazon
    Bedrock and Bedrock AgentCore use cases.
    
    Args:
        query: Detailed description of the use case including:
            - Business scenario and assumptions (e.g., number of users, requests/month)
            - Desired analysis type (TCO, ROI, cost savings, revenue impact)
            - What-if parameters to explore (e.g., varying request volumes, model choices)
            - Bedrock model specifications (model ID, token estimates)
            - AgentCore component requirements (runtime, memory, code interpreter usage)
    
    Returns:
        Agent response containing:
            - Cost breakdown for Bedrock and AgentCore components
            - Business value metrics (cost savings, revenue growth, churn reduction)
            - ROI calculations and payback period
            - What-if analysis results comparing different scenarios
    
    Examples:
        >>> invoke_genai_scenario_analyst_agent(
        ...     "Calculate TCO for a customer support agent using Claude Haiku "
        ...     "with 50,000 questions/month, 3000 input tokens and 1000 output tokens "
        ...     "per question. Include AgentCore runtime and memory costs."
        ... )
        
        >>> invoke_genai_scenario_analyst_agent(
        ...     "What's the ROI of implementing an AI agent that reduces support "
        ...     "ticket resolution time from 15 minutes to 3 minutes? We have "
        ...     "10,000 tickets/month and labor cost is $75/hour."
        ... )
        
        >>> invoke_genai_scenario_analyst_agent(
        ...     "Compare costs between Claude Sonnet and Claude Haiku for a "
        ...     "document processing use case with 100,000 documents/month."
        ... )
    """
    agent = create_agent()
    response = agent(query)
    return response


def main():
    """Run the Business Value Analyst agent in interactive mode."""
    agent = create_agent()
    
    print("Business Value Analyst Agent")
    print("=" * 50)
    print("Calculates ROI, cost savings, and business value for AI implementations")
    print("Type 'quit' to exit\n")
    
    # Example prompt
    print("Example: 'Calculate BVA for an AI agent handling 5000 questions/month'")
    print("         'with $50/hour labor cost and 500 customers'\n")
    
    while True:
        user_input = input("You: ").strip()
        if user_input.lower() in ["quit", "exit", "q"]:
            print("Goodbye!")
            break
        if not user_input:
            continue
            
        response = agent(user_input)
        print(f"\nAgent: {response}\n")


if __name__ == "__main__":
    main()
