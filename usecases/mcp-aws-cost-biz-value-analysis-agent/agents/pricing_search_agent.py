"""
TCO Analyst Agent using Strands Agents framework.

This agent uses Amazon Bedrock Knowledge Base to retrieve AWS pricing
information and AgentCore Code Interpreter to perform calculations
for TCO (Total Cost of Ownership) analysis.
"""

import os
from strands import Agent, tool
from strands.models import BedrockModel
from strands_tools import retrieve
from system_prompt import PRICING_SEARCH_PROMPT

# Configuration
KB_ID = os.environ.get("STRANDS_KNOWLEDGE_BASE_ID", "YOUR_KB_ID_HERE")
REGION = os.environ.get("AWS_REGION", "us-east-1")
MODEL_ID = "us.anthropic.claude-haiku-4-5-20251001-v1:0"


model = BedrockModel(
    model_id=MODEL_ID,
    temperature=0.1,
)


@tool
def filtered_retrieve(text, region="us-east-1"):
    """
        Retrieve AWS services pricing from a knowledge base.
        Args:
            text: A query string
            region: AWS region. If not specified, `us-east-1` by default.
        Return:
            retrieved documents from the knowledge base.
    """
    response = Agent(model=model, tools=[retrieve]).tool.retrieve(
        text,
        enableMetadata=True,
        kb_id=KB_ID,
        region_name=REGION,
        numberOfResults=7,
        score=0.7,
        retrieveFilter={
        "andAll": [
                {"stringContains": {"key": "category", "value": "security"}},
            ]
        }
    )
    print(response)
    return response


agent = Agent(
    model=model,
    system_prompt=PRICING_SEARCH_PROMPT,
    tools=[filtered_retrieve],
)


def main():
    """Run the TCO Analyst agent in interactive mode."""
    
    print("TCO Analyst Agent")
    print("=" * 50)
    print(f"Using Knowledge Base: {KB_ID}")
    print(f"Using Code Interpreter in region: {REGION}")
    print("Type 'quit' to exit\n")
    
    while True:
        user_input = input("You: ").strip()
        if user_input.lower() in ["quit", "exit", "q"]:
            print("Goodbye!")
            break
        if not user_input:
            continue
            
        #response = agent(user_input)
        response = filtered_retrieve(user_input)
        print(f"\nAgent: {response}\n")


if __name__ == "__main__":
    main()
