#!/usr/bin/env python3
"""
Interactive conversational interface for AWS TCO & BVA Analyst.
"""

import os
import sys

# Set environment variables
os.environ["AWS_REGION"] = "us-west-2"
os.environ["STRANDS_KNOWLEDGE_BASE_ID"] = "OAQDDJYZSK"
# Switch models via this env var (e.g., us.anthropic.claude-haiku-4-5-20251001-v1:0 for cheaper runs)
os.environ["MODEL_ID"] = "us.anthropic.claude-sonnet-4-5-20250514-v1:0"

from aws_tco_bva_analyst import AwsTcoBvaAnalyst

def main():
    print("=" * 80)
    print("AWS TCO & BVA ANALYST - CONVERSATIONAL MODE")
    print("=" * 80)
    print("\nWelcome! I can help you analyze AWS costs and business value.")
    print("Type 'exit' or 'quit' to end the conversation.\n")
    
    # Initialize agent
    print("Initializing agent...\n")
    try:
        agent = AwsTcoBvaAnalyst()
        print("✅ Agent ready!\n")
    except Exception as e:
        print(f"❌ Failed to initialize agent: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    # Initialize MCP client and agent once for the entire session
    agent._mcp_client = agent._create_mcp_client()
    
    with agent._mcp_client:
        mcp_tools = agent._mcp_client.list_tools_sync()
        agent._agent = agent._create_agent(mcp_tools)
        
        # Conversational loop
        while True:
            try:
                # Get user input
                query = input("You: ").strip()
                
                # Check for exit commands
                if query.lower() in ['exit', 'quit', 'bye']:
                    print("\nGoodbye! 👋")
                    break
                
                # Skip empty queries
                if not query:
                    continue
                
                # Process query using the same agent instance (maintains history)
                print("\nAgent: ", end="", flush=True)
                response = agent._agent(query)
                print(response)
                print()
                
            except KeyboardInterrupt:
                print("\n\nGoodbye! 👋")
                break
            except Exception as e:
                print(f"\n❌ Error: {e}\n")
                import traceback
                traceback.print_exc()
                print()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
