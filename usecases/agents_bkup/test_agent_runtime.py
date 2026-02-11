#!/usr/bin/env python3
"""
Interactive conversational interface for AWS TCO & BVA Analyst via AgentCore Runtime.
"""

import os
import sys
import json
import boto3
import uuid
from botocore.exceptions import ClientError

# Configuration
RUNTIME_ARN = "arn:aws:bedrock-agentcore:us-west-2:955644293412:runtime/aws_tco_biz_value_analyst-vKU6HZ9Ybt"
REGION = "us-west-2"


def invoke_agentcore_runtime_streaming(runtime_arn: str, prompt: str, session_id: str):
    """
    Invoke AgentCore runtime endpoint with streaming output.
    
    Args:
        runtime_arn: ARN of the AgentCore runtime
        prompt: User query
        session_id: Session ID for conversation continuity (must be 33+ chars)
        
    Yields:
        Response chunks as they arrive
    """
    client = boto3.client('bedrock-agentcore', region_name=REGION)
    
    try:
        # Prepare the payload
        payload = json.dumps({"prompt": prompt}).encode()
        
        # Invoke the agent runtime
        response = client.invoke_agent_runtime(
            agentRuntimeArn=runtime_arn,
            runtimeSessionId=session_id,
            payload=payload
        )
        
        # Read entire response first (AgentCore doesn't support true streaming yet)
        response_body = response["response"]
        full_response = response_body.read().decode('utf-8')
        
        # Parse the JSON response
        try:
            # The response is a JSON string, parse it
            response_text = json.loads(full_response)
            
            # Now stream the formatted text character by character
            for char in response_text:
                yield char
                
        except json.JSONDecodeError:
            # If not JSON, just yield the raw response
            for char in full_response:
                yield char
            
    except ClientError as e:
        yield f"\n❌ Error: {str(e)}"
        if 'Error' in e.response and 'Code' in e.response['Error']:
            yield f"\nError Code: {e.response['Error']['Code']}"
    except Exception as e:
        yield f"\n❌ Error: {str(e)}"


def main():
    print("=" * 80)
    print("AWS TCO & BVA ANALYST - AGENTCORE RUNTIME CONVERSATIONAL MODE")
    print("=" * 80)
    print(f"\nRuntime ARN: {RUNTIME_ARN}")
    print(f"Region: {REGION}")
    print("\nWelcome! I can help you analyze AWS costs and business value.")
    print("Type 'exit' or 'quit' to end the conversation.\n")
    
    # Generate session ID for conversation continuity (must be 33+ chars)
    session_id = str(uuid.uuid4()) + "-session"
    print(f"Session ID: {session_id}\n")
    
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
            
            # Invoke runtime with streaming simulation
            print("\nAgent: ", end="", flush=True)
            
            import time
            word_buffer = ""
            for char in invoke_agentcore_runtime_streaming(RUNTIME_ARN, query, session_id):
                word_buffer += char
                # Stream word by word for better readability
                if char in [' ', '\n', '\t']:
                    print(word_buffer, end="", flush=True)
                    word_buffer = ""
                    time.sleep(0.02)  # Slight delay between words
            
            # Print any remaining characters
            if word_buffer:
                print(word_buffer, end="", flush=True)
            
            print("\n")
            
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
