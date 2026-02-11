#!/usr/bin/env python3
"""
Simple test of AgentCore runtime API.
"""

import boto3
import json

RUNTIME_ARN = "arn:aws:bedrock-agentcore:us-west-2:955644293412:runtime/aws_tco_biz_value_analyst-vKU6HZ9Ybt"
REGION = "us-west-2"

client = boto3.client('bedrock-agentcore', region_name=REGION)

# Prepare the payload
prompt = "Calculate Bedrock costs for 100K questions per month using Claude Haiku"
payload = json.dumps({"prompt": prompt}).encode()

print(f"Invoking runtime: {RUNTIME_ARN}")
print(f"Prompt: {prompt}\n")

try:
    response = client.invoke_agent_runtime(
        agentRuntimeArn=RUNTIME_ARN,
        runtimeSessionId='test-session-123456789012345678901234567890',
        payload=payload
    )
    
    print(f"Response received!")
    print(f"Content Type: {response.get('contentType')}")
    print(f"Response keys: {response.keys()}\n")
    
    # Process streaming response
    if "text/event-stream" in response.get("contentType", ""):
        print("Processing streaming response...")
        for line in response["response"].iter_lines(chunk_size=10):
            if line:
                line = line.decode("utf-8")
                print(line)
    else:
        print("Response:", response)
        
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
