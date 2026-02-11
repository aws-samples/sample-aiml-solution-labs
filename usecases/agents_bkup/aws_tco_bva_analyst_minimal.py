"""
AWS TCO & BVA Analyst - Minimal Version for AgentCore.

This version uses direct Bedrock API calls without Strands framework
for fast initialization in AgentCore runtime.
"""

import os
import json
import boto3
from typing import Dict, Any

# Configuration
REGION = os.environ.get("AWS_REGION", "us-east-1")
MODEL_ID = os.environ.get("MODEL_ID", "us.anthropic.claude-haiku-4-5-20251001-v1:0")
KNOWLEDGE_BASE_ID = os.environ.get("STRANDS_KNOWLEDGE_BASE_ID", "")

# Lazy client initialization
_bedrock_runtime = None
_bedrock_agent_runtime = None


def get_bedrock_runtime():
    """Get or create Bedrock runtime client."""
    global _bedrock_runtime
    if _bedrock_runtime is None:
        _bedrock_runtime = boto3.client('bedrock-runtime', region_name=REGION)
    return _bedrock_runtime


def get_bedrock_agent_runtime():
    """Get or create Bedrock agent runtime client."""
    global _bedrock_agent_runtime
    if _bedrock_agent_runtime is None:
        _bedrock_agent_runtime = boto3.client('bedrock-agent-runtime', region_name=REGION)
    return _bedrock_agent_runtime


def search_pricing(query: str, target_region: str = "us-east-1") -> str:
    """Search for pricing information from Knowledge Base."""
    try:
        client = get_bedrock_agent_runtime()
        response = client.retrieve(
            knowledgeBaseId=KNOWLEDGE_BASE_ID,
            retrievalQuery={'text': query},
            retrievalConfiguration={
                'vectorSearchConfiguration': {
                    'numberOfResults': 10,
                    'overrideSearchType': 'HYBRID',
                    'filter': {
                        'stringContains': {
                            'key': 'x-amz-bedrock-kb-source-uri',
                            'value': f'/{target_region}/'
                        }
                    }
                }
            }
        )
        
        results = []
        for result in response.get('retrievalResults', []):
            content = result.get('content', {}).get('text', '')
            score = result.get('score', 0)
            if score >= 0.2:
                results.append(f"[Score: {score:.2f}] {content}")
        
        return "\n\n".join(results) if results else "No pricing information found."
    except Exception as e:
        return f"Error searching pricing: {str(e)}"


def call_bedrock(prompt: str, system_prompt: str = None) -> str:
    """Call Bedrock model directly."""
    try:
        client = get_bedrock_runtime()
        
        messages = [{"role": "user", "content": prompt}]
        
        body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 4096,
            "temperature": 0.1,
            "messages": messages
        }
        
        if system_prompt:
            body["system"] = system_prompt
        
        response = client.invoke_model(
            modelId=MODEL_ID,
            body=json.dumps(body)
        )
        
        response_body = json.loads(response['body'].read())
        return response_body['content'][0]['text']
    except Exception as e:
        return f"Error calling Bedrock: {str(e)}"


class AwsTcoBvaAnalystMinimal:
    """Minimal AWS TCO & BVA Analyst for fast AgentCore initialization."""
    
    def __init__(self):
        """Initialize with minimal overhead."""
        self.system_prompt = """You are an AWS TCO and Business Value Analyst.

You help customers calculate AWS costs and analyze business value for:
- Amazon Bedrock (foundation models)
- Amazon Bedrock AgentCore (AI agents)
- Business Value Analysis (ROI, cost savings)

When asked about costs:
1. Ask clarifying questions to gather required parameters
2. Search for pricing information using the search_pricing tool
3. Perform calculations
4. Present results clearly

Be concise and accurate."""
    
    def analyze(self, query: str) -> str:
        """Analyze the query and provide response."""
        # For now, simple pass-through to Bedrock
        # In production, you'd add tool calling logic here
        enhanced_prompt = f"""{query}

Available tools:
- search_pricing(query, target_region): Search AWS pricing information
- Calculator tools for Bedrock, AgentCore, and BVA calculations

Please provide a helpful response."""
        
        return call_bedrock(enhanced_prompt, self.system_prompt)
