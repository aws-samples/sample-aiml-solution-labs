#!/usr/bin/env python3
"""
AWS Bedrock Quota Tool for Strands Agents

This module provides a Strands tool to retrieve RPM (Requests Per Minute) 
and TPM (Tokens Per Minute) quotas for AWS Bedrock LLM models.
"""

import boto3
from typing import Optional, Literal
from strands import tool, Agent


@tool
def get_bedrock_quota(
    model_name: str,
    region: str = 'us-east-1',
    inference_type: Literal['on-demand', 'cross-region', 'global-cross-region'] = 'on-demand'
) -> dict:
    """Get RPM and TPM quotas for AWS Bedrock LLM models.
    
    Args:
        model_name: Name of the LLM model (e.g., 'claude-3-sonnet', 'claude 3 haiku', 
            'llama 3.1 70b instruct'). Supports both dash and space formats.
        region: AWS region to query. Defaults to 'us-east-1'.
        inference_type: Type of inference - 'on-demand', 'cross-region', or 'global-cross-region'.
            Defaults to 'on-demand'.
    
    Returns:
        Dictionary with model, region, inference_type, rpm, and tpm values.
    """
    # Validate inference_type
    valid_types = ['on-demand', 'cross-region', 'global-cross-region']
    if inference_type not in valid_types:
        raise ValueError(f"inference_type must be one of {valid_types}")
    
    client = boto3.client('service-quotas', region_name=region)
    
    # Normalize model name - handle both dash and space formats
    model_normalized = model_name.lower().replace('-', ' ')
    # Extract significant words for flexible matching (ignore short words)
    model_words = set(w for w in model_normalized.split() if len(w) >= 2)
    
    # Collect all matching quotas in a single pass, grouped by inference type
    matches = {'on-demand': {}, 'cross-region': {}, 'global-cross-region': {}}
    
    try:
        paginator = client.get_paginator('list_service_quotas')
        
        for page in paginator.paginate(ServiceCode='bedrock'):
            for quota in page['Quotas']:
                name = quota['QuotaName'].lower()
                
                # Skip non-RPM/TPM and customization quotas
                if 'model customization' in name or 'custom model deployment' in name:
                    continue
                if 'requests per minute' not in name and 'tokens per minute' not in name:
                    continue
                
                # Match model name using word-set or substring
                if not model_words.issubset(set(name.split())):
                    if model_normalized not in name:
                        continue
                
                # Classify by inference type
                if 'global cross-region' in name or 'global-cross-region' in name:
                    bucket = 'global-cross-region'
                elif 'cross-region' in name:
                    bucket = 'cross-region'
                elif 'on-demand' in name:
                    bucket = 'on-demand'
                else:
                    continue
                
                if 'requests per minute' in name:
                    matches[bucket]['rpm'] = quota['Value']
                elif 'tokens per minute' in name:
                    matches[bucket]['tpm'] = quota['Value']
                        
    except Exception as e:
        raise Exception(f"Failed to retrieve quotas: {str(e)}")
    
    # Try the requested inference type first, then fall back to others
    fallback_order = {
        'on-demand': ['on-demand', 'cross-region', 'global-cross-region'],
        'cross-region': ['cross-region', 'global-cross-region', 'on-demand'],
        'global-cross-region': ['global-cross-region', 'cross-region', 'on-demand'],
    }
    
    for try_type in fallback_order[inference_type]:
        bucket = matches[try_type]
        if bucket.get('rpm') is not None or bucket.get('tpm') is not None:
            return {
                'model': model_name,
                'region': region,
                'inference_type': try_type,
                'rpm': bucket.get('rpm'),
                'tpm': bucket.get('tpm'),
            }
    
    return {
        'model': model_name,
        'region': region,
        'inference_type': inference_type,
        'rpm': None,
        'tpm': None,
    }


def main():
    """Test the quota tool with a Strands agent using natural language queries."""
    
    # Create agent with the quota tool and system prompt
    agent = Agent(
        tools=[get_bedrock_quota],
        system_prompt=(
            "You are an AWS Bedrock quota assistant. "
            "Use the get_bedrock_quota tool to retrieve RPM and TPM quotas for AWS Bedrock models. "
            "Always provide clear, formatted responses with the quota values."
        )
    )
    
    # Test queries - natural language questions
    queries = [
        "What are the RPM and TPM quotas for claude-3-sonnet in us-east-1?",
        "Get the cross-region quotas for claude 3 haiku",
        "What's the on-demand quota for llama 3.1 70b instruct?",
    ]
    
    print("=" * 60)
    print("Testing AWS Bedrock Quota Tool with Strands Agent")
    print("=" * 60)
    print()
    
    for i, query in enumerate(queries, 1):
        print(f"\nQuery {i}: {query}")
        print("-" * 60)
        
        try:
            # Call agent directly - synchronous!
            result = agent(query)
            print(f"\n✓ Query completed successfully")
        except Exception as e:
            print(f"✗ Error: {e}")
            import traceback
            traceback.print_exc()
        
        print()
    
    print("=" * 60)
    print("All tests completed!")
    print("=" * 60)


if __name__ == '__main__':
    main()
