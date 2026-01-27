"""
Pricing Search Agent using Strands Agents framework.

This agent uses Amazon Bedrock Knowledge Base to retrieve AWS pricing
information for TCO (Total Cost of Ownership) analysis.
"""

import os
import boto3
from typing import List, Dict, Any
from strands import tool

# =============================================================================
# CONFIGURATION CONSTANTS
# =============================================================================
STRANDS_KNOWLEDGE_BASE_ID = os.environ.get("STRANDS_KNOWLEDGE_BASE_ID", "<PLACE_YOUR_KB_ID>")
REGION = os.environ.get("AWS_REGION", "us-east-1")

# Retrieval parameters
RETRIEVE_NUM_RESULTS = 15
RETRIEVE_MIN_SCORE = 0.2

# =============================================================================
# BEDROCK CLIENT
# =============================================================================
bedrock_agent_runtime = boto3.client(
    'bedrock-agent-runtime',
    region_name=REGION
)


def filtered_retrieve(query: str, target_region: str = "us-east-1") -> List[Dict[str, Any]]:
    """
    Retrieve AWS services pricing from a Bedrock Knowledge Base.
    
    Args:
        query: Search query string for pricing information
        target_region: AWS region to filter results (default: us-east-1)
        
    Returns:
        List of retrieved documents with content and metadata
    """
    try:
        response = bedrock_agent_runtime.retrieve(
            knowledgeBaseId=STRANDS_KNOWLEDGE_BASE_ID,
            retrievalQuery={
                'text': query
            },
            retrievalConfiguration={
                'vectorSearchConfiguration': {
                    'numberOfResults': RETRIEVE_NUM_RESULTS,
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
            score = result.get('score', 0)
            
            # Filter by minimum score
            if score < RETRIEVE_MIN_SCORE:
                continue
                
            content = result.get('content', {}).get('text', '')
            location = result.get('location', {})
            metadata = result.get('metadata', {})
            
            # Extract source URI
            source_uri = ''
            if location.get('type') == 'S3':
                source_uri = location.get('s3Location', {}).get('uri', '')
            
            results.append({
                'content': content,
                'score': score,
                'source_uri': source_uri,
                'metadata': metadata
            })
        
        return results
        
    except Exception as e:
        return [{'error': str(e)}]


@tool
def call_pricing_search_agent(query: str, target_region: str = "us-east-1") -> str:
    """
    Search for AWS pricing information from the Knowledge Base.
    
    Args:
        query: Search query for AWS pricing (e.g., "Claude Haiku input token pricing")
        target_region: AWS region to filter results (default: us-east-1)
        
    Returns:
        Retrieved pricing documents as formatted text
    """
    # Directly call filtered_retrieve instead of using nested agent
    # to avoid tool ID conflicts in Strands
    results = filtered_retrieve(query, target_region)
    
    if not results:
        return "No pricing information found for the query."
    
    if isinstance(results, list) and len(results) > 0 and 'error' in results[0]:
        return f"Error retrieving pricing: {results[0]['error']}"
    
    # Format results as text
    output = []
    for i, doc in enumerate(results, 1):
        output.append(f"--- Document {i} (score: {doc.get('score', 'N/A'):.2f}) ---")
        output.append(doc.get('content', ''))
        output.append(f"Source: {doc.get('source_uri', 'N/A')}")
        output.append("")
    
    return "\n".join(output)


def main():
    """Run the Pricing Search in interactive mode."""
    print("Pricing Search Tool")
    print("=" * 50)
    print(f"Knowledge Base: {STRANDS_KNOWLEDGE_BASE_ID}")
    print(f"Region: {REGION}")
    print(f"Results per query: {RETRIEVE_NUM_RESULTS}")
    print(f"Min score threshold: {RETRIEVE_MIN_SCORE}")
    print("Type 'quit' to exit\n")
    
    while True:
        user_input = input("Query: ").strip()
        if user_input.lower() in ["quit", "exit", "q"]:
            print("Goodbye!")
            break
        if not user_input:
            continue
        
        response = call_pricing_search_agent(user_input)
        print(f"\nResults:\n{response}\n")


if __name__ == "__main__":
    main()
