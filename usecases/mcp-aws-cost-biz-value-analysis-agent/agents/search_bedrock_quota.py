#!/usr/bin/env python3
"""
AWS Bedrock Quota Search Tool for Strands Agents.

Queries the Bedrock Knowledge Base for pre-scraped RPM/TPM quota data
stored under /quota_data/{region}/. No live Service Quotas API calls.
"""

import os
import boto3
from typing import List, Dict, Any
from strands import tool

# =============================================================================
# CONFIGURATION
# =============================================================================
STRANDS_KNOWLEDGE_BASE_ID = os.environ.get("STRANDS_KNOWLEDGE_BASE_ID", "<PLACE_YOUR_KB_ID>")
REGION = os.environ.get("AWS_REGION", "us-east-1")

RETRIEVE_NUM_RESULTS = 10
RETRIEVE_MIN_SCORE = 0.2

bedrock_agent_runtime = boto3.client(
    'bedrock-agent-runtime',
    region_name=REGION,
)


def filtered_quota_retrieve(query: str, target_region: str = "us-east-1") -> List[Dict[str, Any]]:
    """
    Retrieve Bedrock quota data from the Knowledge Base.

    Filters results to /quota_data/ and the target region.

    Args:
        query: Search query (e.g. 'Claude Sonnet 4.6 RPM TPM quota').
        target_region: AWS region code to filter on.

    Returns:
        List of dicts with content, score, source_uri, metadata.
    """
    try:
        response = bedrock_agent_runtime.retrieve(
            knowledgeBaseId=STRANDS_KNOWLEDGE_BASE_ID,
            retrievalQuery={'text': query},
            retrievalConfiguration={
                'vectorSearchConfiguration': {
                    'numberOfResults': RETRIEVE_NUM_RESULTS,
                    'overrideSearchType': 'HYBRID',
                    'filter': {
                        'andAll': [
                            {
                                'stringContains': {
                                    'key': 'x-amz-bedrock-kb-source-uri',
                                    'value': '/quota_data/',
                                }
                            },
                            {
                                'stringContains': {
                                    'key': 'x-amz-bedrock-kb-source-uri',
                                    'value': f'/{target_region}/',
                                }
                            },
                        ]
                    },
                }
            },
        )

        results = []
        for result in response.get('retrievalResults', []):
            score = result.get('score', 0)
            if score < RETRIEVE_MIN_SCORE:
                continue

            content = result.get('content', {}).get('text', '')
            location = result.get('location', {})
            source_uri = ''
            if location.get('type') == 'S3':
                source_uri = location.get('s3Location', {}).get('uri', '')

            results.append({
                'content': content,
                'score': score,
                'source_uri': source_uri,
                'metadata': result.get('metadata', {}),
            })

        return results

    except Exception as e:
        return [{'error': str(e)}]


@tool
def call_bedrock_quota_agent(query: str, target_region: str = "us-east-1") -> str:
    """
    Search for Bedrock model RPM/TPM quota information from the Knowledge Base.

    Use this tool to look up default quota limits (RPM, TPM) for Bedrock models.
    Pass the model name exactly as the user provides it.

    Args:
        query: Natural-language quota question, e.g.
            'Claude Sonnet 4.6 RPM TPM quota'
        target_region: AWS region to filter results (default: us-east-1)

    Returns:
        Retrieved quota documents as formatted text.
    """
    results = filtered_quota_retrieve(query, target_region)

    if not results:
        return "No quota information found for the query."

    if isinstance(results, list) and len(results) > 0 and 'error' in results[0]:
        return f"Error retrieving quota: {results[0]['error']}"

    output = []
    for i, doc in enumerate(results, 1):
        output.append(f"--- Quota Result {i} (score: {doc.get('score', 'N/A'):.2f}) ---")
        output.append(doc.get('content', ''))
        output.append(f"Source: {doc.get('source_uri', 'N/A')}")
        output.append("")

    return "\n".join(output)


def main():
    """Interactive test for the quota search tool."""
    print("Bedrock Quota Search Tool")
    print("=" * 50)
    print(f"Knowledge Base: {STRANDS_KNOWLEDGE_BASE_ID}")
    print(f"Region: {REGION}")
    print("Type 'quit' to exit\n")

    while True:
        q = input("Query: ").strip()
        if q.lower() in ("quit", "exit", "q"):
            break
        if not q:
            continue
        print(f"\n{call_bedrock_quota_agent(q)}\n")


if __name__ == '__main__':
    main()
