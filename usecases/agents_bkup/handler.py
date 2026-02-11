"""
Ultra-minimal handler for AgentCore without bedrock-agentcore dependency.
"""

import json
from aws_tco_bva_analyst_minimal import AwsTcoBvaAnalystMinimal

# Global analyst instance
analyst = None


def get_analyst():
    """Get or create analyst instance."""
    global analyst
    if analyst is None:
        analyst = AwsTcoBvaAnalystMinimal()
    return analyst


def handler(event, context):
    """
    Lambda-style handler for AgentCore.
    
    Args:
        event: Event dict with 'prompt' key
        context: Lambda context (unused)
    
    Returns:
        Response dict
    """
    try:
        # Parse input
        if isinstance(event, str):
            event = json.loads(event)
        
        prompt = event.get('prompt', '')
        if not prompt:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing prompt'})
            }
        
        # Get response
        response = get_analyst().analyze(prompt)
        
        return {
            'statusCode': 200,
            'body': json.dumps({'response': response})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
