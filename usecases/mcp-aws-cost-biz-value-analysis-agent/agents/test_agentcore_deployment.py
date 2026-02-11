#!/usr/bin/env python3
"""
Test script for AgentCore deployment of AWS TCO & BVA Analyst Agent.

This script invokes the deployed AgentCore agent using the IAM auth endpoint.
It reads the agent configuration from .bedrock_agentcore.yaml.

Prerequisites:
    1. Agent must be deployed to AgentCore:
       python deployment_helper.py
    
    2. AWS credentials configured with bedrock-agentcore:InvokeAgentRuntime permission

Usage:
    python test_agentcore_deployment.py
    python test_agentcore_deployment.py --query "Calculate Bedrock costs for 10K requests"
    python test_agentcore_deployment.py --verbose
"""

import argparse
import json
import os
import sys
import uuid

import boto3
import yaml


def load_agent_config() -> dict:
    """
    Load agent configuration from .bedrock_agentcore.yaml.
    
    Returns:
        Agent configuration dictionary
    """
    config_path = os.path.join(os.path.dirname(__file__), '.bedrock_agentcore.yaml')
    
    if not os.path.exists(config_path):
        print(f"❌ Configuration file not found: {config_path}")
        print("   Run 'python deployment_helper.py' to deploy the agent first.")
        sys.exit(1)
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config


def get_agent_info(config: dict) -> tuple:
    """
    Extract agent ARN, ID, and name from configuration.
    
    Args:
        config: Agent configuration dictionary
        
    Returns:
        Tuple of (agent_arn, agent_id, agent_name, region)
    """
    default_agent = config.get('default_agent')
    if not default_agent:
        print("❌ No default_agent specified in configuration")
        sys.exit(1)
    
    agents = config.get('agents', {})
    agent_config = agents.get(default_agent)
    
    if not agent_config:
        print(f"❌ Agent '{default_agent}' not found in configuration")
        sys.exit(1)
    
    bedrock_agentcore = agent_config.get('bedrock_agentcore', {})
    agent_arn = bedrock_agentcore.get('agent_arn')
    agent_id = bedrock_agentcore.get('agent_id')
    
    if not agent_arn or not agent_id:
        print(f"❌ Agent ARN or ID not found. Deploy the agent first:")
        print("   python deployment_helper.py")
        sys.exit(1)
    
    aws_config = agent_config.get('aws', {})
    region = aws_config.get('region', 'us-east-1')
    
    return agent_arn, agent_id, default_agent, region


def list_available_endpoints(agent_id: str, region: str) -> list:
    """
    List all available endpoints for the agent.
    
    Args:
        agent_id: Agent runtime ID
        region: AWS region
        
    Returns:
        List of endpoint names
    """
    client = boto3.client('bedrock-agentcore-control', region_name=region)
    
    try:
        response = client.list_agent_runtime_endpoints(
            agentRuntimeId=agent_id
        )
        
        endpoints = []
        for endpoint in response.get('runtimeEndpoints', []):
            endpoint_name = endpoint.get('endpointName')
            status = endpoint.get('status')
            if endpoint_name:
                endpoints.append({
                    'name': endpoint_name,
                    'status': status
                })
        
        return endpoints
    except Exception as e:
        print(f"⚠️  Warning: Could not list endpoints: {e}")
        return []


def invoke_agent(
    agent_arn: str,
    region: str,
    query: str,
    endpoint: str = None,
    verbose: bool = False
) -> dict:
    """
    Invoke the AgentCore deployed agent.
    
    Args:
        agent_arn: ARN of the deployed agent
        region: AWS region
        query: User query to send to the agent
        endpoint: Endpoint qualifier (optional, uses DEFAULT if not specified)
        verbose: Show detailed output
        
    Returns:
        Agent response dictionary
    """
    client = boto3.client('bedrock-agentcore', region_name=region)
    
    # Generate unique session ID
    session_id = str(uuid.uuid4())
    
    # Prepare payload
    payload = json.dumps({"prompt": query}).encode()
    
    if verbose:
        print(f"📤 Sending request...")
        print(f"   Agent ARN: {agent_arn}")
        if endpoint:
            print(f"   Endpoint: {endpoint}")
        print(f"   Session ID: {session_id}")
        print(f"   Query: {query[:100]}...")
    
    # Invoke the agent
    invoke_params = {
        'agentRuntimeArn': agent_arn,
        'runtimeSessionId': session_id,
        'payload': payload
    }
    
    # Only add qualifier if endpoint is specified
    if endpoint:
        invoke_params['qualifier'] = endpoint
    
    response = client.invoke_agent_runtime(**invoke_params)
    
    # Process response
    content_type = response.get("contentType", "")
    
    if "text/event-stream" in content_type:
        # Handle streaming response
        content = []
        for line in response["response"].iter_lines(chunk_size=10):
            if line:
                line = line.decode("utf-8")
                if line.startswith("data: "):
                    line = line[6:]
                    content.append(line)
                    if verbose:
                        print(f"   📥 Chunk: {line[:50]}...")
        return {"response": "\n".join(content), "streaming": True}
    
    elif content_type == "application/json":
        # Handle standard JSON response
        content = []
        for chunk in response.get("response", []):
            content.append(chunk.decode('utf-8'))
        result = ''.join(content)
        try:
            return json.loads(result)
        except json.JSONDecodeError:
            return {"response": result, "streaming": False}
    
    else:
        # Handle other response types
        content = []
        for chunk in response.get("response", []):
            content.append(chunk.decode('utf-8'))
        return {"response": ''.join(content), "content_type": content_type}


def run_test_suite(agent_arn: str, region: str, endpoint: str, verbose: bool = False):
    """
    Run a suite of test queries against the deployed agent.
    
    Args:
        agent_arn: ARN of the deployed agent
        region: AWS region
        endpoint: Endpoint qualifier
        verbose: Show detailed output
    """
    test_queries = [
        {
            "query": "What is the pricing for Claude Haiku in us-east-1?",
            "description": "Pricing lookup",
            "category": "Pricing"
        },
        {
            "query": "Calculate Bedrock costs for 10,000 questions per month using Claude Haiku with 1000 input tokens and 500 output tokens per question",
            "description": "Bedrock cost calculation",
            "category": "Bedrock"
        },
        {
            "query": "Calculate AgentCore costs for an agent running 100 hours/month with 2GB memory",
            "description": "AgentCore cost calculation",
            "category": "AgentCore"
        },
        {
            "query": "What's the ROI if I save 10 minutes per question, process 10,000 questions/month, labor cost is $50/hour, and AI costs $500/month?",
            "description": "Business value calculation",
            "category": "BVA"
        },
    ]
    
    print(f"\n{'='*80}")
    print("🧪 Running Test Suite")
    print(f"{'='*80}")
    
    passed = 0
    failed = 0
    
    for i, test in enumerate(test_queries, 1):
        print(f"\n{'─'*80}")
        print(f"📝 Test {i}/{len(test_queries)}: {test['description']}")
        print(f"   Category: {test['category']}")
        print(f"   Query: {test['query'][:60]}...")
        
        try:
            print("   ⏳ Processing...")
            result = invoke_agent(
                agent_arn=agent_arn,
                region=region,
                query=test['query'],
                endpoint=endpoint,
                verbose=verbose
            )
            
            # Check for errors in response
            if isinstance(result, dict) and 'error' in result:
                print(f"   ⚠️  Agent returned error: {result['error'][:100]}")
                failed += 1
            else:
                print(f"   ✅ Success")
                if verbose:
                    response_str = json.dumps(result, indent=2) if isinstance(result, dict) else str(result)
                    print(f"   Response: {response_str[:200]}...")
                passed += 1
                
        except Exception as e:
            print(f"   ❌ Failed: {str(e)[:100]}")
            if verbose:
                import traceback
                traceback.print_exc()
            failed += 1
    
    # Print summary
    print(f"\n{'='*80}")
    print("📊 Test Summary")
    print(f"{'='*80}")
    print(f"   Total Tests: {passed + failed}")
    print(f"   ✅ Passed: {passed}")
    print(f"   ❌ Failed: {failed}")
    if passed + failed > 0:
        print(f"   Success Rate: {(passed/(passed+failed)*100):.1f}%")
    print(f"{'='*80}")


def main():
    parser = argparse.ArgumentParser(
        description='Test AgentCore deployment of AWS TCO & BVA Analyst Agent',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full test suite
  python test_agentcore_deployment.py
  
  # Test single query
  python test_agentcore_deployment.py --query "Calculate Bedrock costs for 10k questions"
  
  # Use specific endpoint
  python test_agentcore_deployment.py --endpoint endpoint_IAM_auth
  
  # List available endpoints
  python test_agentcore_deployment.py --list-endpoints
  
  # Verbose output
  python test_agentcore_deployment.py --verbose
        """
    )
    parser.add_argument('--query', type=str,
                        help='Test a single query instead of running full suite')
    parser.add_argument('--endpoint', type=str,
                        help='Endpoint qualifier (optional, uses DEFAULT if not specified)')
    parser.add_argument('--list-endpoints', action='store_true',
                        help='List available endpoints and exit')
    parser.add_argument('--verbose', action='store_true',
                        help='Show detailed output')
    
    args = parser.parse_args()
    
    # Load configuration
    print(f"{'='*80}")
    print("🚀 AgentCore Deployment Test")
    print(f"{'='*80}")
    
    config = load_agent_config()
    agent_arn, agent_id, agent_name, region = get_agent_info(config)
    
    print(f"Agent Name: {agent_name}")
    print(f"Agent ID: {agent_id}")
    print(f"Agent ARN: {agent_arn}")
    print(f"Region: {region}")
    
    # List endpoints if requested
    if args.list_endpoints:
        print(f"\n{'='*80}")
        print("📋 Available Endpoints")
        print(f"{'='*80}")
        endpoints = list_available_endpoints(agent_id, region)
        if endpoints:
            for ep in endpoints:
                print(f"  • {ep['name']} (Status: {ep['status']})")
        else:
            print("  No custom endpoints found. Using DEFAULT endpoint.")
        print(f"{'='*80}")
        return
    
    # Show endpoint being used
    if args.endpoint:
        print(f"Endpoint: {args.endpoint}")
    else:
        print(f"Endpoint: DEFAULT (no qualifier)")
        # List available endpoints for reference
        endpoints = list_available_endpoints(agent_id, region)
        if endpoints:
            print(f"\n💡 Available custom endpoints:")
            for ep in endpoints:
                print(f"   • {ep['name']}")
            print(f"   Use --endpoint <name> to use a specific endpoint")
    
    try:
        if args.query:
            # Test single query
            print(f"\n{'='*80}")
            print("🤖 Testing Single Query")
            print(f"{'='*80}")
            print(f"Query: {args.query}")
            print("\n⏳ Processing...")
            
            result = invoke_agent(
                agent_arn=agent_arn,
                region=region,
                query=args.query,
                endpoint=args.endpoint,
                verbose=args.verbose
            )
            
            print(f"\n✅ Agent Response:")
            print(f"{'='*80}")
            if isinstance(result, dict):
                print(json.dumps(result, indent=2))
            else:
                print(result)
            print(f"{'='*80}")
        else:
            # Run full test suite
            run_test_suite(
                agent_arn=agent_arn,
                region=region,
                endpoint=args.endpoint,
                verbose=args.verbose
            )
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
