#!/usr/bin/env python3
"""
Interactive conversational interface for AWS TCO & BVA Analyst via AgentCore Runtime.
Shows real-time CloudWatch log progress while waiting for responses.
"""

import os
import sys
import json
import boto3
import uuid
import time
import threading
import re
from botocore.config import Config
from botocore.exceptions import ClientError

# Configuration — derived from AWS credentials and agentcore config
AGENT_NAME = "aws_tco_biz_value_analyst"


def _get_runtime_config():
    """Derive region, account, and runtime ARN from AWS credentials and agentcore config."""
    import yaml

    # Try to read from .bedrock_agentcore.yaml first
    config_path = os.path.join(os.path.dirname(__file__), ".bedrock_agentcore.yaml")
    if os.path.exists(config_path):
        with open(config_path) as f:
            config = yaml.safe_load(f)
        agent_cfg = config.get("agents", {}).get(AGENT_NAME, {})
        bc = agent_cfg.get("bedrock_agentcore", {})
        aws_cfg = agent_cfg.get("aws", {})
        arn = bc.get("agent_arn", "")
        region = aws_cfg.get("region", os.environ.get("AWS_REGION", "us-west-2"))
        if arn:
            return region, arn

    # Fallback: derive from STS + environment
    region = os.environ.get("AWS_REGION", "us-west-2")
    sts = boto3.client("sts", region_name=region)
    account_id = sts.get_caller_identity()["Account"]
    # Agent ID must be looked up or set via env var
    agent_id = os.environ.get("AGENTCORE_AGENT_ID", f"{AGENT_NAME}-UNKNOWN")
    arn = f"arn:aws:bedrock-agentcore:{region}:{account_id}:runtime/{agent_id}"
    return region, arn


REGION, RUNTIME_ARN = _get_runtime_config()
AGENT_ID = RUNTIME_ARN.split("/")[-1]
LOG_GROUP = f"/aws/bedrock-agentcore/runtimes/{AGENT_ID}-DEFAULT"

# Colors for terminal output
CYAN = "\033[36m"
YELLOW = "\033[33m"
GREEN = "\033[32m"
DIM = "\033[2m"
RESET = "\033[0m"


def tail_logs_live(stop_event: threading.Event, start_time_ms: int):
    """Tail CloudWatch logs in real-time to show agent progress."""
    logs_client = boto3.client('logs', region_name=REGION)
    next_token = None
    seen_messages = set()
    
    while not stop_event.is_set():
        try:
            kwargs = {
                'logGroupName': LOG_GROUP,
                'startTime': start_time_ms,
                'interleaved': True,
                'limit': 50,
            }
            if next_token:
                kwargs['nextToken'] = next_token

            resp = logs_client.filter_log_events(**kwargs)
            
            for event in resp.get('events', []):
                msg = event.get('message', '').strip()
                event_id = event.get('eventId', '')
                
                if event_id in seen_messages:
                    continue
                seen_messages.add(event_id)
                
                # Filter to interesting log lines
                line = _format_log_line(msg)
                if line:
                    print(f"\r{line}")
                    print(f"{CYAN}  ⏳ waiting...{RESET}", end="", flush=True)
            
            next_token = resp.get('nextToken')
            if not next_token:
                # Reset to poll for new events
                next_token = None
                
        except Exception:
            pass  # Don't crash the log tailer
        
        stop_event.wait(2)  # Poll every 2 seconds


def _format_log_line(msg: str) -> str:
    """Format a CloudWatch log line into a readable progress indicator."""
    # Our custom tool call logs
    if 'TOOL_CALL:' in msg:
        tool_name = msg.split('TOOL_CALL:')[-1].strip()
        return f"  {YELLOW}🔧 Calling tool: {tool_name}{RESET}"
    
    # Model response complete
    if 'Model response complete' in msg:
        return f"  {CYAN}📝 Model response complete{RESET}"
    
    # Bedrock model calls
    if 'HTTP Request: POST https://bedrock-runtime' in msg:
        return f"  {CYAN}🤖 Calling Bedrock model...{RESET}"
    
    # Pricing search / KB retrieval
    if 'bedrock:Retrieve' in msg or 'knowledge-base' in msg.lower():
        return f"  {CYAN}📚 Searching pricing knowledge base...{RESET}"
    
    # MCP calls
    if 'knowledge-mcp.global.api.aws' in msg:
        return f"  {CYAN}🌐 Querying AWS Knowledge MCP...{RESET}"
    
    # Invocation completion
    if 'Invocation completed successfully' in msg:
        match = re.search(r'\((\d+\.?\d*)s\)', msg)
        duration = match.group(1) if match else "?"
        return f"  {GREEN}✅ Completed in {duration}s{RESET}"
    
    # Credential loading (skip — too noisy)
    if 'Found credentials' in msg or 'IAM Role' in msg:
        return None
    
    # Strands agent init
    if 'Creating Strands MetricsClient' in msg:
        return f"  {DIM}⚙️  Initializing agent...{RESET}"
    
    # Generic errors
    if 'error' in msg.lower() and 'throttl' not in msg.lower():
        short = msg[:120]
        return f"  {YELLOW}⚠️  {short}{RESET}"
    
    return None


def invoke_agentcore_runtime(runtime_arn: str, prompt: str, session_id: str) -> str:
    """
    Invoke AgentCore runtime with CloudWatch log tailing for progress.
    
    Args:
        runtime_arn: ARN of the AgentCore runtime
        prompt: User query
        session_id: Session ID for conversation continuity (must be 33+ chars)
        
    Returns:
        Response text from the agent
    """
    client = boto3.client(
        'bedrock-agentcore',
        region_name=REGION,
        config=Config(
            read_timeout=300,
            connect_timeout=10,
            retries={'max_attempts': 0}
        )
    )
    
    # Start log tailing before the request
    start_time_ms = int(time.time() * 1000) - 1000  # 1s before now
    stop_logs = threading.Event()
    log_thread = threading.Thread(
        target=tail_logs_live,
        args=(stop_logs, start_time_ms),
        daemon=True
    )
    log_thread.start()
    
    print(f"{CYAN}  ⏳ waiting...{RESET}", end="", flush=True)
    
    try:
        payload = json.dumps({"prompt": prompt}).encode()
        
        response = client.invoke_agent_runtime(
            agentRuntimeArn=runtime_arn,
            runtimeSessionId=session_id,
            payload=payload
        )
        
        response_body = response["response"]
        full_response = response_body.read().decode('utf-8')
        
        # Stop log tailing
        stop_logs.set()
        log_thread.join(timeout=3)
        
        # Parse response
        try:
            response_text = json.loads(full_response)
            if isinstance(response_text, str):
                return response_text
            return json.dumps(response_text, indent=2)
        except json.JSONDecodeError:
            return full_response
            
    except ClientError as e:
        stop_logs.set()
        log_thread.join(timeout=3)
        error_msg = str(e)
        code = e.response.get('Error', {}).get('Code', 'Unknown')
        return f"❌ Error ({code}): {error_msg}"
    except Exception as e:
        stop_logs.set()
        log_thread.join(timeout=3)
        return f"❌ Error: {str(e)}"


def main():
    print("=" * 80)
    print("AWS TCO & BVA ANALYST - AGENTCORE RUNTIME CONVERSATIONAL MODE")
    print("=" * 80)
    print(f"\nRuntime ARN: {RUNTIME_ARN}")
    print(f"Region: {REGION}")
    print("\nWelcome! I can help you analyze AWS costs and business value.")
    print("Type 'exit' or 'quit' to end the conversation.\n")
    
    session_id = str(uuid.uuid4()) + "-session"
    print(f"Session ID: {session_id}\n")
    
    while True:
        try:
            query = input("You: ").strip()
            
            if query.lower() in ['exit', 'quit', 'bye']:
                print("\nGoodbye! 👋")
                break
            
            if not query:
                continue
            
            print(f"\n{DIM}--- Agent processing ---{RESET}")
            
            response_text = invoke_agentcore_runtime(RUNTIME_ARN, query, session_id)
            
            # Clear the progress line and print response
            print(f"\r" + " " * 40)
            print(f"\nAgent: {response_text}")
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
