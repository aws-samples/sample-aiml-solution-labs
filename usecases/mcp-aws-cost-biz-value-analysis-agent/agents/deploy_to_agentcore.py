#!/usr/bin/env python3
"""
Deploy Head Analyst Agent to Amazon Bedrock AgentCore.

This script configures and launches the Head Analyst Agent to AgentCore Runtime.
It uses the AgentCore CLI to handle deployment.

Usage:
    python deploy_to_agentcore.py                    # Configure and deploy
    python deploy_to_agentcore.py --configure-only  # Configure only
    python deploy_to_agentcore.py --launch-only     # Launch only (assumes configured)
    python deploy_to_agentcore.py --status          # Check deployment status
    python deploy_to_agentcore.py --destroy         # Destroy deployment
"""

import argparse
import subprocess
import sys
import os
import json

# =============================================================================
# CONFIGURATION
# =============================================================================
AGENT_NAME = "aws_tco_biz_value_analyst"
ENTRYPOINT = "head_analyst_agentcore.py"
REQUIREMENTS_FILE = "requirements.txt"
DEFAULT_REGION = os.environ.get("AWS_REGION", "us-east-1")
RUNTIME = "PYTHON_3_12"
PROTOCOL = "MCP"

# IAM Role ARN - Update this with your actual role ARN
EXECUTION_ROLE_ARN = os.environ.get(
    "AGENTCORE_EXECUTION_ROLE_ARN",
    ""  # Set via environment variable or update here
)


def run_command(cmd: list, check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command and return the result."""
    print(f"\n>>> Running: {' '.join(cmd)}\n")
    result = subprocess.run(cmd, check=check, capture_output=False)
    return result


def configure_agent(region: str):
    """Configure the agent for AgentCore deployment."""
    print("=" * 60)
    print("CONFIGURING AGENT FOR AGENTCORE")
    print("=" * 60)
    
    cmd = [
        "agentcore", "configure",
        "--entrypoint", ENTRYPOINT,
        "--name", AGENT_NAME,
        "--region", region,
        "--runtime", RUNTIME,
        "--protocol", PROTOCOL,
        "--requirements-file", REQUIREMENTS_FILE,
        "--disable-memory",
        "--non-interactive"
    ]
    
    if EXECUTION_ROLE_ARN:
        cmd.extend(["--execution-role", EXECUTION_ROLE_ARN])
    
    run_command(cmd)
    print("\n✓ Agent configured successfully")


def launch_agent(region: str, local: bool = False):
    """Launch the agent to AgentCore."""
    print("=" * 60)
    print("LAUNCHING AGENT TO AGENTCORE")
    print("=" * 60)
    
    cmd = ["agentcore", "launch", "--agent", AGENT_NAME]
    
    if local:
        cmd.append("--local")
    
    cmd.extend(["--env", f"AWS_REGION={region}"])
    
    run_command(cmd)
    print("\n✓ Agent launched successfully")


def check_status():
    """Check the deployment status."""
    print("=" * 60)
    print("CHECKING AGENT STATUS")
    print("=" * 60)
    
    run_command(["agentcore", "status", "--agent", AGENT_NAME, "--verbose"])


def invoke_agent(prompt: str):
    """Invoke the deployed agent with a test prompt."""
    print("=" * 60)
    print("INVOKING AGENT")
    print("=" * 60)
    
    payload = json.dumps({"prompt": prompt})
    run_command(["agentcore", "invoke", payload, "--agent", AGENT_NAME])


def destroy_agent():
    """Destroy the deployed agent."""
    print("=" * 60)
    print("DESTROYING AGENT DEPLOYMENT")
    print("=" * 60)
    
    run_command(["agentcore", "destroy", "--agent", AGENT_NAME, "--force"])
    print("\n✓ Agent destroyed successfully")


def main():
    parser = argparse.ArgumentParser(
        description="Deploy Head Analyst Agent to Amazon Bedrock AgentCore"
    )
    parser.add_argument(
        "--configure-only",
        action="store_true",
        help="Only configure the agent, don't launch"
    )
    parser.add_argument(
        "--launch-only",
        action="store_true",
        help="Only launch the agent (assumes already configured)"
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Run locally using Docker instead of deploying to cloud"
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Check deployment status"
    )
    parser.add_argument(
        "--invoke",
        type=str,
        metavar="PROMPT",
        help="Invoke the agent with a test prompt"
    )
    parser.add_argument(
        "--destroy",
        action="store_true",
        help="Destroy the deployed agent"
    )
    parser.add_argument(
        "--region",
        type=str,
        default=DEFAULT_REGION,
        help=f"AWS region (default: {DEFAULT_REGION})"
    )
    
    args = parser.parse_args()
    region = args.region
    
    try:
        if args.status:
            check_status()
        elif args.invoke:
            invoke_agent(args.invoke)
        elif args.destroy:
            destroy_agent()
        elif args.configure_only:
            configure_agent(region)
        elif args.launch_only:
            launch_agent(region, local=args.local)
        else:
            configure_agent(region)
            launch_agent(region, local=args.local)
            print("\n" + "=" * 60)
            print("DEPLOYMENT COMPLETE")
            print("=" * 60)
            print(f"\nAgent Name: {AGENT_NAME}")
            print(f"Region: {region}")
            print("\nTo check status:")
            print("  python deploy_to_agentcore.py --status")
            print("\nTo invoke:")
            print("  python deploy_to_agentcore.py --invoke 'Calculate Bedrock costs'")
            print("\nTo destroy:")
            print("  python deploy_to_agentcore.py --destroy")
            
    except subprocess.CalledProcessError as e:
        print(f"\n✗ Command failed with exit code {e.returncode}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nAborted by user")
        sys.exit(1)


if __name__ == "__main__":
    main()
