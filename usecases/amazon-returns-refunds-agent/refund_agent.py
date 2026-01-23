"""
Amazon Returns & Refunds Assistant

Provides accurate, policy-based answers for Amazon returns and refunds
using Bedrock Knowledge Base with country-specific policies.

Usage:
    python refund_agent.py
    
    Or programmatically:
    from refund_agent import agent
    agent("What is the refund policy for a refrigerator I brought 2 years ago in India?")
"""

import sys
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
from strands import Agent
from strands.models import BedrockModel
from strands_tools import retrieve, use_aws

# Configuration Constants
KB_ID = "EK2IHAXS8Q"  # Bedrock Knowledge Base ID

def validate_aws_credentials():
    """Validate AWS credentials and provide helpful error messages."""
    try:
        # Try to get caller identity to validate credentials
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        print(f"✓ AWS credentials validated")
        print(f"  Account: {identity['Account']}")
        print(f"  User/Role: {identity['Arn'].split('/')[-1]}")
        return True
    except NoCredentialsError:
        print("\n❌ ERROR: No AWS credentials found")
        print("\nTo fix this, choose one of the following options:")
        print("\n1. Configure AWS CLI (recommended):")
        print("   aws configure")
        print("   Then enter your AWS Access Key ID and Secret Access Key")
        print("\n2. Set environment variables:")
        print("   export AWS_ACCESS_KEY_ID=your_access_key")
        print("   export AWS_SECRET_ACCESS_KEY=your_secret_key")
        print("   export AWS_DEFAULT_REGION=us-east-1")
        print("\n3. Use AWS SSO:")
        print("   aws sso login --profile your-profile")
        print("   export AWS_PROFILE=your-profile")
        print("\nFor more info: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html")
        return False
    except PartialCredentialsError:
        print("\n❌ ERROR: Incomplete AWS credentials")
        print("\nYou have partial credentials configured. Please ensure both:")
        print("  - AWS_ACCESS_KEY_ID")
        print("  - AWS_SECRET_ACCESS_KEY")
        print("\nRun: aws configure")
        return False
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ExpiredToken':
            print("\n❌ ERROR: AWS credentials have expired")
            print("\nYour AWS session token has expired. To fix this:")
            print("\n1. If using temporary credentials (SSO/assumed role):")
            print("   aws sso login --profile your-profile")
            print("   # or")
            print("   aws sts assume-role --role-arn your-role-arn --role-session-name session")
            print("\n2. If using access keys, they may have been rotated:")
            print("   aws configure")
            print("   # Enter new credentials")
            print("\n3. Check credential expiration:")
            print("   aws sts get-caller-identity")
            return False
        elif error_code == 'InvalidClientTokenId':
            print("\n❌ ERROR: Invalid AWS credentials")
            print("\nYour AWS Access Key ID is invalid or has been deleted.")
            print("\nTo fix this:")
            print("1. Verify your credentials in AWS IAM console")
            print("2. Generate new access keys if needed")
            print("3. Run: aws configure")
            return False
        else:
            print(f"\n❌ ERROR: AWS credential validation failed: {error_code}")
            print(f"Message: {e.response['Error']['Message']}")
            return False
    except Exception as e:
        print(f"\n❌ ERROR: Unexpected error validating credentials: {str(e)}")
        return False

def check_bedrock_permissions():
    """Check if the user has necessary Bedrock permissions."""
    try:
        bedrock = boto3.client('bedrock', region_name='us-west-2')
        # Try to list foundation models as a permission check
        bedrock.list_foundation_models(byProvider='anthropic')
        print("✓ Bedrock permissions validated")
        return True
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'AccessDeniedException':
            print("\n⚠ WARNING: Limited Bedrock permissions")
            print("\nYou may not have full Bedrock access. Required permissions:")
            print("  - bedrock:InvokeModel")
            print("  - bedrock:Retrieve")
            print("\nContact your AWS administrator to grant these permissions.")
            print("Attempting to continue anyway...")
            return True  # Continue anyway, might work for invoke
        else:
            print(f"\n⚠ WARNING: Could not verify Bedrock permissions: {error_code}")
            return True  # Continue anyway
    except Exception as e:
        print(f"\n⚠ WARNING: Could not check Bedrock permissions: {str(e)}")
        return True  # Continue anyway

# Configure Model - Claude Haiku 4.5 (global cross-region inference profile)
model = BedrockModel(
    model_id="us.anthropic.claude-3-5-haiku-20241022-v1:0",
    temperature=0.0,  # Factual, policy-only responses
)

# System Prompt
SYSTEM_PROMPT = f"""You are an Amazon Returns & Refunds assistant. Your role is to provide accurate, concise, policy-only answers about Amazon's return and refund policies.

When a customer asks about returns or refunds:
1. Extract the country from their query (US, UK, IN, etc.) - use ISO-2 country codes
2. Use the retrieve tool with parameters:
   - knowledge_base_id: {KB_ID}
   - query: the customer's question
   - metadata_filter: {{"country": "ISO-2 country code"}} (e.g., {{"country": "IN"}} for India)
3. Check the retrieved policy documents before answering
4. Provide a clear, concise answer based ONLY on the policy information retrieved
5. If the policy doesn't cover their specific case, say so clearly

Always be helpful, accurate, and policy-focused. Do not make up information."""

# Create Agent
agent = Agent(
    model=model,
    system_prompt=SYSTEM_PROMPT,
    tools=[retrieve, use_aws]
)

def main():
    """Main function to run the agent."""
    print("Amazon Returns & Refunds Assistant")
    print("=" * 50)
    
    # Validate AWS credentials before starting
    print("\nValidating AWS credentials...")
    if not validate_aws_credentials():
        print("\n❌ Cannot start agent without valid AWS credentials.")
        sys.exit(1)
    
    # Check Bedrock permissions
    print("\nChecking Bedrock permissions...")
    check_bedrock_permissions()
    
    print("\n" + "=" * 50)
    print("Ask me about Amazon's return and refund policies.")
    print("Type 'exit' or 'quit' to end the conversation.\n")
    
    while True:
        user_input = input("You: ").strip()
        
        if user_input.lower() in ['exit', 'quit']:
            print("Thank you for using Amazon Returns & Refunds Assistant!")
            break
        
        if not user_input:
            continue
        
        try:
            response = agent(user_input)
            print(f"\nAssistant: {response}\n")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ExpiredToken' or error_code == 'ExpiredTokenException':
                print("\n❌ ERROR: Your AWS session has expired")
                print("\nPlease refresh your credentials and try again:")
                print("  aws sso login --profile your-profile")
                print("  # or")
                print("  aws configure")
                break
            elif error_code == 'AccessDeniedException':
                print("\n❌ ERROR: Access denied to Bedrock")
                print("\nYou need the following IAM permissions:")
                print("  - bedrock:InvokeModel")
                print("  - bedrock:Retrieve")
                print("\nContact your AWS administrator.")
                break
            elif error_code == 'ResourceNotFoundException':
                print(f"\n❌ ERROR: Knowledge Base '{KB_ID}' not found")
                print("\nPlease update KB_ID in refund_agent.py with your Knowledge Base ID")
                print("Find it in: AWS Console > Bedrock > Knowledge Bases")
                break
            else:
                print(f"\n❌ ERROR: {error_code}")
                print(f"Message: {e.response['Error']['Message']}\n")
        except Exception as e:
            print(f"\n❌ ERROR: {str(e)}\n")

if __name__ == "__main__":
    main()
