"""Test Amazon Returns & Refunds Agent

Tests the refund agent with a sample query about India's refrigerator return policy.
"""

import sys
from botocore.exceptions import ClientError
from refund_agent import agent, validate_aws_credentials, check_bedrock_permissions

# Test configuration
test_prompt = "What is the refund policy for a refrigerator I brought 2 years ago in India?"

print("\n" + "="*70)
print("Testing Amazon Returns & Refunds Agent")
print("="*70)

# Validate credentials before testing
print("\nValidating AWS credentials...")
if not validate_aws_credentials():
    print("\n❌ Test aborted: Invalid AWS credentials")
    print("Please fix your credentials and try again.")
    sys.exit(1)

print("\nChecking Bedrock permissions...")
check_bedrock_permissions()

print("\n" + "="*70)
print(f"Test Prompt: {test_prompt}\n")

try:
    response = agent(test_prompt)
    print(f"Response:\n{response}\n")
    print("="*70)
    print("✓ Test completed successfully")
    print("="*70 + "\n")
except ClientError as e:
    error_code = e.response['Error']['Code']
    print(f"\n❌ Test failed with AWS error: {error_code}")
    print(f"Message: {e.response['Error']['Message']}")
    
    if error_code in ['ExpiredToken', 'ExpiredTokenException']:
        print("\nYour AWS session has expired. Refresh your credentials:")
        print("  aws sso login --profile your-profile")
    elif error_code == 'AccessDeniedException':
        print("\nYou don't have permission to use Bedrock.")
        print("Required permissions: bedrock:InvokeModel, bedrock:Retrieve")
    elif error_code == 'ResourceNotFoundException':
        print("\nKnowledge Base not found. Update KB_ID in refund_agent.py")
    
    print("\n" + "="*70 + "\n")
    sys.exit(1)
except Exception as e:
    print(f"\n❌ Test failed with error: {str(e)}")
    print("="*70 + "\n")
    sys.exit(1)
