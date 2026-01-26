#!/usr/bin/env python3
"""
Add Bedrock Knowledge Base access policy to an IAM role.

This script creates and attaches an inline IAM policy that grants
permissions to retrieve from a specified Bedrock Knowledge Base.

Usage:
    python add_kb_policy_to_role.py --role-arn <ROLE_ARN> --kb-id <KB_ID>
    python add_kb_policy_to_role.py --role-arn <ROLE_ARN> --kb-id <KB_ID> --region us-west-2
"""

import argparse
import json
import boto3
from botocore.exceptions import ClientError


def get_role_name_from_arn(role_arn: str) -> str:
    """Extract role name from ARN."""
    # ARN format: arn:aws:iam::ACCOUNT_ID:role/ROLE_NAME
    return role_arn.split("/")[-1]


def get_account_id_from_arn(role_arn: str) -> str:
    """Extract account ID from ARN."""
    # ARN format: arn:aws:iam::ACCOUNT_ID:role/ROLE_NAME
    return role_arn.split(":")[4]


def create_kb_policy_document(kb_id: str, account_id: str, region: str) -> dict:
    """
    Create IAM policy document for Bedrock Knowledge Base access.
    
    Args:
        kb_id: Bedrock Knowledge Base ID
        account_id: AWS account ID
        region: AWS region
        
    Returns:
        IAM policy document as dict
    """
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "BedrockKnowledgeBaseRetrieve",
                "Effect": "Allow",
                "Action": [
                    "bedrock:Retrieve",
                    "bedrock:RetrieveAndGenerate"
                ],
                "Resource": f"arn:aws:bedrock:{region}:{account_id}:knowledge-base/{kb_id}"
            },
            {
                "Sid": "BedrockFoundationModelAccess",
                "Effect": "Allow",
                "Action": [
                    "bedrock:InvokeModel",
                    "bedrock:InvokeModelWithResponseStream"
                ],
                "Resource": "arn:aws:bedrock:*::foundation-model/*"
            }
        ]
    }


def add_kb_policy_to_role(role_arn: str, kb_id: str, region: str = "us-east-1") -> bool:
    """
    Add Bedrock Knowledge Base access policy to an IAM role.
    
    Args:
        role_arn: ARN of the IAM role
        kb_id: Bedrock Knowledge Base ID
        region: AWS region for the Knowledge Base
        
    Returns:
        True if successful, False otherwise
    """
    iam_client = boto3.client("iam")
    
    role_name = get_role_name_from_arn(role_arn)
    account_id = get_account_id_from_arn(role_arn)
    policy_name = f"BedrockKB-{kb_id}-Access"
    
    policy_document = create_kb_policy_document(kb_id, account_id, region)
    
    print(f"Role Name: {role_name}")
    print(f"Account ID: {account_id}")
    print(f"Knowledge Base ID: {kb_id}")
    print(f"Region: {region}")
    print(f"Policy Name: {policy_name}")
    print("\nPolicy Document:")
    print(json.dumps(policy_document, indent=2))
    
    try:
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document)
        )
        print(f"\n✓ Successfully added policy '{policy_name}' to role '{role_name}'")
        return True
        
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        error_msg = e.response["Error"]["Message"]
        print(f"\n✗ Failed to add policy: {error_code} - {error_msg}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Add Bedrock Knowledge Base access policy to an IAM role"
    )
    parser.add_argument(
        "--role-arn",
        required=True,
        help="ARN of the IAM role"
    )
    parser.add_argument(
        "--kb-id",
        required=True,
        help="Bedrock Knowledge Base ID"
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="AWS region for the Knowledge Base (default: us-east-1)"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("ADDING BEDROCK KNOWLEDGE BASE POLICY TO IAM ROLE")
    print("=" * 60)
    
    success = add_kb_policy_to_role(
        role_arn=args.role_arn,
        kb_id=args.kb_id,
        region=args.region
    )
    
    if not success:
        exit(1)


if __name__ == "__main__":
    main()
