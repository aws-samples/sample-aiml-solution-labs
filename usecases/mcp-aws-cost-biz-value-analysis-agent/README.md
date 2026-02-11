# AWS TCO & Business Value Analysis Agent

An AI-powered agent for calculating Total Cost of Ownership (TCO), Return on Investment (ROI), and business value metrics for AWS services, with a focus on Amazon Bedrock and Bedrock AgentCore.

## Overview

This agent helps AWS sales teams and customers analyze costs and business value for AI/ML workloads on AWS. It combines:

- **Pricing Search**: Retrieves real-time AWS pricing from a Bedrock Knowledge Base
- **AWS Documentation**: Searches AWS documentation via MCP server
- **Cost Calculators**: Calculates Bedrock, AgentCore, and business value metrics
- **What-If Analysis**: Performs sensitivity analysis for cost optimization

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    AWS TCO & BVA Analyst Agent                      │
│                     (Strands Agents Framework)                      │
├─────────────────────────────────────────────────────────────────────┤
│                              Tools                                  │
├──────────────┬──────────────┬──────────────┬───────────────────────┤
│   Pricing    │   Bedrock    │  AgentCore   │        BVA            │
│   Search     │  Calculator  │  Calculator  │     Calculator        │
│  (KB RAG)    │              │              │                       │
├──────────────┴──────────────┴──────────────┴───────────────────────┤
│                     AWS Knowledge MCP Server                        │
│              (https://knowledge-mcp.global.api.aws)                 │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Amazon Bedrock AgentCore                         │
│                      (Serverless Runtime)                           │
└─────────────────────────────────────────────────────────────────────┘
```

## Components

| File | Description |
|------|-------------|
| `aws_tco_bva_analyst.py` | Main agent with all tools integrated |
| `aws_tco_bva_analyst_agentcore.py` | MCP endpoint for AgentCore deployment |
| `calculator_bedrock.py` | Bedrock cost calculator with what-if analysis |
| `calculator_agentcore.py` | AgentCore cost calculator with what-if analysis |
| `calculator_bva.py` | Business value calculator (ROI, cost savings) |
| `pricing_search_assistant.py` | Pricing search via Bedrock Knowledge Base |
| `deployment_helper.py` | CLI for AgentCore deployment |
| `add_kb_policy_to_role.py` | IAM policy helper for KB access |

## Prerequisites

- Python 3.11+
- AWS CLI configured with appropriate credentials
- Amazon Bedrock model access enabled
- Bedrock Knowledge Base with pricing data (optional)
- AgentCore CLI installed (for deployment)

## Installation

```bash
cd usecases/mcp-aws-cost-biz-value-analysis-agent/agents

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Local Testing

### Interactive Mode

```bash
# Set environment variables
export AWS_REGION=us-east-1
export STRANDS_KNOWLEDGE_BASE_ID=YOUR_KNOWLEDGE_BASE_ID  # Optional
export MODEL_ID=us.anthropic.claude-haiku-4-5-20251001-v1:0  # Optional

# Run interactive agent
python aws_tco_bva_analyst.py
```

### Example Queries

```
You: Calculate monthly cost for Bedrock Claude Haiku with 50,000 questions/month, 
     3000 input tokens and 1000 output tokens per question.

You: What's the ROI of implementing an AI agent that reduces support ticket 
     resolution time from 15 minutes to 3 minutes? We have 10,000 tickets/month 
     and labor cost is $75/hour.

You: Compare costs between Claude Sonnet and Claude Haiku for a document 
     processing use case with 100,000 documents/month.
```

## AgentCore Deployment

### 1. Configure IAM Role

Create an IAM role with the required permissions:

```bash
# If you have a Knowledge Base, add KB access policy
python add_kb_policy_to_role.py \
    --role-arn arn:aws:iam::ACCOUNT_ID:role/YOUR_ROLE_NAME \
    --kb-id YOUR_KB_ID \
    --region us-east-1
```

Required IAM permissions:
- `bedrock:InvokeModel` - For LLM inference
- `bedrock:Retrieve` - For Knowledge Base retrieval (if using KB)
- `bedrock:RetrieveAndGenerate` - For RAG operations (if using KB)

### 2. Set Environment Variables

```bash
export AWS_REGION=us-east-1
export STRANDS_KNOWLEDGE_BASE_ID=YOUR_KNOWLEDGE_BASE_ID
export MODEL_ID=us.anthropic.claude-haiku-4-5-20251001-v1:0  # Optional
export AGENTCORE_EXECUTION_ROLE_ARN=arn:aws:iam::ACCOUNT_ID:role/YOUR_ROLE_NAME
```

### 3. Deploy to AgentCore

```bash
# Configure and launch
python deployment_helper.py

# Or step by step:
python deployment_helper.py --configure-only
python deployment_helper.py --launch-only

# Check status
python deployment_helper.py --status

# Test invocation
python deployment_helper.py --invoke "Calculate Bedrock costs for 10K requests/month"

# Destroy deployment
python deployment_helper.py --destroy
```

### 4. Local Docker Testing

```bash
python deployment_helper.py --local
```

## Inbound Authentication Configuration

When deploying to AgentCore, configure inbound authentication to secure your agent endpoint.

### IAM Authentication (Recommended)

Edit `.bedrock_agentcore.yaml`:

```yaml
agents:
  aws_tco_bva_analyst:
    entrypoint: aws_tco_bva_analyst_agentcore.py
    runtime: PYTHON_3_12
    protocol: MCP
    inbound_auth:
      type: IAM
      allowed_principals:
        - arn:aws:iam::ACCOUNT_ID:role/AllowedCallerRole
        - arn:aws:iam::ACCOUNT_ID:user/AllowedUser
```

### API Key Authentication

```yaml
agents:
  aws_tco_bva_analyst:
    entrypoint: aws_tco_bva_analyst_agentcore.py
    runtime: PYTHON_3_12
    protocol: MCP
    inbound_auth:
      type: API_KEY
```

### No Authentication (Development Only)

```yaml
agents:
  aws_tco_bva_analyst:
    entrypoint: aws_tco_bva_analyst_agentcore.py
    runtime: PYTHON_3_12
    protocol: MCP
    inbound_auth:
      type: NONE
```

## Calculator Tools

### Bedrock Calculator

Calculates monthly AWS Bedrock costs including:
- Input/output token costs per model
- System prompt and conversation history tokens
- Vector database retrieval tokens
- Tool invocation tokens (for agentic use cases)

### AgentCore Calculator

Calculates monthly AgentCore costs including:
- Runtime (vCPU and memory hours)
- Browser tool usage
- Code interpreter usage
- Gateway API calls
- Memory (short-term and long-term)

### BVA Calculator

Calculates business value metrics:
- Cost savings (labor, time, operational)
- Revenue growth impact
- Customer churn reduction
- Risk mitigation value
- ROI and payback period

## Pricing Data Setup

The agent uses a Bedrock Knowledge Base to retrieve AWS pricing information. Follow these steps to set up the pricing data.

### Option 1: Use Existing Knowledge Base

If you already have a Bedrock Knowledge Base with pricing data:

```bash
export STRANDS_KNOWLEDGE_BASE_ID=YOUR_KNOWLEDGE_BASE_ID
```

### Option 2: Create Knowledge Base with Pricing Data

This option creates a new Knowledge Base with OpenSearch Serverless and populates it with AWS pricing data.

#### Step 1: Deploy CloudFormation Stack

The CFN template creates:
- S3 bucket for pricing documents
- OpenSearch Serverless collection (vector store)
- Bedrock Knowledge Base
- Data source configuration
- Required IAM roles

```bash
cd usecases/mcp-aws-cost-biz-value-analysis-agent

# Deploy the stack
aws cloudformation deploy \
    --template-file cfn/aws-tco-biz-value-analysis.yaml \
    --stack-name aws-tco-biz-value-analysis \
    --capabilities CAPABILITY_IAM \
    --region us-east-1

# Get the outputs
aws cloudformation describe-stacks \
    --stack-name aws-tco-biz-value-analysis \
    --query 'Stacks[0].Outputs' \
    --output table
```

Note the following outputs:
- `KnowledgeBaseId` - Use this as your `STRANDS_KNOWLEDGE_BASE_ID`
- `PricingDocsBucketName` - S3 bucket for pricing data
- `DataSourceId` - Data source ID for syncing

#### Step 2: Download AWS Pricing Data

The pricing scraper downloads pricing information from the AWS Pricing API and saves it as text files organized by service and region.

```bash
cd pricing-doc-scraper

# Install dependencies (if not already installed)
pip install requests

# List available services
python price_doc_scraper.py --list

# Download all pricing data (default: US regions only)
python price_doc_scraper.py --output ./pricing_data

# Or download specific services only
python price_doc_scraper.py --service AmazonBedrock --service AmazonSageMaker

# Include all regions (larger dataset)
python price_doc_scraper.py --all-regions
```

Output structure:
```
pricing_data/
├── AmazonBedrock/
│   ├── us-east-1/
│   │   ├── InferenceToken_ABC12345.txt
│   │   └── ...
│   └── us-west-2/
│       └── ...
├── AmazonEC2/
│   └── ...
└── ...
```

#### Step 3: Upload Pricing Data to S3

Sync the downloaded pricing data to the S3 bucket created by CloudFormation:

```bash
# Get bucket name from CloudFormation outputs
BUCKET_NAME=$(aws cloudformation describe-stacks \
    --stack-name aws-tco-biz-value-analysis \
    --query 'Stacks[0].Outputs[?OutputKey==`PricingDocsBucketName`].OutputValue' \
    --output text)

echo "Uploading to bucket: $BUCKET_NAME"

# Sync pricing data to S3
aws s3 sync ./pricing_data s3://$BUCKET_NAME/ --delete

# Verify upload
aws s3 ls s3://$BUCKET_NAME/ --recursive --summarize
```

#### Step 4: Sync Knowledge Base Data Source

After uploading data to S3, trigger a sync to index the documents in the Knowledge Base:

```bash
# Get Knowledge Base ID and Data Source ID
STRANDS_KNOWLEDGE_BASE_ID=$(aws cloudformation describe-stacks \
    --stack-name aws-tco-biz-value-analysis \
    --query 'Stacks[0].Outputs[?OutputKey==`KnowledgeBaseId`].OutputValue' \
    --output text)

DATA_SOURCE_ID=$(aws cloudformation describe-stacks \
    --stack-name aws-tco-biz-value-analysis \
    --query 'Stacks[0].Outputs[?OutputKey==`DataSourceId`].OutputValue' \
    --output text)

echo "Knowledge Base ID: $STRANDS_KNOWLEDGE_BASE_ID"
echo "Data Source ID: $DATA_SOURCE_ID"

# Start ingestion job
aws bedrock-agent start-ingestion-job \
    --knowledge-base-id $STRANDS_KNOWLEDGE_BASE_ID \
    --data-source-id $DATA_SOURCE_ID

# Check ingestion status
aws bedrock-agent list-ingestion-jobs \
    --knowledge-base-id $STRANDS_KNOWLEDGE_BASE_ID \
    --data-source-id $DATA_SOURCE_ID \
    --query 'ingestionJobSummaries[0]'
```

Wait for the ingestion job to complete (status: `COMPLETE`). This may take several minutes depending on the amount of data.

#### Step 5: Configure Agent with Knowledge Base

```bash
# Export the Knowledge Base ID for the agent
export STRANDS_KNOWLEDGE_BASE_ID=$STRANDS_KNOWLEDGE_BASE_ID
export AWS_REGION=us-east-1

# Test the agent
cd ../agents
python aws_tco_bva_analyst.py
```

### Updating Pricing Data

To refresh pricing data (e.g., monthly):

```bash
cd pricing-doc-scraper

# Re-download pricing data
python price_doc_scraper.py --output ./pricing_data

# Sync to S3
aws s3 sync ./pricing_data s3://$BUCKET_NAME/ --delete

# Re-sync Knowledge Base
aws bedrock-agent start-ingestion-job \
    --knowledge-base-id $STRANDS_KNOWLEDGE_BASE_ID \
    --data-source-id $DATA_SOURCE_ID
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `AWS_REGION` | AWS region | `us-east-1` |
| `STRANDS_KNOWLEDGE_BASE_ID` | Bedrock Knowledge Base ID | - |
| `MODEL_ID` | Bedrock model ID | `us.anthropic.claude-haiku-4-5-20251001-v1:0` |
| `AGENTCORE_EXECUTION_ROLE_ARN` | IAM role for AgentCore | - |

## Troubleshooting

### Knowledge Base Not Found

Ensure `STRANDS_KNOWLEDGE_BASE_ID` environment variable is set and the IAM role has `bedrock:Retrieve` permission.

### Model Access Denied

Enable model access in the Amazon Bedrock console for your region.

### MCP Connection Failed

The AWS Knowledge MCP server requires network access to `https://knowledge-mcp.global.api.aws`.

### AgentCore Deployment Failed

Check CloudWatch logs:
```bash
aws logs tail /aws/bedrock-agentcore/runtimes/aws_tco_bva_analyst --follow
```

## License

MIT License - See LICENSE file in repository root.
