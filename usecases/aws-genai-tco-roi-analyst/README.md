# AWS GenAI TCO & ROI Analyst

A full-stack chat agent that analyzes business use cases, estimates Amazon Bedrock and AgentCore costs, advises on Bedrock LLM capacity planning, and provides TCO and ROI analysis.

## Architecture

The solution has four components:

- **AnalystAgent** — Strands-based AI agent deployed on Amazon Bedrock AgentCore with calculator tools and MCP integration
- **CloudFormation Stack** — Provisions S3, OpenSearch Serverless, Bedrock Knowledge Base, DynamoDB, CloudFront, Cognito Identity Pool, scraper Lambda, and EventBridge schedule
- **Chatbot UI** — React app with Cloudscape Design, Cognito auth, session management, and chart rendering
- **Data Scrapers** — Weekly Lambda-triggered scrapers for Bedrock pricing (4 services, all regions) and quota data (all regions)

## Prerequisites

- AWS account with Bedrock model access enabled
- Cognito User Pool (created externally — the stack references it via parameters)
- Python 3.11+
- Node.js 18+
- AWS CLI configured with appropriate credentials
- AgentCore CLI (`pip install bedrock-agentcore-starter-toolkit`)

## Deployment

### 1. Deploy CloudFormation Stack

```bash
aws cloudformation deploy \
  --template-file cfn/aws-tco-biz-value-analysis.yaml \
  --stack-name aws-tco-bva \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    CognitoUserPoolId=<your-user-pool-id> \
    CognitoClientId=<your-client-id> \
    CognitoDomain=<your-cognito-domain>
```

Note the stack outputs — you'll need `KnowledgeBaseId`, `IdentityPoolId`, `CloudFrontURL`, and `PricingDocsBucketName`.

### 2. Run Initial Data Scrape

You can trigger the scraper Lambda manually, or run the scrapers locally:

```bash
# Pricing scraper (4 Bedrock services, all regions)
cd doc_scrapers/pricing-doc-scraper
pip install requests
python price_doc_scraper.py --output ./pricing_data

# Quota scraper (all regions)
cd ../quota-doc-scraper
pip install boto3
python quota_doc_scraper.py --output ./quota_data

# Sync to S3
aws s3 sync ./pricing_data s3://<pricing-docs-bucket>/pricing_data/
aws s3 sync ./quota_data s3://<pricing-docs-bucket>/quota_data/
```

Then trigger a Knowledge Base sync:

```bash
aws bedrock-agent start-ingestion-job \
  --knowledge-base-id <kb-id> \
  --data-source-id <data-source-id>
```

The EventBridge schedule runs this automatically every Sunday at 1 AM PST.

### 3. Deploy the Agent

```bash
cd AnalystAgent/app/AnalystAgent
pip install -r requirements.txt  # or use pyproject.toml with uv

# Set environment variables
export STRANDS_KNOWLEDGE_BASE_ID=<kb-id-from-stack-outputs>
export MODEL_ID=us.anthropic.claude-sonnet-4-5-20250929-v1:0

# Deploy to AgentCore
cd ../../
agentcore deploy
```

### 4. Build and Deploy Chatbot UI

```bash
cd chatbot-ui

# Copy .env.example to .env and fill in values from stack outputs
cp .env.example .env
# Edit .env with your values

npm install
npm run build

# Deploy to S3 + CloudFront
aws s3 sync build/ s3://<frontend-bucket>/
aws cloudfront create-invalidation --distribution-id <dist-id> --paths "/*"
```

## Configuration

### CloudFormation Parameters

| Parameter | Description |
|-----------|-------------|
| `CognitoUserPoolId` | Your Cognito User Pool ID |
| `CognitoClientId` | Your Cognito App Client ID |
| `CognitoDomain` | Your Cognito domain prefix |
| `EmbeddingModelId` | Embedding model (default: `amazon.titan-embed-text-v2:0`) |
| `AgentRuntimeName` | AgentCore runtime name (default: `aws_tco_biz_value_analyst`) |

### Agent Environment Variables

| Variable | Description |
|----------|-------------|
| `STRANDS_KNOWLEDGE_BASE_ID` | Bedrock Knowledge Base ID from stack outputs |
| `MODEL_ID` | Bedrock model ID (default: Claude Sonnet 4.5) |
| `AWS_REGION` | AWS region (default: `us-east-1`) |

### Chatbot UI Environment Variables

See `chatbot-ui/.env.example` for the full list with descriptions.

## Project Structure

```
aws-genai-tco-roi-analyst/
├── AnalystAgent/                    # Strands Agent on AgentCore
│   ├── agentcore/                   # AgentCore deployment config
│   └── app/AnalystAgent/            # Agent source code
│       ├── main.py                  # AgentCore entrypoint
│       ├── system_prompt.py         # System prompt + Pydantic schemas
│       ├── calculator_bedrock.py    # Bedrock cost calculator
│       ├── calculator_agentcore.py  # AgentCore cost calculator
│       ├── calculator_bva.py        # Business value calculator
│       ├── calculator_capacity_planning.py  # Capacity planner
│       ├── search_pricing_info.py   # KB pricing search tool
│       ├── search_bedrock_quota.py  # KB quota search tool
│       ├── model/load.py            # Bedrock model loader
│       └── mcp_client/client.py     # AWS Knowledge MCP client
├── cfn/                             # CloudFormation template
│   └── aws-tco-biz-value-analysis.yaml
├── chatbot-ui/                      # React frontend
│   ├── server/index.js              # Express proxy (local dev)
│   └── src/                         # React components
├── doc_scrapers/                    # Data collection scripts
│   ├── pricing-doc-scraper/         # AWS pricing API scraper
│   └── quota-doc-scraper/           # Service Quotas API scraper
└── README.md
```

## Agent Capabilities

- **Bedrock Cost Estimation** — Multi-model cost calculation with vector DB, tools, and history token accounting
- **AgentCore Cost Estimation** — Runtime, browser, code interpreter, gateway, and memory component costs
- **Business Value Analysis** — ROI, payback period, cost savings, revenue growth, churn reduction
- **Capacity Planning** — RPM/TPM analysis, provisioned throughput comparison, alternative model recommendations
- **What-If Analysis** — 1D and 2D sensitivity analysis for cost parameters
- **Live Pricing Data** — Retrieves current pricing from Knowledge Base (updated weekly)
- **AWS Documentation** — MCP integration with AWS Knowledge server
