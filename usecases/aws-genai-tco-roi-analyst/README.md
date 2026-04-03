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

Note the stack outputs — you'll need `KnowledgeBaseId`, `IdentityPoolId`, `CloudFrontURL`, `PricingDocsBucketName`, `RelevancyGuardrailId`, and `SteeringLogsBucketName`.

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
cd Agent

# Configure with the execution role from stack outputs
agentcore configure \
  --create \
  -n AnalystAgent \
  -e app/AnalystAgent/main.py \
  -dt direct_code_deploy \
  -rt PYTHON_3_12 \
  -r us-west-2 \
  -p HTTP \
  --execution-role <AgentCoreRuntimeRoleArn-from-stack-outputs> \
  --non-interactive

# Deploy with environment variables from stack outputs
agentcore deploy -a AnalystAgent --auto-update-on-conflict \
  --env STRANDS_KNOWLEDGE_BASE_ID=<KnowledgeBaseId> \
  --env AWS_REGION=us-west-2 \
  --env STEERING_MODE=passive \
  --env STEERING_GUARDRAIL_ID=<RelevancyGuardrailId> \
  --env STEERING_GUARDRAIL_VERSION=DRAFT \
  --env STEERING_LOG_BUCKET=<SteeringLogsBucketName>
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

### Relevancy Steering Configuration

The agent includes a relevancy steering handler that scores every KB query and response using an LLM judge and Bedrock Guardrails contextual grounding. Scores are logged to S3 for analysis.

| Variable | Default | Description |
|----------|---------|-------------|
| `STEERING_MODE` | `passive` | `passive` = log scores only, `active` = retry on low scores |
| `STEERING_RETRY_THRESHOLD` | `0.6` | Score below this triggers retry (0.0-1.0) |
| `STEERING_MAX_RETRIES` | `2` | Max retries before accepting the response |
| `STEERING_GUARDRAIL_ID` | | Bedrock Guardrail ID from stack outputs |
| `STEERING_GUARDRAIL_VERSION` | `DRAFT` | Guardrail version |
| `STEERING_LOG_BUCKET` | | S3 bucket for score logs (from stack outputs) |
| `STEERING_JUDGE_MODEL_ID` | `us.anthropic.claude-haiku-4-5-20251001-v1:0` | Model for LLM judge evaluations |

**How it works:**
1. Before each KB tool call, an LLM judge checks if the query matches the user's intent
2. After the final response, both an LLM judge and Bedrock Guardrails score the response for relevancy and grounding
3. All scores are logged to S3 as JSON under `scores/{date}/{session_id}/`
4. In `active` mode, low scores trigger automatic retries (up to `STEERING_MAX_RETRIES`)

### Score Log Structure

Logs are stored in S3 at:
```
s3://{STEERING_LOG_BUCKET}/scores/{YYYY}/{MM}/{DD}/{session_id}/{invocation_id}_{check_type}_{HHMMSS}.json
```

Each agent invocation produces two files sharing the same `invocation_id`:
- `{invocation_id}_before_kb_call_*.json` — scored before the KB lookup runs
- `{invocation_id}_after_response_*.json` — scored after the agent generates its answer

If the agent calls multiple KB tools in one invocation, there will be one `before_kb_call` file per KB tool call.

**Log record fields:**

| Field | Description |
|-------|-------------|
| `timestamp` | UTC ISO timestamp |
| `session_id` | AgentCore session ID (groups all turns in a conversation) |
| `invocation_id` | 8-char UUID (groups all files from one agent invocation) |
| `check_type` | `before_kb_call` or `after_response` |
| `tool_name` | KB tool being called (only for `before_kb_call`) |
| `user_query` | What the user asked |
| `tool_query` | What the agent sent to the KB (only for `before_kb_call`) |
| `llm_judge_score` | 0.0-1.0 from the LLM judge |
| `guardrail_grounding` | 0.0-1.0 from Bedrock Guardrails (only for `after_response`) |
| `guardrail_relevance` | 0.0-1.0 from Bedrock Guardrails (only for `after_response`) |
| `mode` | `passive` or `active` |
| `retry_count` | Number of retries so far in this invocation |
| `details` | LLM judge reasoning for the score |

**What to look for when analyzing:**

- Low `llm_judge_score` on `before_kb_call` → agent is querying the KB with wrong/incomplete terms
- Low `guardrail_grounding` on `after_response` → agent fabricated data not in the KB results
- High `guardrail_relevance` + low `guardrail_grounding` → agent answered the right question with wrong data
- `llm_judge_score` and `guardrail_grounding` disagreeing → one check catches issues the other misses (this is the key experiment insight)
- `retry_count > 0` (in active mode) → the handler triggered a retry

### Running Evaluations

The project includes a Strands Evals-based test suite with 50 test cases across user levels (L100 vague → L400 expert + edge cases). It runs each case through the agent, collects Strands Evals scores (Faithfulness, Output quality) alongside steering scores (LLM judge, Guardrails grounding/relevance), and links them via session_id.

```bash
pip install strands-agents-evals

# Run all 50 cases
python eval_runner.py --output results.json

# Run a specific level
python eval_runner.py --level L300 --cases 5

# Run edge cases only
python eval_runner.py --level edge
```

Results are saved as JSON with both eval scores and S3 steering scores per case. Use this to validate that the steering handler catches issues standard evaluators miss, and to tune `STEERING_RETRY_THRESHOLD` before switching to active mode.

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
│       ├── relevancy_steering_handler.py  # Steering hook: LLM judge + Guardrails scoring
│       ├── relevancy_logger.py      # S3 score logger
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
