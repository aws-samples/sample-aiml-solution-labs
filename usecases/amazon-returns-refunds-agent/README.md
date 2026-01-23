# Amazon Returns & Refunds Assistant

A Strands AI agent that provides accurate, policy-based answers for Amazon returns and refunds using Amazon Bedrock Knowledge Base with country-specific policies.

## Overview

This use case demonstrates how to build an AI assistant that:
- Retrieves country-specific return and refund policies from a Bedrock Knowledge Base
- Uses metadata filtering to ensure accurate, region-specific responses
- Provides factual, policy-only answers without hallucination
- Supports both interactive and programmatic usage
- Includes built-in AWS credential validation and helpful error messages

The agent uses Claude Haiku 4.5 for fast, cost-effective responses and integrates with Amazon Bedrock Knowledge Base for reliable policy retrieval.

### Key Features

- **Automatic Credential Validation**: Validates AWS credentials and Bedrock permissions on startup
- **Helpful Error Messages**: Provides specific instructions for common issues (expired credentials, missing permissions, etc.)
- **Runtime Error Handling**: Detects and handles errors during execution with actionable guidance
- **Country-Specific Filtering**: Uses metadata to retrieve region-specific policies
- **Interactive & Programmatic**: Works as both a CLI tool and Python library

## Architecture

### Components

1. **Model**: Claude Haiku 4.5 (cross-region inference profile)
   - Model ID: `us.anthropic.claude-3-5-haiku-20241022-v1:0`
   - Temperature: 0.0 (factual responses only)
   - Fast and cost-effective for policy queries

2. **Tools**:
   - `retrieve`: Bedrock Knowledge Base integration for policy retrieval
   - `use_aws`: AWS service interactions

3. **System Prompt**: Guides the agent to:
   - Extract country codes from user queries
   - Use metadata filtering for country-specific policies
   - Provide concise, policy-based answers only
   - Avoid making up information

### How It Works

```
User Query → Agent extracts country code → Retrieve tool with metadata filter
                                                    ↓
                                          Bedrock Knowledge Base
                                                    ↓
                                    Country-specific policy documents
                                                    ↓
                                          Agent generates response
```

The agent automatically:
1. Identifies the country from the user's question
2. Converts it to ISO-2 country code (e.g., "India" → "IN")
3. Queries the Knowledge Base with country metadata filter
4. Returns policy-based answers from retrieved documents

## Prerequisites

### AWS Services
- **Amazon Bedrock**: Access to Claude models
- **Bedrock Knowledge Base**: Configured with return/refund policies
  - Must include country metadata field for filtering
  - Example metadata structure: `{"country": "US"}`, `{"country": "IN"}`, etc.

### AWS Permissions
Your AWS credentials need permissions for:
- `bedrock:InvokeModel` - To call Claude
- `bedrock:Retrieve` - To query Knowledge Base

### Python Environment
- Python 3.11 or higher
- AWS credentials configured (via AWS CLI, environment variables, or IAM role)

### Python Packages
```bash
pip install -r requirements.txt
```

Required packages:
- `strands` - AI agent framework
- `strands-tools` - Bedrock integration tools
- `boto3` - AWS SDK

## Setup

### 1. Configure Bedrock Knowledge Base

You need to create and configure your own Bedrock Knowledge Base:

1. **Create Knowledge Base** in Amazon Bedrock console
2. **Upload policy documents** with country-specific return/refund policies
3. **Add metadata** to each document with country field:
   ```json
   {
     "country": "US"
   }
   ```
4. **Note your Knowledge Base ID** (format: `XXXXXXXXXXXX`)

### 2. Update Configuration

Edit `refund_agent.py` and replace the Knowledge Base ID:

```python
KB_ID = "YOUR_KNOWLEDGE_BASE_ID"  # Replace with your KB ID
```

### 3. Configure AWS Credentials

Ensure AWS credentials are configured:

```bash
# Option 1: AWS CLI
aws configure

# Option 2: Environment variables
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1

# Option 3: Use IAM role (if running on EC2/Lambda)
```

### 4. Install Dependencies

```bash
cd usecases/amazon-returns-refunds-agent
pip install -r requirements.txt
```

## Usage

### Interactive Mode

Run the agent in interactive mode for conversational queries:

```bash
python refund_agent.py
```

The agent will automatically validate your AWS credentials and Bedrock permissions before starting:

```
Amazon Returns & Refunds Assistant
==================================================

Validating AWS credentials...
✓ AWS credentials validated
  Account: 123456789012
  User/Role: your-user

Checking Bedrock permissions...
✓ Bedrock permissions validated

==================================================
Ask me about Amazon's return and refund policies.
Type 'exit' or 'quit' to end the conversation.
```

Example interaction:
```
You: What is the refund policy for a refrigerator I brought 2 years ago in India?

Assistant: Based on Amazon India's return policy, refrigerators and other 
large appliances have a 10-day return window from the date of delivery...
```

### Programmatic Usage

Use the agent in your Python code:

```python
from refund_agent import agent

# Single query
response = agent("What is the refund policy for electronics in the US?")
print(response)

# Multiple queries
queries = [
    "Can I return a book after 30 days in the UK?",
    "What items are non-returnable in India?",
    "How long does it take to get a refund in the US?"
]

for query in queries:
    response = agent(query)
    print(f"Q: {query}")
    print(f"A: {response}\n")
```

Or import the components directly:

```python
from strands import Agent
from strands.models import BedrockModel
from strands_tools import retrieve, use_aws

# Create your own agent instance
agent = Agent(
    model=BedrockModel(
        model_id="us.anthropic.claude-3-5-haiku-20241022-v1:0",
        temperature=0.0
    ),
    tools=[retrieve, use_aws],
    system_prompt="Your custom system prompt..."
)
```

### Example Queries

The agent handles various return and refund questions:

```python
# Country-specific policies
"What is the return window for electronics in the US?"
"Can I return clothing without tags in the UK?"
"What is the refund policy for furniture in India?"

# Product-specific questions
"Can I return a refrigerator after 2 years in India?"
"What is the return policy for books in the US?"
"Are cosmetics returnable in the UK?"

# Process questions
"How long does it take to get a refund in the US?"
"What condition must items be in for returns in India?"
"Do I need the original packaging to return items in the UK?"
```

## Configuration

### Knowledge Base ID

The agent requires a Bedrock Knowledge Base ID:

```python
KB_ID = "EK2IHAXS8Q"  # Replace with your Knowledge Base ID
```

To find your Knowledge Base ID:
1. Go to Amazon Bedrock console
2. Navigate to Knowledge Bases
3. Select your knowledge base
4. Copy the Knowledge Base ID from the details page

### Model Configuration

The agent uses Claude Haiku 4.5 with specific settings:

```python
from strands import Agent
from strands.models import BedrockModel
from strands_tools import retrieve, use_aws

model = BedrockModel(
    model_id="us.anthropic.claude-3-5-haiku-20241022-v1:0",
    temperature=0.0,  # Factual responses only
)
```


You can modify:
- `model_id`: Use different Claude models (Sonnet, Opus)
- `temperature`: Adjust response creativity (keep at 0.0 for factual policies)

### Metadata Filtering

The agent uses country-based metadata filtering:

```python
metadata_filter = {"country": "IN"}  # For India
metadata_filter = {"country": "US"}  # For United States
metadata_filter = {"country": "UK"}  # For United Kingdom
```

Ensure your Knowledge Base documents include the `country` metadata field.

## Testing

### Quick Test

Run the included test script:

```bash
python test_refund_agent.py
```

This tests the agent with a sample query about India's refrigerator return policy.

### Root-Level Tests

Convenience test scripts are available at the repository root:

```bash
# Test Kindle eBook return policy (10 days old)
python test_kindle_10days.py

# Test Kindle eBook return query with AWS profile configuration
python query_kindle_return.py

# Test Kindle Book refund with ETA query for India
python test_refund_query.py
```

These scripts demonstrate programmatic usage with different product and timeframe scenarios. The `query_kindle_return.py` script also shows how to configure AWS profiles via environment variables.

### Manual Testing

Test with various queries to verify:
1. Country extraction works correctly
2. Metadata filtering returns country-specific policies
3. Responses are accurate and policy-based
4. Agent handles edge cases (unknown countries, ambiguous queries)

Example test queries:
```bash
python refund_agent.py

# Test country extraction
You: What's the return policy in Germany?

# Test product-specific policies
You: Can I return electronics in the US?

# Test edge cases
You: What if I don't mention a country?
```

## Troubleshooting

The agent includes built-in validation and helpful error messages for common issues. When you run the agent, it automatically validates your AWS credentials and Bedrock permissions before starting.

### Startup Validation

The agent performs these checks on startup:
1. **AWS Credentials**: Validates credentials are configured and not expired
2. **Bedrock Permissions**: Verifies access to Bedrock services

If validation fails, you'll see detailed instructions on how to fix the issue.

### Common Issues

#### 1. No AWS Credentials Found
```
❌ ERROR: No AWS credentials found
```

The agent will display multiple options to configure credentials:

**Option 1: AWS CLI (Recommended)**
```bash
aws configure
# Enter when prompted:
#   AWS Access Key ID: YOUR_ACCESS_KEY
#   AWS Secret Access Key: YOUR_SECRET_KEY
#   Default region: us-east-1
#   Default output format: json
```

**Option 2: Environment Variables**
```bash
# Linux/Mac
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1

# Windows (PowerShell)
$env:AWS_ACCESS_KEY_ID="your_access_key"
$env:AWS_SECRET_ACCESS_KEY="your_secret_key"
$env:AWS_DEFAULT_REGION="us-east-1"
```

**Option 3: AWS SSO**
```bash
aws sso login --profile your-profile
export AWS_PROFILE=your-profile
```

#### 2. AWS Credentials Expired
```
❌ ERROR: Your AWS session has expired
```

**This is the most common runtime issue.** AWS temporary credentials expire after a period of time.

The agent detects expired sessions during runtime and provides specific instructions:

**If using AWS SSO:**
```bash
# Refresh your SSO session
aws sso login --profile your-profile

# Verify it worked
aws sts get-caller-identity --profile your-profile
```

**If using temporary credentials (assumed role):**
```bash
# Re-assume the role
aws sts assume-role \
  --role-arn arn:aws:iam::ACCOUNT_ID:role/ROLE_NAME \
  --role-session-name my-session
```

**If using access keys:**
```bash
# Your keys may have been rotated - reconfigure
aws configure
```

**Quick credential check:**
```bash
# This command will tell you if your credentials are valid
aws sts get-caller-identity
```

#### 3. Invalid AWS Credentials
```
❌ ERROR: Invalid AWS credentials
```

**Solution**: 
- Verify credentials in AWS IAM console
- Generate new access keys if needed
- Run: `aws configure`

#### 4. Access Denied to Bedrock
```
❌ ERROR: Access denied to Bedrock
```

**Solution**: You need the following IAM permissions:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel",
        "bedrock:Retrieve"
      ],
      "Resource": "*"
    }
  ]
}
```

Contact your AWS administrator to grant these permissions.

#### 5. Knowledge Base Not Found
```
❌ ERROR: Knowledge Base 'EK2IHAXS8Q' not found
```

**Solution**: Update `KB_ID` in `refund_agent.py` with your Knowledge Base ID:
1. Go to AWS Console > Bedrock > Knowledge Bases
2. Copy your Knowledge Base ID
3. Update the `KB_ID` constant in the code

#### 6. No Results Returned
```
Assistant: I don't have information about that policy.
```

**Possible causes**:
- Knowledge Base doesn't have documents for that country
- Metadata filtering is too restrictive
- Documents missing country metadata field

**Solution**: 
- Verify documents exist for the requested country
- Check document metadata includes `country` field
- Test without metadata filter to verify documents exist

### Debug Mode

Enable debug logging to troubleshoot issues:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

from refund_agent import agent
response = agent("Your query here")
```

## Cost Considerations

### Bedrock Pricing
- **Claude Haiku 4.5**: ~$0.25 per 1M input tokens, ~$1.25 per 1M output tokens
- **Knowledge Base**: ~$0.10 per 1,000 queries

### Example Cost Calculation
For 10,000 queries per month:
- Average 1,000 input tokens + 500 output tokens per query
- Input: 10M tokens × $0.25 = $2.50
- Output: 5M tokens × $1.25 = $6.25
- KB queries: 10,000 × $0.0001 = $1.00
- **Total**: ~$9.75/month

## Next Steps

### Enhancements
1. **Multi-language support**: Add language detection and translation
2. **Conversation history**: Implement memory for follow-up questions
3. **Analytics**: Track common queries and policy gaps
4. **Web interface**: Build a Streamlit or Gradio UI
5. **Deployment**: Deploy to Lambda or ECS for production use

### Related Use Cases
- Customer service chatbots
- Policy Q&A systems
- Compliance assistants
- Knowledge base search interfaces

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues or questions:
1. Check the Troubleshooting section above
2. Review Strands documentation: https://github.com/aws/strands-agents
3. Open an issue in this repository
