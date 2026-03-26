# Technology Stack

## IMPORTANT!!!!
Always lookup AWS documents for the most updated info on the technologies and coding best practices.

## Languages & Frameworks

- **Python 3.11+**: Primary language for all implementations
- **AWS CDK v2**: Infrastructure as Code using Python
- **FastMCP**: Framework for building MCP servers
- **Strands Agents**: AI agent framework (version >= 1.12.0)
- **Jupyter**: Interactive notebooks for labs

## Key Libraries

### AI/ML Libraries
- `strands-agents` >= 1.12.0
- `strands-agents-tools` >= 0.2.11
- `fastmcp`
- `mcp` (Model Context Protocol)

### AWS SDKs
- `boto3` - AWS SDK for Python
- `botocore` - Low-level AWS service access
- `aws-cdk-lib` - CDK constructs library

### Data Processing
- `pandas` - Data manipulation
- `tiktoken` - Token counting for embeddings
- `psycopg2-binary` - PostgreSQL database adapter

### Utilities
- `pyyaml` - YAML parsing
- `retrying` - Retry logic
- `requests` - HTTP client
- `requests-aws4auth` - AWS signature authentication

## Build & Deployment

### CDK Projects (blog/)

```bash
# Install dependencies
pip install -r requirements.txt

# Bootstrap CDK (first time only)
cdk bootstrap

# Synthesize CloudFormation
cdk synth

# Deploy infrastructure
cdk deploy

# Destroy infrastructure
cdk destroy
```

### MCP Servers (usecases/)

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally
python <server_name>.py

# Deploy to AgentCore
agentcore configure --entrypoint <server_name>.py --name <agent_name> ...
agentcore launch --agent <agent_name>
```

### Jupyter Notebooks (labs/)

```bash
# Install dependencies
pip install -r requirements.txt

# Start Jupyter
jupyter lab

# Or run specific notebook
jupyter notebook <notebook_name>.ipynb
```

## Development Tools

- **Docker**: Required for Lambda layer packaging and local builds
- **AWS CLI**: Must be configured with appropriate credentials
- **uv/uvx**: Python package manager for MCP servers
- **npm**: Required for CDK CLI installation

## Common Commands

```bash
# AWS authentication check
aws sts get-caller-identity

# View CloudWatch logs
aws logs tail /aws/bedrock-agentcore/runtimes/<agent_name> --follow

# Check AgentCore status
agentcore status --agent <agent_name>

# List MCP tools
python test_mcp_client.py
```

## Environment Setup

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure AWS credentials
aws configure
```
