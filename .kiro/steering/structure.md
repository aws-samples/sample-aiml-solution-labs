# Project Structure

## Top-Level Organization

```
├── blog/                    # Production-ready implementations with full infrastructure
├── labs/                    # Jupyter notebooks for experimental use cases
├── usecases/                # Focused implementations of specific patterns
├── README.md                # Repository overview
├── CONTRIBUTING.md          # Contribution guidelines
├── CODE_OF_CONDUCT.md       # Community standards
└── LICENSE                  # MIT License
```

## Blog Projects (Production Infrastructure)

Each blog project is a complete, deployable solution with CDK infrastructure:

```
blog/<project-name>/
├── app.py                   # CDK application entry point
├── cdk.json                 # CDK configuration
├── cdk.context.json         # Environment-specific context (account, region)
├── requirements.txt         # Python dependencies
├── README.md                # Project documentation
├── <project_module>/        # Main CDK stack module
│   ├── __init__.py
│   ├── <project>_stack.py   # Main stack definition
│   ├── networking/          # VPC, security groups
│   ├── database/            # RDS, Aurora constructs
│   ├── processing/          # Lambda functions
│   ├── storage/             # S3 constructs
│   ├── authentication/      # Cognito, secrets
│   ├── messaging/           # SQS, SNS constructs
│   └── layers/              # Lambda layers
└── validation/              # Testing scripts and sample data
```

### CDK Stack Patterns

- **Modular Constructs**: Each AWS service is encapsulated in its own construct class
- **Separation of Concerns**: Networking, database, processing, storage, auth are separate modules
- **Reusable Components**: Constructs can be imported and reused across stacks
- **Clear Dependencies**: Constructs pass references (VPC, security groups, etc.) explicitly

## Labs (Jupyter Notebooks)

Interactive notebooks for learning and experimentation:

```
labs/<lab-name>/
├── <notebook>.ipynb         # Main Jupyter notebook
├── requirements.txt         # Python dependencies
├── README.md                # Lab instructions
├── utils/                   # Helper utilities
└── download/                # Sample data (gitignored)
```

## Use Cases (Focused Implementations)

Standalone implementations demonstrating specific patterns:

```
usecases/<usecase-name>/
├── <main_file>.py           # Primary implementation (MCP server, agent, etc.)
├── requirements.txt         # Python dependencies
├── README.md                # Usage documentation
├── QUICK_START.md           # Quick start guide (optional)
├── <setup_script>.py        # Deployment/setup automation
├── test_<component>.py      # Testing utilities
├── .bedrock_agentcore.yaml  # AgentCore configuration (if applicable)
└── utils/                   # Helper modules
```

## Kiro Specifications

Projects developed with Kiro Spec Mode include structured documentation:

```
.kiro/specs/<project-name>/
├── requirements.md          # EARS-formatted requirements
├── design.md                # Architecture and implementation design
└── tasks.md                 # Implementation task breakdown
```

## Common Patterns

### Lambda Function Structure

```python
# Lambda handler file
import os
import json
import boto3

def lambda_handler(event, context):
    """
    Lambda function handler with clear docstring.
    
    Args:
        event: Lambda event payload
        context: Lambda context object
        
    Returns:
        Response dictionary with statusCode and body
    """
    # Implementation
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Success'})
    }
```

### MCP Server Structure

```python
# MCP server file
from fastmcp import FastMCP

mcp = FastMCP("server-name", host="0.0.0.0", stateless_http=True)

@mcp.tool()
def tool_name(param: str) -> dict:
    """
    Tool description for AI assistants.
    
    Args:
        param: Parameter description
        
    Returns:
        Dictionary with results
    """
    return {"result": "value"}

if __name__ == "__main__":
    mcp.run(transport="streamable-http")
```

### CDK Construct Pattern

```python
# CDK construct file
from aws_cdk import Stack
from constructs import Construct

class MyConstruct(Construct):
    """
    Construct description.
    
    Creates and configures AWS resources for specific functionality.
    """
    
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id)
        
        # Resource creation
        self._resource = create_resource()
    
    def get_resource(self):
        """Getter method for resource reference."""
        return self._resource
```

## File Naming Conventions

- **Python modules**: `snake_case.py`
- **CDK stacks**: `<project>_stack.py`
- **CDK constructs**: `<resource>_construct.py`
- **Lambda handlers**: `<function>_lambda.py`
- **MCP servers**: `<name>_mcp_server.py`
- **Test files**: `test_<component>.py`
- **Setup scripts**: `setup_<component>.py`

## Documentation Standards

- Every project has a `README.md` with overview, prerequisites, installation, and usage
- Complex deployments include separate guides (e.g., `QUICK_START.md`, `AGENTCORE_SETUP.md`)
- Python files include module-level docstrings
- Functions include docstrings with Args, Returns, and Examples sections
- CDK stacks include CfnOutput for important resource identifiers
