# AgentCore Demo

A demonstration project for Amazon Bedrock AgentCore.

## Overview

This project shows how to build, deploy, and run an agent on Amazon Bedrock AgentCore.

> Work in progress. See the roadmap below.

## Structure

```
agentcore_demo/
├── src/            # Agent source code
├── deployment/     # Deployment scripts / IaC
├── test/           # Tests
├── requirements.txt
└── README.md
```

## Prerequisites

- Python 3.10+
- AWS credentials configured with access to Amazon Bedrock / AgentCore
- An AWS region where Bedrock AgentCore is available

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Roadmap

- [ ] Define the agent's purpose and tools
- [ ] Implement the agent
- [ ] Add deployment
- [ ] Add tests
