---
title: "Multi-agent architecture"
weight: 20
---

**Time:** 15 minutes

This module is a short tour of what AgentExpress builds for you, so each step in the labs makes sense.

## One file describes the whole workflow

Everything you design is saved in one file, `workflow.json`. It describes the tools, the agents, the steps, the review gates, the AgentCore features and the access rules. When you deploy, AWS CDK or Terraform reads this file and creates every resource the workflow needs.

You never edit the file by hand in this workshop. The AgentExpress Assistant and the canvas write it for you. Here is a trimmed example:

:::code{language=json showCopyAction=false}
{
  "tools": {
    "websearch": {
      "type": "websearch",
      "description": "AgentCore Web Search, a managed Gateway connector",
      "maxResults": 10
    }
  },
  "agents": {
    "web_search": {
      "runtime": "dedicated",
      "tool": "websearch",
      "agentcore": { "guardrails": { "output": true }, "policy": { "enabled": true } }
    }
  },
  "steps": [
    { "agent": "intake", "hitl": true,
      "branch": { "when": [{ "field": "objective", "exists": false, "goto": "END" }] } },
    { "parallel": ["web_search", "knowledge_research"], "hitl": true },
    { "sequence": ["analysis", "recommendation"], "hitl": true }
  ]
}
:::

## Control Plane and Execution Plane

![AgentExpress Control Plane and Execution Plane](/static/images/architecture-planes.png)

**The Control Plane is where you design.** There is one per AWS account. It holds your workflows and their versions, the shared Library, the sharing settings and the activity log. It runs the AgentExpress Assistant that drafts workflows, and a CodeBuild project that deploys them with AWS CDK or Terraform. It never runs a workflow itself.

**The Execution Plane is where a workflow runs.** Each deployed workflow gets its own, in its own stack, with its own web app and sign-in. It contains the AgentCore resources that run the agents, the tool connections, the knowledge base and the observability data. Your users start runs, review the work at gates and see what each agent did.

**How they connect.** A deploy from the Control Plane creates or updates the Execution Plane for that workflow. One Control Plane can deploy many Execution Planes side by side, and each one moves forward version by version as you improve the workflow.

## What happens during a run

1. A user signs in to the Execution Plane and sends a request.
2. **AgentCore Runtime** starts the orchestrator, a LangGraph graph built from `workflow.json`.
3. Each agent reasons with an **Amazon Bedrock** model. **Guardrails** check its input and output, and **Memory** recalls what it learned about this user.
4. Agents call their tools through **AgentCore Gateway**. **Cedar policies** allow or refuse each call, and **Identity** adds any secret the call needs.
5. At a **review gate**, the run pauses until a human approves, revises or denies the work. Its state is saved in Memory, so the reviewer can take their time.
6. A **branch rule** can read an agent's output and skip ahead or end the run.
7. **Observability** traces every call, and **Evaluations** score the quality of the results.

## AgentCore services in AgentExpress

| Service | What it does in AgentExpress |
|---|---|
| **Runtime** | Runs the orchestrator and its agents. An agent can also have its own dedicated runtime with its own IAM role. |
| **Gateway** | Gives agents one secure endpoint for every tool: knowledge bases, web search, MCP servers, REST APIs and Lambda functions. |
| **Policy** | Checks every tool call against Cedar rules, outside the model's control. |
| **Identity** | Stores API keys in a vault and gives each agent its own credentials. |
| **Memory** | Lets runs pause at review gates, and remembers each user's preferences across runs. |
| **Observability** | Traces tokens, latency and cost for every agent and tool call. |
| **Evaluations** | Scores agent output with built-in or custom LLM judges. |
| **Optimization** | Analyses many runs together to surface failure patterns, user intents and execution summaries. |
| **Bedrock and Guardrails** | Provide the models, and check agent input and output for safety. |

## Orchestration patterns

| Pattern | What it does |
|---|---|
| **Sequence** | Agents run one after another, and each sees the approved output of earlier steps. |
| **Parallel** | Agents run at the same time and meet at one review gate. |
| **Review gate** | A human approves, revises or denies. At a parallel gate, they decide for each agent. |
| **Branch** | A rule on an agent's output skips ahead or ends the run. |
| **Re-run** | After a run, you re-run any step and everything after it, with feedback. |
| **A2A** | A step can delegate to a remote agent. You do not use this today. |

## Any agent framework

Each agent can reason with **no framework**, with **Strands Agents** or with **LangGraph**. Every model call and tool call still goes through AgentExpress, so guardrails, memory, observability and cost tracking work the same way for all of them.
