---
title: "Summary"
weight: 70
---

In four hours, without writing code, you designed, deployed and operated two multi-agent applications on Amazon Bedrock AgentCore.

| | Trip Planner | Blog Post Studio |
|---|---|---|
| **Designed with** | Three prompts and the canvas | Six prompts and a tool from the Library |
| **Agents** | Six, running on Strands and LangGraph | Six, including an image agent and one on a dedicated runtime |
| **Tools** | Web search, an OpenAPI API, a Lambda function and a knowledge base | All of these, plus an MCP server and an API key held in a vault |
| **Human control** | Review gates, a branch rule, re-runs and a stop | Three review gates with per-agent revision |
| **Governance** | Sharing, versions and observability | Guardrails, a Cedar policy, evaluations, identity and memory |
| **Deployed with** | AWS CDK | Terraform |

## Key takeaways

1. **Describe it, then deploy it.** A conversation becomes a workflow, and one deploy turns it into a running application.
2. **One file is the whole system.** `workflow.json` is easy to review, version, share and move between accounts.
3. **People stay in control.** Review gates, revisions, re-runs and branch rules let you correct a run without starting over.
4. **Safety lives outside the model.** Cedar checks tool calls at the Gateway, Identity keeps secrets, and Guardrails check content.
5. **You can see everything.** Every prompt, tool call, decision and cost is recorded, and evaluations score the quality.
6. **Build once, reuse everywhere.** Library items work in any workflow, and agents can use any framework.

## Next steps

- Describe one of your own processes in [Try your own use case](/06-try-your-own).
- Deploy AgentExpress in your own account from [Get your Control Plane](/01-setup/01-deploy-console).
- Export a build and deploy it from your own CI/CD pipeline with AWS CDK or Terraform.
- Explore the code on [GitHub](https://github.com/praven80/AgentExpress/tree/UI).

## Resources

- [Amazon Bedrock AgentCore Developer Guide](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/what-is-bedrock-agentcore.html)
- [Strands Agents](https://strandsagents.com)
- [LangGraph](https://langchain-ai.github.io/langgraph/)

If you used your own account, remember to [clean up](/05-cleanup).
