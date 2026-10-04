---
title: "3. Under the hood"
weight: 43
---

**Time:** 10 minutes | **Where:** Control Plane, while the deploy runs

The Assistant saved each feature as a named building block in the build. Open each tab under **Build manually** as you read the table below.

| Feature | Tab | What it does |
|---|---|---|
| **Guardrails** | Guardrails | Bedrock Guardrails check the planner's input and the writer's and editor's output. A block stops the run. Guardrails check text; the image model filters images itself. |
| **Cedar policy** | Policies | AgentCore Gateway checks every tool call against the policy. `ENFORCE` mode blocks calls; `LOG_ONLY` only records them. By default, agents can reach only the tools they declare. In tool mode *model*, a refused call is reported to the agent, which carries on; in tool mode *direct*, a refused call fails the run, so nothing proceeds without the data it needed. |
| **Evaluations** | Evals | An LLM judge scores the real output, either automatically or when you choose **Evaluate**. |
| **Memory** | Memory | Checkpoints let a run wait at a gate. Long-term memory recalls each user's preferences, about a minute after a run. |
| **Identity** | Identity | Each tool-using agent has its own credentials. The Gateway sends the `cmsApi` key as an `X-API-Key` header. |
| **Dedicated runtime** | Design, then the fact researcher | The fact researcher runs in its own AgentCore Runtime with its own IAM role, and still appears in one connected trace. |
| **Observability** | Not a tab | Every model call, tool call, guardrail check, policy decision and evaluation is traced and costed. |

When the deploy finishes, continue with [Test A](/04-content-studio/04-test-a).
