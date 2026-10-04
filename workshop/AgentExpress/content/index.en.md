---
title: "Build Multi-Agent Workflows on Amazon Bedrock AgentCore with AgentExpress"
weight: 0
---

**Describe a multi-agent workflow in plain English, deploy it to your AWS account, and run it with a human in the loop, without writing code.**

AgentExpress turns a description of your business process or business requirements into a working multi-agent application on **Amazon Bedrock AgentCore**. It creates the agents, the orchestration, the tool connections, the guardrails and policies, the infrastructure and a web app for your users. In this 4-hour workshop you build two of these applications yourself.

In this workshop you work in both parts of AgentExpress:

- In the **Control Plane**, where you design, you describe a workflow to the AgentExpress Assistant, refine it on a canvas, share it with a colleague, and deploy it with AWS CDK or Terraform.
- In the **Execution Plane**, the workflow's own app, you run the workflow with a human approving each stage, re-run steps, and observe cost, prompts and tool calls.

## Why AgentExpress

Building a prototype agent is quick. Putting a team of agents in front of real users takes much longer, because you also need orchestration, human review, secure access to data, safety controls, secrets handling, memory, monitoring, quality measurement and a user interface. AgentExpress gives you each of these out of the box:

| What you need in production | What AgentExpress gives you |
|---|---|
| **Orchestration** of many agents | Sequential, parallel and branching steps, run on **AgentCore Runtime** |
| **Human in the loop** to check and correct the work | **Review gates** to approve, revise or deny, plus re-run from any step and stop |
| **Access** to your data and APIs | **AgentCore Gateway** for knowledge bases, web search, MCP servers, REST APIs and Lambda |
| **Control** over what agents may do | **AgentCore Policy** with Cedar rules on every tool call, and **Bedrock Guardrails** on content |
| **Secrets** that the model never sees | **AgentCore Identity** with a vault for API keys and per-agent credentials |
| **Memory** across steps and runs | **AgentCore Memory** to pause runs at gates and remember each user's preferences |
| **Visibility** into cost and behaviour | **AgentCore Observability** for every agent and tool call, with tokens, latency and cost |
| **Quality measurement** of agent output | **AgentCore Evaluations** with built-in judges and your own custom judges |
| **Optimization** over time | **Insights** across runs: failure patterns, user intents and execution summaries |
| **Infrastructure** and a user interface | **AWS CDK** or **Terraform** deploys, with a web app and sign-in for your users |

## How AgentExpress works

AgentExpress has two parts: a **Control Plane** where you design workflows, and an **Execution Plane** where each workflow runs.

![AgentExpress Control Plane and Execution Plane](/static/images/architecture-planes.png)

| | **Control Plane** | **Execution Plane** |
|---|---|---|
| **Purpose** | Design and manage workflows | Run workflows for your users |
| **How many** | One per AWS account | One for each deployed workflow |
| **What you do there** | **Design** by chat, **build** on a canvas, **reuse** items from the Library, **share** with colleagues, and **version and deploy** | **Run** the workflow, **review** at gates, **re-run** steps, **observe** cost and quality, and **chat** with your runs |

When you deploy a workflow from the Control Plane, it gets its own Execution Plane with its own web app and sign-in. When you change the workflow and deploy again, the same Execution Plane moves to the next version.

## What you will build

You build two complete workflows. The first teaches you how AgentExpress works from end to end. The second adds every AgentCore feature, so you see how to make a workflow safe, measurable and connected to your own systems.

### Lab 1: Trip Planner

:::alert{type="info" header="Lab 1 in numbers"}
You send **3** prompts. They produce **6** agents, **4** tools, **2** review gates and **1** branch rule. **1** deploy, about 7 minutes long, turns them into a running application in your account. You write **0** lines of code.
:::

A travel team needs travel briefs that are researched, follow the company travel policy, and fit the budget. Someone types a destination, the number of days and travellers, a budget and a home currency. Six agents do the rest.

![Trip Planner workflow](/static/images/trip-planner.png)

- **Intake** turns the request into a structured trip brief. If there is no destination, a **branch rule** ends the run straight away, so no time or tokens are wasted. A human reviews the brief before anything else runs.
- **Two researchers work in parallel.** The destination researcher searches the live web for sights, areas to stay, local tips and daily costs. The currency researcher calls the free Frankfurter API for today's exchange rate. A human reviews both results, and can send either one back with feedback.
- **Two agents work in sequence.** The itinerary writer drafts a day-by-day plan that follows the travel policy in a **knowledge base**. The budget checker calls a **Lambda function** to calculate the total cost and checks it against the budget and the policy.
- **The travel brief** agent combines everything into a short, friendly brief. If the trip is over budget or breaks the policy, it suggests two concrete fixes.

Along the way you design with the **AgentExpress Assistant**, refine on the **canvas**, mix **Strands** and **LangGraph** agents, publish tools to the **Library**, **share** the workflow with a colleague, **deploy** it, run it through every **review gate** control, read its **observability** data, and ship a **second version**.

### Lab 2: Blog Post Studio

:::alert{type="info" header="Lab 2 in numbers"}
You send **6** prompts and reuse **1** tool from the Library. They produce **6** agents, **6** tools of **5** types, **3** review gates, a guardrail, a Cedar policy, a custom evaluator, long-term memory and per-agent identities. **1** deploy with Terraform, about 7 minutes long, puts it all in your account. You write **0** lines of code.
:::

A marketing team needs blog posts that are researched, written in the brand's voice, illustrated and published. Someone types a topic and an audience. Six agents produce a finished post with a cover image, and a human approves each stage.

![Blog Post Studio workflow](/static/images/blog-post-studio.png)

- **The planner** writes the brief. A **guardrail** checks the request first, and blocks harmful content or requests for legal, medical or financial advice.
- **Two researchers work in parallel.** The fact researcher searches the web and checks AWS claims against the **AWS Knowledge MCP server**, on its own **dedicated runtime**. The SEO researcher finds keywords with web search and the **Datamuse API**. A **Cedar policy** refuses any search about a competitor.
- **The writer** drafts the post using the brand style guide in a **knowledge base**, and **remembers** each user's preferred tone across runs. **The illustrator** generates a cover image.
- **The editor** looks at the cover image to write alt text, checks the length and keywords with a **Lambda function**, and publishes the post through an API whose key is held in the **AgentCore Identity** vault. A custom **evaluation** scores how well the post follows the style guide.

This lab uses **every AgentCore feature**: Runtime, Gateway with five tool types, Memory, Identity, Policy, Guardrails, Observability and Evaluations. It also reuses the web search tool you published to the Library in Lab 1.

## Who this workshop is for

This workshop is for builders, solution architects, developers and technical product owners who want to develop multi-agent systems and deploy them into production on AWS. You do not need to write code; every step happens in your browser. Some familiarity with LLM agents and basic AWS concepts helps.

## Costs

At an AWS event, the account is provided and cleaned up for you. In your own account, expect a few US dollars for the whole workshop, because every service is serverless and the default model is Claude Haiku 4.5. The [Clean up](/05-cleanup) module removes everything you created.

::::expand{header="Is AgentExpress production ready?"}
The Execution Plane runs on the same managed services you would use in production: AgentCore, Amazon Bedrock and Guardrails, deployed as infrastructure as code. AgentExpress itself is an open-source reference implementation (MIT-0). Review its security notes and adapt it to your standards before you use it with production data, and run this workshop in a sandbox account.
::::
