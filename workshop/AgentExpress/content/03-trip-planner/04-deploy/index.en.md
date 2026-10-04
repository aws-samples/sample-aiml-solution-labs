---
title: "4. Deploy"
weight: 34
---

**Time:** 10 minutes | **Where:** Control Plane | **User:** Alice

A deploy saves the workflow as a numbered **version** and creates its **Execution Plane**. That includes the AgentCore resources, the tools, the knowledge base and a web app with its own sign-in, all in your AWS account.

## Deploy version 1

1. Check that the header shows **Valid**.
2. In the **Deployment** panel, choose **Deploy** and set these values:

   | Field | Value |
   |---|---|
   | **Deploy to** | This console's account |
   | **Region** | `us-east-1` |
   | **Deploy with** | **AWS CDK**. Terraform is the other option, and you use it in Lab 2. |

3. Choose **Deploy**. The panel shows *Deploying version 1 with AWS CDK* and the current phase. The deploy takes about 7 minutes, and it keeps running if you close the browser.

Behind the scenes, the Control Plane's CodeBuild project applies the frozen version to a fresh copy of AgentExpress and runs `cdk deploy`. The stack it creates holds the AgentCore Runtime with your six agents, the AgentCore Gateway with one target per tool, the Cedar policy engine, the knowledge base with your travel policy, AgentCore Memory, the budget Lambda function and the web app with its own sign-in.

:::alert{type="info" header="While you wait"}
- Follow the progress in the **Log** of the Deployment panel.
- **You own everything you build.** Choose **Export → Bundle (workflow, prompts and code)** in the build header. The file contains the workflow, every prompt and the Lambda code, and you can deploy it from your own pipeline with AWS CDK or Terraform.
:::

## Open the Execution Plane

1. When the panel shows **Version 1 deployed with AWS CDK**, choose **Show the temporary password** and copy it.
2. Choose **Open app**. Sign in as `alice@example.com` with the temporary password, and set a new password.
3. You are now in **🧭 AgentExpress - Trip Planner**. This is the app your users would see. It has **Runs**, **Observability**, the **Run Assistant** and its own **Activity** log. It has no Build view or Library, because it runs this one workflow only.

Because the workflow is shared with Bob, he gets his own sign-in to this Execution Plane, and his runs are private to him.

## Versions

Every successful deploy saves the workflow as the next version: version 1, version 2, and so on. Each run records the version it ran with. Old runs keep the outputs they produced, and if a later version adds, removes or reorders agents, old runs still show the graph they ran with. You ship version 2 at the end of this lab.

:::alert{type="success" header="Milestone"}
You went from a plain-English description to a six-agent application running on Amazon Bedrock AgentCore in your own account, in under an hour, without writing code.
:::
