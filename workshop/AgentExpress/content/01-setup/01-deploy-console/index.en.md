---
title: "Get your Control Plane"
weight: 11
---

Choose the tab that matches how you are running the workshop.

::::::tabs{variant="container"}

:::::tab{id="event" label="At an AWS event"}

Your Control Plane is already deployed, and your two users are already created. You do not need the AWS Management Console for this workshop.

On the event dashboard, open **Event outputs** and copy these values:

| Output | What it is |
|---|---|
| **ConsoleUrl** | The URL of your AgentExpress Control Plane |
| **UserAlice** | `alice@example.com`, the user you work as |
| **UserBob** | `bob@example.com`, the colleague you share a workflow with |
| **WorkshopPassword** | The sign-in password for both users |

Next, [sign in](/01-setup/02-sign-in).

:::::

:::::tab{id="own" label="In your own account"}

You deploy the Control Plane with one CloudFormation template. The template runs AWS CDK for you inside AWS CodeBuild, so you do not need any tools on your laptop. It also creates both workshop users and checks that the Bedrock models the labs use are available.

:::alert{type="warning" header="Use a sandbox account"}
The template's CodeBuild role has `AdministratorAccess`, because the Control Plane creates IAM roles and AgentCore resources.
:::

1. Download the template:

   :button[Download agentexpress-console.yaml]{href="/static/agentexpress-console.yaml" action=download}

2. Sign in to the AWS Management Console, switch to **US East (N. Virginia) us-east-1**, and open **CloudFormation → Create stack → With new resources (standard)**.
3. Upload `agentexpress-console.yaml`. Name the stack `agentexpress-workshop` and keep the default parameters.
4. Acknowledge that the stack creates IAM resources, then choose **Submit**.
5. Wait about 10 minutes for the stack to reach `CREATE_COMPLETE`.
6. On the stack's **Outputs** tab, copy **ConsoleUrl**, **UserAlice**, **UserBob** and **WorkshopPassword**.

:::alert{type="info" header="First time using Claude in this account?"}
Amazon Bedrock may ask you to submit Anthropic use-case details once. Open **Amazon Bedrock → Model catalog**, open a Claude model and follow the prompt.
:::

Next, [sign in](/01-setup/02-sign-in).

:::::

::::::
