---
title: "Clean up"
weight: 50
---

**Time:** 10 minutes (the destroys run in the background)

:::alert{type="info" header="At an AWS event"}
Your workshop account is removed after the event. Do **Step 1** to see how a workflow is destroyed, and skip the rest.
:::

## Step 1: destroy the Execution Planes

Sign in to the Control Plane as Alice, then:

1. Open **Trip Planner**. In the **Deployment** panel, choose **Destroy** and confirm. AgentExpress removes it with the tool it was deployed with, AWS CDK.
2. Do the same for **Blog Post Studio**. It is removed with Terraform, and its state in the Control Plane is removed too.
3. When each build shows *Not deployed*, you can delete it with **Project → Delete this build**, and delete your tools from the Library.

Destroy every Execution Plane before Step 2, because the Control Plane is what destroys them.

## Step 2: destroy the Control Plane (your own account)

Open **CloudShell** in us-east-1 and run:

:::code{language=bash showCopyAction=true}
aws codebuild start-build --project-name agentexpress-workshop-console \
  --environment-variables-override name=ACTION,value=destroy,type=PLAINTEXT \
  --query 'build.id' --output text
:::

When the build succeeds, after about 10 minutes, delete the template's stack:

:::code{language=bash showCopyAction=true}
aws cloudformation delete-stack --stack-name agentexpress-workshop
:::

## Step 3: optional leftovers

These are left in place on purpose, because other stacks in the account may use them.

| Leftover | How to remove it |
|---|---|
| CDK bootstrap stack `CDKToolkit` | Delete it in CloudFormation, but only if nothing else in the account uses CDK. |
| CloudWatch Transaction Search, an account setting | Turn it off in CloudWatch, under **Settings → X-Ray traces**. |
| Log groups named `/aws/bedrock-agentcore/runtimes/…` | Delete them in CloudWatch Logs. |
