# FAQ

Short answers to the questions people ask most about building, deploying, running, cost and access. Each answer links to the page with the detail. Use the [index of this guide](README.md), or search the repository, to find your question. For error messages and fixes, see [Troubleshooting](Troubleshooting.md).

# Building

### Do I need to write code?

No. Describe the workflow to the AgentExpress Assistant, or build it on the canvas. The control plane writes `workflow.json` and the agents' prompts for you. You write code only if you want to: a Lambda tool's handler, a code step's script, or your own agent in a repository. See [Build with the AgentExpress Assistant](AgentExpress_Assistant.md) and [Build manually on the canvas](Canvas.md).

### Can the AgentExpress Assistant deploy my build?

No. It drafts and edits the build, but it cannot deploy or publish. You deploy from the **Deployment** panel. See [Deploy a build](Deploying_a_Build.md).

### Can the AgentExpress Assistant set up a feature for me?

Mostly, yes. Ask in plain words and it sets agents, models, stages, gates, tools, guardrails, memory, Cedar policies, triggers, export, limits and the other build settings. It can't enter secrets, write test suites, publish, or do anything in the execution plane. Each feature page has an **Ask the Assistant** section, and [Ask the Assistant](Ask_the_Assistant.md) lists example requests for every feature.

### Why is there no Cedar policy switch on my agent?

Cedar policies apply at the Gateway, to every tool call, whichever agent makes it. To restrict a tool, write a policy on the **Policies** tab. Its **Policy** row then shows on every agent that uses that tool, on the canvas and on the run graph. See [Policies (Cedar)](Policies_and_Cedar.md).

### What is the difference between a Cedar policy, a guardrail and an interceptor?

- A **Cedar policy** allows or denies a tool call at the Gateway, based on the tool and its arguments.
- A **guardrail** screens text going into and out of an agent's model.
- An **interceptor** is a Lambda on every tool call that can audit, block, redact or change it.

See [Guardrails](Guardrails.md) and [Interceptors](Interceptors.md).

### How do I reuse a tool, skill or guardrail in another build?

Publish it to the library from its tab, then pick it in the other build. A deployed build picks up a change on its next deploy. See [Library](Library.md).

### Can I edit a build outside the control plane?

Yes. **Export → Bundle** downloads the build as one file. Edit it yourself or with a coding agent such as Kiro (the `agentexpress-author` skill), then **Project → Import a file…** brings it back. See [Export, edit and import](Export_and_Import.md) and [Code bundle and a coding agent](Code_Bundle_and_Coding_Agent.md).

### Can a step run without a model?

Yes. A generic step (task step) calls one tool, runs a script, reads a document with Textract, or writes a file. See [Stages and steps](Stages_and_Steps.md).

### Can I use a model that is not on Bedrock?

Yes, through a LiteLLM proxy you run, or a harness agent on another provider. See [Models, routing and LiteLLM](Models.md) and [Runtimes](Runtimes.md).

# Deploying

### How long does a deploy take?

The Deploy dialog says 15 to 30 minutes. It runs in the control plane's deploy project, with a 2-hour limit, and keeps going if you close the page. The **Deployment** panel shows progress and the log. See [Deploy a build](Deploying_a_Build.md).

### Should I pick CDK or Terraform?

Either. Both create the same resources from the same `workflow.json`. Pick the one your team already uses. Once deployed, the tool is locked until you destroy the build.

### Can I deploy into a different AWS account?

Yes. Connect the account on **AWS accounts**: the control plane gives you a CloudFormation template that creates a deploy role only this control plane can assume. Then pick it under **Deploy to**. See [Set up AWS accounts](AWS_Accounts.md).

### Can I deploy from my own laptop or pipeline?

Yes. Export the bundle, apply it with `scaffold.py apply <bundle> --exact` in a checkout of the framework, and deploy with CDK or Terraform. See [Deploy from your laptop](Deploy_from_Your_Laptop.md).

### Do I have to redeploy after changing a prompt?

Not if the execution plane has live config turned on. An admin changes an agent's prompt or model settings in the running execution plane and activates it. Tools, gates, guardrails, memory and Cedar always need a redeploy. See [Live config](Live_Config.md).

### How do I sign in to my execution plane?

Click **Open app** on the build page to open its execution plane. With built-in sign-in, use the email after **Sign in as** and the temporary password from **Show the temporary password**. With Okta, Entra ID or Auth0, sign in with that provider. See [Open the execution plane](Opening_the_App.md).

### How do I remove everything a build deployed?

Click **Destroy** on the Deployment panel. See [Clean up](Clean_Up.md).

# Running

### Why are Approve, Revise and Deny greyed out?

You are not in a group granted the `decision` action, or the run is someone else's. Ask an admin to add you to the group, then sign out and back in. See [Human in the loop](Human_in_the_Loop.md).

### Can I close the browser while a run waits at a gate?

Yes. The run is saved in a checkpoint. It is still waiting when you come back, unless the gate has a timeout. See [Resume, restart and re-run](Resume_Restart_Rerun.md).

### Why did my run end early?

The run page says why at the top: a branch rule sent it to END, a reviewer denied it, a guardrail blocked a step, or an agent failed. **Open the Timeline** shows every line. See [The run page and its tabs](Run_Page.md).

### How do I fix one step without starting over?

Click the step, open **Actions** in the step panel, write **Feedback**, and click **Re-run**. That step and everything after it run again. See [Resume, restart and re-run](Resume_Restart_Rerun.md).

### How do I give agents files to read?

When an agent reads run files, **Start run** shows **Attach files**. You can also name files in S3 in the request, within the locations the form lists. See [Start a run: prompt, file or trigger](Starting_a_Run.md).

### Can a run start without anyone typing?

Yes. Triggers start runs from a webhook, a schedule, an EventBridge event, an S3 upload or an SQS message. See [Triggers](Triggers.md).

### How do I show a run to a colleague?

Click **Share** on the run page and create a read-only link. They must sign in to the same execution plane. See [Share a run](Sharing_a_Run.md).

# Cost

### What does a run cost?

Open the run on the Observability page, **Run detail** tab: it shows the run's cost, tokens and calls, split by agent and by call. See [One run in detail](Run_Traces.md).

### Why does the cost not match my AWS bill?

The figures are estimates for planning, from list prices. Private pricing, Region and caching discounts differ. Token counts are exact. See [Cost and spend limits](Cost_and_Limits.md).

### How do I see one deployment's AWS charges?

Activate the `agentexpress:app` tag as a cost allocation tag in Billing. Bedrock model tokens also need an application inference profile, a Bedrock project or IAM principal attribution. See [Cost and spend limits](Cost_and_Limits.md).

### How do I cap spending?

Set `orchestrator.limits`: a daily spend per user, runs per hour per user, and a monthly spend for the whole execution plane. The limits fail open, so they are a cost guard, not a security control. See [Cost and spend limits](Cost_and_Limits.md).

### How do I estimate a monthly cost?

On the Observability **Overview** tab, the **Cost projection** takes your average run cost and a planned number of runs per day, and gives a 30-day figure. See [Observability](Observability.md).

# Access

### Who can see my builds and runs?

Only you, unless you share them or work in a team. Admins can read them but not act on them. See [Sign-in and security](Security.md).

### What is the difference between groups and teams?

Sign-in groups decide which actions a person may take. Teams decide who shares which builds in the control plane, as owner, editor or viewer. See [Teams and workspaces](Teams.md).

### How do I become an admin?

Someone with access to the backend adds you to the admin group, for example with `aws cognito-idp admin-add-user-to-group`, and you sign in again. See [Users, groups and RBAC](Users_Groups_and_RBAC.md).

### Can the Run Assistant do things I am not allowed to do?

No. It is held to the same permissions as the buttons, and sees only your runs. See [Chat with the Run Assistant](Run_Assistant.md).

### Why can I not see Analytics, Memory records or Live config?

They are admin pages. Live config also needs `orchestrator.liveConfig: true`, and Triggers needs at least one trigger. See [The execution plane](Execution_Plane.md).

# Related pages

- [Concepts and glossary](Concepts_and_Glossary.md): terms used across the manual.
- [Troubleshooting](Troubleshooting.md): fixes for common problems.
- [What's new](Whats_New.md): the newest features.
