# Facilitator Guide: Build Multi-Agent Workflows on Amazon Bedrock AgentCore with AgentExpress

---

## Workshop Overview

**What Participants Will Learn:** This workshop teaches participants how to design, deploy and operate multi-agent workflows on Amazon Bedrock AgentCore with AgentExpress. By the end, they can build a multi-agent workflow by conversation and on a canvas, deploy it as its own app, and govern it with review gates, guardrails, Cedar policy, evaluations, memory and identity.

**Learning Objectives:**
- Explain the components of a multi-agent system on AgentCore (Runtime, Gateway, Memory, Identity, Policy, Guardrails, Evaluations, Observability)
- Design a workflow with sequential and parallel stages, review gates and a conditional branch, mixing Strands and LangGraph agents
- Share builds and library items with another user, and deploy a build as a versioned stack
- Operate runs: approve, revise, deny, re-run from a step, stop, and read cost and traces
- Apply guardrails, Cedar policy, evaluations, long-term memory and per-agent identity to a workflow

**Target Audience:** Builders, solution architects and developers with basic AWS and LLM-agent familiarity. No coding required.

**Duration:** 4 hours.

---

## Prerequisites

### Facilitator Preparation
- Dry-run the full workshop in a test event (budget about 4 hours, including both deploys)
- Upload `assets/agentexpress-source.zip` (the framework's UI branch) to the workshop assets bucket; the GitHub repository is private, so the template reads the source from there
- Confirm the event's CloudFormation stack (`static/agentexpress-console.yaml`) reaches `CREATE_COMPLETE`; it runs `cdk deploy` in CodeBuild and takes about 10 minutes
- Read the Troubleshooting section below

### Service Requirements & Quotas
- Region: **us-east-1** for everything, plus **us-west-2** for Stable Diffusion 3.5 Large (Lab 2 cover image)
- Bedrock model access: Claude Haiku 4.5 (agents), Claude Sonnet 5.5 (AgentExpress Assistant), Titan Text Embeddings V2 (knowledge base), Stable Diffusion 3.5 Large in us-west-2. The template invokes each model once; check the CodeBuild log for `WARNING: cannot invoke`. Anthropic models may need the use-case form submitted once per account
- Each participant deploys 1 console stack and 2 build stacks (each with an AgentCore Runtime; Lab 2 adds a dedicated runtime, a Gateway, a Knowledge Base on S3 Vectors and a Cedar policy engine). Default AgentCore and CodeBuild quotas are enough for one participant per account
- CodeBuild ARM `BUILD_GENERAL1_LARGE` builds: 1 for the console, 1 per build deploy or destroy

### Participant Prerequisites
- A modern browser, with a private window or second browser for the second user (Bob)
- Self-paced in their own account: a sandbox account with administrator access

---

## Recommended Agenda

| Time | Module | Duration | Notes |
|---|---|---|---|
| 0:00 | Welcome and Set up | 20 min | Participants copy ConsoleUrl, UserAlice, UserBob and WorkshopPassword from the event outputs |
| 0:20 | Multi-agent architecture | 20 min | Presenter-led; console already deployed |
| 0:40 | Lab 1: Trip Planner | 90 min | Assistant 20, canvas 15, library/sharing 15, deploy 10, runs 20, observability 10 |
| 2:10 | Break | 10 min | Start the version 2 deploy before the break |
| 2:20 | Lab 2: Blog Post Studio | 85 min | Build 30, deploy 10 (read Under the hood meanwhile), tests 35 |
| 3:45 | Clean up and Try your own | 15 min | Destroy builds from the console |

---

## Delivery Tips

### Setup & Environment
- The console is pre-deployed by Workshop Studio. If a participant's stack failed, the CodeBuild log stays in CloudWatch Logs (`/aws/codebuild/agentexpress-workshop-console`) after the rollback. Fix the cause the log names, delete `agentexpress-stack` if it is left in `ROLLBACK_COMPLETE`, then create the template's stack again
- If the CodeBuild project still exists, `aws codebuild start-build --project-name agentexpress-workshop-console` redeploys the console; it recreates the users and writes a new password to the SSM parameter `/agentexpress/workshop/password`
- Ask participants to keep two tabs open: the console (Build) and the deployed app

### Facilitation Strategies
- Use the 7-minute deploys: in Lab 1 show the Activity page; in Lab 2 participants read "Under the hood"
- Participants do not need the AWS Management Console; the event outputs give them everything
- Encourage participants to read the **workflow.json** tab after each Assistant message; it makes the "one file" idea concrete
- The Assistant's agent ids and field names vary between participants. Branch rules must use the intake agent's actual **Output shape** field names
- Run Lab 2 tests in order (A, B, C) as the same user, with about a minute between A and B for memory processing

---

## Troubleshooting

### Top Issues

#### 1. Deploy or run fails with a model access error
**Cause:** Bedrock model not invocable in the account (often Anthropic use-case form, or SD3.5 Large in us-west-2).
**Fix:** In the participant's account, open Amazon Bedrock → Model catalog, open the model and complete any access prompt (in us-west-2 for SD3.5 Large), then deploy again or start a new run.

#### 1b. The Assistant fails with "AccessDeniedException ... private marketplace eligibility"
**Cause:** The account's AWS Marketplace private marketplace does not allow the Assistant's model (Claude Sonnet 5.5 by default). No role inside the account can fix this.
**Fix:** Ask the private marketplace administrator to allow the model, or deploy the template with the `DesignerModel` parameter set to an allowed model, for example `us.anthropic.claude-sonnet-5`.

#### 1c. A Terraform deploy fails creating the Gateway: "not authorized to perform: bedrock-agentcore:AuthorizeAction"
**Cause:** IAM had not yet propagated the Gateway role's new policy when Terraform created the Gateway.
**Fix:** Choose **Deploy** again with Terraform; the state is kept, so it resumes. The framework now waits 25 seconds before creating the Gateway (GitHub commit `c72035b`), so this should not recur.

#### 2. Approve / Revise / Deny buttons are greyed out
**Cause:** The user is in no group.
**Fix:** `aws cognito-idp admin-add-user-to-group --user-pool-id <pool> --username <email> --group-name members`, then sign out and back in.

#### 3. Header shows errors and Deploy is disabled
**Cause:** A validation problem (for example a Cedar policy that references tool input on a whole-target action, or a branch field that does not exist).
**Fix:** Open **Problems** and send the message back to the Assistant, for example `Problems flags the Acme policy, please fix it.`

#### 4. Deploy fails: corpus has no documents
**Fix:** Upload `travel-policy.md` or `brand-style-guide.md` to the corpus named in **Needs your input**, then deploy again.

#### 5. Bob does not see the shared build
**Fix:** Check the email in the Share dialog matches Bob's sign-in exactly, then reload Bob's console.

### Module-Specific Issues

| Module | Symptom | Fix |
|---|---|---|
| Lab 1 runs | Branch never fires | The rule's `field` does not match the intake output; check **Output shape** |
| Lab 1 runs | Re-run button missing | Re-run is available only after the run settles |
| Lab 2 Prompt 1 | Web search tool missing from **Add from library** | It was not published in Lab 1 step 3; publish it from the Trip Planner's Tools tab, or keep the Assistant's own web search tool |
| Lab 2 Test A | No cover image | SD3.5 Large not enabled in us-west-2 |
| Lab 2 Test A | No eval score | Choose **Evaluate** on the editor in Observability; scores render in the agent's Prompts & I/O view |
| Lab 2 Test B | Run fails with `ToolDenied ... denied by the Cedar policy engine` | The researchers are in tool mode *direct*, which fails closed on a refusal. Ask the Assistant to let both researchers choose their own tool calls (tool mode *model*), or set **Tool mode** to *model* on each researcher, then deploy again |
| Lab 2 Test B | The fact researcher's Cedar refusal is in Observability but not on the Timeline | A dedicated agent's timeline lines were not returned to the run. Fixed in the framework (GitHub commit `808e249`); redeploy the build |
| Lab 2 Test B | Memory recall shows *(no matching memories)* | Before GitHub commit `808e249`, the request (with its tone) was not stored, so no preference was ever extracted; redeploy the build, then run Test A again before Test B |
| Lab 2 Test B | Memory not recalled | Wait a minute after Test A; run as the same user |
| Lab 2 Test B | Agent fails with ToolUnavailable | A Cedar `forbid` without a condition hides the tool; ask the Assistant to scope the rule to queries that mention Acme |

### When Nothing Works
- Check the build's **Log** in the Deployment panel, or **Full log in CloudWatch**
- Check the `ax-…-stack` events in CloudFormation
- As a last resort, **Destroy** the build and deploy again

---

## Resources

### For Facilitators
- AgentExpress repository (UI branch): https://github.com/praven80/AgentExpress/tree/UI — DEPLOYMENT.md has CDK flags, users and groups, teardown and troubleshooting

### For Participants (share after the event)
- https://github.com/praven80/AgentExpress/tree/UI
- Amazon Bedrock AgentCore Developer Guide: https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/what-is-bedrock-agentcore.html

---

## Post-Event Actions
- Workshop Studio accounts are cleaned up automatically
- Remind self-paced participants to follow the Clean up page: destroy builds first, then the console
