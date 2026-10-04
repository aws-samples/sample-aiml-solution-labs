---
title: "Try your own use case"
weight: 60
---

**Time:** as long as you like | **Where:** Control Plane, then Execution Plane

The best way to see what AgentExpress can do for your team is to describe one of your own processes. Start from one of the examples below, or write your own. Paste it into a new build's Assistant, deploy it, and run the test prompts.

:::alert{type="info" header="How to write a good first prompt"}
- Say who uses the workflow and what it produces.
- Name each agent and what it hands to the next one. Say which agents run **in parallel**, and where you want to **review** the work.
- For each tool, give its type and URL, and say whether it must be called on every run.
- Give token limits to agents that write long output.
- Put secrets in the secure field when the Assistant asks for them, never in the chat.
:::

## 1. Customer support triage (branching)

:::code{language=text showCopyAction=true}
I want a Support Triage workflow. A support agent pastes a customer email. A classifier reads it and returns category (billing, technical, account, other), urgency (low, medium, high) and the customer's main question. If urgency is high, skip straight to a human-handoff agent that writes an internal escalation note, and end the run. Otherwise a knowledge base researcher looks up our help articles (I'll upload them to the knowledge base) and a reply writer drafts a friendly answer citing the articles. Let me review the draft before the run ends.
:::

When the Assistant asks for your help articles, upload this sample file to the knowledge base corpus it names (for example `help_articles`): choose **Build manually → Tools**, select the knowledge base tool, and in **Documents** choose **Upload**.

:button[Download help-articles.md]{href="/static/kb/help-articles.md" action=download}

::::expand{header="What is in help-articles.md"}
:::code{language=markdown showCopyAction=false}
# Brightpath Help Center

## Billing: I was charged twice
If you see two charges for the same invoice, the second is usually a pending authorisation that drops off within 3 business days. If both charges are still there after 3 business days, we refund the duplicate in full within 5 business days. Reply with the invoice number and the last four digits of the card.

## Billing: refunds
Refunds go back to the original payment method. Card refunds take 5 to 10 business days to appear, depending on your bank. We do not refund partial months after you cancel, but you keep access until the end of the billing period.

## Billing: updating your payment method
Go to Settings > Billing > Payment method and choose Replace card. The new card is used from the next invoice. Failed payments are retried automatically after 3 days.

## Technical: the site is slow or down
Check status.brightpath.example for incidents first. If there is no incident, clear your browser cache and try a private window. If the problem continues, send the time it started, your browser and a screenshot of any error.

## Technical: an update broke something
After a release, most issues come from cached files. Refresh with Ctrl+Shift+R (Cmd+Shift+R on Mac). If the problem is still there, we can roll back your workspace to the previous version within 1 hour while we investigate.

## Account: resetting your password
On the sign-in page, choose Forgot password and enter your email address. The reset link is valid for 30 minutes. If no email arrives, check your spam folder or ask your workspace admin to resend the invite.

## Account: adding or removing users
Workspace admins can add users in Settings > Team > Invite. Removed users lose access immediately; their content stays in the workspace. Billing changes at the next invoice.

## Response times
Standard support replies within 1 business day. Urgent issues that stop your business (the site is down, payments fail for all customers) are answered within 1 hour, 24 hours a day.
:::
::::

| Test prompt | What it shows |
|---|---|
| `Hi, I was charged twice for my March invoice, can you refund one?` | Normal path: KB retrieval, review gate on the draft |
| `Our whole production site is down since your update an hour ago, we are losing orders!` | Branch: high urgency jumps to the escalation note and ends |

## 2. Competitive brief (parallel research)

:::code{language=text showCopyAction=true}
I want a Market Brief workflow for our product team. Someone names a product category and region. A planner lists 3 questions to answer. Then three researchers in parallel: one searches the web for recent news, one for pricing, one for customer reviews, each with sources. Let me review the research. Then an analyst writes a one-page brief: market summary, pricing table, top 3 customer complaints, and 3 opportunities for us.
:::

| Test prompt | What it shows |
|---|---|
| `Electric cargo bikes in Germany` | Three-way parallel web research and per-agent review |

## 3. Expense report check (Lambda + knowledge base + guardrail)

:::code{language=text showCopyAction=true}
I want an Expense Checker. An employee pastes their expense lines. An intake agent turns them into a list of items with date, category, amount and currency. A policy checker uses a knowledge base with our expense policy (I'll upload it) and flags anything not allowed. A calculator calls a small Lambda you write that totals the items per category and flags any item over a limit I pass in. A summary agent writes an approval recommendation. Let me review before the summary. Add a guardrail that anonymises email addresses and phone numbers in the intake input.
:::

When the Assistant asks for your expense policy, upload this sample file to the corpus it names, the same way as in example 1.

:button[Download expense-policy.md]{href="/static/kb/expense-policy.md" action=download}

::::expand{header="What is in expense-policy.md"}
:::code{language=markdown showCopyAction=false}
# Brightpath Expense Policy

- Meals: up to 75 USD per person per day. Team meals need a manager's approval and a list of attendees.
- Alcohol is not reimbursed.
- Hotels: up to 200 USD per night in major cities, 150 USD elsewhere.
- Taxis and ride-hailing: allowed when public transport is not practical. Keep the receipt.
- Any single item over 500 USD needs written pre-approval.
- Expenses must be submitted within 30 days, with an itemised receipt for anything over 25 USD.
- Gifts to clients: up to 50 USD per person per year.
:::
::::

| Test prompt | What it shows |
|---|---|
| `Team dinner 2026-09-12 meals 420 USD; taxi 38 USD; hotel 2 nights 610 USD. Call me at 555-0100.` | Lambda totals, policy flags from the KB, PII anonymised |


## 4. Release notes writer (MCP + memory)

:::code{language=text showCopyAction=true}
I want a Release Notes workflow. Someone pastes a list of changes. A classifier groups them into features, fixes and breaking changes. A fact checker checks any claim about AWS services against the AWS Knowledge MCP server at https://knowledge-mcp.global.api.aws using its aws___search_documentation tool with the search_phrase argument. A writer drafts customer-facing release notes and remembers each user's preferred tone and format across runs. Let me review the draft.
:::

| Test prompt | What it shows |
|---|---|
| `Added Amazon Bedrock AgentCore Memory support; fixed timeout on large uploads; removed the v1 API. Tone: short and upbeat, bullet points.` | MCP calls, memory stored |
| `Added CSV export; fixed login loop on Safari.` (one minute later) | Memory recalled: same tone and format without being asked |

## 5. Interview kit (sequence + evaluation)

:::code{language=text showCopyAction=true}
I want an Interview Kit workflow for hiring managers. Someone pastes a job description. A role analyst extracts the 5 most important skills. Then in sequence: a question writer writes 2 behavioural and 2 technical questions per skill, and a rubric writer writes a 1-to-4 scoring rubric for each question. Let me review the questions before the rubric. Add a custom evaluator called fairness that scores whether the questions avoid personal or protected-characteristic topics, used on the question writer.
:::

| Test prompt | What it shows |
|---|---|
| `Senior data engineer: Python, Spark, AWS Glue, data modelling, mentoring juniors.` | Sequence, review, custom evaluation score in Observability |

## Take it further

- **Export** a build (**Export → Bundle**) and apply it in a clone of the repository with `python3 scaffold.py apply <bundle>.json --exact`, then deploy it with CDK or Terraform from your own pipeline.
- In Kiro, open the repository and use the `agentexpress-author` skill to build a workflow from a description, in code.
- Read [WORKFLOW_REFERENCE.md](https://github.com/praven80/AgentExpress/blob/UI/orchestrator/docs/WORKFLOW_REFERENCE.md) for every `workflow.json` key.
