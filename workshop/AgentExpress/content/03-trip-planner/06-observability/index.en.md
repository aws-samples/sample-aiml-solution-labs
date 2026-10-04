---
title: "6. Observe and improve"
weight: 36
---

**Time:** 10 minutes | **Where:** Execution Plane, then Control Plane | **User:** Alice

When an agent gives an answer you did not expect, you need to see what it was told, which tools it called and what they returned. Observability shows all of that, with the cost of every call. Then you improve the workflow and ship a new version.

## Explore Observability

1. Choose **Observability** and set the time range to **Last 1 hour**.
2. **Overview** shows cost and latency by date, model and user, the cost of each agent, and a cost projection based on your average run.
3. Open **Run detail** for the Lisbon run. It lists tokens, latency and cost for each agent, including the re-runs.
4. Choose **Prompts & I/O** to open the inspector. For each agent you can read:
   - the exact system prompt and user prompt sent to the model, and the model's output;
   - every tool call with its input and result, such as the web searches, the Frankfurter exchange rate, the travel policy retrievals, and the Lambda function's input and output;
   - guardrail and policy decisions, which you see more of in Lab 2.
5. Use **⬇ CSV** or **⬇ JSON** to download the data for your own analysis.

Can you find which agent cost the most, and why?

## Chat with your runs

Open the **Assistant** with the button in the corner of the page, and ask:

:::code{language=text showCopyAction=true}
Summarise this run in three bullets. What did the budget checker conclude?
:::

:::code{language=text showCopyAction=true}
Which of my runs cost the most, and which agent drove that cost?
:::

The Run Assistant answers from your real run data. It can also act for you, through the same permissions as the buttons. Start a new run with this request:

:::code{language=text showCopyAction=true}
3 days in Barcelona for 2 people, budget 1800 EUR. We love architecture and tapas.
:::

When it pauses at the **intake gate** (the first review gate), ask the Assistant:

:::code{language=text showCopyAction=true}
Approve the intake gate on my latest run.
:::

It acts with your permissions, and the **Activity** page records the action.

## Ship version 2

1. Go back to the Control Plane, open Trip Planner, and ask the AgentExpress Assistant:

   :::code{language=text showCopyAction=true}
   Add a packing tip for the destination's weather to the travel brief.
   :::

2. Choose **Deploy** again, with **AWS CDK**, the same tool as version 1. You can start Lab 2 while it runs.
3. When the panel shows **Version 2 deployed**, start a new run: its travel brief now includes a packing tip. This change only edits a prompt, so the graph looks the same. Runs you started before keep the outputs they produced with version 1.

:::alert{type="success" header="Lab 1 complete"}
You took a workflow through its full lifecycle: design, share, deploy, run, review, observe and improve, without writing code.
:::
