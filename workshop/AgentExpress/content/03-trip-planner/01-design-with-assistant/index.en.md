---
title: "1. Design with the Assistant"
weight: 31
---

**Time:** 20 minutes | **Where:** Control Plane | **User:** Alice

The **AgentExpress Assistant** designs the workflow with you. Describe your process the way you would explain it to a colleague. The Assistant writes the agents, prompts, tools, stages and review gates, and asks for anything only you can supply. It never deploys anything; that decision stays with you.

## Create the build

1. Choose **+** next to **Build** in the left navigation.
2. Select the name *My workflow* in the header and rename it to `Trip Planner`.

In the chat, **Enter** sends a prompt and **Shift+Enter** adds a new line. You can undo any change the Assistant makes with **Undo** in the header.

## Prompt 1: the pipeline

Paste this prompt and send it. Notice that it describes a process, not a technical specification.

:::code{language=text showCopyAction=true}
I want a Trip Planner for our travel team. Someone types where they want to go, for how many days, how many travellers, their budget and home currency, and the pipeline produces a ready-to-share travel brief.

An intake agent turns the request into a trip brief: destination, number of days, travellers, total budget, home currency, interests. If no destination is given, end the run. Let me review the brief before anything else runs.

Then two researchers in parallel:
- a destination researcher searches the live web for the top sights, best areas to stay, local tips and typical daily costs, with sources;
- a currency researcher calls the free Frankfurter API for today's exchange rate from the home currency to the destination's currency - GET https://api.frankfurter.app/latest?from=<home currency>&to=<destination currency>, no key needed. Describe it as an OpenAPI tool, and it must call it every run.
Let me review the research before the planning starts.

Then in sequence:
- an itinerary writer drafts a day-by-day plan from the research;
- a budget checker calls a small Lambda you write: it takes the daily cost per person, number of days, travellers, total budget and exchange rate, and returns the total trip cost in both currencies and whether it fits the budget. It must call it once every run.

Finally a travel brief agent combines everything into a short, friendly brief: summary, itinerary, budget check, top tips and sources.

Give each agent 4000 tokens, and the itinerary writer and travel brief 8000.
:::

While it works, the Assistant shows what it is doing: *Thinking…*, *Writing…*, *Updating the build…* and *Checking the change…*. After a minute or two, the preview shows six agents in four stages, with the review gates and the branch rule you asked for.

Behind the scenes, the Assistant has written a system prompt and an output format for each agent, described the Frankfurter API as an OpenAPI tool, written the code for the budget Lambda function, and checked the result against the same rules the deploy uses.

Check the two cards beside the chat:

- **Defaults used** lists what the Assistant chose for you, such as the model.
- **Needs your input** lists what only you can supply. The deploy waits until this list is empty.

If the Assistant asks you a question, answer it in the chat.

## Prompt 2: the knowledge base

:::code{language=text showCopyAction=true}
Also add a knowledge base: I'll upload our company travel policy. The itinerary writer and the travel brief must follow it, and the budget checker should flag anything the policy doesn't allow.
:::

The Assistant adds a knowledge base tool and connects it to the three agents you named. A knowledge base lets agents search your own documents, so their answers follow your rules rather than general knowledge. **Needs your input** now asks for the documents, because only you can provide them.

Upload the policy:

1. Choose **Build manually**, then the **Tools** tab, and select the knowledge base tool.
2. In the **Documents** section, choose **Upload** and select `travel-policy.md`.
3. The file appears in the table with its size. At deploy time, AgentExpress indexes it into an Amazon Bedrock knowledge base.

## Prompt 3: refinements

Real requirements change as you go, and you can keep refining in plain English. Go back to the **AgentExpress Assistant** tab and send:

:::code{language=text showCopyAction=true}
Three tweaks: the itinerary writer should keep each day to at most three activities; the budget checker must use the typical daily cost per person from the destination researcher, not a reduced estimate, so the budget check stays honest; and if the budget checker says the trip is over budget or breaks the travel policy, the travel brief must suggest two concrete ways to fix it.
:::

The Assistant updates only the affected agents' prompts and highlights them in the preview. Everything else stays as it was.

## Check your work

- [ ] The header shows **Valid**. If it shows a count instead, open **Problems** and paste the message back to the Assistant, for example `Problems shows <message>, please fix it.`
- [ ] **Needs your input** is empty.

## Look at what you did not have to write

Open **Build manually** and see what the Assistant produced from your three prompts:

| Where to look | What you find |
|---|---|
| **Design**, then select any agent | A complete system prompt and a structured output format. |
| **Tools**, then **Edit** on the Frankfurter tool | An OpenAPI description of the API, written from one sentence. |
| **Tools**, then **Edit** on the budget tool | The Python code of the Lambda function, which AgentExpress deploys for you. |
| **workflow.json** | The single file that describes the whole workflow. |

## Which agent uses which tool

Select each agent on the **Design** tab to see its tools in the inspector. The Assistant names the tools itself, so your names may differ slightly from the examples below.

| Stage | Agent | Tools | Tool type and example name |
|---|---|---|---|
| 1 | Intake | None | It only reads the request. |
| 2 (parallel) | Destination researcher | Web search | AgentCore Web Search, for example `destinationSearch` |
| 2 (parallel) | Currency researcher | Frankfurter exchange-rate API | OpenAPI, for example `currencyApi` |
| 3 (sequence) | Itinerary writer | Travel policy | Knowledge base, for example `travelPolicyKb` |
| 3 (sequence) | Budget checker | Budget calculator and travel policy | Lambda function, for example `budgetCalculator`, and the knowledge base |
| 4 | Travel brief | Travel policy | Knowledge base |

Every tool call goes through AgentCore Gateway, and each agent can reach only the tools listed here.

:::alert{type="success" header="What you just did"}
Three prompts produced six agents, a parallel stage, two review gates, a branch rule, web search, an external API, a Lambda function and a knowledge base.
:::
