---
title: "2. Build on the canvas"
weight: 32
---

**Time:** 15 minutes | **Where:** Control Plane | **User:** Alice

The chat and the canvas edit the same workflow, so you can switch between them at any time. Use the chat when it is faster to describe a change, and the canvas when you want to see and adjust the details. Choose **Build manually**, then **Design**.

## Tour the canvas

1. Each stage shows as *Parallel* or *Sequence*, with **Review gate** and **Branch** badges where they apply.
2. Select the research stage. Its **Shape** is *Parallel*, and its **Review gate** is on.
3. Select any agent. The inspector shows its **Prompt**, its **model**, and its **AgentCore features**: guardrail, memory, evaluations, identity and Cedar policy.

To add an agent, drag **New agent** onto a stage to run it in parallel, or between two stages to add a new step. To give an agent a tool, drag the tool onto the agent.

## Mix Strands and LangGraph agents

Teams often have agents written with different frameworks. AgentExpress lets each agent choose its own, and still runs them all in one workflow. Select each agent and set **Agent framework**:

| Agent | Agent framework |
|---|---|
| Intake | No framework |
| Destination researcher | **Strands Agents** |
| Currency researcher | **Strands Agents** |
| Itinerary writer | **LangGraph** |
| Budget checker | **Strands Agents** |
| Travel brief | **LangGraph** |

Open the **workflow.json** tab to see `"framework": "strands"` and `"framework": "langgraph"` on those agents. Guardrails, memory, tools and observability work the same way whichever framework an agent uses, because every model call and tool call still goes through AgentExpress.

## Add a branch rule

A branch rule lets an agent's output decide what runs next. It can skip a stage, jump ahead, or end the run. Agents that a branch skips are marked **skipped** and cost nothing.

1. Select the **intake** stage and look at **Branch**. The Assistant already wrote a rule that ends the run when there is no destination.
2. Add a second rule, so that trips longer than 14 days end the run for a human to plan. Use the field names from the intake agent's **Output shape**, for example `days`:

   :::code{language=json showCopyAction=true}
   {"when": [
     {"field": "destination", "exists": false, "goto": "END"},
     {"field": "days", "gt": 14, "goto": "END"}
   ]}
   :::

3. Check that the header still shows **Valid**.

::::expand{header="Branch operators"}
- **Operators:** `equals`, `notEquals`, `in`, `contains`, `exists`, `gt`, `gte`, `lt` and `lte`.
- **Order:** rules are checked in order, and the first match wins.
- **No match:** the run continues to the next step.
- **Target:** `goto` names a later step, or `END` to finish the run.
::::

## Try Undo and Redo

Undo and Redo cover everything: your canvas edits, the Assistant's changes and your uploads. You can experiment freely.

1. Select the **budget checker** and choose **Delete**. The travel brief loses one of its inputs, and the canvas updates.
2. Choose **Undo** in the header. The budget checker comes back with its tool and prompt.
3. Choose **Redo**, then **Undo** again. Make sure the budget checker is on the canvas before you continue.

:::alert{type="success" header="What you just did"}
You put five agents on two different frameworks, added a branch rule by hand, and recovered a deleted agent, all without writing code.
:::
