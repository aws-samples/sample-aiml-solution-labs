---
title: "Lab 1: Trip Planner"
weight: 30
---

**Time:** 90 minutes

Your travel team wants a **Trip Planner**. Someone types where they want to go, for how many days, how many travellers, their budget and home currency. A few minutes later they get a travel brief that follows the company travel policy and says whether the trip fits the budget.

In this lab you build the Trip Planner with three prompts, refine it on the canvas, share it with a colleague, deploy it, and run it with a human approving each stage.

![Trip Planner workflow](/static/images/trip-planner.png)

| Agent | What it does | Tool |
|---|---|---|
| Intake | Turns the request into a trip brief, and ends the run if there is no destination. | None |
| Destination researcher | Finds the top sights, areas to stay, local tips and daily costs. | Web search |
| Currency researcher | Gets today's exchange rate. | Frankfurter API (OpenAPI) |
| Itinerary writer | Writes a day-by-day plan that follows the policy. | Travel policy (knowledge base) |
| Budget checker | Calculates the total cost and checks it against the budget and the policy. | Lambda function and travel policy |
| Travel brief | Writes the final brief, with fixes if the trip is over budget. | Travel policy (knowledge base) |

## Steps

| Step | Where | What you do | Time |
|---|---|---|---|
| [1. Design with the Assistant](/03-trip-planner/01-design-with-assistant) | Control Plane | Build the whole workflow with three prompts. | 20 min |
| [2. Build on the canvas](/03-trip-planner/02-build-manually) | Control Plane | Mix Strands and LangGraph agents, add a branch rule, and try Undo and Redo. | 15 min |
| [3. Library and sharing](/03-trip-planner/03-library-and-sharing) | Control Plane | Publish tools to the Library, and share the workflow with Bob, who edits it. | 15 min |
| [4. Deploy](/03-trip-planner/04-deploy) | Control Plane | Deploy version 1 as its own Execution Plane. | 10 min |
| [5. Run and review](/03-trip-planner/05-run-and-review) | Execution Plane | Approve, revise, deny, branch, re-run and stop runs. | 20 min |
| [6. Observe and improve](/03-trip-planner/06-observability) | Execution Plane | See cost, prompts and tool calls, chat with your runs, and ship version 2. | 10 min |

## File you need

You upload the company travel policy to a knowledge base in step 1. Download it now:

:button[Download travel-policy.md]{href="/static/kb/travel-policy.md" action=download}

This is what the file contains:

:::code{language=markdown showCopyAction=false}
# Brightpath Travel Policy

- Hotels: up to 200 USD per person per night in major cities, 150 USD elsewhere. No 5-star hotels.
- Flights: economy only for trips under 6 hours.
- Meals: up to 75 USD per person per day.
- Activities: at most one paid activity over 100 USD per person per trip.
- Every trip needs at least one free day or half-day with no bookings.
- Travel insurance is required for every international trip.
:::
